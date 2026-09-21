"""AWS-independent full-checksum audit of explicitly inventoried curated months."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from typing import Any

from metropulse.audit_inventory import inventory_id, validate_inventory
from metropulse.audit_models import (
    AuditBounds,
    AuditInventory,
    AuditMonth,
    AuditMonthResult,
    CuratedObject,
    PartitionCatalogReader,
    PinnedObject,
    ScheduledAuditResult,
)
from metropulse.aws.storage import ObjectNotFound, ObjectStorage, StoredObject
from metropulse.compaction_identity import monthly_run_id
from metropulse.continuity import (
    ContinuityEndpoint,
    compose_significant_gap_count,
    evaluate_boundaries,
)
from metropulse.curated_parquet import sha256_bytes, verify_curated_bytes
from metropulse.selection_approval import parse_selection_document


def audit_curated_inventory(
    inventory: AuditInventory,
    *,
    storage: ObjectStorage,
    catalog: PartitionCatalogReader,
    bounds: AuditBounds | None = None,
) -> ScheduledAuditResult:
    """Fully validate exact inventory objects and current Glue partition visibility."""
    limits = bounds or AuditBounds()
    months = validate_inventory(inventory)
    identity = inventory_id(inventory)
    fatal: list[str] = []
    reconciliation: list[str] = []
    drift: list[str] = []
    quality: list[dict[str, Any]] = []
    results: list[AuditMonthResult] = []
    total_bytes = sum(item.curated.byte_size for item in months)
    if len(months) > limits.max_months:
        fatal.append("inventory exceeds the configured month limit")
    if total_bytes > limits.max_total_bytes:
        fatal.append("inventory exceeds the configured total-byte limit")
    if inventory.expected_total_curated_rows > limits.max_total_rows:
        fatal.append("inventory exceeds the configured row limit")

    endpoints: list[ContinuityEndpoint] = []
    total_rows = 0
    within_gaps = 0
    for month in months:
        month_failures: list[str] = []
        label = f"{month.year:04d}-{month.month:02d}"
        try:
            if month.curated.byte_size > limits.max_object_bytes:
                raise ValueError("curated object exceeds the configured object limit")
            selection_bytes = _read_pinned(storage, month.selection).body
            observed_run_id, selection = parse_selection_document(selection_bytes)
            if observed_run_id != month.run_id or monthly_run_id(selection) != month.run_id:
                raise ValueError("approved selection run ID mismatch")
            completion = _json_object(_read_pinned(storage, month.completion).body, "completion")
            publication = _json_object(_read_pinned(storage, month.publication).body, "publication")
            curated = _read_curated(storage, month.curated)
            evidence = verify_curated_bytes(curated.body, expected_rows=month.curated_row_count)
            for finding in _validate_completion(
                month, completion, inventory.compaction_manifest_version
            ):
                reconciliation.append(f"{label}: {finding}")
            _validate_publication(month, publication)
            if (
                evidence.first_timestamp != month.first_timestamp
                or evidence.last_timestamp != month.last_timestamp
            ):
                raise ValueError("curated timestamp boundaries do not match inventory")
            if evidence.valid_timestamp_count != month.curated_row_count:
                raise ValueError("curated valid timestamp count does not reconcile")
            partition = catalog.get_partition(year=f"{month.year:04d}", month=f"{month.month:02d}")
            if partition is None:
                drift.append(f"{label}: Glue partition is missing")
            else:
                values = tuple(str(item) for item in partition.get("Values", []))
                location = partition.get("StorageDescriptor", {}).get("Location")
                if values != month.glue_values:
                    drift.append(f"{label}: Glue partition values differ")
                if location != month.glue_location:
                    drift.append(f"{label}: Glue partition location differs")
            if month.selected_row_count != evidence.row_count:
                reconciliation.append(f"{label}: selected rows differ from curated rows")
            total_rows += evidence.row_count
            within_gaps += month.within_month_gap_count
            endpoints.append(
                ContinuityEndpoint(
                    label=label,
                    partition_date=date(month.year, month.month, 1),
                    first_timestamp=evidence.first_timestamp,
                    last_timestamp=evidence.last_timestamp,
                )
            )
            if month.known_missing_dates:
                quality.append(
                    {
                        "kind": "known_missing_dates",
                        "month": label,
                        "values": [value.isoformat() for value in month.known_missing_dates],
                    }
                )
            if month.terminal_partial_month:
                quality.append({"kind": "terminal_partial_month", "month": label})
        except Exception as error:
            message = f"{label}: {error}"
            fatal.append(message)
            month_failures.append(str(error))
        results.append(
            AuditMonthResult(
                year=f"{month.year:04d}",
                month=f"{month.month:02d}",
                run_id=month.run_id,
                row_count=month.curated_row_count,
                byte_size=month.curated.byte_size,
                status="failed" if month_failures else "passed",
                failures=tuple(month_failures),
            )
        )

    if total_rows != inventory.expected_total_curated_rows:
        reconciliation.append("aggregate curated rows do not match inventory expectation")
    findings, distribution = evaluate_boundaries(tuple(endpoints))
    cross_gaps = sum(count for seconds, count in distribution.items() if seconds > 60)
    overlaps = sum(item.status == "overlap" for item in findings)
    reversed_count = sum(item.status == "reversed_overlap" for item in findings)
    for item in findings:
        if item.status != "normal":
            quality.append({"kind": "cross_month_boundary", **asdict(item)})
    global_gaps = compose_significant_gap_count(within_gaps, distribution)
    status = "failed" if fatal or reconciliation or drift else "passed"
    return ScheduledAuditResult(
        inventory_id=identity,
        month_results=tuple(results),
        fatal_failures=tuple(fatal),
        reconciliation_failures=tuple(reconciliation),
        publication_drift=tuple(drift),
        quality_observations=tuple(quality),
        months_inspected=len(results),
        total_curated_rows=total_rows,
        within_month_gap_count=within_gaps,
        cross_month_gap_count=cross_gaps,
        global_gap_count=global_gaps,
        overlap_count=overlaps,
        reversed_boundary_count=reversed_count,
        status=status,
    )


def _read_pinned(storage: ObjectStorage, reference: PinnedObject) -> StoredObject:
    version = reference.identity_value if reference.identity_kind == "version_id" else None
    try:
        stored = storage.get(reference.bucket, reference.key, version_id=version)
    except ObjectNotFound as error:
        raise ValueError(f"pinned object is missing: {reference.key}") from error
    if reference.identity_kind == "sha256":
        observed = sha256_bytes(stored.body)
    elif reference.identity_kind == "etag":
        observed = stored.head.etag
    else:
        observed = stored.head.version_id
    expected = (
        reference.identity_value.lower()
        if reference.identity_kind == "sha256"
        else reference.identity_value
    )
    if observed != expected:
        raise ValueError(f"pinned object identity mismatch: {reference.key}")
    return stored


def _read_curated(storage: ObjectStorage, reference: CuratedObject) -> StoredObject:
    try:
        stored = storage.get(reference.bucket, reference.key, version_id=reference.version_id)
    except ObjectNotFound as error:
        raise ValueError(f"curated object is missing: {reference.key}") from error
    if len(stored.body) != reference.byte_size or sha256_bytes(stored.body) != reference.sha256:
        raise ValueError("curated checksum or size mismatch")
    return stored


def _json_object(payload: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} evidence is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} evidence must be a JSON object")
    return value


def _validate_completion(
    month: AuditMonth, completion: dict[str, Any], manifest_version: str
) -> tuple[str, ...]:
    curated = completion.get("curated", {})
    expected = {
        "run_id": month.run_id,
        "year": f"{month.year:04d}",
        "month": f"{month.month:02d}",
        "first_valid_event_timestamp_local": _iso(month.first_timestamp),
        "last_valid_event_timestamp_local": _iso(month.last_timestamp),
        "manifest_version": manifest_version,
        "significant_gap_count": month.within_month_gap_count,
        "terminal_partial_month": month.terminal_partial_month,
    }
    if any(completion.get(name) != value for name, value in expected.items()):
        raise ValueError("completion evidence conflicts with inventory")
    reconciliation_findings: list[str] = []
    if completion.get("selected_row_count") != month.selected_row_count:
        reconciliation_findings.append("completion selected rows differ from inventory")
    if completion.get("quarantine_row_count") != month.quarantine_row_count:
        reconciliation_findings.append("completion quarantine rows differ from inventory")
    curated_expected = {
        "key": month.curated.key,
        "sha256": month.curated.sha256,
        "byte_size": month.curated.byte_size,
        "row_count": month.curated_row_count,
    }
    if not isinstance(curated, dict) or any(
        curated.get(name) != value for name, value in curated_expected.items()
    ):
        raise ValueError("completion curated reference conflicts with inventory")
    if completion.get("missing_dates") != [
        value.isoformat() for value in month.known_missing_dates
    ]:
        raise ValueError("completion missing-date findings conflict with inventory")
    reconciliation = completion.get("reconciliation", {})
    if (
        reconciliation.get("curated_equals_selected_valid") is not True
        or reconciliation.get("input_equals_valid_plus_quarantine") is not True
    ):
        reconciliation_findings.append("completion reconciliation is not successful")
    return tuple(reconciliation_findings)


def _validate_publication(month: AuditMonth, publication: dict[str, Any]) -> None:
    if (
        publication.get("contract") != "metropulse-compaction-publication-receipt-v1"
        or publication.get("run_id") != month.run_id
        or publication.get("location") != month.glue_location
        or publication.get("outcome") not in {"created", "updated", "no_op"}
        or not isinstance(publication.get("owner_token"), str)
        or not publication["owner_token"].strip()
    ):
        raise ValueError("historical publication receipt conflicts with inventory")
    try:
        timestamp = datetime.fromisoformat(str(publication["processing_timestamp"]))
    except (KeyError, ValueError) as error:
        raise ValueError("historical publication receipt conflicts with inventory") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("historical publication receipt conflicts with inventory")


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None

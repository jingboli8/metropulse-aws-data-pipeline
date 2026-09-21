"""Canonical scheduled-audit inventory identity and immutable publication."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

from metropulse.audit_models import (
    AuditContractError,
    AuditInventory,
    AuditMonth,
    CuratedObject,
    PinnedObject,
)
from metropulse.aws.storage import ConditionalWriteFailed, ObjectStorage
from metropulse.compaction_identity import canonical_json
from metropulse.compaction_manifest import COMPACTION_MANIFEST_VERSION
from metropulse.curated_parquet import sha256_bytes
from metropulse.schema import SCHEMA_VERSION

AUDIT_INVENTORY_CONTRACT = "metropulse-scheduled-audit-inventory-v1"
AUDIT_INVENTORY_VERSION = "1.0.0"
SUPPORTED_IDENTITY_KINDS = {"version_id", "sha256", "etag"}


@dataclass(frozen=True)
class PublishedInventory:
    """Immutable identity returned after conditional inventory publication."""

    bucket: str
    key: str
    inventory_id: str
    identity_kind: str
    identity_value: str
    created: bool


def inventory_payload(inventory: AuditInventory) -> dict[str, Any]:
    """Validate and serialize inventory material in canonical chronological order."""
    ordered = validate_inventory(inventory)
    return {
        "compaction_manifest_version": inventory.compaction_manifest_version,
        "expected_months": list(inventory.expected_months),
        "expected_total_curated_rows": inventory.expected_total_curated_rows,
        "inventory_version": inventory.inventory_version,
        "known_source_end": inventory.known_source_end.isoformat(),
        "known_source_start": inventory.known_source_start.isoformat(),
        "months": [_month_payload(item) for item in ordered],
        "schema_version": inventory.schema_version,
        "source_name": inventory.source_name.lower(),
    }


def inventory_id(inventory: AuditInventory) -> str:
    """Return the SHA-256 identity of canonical inventory material."""
    return sha256_bytes(canonical_json(inventory_payload(inventory)))


def inventory_document_bytes(inventory: AuditInventory) -> tuple[str, str, bytes]:
    """Return inventory ID, canonical key, and canonical document bytes."""
    identity = inventory_id(inventory)
    key = f"control/audit/source=metropt3/inventory_id={identity}/inventory.json"
    document = {
        "contract": AUDIT_INVENTORY_CONTRACT,
        "inventory": inventory_payload(inventory),
        "inventory_id": identity,
    }
    return identity, key, canonical_json(document) + b"\n"


def parse_inventory_document(payload: bytes) -> tuple[str, AuditInventory]:
    """Parse an exact canonical inventory document and validate its identity."""
    try:
        document = json.loads(payload.decode("utf-8", errors="strict"))
        if document.get("contract") != AUDIT_INVENTORY_CONTRACT:
            raise AuditContractError("unsupported audit inventory contract")
        identity = str(document["inventory_id"])
        inventory = inventory_from_payload(document["inventory"])
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, AuditContractError):
            raise
        raise AuditContractError("audit inventory is malformed") from error
    expected_id, _, canonical = inventory_document_bytes(inventory)
    if identity != expected_id:
        raise AuditContractError("audit inventory ID does not match canonical content")
    if payload != canonical:
        raise AuditContractError("audit inventory document is not canonical")
    return identity, inventory


def publish_inventory(
    inventory: AuditInventory, *, bucket: str, storage: ObjectStorage
) -> PublishedInventory:
    """Conditionally publish one immutable inventory without listing S3."""
    identity, key, body = inventory_document_bytes(inventory)
    checksum = sha256_bytes(body)
    created = True
    try:
        head = storage.put(
            bucket,
            key,
            body,
            content_type="application/json; charset=utf-8",
            content_encoding=None,
            metadata={"inventory-id": identity, "sha256": checksum},
            if_none_match=True,
        )
    except ConditionalWriteFailed:
        created = False
        existing = storage.get(bucket, key)
        if existing.body != body:
            raise AuditContractError("audit inventory key contains conflicting content") from None
        head = existing.head
    kind, value = ("version_id", head.version_id) if head.version_id else ("sha256", checksum)
    return PublishedInventory(bucket, key, identity, kind, str(value), created)


def validate_inventory(inventory: AuditInventory) -> tuple[AuditMonth, ...]:
    """Return canonical month order after complete inventory validation."""
    if inventory.source_name.lower() != "metropt3":
        raise AuditContractError("unsupported audit source")
    if inventory.inventory_version != AUDIT_INVENTORY_VERSION:
        raise AuditContractError("unsupported audit inventory version")
    if inventory.schema_version != SCHEMA_VERSION:
        raise AuditContractError("unsupported curated schema version")
    if inventory.compaction_manifest_version != COMPACTION_MANIFEST_VERSION:
        raise AuditContractError("unsupported compaction manifest version")
    if (
        inventory.known_source_start.tzinfo is not None
        or inventory.known_source_end.tzinfo is not None
    ):
        raise AuditContractError("source timestamps must remain timezone-naive")
    if inventory.known_source_start > inventory.known_source_end:
        raise AuditContractError("known source range is reversed")
    if inventory.expected_total_curated_rows < 0:
        raise AuditContractError("expected row count cannot be negative")
    expected_sequence = _month_sequence(inventory.known_source_start, inventory.known_source_end)
    if inventory.expected_months != expected_sequence:
        raise AuditContractError("expected months must cover the complete known source range")
    ordered = tuple(sorted(inventory.months, key=lambda item: (item.year, item.month)))
    labels = tuple(f"{item.year:04d}-{item.month:02d}" for item in ordered)
    if labels != inventory.expected_months:
        raise AuditContractError("month entries must exactly match ordered expected months")
    if len(labels) != len(set(labels)):
        raise AuditContractError("audit inventory contains duplicate months")
    if sum(item.curated_row_count for item in ordered) != inventory.expected_total_curated_rows:
        raise AuditContractError("inventory aggregate row count does not reconcile")
    for item in ordered:
        _validate_month(item)
    return ordered


def _validate_month(item: AuditMonth) -> None:
    label = f"{item.year:04d}-{item.month:02d}"
    if not 1 <= item.month <= 12 or len(item.run_id) != 64:
        raise AuditContractError(f"invalid month or run ID for {label}")
    if any(character not in "0123456789abcdef" for character in item.run_id):
        raise AuditContractError(f"run ID must be lowercase hexadecimal for {label}")
    for reference in (item.selection, item.completion, item.publication):
        _validate_pinned(reference)
    if item.curated.sha256 != item.curated.sha256.lower() or len(item.curated.sha256) != 64:
        raise AuditContractError(f"invalid curated checksum for {label}")
    if item.curated.byte_size <= 0 or item.curated_row_count < 0:
        raise AuditContractError(f"invalid curated evidence for {label}")
    if item.selected_row_count != item.curated_row_count:
        raise AuditContractError(f"selected and curated rows differ for {label}")
    if item.quarantine_row_count < 0 or item.within_month_gap_count < 0:
        raise AuditContractError(f"negative count for {label}")
    if item.glue_values != (f"{item.year:04d}", f"{item.month:02d}"):
        raise AuditContractError(f"Glue values do not match {label}")
    expected_path = f"/year={item.year:04d}/month={item.month:02d}/run_id={item.run_id}/"
    if not item.glue_location.startswith("s3://") or not item.glue_location.endswith(expected_path):
        raise AuditContractError(f"Glue location does not identify the approved run for {label}")
    expected_curated = expected_path + "part-00000.snappy.parquet"
    if not item.curated.key.endswith(expected_curated.lstrip("/")):
        raise AuditContractError(f"curated key does not identify the approved run for {label}")
    for timestamp in (item.first_timestamp, item.last_timestamp):
        if timestamp is not None and timestamp.tzinfo is not None:
            raise AuditContractError(f"timezone-aware boundary for {label}")
    if (
        item.first_timestamp is not None
        and item.last_timestamp is not None
        and item.first_timestamp > item.last_timestamp
    ):
        raise AuditContractError(f"reversed month boundary for {label}")
    if item.coverage_status not in {"complete", "known_missing_dates", "terminal_partial"}:
        raise AuditContractError(f"unsupported coverage status for {label}")
    if any(
        (value.year, value.month) != (item.year, item.month) for value in item.known_missing_dates
    ):
        raise AuditContractError(f"missing-date fact is outside {label}")


def _validate_pinned(reference: PinnedObject) -> None:
    if (
        not reference.bucket
        or not reference.key
        or reference.identity_kind not in SUPPORTED_IDENTITY_KINDS
        or not reference.identity_value
    ):
        raise AuditContractError("inventory contains an unpinned object reference")
    if reference.identity_kind == "sha256" and (
        len(reference.identity_value) != 64
        or reference.identity_value != reference.identity_value.lower()
        or any(value not in "0123456789abcdef" for value in reference.identity_value)
    ):
        raise AuditContractError("inventory contains an invalid SHA-256 identity")


def _month_payload(item: AuditMonth) -> dict[str, Any]:
    return {
        "completion": asdict(item.completion),
        "coverage_status": item.coverage_status,
        "curated": asdict(item.curated),
        "curated_row_count": item.curated_row_count,
        "first_timestamp": item.first_timestamp.isoformat() if item.first_timestamp else None,
        "glue_location": item.glue_location,
        "glue_values": list(item.glue_values),
        "known_missing_dates": [value.isoformat() for value in item.known_missing_dates],
        "last_timestamp": item.last_timestamp.isoformat() if item.last_timestamp else None,
        "month": f"{item.month:02d}",
        "publication": asdict(item.publication),
        "quarantine_row_count": item.quarantine_row_count,
        "run_id": item.run_id,
        "selected_row_count": item.selected_row_count,
        "selection": asdict(item.selection),
        "terminal_partial_month": item.terminal_partial_month,
        "within_month_gap_count": item.within_month_gap_count,
        "year": f"{item.year:04d}",
    }


def inventory_from_payload(value: Mapping[str, Any]) -> AuditInventory:
    """Create a validated inventory from an explicit reviewed JSON payload."""
    inventory = AuditInventory(
        source_name=str(value["source_name"]),
        known_source_start=datetime.fromisoformat(str(value["known_source_start"])),
        known_source_end=datetime.fromisoformat(str(value["known_source_end"])),
        expected_months=tuple(str(item) for item in value["expected_months"]),
        months=tuple(_month_from_payload(item) for item in value["months"]),
        expected_total_curated_rows=int(value["expected_total_curated_rows"]),
        schema_version=str(value["schema_version"]),
        compaction_manifest_version=str(value["compaction_manifest_version"]),
        inventory_version=str(value["inventory_version"]),
    )
    validate_inventory(inventory)
    return inventory


def _month_from_payload(value: Mapping[str, Any]) -> AuditMonth:
    return AuditMonth(
        year=int(value["year"]),
        month=int(value["month"]),
        run_id=str(value["run_id"]),
        selection=PinnedObject(**value["selection"]),
        completion=PinnedObject(**value["completion"]),
        publication=PinnedObject(**value["publication"]),
        curated=CuratedObject(**value["curated"]),
        glue_values=tuple(str(item) for item in value["glue_values"]),  # type: ignore[arg-type]
        glue_location=str(value["glue_location"]),
        selected_row_count=int(value["selected_row_count"]),
        curated_row_count=int(value["curated_row_count"]),
        quarantine_row_count=int(value["quarantine_row_count"]),
        first_timestamp=_timestamp(value.get("first_timestamp")),
        last_timestamp=_timestamp(value.get("last_timestamp")),
        within_month_gap_count=int(value["within_month_gap_count"]),
        coverage_status=str(value["coverage_status"]),
        known_missing_dates=tuple(
            date.fromisoformat(str(item)) for item in value["known_missing_dates"]
        ),
        terminal_partial_month=bool(value["terminal_partial_month"]),
    )


def _timestamp(value: object) -> datetime | None:
    return None if value is None else datetime.fromisoformat(str(value))


def _month_sequence(start: datetime, end: datetime) -> tuple[str, ...]:
    year, month = start.year, start.month
    values: list[str] = []
    while (year, month) <= (end.year, end.month):
        values.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return tuple(values)

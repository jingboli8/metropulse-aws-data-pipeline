"""Explicit monthly-selection approval without prefix discovery."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from metropulse.aws.storage import ConditionalWriteFailed, ObjectNotFound, ObjectStorage
from metropulse.compaction_identity import canonical_json, monthly_run_id
from metropulse.compaction_manifest import build_selection_document, compaction_keys
from metropulse.compaction_models import (
    ImmutableOutputConflict,
    MonthlySelection,
    SelectedDailyInput,
    SelectionError,
)
from metropulse.compaction_selection import validate_selection
from metropulse.curated_parquet import sha256_bytes


@dataclass(frozen=True)
class ApprovedSelection:
    """Immutable identity returned after conditional selection publication."""

    bucket: str
    key: str
    run_id: str
    identity_kind: str
    identity_value: str
    created: bool


def selection_document_bytes(selection: MonthlySelection) -> tuple[str, str, bytes]:
    """Return deterministic run ID, canonical key, and document bytes."""
    validate_selection(selection)
    run_id = monthly_run_id(selection)
    key = compaction_keys(selection.year, selection.month, run_id)["selection"]
    body = canonical_json(build_selection_document(selection, run_id)) + b"\n"
    return run_id, key, body


def parse_selection_document(payload: bytes) -> tuple[str, MonthlySelection]:
    """Parse and fully validate one canonical selection document."""
    try:
        document = json.loads(payload.decode("utf-8", errors="strict"))
        if document.get("contract") != "metropulse-monthly-selection-v1":
            raise SelectionError("unsupported selection contract")
        run_id = str(document["run_id"])
        selection = selection_from_payload(document["selection"])
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, SelectionError):
            raise
        raise SelectionError("selection document is malformed") from error
    expected_run_id, _, canonical = selection_document_bytes(selection)
    if run_id != expected_run_id:
        raise SelectionError("selection run ID does not match its canonical inputs")
    if payload != canonical:
        raise SelectionError("selection document is not canonical")
    return run_id, selection


def publish_approved_selection(
    selection: MonthlySelection,
    *,
    bucket: str,
    storage: ObjectStorage,
) -> ApprovedSelection:
    """Validate exact markers and conditionally publish one immutable selection."""
    for item in validate_selection(selection):
        validate_selected_completion(storage, item)
    run_id, key, body = selection_document_bytes(selection)
    checksum = sha256_bytes(body)
    created = True
    try:
        head = storage.put(
            bucket,
            key,
            body,
            content_type="application/json; charset=utf-8",
            content_encoding=None,
            metadata={"sha256": checksum, "run-id": run_id},
            if_none_match=True,
        )
    except ConditionalWriteFailed:
        created = False
        existing = storage.get(bucket, key)
        if existing.body != body or sha256_bytes(existing.body) != checksum:
            raise ImmutableOutputConflict(
                "approved selection key contains conflicting content"
            ) from None
        head = existing.head
    if head.version_id:
        kind, value = "version_id", head.version_id
    else:
        kind, value = "sha256", checksum
    return ApprovedSelection(bucket, key, run_id, kind, value, created)


def validate_selected_completion(storage: ObjectStorage, item: SelectedDailyInput) -> None:
    """Validate one exact completion-marker identity and selected evidence."""
    version = (
        item.completion_marker_identity_value
        if item.completion_marker_identity_kind == "version_id"
        else None
    )
    try:
        stored = storage.get(
            item.completion_marker_bucket, item.completion_marker_key, version_id=version
        )
    except ObjectNotFound as error:
        raise SelectionError("selected completion marker is missing") from error
    if item.completion_marker_identity_kind == "sha256":
        observed = sha256_bytes(stored.body)
        expected = item.completion_marker_identity_value.lower()
    elif item.completion_marker_identity_kind == "etag":
        observed, expected = stored.head.etag, item.completion_marker_identity_value
    else:
        observed, expected = stored.head.version_id, item.completion_marker_identity_value
    if observed != expected:
        raise SelectionError("selected completion marker identity mismatch")
    try:
        marker = json.loads(stored.body.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SelectionError("selected completion marker is malformed") from error
    expected_fields = {
        "processing_identity": item.processing_identity,
        "source_date": item.source_date.isoformat(),
        "input_row_count": item.input_row_count,
        "valid_row_count": item.valid_row_count,
        "quarantine_row_count": item.quarantine_row_count,
        "pipeline_version": item.pipeline_version,
        "schema_version": item.schema_version,
        "manifest_version": item.manifest_version,
        "first_valid_event_timestamp_local": _iso(item.first_valid_timestamp),
        "last_valid_event_timestamp_local": _iso(item.last_valid_timestamp),
        "valid_timestamp_count": item.valid_row_count,
    }
    staging = marker.get("staging")
    if any(marker.get(name) != value for name, value in expected_fields.items()) or not isinstance(
        staging, dict
    ):
        raise SelectionError("selected completion marker conflicts with reviewed evidence")
    expected_staging = {
        "bucket": item.staging_bucket,
        "key": item.staging_key,
        "sha256": item.staging_sha256,
        "byte_size": item.staging_byte_size,
    }
    if any(staging.get(name) != value for name, value in expected_staging.items()):
        raise SelectionError("selected staging evidence conflicts with completion marker")


def selection_from_payload(value: Mapping[str, Any]) -> MonthlySelection:
    """Create a validated selection from an explicit reviewed JSON payload."""
    inputs = tuple(_input_from_payload(item) for item in value["inputs"])
    selection = MonthlySelection(
        source_name=str(value["source_name"]),
        year=int(value["year"]),
        month=int(value["month"]),
        inputs=inputs,
        expected_raw_dates=tuple(
            date.fromisoformat(str(item)) for item in value["expected_raw_dates"]
        ),
        known_source_start=date.fromisoformat(str(value["known_source_start"])),
        known_source_end=date.fromisoformat(str(value["known_source_end"])),
        terminal_partial_month=bool(value["terminal_partial_month"]),
    )
    validate_selection(selection)
    return selection


def _input_from_payload(value: Mapping[str, Any]) -> SelectedDailyInput:
    marker = value["completion_marker"]
    staging = value["staging"]
    return SelectedDailyInput(
        source_date=date.fromisoformat(str(value["source_date"])),
        processing_identity=str(value["processing_identity"]),
        completion_marker_bucket=str(marker["bucket"]),
        completion_marker_key=str(marker["key"]),
        completion_marker_identity_kind=str(marker["identity_kind"]),
        completion_marker_identity_value=str(marker["identity_value"]),
        staging_bucket=str(staging["bucket"]),
        staging_key=str(staging["key"]),
        staging_sha256=str(staging["sha256"]),
        staging_byte_size=int(staging["byte_size"]),
        input_row_count=int(value["input_row_count"]),
        valid_row_count=int(value["valid_row_count"]),
        quarantine_row_count=int(value["quarantine_row_count"]),
        first_valid_timestamp=_timestamp(value.get("first_valid_timestamp")),
        last_valid_timestamp=_timestamp(value.get("last_valid_timestamp")),
        pipeline_version=str(value["pipeline_version"]),
        schema_version=str(value["schema_version"]),
        manifest_version=str(value["manifest_version"]),
    )


def _timestamp(value: object) -> datetime | None:
    return None if value is None else datetime.fromisoformat(str(value))


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None

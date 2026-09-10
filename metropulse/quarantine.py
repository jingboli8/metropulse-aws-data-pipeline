"""Deterministic construction and serialization of quarantine records."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from metropulse.models import QuarantineRecord, SourceMetadata
from metropulse.validation import RowValidation


def validate_processing_context(processing_timestamp: datetime, pipeline_version: str) -> None:
    """Require caller-supplied UTC processing time and a non-empty version."""
    if processing_timestamp.tzinfo is None or processing_timestamp.utcoffset() != timedelta(0):
        raise ValueError("processing_timestamp must be timezone-aware UTC")
    if not pipeline_version.strip():
        raise ValueError("pipeline_version must not be empty")


def build_quarantine_record(
    row: RowValidation,
    *,
    source_metadata: SourceMetadata,
    processing_timestamp: datetime,
    pipeline_version: str,
    expected_source_date: date,
) -> QuarantineRecord:
    """Attach caller context to one rejected row without reading external state."""
    if not row.issues:
        raise ValueError("Cannot quarantine a row without rule failures")
    return QuarantineRecord(
        original_record=row.original_record,
        original_field_values=row.original_field_values,
        source_row_number=row.source_row_number,
        rule_ids=tuple(issue.rule_id.value for issue in row.issues),
        rejection_reasons=tuple(issue.reason for issue in row.issues),
        source_bucket=source_metadata.source_bucket,
        source_object_key=source_metadata.source_object_key,
        source_object_version_id=source_metadata.source_object_version_id,
        source_object_etag=source_metadata.source_object_etag,
        source_object_checksum=source_metadata.source_object_checksum,
        processing_timestamp=processing_timestamp,
        pipeline_version=pipeline_version,
        expected_source_date=expected_source_date,
    )


def quarantine_record_json(record: QuarantineRecord) -> str:
    """Serialize a quarantine record with stable ordering and separators."""
    return json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

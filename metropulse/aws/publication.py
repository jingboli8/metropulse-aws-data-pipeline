"""Deterministic S3 output and completion-manifest serialization."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from metropulse.aws.config import LambdaConfig
from metropulse.aws.events import S3ObjectCreatedRecord
from metropulse.aws.idempotency import canonical_json
from metropulse.aws.keys import OutputKeys
from metropulse.manifest import MANIFEST_VERSION, batch_metrics_payload
from metropulse.models import TransformationResult
from metropulse.quarantine import quarantine_record_json
from metropulse.schema import SCHEMA_VERSION


def completion_manifest(
    *,
    record: S3ObjectCreatedRecord,
    config: LambdaConfig,
    identity: str,
    identity_fields: Mapping[str, object],
    keys: OutputKeys,
    processing_timestamp: datetime,
    raw_sha256: str,
    staging_sha256: str,
    staging_size: int,
    quarantine_sha256: str | None,
    quarantine_size: int,
    result: TransformationResult,
) -> dict[str, Any]:
    """Build a portable completion marker after deterministic outputs exist."""
    timestamps = tuple(item.event_timestamp_local for item in result.valid_records)
    quarantine = (
        {
            "bucket": config.destination_bucket,
            "byte_size": quarantine_size,
            "key": keys.quarantine,
            "sha256": quarantine_sha256,
        }
        if result.quarantine_row_count
        else None
    )
    return {
        "batch_metrics": batch_metrics_payload(result),
        "batch_warnings": [
            {
                "message": warning.message,
                "observed_count": warning.observed_count,
                "rule_id": warning.rule_id,
            }
            for warning in result.batch_warnings
        ],
        "completion_key": keys.completed,
        "first_valid_event_timestamp_local": timestamps[0].isoformat() if timestamps else None,
        "input_row_count": result.input_row_count,
        "last_valid_event_timestamp_local": timestamps[-1].isoformat() if timestamps else None,
        "manifest_version": MANIFEST_VERSION,
        "pipeline_version": config.pipeline_version,
        "processing_identity": identity,
        "processing_timestamp": iso_utc(processing_timestamp),
        "quarantine": quarantine,
        "quarantine_row_count": result.quarantine_row_count,
        "raw_byte_size": record.object_size,
        "raw_key": record.key,
        "raw_sha256": raw_sha256,
        "reconciliation": {
            "accounted_row_count": result.reconciliation.accounted_row_count,
            "applicable": result.reconciliation.applicable,
            "input_row_count": result.reconciliation.input_row_count,
            "matches": result.reconciliation.matches,
            "quarantine_row_count": result.reconciliation.quarantine_row_count,
            "valid_row_count": result.reconciliation.valid_row_count,
        },
        "schema_version": SCHEMA_VERSION,
        "source_bucket": record.bucket,
        "source_date": record.source_date.isoformat(),
        "source_identity": dict(identity_fields),
        "staging": {
            "bucket": config.destination_bucket,
            "byte_size": staging_size,
            "key": keys.staging,
            "sha256": staging_sha256,
        },
        "valid_row_count": result.valid_row_count,
        "valid_timestamp_count": len(timestamps),
    }


def quarantine_bytes(result: TransformationResult) -> bytes:
    """Serialize rejected records as stable UTF-8 JSON Lines, or no bytes."""
    if not result.quarantine_records:
        return b""
    return (
        "\n".join(quarantine_record_json(item) for item in result.quarantine_records) + "\n"
    ).encode("utf-8")


def object_metadata(
    *, pipeline_version: str, processing_identity: str, sha256: str, schema_version: str
) -> dict[str, str]:
    """Return bounded user metadata shared by published objects."""
    return {
        "pipeline-version": pipeline_version,
        "processing-identity": processing_identity,
        "schema-version": schema_version,
        "sha256": sha256,
    }


def json_bytes(value: Mapping[str, object]) -> bytes:
    """Serialize a control object deterministically as UTF-8 JSON."""
    return (canonical_json(value) + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    """Return the lowercase SHA-256 for an in-memory object."""
    return hashlib.sha256(value).hexdigest()


def iso_utc(value: datetime) -> str:
    """Serialize an aware operational timestamp in UTC."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

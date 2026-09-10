"""Deterministic daily manifest construction and serialization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from metropulse.local_io import write_text_atomic
from metropulse.local_paths import DailyPaths
from metropulse.models import TransformationResult
from metropulse.schema import SCHEMA_VERSION

MANIFEST_VERSION = "1.0.0"


def _metrics(result: TransformationResult) -> dict[str, Any]:
    metrics = result.batch_metrics
    return {
        "abnormal_sampling_interval_count": metrics.abnormal_sampling_interval_count,
        "empty_input": metrics.empty_input,
        "input_row_count": metrics.input_row_count,
        "maximum_gap_seconds": metrics.maximum_gap_seconds,
        "out_of_order_transition_count": metrics.out_of_order_transition_count,
        "parseable_timestamp_count": metrics.parseable_timestamp_count,
        "quarantine_row_count": metrics.quarantine_row_count,
        "quarantine_rule_counts": [
            {"count": item.count, "rule_id": item.rule_id}
            for item in metrics.quarantine_rule_counts
        ],
        "sampling_interval_distribution": [
            {"count": item.count, "seconds": item.seconds}
            for item in metrics.sampling_interval_distribution
        ],
        "schema_drift": metrics.schema_drift,
        "significant_gap_count": metrics.significant_gap_count,
        "unexpectedly_low_daily_row_count": metrics.unexpectedly_low_daily_row_count,
        "valid_row_count": metrics.valid_row_count,
    }


def build_daily_manifest(
    *,
    paths: DailyPaths,
    source_identity: Mapping[str, object],
    source_date: date,
    pipeline_version: str,
    processing_timestamp: str,
    raw_sha256: str,
    raw_byte_size: int,
    staging_sha256: str,
    staging_byte_size: int,
    quarantine_sha256: str | None,
    quarantine_byte_size: int,
    result: TransformationResult,
) -> dict[str, Any]:
    """Build one portable completion manifest after all data outputs exist."""
    reconciliation = result.reconciliation
    quarantine_present = result.quarantine_row_count > 0
    return {
        "batch_metrics": _metrics(result),
        "batch_warnings": [
            {
                "message": warning.message,
                "observed_count": warning.observed_count,
                "rule_id": warning.rule_id,
            }
            for warning in result.batch_warnings
        ],
        "input_row_count": result.input_row_count,
        "manifest_version": MANIFEST_VERSION,
        "pipeline_version": pipeline_version,
        "processing_timestamp": processing_timestamp,
        "quarantine_byte_size": quarantine_byte_size,
        "quarantine_relative_path": paths.quarantine_relative if quarantine_present else None,
        "quarantine_row_count": result.quarantine_row_count,
        "quarantine_sha256": quarantine_sha256,
        "quarantine_status": "present" if quarantine_present else "absent_zero_rows",
        "raw_byte_size": raw_byte_size,
        "raw_relative_path": paths.raw_relative,
        "raw_sha256": raw_sha256,
        "reconciliation": {
            "accounted_row_count": reconciliation.accounted_row_count,
            "applicable": reconciliation.applicable,
            "input_row_count": reconciliation.input_row_count,
            "matches": reconciliation.matches,
            "quarantine_row_count": reconciliation.quarantine_row_count,
            "valid_row_count": reconciliation.valid_row_count,
        },
        "schema_version": SCHEMA_VERSION,
        "source_date": source_date.isoformat(),
        "source_identity": dict(source_identity),
        "staging_byte_size": staging_byte_size,
        "staging_relative_path": paths.staging_relative,
        "staging_sha256": staging_sha256,
        "valid_row_count": result.valid_row_count,
    }


def manifest_text(manifest: Mapping[str, object]) -> str:
    """Return stable, human-readable manifest JSON."""
    return json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_daily_manifest(path: Path, manifest: Mapping[str, object]) -> None:
    """Publish a daily manifest atomically as the final completion marker."""
    write_text_atomic(path, manifest_text(manifest))

"""Checksum-backed resume checks and atomic publication for one daily partition."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from metropulse.backfill_models import DayOutcome
from metropulse.backfill_source import BackfillError
from metropulse.local_io import sha256_file, write_text_atomic
from metropulse.local_paths import DailyPaths
from metropulse.manifest import MANIFEST_VERSION, build_daily_manifest, manifest_text
from metropulse.models import SourceMetadata
from metropulse.parquet import inspect_daily_parquet, write_daily_parquet
from metropulse.quarantine import quarantine_record_json
from metropulse.schema import SCHEMA_VERSION
from metropulse.transform import transform_daily_csv

ABSOLUTE_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")


def _contains_absolute_path(value: object) -> bool:
    if isinstance(value, str):
        return ABSOLUTE_WINDOWS_PATH.match(value) is not None
    if isinstance(value, Mapping):
        return any(_contains_absolute_path(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_contains_absolute_path(item) for item in value)
    return False


def _read_manifest(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def verify_completed_day(
    paths: DailyPaths,
    *,
    source_identity: Mapping[str, object],
    source_date: date,
    pipeline_version: str,
    processing_timestamp: str,
    generated_raw_sha256: str,
    generated_raw_byte_size: int,
    generated_input_row_count: int,
) -> dict[str, Any] | None:
    """Validate a completion manifest and every referenced generated file."""
    manifest = _read_manifest(paths.manifest)
    if manifest is None or _contains_absolute_path(manifest):
        return None
    expected_values = {
        "input_row_count": generated_input_row_count,
        "manifest_version": MANIFEST_VERSION,
        "pipeline_version": pipeline_version,
        "processing_timestamp": processing_timestamp,
        "raw_byte_size": generated_raw_byte_size,
        "raw_relative_path": paths.raw_relative,
        "raw_sha256": generated_raw_sha256,
        "schema_version": SCHEMA_VERSION,
        "source_date": source_date.isoformat(),
        "source_identity": dict(source_identity),
        "staging_relative_path": paths.staging_relative,
    }
    if any(manifest.get(key) != value for key, value in expected_values.items()):
        return None
    reconciliation = manifest.get("reconciliation", {})
    if (
        not reconciliation.get("applicable")
        or not reconciliation.get("matches")
        or manifest.get("input_row_count")
        != manifest.get("valid_row_count", -1) + manifest.get("quarantine_row_count", -1)
    ):
        return None
    if not paths.raw.is_file() or paths.raw.stat().st_size != generated_raw_byte_size:
        return None
    if sha256_file(paths.raw) != generated_raw_sha256:
        return None
    if not paths.staging.is_file() or paths.staging.stat().st_size != manifest.get(
        "staging_byte_size"
    ):
        return None
    if sha256_file(paths.staging) != manifest.get("staging_sha256"):
        return None
    try:
        parquet_evidence = inspect_daily_parquet(paths.staging)
    except (OSError, ValueError):
        return None
    if (
        parquet_evidence["row_count"] != manifest.get("valid_row_count")
        or not parquet_evidence["schema_matches"]
        or parquet_evidence["timestamp_timezone"] is not None
        or (manifest.get("valid_row_count", 0) > 0 and parquet_evidence["codecs"] != ["SNAPPY"])
    ):
        return None
    quarantine_count = manifest.get("quarantine_row_count")
    if quarantine_count:
        if (
            manifest.get("quarantine_status") != "present"
            or manifest.get("quarantine_relative_path") != paths.quarantine_relative
            or not paths.quarantine.is_file()
            or paths.quarantine.stat().st_size != manifest.get("quarantine_byte_size")
            or sha256_file(paths.quarantine) != manifest.get("quarantine_sha256")
        ):
            return None
    elif (
        manifest.get("quarantine_status") != "absent_zero_rows"
        or manifest.get("quarantine_relative_path") is not None
        or manifest.get("quarantine_sha256") is not None
        or manifest.get("quarantine_byte_size") != 0
        or paths.quarantine.exists()
    ):
        return None
    return manifest


def _outputs_exist(paths: DailyPaths) -> bool:
    return any(
        (
            paths.raw.exists(),
            paths.staging.exists(),
            paths.quarantine.exists(),
            paths.manifest.exists(),
        )
    )


def publish_day(
    *,
    temporary_raw: Path,
    raw_row_count: int,
    paths: DailyPaths,
    source_identity: Mapping[str, object],
    source_date: date,
    processing_datetime: datetime,
    processing_timestamp: str,
    pipeline_version: str,
    force: bool,
) -> DayOutcome:
    """Skip a verified day or atomically rebuild all of its derived outputs."""
    raw_sha256 = sha256_file(temporary_raw)
    raw_byte_size = temporary_raw.stat().st_size
    existing_outputs = _outputs_exist(paths)
    if not force:
        completed = verify_completed_day(
            paths,
            source_identity=source_identity,
            source_date=source_date,
            pipeline_version=pipeline_version,
            processing_timestamp=processing_timestamp,
            generated_raw_sha256=raw_sha256,
            generated_raw_byte_size=raw_byte_size,
            generated_input_row_count=raw_row_count,
        )
        if completed is not None:
            return DayOutcome("skipped", completed)

    paths.manifest.unlink(missing_ok=True)
    raw_matches = (
        paths.raw.is_file()
        and paths.raw.stat().st_size == raw_byte_size
        and sha256_file(paths.raw) == raw_sha256
    )
    if force or not raw_matches:
        paths.raw.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary_raw, paths.raw)

    try:
        with paths.raw.open("r", encoding="utf-8-sig", newline="") as daily_csv:
            result = transform_daily_csv(
                daily_csv,
                expected_source_date=source_date,
                source_metadata=SourceMetadata(
                    source_object_key=paths.raw_relative,
                    source_object_checksum=f"sha256:{raw_sha256}",
                ),
                processing_timestamp=processing_datetime,
                pipeline_version=pipeline_version,
            )
        if result.input_row_count != raw_row_count:
            raise BackfillError(
                f"{source_date}: split count {raw_row_count} differs from transformed count "
                f"{result.input_row_count}."
            )
        if result.batch_errors:
            rule_ids = ", ".join(error.rule_id for error in result.batch_errors)
            raise BackfillError(f"{source_date}: batch errors prevent publication: {rule_ids}")
        if not result.reconciliation.matches:
            raise BackfillError(f"{source_date}: daily row reconciliation failed.")

        staging_sha256, staging_byte_size = write_daily_parquet(
            result.valid_records,
            paths.staging,
            pipeline_version=pipeline_version,
            source_identity=source_identity,
            source_date=source_date,
            raw_sha256=raw_sha256,
            processing_timestamp=processing_timestamp,
        )
        if result.quarantine_records:
            quarantine_text = "".join(
                f"{quarantine_record_json(record)}\n" for record in result.quarantine_records
            )
            write_text_atomic(paths.quarantine, quarantine_text)
            quarantine_sha256 = sha256_file(paths.quarantine)
            quarantine_byte_size = paths.quarantine.stat().st_size
        else:
            paths.quarantine.unlink(missing_ok=True)
            quarantine_sha256 = None
            quarantine_byte_size = 0

        manifest = build_daily_manifest(
            paths=paths,
            source_identity=source_identity,
            source_date=source_date,
            pipeline_version=pipeline_version,
            processing_timestamp=processing_timestamp,
            raw_sha256=raw_sha256,
            raw_byte_size=raw_byte_size,
            staging_sha256=staging_sha256,
            staging_byte_size=staging_byte_size,
            quarantine_sha256=quarantine_sha256,
            quarantine_byte_size=quarantine_byte_size,
            result=result,
        )
        write_text_atomic(paths.manifest, manifest_text(manifest))
    except Exception as error:
        return DayOutcome("failed", None, f"{source_date}: {error}")
    return DayOutcome("rebuilt" if existing_outputs else "processed", manifest)

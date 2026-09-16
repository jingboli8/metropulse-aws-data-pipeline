"""Local-manifest selection and immutable filesystem publication adapter."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path

from metropulse.compaction_identity import canonical_json
from metropulse.compaction_manifest import build_selection_document, compaction_keys
from metropulse.compaction_models import (
    CompactionResult,
    ImmutableOutputConflict,
    MonthlySelection,
    SelectedDailyInput,
)
from metropulse.local_paths import daily_paths


def select_local_month(
    lake_root: Path,
    *,
    year: int,
    month: int,
    known_source_start: date,
    known_source_end: date,
) -> MonthlySelection:
    """Translate explicit local daily manifests into the common approval model."""
    inputs: list[SelectedDailyInput] = []
    manifests = sorted(
        lake_root.glob(
            f"control/source=metropt3/source_date={year:04d}-{month:02d}-*/manifest.json"
        )
    )
    for path in manifests:
        body = path.read_bytes()
        manifest = json.loads(body.decode("utf-8"))
        source_date = date.fromisoformat(manifest["source_date"])
        paths = daily_paths(lake_root, source_date)
        if path.resolve() != paths.manifest.resolve():
            raise ValueError("local manifest path conflicts with its source_date")
        if manifest.get("staging_relative_path") != paths.staging_relative:
            raise ValueError("local manifest staging path is outside the canonical layout")
        inputs.append(
            SelectedDailyInput(
                source_date=source_date,
                processing_identity=hashlib.sha256(
                    canonical_json(
                        {
                            "manifest_sha256": hashlib.sha256(body).hexdigest(),
                            "staging_sha256": manifest["staging_sha256"],
                        }
                    )
                ).hexdigest(),
                completion_marker_bucket="local",
                completion_marker_key=paths.manifest_relative,
                completion_marker_identity_kind="sha256",
                completion_marker_identity_value=hashlib.sha256(body).hexdigest(),
                staging_bucket="local",
                staging_key=manifest["staging_relative_path"],
                staging_sha256=manifest["staging_sha256"],
                staging_byte_size=manifest["staging_byte_size"],
                input_row_count=manifest["input_row_count"],
                valid_row_count=manifest["valid_row_count"],
                quarantine_row_count=manifest["quarantine_row_count"],
                first_valid_timestamp=_timestamp(manifest["first_valid_event_timestamp_local"]),
                last_valid_timestamp=_timestamp(manifest["last_valid_event_timestamp_local"]),
                pipeline_version=manifest["pipeline_version"],
                schema_version=manifest["schema_version"],
                manifest_version=manifest["manifest_version"],
            )
        )
    first = max(date(year, month, 1), known_source_start)
    last = min(date(year, month, monthrange(year, month)[1]), known_source_end)
    expected = tuple(
        date.fromordinal(value) for value in range(first.toordinal(), last.toordinal() + 1)
    )
    return MonthlySelection(
        source_name="metropt3",
        year=year,
        month=month,
        inputs=tuple(inputs),
        expected_raw_dates=expected,
        known_source_start=known_source_start,
        known_source_end=known_source_end,
        terminal_partial_month=last < date(year, month, monthrange(year, month)[1]),
    )


def publish_local_run(
    lake_root: Path, selection: MonthlySelection, result: CompactionResult
) -> str:
    """Publish selection, Parquet, and completion immutably; return created or resumed."""
    keys = compaction_keys(selection.year, selection.month, result.run_id)
    selection_body = canonical_json(build_selection_document(selection, result.run_id)) + b"\n"
    completion_body = canonical_json(result.completion) + b"\n"
    states = [
        _write_immutable(lake_root / Path(*keys["selection"].split("/")), selection_body),
        _write_immutable(lake_root / Path(*keys["curated"].split("/")), result.parquet_bytes),
        _write_immutable(lake_root / Path(*keys["completed"].split("/")), completion_body),
    ]
    return "resumed" if all(state == "existing" for state in states) else "created"


def read_local_staging(lake_root: Path, item: SelectedDailyInput) -> bytes:
    """Read the exact relative staging key selected by a local manifest."""
    expected = daily_paths(lake_root, item.source_date).staging_relative
    if item.staging_key != expected:
        raise ValueError("selected local staging key is outside the canonical layout")
    return lake_root.joinpath(*item.staging_key.split("/")).read_bytes()


def _write_immutable(path: Path, body: bytes) -> str:
    if path.exists():
        if path.read_bytes() != body:
            raise ImmutableOutputConflict(f"immutable local output conflicts: {path.name}")
        return "existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(name)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != body:
                raise ImmutableOutputConflict(
                    f"immutable local output race conflicts: {path.name}"
                ) from None
            return "existing"
        return "created"
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None

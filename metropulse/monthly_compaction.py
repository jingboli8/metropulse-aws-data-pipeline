"""AWS-independent deterministic monthly compaction algorithm."""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa

from metropulse.compaction_identity import (
    DEFAULT_COMPACTOR_VERSION,
    DEFAULT_OUTPUT_CONTRACT_VERSION,
    WriterContract,
    monthly_run_id,
)
from metropulse.compaction_manifest import build_completion_manifest, compaction_keys
from metropulse.compaction_models import (
    BoundaryFinding,
    CompactionBounds,
    CompactionError,
    CompactionResult,
    MonthlySelection,
    SelectedDailyInput,
)
from metropulse.compaction_selection import missing_expected_dates, validate_selection
from metropulse.curated_parquet import (
    read_daily_table,
    serialize_curated_table,
    sha256_bytes,
    verify_curated_bytes,
)


def compact_month(
    selection: MonthlySelection,
    read_staging: Callable[[SelectedDailyInput], bytes],
    *,
    bounds: CompactionBounds | None = None,
    compactor_version: str = DEFAULT_COMPACTOR_VERSION,
    output_contract_version: str = DEFAULT_OUTPUT_CONTRACT_VERSION,
    writer_contract: WriterContract | None = None,
) -> CompactionResult:
    """Validate selected daily objects and return one verified monthly Parquet payload."""
    ordered = validate_selection(selection)
    limits = bounds or CompactionBounds()
    writer = writer_contract or WriterContract()
    if writer != WriterContract():
        raise CompactionError("writer contract differs from the fixed Phase 6 contract")
    _validate_bounds(ordered, limits)
    run_id = monthly_run_id(
        selection,
        compactor_version=compactor_version,
        output_contract_version=output_contract_version,
        writer_contract=writer,
    )
    tables: list[pa.Table] = []
    prior_timestamps: set[object] = set()
    prior_indexes: set[object] = set()
    findings = _boundary_findings(ordered)
    for item in ordered:
        payload = read_staging(item)
        if (
            len(payload) != item.staging_byte_size
            or sha256_bytes(payload) != item.staging_sha256.lower()
        ):
            raise CompactionError("selected staging checksum or size mismatch")
        table = read_daily_table(
            payload,
            expected_date=item.source_date,
            expected_rows=item.valid_row_count,
            expected_first_timestamp=item.first_valid_timestamp,
            expected_last_timestamp=item.last_valid_timestamp,
        )
        timestamps = set(table.column("event_timestamp_local").to_pylist())
        indexes = set(table.column("record_index").to_pylist())
        if prior_timestamps.intersection(timestamps):
            raise CompactionError("duplicate timestamp across selected daily inputs")
        if prior_indexes.intersection(indexes):
            raise CompactionError("duplicate record_index across selected daily inputs")
        prior_timestamps.update(timestamps)
        prior_indexes.update(indexes)
        tables.append(table)
    selected_rows = sum(item.valid_row_count for item in ordered)
    loaded_rows = sum(table.num_rows for table in tables)
    if loaded_rows != selected_rows:
        raise CompactionError("loaded monthly rows do not reconcile")
    combined = pa.concat_tables(tables)
    parquet_bytes = serialize_curated_table(combined, run_id=run_id, writer=writer)
    if len(parquet_bytes) > limits.max_output_bytes:
        raise CompactionError("curated output exceeds configured byte limit")
    evidence = verify_curated_bytes(parquet_bytes, expected_rows=selected_rows)
    significant_gaps = _significant_gap_count(parquet_bytes)
    missing = missing_expected_dates(selection)
    keys = compaction_keys(selection.year, selection.month, run_id)
    quarantine_rows = sum(item.quarantine_row_count for item in ordered)
    completion = build_completion_manifest(
        selection=selection,
        run_id=run_id,
        keys=keys,
        parquet_sha256=sha256_bytes(parquet_bytes),
        parquet_byte_size=len(parquet_bytes),
        selected_rows=selected_rows,
        curated_rows=evidence.row_count,
        quarantine_rows=quarantine_rows,
        first_timestamp=evidence.first_timestamp.isoformat() if evidence.first_timestamp else None,
        last_timestamp=evidence.last_timestamp.isoformat() if evidence.last_timestamp else None,
        valid_timestamp_count=evidence.valid_timestamp_count,
        row_group_count=evidence.row_group_count,
        missing_dates=[value.isoformat() for value in missing],
        boundary_findings=findings,
        significant_gap_count=significant_gaps,
        writer=writer,
        compactor_version=compactor_version,
        output_contract_version=output_contract_version,
    )
    if not all(completion["reconciliation"].values()):
        raise CompactionError("monthly reconciliation failed")
    return CompactionResult(
        run_id=run_id,
        parquet_bytes=parquet_bytes,
        parquet_sha256=sha256_bytes(parquet_bytes),
        parquet_byte_size=len(parquet_bytes),
        selected_row_count=selected_rows,
        curated_row_count=evidence.row_count,
        quarantine_row_count=quarantine_rows,
        first_timestamp=evidence.first_timestamp,
        last_timestamp=evidence.last_timestamp,
        valid_timestamp_count=evidence.valid_timestamp_count,
        row_group_count=evidence.row_group_count,
        missing_dates=missing,
        boundary_findings=findings,
        significant_gap_count=significant_gaps,
        completion=completion,
    )


def _validate_bounds(inputs: tuple[SelectedDailyInput, ...], bounds: CompactionBounds) -> None:
    if len(inputs) > bounds.max_selected_objects:
        raise CompactionError("selected object count exceeds configured limit")
    if sum(item.staging_byte_size for item in inputs) > bounds.max_compressed_input_bytes:
        raise CompactionError("selected compressed bytes exceed configured limit")
    if sum(item.input_row_count for item in inputs) > bounds.max_input_rows:
        raise CompactionError("selected rows exceed configured limit")


def _boundary_findings(inputs: tuple[SelectedDailyInput, ...]) -> tuple[BoundaryFinding, ...]:
    findings: list[BoundaryFinding] = []
    for previous, current in zip(inputs, inputs[1:], strict=False):
        if previous.last_valid_timestamp is None or current.first_valid_timestamp is None:
            findings.append(
                BoundaryFinding(
                    previous.source_date, current.source_date, None, "unassessable_empty_boundary"
                )
            )
            continue
        seconds = (current.first_valid_timestamp - previous.last_valid_timestamp).total_seconds()
        if seconds < 0:
            classification = "reversed_boundary"
        elif seconds == 0:
            classification = "overlapping_boundary"
        elif seconds > 60:
            classification = "gap_over_60_seconds"
        else:
            classification = "observed_boundary"
        findings.append(
            BoundaryFinding(previous.source_date, current.source_date, seconds, classification)
        )
    return tuple(findings)


def _significant_gap_count(payload: bytes) -> int:
    import pyarrow.parquet as pq

    timestamps = (
        pq.read_table(pa.BufferReader(payload), columns=["event_timestamp_local"])
        .column(0)
        .to_pylist()
    )
    return sum(
        1
        for previous, current in zip(timestamps, timestamps[1:], strict=False)
        if (current - previous).total_seconds() > 60
    )

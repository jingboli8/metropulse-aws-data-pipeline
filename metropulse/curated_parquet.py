"""Explicit-schema verification and deterministic curated Parquet writing."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from metropulse.compaction_identity import ROW_GROUP_SIZE, WriterContract, canonical_json
from metropulse.compaction_models import CompactionError
from metropulse.schema import NORMALIZED_ARROW_SCHEMA


@dataclass(frozen=True)
class ParquetEvidence:
    """Independent evidence obtained by reopening Parquet bytes."""

    row_count: int
    row_group_count: int
    codecs: tuple[str, ...]
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    valid_timestamp_count: int


def read_daily_table(
    payload: bytes,
    *,
    expected_date: date,
    expected_rows: int,
    expected_first_timestamp: datetime | None,
    expected_last_timestamp: datetime | None,
) -> pa.Table:
    """Read and verify one selected daily staging object."""
    parquet_file = pq.ParquetFile(pa.BufferReader(payload))
    _verify_schema_and_codecs(parquet_file)
    if parquet_file.metadata.num_rows != expected_rows:
        raise CompactionError("daily Parquet row count does not match its completion marker")
    table = parquet_file.read()
    source_dates = table.column("source_date").to_pylist()
    timestamps = table.column("event_timestamp_local").to_pylist()
    if any(value != expected_date for value in source_dates):
        raise CompactionError("daily Parquet contains a different source_date")
    if any(value.date() != expected_date for value in timestamps):
        raise CompactionError("daily timestamp date conflicts with source_date")
    observed_first = timestamps[0] if timestamps else None
    observed_last = timestamps[-1] if timestamps else None
    if (observed_first, observed_last) != (
        expected_first_timestamp,
        expected_last_timestamp,
    ):
        raise CompactionError("daily Parquet boundary timestamps conflict with completion marker")
    return table.replace_schema_metadata(None)


def serialize_curated_table(table: pa.Table, *, run_id: str, writer: WriterContract) -> bytes:
    """Sort and serialize one monthly table using fixed writer options."""
    order = pc.sort_indices(
        table,
        sort_keys=[("event_timestamp_local", "ascending"), ("record_index", "ascending")],
    )
    sorted_table = pc.take(table, order)
    metadata = dict(NORMALIZED_ARROW_SCHEMA.metadata or {})
    metadata.update(
        {
            b"compaction_run_id": run_id.encode(),
            b"writer_contract": canonical_json(writer.__dict__),
        }
    )
    sorted_table = sorted_table.cast(NORMALIZED_ARROW_SCHEMA).replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    pq.write_table(
        sorted_table,
        sink,
        compression=writer.compression,
        use_dictionary=writer.use_dictionary,
        write_statistics=writer.write_statistics,
        version=writer.parquet_format_version,
        row_group_size=writer.row_group_size,
        coerce_timestamps=writer.coerce_timestamps,
        allow_truncated_timestamps=writer.allow_truncated_timestamps,
    )
    return sink.getvalue().to_pybytes()


def verify_curated_bytes(payload: bytes, *, expected_rows: int) -> ParquetEvidence:
    """Reopen and independently verify schema, codec, order, duplicates, and row groups."""
    parquet_file = pq.ParquetFile(pa.BufferReader(payload))
    _verify_schema_and_codecs(parquet_file)
    if parquet_file.metadata.num_rows != expected_rows:
        raise CompactionError("curated output row count does not reconcile")
    if any(
        parquet_file.metadata.row_group(index).num_rows > ROW_GROUP_SIZE
        for index in range(parquet_file.metadata.num_row_groups)
    ):
        raise CompactionError("curated output exceeds the fixed row-group size")
    table = parquet_file.read(columns=["event_timestamp_local", "record_index"])
    timestamps = table.column("event_timestamp_local").to_pylist()
    indexes = table.column("record_index").to_pylist()
    pairs = list(zip(timestamps, indexes, strict=True))
    if pairs != sorted(pairs):
        raise CompactionError("curated output is not stably sorted")
    if len(set(timestamps)) != len(timestamps):
        raise CompactionError("curated output contains duplicate timestamps")
    if len(set(indexes)) != len(indexes):
        raise CompactionError("curated output contains duplicate record indexes")
    return ParquetEvidence(
        row_count=len(timestamps),
        row_group_count=parquet_file.metadata.num_row_groups,
        codecs=tuple(
            sorted(
                {
                    parquet_file.metadata.row_group(group).column(column).compression
                    for group in range(parquet_file.metadata.num_row_groups)
                    for column in range(parquet_file.metadata.num_columns)
                }
            )
        ),
        first_timestamp=timestamps[0] if timestamps else None,
        last_timestamp=timestamps[-1] if timestamps else None,
        valid_timestamp_count=len(timestamps),
    )


def sha256_bytes(payload: bytes) -> str:
    """Return a lowercase SHA-256 digest."""
    return hashlib.sha256(payload).hexdigest()


def _verify_schema_and_codecs(parquet_file: pq.ParquetFile) -> None:
    schema = parquet_file.schema_arrow
    if not schema.equals(NORMALIZED_ARROW_SCHEMA, check_metadata=False):
        raise CompactionError("Parquet schema does not match the normalized contract")
    timestamp = schema.field("event_timestamp_local").type
    if timestamp != pa.timestamp("ms") or timestamp.tz is not None:
        raise CompactionError("event timestamp must be timestamp[ms] with no timezone")
    codecs = {
        parquet_file.metadata.row_group(group).column(column).compression
        for group in range(parquet_file.metadata.num_row_groups)
        for column in range(parquet_file.metadata.num_columns)
    }
    if parquet_file.metadata.num_rows and codecs != {"SNAPPY"}:
        raise CompactionError("every Parquet column chunk must use Snappy")

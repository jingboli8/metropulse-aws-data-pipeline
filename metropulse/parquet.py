"""Explicit, atomic daily Parquet serialization and inspection."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from metropulse.local_io import atomic_output_path, sha256_file
from metropulse.models import NormalizedRecord
from metropulse.schema import NORMALIZED_ARROW_SCHEMA


def parquet_schema(
    *,
    pipeline_version: str,
    source_identity: Mapping[str, object],
    source_date: date,
    raw_sha256: str,
    processing_timestamp: str,
) -> pa.Schema:
    """Return the stable data schema with deterministic file-level lineage."""
    metadata = dict(NORMALIZED_ARROW_SCHEMA.metadata or {})
    metadata.update(
        {
            b"pipeline_version": pipeline_version.encode(),
            b"processing_timestamp": processing_timestamp.encode(),
            b"raw_sha256": raw_sha256.encode(),
            b"source_date": source_date.isoformat().encode(),
            b"source_identity": json.dumps(
                source_identity, sort_keys=True, separators=(",", ":")
            ).encode(),
        }
    )
    return NORMALIZED_ARROW_SCHEMA.with_metadata(metadata)


def write_daily_parquet(
    records: Sequence[NormalizedRecord],
    final_path: Path,
    *,
    pipeline_version: str,
    source_identity: Mapping[str, object],
    source_date: date,
    raw_sha256: str,
    processing_timestamp: str,
) -> tuple[str, int]:
    """Write one explicit-schema Snappy Parquet file and return checksum and size."""
    schema = parquet_schema(
        pipeline_version=pipeline_version,
        source_identity=source_identity,
        source_date=source_date,
        raw_sha256=raw_sha256,
        processing_timestamp=processing_timestamp,
    )
    table = pa.Table.from_pylist([record.to_dict() for record in records], schema=schema)
    with atomic_output_path(final_path) as temporary_path:
        pq.write_table(
            table,
            temporary_path,
            compression="snappy",
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
        )
    return sha256_file(final_path), final_path.stat().st_size


def inspect_daily_parquet(path: Path) -> dict[str, object]:
    """Read independent schema, row-count, and codec evidence from a Parquet file."""
    parquet_file = pq.ParquetFile(path)
    codecs = sorted(
        {
            parquet_file.metadata.row_group(row_group).column(column).compression
            for row_group in range(parquet_file.metadata.num_row_groups)
            for column in range(parquet_file.metadata.num_columns)
        }
    )
    schema_matches = parquet_file.schema_arrow.equals(NORMALIZED_ARROW_SCHEMA, check_metadata=False)
    return {
        "row_count": parquet_file.metadata.num_rows,
        "row_group_count": parquet_file.metadata.num_row_groups,
        "codecs": codecs,
        "schema_matches": schema_matches,
        "timestamp_timezone": parquet_file.schema_arrow.field("event_timestamp_local").type.tz,
    }

from __future__ import annotations

import hashlib
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from metropulse.compaction_identity import ROW_GROUP_SIZE
from metropulse.compaction_models import CompactionBounds, CompactionError
from metropulse.monthly_compaction import _boundary_findings, compact_month
from metropulse.schema import NORMALIZED_ARROW_SCHEMA


def test_compaction_schema_sort_codec_rows_and_missing_dates(compact_fixture) -> None:
    selection, payloads = compact_fixture()
    selection = replace(selection, inputs=tuple(reversed(selection.inputs)))
    result = compact_month(selection, lambda item: payloads[item.staging_key])
    parquet = pq.ParquetFile(__import__("pyarrow").BufferReader(result.parquet_bytes))
    assert result.curated_row_count == result.selected_row_count == 2
    assert result.quarantine_row_count == 0
    assert result.parquet_sha256
    assert parquet.metadata.row_group(0).num_rows <= ROW_GROUP_SIZE
    assert {parquet.metadata.row_group(0).column(i).compression for i in range(19)} == {"SNAPPY"}
    assert result.completion["reconciliation"] == {
        "curated_equals_selected_valid": True,
        "input_equals_valid_plus_quarantine": True,
    }


def test_gap_is_observation_and_never_quarantine(compact_fixture) -> None:
    selection, payloads = compact_fixture((1, 2), gap=True)
    result = compact_month(selection, lambda item: payloads[item.staging_key])
    assert result.significant_gap_count == 1
    assert result.quarantine_row_count == 0
    assert result.boundary_findings[0].classification == "gap_over_60_seconds"


def test_checksum_size_and_bounds_fail(compact_fixture) -> None:
    selection, payloads = compact_fixture()
    with pytest.raises(CompactionError, match="checksum or size"):
        compact_month(selection, lambda item: payloads[item.staging_key] + b"x")
    with pytest.raises(CompactionError, match="object count"):
        compact_month(
            selection,
            lambda item: payloads[item.staging_key],
            bounds=CompactionBounds(max_selected_objects=1),
        )


def test_cross_day_duplicate_index_fails(compact_fixture) -> None:
    selection, payloads = compact_fixture()
    second = selection.inputs[1]
    table = pq.read_table(pa.BufferReader(payloads[second.staging_key]))
    table = table.set_column(
        table.schema.get_field_index("record_index"), "record_index", pa.array([1], pa.int64())
    ).cast(NORMALIZED_ARROW_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy", use_dictionary=False, version="2.6")
    body = sink.getvalue().to_pybytes()
    payloads[second.staging_key] = body
    second = replace(
        second, staging_sha256=hashlib.sha256(body).hexdigest(), staging_byte_size=len(body)
    )
    duplicate = replace(selection, inputs=(selection.inputs[0], second))
    with pytest.raises(CompactionError, match="duplicate record_index"):
        compact_month(duplicate, lambda item: payloads[item.staging_key])


def test_non_snappy_daily_input_is_rejected(compact_fixture) -> None:
    selection, payloads = compact_fixture((1,))
    item = selection.inputs[0]
    table = pq.read_table(pa.BufferReader(payloads[item.staging_key]))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="gzip")
    body = sink.getvalue().to_pybytes()
    payloads[item.staging_key] = body
    changed = replace(
        item, staging_sha256=hashlib.sha256(body).hexdigest(), staging_byte_size=len(body)
    )
    with pytest.raises(CompactionError, match="Snappy"):
        compact_month(
            replace(selection, inputs=(changed,)), lambda value: payloads[value.staging_key]
        )


def test_completion_boundary_must_match_daily_parquet(compact_fixture) -> None:
    selection, payloads = compact_fixture((1,))
    item = replace(
        selection.inputs[0], first_valid_timestamp=selection.inputs[0].last_valid_timestamp
    )
    item = replace(item, last_valid_timestamp=item.last_valid_timestamp.replace(second=1))
    with pytest.raises(CompactionError, match="boundary timestamps"):
        compact_month(replace(selection, inputs=(item,)), lambda value: payloads[value.staging_key])


def test_empty_fully_quarantined_partition_makes_boundary_unassessable(
    compact_fixture,
) -> None:
    selection, payloads = compact_fixture()
    first = selection.inputs[0]
    table = pa.Table.from_pylist([], schema=NORMALIZED_ARROW_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy", use_dictionary=False, version="2.6")
    body = sink.getvalue().to_pybytes()
    payloads[first.staging_key] = body
    first = replace(
        first,
        staging_sha256=hashlib.sha256(body).hexdigest(),
        staging_byte_size=len(body),
        input_row_count=1,
        valid_row_count=0,
        quarantine_row_count=1,
        first_valid_timestamp=None,
        last_valid_timestamp=None,
    )
    result = compact_month(
        replace(selection, inputs=(first, selection.inputs[1])),
        lambda value: payloads[value.staging_key],
    )
    assert result.curated_row_count == 1
    assert result.quarantine_row_count == 1
    assert result.boundary_findings[0].classification == "unassessable_empty_boundary"


def test_row_count_mismatch_is_rejected(compact_fixture) -> None:
    selection, payloads = compact_fixture((1,))
    item = replace(selection.inputs[0], input_row_count=2, valid_row_count=2)
    with pytest.raises(CompactionError, match="row count"):
        compact_month(replace(selection, inputs=(item,)), lambda value: payloads[value.staging_key])


def test_timezone_annotated_daily_input_is_rejected(compact_fixture) -> None:
    selection, payloads = compact_fixture((1,))
    item = selection.inputs[0]
    table = pq.read_table(pa.BufferReader(payloads[item.staging_key]))
    fields = list(table.schema)
    index = table.schema.get_field_index("event_timestamp_local")
    fields[index] = pa.field("event_timestamp_local", pa.timestamp("ms", tz="UTC"), nullable=False)
    zoned = table.set_column(
        index,
        fields[index],
        table.column(index).cast(pa.timestamp("ms", tz="UTC")),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(zoned, sink, compression="snappy")
    body = sink.getvalue().to_pybytes()
    payloads[item.staging_key] = body
    changed = replace(
        item, staging_sha256=hashlib.sha256(body).hexdigest(), staging_byte_size=len(body)
    )
    with pytest.raises(CompactionError, match="schema"):
        compact_month(
            replace(selection, inputs=(changed,)), lambda value: payloads[value.staging_key]
        )


def test_reversed_and_overlapping_boundaries_are_classified_as_findings(
    compact_fixture,
) -> None:
    selection, _ = compact_fixture()
    previous, current = selection.inputs
    reversed_items = (
        replace(previous, last_valid_timestamp=current.first_valid_timestamp.replace(second=20)),
        current,
    )
    overlap_items = (
        replace(previous, last_valid_timestamp=current.first_valid_timestamp),
        current,
    )
    assert _boundary_findings(reversed_items)[0].classification == "reversed_boundary"
    assert _boundary_findings(overlap_items)[0].classification == "overlapping_boundary"

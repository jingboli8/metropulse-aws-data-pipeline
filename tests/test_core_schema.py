from __future__ import annotations

import json

import pyarrow as pa

from metropulse.normalization import normalize_source_field, normalized_header, source_lineage
from metropulse.schema import (
    ANALOG_SOURCE_FIELDS,
    DIGITAL_SOURCE_FIELDS,
    FIELD_DEFINITIONS,
    NORMALIZED_ARROW_SCHEMA,
    NORMALIZED_FIELDS,
    NORMALIZED_SOURCE_FIELDS,
    SOURCE_FIELDS,
    SOURCE_TO_NORMALIZED,
)


def test_exact_source_header_normalization() -> None:
    assert len(SOURCE_FIELDS) == 17
    assert SOURCE_FIELDS[0] == ""
    assert normalized_header(SOURCE_FIELDS) == NORMALIZED_SOURCE_FIELDS
    assert normalize_source_field("") == "record_index"
    assert normalize_source_field("timestamp") == "event_timestamp_local"


def test_dv_eletric_lineage_mapping_is_explicit() -> None:
    assert normalize_source_field("DV_eletric") == "dv_electric"
    assert source_lineage()["DV_eletric"] == "dv_electric"
    metadata = NORMALIZED_ARROW_SCHEMA.metadata
    assert metadata is not None
    assert json.loads(metadata[b"source_lineage"])["DV_eletric"] == "dv_electric"


def test_arrow_schema_matches_contract_types_and_timezone() -> None:
    assert NORMALIZED_ARROW_SCHEMA.names == list(NORMALIZED_FIELDS)
    assert all(not field.nullable for field in NORMALIZED_ARROW_SCHEMA)
    assert NORMALIZED_ARROW_SCHEMA.field("record_index").type == pa.int64()
    timestamp_type = NORMALIZED_ARROW_SCHEMA.field("event_timestamp_local").type
    assert pa.types.is_timestamp(timestamp_type)
    assert timestamp_type.tz is None
    for source_name in ANALOG_SOURCE_FIELDS:
        assert NORMALIZED_ARROW_SCHEMA.field(SOURCE_TO_NORMALIZED[source_name]).type == pa.float64()
    for source_name in DIGITAL_SOURCE_FIELDS:
        assert NORMALIZED_ARROW_SCHEMA.field(SOURCE_TO_NORMALIZED[source_name]).type == pa.bool_()
    assert NORMALIZED_ARROW_SCHEMA.field("source_date").type == pa.date32()
    assert NORMALIZED_ARROW_SCHEMA.metadata[b"event_timestamp_timezone_status"] == b"unknown"


def test_cross_format_definitions_cover_every_normalized_field() -> None:
    assert (
        tuple(definition.normalized_name for definition in FIELD_DEFINITIONS) == NORMALIZED_FIELDS
    )
    assert all(definition.parquet_type for definition in FIELD_DEFINITIONS)
    assert all(definition.athena_type for definition in FIELD_DEFINITIONS)
    assert not any("label" in name or "failure" in name for name in NORMALIZED_FIELDS)


def test_unknown_source_field_is_rejected() -> None:
    try:
        normalize_source_field("DV_electric")
    except ValueError as error:
        assert "Unknown MetroPT-3 source field" in str(error)
    else:
        raise AssertionError("misspelled source lineage was accepted")

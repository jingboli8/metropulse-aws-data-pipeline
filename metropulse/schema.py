"""Versioned source and normalized schemas for MetroPT-3 telemetry."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pyarrow as pa

SCHEMA_VERSION = "1.0.0"
TIMESTAMP_TIMEZONE_STATUS = "unknown"

SOURCE_FIELDS = (
    "",
    "timestamp",
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Oil_temperature",
    "Motor_current",
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
)

ANALOG_SOURCE_FIELDS = (
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Oil_temperature",
    "Motor_current",
)

DIGITAL_SOURCE_FIELDS = (
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
)

SOURCE_TO_NORMALIZED = {
    "": "record_index",
    "timestamp": "event_timestamp_local",
    "TP2": "tp2",
    "TP3": "tp3",
    "H1": "h1",
    "DV_pressure": "dv_pressure",
    "Reservoirs": "reservoirs",
    "Oil_temperature": "oil_temperature",
    "Motor_current": "motor_current",
    "COMP": "comp",
    "DV_eletric": "dv_electric",
    "Towers": "towers",
    "MPG": "mpg",
    "LPS": "lps",
    "Pressure_switch": "pressure_switch",
    "Oil_level": "oil_level",
    "Caudal_impulses": "caudal_impulses",
}

NORMALIZED_SOURCE_FIELDS = tuple(SOURCE_TO_NORMALIZED[field] for field in SOURCE_FIELDS)
DERIVED_FIELDS = ("source_date", "event_timestamp_timezone_status")
NORMALIZED_FIELDS = (*NORMALIZED_SOURCE_FIELDS, *DERIVED_FIELDS)


@dataclass(frozen=True)
class FieldDefinition:
    """Cross-format type and lineage definition for one normalized field."""

    source_name: str | None
    normalized_name: str
    arrow_type: pa.DataType
    parquet_type: str
    athena_type: str
    nullable: bool
    category: str


FIELD_DEFINITIONS = (
    FieldDefinition("", "record_index", pa.int64(), "INT64", "bigint", False, "identity"),
    FieldDefinition(
        "timestamp",
        "event_timestamp_local",
        pa.timestamp("ms"),
        "INT64/TIMESTAMP(isAdjustedToUTC=false)",
        "timestamp",
        False,
        "time",
    ),
    *(
        FieldDefinition(
            source_name,
            SOURCE_TO_NORMALIZED[source_name],
            pa.float64(),
            "DOUBLE",
            "double",
            False,
            "analog",
        )
        for source_name in ANALOG_SOURCE_FIELDS
    ),
    *(
        FieldDefinition(
            source_name,
            SOURCE_TO_NORMALIZED[source_name],
            pa.bool_(),
            "BOOLEAN",
            "boolean",
            False,
            "digital",
        )
        for source_name in DIGITAL_SOURCE_FIELDS
    ),
    FieldDefinition(None, "source_date", pa.date32(), "INT32/DATE", "date", False, "derived"),
    FieldDefinition(
        None,
        "event_timestamp_timezone_status",
        pa.string(),
        "BYTE_ARRAY/UTF8",
        "string",
        False,
        "derived",
    ),
)

_LINEAGE_JSON = json.dumps(SOURCE_TO_NORMALIZED, sort_keys=True, separators=(",", ":"))

NORMALIZED_ARROW_SCHEMA = pa.schema(
    [
        pa.field(definition.normalized_name, definition.arrow_type, definition.nullable)
        for definition in FIELD_DEFINITIONS
    ],
    metadata={
        b"schema_version": SCHEMA_VERSION.encode(),
        b"event_timestamp_timezone_status": TIMESTAMP_TIMEZONE_STATUS.encode(),
        b"source_lineage": _LINEAGE_JSON.encode(),
    },
)

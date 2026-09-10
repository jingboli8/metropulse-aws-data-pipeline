"""Contracted row-level validation independent of transport and storage."""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import StrEnum

from metropulse.models import NormalizedRecord
from metropulse.parsing import (
    SourceCsvRecord,
    parse_event_timestamp_local,
    parse_numeric,
    parse_record_index,
)
from metropulse.schema import (
    ANALOG_SOURCE_FIELDS,
    DIGITAL_SOURCE_FIELDS,
    SOURCE_FIELDS,
    TIMESTAMP_TIMEZONE_STATUS,
)


class RowRuleId(StrEnum):
    """Stable row-level rule identifiers from the Phase 0 contract."""

    COLUMN_COUNT = "ROW_COLUMN_COUNT"
    TIMESTAMP_PARSE = "ROW_TIMESTAMP_PARSE"
    NUMERIC_PARSE = "ROW_NUMERIC_PARSE"
    REQUIRED_MISSING = "ROW_REQUIRED_MISSING"
    ANALOG_NONFINITE = "ROW_ANALOG_NONFINITE"
    DIGITAL_DOMAIN = "ROW_DIGITAL_DOMAIN"
    SOURCE_DATE_MISMATCH = "ROW_SOURCE_DATE_MISMATCH"
    DUPLICATE_RECORD_INDEX = "ROW_DUPLICATE_RECORD_INDEX"
    DUPLICATE_TIMESTAMP = "ROW_DUPLICATE_TIMESTAMP"


ROW_RULE_ORDER = tuple(RowRuleId)


@dataclass(frozen=True)
class RowIssue:
    """One deterministic rule failure and explanation."""

    rule_id: RowRuleId
    reason: str


@dataclass(frozen=True)
class RowValidation:
    """Intermediate row result used for object-wide duplicate validation."""

    source_row_number: int
    original_record: str
    original_field_values: tuple[str, ...]
    parsed_record_index: int | None
    parsed_timestamp: datetime | None
    normalized_record: NormalizedRecord | None
    issues: tuple[RowIssue, ...]


def _display_source_field(source_name: str) -> str:
    return "<blank index header>" if source_name == "" else source_name


def _ordered_issues(reasons: dict[RowRuleId, str]) -> tuple[RowIssue, ...]:
    return tuple(
        RowIssue(rule_id, reasons[rule_id]) for rule_id in ROW_RULE_ORDER if rule_id in reasons
    )


def validate_source_row(
    record: SourceCsvRecord,
    *,
    source_row_number: int,
    expected_source_date: date,
) -> RowValidation:
    """Parse and validate one row before object-wide duplicate checks."""
    fields = record.field_values
    if len(fields) != len(SOURCE_FIELDS):
        issue = RowIssue(
            RowRuleId.COLUMN_COUNT,
            f"Expected {len(SOURCE_FIELDS)} columns but found {len(fields)}.",
        )
        return RowValidation(
            source_row_number,
            record.original_record,
            fields,
            None,
            None,
            None,
            (issue,),
        )

    values = dict(zip(SOURCE_FIELDS, fields, strict=True))
    reasons: dict[RowRuleId, str] = {}
    missing = [name for name in SOURCE_FIELDS if not values[name].strip()]
    if missing:
        names = ", ".join(_display_source_field(name) for name in missing)
        reasons[RowRuleId.REQUIRED_MISSING] = f"Missing required value(s): {names}."

    record_index: int | None = None
    numeric_failures: list[str] = []
    if "" not in missing:
        try:
            record_index = parse_record_index(values[""])
        except ValueError:
            numeric_failures.append(_display_source_field(""))

    timestamp: datetime | None = None
    if "timestamp" not in missing:
        try:
            timestamp = parse_event_timestamp_local(values["timestamp"])
        except ValueError:
            reasons[RowRuleId.TIMESTAMP_PARSE] = (
                "timestamp is not parseable as YYYY-MM-DD HH:MM:SS."
            )

    analog_values: dict[str, float] = {}
    nonfinite_analog: list[str] = []
    for source_name in ANALOG_SOURCE_FIELDS:
        if source_name in missing:
            continue
        try:
            value = parse_numeric(values[source_name])
        except ValueError:
            numeric_failures.append(source_name)
            continue
        analog_values[source_name] = value
        if not math.isfinite(value):
            nonfinite_analog.append(source_name)

    digital_values: dict[str, bool] = {}
    invalid_digital: list[str] = []
    for source_name in DIGITAL_SOURCE_FIELDS:
        if source_name in missing:
            continue
        try:
            value = parse_numeric(values[source_name])
        except ValueError:
            numeric_failures.append(source_name)
            continue
        if not math.isfinite(value) or value not in {0.0, 1.0}:
            invalid_digital.append(source_name)
            continue
        digital_values[source_name] = value == 1.0

    if numeric_failures:
        reasons[RowRuleId.NUMERIC_PARSE] = (
            f"Unparseable numeric value(s): {', '.join(numeric_failures)}."
        )
    if nonfinite_analog:
        reasons[RowRuleId.ANALOG_NONFINITE] = (
            f"NaN or infinity in analog field(s): {', '.join(nonfinite_analog)}."
        )
    if invalid_digital:
        reasons[RowRuleId.DIGITAL_DOMAIN] = (
            f"Digital value outside numeric 0/1 domain: {', '.join(invalid_digital)}."
        )
    if timestamp is not None and timestamp.date() != expected_source_date:
        reasons[RowRuleId.SOURCE_DATE_MISMATCH] = (
            f"Timestamp date {timestamp.date().isoformat()} does not match expected "
            f"source_date {expected_source_date.isoformat()}."
        )

    issues = _ordered_issues(reasons)
    normalized_record: NormalizedRecord | None = None
    if not issues:
        assert record_index is not None and timestamp is not None
        normalized_record = NormalizedRecord(
            record_index=record_index,
            event_timestamp_local=timestamp,
            tp2=analog_values["TP2"],
            tp3=analog_values["TP3"],
            h1=analog_values["H1"],
            dv_pressure=analog_values["DV_pressure"],
            reservoirs=analog_values["Reservoirs"],
            oil_temperature=analog_values["Oil_temperature"],
            motor_current=analog_values["Motor_current"],
            comp=digital_values["COMP"],
            dv_electric=digital_values["DV_eletric"],
            towers=digital_values["Towers"],
            mpg=digital_values["MPG"],
            lps=digital_values["LPS"],
            pressure_switch=digital_values["Pressure_switch"],
            oil_level=digital_values["Oil_level"],
            caudal_impulses=digital_values["Caudal_impulses"],
            source_date=timestamp.date(),
            event_timestamp_timezone_status=TIMESTAMP_TIMEZONE_STATUS,
        )

    return RowValidation(
        source_row_number,
        record.original_record,
        fields,
        record_index,
        timestamp,
        normalized_record,
        issues,
    )


def apply_duplicate_rules(rows: tuple[RowValidation, ...]) -> tuple[RowValidation, ...]:
    """Quarantine every member of each duplicate group, independent of row order."""
    index_counts = Counter(
        row.parsed_record_index for row in rows if row.parsed_record_index is not None
    )
    timestamp_counts = Counter(
        row.parsed_timestamp for row in rows if row.parsed_timestamp is not None
    )
    results: list[RowValidation] = []

    for row in rows:
        reasons = {issue.rule_id: issue.reason for issue in row.issues}
        if row.parsed_record_index is not None and index_counts[row.parsed_record_index] > 1:
            reasons[RowRuleId.DUPLICATE_RECORD_INDEX] = (
                f"record_index {row.parsed_record_index} occurs more than once in the input object."
            )
        if row.parsed_timestamp is not None and timestamp_counts[row.parsed_timestamp] > 1:
            reasons[RowRuleId.DUPLICATE_TIMESTAMP] = (
                f"timestamp {row.parsed_timestamp.strftime('%Y-%m-%d %H:%M:%S')} occurs more "
                "than once in the input object."
            )
        issues = _ordered_issues(reasons)
        results.append(
            replace(
                row,
                normalized_record=None if issues else row.normalized_record,
                issues=issues,
            )
        )

    return tuple(results)

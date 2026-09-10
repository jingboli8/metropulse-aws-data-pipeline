from __future__ import annotations

import csv
import io
from datetime import UTC, date, datetime

import pytest

from metropulse import SourceMetadata, transform_daily_csv
from metropulse.metrics import BatchRuleId, build_batch_errors, reconcile_counts
from metropulse.quarantine import quarantine_record_json
from metropulse.schema import SOURCE_FIELDS
from metropulse.validation import RowRuleId

EXPECTED_DATE = date(2020, 2, 1)
PROCESSING_TIME = datetime(2026, 9, 10, 9, 30, tzinfo=UTC)
PIPELINE_VERSION = "phase-1-test"
SOURCE_METADATA = SourceMetadata(
    source_bucket="example-raw",
    source_object_key="raw/source_date=2020-02-01/day.csv",
    source_object_version_id="version-1",
    source_object_etag="etag-1",
    source_object_checksum="sha256:abc123",
)
VALID_ROW = [
    "1",
    "2020-02-01 00:00:00",
    "-0.014",
    "8.5",
    "8.2",
    "-0.02",
    "8.5",
    "60.0",
    "4.0",
    "1.0",
    "0.0",
    "1.0",
    "1.0",
    "0.0",
    "1.0",
    "0.0",
    "1.0",
]


def _csv_text(
    rows: list[list[str]],
    *,
    header: tuple[str, ...] = SOURCE_FIELDS,
) -> str:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue()


def _row(*, record_index: int, timestamp: str) -> list[str]:
    row = VALID_ROW.copy()
    row[0] = str(record_index)
    row[1] = timestamp
    return row


def _transform(
    rows: list[list[str]],
    *,
    header: tuple[str, ...] = SOURCE_FIELDS,
):
    return transform_daily_csv(
        _csv_text(rows, header=header),
        expected_source_date=EXPECTED_DATE,
        source_metadata=SOURCE_METADATA,
        processing_timestamp=PROCESSING_TIME,
        pipeline_version=PIPELINE_VERSION,
    )


def _rule_ids(result, row: int = 0) -> tuple[str, ...]:
    return result.quarantine_records[row].rule_ids


def _warning_ids(result) -> tuple[str, ...]:
    return tuple(warning.rule_id for warning in result.batch_warnings)


def _error_ids(result) -> tuple[str, ...]:
    return tuple(error.rule_id for error in result.batch_errors)


def test_fully_valid_row_is_normalized() -> None:
    result = _transform([VALID_ROW])
    assert (result.input_row_count, result.valid_row_count, result.quarantine_row_count) == (
        1,
        1,
        0,
    )
    record = result.valid_records[0]
    assert record.record_index == 1
    assert record.event_timestamp_local == datetime(2020, 2, 1)
    assert record.event_timestamp_local.tzinfo is None
    assert record.event_timestamp_timezone_status == "unknown"
    assert record.source_date == EXPECTED_DATE
    assert record.tp2 == -0.014
    assert record.comp is True
    assert record.dv_electric is False
    assert result.reconciliation.matches


def test_multiple_valid_rows_from_text_stream() -> None:
    rows = [
        _row(record_index=1, timestamp="2020-02-01 00:00:00"),
        _row(record_index=2, timestamp="2020-02-01 00:00:10"),
    ]
    source = io.StringIO(_csv_text(rows), newline="")
    result = transform_daily_csv(
        source,
        expected_source_date=EXPECTED_DATE,
        source_metadata=SOURCE_METADATA,
        processing_timestamp=PROCESSING_TIME,
        pipeline_version=PIPELINE_VERSION,
    )
    assert result.valid_row_count == 2
    assert result.quarantine_records == ()
    assert result.batch_metrics.timestamp_deltas_seconds == (10,)


def test_missing_required_value_is_quarantined() -> None:
    row = VALID_ROW.copy()
    row[5] = ""
    result = _transform([row])
    assert _rule_ids(result) == (RowRuleId.REQUIRED_MISSING,)


def test_invalid_timestamp_is_quarantined() -> None:
    row = VALID_ROW.copy()
    row[1] = "2020-02-01T00:00:00Z"
    result = _transform([row])
    assert _rule_ids(result) == (RowRuleId.TIMESTAMP_PARSE,)


def test_invalid_numeric_value_is_quarantined() -> None:
    row = VALID_ROW.copy()
    row[3] = "not-a-number"
    result = _transform([row])
    assert _rule_ids(result) == (RowRuleId.NUMERIC_PARSE,)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_analog_values_are_quarantined(value: str) -> None:
    row = VALID_ROW.copy()
    row[2] = value
    result = _transform([row])
    assert _rule_ids(result) == (RowRuleId.ANALOG_NONFINITE,)


def test_invalid_digital_domain_is_quarantined() -> None:
    row = VALID_ROW.copy()
    row[9] = "2.0"
    result = _transform([row])
    assert _rule_ids(result) == (RowRuleId.DIGITAL_DOMAIN,)


def test_wrong_column_count_preserves_original_row() -> None:
    row = VALID_ROW[:-1]
    source = _csv_text([row])
    result = transform_daily_csv(
        source,
        expected_source_date=EXPECTED_DATE,
        source_metadata=SOURCE_METADATA,
        processing_timestamp=PROCESSING_TIME,
        pipeline_version=PIPELINE_VERSION,
    )
    assert _rule_ids(result) == (RowRuleId.COLUMN_COUNT,)
    quarantine = result.quarantine_records[0]
    assert quarantine.original_field_values == tuple(row)
    assert quarantine.original_record == source.splitlines(keepends=True)[1]


def test_source_date_mismatch_is_quarantined() -> None:
    row = VALID_ROW.copy()
    row[1] = "2020-02-02 00:00:00"
    result = _transform([row])
    assert _rule_ids(result) == (RowRuleId.SOURCE_DATE_MISMATCH,)


def test_every_duplicate_record_index_occurrence_is_quarantined() -> None:
    rows = [
        _row(record_index=1, timestamp="2020-02-01 00:00:00"),
        _row(record_index=1, timestamp="2020-02-01 00:00:10"),
    ]
    result = _transform(rows)
    assert result.valid_row_count == 0
    assert result.quarantine_row_count == 2
    assert all(
        record.rule_ids == (RowRuleId.DUPLICATE_RECORD_INDEX,)
        for record in result.quarantine_records
    )


def test_every_duplicate_timestamp_occurrence_is_quarantined() -> None:
    rows = [
        _row(record_index=1, timestamp="2020-02-01 00:00:00"),
        _row(record_index=2, timestamp="2020-02-01 00:00:00"),
    ]
    result = _transform(rows)
    assert result.valid_row_count == 0
    assert result.quarantine_row_count == 2
    assert all(
        record.rule_ids == (RowRuleId.DUPLICATE_TIMESTAMP,) for record in result.quarantine_records
    )


def test_rule_order_is_stable_for_compound_duplicate_failures() -> None:
    rows = [
        _row(record_index=1, timestamp="2020-02-01 00:00:00"),
        _row(record_index=1, timestamp="2020-02-01 00:00:00"),
    ]
    result = _transform(rows)
    expected = (RowRuleId.DUPLICATE_RECORD_INDEX, RowRuleId.DUPLICATE_TIMESTAMP)
    assert all(record.rule_ids == expected for record in result.quarantine_records)


def test_non_monotonic_timestamp_is_warning_without_quarantine() -> None:
    rows = [
        _row(record_index=1, timestamp="2020-02-01 00:00:10"),
        _row(record_index=2, timestamp="2020-02-01 00:00:00"),
    ]
    result = _transform(rows)
    assert result.valid_row_count == 2
    assert result.quarantine_row_count == 0
    assert result.batch_metrics.out_of_order_transition_count == 1
    assert BatchRuleId.TIMESTAMP_ORDER in _warning_ids(result)


def test_normal_9_to_13_second_sampling_does_not_quarantine_or_warn() -> None:
    timestamps = [
        "2020-02-01 00:00:00",
        "2020-02-01 00:00:09",
        "2020-02-01 00:00:19",
        "2020-02-01 00:00:30",
        "2020-02-01 00:00:42",
        "2020-02-01 00:00:55",
    ]
    result = _transform(
        [_row(record_index=index, timestamp=value) for index, value in enumerate(timestamps)]
    )
    assert result.valid_row_count == 6
    assert result.quarantine_row_count == 0
    assert result.batch_metrics.timestamp_deltas_seconds == (9, 10, 11, 12, 13)
    assert tuple(
        (item.seconds, item.count) for item in result.batch_metrics.sampling_interval_distribution
    ) == ((9, 1), (10, 1), (11, 1), (12, 1), (13, 1))
    assert result.batch_metrics.abnormal_sampling_interval_count == 0
    assert BatchRuleId.SAMPLING_INTERVAL not in _warning_ids(result)


def test_gap_over_60_seconds_is_metric_and_warning_only() -> None:
    rows = [
        _row(record_index=1, timestamp="2020-02-01 00:00:00"),
        _row(record_index=2, timestamp="2020-02-01 00:01:01"),
    ]
    result = _transform(rows)
    assert result.valid_row_count == 2
    assert result.quarantine_row_count == 0
    assert result.batch_metrics.significant_gap_count == 1
    assert result.batch_metrics.maximum_gap_seconds == 61
    assert BatchRuleId.SIGNIFICANT_GAP in _warning_ids(result)


def test_negative_pressure_is_not_rejected_for_being_negative() -> None:
    row = VALID_ROW.copy()
    row[2] = "-999.5"
    result = _transform([row])
    assert result.valid_row_count == 1
    assert result.valid_records[0].tp2 == -999.5


def test_low_daily_row_count_is_warning_without_quarantine() -> None:
    result = _transform([VALID_ROW])
    assert result.batch_metrics.unexpectedly_low_daily_row_count
    assert BatchRuleId.LOW_ROW_COUNT in _warning_ids(result)
    assert result.valid_row_count == 1
    assert result.quarantine_row_count == 0


@pytest.mark.parametrize("source", ["", _csv_text([])])
def test_empty_input_is_batch_error(source: str) -> None:
    result = transform_daily_csv(
        source,
        expected_source_date=EXPECTED_DATE,
        source_metadata=SOURCE_METADATA,
        processing_timestamp=PROCESSING_TIME,
        pipeline_version=PIPELINE_VERSION,
    )
    assert result.input_row_count == 0
    assert result.batch_metrics.empty_input
    assert BatchRuleId.EMPTY_INPUT in _error_ids(result)
    assert not result.reconciliation.applicable


def test_schema_drift_is_batch_error_without_row_publication() -> None:
    header = (*SOURCE_FIELDS[:-1], "Caudal_impulse")
    result = _transform([VALID_ROW], header=header)
    assert result.input_row_count == 1
    assert result.valid_records == ()
    assert result.quarantine_records == ()
    assert result.batch_metrics.schema_drift
    assert BatchRuleId.SCHEMA_DRIFT in _error_ids(result)
    assert not result.reconciliation.applicable


def test_exact_count_reconciliation() -> None:
    invalid = _row(record_index=2, timestamp="2020-02-01 00:00:10")
    invalid[3] = "bad"
    result = _transform([VALID_ROW, invalid])
    assert (result.input_row_count, result.valid_row_count, result.quarantine_row_count) == (
        2,
        1,
        1,
    )
    assert result.reconciliation.accounted_row_count == 2
    assert result.reconciliation.matches
    assert result.reconciliation.applicable


def test_reconciliation_mismatch_is_deterministic() -> None:
    reconciliation = reconcile_counts(3, 1, 1)
    assert reconciliation.accounted_row_count == 2
    assert not reconciliation.matches
    assert reconciliation.applicable
    errors = build_batch_errors(
        empty_input=False,
        schema_drift=False,
        reconciliation=reconciliation,
    )
    assert tuple(error.rule_id for error in errors) == (BatchRuleId.RECONCILIATION_MISMATCH,)


def test_repeated_execution_is_identical() -> None:
    invalid = _row(record_index=2, timestamp="2020-02-01 00:00:10")
    invalid[9] = "7"
    source = _csv_text([VALID_ROW, invalid])
    arguments = {
        "expected_source_date": EXPECTED_DATE,
        "source_metadata": SOURCE_METADATA,
        "processing_timestamp": PROCESSING_TIME,
        "pipeline_version": PIPELINE_VERSION,
    }
    first = transform_daily_csv(source, **arguments)
    second = transform_daily_csv(source, **arguments)
    assert first == second
    assert quarantine_record_json(first.quarantine_records[0]) == quarantine_record_json(
        second.quarantine_records[0]
    )


def test_quarantine_metadata_is_complete_and_serialization_ready() -> None:
    row = VALID_ROW.copy()
    row[9] = "7"
    quarantine = _transform([row]).quarantine_records[0]
    payload = quarantine.to_dict()
    assert payload == {
        "original_record": _csv_text([row]).splitlines(keepends=True)[1],
        "original_field_values": row,
        "source_row_number": 1,
        "rule_ids": [RowRuleId.DIGITAL_DOMAIN],
        "rejection_reasons": ["Digital value outside numeric 0/1 domain: COMP."],
        "source_bucket": "example-raw",
        "source_object_key": "raw/source_date=2020-02-01/day.csv",
        "source_object_version_id": "version-1",
        "source_object_etag": "etag-1",
        "source_object_checksum": "sha256:abc123",
        "processing_timestamp": "2026-09-10T09:30:00Z",
        "pipeline_version": PIPELINE_VERSION,
        "expected_source_date": "2020-02-01",
    }


def test_processing_timestamp_must_be_caller_supplied_utc() -> None:
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        transform_daily_csv(
            _csv_text([VALID_ROW]),
            expected_source_date=EXPECTED_DATE,
            source_metadata=SOURCE_METADATA,
            processing_timestamp=datetime(2026, 9, 10, 9, 30),
            pipeline_version=PIPELINE_VERSION,
        )

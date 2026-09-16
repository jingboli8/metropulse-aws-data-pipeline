"""Validation and portable serialization of explicit monthly selections."""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any

from metropulse.compaction_models import MonthlySelection, SelectedDailyInput, SelectionError
from metropulse.manifest import MANIFEST_VERSION
from metropulse.schema import SCHEMA_VERSION

SUPPORTED_SOURCE = "metropt3"


def validate_selection(selection: MonthlySelection) -> tuple[SelectedDailyInput, ...]:
    """Validate and return selected inputs in canonical source-date order."""
    if selection.source_name.lower() != SUPPORTED_SOURCE:
        raise SelectionError("unsupported source")
    if not 1 <= selection.month <= 12 or not 1 <= selection.year <= 9999:
        raise SelectionError("invalid target year/month")
    if not selection.inputs:
        raise SelectionError("selection contains no input")
    if selection.known_source_start > selection.known_source_end:
        raise SelectionError("known source range is reversed")
    ordered = tuple(sorted(selection.inputs, key=lambda item: item.source_date))
    dates = [item.source_date for item in ordered]
    if len(set(dates)) != len(dates):
        raise SelectionError("selection contains duplicate approved dates")
    if tuple(sorted(set(selection.expected_raw_dates))) != selection.expected_raw_dates:
        raise SelectionError("expected raw dates must be unique and sorted")
    for item in ordered:
        _validate_input(item, selection.year, selection.month)
    if any(
        value.year != selection.year or value.month != selection.month
        for value in selection.expected_raw_dates
    ):
        raise SelectionError("expected raw inventory contains a date outside the target month")
    if any(value not in selection.expected_raw_dates for value in dates):
        raise SelectionError("selected date is absent from the expected raw-date inventory")
    if not (selection.known_source_start <= dates[0] <= dates[-1] <= selection.known_source_end):
        raise SelectionError("selected dates are outside the known source range")
    last_calendar_date = date(
        selection.year, selection.month, monthrange(selection.year, selection.month)[1]
    )
    expected_terminal_partial = selection.known_source_end < last_calendar_date and (
        selection.known_source_end.year,
        selection.known_source_end.month,
    ) == (selection.year, selection.month)
    if selection.terminal_partial_month != expected_terminal_partial:
        raise SelectionError("terminal partial-month metadata is inconsistent")
    return ordered


def missing_expected_dates(selection: MonthlySelection) -> tuple[date, ...]:
    """Return expected raw dates that have no selected approved input."""
    selected = {item.source_date for item in selection.inputs}
    return tuple(value for value in selection.expected_raw_dates if value not in selected)


def selection_payload(selection: MonthlySelection) -> dict[str, Any]:
    """Return the canonical, portable selection evidence."""
    ordered = validate_selection(selection)
    return {
        "expected_raw_dates": [value.isoformat() for value in selection.expected_raw_dates],
        "inputs": [_input_payload(item) for item in ordered],
        "known_source_end": selection.known_source_end.isoformat(),
        "known_source_start": selection.known_source_start.isoformat(),
        "month": f"{selection.month:02d}",
        "source_name": selection.source_name.lower(),
        "terminal_partial_month": selection.terminal_partial_month,
        "year": f"{selection.year:04d}",
    }


def _validate_input(item: SelectedDailyInput, year: int, month: int) -> None:
    if (item.source_date.year, item.source_date.month) != (year, month):
        raise SelectionError("selected input belongs to a different month")
    if not item.completion_marker_bucket or not item.completion_marker_key:
        raise SelectionError("completion marker is not pinned")
    if item.completion_marker_identity_kind not in {"version_id", "sha256", "etag"}:
        raise SelectionError("unsupported completion-marker identity kind")
    if not item.completion_marker_identity_value:
        raise SelectionError("completion marker is not pinned")
    if item.completion_marker_identity_kind == "sha256" and (
        len(item.completion_marker_identity_value) != 64
        or any(
            value not in "0123456789abcdefABCDEF" for value in item.completion_marker_identity_value
        )
    ):
        raise SelectionError("invalid completion-marker SHA-256")
    if not all(
        (
            item.processing_identity,
            item.staging_bucket,
            item.staging_key,
            item.pipeline_version,
            item.schema_version,
            item.manifest_version,
        )
    ):
        raise SelectionError("selected input contains an empty required identity field")
    if item.input_row_count != item.valid_row_count + item.quarantine_row_count:
        raise SelectionError("selected daily counts do not reconcile")
    if item.valid_row_count < 0 or item.quarantine_row_count < 0:
        raise SelectionError("selected daily counts cannot be negative")
    if item.valid_row_count == 0:
        if item.first_valid_timestamp is not None or item.last_valid_timestamp is not None:
            raise SelectionError("empty daily input has boundary timestamps")
    elif item.first_valid_timestamp is None or item.last_valid_timestamp is None:
        raise SelectionError("nonempty daily input lacks boundary timestamps")
    for value in (item.first_valid_timestamp, item.last_valid_timestamp):
        if value is not None and value.tzinfo is not None:
            raise SelectionError("selected timestamps must remain timezone-naive")
        if value is not None and value.date() != item.source_date:
            raise SelectionError("selected boundary timestamp conflicts with source_date")
    if (
        item.first_valid_timestamp is not None
        and item.last_valid_timestamp is not None
        and item.first_valid_timestamp > item.last_valid_timestamp
    ):
        raise SelectionError("selected daily boundary is reversed")
    if item.schema_version != SCHEMA_VERSION or item.manifest_version != MANIFEST_VERSION:
        raise SelectionError("unsupported schema or manifest version")
    if len(item.staging_sha256) != 64 or any(
        c not in "0123456789abcdefABCDEF" for c in item.staging_sha256
    ):
        raise SelectionError("invalid staging SHA-256")
    if item.staging_byte_size <= 0:
        raise SelectionError("staging byte size must be positive")


def _input_payload(item: SelectedDailyInput) -> dict[str, Any]:
    return {
        "completion_marker": {
            "bucket": item.completion_marker_bucket,
            "identity_kind": item.completion_marker_identity_kind,
            "identity_value": (
                item.completion_marker_identity_value.lower()
                if item.completion_marker_identity_kind == "sha256"
                else item.completion_marker_identity_value
            ),
            "key": item.completion_marker_key,
        },
        "first_valid_timestamp": item.first_valid_timestamp.isoformat()
        if item.first_valid_timestamp
        else None,
        "input_row_count": item.input_row_count,
        "last_valid_timestamp": item.last_valid_timestamp.isoformat()
        if item.last_valid_timestamp
        else None,
        "manifest_version": item.manifest_version,
        "pipeline_version": item.pipeline_version,
        "processing_identity": item.processing_identity,
        "quarantine_row_count": item.quarantine_row_count,
        "schema_version": item.schema_version,
        "source_date": item.source_date.isoformat(),
        "staging": {
            "bucket": item.staging_bucket,
            "byte_size": item.staging_byte_size,
            "key": item.staging_key,
            "sha256": item.staging_sha256.lower(),
        },
        "valid_row_count": item.valid_row_count,
    }

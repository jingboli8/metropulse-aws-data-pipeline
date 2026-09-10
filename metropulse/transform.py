"""Public orchestration API for one in-memory daily CSV object."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import date, datetime

from metropulse.metrics import (
    build_batch_errors,
    build_batch_metrics,
    build_batch_warnings,
    reconcile_counts,
)
from metropulse.models import SourceMetadata, TransformationResult
from metropulse.parsing import read_source_csv
from metropulse.quarantine import build_quarantine_record, validate_processing_context
from metropulse.schema import SOURCE_FIELDS
from metropulse.validation import apply_duplicate_rules, validate_source_row


def transform_daily_csv(
    source: str | Iterable[str],
    *,
    expected_source_date: date,
    source_metadata: SourceMetadata,
    processing_timestamp: datetime,
    pipeline_version: str,
) -> TransformationResult:
    """Parse, validate, and normalize one daily MetroPT-3 CSV object."""
    validate_processing_context(processing_timestamp, pipeline_version)
    header, source_records = read_source_csv(source)
    input_row_count = len(source_records)
    schema_drift = header is not None and header != SOURCE_FIELDS
    empty_input = input_row_count == 0

    if header is None or schema_drift or empty_input:
        reconciliation = reconcile_counts(
            input_row_count,
            0,
            0,
            applicable=False,
        )
        metrics = build_batch_metrics(
            timestamps=(),
            input_row_count=input_row_count,
            valid_row_count=0,
            quarantine_row_count=0,
            schema_drift=schema_drift,
            quarantine_rule_counts=Counter(),
        )
        return TransformationResult(
            valid_records=(),
            quarantine_records=(),
            batch_metrics=metrics,
            batch_warnings=build_batch_warnings(metrics),
            batch_errors=build_batch_errors(
                empty_input=empty_input,
                schema_drift=schema_drift,
                reconciliation=reconciliation,
            ),
            input_row_count=input_row_count,
            valid_row_count=0,
            quarantine_row_count=0,
            reconciliation=reconciliation,
        )

    evaluated = tuple(
        validate_source_row(
            record,
            source_row_number=source_row_number,
            expected_source_date=expected_source_date,
        )
        for source_row_number, record in enumerate(source_records, start=1)
    )
    evaluated = apply_duplicate_rules(evaluated)
    valid_records = tuple(
        row.normalized_record
        for row in evaluated
        if not row.issues and row.normalized_record is not None
    )
    quarantine_records = tuple(
        build_quarantine_record(
            row,
            source_metadata=source_metadata,
            processing_timestamp=processing_timestamp,
            pipeline_version=pipeline_version,
            expected_source_date=expected_source_date,
        )
        for row in evaluated
        if row.issues
    )
    rule_counts = Counter(rule_id for record in quarantine_records for rule_id in record.rule_ids)
    reconciliation = reconcile_counts(
        input_row_count,
        len(valid_records),
        len(quarantine_records),
    )
    metrics = build_batch_metrics(
        timestamps=tuple(
            row.parsed_timestamp for row in evaluated if row.parsed_timestamp is not None
        ),
        input_row_count=input_row_count,
        valid_row_count=len(valid_records),
        quarantine_row_count=len(quarantine_records),
        schema_drift=False,
        quarantine_rule_counts=rule_counts,
    )
    return TransformationResult(
        valid_records=valid_records,
        quarantine_records=quarantine_records,
        batch_metrics=metrics,
        batch_warnings=build_batch_warnings(metrics),
        batch_errors=build_batch_errors(
            empty_input=False,
            schema_drift=False,
            reconciliation=reconciliation,
        ),
        input_row_count=input_row_count,
        valid_row_count=len(valid_records),
        quarantine_row_count=len(quarantine_records),
        reconciliation=reconciliation,
    )

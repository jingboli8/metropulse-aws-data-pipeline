"""Deterministic batch metrics, warnings, and reconciliation."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum

from metropulse.models import (
    BatchError,
    BatchMetrics,
    BatchWarning,
    IntervalFrequency,
    ReconciliationResult,
    RuleCount,
)

EXPECTED_INTERVAL_MIN_SECONDS = 9
EXPECTED_INTERVAL_MAX_SECONDS = 13
SIGNIFICANT_GAP_SECONDS = 60
LOW_DAILY_ROW_COUNT_THRESHOLD = 3_718


class BatchRuleId(StrEnum):
    """Stable batch rule identifiers from the Phase 0 contract."""

    TIMESTAMP_ORDER = "BATCH_TIMESTAMP_ORDER"
    SAMPLING_INTERVAL = "BATCH_SAMPLING_INTERVAL"
    SIGNIFICANT_GAP = "BATCH_SIGNIFICANT_GAP"
    LOW_ROW_COUNT = "BATCH_LOW_ROW_COUNT"
    SCHEMA_DRIFT = "BATCH_SCHEMA_DRIFT"
    EMPTY_INPUT = "BATCH_EMPTY_INPUT"
    RECONCILIATION_MISMATCH = "BATCH_RECONCILIATION_MISMATCH"


def reconcile_counts(
    input_row_count: int,
    valid_row_count: int,
    quarantine_row_count: int,
    *,
    applicable: bool = True,
) -> ReconciliationResult:
    """Compare input rows with valid plus quarantined rows."""
    counts = (input_row_count, valid_row_count, quarantine_row_count)
    if any(count < 0 for count in counts):
        raise ValueError("Row counts cannot be negative")
    accounted = valid_row_count + quarantine_row_count
    return ReconciliationResult(
        input_row_count=input_row_count,
        valid_row_count=valid_row_count,
        quarantine_row_count=quarantine_row_count,
        accounted_row_count=accounted,
        matches=input_row_count == accounted,
        applicable=applicable,
    )


def build_batch_metrics(
    *,
    timestamps: Sequence[datetime],
    input_row_count: int,
    valid_row_count: int,
    quarantine_row_count: int,
    schema_drift: bool,
    quarantine_rule_counts: Counter[str],
) -> BatchMetrics:
    """Calculate ordered timestamp and row-count observations."""
    deltas = tuple(
        int((current - previous).total_seconds())
        for previous, current in zip(timestamps, timestamps[1:], strict=False)
    )
    distribution = Counter(deltas)
    significant_gaps = tuple(delta for delta in deltas if delta > SIGNIFICANT_GAP_SECONDS)
    return BatchMetrics(
        input_row_count=input_row_count,
        valid_row_count=valid_row_count,
        quarantine_row_count=quarantine_row_count,
        parseable_timestamp_count=len(timestamps),
        timestamp_deltas_seconds=deltas,
        sampling_interval_distribution=tuple(
            IntervalFrequency(seconds, distribution[seconds]) for seconds in sorted(distribution)
        ),
        out_of_order_transition_count=sum(delta < 0 for delta in deltas),
        abnormal_sampling_interval_count=sum(
            delta > 0
            and not EXPECTED_INTERVAL_MIN_SECONDS <= delta <= EXPECTED_INTERVAL_MAX_SECONDS
            for delta in deltas
        ),
        significant_gap_count=len(significant_gaps),
        maximum_gap_seconds=max(significant_gaps, default=None),
        unexpectedly_low_daily_row_count=input_row_count < LOW_DAILY_ROW_COUNT_THRESHOLD,
        empty_input=input_row_count == 0,
        schema_drift=schema_drift,
        quarantine_rule_counts=tuple(
            RuleCount(rule_id, quarantine_rule_counts[rule_id])
            for rule_id in sorted(quarantine_rule_counts)
        ),
    )


def build_batch_warnings(metrics: BatchMetrics) -> tuple[BatchWarning, ...]:
    """Create stable, non-quarantining warnings from batch metrics."""
    warnings: list[BatchWarning] = []
    if metrics.out_of_order_transition_count:
        warnings.append(
            BatchWarning(
                BatchRuleId.TIMESTAMP_ORDER.value,
                "Parsed timestamps decrease in source order.",
                metrics.out_of_order_transition_count,
            )
        )
    if metrics.abnormal_sampling_interval_count:
        warnings.append(
            BatchWarning(
                BatchRuleId.SAMPLING_INTERVAL.value,
                "Positive timestamp intervals outside the expected 9-13 second band were observed.",
                metrics.abnormal_sampling_interval_count,
            )
        )
    if metrics.significant_gap_count:
        warnings.append(
            BatchWarning(
                BatchRuleId.SIGNIFICANT_GAP.value,
                "Timestamp gaps greater than 60 seconds were observed; rows were not imputed.",
                metrics.significant_gap_count,
            )
        )
    if metrics.unexpectedly_low_daily_row_count:
        warnings.append(
            BatchWarning(
                BatchRuleId.LOW_ROW_COUNT.value,
                f"Daily input has fewer than {LOW_DAILY_ROW_COUNT_THRESHOLD} rows.",
                metrics.input_row_count,
            )
        )
    return tuple(warnings)


def build_batch_errors(
    *,
    empty_input: bool,
    schema_drift: bool,
    reconciliation: ReconciliationResult,
) -> tuple[BatchError, ...]:
    """Create stable publication-blocking errors from batch state."""
    errors: list[BatchError] = []
    if schema_drift:
        errors.append(
            BatchError(
                BatchRuleId.SCHEMA_DRIFT.value,
                "Source header does not match the exact schema.",
            )
        )
    if empty_input:
        errors.append(BatchError(BatchRuleId.EMPTY_INPUT.value, "Input contains no data records."))
    if reconciliation.applicable and not reconciliation.matches:
        errors.append(
            BatchError(
                BatchRuleId.RECONCILIATION_MISMATCH.value,
                "Input rows do not equal valid rows plus quarantine rows.",
            )
        )
    return tuple(errors)

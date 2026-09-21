"""Pure chronological boundary calculations shared by local and scheduled audits."""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime

EXPECTED_INTERVAL_MIN_SECONDS = 9
EXPECTED_INTERVAL_MAX_SECONDS = 13
SIGNIFICANT_GAP_SECONDS = 60


@dataclass(frozen=True)
class ContinuityEndpoint:
    """The ordered timestamp boundary for one daily or monthly partition."""

    label: str
    partition_date: date
    first_timestamp: datetime | None
    last_timestamp: datetime | None


@dataclass(frozen=True)
class ContinuityFinding:
    """One observed relationship between adjacent partition boundaries."""

    previous_label: str
    current_label: str
    missing_calendar_date_count: int
    delta_seconds: int | None
    status: str


def interval_summary(distribution: Counter[int]) -> dict[str, int | float | None]:
    """Summarize a timestamp-delta distribution without changing observations."""
    significant = [
        seconds
        for seconds, count in sorted(distribution.items())
        if seconds > SIGNIFICANT_GAP_SECONDS
        for _ in range(count)
    ]
    return {
        "abnormal_positive_interval_count": sum(
            count
            for seconds, count in distribution.items()
            if seconds > 0
            and not EXPECTED_INTERVAL_MIN_SECONDS <= seconds <= EXPECTED_INTERVAL_MAX_SECONDS
        ),
        "maximum_significant_gap_seconds": max(significant, default=None),
        "median_significant_gap_seconds": statistics.median(significant) if significant else None,
        "minimum_significant_gap_seconds": min(significant, default=None),
        "significant_gap_count": len(significant),
        "total_delta_count": sum(distribution.values()),
    }


def evaluate_boundaries(
    endpoints: tuple[ContinuityEndpoint, ...],
) -> tuple[tuple[ContinuityFinding, ...], Counter[int]]:
    """Evaluate adjacent ordered endpoints and return findings plus deltas."""
    findings: list[ContinuityFinding] = []
    distribution: Counter[int] = Counter()
    for previous, current in zip(endpoints, endpoints[1:], strict=False):
        missing_dates = max((current.partition_date - previous.partition_date).days - 1, 0)
        if previous.last_timestamp is None or current.first_timestamp is None:
            findings.append(
                ContinuityFinding(
                    previous.label,
                    current.label,
                    missing_dates,
                    None,
                    "unassessable_empty_or_fully_quarantined_partition",
                )
            )
            continue
        if previous.last_timestamp.tzinfo is not None or current.first_timestamp.tzinfo is not None:
            raise ValueError("Boundary timestamps must remain timezone-naive.")
        delta = int((current.first_timestamp - previous.last_timestamp).total_seconds())
        distribution[delta] += 1
        if delta < 0:
            status = "reversed_overlap"
        elif delta == 0:
            status = "overlap"
        elif delta > SIGNIFICANT_GAP_SECONDS:
            status = "significant_gap"
        elif not EXPECTED_INTERVAL_MIN_SECONDS <= delta <= EXPECTED_INTERVAL_MAX_SECONDS:
            status = "abnormal_positive_interval"
        else:
            status = "normal"
        findings.append(
            ContinuityFinding(
                previous.label,
                current.label,
                missing_dates,
                delta,
                status,
            )
        )
    return tuple(findings), distribution


def compose_significant_gap_count(within_partition_count: int, boundaries: Counter[int]) -> int:
    """Compose within-partition and cross-partition significant-gap observations."""
    if within_partition_count < 0:
        raise ValueError("within-partition gap count cannot be negative")
    return within_partition_count + sum(
        count for seconds, count in boundaries.items() if seconds > SIGNIFICANT_GAP_SECONDS
    )

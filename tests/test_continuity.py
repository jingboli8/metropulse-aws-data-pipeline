from collections import Counter
from datetime import date, datetime

from metropulse.continuity import (
    ContinuityEndpoint,
    compose_significant_gap_count,
    evaluate_boundaries,
)


def test_shared_continuity_gap_overlap_reversal_and_unassessable() -> None:
    endpoints = (
        ContinuityEndpoint(
            "a", date(2020, 1, 1), datetime(2020, 1, 1), datetime(2020, 1, 1, 23, 59)
        ),
        ContinuityEndpoint("b", date(2020, 2, 1), datetime(2020, 2, 1), datetime(2020, 2, 1, 1)),
        ContinuityEndpoint("c", date(2020, 3, 1), datetime(2020, 2, 1, 1), datetime(2020, 3, 1, 1)),
        ContinuityEndpoint("d", date(2020, 4, 1), datetime(2020, 3, 1), datetime(2020, 4, 1)),
        ContinuityEndpoint("e", date(2020, 5, 1), None, None),
    )
    findings, distribution = evaluate_boundaries(endpoints)
    assert [item.status for item in findings] == [
        "significant_gap",
        "overlap",
        "reversed_overlap",
        "unassessable_empty_or_fully_quarantined_partition",
    ]
    assert sum(count for seconds, count in distribution.items() if seconds > 60) == 1


def test_generic_continuity_has_no_metropt3_gap_constant() -> None:
    endpoints = (
        ContinuityEndpoint("a", date(2020, 1, 1), datetime(2020, 1, 1), datetime(2020, 1, 1)),
        ContinuityEndpoint(
            "b", date(2020, 2, 1), datetime(2020, 1, 1, 0, 1, 1), datetime(2020, 2, 1)
        ),
    )
    findings, _ = evaluate_boundaries(endpoints)
    assert findings[0].delta_seconds == 61
    assert findings[0].status == "significant_gap"


def test_verified_acceptance_counts_compose_without_a_hard_coded_global_total() -> None:
    boundaries = Counter({61: 1, 120: 2, 500: 1, 10: 3})
    assert compose_significant_gap_count(327, boundaries) == 331

"""Small, testable helpers used by the MetroPT-3 profiler."""

from __future__ import annotations

from datetime import date


def choose_representative_days(daily_rows: list[tuple[date, int]]) -> dict[str, date]:
    """Choose distinct minimum, median-ranked, and maximum daily partitions."""
    if len(daily_rows) < 3:
        raise ValueError("At least three date partitions are required")
    ranked = sorted(daily_rows, key=lambda item: (item[1], item[0]))
    small = ranked[0]
    largest = ranked[-1]
    candidates = [item for item in ranked if item[0] not in {small[0], largest[0]}]
    median_target = sorted(count for _, count in daily_rows)[len(daily_rows) // 2]
    median = min(candidates, key=lambda item: (abs(item[1] - median_target), item[0]))
    return {"small": small[0], "median": median[0], "largest": largest[0]}


def estimated_partition_bytes(total_data_bytes: int, total_rows: int, rows: int) -> int:
    if total_rows <= 0 or rows < 0:
        raise ValueError("Row counts must be valid")
    return round(total_data_bytes * rows / total_rows)

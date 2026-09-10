from datetime import date

import pytest

from metropulse_feasibility.profile import choose_representative_days, estimated_partition_bytes


def test_choose_representative_days_are_distinct() -> None:
    rows = [
        (date(2020, 1, 1), 5),
        (date(2020, 1, 2), 10),
        (date(2020, 1, 3), 20),
        (date(2020, 1, 4), 100),
    ]
    assert choose_representative_days(rows) == {
        "small": date(2020, 1, 1),
        "median": date(2020, 1, 3),
        "largest": date(2020, 1, 4),
    }


def test_choose_representative_days_requires_three() -> None:
    with pytest.raises(ValueError, match="three"):
        choose_representative_days([(date(2020, 1, 1), 1)])


def test_estimated_partition_bytes() -> None:
    assert estimated_partition_bytes(1_000, 100, 25) == 250
    with pytest.raises(ValueError):
        estimated_partition_bytes(1_000, 0, 25)

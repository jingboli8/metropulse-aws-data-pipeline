from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime

import pytest

from metropulse.compaction_models import SelectionError
from metropulse.compaction_selection import missing_expected_dates, validate_selection


def test_selection_orders_inputs_and_reports_missing_dates(compact_fixture) -> None:
    selection, _ = compact_fixture()
    shuffled = replace(selection, inputs=tuple(reversed(selection.inputs)))
    assert [item.source_date.day for item in validate_selection(shuffled)] == [1, 2]
    assert date(2020, 2, 29) in missing_expected_dates(selection)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda selection: replace(selection, inputs=selection.inputs + (selection.inputs[0],)),
        lambda selection: replace(selection, source_name="other"),
        lambda selection: replace(selection, month=3),
        lambda selection: replace(selection, inputs=()),
        lambda selection: replace(
            selection,
            inputs=(replace(selection.inputs[0], completion_marker_identity_value=""),),
        ),
        lambda selection: replace(
            selection,
            inputs=(replace(selection.inputs[0], input_row_count=2), *selection.inputs[1:]),
        ),
    ],
)
def test_invalid_selections_are_rejected(compact_fixture, mutation) -> None:
    selection, _ = compact_fixture()
    with pytest.raises(SelectionError):
        validate_selection(mutation(selection))


def test_partial_terminal_month_must_be_explicit(compact_fixture) -> None:
    selection, _ = compact_fixture((1,))
    september = replace(
        selection,
        year=2020,
        month=9,
        inputs=(
            replace(
                selection.inputs[0],
                source_date=date(2020, 9, 1),
                first_valid_timestamp=datetime(2020, 9, 1),
                last_valid_timestamp=datetime(2020, 9, 1),
            ),
        ),
        expected_raw_dates=(date(2020, 9, 1),),
        terminal_partial_month=True,
    )
    validate_selection(september)

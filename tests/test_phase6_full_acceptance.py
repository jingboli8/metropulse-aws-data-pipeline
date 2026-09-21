from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from metropulse.compaction_local import read_local_staging, select_local_month
from metropulse.monthly_compaction import compact_month

ROOT = Path("data/local-lake")
GLOBAL_AUDIT = Path("artifacts/phase2-global-audit.json")


@pytest.mark.skipif(
    not ROOT.exists() or not GLOBAL_AUDIT.exists(),
    reason="ignored full local backfill/audit evidence is absent",
)
@pytest.mark.full_data
def test_full_local_dataset_monthly_contract() -> None:
    expected = {
        "2020-02": 214850,
        "2020-03": 230448,
        "2020-04": 198734,
        "2020-05": 212800,
        "2020-06": 216514,
        "2020-07": 222638,
        "2020-08": 220434,
        "2020-09": 530,
    }
    total = 0
    gap_total = 0
    input_count = 0
    results = []
    for month_text, rows in expected.items():
        year, month = map(int, month_text.split("-"))
        selection = select_local_month(
            ROOT,
            year=year,
            month=month,
            known_source_start=date(2020, 2, 1),
            known_source_end=date(2020, 9, 1),
        )
        result = compact_month(selection, lambda item: read_local_staging(ROOT, item))
        assert result.curated_row_count == rows
        assert result.selected_row_count == rows
        assert result.quarantine_row_count == 0
        input_count += len(selection.inputs)
        total += rows
        gap_total += result.significant_gap_count
        results.append(result)
        if month_text == "2020-02":
            assert [value.isoformat() for value in result.missing_dates] == ["2020-02-29"]
        elif month_text == "2020-04":
            assert [value.isoformat() for value in result.missing_dates] == ["2020-04-26"]
        else:
            assert result.missing_dates == ()
        if month_text == "2020-09":
            assert result.completion["terminal_partial_month"] is True
    assert total == 1_516_948
    assert input_count == 212
    assert gap_total == 327
    boundaries = [
        (current.first_timestamp - previous.last_timestamp).total_seconds()
        for previous, current in zip(results, results[1:], strict=False)
    ]
    assert sum(value > 60 for value in boundaries) == 4
    assert sum(value <= 0 for value in boundaries) == 0
    assert (
        json.loads(GLOBAL_AUDIT.read_text())["global_interval_metrics"]["significant_gap_count"]
        == 331
    )

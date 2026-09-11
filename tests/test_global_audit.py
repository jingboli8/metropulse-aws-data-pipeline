from __future__ import annotations

import json
from pathlib import Path

import pytest

from metropulse.global_audit import audit_partition_continuity, global_audit_text


def write_manifest(
    output_root: Path,
    source_date: str,
    *,
    first: str | None,
    last: str | None,
    valid_count: int = 2,
    within_deltas: tuple[int, ...] = (10,),
    quarantine_count: int = 0,
) -> Path:
    distribution = [
        {"count": within_deltas.count(seconds), "seconds": seconds}
        for seconds in sorted(set(within_deltas))
    ]
    manifest = {
        "batch_metrics": {
            "parseable_timestamp_count": len(within_deltas) + 1 if within_deltas else valid_count,
            "sampling_interval_distribution": distribution,
        },
        "first_valid_event_timestamp_local": first,
        "input_row_count": valid_count + quarantine_count,
        "last_valid_event_timestamp_local": last,
        "manifest_version": "1.1.0",
        "pipeline_version": "2.0.0-test",
        "quarantine_row_count": quarantine_count,
        "schema_version": "1.0.0",
        "source_date": source_date,
        "valid_row_count": valid_count,
        "valid_timestamp_count": valid_count,
    }
    path = (
        output_root / "control" / "source=metropt3" / f"source_date={source_date}" / "manifest.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_normal_ten_second_partition_boundary(tmp_path: Path) -> None:
    write_manifest(
        tmp_path,
        "2020-02-01",
        first="2020-02-01T23:59:40",
        last="2020-02-01T23:59:50",
    )
    write_manifest(
        tmp_path,
        "2020-02-02",
        first="2020-02-02T00:00:00",
        last="2020-02-02T00:00:10",
    )

    report = audit_partition_continuity(tmp_path)

    assert report["boundary_sampling_distribution"] == [{"count": 1, "seconds": 10}]
    assert report["boundary_interval_metrics"]["significant_gap_count"] == 0
    assert report["reconciliation"]["status"] == "passed"


def test_boundary_gap_and_global_reconciliation_do_not_change_quarantine(tmp_path: Path) -> None:
    first_path = write_manifest(
        tmp_path,
        "2020-02-01",
        first="2020-02-01T23:57:50",
        last="2020-02-01T23:58:00",
    )
    second_path = write_manifest(
        tmp_path,
        "2020-02-02",
        first="2020-02-02T00:00:00",
        last="2020-02-02T00:00:10",
    )
    before = {path: path.read_bytes() for path in (first_path, second_path)}

    report = audit_partition_continuity(tmp_path, expected_global_gap_count=1)

    assert report["within_partition_interval_metrics"]["significant_gap_count"] == 0
    assert report["boundary_interval_metrics"]["significant_gap_count"] == 1
    assert report["global_interval_metrics"]["significant_gap_count"] == 1
    assert report["global_interval_metrics"]["total_delta_count"] == 3
    assert report["reconciliation"]["delta_counts_match"]
    assert report["reconciliation"]["significant_gap_counts_match"]
    assert {path: path.read_bytes() for path in before} == before
    assert all(json.loads(path.read_text())["quarantine_row_count"] == 0 for path in before)


def test_missing_calendar_date_is_reported_and_boundary_is_measured(tmp_path: Path) -> None:
    write_manifest(
        tmp_path,
        "2020-02-01",
        first="2020-02-01T23:59:40",
        last="2020-02-01T23:59:50",
    )
    write_manifest(
        tmp_path,
        "2020-02-03",
        first="2020-02-03T00:00:00",
        last="2020-02-03T00:00:10",
    )

    report = audit_partition_continuity(tmp_path)

    assert report["missing_calendar_date_findings"] == [
        {
            "current_source_date": "2020-02-03",
            "missing_calendar_date_count": 1,
            "previous_source_date": "2020-02-01",
        }
    ]
    assert report["boundary_interval_metrics"]["significant_gap_count"] == 1


def test_overlapping_reversed_boundary_is_reported(tmp_path: Path) -> None:
    write_manifest(
        tmp_path,
        "2020-02-01",
        first="2020-02-01T23:59:50",
        last="2020-02-02T00:00:10",
    )
    write_manifest(
        tmp_path,
        "2020-02-02",
        first="2020-02-02T00:00:00",
        last="2020-02-02T00:00:10",
    )

    report = audit_partition_continuity(tmp_path)

    assert report["boundary_findings"][0]["delta_seconds"] == -10
    assert report["boundary_findings"][0]["status"] == "reversed_overlap"
    assert len(report["overlapping_or_reversed_boundary_findings"]) == 1


@pytest.mark.parametrize("quarantine_count", [0, 1])
def test_empty_or_fully_quarantined_partition_is_unassessable(
    tmp_path: Path, quarantine_count: int
) -> None:
    write_manifest(
        tmp_path,
        "2020-02-01",
        first="2020-02-01T23:59:40",
        last="2020-02-01T23:59:50",
    )
    write_manifest(
        tmp_path,
        "2020-02-02",
        first=None,
        last=None,
        valid_count=0,
        within_deltas=(),
        quarantine_count=quarantine_count,
    )
    write_manifest(
        tmp_path,
        "2020-02-03",
        first="2020-02-03T00:00:00",
        last="2020-02-03T00:00:10",
    )

    report = audit_partition_continuity(tmp_path)

    assert report["empty_or_fully_quarantined_partitions"] == ["2020-02-02"]
    assert report["reconciliation"]["unassessable_boundary_count"] == 2
    assert report["reconciliation"]["status"] == "incomplete"
    assert report["boundary_interval_metrics"]["total_delta_count"] == 0


def test_global_audit_serialization_is_deterministic(tmp_path: Path) -> None:
    write_manifest(
        tmp_path,
        "2020-02-01",
        first="2020-02-01T23:59:40",
        last="2020-02-01T23:59:50",
    )
    write_manifest(
        tmp_path,
        "2020-02-02",
        first="2020-02-02T00:00:00",
        last="2020-02-02T00:00:10",
    )

    first = global_audit_text(audit_partition_continuity(tmp_path))
    second = global_audit_text(audit_partition_continuity(tmp_path))

    assert first == second

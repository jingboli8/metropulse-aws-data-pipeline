from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from metropulse.compaction_cli import main
from metropulse.compaction_local import _write_immutable, select_local_month
from metropulse.local_paths import daily_paths


def _write_local_fixture(root: Path, selection, payloads) -> None:
    for item in selection.inputs:
        paths = daily_paths(root, item.source_date)
        paths.staging.parent.mkdir(parents=True, exist_ok=True)
        paths.manifest.parent.mkdir(parents=True, exist_ok=True)
        paths.staging.write_bytes(payloads[item.staging_key])
        manifest = {
            "first_valid_event_timestamp_local": item.first_valid_timestamp.isoformat(),
            "input_row_count": 1,
            "last_valid_event_timestamp_local": item.last_valid_timestamp.isoformat(),
            "manifest_version": item.manifest_version,
            "pipeline_version": item.pipeline_version,
            "quarantine_row_count": 0,
            "schema_version": item.schema_version,
            "source_date": item.source_date.isoformat(),
            "staging_byte_size": item.staging_byte_size,
            "staging_relative_path": paths.staging_relative,
            "staging_sha256": item.staging_sha256,
            "valid_row_count": 1,
        }
        paths.manifest.write_text(json.dumps(manifest), encoding="utf-8")


def test_cli_creates_then_resumes_deterministic_local_run(tmp_path: Path, compact_fixture) -> None:
    selection, payloads = compact_fixture()
    lake = tmp_path / "lake"
    _write_local_fixture(lake, selection, payloads)
    arguments = [
        "--lake-root",
        str(lake),
        "--target-month",
        "2020-02",
        "--known-source-end",
        "2020-09-01",
        "--processing-timestamp",
        "2026-09-16T00:00:00Z",
    ]
    assert main(arguments) == 0
    assert main(arguments) == 0
    selected = select_local_month(
        lake,
        year=2020,
        month=2,
        known_source_start=date(2020, 2, 1),
        known_source_end=date(2020, 9, 1),
    )
    assert len(selected.inputs) == 2


def test_cli_rejects_naive_processing_time(tmp_path: Path) -> None:
    assert (
        main(["--lake-root", str(tmp_path), "--processing-timestamp", "2026-09-16T00:00:00"]) == 1
    )


def test_cli_supports_separate_output_root(tmp_path: Path, compact_fixture) -> None:
    selection, payloads = compact_fixture()
    lake = tmp_path / "lake"
    output = tmp_path / "output"
    _write_local_fixture(lake, selection, payloads)
    assert (
        main(
            [
                "--lake-root",
                str(lake),
                "--output-root",
                str(output),
                "--target-month",
                "2020-02",
                "--processing-timestamp",
                "2026-09-16T00:00:00Z",
            ]
        )
        == 0
    )
    assert next(output.glob("curated/**/part-00000.snappy.parquet")).is_file()


def test_cli_never_overwrites_conflicting_completed_run(tmp_path: Path, compact_fixture) -> None:
    selection, payloads = compact_fixture()
    lake = tmp_path / "lake"
    _write_local_fixture(lake, selection, payloads)
    arguments = [
        "--lake-root",
        str(lake),
        "--target-month",
        "2020-02",
        "--processing-timestamp",
        "2026-09-16T00:00:00Z",
    ]
    assert main(arguments) == 0
    output = next(lake.glob("curated/**/part-00000.snappy.parquet"))
    output.write_bytes(b"conflict")
    assert main(arguments) == 1
    assert output.read_bytes() == b"conflict"


def test_atomic_local_write_cleans_temporary_file_on_failure(tmp_path: Path, monkeypatch) -> None:
    def fail_link(_source, _target):
        raise OSError("injected link failure")

    monkeypatch.setattr("metropulse.compaction_local.os.link", fail_link)
    with __import__("pytest").raises(OSError, match="injected"):
        _write_immutable(tmp_path / "output.parquet", b"payload")
    assert list(tmp_path.iterdir()) == []

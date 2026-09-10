from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from metropulse.acceptance import verify_local_lake
from metropulse.backfill import (
    BackfillError,
    main,
    prepare_csv_source,
    run_backfill,
    verify_checksum,
)
from metropulse.local_io import atomic_output_path, sha256_file
from metropulse.local_paths import daily_paths
from metropulse.schema import (
    ANALOG_SOURCE_FIELDS,
    DIGITAL_SOURCE_FIELDS,
    NORMALIZED_ARROW_SCHEMA,
    SOURCE_FIELDS,
    SOURCE_TO_NORMALIZED,
)
from metropulse.source_stream import SourceStreamError

PROCESSING_TIME = "2026-09-10T00:00:00"
PIPELINE_VERSION = "2.0.0-test"


def source_row(index: int, timestamp: str, **overrides: str) -> list[str]:
    values = {
        "": str(index),
        "timestamp": timestamp,
        "TP2": "-0.012",
        "TP3": "9.358",
        "H1": "9.34",
        "DV_pressure": "-0.024",
        "Reservoirs": "9.358",
        "Oil_temperature": "53.6",
        "Motor_current": "0.04",
        "COMP": "1.0",
        "DV_eletric": "0.0",
        "Towers": "1.0",
        "MPG": "1.0",
        "LPS": "0.0",
        "Pressure_switch": "1.0",
        "Oil_level": "1.0",
        "Caudal_impulses": "1.0",
    }
    values.update(overrides)
    return [values[field] for field in SOURCE_FIELDS]


def write_source(path: Path, rows: list[list[str]], header: tuple[str, ...] = SOURCE_FIELDS) -> str:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\r\n")
        writer.writerow(header)
        writer.writerows(rows)
    with path.open("r", encoding="utf-8", newline="") as stream:
        return stream.readline()


def execute(
    source_path: Path,
    output_root: Path,
    *,
    force: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
):
    return run_backfill(
        prepare_csv_source(source_path),
        output_root=output_root,
        processing_timestamp=PROCESSING_TIME,
        pipeline_version=PIPELINE_VERSION,
        force=force,
        start_date=start_date,
        end_date=end_date,
    )


def test_daily_split_header_boundary_flush_schema_codec_and_manifest(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    original_header = write_source(
        source_path,
        [
            source_row(1, "2020-02-01 23:59:50"),
            source_row(2, "2020-02-02 00:00:00", DV_eletric="1.0"),
            source_row(3, "2020-02-02 00:00:10", COMP="0.0"),
        ],
    )
    output_root = tmp_path / "lake"

    summary = execute(source_path, output_root)

    assert summary.processed_day_count == 2
    assert summary.completed_day_count == 2
    assert summary.input_row_count == summary.valid_row_count == 3
    assert summary.quarantine_row_count == 0
    assert summary.reconciliation_matches
    first = daily_paths(output_root, date(2020, 2, 1))
    second = daily_paths(output_root, date(2020, 2, 2))
    assert first.raw.read_text(encoding="utf-8").startswith(original_header.rstrip("\r\n"))
    assert second.raw.read_text(encoding="utf-8").startswith(original_header.rstrip("\r\n"))
    assert len(first.raw.read_text(encoding="utf-8").splitlines()) == 2
    assert len(second.raw.read_text(encoding="utf-8").splitlines()) == 3

    table = pq.ParquetFile(second.staging).read()
    assert table.num_rows == 2
    assert table.schema.equals(NORMALIZED_ARROW_SCHEMA, check_metadata=False)
    assert table.schema.field("event_timestamp_local").type.tz is None
    assert table.column("dv_electric").to_pylist() == [True, False]
    for source_name in ANALOG_SOURCE_FIELDS:
        assert table.schema.field(SOURCE_TO_NORMALIZED[source_name]).type == pa.float64()
    for source_name in DIGITAL_SOURCE_FIELDS:
        assert table.schema.field(SOURCE_TO_NORMALIZED[source_name]).type == pa.bool_()
    parquet_file = pq.ParquetFile(second.staging)
    assert {
        parquet_file.metadata.row_group(group).column(column).compression
        for group in range(parquet_file.metadata.num_row_groups)
        for column in range(parquet_file.metadata.num_columns)
    } == {"SNAPPY"}

    manifest = json.loads(second.manifest.read_text(encoding="utf-8"))
    assert manifest["source_date"] == "2020-02-02"
    assert manifest["raw_relative_path"] == ("raw/source=metropt3/source_date=2020-02-02/data.csv")
    assert manifest["staging_relative_path"] == (
        "staging/source=metropt3/year=2020/month=02/day=02/data.parquet"
    )
    assert manifest["quarantine_status"] == "absent_zero_rows"
    assert manifest["quarantine_relative_path"] is None
    assert not first.quarantine.exists()
    assert not second.quarantine.exists()
    manifest_text = second.manifest.read_text(encoding="utf-8")
    assert "D:" not in manifest_text
    assert "C:" not in manifest_text


def test_quarantine_jsonl_and_reconciliation(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    write_source(
        source_path,
        [
            source_row(1, "2020-02-01 00:00:00"),
            source_row(2, "2020-02-01 00:00:10", Motor_current="not-a-number"),
        ],
    )
    output_root = tmp_path / "lake"

    summary = execute(source_path, output_root)

    paths = daily_paths(output_root, date(2020, 2, 1))
    records = [
        json.loads(line) for line in paths.quarantine.read_text(encoding="utf-8").splitlines()
    ]
    assert len(records) == 1
    assert records[0]["rule_ids"] == ["ROW_NUMERIC_PARSE"]
    assert records[0]["processing_timestamp"] == "2026-09-10T00:00:00Z"
    assert records[0]["source_object_key"] == paths.raw_relative
    assert summary.input_row_count == 2
    assert summary.valid_row_count == 1
    assert summary.quarantine_row_count == 1
    assert summary.reconciliation_matches
    manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    assert manifest["quarantine_status"] == "present"
    assert manifest["quarantine_relative_path"] == paths.quarantine_relative
    assert manifest["quarantine_sha256"] == sha256_file(paths.quarantine)
    acceptance = verify_local_lake(output_root)
    assert acceptance["verified"]
    assert acceptance["raw_row_count"] == 2
    assert acceptance["parquet_row_count"] == 1
    assert acceptance["quarantine_row_count"] == 1


def test_wrong_width_row_is_preserved_and_quarantined(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    malformed = source_row(1, "2020-02-01 00:00:00")[:-1]
    write_source(source_path, [malformed])
    output_root = tmp_path / "lake"

    summary = execute(source_path, output_root)

    paths = daily_paths(output_root, date(2020, 2, 1))
    quarantine = json.loads(paths.quarantine.read_text(encoding="utf-8").strip())
    assert summary.input_row_count == summary.quarantine_row_count == 1
    assert summary.valid_row_count == 0
    assert quarantine["rule_ids"] == ["ROW_COLUMN_COUNT"]
    assert quarantine["original_field_values"] == malformed
    assert quarantine["original_record"].endswith("\r\n")


def test_repeated_or_out_of_order_partition_is_rejected(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    write_source(
        source_path,
        [
            source_row(1, "2020-02-01 00:00:00"),
            source_row(2, "2020-02-02 00:00:00"),
            source_row(3, "2020-02-01 00:00:10"),
        ],
    )

    with pytest.raises(SourceStreamError, match="appears after partition"):
        execute(source_path, tmp_path / "lake")


def test_resume_verifies_outputs_and_skips_without_rewriting(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    write_source(source_path, [source_row(1, "2020-02-01 00:00:00")])
    output_root = tmp_path / "lake"
    first = execute(source_path, output_root)
    paths = daily_paths(output_root, date(2020, 2, 1))
    hashes_before = {path: sha256_file(path) for path in (paths.raw, paths.staging, paths.manifest)}
    mtimes_before = {path: path.stat().st_mtime_ns for path in hashes_before}

    second = execute(source_path, output_root)

    assert first.processed_day_count == 1
    assert second.skipped_day_count == 1
    assert second.processed_day_count == second.rebuilt_day_count == 0
    assert {path: sha256_file(path) for path in hashes_before} == hashes_before
    assert {path: path.stat().st_mtime_ns for path in hashes_before} == mtimes_before


@pytest.mark.parametrize("corrupt_target", ["staging", "manifest"])
def test_resume_rebuilds_corrupt_outputs(tmp_path: Path, corrupt_target: str) -> None:
    source_path = tmp_path / "source.csv"
    write_source(source_path, [source_row(1, "2020-02-01 00:00:00")])
    output_root = tmp_path / "lake"
    execute(source_path, output_root)
    paths = daily_paths(output_root, date(2020, 2, 1))
    getattr(paths, corrupt_target).write_text("corrupt", encoding="utf-8")

    summary = execute(source_path, output_root)

    assert summary.rebuilt_day_count == 1
    assert summary.failed_day_count == 0
    assert json.loads(paths.manifest.read_text(encoding="utf-8"))["reconciliation"]["matches"]
    assert pq.ParquetFile(paths.staging).metadata.num_rows == 1


def test_force_rebuild_is_deterministic(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    write_source(source_path, [source_row(1, "2020-02-01 00:00:00")])
    output_root = tmp_path / "lake"
    execute(source_path, output_root)
    paths = daily_paths(output_root, date(2020, 2, 1))
    before = {path: path.read_bytes() for path in (paths.raw, paths.staging, paths.manifest)}

    forced = execute(source_path, output_root, force=True)

    assert forced.rebuilt_day_count == 1
    assert {path: path.read_bytes() for path in before} == before


def test_start_and_end_date_select_only_requested_partitions(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    write_source(
        source_path,
        [
            source_row(1, "2020-02-01 00:00:00"),
            source_row(2, "2020-02-02 00:00:00"),
            source_row(3, "2020-02-03 00:00:00"),
        ],
    )
    output_root = tmp_path / "lake"

    summary = execute(
        source_path,
        output_root,
        start_date=date(2020, 2, 2),
        end_date=date(2020, 2, 2),
    )

    assert summary.dates == ["2020-02-02"]
    assert summary.input_row_count == 1
    assert not daily_paths(output_root, date(2020, 2, 1)).raw.exists()
    assert daily_paths(output_root, date(2020, 2, 2)).raw.exists()
    assert not daily_paths(output_root, date(2020, 2, 3)).raw.exists()


def test_checksum_verification_rejects_a_mismatch(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    write_source(source_path, [source_row(1, "2020-02-01 00:00:00")])

    expected = sha256_file(source_path)
    assert verify_checksum(source_path, expected) == expected
    with pytest.raises(BackfillError, match="Checksum mismatch"):
        verify_checksum(source_path, "0" * 64)


def test_atomic_output_cleans_temporary_file_after_failure(tmp_path: Path) -> None:
    final_path = tmp_path / "nested" / "result.txt"

    with (
        pytest.raises(RuntimeError, match="injected"),
        atomic_output_path(final_path) as temporary_path,
    ):
        temporary_path.write_text("partial", encoding="utf-8")
        raise RuntimeError("injected")

    assert not final_path.exists()
    assert list(final_path.parent.glob(".result.txt.*.tmp")) == []


def test_cli_returns_failure_for_schema_drift(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_path = tmp_path / "source.csv"
    write_source(
        source_path,
        [source_row(1, "2020-02-01 00:00:00")],
        (*SOURCE_FIELDS[:-1], "Caudal_impulse"),
    )

    exit_status = main(
        [
            "--csv",
            str(source_path),
            "--output-root",
            str(tmp_path / "lake"),
            "--processing-timestamp",
            PROCESSING_TIME,
            "--pipeline-version",
            PIPELINE_VERSION,
        ]
    )

    assert exit_status == 1
    assert "header" in capsys.readouterr().err.lower()

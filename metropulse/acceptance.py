"""Independent inspection of a generated Phase 2 local lake."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from metropulse.local_io import sha256_file, write_text_atomic
from metropulse.local_paths import daily_paths
from metropulse.parquet import inspect_daily_parquet
from metropulse.schema import NORMALIZED_ARROW_SCHEMA, SOURCE_FIELDS

ABSOLUTE_WINDOWS_PATH = re.compile(r"(?:^|[\"'\s])[A-Za-z]:[\\/]")


def _csv_row_count_and_path_check(path: Path) -> tuple[int, bool, bool]:
    row_count = 0
    contains_absolute_path = False
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        try:
            header = tuple(next(reader))
        except StopIteration as error:
            raise ValueError(f"Empty raw CSV: {path}") from error
        if header != SOURCE_FIELDS:
            raise ValueError(f"Raw header mismatch: {path}")
        for fields in reader:
            row_count += 1
            contains_absolute_path |= any(
                ABSOLUTE_WINDOWS_PATH.search(value) is not None for value in fields
            )
    with path.open("r", encoding="utf-8", newline="") as stream:
        header_text_matches = stream.readline() == ",".join(SOURCE_FIELDS) + "\r\n"
    return row_count, contains_absolute_path, header_text_matches


def _jsonl_count_and_path_check(path: Path) -> tuple[int, bool]:
    count = 0
    contains_absolute_path = False
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            value = json.loads(line)
            count += 1
            serialized = json.dumps(value, ensure_ascii=False, sort_keys=True)
            contains_absolute_path |= ABSOLUTE_WINDOWS_PATH.search(serialized) is not None
    return count, contains_absolute_path


def verify_local_lake(output_root: Path) -> dict[str, Any]:
    """Independently verify counts, checksums, schemas, codecs, and portable paths."""
    raw_files = sorted(output_root.glob("raw/source=metropt3/source_date=*/data.csv"))
    staging_files = sorted(
        output_root.glob("staging/source=metropt3/year=*/month=*/day=*/data.parquet")
    )
    quarantine_files = sorted(
        output_root.glob("quarantine/source=metropt3/source_date=*/rejected.jsonl")
    )
    manifest_files = sorted(output_root.glob("control/source=metropt3/source_date=*/manifest.json"))
    failures: list[str] = []
    raw_rows = 0
    parquet_rows = 0
    quarantine_rows = 0
    manifest_input_rows = 0
    manifest_valid_rows = 0
    manifest_quarantine_rows = 0
    gap_count = 0
    warning_day_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    dates: list[str] = []
    codecs: set[str] = set()
    absolute_path_occurrences = 0
    raw_header_mismatch_count = 0

    manifests: dict[str, dict[str, Any]] = {}
    for path in manifest_files:
        try:
            text = path.read_text(encoding="utf-8")
            absolute_path_occurrences += int(ABSOLUTE_WINDOWS_PATH.search(text) is not None)
            manifest = json.loads(text)
            source_date = str(manifest["source_date"])
            manifests[source_date] = manifest
            dates.append(source_date)
            manifest_input_rows += int(manifest["input_row_count"])
            manifest_valid_rows += int(manifest["valid_row_count"])
            manifest_quarantine_rows += int(manifest["quarantine_row_count"])
            gap_count += int(manifest["batch_metrics"]["significant_gap_count"])
            for warning in manifest["batch_warnings"]:
                warning_day_counts[str(warning["rule_id"])] += 1
                warning_counts[str(warning["rule_id"])] += int(warning["observed_count"])
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            failures.append(f"Unreadable manifest {path.name}: {error}")

    for source_date, manifest in sorted(manifests.items()):
        try:
            paths = daily_paths(output_root, date.fromisoformat(source_date))
            raw_count, raw_has_path, header_matches = _csv_row_count_and_path_check(paths.raw)
            raw_rows += raw_count
            absolute_path_occurrences += int(raw_has_path)
            raw_header_mismatch_count += int(not header_matches)
            if raw_count != manifest["input_row_count"]:
                failures.append(f"{source_date}: raw row count mismatch")
            if sha256_file(paths.raw) != manifest["raw_sha256"]:
                failures.append(f"{source_date}: raw checksum mismatch")

            evidence = inspect_daily_parquet(paths.staging)
            parquet_rows += int(evidence["row_count"])
            codecs.update(str(codec) for codec in evidence["codecs"])
            if evidence["row_count"] != manifest["valid_row_count"]:
                failures.append(f"{source_date}: Parquet row count mismatch")
            if not evidence["schema_matches"] or evidence["timestamp_timezone"] is not None:
                failures.append(f"{source_date}: Parquet schema mismatch")
            if evidence["row_count"] and evidence["codecs"] != ["SNAPPY"]:
                failures.append(f"{source_date}: Parquet codec mismatch")
            if sha256_file(paths.staging) != manifest["staging_sha256"]:
                failures.append(f"{source_date}: staging checksum mismatch")
            parquet_metadata = pq.ParquetFile(paths.staging).schema_arrow.metadata or {}
            absolute_path_occurrences += sum(
                ABSOLUTE_WINDOWS_PATH.search(value.decode(errors="replace")) is not None
                for value in parquet_metadata.values()
            )

            if manifest["quarantine_row_count"]:
                count, quarantine_has_path = _jsonl_count_and_path_check(paths.quarantine)
                quarantine_rows += count
                absolute_path_occurrences += int(quarantine_has_path)
                if count != manifest["quarantine_row_count"]:
                    failures.append(f"{source_date}: quarantine row count mismatch")
                if sha256_file(paths.quarantine) != manifest["quarantine_sha256"]:
                    failures.append(f"{source_date}: quarantine checksum mismatch")
            elif paths.quarantine.exists():
                failures.append(f"{source_date}: unexpected zero-row quarantine file")
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            failures.append(f"{source_date}: verification failed: {error}")

    if len(raw_files) != len(manifest_files):
        failures.append("Raw file count differs from manifest count")
    if len(staging_files) != len(manifest_files):
        failures.append("Staging file count differs from manifest count")
    if quarantine_rows != manifest_quarantine_rows:
        failures.append("Aggregate quarantine rows differ from manifests")
    if raw_rows != manifest_input_rows:
        failures.append("Aggregate raw rows differ from manifests")
    if parquet_rows != manifest_valid_rows:
        failures.append("Aggregate Parquet rows differ from manifests")
    if raw_rows != parquet_rows + quarantine_rows:
        failures.append("Aggregate input/valid/quarantine reconciliation failed")
    if absolute_path_occurrences:
        failures.append("Generated output contains an absolute Windows path")
    if raw_header_mismatch_count:
        failures.append("Generated raw CSV does not preserve the exact source header")

    return {
        "absolute_path_occurrences": absolute_path_occurrences,
        "aggregate_reconciliation_matches": raw_rows == parquet_rows + quarantine_rows,
        "date_count": len(dates),
        "date_range": {
            "end": max(dates) if dates else None,
            "start": min(dates) if dates else None,
        },
        "failures": failures,
        "manifest_count": len(manifest_files),
        "manifest_input_row_count": manifest_input_rows,
        "manifest_quarantine_row_count": manifest_quarantine_rows,
        "manifest_valid_row_count": manifest_valid_rows,
        "parquet_codecs": sorted(codecs),
        "parquet_file_count": len(staging_files),
        "parquet_row_count": parquet_rows,
        "parquet_schema": str(NORMALIZED_ARROW_SCHEMA),
        "quarantine_byte_size": sum(path.stat().st_size for path in quarantine_files),
        "quarantine_file_count": len(quarantine_files),
        "quarantine_row_count": quarantine_rows,
        "raw_byte_size": sum(path.stat().st_size for path in raw_files),
        "raw_file_count": len(raw_files),
        "raw_header_mismatch_count": raw_header_mismatch_count,
        "raw_row_count": raw_rows,
        "significant_gap_count": gap_count,
        "staging_byte_size": sum(path.stat().st_size for path in staging_files),
        "verified": not failures,
        "warning_day_counts": dict(sorted(warning_day_counts.items())),
        "warning_observation_counts": dict(sorted(warning_counts.items())),
    }


def main() -> int:
    """Run independent acceptance checks and persist an ignored JSON report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path("data/local-lake"))
    parser.add_argument("--evidence", type=Path, required=True)
    arguments = parser.parse_args()
    report = verify_local_lake(arguments.output_root)
    write_text_atomic(arguments.evidence, json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

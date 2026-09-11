"""Reproducible local daily backfill CLI for the verified MetroPT-3 source."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zipfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from metropulse.backfill_models import BackfillSummary
from metropulse.backfill_source import (
    BackfillError,
    SourceInput,
    parse_processing_timestamp,
    prepare_archive_source,
    prepare_csv_source,
    verify_checksum,
)
from metropulse.daily_backfill import publish_day
from metropulse.local_io import temporary_output_path, write_text_atomic
from metropulse.local_paths import DailyPaths, daily_paths
from metropulse.source_stream import SourceStreamError, open_source_stream

DEFAULT_OUTPUT_ROOT = Path("data/local-lake")

__all__ = [
    "BackfillError",
    "BackfillSummary",
    "SourceInput",
    "main",
    "prepare_archive_source",
    "prepare_csv_source",
    "run_backfill",
    "verify_checksum",
]


def run_backfill(
    source: SourceInput,
    *,
    output_root: Path,
    processing_timestamp: str,
    pipeline_version: str,
    force: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
) -> BackfillSummary:
    """Stream, split, validate, and publish selected daily local partitions."""
    if not pipeline_version.strip():
        raise BackfillError("Pipeline version must not be empty.")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise BackfillError("start-date must not be after end-date.")
    try:
        processing_datetime, canonical_timestamp = parse_processing_timestamp(processing_timestamp)
    except argparse.ArgumentTypeError as error:
        raise BackfillError(str(error)) from error
    summary = BackfillSummary(source.identity, canonical_timestamp, pipeline_version)
    started = time.time()
    current_date: date | None = None
    current_row_count = 0
    selected_partition: tuple[DailyPaths, Any, tuple[Path, Any]] | None = None
    closed_dates: set[date] = set()

    def selected(value: date) -> bool:
        return (start_date is None or value >= start_date) and (
            end_date is None or value <= end_date
        )

    def open_partition(
        value: date, header_text: str
    ) -> tuple[DailyPaths, Any, tuple[Path, Any]] | None:
        if not selected(value):
            return None
        paths = daily_paths(output_root, value)
        temporary_context = temporary_output_path(paths.raw)
        temporary_path = temporary_context.__enter__()
        stream = temporary_path.open("w", encoding="utf-8", newline="")
        stream.write(header_text)
        return paths, temporary_context, (temporary_path, stream)

    def finalize_partition(value: date) -> None:
        nonlocal selected_partition
        if selected_partition is None:
            return
        paths, temporary_context, temporary_state = selected_partition
        temporary_path, stream = temporary_state
        try:
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
            outcome = publish_day(
                temporary_raw=temporary_path,
                raw_row_count=current_row_count,
                paths=paths,
                source_identity=source.identity,
                source_date=value,
                processing_datetime=processing_datetime,
                processing_timestamp=canonical_timestamp,
                pipeline_version=pipeline_version,
                force=force,
            )
            summary.add_outcome(value, outcome)
        finally:
            if not stream.closed:
                stream.close()
            temporary_context.__exit__(None, None, None)
            selected_partition = None

    try:
        with source.open_text() as source_text:
            source_stream = open_source_stream(source_text)
            saw_row = False
            for _, row_date, record in source_stream.records:
                saw_row = True
                if current_date is None:
                    current_date = row_date
                    current_row_count = 0
                    selected_partition = open_partition(row_date, source_stream.header_text)
                elif row_date != current_date:
                    finalize_partition(current_date)
                    closed_dates.add(current_date)
                    if row_date in closed_dates or row_date < current_date:
                        raise SourceStreamError(
                            f"Source date {row_date} appears after partition {current_date} was "
                            "finalized."
                        )
                    current_date = row_date
                    current_row_count = 0
                    selected_partition = open_partition(row_date, source_stream.header_text)
                current_row_count += 1
                if selected_partition is not None:
                    selected_partition[2][1].write(record.original_record)
            if not saw_row:
                raise SourceStreamError("Source CSV has a header but no data rows.")
            if current_date is not None:
                finalize_partition(current_date)
    finally:
        if selected_partition is not None:
            _, temporary_context, temporary_state = selected_partition
            temporary_state[1].close()
            temporary_context.__exit__(None, None, None)
        summary.duration_seconds = time.time() - started
    return summary


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from error


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the local-only command-line interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--archive", type=Path, help="Verified official UCI ZIP")
    source_group.add_argument("--csv", type=Path, help="Existing source CSV")
    parser.add_argument(
        "--expected-source-sha256",
        help="Optional expected SHA-256 for --csv (the official ZIP digest is fixed)",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--processing-timestamp", required=True)
    parser.add_argument("--pipeline-version", required=True)
    parser.add_argument("--start-date", type=_parse_date)
    parser.add_argument("--end-date", type=_parse_date)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--evidence", type=Path, help="Optional ignored JSON summary path")
    return parser


def _print_summary(summary: BackfillSummary) -> None:
    evidence = summary.to_dict()
    print(
        "Backfill summary: "
        f"processed={evidence['processed_day_count']} "
        f"skipped={evidence['skipped_day_count']} "
        f"rebuilt={evidence['rebuilt_day_count']} "
        f"manifest_upgraded={evidence['manifest_upgraded_day_count']} "
        f"failed={evidence['failed_day_count']}"
    )
    print(
        f"Dates: {evidence['date_count']} "
        f"({evidence['date_range']['start']} through {evidence['date_range']['end']})"
    )
    print(
        f"Rows: input={evidence['input_row_count']} valid={evidence['valid_row_count']} "
        f"quarantine={evidence['quarantine_row_count']} "
        f"reconciled={evidence['reconciliation_matches']}"
    )
    print(
        f"Bytes: raw={evidence['raw_byte_size']} staging={evidence['staging_byte_size']} "
        f"quarantine={evidence['quarantine_byte_size']}"
    )
    print(f"Warning days: {json.dumps(evidence['warning_day_counts'], sort_keys=True)}")
    print(
        "Warning observations: "
        f"{json.dumps(evidence['warning_observation_counts'], sort_keys=True)}"
    )
    print(f"Duration seconds: {evidence['duration_seconds']}")
    for error in summary.errors:
        print(f"ERROR: {error}", file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the local backfill CLI and return a process exit status."""
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.archive:
            if arguments.expected_source_sha256:
                raise BackfillError("--expected-source-sha256 applies only to --csv.")
            source = prepare_archive_source(arguments.archive)
        else:
            source = prepare_csv_source(arguments.csv, arguments.expected_source_sha256)
        summary = run_backfill(
            source,
            output_root=arguments.output_root,
            processing_timestamp=arguments.processing_timestamp,
            pipeline_version=arguments.pipeline_version,
            force=arguments.force,
            start_date=arguments.start_date,
            end_date=arguments.end_date,
        )
        if arguments.evidence:
            write_text_atomic(
                arguments.evidence,
                json.dumps(summary.to_dict(), indent=2, sort_keys=True) + "\n",
            )
        _print_summary(summary)
    except (BackfillError, SourceStreamError, OSError, zipfile.BadZipFile) as error:
        print(f"Backfill failed: {error}", file=sys.stderr)
        return 1
    return 0 if summary.reconciliation_matches else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Offline CLI for deterministic local monthly compaction."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

from metropulse.compaction_identity import (
    DEFAULT_COMPACTOR_VERSION,
    DEFAULT_OUTPUT_CONTRACT_VERSION,
)
from metropulse.compaction_local import publish_local_run, read_local_staging, select_local_month
from metropulse.monthly_compaction import compact_month


def build_parser() -> argparse.ArgumentParser:
    """Build the local compaction command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, default=Path("data/local-lake"))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--target-month", action="append", help="YYYY-MM; repeat or omit for all")
    parser.add_argument("--known-source-start", type=date.fromisoformat, default=date(2020, 2, 1))
    parser.add_argument("--known-source-end", type=date.fromisoformat, default=date(2020, 9, 1))
    parser.add_argument("--processing-timestamp", required=True)
    parser.add_argument("--compactor-version", default=DEFAULT_COMPACTOR_VERSION)
    parser.add_argument("--output-contract-version", default=DEFAULT_OUTPUT_CONTRACT_VERSION)
    parser.add_argument("--evidence", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Compact selected local months and print machine-readable acceptance evidence."""
    arguments = build_parser().parse_args(argv)
    try:
        processing_time = datetime.fromisoformat(
            arguments.processing_timestamp.replace("Z", "+00:00")
        )
        if processing_time.tzinfo is None or processing_time.utcoffset() is None:
            raise ValueError("processing timestamp must include an offset")
        if not arguments.compactor_version or not arguments.output_contract_version:
            raise ValueError("compactor and output contract versions must be nonempty")
        months = _months(arguments)
        output_root = arguments.output_root or arguments.lake_root
        started = time.perf_counter()
        results = []
        compactions = []
        for year, month in months:
            selection = select_local_month(
                arguments.lake_root,
                year=year,
                month=month,
                known_source_start=arguments.known_source_start,
                known_source_end=arguments.known_source_end,
            )
            result = compact_month(
                selection,
                lambda item: read_local_staging(arguments.lake_root, item),
                compactor_version=arguments.compactor_version,
                output_contract_version=arguments.output_contract_version,
            )
            outcome = publish_local_run(output_root, selection, result)
            compactions.append(result)
            results.append(
                {
                    "byte_size": result.parquet_byte_size,
                    "curated_rows": result.curated_row_count,
                    "missing_dates": [value.isoformat() for value in result.missing_dates],
                    "month": f"{year:04d}-{month:02d}",
                    "outcome": outcome,
                    "quarantine_rows": result.quarantine_row_count,
                    "run_id": result.run_id,
                    "sha256": result.parquet_sha256,
                    "significant_gap_count": result.significant_gap_count,
                }
            )
        month_boundaries = []
        for previous, current in zip(compactions, compactions[1:], strict=False):
            seconds = (
                (current.first_timestamp - previous.last_timestamp).total_seconds()
                if previous.last_timestamp is not None and current.first_timestamp is not None
                else None
            )
            month_boundaries.append(seconds)
        within_month_gaps = sum(item["significant_gap_count"] for item in results)
        cross_month_gaps = sum(
            1 for seconds in month_boundaries if seconds is not None and seconds > 60
        )
        evidence = {
            "contract": "metropulse-local-compaction-acceptance-v1",
            "duration_seconds": round(time.perf_counter() - started, 3),
            "months": results,
            "processing_timestamp": processing_time.isoformat(),
            "summary": {
                "curated_rows": sum(item["curated_rows"] for item in results),
                "input_count": sum(
                    len(
                        select_local_month(
                            arguments.lake_root,
                            year=int(item["month"][:4]),
                            month=int(item["month"][5:]),
                            known_source_start=arguments.known_source_start,
                            known_source_end=arguments.known_source_end,
                        ).inputs
                    )
                    for item in results
                ),
                "month_count": len(results),
                "quarantine_rows": sum(item["quarantine_rows"] for item in results),
                "within_month_significant_gap_count": within_month_gaps,
                "cross_month_significant_gap_count": cross_month_gaps,
                "global_significant_gap_count": within_month_gaps + cross_month_gaps,
                "overlapping_or_reversed_month_boundary_count": sum(
                    1 for seconds in month_boundaries if seconds is not None and seconds <= 0
                ),
            },
        }
        text = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
        print(text, end="")
        if arguments.evidence:
            arguments.evidence.parent.mkdir(parents=True, exist_ok=True)
            arguments.evidence.write_text(text, encoding="utf-8")
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(f"monthly compaction failed: {error}", file=sys.stderr)
        return 1


def _months(arguments: argparse.Namespace) -> list[tuple[int, int]]:
    if arguments.target_month:
        parsed: list[tuple[int, int]] = []
        for value in arguments.target_month:
            parts = value.split("-")
            if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
                raise ValueError(f"invalid target month: {value}")
            year, month = map(int, parts)
            if not 1 <= month <= 12:
                raise ValueError(f"invalid target month: {value}")
            parsed.append((year, month))
        if len(set(parsed)) != len(parsed):
            raise ValueError("target months must be unique")
        return sorted(parsed)
    result: list[tuple[int, int]] = []
    year, month = arguments.known_source_start.year, arguments.known_source_start.month
    while (year, month) <= (arguments.known_source_end.year, arguments.known_source_end.month):
        result.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return result


if __name__ == "__main__":
    raise SystemExit(main())

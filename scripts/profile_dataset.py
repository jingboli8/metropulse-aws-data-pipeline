"""Profile MetroPT-3 and test exactly three daily Snappy Parquet partitions."""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from metropulse_feasibility.profile import choose_representative_days, estimated_partition_bytes

ANALOGUE = [
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Oil_temperature",
    "Motor_current",
]
DIGITAL = [
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
]
NORMALIZED_COLUMNS = ["record_index", "timestamp", *ANALOGUE, *DIGITAL]
QUANTILES = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]


def json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    raise TypeError(f"Cannot serialize {type(value)}")


def scalar(
    connection: duckdb.DuckDBPyConnection, query: str, parameters: list[Any] | None = None
) -> Any:
    return connection.execute(query, parameters or []).fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path("data/raw/MetroPT3(AirCompressor).csv"))
    parser.add_argument("--archive", type=Path, default=Path("data/raw/metropt-3-dataset.zip"))
    parser.add_argument("--staged-dir", type=Path, default=Path("data/staged/feasibility"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/profile.json"))
    args = parser.parse_args()

    if not args.csv.is_file():
        raise FileNotFoundError(args.csv)
    args.staged_dir.mkdir(parents=True, exist_ok=True)
    for existing in args.staged_dir.glob("*.parquet"):
        existing.unlink()

    with args.csv.open("r", encoding="utf-8", newline="") as source:
        raw_columns = next(csv.reader(source))
        header_bytes = len((",".join(raw_columns) + "\n").encode("utf-8"))

    connection = duckdb.connect()
    connection.execute("SET preserve_insertion_order=true")
    csv_path = args.csv.resolve().as_posix().replace("'", "''")
    read_csv = (
        f"read_csv('{csv_path}', header=true, all_varchar=true, "
        "null_padding=false, strict_mode=true)"
    )
    select_parts = [
        'TRY_CAST("column00" AS BIGINT) AS record_index',
        "TRY_STRPTIME(timestamp, '%Y-%m-%d %H:%M:%S') AS timestamp",
        *[f'TRY_CAST("{column}" AS DOUBLE) AS "{column}"' for column in ANALOGUE + DIGITAL],
    ]
    connection.execute(
        f"CREATE TEMP VIEW raw AS SELECT * FROM {read_csv};"
        f"CREATE TEMP VIEW typed AS SELECT {', '.join(select_parts)} FROM raw"
    )

    row_count = scalar(connection, "SELECT count(*) FROM typed")
    inferred = connection.execute("DESCRIBE SELECT * FROM typed").fetchall()
    timestamp_summary = connection.execute(
        """
        WITH ordered AS (
          SELECT timestamp, lag(timestamp) OVER () AS previous_timestamp FROM typed
        )
        SELECT min(timestamp), max(timestamp),
               count(*) FILTER (WHERE timestamp < previous_timestamp),
               count(*) FILTER (WHERE timestamp = previous_timestamp)
        FROM ordered
        """
    ).fetchone()
    timestamp_failures = scalar(
        connection,
        "SELECT count(*) FROM raw WHERE timestamp IS NOT NULL "
        "AND TRY_STRPTIME(timestamp, '%Y-%m-%d %H:%M:%S') IS NULL",
    )

    all_columns_sql = ", ".join(f'"{column}"' for column in NORMALIZED_COLUMNS)
    duplicates_full = scalar(
        connection,
        "SELECT coalesce(sum(c - 1), 0) FROM "
        f"(SELECT count(*) c FROM typed GROUP BY {all_columns_sql} HAVING c > 1)",
    )
    duplicate_indexes = scalar(
        connection,
        "SELECT coalesce(sum(c - 1), 0) FROM "
        "(SELECT count(*) c FROM typed GROUP BY record_index HAVING c > 1)",
    )
    duplicate_timestamps = scalar(
        connection,
        "SELECT coalesce(sum(c - 1), 0) FROM "
        "(SELECT count(*) c FROM typed GROUP BY timestamp HAVING c > 1)",
    )

    interval_rows = connection.execute(
        """
        WITH deltas AS (
          SELECT date_diff('second', lag(timestamp) OVER (), timestamp) AS delta_seconds
          FROM typed
        )
        SELECT delta_seconds, count(*) count FROM deltas WHERE delta_seconds IS NOT NULL
        GROUP BY delta_seconds ORDER BY count DESC, delta_seconds
        """
    ).fetchall()
    mode_interval = interval_rows[0][0]
    significant_gap_seconds = max(60, mode_interval * 2)
    gap_summary = connection.execute(
        """
        WITH deltas AS (
          SELECT timestamp,
                 date_diff('second', lag(timestamp) OVER (), timestamp) AS delta_seconds
          FROM typed
        ), gaps AS (SELECT * FROM deltas WHERE delta_seconds > ?)
        SELECT count(*), min(delta_seconds), median(delta_seconds), max(delta_seconds),
               sum(delta_seconds) FROM gaps
        """,
        [significant_gap_seconds],
    ).fetchone()
    largest_gaps = connection.execute(
        """
        WITH deltas AS (
          SELECT timestamp,
                 date_diff('second', lag(timestamp) OVER (), timestamp) AS delta_seconds
          FROM typed
        ) SELECT timestamp, delta_seconds FROM deltas WHERE delta_seconds > ?
        ORDER BY delta_seconds DESC, timestamp LIMIT 20
        """,
        [significant_gap_seconds],
    ).fetchall()

    daily_rows = connection.execute(
        "SELECT CAST(timestamp AS DATE) AS partition_date, count(*) AS partition_rows "
        "FROM typed GROUP BY partition_date ORDER BY partition_date"
    ).fetchall()
    daily_counts = [count for _, count in daily_rows]
    daily_stats = {
        "partition_count": len(daily_rows),
        "minimum_rows": min(daily_counts),
        "median_rows": int(
            scalar(
                connection,
                "SELECT median(partition_rows) FROM "
                "(SELECT count(*) AS partition_rows FROM typed "
                "GROUP BY CAST(timestamp AS DATE))",
            )
        ),
        "maximum_rows": max(daily_counts),
        "largest_day": max(daily_rows, key=lambda item: (item[1], item[0]))[0],
    }

    nulls: dict[str, dict[str, float | int]] = {}
    for column in NORMALIZED_COLUMNS:
        count = scalar(connection, f'SELECT count(*) FILTER (WHERE "{column}" IS NULL) FROM typed')
        nulls[column] = {"count": count, "percentage": count * 100 / row_count}

    non_numeric: dict[str, int] = {}
    for column in ANALOGUE:
        non_numeric[column] = scalar(
            connection,
            f'SELECT count(*) FROM raw WHERE "{column}" IS NOT NULL '
            f'AND TRY_CAST("{column}" AS DOUBLE) IS NULL',
        )

    digital_frequencies: dict[str, list[dict[str, Any]]] = {}
    for column in DIGITAL:
        rows = connection.execute(
            f'SELECT "{column}", count(*) count FROM typed GROUP BY "{column}" ORDER BY "{column}"'
        ).fetchall()
        digital_frequencies[column] = [{"value": value, "count": count} for value, count in rows]

    analogue_statistics: dict[str, dict[str, Any]] = {}
    for column in ANALOGUE:
        values = connection.execute(
            f'SELECT min("{column}"), max("{column}"), avg("{column}"), '
            f'quantile_cont("{column}", {QUANTILES}) FROM typed'
        ).fetchone()
        analogue_statistics[column] = {
            "min": values[0],
            "max": values[1],
            "mean": values[2],
            "percentiles": dict(zip((str(q) for q in QUANTILES), values[3], strict=True)),
        }

    cardinality: dict[str, dict[str, Any]] = {}
    for column in ANALOGUE + DIGITAL:
        distinct, top_count = connection.execute(
            f'SELECT count(DISTINCT "{column}"), max(c) FROM '
            f'(SELECT "{column}", count(*) c FROM typed GROUP BY "{column}")'
        ).fetchone()
        cardinality[column] = {
            "distinct_values": distinct,
            "dominant_value_percentage": top_count * 100 / row_count,
            "constant": distinct <= 1,
            "near_constant_at_99_percent": top_count / row_count >= 0.99,
        }

    impossible_checks: dict[str, Any] = {
        "documented_hard_validity_rules": [],
        "interpretation": (
            "UCI gives operational examples and thresholds, not hard physical validity bounds. "
            "Negative readings are therefore flagged as empirical review items, "
            "not declared impossible."
        ),
        "non_finite_counts": {},
        "negative_counts": {},
    }
    for column in ANALOGUE:
        impossible_checks["non_finite_counts"][column] = scalar(
            connection,
            f'SELECT count(*) FROM typed WHERE NOT isfinite("{column}")',
        )
        impossible_checks["negative_counts"][column] = scalar(
            connection,
            f'SELECT count(*) FROM typed WHERE "{column}" < 0',
        )

    total_data_bytes = args.csv.stat().st_size - header_bytes
    daily_stats["estimated_typical_csv_bytes"] = estimated_partition_bytes(
        total_data_bytes, row_count, daily_stats["median_rows"]
    )
    daily_stats["estimated_maximum_csv_bytes"] = estimated_partition_bytes(
        total_data_bytes, row_count, daily_stats["maximum_rows"]
    )

    selected_days = choose_representative_days(daily_rows)
    parquet_results: dict[str, dict[str, Any]] = {}
    projected_columns = ", ".join(f'"{column}"' for column in NORMALIZED_COLUMNS)
    for label, day in selected_days.items():
        output = args.staged_dir / f"{label}-{day.isoformat()}.parquet"
        output_sql = output.resolve().as_posix().replace("'", "''")
        connection.execute(
            f"COPY (SELECT {projected_columns} FROM typed WHERE CAST(timestamp AS DATE) = ?) "
            f"TO '{output_sql}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
            [day],
        )
        csv_rows = next(count for candidate, count in daily_rows if candidate == day)
        parquet_rows = pq.ParquetFile(output).metadata.num_rows
        if parquet_rows != csv_rows:
            raise RuntimeError(f"Parquet row mismatch for {day}: {parquet_rows} != {csv_rows}")
        estimated_csv_bytes = estimated_partition_bytes(total_data_bytes, row_count, csv_rows)
        parquet_results[label] = {
            "date": day,
            "csv_rows": csv_rows,
            "parquet_rows": parquet_rows,
            "estimated_csv_bytes": estimated_csv_bytes,
            "parquet_bytes": output.stat().st_size,
            "estimated_csv_to_parquet_ratio": estimated_csv_bytes / output.stat().st_size,
            "compression": "SNAPPY",
            "path": str(output),
        }

    recommended_types = {
        "record_index": "BIGINT (rename the blank source header)",
        "timestamp": "TIMESTAMP (timezone-naive; source provides no timezone)",
        **{column: "DOUBLE" for column in ANALOGUE},
        **{
            column: (
                "BOOLEAN"
                if {item["value"] for item in digital_frequencies[column]} <= {0.0, 1.0}
                else "DOUBLE"
            )
            for column in DIGITAL
        },
    }
    profile = {
        "generated_at": datetime.now().astimezone(),
        "files": {
            "archive_bytes": args.archive.stat().st_size,
            "csv_bytes": args.csv.stat().st_size,
        },
        "schema": {
            "raw_header_columns": raw_columns,
            "normalized_columns": NORMALIZED_COLUMNS,
            "column_count": len(raw_columns),
            "inferred_types": {row[0]: row[1] for row in inferred},
            "recommended_types": recommended_types,
        },
        "row_count": row_count,
        "duplicate_full_rows": duplicates_full,
        "duplicate_index_values": duplicate_indexes,
        "timestamps": {
            "parsing_failures": timestamp_failures,
            "minimum": timestamp_summary[0],
            "maximum": timestamp_summary[1],
            "out_of_order_transitions": timestamp_summary[2],
            "sorted_non_decreasing": timestamp_summary[2] == 0,
            "adjacent_duplicate_transitions": timestamp_summary[3],
            "duplicate_timestamp_values": duplicate_timestamps,
            "interval_distribution": [
                {"seconds": seconds, "count": count} for seconds, count in interval_rows
            ],
            "significant_gap_definition": f"delta > {significant_gap_seconds} seconds",
            "significant_gaps": {
                "count": gap_summary[0],
                "minimum_seconds": gap_summary[1],
                "median_seconds": gap_summary[2],
                "maximum_seconds": gap_summary[3],
                "total_seconds": gap_summary[4],
                "largest_20": [
                    {"timestamp_after_gap": timestamp, "seconds": seconds}
                    for timestamp, seconds in largest_gaps
                ],
            },
        },
        "daily": daily_stats,
        "nulls": nulls,
        "non_numeric_analogue_values": non_numeric,
        "digital_frequencies": digital_frequencies,
        "analogue_statistics": analogue_statistics,
        "constant_and_near_constant": cardinality,
        "validity_review": impossible_checks,
        "parquet_feasibility": parquet_results,
    }
    if not math.isfinite(sum(item["mean"] for item in analogue_statistics.values())):
        raise RuntimeError("Non-finite analogue summary detected")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(profile, indent=2, default=json_default) + "\n", encoding="utf-8"
    )
    print(f"Profile written: {args.output}")
    print(
        json.dumps(
            {"row_count": row_count, "daily": daily_stats, "parquet": parquet_results},
            indent=2,
            default=json_default,
        )
    )


if __name__ == "__main__":
    main()

"""Offline real-day acceptance runner using only in-memory fake S3."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from metropulse.aws.config import LambdaConfig
from metropulse.aws.events import parse_s3_record
from metropulse.aws.observability import StructuredObserver
from metropulse.aws.processor import ObjectProcessor
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.local_io import write_text_atomic
from metropulse.schema import NORMALIZED_ARROW_SCHEMA


def run_acceptance(
    raw_path: Path, source_date: date, *, pipeline_version: str
) -> dict[str, object]:
    """Run one local raw day through the complete adapter with fake storage."""
    body = raw_path.read_bytes()
    config = LambdaConfig(
        expected_source_name="metropt3",
        raw_prefix="raw",
        staging_prefix="staging",
        quarantine_prefix="quarantine",
        control_prefix="control",
        pipeline_version=pipeline_version,
        maximum_input_bytes=max(len(body) + 1_000_000, 20_000_000),
        environment_name="acceptance",
        destination_bucket="acceptance-output",
    )
    source_bucket = "acceptance-source"
    source_key = f"raw/source=metropt3/source_date={source_date.isoformat()}/data.csv"
    storage = InMemoryObjectStorage()
    source_sha256 = hashlib.sha256(body).hexdigest()
    event_timestamp = datetime(2026, 9, 11, tzinfo=UTC)
    storage.seed(
        source_bucket,
        source_key,
        body,
        version_id="acceptance-version-1",
        etag="acceptance-etag",
        checksum_sha256=source_sha256,
        last_modified=event_timestamp,
    )
    event = {
        "eventName": "ObjectCreated:Put",
        "eventSource": "aws:s3",
        "eventTime": "2026-09-11T00:00:00Z",
        "s3": {
            "bucket": {"name": source_bucket},
            "object": {
                "eTag": "acceptance-etag",
                "key": source_key,
                "sequencer": "acceptance-sequencer",
                "size": len(body),
                "versionId": "acceptance-version-1",
            },
        },
    }
    record = parse_s3_record(event, 0, config)
    logs: list[dict[str, object]] = []
    processor = ObjectProcessor(
        config=config,
        storage=storage,
        observer=StructuredObserver("acceptance", pipeline_version, logs.append),
        utc_now=lambda: event_timestamp,
        monotonic_ms=lambda: 0.0,
    )
    first = processor.process(record, request_id="acceptance-request-1")
    published_before_retry = {
        f"{bucket}/{key}": hashlib.sha256(entry.body).hexdigest()
        for (bucket, key), entry in sorted(storage.objects.items())
    }
    second = processor.process(record, request_id="acceptance-request-2")
    published_after_retry = {
        f"{bucket}/{key}": hashlib.sha256(entry.body).hexdigest()
        for (bucket, key), entry in sorted(storage.objects.items())
    }
    parquet_body = storage.get(config.destination_bucket, first.staging_key).body
    parquet_file = pq.ParquetFile(pa.BufferReader(parquet_body))
    codecs = sorted(
        {
            parquet_file.metadata.row_group(group).column(column).compression
            for group in range(parquet_file.metadata.num_row_groups)
            for column in range(parquet_file.metadata.num_columns)
        }
    )
    completion = storage.get(config.destination_bucket, first.completion_key).body
    report = {
        "completion_marker_present": bool(completion),
        "explicit_schema_matches": parquet_file.schema_arrow.equals(
            NORMALIZED_ARROW_SCHEMA, check_metadata=False
        ),
        "fake_storage_deterministic_after_retry": published_before_retry == published_after_retry,
        "first_outcome": first.outcome,
        "input_row_count": first.input_row_count,
        "network_or_aws_calls": 0,
        "parquet_codecs": codecs,
        "parquet_row_count": parquet_file.metadata.num_rows,
        "quarantine_object_present": first.quarantine_key is not None,
        "quarantine_row_count": first.quarantine_row_count,
        "second_outcome": second.outcome,
        "source_date": source_date.isoformat(),
        "source_sha256": source_sha256,
        "timestamp_timezone": parquet_file.schema_arrow.field("event_timestamp_local").type.tz,
        "valid_row_count": first.valid_row_count,
        "verified": (
            first.outcome == "processed"
            and second.outcome == "duplicate_skipped"
            and first.input_row_count == first.valid_row_count + first.quarantine_row_count
            and first.valid_row_count == parquet_file.metadata.num_rows
            and codecs == ["SNAPPY"]
            and parquet_file.schema_arrow.equals(NORMALIZED_ARROW_SCHEMA, check_metadata=False)
            and published_before_retry == published_after_retry
        ),
    }
    return report


def main(argv: list[str] | None = None) -> int:
    """Run the fake-S3 acceptance and optionally write ignored JSON evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--source-date", required=True, type=date.fromisoformat)
    parser.add_argument("--pipeline-version", default="3.0.0")
    parser.add_argument("--evidence", type=Path)
    arguments = parser.parse_args(argv)
    report = run_acceptance(
        arguments.raw,
        arguments.source_date,
        pipeline_version=arguments.pipeline_version,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    print(text, end="")
    if arguments.evidence:
        write_text_atomic(arguments.evidence, text)
    return 0 if report["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

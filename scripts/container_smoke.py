"""Deterministic in-container processor and thin-handler smoke test."""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from metropulse.aws.config import LambdaConfig
from metropulse.aws.lambda_handler import handle_event
from metropulse.aws.observability import StructuredObserver
from metropulse.aws.processor import ObjectProcessor
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.schema import NORMALIZED_ARROW_SCHEMA, SOURCE_FIELDS


def _csv_body() -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\r\n")
    writer.writerow(SOURCE_FIELDS)
    writer.writerow(
        [
            "1",
            "2020-02-01 00:00:00",
            "-0.012",
            "9.358",
            "9.34",
            "-0.024",
            "9.358",
            "53.6",
            "0.04",
            "1.0",
            "0.0",
            "1.0",
            "1.0",
            "0.0",
            "1.0",
            "1.0",
            "1.0",
        ]
    )
    return stream.getvalue().encode("utf-8")


def main() -> None:
    fixed_time = datetime(2026, 9, 15, tzinfo=UTC)
    body = _csv_body()
    source_bucket = "offline-source"
    destination_bucket = "offline-output"
    key = "raw/source=metropt3/source_date=2020-02-01/data.csv"
    checksum = hashlib.sha256(body).hexdigest()
    storage = InMemoryObjectStorage()
    storage.seed(
        source_bucket,
        key,
        body,
        version_id="offline-version-1",
        etag="offline-etag-1",
        checksum_sha256=checksum,
        last_modified=fixed_time,
    )
    config = LambdaConfig(
        expected_source_name="metropt3",
        raw_prefix="raw",
        staging_prefix="staging",
        quarantine_prefix="quarantine",
        control_prefix="control",
        pipeline_version="4.0.0-smoke",
        maximum_input_bytes=1_000_000,
        environment_name="offline",
        destination_bucket=destination_bucket,
    )
    logs: list[dict[str, object]] = []
    observer = StructuredObserver("offline", config.pipeline_version, logs.append)
    processor = ObjectProcessor(
        config=config,
        storage=storage,
        observer=observer,
        utc_now=lambda: fixed_time,
        monotonic_ms=lambda: 1000.0,
    )
    event = {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "eventSource": "aws:s3",
                "eventTime": "2026-09-15T00:00:00Z",
                "s3": {
                    "bucket": {"name": source_bucket},
                    "object": {
                        "eTag": "offline-etag-1",
                        "key": key,
                        "sequencer": "0000000000000001",
                        "size": len(body),
                        "versionId": "offline-version-1",
                    },
                },
            }
        ]
    }
    context = SimpleNamespace(aws_request_id="offline-request-1")
    first = handle_event(event, context, config=config, processor=processor, observer=observer)
    context.aws_request_id = "offline-request-2"
    second = handle_event(event, context, config=config, processor=processor, observer=observer)
    first_result = first["results"][0]
    second_result = second["results"][0]
    if first_result["outcome"] != "processed":
        raise AssertionError("first processing outcome was not processed")
    if second_result["outcome"] != "duplicate_skipped":
        raise AssertionError("second processing outcome was not duplicate_skipped")
    parquet_body = storage.get(destination_bucket, str(first_result["staging_key"])).body
    parquet = pq.ParquetFile(pa.BufferReader(parquet_body))
    if parquet.metadata.num_rows != 1:
        raise AssertionError("Parquet row count did not reconcile")
    if not parquet.schema_arrow.equals(NORMALIZED_ARROW_SCHEMA, check_metadata=False):
        raise AssertionError("Parquet schema differs from the explicit contract")
    codecs = {
        parquet.metadata.row_group(group).column(column).compression
        for group in range(parquet.metadata.num_row_groups)
        for column in range(parquet.metadata.num_columns)
    }
    if codecs != {"SNAPPY"}:
        raise AssertionError(f"unexpected Parquet codecs: {sorted(codecs)}")
    if first_result["input_row_count"] != 1 or first_result["quarantine_row_count"] != 0:
        raise AssertionError("smoke-test counts did not reconcile")
    print(
        f"SMOKE_OK rows=1 quarantine=0 codec=SNAPPY identity={first_result['processing_identity']}"
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from metropulse.aws.errors import (
    CompletionMarkerInvalid,
    ConcurrentProcessing,
    ObjectIdentityMismatch,
    ProcessingError,
)
from metropulse.aws.events import parse_s3_record
from metropulse.aws.idempotency import processing_identity
from metropulse.aws.keys import output_keys
from metropulse.aws.processor import ObjectProcessor
from metropulse.manifest import MANIFEST_VERSION
from metropulse.schema import NORMALIZED_ARROW_SCHEMA, SCHEMA_VERSION


def setup_processor(
    *,
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    body: bytes,
    event_overrides: dict[str, object] | None = None,
):
    raw = event_record_factory(size=len(body), **(event_overrides or {}))
    source = fake_storage.seed(
        "source-bucket",
        "raw/source=metropt3/source_date=2020-02-01/data.csv",
        body,
        version_id="version-1",
        etag="etag-1",
        checksum_sha256=hashlib.sha256(body).hexdigest(),
        last_modified=datetime(2026, 9, 11, 7, 30, tzinfo=UTC),
    )
    record = parse_s3_record(raw, 0, aws_config)
    observer, logs = captured_observer
    processor = ObjectProcessor(
        config=aws_config,
        storage=fake_storage,
        observer=observer,
        utc_now=fixed_clock,
        monotonic_ms=lambda: 100.0,
    )
    return processor, record, source, logs


def test_valid_csv_produces_explicit_snappy_parquet_manifest_and_no_quarantine(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    processor, record, _, logs = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )

    result = processor.process(record, request_id="request-1")
    marker = json.loads(fake_storage.get(aws_config.destination_bucket, result.completion_key).body)
    parquet = fake_storage.get(aws_config.destination_bucket, result.staging_key).body
    parquet_file = pq.ParquetFile(pa.BufferReader(parquet))

    assert result.outcome == "processed"
    assert (result.input_row_count, result.valid_row_count, result.quarantine_row_count) == (
        2,
        2,
        0,
    )
    assert result.quarantine_key is None
    assert parquet_file.metadata.num_rows == 2
    assert parquet_file.schema_arrow.equals(NORMALIZED_ARROW_SCHEMA, check_metadata=False)
    assert parquet_file.schema_arrow.field("event_timestamp_local").type.tz is None
    assert {
        parquet_file.metadata.row_group(group).column(column).compression
        for group in range(parquet_file.metadata.num_row_groups)
        for column in range(parquet_file.metadata.num_columns)
    } == {"SNAPPY"}
    assert marker["reconciliation"]["matches"]
    assert marker["processing_timestamp"] == "2026-09-11T07:30:00Z"
    marker_text = json.dumps(marker, sort_keys=True)
    assert re.search(r"[A-Za-z]:\\", marker_text) is None
    assert (
        aws_config.destination_bucket,
        output_keys(record.source_date, result.processing_identity, aws_config).quarantine,
    ) not in fake_storage.objects
    assert any(log.get("event_name") == "processing_completed" for log in logs)


@pytest.mark.parametrize("wrong_date", [False, True])
def test_invalid_numeric_or_source_date_row_publishes_quarantine(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
    wrong_date: bool,
) -> None:
    body = csv_factory(invalid=not wrong_date, wrong_date=wrong_date)
    processor, record, _, _ = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )

    result = processor.process(record, request_id="request-1")
    quarantine = fake_storage.get(aws_config.destination_bucket, result.quarantine_key).body
    payload = json.loads(quarantine.decode("utf-8").splitlines()[0])

    assert result.input_row_count == 2
    assert result.valid_row_count == (0 if wrong_date else 1)
    assert result.quarantine_row_count == (2 if wrong_date else 1)
    expected_rule = "ROW_SOURCE_DATE_MISMATCH" if wrong_date else "ROW_NUMERIC_PARSE"
    assert expected_rule in payload["rule_ids"]
    assert body not in b"".join(
        json.dumps(log, sort_keys=True).encode("utf-8") for log in captured_observer[1]
    )


def test_oversize_is_rejected_before_body_read(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
) -> None:
    config = type(aws_config)(**{**aws_config.__dict__, "maximum_input_bytes": 1})
    body = csv_factory()
    processor, record, _, _ = setup_processor(
        aws_config=config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )

    with pytest.raises(ProcessingError, match="limit"):
        processor.process(record, request_id="request-1")
    assert fake_storage.get_counts.get((record.bucket, record.key), 0) == 0


def test_strict_decoding_and_event_object_identity_mismatch(
    aws_config, event_record_factory, fake_storage, captured_observer, fixed_clock
) -> None:
    processor, record, _, logs = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=b"\xff\xfe",
    )
    with pytest.raises(ProcessingError, match="UTF-8"):
        processor.process(record, request_id="request-1")

    bad_raw = event_record_factory(size=2, etag="stale")
    bad_record = parse_s3_record(bad_raw, 1, aws_config)
    with pytest.raises(ObjectIdentityMismatch, match="ETag"):
        processor.process(bad_record, request_id="request-2")
    mismatch = [log for log in logs if log.get("event_name") == "object_identity_mismatch"]
    assert mismatch[0]["aws_request_id"] == "request-2"
    assert mismatch[0]["source_key"] == record.key


def test_output_identity_checksums_sizes_and_retry_are_deterministic(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory(invalid=True)
    processor, record, head, _ = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )
    expected_id, _ = processing_identity(
        source_bucket=record.bucket,
        source_key=record.key,
        version_id=head.version_id,
        checksum_sha256=head.checksum_sha256,
        etag=head.etag,
        pipeline_version=aws_config.pipeline_version,
        schema_version=SCHEMA_VERSION,
        manifest_version=MANIFEST_VERSION,
    )

    first = processor.process(record, request_id="request-1")
    data_before = {
        key: entry.body
        for key, entry in fake_storage.objects.items()
        if key[1].endswith(("data.parquet", "rejected.jsonl", "completed.json"))
    }
    second = processor.process(record, request_id="request-2")
    marker = json.loads(fake_storage.get(aws_config.destination_bucket, first.completion_key).body)

    assert first.processing_identity == expected_id
    assert second.outcome == "duplicate_skipped"
    assert data_before == {
        key: entry.body for key, entry in fake_storage.objects.items() if key in data_before
    }
    for reference in (marker["staging"], marker["quarantine"]):
        stored = fake_storage.get(reference["bucket"], reference["key"])
        assert reference["byte_size"] == len(stored.body)
        assert reference["sha256"] == hashlib.sha256(stored.body).hexdigest()
        assert stored.head.metadata["sha256"] == reference["sha256"]


def test_identity_is_canonical_and_versioned_reprocessing_uses_new_keys(aws_config) -> None:
    arguments = {
        "source_bucket": "source-bucket",
        "source_key": "raw/source=metropt3/source_date=2020-02-01/data.csv",
        "checksum_sha256": "checksum",
        "etag": "opaque-etag",
        "pipeline_version": aws_config.pipeline_version,
        "schema_version": SCHEMA_VERSION,
        "manifest_version": MANIFEST_VERSION,
    }
    first, first_fields = processing_identity(version_id="version-1", **arguments)
    repeated, repeated_fields = processing_identity(version_id="version-1", **arguments)
    reprocessed, _ = processing_identity(version_id="version-2", **arguments)
    etag_only, etag_fields = processing_identity(
        **{**arguments, "version_id": None, "checksum_sha256": None}
    )

    assert first == repeated
    assert first_fields == repeated_fields
    assert first != reprocessed
    assert etag_only != first
    assert etag_fields["object_identity_kind"] == "etag"
    assert output_keys(date(2020, 2, 1), first, aws_config) != output_keys(
        date(2020, 2, 1), reprocessed, aws_config
    )


@pytest.mark.parametrize("corruption", ["missing", "checksum"])
def test_existing_marker_requires_valid_referenced_outputs(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
    corruption: str,
) -> None:
    processor, record, _, _ = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=csv_factory(),
    )
    first = processor.process(record, request_id="request-1")
    identity = (aws_config.destination_bucket, first.staging_key)
    if corruption == "missing":
        fake_storage.objects.pop(identity)
    else:
        fake_storage.corrupt_body(*identity, b"corrupt")

    with pytest.raises(CompletionMarkerInvalid):
        processor.process(record, request_id="request-2")


def test_partial_output_without_marker_is_overwritten(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    processor, record, head, _ = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )
    input_id, _ = processing_identity(
        source_bucket=record.bucket,
        source_key=record.key,
        version_id=head.version_id,
        checksum_sha256=head.checksum_sha256,
        etag=head.etag,
        pipeline_version=aws_config.pipeline_version,
        schema_version=SCHEMA_VERSION,
        manifest_version=MANIFEST_VERSION,
    )
    keys = output_keys(record.source_date, input_id, aws_config)
    fake_storage.seed(aws_config.destination_bucket, keys.staging, b"partial")

    result = processor.process(record, request_id="request-1")

    assert result.outcome == "processed"
    assert fake_storage.get(aws_config.destination_bucket, keys.staging).body != b"partial"


def test_conditional_completion_race_lost_valid_and_inconsistent(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    processor, record, head, logs = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )
    input_id, _ = processing_identity(
        source_bucket=record.bucket,
        source_key=record.key,
        version_id=head.version_id,
        checksum_sha256=head.checksum_sha256,
        etag=head.etag,
        pipeline_version=aws_config.pipeline_version,
        schema_version=SCHEMA_VERSION,
        manifest_version=MANIFEST_VERSION,
    )
    keys = output_keys(record.source_date, input_id, aws_config)
    race_key = (aws_config.destination_bucket, keys.completed)
    fake_storage.conditional_races[race_key] = lambda value: value

    result = processor.process(record, request_id="request-1")

    assert result.outcome == "concurrent_success"
    names = [log.get("event_name") for log in logs]
    assert "completion_race_lost" in names
    assert "completion_race_validated" in names

    other_storage = type(fake_storage)()
    other_observer = (type(captured_observer[0])("test", "3.0.0-test", lambda payload: None), [])
    other_processor, other_record, other_head, _ = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=other_storage,
        captured_observer=other_observer,
        fixed_clock=fixed_clock,
        body=body,
    )
    other_id, _ = processing_identity(
        source_bucket=other_record.bucket,
        source_key=other_record.key,
        version_id=other_head.version_id,
        checksum_sha256=other_head.checksum_sha256,
        etag=other_head.etag,
        pipeline_version=aws_config.pipeline_version,
        schema_version=SCHEMA_VERSION,
        manifest_version=MANIFEST_VERSION,
    )
    other_keys = output_keys(other_record.source_date, other_id, aws_config)
    other_storage.conditional_races[(aws_config.destination_bucket, other_keys.completed)] = (
        lambda value: b"{}\n"
    )
    with pytest.raises(CompletionMarkerInvalid, match="Completion marker"):
        other_processor.process(other_record, request_id="request-2")


def test_active_claim_defers_and_stale_claim_is_conditionally_recovered(
    aws_config,
    event_record_factory,
    fake_storage,
    captured_observer,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    processor, record, head, _ = setup_processor(
        aws_config=aws_config,
        event_record_factory=event_record_factory,
        fake_storage=fake_storage,
        captured_observer=captured_observer,
        fixed_clock=fixed_clock,
        body=body,
    )
    input_id, _ = processing_identity(
        source_bucket=record.bucket,
        source_key=record.key,
        version_id=head.version_id,
        checksum_sha256=head.checksum_sha256,
        etag=head.etag,
        pipeline_version=aws_config.pipeline_version,
        schema_version=SCHEMA_VERSION,
        manifest_version=MANIFEST_VERSION,
    )
    keys = output_keys(record.source_date, input_id, aws_config)
    claim_key = (aws_config.destination_bucket, keys.claim)
    fake_storage.seed(
        *claim_key,
        json.dumps(
            {
                "expires_at": "2026-09-11T08:15:00Z",
                "processing_identity": input_id,
            }
        ).encode(),
        etag="claim-etag",
    )
    with pytest.raises(ConcurrentProcessing, match="active"):
        processor.process(record, request_id="request-2")

    fake_storage.seed(
        *claim_key,
        json.dumps(
            {
                "expires_at": "2026-09-11T07:59:59Z",
                "processing_identity": input_id,
            }
        ).encode(),
        etag="stale-claim-etag",
    )
    result = processor.process(record, request_id="request-3")
    assert result.outcome == "processed"

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from metropulse.aws.config import ConfigurationError, LambdaConfig
from metropulse.aws.errors import ProcessingError
from metropulse.aws.events import InvalidEvent, event_records, parse_s3_record
from metropulse.aws.lambda_handler import InvocationFailure, handle_event
from metropulse.aws.processor import ObjectProcessor, ProcessingResult


@dataclass
class Context:
    aws_request_id: str = "aws-request-1"


class RecordingProcessor:
    def __init__(self) -> None:
        self.records = []

    def process(self, record, *, request_id):
        self.records.append((record, request_id))
        return ProcessingResult(
            record_index=record.record_index,
            outcome="processed",
            processing_identity="a" * 64,
            input_row_count=1,
            valid_row_count=1,
            quarantine_row_count=0,
            staging_key="staging/data.parquet",
            quarantine_key=None,
            completion_key="control/completed.json",
        )


def test_multiple_records_and_thin_handler_delegation(
    aws_config, captured_observer, event_record_factory
) -> None:
    processor = RecordingProcessor()
    event = {"Records": [event_record_factory(), event_record_factory()]}

    response = handle_event(
        event,
        Context(),
        config=aws_config,
        processor=processor,
        observer=captured_observer[0],
    )

    assert response["record_count"] == 2
    assert len(processor.records) == 2
    assert all(request_id == "aws-request-1" for _, request_id in processor.records)


def test_partial_failure_processes_other_records_and_retry_skips_completed(
    aws_config,
    captured_observer,
    event_record_factory,
    fake_storage,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    fake_storage.seed(
        "source-bucket",
        "raw/source=metropt3/source_date=2020-02-01/data.csv",
        body,
        version_id="version-1",
        etag="etag-1",
        checksum_sha256=hashlib.sha256(body).hexdigest(),
        last_modified=datetime(2026, 9, 11, 7, 30, tzinfo=UTC),
    )
    processor = ObjectProcessor(
        config=aws_config,
        storage=fake_storage,
        observer=captured_observer[0],
        utc_now=fixed_clock,
        monotonic_ms=lambda: 100.0,
    )
    event = {
        "Records": [
            event_record_factory(size=len(body)),
            event_record_factory(event_name="ObjectRemoved:Delete"),
        ]
    }

    with pytest.raises(InvocationFailure) as first:
        handle_event(
            event,
            Context(),
            config=aws_config,
            processor=processor,
            observer=captured_observer[0],
        )
    with pytest.raises(InvocationFailure) as second:
        handle_event(
            event,
            Context(),
            config=aws_config,
            processor=processor,
            observer=captured_observer[0],
        )

    assert first.value.results[0]["outcome"] == "processed"
    assert second.value.results[0]["outcome"] == "duplicate_skipped"
    assert first.value.errors[0]["record_index"] == 1
    assert any(log.get("event_name") == "invalid_event" for log in captured_observer[1])


def test_duplicate_records_in_one_event_process_once_then_skip(
    aws_config,
    captured_observer,
    event_record_factory,
    fake_storage,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    fake_storage.seed(
        "source-bucket",
        "raw/source=metropt3/source_date=2020-02-01/data.csv",
        body,
        version_id="version-1",
        etag="etag-1",
        checksum_sha256=hashlib.sha256(body).hexdigest(),
        last_modified=datetime(2026, 9, 11, 7, 30, tzinfo=UTC),
    )
    processor = ObjectProcessor(
        config=aws_config,
        storage=fake_storage,
        observer=captured_observer[0],
        utc_now=fixed_clock,
        monotonic_ms=lambda: 100.0,
    )
    duplicate = event_record_factory(size=len(body))

    response = handle_event(
        {"Records": [duplicate, duplicate]},
        Context(),
        config=aws_config,
        processor=processor,
        observer=captured_observer[0],
    )

    assert [result["outcome"] for result in response["results"]] == [
        "processed",
        "duplicate_skipped",
    ]


def test_observability_uses_required_events_low_cardinality_dimensions_and_winner_metrics(
    aws_config,
    captured_observer,
    event_record_factory,
    fake_storage,
    fixed_clock,
    csv_factory,
) -> None:
    body = csv_factory()
    fake_storage.seed(
        "source-bucket",
        "raw/source=metropt3/source_date=2020-02-01/data.csv",
        body,
        version_id="version-1",
        etag="etag-1",
        checksum_sha256=hashlib.sha256(body).hexdigest(),
        last_modified=datetime(2026, 9, 11, 7, 30, tzinfo=UTC),
    )
    record = parse_s3_record(event_record_factory(size=len(body)), 0, aws_config)
    processor = ObjectProcessor(
        config=aws_config,
        storage=fake_storage,
        observer=captured_observer[0],
        utc_now=fixed_clock,
        monotonic_ms=lambda: 42.0,
    )

    processor.process(record, request_id="request-1")
    processor.process(record, request_id="request-2")

    logs = captured_observer[1]
    event_names = {log.get("event_name") for log in logs}
    assert {
        "record_received",
        "processing_started",
        "processing_completed",
        "duplicate_skipped",
    } <= event_names
    metric_logs = [log for log in logs if "_aws" in log]
    success = [log for log in metric_logs if log.get("ObjectsProcessed") == 1]
    duplicates = [log for log in metric_logs if log.get("ObjectsSkippedDuplicate") == 1]
    assert len(success) == len(duplicates) == 1
    assert success[0]["InputRows"] == success[0]["ValidRows"] == 2
    assert success[0]["QuarantineRows"] == 0
    assert success[0]["ProcessingDurationMs"] == 0
    completed = next(log for log in logs if log.get("event_name") == "processing_completed")
    assert completed["aws_request_id"] == "request-1"
    assert completed["source_bucket"] == "source-bucket"
    assert completed["source_key"].endswith("/data.csv")
    assert len(completed["processing_identity"]) == 64
    assert completed["outcome"] == "processed"
    assert completed["input_row_count"] == completed["valid_row_count"] == 2
    dimensions = success[0]["_aws"]["CloudWatchMetrics"][0]["Dimensions"]
    assert dimensions == [["Environment", "PipelineVersion"]]
    assert all(
        forbidden not in dimensions[0]
        for forbidden in ("source_key", "processing_identity", "aws_request_id", "source_date")
    )


def test_failure_emits_operational_failure_metrics_without_raw_body(
    aws_config, captured_observer, event_record_factory, fake_storage, fixed_clock
) -> None:
    body = b"secret-raw-record-\xff"
    fake_storage.seed(
        "source-bucket",
        "raw/source=metropt3/source_date=2020-02-01/data.csv",
        body,
        version_id="version-1",
        etag="etag-1",
        last_modified=datetime(2026, 9, 11, 7, 30, tzinfo=UTC),
    )
    record = parse_s3_record(event_record_factory(size=len(body)), 0, aws_config)
    processor = ObjectProcessor(
        config=aws_config,
        storage=fake_storage,
        observer=captured_observer[0],
        utc_now=fixed_clock,
        monotonic_ms=lambda: 1.0,
    )

    with pytest.raises(ProcessingError, match="UTF-8"):
        processor.process(record, request_id="request-1")

    serialized = str(captured_observer[1])
    assert "secret-raw-record" not in serialized
    assert any(log.get("event_name") == "processing_failed" for log in captured_observer[1])
    assert any(log.get("ObjectsFailed") == 1 for log in captured_observer[1])


def test_environment_configuration_validation(aws_config) -> None:
    environment = {
        "METROPULSE_EXPECTED_SOURCE_NAME": aws_config.expected_source_name,
        "METROPULSE_RAW_PREFIX": aws_config.raw_prefix,
        "METROPULSE_STAGING_PREFIX": aws_config.staging_prefix,
        "METROPULSE_QUARANTINE_PREFIX": aws_config.quarantine_prefix,
        "METROPULSE_CONTROL_PREFIX": aws_config.control_prefix,
        "METROPULSE_PIPELINE_VERSION": aws_config.pipeline_version,
        "METROPULSE_MAXIMUM_INPUT_BYTES": str(aws_config.maximum_input_bytes),
        "METROPULSE_ENVIRONMENT": aws_config.environment_name,
        "METROPULSE_DESTINATION_BUCKET": aws_config.destination_bucket,
    }
    assert LambdaConfig.from_environment(environment) == aws_config
    with pytest.raises(ConfigurationError, match="METROPULSE_CONTROL_PREFIX"):
        LambdaConfig.from_environment(
            {key: value for key, value in environment.items() if key != "METROPULSE_CONTROL_PREFIX"}
        )
    with pytest.raises(ConfigurationError, match="positive"):
        LambdaConfig.from_environment({**environment, "METROPULSE_MAXIMUM_INPUT_BYTES": "0"})


@pytest.mark.parametrize("event", [{}, {"Records": []}, {"Records": "bad"}])
def test_malformed_invocation_envelope(event) -> None:
    with pytest.raises(InvalidEvent):
        event_records(event)

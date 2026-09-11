"""Thin Lambda entry point for independent S3 event-record processing."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

from metropulse.aws.config import LambdaConfig
from metropulse.aws.events import InvalidEvent, event_records, parse_s3_record
from metropulse.aws.observability import StructuredObserver, safe_error
from metropulse.aws.processor import ObjectProcessor, ProcessingResult
from metropulse.aws.storage import Boto3S3Storage


class Processor(Protocol):
    """Per-record dependency accepted by the thin handler."""

    def process(self, record: Any, *, request_id: str | None) -> ProcessingResult: ...


@dataclass(frozen=True)
class InvocationFailure(RuntimeError):
    """One or more records failed after all records were attempted."""

    results: tuple[dict[str, object], ...]
    errors: tuple[dict[str, object], ...]

    def __str__(self) -> str:
        return f"{len(self.errors)} of {len(self.results) + len(self.errors)} S3 records failed"


def handle_event(
    event: Any,
    context: Any,
    *,
    config: LambdaConfig,
    processor: Processor,
    observer: StructuredObserver,
) -> dict[str, object]:
    """Process every record independently, then fail the invocation if any failed."""
    request_id = getattr(context, "aws_request_id", None)
    raw_records = event_records(event)
    results: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    for index, raw_record in enumerate(raw_records):
        try:
            record = parse_s3_record(raw_record, index, config)
            results.append(processor.process(record, request_id=request_id).to_dict())
        except InvalidEvent as error:
            details = {"aws_request_id": request_id, "record_index": index, **safe_error(error)}
            observer.event("invalid_event", **details, outcome="failed")
            errors.append(details)
        except Exception as error:
            errors.append(
                {"aws_request_id": request_id, "record_index": index, **safe_error(error)}
            )
    if errors:
        raise InvocationFailure(tuple(results), tuple(errors))
    return {"failed_record_count": 0, "record_count": len(results), "results": results}


def lambda_handler(event: Any, context: Any) -> dict[str, object]:
    """Create the boto3 boundary lazily and delegate all behavior."""
    config = LambdaConfig.from_environment(os.environ)
    observer = StructuredObserver(config.environment_name, config.pipeline_version)
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required in the Lambda runtime") from error
    processor = ObjectProcessor(
        config=config,
        storage=Boto3S3Storage(boto3.client("s3")),
        observer=observer,
    )
    return handle_event(event, context, config=config, processor=processor, observer=observer)

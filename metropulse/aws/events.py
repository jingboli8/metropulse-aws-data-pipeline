"""Strict parsing of S3 ObjectCreated notification records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import unquote_plus

from metropulse.aws.config import LambdaConfig
from metropulse.aws.keys import parse_raw_key


class InvalidEvent(ValueError):
    """An event record does not match the supported S3 input contract."""


@dataclass(frozen=True)
class S3ObjectCreatedRecord:
    """Normalized fields captured from one supported S3 event record."""

    record_index: int
    bucket: str
    key: str
    source_date: date
    version_id: str | None
    etag: str | None
    object_size: int
    sequencer: str | None
    event_time: datetime


def event_records(event: Any) -> tuple[Any, ...]:
    """Return raw records while rejecting a malformed invocation envelope."""
    if not isinstance(event, dict) or not isinstance(event.get("Records"), list):
        raise InvalidEvent("Event must contain a Records list")
    if not event["Records"]:
        raise InvalidEvent("Event Records must not be empty")
    return tuple(event["Records"])


def parse_s3_record(raw: Any, index: int, config: LambdaConfig) -> S3ObjectCreatedRecord:
    """Validate and normalize one S3 ObjectCreated record."""
    try:
        if not isinstance(raw, dict):
            raise TypeError("record is not an object")
        if raw.get("eventSource") != "aws:s3":
            raise InvalidEvent("Unsupported event source")
        event_name = raw.get("eventName")
        if not isinstance(event_name, str) or not event_name.startswith("ObjectCreated:"):
            raise InvalidEvent("Unsupported event name")
        event_time = _utc_timestamp(raw.get("eventTime"))
        s3 = raw["s3"]
        bucket = s3["bucket"]["name"]
        object_data = s3["object"]
        encoded_key = object_data["key"]
        size = object_data["size"]
        if not isinstance(bucket, str) or not bucket:
            raise InvalidEvent("S3 bucket name is missing")
        if not isinstance(encoded_key, str) or not encoded_key:
            raise InvalidEvent("S3 object key is missing")
        if not isinstance(size, int) or size < 0:
            raise InvalidEvent("S3 object size is invalid")
        key = unquote_plus(encoded_key)
        source_date = parse_raw_key(key, config)
        version_id = _optional_text(object_data.get("versionId"))
        etag = _optional_text(object_data.get("eTag"))
        sequencer = _optional_text(object_data.get("sequencer"))
    except InvalidEvent:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise InvalidEvent(f"Malformed S3 record at index {index}: {error}") from error
    return S3ObjectCreatedRecord(
        record_index=index,
        bucket=bucket,
        key=key,
        source_date=source_date,
        version_id=version_id,
        etag=etag,
        object_size=size,
        sequencer=sequencer,
        event_time=event_time,
    )


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise InvalidEvent("Optional S3 identity values must be non-empty strings")
    return value.strip().strip('"')


def _utc_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise InvalidEvent("eventTime is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise InvalidEvent("eventTime is not ISO 8601") from error
    if parsed.tzinfo is None:
        raise InvalidEvent("eventTime must contain an offset")
    return parsed.astimezone(UTC)

from __future__ import annotations

import csv
import io
from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import quote_plus

import pytest

from metropulse.aws.config import LambdaConfig
from metropulse.aws.observability import StructuredObserver
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.schema import SOURCE_FIELDS


@pytest.fixture
def aws_config() -> LambdaConfig:
    return LambdaConfig(
        expected_source_name="metropt3",
        raw_prefix="raw",
        staging_prefix="staging",
        quarantine_prefix="quarantine",
        control_prefix="control",
        pipeline_version="3.0.0-test",
        maximum_input_bytes=1_000_000,
        environment_name="test",
        destination_bucket="processed-bucket",
    )


@pytest.fixture
def fake_storage() -> InMemoryObjectStorage:
    return InMemoryObjectStorage()


@pytest.fixture
def captured_observer() -> tuple[StructuredObserver, list[dict[str, object]]]:
    records: list[dict[str, object]] = []
    return StructuredObserver("test", "3.0.0-test", records.append), records


@pytest.fixture
def csv_factory() -> Callable[..., bytes]:
    def make(*, invalid: bool = False, wrong_date: bool = False, rows: int = 2) -> bytes:
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\r\n")
        writer.writerow(SOURCE_FIELDS)
        for index in range(rows):
            day = "2020-02-02" if wrong_date else "2020-02-01"
            values = [
                str(index + 1),
                f"{day} 00:00:{index * 10:02d}",
                "-0.012",
                "9.358",
                "9.34",
                "-0.024",
                "9.358",
                "53.6",
                "bad" if invalid and index == rows - 1 else "0.04",
                "1.0",
                "0.0",
                "1.0",
                "1.0",
                "0.0",
                "1.0",
                "1.0",
                "1.0",
            ]
            writer.writerow(values)
        return output.getvalue().encode("utf-8")

    return make


@pytest.fixture
def event_record_factory() -> Callable[..., dict[str, object]]:
    def make(
        *,
        key: str = "raw/source=metropt3/source_date=2020-02-01/data.csv",
        bucket: str = "source-bucket",
        size: int = 123,
        etag: str | None = "etag-1",
        version_id: str | None = "version-1",
        event_source: str = "aws:s3",
        event_name: str = "ObjectCreated:Put",
    ) -> dict[str, object]:
        object_data: dict[str, object] = {
            "eTag": etag,
            "key": quote_plus(key, safe="/="),
            "sequencer": "0055AED6DCD90281E5",
            "size": size,
        }
        if version_id is not None:
            object_data["versionId"] = version_id
        if etag is None:
            object_data.pop("eTag")
        return {
            "eventName": event_name,
            "eventSource": event_source,
            "eventTime": "2026-09-11T08:00:00Z",
            "s3": {"bucket": {"name": bucket}, "object": object_data},
        }

    return make


@pytest.fixture
def fixed_clock() -> Callable[[], datetime]:
    return lambda: datetime(2026, 9, 11, 8, 0, tzinfo=UTC)

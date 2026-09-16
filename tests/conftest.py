from __future__ import annotations

import csv
import io
from collections.abc import Callable
from datetime import UTC, date, datetime
from urllib.parse import quote_plus

import pytest

from metropulse.aws.config import LambdaConfig
from metropulse.aws.observability import StructuredObserver
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_models import MonthlySelection, SelectedDailyInput
from metropulse.models import NormalizedRecord
from metropulse.parquet import serialize_daily_parquet
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


@pytest.fixture
def compact_fixture():
    """Build small selected inputs and valid daily Parquet payloads."""
    import hashlib

    def make(days: tuple[int, ...] = (1, 2), *, gap: bool = False):
        inputs = []
        payloads = {}
        for offset, day in enumerate(days):
            source_date = date(2020, 2, day)
            second = 0 if not gap or offset == 0 else 40
            timestamp = datetime(2020, 2, day, 0, 0, second)
            record = NormalizedRecord(
                record_index=offset + 1,
                event_timestamp_local=timestamp,
                tp2=-0.1,
                tp3=9.0,
                h1=8.0,
                dv_pressure=-0.2,
                reservoirs=9.1,
                oil_temperature=50.0,
                motor_current=1.0,
                comp=True,
                dv_electric=False,
                towers=True,
                mpg=False,
                lps=True,
                pressure_switch=False,
                oil_level=True,
                caudal_impulses=False,
                source_date=source_date,
                event_timestamp_timezone_status="unknown",
            )
            body = serialize_daily_parquet(
                [record],
                pipeline_version="2.0.0",
                source_identity={"fixture": True},
                source_date=source_date,
                raw_sha256="a" * 64,
                processing_timestamp="2026-09-10T00:00:00Z",
            )
            key = f"staging/day={day:02d}/data.parquet"
            item = SelectedDailyInput(
                source_date=source_date,
                processing_identity=f"input-{day}",
                completion_marker_bucket="bucket",
                completion_marker_key=f"control/day={day:02d}/completed.json",
                completion_marker_identity_kind="sha256",
                completion_marker_identity_value="b" * 64,
                staging_bucket="bucket",
                staging_key=key,
                staging_sha256=hashlib.sha256(body).hexdigest(),
                staging_byte_size=len(body),
                input_row_count=1,
                valid_row_count=1,
                quarantine_row_count=0,
                first_valid_timestamp=timestamp,
                last_valid_timestamp=timestamp,
                pipeline_version="2.0.0",
                schema_version="1.0.0",
                manifest_version="1.1.0",
            )
            inputs.append(item)
            payloads[key] = body
        selection = MonthlySelection(
            source_name="metropt3",
            year=2020,
            month=2,
            inputs=tuple(inputs),
            expected_raw_dates=tuple(date(2020, 2, day) for day in range(1, 30)),
            known_source_start=date(2020, 2, 1),
            known_source_end=date(2020, 9, 1),
        )
        return selection, payloads

    return make

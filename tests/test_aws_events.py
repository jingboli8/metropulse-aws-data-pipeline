from __future__ import annotations

from urllib.parse import quote_plus

import pytest

from metropulse.aws.config import ConfigurationError, LambdaConfig
from metropulse.aws.events import InvalidEvent, event_records, parse_s3_record
from metropulse.aws.keys import InvalidObjectKey


def test_valid_object_created_event_and_missing_optional_version(
    aws_config, event_record_factory
) -> None:
    raw = event_record_factory(version_id=None)

    parsed = parse_s3_record(raw, 0, aws_config)

    assert parsed.bucket == "source-bucket"
    assert parsed.key == "raw/source=metropt3/source_date=2020-02-01/data.csv"
    assert parsed.source_date.isoformat() == "2020-02-01"
    assert parsed.version_id is None
    assert parsed.etag == "etag-1"
    assert parsed.sequencer == "0055AED6DCD90281E5"


def test_s3_key_decodes_plus_spaces_and_percent_escapes(aws_config, event_record_factory) -> None:
    config = LambdaConfig(**{**aws_config.__dict__, "expected_source_name": "metro pt3+unit"})
    key = "raw/source=metro pt3+unit/source_date=2020-02-01/data.csv"
    raw = event_record_factory(key=key)
    raw["s3"]["object"]["key"] = quote_plus(key, safe="/=")

    parsed = parse_s3_record(raw, 0, config)

    assert parsed.key == key


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("eventSource", "aws:sns", "source"),
        ("eventName", "ObjectRemoved:Delete", "name"),
    ],
)
def test_invalid_source_or_event_name(
    aws_config, event_record_factory, field: str, value: str, message: str
) -> None:
    raw = event_record_factory()
    raw[field] = value
    with pytest.raises(InvalidEvent, match=message):
        parse_s3_record(raw, 0, aws_config)


@pytest.mark.parametrize(
    "key",
    [
        "landing/source=metropt3/source_date=2020-02-01/data.csv",
        "raw/source=metropt3/source_date=2020-02-01/data.parquet",
        "raw/source=other/source_date=2020-02-01/data.csv",
        "raw/source=metropt3/no_date/data.csv",
        "raw/source=metropt3/source_date=2020-99-01/data.csv",
    ],
)
def test_wrong_prefix_suffix_source_or_date_is_rejected(
    aws_config, event_record_factory, key: str
) -> None:
    with pytest.raises((InvalidEvent, InvalidObjectKey)):
        parse_s3_record(event_record_factory(key=key), 0, aws_config)


def test_multiple_records_are_retained(aws_config, event_record_factory) -> None:
    event = {"Records": [event_record_factory(), event_record_factory()]}
    assert len(event_records(event)) == 2


@pytest.mark.parametrize("event", [{}, {"Records": []}, {"Records": "bad"}])
def test_malformed_envelope_is_rejected(event) -> None:
    with pytest.raises(InvalidEvent):
        event_records(event)


def test_malformed_record_is_rejected(aws_config) -> None:
    with pytest.raises(InvalidEvent, match="eventTime"):
        parse_s3_record({"eventSource": "aws:s3", "eventName": "ObjectCreated:Put"}, 0, aws_config)


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
    environment.pop("METROPULSE_PIPELINE_VERSION")
    with pytest.raises(ConfigurationError, match="METROPULSE_PIPELINE_VERSION"):
        LambdaConfig.from_environment(environment)

"""Validated non-secret Lambda configuration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


class ConfigurationError(ValueError):
    """Lambda configuration is missing or malformed."""


@dataclass(frozen=True)
class LambdaConfig:
    """Runtime configuration supplied through non-secret environment variables."""

    expected_source_name: str
    raw_prefix: str
    staging_prefix: str
    quarantine_prefix: str
    control_prefix: str
    pipeline_version: str
    maximum_input_bytes: int
    environment_name: str
    destination_bucket: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> LambdaConfig:
        """Build and validate configuration from an environment mapping."""
        names = {
            "expected_source_name": "METROPULSE_EXPECTED_SOURCE_NAME",
            "raw_prefix": "METROPULSE_RAW_PREFIX",
            "staging_prefix": "METROPULSE_STAGING_PREFIX",
            "quarantine_prefix": "METROPULSE_QUARANTINE_PREFIX",
            "control_prefix": "METROPULSE_CONTROL_PREFIX",
            "pipeline_version": "METROPULSE_PIPELINE_VERSION",
            "maximum_input_bytes": "METROPULSE_MAXIMUM_INPUT_BYTES",
            "environment_name": "METROPULSE_ENVIRONMENT",
            "destination_bucket": "METROPULSE_DESTINATION_BUCKET",
        }
        missing = [name for name in names.values() if not environment.get(name, "").strip()]
        if missing:
            raise ConfigurationError(
                f"Missing required configuration: {', '.join(sorted(missing))}"
            )
        try:
            maximum_input_bytes = int(environment[names["maximum_input_bytes"]])
        except ValueError as error:
            raise ConfigurationError("METROPULSE_MAXIMUM_INPUT_BYTES must be an integer") from error
        if maximum_input_bytes <= 0:
            raise ConfigurationError("METROPULSE_MAXIMUM_INPUT_BYTES must be positive")
        prefixes = {
            field: _validate_prefix(environment[name], name)
            for field, name in names.items()
            if field.endswith("_prefix")
        }
        return cls(
            expected_source_name=_plain_value(
                environment[names["expected_source_name"]], names["expected_source_name"]
            ),
            raw_prefix=prefixes["raw_prefix"],
            staging_prefix=prefixes["staging_prefix"],
            quarantine_prefix=prefixes["quarantine_prefix"],
            control_prefix=prefixes["control_prefix"],
            pipeline_version=_plain_value(
                environment[names["pipeline_version"]], names["pipeline_version"]
            ),
            maximum_input_bytes=maximum_input_bytes,
            environment_name=_plain_value(
                environment[names["environment_name"]], names["environment_name"]
            ),
            destination_bucket=_plain_value(
                environment[names["destination_bucket"]], names["destination_bucket"]
            ),
        )


def _plain_value(value: str, name: str) -> str:
    value = value.strip()
    if any(character in value for character in "\\/\r\n"):
        raise ConfigurationError(f"{name} contains unsupported characters")
    return value


def _validate_prefix(value: str, name: str) -> str:
    value = value.strip().strip("/")
    if not value or "\\" in value or "//" in value or ".." in value.split("/"):
        raise ConfigurationError(f"{name} is not a safe S3 prefix")
    return value

"""Validated non-secret configuration for Phase 7 Lambda functions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from metropulse.audit_models import AuditBounds
from metropulse.aws.config import ConfigurationError
from metropulse.compaction_models import CompactionBounds


@dataclass(frozen=True)
class OperationsConfig:
    """Shared compaction and audit settings supplied through environment variables."""

    environment: str
    component_version: str
    destination_bucket: str
    glue_database: str
    glue_table: str
    compaction_bounds: CompactionBounds
    audit_bounds: AuditBounds
    audit_temp_directory: Path

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> OperationsConfig:
        """Parse and validate the Phase 7 Lambda environment contract."""
        required = (
            "METROPULSE_ENVIRONMENT",
            "METROPULSE_COMPONENT_VERSION",
            "METROPULSE_DESTINATION_BUCKET",
            "METROPULSE_GLUE_DATABASE",
            "METROPULSE_GLUE_TABLE",
        )
        missing = [name for name in required if not environment.get(name, "").strip()]
        if missing:
            raise ConfigurationError(
                f"Missing required configuration: {', '.join(sorted(missing))}"
            )
        temp = Path(environment.get("METROPULSE_AUDIT_TMP", "/tmp/metropulse-audit"))
        normalized = temp.as_posix().rstrip("/")
        if normalized != "/tmp/metropulse-audit":
            raise ConfigurationError("METROPULSE_AUDIT_TMP must be /tmp/metropulse-audit")
        return cls(
            environment=_plain(environment["METROPULSE_ENVIRONMENT"]),
            component_version=_plain(environment["METROPULSE_COMPONENT_VERSION"]),
            destination_bucket=_plain(environment["METROPULSE_DESTINATION_BUCKET"]),
            glue_database=_plain(environment["METROPULSE_GLUE_DATABASE"]),
            glue_table=_plain(environment["METROPULSE_GLUE_TABLE"]),
            compaction_bounds=CompactionBounds(
                max_selected_objects=_integer(environment, "METROPULSE_MAX_SELECTED_OBJECTS", 31),
                max_compressed_input_bytes=_integer(
                    environment, "METROPULSE_MAX_COMPACTION_INPUT_BYTES", 128 * 1024 * 1024
                ),
                max_input_rows=_integer(environment, "METROPULSE_MAX_COMPACTION_ROWS", 500_000),
                max_output_bytes=_integer(
                    environment, "METROPULSE_MAX_COMPACTION_OUTPUT_BYTES", 128 * 1024 * 1024
                ),
            ),
            audit_bounds=AuditBounds(
                max_months=_integer(environment, "METROPULSE_MAX_AUDIT_MONTHS", 24),
                max_object_bytes=_integer(
                    environment, "METROPULSE_MAX_AUDIT_OBJECT_BYTES", 128 * 1024 * 1024
                ),
                max_total_bytes=_integer(
                    environment, "METROPULSE_MAX_AUDIT_TOTAL_BYTES", 256 * 1024 * 1024
                ),
                max_total_rows=_integer(environment, "METROPULSE_MAX_AUDIT_ROWS", 2_000_000),
            ),
            audit_temp_directory=temp,
        )


def _plain(value: str) -> str:
    value = value.strip()
    if not value or any(character in value for character in "\\\r\n"):
        raise ConfigurationError("operation configuration contains unsupported characters")
    return value


def _integer(environment: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(environment.get(name, str(default)))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if value <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return value

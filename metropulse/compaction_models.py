"""Immutable models shared by local and AWS monthly compaction adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol


class CompactionError(RuntimeError):
    """Base error for a rejected monthly compaction operation."""


class SelectionError(CompactionError):
    """The explicit approved-input selection violates its contract."""


class ImmutableOutputConflict(CompactionError):
    """An immutable key exists with content other than the requested content."""


class PublicationConflict(CompactionError):
    """The catalog or publication claim has an unexpected current state."""


@dataclass(frozen=True)
class SelectedDailyInput:
    """One pinned daily completion marker and its approved staging object."""

    source_date: date
    processing_identity: str
    completion_marker_bucket: str
    completion_marker_key: str
    completion_marker_identity_kind: str
    completion_marker_identity_value: str
    staging_bucket: str
    staging_key: str
    staging_sha256: str
    staging_byte_size: int
    input_row_count: int
    valid_row_count: int
    quarantine_row_count: int
    first_valid_timestamp: datetime | None
    last_valid_timestamp: datetime | None
    pipeline_version: str
    schema_version: str
    manifest_version: str


@dataclass(frozen=True)
class MonthlySelection:
    """Explicitly approved daily inputs and source-range coverage for one month."""

    source_name: str
    year: int
    month: int
    inputs: tuple[SelectedDailyInput, ...]
    expected_raw_dates: tuple[date, ...]
    known_source_start: date
    known_source_end: date
    terminal_partial_month: bool = False


@dataclass(frozen=True)
class CompactionBounds:
    """Resource limits applied before monthly data is materialized."""

    max_selected_objects: int = 31
    max_compressed_input_bytes: int = 128 * 1024 * 1024
    max_input_rows: int = 500_000
    max_output_bytes: int = 128 * 1024 * 1024


@dataclass(frozen=True)
class BoundaryFinding:
    """One boundary observation between adjacent selected inputs."""

    previous_date: date
    current_date: date
    seconds: float | None
    classification: str


@dataclass(frozen=True)
class CompactionResult:
    """Verified monthly output plus stable completion evidence."""

    run_id: str
    parquet_bytes: bytes
    parquet_sha256: str
    parquet_byte_size: int
    selected_row_count: int
    curated_row_count: int
    quarantine_row_count: int
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    valid_timestamp_count: int
    row_group_count: int
    missing_dates: tuple[date, ...]
    boundary_findings: tuple[BoundaryFinding, ...]
    significant_gap_count: int
    completion: dict[str, Any]


class PartitionPublisher(Protocol):
    """Publish one completed run as the visible Glue partition location."""

    def publish(
        self,
        *,
        year: str,
        month: str,
        location: str,
        expected_current_location: str | None,
    ) -> str: ...

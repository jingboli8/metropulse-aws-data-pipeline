"""Immutable contracts for scheduled curated-data integrity audits."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol


class AuditContractError(ValueError):
    """An audit invocation or inventory violates its deterministic contract."""


@dataclass(frozen=True)
class PinnedObject:
    """One exact object and immutable identity."""

    bucket: str
    key: str
    identity_kind: str
    identity_value: str


@dataclass(frozen=True)
class CuratedObject:
    """One immutable curated Parquet object with expected content evidence."""

    bucket: str
    key: str
    sha256: str
    byte_size: int
    version_id: str | None = None


@dataclass(frozen=True)
class AuditMonth:
    """The approved current state expected for one source month."""

    year: int
    month: int
    run_id: str
    selection: PinnedObject
    completion: PinnedObject
    publication: PinnedObject
    curated: CuratedObject
    glue_values: tuple[str, str]
    glue_location: str
    selected_row_count: int
    curated_row_count: int
    quarantine_row_count: int
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    within_month_gap_count: int
    coverage_status: str
    known_missing_dates: tuple[date, ...]
    terminal_partial_month: bool


@dataclass(frozen=True)
class AuditInventory:
    """Exact immutable approved state for a complete scheduled audit."""

    source_name: str
    known_source_start: datetime
    known_source_end: datetime
    expected_months: tuple[str, ...]
    months: tuple[AuditMonth, ...]
    expected_total_curated_rows: int
    schema_version: str
    compaction_manifest_version: str
    inventory_version: str


@dataclass(frozen=True)
class AuditBounds:
    """Hard limits for one scheduled full-checksum audit."""

    max_months: int = 24
    max_object_bytes: int = 128 * 1024 * 1024
    max_total_bytes: int = 256 * 1024 * 1024
    max_total_rows: int = 2_000_000


@dataclass(frozen=True)
class AuditMonthResult:
    """Verified or failed result for one inventory month."""

    year: str
    month: str
    run_id: str
    row_count: int
    byte_size: int
    status: str
    failures: tuple[str, ...]


@dataclass(frozen=True)
class ScheduledAuditResult:
    """Complete operational, reconciliation, and quality audit result."""

    inventory_id: str
    month_results: tuple[AuditMonthResult, ...]
    fatal_failures: tuple[str, ...]
    reconciliation_failures: tuple[str, ...]
    publication_drift: tuple[str, ...]
    quality_observations: tuple[dict[str, Any], ...]
    months_inspected: int
    total_curated_rows: int
    within_month_gap_count: int
    cross_month_gap_count: int
    global_gap_count: int
    overlap_count: int
    reversed_boundary_count: int
    status: str

    @property
    def operational_failure(self) -> bool:
        """Return whether the invocation must fail after emitting evidence."""
        return bool(self.fatal_failures or self.reconciliation_failures or self.publication_drift)


class PartitionCatalogReader(Protocol):
    """Read-only catalog boundary used by the scheduled audit."""

    def get_partition(self, *, year: str, month: str) -> dict[str, Any] | None: ...

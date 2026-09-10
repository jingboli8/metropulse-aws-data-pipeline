"""Typed daily and aggregate results for the local backfill."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass(frozen=True)
class DayOutcome:
    """Completion status and manifest for one selected source date."""

    status: str
    manifest: Mapping[str, Any] | None
    error: str | None = None


@dataclass
class BackfillSummary:
    """Aggregate local run evidence independent of machine paths."""

    source_identity: Mapping[str, object]
    processing_timestamp: str
    pipeline_version: str
    processed_day_count: int = 0
    skipped_day_count: int = 0
    rebuilt_day_count: int = 0
    failed_day_count: int = 0
    input_row_count: int = 0
    valid_row_count: int = 0
    quarantine_row_count: int = 0
    raw_byte_size: int = 0
    staging_byte_size: int = 0
    quarantine_byte_size: int = 0
    dates: list[str] = field(default_factory=list)
    warning_day_counts: Counter[str] = field(default_factory=Counter)
    warning_observation_counts: Counter[str] = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def completed_day_count(self) -> int:
        """Return all successful selected partitions, including verified skips."""
        return self.processed_day_count + self.skipped_day_count + self.rebuilt_day_count

    @property
    def reconciliation_matches(self) -> bool:
        """Return whether all selected rows and partitions reconciled."""
        return (
            self.failed_day_count == 0
            and self.input_row_count == self.valid_row_count + self.quarantine_row_count
        )

    def add_outcome(self, source_date: date, outcome: DayOutcome) -> None:
        """Accumulate a daily result in deterministic source-date order."""
        self.dates.append(source_date.isoformat())
        if outcome.status == "failed":
            self.failed_day_count += 1
            self.errors.append(outcome.error or f"{source_date}: unknown failure")
            return
        counters = {
            "processed": "processed_day_count",
            "rebuilt": "rebuilt_day_count",
            "skipped": "skipped_day_count",
        }
        if outcome.status not in counters:
            raise ValueError(f"Unknown day outcome: {outcome.status}")
        setattr(self, counters[outcome.status], getattr(self, counters[outcome.status]) + 1)
        if outcome.manifest is None:
            raise ValueError("A successful day requires a manifest")
        manifest = outcome.manifest
        self.input_row_count += int(manifest["input_row_count"])
        self.valid_row_count += int(manifest["valid_row_count"])
        self.quarantine_row_count += int(manifest["quarantine_row_count"])
        self.raw_byte_size += int(manifest["raw_byte_size"])
        self.staging_byte_size += int(manifest["staging_byte_size"])
        self.quarantine_byte_size += int(manifest["quarantine_byte_size"])
        for warning in manifest["batch_warnings"]:
            rule_id = str(warning["rule_id"])
            self.warning_day_counts[rule_id] += 1
            self.warning_observation_counts[rule_id] += int(warning["observed_count"])

    def to_dict(self) -> dict[str, object]:
        """Return portable JSON-ready evidence for the complete run."""
        return {
            "completed_day_count": self.completed_day_count,
            "date_count": len(self.dates),
            "date_range": {
                "end": self.dates[-1] if self.dates else None,
                "start": self.dates[0] if self.dates else None,
            },
            "duration_seconds": round(self.duration_seconds, 6),
            "errors": list(self.errors),
            "failed_day_count": self.failed_day_count,
            "input_row_count": self.input_row_count,
            "pipeline_version": self.pipeline_version,
            "processed_day_count": self.processed_day_count,
            "processing_timestamp": self.processing_timestamp,
            "quarantine_byte_size": self.quarantine_byte_size,
            "quarantine_row_count": self.quarantine_row_count,
            "raw_byte_size": self.raw_byte_size,
            "rebuilt_day_count": self.rebuilt_day_count,
            "reconciliation_matches": self.reconciliation_matches,
            "skipped_day_count": self.skipped_day_count,
            "source_identity": dict(self.source_identity),
            "staging_byte_size": self.staging_byte_size,
            "valid_row_count": self.valid_row_count,
            "warning_day_counts": dict(sorted(self.warning_day_counts.items())),
            "warning_observation_counts": dict(sorted(self.warning_observation_counts.items())),
        }

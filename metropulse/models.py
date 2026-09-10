"""Typed records returned by the AWS-independent transformation core."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class SourceMetadata:
    """Optional object lineage supplied by a caller or future adapter."""

    source_bucket: str | None = None
    source_object_key: str | None = None
    source_object_version_id: str | None = None
    source_object_etag: str | None = None
    source_object_checksum: str | None = None


@dataclass(frozen=True)
class NormalizedRecord:
    """One valid normalized observation of all 15 MetroPT-3 signals."""

    record_index: int
    event_timestamp_local: datetime
    tp2: float
    tp3: float
    h1: float
    dv_pressure: float
    reservoirs: float
    oil_temperature: float
    motor_current: float
    comp: bool
    dv_electric: bool
    towers: bool
    mpg: bool
    lps: bool
    pressure_switch: bool
    oil_level: bool
    caudal_impulses: bool
    source_date: date
    event_timestamp_timezone_status: str

    def to_dict(self) -> dict[str, Any]:
        """Return values keyed in normalized schema order."""
        return {
            "record_index": self.record_index,
            "event_timestamp_local": self.event_timestamp_local,
            "tp2": self.tp2,
            "tp3": self.tp3,
            "h1": self.h1,
            "dv_pressure": self.dv_pressure,
            "reservoirs": self.reservoirs,
            "oil_temperature": self.oil_temperature,
            "motor_current": self.motor_current,
            "comp": self.comp,
            "dv_electric": self.dv_electric,
            "towers": self.towers,
            "mpg": self.mpg,
            "lps": self.lps,
            "pressure_switch": self.pressure_switch,
            "oil_level": self.oil_level,
            "caudal_impulses": self.caudal_impulses,
            "source_date": self.source_date,
            "event_timestamp_timezone_status": self.event_timestamp_timezone_status,
        }


@dataclass(frozen=True)
class QuarantineRecord:
    """Deterministic, serialization-ready rejected-row record."""

    original_record: str
    original_field_values: tuple[str, ...]
    source_row_number: int
    rule_ids: tuple[str, ...]
    rejection_reasons: tuple[str, ...]
    source_bucket: str | None
    source_object_key: str | None
    source_object_version_id: str | None
    source_object_etag: str | None
    source_object_checksum: str | None
    processing_timestamp: datetime
    pipeline_version: str
    expected_source_date: date

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible mapping with stable keys and values."""
        processing_timestamp = self.processing_timestamp.isoformat().replace("+00:00", "Z")
        return {
            "original_record": self.original_record,
            "original_field_values": list(self.original_field_values),
            "source_row_number": self.source_row_number,
            "rule_ids": list(self.rule_ids),
            "rejection_reasons": list(self.rejection_reasons),
            "source_bucket": self.source_bucket,
            "source_object_key": self.source_object_key,
            "source_object_version_id": self.source_object_version_id,
            "source_object_etag": self.source_object_etag,
            "source_object_checksum": self.source_object_checksum,
            "processing_timestamp": processing_timestamp,
            "pipeline_version": self.pipeline_version,
            "expected_source_date": self.expected_source_date.isoformat(),
        }


@dataclass(frozen=True)
class IntervalFrequency:
    """Count for one observed adjacent timestamp delta."""

    seconds: int
    count: int


@dataclass(frozen=True)
class RuleCount:
    """Number of quarantined rows carrying one rule ID."""

    rule_id: str
    count: int


@dataclass(frozen=True)
class BatchMetrics:
    """Metrics that describe a complete daily input without changing row validity."""

    input_row_count: int
    valid_row_count: int
    quarantine_row_count: int
    parseable_timestamp_count: int
    timestamp_deltas_seconds: tuple[int, ...]
    sampling_interval_distribution: tuple[IntervalFrequency, ...]
    out_of_order_transition_count: int
    abnormal_sampling_interval_count: int
    significant_gap_count: int
    maximum_gap_seconds: int | None
    unexpectedly_low_daily_row_count: bool
    empty_input: bool
    schema_drift: bool
    quarantine_rule_counts: tuple[RuleCount, ...]


@dataclass(frozen=True)
class BatchWarning:
    """Non-quarantining batch observation requiring visibility."""

    rule_id: str
    message: str
    observed_count: int


@dataclass(frozen=True)
class BatchError:
    """Batch condition that prevents publication of a completion marker."""

    rule_id: str
    message: str


@dataclass(frozen=True)
class ReconciliationResult:
    """Deterministic comparison of input and accounted row counts."""

    input_row_count: int
    valid_row_count: int
    quarantine_row_count: int
    accounted_row_count: int
    matches: bool
    applicable: bool


@dataclass(frozen=True)
class TransformationResult:
    """Complete result of transforming one daily MetroPT-3 CSV object."""

    valid_records: tuple[NormalizedRecord, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    batch_metrics: BatchMetrics
    batch_warnings: tuple[BatchWarning, ...]
    batch_errors: tuple[BatchError, ...]
    input_row_count: int
    valid_row_count: int
    quarantine_row_count: int
    reconciliation: ReconciliationResult

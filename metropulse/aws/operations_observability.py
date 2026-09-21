"""Structured logs and low-cardinality EMF for compaction and scheduled audit."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from metropulse.aws.observability import print_json

OPERATIONS_METRIC_UNITS = {
    "AuditDurationMs": "Milliseconds",
    "AuditRowReconciliationFailures": "Count",
    "AuditRunsFailed": "Count",
    "AuditRunsSucceeded": "Count",
    "CompactionDurationMs": "Milliseconds",
    "CompactionInputRows": "Count",
    "CompactionOutputRows": "Count",
    "CompactionReconciliationFailures": "Count",
    "CompactionRunsFailed": "Count",
    "CompactionRunsNoOp": "Count",
    "CompactionRunsStarted": "Count",
    "CompactionRunsSucceeded": "Count",
    "CrossMonthOverlaps": "Count",
    "CrossMonthReversedBoundaries": "Count",
    "CrossMonthSignificantGaps": "Count",
    "ImmutableOutputConflicts": "Count",
    "MissingPublications": "Count",
    "MonthsInspected": "Count",
    "PublicationConflicts": "Count",
    "PublicationDrift": "Count",
    "SelectedDays": "Count",
}


@dataclass
class OperationsObserver:
    """Emit stable operations events and EMF through one injected sink."""

    environment: str
    component_version: str
    sink: Callable[[Mapping[str, object]], None] = print_json

    def event(self, event_name: str, **fields: object) -> None:
        """Emit one structured event without raw data or environment contents."""
        self.sink(
            {
                "component_version": self.component_version,
                "environment": self.environment,
                "event_name": event_name,
                **fields,
            }
        )

    def metrics(self, timestamp: datetime, values: Mapping[str, int | float]) -> None:
        """Emit one EMF record with bounded Environment and ComponentVersion dimensions."""
        unknown = set(values) - OPERATIONS_METRIC_UNITS.keys()
        if unknown:
            raise ValueError(f"unsupported operations metrics: {sorted(unknown)}")
        metrics = [{"Name": name, "Unit": OPERATIONS_METRIC_UNITS[name]} for name in sorted(values)]
        self.sink(
            {
                "ComponentVersion": self.component_version,
                "Environment": self.environment,
                "_aws": {
                    "CloudWatchMetrics": [
                        {
                            "Dimensions": [["Environment", "ComponentVersion"]],
                            "Metrics": metrics,
                            "Namespace": "MetroPulse/DataPipeline",
                        }
                    ],
                    "Timestamp": int(timestamp.timestamp() * 1000),
                },
                "event_name": "operations_metrics_emitted",
                **dict(values),
            }
        )

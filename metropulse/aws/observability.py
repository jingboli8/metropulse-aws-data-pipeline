"""Structured operational logs and CloudWatch Embedded Metric Format records."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

METRIC_UNITS = {
    "InputRows": "Count",
    "ObjectsFailed": "Count",
    "ObjectsProcessed": "Count",
    "ObjectsSkippedDuplicate": "Count",
    "ProcessingDurationMs": "Milliseconds",
    "QuarantineRate": "Percent",
    "QuarantineRows": "Count",
    "ReconciliationFailures": "Count",
    "ValidRows": "Count",
}


def json_line(payload: Mapping[str, object]) -> str:
    """Return one stable JSON log line."""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def print_json(payload: Mapping[str, object]) -> None:
    """Write one structured record to the Lambda log stream."""
    print(json_line(payload))


@dataclass
class StructuredObserver:
    """Emit stable JSON events and EMF without direct CloudWatch API calls."""

    environment: str
    pipeline_version: str
    sink: Callable[[Mapping[str, object]], None] = print_json

    def event(self, event_name: str, **fields: object) -> None:
        """Emit one operational event."""
        self.sink(
            {
                "environment": self.environment,
                "event_name": event_name,
                "pipeline_version": self.pipeline_version,
                **fields,
            }
        )

    def metrics(self, timestamp: datetime, values: Mapping[str, int | float]) -> None:
        """Emit one low-cardinality CloudWatch EMF record."""
        definitions = [{"Name": name, "Unit": METRIC_UNITS[name]} for name in sorted(values)]
        self.sink(
            {
                "Environment": self.environment,
                "PipelineVersion": self.pipeline_version,
                "_aws": {
                    "CloudWatchMetrics": [
                        {
                            "Dimensions": [["Environment", "PipelineVersion"]],
                            "Metrics": definitions,
                            "Namespace": "MetroPulse/DataPipeline",
                        }
                    ],
                    "Timestamp": int(timestamp.timestamp() * 1000),
                },
                "event_name": "metrics_emitted",
                **dict(values),
            }
        )


def safe_error(error: Exception) -> dict[str, Any]:
    """Return bounded error fields without raw input content."""
    return {"error_message": str(error)[:500], "error_type": type(error).__name__}

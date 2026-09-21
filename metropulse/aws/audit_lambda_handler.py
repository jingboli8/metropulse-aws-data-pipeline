"""Thin scheduled/manual Lambda handler for one exact curated audit inventory."""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from metropulse.audit_models import PinnedObject, ScheduledAuditResult
from metropulse.aws.audit_processor import ScheduledAuditProcessor
from metropulse.aws.catalog import GluePartitionReader
from metropulse.aws.operations_config import OperationsConfig
from metropulse.aws.operations_observability import OperationsObserver
from metropulse.aws.storage import Boto3S3Storage

AUDIT_INVOCATION_CONTRACT = "metropulse-scheduled-audit-invocation-v1"
_INVENTORY_KEY = re.compile(
    r"^control/audit/source=metropt3/inventory_id=([0-9a-f]{64})/inventory\.json$"
)


@dataclass(frozen=True)
class AuditInvocationFailure(RuntimeError):
    """Operational integrity failed after complete evidence emission."""

    result: ScheduledAuditResult

    def __str__(self) -> str:
        return "scheduled audit found operational integrity failures"


def handle_event(
    event: Any,
    context: Any,
    *,
    config: OperationsConfig,
    processor: ScheduledAuditProcessor,
    observer: OperationsObserver,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Validate, execute, emit evidence, then fail if operational integrity is broken."""
    started = monotonic()
    reference, audit_time, scheduler = _parse_event(event)
    request_id = getattr(context, "aws_request_id", None)
    observer.event(
        "audit_started",
        audit_time=audit_time.isoformat(),
        aws_request_id=request_id,
        inventory_key=reference.key,
        scheduler=scheduler,
    )
    try:
        result = processor.process(reference)
    except Exception as error:
        duration = max((monotonic() - started) * 1000, 0.0)
        observer.metrics(
            audit_time,
            {"AuditDurationMs": duration, "AuditRunsFailed": 1},
        )
        observer.event(
            "audit_failed",
            aws_request_id=request_id,
            duration_ms=duration,
            error_message=str(error)[:500],
            error_type=type(error).__name__,
        )
        raise
    duration = max((monotonic() - started) * 1000, 0.0)
    metrics: dict[str, int | float] = {
        "AuditDurationMs": duration,
        "AuditRowReconciliationFailures": len(result.reconciliation_failures),
        "AuditRunsFailed": int(result.operational_failure),
        "AuditRunsSucceeded": int(not result.operational_failure),
        "CrossMonthOverlaps": result.overlap_count,
        "CrossMonthReversedBoundaries": result.reversed_boundary_count,
        "CrossMonthSignificantGaps": result.cross_month_gap_count,
        "MissingPublications": sum(
            "missing" in value.lower() for value in result.publication_drift
        ),
        "MonthsInspected": result.months_inspected,
        "PublicationDrift": len(result.publication_drift),
    }
    observer.metrics(audit_time, metrics)
    observer.event(
        "audit_completed" if not result.operational_failure else "audit_failed",
        aws_request_id=request_id,
        cross_month_gap_count=result.cross_month_gap_count,
        duration_ms=duration,
        fatal_failure_count=len(result.fatal_failures),
        global_gap_count=result.global_gap_count,
        inventory_id=result.inventory_id,
        months_inspected=result.months_inspected,
        outcome=result.status,
        publication_drift_count=len(result.publication_drift),
        reconciliation_failure_count=len(result.reconciliation_failures),
        total_curated_rows=result.total_curated_rows,
        within_month_gap_count=result.within_month_gap_count,
    )
    if result.operational_failure:
        raise AuditInvocationFailure(result)
    return _bounded_result(result)


def lambda_handler(event: Any, context: Any) -> dict[str, object]:
    """Create read-only boto3 boundaries lazily and delegate the audit invocation."""
    config = OperationsConfig.from_environment(os.environ)
    config.audit_temp_directory.mkdir(parents=True, exist_ok=True)
    observer = OperationsObserver(config.environment, config.component_version)
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required in the Lambda runtime") from error
    processor = ScheduledAuditProcessor(
        storage=Boto3S3Storage(boto3.client("s3")),
        catalog=GluePartitionReader(
            boto3.client("glue"), database=config.glue_database, table=config.glue_table
        ),
        bounds=config.audit_bounds,
    )
    return handle_event(
        event,
        context,
        config=config,
        processor=processor,
        observer=observer,
    )


def _parse_event(event: Any) -> tuple[PinnedObject, datetime, dict[str, object]]:
    if not isinstance(event, Mapping) or event.get("contract") != AUDIT_INVOCATION_CONTRACT:
        raise ValueError("unsupported scheduled-audit invocation contract")
    if event.get("verification_mode") != "full_checksum":
        raise ValueError("verification_mode must be full_checksum")
    raw = event.get("inventory")
    if not isinstance(raw, Mapping):
        raise ValueError("exact pinned inventory reference is required")
    reference = PinnedObject(
        bucket=str(raw.get("bucket", "")),
        key=str(raw.get("key", "")),
        identity_kind=str(raw.get("identity_kind", "")),
        identity_value=str(raw.get("identity_value", "")),
    )
    if (
        not reference.bucket
        or not reference.key
        or reference.identity_kind not in {"version_id", "sha256", "etag"}
        or not reference.identity_value
    ):
        raise ValueError("audit inventory reference is unpinned")
    match = _INVENTORY_KEY.fullmatch(reference.key)
    if match is None:
        raise ValueError("audit inventory key is outside the canonical layout")
    if reference.identity_kind == "sha256" and not re.fullmatch(
        r"[0-9a-f]{64}", reference.identity_value
    ):
        raise ValueError("inventory SHA-256 must be lowercase hexadecimal")
    supplied_time = event.get("audit_time", event.get("scheduled_time"))
    audit_time = _aware_time(supplied_time)
    scheduler = _scheduler_context(event)
    return reference, audit_time, scheduler


def _scheduler_context(event: Mapping[str, object]) -> dict[str, object]:
    present = [
        name
        for name in ("schedule_arn", "scheduled_time", "execution_id", "attempt_number")
        if name in event
    ]
    if not present:
        return {"source": "manual"}
    if len(present) != 4:
        raise ValueError("Scheduler context must be complete when supplied")
    values = {name: str(event[name]) for name in present}
    if any(not value or len(value) > 1024 or "\n" in value for value in values.values()):
        raise ValueError("Scheduler context is malformed")
    try:
        attempt = int(values["attempt_number"])
    except ValueError as error:
        raise ValueError("Scheduler attempt number is malformed") from error
    if attempt < 1:
        raise ValueError("Scheduler attempt number must be positive")
    return {**values, "attempt_number": attempt, "source": "scheduler"}


def _aware_time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("audit time must be ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("audit time must include an offset")
    return parsed.astimezone(UTC)


def _bounded_result(result: ScheduledAuditResult) -> dict[str, object]:
    return {
        "cross_month_gap_count": result.cross_month_gap_count,
        "global_gap_count": result.global_gap_count,
        "inventory_id": result.inventory_id,
        "months_inspected": result.months_inspected,
        "outcome": result.status,
        "total_curated_rows": result.total_curated_rows,
        "within_month_gap_count": result.within_month_gap_count,
    }

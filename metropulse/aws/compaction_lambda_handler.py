"""Thin on-demand Lambda handler for exact approved monthly selections."""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from metropulse.aws.catalog import GluePartitionPublisher
from metropulse.aws.compaction_processor import S3MonthlyCompactionProcessor
from metropulse.aws.operations_config import OperationsConfig
from metropulse.aws.operations_observability import OperationsObserver
from metropulse.aws.storage import Boto3S3Storage, ObjectNotFound, ObjectStorage
from metropulse.compaction_models import ImmutableOutputConflict, PublicationConflict
from metropulse.curated_parquet import sha256_bytes
from metropulse.selection_approval import parse_selection_document

COMPACTION_INVOCATION_CONTRACT = "metropulse-compaction-invocation-v1"
_KEY = re.compile(
    r"^control/compaction/source=metropt3/year=(\d{4})/month=(\d{2})/"
    r"run_id=([0-9a-f]{64})/selection\.json$"
)


class CompactionInvocationError(ValueError):
    """The on-demand compaction event is malformed or unpinned."""


def handle_event(
    event: Any,
    context: Any,
    *,
    config: OperationsConfig,
    storage: ObjectStorage,
    publisher: Any,
    observer: OperationsObserver,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Validate one invocation, load its exact selection, and compact it."""
    started = monotonic()
    request_id = getattr(context, "aws_request_id", None)
    if not isinstance(request_id, str) or not request_id.strip():
        raise CompactionInvocationError("Lambda request ID is required for claim ownership")
    owner_token = f"compaction:{request_id}"
    processing_time: datetime | None = None
    run_id: str | None = None
    try:
        year, month, reference, processing_time, expected_location, authorized_etag = _parse_event(
            event
        )
        selection_bytes = _read_pinned_selection(storage, reference)
        run_id, selection = parse_selection_document(selection_bytes)
        match = _KEY.fullmatch(reference["key"])
        if match is None or match.groups() != (year, month, run_id):
            raise CompactionInvocationError("selection key conflicts with target month or run ID")
        if (selection.year, selection.month) != (int(year), int(month)):
            raise CompactionInvocationError("selection body conflicts with target month")
        selected_rows = sum(item.valid_row_count for item in selection.inputs)
        observer.event(
            "compaction_started",
            aws_request_id=request_id,
            month=f"{year}-{month}",
            owner_token=owner_token,
            run_id=run_id,
            selected_days=len(selection.inputs),
        )
        observer.metrics(processing_time, {"CompactionRunsStarted": 1})
        processor = S3MonthlyCompactionProcessor(
            storage,
            publisher,
            bounds=config.compaction_bounds,
        )
        result = processor.process(
            selection,
            destination_bucket=config.destination_bucket,
            owner_token=owner_token,
            processing_timestamp=processing_time,
            expected_current_location=expected_location,
            authorized_publication_claim_etag=authorized_etag,
        )
        duration = max((monotonic() - started) * 1000, 0.0)
        values: dict[str, int | float] = {
            "CompactionDurationMs": duration,
            "CompactionRunsSucceeded": 1,
        }
        if result["newly_published"]:
            values.update(
                {
                    "CompactionInputRows": selected_rows,
                    "CompactionOutputRows": selected_rows,
                    "SelectedDays": len(selection.inputs),
                }
            )
        else:
            values["CompactionRunsNoOp"] = 1
        observer.metrics(processing_time, values)
        observer.event(
            "compaction_completed",
            aws_request_id=request_id,
            duration_ms=duration,
            month=f"{year}-{month}",
            outcome=result["status"],
            owner_token=owner_token,
            run_id=run_id,
        )
        return {
            "location": result["location"],
            "month": f"{year}-{month}",
            "outcome": result["status"],
            "run_id": run_id,
        }
    except Exception as error:
        timestamp = processing_time or datetime(1970, 1, 1, tzinfo=UTC)
        duration = max((monotonic() - started) * 1000, 0.0)
        values = {
            "CompactionDurationMs": duration,
            "CompactionRunsFailed": 1,
            "CompactionReconciliationFailures": int("reconcil" in str(error).lower()),
            "ImmutableOutputConflicts": int(isinstance(error, ImmutableOutputConflict)),
            "PublicationConflicts": int(isinstance(error, PublicationConflict)),
        }
        observer.metrics(timestamp, values)
        observer.event(
            "compaction_failed",
            aws_request_id=request_id,
            duration_ms=duration,
            error_message=str(error)[:500],
            error_type=type(error).__name__,
            owner_token=owner_token,
            run_id=run_id,
        )
        raise


def lambda_handler(event: Any, context: Any) -> dict[str, object]:
    """Create boto3 boundaries lazily and delegate the on-demand invocation."""
    config = OperationsConfig.from_environment(os.environ)
    observer = OperationsObserver(config.environment, config.component_version)
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required in the Lambda runtime") from error
    storage = Boto3S3Storage(boto3.client("s3"))
    publisher = GluePartitionPublisher(
        boto3.client("glue"), database=config.glue_database, table=config.glue_table
    )
    return handle_event(
        event,
        context,
        config=config,
        storage=storage,
        publisher=publisher,
        observer=observer,
    )


def _parse_event(
    event: Any,
) -> tuple[str, str, dict[str, str], datetime, str | None, str | None]:
    if not isinstance(event, Mapping) or event.get("contract") != COMPACTION_INVOCATION_CONTRACT:
        raise CompactionInvocationError("unsupported compaction invocation contract")
    if "selection_document" in event or "selection_payload" in event:
        raise CompactionInvocationError("inline selections are not accepted")
    year, month = str(event.get("year", "")), str(event.get("month", ""))
    if (
        not re.fullmatch(r"\d{4}", year)
        or not re.fullmatch(r"\d{2}", month)
        or not 1 <= int(month) <= 12
    ):
        raise CompactionInvocationError("year and month must use YYYY and MM")
    reference = event.get("selection")
    if not isinstance(reference, Mapping):
        raise CompactionInvocationError("exact selection reference is required")
    parsed = {
        name: str(reference.get(name, ""))
        for name in ("bucket", "key", "identity_kind", "identity_value")
    }
    if not all(parsed.values()) or parsed["identity_kind"] not in {"version_id", "sha256", "etag"}:
        raise CompactionInvocationError("selection reference is unpinned")
    if parsed["identity_kind"] == "sha256" and not re.fullmatch(
        r"[0-9a-f]{64}", parsed["identity_value"]
    ):
        raise CompactionInvocationError("selection SHA-256 must be lowercase hexadecimal")
    processing_time = _aware_time(event.get("processing_timestamp"), "processing_timestamp")
    expected = _optional_text(event.get("expected_current_glue_location"))
    authorized = _optional_text(event.get("authorized_publication_claim_etag"))
    return year, month, parsed, processing_time, expected, authorized


def _read_pinned_selection(storage: ObjectStorage, reference: Mapping[str, str]) -> bytes:
    version = reference["identity_value"] if reference["identity_kind"] == "version_id" else None
    try:
        stored = storage.get(reference["bucket"], reference["key"], version_id=version)
    except ObjectNotFound as error:
        raise CompactionInvocationError("pinned selection is missing") from error
    if reference["identity_kind"] == "sha256":
        observed = sha256_bytes(stored.body)
    elif reference["identity_kind"] == "etag":
        observed = stored.head.etag
    else:
        observed = stored.head.version_id
    if observed != reference["identity_value"]:
        raise CompactionInvocationError("pinned selection identity mismatch")
    return stored.body


def _aware_time(value: object, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise CompactionInvocationError(f"{name} must be ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CompactionInvocationError(f"{name} must include an offset")
    return parsed.astimezone(UTC)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or len(text) > 1024 or "\n" in text or "\r" in text:
        raise CompactionInvocationError("optional invocation field is malformed")
    return text

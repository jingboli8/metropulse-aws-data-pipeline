"""Idempotent orchestration for one daily raw S3 object."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from metropulse.aws.config import LambdaConfig
from metropulse.aws.coordination import ControlCoordinator
from metropulse.aws.errors import (
    CompletionMarkerInvalid,
    ObjectIdentityMismatch,
    ProcessingError,
)
from metropulse.aws.events import S3ObjectCreatedRecord
from metropulse.aws.idempotency import processing_identity
from metropulse.aws.keys import output_keys
from metropulse.aws.observability import StructuredObserver, safe_error
from metropulse.aws.publication import (
    completion_manifest,
    iso_utc,
    json_bytes,
    object_metadata,
    quarantine_bytes,
    sha256_bytes,
)
from metropulse.aws.storage import ConditionalWriteFailed, ObjectHead, ObjectStorage
from metropulse.manifest import MANIFEST_VERSION
from metropulse.models import SourceMetadata
from metropulse.parquet import serialize_daily_parquet
from metropulse.schema import SCHEMA_VERSION
from metropulse.transform import transform_daily_csv


@dataclass(frozen=True)
class ProcessingResult:
    """Deterministic per-record invocation result."""

    record_index: int
    outcome: str
    processing_identity: str
    input_row_count: int
    valid_row_count: int
    quarantine_row_count: int
    staging_key: str
    quarantine_key: str | None
    completion_key: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-ready response mapping."""
        return {
            "completion_key": self.completion_key,
            "input_row_count": self.input_row_count,
            "outcome": self.outcome,
            "processing_identity": self.processing_identity,
            "quarantine_key": self.quarantine_key,
            "quarantine_row_count": self.quarantine_row_count,
            "record_index": self.record_index,
            "staging_key": self.staging_key,
            "valid_row_count": self.valid_row_count,
        }


class ObjectProcessor:
    """Coordinate storage and the existing pure daily transformation core."""

    def __init__(
        self,
        *,
        config: LambdaConfig,
        storage: ObjectStorage,
        observer: StructuredObserver,
        utc_now: Callable[[], datetime] | None = None,
        monotonic_ms: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.observer = observer
        self.utc_now = utc_now or (lambda: datetime.now(UTC))
        self.monotonic_ms = monotonic_ms or (lambda: time.monotonic() * 1000)
        self.control = ControlCoordinator(
            config=config,
            storage=storage,
            utc_now=self.utc_now,
        )

    def process(self, record: S3ObjectCreatedRecord, *, request_id: str | None) -> ProcessingResult:
        """Process one event record, publishing completion last and conditionally."""
        started = self.monotonic_ms()
        common: dict[str, object] = {
            "aws_request_id": request_id,
            "record_index": record.record_index,
            "source_bucket": record.bucket,
            "source_key": record.key,
        }
        self.observer.event(
            "record_received",
            **common,
            event_time=iso_utc(record.event_time),
            object_size=record.object_size,
            sequencer=record.sequencer,
            source_etag=record.etag,
            source_version_id=record.version_id,
        )
        try:
            return self._process(record, request_id=request_id, started=started, common=common)
        except Exception as error:
            duration = max(self.monotonic_ms() - started, 0.0)
            self.observer.event(
                "processing_failed",
                **common,
                duration_ms=duration,
                outcome="failed",
                **safe_error(error),
            )
            self.observer.metrics(
                record.event_time,
                {
                    "ObjectsFailed": 1,
                    "ProcessingDurationMs": duration,
                    "ReconciliationFailures": int("reconcil" in str(error).lower()),
                },
            )
            raise

    def _process(
        self,
        record: S3ObjectCreatedRecord,
        *,
        request_id: str | None,
        started: float,
        common: dict[str, object],
    ) -> ProcessingResult:
        head = self.storage.head(record.bucket, record.key, version_id=record.version_id)
        self._verify_event_identity(record, head, common)
        if head.byte_size > self.config.maximum_input_bytes:
            raise ProcessingError(
                f"Input object is {head.byte_size} bytes; "
                f"limit is {self.config.maximum_input_bytes}"
            )
        input_id, identity_fields = processing_identity(
            source_bucket=record.bucket,
            source_key=record.key,
            version_id=head.version_id or record.version_id,
            checksum_sha256=head.checksum_sha256,
            etag=head.etag or record.etag,
            pipeline_version=self.config.pipeline_version,
            schema_version=SCHEMA_VERSION,
            manifest_version=MANIFEST_VERSION,
        )
        keys = output_keys(record.source_date, input_id, self.config)
        common["processing_identity"] = input_id
        traced = {**common, "processing_identity": input_id}
        existing = self.control.completion(keys, input_id)
        if existing is not None:
            result = self._result_from_manifest(record.record_index, existing, "duplicate_skipped")
            self.observer.event("duplicate_skipped", **traced, outcome=result.outcome)
            self.observer.metrics(record.event_time, {"ObjectsSkippedDuplicate": 1})
            return result

        self.control.acquire_claim(
            keys,
            identity_fields,
            request_id=request_id,
            processing_identity=input_id,
        )
        source = self.storage.get(record.bucket, record.key, version_id=record.version_id)
        self._verify_read_identity(head, source.head)
        if len(source.body) != head.byte_size or len(source.body) > self.config.maximum_input_bytes:
            raise ObjectIdentityMismatch("Object body size does not match verified metadata")
        raw_sha256 = sha256_bytes(source.body)
        if head.checksum_sha256 and raw_sha256 != head.checksum_sha256.lower():
            raise ObjectIdentityMismatch("Object SHA-256 checksum differs from metadata")
        try:
            csv_text = source.body.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ProcessingError("Raw CSV is not valid UTF-8") from error
        processing_timestamp = _as_utc(
            head.last_modified or record.event_time, "processing timestamp"
        )
        source_details = {
            **identity_fields,
            "etag": head.etag or record.etag,
            "raw_sha256": raw_sha256,
            "version_id": head.version_id or record.version_id,
        }
        self.observer.event(
            "processing_started",
            **traced,
            processing_timestamp=iso_utc(processing_timestamp),
        )
        transformed = transform_daily_csv(
            csv_text,
            expected_source_date=record.source_date,
            source_metadata=SourceMetadata(
                source_bucket=record.bucket,
                source_object_key=record.key,
                source_object_version_id=head.version_id or record.version_id,
                source_object_etag=head.etag or record.etag,
                source_object_checksum=f"sha256:{raw_sha256}",
            ),
            processing_timestamp=processing_timestamp,
            pipeline_version=self.config.pipeline_version,
        )
        if transformed.batch_errors or not transformed.reconciliation.matches:
            messages = "; ".join(error.message for error in transformed.batch_errors)
            raise ProcessingError(
                f"Batch validation failed: {messages or 'reconciliation mismatch'}"
            )
        parquet = serialize_daily_parquet(
            transformed.valid_records,
            pipeline_version=self.config.pipeline_version,
            source_identity=source_details,
            source_date=record.source_date,
            raw_sha256=raw_sha256,
            processing_timestamp=iso_utc(processing_timestamp),
        )
        quarantine = quarantine_bytes(transformed)
        staging_sha256 = sha256_bytes(parquet)
        quarantine_sha256 = sha256_bytes(quarantine) if quarantine else None
        self._write_output(
            keys.staging, parquet, "application/vnd.apache.parquet", staging_sha256, input_id
        )
        if quarantine:
            self._write_output(
                keys.quarantine,
                quarantine,
                "application/x-ndjson; charset=utf-8",
                quarantine_sha256,
                input_id,
            )
        manifest = completion_manifest(
            record=record,
            config=self.config,
            identity=input_id,
            identity_fields=source_details,
            keys=keys,
            processing_timestamp=processing_timestamp,
            raw_sha256=raw_sha256,
            staging_sha256=staging_sha256,
            staging_size=len(parquet),
            quarantine_sha256=quarantine_sha256,
            quarantine_size=len(quarantine),
            result=transformed,
        )
        marker_body = json_bytes(manifest)
        marker_metadata = object_metadata(
            pipeline_version=self.config.pipeline_version,
            processing_identity=input_id,
            sha256=sha256_bytes(marker_body),
            schema_version=SCHEMA_VERSION,
        )
        try:
            self.storage.put(
                self.config.destination_bucket,
                keys.completed,
                marker_body,
                content_type="application/json; charset=utf-8",
                content_encoding=None,
                metadata=marker_metadata,
                if_none_match=True,
            )
        except ConditionalWriteFailed as race_error:
            self.observer.event("completion_race_lost", **traced, outcome="race_lost")
            winner = self.control.completion(keys, input_id)
            if winner is None:
                raise CompletionMarkerInvalid(
                    "Completion race winner is absent or inconsistent"
                ) from race_error
            result = self._result_from_manifest(record.record_index, winner, "concurrent_success")
            self.observer.event("completion_race_validated", **traced, outcome=result.outcome)
            self.observer.metrics(record.event_time, {"ObjectsSkippedDuplicate": 1})
            return result
        duration = max(self.monotonic_ms() - started, 0.0)
        completed = self._result_from_manifest(record.record_index, manifest, "processed")
        self.observer.event(
            "processing_completed",
            **traced,
            duration_ms=duration,
            input_row_count=completed.input_row_count,
            outcome=completed.outcome,
            quarantine_row_count=completed.quarantine_row_count,
            valid_row_count=completed.valid_row_count,
        )
        self.observer.metrics(
            processing_timestamp,
            {
                "InputRows": completed.input_row_count,
                "ObjectsProcessed": 1,
                "ProcessingDurationMs": duration,
                "QuarantineRate": (
                    completed.quarantine_row_count / completed.input_row_count * 100
                    if completed.input_row_count
                    else 0.0
                ),
                "QuarantineRows": completed.quarantine_row_count,
                "ReconciliationFailures": 0,
                "ValidRows": completed.valid_row_count,
            },
        )
        return completed

    def _verify_event_identity(
        self,
        record: S3ObjectCreatedRecord,
        head: ObjectHead,
        common: Mapping[str, object],
    ) -> None:
        mismatches = []
        if record.object_size != head.byte_size:
            mismatches.append("size")
        if record.etag and head.etag and record.etag.strip('"') != head.etag.strip('"'):
            mismatches.append("ETag")
        if record.version_id and record.version_id != head.version_id:
            mismatches.append("version ID")
        if mismatches:
            self.observer.event(
                "object_identity_mismatch",
                **common,
                mismatched_fields=sorted(mismatches),
            )
            raise ObjectIdentityMismatch(f"Event/object identity mismatch: {', '.join(mismatches)}")

    @staticmethod
    def _verify_read_identity(expected: ObjectHead, observed: ObjectHead) -> None:
        if (
            expected.byte_size != observed.byte_size
            or expected.etag != observed.etag
            or expected.version_id != observed.version_id
            or expected.checksum_sha256 != observed.checksum_sha256
        ):
            raise ObjectIdentityMismatch("Object changed between metadata and body reads")

    def _write_output(
        self, key: str, body: bytes, content_type: str, sha256: str | None, input_id: str
    ) -> None:
        assert sha256 is not None
        self.storage.put(
            self.config.destination_bucket,
            key,
            body,
            content_type=content_type,
            content_encoding=None,
            metadata=object_metadata(
                pipeline_version=self.config.pipeline_version,
                processing_identity=input_id,
                sha256=sha256,
                schema_version=SCHEMA_VERSION,
            ),
        )

    @staticmethod
    def _result_from_manifest(
        record_index: int, manifest: Mapping[str, Any], outcome: str
    ) -> ProcessingResult:
        quarantine = manifest.get("quarantine")
        return ProcessingResult(
            record_index=record_index,
            outcome=outcome,
            processing_identity=str(manifest["processing_identity"]),
            input_row_count=int(manifest["input_row_count"]),
            valid_row_count=int(manifest["valid_row_count"]),
            quarantine_row_count=int(manifest["quarantine_row_count"]),
            staging_key=str(manifest["staging"]["key"]),
            quarantine_key=str(quarantine["key"]) if isinstance(quarantine, dict) else None,
            completion_key=str(manifest["completion_key"]),
        )


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProcessingError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)

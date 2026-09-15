"""Conditional S3 claim and completion-marker coordination."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from metropulse.aws.config import LambdaConfig
from metropulse.aws.errors import CompletionMarkerInvalid, ConcurrentProcessing, ProcessingError
from metropulse.aws.keys import OutputKeys
from metropulse.aws.publication import iso_utc, json_bytes, object_metadata, sha256_bytes
from metropulse.aws.storage import ConditionalWriteFailed, ObjectNotFound, ObjectStorage
from metropulse.manifest import MANIFEST_VERSION
from metropulse.schema import SCHEMA_VERSION

CLAIM_TTL = timedelta(minutes=16)


class ControlCoordinator:
    """Validate completion and conditionally own one deterministic processing ID."""

    def __init__(
        self,
        *,
        config: LambdaConfig,
        storage: ObjectStorage,
        utc_now: Callable[[], datetime],
    ) -> None:
        self.config = config
        self.storage = storage
        self.utc_now = utc_now

    def completion(self, keys: OutputKeys, input_id: str) -> dict[str, Any] | None:
        """Return a fully verified completion marker, or None when absent."""
        try:
            stored = self.storage.get(self.config.destination_bucket, keys.completed)
        except ObjectNotFound:
            return None
        try:
            marker = json.loads(stored.body.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CompletionMarkerInvalid("Completion marker is not valid UTF-8 JSON") from error
        marker_metadata = stored.head.metadata or {}
        if marker_metadata.get("sha256") != sha256_bytes(stored.body):
            raise CompletionMarkerInvalid("Completion marker checksum metadata is inconsistent")
        if not isinstance(marker, dict) or marker.get("processing_identity") != input_id:
            raise CompletionMarkerInvalid("Completion marker identity is inconsistent")
        expected_values = {
            "completion_key": keys.completed,
            "manifest_version": MANIFEST_VERSION,
            "pipeline_version": self.config.pipeline_version,
            "schema_version": SCHEMA_VERSION,
        }
        if any(marker.get(name) != value for name, value in expected_values.items()):
            raise CompletionMarkerInvalid(
                "Completion marker contract version or key is inconsistent"
            )
        input_rows, valid_rows, quarantine_rows, staging = _counts_and_staging(marker)
        if input_rows != valid_rows + quarantine_rows:
            raise CompletionMarkerInvalid("Completion marker row counts do not reconcile")
        if not _expected_reference(staging, self.config.destination_bucket, keys.staging):
            raise CompletionMarkerInvalid("Completion marker staging key is inconsistent")
        self._verify_reference(staging)
        quarantine = marker.get("quarantine")
        if quarantine_rows > 0:
            if not _expected_reference(quarantine, self.config.destination_bucket, keys.quarantine):
                raise CompletionMarkerInvalid("Completion marker omits quarantine output")
            self._verify_reference(quarantine)
        elif quarantine is not None:
            raise CompletionMarkerInvalid("Zero-quarantine marker references an object")
        _verify_reconciliation(marker, input_rows, valid_rows, quarantine_rows)
        return marker

    def acquire_claim(
        self,
        keys: OutputKeys,
        identity_fields: Mapping[str, object],
        *,
        request_id: str | None,
        processing_identity: str,
    ) -> None:
        """Create a claim or conditionally replace an expired claim."""
        now = _as_utc(self.utc_now(), "claim clock")
        claim = {
            "expires_at": iso_utc(now + CLAIM_TTL),
            "owner_token": request_id or "unavailable",
            "processing_identity": processing_identity,
            "source_identity": dict(identity_fields),
            "started_at": iso_utc(now),
        }
        body = json_bytes(claim)
        metadata = object_metadata(
            pipeline_version=self.config.pipeline_version,
            processing_identity=processing_identity,
            sha256=sha256_bytes(body),
            schema_version=SCHEMA_VERSION,
        )
        try:
            self.storage.put(
                self.config.destination_bucket,
                keys.claim,
                body,
                content_type="application/json; charset=utf-8",
                content_encoding=None,
                metadata=metadata,
                if_none_match=True,
            )
            return
        except ConditionalWriteFailed:
            pass
        try:
            existing = self.storage.get(self.config.destination_bucket, keys.claim)
            existing_claim = json.loads(existing.body.decode("utf-8"))
            expires_at = datetime.fromisoformat(existing_claim["expires_at"].replace("Z", "+00:00"))
        except (ObjectNotFound, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ConcurrentProcessing("Existing processing claim is unreadable") from error
        if expires_at > now:
            raise ConcurrentProcessing("Another invocation owns an active processing claim")
        try:
            self.storage.put(
                self.config.destination_bucket,
                keys.claim,
                body,
                content_type="application/json; charset=utf-8",
                content_encoding=None,
                metadata=metadata,
                if_match=existing.head.etag,
            )
        except ConditionalWriteFailed as error:
            raise ConcurrentProcessing("Another invocation replaced the stale claim") from error

    def _verify_reference(self, reference: Mapping[str, object]) -> None:
        try:
            stored = self.storage.get(str(reference["bucket"]), str(reference["key"]))
            expected_size = int(reference["byte_size"])
            expected_sha256 = str(reference["sha256"])
        except (KeyError, ObjectNotFound, TypeError, ValueError) as error:
            raise CompletionMarkerInvalid("Referenced output is missing or malformed") from error
        metadata = stored.head.metadata or {}
        if (
            len(stored.body) != expected_size
            or sha256_bytes(stored.body) != expected_sha256
            or metadata.get("sha256") != expected_sha256
        ):
            raise CompletionMarkerInvalid("Referenced output checksum or size does not match")


def _counts_and_staging(marker: Mapping[str, Any]) -> tuple[int, int, int, Mapping[str, object]]:
    try:
        input_rows = int(marker["input_row_count"])
        valid_rows = int(marker["valid_row_count"])
        quarantine_rows = int(marker["quarantine_row_count"])
        staging = marker["staging"]
        if not isinstance(staging, dict):
            raise TypeError("staging is not an object")
    except (KeyError, TypeError, ValueError) as error:
        raise CompletionMarkerInvalid("Completion marker row counts are malformed") from error
    return input_rows, valid_rows, quarantine_rows, staging


def _expected_reference(reference: Any, bucket: str, key: str) -> bool:
    return (
        isinstance(reference, dict)
        and reference.get("bucket") == bucket
        and reference.get("key") == key
    )


def _verify_reconciliation(
    marker: Mapping[str, Any], input_rows: int, valid_rows: int, quarantine_rows: int
) -> None:
    reconciliation = marker.get("reconciliation", {})
    expected = {
        "accounted_row_count": valid_rows + quarantine_rows,
        "applicable": True,
        "input_row_count": input_rows,
        "matches": True,
        "quarantine_row_count": quarantine_rows,
        "valid_row_count": valid_rows,
    }
    if not isinstance(reconciliation, dict) or any(
        reconciliation.get(name) != value for name, value in expected.items()
    ):
        raise CompletionMarkerInvalid("Completion marker reconciliation is invalid")


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProcessingError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)

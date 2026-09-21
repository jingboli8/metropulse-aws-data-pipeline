"""Marker-driven S3 compaction orchestration and strict publication claims."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from metropulse.aws.storage import ConditionalWriteFailed, ObjectNotFound, ObjectStorage
from metropulse.compaction_identity import canonical_json
from metropulse.compaction_manifest import (
    build_selection_document,
    compaction_keys,
    glue_partition_location,
)
from metropulse.compaction_models import (
    CompactionBounds,
    ImmutableOutputConflict,
    MonthlySelection,
    PartitionPublisher,
    PublicationConflict,
    SelectedDailyInput,
)
from metropulse.curated_parquet import sha256_bytes
from metropulse.monthly_compaction import compact_month


class S3MonthlyCompactionProcessor:
    """Compact exact pinned markers without listing S3, then publish through Glue."""

    def __init__(
        self,
        storage: ObjectStorage,
        publisher: PartitionPublisher,
        *,
        bounds: CompactionBounds | None = None,
    ) -> None:
        self.storage = storage
        self.publisher = publisher
        self.bounds = bounds

    def process(
        self,
        selection: MonthlySelection,
        *,
        destination_bucket: str,
        owner_token: str,
        processing_timestamp: datetime,
        expected_current_location: str | None = None,
        authorized_publication_claim_etag: str | None = None,
    ) -> dict[str, Any]:
        """Validate, construct, complete, and expose one immutable monthly run."""
        if processing_timestamp.tzinfo is None or processing_timestamp.utcoffset() is None:
            raise ValueError("processing_timestamp must be timezone-aware")
        if not owner_token.strip():
            raise ValueError("owner_token must not be empty")
        for item in selection.inputs:
            self._validate_marker(item)
        result = compact_month(selection, self._read_staging, bounds=self.bounds)
        keys = compaction_keys(selection.year, selection.month, result.run_id)
        selection_body = canonical_json(build_selection_document(selection, result.run_id)) + b"\n"
        completion_body = canonical_json(result.completion) + b"\n"
        location = glue_partition_location(
            destination_bucket, selection.year, selection.month, result.run_id
        )
        prior_receipt = self._read_receipt(destination_bucket, keys["published"])
        if prior_receipt is not None:
            self._verify_completed_publication(
                destination_bucket,
                keys,
                selection_body=selection_body,
                curated_body=result.parquet_bytes,
                completion_body=completion_body,
                receipt=prior_receipt,
                run_id=result.run_id,
                location=location,
            )
            publication = self.publisher.publish(
                year=f"{selection.year:04d}",
                month=f"{selection.month:02d}",
                location=location,
                expected_current_location=location,
            )
            return self._result(
                result.run_id,
                location,
                publication,
                owner_token,
                processing_timestamp,
                newly_completed=False,
                newly_published=False,
            )

        construction_claim = (
            canonical_json(
                {
                    "contract": "metropulse-compaction-construction-claim-v1",
                    "owner_token": owner_token,
                    "run_id": result.run_id,
                }
            )
            + b"\n"
        )
        self._acquire_owned_claim(
            destination_bucket, keys["claim"], construction_claim, "construction"
        )
        self._verify_claim(destination_bucket, keys["claim"], construction_claim)
        self._put_immutable(
            destination_bucket, keys["selection"], selection_body, "application/json"
        )
        self._put_immutable(
            destination_bucket,
            keys["curated"],
            result.parquet_bytes,
            "application/vnd.apache.parquet",
        )
        newly_completed = self._put_immutable(
            destination_bucket, keys["completed"], completion_body, "application/json"
        )
        claim = {
            "contract": "metropulse-compaction-publication-claim-v1",
            "owner_token": owner_token,
            "run_id": result.run_id,
        }
        claim_body = canonical_json(claim) + b"\n"
        self._acquire_publication_claim(
            destination_bucket,
            keys["publication_claim"],
            claim_body,
            authorized_previous_etag=authorized_publication_claim_etag,
        )
        self._verify_claim(destination_bucket, keys["publication_claim"], claim_body)
        publication = self.publisher.publish(
            year=f"{selection.year:04d}",
            month=f"{selection.month:02d}",
            location=location,
            expected_current_location=expected_current_location,
        )
        self._verify_claim(destination_bucket, keys["publication_claim"], claim_body)
        receipt = {
            "contract": "metropulse-compaction-publication-receipt-v1",
            "location": location,
            "owner_token": owner_token,
            "outcome": publication,
            "processing_timestamp": processing_timestamp.isoformat(),
            "run_id": result.run_id,
        }
        newly_published = self._put_immutable(
            destination_bucket,
            keys["published"],
            canonical_json(receipt) + b"\n",
            "application/json",
        )
        return self._result(
            result.run_id,
            location,
            publication,
            owner_token,
            processing_timestamp,
            newly_completed=newly_completed,
            newly_published=newly_published,
        )

    def _validate_marker(self, item: SelectedDailyInput) -> None:
        version = (
            item.completion_marker_identity_value
            if item.completion_marker_identity_kind == "version_id"
            else None
        )
        stored = self.storage.get(
            item.completion_marker_bucket, item.completion_marker_key, version_id=version
        )
        if item.completion_marker_identity_kind == "sha256":
            observed = sha256_bytes(stored.body)
        elif item.completion_marker_identity_kind == "etag":
            observed = stored.head.etag
        else:
            observed = stored.head.version_id
        expected_identity = (
            item.completion_marker_identity_value.lower()
            if item.completion_marker_identity_kind == "sha256"
            else item.completion_marker_identity_value
        )
        if observed != expected_identity:
            raise ImmutableOutputConflict("pinned completion marker identity mismatch")
        marker = json.loads(stored.body.decode("utf-8"))
        staging = marker.get("staging", {})
        reconciliation = marker.get("reconciliation", {})
        expected = {
            "processing_identity": item.processing_identity,
            "source_date": item.source_date.isoformat(),
            "input_row_count": item.input_row_count,
            "valid_row_count": item.valid_row_count,
            "quarantine_row_count": item.quarantine_row_count,
            "pipeline_version": item.pipeline_version,
            "schema_version": item.schema_version,
            "manifest_version": item.manifest_version,
            "first_valid_event_timestamp_local": (
                item.first_valid_timestamp.isoformat() if item.first_valid_timestamp else None
            ),
            "last_valid_event_timestamp_local": (
                item.last_valid_timestamp.isoformat() if item.last_valid_timestamp else None
            ),
            "valid_timestamp_count": item.valid_row_count,
        }
        if any(marker.get(key) != value for key, value in expected.items()) or any(
            staging.get(key) != value
            for key, value in {
                "bucket": item.staging_bucket,
                "key": item.staging_key,
                "sha256": item.staging_sha256,
                "byte_size": item.staging_byte_size,
            }.items()
        ):
            raise ImmutableOutputConflict("completion marker conflicts with selected evidence")
        expected_reconciliation = {
            "accounted_row_count": item.valid_row_count + item.quarantine_row_count,
            "applicable": True,
            "input_row_count": item.input_row_count,
            "matches": True,
            "quarantine_row_count": item.quarantine_row_count,
            "valid_row_count": item.valid_row_count,
        }
        if reconciliation != expected_reconciliation:
            raise ImmutableOutputConflict("completion marker reconciliation is invalid")
        if (
            not marker.get("source_bucket")
            or not marker.get("raw_key")
            or not marker.get("raw_sha256")
        ):
            raise ImmutableOutputConflict("completion marker lacks raw source evidence")
        quarantine = marker.get("quarantine")
        if item.quarantine_row_count:
            if not isinstance(quarantine, dict):
                raise ImmutableOutputConflict("completion marker lacks quarantine evidence")
            self._validate_reference(quarantine)
        elif quarantine is not None:
            raise ImmutableOutputConflict("zero-quarantine completion references an object")

    def _read_staging(self, item: SelectedDailyInput) -> bytes:
        return self.storage.get(item.staging_bucket, item.staging_key).body

    def _validate_reference(self, reference: dict[str, Any]) -> None:
        try:
            stored = self.storage.get(str(reference["bucket"]), str(reference["key"]))
            size = int(reference["byte_size"])
            checksum = str(reference["sha256"])
        except (KeyError, ObjectNotFound, TypeError, ValueError) as error:
            raise ImmutableOutputConflict("completion reference is missing or malformed") from error
        if len(stored.body) != size or sha256_bytes(stored.body) != checksum:
            raise ImmutableOutputConflict("completion reference checksum or size mismatch")

    def _put_immutable(self, bucket: str, key: str, body: bytes, content_type: str) -> bool:
        checksum = sha256_bytes(body)
        try:
            self.storage.put(
                bucket,
                key,
                body,
                content_type=content_type,
                content_encoding=None,
                metadata={"sha256": checksum},
                if_none_match=True,
            )
            return True
        except ConditionalWriteFailed:
            existing = self.storage.get(bucket, key)
            if len(existing.body) != len(body) or sha256_bytes(existing.body) != checksum:
                raise ImmutableOutputConflict(f"immutable S3 object conflicts: {key}") from None
            return False

    def _acquire_owned_claim(self, bucket: str, key: str, body: bytes, claim_kind: str) -> None:
        try:
            self.storage.put(
                bucket,
                key,
                body,
                content_type="application/json",
                content_encoding=None,
                metadata={"sha256": sha256_bytes(body)},
                if_none_match=True,
            )
        except ConditionalWriteFailed:
            existing = self.storage.get(bucket, key)
            if existing.body != body:
                raise PublicationConflict(
                    f"{claim_kind} claim is owned by another invocation"
                ) from None

    def _acquire_publication_claim(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        authorized_previous_etag: str | None,
    ) -> None:
        try:
            self.storage.put(
                bucket,
                key,
                body,
                content_type="application/json",
                content_encoding=None,
                metadata={"sha256": sha256_bytes(body)},
                if_none_match=True,
            )
        except ConditionalWriteFailed:
            existing = self.storage.get(bucket, key)
            if existing.body == body:
                return
            if authorized_previous_etag is None or existing.head.etag != authorized_previous_etag:
                raise PublicationConflict(
                    "month publication claim is owned; stale claims require operator reconciliation"
                ) from None
            try:
                self.storage.put(
                    bucket,
                    key,
                    body,
                    content_type="application/json",
                    content_encoding=None,
                    metadata={"sha256": sha256_bytes(body)},
                    if_match=authorized_previous_etag,
                )
            except ConditionalWriteFailed as error:
                raise PublicationConflict(
                    "publication claim changed after operator authorization"
                ) from error

    def _verify_claim(self, bucket: str, key: str, body: bytes) -> None:
        try:
            observed = self.storage.get(bucket, key).body
        except ObjectNotFound as error:
            raise PublicationConflict("publication claim disappeared") from error
        if observed != body:
            raise PublicationConflict("publication claim ownership changed")

    def _read_receipt(self, bucket: str, key: str) -> dict[str, Any] | None:
        try:
            value = json.loads(self.storage.get(bucket, key).body.decode("utf-8", errors="strict"))
        except ObjectNotFound:
            return None
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ImmutableOutputConflict("publication receipt is malformed") from error
        if not isinstance(value, dict):
            raise ImmutableOutputConflict("publication receipt is malformed")
        return value

    def _verify_completed_publication(
        self,
        bucket: str,
        keys: dict[str, str],
        *,
        selection_body: bytes,
        curated_body: bytes,
        completion_body: bytes,
        receipt: dict[str, Any],
        run_id: str,
        location: str,
    ) -> None:
        if (
            receipt.get("contract") != "metropulse-compaction-publication-receipt-v1"
            or receipt.get("location") != location
            or receipt.get("run_id") != run_id
            or receipt.get("outcome") not in {"created", "updated", "no_op"}
            or not isinstance(receipt.get("owner_token"), str)
            or not receipt["owner_token"].strip()
        ):
            raise ImmutableOutputConflict("immutable publication receipt conflicts")
        try:
            timestamp = datetime.fromisoformat(str(receipt["processing_timestamp"]))
        except (KeyError, ValueError) as error:
            raise ImmutableOutputConflict("immutable publication receipt conflicts") from error
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ImmutableOutputConflict("immutable publication receipt conflicts")
        for key, body in (
            (keys["selection"], selection_body),
            (keys["curated"], curated_body),
            (keys["completed"], completion_body),
        ):
            try:
                existing = self.storage.get(bucket, key)
            except ObjectNotFound as error:
                raise ImmutableOutputConflict(
                    "completed publication references missing immutable output"
                ) from error
            if existing.body != body:
                raise ImmutableOutputConflict(
                    "completed publication references conflicting immutable output"
                )

    @staticmethod
    def _result(
        run_id: str,
        location: str,
        publication: str,
        owner_token: str,
        processing_timestamp: datetime,
        *,
        newly_completed: bool,
        newly_published: bool,
    ) -> dict[str, Any]:
        return {
            "location": location,
            "newly_completed": newly_completed,
            "newly_published": newly_published,
            "outcome": publication,
            "owner_token": owner_token,
            "processing_timestamp": processing_timestamp.isoformat(),
            "run_id": run_id,
            "status": "published" if newly_published else "verified_no_op",
        }

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from metropulse.aws.storage import ObjectHead
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_models import ImmutableOutputConflict, PublicationConflict
from metropulse.curated_parquet import sha256_bytes
from metropulse.selection_approval import (
    parse_selection_document,
    publish_approved_selection,
    selection_document_bytes,
)


def _seed_markers(storage: InMemoryObjectStorage, selection) -> None:
    for item in selection.inputs:
        marker = {
            "first_valid_event_timestamp_local": item.first_valid_timestamp.isoformat(),
            "input_row_count": item.input_row_count,
            "last_valid_event_timestamp_local": item.last_valid_timestamp.isoformat(),
            "manifest_version": item.manifest_version,
            "pipeline_version": item.pipeline_version,
            "processing_identity": item.processing_identity,
            "quarantine_row_count": item.quarantine_row_count,
            "quarantine": None,
            "raw_key": f"raw/source=metropt3/source_date={item.source_date.isoformat()}/data.csv",
            "raw_sha256": "f" * 64,
            "reconciliation": {
                "accounted_row_count": item.valid_row_count + item.quarantine_row_count,
                "applicable": True,
                "input_row_count": item.input_row_count,
                "matches": True,
                "quarantine_row_count": item.quarantine_row_count,
                "valid_row_count": item.valid_row_count,
            },
            "schema_version": item.schema_version,
            "source_bucket": "source",
            "source_date": item.source_date.isoformat(),
            "staging": {
                "bucket": item.staging_bucket,
                "byte_size": item.staging_byte_size,
                "key": item.staging_key,
                "sha256": item.staging_sha256,
            },
            "valid_row_count": item.valid_row_count,
            "valid_timestamp_count": item.valid_row_count,
        }
        body = json.dumps(marker, sort_keys=True, separators=(",", ":")).encode()
        storage.seed(item.completion_marker_bucket, item.completion_marker_key, body)
    updated = tuple(
        replace(
            item,
            completion_marker_identity_value=sha256_bytes(
                storage.get(item.completion_marker_bucket, item.completion_marker_key).body
            ),
        )
        for item in selection.inputs
    )
    return replace(selection, inputs=updated)


def test_selection_approval_is_canonical_pinned_and_idempotent(compact_fixture) -> None:
    selection, _ = compact_fixture()
    storage = InMemoryObjectStorage()
    selection = _seed_markers(storage, selection)
    first = publish_approved_selection(selection, bucket="lake", storage=storage)
    second = publish_approved_selection(selection, bucket="lake", storage=storage)
    assert first.created is True
    assert second.created is False
    assert first.identity_kind == second.identity_kind == "sha256"
    assert first.identity_value == second.identity_value
    run_id, parsed = parse_selection_document(storage.get("lake", first.key).body)
    assert run_id == first.run_id
    assert parsed == selection


def test_selection_approval_rejects_marker_or_immutable_content_conflict(compact_fixture) -> None:
    selection, _ = compact_fixture()
    storage = InMemoryObjectStorage()
    selection = _seed_markers(storage, selection)
    run_id, key, _ = selection_document_bytes(selection)
    storage.seed("lake", key, b"conflict")
    with pytest.raises(ImmutableOutputConflict):
        publish_approved_selection(selection, bucket="lake", storage=storage)
    assert run_id in key


def test_same_run_concurrent_invocations_have_distinct_claim_owners(compact_fixture) -> None:
    from metropulse.aws.compaction_processor import S3MonthlyCompactionProcessor
    from metropulse.aws.compaction_testing import FakePartitionPublisher
    from metropulse.compaction_identity import monthly_run_id
    from metropulse.compaction_manifest import compaction_keys

    selection, payloads = compact_fixture()
    storage = InMemoryObjectStorage()
    selection = _seed_markers(storage, selection)
    for key, body in payloads.items():
        storage.seed("bucket", key, body)
    run_id = monthly_run_id(selection)
    claim_key = compaction_keys(2020, 2, run_id)["claim"]
    owner_a = (
        b'{"contract":"metropulse-compaction-construction-claim-v1",'
        b'"owner_token":"compaction:request-a","run_id":"' + run_id.encode() + b'"}\n'
    )
    storage.seed("lake", claim_key, owner_a)
    processor = S3MonthlyCompactionProcessor(storage, FakePartitionPublisher())
    with pytest.raises(PublicationConflict, match="another invocation"):
        processor.process(
            selection,
            destination_bucket="lake",
            owner_token="compaction:request-b",
            processing_timestamp=__import__("datetime").datetime.fromisoformat(
                "2026-09-18T00:00:00+00:00"
            ),
        )
    assert storage.get("lake", claim_key).body == owner_a


class _VersionedStorage(InMemoryObjectStorage):
    def put(self, *args, **kwargs):
        head = super().put(*args, **kwargs)
        bucket, key = args[0], args[1]
        entry = self.objects[(bucket, key)]
        entry.head = ObjectHead(
            byte_size=head.byte_size,
            etag=head.etag,
            version_id="version-approved",
            metadata=head.metadata,
        )
        return entry.head


def test_selection_publication_returns_version_id_when_available(compact_fixture) -> None:
    selection, _ = compact_fixture()
    storage = _VersionedStorage()
    selection = _seed_markers(storage, selection)
    approved = publish_approved_selection(selection, bucket="lake", storage=storage)
    assert approved.identity_kind == "version_id"
    assert approved.identity_value == "version-approved"

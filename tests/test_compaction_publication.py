from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from metropulse.aws.catalog import GluePartitionPublisher
from metropulse.aws.compaction_processor import S3MonthlyCompactionProcessor
from metropulse.aws.compaction_testing import FakePartitionPublisher
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_manifest import compaction_keys, glue_partition_location
from metropulse.compaction_models import PublicationConflict


def _seed_selection(compact_fixture):
    selection, payloads = compact_fixture()
    storage = InMemoryObjectStorage()
    items = []
    for item in selection.inputs:
        storage.seed(item.staging_bucket, item.staging_key, payloads[item.staging_key])
        marker = {
            "input_row_count": item.input_row_count,
            "first_valid_event_timestamp_local": item.first_valid_timestamp.isoformat(),
            "last_valid_event_timestamp_local": item.last_valid_timestamp.isoformat(),
            "manifest_version": item.manifest_version,
            "pipeline_version": item.pipeline_version,
            "processing_identity": item.processing_identity,
            "quarantine": None,
            "quarantine_row_count": item.quarantine_row_count,
            "raw_key": f"raw/source=metropt3/source_date={item.source_date}/data.csv",
            "raw_sha256": "a" * 64,
            "reconciliation": {
                "accounted_row_count": item.valid_row_count + item.quarantine_row_count,
                "applicable": True,
                "input_row_count": item.input_row_count,
                "matches": True,
                "quarantine_row_count": item.quarantine_row_count,
                "valid_row_count": item.valid_row_count,
            },
            "schema_version": item.schema_version,
            "source_bucket": "raw-bucket",
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
        items.append(
            replace(
                item,
                completion_marker_identity_value=hashlib.sha256(body).hexdigest(),
            )
        )
    return replace(selection, inputs=tuple(items)), storage


def test_fake_publisher_create_noop_replace_and_preserve_on_failure() -> None:
    publisher = FakePartitionPublisher()
    old = "s3://bucket/curated/metropt3/year=2020/month=02/run_id=old/"
    new = "s3://bucket/curated/metropt3/year=2020/month=02/run_id=new/"
    assert (
        publisher.publish(year="2020", month="02", location=old, expected_current_location=None)
        == "created"
    )
    assert (
        publisher.publish(year="2020", month="02", location=old, expected_current_location=None)
        == "no_op"
    )
    publisher.fail_before_mutation = True
    with pytest.raises(RuntimeError):
        publisher.publish(year="2020", month="02", location=new, expected_current_location=old)
    assert publisher.locations[("2020", "02")] == old
    publisher.fail_before_mutation = False
    with pytest.raises(PublicationConflict):
        publisher.publish(year="2020", month="02", location=new, expected_current_location="wrong")
    assert (
        publisher.publish(year="2020", month="02", location=new, expected_current_location=old)
        == "updated"
    )


def test_fake_s3_glue_end_to_end_and_idempotent_resume(compact_fixture) -> None:
    selection, storage = _seed_selection(compact_fixture)
    publisher = FakePartitionPublisher()
    processor = S3MonthlyCompactionProcessor(storage, publisher)
    timestamp = datetime(2026, 9, 16, tzinfo=UTC)
    first = processor.process(
        selection,
        destination_bucket="lake",
        claim_owner="test-owner",
        processing_timestamp=timestamp,
    )
    assert first["outcome"] == "created"
    assert first["location"].endswith("/")
    second = processor.process(
        selection,
        destination_bucket="lake",
        claim_owner="retry-owner",
        processing_timestamp=datetime(2026, 9, 17, tzinfo=UTC),
    )
    assert second["outcome"] == "no_op"
    run_id = first["run_id"]
    keys = compaction_keys(2020, 2, run_id)
    assert storage.get("lake", keys["completed"]).body
    assert publisher.locations[("2020", "02")] == glue_partition_location("lake", 2020, 2, run_id)


def test_publication_claim_is_never_automatically_taken_over(compact_fixture) -> None:
    selection, storage = _seed_selection(compact_fixture)
    publisher = FakePartitionPublisher()
    processor = S3MonthlyCompactionProcessor(storage, publisher)
    result = __import__("metropulse.monthly_compaction", fromlist=["compact_month"]).compact_month(
        selection, lambda item: storage.get(item.staging_bucket, item.staging_key).body
    )
    claim_key = compaction_keys(2020, 2, result.run_id)["publication_claim"]
    storage.seed("lake", claim_key, b'{"claim_owner":"former"}\n')
    with pytest.raises(PublicationConflict, match="operator reconciliation"):
        processor.process(
            selection,
            destination_bucket="lake",
            claim_owner="new",
            processing_timestamp=datetime(2026, 9, 16, tzinfo=UTC),
        )
    assert publisher.locations == {}


def test_publication_claim_replacement_requires_exact_operator_authorized_etag(
    compact_fixture,
) -> None:
    selection, storage = _seed_selection(compact_fixture)
    publisher = FakePartitionPublisher()
    processor = S3MonthlyCompactionProcessor(storage, publisher)
    result = __import__("metropulse.monthly_compaction", fromlist=["compact_month"]).compact_month(
        selection, lambda item: storage.get(item.staging_bucket, item.staging_key).body
    )
    claim_key = compaction_keys(2020, 2, result.run_id)["publication_claim"]
    head = storage.seed("lake", claim_key, b'{"claim_owner":"reviewed-former"}\n')
    receipt = processor.process(
        selection,
        destination_bucket="lake",
        claim_owner="authorized-new",
        processing_timestamp=datetime(2026, 9, 16, tzinfo=UTC),
        authorized_publication_claim_etag=head.etag,
    )
    assert receipt["outcome"] == "created"


class _NotFound(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "EntityNotFoundException"}}


class _FakeGlue:
    def __init__(self) -> None:
        from metropulse.schema import FIELD_DEFINITIONS

        self.table = {
            "StorageDescriptor": {
                "Columns": [
                    {"Name": field.normalized_name, "Type": field.athena_type}
                    for field in FIELD_DEFINITIONS
                ],
                "InputFormat": "parquet-input",
            },
            "PartitionKeys": [
                {"Name": "year", "Type": "string"},
                {"Name": "month", "Type": "string"},
            ],
        }
        self.partition = None
        self.raise_after_create = False
        self.raise_after_update = False

    def get_table(self, **_kwargs):
        return {"Table": self.table}

    def get_partition(self, **_kwargs):
        if self.partition is None:
            raise _NotFound()
        return {"Partition": self.partition}

    def create_partition(self, **kwargs):
        self.partition = kwargs["PartitionInput"]
        if self.raise_after_create:
            raise RuntimeError("ambiguous create response")

    def update_partition(self, **kwargs):
        self.partition = kwargs["PartitionInput"]
        if self.raise_after_update:
            raise RuntimeError("ambiguous update response")


def test_injected_glue_adapter_create_noop_guarded_update_and_readback() -> None:
    client = _FakeGlue()
    publisher = GluePartitionPublisher(client, database="metropulse_dev", table="metropt3_curated")
    old = "s3://lake/curated/metropt3/year=2020/month=02/run_id=old/"
    new = "s3://lake/curated/metropt3/year=2020/month=02/run_id=new/"
    assert (
        publisher.publish(year="2020", month="02", location=old, expected_current_location=None)
        == "created"
    )
    assert (
        publisher.publish(year="2020", month="02", location=old, expected_current_location=None)
        == "no_op"
    )
    with pytest.raises(PublicationConflict):
        publisher.publish(year="2020", month="02", location=new, expected_current_location="wrong")
    assert (
        publisher.publish(year="2020", month="02", location=new, expected_current_location=old)
        == "updated"
    )
    assert client.partition["Values"] == ["2020", "02"]
    assert client.partition["StorageDescriptor"]["Location"] == new


def test_glue_ambiguous_create_is_accepted_only_after_exact_readback() -> None:
    client = _FakeGlue()
    client.raise_after_create = True
    publisher = GluePartitionPublisher(client, database="metropulse_dev", table="metropt3_curated")
    location = "s3://lake/curated/metropt3/year=2020/month=02/run_id=run/"
    assert (
        publisher.publish(
            year="2020", month="02", location=location, expected_current_location=None
        )
        == "created"
    )


def test_glue_ambiguous_update_is_accepted_only_after_exact_readback() -> None:
    client = _FakeGlue()
    publisher = GluePartitionPublisher(client, database="metropulse_dev", table="metropt3_curated")
    old = "s3://lake/curated/metropt3/year=2020/month=02/run_id=old/"
    new = "s3://lake/curated/metropt3/year=2020/month=02/run_id=new/"
    publisher.publish(year="2020", month="02", location=old, expected_current_location=None)
    client.raise_after_update = True
    assert (
        publisher.publish(year="2020", month="02", location=new, expected_current_location=old)
        == "updated"
    )


def test_missing_or_corrupt_selected_marker_fails_before_publication(compact_fixture) -> None:
    selection, storage = _seed_selection(compact_fixture)
    publisher = FakePartitionPublisher()
    storage.remove(
        selection.inputs[0].completion_marker_bucket,
        selection.inputs[0].completion_marker_key,
    )
    with pytest.raises(FileNotFoundError):
        S3MonthlyCompactionProcessor(storage, publisher).process(
            selection,
            destination_bucket="lake",
            claim_owner="test",
            processing_timestamp=datetime(2026, 9, 16, tzinfo=UTC),
        )
    assert publisher.locations == {}


def test_existing_conflicting_immutable_output_fails(compact_fixture) -> None:
    selection, storage = _seed_selection(compact_fixture)
    publisher = FakePartitionPublisher()
    result = __import__("metropulse.monthly_compaction", fromlist=["compact_month"]).compact_month(
        selection, lambda item: storage.get(item.staging_bucket, item.staging_key).body
    )
    key = compaction_keys(2020, 2, result.run_id)["curated"]
    storage.seed("lake", key, b"conflict")
    with pytest.raises(RuntimeError, match="immutable S3 object conflicts"):
        S3MonthlyCompactionProcessor(storage, publisher).process(
            selection,
            destination_bucket="lake",
            claim_owner="test",
            processing_timestamp=datetime(2026, 9, 16, tzinfo=UTC),
        )
    assert publisher.locations == {}


def test_completion_conditional_race_accepts_identical_winner(compact_fixture) -> None:
    selection, storage = _seed_selection(compact_fixture)
    result = __import__("metropulse.monthly_compaction", fromlist=["compact_month"]).compact_month(
        selection, lambda item: storage.get(item.staging_bucket, item.staging_key).body
    )
    key = compaction_keys(2020, 2, result.run_id)["completed"]
    storage.conditional_races[("lake", key)] = lambda attempted: attempted
    publisher = FakePartitionPublisher()
    receipt = S3MonthlyCompactionProcessor(storage, publisher).process(
        selection,
        destination_bucket="lake",
        claim_owner="test",
        processing_timestamp=datetime(2026, 9, 16, tzinfo=UTC),
    )
    assert receipt["outcome"] == "created"

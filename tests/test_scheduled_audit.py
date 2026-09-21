from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime

import pytest

from metropulse.audit_inventory import AUDIT_INVENTORY_VERSION
from metropulse.audit_models import (
    AuditBounds,
    AuditInventory,
    AuditMonth,
    CuratedObject,
    PinnedObject,
)
from metropulse.aws.audit_processor import ScheduledAuditProcessor
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_identity import canonical_json
from metropulse.compaction_manifest import (
    COMPACTION_MANIFEST_VERSION,
    build_selection_document,
    compaction_keys,
    glue_partition_location,
)
from metropulse.curated_parquet import sha256_bytes
from metropulse.monthly_compaction import compact_month
from metropulse.scheduled_audit import audit_curated_inventory
from metropulse.schema import SCHEMA_VERSION


class _Catalog:
    def __init__(self, values: tuple[str, str], location: str) -> None:
        self.values = values
        self.location = location

    def get_partition(self, *, year: str, month: str):
        return {
            "Values": list(self.values),
            "StorageDescriptor": {"Location": self.location},
        }


def _seed_month(compact_fixture):
    selection, payloads = compact_fixture()
    result = compact_month(selection, lambda item: payloads[item.staging_key])
    keys = compaction_keys(2020, 2, result.run_id)
    location = glue_partition_location("lake", 2020, 2, result.run_id)
    selection_body = canonical_json(build_selection_document(selection, result.run_id)) + b"\n"
    completion_body = canonical_json(result.completion) + b"\n"
    publication_body = (
        canonical_json(
            {
                "contract": "metropulse-compaction-publication-receipt-v1",
                "location": location,
                "owner_token": "compaction:fixture",
                "outcome": "created",
                "processing_timestamp": "2026-09-18T00:00:00+00:00",
                "run_id": result.run_id,
            }
        )
        + b"\n"
    )
    storage = InMemoryObjectStorage()
    for key, body in (
        (keys["selection"], selection_body),
        (keys["completed"], completion_body),
        (keys["published"], publication_body),
        (keys["curated"], result.parquet_bytes),
    ):
        storage.seed("lake", key, body)
    month = AuditMonth(
        year=2020,
        month=2,
        run_id=result.run_id,
        selection=PinnedObject("lake", keys["selection"], "sha256", sha256_bytes(selection_body)),
        completion=PinnedObject("lake", keys["completed"], "sha256", sha256_bytes(completion_body)),
        publication=PinnedObject(
            "lake", keys["published"], "sha256", sha256_bytes(publication_body)
        ),
        curated=CuratedObject(
            "lake", keys["curated"], result.parquet_sha256, result.parquet_byte_size
        ),
        glue_values=("2020", "02"),
        glue_location=location,
        selected_row_count=result.selected_row_count,
        curated_row_count=result.curated_row_count,
        quarantine_row_count=result.quarantine_row_count,
        first_timestamp=result.first_timestamp,
        last_timestamp=result.last_timestamp,
        within_month_gap_count=result.significant_gap_count,
        coverage_status="known_missing_dates",
        known_missing_dates=result.missing_dates,
        terminal_partial_month=False,
    )
    inventory = AuditInventory(
        source_name="metropt3",
        known_source_start=datetime(2020, 2, 1),
        known_source_end=datetime(2020, 2, 28, 23, 59, 50),
        expected_months=("2020-02",),
        months=(month,),
        expected_total_curated_rows=result.curated_row_count,
        schema_version=SCHEMA_VERSION,
        compaction_manifest_version=COMPACTION_MANIFEST_VERSION,
        inventory_version=AUDIT_INVENTORY_VERSION,
    )
    return storage, inventory, _Catalog(("2020", "02"), location)


def test_full_checksum_audit_validates_rows_parquet_glue_and_quality(compact_fixture) -> None:
    storage, inventory, catalog = _seed_month(compact_fixture)
    result = audit_curated_inventory(inventory, storage=storage, catalog=catalog)
    assert result.status == "passed"
    assert result.total_curated_rows == 2
    assert result.within_month_gap_count == 1
    assert result.cross_month_gap_count == 0
    assert result.global_gap_count == 1
    assert any(item["kind"] == "known_missing_dates" for item in result.quality_observations)


def test_publication_drift_and_completion_row_mismatch_are_separate(compact_fixture) -> None:
    storage, inventory, _ = _seed_month(compact_fixture)
    month = inventory.months[0]
    completion = json.loads(storage.get("lake", month.completion.key).body)
    completion["selected_row_count"] = 99
    body = canonical_json(completion) + b"\n"
    storage.seed("lake", month.completion.key, body)
    changed = replace(
        month,
        completion=replace(month.completion, identity_value=sha256_bytes(body)),
    )
    inventory = replace(inventory, months=(changed,))
    catalog = _Catalog(("2020", "02"), "s3://lake/wrong/")
    result = audit_curated_inventory(inventory, storage=storage, catalog=catalog)
    assert result.status == "failed"
    assert result.reconciliation_failures
    assert result.publication_drift
    assert not result.fatal_failures


def test_missing_or_corrupt_referenced_object_is_fatal(compact_fixture) -> None:
    storage, inventory, catalog = _seed_month(compact_fixture)
    storage.remove("lake", inventory.months[0].curated.key)
    result = audit_curated_inventory(inventory, storage=storage, catalog=catalog)
    assert result.status == "failed"
    assert "missing" in result.fatal_failures[0]


def test_malformed_publication_receipt_is_fatal(compact_fixture) -> None:
    storage, inventory, catalog = _seed_month(compact_fixture)
    reference = inventory.months[0].publication
    malformed = canonical_json(
        {"location": inventory.months[0].glue_location, "run_id": inventory.months[0].run_id}
    )
    storage.seed(reference.bucket, reference.key, malformed)
    changed = replace(reference, identity_value=sha256_bytes(malformed))
    inventory = replace(inventory, months=(replace(inventory.months[0], publication=changed),))
    result = audit_curated_inventory(inventory, storage=storage, catalog=catalog)
    assert result.status == "failed"
    assert "publication receipt conflicts" in result.fatal_failures[0]


def test_audit_bounds_are_enforced(compact_fixture) -> None:
    storage, inventory, catalog = _seed_month(compact_fixture)
    result = audit_curated_inventory(
        inventory,
        storage=storage,
        catalog=catalog,
        bounds=AuditBounds(max_months=1, max_object_bytes=1, max_total_bytes=1, max_total_rows=1),
    )
    assert result.status == "failed"
    assert len(result.fatal_failures) >= 3


def test_pinned_inventory_processor_rejects_identity_corruption(compact_fixture) -> None:
    from metropulse.audit_inventory import inventory_document_bytes

    storage, inventory, catalog = _seed_month(compact_fixture)
    identity, key, body = inventory_document_bytes(inventory)
    storage.seed("lake", key, body)
    processor = ScheduledAuditProcessor(storage=storage, catalog=catalog, bounds=AuditBounds())
    reference = PinnedObject("lake", key, "sha256", sha256_bytes(body))
    assert processor.process(reference).inventory_id == identity
    with pytest.raises(Exception, match="identity mismatch"):
        processor.process(replace(reference, identity_value="f" * 64))

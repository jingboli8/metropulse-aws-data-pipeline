from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime

import pytest

from metropulse.audit_inventory import (
    AUDIT_INVENTORY_VERSION,
    inventory_document_bytes,
    parse_inventory_document,
    publish_inventory,
)
from metropulse.audit_models import (
    AuditContractError,
    AuditInventory,
    AuditMonth,
    CuratedObject,
    PinnedObject,
)
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_manifest import COMPACTION_MANIFEST_VERSION
from metropulse.schema import SCHEMA_VERSION


def inventory_fixture() -> AuditInventory:
    run_id = "a" * 64
    root = f"control/compaction/source=metropt3/year=2020/month=02/run_id={run_id}"
    month = AuditMonth(
        year=2020,
        month=2,
        run_id=run_id,
        selection=PinnedObject("lake", f"{root}/selection.json", "sha256", "1" * 64),
        completion=PinnedObject("lake", f"{root}/completed.json", "sha256", "2" * 64),
        publication=PinnedObject("lake", f"{root}/published.json", "sha256", "3" * 64),
        curated=CuratedObject(
            "lake",
            f"curated/metropt3/year=2020/month=02/run_id={run_id}/part-00000.snappy.parquet",
            "4" * 64,
            100,
        ),
        glue_values=("2020", "02"),
        glue_location=f"s3://lake/curated/metropt3/year=2020/month=02/run_id={run_id}/",
        selected_row_count=10,
        curated_row_count=10,
        quarantine_row_count=0,
        first_timestamp=datetime(2020, 2, 1),
        last_timestamp=datetime(2020, 2, 28),
        within_month_gap_count=2,
        coverage_status="known_missing_dates",
        known_missing_dates=(date(2020, 2, 29),),
        terminal_partial_month=False,
    )
    return AuditInventory(
        source_name="metropt3",
        known_source_start=datetime(2020, 2, 1),
        known_source_end=datetime(2020, 2, 28, 23, 59, 50),
        expected_months=("2020-02",),
        months=(month,),
        expected_total_curated_rows=10,
        schema_version=SCHEMA_VERSION,
        compaction_manifest_version=COMPACTION_MANIFEST_VERSION,
        inventory_version=AUDIT_INVENTORY_VERSION,
    )


def test_inventory_identity_is_canonical_and_publication_is_idempotent() -> None:
    inventory = inventory_fixture()
    identity, key, body = inventory_document_bytes(inventory)
    assert key == f"control/audit/source=metropt3/inventory_id={identity}/inventory.json"
    assert parse_inventory_document(body) == (identity, inventory)
    storage = InMemoryObjectStorage()
    first = publish_inventory(inventory, bucket="lake", storage=storage)
    second = publish_inventory(inventory, bucket="lake", storage=storage)
    assert first.created is True
    assert second.created is False
    assert first.identity_value == second.identity_value


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: replace(value, expected_months=()),
        lambda value: replace(value, months=value.months + value.months),
        lambda value: replace(value, expected_total_curated_rows=9),
        lambda value: replace(value, inventory_version="unsupported"),
    ],
)
def test_inventory_rejects_incomplete_duplicate_or_inconsistent_content(mutation) -> None:
    with pytest.raises(AuditContractError):
        inventory_document_bytes(mutation(inventory_fixture()))


def test_inventory_corruption_and_conflicting_immutable_key_fail() -> None:
    inventory = inventory_fixture()
    identity, key, body = inventory_document_bytes(inventory)
    with pytest.raises(AuditContractError):
        parse_inventory_document(body.replace(identity.encode(), b"f" * 64, 1))
    storage = InMemoryObjectStorage()
    storage.seed("lake", key, b"conflict")
    with pytest.raises(AuditContractError, match="conflicting"):
        publish_inventory(inventory, bucket="lake", storage=storage)

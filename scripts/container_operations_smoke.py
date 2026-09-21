"""Deterministic in-container compaction and scheduled-audit smoke test."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

from metropulse.audit_inventory import AUDIT_INVENTORY_VERSION, inventory_document_bytes
from metropulse.audit_models import (
    AuditBounds,
    AuditInventory,
    AuditMonth,
    CuratedObject,
    PinnedObject,
)
from metropulse.aws.audit_processor import ScheduledAuditProcessor
from metropulse.aws.compaction_processor import S3MonthlyCompactionProcessor
from metropulse.aws.compaction_testing import FakePartitionPublisher
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_identity import canonical_json
from metropulse.compaction_manifest import (
    COMPACTION_MANIFEST_VERSION,
    compaction_keys,
    glue_partition_location,
)
from metropulse.compaction_models import MonthlySelection, PublicationConflict, SelectedDailyInput
from metropulse.curated_parquet import sha256_bytes
from metropulse.models import NormalizedRecord
from metropulse.monthly_compaction import compact_month
from metropulse.parquet import serialize_daily_parquet
from metropulse.schema import SCHEMA_VERSION


class _Catalog:
    def __init__(self, location: str) -> None:
        self.location = location

    def get_partition(self, *, year: str, month: str) -> dict[str, object]:
        return {"Values": [year, month], "StorageDescriptor": {"Location": self.location}}


def _record(source_date: date, index: int, timestamp: datetime) -> NormalizedRecord:
    return NormalizedRecord(
        record_index=index,
        event_timestamp_local=timestamp,
        tp2=-0.1,
        tp3=9.0,
        h1=8.0,
        dv_pressure=-0.2,
        reservoirs=9.1,
        oil_temperature=50.0,
        motor_current=1.0,
        comp=True,
        dv_electric=False,
        towers=True,
        mpg=False,
        lps=True,
        pressure_switch=False,
        oil_level=True,
        caudal_impulses=False,
        source_date=source_date,
        event_timestamp_timezone_status="unknown",
    )


def _selection(storage: InMemoryObjectStorage) -> MonthlySelection:
    items: list[SelectedDailyInput] = []
    timestamps = (datetime(2020, 2, 1, 23, 58), datetime(2020, 2, 2, 0, 0))
    for index, timestamp in enumerate(timestamps, start=1):
        source_date = timestamp.date()
        body = serialize_daily_parquet(
            [_record(source_date, index, timestamp)],
            pipeline_version="7.0.0-smoke",
            source_identity={"fixture": True},
            source_date=source_date,
            raw_sha256="a" * 64,
            processing_timestamp="2026-09-18T00:00:00Z",
        )
        staging_key = f"staging/source=metropt3/day={source_date.isoformat()}/data.parquet"
        storage.seed("lake", staging_key, body)
        marker_key = f"control/validation/source_date={source_date.isoformat()}/completed.json"
        marker = {
            "first_valid_event_timestamp_local": timestamp.isoformat(),
            "input_row_count": 1,
            "last_valid_event_timestamp_local": timestamp.isoformat(),
            "manifest_version": "1.1.0",
            "pipeline_version": "7.0.0-smoke",
            "processing_identity": f"smoke-{source_date.isoformat()}",
            "quarantine": None,
            "quarantine_row_count": 0,
            "raw_key": f"raw/source=metropt3/source_date={source_date.isoformat()}/data.csv",
            "raw_sha256": "a" * 64,
            "reconciliation": {
                "accounted_row_count": 1,
                "applicable": True,
                "input_row_count": 1,
                "matches": True,
                "quarantine_row_count": 0,
                "valid_row_count": 1,
            },
            "schema_version": SCHEMA_VERSION,
            "source_bucket": "lake",
            "source_date": source_date.isoformat(),
            "staging": {
                "bucket": "lake",
                "byte_size": len(body),
                "key": staging_key,
                "sha256": sha256_bytes(body),
            },
            "valid_row_count": 1,
            "valid_timestamp_count": 1,
        }
        marker_body = canonical_json(marker)
        storage.seed("lake", marker_key, marker_body)
        items.append(
            SelectedDailyInput(
                source_date=source_date,
                processing_identity=f"smoke-{source_date.isoformat()}",
                completion_marker_bucket="lake",
                completion_marker_key=marker_key,
                completion_marker_identity_kind="sha256",
                completion_marker_identity_value=sha256_bytes(marker_body),
                staging_bucket="lake",
                staging_key=staging_key,
                staging_sha256=sha256_bytes(body),
                staging_byte_size=len(body),
                input_row_count=1,
                valid_row_count=1,
                quarantine_row_count=0,
                first_valid_timestamp=timestamp,
                last_valid_timestamp=timestamp,
                pipeline_version="7.0.0-smoke",
                schema_version=SCHEMA_VERSION,
                manifest_version="1.1.0",
            )
        )
    return MonthlySelection(
        source_name="metropt3",
        year=2020,
        month=2,
        inputs=tuple(items),
        expected_raw_dates=(date(2020, 2, 1), date(2020, 2, 2)),
        known_source_start=date(2020, 2, 1),
        known_source_end=date(2020, 2, 2),
        terminal_partial_month=True,
    )


def main() -> None:
    storage = InMemoryObjectStorage()
    selection = _selection(storage)
    publisher = FakePartitionPublisher()
    processor = S3MonthlyCompactionProcessor(storage, publisher)
    timestamp = datetime(2026, 9, 18, tzinfo=UTC)
    first = processor.process(
        selection,
        destination_bucket="lake",
        owner_token="compaction:smoke-first",
        processing_timestamp=timestamp,
    )
    second = processor.process(
        selection,
        destination_bucket="lake",
        owner_token="compaction:smoke-retry",
        processing_timestamp=timestamp,
    )
    if first["status"] != "published" or second["status"] != "verified_no_op":
        raise AssertionError("compaction idempotency contract failed")

    keys = compaction_keys(2020, 2, str(first["run_id"]))
    location = glue_partition_location("lake", 2020, 2, str(first["run_id"]))
    completion_body = storage.get("lake", keys["completed"]).body
    completion = json.loads(completion_body)
    publication_body = storage.get("lake", keys["published"]).body
    curated_body = storage.get("lake", keys["curated"]).body
    selection_body = storage.get("lake", keys["selection"]).body
    month = AuditMonth(
        year=2020,
        month=2,
        run_id=str(first["run_id"]),
        selection=PinnedObject("lake", keys["selection"], "sha256", sha256_bytes(selection_body)),
        completion=PinnedObject("lake", keys["completed"], "sha256", sha256_bytes(completion_body)),
        publication=PinnedObject(
            "lake", keys["published"], "sha256", sha256_bytes(publication_body)
        ),
        curated=CuratedObject(
            "lake", keys["curated"], sha256_bytes(curated_body), len(curated_body)
        ),
        glue_values=("2020", "02"),
        glue_location=location,
        selected_row_count=2,
        curated_row_count=2,
        quarantine_row_count=0,
        first_timestamp=datetime.fromisoformat(completion["first_valid_event_timestamp_local"]),
        last_timestamp=datetime.fromisoformat(completion["last_valid_event_timestamp_local"]),
        within_month_gap_count=int(completion["significant_gap_count"]),
        coverage_status="terminal_partial",
        known_missing_dates=(),
        terminal_partial_month=True,
    )
    inventory = AuditInventory(
        source_name="metropt3",
        known_source_start=datetime(2020, 2, 1),
        known_source_end=datetime(2020, 2, 2, 23, 59, 59),
        expected_months=("2020-02",),
        months=(month,),
        expected_total_curated_rows=2,
        schema_version=SCHEMA_VERSION,
        compaction_manifest_version=COMPACTION_MANIFEST_VERSION,
        inventory_version=AUDIT_INVENTORY_VERSION,
    )
    inventory_id, inventory_key, inventory_body = inventory_document_bytes(inventory)
    storage.seed("lake", inventory_key, inventory_body)
    audit = ScheduledAuditProcessor(
        storage=storage,
        catalog=_Catalog(location),
        bounds=AuditBounds(),
    ).process(PinnedObject("lake", inventory_key, "sha256", sha256_bytes(inventory_body)))
    if audit.status != "passed" or audit.total_curated_rows != 2 or audit.global_gap_count != 1:
        raise AssertionError("scheduled audit row/gap reconciliation failed")

    conflict_storage = InMemoryObjectStorage()
    conflicting = _selection(conflict_storage)
    conflict_result = compact_month(
        conflicting,
        lambda item: conflict_storage.get(item.staging_bucket, item.staging_key).body,
    )
    conflict_claim_key = compaction_keys(2020, 2, conflict_result.run_id)["publication_claim"]
    conflict_storage.seed("lake", conflict_claim_key, b'{"owner_token":"former"}\n')
    try:
        S3MonthlyCompactionProcessor(conflict_storage, FakePartitionPublisher()).process(
            conflicting,
            destination_bucket="lake",
            owner_token="compaction:smoke-conflict",
            processing_timestamp=timestamp,
        )
    except PublicationConflict:
        pass
    else:
        raise AssertionError("stale/conflicting compaction claim was accepted")
    print(
        "OPERATIONS_SMOKE_OK "
        f"rows={audit.total_curated_rows} gaps={audit.global_gap_count} "
        f"inventory={inventory_id} run_id={first['run_id']}"
    )


if __name__ == "__main__":
    main()

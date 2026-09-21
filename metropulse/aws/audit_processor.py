"""Read-only storage/catalog adapter for one pinned scheduled-audit inventory."""

from __future__ import annotations

from metropulse.audit_inventory import parse_inventory_document
from metropulse.audit_models import (
    AuditBounds,
    AuditContractError,
    PartitionCatalogReader,
    PinnedObject,
    ScheduledAuditResult,
)
from metropulse.aws.storage import ObjectNotFound, ObjectStorage
from metropulse.curated_parquet import sha256_bytes
from metropulse.scheduled_audit import audit_curated_inventory


class ScheduledAuditProcessor:
    """Fetch one exact inventory and run the bounded AWS-independent audit."""

    def __init__(
        self,
        *,
        storage: ObjectStorage,
        catalog: PartitionCatalogReader,
        bounds: AuditBounds,
    ) -> None:
        self.storage = storage
        self.catalog = catalog
        self.bounds = bounds

    def process(self, reference: PinnedObject) -> ScheduledAuditResult:
        """Verify the inventory pin and audit only its exact referenced objects."""
        version = reference.identity_value if reference.identity_kind == "version_id" else None
        try:
            stored = self.storage.get(reference.bucket, reference.key, version_id=version)
        except ObjectNotFound as error:
            raise AuditContractError("pinned audit inventory is missing") from error
        if reference.identity_kind == "sha256":
            observed = sha256_bytes(stored.body)
            expected = reference.identity_value.lower()
        elif reference.identity_kind == "etag":
            observed, expected = stored.head.etag, reference.identity_value
        else:
            observed, expected = stored.head.version_id, reference.identity_value
        if observed != expected:
            raise AuditContractError("pinned audit inventory identity mismatch")
        identity, inventory = parse_inventory_document(stored.body)
        expected_key = f"control/audit/source=metropt3/inventory_id={identity}/inventory.json"
        if reference.key != expected_key:
            raise AuditContractError("audit inventory key does not match its identity")
        return audit_curated_inventory(
            inventory,
            storage=self.storage,
            catalog=self.catalog,
            bounds=self.bounds,
        )

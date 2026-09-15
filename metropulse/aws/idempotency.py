"""Canonical processing identities for at-least-once object delivery."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping


def canonical_json(value: Mapping[str, object]) -> str:
    """Serialize an identity mapping without relying on dictionary order."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def processing_identity(
    *,
    source_bucket: str,
    source_key: str,
    version_id: str | None,
    checksum_sha256: str | None,
    etag: str | None,
    pipeline_version: str,
    schema_version: str,
    manifest_version: str,
) -> tuple[str, dict[str, object]]:
    """Return SHA-256 input ID and its canonical, auditable identity fields."""
    if version_id:
        identity_kind, identity_value = "version_id", version_id
    elif checksum_sha256:
        identity_kind, identity_value = "sha256", checksum_sha256
    elif etag:
        identity_kind, identity_value = "etag", etag
    else:
        raise ValueError("Source object requires a version ID, checksum, or ETag")
    fields: dict[str, object] = {
        "manifest_version": manifest_version,
        "object_identity_kind": identity_kind,
        "object_identity_value": identity_value,
        "pipeline_version": pipeline_version,
        "schema_version": schema_version,
        "source_bucket": source_bucket,
        "source_key": source_key,
    }
    digest = hashlib.sha256(canonical_json(fields).encode("utf-8")).hexdigest()
    return digest, fields

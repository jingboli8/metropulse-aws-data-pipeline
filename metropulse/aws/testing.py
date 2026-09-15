"""Deterministic in-memory object storage for offline adapter tests."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from metropulse.aws.storage import (
    ConditionalWriteFailed,
    ObjectHead,
    ObjectNotFound,
    StoredObject,
)


@dataclass
class _Entry:
    body: bytes
    head: ObjectHead
    content_type: str
    content_encoding: str | None


class InMemoryObjectStorage:
    """Model S3 bodies, metadata, versions, and conditional writes without I/O."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], _Entry] = {}
        self.get_counts: dict[tuple[str, str], int] = {}
        self.put_counts: dict[tuple[str, str], int] = {}
        self.conditional_races: dict[tuple[str, str], Callable[[bytes], bytes]] = {}

    def seed(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        version_id: str | None = None,
        etag: str | None = None,
        checksum_sha256: str | None = None,
        last_modified: datetime | None = None,
        metadata: dict[str, str] | None = None,
        content_type: str = "application/octet-stream",
        content_encoding: str | None = None,
    ) -> ObjectHead:
        """Seed a source or prior output object for an offline scenario."""
        head = ObjectHead(
            byte_size=len(body),
            etag=etag or hashlib.sha256(body).hexdigest()[:32],
            version_id=version_id,
            checksum_sha256=checksum_sha256,
            last_modified=last_modified or datetime(2026, 9, 11, tzinfo=UTC),
            metadata=dict(metadata or {}),
        )
        self.objects[(bucket, key)] = _Entry(body, head, content_type, content_encoding)
        return head

    def head(self, bucket: str, key: str, *, version_id: str | None = None) -> ObjectHead:
        entry = self._entry(bucket, key)
        self._check_version(entry, version_id)
        return entry.head

    def get(self, bucket: str, key: str, *, version_id: str | None = None) -> StoredObject:
        entry = self._entry(bucket, key)
        self._check_version(entry, version_id)
        identity = (bucket, key)
        self.get_counts[identity] = self.get_counts.get(identity, 0) + 1
        return StoredObject(entry.body, entry.head)

    def put(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        content_type: str,
        content_encoding: str | None,
        metadata: dict[str, str],
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> ObjectHead:
        identity = (bucket, key)
        existing = self.objects.get(identity)
        if if_none_match and existing is not None:
            raise ConditionalWriteFailed(key)
        if if_match is not None and (existing is None or existing.head.etag != if_match):
            raise ConditionalWriteFailed(key)
        if if_none_match and identity in self.conditional_races:
            raced_body = self.conditional_races.pop(identity)(body)
            self.seed(
                bucket,
                key,
                raced_body,
                metadata=dict(metadata),
                content_type=content_type,
                content_encoding=content_encoding,
            )
            raise ConditionalWriteFailed(key)
        head = self.seed(
            bucket,
            key,
            body,
            metadata=dict(metadata),
            content_type=content_type,
            content_encoding=content_encoding,
        )
        self.put_counts[identity] = self.put_counts.get(identity, 0) + 1
        return head

    def corrupt_body(self, bucket: str, key: str, body: bytes) -> None:
        """Replace bytes while retaining prior metadata for integrity tests."""
        entry = self._entry(bucket, key)
        self.objects[(bucket, key)] = _Entry(
            body,
            ObjectHead(
                byte_size=len(body),
                etag=entry.head.etag,
                version_id=entry.head.version_id,
                checksum_sha256=entry.head.checksum_sha256,
                last_modified=entry.head.last_modified,
                metadata=entry.head.metadata,
            ),
            entry.content_type,
            entry.content_encoding,
        )

    def remove(self, bucket: str, key: str) -> None:
        """Remove one object to model an incomplete prior attempt."""
        self.objects.pop((bucket, key), None)

    def _entry(self, bucket: str, key: str) -> _Entry:
        try:
            return self.objects[(bucket, key)]
        except KeyError as error:
            raise ObjectNotFound(f"s3://{bucket}/{key}") from error

    @staticmethod
    def _check_version(entry: _Entry, version_id: str | None) -> None:
        if version_id is not None and entry.head.version_id != version_id:
            raise ObjectNotFound(f"version {version_id}")

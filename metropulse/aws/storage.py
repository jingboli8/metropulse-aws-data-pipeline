"""Small object-storage protocol plus the boto3 translation boundary."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol


class ObjectNotFound(FileNotFoundError):
    """Requested object does not exist."""


class ConditionalWriteFailed(RuntimeError):
    """An S3 write precondition did not hold."""


@dataclass(frozen=True)
class ObjectHead:
    """Storage metadata needed for identity and integrity decisions."""

    byte_size: int
    etag: str
    version_id: str | None = None
    checksum_sha256: str | None = None
    last_modified: datetime | None = None
    metadata: dict[str, str] | None = None


@dataclass(frozen=True)
class StoredObject:
    """One object body and its current metadata."""

    body: bytes
    head: ObjectHead


class ObjectStorage(Protocol):
    """Minimal storage operations required by the orchestration adapter."""

    def head(self, bucket: str, key: str, *, version_id: str | None = None) -> ObjectHead: ...

    def get(self, bucket: str, key: str, *, version_id: str | None = None) -> StoredObject: ...

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
    ) -> ObjectHead: ...


class Boto3S3Storage:
    """Translate the storage protocol to an injected boto3 S3 client."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def head(self, bucket: str, key: str, *, version_id: str | None = None) -> ObjectHead:
        arguments = _object_arguments(bucket, key, version_id)
        try:
            response = self._client.head_object(**arguments)
        except Exception as error:
            if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                raise ObjectNotFound(f"s3://{bucket}/{key}") from error
            raise
        return _head_from_response(response)

    def get(self, bucket: str, key: str, *, version_id: str | None = None) -> StoredObject:
        arguments = _object_arguments(bucket, key, version_id)
        try:
            response = self._client.get_object(**arguments)
        except Exception as error:
            if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                raise ObjectNotFound(f"s3://{bucket}/{key}") from error
            raise
        return StoredObject(response["Body"].read(), _head_from_response(response))

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
        arguments: dict[str, Any] = {
            "Body": body,
            "Bucket": bucket,
            "ContentType": content_type,
            "Key": key,
            "Metadata": metadata,
        }
        if content_encoding:
            arguments["ContentEncoding"] = content_encoding
        if if_none_match:
            arguments["IfNoneMatch"] = "*"
        if if_match:
            arguments["IfMatch"] = if_match
        try:
            response = self._client.put_object(**arguments)
        except Exception as error:
            if _error_code(error) in {"PreconditionFailed", "412", "ConditionalRequestConflict"}:
                raise ConditionalWriteFailed(key) from error
            raise
        return ObjectHead(
            byte_size=len(body),
            etag=str(response.get("ETag", "")).strip('"'),
            version_id=response.get("VersionId"),
            metadata=dict(metadata),
        )


def _object_arguments(bucket: str, key: str, version_id: str | None) -> dict[str, str]:
    arguments = {"Bucket": bucket, "Key": key}
    if version_id:
        arguments["VersionId"] = version_id
    return arguments


def _head_from_response(response: dict[str, Any]) -> ObjectHead:
    checksum = response.get("ChecksumSHA256")
    if checksum:
        checksum = base64.b64decode(checksum).hex()
    last_modified = response.get("LastModified")
    if last_modified is not None and last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=UTC)
    return ObjectHead(
        byte_size=int(response["ContentLength"]),
        etag=str(response.get("ETag", "")).strip('"'),
        version_id=response.get("VersionId"),
        checksum_sha256=checksum,
        last_modified=last_modified,
        metadata=dict(response.get("Metadata", {})),
    )


def _error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        details = response.get("Error", {})
        if isinstance(details, dict):
            return str(details.get("Code"))
    return None

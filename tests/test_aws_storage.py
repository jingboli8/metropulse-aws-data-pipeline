from __future__ import annotations

import io

import pytest

from metropulse.aws.storage import Boto3S3Storage, ConditionalWriteFailed, ObjectNotFound


class ClientError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class FakeBotoClient:
    def __init__(self) -> None:
        self.put_arguments = None

    def head_object(self, **arguments):
        if arguments["Key"] == "missing":
            raise ClientError("NoSuchKey")
        return {"ContentLength": 3, "ETag": '"opaque-etag"', "Metadata": {"x": "y"}}

    def get_object(self, **arguments):
        return {
            "Body": io.BytesIO(b"abc"),
            "ContentLength": 3,
            "ETag": '"opaque-etag"',
            "Metadata": {"x": "y"},
        }

    def put_object(self, **arguments):
        self.put_arguments = arguments
        if arguments["Key"] == "raced":
            raise ClientError("PreconditionFailed")
        return {"ETag": '"new-etag"'}


def test_boto3_boundary_translates_reads_and_conditional_writes_offline() -> None:
    client = FakeBotoClient()
    storage = Boto3S3Storage(client)

    assert storage.head("bucket", "key").etag == "opaque-etag"
    assert storage.get("bucket", "key").body == b"abc"
    written = storage.put(
        "bucket",
        "key",
        b"abc",
        content_type="application/json",
        content_encoding=None,
        metadata={"sha256": "digest"},
        if_none_match=True,
    )

    assert written.etag == "new-etag"
    assert client.put_arguments["IfNoneMatch"] == "*"
    patienter = Boto3S3Storage(client)
    with pytest.raises(ConditionalWriteFailed):
        patienter.put(
            "bucket",
            "raced",
            b"abc",
            content_type="application/json",
            content_encoding=None,
            metadata={},
            if_none_match=True,
        )
    with pytest.raises(ObjectNotFound):
        storage.head("bucket", "missing")

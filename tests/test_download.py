from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from metropulse_feasibility.download import (
    extract_archive,
    inspect_zip,
    read_manifest_checksum,
    safe_member_path,
    sha256_stream,
)


def test_sha256_stream() -> None:
    assert sha256_stream(io.BytesIO(b"metropt")) == (
        "2aedde44fdc04e3957fb07209b4962b60c120e2916539cb4b79a8ac63cea1077"
    )


def test_safe_member_path_rejects_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsafe"):
        safe_member_path(tmp_path, "../escape.csv")


def test_inspect_and_extract_zip(tmp_path: Path) -> None:
    archive_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("nested/sample.csv", "a,b\n1,2\n")
    members = inspect_zip(archive_path)
    assert [(member.name, member.uncompressed_size) for member in members] == [
        ("nested/sample.csv", 8)
    ]
    extracted = extract_archive(archive_path, tmp_path / "raw")
    assert extracted[0].read_text(encoding="utf-8") == "a,b\n1,2\n"


def test_read_manifest_checksum(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    assert read_manifest_checksum(manifest) is None
    checksum = "a" * 64
    manifest.write_text(json.dumps({"sha256": checksum}), encoding="utf-8")
    assert read_manifest_checksum(manifest) == checksum


def test_inspect_zip_rejects_corruption(tmp_path: Path) -> None:
    archive_path = tmp_path / "corrupt.zip"
    archive_path.write_bytes(b"not a zip")
    with pytest.raises(zipfile.BadZipFile):
        inspect_zip(archive_path)

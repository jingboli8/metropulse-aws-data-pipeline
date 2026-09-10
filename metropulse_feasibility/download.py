"""Safe, streaming download and ZIP verification helpers."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO

UCI_DATASET_URL = "https://archive.ics.uci.edu/static/public/791/metropt%2B3%2Bdataset.zip"
EXPECTED_CSV = "MetroPT3(AirCompressor).csv"


@dataclass(frozen=True)
class ArchiveMember:
    name: str
    compressed_size: int
    uncompressed_size: int


def sha256_stream(stream: BinaryIO, chunk_size: int = 1024 * 1024) -> str:
    """Return SHA-256 for a binary stream without reading it all into memory."""
    digest = hashlib.sha256()
    while chunk := stream.read(chunk_size):
        digest.update(chunk)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return sha256_stream(stream)


def safe_member_path(destination: Path, member_name: str) -> Path:
    """Resolve a ZIP member below destination, rejecting traversal and absolute paths."""
    member = PurePosixPath(member_name)
    if member.is_absolute() or ".." in member.parts:
        raise ValueError(f"Unsafe ZIP member path: {member_name!r}")
    target = destination.joinpath(*member.parts).resolve()
    root = destination.resolve()
    if target != root and root not in target.parents:
        raise ValueError(f"ZIP member escapes extraction directory: {member_name!r}")
    return target


def inspect_zip(path: Path) -> list[ArchiveMember]:
    """Fully test the archive and return member metadata."""
    with zipfile.ZipFile(path) as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise zipfile.BadZipFile(f"CRC failure in ZIP member: {bad_member}")
        return [
            ArchiveMember(info.filename, info.compress_size, info.file_size)
            for info in archive.infolist()
            if not info.is_dir()
        ]


def download_archive(url: str, destination: Path, chunk_size: int = 1024 * 1024) -> str:
    """Download to .partial, verify ZIP, then atomically replace destination."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".partial")
    partial.unlink(missing_ok=True)
    digest = hashlib.sha256()
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "MetroPulse/0.1"})
        with urllib.request.urlopen(request, timeout=60) as response, partial.open("xb") as output:
            if response.status != 200:
                raise RuntimeError(f"Download returned HTTP {response.status}")
            while chunk := response.read(chunk_size):
                output.write(chunk)
                digest.update(chunk)
            output.flush()
            os.fsync(output.fileno())
        inspect_zip(partial)
        partial.replace(destination)
    except Exception:
        partial.unlink(missing_ok=True)
        raise
    return digest.hexdigest()


def extract_archive(archive_path: Path, destination: Path) -> list[Path]:
    """Extract regular files safely and idempotently beneath destination."""
    destination.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(archive_path) as archive:
        for info in archive.infolist():
            target = safe_member_path(destination, info.filename)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and target.stat().st_size == info.file_size:
                extracted.append(target)
                continue
            partial = target.with_name(target.name + ".partial")
            partial.unlink(missing_ok=True)
            with archive.open(info) as source, partial.open("xb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            if partial.stat().st_size != info.file_size:
                partial.unlink(missing_ok=True)
                raise OSError(f"Extracted size mismatch for {info.filename}")
            partial.replace(target)
            extracted.append(target)
    return extracted


def write_manifest(path: Path, archive_path: Path, sha256: str) -> None:
    members = inspect_zip(archive_path)
    payload = {
        "source_url": UCI_DATASET_URL,
        "archive": str(archive_path),
        "archive_size_bytes": archive_path.stat().st_size,
        "sha256": sha256,
        "members": [asdict(member) for member in members],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_manifest_checksum(path: Path) -> str | None:
    """Read a prior checksum so reruns detect an unexpected archive change."""
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    checksum = payload.get("sha256")
    if not isinstance(checksum, str) or len(checksum) != 64:
        raise ValueError(f"Invalid SHA-256 in manifest: {path}")
    return checksum

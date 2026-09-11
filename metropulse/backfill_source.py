"""Verified local source inputs and deterministic processing-time parsing."""

from __future__ import annotations

import argparse
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import TextIOWrapper
from pathlib import Path
from typing import TextIO

from metropulse.local_io import sha256_file

OFFICIAL_ZIP_SHA256 = "aab991a970e58210de853bb8078ce0e63abb4d9412fdc5c79792dae3d8e1721a"
OFFICIAL_ZIP_SIZE = 218_381_995
OFFICIAL_CSV_MEMBER = "MetroPT3(AirCompressor).csv"
OFFICIAL_CSV_SIZE = 218_300_507


class BackfillError(RuntimeError):
    """A local backfill contract or publication step failed."""


@dataclass(frozen=True)
class SourceInput:
    """Verified source description without a machine-specific stored path."""

    path: Path
    kind: str
    sha256: str
    byte_size: int
    member_name: str | None = None
    member_byte_size: int | None = None

    @property
    def identity(self) -> dict[str, object]:
        """Return portable source identity suitable for manifests."""
        identity: dict[str, object] = {
            "byte_size": self.byte_size,
            "kind": self.kind,
            "name": self.path.name,
            "sha256": self.sha256,
        }
        if self.member_name is not None:
            identity["member_byte_size"] = self.member_byte_size
            identity["member_name"] = self.member_name
        return identity

    @contextmanager
    def open_text(self) -> Iterator[TextIO]:
        """Open the CSV as newline-preserving UTF-8 text without extraction."""
        if self.kind == "official_uci_zip":
            with (
                zipfile.ZipFile(self.path) as archive,
                archive.open(self.member_name or OFFICIAL_CSV_MEMBER, "r") as binary,
                TextIOWrapper(binary, encoding="utf-8-sig", newline="") as text,
            ):
                yield text
            return
        with self.path.open("r", encoding="utf-8-sig", newline="") as text:
            yield text


def verify_checksum(path: Path, expected_sha256: str) -> str:
    """Hash a file and fail before processing when it differs from expectation."""
    actual = sha256_file(path)
    if actual != expected_sha256.lower():
        raise BackfillError(
            f"Checksum mismatch for {path.name}: expected {expected_sha256.lower()}, got {actual}."
        )
    return actual


def prepare_archive_source(path: Path) -> SourceInput:
    """Verify the official archive identity and locate its contracted CSV member."""
    if not path.is_file():
        raise BackfillError(f"Source archive does not exist: {path}")
    if path.stat().st_size != OFFICIAL_ZIP_SIZE:
        raise BackfillError(
            f"Official ZIP size mismatch: expected {OFFICIAL_ZIP_SIZE}, got {path.stat().st_size}."
        )
    checksum = verify_checksum(path, OFFICIAL_ZIP_SHA256)
    try:
        with zipfile.ZipFile(path) as archive:
            candidates = [
                item for item in archive.infolist() if item.filename == OFFICIAL_CSV_MEMBER
            ]
            if len(candidates) != 1:
                raise BackfillError(
                    f"Archive must contain exactly one {OFFICIAL_CSV_MEMBER!r} member."
                )
            member = candidates[0]
            if member.file_size != OFFICIAL_CSV_SIZE:
                raise BackfillError(
                    f"CSV member size mismatch: expected {OFFICIAL_CSV_SIZE}, got "
                    f"{member.file_size}."
                )
    except zipfile.BadZipFile as error:
        raise BackfillError("Source archive is not a readable ZIP file.") from error
    return SourceInput(
        path=path,
        kind="official_uci_zip",
        sha256=checksum,
        byte_size=path.stat().st_size,
        member_name=OFFICIAL_CSV_MEMBER,
        member_byte_size=member.file_size,
    )


def prepare_csv_source(path: Path, expected_sha256: str | None = None) -> SourceInput:
    """Hash a caller-selected CSV and optionally require a known digest."""
    if not path.is_file():
        raise BackfillError(f"Source CSV does not exist: {path}")
    checksum = sha256_file(path)
    if expected_sha256 is not None and checksum != expected_sha256.lower():
        expected = expected_sha256.lower()
        raise BackfillError(
            f"Checksum mismatch for {path.name}: expected {expected}, got {checksum}."
        )
    return SourceInput(path, "csv", checksum, path.stat().st_size)


def parse_processing_timestamp(value: str) -> tuple[datetime, str]:
    """Parse a fixed UTC processing time; a missing offset means UTC by run contract."""
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as error:
        raise argparse.ArgumentTypeError("processing timestamp must be ISO 8601") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    if parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("processing timestamp must be UTC or have no offset")
    parsed = parsed.astimezone(UTC)
    return parsed, parsed.isoformat().replace("+00:00", "Z")

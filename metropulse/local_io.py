"""Local checksum and atomic-write primitives for generated lake files."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

HASH_CHUNK_SIZE = 1024 * 1024


def sha256_file(path: Path) -> str:
    """Return the lowercase SHA-256 digest of a file using bounded memory."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def temporary_output_path(final_path: Path) -> Iterator[Path]:
    """Yield a sibling temporary path and remove it when the context exits."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=final_path.parent,
        prefix=f".{final_path.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        yield temporary_path
    finally:
        temporary_path.unlink(missing_ok=True)


@contextmanager
def atomic_output_path(final_path: Path) -> Iterator[Path]:
    """Yield a sibling temporary path and atomically publish it on success."""
    with temporary_output_path(final_path) as temporary_path:
        yield temporary_path
        os.replace(temporary_path, final_path)


def write_text_atomic(final_path: Path, text: str) -> None:
    """Write UTF-8 text through a temporary file and atomically publish it."""
    with (
        atomic_output_path(final_path) as temporary_path,
        temporary_path.open("w", encoding="utf-8", newline="") as stream,
    ):
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())

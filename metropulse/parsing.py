"""CSV record capture and scalar coercion helpers."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


@dataclass(frozen=True)
class SourceCsvRecord:
    """One parsed CSV record plus its original serialized text."""

    field_values: tuple[str, ...]
    original_record: str


class _TrackingLineIterator:
    def __init__(self, lines: Iterable[str]) -> None:
        self._lines: Iterator[str] = iter(lines)
        self._consumed: list[str] = []

    def __iter__(self) -> _TrackingLineIterator:
        return self

    def __next__(self) -> str:
        line = next(self._lines)
        self._consumed.append(line)
        return line

    def take_consumed(self) -> str:
        consumed = "".join(self._consumed)
        self._consumed.clear()
        return consumed


def read_source_csv(
    source: str | Iterable[str],
) -> tuple[tuple[str, ...] | None, tuple[SourceCsvRecord, ...]]:
    """Read CSV without filesystem access and retain each original record's text."""
    lines: Iterable[str] = io.StringIO(source, newline="") if isinstance(source, str) else source
    tracker = _TrackingLineIterator(lines)
    reader = csv.reader(tracker, strict=True)
    try:
        header = tuple(next(reader))
    except StopIteration:
        return None, ()
    tracker.take_consumed()

    records: list[SourceCsvRecord] = []
    for fields in reader:
        records.append(SourceCsvRecord(tuple(fields), tracker.take_consumed()))
    return header, tuple(records)


def parse_record_index(value: str) -> int:
    """Parse a source record index constrained to Arrow signed int64."""
    parsed = int(value)
    if not INT64_MIN <= parsed <= INT64_MAX:
        raise ValueError("record index is outside signed int64 range")
    return parsed


def parse_event_timestamp_local(value: str) -> datetime:
    """Parse an exact second-precision timestamp without assigning a timezone."""
    parsed = datetime.strptime(value, TIMESTAMP_FORMAT)
    if parsed.strftime(TIMESTAMP_FORMAT) != value:
        raise ValueError("timestamp does not match YYYY-MM-DD HH:MM:SS exactly")
    return parsed


def parse_numeric(value: str) -> float:
    """Parse one numeric sensor value without applying physical bounds."""
    return float(value)

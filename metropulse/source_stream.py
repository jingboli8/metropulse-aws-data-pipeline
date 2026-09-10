"""Streaming capture and daily routing for the monolithic source CSV."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date

from metropulse.parsing import SourceCsvRecord, _TrackingLineIterator, parse_event_timestamp_local
from metropulse.schema import SOURCE_FIELDS


class SourceStreamError(ValueError):
    """The source cannot be split into trustworthy daily partitions."""


@dataclass(frozen=True)
class SourceStream:
    """Exact header text and a lazy sequence of source records."""

    header: tuple[str, ...]
    header_text: str
    records: Iterator[tuple[int, date, SourceCsvRecord]]


def open_source_stream(lines: Iterable[str]) -> SourceStream:
    """Parse the header and lazily derive a routing date for each CSV record."""
    tracker = _TrackingLineIterator(lines)
    reader = csv.reader(tracker, strict=True)
    try:
        header = tuple(next(reader))
    except StopIteration as error:
        raise SourceStreamError("Source CSV is empty.") from error
    header_text = tracker.take_consumed()
    if header != SOURCE_FIELDS:
        raise SourceStreamError("Source header does not match the exact 17-column contract.")

    def records() -> Iterator[tuple[int, date, SourceCsvRecord]]:
        for row_number, fields in enumerate(reader, start=1):
            original_record = tracker.take_consumed()
            record = SourceCsvRecord(tuple(fields), original_record)
            if len(fields) < 2:
                raise SourceStreamError(
                    f"Source row {row_number} has no timestamp field and cannot be routed."
                )
            try:
                source_date = parse_event_timestamp_local(fields[1]).date()
            except (ValueError, IndexError) as error:
                raise SourceStreamError(
                    f"Source row {row_number} has an unparseable routing timestamp."
                ) from error
            yield row_number, source_date, record

    return SourceStream(header, header_text, records())

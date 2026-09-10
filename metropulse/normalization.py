"""Exact source-name normalization with explicit lineage."""

from __future__ import annotations

from collections.abc import Sequence

from metropulse.schema import SOURCE_FIELDS, SOURCE_TO_NORMALIZED


def normalize_source_field(source_name: str) -> str:
    """Return the contracted normalized name for an exact source field."""
    try:
        return SOURCE_TO_NORMALIZED[source_name]
    except KeyError as error:
        raise ValueError(f"Unknown MetroPT-3 source field: {source_name!r}") from error


def normalized_header(source_header: Sequence[str]) -> tuple[str, ...]:
    """Validate and normalize the exact 17-column source header."""
    header = tuple(source_header)
    if header != SOURCE_FIELDS:
        raise ValueError(f"Source header does not match schema version: {header!r}")
    return tuple(normalize_source_field(field) for field in header)


def source_lineage() -> dict[str, str]:
    """Return a copy of the exact source-to-normalized mapping."""
    return dict(SOURCE_TO_NORMALIZED)

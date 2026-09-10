"""Deterministic local paths mirroring the planned S3 data zones."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class DailyPaths:
    """Absolute local targets and portable paths for one source date."""

    raw: Path
    staging: Path
    quarantine: Path
    manifest: Path
    raw_relative: str
    staging_relative: str
    quarantine_relative: str
    manifest_relative: str


def daily_paths(output_root: Path, source_date: date) -> DailyPaths:
    """Build the canonical Phase 2 local-lake paths for one day."""
    date_text = source_date.isoformat()
    raw_relative = PurePosixPath("raw", "source=metropt3", f"source_date={date_text}", "data.csv")
    staging_relative = PurePosixPath(
        "staging",
        "source=metropt3",
        f"year={source_date:%Y}",
        f"month={source_date:%m}",
        f"day={source_date:%d}",
        "data.parquet",
    )
    quarantine_relative = PurePosixPath(
        "quarantine", "source=metropt3", f"source_date={date_text}", "rejected.jsonl"
    )
    manifest_relative = PurePosixPath(
        "control", "source=metropt3", f"source_date={date_text}", "manifest.json"
    )

    def local(relative: PurePosixPath) -> Path:
        return output_root.joinpath(*relative.parts)

    return DailyPaths(
        raw=local(raw_relative),
        staging=local(staging_relative),
        quarantine=local(quarantine_relative),
        manifest=local(manifest_relative),
        raw_relative=raw_relative.as_posix(),
        staging_relative=staging_relative.as_posix(),
        quarantine_relative=quarantine_relative.as_posix(),
        manifest_relative=manifest_relative.as_posix(),
    )

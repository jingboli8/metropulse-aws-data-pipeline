"""Canonical S3 key parsing and deterministic output mapping."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from metropulse.aws.config import LambdaConfig


class InvalidObjectKey(ValueError):
    """An object key is outside the canonical raw contract."""


@dataclass(frozen=True)
class OutputKeys:
    """All deterministic output and control keys for one processing identity."""

    staging: str
    quarantine: str
    claim: str
    completed: str


def parse_raw_key(key: str, config: LambdaConfig) -> date:
    """Return the source date from one exact canonical daily raw key."""
    pattern = re.compile(
        rf"^{re.escape(config.raw_prefix)}/source={re.escape(config.expected_source_name)}/"
        r"source_date=(\d{4}-\d{2}-\d{2})/data\.csv$"
    )
    match = pattern.fullmatch(key)
    if not match:
        raise InvalidObjectKey("Object key does not match the canonical raw prefix and CSV suffix")
    try:
        return date.fromisoformat(match.group(1))
    except ValueError as error:
        raise InvalidObjectKey("Object key contains an invalid source_date") from error


def output_keys(source_date: date, processing_identity: str, config: LambdaConfig) -> OutputKeys:
    """Map one input identity to non-recursive deterministic S3 keys."""
    source = config.expected_source_name
    day = source_date.isoformat()
    identity = f"input_id={processing_identity}"
    return OutputKeys(
        staging=(
            f"{config.staging_prefix}/source={source}/year={source_date:%Y}/"
            f"month={source_date:%m}/day={source_date:%d}/{identity}/data.parquet"
        ),
        quarantine=(
            f"{config.quarantine_prefix}/source={source}/source_date={day}/"
            f"{identity}/rejected.jsonl"
        ),
        claim=f"{config.control_prefix}/validation/{identity}/claim.json",
        completed=f"{config.control_prefix}/validation/{identity}/completed.json",
    )

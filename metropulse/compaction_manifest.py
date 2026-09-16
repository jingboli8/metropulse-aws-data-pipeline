"""Deterministic monthly keys and immutable evidence documents."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from metropulse.compaction_identity import (
    DEFAULT_COMPACTOR_VERSION,
    DEFAULT_OUTPUT_CONTRACT_VERSION,
    WriterContract,
)
from metropulse.compaction_models import BoundaryFinding, MonthlySelection
from metropulse.compaction_selection import selection_payload

COMPACTION_MANIFEST_VERSION = "1.0.0"


def compaction_keys(year: int, month: int, run_id: str) -> dict[str, str]:
    """Return every canonical data and control key for one immutable run."""
    root = f"control/compaction/source=metropt3/year={year:04d}/month={month:02d}"
    run_root = f"{root}/run_id={run_id}"
    return {
        "curated": (
            f"curated/metropt3/year={year:04d}/month={month:02d}/"
            f"run_id={run_id}/part-00000.snappy.parquet"
        ),
        "selection": f"{run_root}/selection.json",
        "claim": f"{run_root}/claim.json",
        "completed": f"{run_root}/completed.json",
        "published": f"{run_root}/published.json",
        "publication_claim": f"{root}/publication/claim.json",
    }


def glue_partition_location(bucket: str, year: int, month: int, run_id: str) -> str:
    """Return the exact completed run directory used by the Glue partition."""
    return f"s3://{bucket}/curated/metropt3/year={year:04d}/month={month:02d}/run_id={run_id}/"


def build_completion_manifest(
    *,
    selection: MonthlySelection,
    run_id: str,
    keys: dict[str, str],
    parquet_sha256: str,
    parquet_byte_size: int,
    selected_rows: int,
    curated_rows: int,
    quarantine_rows: int,
    first_timestamp: str | None,
    last_timestamp: str | None,
    valid_timestamp_count: int,
    row_group_count: int,
    missing_dates: list[str],
    boundary_findings: tuple[BoundaryFinding, ...],
    significant_gap_count: int,
    writer: WriterContract,
    compactor_version: str = DEFAULT_COMPACTOR_VERSION,
    output_contract_version: str = DEFAULT_OUTPUT_CONTRACT_VERSION,
) -> dict[str, Any]:
    """Build completion evidence after the output has been reopened and verified."""
    return {
        "boundary_findings": [
            {
                "classification": item.classification,
                "current_date": item.current_date.isoformat(),
                "previous_date": item.previous_date.isoformat(),
                "seconds": item.seconds,
            }
            for item in boundary_findings
        ],
        "compactor_version": compactor_version,
        "curated": {
            "byte_size": parquet_byte_size,
            "key": keys["curated"],
            "row_count": curated_rows,
            "sha256": parquet_sha256,
        },
        "first_valid_event_timestamp_local": first_timestamp,
        "last_valid_event_timestamp_local": last_timestamp,
        "manifest_version": COMPACTION_MANIFEST_VERSION,
        "missing_dates": missing_dates,
        "month": f"{selection.month:02d}",
        "output_contract_version": output_contract_version,
        "quarantine_row_count": quarantine_rows,
        "reconciliation": {
            "curated_equals_selected_valid": curated_rows == selected_rows,
            "input_equals_valid_plus_quarantine": sum(
                item.input_row_count for item in selection.inputs
            )
            == selected_rows + quarantine_rows,
        },
        "row_group_count": row_group_count,
        "run_id": run_id,
        "selected_input_count": len(selection.inputs),
        "selected_row_count": selected_rows,
        "selection_key": keys["selection"],
        "significant_gap_count": significant_gap_count,
        "source_name": selection.source_name.lower(),
        "terminal_partial_month": selection.terminal_partial_month,
        "valid_timestamp_count": valid_timestamp_count,
        "writer_contract": asdict(writer),
        "year": f"{selection.year:04d}",
    }


def build_selection_document(selection: MonthlySelection, run_id: str) -> dict[str, Any]:
    """Build immutable selection evidence tied to its deterministic run ID."""
    return {
        "contract": "metropulse-monthly-selection-v1",
        "run_id": run_id,
        "selection": selection_payload(selection),
    }

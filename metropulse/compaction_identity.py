"""Canonical deterministic identity for one monthly compaction run."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import pyarrow

from metropulse.compaction_models import MonthlySelection
from metropulse.compaction_selection import selection_payload

RUN_CONTRACT = "metropulse-monthly-compaction-run-v1"
DEFAULT_COMPACTOR_VERSION = "1.0.0"
DEFAULT_OUTPUT_CONTRACT_VERSION = "1.0.0"
ROW_GROUP_SIZE = 65_536


@dataclass(frozen=True)
class WriterContract:
    """Material Parquet writer settings included in the run identity."""

    pyarrow_version: str = pyarrow.__version__
    parquet_format_version: str = "2.6"
    compression: str = "snappy"
    use_dictionary: bool = False
    write_statistics: bool = True
    row_group_size: int = ROW_GROUP_SIZE
    timestamp_unit: str = "ms"
    coerce_timestamps: str = "ms"
    allow_truncated_timestamps: bool = False


def canonical_json(value: Any) -> bytes:
    """Serialize canonical JSON as compact UTF-8 with sorted object keys."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def run_identity_payload(
    selection: MonthlySelection,
    *,
    compactor_version: str = DEFAULT_COMPACTOR_VERSION,
    output_contract_version: str = DEFAULT_OUTPUT_CONTRACT_VERSION,
    writer_contract: WriterContract | None = None,
) -> dict[str, Any]:
    """Build the complete material-input identity payload."""
    writer = writer_contract or WriterContract()
    selected = selection_payload(selection)
    return {
        "compactor_version": compactor_version,
        "contract": RUN_CONTRACT,
        "inputs": selected["inputs"],
        "manifest_versions": sorted({item.manifest_version for item in selection.inputs}),
        "month": f"{selection.month:02d}",
        "output_contract_version": output_contract_version,
        "pipeline_versions": sorted({item.pipeline_version for item in selection.inputs}),
        "schema_versions": sorted({item.schema_version for item in selection.inputs}),
        "source_name": selection.source_name.lower(),
        "writer_contract": asdict(writer),
        "year": f"{selection.year:04d}",
    }


def monthly_run_id(
    selection: MonthlySelection,
    *,
    compactor_version: str = DEFAULT_COMPACTOR_VERSION,
    output_contract_version: str = DEFAULT_OUTPUT_CONTRACT_VERSION,
    writer_contract: WriterContract | None = None,
) -> str:
    """Return lowercase SHA-256 for canonical material inputs and writer settings."""
    payload = run_identity_payload(
        selection,
        compactor_version=compactor_version,
        output_contract_version=output_contract_version,
        writer_contract=writer_contract,
    )
    return hashlib.sha256(canonical_json(payload)).hexdigest()

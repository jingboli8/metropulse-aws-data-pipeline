"""Operator CLI for conditionally publishing explicit Phase 7 control documents."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from metropulse.audit_inventory import inventory_from_payload, publish_inventory
from metropulse.aws.storage import Boto3S3Storage
from metropulse.selection_approval import publish_approved_selection, selection_from_payload


def build_parser() -> argparse.ArgumentParser:
    """Build the explicit, no-discovery control-publication CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    selection = subcommands.add_parser("approve-selection")
    selection.add_argument("--selection", type=Path, required=True)
    selection.add_argument("--bucket", required=True)
    inventory = subcommands.add_parser("publish-audit-inventory")
    inventory.add_argument("--inventory", type=Path, required=True)
    inventory.add_argument("--bucket", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Publish one exact reviewed document through conditional S3 creation."""
    arguments = build_parser().parse_args(argv)
    payload = _read_object(
        arguments.selection if arguments.command == "approve-selection" else arguments.inventory
    )
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required for operator publication") from error
    storage = Boto3S3Storage(boto3.client("s3"))
    if arguments.command == "approve-selection":
        outcome = publish_approved_selection(
            selection_from_payload(payload), bucket=arguments.bucket, storage=storage
        )
    else:
        outcome = publish_inventory(
            inventory_from_payload(payload), bucket=arguments.bucket, storage=storage
        )
    print(json.dumps(outcome.__dict__, sort_keys=True, separators=(",", ":")))
    return 0


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("reviewed input must be one JSON object")
    return value


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[1]
EVIDENCE_PATH = ROOT / "docs" / "evidence" / "local-acceptance-summary.json"


def tracked_paths() -> tuple[Path, ...]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return tuple(ROOT / value.decode() for value in result.stdout.split(b"\0") if value)


def test_generated_and_private_artifacts_are_untracked() -> None:
    forbidden_parts = {".terraform", ".venv", "__pycache__", "artifacts", "data"}
    forbidden_suffixes = {
        ".csv",
        ".jsonl",
        ".parquet",
        ".plan",
        ".pyc",
        ".pyo",
        ".tfstate",
        ".zip",
    }
    failures = []
    for path in tracked_paths():
        relative = path.relative_to(ROOT)
        if (
            forbidden_parts.intersection(relative.parts)
            or path.suffix.lower() in forbidden_suffixes
        ):
            failures.append(relative.as_posix())
        if ".tfstate." in path.name or path.name == "terraform.tfstate":
            failures.append(relative.as_posix())
    assert failures == []


def test_tracked_files_are_small_text_contracts() -> None:
    failures = []
    for path in tracked_paths():
        content = path.read_bytes()
        if len(content) > 1_000_000 or b"\0" in content:
            failures.append(path.relative_to(ROOT).as_posix())
    assert failures == []


def test_sanitized_evidence_reconciles() -> None:
    raw = EVIDENCE_PATH.read_text(encoding="utf-8")
    evidence = json.loads(raw)
    assert evidence["scope"] == "local_only"
    assert evidence["source_rows"] == 1_516_948
    assert evidence["daily_inputs"] == 212
    assert evidence["monthly_outputs"] == 8
    assert evidence["quality"]["quarantine_rows"] == 0
    assert evidence["quality"]["within_month_gaps_over_60_seconds"] == 327
    assert evidence["quality"]["cross_month_gaps_over_60_seconds"] == 4
    assert evidence["quality"]["total_gaps_over_60_seconds"] == 327 + 4
    assert evidence["storage_verification"]["exact_normalized_schema"] is True
    assert evidence["storage_verification"]["timestamp_timezone"] is None
    assert evidence["storage_verification"]["all_column_chunks_snappy"] is True
    assert evidence["container_verification"]["pyarrow_version"] == "21.0.0"
    assert evidence["container_verification"]["image_size_bytes"] == 233_386_762
    assert evidence["aws_verification"]["deployed"] is False
    assert evidence["aws_verification"]["api_calls_performed"] is False

    drive_path = re.compile(r"(?i)\b[a-z]:[\\/]")
    user_profile = re.compile(r"(?i)(?:users[\\/]|/home/)[^\\/\s]+")
    secret_token = re.compile(
        r"(?i)(?:secret|token|password|access_key)\s*[:=]\s*['\"]?[A-Za-z0-9/+]{12,}"
    )
    assert drive_path.search(raw) is None
    assert user_profile.search(raw) is None
    assert secret_token.search(raw) is None


def test_readme_states_aws_deployment_boundary() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8").lower()
    assert "not deployed to aws" in readme
    assert "no real aws api" in readme
    assert "optional phase 9" in readme

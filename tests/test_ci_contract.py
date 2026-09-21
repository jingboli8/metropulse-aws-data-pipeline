from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "ci.yml"
WORKFLOW = WORKFLOW_PATH.read_text(encoding="utf-8")


def test_workflow_is_valid_yaml() -> None:
    parsed = yaml.safe_load(WORKFLOW)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "CI"
    assert "on" in parsed
    assert isinstance(parsed["jobs"], dict)


def test_ci_triggers_permissions_and_concurrency() -> None:
    assert re.search(r'(?m)^"on":$', WORKFLOW)
    assert re.search(r"(?m)^  pull_request:$", WORKFLOW)
    assert re.search(r"(?m)^  push:$", WORKFLOW)
    assert re.search(r"(?m)^      - main$", WORKFLOW)
    assert re.search(r"(?m)^  workflow_dispatch:$", WORKFLOW)
    assert re.search(r"(?ms)^permissions:\n  contents: read\n", WORKFLOW)
    assert "cancel-in-progress: true" in WORKFLOW
    assert "id-token:" not in WORKFLOW


def test_all_third_party_actions_use_reviewed_full_commit_shas() -> None:
    uses = re.findall(r"(?m)^\s*- uses: ([^\s]+)(?:\s+#.*)?$", WORKFLOW)
    assert uses
    for action in uses:
        assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action), action

    release_comments = {
        "actions/checkout": "v7.0.1",
        "actions/setup-python": "v7.0.0",
        "hashicorp/setup-terraform": "v4.0.1",
        "actions/cache": "v5.0.0",
    }
    for action, release in release_comments.items():
        assert f"# {action} {release}" in WORKFLOW


def test_python_matrix_and_strict_marker_contract() -> None:
    assert 'python-version: ["3.11", "3.12"]' in WORKFLOW
    assert 'pip install -c requirements-ci.txt -e ".[dev]"' in WORKFLOW
    assert '--strict-markers -m "not full_data"' in WORKFLOW
    assert "cache: pip" in WORKFLOW
    assert "pyproject.toml" in WORKFLOW
    assert "requirements-ci.txt" in WORKFLOW

    configuration = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert any(
        marker.startswith("full_data:")
        for marker in configuration["tool"]["pytest"]["ini_options"]["markers"]
    )


def test_terraform_contracts_cover_all_roots_without_live_operations() -> None:
    assert "terraform_version: 1.16.2" in WORKFLOW
    assert 'TF_IN_AUTOMATION: "1"' in WORKFLOW
    assert 'AWS_EC2_METADATA_DISABLED: "true"' in WORKFLOW
    assert "terraform fmt -check -recursive infra" in WORKFLOW
    assert "for root in bootstrap platform query operations" in WORKFLOW
    assert "init -backend=false -input=false -lockfile=readonly" in WORKFLOW
    assert " validate" in WORKFLOW
    assert " test" in WORKFLOW

    prohibited = (
        "terraform plan",
        "terraform apply",
        "aws-actions/configure-aws-credentials",
        "docker build",
        "docker run",
        "deployment:",
    )
    lowered = WORKFLOW.lower()
    assert [value for value in prohibited if value in lowered] == []


def test_powershell_is_parsed_and_never_executed() -> None:
    assert "windows-latest" in WORKFLOW
    assert "timeout-minutes: 5" in WORKFLOW
    assert "git ls-files '*.ps1'" in WORKFLOW
    assert "[System.Management.Automation.Language.Parser]::ParseFile" in WORKFLOW
    assert "verify_lambda_container.ps1" not in WORKFLOW


def test_public_license_and_evidence_exist() -> None:
    assert (ROOT / "LICENSE").is_file()
    assert (ROOT / "docs" / "portfolio-evidence.md").is_file()
    assert (ROOT / "docs" / "evidence" / "local-acceptance-summary.json").is_file()

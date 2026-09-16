"""Audit the final Lambda task directory without external Unix utilities."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

REQUIRED_TOP_LEVEL_ENTRIES = {
    "metropulse",
    "pyarrow",
    "pyarrow-21.0.0.dist-info",
}
OPTIONAL_TOP_LEVEL_ENTRIES = {
    "pyarrow.libs",
}
FORBIDDEN_DIRECTORIES = {
    "data",
    "artifacts",
    ".git",
    ".venv",
    "tests",
    ".aws",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "build",
    "dist",
}
FORBIDDEN_FILENAMES = {
    "credentials",
    "Dockerfile",
    ".dockerignore",
    "pyproject.toml",
    "requirements-lambda.txt",
}
FORBIDDEN_SUFFIXES = {".csv", ".zip", ".parquet", ".jsonl", ".pyc", ".pyo"}
FIRST_PARTY_TEXT_SUFFIXES = {".py", ".txt", ".json", ".toml", ".cfg"}
FIRST_PARTY_ROOTS = ("metropulse",)

FIRST_PARTY_LEAKAGE_PATTERNS = (
    (
        "Windows absolute path",
        re.compile(r"(?<![A-Za-z0-9_])[A-Za-z]:\\"),
    ),
    (
        "POSIX user-profile path",
        re.compile(r"(?<![A-Za-z0-9_])/(?:home|Users)/[A-Za-z0-9._-]+/"),
    ),
    (
        "AWS access key ID",
        re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    ),
    (
        "secret assignment",
        re.compile(
            r"(?i)\b(?:AWS_SECRET_ACCESS_KEY|SECRET_ACCESS_KEY|PASSWORD|TOKEN)"
            r"\s*[=:]\s*['\"]?[^\s'\"]{8,}"
        ),
    ),
    (
        "private key material",
        re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    ),
)


def audit_task_root(task_root: Path) -> int:
    """Run dependency, filesystem, and first-party leakage checks."""
    root = task_root.resolve(strict=True)
    audit_allowed_dependency_inventory(root)
    inspected = audit_filesystem_hygiene(root)
    for first_party_name in FIRST_PARTY_ROOTS:
        audit_first_party_content(root / first_party_name, task_root=root)
    return inspected


def audit_allowed_dependency_inventory(root: Path) -> None:
    """Reject missing or unexpected top-level runtime content."""
    actual = {path.name for path in root.iterdir()}
    allowed = REQUIRED_TOP_LEVEL_ENTRIES | OPTIONAL_TOP_LEVEL_ENTRIES
    missing = sorted(REQUIRED_TOP_LEVEL_ENTRIES - actual)
    unexpected = sorted(actual - allowed)
    if missing or unexpected:
        raise AssertionError(
            f"invalid top-level runtime inventory; missing={missing}, unexpected={unexpected}"
        )


def audit_filesystem_hygiene(root: Path) -> int:
    """Reject generated, project-only, or escaping content anywhere in the task tree."""
    inspected = 0
    for path in root.rglob("*"):
        inspected += 1
        relative = path.relative_to(root)
        resolved = path.resolve(strict=False)
        try:
            resolved.relative_to(root)
        except ValueError as error:
            raise AssertionError(f"path escapes Lambda task root: {path}") from error
        if any(part in FORBIDDEN_DIRECTORIES for part in relative.parts):
            raise AssertionError(f"forbidden directory in Lambda task root: {relative}")
        if path.is_file() and (
            path.name in FORBIDDEN_FILENAMES or path.suffix.lower() in FORBIDDEN_SUFFIXES
        ):
            raise AssertionError(f"forbidden file in Lambda task root: {relative}")
    return inspected


def audit_first_party_content(first_party_root: Path, *, task_root: Path) -> None:
    """Scan repository-owned runtime text for local paths and secret-like material."""
    resolved_first_party = first_party_root.resolve(strict=True)
    try:
        resolved_first_party.relative_to(task_root)
    except ValueError as error:
        raise AssertionError(
            f"first-party root escapes Lambda task root: {first_party_root}"
        ) from error
    for path in resolved_first_party.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in FIRST_PARTY_TEXT_SUFFIXES:
            continue
        relative = path.relative_to(task_root)
        text = path.read_text(encoding="utf-8", errors="strict")
        for finding, pattern in FIRST_PARTY_LEAKAGE_PATTERNS:
            if pattern.search(text):
                raise AssertionError(f"{finding} in first-party Lambda content: {relative}")


def main() -> None:
    """Audit the supplied task root and emit one stable success marker."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_root", type=Path)
    arguments = parser.parse_args()
    inspected = audit_task_root(arguments.task_root)
    print(f"TASK_CONTENT_OK inspected={inspected}")


if __name__ == "__main__":
    main()

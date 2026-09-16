from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).parents[1]
MARKDOWN_FILES = (
    ROOT / "README.md",
    *sorted((ROOT / "docs").rglob("*.md")),
    *sorted((ROOT / "sql").rglob("*.md")),
)
MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
TABLE_SEPARATOR = re.compile(r"^\|(?:\s*:?-{3,}:?\s*\|)+$")


def test_markdown_has_no_trailing_whitespace() -> None:
    failures = [
        f"{path.relative_to(ROOT)}:{line_number}"
        for path in MARKDOWN_FILES
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if line.endswith((" ", "\t"))
    ]
    assert failures == []


def test_local_markdown_links_resolve() -> None:
    failures: list[str] = []
    for path in MARKDOWN_FILES:
        for target in MARKDOWN_LINK.findall(path.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target_path = unquote(target.split("#", maxsplit=1)[0])
            if target_path and not (path.parent / target_path).resolve().exists():
                failures.append(f"{path.relative_to(ROOT)} -> {target}")
    assert failures == []


def test_markdown_tables_have_consistent_columns() -> None:
    failures: list[str] = []
    for path in MARKDOWN_FILES:
        lines = path.read_text(encoding="utf-8").splitlines()
        index = 0
        while index < len(lines):
            if not lines[index].startswith("|"):
                index += 1
                continue
            start = index
            block: list[str] = []
            while index < len(lines) and lines[index].startswith("|"):
                block.append(lines[index])
                index += 1
            if len(block) < 2 or TABLE_SEPARATOR.fullmatch(block[1]) is None:
                failures.append(f"{path.relative_to(ROOT)}:{start + 1} missing separator")
                continue
            column_counts = {line.count("|") - 1 for line in block}
            if len(column_counts) != 1:
                failures.append(f"{path.relative_to(ROOT)}:{start + 1} inconsistent columns")
    assert failures == []


def test_markdown_fences_are_balanced() -> None:
    failures: list[str] = []
    for path in MARKDOWN_FILES:
        open_line: int | None = None
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.startswith("```"):
                continue
            open_line = line_number if open_line is None else None
        if open_line is not None:
            failures.append(f"{path.relative_to(ROOT)}:{open_line}")
    assert failures == []

"""Remove non-runtime Python content from a bounded Lambda task directory."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

LAMBDA_TASK_ROOT = Path("/var/task")


def clean_task_root(task_root: Path, *, allowed_root: Path = LAMBDA_TASK_ROOT) -> None:
    """Remove tests and bytecode without traversing outside ``allowed_root``."""
    root = task_root.resolve(strict=True)
    boundary = allowed_root.resolve(strict=True)
    if root != boundary:
        raise ValueError(f"cleanup root must resolve exactly to {boundary}")

    pyarrow_tests = root / "pyarrow" / "tests"
    _remove_directory(pyarrow_tests, root)

    for cache_directory in tuple(root.rglob("__pycache__")):
        _remove_directory(cache_directory, root)

    for pattern in ("*.pyc", "*.pyo"):
        for bytecode_file in tuple(root.rglob(pattern)):
            _require_within(bytecode_file, root)
            if bytecode_file.is_symlink() or bytecode_file.is_file():
                bytecode_file.unlink(missing_ok=True)


def _remove_directory(path: Path, root: Path) -> None:
    _require_within(path, root)
    if path.is_symlink():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


def _require_within(path: Path, root: Path) -> None:
    try:
        path.resolve(strict=False).relative_to(root)
    except ValueError as error:
        raise ValueError(f"refusing to clean outside {root}: {path}") from error


def main() -> None:
    """Clean the fixed Lambda task root supplied by the Docker build."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_root", type=Path)
    arguments = parser.parse_args()
    clean_task_root(arguments.task_root)


if __name__ == "__main__":
    main()

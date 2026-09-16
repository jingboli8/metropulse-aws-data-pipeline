from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.audit_lambda_task import audit_task_root
from scripts.clean_lambda_task import clean_task_root

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra"


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _runtime_tree(tmp_path: Path, *, include_optional_libraries: bool = False) -> Path:
    task_root = tmp_path / "task"
    for directory in (
        "metropulse",
        "pyarrow",
        "pyarrow-21.0.0.dist-info",
    ):
        (task_root / directory).mkdir(parents=True)
    if include_optional_libraries:
        (task_root / "pyarrow.libs").mkdir()
    (task_root / "metropulse" / "__init__.py").write_text("", encoding="utf-8")
    return task_root


def test_docker_context_is_an_explicit_application_allowlist() -> None:
    patterns = {
        line.strip()
        for line in _read(".dockerignore").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert patterns == {
        "**",
        "!Dockerfile",
        "!requirements-lambda.txt",
        "!metropulse/",
        "!metropulse/**",
        "!scripts/",
        "!scripts/clean_lambda_task.py",
        "metropulse/**/__pycache__/",
        "metropulse/**/*.pyc",
        "metropulse/**/*.pyo",
    }


def test_lambda_container_is_immutable_amd64_ready_and_minimal() -> None:
    dockerfile = _read("Dockerfile")
    requirements = _read("requirements-lambda.txt")
    assert re.search(r"public\.ecr\.aws/lambda/python@sha256:[0-9a-f]{64}", dockerfile)
    assert ":latest" not in dockerfile
    assert "COPY ." not in dockerfile
    assert 'CMD ["metropulse.aws.lambda_handler.lambda_handler"]' in dockerfile
    assert "--require-hashes" in dockerfile
    assert "scripts/clean_lambda_task.py" in dockerfile
    assert re.search(r"\bfind\b", dockerfile) is None
    assert "# syntax=" not in dockerfile
    assert "COPY --from=dependencies /var/task/" in dockerfile
    assert "pyarrow==21.0.0" in requirements
    assert re.search(r"--hash=sha256:[0-9a-f]{64}", requirements)
    assert "duckdb" not in requirements
    assert "pytest" not in requirements
    assert "ruff" not in requirements


def test_lambda_cleanup_uses_bounded_python_standard_library(tmp_path: Path) -> None:
    task_root = tmp_path / "task"
    tests_directory = task_root / "pyarrow" / "tests"
    cache_directory = task_root / "metropulse" / "__pycache__"
    tests_directory.mkdir(parents=True)
    cache_directory.mkdir(parents=True)
    (tests_directory / "fixture.txt").write_text("fixture", encoding="utf-8")
    (cache_directory / "module.pyc").write_bytes(b"bytecode")
    (task_root / "orphan.pyo").write_bytes(b"bytecode")
    retained = task_root / "metropulse" / "module.py"
    retained.write_text("VALUE = 1\n", encoding="utf-8")

    clean_task_root(task_root, allowed_root=task_root)

    assert not tests_directory.exists()
    assert not cache_directory.exists()
    assert not (task_root / "orphan.pyo").exists()
    assert retained.read_text(encoding="utf-8") == "VALUE = 1\n"

    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(ValueError, match="cleanup root must resolve exactly"):
        clean_task_root(outside, allowed_root=task_root)


def test_third_party_pyarrow_path_literals_are_not_first_party_leaks(tmp_path: Path) -> None:
    task_root = _runtime_tree(tmp_path)
    parquet = task_root / "pyarrow" / "parquet"
    parquet.mkdir()
    (parquet / "core.py").write_text(
        'message = f"table:\\n{table} vs. \\nfile:\\n{schema}"\n'
        r'example = r"C:\data\example.parquet"' + "\n",
        encoding="utf-8",
    )

    assert audit_task_root(task_root) > 0


@pytest.mark.parametrize("include_optional_libraries", [False, True])
def test_runtime_inventory_accepts_optional_pyarrow_libraries(
    tmp_path: Path, include_optional_libraries: bool
) -> None:
    task_root = _runtime_tree(tmp_path, include_optional_libraries=include_optional_libraries)

    assert audit_task_root(task_root) > 0


@pytest.mark.parametrize(
    "required_entry",
    ["metropulse", "pyarrow", "pyarrow-21.0.0.dist-info"],
)
def test_runtime_inventory_rejects_missing_required_entry(
    tmp_path: Path, required_entry: str
) -> None:
    task_root = _runtime_tree(tmp_path)
    shutil.rmtree(task_root / required_entry)

    with pytest.raises(AssertionError, match=rf"missing=.*{re.escape(required_entry)}"):
        audit_task_root(task_root)


def test_windows_path_inside_first_party_code_fails(tmp_path: Path) -> None:
    task_root = _runtime_tree(tmp_path)
    (task_root / "metropulse" / "leak.py").write_text(
        r'LOCAL_PATH = r"C:\data\example.parquet"' + "\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="Windows absolute path in first-party"):
        audit_task_root(task_root)


@pytest.mark.parametrize(
    "leaked_path",
    [
        r"D:\portfolio\metropulse-aws-data-pipeline\data",
        r"C:\Users\local-user\Documents\metropulse",
        "/home/local-user/metropulse/data",
    ],
)
def test_machine_specific_first_party_path_fails(tmp_path: Path, leaked_path: str) -> None:
    task_root = _runtime_tree(tmp_path)
    (task_root / "metropulse" / "leak.py").write_text(
        f"LOCAL_PATH = {leaked_path!r}\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="path in first-party"):
        audit_task_root(task_root)


@pytest.mark.parametrize(
    "relative_path",
    [
        "pyarrow/tests/example.py",
        "metropulse/__pycache__/module.py",
        "pyarrow/generated.pyc",
        "pyarrow/generated.pyo",
    ],
)
def test_whole_tree_hygiene_still_rejects_forbidden_content(
    tmp_path: Path, relative_path: str
) -> None:
    task_root = _runtime_tree(tmp_path)
    forbidden = task_root / relative_path
    forbidden.parent.mkdir(parents=True, exist_ok=True)
    forbidden.write_bytes(b"forbidden")

    with pytest.raises(AssertionError, match="forbidden"):
        audit_task_root(task_root)


@pytest.mark.parametrize("entry_name", ["unexpected_package", "unexpected_runtime.py"])
def test_runtime_inventory_rejects_unexpected_top_level_entry(
    tmp_path: Path, entry_name: str
) -> None:
    task_root = _runtime_tree(tmp_path)
    unexpected = task_root / entry_name
    if unexpected.suffix:
        unexpected.write_text("VALUE = 1\n", encoding="utf-8")
    else:
        unexpected.mkdir()

    with pytest.raises(AssertionError, match=rf"unexpected=.*{re.escape(entry_name)}"):
        audit_task_root(task_root)


def test_terraform_defines_only_phase4_service_resources() -> None:
    phase4_roots = (INFRA / "bootstrap", INFRA / "platform")
    terraform = "\n".join(
        path.read_text(encoding="utf-8") for root in phase4_roots for path in root.rglob("*.tf")
    )
    resource_types = set(re.findall(r'^resource\s+"([^"]+)"', terraform, re.MULTILINE))
    assert resource_types == {
        "aws_cloudwatch_log_group",
        "aws_ecr_lifecycle_policy",
        "aws_ecr_repository",
        "aws_ecr_repository_policy",
        "aws_iam_role",
        "aws_iam_role_policy",
        "aws_lambda_function",
        "aws_lambda_function_event_invoke_config",
        "aws_lambda_permission",
        "aws_s3_bucket",
        "aws_s3_bucket_lifecycle_configuration",
        "aws_s3_bucket_notification",
        "aws_s3_bucket_ownership_controls",
        "aws_s3_bucket_policy",
        "aws_s3_bucket_public_access_block",
        "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_versioning",
    }


def test_lambda_iam_and_notification_are_narrow() -> None:
    iam = _read("infra/platform/iam.tf")
    lambda_config = _read("infra/platform/lambda.tf")
    assert '"s3:*"' not in iam
    assert '"logs:*"' not in iam
    assert 'Resource = "*"' not in iam
    assert '"s3:GetObjectVersion"' in iam
    assert '/raw/source=metropt3/*"' in iam
    for prefix in ("staging", "quarantine"):
        assert f'/{prefix}/source=metropt3/*"' in iam
    assert '/control/validation/*"' in iam
    assert 'filter_prefix       = "raw/source=metropt3/"' in lambda_config
    assert 'filter_suffix       = "data.csv"' in lambda_config
    assert "depends_on = [aws_lambda_permission.allow_data_lake]" in lambda_config


def test_platform_rejects_mutable_images_and_force_destroy_defaults_off() -> None:
    variables = _read("infra/platform/variables.tf")
    s3 = _read("infra/platform/s3.tf")
    assert "@sha256:[0-9a-f]{64}$" in variables
    assert re.search(
        r'variable "allow_force_destroy"\s*\{.*?default\s*=\s*false',
        variables,
        re.DOTALL,
    )
    assert s3.count("force_destroy = var.allow_force_destroy") == 2
    assert s3.count('sse_algorithm = "AES256"') == 2
    assert s3.count('object_ownership = "BucketOwnerEnforced"') == 2
    assert s3.count("block_public_acls       = true") == 2


def test_container_verifier_has_no_push_or_aws_command() -> None:
    script = _read("scripts/verify_lambda_container.ps1")
    assert re.search(r'"--platform",\s*"linux/amd64"', script)
    assert '"--load"' in script
    assert "& docker @buildArguments" in script
    assert "container_smoke.py" in script
    assert "audit_lambda_task.py" in script
    assert "pyarrow.__version__" in script
    assert "metropulse.aws.lambda_handler.lambda_handler" in script
    assert "HISTORY_SECRET_SCAN_OK" in script
    assert re.search(r"\bfind\b", script) is None
    assert "/bin/sh" not in script
    assert "LAMBDA_DEFAULT_USER_CONFIG_OK" in script
    assert "docker push" not in script.lower()
    assert not re.search(r"(?im)^\s*aws(?:\.exe)?\s", script)
    assert "CONTAINER_VERIFICATION_OK" in script


def test_container_verifier_gates_inspection_on_loaded_image() -> None:
    script = _read("scripts/verify_lambda_container.ps1")
    build = script.index("& docker @buildArguments")
    build_exit = script.index("$buildExitCode = $LASTEXITCODE", build)
    build_guard = script.index("Docker build failed with exit code", build_exit)
    image_lookup = script.index("$localImageId = & docker @imageListArguments", build_guard)
    missing_guard = script.index("tagged image '$ImageName' is unavailable", image_lookup)
    first_inspect = script.index("$imageId = & docker @inspectIdArguments", missing_guard)
    first_run = script.index("$versions = & docker @importArguments", first_inspect)

    assert build < build_exit < build_guard < image_lookup < missing_guard
    assert missing_guard < first_inspect < first_run


def test_container_verifier_stops_when_loaded_image_is_unavailable(tmp_path: Path) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is required to exercise the PowerShell verifier")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    call_log = tmp_path / "docker-calls.txt"
    (fake_bin / "docker.cmd").write_text(
        "@echo off\r\n"
        'echo %*>>"%FAKE_DOCKER_LOG%"\r\n'
        'if "%1"=="info" goto info\r\n'
        'if "%1"=="buildx" exit /b 0\r\n'
        'if "%1"=="image" if "%2"=="ls" exit /b 0\r\n'
        "exit /b 88\r\n"
        ":info\r\n"
        "echo linux/amd64\r\n"
        "exit /b 0\r\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["FAKE_DOCKER_LOG"] = str(call_log)

    completed = subprocess.run(
        [powershell, "-NoProfile", "-File", str(ROOT / "scripts/verify_lambda_container.ps1")],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "tagged image 'metropulse-lambda:phase4-local' is unavailable" in (
        completed.stdout + completed.stderr
    )
    calls = call_log.read_text(encoding="utf-8").splitlines()
    assert any(call.startswith("buildx build ") and "--load" in call for call in calls)
    assert any(call.startswith("image ls ") for call in calls)
    assert not any(call.startswith(("image inspect ", "run ", "history ")) for call in calls)

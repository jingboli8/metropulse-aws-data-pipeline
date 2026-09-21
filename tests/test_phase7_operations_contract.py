from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OPERATIONS = ROOT / "infra" / "operations"


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _terraform() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in OPERATIONS.glob("*.tf"))


def test_operations_root_is_explicit_and_has_no_remote_state() -> None:
    terraform = _terraform()
    assert "terraform_remote_state" not in terraform
    assert 'resource "aws_scheduler_schedule" "audit"' in terraform
    assert 'resource "aws_lambda_function" "compaction"' in terraform
    assert 'resource "aws_lambda_function" "audit"' in terraform
    assert 'resource "aws_cloudwatch_dashboard" "operations"' in terraform
    assert 'resource "aws_cloudwatch_metric_alarm"' in terraform
    assert "aws_cloudwatch_event" not in terraform
    assert "aws_sqs" not in terraform
    assert "aws_sns" not in terraform
    assert "aws_sfn" not in terraform


def test_schedule_is_weekly_audit_only_and_disabled_by_default() -> None:
    scheduler = _read("infra/operations/scheduler.tf")
    variables = _read("infra/operations/variables.tf")
    assert 'default     = "cron(0 6 ? * MON *)"' in variables
    assert re.search(
        r'variable "audit_schedule_enabled"\s*\{.*?default\s*=\s*false',
        variables,
        re.DOTALL,
    )
    assert (
        'state                        = var.audit_schedule_enabled ? "ENABLED" : "DISABLED"'
        in scheduler
    )
    assert 'schedule_expression_timezone = "UTC"' in scheduler
    assert "metropulse-scheduled-audit-invocation-v1" in scheduler
    assert "compaction" not in scheduler.lower()
    assert "maximum_event_age_in_seconds = 3600" in scheduler
    assert "maximum_retry_attempts       = 2" in scheduler


def test_misleading_heartbeat_is_omitted_and_failure_metrics_ignore_absence() -> None:
    observability = _read("infra/operations/observability.tf")
    assert "audit_heartbeat" not in observability
    assert "AuditRunsSucceeded" not in observability
    assert observability.count('treat_missing_data  = "notBreaching"') >= 4


def test_operations_iam_is_separate_narrow_and_has_no_list_or_metric_api() -> None:
    iam = _read("infra/operations/iam.tf")
    assert 'resource "aws_iam_role" "compaction"' in iam
    assert 'resource "aws_iam_role" "audit"' in iam
    assert 'resource "aws_iam_role" "scheduler"' in iam
    for forbidden in (
        "s3:ListBucket",
        "s3:*",
        "logs:*",
        "cloudwatch:PutMetricData",
        "athena:",
        "glue:DeletePartition",
        "ecr:",
    ):
        assert forbidden not in iam
    assert re.search(r'Action\s*=\s*\["s3:GetObject", "s3:GetObjectVersion"\]', iam)
    assert re.search(r'Action\s*=\s*\["s3:PutObject"\]', iam)
    assert re.search(r'Action\s*=\s*\["glue:GetTable", "glue:GetPartition"\]', iam)
    assert "glue:CreatePartition" in iam
    assert "glue:UpdatePartition" in iam
    scheduler_policy = iam.split('resource "aws_iam_role_policy" "scheduler"', 1)[1]
    assert 'Action   = ["lambda:InvokeFunction"]' in scheduler_policy
    assert "aws_lambda_function.audit.arn" in scheduler_policy
    assert "aws_lambda_function.compaction.arn" not in scheduler_policy

    audit_policy = iam.split('resource "aws_iam_role_policy" "audit"', 1)[1].split(
        'data "aws_iam_policy_document" "scheduler_assume"', 1
    )[0]
    assert "s3:PutObject" not in audit_policy
    assert "glue:CreatePartition" not in audit_policy
    assert "glue:UpdatePartition" not in audit_policy


def test_approval_and_audit_code_never_list_or_choose_s3_objects() -> None:
    sources = "\n".join(
        _read(path)
        for path in (
            "metropulse/selection_approval.py",
            "metropulse/audit_inventory.py",
            "metropulse/scheduled_audit.py",
            "metropulse/aws/audit_processor.py",
            "metropulse/aws/compaction_lambda_handler.py",
        )
    )
    assert ".list_objects" not in sources
    assert ".list_objects_v2" not in sources
    assert "list_objects(" not in sources


def test_operations_metrics_have_only_bounded_dimensions() -> None:
    source = _read("metropulse/aws/operations_observability.py")
    assert '"Environment": self.environment' in source
    assert '"ComponentVersion": self.component_version' in source
    dimensions = re.search(r'"Dimensions": \[(.*?)\]', source, re.DOTALL)
    assert dimensions is not None
    assert set(re.findall(r'"([A-Za-z]+)"', dimensions.group(1))) == {
        "Environment",
        "ComponentVersion",
    }
    for forbidden in ("RunId", "InventoryId", "ObjectKey", "RequestId", "SourceDate"):
        assert forbidden not in source


def test_control_lifecycle_retains_current_evidence() -> None:
    s3 = _read("infra/platform/s3.tf")
    block = re.search(r'rule \{\s+id\s*=\s*"control-operational"(.*?)\n  \}', s3, re.DOTALL)
    assert block is not None
    assert re.search(r"(?m)^\s+expiration\s+\{", block.group(1)) is None
    assert "noncurrent_version_expiration" in block.group(1)
    assert "staging_retention_days" in s3


def test_operations_root_excludes_unrequested_services() -> None:
    terraform = _terraform().lower()
    for forbidden in (
        "redshift",
        "kinesis",
        "kafka",
        "airflow",
        "emr",
        "quicksight",
        "sagemaker",
        "stepfunctions",
        "aws_glue_crawler",
        "aws_athena",
    ):
        assert forbidden not in terraform

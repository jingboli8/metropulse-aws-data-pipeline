from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
QUERY_ROOT = ROOT / "infra" / "query"
SQL_ROOT = ROOT / "sql"

EXPECTED_COLUMNS = [
    ("record_index", "bigint"),
    ("event_timestamp_local", "timestamp"),
    ("tp2", "double"),
    ("tp3", "double"),
    ("h1", "double"),
    ("dv_pressure", "double"),
    ("reservoirs", "double"),
    ("oil_temperature", "double"),
    ("motor_current", "double"),
    ("comp", "boolean"),
    ("dv_electric", "boolean"),
    ("towers", "boolean"),
    ("mpg", "boolean"),
    ("lps", "boolean"),
    ("pressure_switch", "boolean"),
    ("oil_level", "boolean"),
    ("caudal_impulses", "boolean"),
    ("source_date", "date"),
    ("event_timestamp_timezone_status", "string"),
]
ROUTINE_SQL = (
    "row_counts_by_date.sql",
    "analogue_sensor_aggregates.sql",
    "digital_state_rates.sql",
)


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _query_terraform() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in QUERY_ROOT.glob("*.tf"))


def test_glue_database_and_table_names_are_exact() -> None:
    locals_tf = _read("infra/query/locals.tf")
    catalog = _read("infra/query/catalog.tf")

    assert re.search(
        r'database_name\s*=\s*"metropulse_\$\{var\.environment\}"',
        locals_tf,
    )
    assert 'table_name     = "metropt3_curated"' in locals_tf
    assert "name        = local.database_name" in catalog
    assert "name          = local.table_name" in catalog


def test_glue_columns_and_partitions_match_contract_order() -> None:
    catalog = _read("infra/query/catalog.tf")
    columns = re.findall(
        r"(?ms)^    columns \{\s+name\s*=\s*\"([^\"]+)\"\s+type\s*=\s*\"([^\"]+)\"",
        catalog,
    )
    partitions = re.findall(
        r"(?ms)^  partition_keys \{\s+name\s*=\s*\"([^\"]+)\"\s+type\s*=\s*\"([^\"]+)\"",
        catalog,
    )

    assert columns == EXPECTED_COLUMNS
    assert partitions == [("year", "string"), ("month", "string")]


def test_glue_table_is_curated_external_snappy_parquet_without_projection() -> None:
    catalog = _read("infra/query/catalog.tf")

    assert 'table_type    = "EXTERNAL_TABLE"' in catalog
    assert (
        'location                  = "s3://${var.data_lake_bucket_name}/curated/metropt3/"'
        in catalog
    )
    assert 'classification       = "parquet"' in catalog
    assert 'compressionType      = "snappy"' in catalog
    assert '"projection.enabled" = "false"' in catalog
    assert 'typeOfData           = "file"' in catalog
    assert "ParquetHiveSerDe" in catalog
    assert "MapredParquetInputFormat" in catalog
    assert "MapredParquetOutputFormat" in catalog
    for forbidden in ("/raw/", "/staging/", "/quarantine/", "/control/"):
        assert forbidden not in catalog


def test_query_root_defines_only_catalog_table_and_workgroup() -> None:
    terraform = _query_terraform()
    resource_types = set(re.findall(r'^resource\s+"([^"]+)"', terraform, re.MULTILINE))

    assert resource_types == {
        "aws_athena_workgroup",
        "aws_glue_catalog_database",
        "aws_glue_catalog_table",
    }
    assert "aws_glue_crawler" not in terraform
    assert 'resource "aws_glue_partition"' not in terraform
    assert "storage.location.template" not in terraform
    assert "aws_athena_named_query" not in terraform
    assert "terraform_remote_state" not in terraform


def test_workgroup_enforces_engine_results_encryption_and_cost_controls() -> None:
    athena = _read("infra/query/athena.tf")
    variables = _read("infra/query/variables.tf")
    locals_tf = _read("infra/query/locals.tf")

    assert "enforce_workgroup_configuration    = true" in athena
    assert 'selected_engine_version = "Athena engine version 3"' in athena
    assert "publish_cloudwatch_metrics_enabled = true" in athena
    assert "requester_pays_enabled             = false" in athena
    assert 'encryption_option = "SSE_S3"' in athena
    assert "expected_bucket_owner = var.athena_results_bucket_expected_owner" in athena
    assert (
        'output_location       = "s3://${var.athena_results_bucket_name}/${local.result_prefix}"'
        in athena
    )
    assert 'result_prefix  = "workgroups/${local.workgroup_name}/"' in locals_tf
    assert re.search(
        r'variable "bytes_scanned_cutoff_per_query"\s*\{.*?default\s*=\s*268435456',
        variables,
        re.DOTALL,
    )
    assert "var.bytes_scanned_cutoff_per_query >= 10000000" in variables
    assert 'can(regex("^[0-9]{12}$", var.athena_results_bucket_expected_owner))' in variables


def test_query_resources_use_standard_tags() -> None:
    locals_tf = _read("infra/query/locals.tf")
    athena = _read("infra/query/athena.tf")

    for tag in ("Project", "Environment", "ManagedBy", "Owner", "Purpose"):
        assert re.search(rf"^\s*{tag}\s*=", locals_tf, re.MULTILINE)
    assert "tags = local.common_tags" in athena


@pytest.mark.parametrize("sql_file", ROUTINE_SQL)
def test_routine_queries_use_explicit_columns_and_month_partition_filters(
    sql_file: str,
) -> None:
    sql = (SQL_ROOT / sql_file).read_text(encoding="utf-8")

    assert re.search(r"(?i)\bselect\s+\*", sql) is None
    assert re.search(r"(?i)\byear\s*=", sql)
    assert re.search(r"(?i)\bmonth\s*=", sql)
    assert "metropulse_dev.metropt3_curated" in sql


def test_gap_query_preserves_complete_history_continuity() -> None:
    sql = _read("sql/sampling_gaps.sql")

    assert re.search(r"(?i)\blag\s*\(event_timestamp_local\)\s+over", sql)
    assert "ORDER BY event_timestamp_local, record_index" in sql
    assert re.search(r"(?i)date_diff\s*\(\s*'second'", sql)
    assert "interval_seconds > 60" in sql
    assert "complete-history scan" in sql
    assert re.search(r"(?i)\byear\s*=|\bmonth\s*=", sql) is None
    assert "imputed" in sql


def test_sql_avoids_wildcards_timezone_claims_and_unsupported_labels() -> None:
    sql_text = "\n".join(path.read_text(encoding="utf-8") for path in SQL_ROOT.glob("*.sql"))
    lowered = sql_text.lower()

    assert re.search(r"(?i)\bselect\s+\*", sql_text) is None
    assert " utc" not in lowered
    assert "failure" not in lowered
    assert "defect" not in lowered


def test_phase5_excludes_orchestration_compute_alarms_and_other_platforms() -> None:
    terraform = _query_terraform().lower()
    forbidden = (
        "aws_cloudwatch_metric_alarm",
        "aws_lambda_function",
        "aws_scheduler_schedule",
        "aws_cloudwatch_event",
        "aws_glue_crawler",
        "aws_glue_partition",
        "aws_redshift",
        "aws_kinesis",
        "aws_emr",
        "aws_sagemaker",
        "aws_quicksight",
        "kafka",
        "airflow",
        "dbt",
        "machine learning",
    )

    for value in forbidden:
        assert value not in terraform

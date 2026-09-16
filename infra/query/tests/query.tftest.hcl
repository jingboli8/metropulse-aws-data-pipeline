mock_provider "aws" {}

variables {
  data_lake_bucket_name                = "metropulse-dev-data-example"
  athena_results_bucket_name           = "metropulse-dev-query-example"
  athena_results_bucket_expected_owner = "000000000000"
}

run "curated_query_contract" {
  command = plan

  assert {
    condition     = aws_glue_catalog_database.query.name == "metropulse_dev"
    error_message = "Glue database name must be environment-scoped."
  }

  assert {
    condition = (
      aws_glue_catalog_table.curated.database_name == "metropulse_dev" &&
      aws_glue_catalog_table.curated.name == "metropt3_curated" &&
      aws_glue_catalog_table.curated.table_type == "EXTERNAL_TABLE"
    )
    error_message = "Glue must define the exact curated external table."
  }

  assert {
    condition = [
      for column in aws_glue_catalog_table.curated.storage_descriptor[0].columns :
      "${column.name}:${column.type}"
      ] == [
      "record_index:bigint",
      "event_timestamp_local:timestamp",
      "tp2:double",
      "tp3:double",
      "h1:double",
      "dv_pressure:double",
      "reservoirs:double",
      "oil_temperature:double",
      "motor_current:double",
      "comp:boolean",
      "dv_electric:boolean",
      "towers:boolean",
      "mpg:boolean",
      "lps:boolean",
      "pressure_switch:boolean",
      "oil_level:boolean",
      "caudal_impulses:boolean",
      "source_date:date",
      "event_timestamp_timezone_status:string"
    ]
    error_message = "Glue column order and types must exactly match the Parquet contract."
  }

  assert {
    condition = [
      for partition in aws_glue_catalog_table.curated.partition_keys :
      "${partition.name}:${partition.type}"
    ] == ["year:string", "month:string"]
    error_message = "Only year and month may be query partitions."
  }

  assert {
    condition = (
      aws_glue_catalog_table.curated.storage_descriptor[0].location == "s3://metropulse-dev-data-example/curated/metropt3/" &&
      aws_glue_catalog_table.curated.parameters["classification"] == "parquet" &&
      aws_glue_catalog_table.curated.parameters["compressionType"] == "snappy" &&
      aws_glue_catalog_table.curated.parameters["projection.enabled"] == "false" &&
      aws_glue_catalog_table.curated.storage_descriptor[0].ser_de_info[0].serialization_library == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    )
    error_message = "The curated table must use explicit Snappy Parquet metadata without projection."
  }

  assert {
    condition = (
      aws_athena_workgroup.query.name == "metropulse-dev-analytics" &&
      aws_athena_workgroup.query.configuration[0].enforce_workgroup_configuration &&
      aws_athena_workgroup.query.configuration[0].publish_cloudwatch_metrics_enabled &&
      !aws_athena_workgroup.query.configuration[0].requester_pays_enabled &&
      aws_athena_workgroup.query.configuration[0].bytes_scanned_cutoff_per_query == 268435456 &&
      aws_athena_workgroup.query.configuration[0].engine_version[0].selected_engine_version == "Athena engine version 3"
    )
    error_message = "Athena workgroup execution and cost controls are incomplete."
  }

  assert {
    condition = (
      aws_athena_workgroup.query.configuration[0].result_configuration[0].output_location == "s3://metropulse-dev-query-example/workgroups/metropulse-dev-analytics/" &&
      aws_athena_workgroup.query.configuration[0].result_configuration[0].expected_bucket_owner == "000000000000" &&
      aws_athena_workgroup.query.configuration[0].result_configuration[0].encryption_configuration[0].encryption_option == "SSE_S3"
    )
    error_message = "Athena results must use the enforced encrypted workgroup prefix."
  }

  assert {
    condition = alltrue([
      for name in ["Project", "Environment", "ManagedBy", "Owner", "Purpose"] :
      contains(keys(local.common_tags), name)
    ])
    error_message = "Required inventory and cost-allocation tags must be defined."
  }
}

run "reject_too_small_scan_cutoff" {
  command = plan

  variables {
    bytes_scanned_cutoff_per_query = 9999999
  }

  expect_failures = [var.bytes_scanned_cutoff_per_query]
}

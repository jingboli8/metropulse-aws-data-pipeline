resource "aws_cloudwatch_log_group" "compaction" {
  name              = "/aws/lambda/${local.compaction_function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "audit" {
  name              = "/aws/lambda/${local.audit_function_name}"
  retention_in_days = var.log_retention_days
}

locals {
  operations_environment = {
    METROPULSE_AUDIT_TMP                   = "/tmp/metropulse-audit"
    METROPULSE_COMPONENT_VERSION           = var.component_version
    METROPULSE_DESTINATION_BUCKET          = var.data_lake_bucket_name
    METROPULSE_ENVIRONMENT                 = var.environment
    METROPULSE_GLUE_DATABASE               = var.glue_database_name
    METROPULSE_GLUE_TABLE                  = var.glue_table_name
    METROPULSE_MAX_AUDIT_MONTHS            = "24"
    METROPULSE_MAX_AUDIT_OBJECT_BYTES      = tostring(128 * 1024 * 1024)
    METROPULSE_MAX_AUDIT_ROWS              = "2000000"
    METROPULSE_MAX_AUDIT_TOTAL_BYTES       = tostring(256 * 1024 * 1024)
    METROPULSE_MAX_COMPACTION_INPUT_BYTES  = tostring(128 * 1024 * 1024)
    METROPULSE_MAX_COMPACTION_OUTPUT_BYTES = tostring(128 * 1024 * 1024)
    METROPULSE_MAX_COMPACTION_ROWS         = "500000"
    METROPULSE_MAX_SELECTED_OBJECTS        = "31"
  }
}

resource "aws_lambda_function" "compaction" {
  function_name = local.compaction_function_name
  role          = aws_iam_role.compaction.arn
  package_type  = "Image"
  image_uri     = var.lambda_image_uri
  architectures = ["x86_64"]

  memory_size                    = 2048
  timeout                        = 300
  reserved_concurrent_executions = 1
  ephemeral_storage { size = 1024 }
  image_config { command = ["metropulse.aws.compaction_lambda_handler.lambda_handler"] }
  environment { variables = local.operations_environment }

  depends_on = [aws_cloudwatch_log_group.compaction, aws_iam_role_policy.compaction]
}

resource "aws_lambda_function" "audit" {
  function_name = local.audit_function_name
  role          = aws_iam_role.audit.arn
  package_type  = "Image"
  image_uri     = var.lambda_image_uri
  architectures = ["x86_64"]

  memory_size                    = 1024
  timeout                        = 300
  reserved_concurrent_executions = 1
  ephemeral_storage { size = 512 }
  image_config { command = ["metropulse.aws.audit_lambda_handler.lambda_handler"] }
  environment { variables = local.operations_environment }

  depends_on = [aws_cloudwatch_log_group.audit, aws_iam_role_policy.audit]
}

resource "aws_lambda_function_event_invoke_config" "audit" {
  function_name                = aws_lambda_function.audit.function_name
  maximum_event_age_in_seconds = 3600
  maximum_retry_attempts       = 2
}

resource "aws_cloudwatch_log_group" "validation" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "validation" {
  function_name = local.function_name
  role          = aws_iam_role.validation.arn
  package_type  = "Image"
  image_uri     = var.lambda_image_uri
  architectures = ["x86_64"]

  memory_size                    = var.lambda_memory_mb
  timeout                        = var.lambda_timeout_seconds
  reserved_concurrent_executions = var.reserved_concurrency

  ephemeral_storage {
    size = var.lambda_ephemeral_storage_mb
  }

  image_config {
    command = ["metropulse.aws.lambda_handler.lambda_handler"]
  }

  environment {
    variables = {
      METROPULSE_CONTROL_PREFIX       = "control"
      METROPULSE_DESTINATION_BUCKET   = aws_s3_bucket.data_lake.id
      METROPULSE_ENVIRONMENT          = var.environment
      METROPULSE_EXPECTED_SOURCE_NAME = "metropt3"
      METROPULSE_MAXIMUM_INPUT_BYTES  = tostring(var.maximum_input_bytes)
      METROPULSE_PIPELINE_VERSION     = var.pipeline_version
      METROPULSE_QUARANTINE_PREFIX    = "quarantine"
      METROPULSE_RAW_PREFIX           = "raw"
      METROPULSE_STAGING_PREFIX       = "staging"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.validation,
    aws_iam_role_policy.validation
  ]
}

resource "aws_lambda_function_event_invoke_config" "validation" {
  function_name                = aws_lambda_function.validation.function_name
  maximum_event_age_in_seconds = 3600
  maximum_retry_attempts       = 2
}

resource "aws_lambda_permission" "allow_data_lake" {
  statement_id  = "AllowDataLakeObjectCreated"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.validation.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake.arn
}

resource "aws_s3_bucket_notification" "raw_validation" {
  bucket = aws_s3_bucket.data_lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.validation.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/source=metropt3/"
    filter_suffix       = "data.csv"
  }

  depends_on = [aws_lambda_permission.allow_data_lake]
}

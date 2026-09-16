mock_provider "aws" {
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::000000000000:role/mock-lambda-role"
      id  = "mock-lambda-role"
    }
  }

  mock_resource "aws_lambda_function" {
    defaults = {
      arn           = "arn:aws:lambda:eu-west-1:000000000000:function:mock-validation"
      function_name = "mock-validation"
    }
  }

  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:eu-west-1:000000000000:log-group:/aws/lambda/mock-validation"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-metropulse-bucket"
      id  = "mock-metropulse-bucket"
    }
  }
}

variables {
  data_lake_bucket_name      = "metropulse-dev-data-example"
  athena_results_bucket_name = "metropulse-dev-query-example"
  lambda_image_uri           = "000000000000.dkr.ecr.eu-west-1.amazonaws.com/metropulse@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}

run "secure_platform_contract" {
  command = apply

  assert {
    condition = (
      aws_s3_bucket_public_access_block.data_lake.block_public_acls &&
      aws_s3_bucket_public_access_block.data_lake.block_public_policy &&
      aws_s3_bucket_public_access_block.data_lake.ignore_public_acls &&
      aws_s3_bucket_public_access_block.data_lake.restrict_public_buckets &&
      aws_s3_bucket_public_access_block.athena_results.block_public_acls &&
      aws_s3_bucket_public_access_block.athena_results.block_public_policy &&
      aws_s3_bucket_public_access_block.athena_results.ignore_public_acls &&
      aws_s3_bucket_public_access_block.athena_results.restrict_public_buckets
    )
    error_message = "Both buckets must block every form of public access."
  }

  assert {
    condition     = aws_s3_bucket_versioning.data_lake.versioning_configuration[0].status == "Enabled"
    error_message = "The data-lake bucket must enable versioning."
  }

  assert {
    condition = alltrue([
      for id in [
        "raw-version-retention",
        "staging-temporary",
        "quarantine-diagnostic",
        "control-operational",
        "curated-version-retention"
        ] : contains(
        [for rule in aws_s3_bucket_lifecycle_configuration.data_lake.rule : rule.id],
        id
      )
    ])
    error_message = "Every documented data-lake zone must have an explicit lifecycle rule."
  }

  assert {
    condition = (
      one(one(aws_s3_bucket_server_side_encryption_configuration.data_lake.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256" &&
      one(one(aws_s3_bucket_server_side_encryption_configuration.athena_results.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"
    )
    error_message = "Both buckets must have default encryption."
  }

  assert {
    condition = (
      strcontains(aws_s3_bucket_policy.data_lake_tls.policy, "aws:SecureTransport") &&
      strcontains(aws_s3_bucket_policy.athena_results_tls.policy, "aws:SecureTransport")
    )
    error_message = "Both buckets must reject non-TLS access."
  }

  assert {
    condition = (
      aws_s3_bucket_notification.raw_validation.lambda_function[0].filter_prefix == "raw/source=metropt3/" &&
      aws_s3_bucket_notification.raw_validation.lambda_function[0].filter_suffix == "data.csv"
    )
    error_message = "S3 notification must be limited to canonical raw daily CSV keys."
  }

  assert {
    condition = (
      aws_lambda_function.validation.package_type == "Image" &&
      aws_lambda_function.validation.image_uri == var.lambda_image_uri &&
      aws_lambda_function.validation.image_config[0].command[0] == "metropulse.aws.lambda_handler.lambda_handler" &&
      aws_lambda_function.validation.architectures[0] == "x86_64"
    )
    error_message = "Lambda must use the digest-pinned amd64 image and configured handler."
  }

  assert {
    condition     = aws_lambda_function.validation.reserved_concurrent_executions == 2
    error_message = "Development reserved concurrency must stay small."
  }

  assert {
    condition     = aws_cloudwatch_log_group.validation.retention_in_days == 14
    error_message = "CloudWatch logs must have finite retention."
  }

  assert {
    condition = alltrue([
      for name in [
        "METROPULSE_EXPECTED_SOURCE_NAME",
        "METROPULSE_RAW_PREFIX",
        "METROPULSE_STAGING_PREFIX",
        "METROPULSE_QUARANTINE_PREFIX",
        "METROPULSE_CONTROL_PREFIX",
        "METROPULSE_PIPELINE_VERSION",
        "METROPULSE_MAXIMUM_INPUT_BYTES",
        "METROPULSE_ENVIRONMENT",
        "METROPULSE_DESTINATION_BUCKET"
      ] : contains(keys(aws_lambda_function.validation.environment[0].variables), name)
    ])
    error_message = "Lambda environment must satisfy the Phase 3 configuration contract."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.validation.policy, "\"s3:*\"")
    error_message = "Lambda execution IAM must not contain s3:* permissions."
  }

  assert {
    condition = alltrue([
      for name in ["Project", "Environment", "ManagedBy", "Owner", "Purpose"] :
      contains(keys(local.common_tags), name)
    ])
    error_message = "Required inventory and cost-allocation tags must be defined."
  }
}

run "reject_mutable_image_tag" {
  command = plan

  variables {
    lambda_image_uri = "000000000000.dkr.ecr.eu-west-1.amazonaws.com/metropulse:latest"
  }

  expect_failures = [var.lambda_image_uri]
}

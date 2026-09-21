mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::000000000000:role/mock-operations-role"
      id  = "mock-operations-role"
    }
  }

  mock_resource "aws_lambda_function" {
    defaults = {
      arn           = "arn:aws:lambda:eu-west-1:000000000000:function:mock-operations"
      function_name = "mock-operations"
    }
  }

  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:eu-west-1:000000000000:log-group:/aws/lambda/mock-operations"
    }
  }
}

variables {
  aws_account_id           = "000000000000"
  data_lake_bucket_name    = "metropulse-dev-data-example"
  lambda_image_uri         = "000000000000.dkr.ecr.eu-west-1.amazonaws.com/metropulse@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  validation_function_name = "metropulse-dev-validate-raw"
}

run "disabled_static_source_operations" {
  command = plan

  assert {
    condition = (
      aws_lambda_function.compaction.memory_size == 2048 &&
      aws_lambda_function.compaction.timeout == 300 &&
      aws_lambda_function.compaction.ephemeral_storage[0].size == 1024 &&
      aws_lambda_function.compaction.reserved_concurrent_executions == 1 &&
      aws_lambda_function.compaction.image_config[0].command[0] == "metropulse.aws.compaction_lambda_handler.lambda_handler"
    )
    error_message = "The on-demand compaction Lambda contract changed."
  }

  assert {
    condition = (
      aws_lambda_function.audit.memory_size == 1024 &&
      aws_lambda_function.audit.timeout == 300 &&
      aws_lambda_function.audit.ephemeral_storage[0].size == 512 &&
      aws_lambda_function.audit.reserved_concurrent_executions == 1 &&
      aws_lambda_function.audit.image_config[0].command[0] == "metropulse.aws.audit_lambda_handler.lambda_handler"
    )
    error_message = "The scheduled audit Lambda contract changed."
  }

  assert {
    condition = (
      aws_scheduler_schedule.audit.state == "DISABLED" &&
      aws_scheduler_schedule.audit.schedule_expression == "cron(0 6 ? * MON *)" &&
      aws_scheduler_schedule.audit.schedule_expression_timezone == "UTC" &&
      aws_scheduler_schedule.audit.flexible_time_window[0].mode == "OFF" &&
      aws_scheduler_schedule.audit.target[0].retry_policy[0].maximum_event_age_in_seconds == 3600 &&
      aws_scheduler_schedule.audit.target[0].retry_policy[0].maximum_retry_attempts == 2
    )
    error_message = "The weekly audit schedule must default to disabled with bounded retries."
  }

  assert {
    condition = (
      aws_lambda_function_event_invoke_config.audit.maximum_event_age_in_seconds == 3600 &&
      aws_lambda_function_event_invoke_config.audit.maximum_retry_attempts == 2
    )
    error_message = "Audit asynchronous retries must remain bounded."
  }

  assert {
    condition = (
      aws_cloudwatch_log_group.compaction.retention_in_days == 14 &&
      aws_cloudwatch_log_group.audit.retention_in_days == 14
    )
    error_message = "Operations logs require finite retention."
  }
}

run "enabled_schedule_requires_exact_inventory" {
  command = plan

  variables {
    audit_schedule_enabled         = true
    audit_inventory_bucket         = "metropulse-dev-data-example"
    audit_inventory_key            = "control/audit/source=metropt3/inventory_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/inventory.json"
    audit_inventory_identity_kind  = "sha256"
    audit_inventory_identity_value = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }

  assert {
    condition     = aws_scheduler_schedule.audit.state == "ENABLED"
    error_message = "Explicitly enabling a pinned audit inventory must enable the schedule."
  }

}

run "reject_enabled_schedule_without_inventory" {
  command = plan

  variables {
    audit_schedule_enabled = true
  }

  expect_failures = [aws_scheduler_schedule.audit]
}

locals {
  lambda_functions = {
    validation = var.validation_function_name
    compaction = aws_lambda_function.compaction.function_name
    audit      = aws_lambda_function.audit.function_name
  }
  operations_failure_metrics = {
    CompactionReconciliationFailures = "Compaction reconciliation failed"
    ImmutableOutputConflicts         = "Immutable compaction output conflicted"
    PublicationConflicts             = "Compaction publication conflicted"
    AuditRowReconciliationFailures   = "Audit row reconciliation failed"
    MissingPublications              = "Audit found missing publication"
    PublicationDrift                 = "Audit found catalog publication drift"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = local.lambda_functions
  alarm_name          = "${var.project_name}-${var.environment}-${each.key}-errors"
  alarm_description   = "${each.key} Lambda reported an execution error."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = each.value }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  for_each            = local.lambda_functions
  alarm_name          = "${var.project_name}-${var.environment}-${each.key}-throttles"
  alarm_description   = "${each.key} Lambda was throttled."
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  dimensions          = { FunctionName = each.value }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "operations_failures" {
  for_each            = local.operations_failure_metrics
  alarm_name          = "${var.project_name}-${var.environment}-${each.key}"
  alarm_description   = each.value
  namespace           = "MetroPulse/DataPipeline"
  metric_name         = each.key
  dimensions          = { Environment = var.environment, ComponentVersion = var.component_version }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "validation_reconciliation" {
  alarm_name          = "${var.project_name}-${var.environment}-validation-reconciliation"
  alarm_description   = "Daily validation reconciliation failed."
  namespace           = "MetroPulse/DataPipeline"
  metric_name         = "ReconciliationFailures"
  dimensions          = { Environment = var.environment, PipelineVersion = var.validation_pipeline_version }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "quarantine_rate" {
  alarm_name          = "${var.project_name}-${var.environment}-quarantine-rate"
  alarm_description   = "Daily validation quarantine rate exceeded the reviewed threshold."
  namespace           = "MetroPulse/DataPipeline"
  metric_name         = "QuarantineRate"
  dimensions          = { Environment = var.environment, PipelineVersion = var.validation_pipeline_version }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.quarantine_rate_alarm_percent
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_dashboard" "operations" {
  dashboard_name = "${var.project_name}-${var.environment}-operations"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Lambda errors and throttles"
          region = var.aws_region
          stat   = "Sum"
          period = 300
          metrics = flatten([
            for name in values(local.lambda_functions) : [
              ["AWS/Lambda", "Errors", "FunctionName", name],
              ["AWS/Lambda", "Throttles", "FunctionName", name]
            ]
          ])
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Validation, compaction, and audit integrity"
          region = var.aws_region
          stat   = "Sum"
          period = 300
          metrics = [
            ["MetroPulse/DataPipeline", "ReconciliationFailures", "Environment", var.environment, "PipelineVersion", var.validation_pipeline_version],
            ["MetroPulse/DataPipeline", "PublicationConflicts", "Environment", var.environment, "ComponentVersion", var.component_version],
            ["MetroPulse/DataPipeline", "PublicationDrift", "Environment", var.environment, "ComponentVersion", var.component_version],
            ["MetroPulse/DataPipeline", "MissingPublications", "Environment", var.environment, "ComponentVersion", var.component_version]
          ]
        }
      }
    ]
  })
}

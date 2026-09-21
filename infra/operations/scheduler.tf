resource "aws_scheduler_schedule" "audit" {
  name                         = local.schedule_name
  description                  = "Weekly full-checksum audit of explicit immutable MetroPT-3 inventory."
  schedule_expression          = var.audit_schedule_expression
  schedule_expression_timezone = "UTC"
  state                        = var.audit_schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window { mode = "OFF" }

  target {
    arn      = aws_lambda_function.audit.arn
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      attempt_number = "<aws.scheduler.attempt-number>"
      contract       = "metropulse-scheduled-audit-invocation-v1"
      execution_id   = "<aws.scheduler.execution-id>"
      inventory = {
        bucket         = var.audit_inventory_bucket
        identity_kind  = var.audit_inventory_identity_kind
        identity_value = var.audit_inventory_identity_value
        key            = var.audit_inventory_key
      }
      schedule_arn      = "<aws.scheduler.schedule-arn>"
      scheduled_time    = "<aws.scheduler.scheduled-time>"
      verification_mode = "full_checksum"
    })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }

  lifecycle {
    precondition {
      condition = !var.audit_schedule_enabled || (
        var.audit_inventory_bucket != "" &&
        can(regex("^control/audit/source=metropt3/inventory_id=[0-9a-f]{64}/inventory\\.json$", var.audit_inventory_key)) &&
        contains(["version_id", "sha256", "etag"], var.audit_inventory_identity_kind) &&
        var.audit_inventory_identity_value != ""
      )
      error_message = "An enabled audit schedule requires an exact pinned canonical inventory."
    }
  }
}

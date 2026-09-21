output "compaction_function_name" {
  value       = aws_lambda_function.compaction.function_name
  description = "On-demand exact-selection monthly compaction function."
}

output "audit_function_name" {
  value       = aws_lambda_function.audit.function_name
  description = "Read-only scheduled curated integrity-audit function."
}

output "audit_schedule_name" {
  value       = aws_scheduler_schedule.audit.name
  description = "Weekly audit schedule name."
}

output "audit_schedule_state" {
  value       = aws_scheduler_schedule.audit.state
  description = "DISABLED by default until an exact inventory exists."
}

output "operations_log_groups" {
  value = {
    audit      = aws_cloudwatch_log_group.audit.name
    compaction = aws_cloudwatch_log_group.compaction.name
  }
  description = "Finite-retention operations log groups."
}

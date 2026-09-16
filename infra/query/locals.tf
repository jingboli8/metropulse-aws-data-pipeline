locals {
  database_name  = "metropulse_${var.environment}"
  table_name     = "metropt3_curated"
  workgroup_name = "${var.project_name}-${var.environment}-analytics"
  result_prefix  = "workgroups/${local.workgroup_name}/"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = var.owner
    Purpose     = "MetroPT-3 curated analytics portfolio"
  }
}

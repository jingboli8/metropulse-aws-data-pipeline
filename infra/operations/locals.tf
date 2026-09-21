locals {
  compaction_function_name = "${var.project_name}-${var.environment}-compact-monthly"
  audit_function_name      = "${var.project_name}-${var.environment}-audit-curated"
  schedule_name            = "${var.project_name}-${var.environment}-weekly-integrity-audit"
  data_lake_arn            = "arn:aws:s3:::${var.data_lake_bucket_name}"
  schedule_arn             = "arn:aws:scheduler:${var.aws_region}:${var.aws_account_id}:schedule/default/${local.schedule_name}"
  glue_catalog_arn         = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog"
  glue_database_arn        = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/${var.glue_database_name}"
  glue_table_arn           = "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${var.glue_database_name}/${var.glue_table_name}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = var.owner
    Purpose     = "MetroPT-3 scheduled integrity operations"
  }
}

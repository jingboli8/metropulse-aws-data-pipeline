locals {
  function_name = "${var.project_name}-${var.environment}-validate-daily"
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = var.owner
    Purpose     = "MetroPT-3 data-quality portfolio"
  }
}

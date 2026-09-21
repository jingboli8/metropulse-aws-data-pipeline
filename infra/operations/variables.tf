variable "aws_region" {
  description = "AWS Region containing the existing platform, catalog, and image."
  type        = string
  default     = "eu-west-1"
}

variable "aws_account_id" {
  description = "Explicit twelve-digit deployment account ID used only to scope ARNs."
  type        = string
  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be exactly 12 decimal digits."
  }
}

variable "project_name" {
  description = "Short project identifier used in names and tags."
  type        = string
  default     = "metropulse"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "project_name must be 3-31 lowercase letters, digits, or hyphens."
  }
}

variable "environment" {
  description = "Deployment environment identifier."
  type        = string
  default     = "dev"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be 2-16 lowercase letters, digits, or hyphens."
  }
}

variable "owner" {
  description = "Non-personal ownership tag used for inventory."
  type        = string
  default     = "data-engineering-portfolio"
}

variable "data_lake_bucket_name" {
  description = "Existing private data-lake bucket created by infra/platform."
  type        = string
  validation {
    condition = (
      length(var.data_lake_bucket_name) >= 3 &&
      length(var.data_lake_bucket_name) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.data_lake_bucket_name)) &&
      !strcontains(var.data_lake_bucket_name, "..")
    )
    error_message = "data_lake_bucket_name must be a valid S3 bucket name."
  }
}

variable "lambda_image_uri" {
  description = "Same-Region ECR image URI pinned by sha256 digest."
  type        = string
  validation {
    condition     = can(regex("^[^[:space:]]+@sha256:[0-9a-f]{64}$", var.lambda_image_uri))
    error_message = "lambda_image_uri must end in @sha256:<64 lowercase hex characters>."
  }
}

variable "component_version" {
  description = "Compaction/audit component version used in logs and EMF dimensions."
  type        = string
  default     = "7.0.0"
}

variable "validation_function_name" {
  description = "Existing validation Lambda name for health alarms and the dashboard."
  type        = string
}

variable "validation_pipeline_version" {
  description = "Existing validation metric dimension value."
  type        = string
  default     = "4.0.0"
}

variable "glue_database_name" {
  description = "Existing Glue database created by infra/query."
  type        = string
  default     = "metropulse_dev"
}

variable "glue_table_name" {
  description = "Existing explicit curated Glue table."
  type        = string
  default     = "metropt3_curated"
}

variable "log_retention_days" {
  description = "Finite retention for compaction and audit Lambda logs."
  type        = number
  default     = 14
}

variable "audit_schedule_enabled" {
  description = "Enable only after exact curated partitions and inventory exist."
  type        = bool
  default     = false
}

variable "audit_schedule_expression" {
  description = "Weekly integrity audit at Monday 06:00 UTC."
  type        = string
  default     = "cron(0 6 ? * MON *)"
  validation {
    condition     = var.audit_schedule_expression == "cron(0 6 ? * MON *)"
    error_message = "The historical-source audit schedule must remain weekly Monday 06:00 UTC."
  }
}

variable "audit_inventory_bucket" {
  description = "Exact bucket holding the immutable audit inventory."
  type        = string
  default     = ""
}

variable "audit_inventory_key" {
  description = "Exact canonical immutable audit inventory key."
  type        = string
  default     = ""
}

variable "audit_inventory_identity_kind" {
  description = "Pinned inventory identity: version_id, sha256, or opaque etag."
  type        = string
  default     = ""
  validation {
    condition     = var.audit_inventory_identity_kind == "" || contains(["version_id", "sha256", "etag"], var.audit_inventory_identity_kind)
    error_message = "audit_inventory_identity_kind must be empty, version_id, sha256, or etag."
  }
}

variable "audit_inventory_identity_value" {
  description = "Exact immutable inventory version/checksum/opaque ETag."
  type        = string
  default     = ""
}

variable "quarantine_rate_alarm_percent" {
  description = "Aggregate validation quarantine-rate alarm threshold."
  type        = number
  default     = 5
}

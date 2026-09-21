variable "aws_region" {
  description = "AWS Region for the platform and its same-Region ECR image."
  type        = string
  default     = "eu-west-1"
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
  description = "Non-personal ownership tag used for resource inventory."
  type        = string
  default     = "data-engineering-portfolio"
}

variable "data_lake_bucket_name" {
  description = "Globally unique private bucket name for raw and processed data."
  type        = string
}

variable "athena_results_bucket_name" {
  description = "Globally unique private bucket name reserved for Athena query results."
  type        = string
}

variable "lambda_image_uri" {
  description = "Same-Region ECR image URI pinned by sha256 digest. Mutable tags are rejected."
  type        = string

  validation {
    condition     = can(regex("^[^[:space:]]+@sha256:[0-9a-f]{64}$", var.lambda_image_uri))
    error_message = "lambda_image_uri must end in @sha256:<64 lowercase hex characters>."
  }
}

variable "pipeline_version" {
  description = "Application version included in processing identities and object metadata."
  type        = string
  default     = "4.0.0"
}

variable "maximum_input_bytes" {
  description = "Hard input-object limit with headroom above verified daily CSV sizes."
  type        = number
  default     = 26214400

  validation {
    condition     = var.maximum_input_bytes >= 1048576 && var.maximum_input_bytes <= 52428800
    error_message = "maximum_input_bytes must be between 1 MiB and 50 MiB."
  }
}

variable "lambda_memory_mb" {
  description = "Memory for PyArrow processing of a bounded daily object."
  type        = number
  default     = 2048
}

variable "lambda_timeout_seconds" {
  description = "Bounded validation timeout."
  type        = number
  default     = 300
}

variable "lambda_ephemeral_storage_mb" {
  description = "Writable /tmp allocation; application processing is memory-first."
  type        = number
  default     = 1024
}

variable "reserved_concurrency" {
  description = "Small development concurrency cap protecting cost and downstream capacity."
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "Finite retention for the validation Lambda log group."
  type        = number
  default     = 14
}

variable "staging_retention_days" {
  description = "Current staging objects expire after this curated-data buffer."
  type        = number
  default     = 90
}

variable "quarantine_retention_days" {
  description = "Current quarantine objects remain available for diagnosis."
  type        = number
  default     = 365
}

variable "control_noncurrent_retention_days" {
  description = "Bounded retention for superseded control-object versions; current evidence does not expire."
  type        = number
  default     = 90
}

variable "athena_results_retention_days" {
  description = "Query results expiration; Athena resources arrive in Phase 5."
  type        = number
  default     = 30
}

variable "allow_force_destroy" {
  description = "Dev-only escape hatch for deliberate teardown after data backup review."
  type        = bool
  default     = false
}

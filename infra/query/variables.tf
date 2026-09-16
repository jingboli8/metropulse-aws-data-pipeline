variable "aws_region" {
  description = "AWS Region containing the platform buckets and query resources."
  type        = string
  default     = "eu-west-1"

  validation {
    condition     = length(trimspace(var.aws_region)) > 0
    error_message = "aws_region must not be empty."
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
  description = "Non-personal ownership tag used for resource inventory."
  type        = string
  default     = "data-engineering-portfolio"

  validation {
    condition     = length(trimspace(var.owner)) > 0
    error_message = "owner must not be empty."
  }
}

variable "data_lake_bucket_name" {
  description = "Existing private data-lake bucket created by infra/platform."
  type        = string

  validation {
    condition = (
      length(var.data_lake_bucket_name) >= 3 &&
      length(var.data_lake_bucket_name) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.data_lake_bucket_name)) &&
      !strcontains(var.data_lake_bucket_name, "..") &&
      !can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$", var.data_lake_bucket_name))
    )
    error_message = "data_lake_bucket_name must be a valid non-IP S3 bucket name."
  }
}

variable "athena_results_bucket_name" {
  description = "Existing private Athena-results bucket created by infra/platform."
  type        = string

  validation {
    condition = (
      length(var.athena_results_bucket_name) >= 3 &&
      length(var.athena_results_bucket_name) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.athena_results_bucket_name)) &&
      !strcontains(var.athena_results_bucket_name, "..") &&
      !can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$", var.athena_results_bucket_name))
    )
    error_message = "athena_results_bucket_name must be a valid non-IP S3 bucket name."
  }
}

variable "athena_results_bucket_expected_owner" {
  description = "Twelve-digit AWS account ID expected to own the Athena-results bucket."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.athena_results_bucket_expected_owner))
    error_message = "athena_results_bucket_expected_owner must be exactly 12 decimal digits."
  }
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Maximum bytes one Athena query may scan. Athena requires at least 10,000,000."
  type        = number
  default     = 268435456

  validation {
    condition     = var.bytes_scanned_cutoff_per_query >= 10000000
    error_message = "bytes_scanned_cutoff_per_query must be at least 10,000,000 bytes."
  }
}

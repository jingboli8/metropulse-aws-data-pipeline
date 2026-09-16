variable "aws_region" {
  description = "AWS Region for ECR. The Lambda platform must use the same Region."
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Short project identifier used in resource names and tags."
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

variable "allow_force_delete" {
  description = "Dev-only escape hatch to remove an ECR repository containing images."
  type        = bool
  default     = false
}

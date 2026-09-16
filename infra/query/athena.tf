resource "aws_athena_workgroup" "query" {
  name          = local.workgroup_name
  description   = "Bounded curated MetroPT-3 analytics for ${var.environment}."
  state         = "ENABLED"
  force_destroy = false

  configuration {
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff_per_query
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    requester_pays_enabled             = false

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      expected_bucket_owner = var.athena_results_bucket_expected_owner
      output_location       = "s3://${var.athena_results_bucket_name}/${local.result_prefix}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.common_tags
}

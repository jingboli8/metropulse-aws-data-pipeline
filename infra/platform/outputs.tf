output "data_lake_bucket_name" {
  description = "Private versioned data-lake bucket."
  value       = aws_s3_bucket.data_lake.id
}

output "athena_results_bucket_name" {
  description = "Private short-retention query-results bucket reserved for Phase 5."
  value       = aws_s3_bucket.athena_results.id
}

output "validation_function_name" {
  description = "Daily raw-object validation Lambda name."
  value       = aws_lambda_function.validation.function_name
}

output "validation_function_image_uri" {
  description = "Digest-pinned container image configured on the Lambda."
  value       = aws_lambda_function.validation.image_uri
}

output "raw_notification_filter" {
  description = "Filter proving generated outputs cannot recursively invoke validation."
  value = {
    prefix = "raw/source=metropt3/"
    suffix = "data.csv"
  }
}

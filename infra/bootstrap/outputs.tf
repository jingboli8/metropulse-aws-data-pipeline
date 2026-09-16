output "repository_name" {
  description = "ECR repository name used by local build and push commands."
  value       = aws_ecr_repository.lambda.name
}

output "repository_url" {
  description = "ECR repository URL. Append @sha256:<digest> for platform input."
  value       = aws_ecr_repository.lambda.repository_url
}

output "registry_id" {
  description = "Registry identifier returned by ECR after bootstrap creation."
  value       = aws_ecr_repository.lambda.registry_id
}

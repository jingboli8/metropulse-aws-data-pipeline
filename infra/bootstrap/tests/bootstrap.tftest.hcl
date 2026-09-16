mock_provider "aws" {}

run "secure_ecr_defaults" {
  command = plan

  assert {
    condition     = aws_ecr_repository.lambda.image_tag_mutability == "IMMUTABLE"
    error_message = "ECR tags must be immutable."
  }

  assert {
    condition     = aws_ecr_repository.lambda.image_scanning_configuration[0].scan_on_push
    error_message = "ECR scan-on-push must be enabled."
  }

  assert {
    condition     = aws_ecr_repository.lambda.encryption_configuration[0].encryption_type == "AES256"
    error_message = "ECR encryption must be configured."
  }

  assert {
    condition     = aws_ecr_repository.lambda.force_delete == false
    error_message = "ECR force deletion must be disabled by default."
  }

  assert {
    condition     = jsondecode(aws_ecr_lifecycle_policy.lambda.policy).rules[1].selection.countNumber == 5
    error_message = "ECR lifecycle must retain only five tagged build images."
  }

  assert {
    condition = (
      strcontains(aws_ecr_repository_policy.lambda_pull.policy, "ecr:BatchGetImage") &&
      strcontains(aws_ecr_repository_policy.lambda_pull.policy, "ecr:GetDownloadUrlForLayer")
    )
    error_message = "ECR must grant Lambda only the image retrieval actions it needs."
  }
}

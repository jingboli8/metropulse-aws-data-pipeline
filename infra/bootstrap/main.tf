locals {
  repository_name = "${var.project_name}-${var.environment}-validation-lambda"
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = var.owner
    Purpose     = "MetroPT-3 data-quality portfolio"
  }
}

resource "aws_ecr_repository" "lambda" {
  name                 = local.repository_name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.allow_force_delete

  encryption_configuration {
    encryption_type = "AES256"
  }

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "lambda" {
  repository = aws_ecr_repository.lambda.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after seven days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Retain only five tagged images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["build-"]
          countType     = "imageCountMoreThan"
          countNumber   = 5
        }
        action = { type = "expire" }
      }
    ]
  })
}

resource "aws_ecr_repository_policy" "lambda_pull" {
  repository = aws_ecr_repository.lambda.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowLambdaImageRetrieval"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = [
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ]
    }]
  })
}

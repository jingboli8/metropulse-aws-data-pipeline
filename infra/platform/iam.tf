resource "aws_iam_role" "validation" {
  name = "${local.function_name}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "LambdaAssumeRole"
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "validation" {
  name = "${local.function_name}-policy"
  role = aws_iam_role.validation.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRawObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = ["${aws_s3_bucket.data_lake.arn}/raw/source=metropt3/*"]
      },
      {
        Sid    = "ReadWriteValidationOutputs"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/staging/source=metropt3/*",
          "${aws_s3_bucket.data_lake.arn}/quarantine/source=metropt3/*",
          "${aws_s3_bucket.data_lake.arn}/control/validation/*"
        ]
      },
      {
        Sid    = "WriteValidationLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = ["${aws_cloudwatch_log_group.validation.arn}:*"]
      }
    ]
  })
}

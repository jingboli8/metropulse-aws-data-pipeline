data "aws_iam_policy_document" "lambda_assume" {
  statement {
    sid     = "LambdaAssumeRole"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "compaction" {
  name               = "${local.compaction_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "compaction" {
  name = "${local.compaction_function_name}-policy"
  role = aws_iam_role.compaction.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadExactCompactionInputs"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = [
          "${local.data_lake_arn}/control/validation/*",
          "${local.data_lake_arn}/control/compaction/*",
          "${local.data_lake_arn}/staging/source=metropt3/*",
          "${local.data_lake_arn}/curated/metropt3/*"
        ]
      },
      {
        Sid      = "WriteImmutableCompactionOutputs"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${local.data_lake_arn}/control/compaction/*", "${local.data_lake_arn}/curated/metropt3/*"]
      },
      {
        Sid      = "PublishExactCuratedPartition"
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetPartition", "glue:CreatePartition", "glue:UpdatePartition"]
        Resource = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]
      },
      {
        Sid      = "WriteCompactionLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.compaction.arn}:*"]
      }
    ]
  })
}

resource "aws_iam_role" "audit" {
  name               = "${local.audit_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "audit" {
  name = "${local.audit_function_name}-policy"
  role = aws_iam_role.audit.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadExactAuditEvidence"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = [
          "${local.data_lake_arn}/control/audit/*",
          "${local.data_lake_arn}/control/compaction/*",
          "${local.data_lake_arn}/curated/metropt3/*"
        ]
      },
      {
        Sid      = "ReadCuratedPartitions"
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetPartition"]
        Resource = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]
      },
      {
        Sid      = "WriteAuditLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.audit.arn}:*"]
      }
    ]
  })
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    sid     = "SchedulerAssumeRole"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.aws_account_id]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [local.schedule_arn]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.schedule_name}-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${local.schedule_name}-policy"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InvokeOnlyAuditLambda"
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = [aws_lambda_function.audit.arn]
    }]
  })
}

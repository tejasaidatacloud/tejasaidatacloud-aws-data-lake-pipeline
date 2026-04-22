##############################################################################
# AWS Data Lake Pipeline – Terraform Root Module
# Author: Sai Teja
##############################################################################

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Configure via -backend-config or environment variables
    key    = "data-lake/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "aws-data-lake-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "data-engineering"
    }
  }
}

##############################################################################
# S3 – Data Lake Buckets
##############################################################################

resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-${var.environment}-data-lake"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "raw-layer-lifecycle"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
  }

  rule {
    id     = "cleaned-layer-lifecycle"
    status = "Enabled"
    filter { prefix = "cleaned/" }
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Artifacts bucket for Glue scripts
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.project_name}-${var.environment}-artifacts"
  force_destroy = true
}

resource "aws_s3_object" "glue_raw_to_cleaned_script" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue_scripts/raw_to_cleaned.py"
  source = "${path.module}/../../glue_jobs/raw_to_cleaned.py"
  etag   = filemd5("${path.module}/../../glue_jobs/raw_to_cleaned.py")
}

resource "aws_s3_object" "glue_cleaned_to_curated_script" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue_scripts/cleaned_to_curated.py"
  source = "${path.module}/../../glue_jobs/cleaned_to_curated.py"
  etag   = filemd5("${path.module}/../../glue_jobs/cleaned_to_curated.py")
}

##############################################################################
# IAM – Glue & Lambda Roles
##############################################################################

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-${var.environment}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "logs:*"]
        Resource = "*"
      }
    ]
  })
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project_name}-${var.environment}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_permissions" {
  name = "lambda-permissions"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:PutParameter"]
        Resource = "arn:aws:ssm:*:*:parameter/data-lake/*"
      }
    ]
  })
}

##############################################################################
# AWS Glue – ETL Jobs
##############################################################################

resource "aws_glue_job" "raw_to_cleaned" {
  name         = "data-lake-${var.environment}-raw-to-cleaned"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  timeout      = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/glue_scripts/raw_to_cleaned.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.artifacts.id}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.artifacts.id}/tmp/"
    "--s3_bucket"                        = aws_s3_bucket.data_lake.id
    "--env"                              = var.environment
  }

  tags = { Name = "raw-to-cleaned" }
}

resource "aws_glue_job" "cleaned_to_curated" {
  name         = "data-lake-${var.environment}-cleaned-to-curated"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = "G.2X"
  number_of_workers = 4
  timeout      = 120

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/glue_scripts/cleaned_to_curated.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.artifacts.id}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.artifacts.id}/tmp/"
    "--s3_bucket"                        = aws_s3_bucket.data_lake.id
    "--env"                              = var.environment
  }

  tags = { Name = "cleaned-to-curated" }
}

##############################################################################
# AWS Glue – Data Catalog
##############################################################################

resource "aws_glue_catalog_database" "data_lake" {
  name        = "data_lake_${var.environment}"
  description = "Data Lake catalog – ${var.environment}"
}

resource "aws_glue_crawler" "cleaned_layer" {
  name          = "data-lake-${var.environment}-cleaned-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.data_lake.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/cleaned/"
  }

  schedule      = "cron(30 3 * * ? *)"   # 30 min after cleaned job
  configuration = jsonencode({
    Version = 1.0
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })
}

resource "aws_glue_crawler" "curated_layer" {
  name          = "data-lake-${var.environment}-curated-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.data_lake.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/curated/"
  }

  schedule = "cron(0 5 * * ? *)"
}

##############################################################################
# Lambda Functions
##############################################################################

data "archive_file" "ingestion_lambda" {
  type        = "zip"
  source_file = "${path.module}/../../lambdas/ingestion_lambda/lambda_function.py"
  output_path = "${path.module}/build/ingestion_lambda.zip"
}

resource "aws_lambda_function" "ingestion" {
  function_name    = "data-lake-${var.environment}-api-ingestion"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.ingestion_lambda.output_path
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256
  timeout          = 300
  memory_size      = 512

  environment {
    variables = {
      S3_BUCKET  = aws_s3_bucket.data_lake.id
      AWS_REGION = var.aws_region
      ENV        = var.environment
    }
  }

  tracing_config { mode = "Active" }
  tags = { Name = "api-ingestion" }
}

data "archive_file" "validation_lambda" {
  type        = "zip"
  source_file = "${path.module}/../../lambdas/validation_lambda/lambda_function.py"
  output_path = "${path.module}/build/validation_lambda.zip"
}

resource "aws_lambda_function" "validation" {
  function_name    = "data-lake-${var.environment}-data-validator"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.validation_lambda.output_path
  source_code_hash = data.archive_file.validation_lambda.output_base64sha256
  timeout          = 300
  memory_size      = 512

  environment {
    variables = {
      S3_BUCKET      = aws_s3_bucket.data_lake.id
      AWS_REGION     = var.aws_region
      MAX_NULL_RATE  = "0.05"
      MIN_ROW_COUNT  = "10"
    }
  }

  tracing_config { mode = "Active" }
  tags = { Name = "data-validator" }
}

##############################################################################
# CloudWatch – Alarms & Dashboard
##############################################################################

resource "aws_cloudwatch_metric_alarm" "pipeline_failure" {
  alarm_name          = "data-lake-${var.environment}-pipeline-failure"
  alarm_description   = "Triggers when the data lake pipeline fails"
  namespace           = "DataLakePipeline"
  metric_name         = "PipelineFailure"
  dimensions          = { Environment = var.environment, Pipeline = "DataLake" }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "validation_failure" {
  alarm_name          = "data-lake-${var.environment}-validation-failure"
  alarm_description   = "Data quality validation failures"
  namespace           = "DataLakePipeline"
  metric_name         = "ValidationFailed"
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 5
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "ingestion_errors" {
  alarm_name          = "data-lake-${var.environment}-ingestion-errors"
  alarm_description   = "Data ingestion errors"
  namespace           = "DataLakePipeline"
  metric_name         = "IngestionErrors"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 3
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_dashboard" "data_lake" {
  dashboard_name = "DataLakePipeline-${var.environment}"
  dashboard_body = file("${path.module}/../../monitoring/cloudwatch_dashboard.json")
}

##############################################################################
# SNS – Alerts
##############################################################################

resource "aws_sns_topic" "alerts" {
  name = "data-lake-${var.environment}-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

##############################################################################
# EventBridge – Daily Schedule Trigger
##############################################################################

resource "aws_cloudwatch_event_rule" "daily_ingestion" {
  name                = "data-lake-${var.environment}-daily-ingestion"
  description         = "Trigger data lake ingestion daily at 01:45 UTC"
  schedule_expression = "cron(45 1 * * ? *)"
}

resource "aws_cloudwatch_event_target" "ingestion_lambda" {
  rule      = aws_cloudwatch_event_rule.daily_ingestion.name
  target_id = "InvokeLambda"
  arn       = aws_lambda_function.ingestion.arn
  input = jsonencode({
    source = "eventbridge-schedule"
  })
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_ingestion.arn
}

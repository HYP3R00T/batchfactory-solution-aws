locals {
  prefix = "${var.project_prefix}-${var.env}"
  tags = {
    Project = var.project_prefix
    Env     = var.env
  }
}

# STORAGE

# S3 bucket for CSV uploads and processed JSON outputs
module "ingest_bucket" {
  source        = "../../modules/s3"
  bucket_name   = "${local.prefix}-ingest"
  force_destroy = true # Allow deletion for dev environment
  tags          = local.tags
}

# DynamoDB table to track job status
module "jobs_table" {
  source        = "../../modules/dynamodb"
  table_name    = "${local.prefix}-jobs"
  ttl_enabled   = true
  ttl_attribute = "ttlEpoch"
  tags          = local.tags
}

# MESSAGING

# SQS queue to decouple validator from processor (with DLQ for failures)
module "processing_queue" {
  source                     = "../../modules/sqs"
  queue_name                 = "${local.prefix}-processing"
  visibility_timeout_seconds = 300
  max_receive_count          = 3
  tags                       = local.tags
}

# LAMBDA FUNCTIONS

# Validator: Triggered by S3, validates CSV, sends to SQS
module "validator_lambda" {
  source        = "../../modules/lambda"
  function_name = "${local.prefix}-validator"
  role_arn      = aws_iam_role.validator_role.arn
  filename      = var.validator_lambda_artifact
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60
  memory_size   = 256
  environment = {
    JOBS_TABLE    = module.jobs_table.table_name
    SQS_QUEUE_URL = module.processing_queue.queue_url
  }
  tags = local.tags
}

# Processor: Triggered by SQS, converts CSV to JSON, writes to S3
module "processor_lambda" {
  source        = "../../modules/lambda"
  function_name = "${local.prefix}-processor"
  role_arn      = aws_iam_role.processor_role.arn
  filename      = var.processor_lambda_artifact
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 512
  environment = {
    JOBS_TABLE       = module.jobs_table.table_name
    PROCESSED_PREFIX = "processed/"
  }
  tags = local.tags
}

# API Reader: Serves job status via API Gateway
module "api_reader_lambda" {
  source        = "../../modules/lambda"
  function_name = "${local.prefix}-api-reader"
  role_arn      = aws_iam_role.api_reader_role.arn
  filename      = var.api_lambda_artifact
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30
  memory_size   = 128
  environment = {
    JOBS_TABLE = module.jobs_table.table_name
  }
  tags = local.tags
}

# API GATEWAY

# HTTP API to expose GET /jobs/{id} endpoint
module "api_gateway" {
  source      = "../../modules/apigateway"
  name_prefix = local.prefix
  lambda_arn  = module.api_reader_lambda.lambda_arn
  lambda_name = module.api_reader_lambda.lambda_name
  stage_name  = var.env
}

# MONITORING

# CloudWatch alarms for errors and DLQ
module "cloudwatch" {
  source       = "../../modules/cloudwatch"
  name_prefix  = local.prefix
  lambda_names = [
    module.validator_lambda.lambda_name,
    module.processor_lambda.lambda_name,
    module.api_reader_lambda.lambda_name
  ]
  dlq_name = module.processing_queue.dlq_name
  tags     = local.tags
}

# IAM ROLES

# Validator Lambda role
resource "aws_iam_role" "validator_role" {
  name = "${local.prefix}-validator-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = local.tags
}

# Processor Lambda role
resource "aws_iam_role" "processor_role" {
  name = "${local.prefix}-processor-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = local.tags
}

# API Reader Lambda role
resource "aws_iam_role" "api_reader_role" {
  name = "${local.prefix}-api-reader-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = local.tags
}

# IAM POLICIES

# Validator: CloudWatch Logs + S3 read/write/delete + SQS send + DynamoDB write
resource "aws_iam_role_policy" "validator_policy" {
  name = "${local.prefix}-validator-policy"
  role = aws_iam_role.validator_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${module.ingest_bucket.bucket_arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = module.processing_queue.queue_arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:UpdateItem"]
        Resource = module.jobs_table.table_arn
      }
    ]
  })
}

# Processor: CloudWatch Logs + S3 read/write + SQS receive + DynamoDB write
resource "aws_iam_role_policy" "processor_policy" {
  name = "${local.prefix}-processor-policy"
  role = aws_iam_role.processor_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${module.ingest_bucket.bucket_arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = module.processing_queue.queue_arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:UpdateItem"]
        Resource = module.jobs_table.table_arn
      }
    ]
  })
}

# API Reader: CloudWatch Logs + DynamoDB read
resource "aws_iam_role_policy" "api_reader_policy" {
  name = "${local.prefix}-api-reader-policy"
  role = aws_iam_role.api_reader_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem"]
        Resource = module.jobs_table.table_arn
      }
    ]
  })
}

# EVENT TRIGGERS

# Allow S3 to invoke Validator Lambda
resource "aws_lambda_permission" "s3_invoke_validator" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = module.validator_lambda.lambda_name
  principal     = "s3.amazonaws.com"
  source_arn    = module.ingest_bucket.bucket_arn
}

# S3 notification: trigger Validator when CSV uploaded to uploads/
resource "aws_s3_bucket_notification" "csv_upload_trigger" {
  bucket = module.ingest_bucket.bucket_id

  lambda_function {
    lambda_function_arn = module.validator_lambda.lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "uploads/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.s3_invoke_validator]
}

# SQS trigger: invoke Processor when messages arrive
resource "aws_lambda_event_source_mapping" "sqs_to_processor" {
  event_source_arn = module.processing_queue.queue_arn
  function_name    = module.processor_lambda.lambda_name
  batch_size       = 1
}

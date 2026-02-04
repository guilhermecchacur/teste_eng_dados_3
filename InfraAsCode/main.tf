locals {
  tags = {
    projeto = var.project_tag_value
  }

  bucket_name = "${var.bucket_name_prefix}-src"

  # Chaves no S3
  main_key     = "${var.s3_prefix}/etl/main.py"
  config_key   = "${var.s3_prefix}/etl/config.json"
  utils_key    = "${var.s3_prefix}/utils/utils.zip"
  analysis_key = "${var.s3_prefix}/analysis/main.py"

  main_s3_uri     = "s3://${local.bucket_name}/${local.main_key}"
  config_s3_uri   = "s3://${local.bucket_name}/${local.config_key}"
  utils_s3_uri    = "s3://${local.bucket_name}/${local.utils_key}"
  analysis_s3_uri = "s3://${local.bucket_name}/${local.analysis_key}"
}

#########################
# S3 Bucket

resource "aws_s3_bucket" "src" {
  bucket = local.bucket_name
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "src" {
  bucket                  = aws_s3_bucket.src.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "src" {
  bucket = aws_s3_bucket.src.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

#########################
# Zip do diretório utils/

data "archive_file" "utils_zip" {
  type        = "zip"
  source_dir  = var.local_utils_dir
  output_path = "${path.module}/.build/utils.zip"
}

#########################
# Upload dos artefatos

resource "aws_s3_object" "main" {
  bucket       = aws_s3_bucket.src.id
  key          = local.main_key
  source       = var.local_main_path
  etag         = filemd5(var.local_main_path)
  content_type = "text/x-python"
  tags         = local.tags
}

resource "aws_s3_object" "config" {
  bucket       = aws_s3_bucket.src.id
  key          = local.config_key
  source       = var.local_config_path
  etag         = filemd5(var.local_config_path)
  content_type = "application/json"
  tags         = local.tags
}

resource "aws_s3_object" "utils" {
  bucket       = aws_s3_bucket.src.id
  key          = local.utils_key
  source       = data.archive_file.utils_zip.output_path
  etag         = filemd5(data.archive_file.utils_zip.output_path)
  content_type = "application/zip"
  tags         = local.tags
}

resource "aws_s3_object" "analysis" {
  bucket       = aws_s3_bucket.src.id
  key          = local.analysis_key
  source       = var.local_analysis_path
  etag         = filemd5(var.local_analysis_path)
  content_type = "text/x-python"
  tags         = local.tags
}

#########################
# IAM

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "${var.job_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = local.tags
}

data "aws_iam_policy_document" "s3_only_policy" {
  statement {
    sid     = "ListBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket"]

    resources = [
      aws_s3_bucket.src.arn
    ]
  }

  statement {
    sid     = "ReadWriteObjects"
    effect  = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.src.arn}/*",
      "${var.lading_bucket_arn}/*",
      "${var.bronze_bucket_arn}/*",
      "${var.silver_bucket_arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3_inline" {
  name   = "${var.job_name}-s3-only"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.s3_only_policy.json
}

#########################
# Glue Job ETL

resource "aws_glue_job" "etl" {
  name     = var.job_name_etl
  role_arn = aws_iam_role.glue_role.arn

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers

  command {
    name            = "glueetl"
    script_location = local.main_s3_uri
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--extra-py-files"          = local.utils_s3_uri
    "--files"                   = local.config_s3_uri
    "--config"                  = "config.json"
    "--enable-metric"           = "true"
    "--enable-spark-ui"         = "true"
    "--enable-glue-datacatalog" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.tags

  depends_on = [
    aws_s3_object.main,
    aws_s3_object.config,
    aws_s3_object.utils,
    aws_iam_role_policy.glue_s3_inline
  ]
}

#########################
# Glue Job Analysis

resource "aws_glue_job" "analysis" {
  name     = var.job_name_analysis
  role_arn = aws_iam_role.glue_role.arn

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers

  command {
    name            = "glueetl"
    script_location = local.analysis_s3_uri
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--enable-metric"           = "true"
    "--enable-spark-ui"         = "true"
    "--enable-glue-datacatalog" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.tags

  depends_on = [
    aws_s3_object.analysis,
    aws_iam_role_policy.glue_s3_inline
  ]
}

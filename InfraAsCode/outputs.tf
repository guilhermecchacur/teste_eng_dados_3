output "bucket_name" {
  value = aws_s3_bucket.src.bucket
}

output "main_s3_uri" {
  value = local.main_s3_uri
}

output "config_s3_uri" {
  value = local.config_s3_uri
}

output "utils_s3_uri" {
  value = local.utils_s3_uri
}

output "analysis_s3_uri" {
  value = local.analysis_s3_uri
}

output "glue_job_etl_name" {
  value = aws_glue_job.etl.name
}

output "glue_job_analysis_name" {
  value = aws_glue_job.analysis.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

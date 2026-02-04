variable "aws_region" {
  type    = string
  default = "sa-east-1"
}

variable "job_name_etl" {
  type    = string
  default = "glue-job-etl-teste-eng-dados"
}

variable "job_name_analysis" {
  type    = string
  default = "glue-job-analysis-teste-eng-dados"
}

variable "bucket_name_prefix" {
  type    = string
  default = "teste-eng-dados"
}

variable "project_tag_value" {
  type    = string
  default = "teste_eng_dados"
}

# Glue sizing
variable "glue_version" {
  type    = string
  default = "5.0"
}

variable "worker_type" {
  type    = string
  default = "G.1X"
}

variable "number_of_workers" {
  type    = number
  default = 10
}

# Paths locais
variable "local_main_path" {
  type    = string
  default = "../ETL/main.py"
}

variable "local_config_path" {
  type    = string
  default = "../ETL/config.json"
}

variable "local_utils_dir" {
  type    = string
  default = "../utils"
}

variable "local_analysis_path" {
  type    = string
  default = "../AnaliseDados/main.py"
}

# Prefixos S3
variable "s3_prefix" {
  type    = string
  default = "glue-artifacts"
}

# arn dos buckets do lake
variable "lading_bucket_arn" {
  type    = string
  default = "arn_landing"
}

variable "bronze_bucket_arn" {
  type    = string
  default = "arn_bronze"
}

variable "silver_bucket_arn" {
  type    = string
  default = "arn_silver"
}
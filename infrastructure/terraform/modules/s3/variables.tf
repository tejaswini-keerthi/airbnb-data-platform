variable "bronze_bucket_name" {
  description = "S3 bucket name for Bronze layer"
  type        = string
}

variable "silver_bucket_name" {
  description = "S3 bucket name for Silver layer"
  type        = string
}

variable "gold_bucket_name" {
  description = "S3 bucket name for Gold layer"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}
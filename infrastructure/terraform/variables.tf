variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  sensitive   = true
}

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
  default     = "dev"
}
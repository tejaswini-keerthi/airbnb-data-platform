output "bronze_bucket_arn" {
  description = "ARN of the Bronze S3 bucket"
  value       = module.s3.bronze_bucket_arn
}

output "silver_bucket_arn" {
  description = "ARN of the Silver S3 bucket"
  value       = module.s3.silver_bucket_arn
}

output "gold_bucket_arn" {
  description = "ARN of the Gold S3 bucket"
  value       = module.s3.gold_bucket_arn
}
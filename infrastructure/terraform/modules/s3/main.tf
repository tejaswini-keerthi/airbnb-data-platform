resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name

  tags = {
    Name        = var.bronze_bucket_name
    Layer       = "bronze"
    Environment = var.environment
    Project     = "airbnb-data-platform"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name

  tags = {
    Name        = var.silver_bucket_name
    Layer       = "silver"
    Environment = var.environment
    Project     = "airbnb-data-platform"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = var.gold_bucket_name

  tags = {
    Name        = var.gold_bucket_name
    Layer       = "gold"
    Environment = var.environment
    Project     = "airbnb-data-platform"
  }
}

# Block all public access on all three buckets
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "bronze_bucket_arn" {
  value = aws_s3_bucket.bronze.arn
}

output "silver_bucket_arn" {
  value = aws_s3_bucket.silver.arn
}

output "gold_bucket_arn" {
  value = aws_s3_bucket.gold.arn
}
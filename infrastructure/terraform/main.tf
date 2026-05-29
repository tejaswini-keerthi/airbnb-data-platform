terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
}

module "s3" {
  source = "./modules/s3"

  bronze_bucket_name  = var.bronze_bucket_name
  silver_bucket_name  = var.silver_bucket_name
  gold_bucket_name    = var.gold_bucket_name
  environment         = var.environment
}
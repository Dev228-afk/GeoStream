terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "geostream"
      Environment = var.environment
      ManagedBy   = "terraform"
      Portfolio   = "apple-ads-prep"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-west-2"
}

variable "environment" {
  type    = string
  default = "demo"
}

resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "geostream-artifacts-${var.environment}-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts_ttl" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "expire-benchmark-artifacts"
    status = "Enabled"
    expiration {
      days = 14
    }
  }
}

resource "aws_ecr_repository" "pipeline" {
  name                 = "geostream-${var.environment}"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/ecs-eks/geostream-${var.environment}"
  retention_in_days = 7
}

resource "aws_iam_role" "worker_role" {
  name_prefix = "geostream-${var.environment}-worker-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}


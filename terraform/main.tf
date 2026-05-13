# ============================================================
# Ad Analytics — Terraform Root Module
# ============================================================
# AWS Free-Tier compatible infrastructure for optional cloud deployment.
# Usage:
#   cd terraform
#   terraform init
#   terraform plan
#   terraform apply
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment for remote state storage:
  # backend "s3" {
  #   bucket         = "ad-analytics-terraform-state"
  #   key            = "infrastructure/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "ad-analytics"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ============================================================
# VPC & Networking
# ============================================================
module "vpc" {
  source      = "./modules/vpc"
  project     = var.project_name
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
}

# ============================================================
# Storage (S3 + DynamoDB)
# ============================================================
module "storage" {
  source      = "./modules/storage"
  project     = var.project_name
  environment = var.environment
}

# ============================================================
# IAM Roles & Policies
# ============================================================
module "iam" {
  source      = "./modules/iam"
  project     = var.project_name
  environment = var.environment
  dynamodb_table_arn = module.storage.dynamodb_table_arn
}

# ============================================================
# ECR Repository (Container Registry)
# ============================================================
resource "aws_ecr_repository" "flink_job" {
  name                 = "${var.project_name}-flink-job"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "producer" {
  name                 = "${var.project_name}-producer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "consumer" {
  name                 = "${var.project_name}-consumer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

# ============================================================
# CloudWatch Log Groups
# ============================================================
resource "aws_cloudwatch_log_group" "flink" {
  name              = "/ecs/${var.project_name}/flink"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/${var.project_name}/producer"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/ecs/${var.project_name}/consumer"
  retention_in_days = 7
}
# ============================================================
# ECS Cluster & Services
# ============================================================
module "ecs" {
  source             = "./modules/ecs"
  project            = var.project_name
  environment        = var.environment
  aws_region         = var.aws_region
  vpc_id             = module.vpc.vpc_id
  public_subnets     = module.vpc.public_subnet_ids
  execution_role_arn = module.iam.ecs_task_execution_role_arn
  task_role_arn      = module.iam.ecs_task_role_arn
  log_group_name     = aws_cloudwatch_log_group.flink.name
  
  # Image placeholders (will be updated after ECR push)
  flink_image    = "${aws_ecr_repository.flink_job.repository_url}:latest"
  producer_image = "${aws_ecr_repository.producer.repository_url}:latest"
  consumer_image = "${aws_ecr_repository.consumer.repository_url}:latest"
}

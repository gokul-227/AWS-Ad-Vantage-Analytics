output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}


output "dynamodb_table_name" {
  description = "DynamoDB table for metrics"
  value       = module.storage.dynamodb_table_name
}

output "ecr_flink_repo" {
  description = "ECR repository URL for Flink job"
  value       = aws_ecr_repository.flink_job.repository_url
}

output "ecr_producer_repo" {
  description = "ECR repository URL for producer"
  value       = aws_ecr_repository.producer.repository_url
}

output "ecr_consumer_repo" {
  description = "ECR repository URL for consumer"
  value       = aws_ecr_repository.consumer.repository_url
}

output "ecs_cluster_name" {
  value = module.ecs.cluster_name
}

output "public_subnet_ids" {
  value = module.vpc.public_subnet_ids
}

output "ecs_tasks_security_group_id" {
  value = module.ecs.security_group_id
}

output "cluster_name" { value = aws_ecs_cluster.main.name }
output "cluster_id" { value = aws_ecs_cluster.main.id }
output "security_group_id" { value = aws_security_group.ecs_tasks.id }

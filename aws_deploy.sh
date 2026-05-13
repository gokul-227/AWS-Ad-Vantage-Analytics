#!/usr/bin/env bash
# ============================================================
# aws_deploy.sh — One-command to deploy to AWS
# ============================================================
set -e

echo "🏗️  Phase 1: Creating AWS Infrastructure (VPC, ECR, S3, DynamoDB)..."
cd terraform
terraform init
terraform apply -auto-approve

# Extract outputs
AWS_REGION=$(terraform output -raw aws_region 2>/dev/null || echo "us-east-1")
CLUSTER_NAME=$(terraform output -raw ecs_cluster_name)
SG_ID=$(terraform output -raw ecs_tasks_security_group_id)
# Handle array output for subnets
SUBNET_1=$(terraform output -json public_subnet_ids | jq -r '.[0]')
SUBNET_2=$(terraform output -json public_subnet_ids | jq -r '.[1]')

echo "🐳 Phase 2: Building and Pushing Docker images to ECR..."
cd ..
chmod +x scripts/build-and-push.sh
./scripts/build-and-push.sh

echo "🚀 Phase 3: Initializing Kafka and Submitting Flink Job..."
# Run Kafka Init
aws ecs run-task \
    --cluster $CLUSTER_NAME \
    --task-definition ad-analytics-kafka-init \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_1,$SUBNET_2],securityGroups=[$SG_ID],assignPublicIp=ENABLED}"

echo "⏳ Waiting for Kafka Init to finish..."
sleep 45

# Run Flink Job Submit
aws ecs run-task \
    --cluster $CLUSTER_NAME \
    --task-definition ad-analytics-job-submit \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_1,$SUBNET_2],securityGroups=[$SG_ID],assignPublicIp=ENABLED}"

echo ""
echo "✅ AWS Deployment Complete!"
echo "📊 HOW TO VERIFY FEATURES IN AWS:"
echo "1. Check DynamoDB for live metrics: ad-analytics-dev-metrics"
echo "2. Check CloudWatch Logs: /ecs/ad-analytics/flink"
echo "3. Use the following command to see data in DynamoDB:"
echo "   aws dynamodb scan --table-name ad-analytics-dev-metrics --limit 10"

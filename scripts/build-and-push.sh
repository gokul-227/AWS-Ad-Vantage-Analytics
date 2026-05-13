#!/usr/bin/env bash
# ============================================================
# build-and-push.sh — Build and push Docker images to ECR
# ============================================================
set -euo pipefail

AWS_REGION="us-east-1"
AWS_ACCOUNT="612681030789"
PROJECT="ad-analytics"

echo "🔐 Logging into ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com

echo "🏗️ Building Flink Job image..."
docker build -t $PROJECT-flink-job -f flink-job/Dockerfile .
docker tag $PROJECT-flink-job:latest $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT-flink-job:latest

echo "🏗️ Building Producer image..."
docker build -t $PROJECT-producer -f producer/Dockerfile .
docker tag $PROJECT-producer:latest $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT-producer:latest

echo "🏗️ Building Consumer image..."
docker build -t $PROJECT-consumer -f consumer/Dockerfile .
docker tag $PROJECT-consumer:latest $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT-consumer:latest

echo "🚀 Pushing images to ECR..."
docker push $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT-flink-job:latest
docker push $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT-producer:latest
docker push $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$PROJECT-consumer:latest

echo "✅ All images pushed successfully!"

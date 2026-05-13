#!/usr/bin/env bash
# ============================================================
# destroy.sh — Cleanup all resources (Local & AWS)
# ============================================================

echo "🧹 Cleaning up Local Docker resources..."
docker compose down -v

echo "🧹 Cleaning up AWS Infrastructure..."
cd terraform
terraform destroy -auto-approve

echo "✅ All resources destroyed successfully!"

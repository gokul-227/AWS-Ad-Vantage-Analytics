#!/usr/bin/env bash
# ============================================================
# aws_destroy.sh — One-command to clean up everything in AWS
# ============================================================
set -e

echo "⚠️  WARNING: This will delete all resources in AWS and you will lose all data!"
read -p "Are you sure? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

echo "🧨 Destroying AWS Infrastructure..."
cd terraform
terraform destroy -auto-approve

echo "✅ AWS Cleanup Complete!"

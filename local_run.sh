#!/usr/bin/env bash
# ============================================================
# local_run.sh — One-command to run everything locally
# ============================================================

echo "🐳 Starting local Docker containers..."
docker compose up -d --build

echo "⏳ Waiting for Flink and Kafka to initialize (this takes ~60-90s)..."
# Simple loop to wait for Flink UI
until curl -sf http://localhost:8081/overview > /dev/null; do
    echo -n "."
    sleep 5
done
echo "✅ Flink UI is UP!"

echo "🛡️ Running validation..."
./validate.sh

echo ""
echo "📊 HOW TO VERIFY FEATURES LOCALLY:"
echo "1. Open Flink Dashboard: http://localhost:8081"
echo "2. Open Kafka UI: http://localhost:8080"
echo "3. Watch real-time metrics in your terminal:"
echo "   docker compose logs -f consumer"
echo ""
echo "✅ Done!"

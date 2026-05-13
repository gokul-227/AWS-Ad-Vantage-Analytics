#!/usr/bin/env bash
# Wait for Kafka to be ready
set -euo pipefail
KAFKA_BOOTSTRAP="${1:-kafka:9092}"
MAX_RETRIES="${2:-30}"

echo "Waiting for Kafka at $KAFKA_BOOTSTRAP..."
for i in $(seq 1 $MAX_RETRIES); do
    if kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list &>/dev/null 2>&1; then
        echo "Kafka is ready!"
        exit 0
    fi
    echo "Attempt $i/$MAX_RETRIES - Kafka not ready, retrying in 5s..."
    sleep 5
done
echo "ERROR: Kafka did not become ready in time."
exit 1

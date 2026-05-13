#!/usr/bin/env bash
# Create Kafka topics with proper configuration
set -euo pipefail
KAFKA_BOOTSTRAP="${1:-kafka:9092}"

echo "=== Creating Kafka Topics ==="

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists --topic views-topic \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=86400000

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists --topic clicks-topic \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=86400000

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists --topic metrics-topic \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=86400000

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists --topic dead-letter-topic \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000

echo "=== Topics Created ==="
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list

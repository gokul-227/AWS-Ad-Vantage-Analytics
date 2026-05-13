#!/usr/bin/env python3
"""
Ad Analytics Metrics Consumer

Reads computed metrics from Kafka and displays them with pretty formatting.

Environment Variables:
  KAFKA_BOOTSTRAP_SERVERS  Kafka broker address (default: kafka:9092)
  KAFKA_METRICS_TOPIC      Metrics topic name (default: metrics-topic)
  CONSUMER_PRETTY_PRINT    Enable colored output (default: true)
  CONSUMER_OUTPUT_FILE     Optional file path for JSON output
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import boto3
from decimal import Decimal

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
METRICS_TOPIC = os.getenv("KAFKA_METRICS_TOPIC", "metrics-topic")
PRETTY_PRINT = os.getenv("CONSUMER_PRETTY_PRINT", "true").lower() == "true"
OUTPUT_FILE = os.getenv("CONSUMER_OUTPUT_FILE", "")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("consumer")

# ANSI color codes
COLORS = {
    "TOTAL_VIEWS":              "\033[96m",   # Cyan
    "TOTAL_CLICKS":             "\033[93m",   # Yellow
    "VIEWS_WITH_CLICKS":        "\033[92m",   # Green
    "CTR":                      "\033[95m",   # Magenta
    "SLIDING_VIEWS_PER_AD_SITE":"\033[94m",   # Blue
    "SLIDING_VIEWS_PER_AD":     "\033[34m",   # Dark Blue
    "AVG_CLICKS_PER_VIEW":      "\033[91m",   # Red
    "CLICK_HISTOGRAM":          "\033[33m",   # Dark Yellow
}
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"


def create_consumer(max_retries=30, retry_delay=5):
    """Create Kafka consumer with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                METRICS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="metrics-console-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=-1,
            )
            logger.info("Connected to Kafka at %s, topic: %s", KAFKA_BOOTSTRAP, METRICS_TOPIC)
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (attempt %d/%d)...", attempt, max_retries)
            time.sleep(retry_delay)
    logger.error("Could not connect to Kafka")
    sys.exit(1)


def format_timestamp(ts_ms):
    """Convert unix ms timestamp to readable string."""
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ts_ms)


def pretty_print_metric(metric):
    """Print a metric with colors and formatting."""
    name = metric.get("metricName", "UNKNOWN")
    ad = metric.get("adName", "?")
    site = metric.get("siteName", "?")
    value = metric.get("value", 0)
    ts = metric.get("timestamp", 0)
    color = COLORS.get(name, "")

    header = f"{color}{BOLD}[{name}]{RESET}"
    info = f"{DIM}Ad={ad}, Site={site}{RESET}"

    # Format value based on metric type
    if name == "CTR":
        val_str = f"{value:.4f} ({value*100:.2f}%)"
    elif name in ("AVG_CLICKS_PER_VIEW",):
        val_str = f"{value:.3f}"
    else:
        val_str = f"{int(value)}"

    line = f"{header} {info} | Value: {BOLD}{val_str}{RESET}"

    # Add window info if present
    ws = metric.get("windowStart")
    we = metric.get("windowEnd")
    if ws and we:
        line += f" | Window: [{format_timestamp(ws)} → {format_timestamp(we)}]"

    # Add details
    details = metric.get("details", {})
    if details and name == "CLICK_HISTOGRAM":
        line += f"\n    Histogram: {json.dumps(details, indent=None)}"
    elif details:
        detail_parts = [f"{k}={v}" for k, v in details.items()]
        line += f" | {', '.join(detail_parts)}"

    print(line)


def plain_print_metric(metric):
    """Print metric as JSON."""
    print(json.dumps(metric))


def write_to_file(metric, filepath):
    """Append metric as JSON line to file."""
    try:
        with open(filepath, "a") as f:
            f.write(json.dumps(metric) + "\n")
    except IOError as e:
        logger.error("Failed to write to %s: %s", filepath, e)


def get_dynamodb_table():
    if not DYNAMODB_TABLE:
        return None
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        return table
    except Exception as e:
        logger.error("Failed to connect to DynamoDB: %s", e)
        return None


def write_to_dynamodb(table, metric):
    try:
        # DynamoDB doesn't like floats, convert to Decimal
        item = json.loads(json.dumps(metric), parse_float=Decimal)
        # Add a unique key for DynamoDB if not present
        if 'metricKey' not in item:
            item['metricKey'] = f"{item.get('metricName')}_{item.get('adName', 'ALL')}_{item.get('siteName', 'ALL')}"
        table.put_item(Item=item)
    except Exception as e:
        logger.error("Failed to write to DynamoDB: %s", e)


if __name__ == "__main__":
    logger.info("Ad Analytics Consumer starting...")
    logger.info("Kafka: %s, Topic: %s, Pretty: %s", KAFKA_BOOTSTRAP, METRICS_TOPIC, PRETTY_PRINT)
    if DYNAMODB_TABLE:
        logger.info("DynamoDB Persistence Enabled: %s", DYNAMODB_TABLE)

    consumer = create_consumer()
    db_table = get_dynamodb_table()
    count = 0

    print(f"\n{BOLD}{'='*70}")
    print(f"  Ad Analytics - Real-Time Metrics Consumer")
    print(f"  Listening on: {METRICS_TOPIC}")
    print(f"{'='*70}{RESET}\n")

    try:
        for message in consumer:
            metric = message.value
            count += 1

            if PRETTY_PRINT:
                pretty_print_metric(metric)
            else:
                plain_print_metric(metric)

            if OUTPUT_FILE:
                write_to_file(metric, OUTPUT_FILE)

            if db_table:
                write_to_dynamodb(db_table, metric)

            if count % 100 == 0:
                logger.info("Consumed %d metrics so far", count)

    except KeyboardInterrupt:
        logger.info("Consumer shutting down...")
    finally:
        consumer.close()
        logger.info("Consumer stopped. Total metrics consumed: %d", count)

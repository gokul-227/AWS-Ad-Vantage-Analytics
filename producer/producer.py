#!/usr/bin/env python3
"""
Ad Analytics Event Producer

Reads CSV files or generates random events and publishes them to Kafka.

Modes:
  - csv:    Replays events from CSV files (default)
  - random: Generates synthetic events continuously

Environment Variables:
  KAFKA_BOOTSTRAP_SERVERS  Kafka broker address (default: kafka:9092)
  KAFKA_VIEWS_TOPIC        Views topic name (default: views-topic)
  KAFKA_CLICKS_TOPIC       Clicks topic name (default: clicks-topic)
  PRODUCER_MODE            'csv' or 'random' (default: csv)
  PRODUCER_SPEED_MULTIPLIER  How fast to replay CSV events (default: 10)
  PRODUCER_DELAY_MS        Delay between events in random mode (default: 100)
"""

import csv
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
VIEWS_TOPIC = os.getenv("KAFKA_VIEWS_TOPIC", "views-topic")
CLICKS_TOPIC = os.getenv("KAFKA_CLICKS_TOPIC", "clicks-topic")
MODE = os.getenv("PRODUCER_MODE", "csv")
SPEED_MULTIPLIER = int(os.getenv("PRODUCER_SPEED_MULTIPLIER", "10"))
DELAY_MS = int(os.getenv("PRODUCER_DELAY_MS", "100"))
VIEWS_CSV = os.getenv("VIEWS_CSV_PATH", "/data/views.csv")
CLICKS_CSV = os.getenv("CLICKS_CSV_PATH", "/data/clicks.csv")

# Ads and sites for random generation
AD_NAMES = ["Happy Bird", "Ocean Breeze", "Mountain Dew", "Sunset Valley", "Cloud Nine"]
SITE_NAMES = ["Google", "Facebook", "Twitter", "Instagram", "YouTube"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("producer")


def create_producer(max_retries=30, retry_delay=5):
    """Create Kafka producer with retry logic for startup ordering."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=5,
                compression_type="gzip",
                max_block_ms=60000,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready (attempt %d/%d), retrying in %ds...",
                attempt, max_retries, retry_delay
            )
            time.sleep(retry_delay)

    logger.error("Could not connect to Kafka after %d attempts", max_retries)
    sys.exit(1)


def on_send_success(record_metadata):
    logger.debug(
        "Sent to %s [partition=%d, offset=%d]",
        record_metadata.topic, record_metadata.partition, record_metadata.offset
    )


def on_send_error(ex):
    logger.error("Failed to send message: %s", ex)


def read_csv(filepath):
    """Read CSV file and return list of event dicts."""
    events = []
    try:
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                events.append({
                    "ID": int(row["ID"]),
                    "AdName": row["AdName"],
                    "SiteName": row["SiteName"],
                    "Time": int(row["Time"])
                })
        logger.info("Loaded %d events from %s", len(events), filepath)
    except FileNotFoundError:
        logger.warning("CSV file not found: %s", filepath)
    return events


def run_csv_mode(producer):
    """Replay events from CSV files with configurable speed."""
    logger.info("=== CSV Replay Mode (speed=%dx) ===", SPEED_MULTIPLIER)

    views = read_csv(VIEWS_CSV)
    clicks = read_csv(CLICKS_CSV)

    if not views:
        logger.error("No view events loaded. Check CSV path: %s", VIEWS_CSV)
        return

    # Merge and sort all events by timestamp
    all_events = [(e, VIEWS_TOPIC) for e in views] + [(e, CLICKS_TOPIC) for e in clicks]
    all_events.sort(key=lambda x: x[0]["Time"])

    logger.info("Replaying %d total events...", len(all_events))

    prev_time = all_events[0][0]["Time"] if all_events else 0
    sent_count = 0

    for event, topic in all_events:
        # Simulate time delay between events
        time_diff = event["Time"] - prev_time
        if time_diff > 0 and SPEED_MULTIPLIER > 0:
            sleep_time = (time_diff / 1000.0) / SPEED_MULTIPLIER
            sleep_time = min(sleep_time, 2.0)  # Cap at 2 seconds
            time.sleep(sleep_time)

        key = f"{event['AdName']}|{event['SiteName']}"
        producer.send(topic, key=key, value=event).add_callback(
            on_send_success).add_errback(on_send_error)

        sent_count += 1
        if sent_count % 50 == 0:
            logger.info("Sent %d/%d events", sent_count, len(all_events))
            producer.flush()

        prev_time = event["Time"]

    producer.flush()
    logger.info("=== CSV replay complete: %d events sent ===", sent_count)

    # After CSV replay, switch to random mode to keep streaming
    logger.info("Switching to random event generation...")
    run_random_mode(producer, start_id=max(e[0]["ID"] for e in all_events) + 1)


def run_random_mode(producer, start_id=1000):
    """Generate random events continuously."""
    logger.info("=== Random Event Generator Mode (delay=%dms) ===", DELAY_MS)

    event_id = start_id
    while True:
        ad = random.choice(AD_NAMES)
        site = random.choice(SITE_NAMES)
        now_ms = int(time.time() * 1000)

        # Generate a view event
        view = {"ID": event_id, "AdName": ad, "SiteName": site, "Time": now_ms}
        key = f"{ad}|{site}"
        producer.send(VIEWS_TOPIC, key=key, value=view).add_callback(
            on_send_success).add_errback(on_send_error)

        # ~40% chance of generating a click for this view
        if random.random() < 0.4:
            click_delay = random.randint(100, 5000)
            click = {"ID": event_id, "AdName": ad, "SiteName": site,
                     "Time": now_ms + click_delay}
            producer.send(CLICKS_TOPIC, key=key, value=click).add_callback(
                on_send_success).add_errback(on_send_error)

        event_id += 1
        if event_id % 100 == 0:
            logger.info("Generated %d events (current ID: %d)", event_id - start_id, event_id)
            producer.flush()

        time.sleep(DELAY_MS / 1000.0)


if __name__ == "__main__":
    logger.info("Ad Analytics Producer starting...")
    logger.info("Mode: %s, Kafka: %s", MODE, KAFKA_BOOTSTRAP)

    prod = create_producer()

    try:
        if MODE == "csv":
            run_csv_mode(prod)
        elif MODE == "random":
            run_random_mode(prod)
        else:
            logger.error("Unknown mode: %s (use 'csv' or 'random')", MODE)
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Producer shutting down...")
    finally:
        prod.flush()
        prod.close()
        logger.info("Producer stopped.")

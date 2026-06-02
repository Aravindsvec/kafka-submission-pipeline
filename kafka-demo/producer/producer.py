"""
producer.py
-----------
Publishes user-submitted form data to a Kafka topic.
Called by the FastAPI server when a user submits data through the UI.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "config"))

import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer
from kafka_config import PRODUCER_CONFIG, TOPIC_CONFIG

log = logging.getLogger("producer")


def _delivery_report(err, msg) -> None:
    """Callback invoked by Kafka after each message is acknowledged (or fails)."""
    if err:
        log.error("Delivery failed  key=%s  error=%s", msg.key(), err)
    else:
        log.info(
            "✓ Delivered  topic=%s  partition=%d  offset=%d  key=%s",
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode() if msg.key() else None,
        )


def publish_submission(data: dict) -> str:
    """
    Publish a single user submission to Kafka.

    Args:
        data: Dict of form fields submitted by the user.

    Returns:
        The message_id assigned to this submission.
    """
    producer = Producer(PRODUCER_CONFIG)
    topic    = TOPIC_CONFIG["name"]

    message_id = str(uuid4())

    payload = {
        "message_id": message_id,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        **data,
    }

    producer.produce(
        topic=topic,
        key=message_id.encode(),
        value=json.dumps(payload).encode(),
        callback=_delivery_report,
    )
    producer.flush(timeout=30)
    log.info("Published submission  message_id=%s  topic=%s", message_id, topic)

    return message_id

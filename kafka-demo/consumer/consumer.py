"""
consumer.py
-----------
Subscribes to the Kafka topic and saves each user submission to MongoDB.
Runs as a background thread started by run.py.

Kafka offsets are committed manually after a successful MongoDB insert
(at-least-once delivery guarantee).
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "config"))

import json
import logging
import threading

from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient

from kafka_config import CONSUMER_CONFIG, TOPIC_CONFIG
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

log = logging.getLogger("consumer")

_stop_event = threading.Event()


def _get_mongo_collection():
    """Return the MongoDB collection used to store submissions."""
    return MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLLECTION]


def consume_messages(stop_event: threading.Event = None, poll_timeout: float = 2.0) -> None:
    """
    Continuously consume messages from Kafka and store them in MongoDB.

    Args:
        stop_event:   threading.Event that signals a clean shutdown when set.
        poll_timeout: Seconds to wait for a message before looping.
    """
    if stop_event is None:
        stop_event = threading.Event()

    consumer   = Consumer(CONSUMER_CONFIG)
    collection = _get_mongo_collection()
    topic      = TOPIC_CONFIG["name"]

    consumer.subscribe([topic])
    log.info("Consumer subscribed to topic='%s'  group='%s'", topic, CONSUMER_CONFIG["group.id"])

    received = 0

    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=poll_timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            received += 1
            payload = json.loads(msg.value().decode())

            if not isinstance(payload, dict):
                log.warning("Skipping non-dict message: %r", payload)
                consumer.commit(message=msg, asynchronous=False)
                continue

            log.info(
                "[%d] partition=%d  offset=%d  message_id=%s",
                received, msg.partition(), msg.offset(),
                payload.get("message_id"),
            )

            collection.insert_one({
                **payload,
                "_kafka_partition": msg.partition(),
                "_kafka_offset":    msg.offset(),
            })

            consumer.commit(message=msg, asynchronous=False)

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
    except Exception as exc:
        log.error("Consumer error: %s", exc)
    finally:
        consumer.close()
        log.info("Consumer closed. Total messages received: %d", received)


def start_consumer_thread() -> threading.Thread:
    """Start the consumer in a background daemon thread and return it."""
    stop_event = threading.Event()
    thread = threading.Thread(
        target=consume_messages,
        kwargs={"stop_event": stop_event},
        daemon=True,
        name="kafka-consumer",
    )
    thread.start()
    log.info("Consumer thread started.")
    return thread, stop_event


# ── Standalone entry point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    consume_messages()

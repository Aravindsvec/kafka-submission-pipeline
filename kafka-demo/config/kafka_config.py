"""
kafka_config.py
---------------
Pre-built configuration dicts for Kafka topic, producer, and consumer.
"""

from config import (
    BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    NUM_PARTITIONS,
    REPLICATION_FACTOR,
    GROUP_ID,
)

TOPIC_CONFIG = {
    "name":               TOPIC_NAME,
    "num_partitions":     NUM_PARTITIONS,
    "replication_factor": REPLICATION_FACTOR,
    "config": {
        "retention.ms":    "86400000",
        "cleanup.policy":  "delete",
        "compression.type": "lz4",
    },
}

PRODUCER_CONFIG = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "acks":               "all",
    "retries":            5,
    "linger.ms":          5,
    "batch.size":         16384,
    "compression.type":   "lz4",
    "enable.idempotence": False,
}

CONSUMER_CONFIG = {
    "bootstrap.servers":    BOOTSTRAP_SERVERS,
    "group.id":             GROUP_ID,
    "auto.offset.reset":    "earliest",
    "enable.auto.commit":   False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms":   45000,
}

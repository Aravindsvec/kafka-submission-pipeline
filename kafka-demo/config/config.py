"""
config.py
---------
Loads shared configuration from server/.env and exposes typed constants
used by producer, consumer, and server modules.
"""

import os
from dotenv import load_dotenv

_ENV_PATH = os.path.join(os.path.dirname(__file__), "..", "server", ".env")
load_dotenv(_ENV_PATH)

# ── Kafka ──────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME         = os.getenv("KAFKA_TOPIC",             "user-submissions")
NUM_PARTITIONS     = int(os.getenv("KAFKA_NUM_PARTITIONS",     "3"))
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
GROUP_ID           = os.getenv("KAFKA_GROUP_ID", "submissions-group")

# ── MongoDB ────────────────────────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB",         "kafka_submissions")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "submissions")

# ── UI ─────────────────────────────────────────────────────────────────────────
PAGE_TITLE = os.getenv("PAGE_TITLE", "Kafka Submission Pipeline")

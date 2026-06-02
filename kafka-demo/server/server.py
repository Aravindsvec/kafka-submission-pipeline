"""
server.py
---------
FastAPI application that:
  - Serves the UI at GET /
  - Accepts form submissions at POST /submit
  - Returns recent submissions at GET /submissions
  - Manages the Kafka topic via admin endpoints
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "config"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

import logging
from typing import Optional
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo import MongoClient

from config import BOOTSTRAP_SERVERS, MONGO_URI, MONGO_DB, MONGO_COLLECTION, PAGE_TITLE
from kafka_config import TOPIC_CONFIG
from producer import publish_submission

log = logging.getLogger("server")

app = FastAPI(title="Kafka Submission Demo", version="2.0.0")

# ── Serve static files ────────────────────────────────────────────────────────
_STATIC_DIR    = os.path.join(os.path.dirname(__file__), "..", "static")
_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")

if os.path.isdir(_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

templates = Jinja2Templates(directory=_TEMPLATES_DIR)


# ── Pydantic models ───────────────────────────────────────────────────────────

class SubmissionPayload(BaseModel):
    name: str
    email: str
    category: str
    message: str
    priority: Optional[str] = "normal"


# ── MongoDB helper ────────────────────────────────────────────────────────────

def _get_collection():
    return MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLLECTION]


# ── Admin helpers ─────────────────────────────────────────────────────────────

def _admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def ensure_topic() -> None:
    """Create the Kafka topic if it doesn't already exist."""
    admin      = _admin_client()
    topic_name = TOPIC_CONFIG["name"]

    if topic_name in admin.list_topics(timeout=10).topics:
        log.info("Topic '%s' already exists.", topic_name)
        return

    futures = admin.create_topics([NewTopic(
        topic=topic_name,
        num_partitions=TOPIC_CONFIG["num_partitions"],
        replication_factor=TOPIC_CONFIG["replication_factor"],
        config=TOPIC_CONFIG["config"],
    )])

    for topic, future in futures.items():
        try:
            future.result()
            log.info("✓ Topic '%s' created.", topic)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_ui(request: Request):
    """Render the main UI via Jinja2 template."""
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "page_title": PAGE_TITLE},
    )


@app.post("/submit")
async def submit_data(payload: SubmissionPayload):
    """
    Accept a user submission, publish it to Kafka, and return the message_id.
    The consumer will asynchronously write it to MongoDB.
    """
    try:
        data = payload.model_dump()
        message_id = publish_submission(data)
        return JSONResponse({
            "status": "published",
            "message_id": message_id,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        log.error("Failed to publish: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/submissions")
async def get_submissions(limit: int = 20, skip: int = 0):
    """Return recent submissions stored in MongoDB."""
    try:
        collection = _get_collection()
        docs = list(
            collection.find({}, {"_id": 0, "_kafka_partition": 0, "_kafka_offset": 0})
                      .sort("submitted_at", -1)
                      .skip(skip)
                      .limit(limit)
        )
        total = collection.count_documents({})
        return JSONResponse({"total": total, "submissions": docs})
    except Exception as exc:
        log.error("MongoDB query failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health")
async def health():
    """Health check — verifies Kafka and MongoDB connectivity."""
    status = {"kafka": "unknown", "mongodb": "unknown"}

    try:
        _admin_client().list_topics(timeout=5)
        status["kafka"] = "ok"
    except Exception as exc:
        status["kafka"] = f"error: {exc}"

    try:
        MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000).server_info()
        status["mongodb"] = "ok"
    except Exception as exc:
        status["mongodb"] = f"error: {exc}"

    all_ok = all(v == "ok" for v in status.values())
    return JSONResponse(status, status_code=200 if all_ok else 503)


@app.get("/topics")
async def list_topics():
    """List all Kafka topics."""
    try:
        topics = _admin_client().list_topics(timeout=10).topics
        return JSONResponse({
            "topics": [
                {"name": name, "partitions": len(meta.partitions)}
                for name, meta in sorted(topics.items())
            ]
        })
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

"""
run.py
------
Main entry point for the Kafka Submission Demo.

Starts:
  1. Ensures the Kafka topic exists.
  2. Launches the Kafka consumer in a background thread.
  3. Starts the FastAPI server (uvicorn) on http://localhost:8000

Usage:
    python run.py
    test
    python run.py --host 0.0.0.0 --port 8000
"""

import sys
import os
import argparse
import logging
import time
import threading

# ── Path setup ─────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "config"))
sys.path.insert(0, os.path.join(ROOT, "producer"))
sys.path.insert(0, os.path.join(ROOT, "consumer"))
sys.path.insert(0, os.path.join(ROOT, "server"))

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run")


def banner():
    print("""
╔══════════════════════════════════════════════════════╗
║          Kafka Submission Pipeline  v2.0             ║
║                                                      ║
║   UI       →  http://localhost:8000                  ║
║   API docs →  http://localhost:8000/docs             ║
║   Health   →  http://localhost:8000/health           ║
╚══════════════════════════════════════════════════════╝
""")


def ensure_topic():
    """Create the Kafka topic if it doesn't already exist."""
    log.info("Checking Kafka topic …")
    from server import ensure_topic as _ensure
    try:
        _ensure()
    except Exception as exc:
        log.error("Could not create topic: %s", exc)
        log.error("Make sure Kafka is running on localhost:9092")
        sys.exit(1)


def start_consumer():
    """Start the Kafka consumer in a background daemon thread."""
    log.info("Starting Kafka consumer thread …")
    from consumer import start_consumer_thread
    thread, stop_event = start_consumer_thread()
    return thread, stop_event


def start_server(host: str, port: int):
    """Start the FastAPI/uvicorn server (blocking)."""
    import uvicorn
    log.info("Starting FastAPI server on http://%s:%d …", host, port)
    uvicorn.run(
        "server:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
        app_dir=os.path.join(ROOT, "server"),
    )


def main():
    parser = argparse.ArgumentParser(description="Kafka Submission Pipeline")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    parser.add_argument("--skip-topic", action="store_true", help="Skip topic creation check")
    args = parser.parse_args()

    banner()

    # Step 1 — Ensure topic exists
    if not args.skip_topic:
        ensure_topic()
    else:
        log.info("Skipping topic creation (--skip-topic flag set).")

    # Step 2 — Start consumer in background
    consumer_thread, consumer_stop = start_consumer()
    log.info("Consumer thread is running (daemon=True).")

    # Brief pause so consumer subscribes before server accepts requests
    time.sleep(1)

    # Step 3 — Start FastAPI server (blocks until Ctrl+C)
    try:
        start_server(args.host, args.port)
    except KeyboardInterrupt:
        log.info("Shutting down …")
    finally:
        log.info("Signalling consumer thread to stop …")
        consumer_stop.set()
        consumer_thread.join(timeout=5)
        log.info("Goodbye.")


if __name__ == "__main__":
    main()

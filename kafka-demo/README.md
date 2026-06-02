# Kafka Submission Pipeline

A full-stack demo that accepts user input through a browser UI, publishes it to Kafka via a FastAPI backend, and stores it in MongoDB via a background consumer.

## Architecture

```
Browser UI  →  POST /submit  →  FastAPI  →  Producer  →  Kafka  →  Consumer (thread)  →  MongoDB
                                                                         ↑
                                            GET /submissions  ←  MongoDB query
```

## Project Structure

```
kafka-demo/
├── run.py                   ← main entry point (start everything here)
├── requirements.txt
├── config/
│   ├── config.py            ← loads .env, exports typed constants
│   └── kafka_config.py      ← Kafka producer/consumer/topic config dicts
├── producer/
│   └── producer.py          ← publish_submission(data) → Kafka
├── consumer/
│   └── consumer.py          ← consumes Kafka topic → MongoDB (runs as background thread)
├── server/
│   ├── server.py            ← FastAPI app with /submit, /submissions, /health endpoints
│   └── .env                 ← Kafka + MongoDB connection settings
└── static/
    └── index.html           ← browser UI (form + live feed)
```

## Prerequisites

Make sure the following are running locally before starting the app:

- **Kafka** on `localhost:9092` (KRaft or ZooKeeper mode)
- **MongoDB** on `localhost:27017`
- **Python 3.10+**

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. (Optional) Edit connection settings

Open `server/.env` and update if your Kafka or MongoDB use non-default ports:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=user-submissions
KAFKA_NUM_PARTITIONS=3
KAFKA_REPLICATION_FACTOR=1
KAFKA_GROUP_ID=submissions-group

MONGO_URI=mongodb://localhost:27017
MONGO_DB=kafka_submissions
MONGO_COLLECTION=submissions
```

### 3. Run the application

```bash
python run.py
```

This single command:
1. Creates the Kafka topic `user-submissions` (if it doesn't exist)
2. Starts the Kafka consumer in a background thread
3. Starts the FastAPI + uvicorn server on `http://localhost:8000`

### 4. Open the UI

```
http://localhost:8000
```

Fill in the form and click **Publish to Kafka →**. Your submission travels through the pipeline and appears in the live feed within ~1–2 seconds.

---

## API Endpoints

| Method | Path            | Description                          |
|--------|-----------------|--------------------------------------|
| GET    | `/`             | Serve the browser UI                 |
| POST   | `/submit`       | Publish a submission to Kafka        |
| GET    | `/submissions`  | Fetch stored submissions from MongoDB|
| GET    | `/health`       | Check Kafka + MongoDB connectivity   |
| GET    | `/topics`       | List Kafka topics                    |
| GET    | `/docs`         | Interactive Swagger UI (FastAPI)     |

### POST `/submit` payload

```json
{
  "name":     "Jane Smith",
  "email":    "jane@example.com",
  "category": "feedback",
  "priority": "normal",
  "message":  "Love the product!"
}
```

### GET `/submissions` query params

| Param  | Default | Description              |
|--------|---------|--------------------------|
| limit  | 20      | Number of results        |
| skip   | 0       | Offset for pagination    |

---

## MongoDB

Data is stored in:
- **Database**: `kafka_submissions`
- **Collection**: `submissions`

Each document looks like:

```json
{
  "message_id":   "uuid4",
  "submitted_at": "2026-06-02T10:30:00+00:00",
  "name":         "Jane Smith",
  "email":        "jane@example.com",
  "category":     "feedback",
  "priority":     "normal",
  "message":      "Love the product!"
}
```

---

## Stopping

Press `Ctrl+C` in the terminal. The consumer thread is gracefully signalled to stop.

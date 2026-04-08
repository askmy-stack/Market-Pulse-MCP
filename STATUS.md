# Project Status & Execution Guide

---

## What Has Been Built

### Core Files

| File | Purpose |
|---|---|
| `producer.py` | Reads CSV → validates → publishes to Kafka every 1s |
| `consumer.py` | Reads Kafka → batches 1000 msgs or 60s → writes Parquet to S3 |
| `models.py` | Pydantic v2 schema — validates every message before/after Kafka |
| `dlq_monitor.py` | Watches the Dead Letter Queue; alerts on failed messages |
| `anomaly_detector.py` | Detects price spikes (Z-score) → calls Claude API for explanation |
| `dashboard.py` | Streamlit web UI — live candlestick chart + pipeline health metrics |
| `schemas/stock_record.avsc` | Avro schema for AWS Glue Schema Registry |

### Infrastructure Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Local Kafka (Redpanda) + Prometheus + Grafana — no AWS needed |
| `prometheus.yml` | Tells Prometheus where to scrape metrics from |
| `requirements.txt` | All Python dependencies with pinned versions |
| `.env.example` | Template for all config values — copy to `.env` |
| `.gitignore` | Prevents secrets and temp files from being committed |
| `ruff.toml` | Linting rules (excludes old notebooks) |
| `mypy.ini` | Type-checking config |
| `.github/workflows/ci.yml` | GitHub Actions — runs lint + type-check + schema tests on every push |

### What Was Fixed vs Original Notebooks

| Original Problem | Fixed In |
|---|---|
| `kafka-python` unmaintained since 2021 | Replaced with `confluent-kafka` in all scripts |
| `producer.flush()` was unreachable code (after infinite loop) | Moved to `finally` block in `producer.py` |
| 1 S3 file per message → 86,400 files/day | Micro-batch: 1 file per 60s or 1000 msgs |
| No validation — bad data crashes silently | Pydantic v2 validates every message |
| Hardcoded S3 bucket, IPs, topic names | All config in `.env` file |
| No error recovery | Failed messages routed to Dead Letter Queue |
| No logging or metrics | `structlog` JSON logs + 6 Prometheus metrics |

---

## What Still Needs to Be Done

### 1. Avro + AWS Glue Schema Registry *(optional — advanced)*
The schema file `schemas/stock_record.avsc` exists but the producer/consumer still use plain JSON.
To complete this:
- Install `aws-glue-schema-registry` Python library
- Register the schema in AWS Glue Console
- Update `producer.py` to serialize with Avro instead of JSON
- Update `consumer.py` to deserialize Avro

### 2. OpenTelemetry Distributed Tracing *(optional)*
Planned in roadmap but not implemented. Would add end-to-end trace spans:
`produce_message → consume_message → s3_write`

### 3. Demo Screencast *(portfolio polish)*
A 2-minute screen recording showing the live dashboard + an anomaly alert firing.
Link from README for recruiters/reviewers.

### 4. Merge PR #1 to `main`
All code is on branch `claude/add-claude-documentation-IIk3b`.
PR #1 is open and CI is passing. Needs a final review and merge.

---

## Step-by-Step Execution

### Option A — Run Locally (No AWS Required)

**Step 1 — Start local Kafka**
```bash
docker-compose up -d
# Redpanda broker: localhost:9092
# Redpanda Console UI: http://localhost:8080
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000  (admin / admin)
```

**Step 2 — Configure environment**
```bash
cp .env.example .env
# Edit .env — set these two values:
#   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
#   S3_BUCKET=your-bucket-name   (or leave for local testing)
```

**Step 3 — Install dependencies**
```bash
pip install -r requirements.txt
```

**Step 4 — Run the pipeline (4 separate terminals)**
```bash
# Terminal 1 — Producer (sends 1 message/second)
python producer.py

# Terminal 2 — Consumer (batches to S3)
python consumer.py

# Terminal 3 — Anomaly Detector (requires ANTHROPIC_API_KEY in .env)
python anomaly_detector.py

# Terminal 4 — Dashboard
streamlit run dashboard.py
# Opens at http://localhost:8501
```

**Step 5 — Monitor**
- Redpanda Console → `http://localhost:8080` — see messages in `demo_test` topic
- Dashboard → `http://localhost:8501` — live chart + pipeline health
- Grafana → `http://localhost:3000` — Prometheus metrics

---

### Option B — Run on AWS

**Prerequisites:**
- AWS EC2 instance running Kafka (ZooKeeper + Broker on port 9092)
- S3 bucket created
- AWS credentials configured (`aws configure` or IAM instance profile)

**Step 1 — Configure environment**
```bash
cp .env.example .env
# Edit .env:
#   KAFKA_BOOTSTRAP_SERVERS=<EC2-PUBLIC-IP>:9092
#   S3_BUCKET=your-s3-bucket-name
#   ANTHROPIC_API_KEY=your-key      # for anomaly detector
```

**Step 2 — EC2 security group**
Ensure inbound TCP port `9092` is open from your IP address in the EC2 security group.

**Step 3 — Run the pipeline**
Same four commands as Option A, Steps 3–4.

**Step 4 — Query data in Athena**
After data lands in S3:
1. AWS Console → Glue → Crawlers → Create crawler pointing to your S3 bucket
2. Run crawler — it populates the Glue Data Catalog
3. AWS Console → Athena → run SQL:

```sql
SELECT "index", date, closeusd
FROM stock_market_raw
WHERE year = '2021'
ORDER BY closeusd DESC
LIMIT 20;
```

---

## How the Pipeline Works (Simple View)

```
indexProcessed.csv
       │  (random row every 1s)
       ▼
  producer.py
  ─ validates with Pydantic
  ─ publishes to Kafka topic: demo_test
  ─ partition key = stock symbol (e.g. HSI)
       │
       ├──────────────────────────────────────────────────┐
       │                                                  │
       ▼                                                  ▼
  consumer.py                                  anomaly_detector.py
  ─ reads from demo_test                        ─ separate consumer group
  ─ validates each message                      ─ rolling Z-score per symbol
  ─ buffers up to 1000 msgs or 60s             ─ if |Z| ≥ 2.5σ → calls Claude API
  ─ writes Parquet batch to S3                 ─ publishes alert to demo_alerts topic
  ─ routes failures to demo_test_dlq           ─ optional Slack notification
  ─ exposes metrics on :8000/metrics
       │
       ▼
  Amazon S3  (Parquet files, partitioned by year/month/day)
       │
  AWS Glue Crawler
       │
  Glue Data Catalog
       │
  Amazon Athena (SQL queries)
```

---

## Key Config Values in `.env`

| Variable | Example | Required For |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | All scripts |
| `S3_BUCKET` | `my-stock-bucket` | `consumer.py` |
| `ANTHROPIC_API_KEY` | `sk-ant-...` | `anomaly_detector.py` |
| `SLACK_WEBHOOK_URL` | `https://hooks.slack.com/...` | Optional alerts |
| `CONSUMER_BATCH_SIZE` | `1000` | Tune S3 write frequency |
| `ANOMALY_Z_SCORE_THRESHOLD` | `2.5` | Tune alert sensitivity |

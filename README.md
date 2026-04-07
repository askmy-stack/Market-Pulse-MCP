# Real-Time Stock Market Data Processing Engine

A production-grade streaming pipeline that ingests historical OHLCV stock data through Apache Kafka,
archives it to Amazon S3 as Parquet, and layers AI anomaly detection and a real-time dashboard on top.

```
indexProcessed.csv
       │
       ▼
 producer.py ──────► Apache Kafka (EC2 / Redpanda) ──────────────────────────────────────┐
  confluent-kafka      topic: demo_test (8 partitions)                                    │
  Pydantic validation  partition key: Index symbol                                        │
  structlog JSON logs                                                                     │
                                                                                          │
                    ┌─────────────────────────────────────────────────────────────────────┘
                    │                              │                          │
                    ▼                              ▼                          ▼
             consumer.py                 anomaly_detector.py           dashboard.py
           (group: s3-archiver)         (group: anomaly-detector)    (group: dashboard)
           Parquet micro-batching        rolling Z-score per symbol    live candlestick chart
           Prometheus metrics            Claude API narrative alerts    Prometheus health panel
           DLQ on failure                → demo_alerts topic
                    │
                    ▼
              Amazon S3
         (Parquet, partitioned
          by year/month/day)
                    │
            AWS Glue Crawler
                    │
         AWS Glue Data Catalog
                    │
           Amazon Athena (SQL)
```

---

## Quickstart (local — no AWS required)

**Prerequisites:** Docker, Docker Compose, Python 3.11+

```bash
# 1. Clone and configure
git clone <repo-url>
cd Real-time-Stock-Market-Data-Processing-Engine-using-Kafka
cp .env.example .env
# Edit .env — set KAFKA_BOOTSTRAP_SERVERS=localhost:9092, S3_BUCKET=<your-bucket>

# 2. Start Redpanda + Console + Prometheus + Grafana
docker-compose up -d

# 3. Verify topics exist (auto-created by init-topics container)
#    Console UI: http://localhost:8080

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Run the pipeline (three separate terminals)
python producer.py          # streams CSV rows to Kafka every 1s
python consumer.py          # batches to S3 as Parquet every 60s / 1000 msgs
python anomaly_detector.py  # Z-score anomaly detection + Claude API alerts

# 6. Open the dashboard
streamlit run dashboard.py  # http://localhost:8501

# 7. Monitor metrics
#    Prometheus: http://localhost:9090
#    Grafana:    http://localhost:3000  (admin / admin)
```

---

## AWS Deployment

```bash
# On your EC2 instance — set Kafka bootstrap server IP in .env
KAFKA_BOOTSTRAP_SERVERS=<EC2-PUBLIC-IP>:9092

# Configure AWS credentials (IAM role preferred over access keys)
aws configure   # or use instance profile / EKS IRSA

# Run producer and consumer — same commands as above
python producer.py
python consumer.py
```

After data lands in S3, run the AWS Glue crawler to populate the Data Catalog,
then query with Athena:

```sql
SELECT "index", date, closeusd
FROM stock_market_raw
WHERE year = '2021'
ORDER BY closeusd DESC
LIMIT 20;
```

---

## Project Structure

```
├── producer.py            # Kafka producer (confluent-kafka, Pydantic, structlog)
├── consumer.py            # Kafka consumer → S3 Parquet (micro-batch, Prometheus, DLQ)
├── anomaly_detector.py    # Hero Feature 1: rolling Z-score + Claude API alerts
├── dashboard.py           # Hero Feature 2: Streamlit live chart + pipeline health
├── dlq_monitor.py         # Dead-letter queue monitor + Slack alerting
├── models.py              # Pydantic v2 StockRecord and DLQRecord schemas
├── schemas/
│   └── stock_record.avsc  # Hero Feature 3: Avro schema for Glue Schema Registry
├── docker-compose.yml     # Redpanda + Console + Prometheus + Grafana
├── prometheus.yml         # Prometheus scrape config
├── requirements.txt       # Pinned Python dependencies
├── .env.example           # Configuration template (copy to .env)
├── .github/
│   └── workflows/
│       └── ci.yml         # GitHub Actions: ruff lint + mypy + schema validation
├── KafkaProducer.ipynb    # Original notebook (retained as tutorial artifact)
├── KafkaConsumer.ipynb    # Original notebook (retained as tutorial artifact)
├── indexProcessed.csv     # OHLCV dataset: 104K rows, 1986–2021
├── ROADMAP.md             # Full technical audit + feature roadmap
└── CLAUDE.md              # AI assistant guide for this codebase
```

---

## Features

### Core Pipeline
- **`confluent-kafka-python`** — C-backed Kafka client; 3–5× throughput vs `kafka-python`
- **Idempotent producer** (`enable.idempotence=True`) — prevents duplicate messages on retry
- **Partition by symbol** — all records for a given index land in the same partition; ordering preserved
- **Micro-batch Parquet writes** — buffers 1000 messages or 60s, whichever comes first; ~60× fewer S3 files vs one-per-message; Snappy compression; Athena columnar pruning
- **Dead Letter Queue** — validation failures and S3 errors routed to `demo_test_dlq` instead of crashing
- **Graceful shutdown** — `SIGINT`/`SIGTERM` flush the in-flight batch before exit
- **Structured logging** — `structlog` JSON output; ships to CloudWatch Logs without parsing rules
- **Pydantic v2 validation** — every message validated against `StockRecord` before produce and after consume

### Hero Feature 1 — AI Anomaly Detection (`anomaly_detector.py`)
- Maintains a rolling Z-score window (configurable, default 20 ticks) per index symbol
- Triggers when |Z| ≥ 2.5σ — alerts published to `demo_alerts` Kafka topic
- Calls **Claude API** (`claude-haiku-4-5-20251001`) for a one-sentence plain-English explanation
- Runs in its **own consumer group** — independent of the S3 archiver's offsets
- Optional Slack webhook integration

### Hero Feature 2 — Real-Time Dashboard (`dashboard.py`)
- **Panel 1:** Live Plotly candlestick chart, refreshed every 2s, per selected symbol
- **Panel 2:** Pipeline health metrics scraped from Prometheus — lag, throughput, S3 write latency, DLQ depth
- Background Kafka consumer thread feeds the chart without blocking the Streamlit render loop

### Hero Feature 3 — Avro Schema (`schemas/stock_record.avsc`)
- Avro schema with BACKWARD compatibility — `SentimentScore` nullable field added safely
- Register with **AWS Glue Schema Registry** for schema evolution without breaking consumers
- Glue Data Catalog auto-updated when schema evolves

### Observability
| Metric | Type | Description |
|---|---|---|
| `messages_consumed_total` | Counter | Messages consumed, by topic + partition |
| `consumer_lag_messages` | Gauge | Messages behind Kafka head |
| `s3_writes_total` | Counter | Successful Parquet batch writes |
| `s3_write_duration_seconds` | Histogram | S3 write latency distribution |
| `dlq_messages_total` | Counter | Failed messages routed to DLQ |
| `batch_size_records` | Histogram | Records per Parquet file |

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` and fill in your values.
Never commit `.env` — it is in `.gitignore`.

Key variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | — | **Required.** `host:9092` |
| `KAFKA_TOPIC` | `demo_test` | Primary topic name |
| `S3_BUCKET` | — | **Required.** Target S3 bucket |
| `CONSUMER_BATCH_SIZE` | `1000` | Max records per Parquet file |
| `CONSUMER_BATCH_FLUSH_SECONDS` | `60` | Max seconds between S3 flushes |
| `ANTHROPIC_API_KEY` | — | Required for `anomaly_detector.py` |
| `ANOMALY_Z_SCORE_THRESHOLD` | `2.5` | Standard deviations to trigger alert |
| `PROMETHEUS_PORT` | `8000` | Port for `/metrics` endpoint |

---

## CI

GitHub Actions runs on every push and pull request:
- **`ruff`** — lint all Python files
- **`mypy`** — type-check all Python files
- **Schema validation** — inline Python test of `StockRecord` valid/invalid cases

See `.github/workflows/ci.yml`.

---

## Dataset

`indexProcessed.csv` — 104,225 rows of daily OHLCV data for global stock indices (1986–2021).

| Column | Description |
|---|---|
| Index | Symbol (HSI, SPX, J203.JO, …) |
| Date | Trading date (YYYY-MM-DD) |
| Open / High / Low / Close | Session prices |
| Adj Close | Dividend/split-adjusted close |
| Volume | Shares / contracts traded |
| CloseUSD | Close converted to USD |

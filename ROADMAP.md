# Technical Audit & Feature Roadmap

> **Goal:** Transform this proof-of-concept Kafka pipeline into a best-in-class portfolio project
> that demonstrates production-grade streaming architecture to Senior Engineers.

---

## Table of Contents

1. [Technical Debt Audit](#1-technical-debt-audit)
2. [Modernization — 2024/2025 Stack Updates](#2-modernization--20242025-stack-updates)
3. [Hero Features](#3-hero-features)
4. [Production Readiness](#4-production-readiness)
5. [Resume Talking Points](#5-resume-talking-points)
6. [Implementation Phases](#6-implementation-phases)

---

## 1. Technical Debt Audit

A full code inspection of `KafkaProducer.ipynb` and `KafkaConsumer.ipynb` surfaced the following
issues, ranked by severity.

### Critical

| Issue | Location | Detail |
|---|---|---|
| `kafka-python` is unmaintained | Both notebooks | Last release: 2021. No Schema Registry, no transactions, no OAuth. Replace with `confluent-kafka-python`. |
| Hardcoded S3 bucket name in git history | `KafkaConsumer.ipynb` cell 5 | `kafka-stock-market-tutorial-youtube-darshil` is now permanently in VCS. Anyone with repo access can attempt to enumerate or access this bucket. |
| No error handling anywhere | Both notebooks | A single Kafka disconnect, S3 throttle, or CSV read error causes a hard crash with no recovery. |
| No logging | Both notebooks | Zero observability. Impossible to diagnose failures in any environment. |
| One S3 file per message | `KafkaConsumer.ipynb` cell 5 | At 1 msg/sec → 86,400 PUT requests/day → ~$0.43/day in S3 API costs alone, plus Athena scans every tiny file. 1000× worse performance than batching. |

### High

| Issue | Location | Detail |
|---|---|---|
| `producer.flush()` is unreachable code | `KafkaProducer.ipynb` cell 8 | Placed after an infinite `while True:` loop. The buffer is never flushed on any exit path. |
| No schema validation | Both notebooks | Raw `dict` from CSV goes directly to Kafka. A column rename in the CSV silently corrupts all downstream consumers. |
| Hardcoded config everywhere | Both notebooks | Topic (`demo_test`), CSV path (`data/indexProcessed.csv`), Kafka port (`:9092`), sleep interval (`1`), S3 bucket — all hardcoded. Zero environment portability. |
| No graceful shutdown | Both notebooks | `SIGINT` (Ctrl+C) is unhandled. Kafka connections and S3 file handles are not closed cleanly. |

### Medium

| Issue | Location | Detail |
|---|---|---|
| No `requirements.txt` | Repo root | `pip install kafka-python` in a notebook cell is not reproducible. No pinned versions. |
| No Docker / local dev setup | Repo root | Requires a live AWS EC2 Kafka instance to run anything. Blocks all local development and testing. |
| Duplicate `json` imports | `KafkaConsumer.ipynb` | `from json import dumps, loads` and `import json` both present. |
| Debug/test cells left in notebooks | Both notebooks | A test message cell in the producer and commented-out `print` in the consumer are notebook clutter. |

---

## 2. Modernization — 2024/2025 Stack Updates

### 2.1 Replace `kafka-python` → `confluent-kafka-python`

`kafka-python` has been in maintenance-only mode since 2021. The community standard is now
[`confluent-kafka-python`](https://github.com/confluentinc/confluent-kafka-python) — a thin Python
wrapper over `librdkafka` (C library), actively maintained by Confluent.

**Key advantages:**
- 3–5× higher throughput than `kafka-python` due to the C backend
- Native Schema Registry client (`confluent_kafka.schema_registry`)
- Full transaction / exactly-once API
- SASL/SSL, OAuth, mTLS support
- First-class AWS MSK compatibility

**Migration diff (producer):**
```python
# Before
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=[':9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# After
from confluent_kafka import Producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'enable.idempotence': True})
producer.produce(KAFKA_TOPIC, value=json.dumps(record).encode('utf-8'),
                 on_delivery=delivery_callback)
```

### 2.2 Notebooks → Python Scripts

Retain notebooks as interactive demo artifacts, but extract all runnable logic into
`producer.py` and `consumer.py`:

- Proper `if __name__ == "__main__"` entrypoints
- `signal.signal(signal.SIGTERM, ...)` for graceful shutdown (Docker/Kubernetes friendly)
- `argparse` or `python-dotenv` for configuration

### 2.3 Schema: Raw JSON → Avro + AWS Glue Schema Registry

The project already uses AWS Glue. Add **AWS Glue Schema Registry** (free tier) for Avro
serialization:

```
Producer → serialize with Avro schema → Kafka → Consumer → deserialize + validate → S3 (Parquet)
```

Benefits:
- Schema evolution with BACKWARD/FORWARD/FULL compatibility modes
- Consumer gets a `SchemaRegistryException` instead of silent data corruption
- Glue Data Catalog auto-updated when schema evolves
- Athena can query Parquet 10× faster than JSON with 5× compression

### 2.4 Local Development: Redpanda via Docker Compose

[Redpanda](https://redpanda.com/) is a Kafka-compatible broker written in C++. It:
- Starts in <1 second (vs. ~30s for Kafka + ZooKeeper)
- Runs as a single container (no ZooKeeper)
- Has an identical Kafka API — no code changes required
- Includes Redpanda Console (web UI) for topic inspection

See `docker-compose.yml` in this repo for a ready-to-use local setup.

### 2.5 Dependency Pinning

See `requirements.txt` for pinned versions of all dependencies. Key versions:
- `confluent-kafka>=2.3.0` — stable C-backed client
- `pandas>=2.0.0` — ArrowDtype backend (3–5× faster CSV parsing)
- `pydantic>=2.0.0` — Rust-backed validation (10× faster than v1)
- `pyarrow>=14.0.0` — Parquet I/O for S3 writes
- `structlog>=23.0.0` — structured JSON logging

---

## 3. Hero Features

These three features are deliberately chosen to provoke deep interview conversations and demonstrate
production architecture thinking — not just "I followed a tutorial."

---

### Hero Feature 1 — AI Anomaly Detection with LLM Narrative Alerts

**What it does:**

A third consumer (`anomaly_detector.py`) subscribes to `demo_test` in its own consumer group
(isolated from the S3 archiver). It maintains a rolling window of the last 20 `CloseUSD` values
per stock index symbol. When a new message's price deviates by more than 2.5 standard deviations
from the rolling mean, it:

1. Calls the Claude API (`claude-haiku-4-5-20251001` — fast, cheap) with the anomalous record
   and the 5 prior records
2. Asks for a one-sentence plain-English explanation of the anomaly
3. Publishes the alert (symbol, z-score, Claude's explanation, timestamp) to a `demo_alerts`
   Kafka topic
4. Optionally fires a Slack webhook

**Example alert output:**
```json
{
  "symbol": "HSI",
  "timestamp": "2021-05-12",
  "close_usd": 28482.11,
  "z_score": 3.14,
  "explanation": "HSI closed 3.1σ above its 20-day rolling mean, consistent with post-pandemic
                  recovery momentum and a broader Asian markets rally reported in May 2021.",
  "alert_fired_at": "2024-01-15T10:32:01Z"
}
```

**Why it impresses a Senior Engineer:**
- Shows consumer group isolation — the anomaly detector doesn't affect the S3 archiver's offsets
- Raises a real architecture question: *should LLM calls be synchronous in the consumer loop?*
  Answer: no — they should be decoupled via an async task queue (Celery, `asyncio`, or another
  Kafka topic) to prevent head-of-line blocking on the hot path
- Demonstrates multi-stage stream processing, not just "read and write"

**New files:** `anomaly_detector.py`, updated `requirements.txt` (`anthropic>=0.25.0`)

---

### Hero Feature 2 — Real-Time Streamlit Dashboard with Kafka Lag Monitoring

**What it does:**

A `dashboard.py` Streamlit app with two panels, refreshed every 2 seconds:

**Panel 1 — Live Market Feed:**
- Plotly `Candlestick` chart of the last 50 OHLCV records per selected index symbol
- Sourced directly from an in-memory ring buffer updated by a background thread consuming Kafka

**Panel 2 — Pipeline Health:**
| Metric | Source |
|---|---|
| Consumer lag (msgs behind) | Prometheus gauge from consumer |
| Throughput (msgs/sec) | Prometheus counter delta |
| S3 write latency (p50/p95) | Prometheus histogram |
| DLQ depth (failed messages) | Prometheus counter |
| Error rate | Prometheus counter |

**Why it impresses a Senior Engineer:**
- Consumer lag is *the* canonical KPI for Kafka health — knowing it exists and visualizing it
  signals you understand Kafka internals
- Forces you to understand partition assignment, offset tracking, and `__consumer_offsets`
- Demonstrates the full observability loop: instrument → scrape → visualize
- Prompts the question: *"How do you prevent the dashboard's Kafka consumer from affecting
  the main consumer group's lag?"* — Answer: separate consumer group with `auto.offset.reset=latest`

**New files:** `dashboard.py`, updated `requirements.txt` (`streamlit`, `plotly`, `prometheus_client`)

---

### Hero Feature 3 — Schema Registry + Dead Letter Queue (DLQ) Pattern

**What it does:**

**Schema Registry:**
- Register the stock record Avro schema with AWS Glue Schema Registry (free, already in stack)
- Producer serializes with `aws-glue-schema-registry` Python library
- Consumer validates on deserialize — schema mismatch raises an exception before S3 write
- Schema evolution example: add a nullable `sentiment_score: float | null` field with
  BACKWARD compatibility — old consumers continue working, new consumers read the new field

**Dead Letter Queue:**
- Any message that fails Avro deserialization, Pydantic validation, or S3 write is not silently
  dropped — it is published to `demo_test_dlq` Kafka topic with error metadata:
  ```json
  {"original_payload": "...", "error": "AvroException: unknown field 'Extra'",
   "failed_at": "2024-01-15T10:32:01Z", "consumer_id": "consumer-0", "partition": 3, "offset": 4821}
  ```
- A `dlq_monitor.py` script alerts (Slack/log) when DLQ depth exceeds a threshold

**Why it impresses a Senior Engineer:**
- DLQ is a standard production pattern (Lambda, SQS, Kafka Streams all support it) — its
  absence is the first thing an SRE will flag in a code review
- Schema Registry enforces a data contract — without it, a producer deploy can silently
  break all consumers (the "schema drift" problem)
- Prompts the question: *"What's the difference between BACKWARD and FORWARD schema
  compatibility?"*
  - **BACKWARD**: new schema can read data written by old schema (add optional fields)
  - **FORWARD**: old schema can read data written by new schema (remove optional fields)
  - **FULL**: both directions simultaneously

**New files:** Updated `producer.py` / `consumer.py`, `dlq_monitor.py`,
`schemas/stock_record.avsc`

---

## 4. Production Readiness

### 4.1 Scalability

**Kafka topic partitioning:**
```
Current:  1 partition, no partition key → all messages to 1 consumer
Target:   8 partitions, partition key = Index symbol (hash)
Result:   Each stock index always lands in the same partition → ordering preserved per symbol
          → 8 consumer instances can process in parallel
```

**Consumer group parallelism:**
- Run 2–3 `consumer.py` instances in the same consumer group
- Kafka auto-assigns partitions via the group coordinator
- Adding instances triggers a rebalance — handled automatically by `confluent-kafka-python`

**Batched S3 writes (Parquet):**
```
Current:  1 JSON file per message → 86,400 files/day
Target:   Flush buffer every 60s or 1,000 messages → 1,440 files/day (60× fewer)
Format:   Parquet (pyarrow) → 5× compression vs JSON, 10× faster Athena scans
Cost:     S3 PUT costs drop from ~$0.43/day to ~$0.007/day
```

**Backpressure:**
- `confluent-kafka-python`'s `poll()` loop naturally applies backpressure — if the consumer
  is slow, Kafka holds messages and consumer lag grows (visible on the dashboard)
- Set `max.poll.interval.ms` appropriately to avoid spurious rebalances during S3 writes

### 4.2 Security

| Concern | Current State | Target State |
|---|---|---|
| Kafka transport | Plaintext | SASL/SCRAM-SHA-512 + TLS (`security.protocol=SASL_SSL`) |
| AWS credentials | `aws configure` / env vars | IAM instance profile (EC2) or IAM Roles for Service Accounts (EKS). No long-lived keys. |
| S3 encryption | None specified | SSE-S3 (AES-256) at bucket level; optionally SSE-KMS for audit trail |
| S3 access | Bucket-level permissions | Least-privilege: `s3:PutObject` on `arn:aws:s3:::bucket/prefix/*` only |
| Secrets | Hardcoded in notebooks | AWS Secrets Manager (prod) or `.env` / `python-dotenv` (local). `.env` in `.gitignore`. |
| Network | EC2 SG open to `0.0.0.0/0` | Restrict inbound 9092 to specific CIDR; use VPC endpoints for S3 |

### 4.3 Observability

**Structured Logging (`structlog`):**
```python
log = structlog.get_logger()
log.info("message_produced", topic=KAFKA_TOPIC, symbol=record["Index"],
         date=record["Date"], latency_ms=elapsed)
```
JSON log output ships directly to CloudWatch Logs without a parsing rule.
Add `trace_id` (UUID per producer run) as a correlation field for distributed tracing.

**Prometheus Metrics (exposed on `:8000/metrics`):**

| Metric | Type | Labels |
|---|---|---|
| `messages_produced_total` | Counter | `topic`, `symbol` |
| `messages_consumed_total` | Counter | `topic`, `partition` |
| `consumer_lag_messages` | Gauge | `topic`, `partition`, `consumer_group` |
| `s3_write_duration_seconds` | Histogram | `bucket`, `status` |
| `dlq_messages_total` | Counter | `topic`, `error_type` |
| `anomaly_alerts_total` | Counter | `symbol`, `severity` |

**Distributed Tracing (OpenTelemetry):**
- Propagate W3C `traceparent` header as a Kafka message header
- Spans: `produce_message` → `consume_message` → `s3_write`
- Export to AWS X-Ray or a local Jaeger instance (included in `docker-compose.yml`)

**Alerting:**
- CloudWatch Alarm: `consumer_lag_messages > 1000` → SNS → email/Slack
- CloudWatch Alarm: `dlq_messages_total rate > 0` → immediate page

---

## 5. Resume Talking Points

These are the three questions a Senior Engineer or Staff Engineer is most likely to ask
based on this project. Have a confident, detailed answer ready for each.

---

### Talking Point 1 — "I designed an exactly-once delivery pipeline on Kafka"

**What to say:**
> "By default, Kafka gives you at-least-once delivery — a producer can retry a failed send
> and the broker may write the same message twice. I addressed this with two changes:
> idempotent producer (`enable.idempotence=True`) which assigns a producer ID + sequence
> number so the broker deduplicates retries, and transactional writes
> (`producer.init_transactions()`) which atomically commits a batch of messages across
> partitions. On the consumer side, `isolation.level=read_committed` ensures only
> committed transactions are visible. The tradeoff is ~10% throughput reduction and
> increased broker load — acceptable for financial data where duplicate records
> would corrupt aggregations."

**Follow-up they'll ask:** *"What happens to exactly-once semantics at the S3 sink?"*
Answer: S3 writes are not transactional — you need idempotent file naming (content-hash
or offset-range in the filename) to make the consumer-to-S3 leg idempotent.

---

### Talking Point 2 — "I solved the S3 small-files problem with micro-batch windowing"

**What to say:**
> "The original design wrote one JSON file to S3 per Kafka message. At 1 message/second
> that's 86,400 files/day. The problems compound: Athena scans every file individually
> (each scan has a minimum cost floor), Glue crawler takes hours to catalog millions of
> objects, S3 LIST operations become expensive, and the S3 PUT request cost alone runs
> ~$0.43/day. I replaced this with a time-and-size-bounded buffer: flush to S3 every 60
> seconds or 1,000 messages, whichever comes first. I also switched from JSON to Parquet
> (via `pyarrow`) which gives 5× compression and columnar pruning — Athena only reads
> the columns in your `SELECT` clause. Together this cuts daily file count from 86,400
> to ~1,440 and reduces Athena scan costs by roughly 80%."

**Follow-up they'll ask:** *"How do you handle late-arriving messages in a windowed buffer?"*
Answer: Accept a configurable late-arrival tolerance (e.g., 30s). Messages arriving after
the window closes go to the next window. For strict ordering, add a watermark based on
message timestamps rather than wall-clock time.

---

### Talking Point 3 — "I integrated an LLM into a real-time stream for anomaly narration"

**What to say:**
> "I added a third consumer in its own consumer group — it doesn't interfere with the S3
> archiver's offsets at all. It maintains a per-symbol rolling Z-score over the last 20
> price ticks. When a spike exceeds 2.5σ, it makes an async call to the Claude API with
> the anomalous record and 5-tick context window, asking for a one-sentence plain-English
> explanation. The LLM call is decoupled from the main consumer loop — the anomaly event
> is published to a `demo_alerts` Kafka topic and a separate async worker handles the API
> call. This prevents the ~300ms LLM latency from blocking the consumer's `poll()` loop,
> which would cause consumer lag to spike and potentially trigger a group rebalance."

**Follow-up they'll ask:** *"How do you control LLM API costs at scale?"*
Answer: Use `claude-haiku-4-5-20251001` (cheapest, fastest model). Add a rate limiter
(token bucket, 10 alerts/minute max). Cache explanations for recurring anomaly patterns
using a Redis TTL cache keyed by (symbol, z-score bucket).

---

## 6. Implementation Phases

### Phase 0 — Foundation (1–2 days) ✅ started

- [x] `requirements.txt` — pinned dependency versions
- [x] `docker-compose.yml` — Redpanda + Redpanda Console for local dev
- [x] `.env.example` — template for all configuration values
- [x] `.gitignore` — exclude `.env`, credentials, notebook checkpoints
- [ ] `producer.py` — script version of producer with env-var config, graceful shutdown, `structlog`
- [ ] `consumer.py` — script version of consumer with env-var config, graceful shutdown, `structlog`
- [ ] Fix notebooks: move `producer.flush()` before loop exit, remove debug cells, fix duplicate imports

### Phase 1 — Production Readiness (2–3 days)

- [ ] Micro-batch S3 writes (time/size window → Parquet via `pyarrow`)
- [ ] Pydantic v2 schema for `StockRecord` — validate before produce and after consume
- [ ] `prometheus_client` metrics in consumer (lag, throughput, S3 latency, errors)
- [ ] Dead Letter Queue (`demo_test_dlq` topic + `dlq_monitor.py`)
- [ ] SASL/SSL config options in `producer.py` and `consumer.py`

### Phase 2 — Hero Features (3–5 days)

- [ ] Avro serialization + AWS Glue Schema Registry (`schemas/stock_record.avsc`)
- [ ] `anomaly_detector.py` — rolling Z-score + Claude API alerts
- [ ] `dashboard.py` — Streamlit live chart + Prometheus-backed pipeline health panel
- [ ] `dlq_monitor.py` — DLQ depth alerting

### Phase 3 — Portfolio Polish (1 day)

- [ ] Expand `README.md` — architecture diagram, feature list, setup instructions, demo GIF
- [ ] GitHub Actions CI — `ruff` lint, `mypy` type check, Pydantic schema unit tests
- [ ] Demo screencast (2 min) linked from README

---

## Estimated Impact on Resume

| Signal | Before | After |
|---|---|---|
| Technologies demonstrated | Kafka, S3, Glue, Athena | + Confluent Schema Registry, Prometheus, Streamlit, Avro, OpenTelemetry, Claude API |
| Architecture patterns | Pub/sub | + DLQ, micro-batching, consumer groups, exactly-once, schema evolution, LLM integration |
| Operational maturity | None | Structured logging, metrics, alerting, graceful shutdown, Docker dev environment |
| Interview talking points | "I streamed CSV to S3" | Exactly-once semantics, small-files problem, LLM decoupling, schema compatibility modes |

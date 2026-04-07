"""
Kafka consumer — reads stock records from Kafka and archives to S3 as Parquet.

Key design decisions vs the original notebook:
  - Micro-batch writer: buffers messages and flushes every BATCH_SIZE messages
    or BATCH_FLUSH_SECONDS seconds, whichever comes first.  This reduces S3 PUT
    requests from ~86 400/day (one per message) to ~1 440/day (one per minute).
  - Parquet format: ~5x compression vs JSON; Athena only reads queried columns.
  - Dead Letter Queue: messages that fail validation or S3 writes are routed to
    KAFKA_DLQ_TOPIC instead of crashing the consumer.
  - Prometheus metrics: exposed on PROMETHEUS_PORT for Grafana/CloudWatch scraping.
  - Graceful shutdown: SIGINT/SIGTERM flush the current batch before exit.

Configuration: see .env.example — all values loaded from environment / .env.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import ValidationError

from models import DLQRecord, StockRecord

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "demo_test")
KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "demo_test_dlq")
CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID", "stock-market-consumer")
BATCH_SIZE: int = int(os.getenv("CONSUMER_BATCH_SIZE", "1000"))
BATCH_FLUSH_SECONDS: int = int(os.getenv("CONSUMER_BATCH_FLUSH_SECONDS", "60"))
S3_BUCKET: str = os.environ["S3_BUCKET"]
S3_PREFIX: str = os.getenv("S3_PREFIX", "stock-market/raw/")
PROMETHEUS_PORT: int = int(os.getenv("PROMETHEUS_PORT", "8000"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

CONSUMER_ID: str = str(uuid.uuid4())[:8]

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(__import__("logging"), LOG_LEVEL, 20)
    ),
)
log = structlog.get_logger("consumer").bind(consumer_id=CONSUMER_ID)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
MESSAGES_CONSUMED = Counter(
    "messages_consumed_total",
    "Total Kafka messages successfully consumed",
    ["topic", "partition"],
)
MESSAGES_DLQ = Counter(
    "dlq_messages_total",
    "Messages routed to the dead-letter queue",
    ["topic", "error_type"],
)
S3_WRITES = Counter("s3_writes_total", "Successful Parquet batch writes to S3", ["bucket"])
S3_WRITE_ERRORS = Counter("s3_write_errors_total", "Failed S3 batch writes", ["bucket"])
S3_WRITE_DURATION = Histogram(
    "s3_write_duration_seconds",
    "Time to write a Parquet batch to S3",
    ["bucket"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)
CONSUMER_LAG = Gauge(
    "consumer_lag_messages",
    "Estimated consumer lag (messages behind head)",
    ["topic", "partition"],
)
BATCH_SIZE_HIST = Histogram(
    "batch_size_records",
    "Number of records per Parquet batch written to S3",
    buckets=[10, 50, 100, 250, 500, 1000, 2000],
)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_running = True


def _handle_shutdown(signum: int, frame: Any) -> None:
    global _running
    log.info("shutdown_signal_received", signum=signum)
    _running = False


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)

# ---------------------------------------------------------------------------
# Parquet batch → S3
# ---------------------------------------------------------------------------

# Arrow schema matching StockRecord fields (keeps Athena-friendly column names)
_ARROW_SCHEMA = pa.schema([
    pa.field("Index", pa.string()),
    pa.field("Date", pa.string()),
    pa.field("Open", pa.float64()),
    pa.field("High", pa.float64()),
    pa.field("Low", pa.float64()),
    pa.field("Close", pa.float64()),
    pa.field("Adj Close", pa.float64()),
    pa.field("Volume", pa.float64()),
    pa.field("CloseUSD", pa.float64()),
])


def _flush_batch_to_s3(batch: list[dict], s3fs: Any) -> bool:
    """Write *batch* as a Parquet file to S3.  Returns True on success."""
    if not batch:
        return True

    now = datetime.now(timezone.utc)
    key = (
        f"{S3_PREFIX}"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"stock_market_{now.strftime('%Y%m%dT%H%M%SZ')}_{CONSUMER_ID}.parquet"
    )
    s3_path = f"s3://{S3_BUCKET}/{key}"

    t_start = time.monotonic()
    try:
        table = pa.Table.from_pylist(batch, schema=_ARROW_SCHEMA)
        buf = BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        with s3fs.open(s3_path, "wb") as f:
            f.write(buf.read())

        elapsed = time.monotonic() - t_start
        S3_WRITES.labels(bucket=S3_BUCKET).inc()
        S3_WRITE_DURATION.labels(bucket=S3_BUCKET).observe(elapsed)
        BATCH_SIZE_HIST.observe(len(batch))

        log.info(
            "batch_flushed_to_s3",
            path=s3_path,
            records=len(batch),
            duration_s=round(elapsed, 3),
        )
        return True

    except Exception as exc:
        S3_WRITE_ERRORS.labels(bucket=S3_BUCKET).inc()
        log.error("s3_write_failed", path=s3_path, error=str(exc))
        return False


# ---------------------------------------------------------------------------
# DLQ producer
# ---------------------------------------------------------------------------

def _send_to_dlq(
    dlq_producer: Producer,
    raw: str,
    error: str,
    topic: str,
    partition: int,
    offset: int,
) -> None:
    record = DLQRecord(
        original_payload=raw,
        error=error,
        failed_at=datetime.now(timezone.utc).isoformat(),
        consumer_id=CONSUMER_ID,
        topic=topic,
        partition=partition,
        offset=offset,
    )
    dlq_producer.produce(
        topic=KAFKA_DLQ_TOPIC,
        value=json.dumps(record.model_dump()).encode("utf-8"),
    )
    dlq_producer.poll(0)
    log.warning(
        "message_sent_to_dlq",
        dlq_topic=KAFKA_DLQ_TOPIC,
        original_topic=topic,
        partition=partition,
        offset=offset,
        error=error,
    )
    MESSAGES_DLQ.labels(topic=topic, error_type=type(error).__name__).inc()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Import s3fs here so missing AWS creds give a clear error at startup
    import s3fs as s3fs_lib

    log.info(
        "consumer_starting",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        group_id=CONSUMER_GROUP_ID,
        batch_size=BATCH_SIZE,
        flush_seconds=BATCH_FLUSH_SECONDS,
        s3_bucket=S3_BUCKET,
    )

    start_http_server(PROMETHEUS_PORT)
    log.info("prometheus_metrics_server_started", port=PROMETHEUS_PORT)

    s3 = s3fs_lib.S3FileSystem()

    base_config: dict = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,      # manual commit after successful S3 write
        "isolation.level": "read_committed",  # exactly-once: skip uncommitted txn messages
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
    }

    if security_protocol := os.getenv("KAFKA_SECURITY_PROTOCOL"):
        base_config["security.protocol"] = security_protocol
        base_config["sasl.mechanism"] = os.environ["KAFKA_SASL_MECHANISM"]
        base_config["sasl.username"] = os.environ["KAFKA_SASL_USERNAME"]
        base_config["sasl.password"] = os.environ["KAFKA_SASL_PASSWORD"]

    consumer = Consumer(base_config)
    consumer.subscribe([KAFKA_TOPIC])

    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    batch: list[dict] = []
    last_flush_time = time.monotonic()
    last_committed_offsets: dict[int, int] = {}  # partition → offset

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            # --- flush on time window even if no new messages ---
            time_to_flush = (time.monotonic() - last_flush_time) >= BATCH_FLUSH_SECONDS
            size_to_flush = len(batch) >= BATCH_SIZE

            if (time_to_flush or size_to_flush) and batch:
                success = _flush_batch_to_s3(batch, s3)
                if success:
                    consumer.commit(asynchronous=False)
                    batch.clear()
                    last_flush_time = time.monotonic()
                else:
                    # S3 write failed — send entire batch to DLQ
                    for item in batch:
                        _send_to_dlq(
                            dlq_producer,
                            raw=json.dumps(item),
                            error="S3 write failed after flush",
                            topic=KAFKA_TOPIC,
                            partition=-1,
                            offset=-1,
                        )
                    batch.clear()
                    last_flush_time = time.monotonic()

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug(
                        "partition_eof",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                else:
                    log.error("kafka_error", error=str(msg.error()))
                continue

            # --- deserialize and validate ---
            raw_bytes = msg.value()
            try:
                raw_dict = json.loads(raw_bytes.decode("utf-8"))
                record = StockRecord.model_validate(raw_dict)
            except (json.JSONDecodeError, ValidationError, UnicodeDecodeError) as exc:
                _send_to_dlq(
                    dlq_producer,
                    raw=raw_bytes.decode("utf-8", errors="replace"),
                    error=str(exc),
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                continue

            batch.append(record.to_kafka_dict())

            MESSAGES_CONSUMED.labels(
                topic=msg.topic(), partition=str(msg.partition())
            ).inc()

            log.debug(
                "message_consumed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                symbol=record.Index,
                date=record.Date,
            )

    finally:
        # Flush remaining batch on shutdown
        if batch:
            log.info("flushing_remaining_batch_on_shutdown", records=len(batch))
            _flush_batch_to_s3(batch, s3)
            consumer.commit(asynchronous=False)

        dlq_producer.flush(timeout=5)
        consumer.close()
        log.info("consumer_stopped")


if __name__ == "__main__":
    main()

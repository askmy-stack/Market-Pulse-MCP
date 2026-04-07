"""
Kafka producer — streams random OHLCV records from indexProcessed.csv.

Configuration is loaded entirely from environment variables (see .env.example).
Run locally:
    cp .env.example .env   # fill in your values
    python producer.py

Or against the Docker Compose Redpanda stack:
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python producer.py
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from typing import Any

import pandas as pd
import structlog
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv
from pydantic import ValidationError

from models import StockRecord

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration (all values from environment / .env)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "demo_test")
PRODUCER_CSV_PATH: str = os.getenv("PRODUCER_CSV_PATH", "data/indexProcessed.csv")
PRODUCER_INTERVAL_SECONDS: float = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

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
log = structlog.get_logger("producer")

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
# Kafka delivery callback
# ---------------------------------------------------------------------------
def _on_delivery(err: Any, msg: Any) -> None:
    if err:
        log.error(
            "delivery_failed",
            topic=msg.topic(),
            partition=msg.partition(),
            error=str(err),
        )
    else:
        log.debug(
            "delivery_confirmed",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("producer_starting", bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)

    # Load dataset
    try:
        df = pd.read_csv(PRODUCER_CSV_PATH)
    except FileNotFoundError:
        log.error("csv_not_found", path=PRODUCER_CSV_PATH)
        sys.exit(1)

    log.info("dataset_loaded", rows=len(df), path=PRODUCER_CSV_PATH)

    # Build Confluent producer
    producer_config: dict = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "enable.idempotence": True,          # exactly-once producer-side
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }

    # Optional SASL/SSL (set KAFKA_SECURITY_PROTOCOL in .env for production)
    if security_protocol := os.getenv("KAFKA_SECURITY_PROTOCOL"):
        producer_config["security.protocol"] = security_protocol
        producer_config["sasl.mechanism"] = os.environ["KAFKA_SASL_MECHANISM"]
        producer_config["sasl.username"] = os.environ["KAFKA_SASL_USERNAME"]
        producer_config["sasl.password"] = os.environ["KAFKA_SASL_PASSWORD"]

    producer = Producer(producer_config)

    messages_sent = 0
    messages_failed = 0

    try:
        while _running:
            t_start = time.monotonic()

            raw = df.sample(1).to_dict(orient="records")[0]

            # Validate with Pydantic before putting on the wire
            try:
                record = StockRecord.model_validate(raw)
            except ValidationError as exc:
                log.warning("validation_failed", error=str(exc), raw=raw)
                messages_failed += 1
                time.sleep(PRODUCER_INTERVAL_SECONDS)
                continue

            payload = json.dumps(record.to_kafka_dict()).encode("utf-8")

            # Partition by Index symbol so all records for a symbol land together
            producer.produce(
                topic=KAFKA_TOPIC,
                value=payload,
                key=record.Index.encode("utf-8"),
                on_delivery=_on_delivery,
            )
            producer.poll(0)  # trigger delivery callbacks without blocking

            messages_sent += 1
            elapsed_ms = (time.monotonic() - t_start) * 1000

            log.info(
                "message_produced",
                topic=KAFKA_TOPIC,
                symbol=record.Index,
                date=record.Date,
                close_usd=record.CloseUSD,
                latency_ms=round(elapsed_ms, 2),
                total_sent=messages_sent,
            )

            time.sleep(max(0, PRODUCER_INTERVAL_SECONDS - (time.monotonic() - t_start)))

    finally:
        log.info(
            "producer_shutting_down",
            messages_sent=messages_sent,
            messages_failed=messages_failed,
        )
        # Flush all buffered messages before exit (previously unreachable!)
        remaining = producer.flush(timeout=10)
        if remaining:
            log.warning("flush_incomplete", unflushed_messages=remaining)
        else:
            log.info("flush_complete")


if __name__ == "__main__":
    main()

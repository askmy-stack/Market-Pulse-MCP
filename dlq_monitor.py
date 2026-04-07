"""
Dead-Letter Queue monitor.

Polls the DLQ topic and logs / alerts whenever messages are present.
Run this alongside the consumer to detect and triage failed messages.

    python dlq_monitor.py

Alerts go to:
  - structlog (always)
  - Slack webhook (if SLACK_WEBHOOK_URL is set in .env)
"""

from __future__ import annotations

import json
import os
import signal
from typing import Any

import requests
import structlog
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from models import DLQRecord

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "demo_test_dlq")
SLACK_WEBHOOK_URL: str | None = os.getenv("SLACK_WEBHOOK_URL")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
DLQ_ALERT_THRESHOLD: int = int(os.getenv("DLQ_ALERT_THRESHOLD", "1"))  # alert on first message

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
log = structlog.get_logger("dlq_monitor")

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
# Slack alerting
# ---------------------------------------------------------------------------

def _slack_alert(dlq: DLQRecord) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    text = (
        f":warning: *DLQ Alert* — message failed processing\n"
        f"*Topic:* `{dlq.topic}` | *Partition:* {dlq.partition} | *Offset:* {dlq.offset}\n"
        f"*Error:* `{dlq.error}`\n"
        f"*Consumer:* `{dlq.consumer_id}` | *Failed at:* {dlq.failed_at}\n"
        f"*Payload (truncated):* ```{dlq.original_payload[:300]}```"
    )
    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": text},
            timeout=5,
        )
        resp.raise_for_status()
    except Exception as exc:
        log.warning("slack_alert_failed", error=str(exc))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info(
        "dlq_monitor_starting",
        topic=KAFKA_DLQ_TOPIC,
        alert_threshold=DLQ_ALERT_THRESHOLD,
        slack_enabled=bool(SLACK_WEBHOOK_URL),
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "dlq-monitor",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_DLQ_TOPIC])

    total_seen = 0

    try:
        while _running:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka_error", error=str(msg.error()))
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                dlq = DLQRecord.model_validate(payload)
            except Exception as exc:
                log.error(
                    "dlq_record_parse_failed",
                    error=str(exc),
                    raw=msg.value().decode("utf-8", errors="replace")[:500],
                )
                continue

            total_seen += 1

            log.warning(
                "dlq_message_received",
                total_seen=total_seen,
                original_topic=dlq.topic,
                partition=dlq.partition,
                offset=dlq.offset,
                error=dlq.error,
                consumer_id=dlq.consumer_id,
                failed_at=dlq.failed_at,
            )

            if total_seen >= DLQ_ALERT_THRESHOLD:
                _slack_alert(dlq)

    finally:
        consumer.close()
        log.info("dlq_monitor_stopped", total_messages_seen=total_seen)


if __name__ == "__main__":
    main()

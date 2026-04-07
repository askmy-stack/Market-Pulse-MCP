"""
Hero Feature 1 — AI Anomaly Detector with LLM Narrative Alerts.

Architecture:
  - Runs as a SEPARATE consumer group from consumer.py — does not affect the
    S3 archiver's offsets at all.
  - Maintains a rolling window of the last ANOMALY_ROLLING_WINDOW CloseUSD
    values per stock index symbol.
  - When a new message's price deviates by more than ANOMALY_Z_SCORE_THRESHOLD
    standard deviations from the rolling mean, it:
      1. Calls the Claude API (claude-haiku — fast, low-cost) with the anomalous
         record + recent context, requesting a one-sentence plain-English explanation.
      2. Publishes the alert to the KAFKA_ALERTS_TOPIC Kafka topic.
      3. Optionally fires a Slack webhook.
  - LLM calls are made synchronously here for simplicity; in a high-throughput
    system, decouple them with asyncio or a Celery worker to avoid blocking poll().

Configuration: see .env.example
"""

from __future__ import annotations

import json
import os
import signal
from collections import defaultdict, deque
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Any

import requests
import structlog
from anthropic import Anthropic
from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

from models import StockRecord

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "demo_test")
KAFKA_ALERTS_TOPIC: str = os.getenv("KAFKA_ALERTS_TOPIC", "demo_alerts")
ANTHROPIC_API_KEY: str = os.environ["ANTHROPIC_API_KEY"]
ANOMALY_Z_SCORE_THRESHOLD: float = float(os.getenv("ANOMALY_Z_SCORE_THRESHOLD", "2.5"))
ANOMALY_ROLLING_WINDOW: int = int(os.getenv("ANOMALY_ROLLING_WINDOW", "20"))
SLACK_WEBHOOK_URL: str | None = os.getenv("SLACK_WEBHOOK_URL")
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
log = structlog.get_logger("anomaly_detector")

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
# Rolling Z-score per symbol
# ---------------------------------------------------------------------------

# symbol → deque of recent CloseUSD values
_windows: dict[str, deque[float]] = defaultdict(
    lambda: deque(maxlen=ANOMALY_ROLLING_WINDOW)
)
# symbol → deque of recent full records (for LLM context)
_recent_records: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=6))


def _compute_z_score(symbol: str, value: float) -> float | None:
    """Return the Z-score of *value* against the rolling window, or None if not enough data."""
    window = list(_windows[symbol])
    if len(window) < 5:  # need at least 5 points for a meaningful Z-score
        return None
    mu = mean(window)
    sigma = stdev(window)
    if sigma == 0:
        return 0.0
    return (value - mu) / sigma


# ---------------------------------------------------------------------------
# Claude API — narrative explanation
# ---------------------------------------------------------------------------

def _explain_anomaly(record: StockRecord, z_score: float, context: list[dict]) -> str:
    """Ask Claude to explain the anomaly in one sentence."""
    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    context_str = "\n".join(
        f"  {r['Date']}: CloseUSD={r['CloseUSD']}" for r in context
    )
    prompt = (
        f"You are a financial data analyst. A stock index just triggered an anomaly alert.\n\n"
        f"Index: {record.Index}\n"
        f"Anomalous record: Date={record.Date}, CloseUSD={record.CloseUSD}, "
        f"Open={record.Open}, High={record.High}, Low={record.Low}, Volume={record.Volume}\n"
        f"Z-score: {z_score:.2f} (threshold: ±{ANOMALY_Z_SCORE_THRESHOLD})\n\n"
        f"Recent price history (last {len(context)} records):\n{context_str}\n\n"
        f"In exactly one sentence, explain what this anomaly might indicate. "
        f"Be specific about the direction and magnitude. Do not use financial advice language."
    )

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


# ---------------------------------------------------------------------------
# Alert dispatch
# ---------------------------------------------------------------------------

def _publish_alert(
    producer: Producer,
    record: StockRecord,
    z_score: float,
    explanation: str,
) -> None:
    alert = {
        "symbol": record.Index,
        "timestamp": record.Date,
        "close_usd": record.CloseUSD,
        "z_score": round(z_score, 4),
        "threshold": ANOMALY_Z_SCORE_THRESHOLD,
        "explanation": explanation,
        "alert_fired_at": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(
        topic=KAFKA_ALERTS_TOPIC,
        value=json.dumps(alert).encode("utf-8"),
        key=record.Index.encode("utf-8"),
    )
    producer.poll(0)

    log.warning(
        "anomaly_alert_fired",
        symbol=record.Index,
        date=record.Date,
        close_usd=record.CloseUSD,
        z_score=round(z_score, 4),
        explanation=explanation,
    )

    if SLACK_WEBHOOK_URL:
        direction = "spiked above" if z_score > 0 else "dropped below"
        text = (
            f":rotating_light: *Anomaly Alert — {record.Index}*\n"
            f"Z-score: `{z_score:.2f}` ({direction} {ANOMALY_Z_SCORE_THRESHOLD}σ threshold)\n"
            f"Date: {record.Date} | CloseUSD: {record.CloseUSD}\n"
            f"_{explanation}_"
        )
        try:
            requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5).raise_for_status()
        except Exception as exc:
            log.warning("slack_alert_failed", error=str(exc))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info(
        "anomaly_detector_starting",
        topic=KAFKA_TOPIC,
        alerts_topic=KAFKA_ALERTS_TOPIC,
        z_threshold=ANOMALY_Z_SCORE_THRESHOLD,
        window=ANOMALY_ROLLING_WINDOW,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "anomaly-detector",   # independent consumer group — own offsets
            "auto.offset.reset": "latest",    # only care about new messages, not history
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    alert_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    total_processed = 0
    total_alerts = 0

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka_error", error=str(msg.error()))
                continue

            try:
                raw = json.loads(msg.value().decode("utf-8"))
                record = StockRecord.model_validate(raw)
            except Exception as exc:
                log.warning("message_parse_failed", error=str(exc))
                continue

            total_processed += 1
            symbol = record.Index
            price = record.CloseUSD

            # Store for context before updating window
            context = list(_recent_records[symbol])
            _recent_records[symbol].append(record.to_kafka_dict())

            z_score = _compute_z_score(symbol, price)

            # Update rolling window AFTER computing z-score
            _windows[symbol].append(price)

            if z_score is not None and abs(z_score) >= ANOMALY_Z_SCORE_THRESHOLD:
                log.info(
                    "anomaly_candidate",
                    symbol=symbol,
                    z_score=round(z_score, 4),
                    price=price,
                    calling_claude=True,
                )
                try:
                    explanation = _explain_anomaly(record, z_score, context)
                except Exception as exc:
                    log.error("claude_api_failed", error=str(exc))
                    explanation = (
                        f"Anomaly detected (Z={z_score:.2f}); LLM explanation unavailable."
                    )

                _publish_alert(alert_producer, record, z_score, explanation)
                total_alerts += 1
            else:
                log.debug(
                    "record_processed",
                    symbol=symbol,
                    price=price,
                    z_score=round(z_score, 4) if z_score is not None else None,
                    window_size=len(_windows[symbol]),
                )

    finally:
        alert_producer.flush(timeout=5)
        consumer.close()
        log.info(
            "anomaly_detector_stopped",
            total_processed=total_processed,
            total_alerts=total_alerts,
        )


if __name__ == "__main__":
    main()

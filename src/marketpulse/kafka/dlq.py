"""Dead-letter queue helpers for poison Kafka messages."""

from __future__ import annotations

import json
import time
import traceback
from typing import Any

from confluent_kafka import Message, Producer

from marketpulse.kafka.topics import PIPELINE_DLQ
from marketpulse.observability.logging import get_logger

logger = get_logger(__name__)


def build_dlq_payload(
    msg: Message,
    *,
    component: str,
    error: BaseException,
) -> dict[str, Any]:
    """Serialize a failed message plus error context for the DLQ topic."""
    raw = msg.value()
    try:
        original = json.loads(raw.decode("utf-8")) if raw else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        original = {"raw_base64": None if raw is None else raw.hex()}

    return {
        "dlq_version": 1,
        "component": component,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "error_traceback": "".join(traceback.format_exception(type(error), error, error.__traceback__)),
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "source_key": None if msg.key() is None else msg.key().decode("utf-8", errors="replace"),
        "failed_at": time.time(),
        "original_payload": original,
    }


def route_poison_message(
    producer: Producer,
    msg: Message,
    *,
    component: str,
    error: BaseException,
) -> None:
    """Publish a poison message to ``pipeline_dlq`` instead of dropping it."""
    payload = build_dlq_payload(msg, component=component, error=error)
    key = payload.get("source_key") or f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
    producer.produce(
        topic=PIPELINE_DLQ,
        key=str(key),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.poll(0)
    logger.warning(
        "poison_message_routed_to_dlq",
        component=component,
        source_topic=msg.topic(),
        error_type=type(error).__name__,
    )

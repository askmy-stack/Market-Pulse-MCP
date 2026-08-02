"""Tests for Kafka dead-letter queue helpers."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from marketpulse.kafka.dlq import build_dlq_payload, route_poison_message
from marketpulse.kafka.topics import PIPELINE_DLQ


def _fake_msg(**overrides):
    base = {
        "topic": lambda: "stock_ticks",
        "partition": lambda: 0,
        "offset": lambda: 42,
        "key": lambda: b"AAPL",
        "value": lambda: json.dumps({"symbol": "AAPL", "price": 1}).encode(),
    }
    base.update(overrides)
    return SimpleNamespace(**{k: v if callable(v) else (lambda vv=v: vv) for k, v in base.items()})


def test_build_dlq_payload_includes_error_and_source():
    msg = _fake_msg()
    payload = build_dlq_payload(msg, component="stock-consumer", error=ValueError("bad tick"))
    assert payload["component"] == "stock-consumer"
    assert payload["error_type"] == "ValueError"
    assert payload["error_message"] == "bad tick"
    assert payload["source_topic"] == "stock_ticks"
    assert payload["source_offset"] == 42
    assert payload["original_payload"]["symbol"] == "AAPL"


def test_route_poison_message_publishes_to_pipeline_dlq():
    producer = MagicMock()
    msg = _fake_msg()
    route_poison_message(producer, msg, component="news-consumer", error=RuntimeError("boom"))
    assert producer.produce.call_count == 1
    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == PIPELINE_DLQ
    body = json.loads(kwargs["value"].decode())
    assert body["component"] == "news-consumer"
    assert body["error_type"] == "RuntimeError"

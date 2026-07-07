"""Kafka client utilities."""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel

from marketpulse.config import get_settings


def create_producer() -> Producer:
    settings = get_settings()
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.kafka_client_id,
        }
    )


def create_consumer(group_id: str | None = None, topics: list[str] | None = None) -> Consumer:
    settings = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id or settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    if topics:
        consumer.subscribe(topics)
    return consumer


def publish_event(producer: Producer, topic: str, key: str, event: BaseModel) -> None:
    payload = event.model_dump_json()
    producer.produce(topic=topic, key=key, value=payload)
    producer.poll(0)


def parse_message(value: bytes | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value.decode("utf-8"))

"""News consumer — validates, persists, and publishes embeddings."""

from __future__ import annotations

import signal

from marketpulse.db.repository import Repository
from marketpulse.db.session import get_db, init_db
from marketpulse.kafka.client import create_consumer, create_producer, parse_message, publish_event
from marketpulse.kafka.dlq import route_poison_message
from marketpulse.kafka.topics import COMPANY_NEWS, MARKET_NEWS, NEWS_EMBEDDINGS
from marketpulse.news.embeddings import embed_news
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.observability.metrics import (
    EMBEDDINGS_PUBLISHED,
    NEWS_ARTICLES_INGESTED,
    NEWS_PROCESSED,
    PIPELINE_LAG,
)
from marketpulse.schemas.events import NewsEvent, PipelineHealthEvent

logger = get_logger(__name__)
_running = True


def _shutdown(*_args) -> None:
    global _running
    _running = False


def run() -> None:
    setup_logging()
    init_db()
    consumer = create_consumer(group_id="news-consumer", topics=[MARKET_NEWS, COMPANY_NEWS])
    producer = create_producer()
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    logger.info("news_consumer_started")

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("consumer_error", error=str(msg.error()))
            continue
        try:
            data = parse_message(msg.value())
            event = NewsEvent.model_validate(data)
            with get_db() as session:
                repo = Repository(session)
                repo.save_news(event)
                repo.save_health(
                    PipelineHealthEvent(
                        component="news-consumer", status="healthy", message="news processed"
                    )
                )

                embedding_payload = embed_news(event)
                repo.save_news_embedding(event.event_id, embedding_payload)
                publish_event(producer, NEWS_EMBEDDINGS, event.event_id, embedding_payload)
                producer.flush(1)
                EMBEDDINGS_PUBLISHED.labels(source=event.source).inc()

            NEWS_PROCESSED.labels(category=event.category.value).inc()
            NEWS_ARTICLES_INGESTED.labels(category=event.category.value).inc()
            if msg.timestamp()[1] is not None:
                import time

                lag = time.time() - (msg.timestamp()[1] / 1000.0)
                PIPELINE_LAG.labels(component="news-consumer", topic=msg.topic()).set(max(0, lag))
            logger.info("news_saved", headline=event.headline[:60])
        except Exception as exc:
            logger.error("news_processing_failed", error=str(exc))
            route_poison_message(producer, msg, component="news-consumer", error=exc)
            producer.flush(1)

    consumer.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()

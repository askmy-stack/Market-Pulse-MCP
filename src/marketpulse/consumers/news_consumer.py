"""News consumer — validates and persists news events."""

from __future__ import annotations

import signal

from marketpulse.db.repository import Repository
from marketpulse.db.session import get_db, init_db
from marketpulse.kafka.client import create_consumer, parse_message
from marketpulse.kafka.topics import COMPANY_NEWS, MARKET_NEWS
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.observability.metrics import NEWS_PROCESSED
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
                    PipelineHealthEvent(component="news-consumer", status="healthy", message="news processed")
                )
            NEWS_PROCESSED.labels(category=event.category.value).inc()
            logger.info("news_saved", headline=event.headline[:60])
        except Exception as exc:
            logger.error("news_processing_failed", error=str(exc))

    consumer.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()

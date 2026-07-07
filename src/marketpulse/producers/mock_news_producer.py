"""Mock news producer."""

from __future__ import annotations

import time

from marketpulse.config import get_settings
from marketpulse.kafka.client import create_producer, publish_event
from marketpulse.kafka.topics import COMPANY_NEWS, MARKET_NEWS
from marketpulse.news.cleaner import clean_headline
from marketpulse.news.providers import generate_news_batch
from marketpulse.news.sentiment import analyze_sentiment
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.schemas.events import NewsCategory

logger = get_logger(__name__)


class MockNewsProducer:
    def __init__(self):
        self.settings = get_settings()
        self.producer = create_producer()

    def run(self) -> None:
        logger.info("starting_mock_news_producer")
        while True:
            for event in generate_news_batch():
                event.headline = clean_headline(event.headline)
                event.sentiment_score = analyze_sentiment(event.headline + " " + event.summary)
                topic = MARKET_NEWS if event.category == NewsCategory.MARKET else COMPANY_NEWS
                key = event.symbols[0] if event.symbols else "market"
                publish_event(self.producer, topic, key, event)
                self.producer.flush(1)
                logger.info("published_news", headline=event.headline[:60], category=event.category.value)
            time.sleep(self.settings.mock_news_interval_seconds)


def main() -> None:
    setup_logging()
    MockNewsProducer().run()


if __name__ == "__main__":
    main()

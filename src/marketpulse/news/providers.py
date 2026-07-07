"""News data providers — mock, NewsAPI, and Finnhub."""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from datetime import datetime

import httpx

from marketpulse.config import get_settings
from marketpulse.observability.logging import get_logger
from marketpulse.schemas.events import NewsCategory, NewsEvent

logger = get_logger(__name__)

MARKET_HEADLINES = [
    "Fed signals cautious stance on rate cuts amid inflation data",
    "S&P 500 edges higher as tech leads broad market rally",
    "Oil prices slip on demand concerns",
    "Treasury yields rise ahead of jobs report",
    "Global markets mixed as investors weigh growth outlook",
]

COMPANY_TEMPLATES = [
    "{symbol} beats earnings expectations, shares jump in pre-market",
    "{symbol} announces new AI product line",
    "{symbol} faces regulatory scrutiny over data practices",
    "Analysts upgrade {symbol} citing strong cloud growth",
    "{symbol} CEO outlines long-term strategy at investor day",
    "Supply chain improvements boost {symbol} margins",
]


class NewsProvider(ABC):
    @abstractmethod
    def fetch_batch(self) -> list[NewsEvent]:
        pass


class MockNewsProvider(NewsProvider):
    def fetch_batch(self) -> list[NewsEvent]:
        events = [self._market_news()]
        if random.random() > 0.3:
            events.append(self._company_news())
        return events

    def _market_news(self) -> NewsEvent:
        headline = random.choice(MARKET_HEADLINES)
        return NewsEvent(
            headline=headline,
            summary=f"Market update: {headline}. Broad indices reacted with moderate moves.",
            category=NewsCategory.MARKET,
            symbols=[],
            sentiment_score=random.uniform(-0.3, 0.5),
            published_at=datetime.utcnow(),
            source="mock_market_wire",
        )

    def _company_news(self, symbol: str | None = None) -> NewsEvent:
        settings = get_settings()
        symbol = symbol or random.choice(settings.symbol_list)
        headline = random.choice(COMPANY_TEMPLATES).format(symbol=symbol)
        return NewsEvent(
            headline=headline,
            summary=f"Company news for {symbol}: {headline}",
            category=NewsCategory.COMPANY,
            symbols=[symbol],
            sentiment_score=random.uniform(-0.8, 0.8),
            published_at=datetime.utcnow(),
            source="mock_company_wire",
        )


class NewsAPIProvider(NewsProvider):
    """Fetch headlines from NewsAPI.org."""

    BASE_URL = "https://newsapi.org/v2/top-headlines"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or get_settings().news_api_key
        if not self.api_key:
            raise ValueError("NEWS_API_KEY is required for NewsAPIProvider")

    def fetch_batch(self) -> list[NewsEvent]:
        events: list[NewsEvent] = []
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(
                    self.BASE_URL,
                    params={
                        "category": "business",
                        "language": "en",
                        "pageSize": 5,
                        "apiKey": self.api_key,
                    },
                )
                resp.raise_for_status()
                articles = resp.json().get("articles", [])
                for article in articles:
                    title = article.get("title") or "Market headline"
                    if title == "[Removed]":
                        continue
                    events.append(
                        NewsEvent(
                            headline=title,
                            summary=article.get("description") or title,
                            category=NewsCategory.MARKET,
                            symbols=[],
                            published_at=datetime.utcnow(),
                            source=article.get("source", {}).get("name", "newsapi"),
                            url=article.get("url"),
                        )
                    )
        except Exception as exc:
            logger.warning("newsapi_fetch_failed", error=str(exc))
        return events or MockNewsProvider().fetch_batch()


class FinnhubProvider(NewsProvider):
    """Fetch company news from Finnhub."""

    BASE_URL = "https://finnhub.io/api/v1/news"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or get_settings().finnhub_api_key
        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY is required for FinnhubProvider")

    def fetch_batch(self) -> list[NewsEvent]:
        settings = get_settings()
        events: list[NewsEvent] = []
        symbol = random.choice(settings.symbol_list)
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(
                    f"{self.BASE_URL}",
                    params={"category": "general", "token": self.api_key},
                )
                resp.raise_for_status()
                for item in resp.json()[:5]:
                    events.append(
                        NewsEvent(
                            headline=item.get("headline", "Finnhub headline"),
                            summary=item.get("summary", ""),
                            category=NewsCategory.COMPANY
                            if symbol in (item.get("related") or "")
                            else NewsCategory.MARKET,
                            symbols=[symbol] if symbol in (item.get("related") or "") else [],
                            published_at=datetime.utcfromtimestamp(item.get("datetime", 0)),
                            source="finnhub",
                            url=item.get("url"),
                        )
                    )
        except Exception as exc:
            logger.warning("finnhub_fetch_failed", error=str(exc))
        return events or MockNewsProvider().fetch_batch()


def get_news_provider() -> NewsProvider:
    settings = get_settings()
    provider = settings.effective_news_provider
    if provider == "newsapi" and settings.news_api_key:
        return NewsAPIProvider(settings.news_api_key)
    if provider == "finnhub" and settings.finnhub_api_key:
        return FinnhubProvider(settings.finnhub_api_key)
    return MockNewsProvider()


# Backward-compatible helpers
def generate_market_news() -> NewsEvent:
    return MockNewsProvider()._market_news()


def generate_company_news(symbol: str | None = None) -> NewsEvent:
    return MockNewsProvider()._company_news(symbol)


def generate_news_batch() -> list[NewsEvent]:
    return get_news_provider().fetch_batch()

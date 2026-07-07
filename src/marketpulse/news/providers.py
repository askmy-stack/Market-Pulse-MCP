"""Mock news data providers."""

from __future__ import annotations

import random
from datetime import datetime

from marketpulse.config import get_settings
from marketpulse.schemas.events import NewsCategory, NewsEvent

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


def generate_market_news() -> NewsEvent:
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


def generate_company_news(symbol: str | None = None) -> NewsEvent:
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


def generate_news_batch() -> list[NewsEvent]:
    events = [generate_market_news()]
    if random.random() > 0.3:
        events.append(generate_company_news())
    return events

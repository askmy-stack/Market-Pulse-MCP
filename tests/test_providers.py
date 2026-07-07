"""Test news data providers."""

from unittest.mock import MagicMock, patch

from marketpulse.news.providers import (
    FinnhubProvider,
    MockNewsProvider,
    NewsAPIProvider,
    get_news_provider,
)
from marketpulse.schemas.events import NewsCategory


def test_mock_provider_returns_events():
    events = MockNewsProvider().fetch_batch()
    assert len(events) >= 1
    assert all(e.headline for e in events)


def test_mock_provider_market_category():
    event = MockNewsProvider()._market_news()
    assert event.category == NewsCategory.MARKET


@patch("marketpulse.news.providers.httpx.Client")
def test_newsapi_provider_parses_articles(mock_client_cls):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "articles": [
            {
                "title": "Markets rally",
                "description": "Stocks up",
                "source": {"name": "Reuters"},
                "url": "http://x",
            }
        ]
    }
    mock_resp.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp

    events = NewsAPIProvider(api_key="test-key").fetch_batch()
    assert len(events) == 1
    assert events[0].headline == "Markets rally"


@patch("marketpulse.news.providers.httpx.Client")
def test_finnhub_provider_parses_articles(mock_client_cls):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {
            "headline": "Tech stocks surge",
            "summary": "NASDAQ up",
            "datetime": 1700000000,
            "url": "http://y",
            "related": "AAPL",
        }
    ]
    mock_resp.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp

    events = FinnhubProvider(api_key="test-key").fetch_batch()
    assert len(events) >= 1


def test_get_news_provider_defaults_to_mock(monkeypatch):
    monkeypatch.delenv("NEWS_API_KEY", raising=False)
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setenv("ENABLE_REAL_NEWS_DATA", "false")
    from marketpulse.config import get_settings

    get_settings.cache_clear()
    provider = get_news_provider()
    assert isinstance(provider, MockNewsProvider)
    get_settings.cache_clear()

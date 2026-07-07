"""Test event schema validation."""

from datetime import datetime

from marketpulse.schemas.events import (
    AnomalySeverity,
    CorrelatedMarketContextEvent,
    NewsCategory,
    NewsEvent,
    StockAnomalyEvent,
    StockFeatureEvent,
    StockTickEvent,
)


def test_stock_tick_event():
    event = StockTickEvent(symbol="AAPL", price=190.5, volume=1000)
    assert event.symbol == "AAPL"
    assert event.price == 190.5
    assert event.event_id


def test_news_event():
    event = NewsEvent(
        headline="Test headline",
        summary="Test summary",
        category=NewsCategory.MARKET,
    )
    assert event.category == NewsCategory.MARKET


def test_stock_feature_event():
    event = StockFeatureEvent(symbol="MSFT", z_score=2.1, volatility=0.02)
    assert event.z_score == 2.1


def test_stock_anomaly_event():
    event = StockAnomalyEvent(
        symbol="TSLA",
        anomaly_type="price_zscore",
        severity=AnomalySeverity.HIGH,
        z_score=3.0,
        volume_ratio=2.5,
        price=250.0,
        price_change_pct=-2.5,
        description="test anomaly",
    )
    assert event.severity == AnomalySeverity.HIGH


def test_correlated_context_event():
    event = CorrelatedMarketContextEvent(
        symbol="NVDA",
        price_change_pct=1.5,
        sentiment_summary="neutral",
        explanation="test explanation",
    )
    assert event.symbol == "NVDA"
    assert event.timestamp <= datetime.utcnow()

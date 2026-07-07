"""Test news correlation logic."""

from datetime import datetime, timedelta

from marketpulse.news.correlator import NewsCorrelator
from marketpulse.schemas.events import AnomalySeverity, NewsCategory, NewsEvent, StockAnomalyEvent


def test_correlate_news_within_window():
    now = datetime.utcnow()
    anomaly = StockAnomalyEvent(
        symbol="AAPL",
        timestamp=now,
        anomaly_type="price_zscore",
        severity=AnomalySeverity.HIGH,
        z_score=3.0,
        volume_ratio=2.0,
        price=190.0,
        price_change_pct=2.0,
        description="test",
    )
    news = [
        NewsEvent(
            headline="AAPL beats earnings",
            summary="Strong quarter",
            category=NewsCategory.COMPANY,
            symbols=["AAPL"],
            sentiment_score=0.7,
            published_at=now - timedelta(minutes=10),
        ),
        NewsEvent(
            headline="Unrelated market news",
            summary="Broad market",
            category=NewsCategory.MARKET,
            symbols=[],
            sentiment_score=0.1,
            published_at=now - timedelta(minutes=5),
        ),
    ]
    correlator = NewsCorrelator(window_minutes=30)
    related = correlator.correlate(anomaly, news)
    assert len(related) >= 1


def test_correlate_excludes_old_news():
    now = datetime.utcnow()
    anomaly = StockAnomalyEvent(
        symbol="MSFT",
        timestamp=now,
        anomaly_type="price_zscore",
        severity=AnomalySeverity.MEDIUM,
        z_score=2.8,
        volume_ratio=1.5,
        price=420.0,
        price_change_pct=-1.0,
        description="test",
    )
    news = [
        NewsEvent(
            headline="Old MSFT news",
            summary="old",
            category=NewsCategory.COMPANY,
            symbols=["MSFT"],
            sentiment_score=0.5,
            published_at=now - timedelta(hours=2),
        )
    ]
    correlator = NewsCorrelator(window_minutes=30)
    related = correlator.correlate(anomaly, news)
    assert related == []

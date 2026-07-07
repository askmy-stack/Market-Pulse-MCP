"""Time-window news correlation with anomalies."""

from __future__ import annotations

from datetime import timedelta

from marketpulse.config import get_settings
from marketpulse.db.models import NewsArticle
from marketpulse.schemas.events import NewsEvent, StockAnomalyEvent


class NewsCorrelator:
    def __init__(self, window_minutes: int | None = None):
        self.window_minutes = window_minutes or get_settings().news_correlation_window_minutes

    def correlate(
        self,
        anomaly: StockAnomalyEvent,
        news_articles: list[NewsArticle] | list[NewsEvent],
    ) -> list[str]:
        cutoff = anomaly.timestamp - timedelta(minutes=self.window_minutes)
        related: list[tuple[str, float]] = []

        for article in news_articles:
            if isinstance(article, NewsEvent):
                pub = article.published_at
                symbols = article.symbols
                event_id = article.event_id
                sentiment = article.sentiment_score
            else:
                pub = article.published_at
                symbols = article.symbols or []
                event_id = article.event_id
                sentiment = article.sentiment_score

            if pub < cutoff or pub > anomaly.timestamp + timedelta(minutes=5):
                continue
            if anomaly.symbol not in symbols and symbols:
                continue

            time_delta = abs((anomaly.timestamp - pub).total_seconds()) / 60
            score = abs(sentiment) + max(0, 1 - time_delta / self.window_minutes)
            related.append((event_id, score))

        related.sort(key=lambda x: x[1], reverse=True)
        return [event_id for event_id, _ in related[:5]]

    def build_sentiment_summary(self, news_ids: list[str], articles: list[NewsArticle]) -> str:
        if not news_ids:
            return "No correlated news found in the time window."
        matched = [a for a in articles if a.event_id in news_ids]
        if not matched:
            return "No correlated news found in the time window."
        avg = sum(a.sentiment_score for a in matched) / len(matched)
        tone = "positive" if avg > 0.15 else "negative" if avg < -0.15 else "neutral"
        headlines = "; ".join(a.headline[:80] for a in matched[:3])
        return f"Correlated news sentiment is {tone} (avg={avg:.2f}). Headlines: {headlines}"

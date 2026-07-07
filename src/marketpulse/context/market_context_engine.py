"""Market context correlation engine."""

from __future__ import annotations

from marketpulse import DISCLAIMER
from marketpulse.db.repository import Repository
from marketpulse.news.correlator import NewsCorrelator
from marketpulse.schemas.events import CorrelatedMarketContextEvent, StockAnomalyEvent


class MarketContextEngine:
    def __init__(self, repo: Repository):
        self.repo = repo
        self.correlator = NewsCorrelator()

    def build_context(
        self, symbol: str, anomaly: StockAnomalyEvent | None = None
    ) -> CorrelatedMarketContextEvent:
        symbol = symbol.upper()
        features = self.repo.get_latest_features(symbol)
        price_change = (features.return_1m * 100) if features else 0.0

        anomalies = [anomaly] if anomaly else self.repo.get_anomalies_for_symbol(symbol, limit=3)
        anomaly_ids = [a.event_id for a in anomalies]

        news = self.repo.get_news_in_window(symbol, self.correlator.window_minutes)
        related_ids: list[str] = []
        if anomalies:
            related_ids = self.correlator.correlate(anomalies[0], news)
            anomalies[0].related_news_ids = related_ids

        sentiment_summary = self.correlator.build_sentiment_summary(related_ids, news)
        explanation = self._build_explanation(
            symbol, price_change, anomalies, related_ids, sentiment_summary
        )

        confidence = min(0.9, 0.3 + 0.1 * len(related_ids) + 0.1 * len(anomaly_ids))

        event = CorrelatedMarketContextEvent(
            symbol=symbol,
            price_change_pct=price_change,
            anomaly_ids=anomaly_ids,
            news_ids=related_ids,
            sentiment_summary=sentiment_summary,
            explanation=explanation,
            confidence=confidence,
            metadata={"disclaimer": DISCLAIMER},
        )
        self.repo.save_context(event)
        return event

    def _build_explanation(
        self,
        symbol: str,
        price_change: float,
        anomalies: list,
        news_ids: list[str],
        sentiment: str,
    ) -> str:
        direction = "rose" if price_change >= 0 else "fell"
        parts = [
            f"{symbol} {direction} approximately {abs(price_change):.2f}% in the recent window.",
        ]
        if anomalies:
            parts.append(
                f"Detected {len(anomalies)} anomal{'y' if len(anomalies) == 1 else 'ies'}: {anomalies[0].description}."
            )
        if news_ids:
            parts.append(f"Found {len(news_ids)} correlated news item(s). {sentiment}")
        else:
            parts.append("No strongly correlated news was found in the time window.")
        parts.append(DISCLAIMER)
        return " ".join(parts)

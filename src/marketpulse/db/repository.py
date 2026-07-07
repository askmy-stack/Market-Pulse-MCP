"""Data access repository."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from marketpulse.db.models import (
    MarketBrief,
    MarketContext,
    NewsArticle,
    NewsEmbedding,
    PipelineHealth,
    StockAnomaly,
    StockFeature,
    StockTick,
)
from marketpulse.schemas.events import (
    CorrelatedMarketContextEvent,
    MarketBriefEvent,
    NewsEvent,
    PipelineHealthEvent,
    StockAnomalyEvent,
    StockFeatureEvent,
    StockTickEvent,
)


class Repository:
    def __init__(self, session: Session):
        self.session = session

    def save_tick(self, event: StockTickEvent) -> StockTick:
        row = StockTick(
            event_id=event.event_id,
            symbol=event.symbol,
            price=event.price,
            volume=event.volume,
            bid=event.bid,
            ask=event.ask,
            timestamp=event.timestamp,
            source=event.source,
        )
        self.session.merge(row)
        return row

    def save_feature(self, event: StockFeatureEvent) -> StockFeature:
        row = StockFeature(
            event_id=event.event_id,
            symbol=event.symbol,
            timestamp=event.timestamp,
            return_1m=event.return_1m,
            return_5m=event.return_5m,
            volatility=event.volatility,
            z_score=event.z_score,
            volume_ratio=event.volume_ratio,
            price=event.price,
            window_size=event.window_size,
        )
        self.session.merge(row)
        return row

    def save_anomaly(self, event: StockAnomalyEvent) -> StockAnomaly:
        row = StockAnomaly(
            event_id=event.event_id,
            symbol=event.symbol,
            timestamp=event.timestamp,
            anomaly_type=event.anomaly_type,
            severity=event.severity.value,
            z_score=event.z_score,
            volume_ratio=event.volume_ratio,
            price=event.price,
            price_change_pct=event.price_change_pct,
            description=event.description,
            related_news_ids=event.related_news_ids,
        )
        self.session.merge(row)
        return row

    def save_news(self, event: NewsEvent) -> NewsArticle:
        row = NewsArticle(
            event_id=event.event_id,
            headline=event.headline,
            summary=event.summary,
            category=event.category.value,
            symbols=event.symbols,
            sentiment_score=event.sentiment_score,
            published_at=event.published_at,
            source=event.source,
            url=event.url,
        )
        self.session.merge(row)
        return row

    def save_news_embedding(self, event_id: str, payload: dict[str, Any]) -> NewsEmbedding:
        row = NewsEmbedding(
            event_id=event_id,
            embedding_model=payload.get("embedding_model", "hash"),
            dimension=payload.get("dimension", len(payload.get("embedding", []))),
            embedding=payload.get("embedding", []),
            created_at=datetime.utcnow(),
        )
        self.session.merge(row)
        return row

    def save_context(self, event: CorrelatedMarketContextEvent) -> MarketContext:
        row = MarketContext(
            event_id=event.event_id,
            symbol=event.symbol,
            timestamp=event.timestamp,
            price_change_pct=event.price_change_pct,
            anomaly_ids=event.anomaly_ids,
            news_ids=event.news_ids,
            sentiment_summary=event.sentiment_summary,
            explanation=event.explanation,
            confidence=event.confidence,
            context_metadata=event.metadata,
        )
        self.session.merge(row)
        return row

    def save_brief(self, event: MarketBriefEvent) -> MarketBrief:
        row = MarketBrief(
            event_id=event.event_id,
            brief_type=event.brief_type,
            symbols=event.symbols,
            content=event.content,
            generated_at=event.generated_at,
            disclaimer=event.disclaimer,
        )
        self.session.merge(row)
        return row

    def save_health(self, event: PipelineHealthEvent) -> PipelineHealth:
        row = PipelineHealth(
            component=event.component,
            status=event.status,
            timestamp=event.timestamp,
            message=event.message,
            metrics=event.metrics,
        )
        self.session.add(row)
        return row

    def get_latest_quote(self, symbol: str) -> StockTick | None:
        return (
            self.session.query(StockTick)
            .filter(StockTick.symbol == symbol.upper())
            .order_by(desc(StockTick.timestamp))
            .first()
        )

    def get_recent_ticks(self, symbol: str, limit: int = 50) -> list[StockTick]:
        return (
            self.session.query(StockTick)
            .filter(StockTick.symbol == symbol.upper())
            .order_by(desc(StockTick.timestamp))
            .limit(limit)
            .all()
        )

    def get_latest_features(self, symbol: str) -> StockFeature | None:
        return (
            self.session.query(StockFeature)
            .filter(StockFeature.symbol == symbol.upper())
            .order_by(desc(StockFeature.timestamp))
            .first()
        )

    def get_symbols(self) -> list[str]:
        rows = self.session.query(StockTick.symbol).distinct().all()
        return sorted({r[0] for r in rows})

    def get_latest_news(self, limit: int = 20) -> list[NewsArticle]:
        return (
            self.session.query(NewsArticle)
            .order_by(desc(NewsArticle.published_at))
            .limit(limit)
            .all()
        )

    def get_market_news(self, limit: int = 20) -> list[NewsArticle]:
        return (
            self.session.query(NewsArticle)
            .filter(NewsArticle.category == "market")
            .order_by(desc(NewsArticle.published_at))
            .limit(limit)
            .all()
        )

    def get_company_news(self, symbol: str, limit: int = 20) -> list[NewsArticle]:
        symbol = symbol.upper()
        rows = (
            self.session.query(NewsArticle)
            .order_by(desc(NewsArticle.published_at))
            .limit(200)
            .all()
        )
        return [r for r in rows if symbol in (r.symbols or [])][:limit]

    def get_news_sentiment(self, symbol: str) -> dict[str, Any]:
        articles = self.get_company_news(symbol, limit=50)
        if not articles:
            return {"symbol": symbol.upper(), "avg_sentiment": 0.0, "count": 0, "articles": []}
        scores = [a.sentiment_score for a in articles]
        return {
            "symbol": symbol.upper(),
            "avg_sentiment": sum(scores) / len(scores),
            "count": len(articles),
            "articles": [
                {"headline": a.headline, "sentiment": a.sentiment_score} for a in articles[:10]
            ],
        }

    def get_anomalies(self, limit: int = 50) -> list[StockAnomaly]:
        return (
            self.session.query(StockAnomaly)
            .order_by(desc(StockAnomaly.timestamp))
            .limit(limit)
            .all()
        )

    def get_anomaly(self, anomaly_id: str) -> StockAnomaly | None:
        return self.session.query(StockAnomaly).filter(StockAnomaly.event_id == anomaly_id).first()

    def get_anomalies_for_symbol(self, symbol: str, limit: int = 20) -> list[StockAnomaly]:
        return (
            self.session.query(StockAnomaly)
            .filter(StockAnomaly.symbol == symbol.upper())
            .order_by(desc(StockAnomaly.timestamp))
            .limit(limit)
            .all()
        )

    def get_latest_context(self, symbol: str) -> MarketContext | None:
        return (
            self.session.query(MarketContext)
            .filter(MarketContext.symbol == symbol.upper())
            .order_by(desc(MarketContext.timestamp))
            .first()
        )

    def get_context_anomalies(self, symbol: str) -> list[StockAnomaly]:
        return self.get_anomalies_for_symbol(symbol)

    def get_news_in_window(self, symbol: str, window_minutes: int) -> list[NewsArticle]:
        cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
        rows = self.get_company_news(symbol, limit=100)
        return [r for r in rows if r.published_at >= cutoff]

    def get_pipeline_health(self) -> list[PipelineHealth]:
        return (
            self.session.query(PipelineHealth)
            .order_by(desc(PipelineHealth.timestamp))
            .limit(20)
            .all()
        )

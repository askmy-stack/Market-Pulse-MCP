"""Pydantic event schemas."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class NewsCategory(str, Enum):
    MARKET = "market"
    COMPANY = "company"
    EARNINGS = "earnings"
    MACRO = "macro"


class AnomalySeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class StockTickEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    price: float
    volume: int
    bid: float | None = None
    ask: float | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = "mock"


class NewsEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    headline: str
    summary: str
    category: NewsCategory
    symbols: list[str] = Field(default_factory=list)
    sentiment_score: float = 0.0
    published_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "mock"
    url: str | None = None


class StockFeatureEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    return_1m: float = 0.0
    return_5m: float = 0.0
    volatility: float = 0.0
    z_score: float = 0.0
    volume_ratio: float = 1.0
    price: float = 0.0
    window_size: int = 20


class StockAnomalyEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    anomaly_type: str
    severity: AnomalySeverity
    z_score: float
    volume_ratio: float
    price: float
    price_change_pct: float
    description: str
    related_news_ids: list[str] = Field(default_factory=list)


class CorrelatedMarketContextEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    price_change_pct: float
    anomaly_ids: list[str] = Field(default_factory=list)
    news_ids: list[str] = Field(default_factory=list)
    sentiment_summary: str
    explanation: str
    confidence: float = 0.5
    metadata: dict[str, Any] = Field(default_factory=dict)


class MarketBriefEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    brief_type: str
    symbols: list[str] = Field(default_factory=list)
    content: str
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    disclaimer: str = "Correlation-based context only. Not financial advice. No price predictions."


class PipelineHealthEvent(BaseModel):
    component: str
    status: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    message: str = ""
    metrics: dict[str, Any] = Field(default_factory=dict)

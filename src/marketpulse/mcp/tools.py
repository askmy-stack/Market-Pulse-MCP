"""MCP tool implementations — connect to live pipeline via API/DB."""

from __future__ import annotations

import json
from typing import Any

import httpx

from marketpulse import DISCLAIMER
from marketpulse.config import get_settings
from marketpulse.context.explanation import explain_stock_move
from marketpulse.db.repository import Repository
from marketpulse.db.session import get_db, init_db
from marketpulse.news.correlator import NewsCorrelator


class MarketPulseTools:
    def __init__(self, api_base: str | None = None):
        self.api_base = (api_base or get_settings().mcp_api_base_url).rstrip("/")
        init_db()

    def _get(self, path: str) -> Any:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(f"{self.api_base}{path}")
            resp.raise_for_status()
            return resp.json()

    def _post(self, path: str, json_body: dict | None = None) -> Any:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(f"{self.api_base}{path}", json=json_body or {})
            resp.raise_for_status()
            return resp.json()

    def get_latest_quote(self, symbol: str) -> str:
        data = self._get(f"/quotes/{symbol.upper()}/latest")
        return json.dumps({**data, "disclaimer": DISCLAIMER}, default=str)

    def get_recent_ticks(self, symbol: str, limit: int = 20) -> str:
        data = self._get(f"/quotes/{symbol.upper()}/recent?limit={limit}")
        return json.dumps(data, default=str)

    def get_stock_features(self, symbol: str) -> str:
        data = self._get(f"/features/{symbol.upper()}")
        return json.dumps(data, default=str)

    def detect_market_anomalies(self, limit: int = 20) -> str:
        data = self._get(f"/anomalies?limit={limit}")
        return json.dumps({**data, "disclaimer": DISCLAIMER}, default=str)

    def get_pipeline_health(self) -> str:
        return json.dumps(self._get("/pipeline/health"), default=str)

    def list_tracked_symbols(self) -> str:
        return json.dumps(self._get("/symbols"), default=str)

    def get_company_news(self, symbol: str, limit: int = 10) -> str:
        data = self._get(f"/news/company/{symbol.upper()}?limit={limit}")
        return json.dumps(data, default=str)

    def get_market_news(self, limit: int = 10) -> str:
        data = self._get(f"/news/market?limit={limit}")
        return json.dumps(data, default=str)

    def analyze_news_sentiment(self, symbol: str) -> str:
        data = self._get(f"/news/sentiment/{symbol.upper()}")
        return json.dumps({**data, "disclaimer": DISCLAIMER}, default=str)

    def find_news_related_to_anomaly(self, anomaly_id: str) -> str:
        from marketpulse.schemas.events import AnomalySeverity, StockAnomalyEvent

        with get_db() as session:
            repo = Repository(session)
            row = repo.get_anomaly(anomaly_id)
            if not row:
                return json.dumps({"error": "Anomaly not found"})
            anomaly = StockAnomalyEvent(
                event_id=row.event_id,
                symbol=row.symbol,
                timestamp=row.timestamp,
                anomaly_type=row.anomaly_type,
                severity=AnomalySeverity(row.severity),
                z_score=row.z_score,
                volume_ratio=row.volume_ratio,
                price=row.price,
                price_change_pct=row.price_change_pct,
                description=row.description,
                related_news_ids=row.related_news_ids or [],
            )
            news = repo.get_news_in_window(row.symbol, NewsCorrelator().window_minutes)
            related = NewsCorrelator().correlate(anomaly, news)
        return json.dumps({"anomaly_id": anomaly_id, "related_news_ids": related}, default=str)

    def summarize_stock_context(self, symbol: str) -> str:
        try:
            data = self._get(f"/context/{symbol.upper()}")
        except httpx.HTTPStatusError:
            with get_db() as session:
                from marketpulse.context.market_context_engine import MarketContextEngine

                ctx = MarketContextEngine(Repository(session)).build_context(symbol)
                data = ctx.model_dump()
        return json.dumps({**data, "disclaimer": DISCLAIMER}, default=str)

    def generate_market_brief(self) -> str:
        data = self._post("/brief/market")
        return json.dumps(data, default=str)

    def explain_stock_move(self, symbol: str) -> str:
        try:
            data = self._get(f"/explain/{symbol.upper()}")
        except httpx.HTTPStatusError:
            with get_db() as session:
                data = explain_stock_move(Repository(session), symbol)
        return json.dumps(data, default=str)

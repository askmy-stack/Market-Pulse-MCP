"""FastAPI application."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from prometheus_client import make_asgi_app
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

from marketpulse import DISCLAIMER, __version__
from marketpulse.api.auth import APIKeyMiddleware
from marketpulse.config import get_settings
from marketpulse.context.brief_generator import BriefGenerator
from marketpulse.context.explanation import explain_stock_move
from marketpulse.db.repository import Repository
from marketpulse.db.session import get_db, init_db
from marketpulse.observability.logging import setup_logging
from marketpulse.observability.metrics import API_REQUEST_LATENCY, start_metrics_server


def _serialize(obj: Any) -> dict:
    if hasattr(obj, "__table__"):
        return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return dict(obj)


class LatencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        endpoint = request.url.path
        API_REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(
            time.perf_counter() - start
        )
        return response


def create_app() -> FastAPI:
    setup_logging()
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        init_db()
        start_metrics_server()
        yield

    app = FastAPI(
        title="MarketPulse MCP API",
        description="Real-time market intelligence pipeline API",
        version=__version__,
        lifespan=lifespan,
    )

    app.add_middleware(LatencyMiddleware)
    app.add_middleware(APIKeyMiddleware)

    if settings.metrics_enabled:
        app.mount("/metrics", make_asgi_app())

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "service": "marketpulse-api", "version": __version__}

    @app.get("/pipeline/health")
    def pipeline_health() -> dict:
        with get_db() as session:
            rows = Repository(session).get_pipeline_health()
        return {
            "components": [
                {
                    "component": r.component,
                    "status": r.status,
                    "timestamp": r.timestamp.isoformat(),
                    "message": r.message,
                }
                for r in rows
            ]
        }

    @app.get("/symbols")
    def list_symbols() -> dict:
        with get_db() as session:
            symbols = Repository(session).get_symbols()
        if not symbols:
            symbols = settings.symbol_list
        return {"symbols": symbols}

    @app.get("/quotes/{symbol}/latest")
    def latest_quote(symbol: str) -> dict:
        with get_db() as session:
            quote = Repository(session).get_latest_quote(symbol)
            if not quote:
                raise HTTPException(404, f"No quote found for {symbol}")
            return _serialize(quote)

    @app.get("/quotes/{symbol}/recent")
    def recent_quotes(symbol: str, limit: int = 50) -> dict:
        with get_db() as session:
            ticks = Repository(session).get_recent_ticks(symbol, limit)
        return {"symbol": symbol.upper(), "ticks": [_serialize(t) for t in ticks]}

    @app.get("/features/{symbol}")
    def get_features(symbol: str) -> dict:
        with get_db() as session:
            features = Repository(session).get_latest_features(symbol)
        if not features:
            raise HTTPException(404, f"No features for {symbol}")
        return _serialize(features)

    @app.get("/news/latest")
    def news_latest(limit: int = 20) -> dict:
        with get_db() as session:
            articles = Repository(session).get_latest_news(limit)
        return {"articles": [_serialize(a) for a in articles]}

    @app.get("/news/market")
    def news_market(limit: int = 20) -> dict:
        with get_db() as session:
            articles = Repository(session).get_market_news(limit)
        return {"articles": [_serialize(a) for a in articles]}

    @app.get("/news/company/{symbol}")
    def news_company(symbol: str, limit: int = 20) -> dict:
        with get_db() as session:
            articles = Repository(session).get_company_news(symbol, limit)
        return {"symbol": symbol.upper(), "articles": [_serialize(a) for a in articles]}

    @app.get("/news/sentiment/{symbol}")
    def news_sentiment(symbol: str) -> dict:
        with get_db() as session:
            return Repository(session).get_news_sentiment(symbol)

    @app.get("/anomalies")
    def list_anomalies(limit: int = 50) -> dict:
        with get_db() as session:
            rows = Repository(session).get_anomalies(limit)
        return {"anomalies": [_serialize(a) for a in rows]}

    @app.get("/anomalies/{anomaly_id}")
    def get_anomaly(anomaly_id: str) -> dict:
        with get_db() as session:
            row = Repository(session).get_anomaly(anomaly_id)
        if not row:
            raise HTTPException(404, "Anomaly not found")
        return _serialize(row)

    @app.get("/anomalies/symbol/{symbol}")
    def anomalies_by_symbol(symbol: str, limit: int = 20) -> dict:
        with get_db() as session:
            rows = Repository(session).get_anomalies_for_symbol(symbol, limit)
        return {"symbol": symbol.upper(), "anomalies": [_serialize(a) for a in rows]}

    @app.get("/context/{symbol}")
    def get_context(symbol: str) -> dict:
        with get_db() as session:
            ctx = Repository(session).get_latest_context(symbol)
        if not ctx:
            raise HTTPException(404, f"No context for {symbol}")
        return _serialize(ctx)

    @app.get("/context/{symbol}/anomalies")
    def context_anomalies(symbol: str) -> dict:
        with get_db() as session:
            rows = Repository(session).get_context_anomalies(symbol)
        return {"symbol": symbol.upper(), "anomalies": [_serialize(a) for a in rows]}

    @app.get("/explain/{symbol}")
    def explain(symbol: str) -> dict:
        with get_db() as session:
            return explain_stock_move(Repository(session), symbol)

    class BriefRequest(BaseModel):
        symbols: list[str] | None = None

    @app.post("/brief/market")
    def brief_market() -> dict:
        with get_db() as session:
            brief = BriefGenerator(Repository(session)).generate_market_brief()
        return _serialize(brief)

    @app.post("/brief/symbol")
    def brief_symbol(body: BriefRequest) -> dict:
        if not body.symbols:
            raise HTTPException(400, "symbols required")
        with get_db() as session:
            gen = BriefGenerator(Repository(session))
            briefs = [gen.generate_symbol_brief(s) for s in body.symbols]
        return {"briefs": [_serialize(b) for b in briefs], "disclaimer": DISCLAIMER}

    return app


def main() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "marketpulse.api.app:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
    )


if __name__ == "__main__":
    main()

"""FastAPI application."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import make_asgi_app
from pydantic import BaseModel
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from starlette.middleware.base import BaseHTTPMiddleware

from marketpulse import DISCLAIMER, __version__
from marketpulse.api.auth import APIKeyMiddleware
from marketpulse.api.rate_limit import MAX_QUERY_LIMIT, api_rate_limit, limiter
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


def _paginate(items: list, offset: int, limit: int) -> tuple[dict, list]:
    has_next = len(items) > limit
    metadata = {
        "offset": offset,
        "limit": limit,
        "next_offset": offset + limit if has_next else None,
    }
    return metadata, items[:limit]


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

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(LatencyMiddleware)
    app.add_middleware(APIKeyMiddleware)

    if settings.metrics_enabled:
        app.mount("/metrics", make_asgi_app())

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "service": "marketpulse-api", "version": __version__}

    @app.get("/pipeline/health")
    @limiter.limit(api_rate_limit)
    def pipeline_health(request: Request, response: Response) -> dict:
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
    @limiter.limit(api_rate_limit)
    def list_symbols(request: Request, response: Response) -> dict:
        with get_db() as session:
            symbols = Repository(session).get_symbols()
        if not symbols:
            symbols = settings.symbol_list
        return {"symbols": symbols}

    @app.get("/quotes/{symbol}/latest")
    @limiter.limit(api_rate_limit)
    def latest_quote(symbol: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            quote = Repository(session).get_latest_quote(symbol)
            if not quote:
                raise HTTPException(404, f"No quote found for {symbol}")
            return _serialize(quote)

    @app.get("/quotes/{symbol}/recent")
    @limiter.limit(api_rate_limit)
    def recent_quotes(
        symbol: str,
        request: Request,
        response: Response,
        offset: int = Query(0, ge=0),
        limit: int = Query(20, ge=1, le=MAX_QUERY_LIMIT),
    ) -> dict:
        with get_db() as session:
            ticks = Repository(session).get_recent_ticks(symbol, limit + 1, offset)
            metadata, ticks = _paginate(ticks, offset, limit)
            return {
                "symbol": symbol.upper(),
                "ticks": [_serialize(t) for t in ticks],
                "metadata": metadata,
            }

    @app.get("/features/{symbol}")
    @limiter.limit(api_rate_limit)
    def get_features(symbol: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            features = Repository(session).get_latest_features(symbol)
        if not features:
            raise HTTPException(404, f"No features for {symbol}")
        return _serialize(features)

    @app.get("/news/latest")
    @limiter.limit(api_rate_limit)
    def news_latest(
        request: Request,
        response: Response,
        offset: int = Query(0, ge=0),
        limit: int = Query(20, ge=1, le=MAX_QUERY_LIMIT),
    ) -> dict:
        with get_db() as session:
            articles = Repository(session).get_latest_news(limit + 1, offset)
            metadata, articles = _paginate(articles, offset, limit)
            return {
                "articles": [_serialize(a) for a in articles],
                "metadata": metadata,
            }

    @app.get("/news/market")
    @limiter.limit(api_rate_limit)
    def news_market(
        request: Request,
        response: Response,
        limit: int = Query(20, ge=1, le=MAX_QUERY_LIMIT),
    ) -> dict:
        with get_db() as session:
            articles = Repository(session).get_market_news(limit)
        return {"articles": [_serialize(a) for a in articles]}

    @app.get("/news/company/{symbol}")
    @limiter.limit(api_rate_limit)
    def news_company(
        symbol: str,
        request: Request,
        response: Response,
        limit: int = Query(20, ge=1, le=MAX_QUERY_LIMIT),
    ) -> dict:
        with get_db() as session:
            articles = Repository(session).get_company_news(symbol, limit)
        return {"symbol": symbol.upper(), "articles": [_serialize(a) for a in articles]}

    @app.get("/news/sentiment/{symbol}")
    @limiter.limit(api_rate_limit)
    def news_sentiment(symbol: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            return Repository(session).get_news_sentiment(symbol)

    @app.get("/anomalies")
    @limiter.limit(api_rate_limit)
    def list_anomalies(
        request: Request,
        response: Response,
        offset: int = Query(0, ge=0),
        limit: int = Query(20, ge=1, le=MAX_QUERY_LIMIT),
    ) -> dict:
        with get_db() as session:
            rows = Repository(session).get_anomalies(limit + 1, offset)
            metadata, rows = _paginate(rows, offset, limit)
            return {
                "anomalies": [_serialize(a) for a in rows],
                "metadata": metadata,
            }

    @app.get("/anomalies/{anomaly_id}")
    @limiter.limit(api_rate_limit)
    def get_anomaly(anomaly_id: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            row = Repository(session).get_anomaly(anomaly_id)
        if not row:
            raise HTTPException(404, "Anomaly not found")
        return _serialize(row)

    @app.get("/anomalies/symbol/{symbol}")
    @limiter.limit(api_rate_limit)
    def anomalies_by_symbol(
        symbol: str,
        request: Request,
        response: Response,
        limit: int = Query(20, ge=1, le=MAX_QUERY_LIMIT),
    ) -> dict:
        with get_db() as session:
            rows = Repository(session).get_anomalies_for_symbol(symbol, limit)
        return {"symbol": symbol.upper(), "anomalies": [_serialize(a) for a in rows]}

    @app.get("/context/{symbol}")
    @limiter.limit(api_rate_limit)
    def get_context(symbol: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            ctx = Repository(session).get_latest_context(symbol)
        if not ctx:
            raise HTTPException(404, f"No context for {symbol}")
        return _serialize(ctx)

    @app.get("/context/{symbol}/anomalies")
    @limiter.limit(api_rate_limit)
    def context_anomalies(symbol: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            rows = Repository(session).get_context_anomalies(symbol)
        return {"symbol": symbol.upper(), "anomalies": [_serialize(a) for a in rows]}

    @app.get("/explain/{symbol}")
    @limiter.limit(api_rate_limit)
    def explain(symbol: str, request: Request, response: Response) -> dict:
        with get_db() as session:
            return explain_stock_move(Repository(session), symbol)

    class BriefRequest(BaseModel):
        symbols: list[str] | None = None

    @app.post("/brief/market")
    @limiter.limit(api_rate_limit)
    def brief_market(request: Request, response: Response) -> dict:
        with get_db() as session:
            brief = BriefGenerator(Repository(session)).generate_market_brief()
        return _serialize(brief)

    @app.post("/brief/symbol")
    @limiter.limit(api_rate_limit)
    def brief_symbol(body: BriefRequest, request: Request, response: Response) -> dict:
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

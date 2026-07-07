"""Stock move explanation builder."""

from __future__ import annotations

from marketpulse import DISCLAIMER
from marketpulse.context.market_context_engine import MarketContextEngine
from marketpulse.db.repository import Repository


def explain_stock_move(repo: Repository, symbol: str) -> dict:
    engine = MarketContextEngine(repo)
    context = engine.build_context(symbol)
    return {
        "symbol": symbol.upper(),
        "price_change_pct": context.price_change_pct,
        "explanation": context.explanation,
        "sentiment_summary": context.sentiment_summary,
        "anomaly_ids": context.anomaly_ids,
        "news_ids": context.news_ids,
        "confidence": context.confidence,
        "disclaimer": DISCLAIMER,
    }

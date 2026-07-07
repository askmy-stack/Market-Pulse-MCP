"""Market and symbol brief generators."""

from __future__ import annotations

from datetime import datetime

from marketpulse import DISCLAIMER
from marketpulse.db.repository import Repository
from marketpulse.schemas.events import MarketBriefEvent


class BriefGenerator:
    def __init__(self, repo: Repository):
        self.repo = repo

    def generate_market_brief(self) -> MarketBriefEvent:
        news = self.repo.get_market_news(limit=5)
        anomalies = self.repo.get_anomalies(limit=5)
        symbols = self.repo.get_symbols()

        lines = [
            f"MarketPulse Brief — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            f"Tracking {len(symbols)} symbols: {', '.join(symbols[:8])}.",
        ]
        if news:
            lines.append("Top market headlines:")
            lines.extend(f"  • {n.headline}" for n in news[:3])
        if anomalies:
            lines.append(f"Recent anomalies detected: {len(anomalies)}")
            lines.extend(f"  • {a.symbol}: {a.description}" for a in anomalies[:3])
        lines.append(DISCLAIMER)

        event = MarketBriefEvent(
            brief_type="market",
            symbols=symbols,
            content="\n".join(lines),
            disclaimer=DISCLAIMER,
        )
        self.repo.save_brief(event)
        return event

    def generate_symbol_brief(self, symbol: str) -> MarketBriefEvent:
        symbol = symbol.upper()
        quote = self.repo.get_latest_quote(symbol)
        features = self.repo.get_latest_features(symbol)
        news = self.repo.get_company_news(symbol, limit=3)
        anomalies = self.repo.get_anomalies_for_symbol(symbol, limit=3)

        price = quote.price if quote else 0.0
        lines = [
            f"{symbol} Brief — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            f"Latest price: ${price:.2f}",
        ]
        if features:
            lines.append(
                f"Features: 1m return={features.return_1m:.4f}, vol={features.volatility:.4f}, z={features.z_score:.2f}"
            )
        if news:
            lines.append("Recent news:")
            lines.extend(f"  • {n.headline}" for n in news)
        if anomalies:
            lines.append("Recent anomalies:")
            lines.extend(f"  • {a.description}" for a in anomalies)
        lines.append(DISCLAIMER)

        event = MarketBriefEvent(
            brief_type="symbol",
            symbols=[symbol],
            content="\n".join(lines),
            disclaimer=DISCLAIMER,
        )
        self.repo.save_brief(event)
        return event

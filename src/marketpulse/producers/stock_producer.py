"""Stock tick producers — yfinance real data with mock fallback."""

from __future__ import annotations

import time
from datetime import datetime

from marketpulse.config import get_settings
from marketpulse.kafka.client import create_producer, publish_event
from marketpulse.kafka.topics import STOCK_TICKS
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.observability.metrics import LATEST_PRICE, STOCK_TICKS_INGESTED
from marketpulse.producers.mock_stock_producer import MockStockProducer
from marketpulse.schemas.events import StockTickEvent

logger = get_logger(__name__)


class YFinanceStockProducer:
    """Fetch live quotes via yfinance; falls back to mock on import or runtime errors."""

    def __init__(self):
        self.settings = get_settings()
        self.producer = create_producer()
        self._yf = None
        self._available = self._try_import()

    def _try_import(self) -> bool:
        try:
            import yfinance as yf  # noqa: F401

            self._yf = __import__("yfinance")
            return True
        except ImportError:
            logger.warning("yfinance_not_installed", fallback="mock")
            return False

    def _fetch_ticks(self) -> list[StockTickEvent]:
        if not self._available or self._yf is None:
            return []

        ticks: list[StockTickEvent] = []
        for symbol in self.settings.symbol_list:
            try:
                ticker = self._yf.Ticker(symbol)
                info = ticker.fast_info
                price = float(
                    getattr(info, "last_price", None) or getattr(info, "lastPrice", 0) or 0
                )
                if price <= 0:
                    hist = ticker.history(period="1d", interval="1m")
                    if hist.empty:
                        continue
                    price = float(hist["Close"].iloc[-1])
                    volume = int(hist["Volume"].iloc[-1])
                else:
                    volume = int(getattr(info, "last_volume", 10000) or 10000)

                spread = price * 0.0005
                tick = StockTickEvent(
                    symbol=symbol,
                    price=round(price, 2),
                    volume=volume,
                    bid=round(price - spread, 2),
                    ask=round(price + spread, 2),
                    timestamp=datetime.utcnow(),
                    source="yfinance",
                )
                ticks.append(tick)
            except Exception as exc:
                logger.warning("yfinance_fetch_failed", symbol=symbol, error=str(exc))
        return ticks

    def run(self) -> None:
        if not self._available:
            logger.info("yfinance_unavailable_using_mock")
            MockStockProducer().run()
            return

        logger.info("starting_yfinance_stock_producer", symbols=self.settings.symbol_list)
        while True:
            ticks = self._fetch_ticks()
            if not ticks:
                logger.warning("yfinance_no_data_fallback_tick")
                mock = MockStockProducer()
                for symbol in self.settings.symbol_list[:1]:
                    tick = mock.generate_tick(symbol)
                    ticks = [tick]
                    break

            for tick in ticks:
                publish_event(self.producer, STOCK_TICKS, tick.symbol, tick)
                self.producer.flush(1)
                STOCK_TICKS_INGESTED.labels(symbol=tick.symbol).inc()
                LATEST_PRICE.labels(symbol=tick.symbol).set(tick.price)
                logger.info("published_yfinance_tick", symbol=tick.symbol, price=tick.price)

            time.sleep(self.settings.yfinance_poll_interval_seconds)


def create_stock_producer():
    settings = get_settings()
    if settings.real_stock_enabled:
        return YFinanceStockProducer()
    return MockStockProducer()


def main() -> None:
    setup_logging()
    create_stock_producer().run()


if __name__ == "__main__":
    main()

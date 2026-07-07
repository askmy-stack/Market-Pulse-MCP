"""Mock stock tick producer."""

from __future__ import annotations

import random
import time

from marketpulse.config import get_settings
from marketpulse.kafka.client import create_producer, publish_event
from marketpulse.kafka.topics import STOCK_TICKS
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.schemas.events import StockTickEvent

logger = get_logger(__name__)

BASE_PRICES = {
    "AAPL": 190.0,
    "MSFT": 420.0,
    "GOOGL": 175.0,
    "AMZN": 185.0,
    "TSLA": 250.0,
    "NVDA": 900.0,
    "META": 500.0,
    "JPM": 195.0,
}


class MockStockProducer:
    def __init__(self):
        self.settings = get_settings()
        self.producer = create_producer()
        self.prices = {s: BASE_PRICES.get(s, 100.0) for s in self.settings.symbol_list}

    def generate_tick(self, symbol: str) -> StockTickEvent:
        symbol = symbol.upper()
        base = self.prices[symbol]
        shock = random.gauss(0, 0.003)
        if random.random() < 0.02:
            shock += random.choice([-1, 1]) * random.uniform(0.01, 0.03)
        price = max(1.0, base * (1 + shock))
        self.prices[symbol] = price
        volume = int(random.uniform(1000, 50000))
        spread = price * 0.0005
        return StockTickEvent(
            symbol=symbol,
            price=round(price, 2),
            volume=volume,
            bid=round(price - spread, 2),
            ask=round(price + spread, 2),
            source="mock",
        )

    def run(self) -> None:
        logger.info("starting_mock_stock_producer", symbols=self.settings.symbol_list)
        while True:
            for symbol in self.settings.symbol_list:
                tick = self.generate_tick(symbol)
                publish_event(self.producer, STOCK_TICKS, tick.symbol, tick)
                self.producer.flush(1)
                logger.info("published_tick", symbol=tick.symbol, price=tick.price)
            time.sleep(self.settings.mock_tick_interval_seconds)


def main() -> None:
    setup_logging()
    MockStockProducer().run()


if __name__ == "__main__":
    main()

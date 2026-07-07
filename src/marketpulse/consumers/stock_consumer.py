"""Stock tick consumer — validates and persists ticks."""

from __future__ import annotations

import signal

from marketpulse.db.repository import Repository
from marketpulse.db.session import get_db, init_db
from marketpulse.kafka.client import create_consumer, parse_message
from marketpulse.kafka.topics import STOCK_TICKS
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.observability.metrics import TICKS_PROCESSED
from marketpulse.schemas.events import PipelineHealthEvent, StockTickEvent

logger = get_logger(__name__)
_running = True


def _shutdown(*_args) -> None:
    global _running
    _running = False


def run() -> None:
    setup_logging()
    init_db()
    consumer = create_consumer(group_id="stock-consumer", topics=[STOCK_TICKS])
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    logger.info("stock_consumer_started")

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("consumer_error", error=str(msg.error()))
            continue
        try:
            data = parse_message(msg.value())
            event = StockTickEvent.model_validate(data)
            with get_db() as session:
                repo = Repository(session)
                repo.save_tick(event)
                repo.save_health(
                    PipelineHealthEvent(component="stock-consumer", status="healthy", message="tick processed")
                )
            TICKS_PROCESSED.labels(symbol=event.symbol).inc()
            logger.info("tick_saved", symbol=event.symbol, price=event.price)
        except Exception as exc:
            logger.error("tick_processing_failed", error=str(exc))

    consumer.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()

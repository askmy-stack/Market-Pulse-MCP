"""Stream processor — features, anomalies, context correlation."""

from __future__ import annotations

import signal
import time

from marketpulse.anomaly.detector import AnomalyDetector
from marketpulse.context.market_context_engine import MarketContextEngine
from marketpulse.db.repository import Repository
from marketpulse.db.session import get_db, init_db
from marketpulse.features.feature_store import FeatureStore
from marketpulse.kafka.client import create_consumer, create_producer, parse_message, publish_event
from marketpulse.kafka.dlq import route_poison_message
from marketpulse.kafka.topics import (
    MARKET_CONTEXT,
    PIPELINE_HEALTH,
    STOCK_ANOMALIES,
    STOCK_FEATURES,
    STOCK_TICKS,
)
from marketpulse.observability.logging import get_logger, setup_logging
from marketpulse.observability.metrics import (
    FEATURES_COMPUTED,
    PIPELINE_LAG,
    PROCESSING_LATENCY,
    SYMBOL_VOLATILITY,
    record_anomaly,
)
from marketpulse.schemas.events import PipelineHealthEvent, StockTickEvent

logger = get_logger(__name__)
_running = True


def _shutdown(*_args) -> None:
    global _running
    _running = False


def run() -> None:
    setup_logging()
    init_db()
    consumer = create_consumer(group_id="stream-processor", topics=[STOCK_TICKS])
    producer = create_producer()
    detector = AnomalyDetector()
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    logger.info("stream_processor_started")

    while _running:
        start = time.time()
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("processor_error", error=str(msg.error()))
            continue
        try:
            data = parse_message(msg.value())
            tick = StockTickEvent.model_validate(data)
            with get_db() as session:
                repo = Repository(session)
                store = FeatureStore(repo)
                features = store.process_tick(tick)
                publish_event(producer, STOCK_FEATURES, features.symbol, features)
                FEATURES_COMPUTED.labels(symbol=features.symbol).inc()
                SYMBOL_VOLATILITY.labels(symbol=features.symbol).set(features.volatility)

                anomaly = detector.detect(features)
                if anomaly:
                    publish_event(producer, STOCK_ANOMALIES, anomaly.symbol, anomaly)
                    repo.save_anomaly(anomaly)
                    record_anomaly(
                        anomaly.symbol,
                        anomaly.severity.value,
                        len(anomaly.related_news_ids),
                        anomaly.event_id,
                    )

                    engine = MarketContextEngine(repo)
                    context = engine.build_context(anomaly.symbol, anomaly)
                    publish_event(producer, MARKET_CONTEXT, context.symbol, context)
                    logger.info(
                        "anomaly_detected", symbol=anomaly.symbol, severity=anomaly.severity.value
                    )

                repo.save_health(
                    PipelineHealthEvent(
                        component="stream-processor",
                        status="healthy",
                        message="tick processed",
                        metrics={"symbol": tick.symbol},
                    )
                )
            producer.flush(1)
            lag = time.time() - start
            PIPELINE_LAG.labels(component="stream-processor", topic=STOCK_TICKS).set(lag)
            PROCESSING_LATENCY.labels(component="stream-processor").observe(lag)
        except Exception as exc:
            logger.error("processor_failed", error=str(exc))
            route_poison_message(producer, msg, component="stream-processor", error=exc)
            with get_db() as session:
                Repository(session).save_health(
                    PipelineHealthEvent(
                        component="stream-processor", status="degraded", message=str(exc)
                    )
                )
            publish_event(
                producer,
                PIPELINE_HEALTH,
                "stream-processor",
                PipelineHealthEvent(
                    component="stream-processor", status="degraded", message=str(exc)
                ),
            )
            producer.flush(1)

    consumer.close()


def main() -> None:
    run()


if __name__ == "__main__":
    main()

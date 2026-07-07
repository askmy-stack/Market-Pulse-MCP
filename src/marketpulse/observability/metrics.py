"""Prometheus metrics."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from marketpulse.config import get_settings

TICKS_PROCESSED = Counter("marketpulse_ticks_processed_total", "Stock ticks processed", ["symbol"])
NEWS_PROCESSED = Counter("marketpulse_news_processed_total", "News articles processed", ["category"])
ANOMALIES_DETECTED = Counter("marketpulse_anomalies_detected_total", "Anomalies detected", ["symbol", "severity"])
FEATURES_COMPUTED = Counter("marketpulse_features_computed_total", "Features computed", ["symbol"])
PROCESSING_LATENCY = Histogram("marketpulse_processing_seconds", "Processing latency", ["component"])
PIPELINE_HEALTH_GAUGE = Gauge("marketpulse_pipeline_health", "Pipeline component health", ["component"])

_metrics_started = False


def start_metrics_server() -> None:
    global _metrics_started
    settings = get_settings()
    if settings.metrics_enabled and not _metrics_started:
        try:
            start_http_server(settings.prometheus_port)
            _metrics_started = True
        except OSError:
            pass

"""Prometheus metrics."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from marketpulse.config import get_settings

# Legacy marketpulse_* metrics (kept for backward compatibility)
TICKS_PROCESSED = Counter("marketpulse_ticks_processed_total", "Stock ticks processed", ["symbol"])
NEWS_PROCESSED = Counter(
    "marketpulse_news_processed_total", "News articles processed", ["category"]
)
ANOMALIES_DETECTED = Counter(
    "marketpulse_anomalies_detected_total", "Anomalies detected", ["symbol", "severity"]
)
FEATURES_COMPUTED = Counter("marketpulse_features_computed_total", "Features computed", ["symbol"])
PROCESSING_LATENCY = Histogram(
    "marketpulse_processing_seconds", "Processing latency", ["component"]
)
PIPELINE_HEALTH_GAUGE = Gauge(
    "marketpulse_pipeline_health", "Pipeline component health", ["component"]
)

# Dashboard-aligned metrics (v0.4)
STOCK_TICKS_INGESTED = Counter(
    "market_stock_ticks_ingested_total", "Stock ticks ingested into pipeline", ["symbol"]
)
NEWS_ARTICLES_INGESTED = Counter(
    "market_news_articles_ingested_total", "News articles ingested", ["category"]
)
ANOMALIES_BY_SYMBOL = Counter(
    "market_anomalies_by_symbol_total", "Anomalies detected by symbol", ["symbol"]
)
ANOMALIES_BY_SEVERITY = Counter(
    "market_anomalies_by_severity_total", "Anomalies detected by severity", ["severity"]
)
PIPELINE_LAG = Gauge(
    "market_pipeline_lag_seconds", "Kafka consumer lag in seconds", ["component", "topic"]
)
API_REQUEST_LATENCY = Histogram(
    "market_api_request_latency_seconds",
    "API request latency",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
MCP_TOOL_CALLS = Counter("market_mcp_tool_calls_total", "MCP tool invocations", ["tool"])
SYMBOL_VOLATILITY = Gauge("market_symbol_volatility", "Latest volatility per symbol", ["symbol"])
LATEST_PRICE = Gauge("market_latest_price", "Latest price per symbol", ["symbol"])
ANOMALY_RELATED_NEWS = Gauge(
    "market_anomaly_related_news_count", "Related news count per anomaly", ["symbol", "anomaly_id"]
)
EMBEDDINGS_PUBLISHED = Counter(
    "market_news_embeddings_published_total", "News embeddings published", ["source"]
)

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


def record_anomaly(
    symbol: str, severity: str, related_news_count: int = 0, anomaly_id: str = ""
) -> None:
    """Record anomaly across dashboard metrics."""
    ANOMALIES_DETECTED.labels(symbol=symbol, severity=severity).inc()
    ANOMALIES_BY_SYMBOL.labels(symbol=symbol).inc()
    ANOMALIES_BY_SEVERITY.labels(severity=severity).inc()
    if anomaly_id:
        ANOMALY_RELATED_NEWS.labels(symbol=symbol, anomaly_id=anomaly_id).set(related_news_count)

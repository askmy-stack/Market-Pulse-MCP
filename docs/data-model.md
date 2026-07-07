# Data Model

## Event Schemas (Pydantic)

### StockTickEvent
- `symbol`, `price`, `volume`, `bid`, `ask`, `timestamp`, `source`

### NewsEvent
- `headline`, `summary`, `category`, `symbols`, `sentiment_score`, `published_at`

### StockFeatureEvent
- `return_1m`, `return_5m`, `volatility`, `z_score`, `volume_ratio`, `price`

### StockAnomalyEvent
- `anomaly_type`, `severity`, `z_score`, `volume_ratio`, `price_change_pct`, `description`

### CorrelatedMarketContextEvent
- `price_change_pct`, `anomaly_ids`, `news_ids`, `sentiment_summary`, `explanation`, `confidence`

## PostgreSQL Tables

| Table | Purpose |
|-------|---------|
| `stock_ticks` | Raw tick storage |
| `stock_features` | Computed rolling features |
| `stock_anomalies` | Detected anomalies |
| `news_articles` | News storage |
| `market_context` | Correlated context |
| `market_briefs` | Generated briefs |
| `pipeline_health` | Health telemetry |

## Feature Calculations

- **return_1m / return_5m** — Price returns over 1 and 5 tick windows
- **volatility** — Standard deviation of returns
- **z_score** — Price deviation from rolling mean
- **volume_ratio** — Current volume vs rolling average

## Anomaly Detection

Triggers when `|z_score| >= threshold` OR `volume_ratio >= spike_ratio`.

Severity scales with combined z-score and volume deviation.

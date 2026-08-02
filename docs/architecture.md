# Architecture

MarketPulse MCP is a real-time market intelligence pipeline designed for AI agents.

## Data Flow

```
Mock Producers → Redpanda Topics → Consumers/Processor → PostgreSQL → FastAPI / MCP
```

## Components

| Component | Role |
|-----------|------|
| `mock_stock_producer` | Generates realistic stock ticks |
| `mock_news_producer` | Generates market and company news |
| `stock_consumer` | Validates and persists ticks |
| `news_consumer` | Validates and persists news |
| `stream_processor` | Computes features, detects anomalies, correlates context |
| `api` | REST API for quotes, news, anomalies, context |
| `mcp-server` | MCP tools for AI agent integration |

## Kafka Topics

- `stock_ticks` — raw price/volume events
- `market_news` / `company_news` — news events
- `stock_features` — rolling computed features
- `stock_anomalies` — detected anomalies
- `market_context` — correlated context events
- `market_briefs` — generated briefs
- `pipeline_health` — health telemetry
- `pipeline_dlq` — poison / failed consumer messages (dead-letter queue)

## Storage

PostgreSQL stores ticks, features, anomalies, news, context, and briefs for API and MCP queries.

## Observability

- Structured logging via `structlog`
- Prometheus metrics at `/metrics`
- Optional Grafana/Prometheus via Docker Compose

![Architecture](../assets/Architecture.jpg)

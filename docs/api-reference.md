# API Reference

Base URL: `http://localhost:8000`

## Authentication

When `API_KEY` is set in the environment, all routes except `/health`, `/metrics`, and `/docs` require the `X-API-Key` header:

```bash
curl -H "X-API-Key: your-secret-key" http://localhost:8000/symbols
```

If `API_KEY` is unset (default), authentication is disabled.

API routes (except `/health`) are rate limited per IP via slowapi
(`MARKETPULSE_RATE_LIMIT`, default `60/minute`). List endpoints accept
`limit` up to **500** (`Query(..., le=500)`); larger values return 422.

## Health

- `GET /health` — API health check (no auth required)
- `GET /pipeline/health` — Pipeline component status

## Quotes & Features

- `GET /symbols` — List tracked symbols
- `GET /quotes/{symbol}/latest` — Latest quote
- `GET /quotes/{symbol}/recent?limit=50` — Recent ticks
- `GET /features/{symbol}` — Latest computed features

## News

- `GET /news/latest` — All recent news
- `GET /news/market` — Market headlines
- `GET /news/company/{symbol}` — Company news
- `GET /news/sentiment/{symbol}` — Sentiment analysis

## Anomalies

- `GET /anomalies` — Recent anomalies
- `GET /anomalies/{anomaly_id}` — Single anomaly
- `GET /anomalies/symbol/{symbol}` — Symbol anomalies

## Context & Explanation

- `GET /context/{symbol}` — Latest correlated context
- `GET /context/{symbol}/anomalies` — Context anomalies
- `GET /explain/{symbol}` — Full stock move explanation

## Briefs

- `POST /brief/market` — Generate market brief
- `POST /brief/symbol` — Generate symbol brief (`{"symbols": ["AAPL"]}`)

## Metrics

- `GET /metrics` — Prometheus metrics (no auth required)

## Observability

- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9091

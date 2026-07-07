# API Reference

Base URL: `http://localhost:8000`

## Health

- `GET /health` — API health check
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

- `GET /metrics` — Prometheus metrics

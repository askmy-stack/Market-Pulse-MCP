# Open Source Roadmap

## v0.1 — MVP ✅
- [x] Mock stock and news producers
- [x] Redpanda streaming pipeline
- [x] Feature computation and anomaly detection
- [x] News correlation engine
- [x] FastAPI REST API
- [x] MCP server with 13 tools
- [x] Docker Compose local stack
- [x] Prometheus metrics

## v0.2 — Real Data ✅
- [x] yfinance stock producer (`ENABLE_REAL_STOCK_DATA` / `YFINANCE_ENABLED`)
- [x] NewsAPI provider (`NEWS_API_KEY` + `ENABLE_REAL_NEWS_DATA`)
- [x] Finnhub provider (`FINNHUB_API_KEY`)
- [x] Embedding-based news similarity (`news_embeddings` topic + DB storage)
- [x] Hash / sentence-transformers embedding fallback

## v0.3 — Agent Experience (partial)
- [x] MCP tool examples for Cursor (`examples/mcp/cursor-mcp.json`)
- [ ] Multi-tenant symbol watchlists
- [ ] Historical replay mode from CSV/archive
- [ ] WebSocket live quote streaming

## v0.4 — Observability ✅
- [x] Grafana dashboards (auto-provisioned at http://localhost:3000)
- [x] Dashboard JSON export (`deploy/grafana/dashboards/marketpulse.json`)
- [x] Pipeline lag, API latency, MCP tool call metrics
- [x] Prometheus datasource auto-provisioned

## v0.5 — Open Source Readiness ✅
- [x] Good First Issues on GitHub
- [x] `docs/screenshots/` with representative images
- [x] Pre-commit hooks (ruff check + format)
- [x] CONTRIBUTING.md updated

## Production Hardening ✅
- [x] Optional API key auth (`API_KEY` env var)
- [x] TimescaleDB hypertable migration (`deploy/timescaledb/init.sql`)
- [x] Terraform AWS skeleton (`deploy/terraform/`)
- [x] Kubernetes Helm chart (`deploy/helm/marketpulse/`)
- [x] Docker health checks on all services

## v1.0 — Planned
- [ ] Rate limiting and multi-tenant auth
- [ ] Full production Terraform (MSK, autoscaling)
- [ ] Cloud deployment guides (Confluent Cloud, Redpanda Cloud)
- [ ] API pagination
- [ ] Additional anomaly detectors (LOF, Isolation Forest)
- [ ] Alert integrations (Slack, Discord)

## Non-Goals

- Stock price prediction
- Trading signals or financial advice
- Automated order execution

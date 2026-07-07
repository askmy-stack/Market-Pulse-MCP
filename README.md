# MarketPulse MCP

> Real-time market intelligence pipeline and MCP skill server for AI agents.

**Repository:** `kafka-stock-pipeline` · **Project:** MarketPulse MCP

MarketPulse MCP transforms streaming market data into agent-ready context. It ingests stock ticks and news through a Kafka-compatible pipeline (Redpanda), computes rolling features, detects anomalies, correlates news within time windows, and exposes everything via a FastAPI REST API and MCP tools — including the hero tool `explain_stock_move`.

**This is correlation-based market context, not financial advice. MarketPulse does not predict stock prices.**

![Architecture](assets/Architecture.jpg)

## Why it exists

AI agents need structured, real-time market context — not raw tick feeds. MarketPulse MCP bridges streaming data engineering and agent tooling:

- **Streaming pipeline** — Redpanda topics, validated Pydantic schemas, consumer groups
- **Intelligence layer** — rolling features, z-score anomalies, news correlation
- **Agent interface** — 13 MCP tools connected to live pipeline data
- **Portfolio-ready** — Docker Compose, tests, CI, observability

## Architecture

```
Producers → Redpanda → Consumers/Processor → PostgreSQL → FastAPI + MCP Server
```

| Service | Purpose |
|---------|---------|
| `stock-producer` | Mock stock ticks |
| `news-producer` | Mock market/company news |
| `stock-consumer` | Persist validated ticks |
| `news-consumer` | Persist validated news |
| `stream-processor` | Features, anomalies, context |
| `api` | REST API (port 8000) |
| `mcp-server` | MCP tools for agents |
| `prometheus` | Metrics scraping |

See [docs/architecture.md](docs/architecture.md) for details.

## Features

- **9 Kafka topics** — `stock_ticks`, `market_news`, `company_news`, `stock_features`, `stock_anomalies`, `news_embeddings`, `market_context`, `market_briefs`, `pipeline_health`
- **Pydantic event schemas** — validated at every stage
- **Rolling features** — returns, volatility, z-score, volume ratio
- **Anomaly detection** — price z-score + volume spike with severity
- **News correlation** — time-window matching with sentiment
- **Context engine** — correlated explanations with disclaimers
- **Mock data by default** — no API keys required

## Quickstart

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local dev)

### Run the full stack

```bash
git clone https://github.com/askmy-stack/kafka-stock-pipeline.git
cd kafka-stock-pipeline
cp .env.example .env
make up
```

Wait ~30 seconds for services to initialize, then:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/symbols
curl http://localhost:8000/explain/AAPL
```

### Local development (without Docker)

```bash
make install
cp .env.example .env
# Start Redpanda + Postgres separately, then:
make init-db
make api          # Terminal 1
make produce-stock # Terminal 2
make produce-news  # Terminal 3
```

## Agent Workflow

1. Start the pipeline (`make up`)
2. Configure MCP in Cursor — see [examples/mcp/cursor-mcp.json](examples/mcp/cursor-mcp.json)
3. Ask your agent: *"Explain why AAPL moved recently"*
4. Agent calls `explain_stock_move` → gets correlated anomalies + news + disclaimer

```bash
make mcp  # Run MCP server standalone
```

## API Examples

```bash
# Latest quote
curl http://localhost:8000/quotes/AAPL/latest

# Recent anomalies
curl http://localhost:8000/anomalies

# News sentiment
curl http://localhost:8000/news/sentiment/TSLA

# Generate market brief
curl -X POST http://localhost:8000/brief/market

# Pipeline health
curl http://localhost:8000/pipeline/health
```

Full reference: [docs/api-reference.md](docs/api-reference.md)

## MCP Tools

| Tool | Description |
|------|-------------|
| `explain_stock_move` | **Hero** — full move explanation |
| `get_latest_quote` | Latest price for a symbol |
| `get_stock_features` | Rolling computed features |
| `detect_market_anomalies` | Recent anomalies |
| `summarize_stock_context` | Correlated context |
| `generate_market_brief` | Market-wide brief |
| ... | [Full list](docs/mcp-tools.md) |

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make up` | Start full Docker stack |
| `make down` | Stop and remove volumes |
| `make produce-stock` | Run mock stock producer |
| `make produce-news` | Run mock news producer |
| `make api` | Start FastAPI dev server |
| `make mcp` | Start MCP server |
| `make test` | Run pytest |
| `make lint` | Run ruff linter |

## Testing

```bash
make install
make test
make lint
```

## Roadmap

See [docs/open-source-roadmap.md](docs/open-source-roadmap.md).

## Original Notebooks

The original Jupyter notebooks are preserved for reference:

- `KafkaProducer.ipynb` — early producer prototype
- `KafkaConsumer.ipynb` — early consumer prototype
- `indexProcessed.csv` — sample processed data

The `src/marketpulse/` package is the production implementation.

## Disclaimer

**MarketPulse MCP provides correlation-based market context for informational and educational purposes only. It does not constitute financial advice, investment recommendations, or price predictions. Past patterns do not guarantee future results. Always do your own research and consult a qualified financial advisor.**

## License

MIT — see [LICENSE](LICENSE)


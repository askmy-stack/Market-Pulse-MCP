# MarketPulse Helm Chart

Deploy MarketPulse MCP to Kubernetes.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- Container image built and pushed to your registry
- Kafka/Redpanda and PostgreSQL reachable from the cluster

## Install

```bash
# Build and push image
docker build -t your-registry/marketpulse:0.5.0 .
docker push your-registry/marketpulse:0.5.0

# Install chart
helm install marketpulse ./deploy/helm/marketpulse \
  --set image.repository=your-registry/marketpulse \
  --set image.tag=0.5.0 \
  --set kafka.bootstrapServers=redpanda:9092 \
  --set postgres.host=postgres.default.svc.cluster.local
```

## Upgrade

```bash
helm upgrade marketpulse ./deploy/helm/marketpulse -f my-values.yaml
```

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `marketpulse` | Container image |
| `kafka.bootstrapServers` | `redpanda:9092` | Kafka brokers |
| `postgres.host` | `postgres` | PostgreSQL host |
| `api.apiKey` | `""` | Optional API key auth |
| `producers.stock.enableRealStockData` | `false` | Enable yfinance |
| `producers.news.enableRealNewsData` | `false` | Enable NewsAPI |

## Components

- `api` — FastAPI REST service (ClusterIP on port 8000)
- `mcp` — MCP server for AI agents
- `stock-producer` / `news-producer` — data ingestion
- `stock-consumer` / `news-consumer` / `processor` — pipeline processing

## Uninstall

```bash
helm uninstall marketpulse
```

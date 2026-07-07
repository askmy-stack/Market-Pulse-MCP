# Terraform AWS Deployment (Skeleton)

Functional skeleton for deploying MarketPulse MCP on AWS. This is **not** a production-ready configuration — customize for your environment.

## Architecture

```
VPC
├── Public subnets  → ALB → ECS Fargate (api, mcp-server, processors, producers)
├── Private subnets → RDS PostgreSQL (TimescaleDB-compatible)
└── Kafka           → Use Amazon MSK **or** external Redpanda Cloud (recommended for simplicity)
```

## Why MSK is optional

Managed Kafka (MSK) adds significant cost and operational overhead. For most contributors:

- **Local dev**: Docker Compose + Redpanda (included)
- **Cloud**: [Redpanda Cloud](https://redpanda.com/cloud) or Confluent Cloud — set `KAFKA_BOOTSTRAP_SERVERS` in ECS task env

## Prerequisites

- Terraform >= 1.5
- AWS CLI configured
- S3 bucket for remote state (optional)

## Quick start

```bash
cd deploy/terraform
terraform init
terraform plan -var-file=terraform.tfvars.example
# terraform apply  # after customizing variables
```

## Modules

| File | Purpose |
|------|---------|
| `main.tf` | Provider, VPC module, ECS cluster |
| `rds.tf` | RDS PostgreSQL instance |
| `ecs.tf` | Fargate services for api, processors |
| `variables.tf` | Input variables |
| `outputs.tf` | ALB DNS, RDS endpoint |

## Environment variables

Set these in ECS task definitions (see `ecs.tf`):

- `KAFKA_BOOTSTRAP_SERVERS` — MSK bootstrap or Redpanda Cloud brokers
- `POSTGRES_HOST` — RDS endpoint
- `API_KEY` — optional API authentication
- `ENABLE_REAL_STOCK_DATA` / `NEWS_API_KEY` — real data providers

## MSK (optional)

Uncomment the `msk.tf` block and set `enable_msk = true` in `terraform.tfvars`. Expect ~$300+/month for a minimal MSK cluster.

## Kubernetes alternative

See `deploy/helm/marketpulse/` for a Helm-based deployment.

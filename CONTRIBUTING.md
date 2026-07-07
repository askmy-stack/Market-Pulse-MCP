# Contributing to MarketPulse MCP

Thank you for your interest in contributing!

## Development setup

```bash
git clone https://github.com/askmy-stack/kafka-stock-pipeline.git
cd kafka-stock-pipeline
make install
cp .env.example .env
make up
```

## Running tests

```bash
make test
make lint
```

## Pull request guidelines

- Keep changes focused and well-tested
- Run `make lint` and `make test` before submitting
- Update documentation for user-facing changes
- Do not commit secrets or `.env` files

## Code style

- Python 3.11+
- Ruff for linting and formatting
- Type hints encouraged

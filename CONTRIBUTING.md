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

## Pre-commit hooks

Install and run pre-commit hooks for consistent formatting:

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

Hooks configured in `.pre-commit-config.yaml`:
- **ruff check** — linting with auto-fix
- **ruff format** — code formatting

## Running tests

```bash
make test
make lint
```

## Pull request guidelines

- Keep changes focused and well-tested
- Run `make lint` and `make test` before submitting
- Run `pre-commit run --all-files` before submitting
- Update documentation for user-facing changes
- Do not commit secrets or `.env` files

## Code style

- Python 3.11+
- Ruff for linting and formatting
- Type hints encouraged

## Good First Issues

Check [GitHub Issues](https://github.com/askmy-stack/kafka-stock-pipeline/issues?q=label%3A%22good+first+issue%22) for beginner-friendly tasks.

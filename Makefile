.PHONY: up down build logs produce-stock produce-news api test lint format mcp init-db

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build

logs:
	docker compose logs -f

produce-stock:
	python -m marketpulse.producers.mock_stock_producer

produce-news:
	python -m marketpulse.producers.mock_news_producer

api:
	uvicorn marketpulse.api.app:create_app --factory --host 0.0.0.0 --port 8000 --reload

test:
	pytest tests/ -v

lint:
	ruff check src tests

format:
	ruff check --fix src tests
	ruff format src tests

mcp:
	python -m marketpulse.mcp.server

init-db:
	python -c "from marketpulse.db.session import init_db; init_db()"

install:
	pip install -e ".[dev]"

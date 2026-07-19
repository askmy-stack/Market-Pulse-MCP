"""Test FastAPI endpoints."""

from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from marketpulse.api.app import create_app
from marketpulse.db import session as db_session
from marketpulse.db.models import Base, NewsArticle, StockAnomaly, StockTick


@pytest.fixture
def client(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine)

    import marketpulse.db.models as models

    monkeypatch.setattr(db_session, "get_session_factory", lambda: TestSession)
    monkeypatch.setattr(models, "get_engine", lambda: engine)
    monkeypatch.setattr(models, "get_session_factory", lambda: TestSession)
    models._engine = engine
    models._SessionLocal = TestSession

    with TestSession() as s:
        s.add(
            StockTick(
                event_id="tick-1",
                symbol="AAPL",
                price=190.0,
                volume=1000,
                timestamp=datetime.utcnow(),
                source="test",
            )
        )
        for i in range(5):
            s.add(
                NewsArticle(
                    event_id=f"news-{i}",
                    headline=f"Headline {i}",
                    summary=f"Summary {i}",
                    category="market",
                    symbols=["AAPL"],
                    sentiment_score=0.5,
                    published_at=datetime(2025, 1, 1, 0, i),
                    source="test",
                )
            )
        for i in range(5):
            s.add(
                StockAnomaly(
                    event_id=f"anomaly-{i}",
                    symbol="AAPL",
                    timestamp=datetime(2025, 1, 1, 0, i),
                    anomaly_type="volume_spike",
                    severity="high",
                    z_score=3.0,
                    volume_ratio=2.5,
                    price=190.0,
                    price_change_pct=5.0,
                    description=f"Anomaly {i}",
                )
            )
        s.commit()

    return TestClient(create_app())


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_symbols(client):
    resp = client.get("/symbols")
    assert resp.status_code == 200
    assert "AAPL" in resp.json()["symbols"]


def test_latest_quote(client):
    resp = client.get("/quotes/AAPL/latest")
    assert resp.status_code == 200
    assert resp.json()["symbol"] == "AAPL"


def test_explain_includes_disclaimer(client):
    resp = client.get("/explain/AAPL")
    assert resp.status_code == 200
    assert "disclaimer" in resp.json()


def test_pagination_news_latest_default(client):
    resp = client.get("/news/latest")
    assert resp.status_code == 200
    body = resp.json()
    assert "metadata" in body
    assert len(body["articles"]) <= 20
    assert body["metadata"]["offset"] == 0
    assert body["metadata"]["limit"] == 20


def test_pagination_news_latest_offset_limit(client):
    resp = client.get("/news/latest?offset=0&limit=2")
    body = resp.json()
    assert len(body["articles"]) == 2
    assert body["metadata"]["offset"] == 0
    assert body["metadata"]["limit"] == 2
    assert body["metadata"]["next_offset"] == 2


def test_pagination_news_latest_last_page(client):
    resp = client.get("/news/latest?offset=4&limit=2")
    body = resp.json()
    assert len(body["articles"]) == 1
    assert body["metadata"]["offset"] == 4
    assert body["metadata"]["limit"] == 2
    assert body["metadata"]["next_offset"] is None


def test_pagination_anomalies_default(client):
    resp = client.get("/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert "metadata" in body
    assert len(body["anomalies"]) <= 20
    assert body["metadata"]["offset"] == 0
    assert body["metadata"]["limit"] == 20


def test_pagination_anomalies_offset_limit(client):
    resp = client.get("/anomalies?offset=0&limit=2")
    body = resp.json()
    assert len(body["anomalies"]) == 2
    assert body["metadata"]["offset"] == 0
    assert body["metadata"]["limit"] == 2
    assert body["metadata"]["next_offset"] == 2


def test_pagination_anomalies_beyond_data(client):
    resp = client.get("/anomalies?offset=100&limit=10")
    body = resp.json()
    assert len(body["anomalies"]) == 0
    assert body["metadata"]["offset"] == 100
    assert body["metadata"]["limit"] == 10
    assert body["metadata"]["next_offset"] is None


def test_pagination_recent_quotes_default(client):
    resp = client.get("/quotes/AAPL/recent")
    assert resp.status_code == 200
    body = resp.json()
    assert "metadata" in body
    assert len(body["ticks"]) <= 20
    assert body["metadata"]["offset"] == 0
    assert body["metadata"]["limit"] == 20


def test_pagination_recent_quotes_offset_limit(client):
    resp = client.get("/quotes/AAPL/recent?offset=0&limit=1")
    body = resp.json()
    assert len(body["ticks"]) == 1
    assert body["metadata"]["offset"] == 0
    assert body["metadata"]["limit"] == 1
    assert body["metadata"]["next_offset"] is None

"""Test FastAPI endpoints."""

from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from marketpulse.api.app import create_app
from marketpulse.db import session as db_session
from marketpulse.db.models import Base, StockTick


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

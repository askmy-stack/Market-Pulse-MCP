"""Test API key authentication."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from marketpulse.api.app import create_app
from marketpulse.config import get_settings
from marketpulse.db import session as db_session
from marketpulse.db.models import Base


@pytest.fixture
def client_with_auth(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-secret-key")
    get_settings.cache_clear()

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

    yield TestClient(create_app())
    get_settings.cache_clear()
    monkeypatch.delenv("API_KEY", raising=False)


def test_health_exempt_from_auth(client_with_auth):
    resp = client_with_auth.get("/health")
    assert resp.status_code == 200


def test_protected_route_requires_api_key(client_with_auth):
    resp = client_with_auth.get("/symbols")
    assert resp.status_code == 401


def test_protected_route_with_valid_api_key(client_with_auth):
    resp = client_with_auth.get("/symbols", headers={"X-API-Key": "test-secret-key"})
    assert resp.status_code == 200


def test_protected_route_rejects_invalid_key(client_with_auth):
    resp = client_with_auth.get("/symbols", headers={"X-API-Key": "wrong"})
    assert resp.status_code == 401

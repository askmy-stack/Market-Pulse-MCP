"""Unit coverage for every MarketPulse MCP tool wrapper."""

from __future__ import annotations

import importlib
import json
from contextlib import nullcontext
from unittest.mock import MagicMock

import httpx
import pytest

from marketpulse.mcp.tools import MarketPulseTools
from marketpulse.observability.metrics import MCP_TOOL_CALLS


@pytest.fixture
def mocked_tools(monkeypatch):
    monkeypatch.setattr("marketpulse.mcp.tools.init_db", lambda: None)
    response = MagicMock()
    response.json.return_value = {"ok": True}
    response.raise_for_status.return_value = None
    client = MagicMock()
    client.get.return_value = response
    client.post.return_value = response
    client_context = MagicMock()
    client_context.__enter__.return_value = client
    monkeypatch.setattr("marketpulse.mcp.tools.httpx.Client", lambda **_kwargs: client_context)
    return MarketPulseTools(api_base="http://test"), client


@pytest.mark.parametrize(
    "method,args,http_method,path",
    [
        ("get_latest_quote", ("aapl",), "get", "/quotes/AAPL/latest"),
        ("get_recent_ticks", ("aapl", 7), "get", "/quotes/AAPL/recent?limit=7"),
        ("get_stock_features", ("aapl",), "get", "/features/AAPL"),
        ("detect_market_anomalies", (7,), "get", "/anomalies?limit=7"),
        ("get_pipeline_health", (), "get", "/pipeline/health"),
        ("list_tracked_symbols", (), "get", "/symbols"),
        ("get_company_news", ("aapl", 7), "get", "/news/company/AAPL?limit=7"),
        ("get_market_news", (7,), "get", "/news/market?limit=7"),
        ("analyze_news_sentiment", ("aapl",), "get", "/news/sentiment/AAPL"),
        ("summarize_stock_context", ("aapl",), "get", "/context/AAPL"),
        ("generate_market_brief", (), "post", "/brief/market"),
        ("explain_stock_move", ("aapl",), "get", "/explain/AAPL"),
    ],
)
def test_http_backed_tools(mocked_tools, method, args, http_method, path):
    tools, client = mocked_tools

    result = getattr(tools, method)(*args)

    assert json.loads(result)["ok"] is True
    request = getattr(client, http_method)
    request.assert_called_once()
    assert request.call_args.args[0] == f"http://test{path}"


def test_find_news_related_to_missing_anomaly_uses_database(monkeypatch):
    monkeypatch.setattr("marketpulse.mcp.tools.init_db", lambda: None)
    repository = MagicMock()
    repository.get_anomaly.return_value = None
    monkeypatch.setattr("marketpulse.mcp.tools.get_db", lambda: nullcontext(MagicMock()))
    monkeypatch.setattr("marketpulse.mcp.tools.Repository", lambda _session: repository)

    result = MarketPulseTools(api_base="http://test").find_news_related_to_anomaly("missing")

    assert json.loads(result) == {"error": "Anomaly not found"}
    repository.get_anomaly.assert_called_once_with("missing")


@pytest.mark.parametrize(
    "error",
    [
        httpx.HTTPStatusError(
            "not found",
            request=httpx.Request("GET", "http://test/missing"),
            response=httpx.Response(404),
        ),
        httpx.ReadTimeout("timed out", request=httpx.Request("GET", "http://test/slow")),
    ],
)
def test_http_errors_propagate(monkeypatch, error):
    monkeypatch.setattr("marketpulse.mcp.tools.init_db", lambda: None)
    client = MagicMock()
    client.get.side_effect = error
    client_context = MagicMock()
    client_context.__enter__.return_value = client
    monkeypatch.setattr("marketpulse.mcp.tools.httpx.Client", lambda **_kwargs: client_context)

    with pytest.raises(type(error)):
        MarketPulseTools(api_base="http://test").get_pipeline_health()


@pytest.mark.asyncio
async def test_call_tool_increments_metric(monkeypatch):
    monkeypatch.setattr("marketpulse.mcp.tools.init_db", lambda: None)
    server = importlib.import_module("marketpulse.mcp.server")
    monkeypatch.setattr(server.tools_client, "get_pipeline_health", lambda: '{"ok": true}')
    counter = MCP_TOOL_CALLS.labels(tool="get_pipeline_health")
    before = counter._value.get()

    result = await server.call_tool("get_pipeline_health", {})

    assert result[0].text == '{"ok": true}'
    assert counter._value.get() == before + 1

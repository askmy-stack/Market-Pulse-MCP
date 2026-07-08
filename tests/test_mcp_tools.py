"""Tests for MCP tool wrappers (MarketPulseTools).
"""






from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

from marketpulse.mcp.tools import MarketPulseTools


# Helpers
def _ok_response(payload: dict) -> MagicMock:
    """A mock httpx.Response-like object that succeeds."""
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp

def _http_error_response(status_code: int = 404) -> MagicMock:
    """A mock httpx.Response-like object whose raise_for_status() raises."""
    request = httpx.Request("GET", "http://test")
    response = httpx.Response(status_code, request=request)
    resp = MagicMock()
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "error", request=request, response=response
    )
    return resp

def _mock_get_db():
    """A mock get_db() context manager yielding a mock session."""
    mock_session = MagicMock()
    mock_get_db_cm = MagicMock()
    mock_get_db_cm.return_value.__enter__.return_value = mock_session
    mock_get_db_cm.return_value.__exit__.return_value = False
    return mock_session, mock_get_db_cm
@pytest.fixture

def tools():
    """A MarketPulseTools instance with init_db patched out (no real DB)."""
    with patch("marketpulse.mcp.tools.init_db"):
        yield MarketPulseTools(api_base="http://test")








# Simple API backed GET tools
GET_TOOLS = [
    ("get_latest_quote", ("AAPL",), {}, {"symbol": "AAPL", "price": 190.0}, True),
    ("get_recent_ticks", ("AAPL",), {}, {"symbol": "AAPL", "ticks": []}, False),
    ("get_stock_features", ("AAPL",), {}, {"symbol": "AAPL", "volatility": 0.02}, False),
    ("detect_market_anomalies", (), {}, {"anomalies": []}, True),
    ("get_pipeline_health", (), {}, {"status": "ok"}, False),
    ("list_tracked_symbols", (), {}, {"symbols": ["AAPL", "MSFT"]}, False),
    ("get_company_news", ("AAPL",), {}, {"symbol": "AAPL", "articles": []}, False),
    ("get_market_news", (), {}, {"articles": []}, False),
    ("analyze_news_sentiment", ("AAPL",), {}, {"symbol": "AAPL", "avg_sentiment": 0.1}, True),
]






GET_TOOL_IDS = [name for name, *_ in GET_TOOLS]






@pytest.mark.parametrize("method_name,args,kwargs,payload,expects_disclaimer", GET_TOOLS, ids=GET_TOOL_IDS)
def test_get_tool_success(tools, method_name, args, kwargs, payload, expects_disclaimer):
    mock_resp = _ok_response(payload)
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp
        result = getattr(tools, method_name)(*args, **kwargs)
    parsed = json.loads(result)
    for key, value in payload.items():
        assert parsed[key] == value
    if expects_disclaimer:
        assert "disclaimer" in parsed



@pytest.mark.parametrize("method_name,args,kwargs,payload,_disclaimer", GET_TOOLS, ids=GET_TOOL_IDS)
def test_get_tool_404(tools, method_name, args, kwargs, payload, _disclaimer):
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.return_value = _http_error_response(404)
        with pytest.raises(httpx.HTTPStatusError):
            getattr(tools, method_name)(*args, **kwargs)



@pytest.mark.parametrize("method_name,args,kwargs,payload,_disclaimer", GET_TOOLS, ids=GET_TOOL_IDS)
def test_get_tool_timeout(tools, method_name, args, kwargs, payload, _disclaimer):
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.side_effect = httpx.TimeoutException(
            "timed out"
        )
        with pytest.raises(httpx.TimeoutException):
            getattr(tools, method_name)(*args, **kwargs)


def test_get_recent_ticks_uses_limit_in_query(tools):
    mock_resp = _ok_response({"symbol": "AAPL", "ticks": []})
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.get.return_value = mock_resp
        tools.get_recent_ticks("aapl", limit=5)

    called_url = mock_client.get.call_args[0][0]
    assert "AAPL" in called_url
    assert "limit=5" in called_url







# POST-backed tool: generate_market_brief
def test_generate_market_brief_success(tools):
    mock_resp = _ok_response({"brief_type": "market", "content": "Markets were mixed today."})
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp
        result = tools.generate_market_brief()

    parsed = json.loads(result)
    assert parsed["brief_type"] == "market"

def test_generate_market_brief_404(tools):
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.post.return_value = _http_error_response(404)
        with pytest.raises(httpx.HTTPStatusError):
            tools.generate_market_brief()

def test_generate_market_brief_timeout(tools):
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.post.side_effect = httpx.TimeoutException(
            "timed out"
        )
        with pytest.raises(httpx.TimeoutException):
            tools.generate_market_brief()





# DB-only tool: find_news_related_to_anomaly
def test_find_news_related_to_anomaly_not_found(tools):
    _, mock_get_db_cm = _mock_get_db()
    mock_repo = MagicMock()
    mock_repo.get_anomaly.return_value = None
    with patch("marketpulse.mcp.tools.get_db", mock_get_db_cm), patch(
        "marketpulse.mcp.tools.Repository", return_value=mock_repo
    ):
        result = tools.find_news_related_to_anomaly("missing-id")
    assert json.loads(result) == {"error": "Anomaly not found"}






def test_find_news_related_to_anomaly_found(tools):
    _, mock_get_db_cm = _mock_get_db()
    anomaly_row = MagicMock()
    anomaly_row.event_id = "anomaly-1"
    anomaly_row.symbol = "AAPL"
    anomaly_row.timestamp = datetime.now(timezone.utc)
    anomaly_row.anomaly_type = "price_spike"
    anomaly_row.severity = "high"
    anomaly_row.z_score = 3.2
    anomaly_row.volume_ratio = 2.1
    anomaly_row.price = 190.0
    anomaly_row.price_change_pct = 5.0
    anomaly_row.description = "Sharp move"
    anomaly_row.related_news_ids = []
    mock_repo = MagicMock()
    mock_repo.get_anomaly.return_value = anomaly_row
    mock_repo.get_news_in_window.return_value = []
    mock_correlator = MagicMock()
    mock_correlator.window_minutes = 30
    mock_correlator.correlate.return_value = ["news-1", "news-2"]
    with patch("marketpulse.mcp.tools.get_db", mock_get_db_cm), patch(
        "marketpulse.mcp.tools.Repository", return_value=mock_repo
    ), patch("marketpulse.mcp.tools.NewsCorrelator", return_value=mock_correlator):
        result = tools.find_news_related_to_anomaly("anomaly-1")
    parsed = json.loads(result)
    assert parsed["anomaly_id"] == "anomaly-1"
    assert parsed["related_news_ids"] == ["news-1", "news-2"]

def test_find_news_related_to_anomaly_db_error_propagates(tools):
    _, mock_get_db_cm = _mock_get_db()
    mock_repo = MagicMock()
    mock_repo.get_anomaly.side_effect = RuntimeError("db unavailable")

    with patch("marketpulse.mcp.tools.get_db", mock_get_db_cm), patch(
        "marketpulse.mcp.tools.Repository", return_value=mock_repo
    ):
        with pytest.raises(RuntimeError):
            tools.find_news_related_to_anomaly("anomaly-1")






# API-first/DB-fallback tool: summarize_stock_context
def test_summarize_stock_context_success_via_api(tools):
    mock_resp = _ok_response({"symbol": "AAPL", "explanation": "context from api"})
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp
        result = tools.summarize_stock_context("AAPL")
    parsed = json.loads(result)
    assert parsed["symbol"] == "AAPL"
    assert parsed["explanation"] == "context from api"
    assert "disclaimer" in parsed

def test_summarize_stock_context_falls_back_to_db_on_404(tools):
    _, mock_get_db_cm = _mock_get_db()
    mock_context = MagicMock()
    mock_context.model_dump.return_value = {"symbol": "AAPL", "explanation": "from db"}
    mock_engine = MagicMock()
    mock_engine.build_context.return_value = mock_context

    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls, patch(
        "marketpulse.mcp.tools.get_db", mock_get_db_cm
    ), patch("marketpulse.mcp.tools.Repository"), patch(
        "marketpulse.context.market_context_engine.MarketContextEngine",
        return_value=mock_engine,
    ):
        mock_client_cls.return_value.__enter__.return_value.get.return_value = _http_error_response(404)
        result = tools.summarize_stock_context("AAPL")
    parsed = json.loads(result)
    assert parsed["symbol"] == "AAPL"
    assert parsed["explanation"] == "from db"
    assert "disclaimer" in parsed
    mock_engine.build_context.assert_called_once_with("AAPL")

def test_summarize_stock_context_timeout_propagates(tools):
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.side_effect = httpx.TimeoutException(
            "timed out"
        )
        with pytest.raises(httpx.TimeoutException):
            tools.summarize_stock_context("AAPL")





# API-first / DB-fallback tool: explain_stock_move
def test_explain_stock_move_success_via_api(tools):
    mock_resp = _ok_response(
        {"symbol": "MSFT", "explanation": "MSFT rose 1%", "disclaimer": "not financial advice"}
    )
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp
        result = tools.explain_stock_move("MSFT")

    parsed = json.loads(result)
    assert parsed["symbol"] == "MSFT"
    assert "disclaimer" in parsed



def test_explain_stock_move_falls_back_to_db_on_404(tools):
    _, mock_get_db_cm = _mock_get_db()

    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls, patch(
        "marketpulse.mcp.tools.get_db", mock_get_db_cm
    ), patch("marketpulse.mcp.tools.Repository"), patch(
        "marketpulse.mcp.tools.explain_stock_move",
        return_value={"symbol": "MSFT", "explanation": "from db", "disclaimer": "x"},
    ) as mock_explain:
        mock_client_cls.return_value.__enter__.return_value.get.return_value = _http_error_response(404)
        result = tools.explain_stock_move("MSFT")

    parsed = json.loads(result)
    assert parsed["symbol"] == "MSFT"
    assert parsed["explanation"] == "from db"
    mock_explain.assert_called_once()




def test_explain_stock_move_timeout_propagates(tools):
    with patch("marketpulse.mcp.tools.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value.get.side_effect = httpx.TimeoutException(
            "timed out"
        )
        with pytest.raises(httpx.TimeoutException):
            tools.explain_stock_move("MSFT")








# Sanity check: all 13 tools are exercised above
ALL_TOOL_METHODS = {
    "get_latest_quote",
    "get_recent_ticks",
    "get_stock_features",
    "detect_market_anomalies",
    "get_pipeline_health",
    "list_tracked_symbols",
    "get_company_news",
    "get_market_news",
    "analyze_news_sentiment",
    "find_news_related_to_anomaly",
    "summarize_stock_context",
    "generate_market_brief",
    "explain_stock_move",
}






def test_all_13_tools_are_covered():
    covered = {name for name, *_ in GET_TOOLS} | {
        "generate_market_brief",
        "find_news_related_to_anomaly",
        "summarize_stock_context",
        "explain_stock_move",
    }
    assert covered == ALL_TOOL_METHODS
    assert len(ALL_TOOL_METHODS) == 13

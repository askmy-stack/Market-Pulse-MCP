"""Tests for the MCP server's tool dispatch and metrics instrumentation."""



from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from marketpulse.observability.metrics import MCP_TOOL_CALLS


@pytest.fixture(scope="module")
def server_module():
    """Import marketpulse.mcp.server once, with DB init mocked out."""
    with patch("marketpulse.mcp.tools.init_db"):
        module = importlib.import_module("marketpulse.mcp.server")
    return module

@pytest.fixture
def mock_tools_client(server_module, monkeypatch):
    """Replace the module-level tools_client singleton with a MagicMock."""
    mock_client = MagicMock()
    monkeypatch.setattr(server_module, "tools_client", mock_client)
    return mock_client

def _counter_value(tool_name: str) -> float:
    return MCP_TOOL_CALLS.labels(tool=tool_name)._value.get()





# (tool_name, call_arguments, expected_client_method, expected_call_args)
DISPATCH_CASES = [
    ("get_latest_quote", {"symbol": "AAPL"}, "get_latest_quote", ("AAPL",)),
    ("get_recent_ticks", {"symbol": "AAPL", "limit": 10}, "get_recent_ticks", ("AAPL", 10)),
    ("get_recent_ticks", {"symbol": "AAPL"}, "get_recent_ticks", ("AAPL", 20)),
    ("get_stock_features", {"symbol": "AAPL"}, "get_stock_features", ("AAPL",)),
    ("detect_market_anomalies", {"limit": 5}, "detect_market_anomalies", (5,)),
    ("detect_market_anomalies", {}, "detect_market_anomalies", (20,)),
    ("get_pipeline_health", {}, "get_pipeline_health", ()),
    ("list_tracked_symbols", {}, "list_tracked_symbols", ()),
    ("get_company_news", {"symbol": "AAPL", "limit": 3}, "get_company_news", ("AAPL", 3)),
    ("get_company_news", {"symbol": "AAPL"}, "get_company_news", ("AAPL", 10)),
    ("get_market_news", {"limit": 7}, "get_market_news", (7,)),
    ("get_market_news", {}, "get_market_news", (10,)),
    ("analyze_news_sentiment", {"symbol": "AAPL"}, "analyze_news_sentiment", ("AAPL",)),
    (
        "find_news_related_to_anomaly",
        {"anomaly_id": "anom-1"},
        "find_news_related_to_anomaly",
        ("anom-1",),
    ),
    ("summarize_stock_context", {"symbol": "AAPL"}, "summarize_stock_context", ("AAPL",)),
    ("generate_market_brief", {}, "generate_market_brief", ()),
    ("explain_stock_move", {"symbol": "AAPL"}, "explain_stock_move", ("AAPL",)),
]




DISPATCH_IDS = [f"{name}-{args}" for name, args, *_ in DISPATCH_CASES]





@pytest.mark.parametrize(
    "tool_name,arguments,client_method,expected_args", DISPATCH_CASES, ids=DISPATCH_IDS
)
async def test_call_tool_dispatches_to_correct_method(
    server_module, mock_tools_client, tool_name, arguments, client_method, expected_args
):
    getattr(mock_tools_client, client_method).return_value = "some result"
    result = await server_module.call_tool(tool_name, arguments)
    getattr(mock_tools_client, client_method).assert_called_once_with(*expected_args)
    assert result[0].text == "some result"

@pytest.mark.parametrize(
    "tool_name,arguments,client_method,_expected_args", DISPATCH_CASES, ids=DISPATCH_IDS
)




async def test_call_tool_increments_metric_for_each_tool(
    server_module, mock_tools_client, tool_name, arguments, client_method, _expected_args
):
    getattr(mock_tools_client, client_method).return_value = "some result"
    before = _counter_value(tool_name)
    await server_module.call_tool(tool_name, arguments)
    after = _counter_value(tool_name)
    assert after == before + 1



async def test_call_tool_unknown_tool_does_not_increment_metric(server_module, mock_tools_client):
    before = _counter_value("totally_unknown_tool")

    result = await server_module.call_tool("totally_unknown_tool", {})

    assert "Unknown tool" in result[0].text
    after = _counter_value("totally_unknown_tool")
    assert after == before
    mock_tools_client.assert_not_called()


async def test_call_tool_increments_metric_even_when_underlying_call_fails(
    server_module, mock_tools_client
):
    """The counter is incremented before the tool is invoked, so a failing
    tool call still counts as an attempted call."""
    mock_tools_client.get_latest_quote.side_effect = RuntimeError("upstream boom")
    before = _counter_value("get_latest_quote")

    with pytest.raises(RuntimeError):
        await server_module.call_tool("get_latest_quote", {"symbol": "AAPL"})

    after = _counter_value("get_latest_quote")
    assert after == before + 1


async def test_list_tools_returns_all_13_tools(server_module):
    tools = await server_module.list_tools()
    names = {t.name for t in tools}
    assert len(tools) == 13
    assert names == {name for name, *_ in DISPATCH_CASES}

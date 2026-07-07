"""Test MCP tool wrappers."""

from unittest.mock import MagicMock, patch

from marketpulse.mcp.tools import MarketPulseTools


@patch("marketpulse.mcp.tools.init_db")
@patch("marketpulse.mcp.tools.httpx.Client")
def test_get_latest_quote(mock_client_cls, mock_init_db):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"symbol": "AAPL", "price": 190.0}
    mock_resp.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp

    tools = MarketPulseTools(api_base="http://test")
    result = tools.get_latest_quote("AAPL")
    assert "AAPL" in result
    assert "disclaimer" in result


@patch("marketpulse.mcp.tools.init_db")
@patch("marketpulse.mcp.tools.httpx.Client")
def test_explain_stock_move(mock_client_cls, mock_init_db):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "symbol": "MSFT",
        "explanation": "MSFT rose 1%",
        "disclaimer": "not financial advice",
    }
    mock_resp.raise_for_status = MagicMock()
    mock_client_cls.return_value.__enter__.return_value.get.return_value = mock_resp

    tools = MarketPulseTools(api_base="http://test")
    result = tools.explain_stock_move("MSFT")
    assert "MSFT" in result
    assert "disclaimer" in result

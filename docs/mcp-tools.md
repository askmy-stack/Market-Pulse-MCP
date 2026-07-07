# MCP Tools Reference

MarketPulse MCP exposes tools for AI agents to query live pipeline data.

## Tools

| Tool | Description |
|------|-------------|
| `get_latest_quote` | Latest price/volume for a symbol |
| `get_recent_ticks` | Recent tick history |
| `get_stock_features` | Rolling returns, volatility, z-score |
| `detect_market_anomalies` | Recent anomalies across symbols |
| `get_pipeline_health` | Component health status |
| `list_tracked_symbols` | Active symbols in pipeline |
| `get_company_news` | Company-specific news |
| `get_market_news` | Broad market headlines |
| `analyze_news_sentiment` | Sentiment summary for a symbol |
| `find_news_related_to_anomaly` | Time-window news correlation |
| `summarize_stock_context` | Correlated context summary |
| `generate_market_brief` | Market-wide brief |
| `explain_stock_move` | **Hero tool** — full move explanation |

## Hero Tool: `explain_stock_move`

Combines price change, detected anomalies, and time-correlated news into a narrative explanation.

**Always includes disclaimer:** correlation-based context only, not financial advice, no price predictions.

## Configuration

```json
{
  "mcpServers": {
    "marketpulse": {
      "command": "python",
      "args": ["-m", "marketpulse.mcp.server"],
      "env": {
        "MCP_API_BASE_URL": "http://localhost:8000"
      }
    }
  }
}
```

See `examples/mcp/cursor-mcp.json` for a full example.

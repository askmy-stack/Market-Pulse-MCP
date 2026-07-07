"""MarketPulse MCP server."""

from __future__ import annotations

import asyncio

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from marketpulse.config import get_settings
from marketpulse.mcp.tools import MarketPulseTools
from marketpulse.observability.logging import setup_logging

server = Server(get_settings().mcp_server_name)
tools_client = MarketPulseTools()


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(name="get_latest_quote", description="Get latest stock quote for a symbol", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}),
        Tool(name="get_recent_ticks", description="Get recent stock ticks", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}, "limit": {"type": "integer", "default": 20}}, "required": ["symbol"]}),
        Tool(name="get_stock_features", description="Get computed rolling features", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}),
        Tool(name="detect_market_anomalies", description="List recent market anomalies", inputSchema={"type": "object", "properties": {"limit": {"type": "integer", "default": 20}}}),
        Tool(name="get_pipeline_health", description="Get pipeline component health", inputSchema={"type": "object", "properties": {}}),
        Tool(name="list_tracked_symbols", description="List tracked stock symbols", inputSchema={"type": "object", "properties": {}}),
        Tool(name="get_company_news", description="Get company news for a symbol", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}, "limit": {"type": "integer", "default": 10}}, "required": ["symbol"]}),
        Tool(name="get_market_news", description="Get latest market news", inputSchema={"type": "object", "properties": {"limit": {"type": "integer", "default": 10}}}),
        Tool(name="analyze_news_sentiment", description="Analyze news sentiment for a symbol", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}),
        Tool(name="find_news_related_to_anomaly", description="Find news correlated with an anomaly", inputSchema={"type": "object", "properties": {"anomaly_id": {"type": "string"}}, "required": ["anomaly_id"]}),
        Tool(name="summarize_stock_context", description="Summarize correlated market context", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}),
        Tool(name="generate_market_brief", description="Generate a market-wide brief", inputSchema={"type": "object", "properties": {}}),
        Tool(name="explain_stock_move", description="Hero tool: explain a stock move with correlated news and anomalies", inputSchema={"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    mapping = {
        "get_latest_quote": lambda: tools_client.get_latest_quote(arguments["symbol"]),
        "get_recent_ticks": lambda: tools_client.get_recent_ticks(arguments["symbol"], arguments.get("limit", 20)),
        "get_stock_features": lambda: tools_client.get_stock_features(arguments["symbol"]),
        "detect_market_anomalies": lambda: tools_client.detect_market_anomalies(arguments.get("limit", 20)),
        "get_pipeline_health": lambda: tools_client.get_pipeline_health(),
        "list_tracked_symbols": lambda: tools_client.list_tracked_symbols(),
        "get_company_news": lambda: tools_client.get_company_news(arguments["symbol"], arguments.get("limit", 10)),
        "get_market_news": lambda: tools_client.get_market_news(arguments.get("limit", 10)),
        "analyze_news_sentiment": lambda: tools_client.analyze_news_sentiment(arguments["symbol"]),
        "find_news_related_to_anomaly": lambda: tools_client.find_news_related_to_anomaly(arguments["anomaly_id"]),
        "summarize_stock_context": lambda: tools_client.summarize_stock_context(arguments["symbol"]),
        "generate_market_brief": lambda: tools_client.generate_market_brief(),
        "explain_stock_move": lambda: tools_client.explain_stock_move(arguments["symbol"]),
    }
    if name not in mapping:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]
    result = await asyncio.to_thread(mapping[name])
    return [TextContent(type="text", text=result)]


async def run_server() -> None:
    setup_logging()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main() -> None:
    asyncio.run(run_server())


if __name__ == "__main__":
    main()

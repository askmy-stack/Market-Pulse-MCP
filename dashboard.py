"""
Hero Feature 2 — Real-Time Streamlit Dashboard.

Two panels:
  Panel 1 — Live Market Feed
    Candlestick chart of the last N OHLCV records for a selected index symbol.
    A background thread consumes from Kafka and writes to an in-memory ring buffer.
    The Streamlit main loop re-renders every AUTO_REFRESH_SECONDS seconds.

  Panel 2 — Pipeline Health
    Prometheus metrics scraped from the consumer process (localhost:PROMETHEUS_PORT):
      - Consumer lag, throughput (msg/s), S3 write latency, DLQ depth, error rate.

Run:
    streamlit run dashboard.py
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime

import plotly.graph_objects as go
import requests
import streamlit as st
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "demo_test")
PROMETHEUS_PORT: int = int(os.getenv("PROMETHEUS_PORT", "8000"))
CHART_WINDOW: int = 50           # candlestick bars to display
AUTO_REFRESH_SECONDS: int = 2    # dashboard refresh interval

# ---------------------------------------------------------------------------
# Shared ring buffer (background thread → Streamlit main thread)
# ---------------------------------------------------------------------------
# symbol → deque of OHLCV dicts
_buffer: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=CHART_WINDOW))
_buffer_lock = threading.Lock()
_consumer_thread_started = False


def _kafka_consumer_thread() -> None:
    """Background thread: consume from Kafka and fill the ring buffer."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "dashboard-consumer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None or msg.error():
                continue
            try:
                record = json.loads(msg.value().decode("utf-8"))
                symbol = record.get("Index", "UNKNOWN")
                with _buffer_lock:
                    _buffer[symbol].append(record)
            except Exception:
                pass
    finally:
        consumer.close()


def _ensure_consumer_thread() -> None:
    global _consumer_thread_started
    if not _consumer_thread_started:
        t = threading.Thread(target=_kafka_consumer_thread, daemon=True)
        t.start()
        _consumer_thread_started = True


# ---------------------------------------------------------------------------
# Prometheus metric scraping
# ---------------------------------------------------------------------------

def _fetch_prometheus_metrics() -> dict[str, float]:
    """Scrape the consumer's /metrics endpoint and parse key gauges/counters."""
    metrics: dict[str, float] = {}
    try:
        resp = requests.get(
            f"http://localhost:{PROMETHEUS_PORT}/metrics", timeout=2
        )
        resp.raise_for_status()
        for line in resp.text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            if len(parts) >= 2:
                name = parts[0].split("{")[0]  # strip label selectors
                try:
                    metrics[name] = float(parts[-1])
                except ValueError:
                    pass
    except Exception:
        pass
    return metrics


# ---------------------------------------------------------------------------
# Streamlit layout
# ---------------------------------------------------------------------------

def _candlestick_chart(symbol: str, records: list[dict]) -> go.Figure:
    if not records:
        fig = go.Figure()
        fig.update_layout(title=f"No data yet for {symbol}", height=400)
        return fig

    dates = [r.get("Date", "") for r in records]
    opens = [float(r.get("Open", 0)) for r in records]
    highs = [float(r.get("High", 0)) for r in records]
    lows = [float(r.get("Low", 0)) for r in records]
    closes = [float(r.get("Close", 0)) for r in records]

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=dates,
                open=opens,
                high=highs,
                low=lows,
                close=closes,
                name=symbol,
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            )
        ]
    )
    fig.update_layout(
        title=f"{symbol} — Live OHLCV Feed (last {len(records)} ticks)",
        xaxis_title="Date",
        yaxis_title="Price",
        height=420,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
    )
    return fig


def main() -> None:
    st.set_page_config(
        page_title="Stock Market Pipeline Dashboard",
        page_icon=":chart_with_upwards_trend:",
        layout="wide",
    )

    _ensure_consumer_thread()

    st.title("Real-Time Stock Market Pipeline Dashboard")
    st.caption(
        f"Consuming from Kafka topic `{KAFKA_TOPIC}` on `{KAFKA_BOOTSTRAP_SERVERS}` "
        f"— refreshes every {AUTO_REFRESH_SECONDS}s"
    )

    # -----------------------------------------------------------------------
    # Panel 1 — Live Market Feed
    # -----------------------------------------------------------------------
    st.header("Live Market Feed")

    with _buffer_lock:
        available_symbols = sorted(_buffer.keys()) or ["(waiting for data…)"]

    selected_symbol = st.selectbox("Index symbol", available_symbols)

    with _buffer_lock:
        records = list(_buffer.get(selected_symbol, []))

    st.plotly_chart(
        _candlestick_chart(selected_symbol, records),
        use_container_width=True,
    )

    if records:
        latest = records[-1]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("CloseUSD", f"${float(latest.get('CloseUSD', 0)):,.2f}")
        col2.metric("High", f"${float(latest.get('High', 0)):,.2f}")
        col3.metric("Low", f"${float(latest.get('Low', 0)):,.2f}")
        col4.metric("Volume", f"{float(latest.get('Volume', 0)):,.0f}")

    st.divider()

    # -----------------------------------------------------------------------
    # Panel 2 — Pipeline Health
    # -----------------------------------------------------------------------
    st.header("Pipeline Health")

    prom = _fetch_prometheus_metrics()

    if not prom:
        st.warning(
            f"Could not reach Prometheus metrics at `localhost:{PROMETHEUS_PORT}/metrics`. "
            f"Is `consumer.py` running?"
        )
    else:
        h1, h2, h3, h4, h5 = st.columns(5)

        h1.metric(
            "Messages Consumed",
            f"{int(prom.get('messages_consumed_total', 0)):,}",
        )
        h2.metric(
            "Consumer Lag",
            f"{int(prom.get('consumer_lag_messages', 0)):,} msgs",
        )
        h3.metric(
            "S3 Writes",
            f"{int(prom.get('s3_writes_total', 0)):,}",
        )
        h4.metric(
            "S3 Write Errors",
            f"{int(prom.get('s3_write_errors_total', 0)):,}",
        )
        h5.metric(
            "DLQ Messages",
            f"{int(prom.get('dlq_messages_total', 0)):,}",
        )

        s3_latency = prom.get("s3_write_duration_seconds_sum", 0) / max(
            prom.get("s3_write_duration_seconds_count", 1), 1
        )
        st.caption(f"Average S3 batch write latency: **{s3_latency:.3f}s**")

    st.divider()
    st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

    # Auto-refresh
    time.sleep(AUTO_REFRESH_SECONDS)
    st.rerun()


if __name__ == "__main__":
    main()

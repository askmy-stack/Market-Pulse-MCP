# MarketPulse MCP

> Real-time stock market data processing pipeline built with Apache Kafka.

A data engineering exercise building a streaming pipeline that ingests real-time stock market data, processes it through Kafka topics, and outputs structured analytics. Demonstrates producer/consumer patterns, topic partitioning, and stream processing fundamentals.

## What it does

- Produces real-time stock tick data into Kafka topics
- Consumes and processes messages with configurable consumer groups
- Outputs structured analytics from the stream

## Stack

- **Languages:** Python
- **Streaming:** Apache Kafka
- **Tooling:** Jupyter Notebook

## Setup

```bash
git clone https://github.com/askmy-stack/kafka-stock-pipeline.git
cd kafka-stock-pipeline
docker compose up -d
python producer.py
python consumer.py
```

## What I learned

Partitioning by ticker symbol keeps all events for a symbol ordered on the same partition — critical for correct windowed aggregations downstream.

## License

MIT


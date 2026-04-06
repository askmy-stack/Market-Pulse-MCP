# CLAUDE.md — AI Assistant Guide

This file provides context for AI assistants (Claude and others) working in this repository.

## Project Overview

This is a **real-time stock market data processing pipeline** built on Apache Kafka and AWS. It simulates streaming stock market data through a Kafka broker and archives the stream to Amazon S3 for downstream analytics via AWS Glue and Athena.

The project is implemented entirely as Jupyter notebooks and is tutorial/PoC in nature.

## Repository Structure

```
Real-time-Stock-Market-Data-Processing-Engine-using-Kafka/
├── KafkaProducer.ipynb     # Reads CSV data and publishes to Kafka topic
├── KafkaConsumer.ipynb     # Subscribes to Kafka topic, writes records to S3
├── indexProcessed.csv      # Historical stock index OHLCV dataset (~9.6 MB, 104K rows)
├── Architecture.jpg        # System architecture diagram
└── README.md               # Project title only
```

No `requirements.txt`, `Dockerfile`, `docker-compose.yml`, `.env`, or test files exist yet.

## Technology Stack

| Layer | Technology |
|---|---|
| Language | Python 3 (Jupyter notebooks) |
| Message broker | Apache Kafka (hosted on AWS EC2) |
| Data source | `indexProcessed.csv` (OHLCV stock data, 1986–2021) |
| Data sink | Amazon S3 |
| Catalog / ETL | AWS Glue (crawler + Data Catalog) |
| Query engine | Amazon Athena |
| Key libraries | `kafka-python`, `pandas`, `s3fs`, `json`, `time` |

## Notebook Descriptions

### KafkaProducer.ipynb

- Installs `kafka-python` via `pip install kafka-python`.
- Reads `data/indexProcessed.csv` using pandas.
- Creates a `KafkaProducer` with JSON serialization connected to `:9092`.
- Continuously picks a **random** row from the dataset and publishes it to the `demo_test` Kafka topic every 1 second.
- Calls `producer.flush()` at the end to drain the buffer.

### KafkaConsumer.ipynb

- Creates a `KafkaConsumer` subscribed to the `demo_test` topic, connected to `:9092`.
- Uses JSON deserialization for incoming messages.
- Iterates over consumed messages and writes each as a JSON file to S3:
  - Bucket: `kafka-stock-market-tutorial-youtube-darshil`
  - File naming: `stock_market_{count}.json`

## Dataset

`indexProcessed.csv` columns:

| Column | Description |
|---|---|
| Index | Stock index symbol (e.g., HSI, J203.JO) |
| Date | Trading date |
| Open | Opening price |
| High | Intraday high |
| Low | Intraday low |
| Close | Closing price |
| Adj Close | Adjusted close |
| Volume | Trading volume |
| CloseUSD | Close price in USD |

## Configuration — Hardcoded Values to Change

Before running, replace these hardcoded values in both notebooks:

| Setting | Current Value | Notes |
|---|---|---|
| Kafka bootstrap server | `:9092` | Prepend the EC2 public IP, e.g. `1.2.3.4:9092` |
| Kafka topic | `demo_test` | Change if using a different topic |
| S3 bucket | `kafka-stock-market-tutorial-youtube-darshil` | Must exist and be accessible |
| CSV path | `data/indexProcessed.csv` | Adjust if the file is in a different location |

AWS credentials must be configured in the environment (e.g., via `aws configure`, environment variables, or an IAM instance profile) before running the consumer.

## Architecture

```
indexProcessed.csv
       |
       v
KafkaProducer.ipynb  -->  Apache Kafka (EC2 :9092, topic: demo_test)
                                   |
                                   v
                         KafkaConsumer.ipynb  -->  Amazon S3
                                                       |
                                               AWS Glue Crawler
                                                       |
                                            AWS Glue Data Catalog
                                                       |
                                             Amazon Athena (SQL)
```

## Running the Pipeline

1. **Start Kafka** on your EC2 instance (ZooKeeper + Broker on port 9092). Ensure the security group allows inbound TCP on 9092 from your IP.
2. **Update the bootstrap server IP** in both notebooks.
3. **Run KafkaProducer.ipynb** — starts publishing messages every second.
4. **Run KafkaConsumer.ipynb** (in a separate kernel/machine) — starts consuming and writing to S3.
5. **Run the AWS Glue crawler** over the S3 bucket to populate the Data Catalog.
6. **Query with Athena** using the cataloged table.

## Development Conventions

- All logic lives in Jupyter notebooks. If converting to `.py` scripts, mirror the same two-file separation (producer / consumer).
- Do not commit AWS credentials, EC2 IPs, or S3 bucket names. Move these to environment variables or a `.env` file (add `.env` to `.gitignore`).
- The dataset file (`indexProcessed.csv`) is already committed. Avoid re-committing it if modified; it is ~9.6 MB.
- Prefer JSON serialization for Kafka messages (consistent with existing code).

## Suggested Improvements (for future work)

- Add `requirements.txt` with pinned versions of `kafka-python`, `pandas`, `s3fs`.
- Extract hardcoded config values into environment variables or a `config.py`.
- Add error handling and retry logic in both producer and consumer.
- Add logging instead of bare `print` statements.
- Add a `docker-compose.yml` for local Kafka development (Zookeeper + Broker).
- Add basic unit/integration tests.
- Add a `.env.example` template.

## Branch Strategy

Active development branch: `claude/add-claude-documentation-IIk3b`
Main branch: `main`

Push all changes to the designated feature branch and open a PR to `main`.

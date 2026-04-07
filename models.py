"""
Pydantic v2 schema for stock market records.

Used by both producer.py (validate before publishing) and
consumer.py (validate after deserializing from Kafka).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class StockRecord(BaseModel):
    """A single OHLCV stock index record from indexProcessed.csv."""

    Index: str = Field(..., description="Stock index symbol, e.g. HSI, SPX")
    Date: str = Field(..., description="Trading date as string (YYYY-MM-DD)")
    Open: float = Field(..., ge=0, description="Opening price")
    High: float = Field(..., ge=0, description="Intraday high")
    Low: float = Field(..., ge=0, description="Intraday low")
    Close: float = Field(..., ge=0, description="Closing price")
    Adj_Close: float = Field(..., ge=0, alias="Adj Close", description="Adjusted close")
    Volume: float = Field(..., ge=0, description="Trading volume")
    CloseUSD: float = Field(..., ge=0, description="Close price converted to USD")

    model_config = {"populate_by_name": True}

    @field_validator("Index")
    @classmethod
    def index_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Index symbol must not be empty")
        return v.strip()

    @model_validator(mode="after")
    def high_gte_low(self) -> "StockRecord":
        if self.High < self.Low:
            raise ValueError(
                f"High ({self.High}) must be >= Low ({self.Low}) for {self.Index}"
            )
        return self

    def to_kafka_dict(self) -> dict:
        """Serialise to a plain dict for JSON encoding onto the Kafka wire."""
        return self.model_dump(by_alias=True)


class DLQRecord(BaseModel):
    """Envelope written to the dead-letter-queue topic on processing failure."""

    original_payload: str = Field(..., description="Raw message bytes as a string")
    error: str = Field(..., description="Exception message")
    failed_at: str = Field(..., description="ISO-8601 UTC timestamp of failure")
    consumer_id: str = Field(..., description="consumer_id that failed")
    topic: str
    partition: int
    offset: int

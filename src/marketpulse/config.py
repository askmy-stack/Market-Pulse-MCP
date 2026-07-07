"""Application configuration."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_client_id: str = "marketpulse"
    kafka_group_id: str = "marketpulse-group"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "marketpulse"
    postgres_user: str = "marketpulse"
    postgres_password: str = "marketpulse"

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    enable_real_stock_data: bool = False
    mock_symbols: str = "AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META,JPM"
    mock_tick_interval_seconds: float = 1.0
    mock_news_interval_seconds: float = 5.0

    rolling_window_size: int = 20
    anomaly_zscore_threshold: float = 2.5
    volume_spike_ratio: float = 2.0
    news_correlation_window_minutes: int = 30

    prometheus_port: int = 9090
    metrics_enabled: bool = True

    mcp_server_name: str = "marketpulse"
    mcp_api_base_url: str = "http://localhost:8000"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.mock_symbols.split(",") if s.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    APP_NAME: str = "ingestly"

    # Storage: PostgreSQL with asyncpg (provided via environment)
    DATABASE_URL: str = Field(...)

    # Queue: RabbitMQ AMQP URI (provided via environment)
    RABBITMQ_URL: str = Field(...)
    EVENTS_QUEUE: str = "events.inbound"
    EVENTS_RETRY: str = "events.retry"
    EVENTS_DLQ: str = "events.dlq"

    # Retry policy
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_BASE_SECONDS: float = 2.0
    RETRY_DELAYS_SECONDS: str = "5,30,300"
    WORKER_SHUTDOWN_TIMEOUT_SECONDS: float = 15.0

    # Security & Limits (secrets required via environment, no defaults)
    WEBHOOK_SECRET: str = Field(...)
    INGEST_API_KEY: str = Field(...)
    REPLAY_API_KEY: str = Field(...)
    WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS: int = 300
    MAX_BODY_BYTES: int = 1_048_576  # 1 MB limit
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_REQUESTS: int = 120
    RATE_LIMIT_WINDOW_SECONDS: int = 60

    # Retention Policy
    OUTBOX_RETENTION_HOURS: int = 24
    RAW_EVENT_RETENTION_DAYS: int = 7
    ENABLE_AUTO_RETENTION: bool = True


settings = Settings()  # type: ignore[call-arg]


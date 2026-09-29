"""Runtime configuration.

Everything the pipeline needs to reach Trino, Kafka and the Iceberg catalog.
Deliberately flat: a lakehouse job that needs a config object graph to start is
a lakehouse job nobody can run on a Friday afternoon.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_", env_file=".env", extra="ignore")

    # --- Trino ---
    trino_host: str = "localhost"
    trino_port: int = 8080
    trino_user: str = "lakehouse"
    trino_catalog: str = "iceberg"

    # --- Medallion schemas ---
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "transactions"
    kafka_consumer_group: str = "bronze-loader"

    # --- Ingestion ---
    batch_size: int = Field(default=500, ge=1, le=10_000)
    poll_timeout_ms: int = Field(default=5_000, ge=100)

    # --- Reconciliation tolerances ---
    # Amounts are integer minor units, so the row-level tolerance is genuinely zero.
    # The relative tolerance exists only for derived/FX-converted aggregates.
    amount_tolerance_minor: int = Field(default=0, ge=0)
    relative_tolerance: float = Field(default=0.0, ge=0.0, le=0.01)

    @field_validator("trino_port", "kafka_bootstrap_servers")
    @classmethod
    def _non_empty(cls, v: object) -> object:
        if isinstance(v, str) and not v.strip():
            raise ValueError("must not be blank")
        return v

    def table(self, schema: str, name: str) -> str:
        """Fully-qualified Trino table reference."""
        return f"{self.trino_catalog}.{schema}.{name}"

    @property
    def bronze_transactions(self) -> str:
        return self.table(self.bronze_schema, "transactions_raw")

    @property
    def silver_transactions(self) -> str:
        return self.table(self.silver_schema, "silver_transactions")

    @property
    def gold_fact(self) -> str:
        return self.table(self.gold_schema, "fct_transaction")


def load_settings(**overrides: object) -> Settings:
    return Settings(**overrides)  # type: ignore[arg-type]

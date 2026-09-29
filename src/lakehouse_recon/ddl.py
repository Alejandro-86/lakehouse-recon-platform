"""Medallion bootstrap: schemas and the bronze Iceberg table.

Silver and gold are owned by dbt — defining them here too would give the
project two sources of truth for the same tables, which is the actual cause of
most "why does prod not match the repo" incidents.
"""

from __future__ import annotations

from lakehouse_recon.config import Settings

BRONZE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    event_id varchar,
    ledger_entry_id varchar,
    account_id varchar,
    counterparty_account_id varchar,
    amount_minor bigint,
    signed_amount_minor bigint,
    currency varchar,
    direction varchar,
    transaction_type varchar,
    occurred_at timestamp(6) with time zone,
    source_system varchar,
    ingested_at timestamp(6) with time zone,
    batch_id varchar
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(occurred_at)'],
    sorted_by = ARRAY['occurred_at']
)
""".strip()


def bootstrap_statements(settings: Settings) -> list[str]:
    """Idempotent statements that create the medallion layout from nothing."""
    catalog = settings.trino_catalog
    schemas = [settings.bronze_schema, settings.silver_schema, settings.gold_schema]
    statements = [f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}" for schema in schemas]
    statements.append(BRONZE_TABLE_DDL.format(table=settings.bronze_transactions))
    return statements

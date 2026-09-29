from __future__ import annotations

import pytest
from pydantic import ValidationError

from lakehouse_recon.config import Settings, load_settings
from lakehouse_recon.ddl import bootstrap_statements


def test_table_helper_builds_a_qualified_name():
    settings = Settings(trino_catalog="iceberg")
    assert settings.table("silver", "x") == "iceberg.silver.x"


def test_layer_shortcuts_match_the_dbt_model_names():
    settings = Settings()
    assert settings.bronze_transactions.endswith("bronze.transactions_raw")
    assert settings.silver_transactions.endswith("silver.silver_transactions")
    assert settings.gold_fact.endswith("gold.fct_transaction")


def test_batch_size_is_bounded():
    with pytest.raises(ValidationError):
        Settings(batch_size=0)
    with pytest.raises(ValidationError):
        Settings(batch_size=50_000)


def test_relative_tolerance_is_capped_at_one_percent():
    with pytest.raises(ValidationError):
        Settings(relative_tolerance=0.5)


def test_amount_tolerance_defaults_to_zero():
    """Integer minor units make exact reconciliation achievable; keep it the default."""
    assert Settings().amount_tolerance_minor == 0
    assert Settings().relative_tolerance == 0.0


def test_overrides_are_applied():
    assert load_settings(trino_host="warehouse.internal").trino_host == "warehouse.internal"


def test_bootstrap_creates_every_schema_then_the_bronze_table():
    statements = bootstrap_statements(Settings())
    assert len(statements) == 4
    assert all("CREATE SCHEMA IF NOT EXISTS" in s for s in statements[:3])
    assert "CREATE TABLE IF NOT EXISTS iceberg.bronze.transactions_raw" in statements[3]


def test_bronze_table_is_partitioned_and_sorted_for_the_reconciliation_grain():
    ddl = bootstrap_statements(Settings())[3]
    assert "partitioning = ARRAY['month(occurred_at)']" in ddl
    assert "sorted_by = ARRAY['occurred_at']" in ddl

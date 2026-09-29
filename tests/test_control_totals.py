from __future__ import annotations

import pytest

from lakehouse_recon.control_totals import (
    TrinoControlTotals,
    control_total_sql,
    distinct_key_sql,
)
from lakehouse_recon.reconciliation import Layer

from .conftest import FakeRunner


def test_control_total_sql_aggregates_the_three_totals():
    sql = control_total_sql("iceberg.silver.silver_transactions", partition_expr="business_date")
    assert "COUNT(*) AS row_count" in sql
    assert "SUM(signed_amount_minor)" in sql
    assert "COUNT(DISTINCT event_id) AS distinct_event_ids" in sql
    assert "GROUP BY business_date" in sql


def test_partition_expression_can_be_a_function_not_just_a_column():
    sql = control_total_sql("iceberg.bronze.transactions_raw", partition_expr="date(occurred_at)")
    assert "CAST(date(occurred_at) AS varchar) AS partition_key" in sql


def test_dedupe_mode_wraps_the_source_in_a_first_arrival_window():
    sql = control_total_sql(
        "iceberg.bronze.transactions_raw",
        partition_expr="date(occurred_at)",
        dedupe_on="event_id",
    )
    assert "ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at)" in sql
    assert "WHERE _rn = 1" in sql


def test_dedupe_mode_keeps_the_predicate_inside_the_window():
    sql = control_total_sql(
        "iceberg.bronze.transactions_raw",
        partition_expr="date(occurred_at)",
        dedupe_on="event_id",
        where="batch_id = 'b1'",
    )
    assert sql.count("WHERE batch_id = 'b1'") == 1
    assert sql.index("WHERE batch_id = 'b1'") < sql.index("WHERE _rn = 1")


def test_where_clause_is_applied_without_dedupe():
    sql = control_total_sql(
        "iceberg.gold.fct_transaction",
        partition_expr="business_date",
        where="business_date >= DATE '2026-09-01'",
    )
    assert "WHERE business_date >= DATE '2026-09-01'" in sql


@pytest.mark.parametrize(
    "table",
    [
        "iceberg.bronze.t; DROP TABLE users",
        "iceberg.bronze.t--",
        "",
        ".bronze.t",
        "iceberg..t",
    ],
)
def test_identifiers_are_validated_against_injection(table):
    with pytest.raises(ValueError, match="unsafe table"):
        control_total_sql(table, partition_expr="business_date")


def test_blank_partition_expression_is_rejected():
    with pytest.raises(ValueError, match="must not be blank"):
        control_total_sql("iceberg.bronze.t", partition_expr="  ")


def test_fetch_maps_rows_onto_control_totals():
    runner = FakeRunner([[("2026-09-01", 100, 50_000, 98), ("2026-09-02", 40, 20_000, 40)]])
    totals = TrinoControlTotals(runner).fetch("iceberg.bronze.transactions_raw", Layer.BRONZE)
    assert [t.partition_key for t in totals] == ["2026-09-01", "2026-09-02"]
    assert totals[0].distinct_event_ids == 98
    assert all(t.layer is Layer.BRONZE for t in totals)


def test_fetch_passes_dedupe_through_to_the_sql():
    runner = FakeRunner([[]])
    TrinoControlTotals(runner).fetch(
        "iceberg.bronze.transactions_raw", Layer.BRONZE, dedupe_on="event_id"
    )
    assert "ROW_NUMBER()" in runner.last_sql


def test_distinct_key_sql_excludes_nulls():
    sql = distinct_key_sql("iceberg.gold.fct_transaction", "account_id")
    assert sql == (
        "SELECT DISTINCT account_id FROM iceberg.gold.fct_transaction "
        "WHERE account_id IS NOT NULL"
    )

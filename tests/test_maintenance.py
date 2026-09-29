from __future__ import annotations

import pytest

from lakehouse_recon.maintenance import (
    IcebergMaintenance,
    add_column_sql,
    expire_snapshots_sql,
    metadata_table_sql,
    optimize_sql,
    remove_orphan_files_sql,
    rollback_sql,
    set_partitioning_sql,
    time_travel_sql,
)

from .conftest import FakeRunner

TABLE = "iceberg.bronze.transactions_raw"


def test_metadata_table_quotes_the_dollar_suffix():
    assert metadata_table_sql(TABLE, "snapshots") == (
        'SELECT * FROM iceberg.bronze."transactions_raw$snapshots"'
    )


def test_metadata_table_honours_a_limit():
    assert metadata_table_sql(TABLE, "history", limit=5).endswith("LIMIT 5")


def test_unknown_metadata_table_is_rejected():
    with pytest.raises(ValueError, match="unknown metadata table"):
        metadata_table_sql(TABLE, "everything")


def test_table_must_be_three_parts_for_metadata_access():
    with pytest.raises(ValueError, match="catalog.schema.name"):
        metadata_table_sql("bronze_only", "snapshots")


def test_time_travel_by_snapshot_id():
    assert time_travel_sql(TABLE, snapshot_id=123) == f"SELECT * FROM {TABLE} FOR VERSION AS OF 123"


def test_time_travel_by_timestamp():
    sql = time_travel_sql(TABLE, as_of="2026-09-01 00:00:00 UTC")
    assert "FOR TIMESTAMP AS OF TIMESTAMP '2026-09-01 00:00:00 UTC'" in sql


@pytest.mark.parametrize(
    "kwargs", [{}, {"snapshot_id": 1, "as_of": "2026-09-01 00:00:00 UTC"}]
)
def test_time_travel_requires_exactly_one_anchor(kwargs):
    with pytest.raises(ValueError, match="exactly one"):
        time_travel_sql(TABLE, **kwargs)


def test_rollback_uses_positional_arguments():
    """`table` is reserved in Trino's grammar, so named arguments are a syntax error."""
    assert rollback_sql("iceberg", "bronze", "transactions_raw", 999) == (
        "CALL iceberg.system.rollback_to_snapshot('bronze', 'transactions_raw', 999)"
    )


def test_expire_snapshots_carries_the_retention_threshold():
    assert "retention_threshold => '30d'" in expire_snapshots_sql(TABLE, "30d")


def test_optimize_defaults_to_a_128mb_target():
    assert "file_size_threshold => '128MB'" in optimize_sql(TABLE)


def test_optimize_can_be_scoped_to_recent_partitions():
    sql = optimize_sql(TABLE, where="occurred_at > current_date - interval '7' day")
    assert sql.endswith("WHERE occurred_at > current_date - interval '7' day")


def test_remove_orphan_files_statement():
    assert remove_orphan_files_sql(TABLE, "14d").startswith(f"ALTER TABLE {TABLE} EXECUTE")


def test_add_column_is_idempotent_by_default():
    assert add_column_sql(TABLE, "settlement_ref", "varchar") == (
        f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS settlement_ref varchar"
    )


def test_add_column_rejects_a_blank_type():
    with pytest.raises(ValueError, match="must not be blank"):
        add_column_sql(TABLE, "c", "  ")


def test_partition_evolution_sets_an_array_property():
    sql = set_partitioning_sql(TABLE, ["month(occurred_at)", "currency"])
    assert sql.endswith("SET PROPERTIES partitioning = ARRAY['month(occurred_at)', 'currency']")


def test_partition_evolution_requires_a_spec():
    with pytest.raises(ValueError, match="must not be empty"):
        set_partitioning_sql(TABLE, [])


def test_snapshots_are_parsed_into_dataclasses():
    runner = FakeRunner(
        [[("2026-09-01 10:00:00", 111, None, "append", "s3://m.avro", {"added-records": "500"})]]
    )
    snaps = IcebergMaintenance(runner).snapshots(TABLE)
    assert snaps[0].snapshot_id == 111
    assert snaps[0].operation == "append"
    assert snaps[0].summary["added-records"] == "500"


def test_latest_snapshot_id_is_none_on_an_empty_table():
    assert IcebergMaintenance(FakeRunner([[]])).latest_snapshot_id(TABLE) is None


def test_rollback_splits_the_qualified_table_name():
    runner = FakeRunner()
    IcebergMaintenance(runner).rollback(TABLE, 42)
    assert "rollback_to_snapshot" in runner.last_sql
    assert "'bronze', 'transactions_raw', 42" in runner.last_sql

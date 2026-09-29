"""Iceberg table maintenance and time travel, as Trino SQL.

These are the operations that separate "I have read about Iceberg" from "I have
run Iceberg": snapshot inspection, time travel, rollback, expiring snapshots,
compacting small files, orphan-file cleanup, schema evolution and partition
evolution.

Builders return SQL strings so they can be asserted on in unit tests; the
:class:`IcebergMaintenance` wrapper executes them. Every statement here is
Trino's Iceberg-connector dialect.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from lakehouse_recon.control_totals import QueryRunner, _validate_identifier

_METADATA_TABLES = ("snapshots", "history", "manifests", "files", "partitions", "refs")


def metadata_table_sql(table: str, metadata: str, limit: int | None = None) -> str:
    """Iceberg exposes metadata as ``"schema.table$snapshots"`` style tables."""
    _validate_identifier(table, "table")
    if metadata not in _METADATA_TABLES:
        raise ValueError(f"unknown metadata table {metadata!r}; expected one of {_METADATA_TABLES}")
    catalog, _, rest = table.partition(".")
    schema, _, name = rest.partition(".")
    if not (catalog and schema and name):
        raise ValueError(f"table must be catalog.schema.name, got {table!r}")
    suffix = f"\nLIMIT {int(limit)}" if limit else ""
    return f'SELECT * FROM {catalog}.{schema}."{name}${metadata}"{suffix}'


def time_travel_sql(table: str, *, snapshot_id: int | None = None, as_of: str | None = None) -> str:
    """Read the table as it was.

    ``FOR VERSION AS OF`` pins a snapshot id; ``FOR TIMESTAMP AS OF`` pins a
    wall-clock moment. Exactly one, because "roughly when" is how you reconcile
    against the wrong state and conclude the pipeline is fine.
    """
    _validate_identifier(table, "table")
    if (snapshot_id is None) == (as_of is None):
        raise ValueError("pass exactly one of snapshot_id or as_of")
    if snapshot_id is not None:
        return f"SELECT * FROM {table} FOR VERSION AS OF {int(snapshot_id)}"
    return f"SELECT * FROM {table} FOR TIMESTAMP AS OF TIMESTAMP '{as_of}'"


def rollback_sql(catalog: str, schema: str, name: str, snapshot_id: int) -> str:
    """Undo a bad load without restoring from backup.

    This is the operational argument for Iceberg in a financial pipeline: a
    corrupt publish is one statement away from being reversed, and the reversal
    is itself a new snapshot rather than destructive.
    """
    for part, what in ((catalog, "catalog"), (schema, "schema"), (name, "table")):
        _validate_identifier(part, what)
    # Positional arguments, not named: `table` is a reserved word in Trino's
    # grammar, so `table => '...'` is a syntax error against a live coordinator.
    return (
        f"CALL {catalog}.system.rollback_to_snapshot("
        f"'{schema}', '{name}', {int(snapshot_id)})"
    )


def expire_snapshots_sql(table: str, retention: str = "7d") -> str:
    """Old snapshots keep the data files they reference alive; storage grows forever."""
    _validate_identifier(table, "table")
    return f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')"


def optimize_sql(table: str, file_size_threshold: str = "128MB", where: str | None = None) -> str:
    """Compaction. Streaming ingestion produces small files; small files kill scan performance.

    The optional predicate keeps compaction scoped to recent partitions instead
    of rewriting the whole table every night.
    """
    _validate_identifier(table, "table")
    stmt = f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')"
    return f"{stmt} WHERE {where}" if where else stmt


def remove_orphan_files_sql(table: str, retention: str = "7d") -> str:
    _validate_identifier(table, "table")
    return (
        f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')"
    )


def add_column_sql(table: str, column: str, sql_type: str, *, if_not_exists: bool = True) -> str:
    """Schema evolution without rewriting data — Iceberg tracks column ids, not positions."""
    _validate_identifier(table, "table")
    _validate_identifier(column, "column")
    if not sql_type.strip():
        raise ValueError("sql_type must not be blank")
    guard = "IF NOT EXISTS " if if_not_exists else ""
    return f"ALTER TABLE {table} ADD COLUMN {guard}{column} {sql_type}"


def set_partitioning_sql(table: str, partitioning: Sequence[str]) -> str:
    """Partition evolution: change the layout for *future* writes, leave history alone.

    Hive would require a rewrite and a backfill. This is the other reason to
    pick Iceberg for a table whose access pattern you have not finished learning.
    """
    _validate_identifier(table, "table")
    if not partitioning:
        raise ValueError("partitioning must not be empty")
    spec = ", ".join(f"'{p}'" for p in partitioning)
    return f"ALTER TABLE {table} SET PROPERTIES partitioning = ARRAY[{spec}]"


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: int
    committed_at: str
    operation: str
    summary: dict[str, str]


class IcebergMaintenance:
    """Executes the maintenance statements against a live Trino connection."""

    def __init__(self, runner: QueryRunner) -> None:
        self.runner = runner

    def snapshots(self, table: str, limit: int = 20) -> list[Snapshot]:
        rows = self.runner.query(metadata_table_sql(table, "snapshots", limit=limit))
        snapshots: list[Snapshot] = []
        for row in rows:
            committed_at, snapshot_id, _parent, operation, _manifest, summary = (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5] if len(row) > 5 else {},
            )
            snapshots.append(
                Snapshot(
                    snapshot_id=int(snapshot_id),
                    committed_at=str(committed_at),
                    operation=str(operation),
                    summary=dict(summary or {}),
                )
            )
        return snapshots

    def latest_snapshot_id(self, table: str) -> int | None:
        snaps = self.snapshots(table, limit=1)
        return snaps[0].snapshot_id if snaps else None

    def rollback(self, table: str, snapshot_id: int) -> None:
        catalog, schema, name = table.split(".")
        self.runner.query(rollback_sql(catalog, schema, name, snapshot_id))

    def optimize(self, table: str, **kwargs: str) -> None:
        self.runner.query(optimize_sql(table, **kwargs))

    def expire_snapshots(self, table: str, retention: str = "7d") -> None:
        self.runner.query(expire_snapshots_sql(table, retention))

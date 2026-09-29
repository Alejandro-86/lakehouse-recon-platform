"""SQL that produces control totals, and the Trino adapter that runs it.

Split from :mod:`lakehouse_recon.reconciliation` on purpose: the checks are pure
and testable without a cluster, and the only thing that needs a real Trino
connection is fetching the numbers. Swapping the warehouse means rewriting this
file and nothing else.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from lakehouse_recon.reconciliation import ControlTotal, Layer

_IDENTIFIER_OK = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.")


def _validate_identifier(value: str, what: str) -> str:
    """Reject anything that is not a plain dotted identifier.

    Table and column names cannot be bound as query parameters, so they are
    validated against an allowlist instead of interpolated hopefully. Values
    (dates, ids) always go through bound parameters — see :meth:`TrinoControlTotals`.
    """
    if not value or not set(value) <= _IDENTIFIER_OK:
        raise ValueError(f"unsafe {what}: {value!r}")
    if ".." in value or value.startswith(".") or value.endswith("."):
        raise ValueError(f"unsafe {what}: {value!r}")
    return value


def control_total_sql(
    table: str,
    *,
    partition_expr: str,
    amount_column: str = "signed_amount_minor",
    key_column: str = "event_id",
    where: str | None = None,
    dedupe_on: str | None = None,
    order_by: str = "ingested_at",
) -> str:
    """One row per partition: row count, summed minor units, distinct keys.

    ``partition_expr`` is an expression rather than a bare column so the same
    builder can reconcile at date grain (``date(occurred_at)``) or batch grain
    (``batch_id``) without a second code path.

    ``dedupe_on`` wraps the source in a first-arrival window before aggregating.
    Bronze keeps at-least-once duplicates on purpose, so comparing its raw sums
    to anything downstream would always break; deduplicating *inside the
    control total* compares like with like without mutating the bronze table.
    """
    _validate_identifier(table, "table")
    _validate_identifier(amount_column, "amount column")
    _validate_identifier(key_column, "key column")
    if not partition_expr.strip():
        raise ValueError("partition_expr must not be blank")

    predicate = f"\nWHERE {where}" if where else ""

    if dedupe_on:
        _validate_identifier(dedupe_on, "dedupe column")
        _validate_identifier(order_by, "order column")
        source = (
            "(\n"
            "    SELECT * FROM (\n"
            "        SELECT src.*, ROW_NUMBER() OVER ("
            f"PARTITION BY {dedupe_on} ORDER BY {order_by}) AS _rn\n"
            f"        FROM {table} src"
            f"{predicate}\n"
            "    ) ranked\n"
            "    WHERE _rn = 1\n"
            ")"
        )
        predicate = ""
    else:
        source = table

    return (
        "SELECT\n"
        f"    CAST({partition_expr} AS varchar) AS partition_key,\n"
        "    COUNT(*) AS row_count,\n"
        f"    COALESCE(SUM({amount_column}), 0) AS amount_minor_sum,\n"
        f"    COUNT(DISTINCT {key_column}) AS distinct_event_ids\n"
        f"FROM {source}"
        f"{predicate}\n"
        f"GROUP BY {partition_expr}\n"
        "ORDER BY 1"
    )


class QueryRunner(Protocol):
    """Minimal surface a warehouse client has to provide."""

    def query(self, sql: str, params: Sequence[object] | None = ...) -> list[tuple]:
        ...


class TrinoControlTotals:
    """Fetches control totals for a layer from Trino."""

    def __init__(self, runner: QueryRunner) -> None:
        self.runner = runner

    def fetch(
        self,
        table: str,
        layer: Layer,
        *,
        partition_expr: str = "date(occurred_at)",
        amount_column: str = "signed_amount_minor",
        key_column: str = "event_id",
        where: str | None = None,
        dedupe_on: str | None = None,
        params: Sequence[object] | None = None,
    ) -> list[ControlTotal]:
        sql = control_total_sql(
            table,
            partition_expr=partition_expr,
            amount_column=amount_column,
            key_column=key_column,
            where=where,
            dedupe_on=dedupe_on,
        )
        rows = self.runner.query(sql, params)
        return [
            ControlTotal(
                layer=layer,
                partition_key=str(row[0]),
                row_count=int(row[1]),
                amount_minor_sum=int(row[2]),
                distinct_event_ids=int(row[3]),
            )
            for row in rows
        ]


def distinct_key_sql(table: str, column: str) -> str:
    """Key set for a referential-integrity check."""
    _validate_identifier(table, "table")
    _validate_identifier(column, "column")
    return f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"

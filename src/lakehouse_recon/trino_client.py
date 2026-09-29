"""Thin Trino client.

Deliberately small. The interesting logic lives in modules that take a
``QueryRunner``, so tests never need a cluster and the warehouse stays swappable.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from lakehouse_recon.config import Settings

logger = logging.getLogger(__name__)


class TrinoRunner:
    """Executes SQL against Trino, reconnecting lazily."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._conn: Any | None = None

    def _connect(self) -> Any:
        if self._conn is None:
            import trino  # imported lazily so unit tests need no driver

            self._conn = trino.dbapi.connect(
                host=self.settings.trino_host,
                port=self.settings.trino_port,
                user=self.settings.trino_user,
                catalog=self.settings.trino_catalog,
                schema=self.settings.bronze_schema,
            )
        return self._conn

    def query(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]:
        cursor = self._connect().cursor()
        logger.debug("executing: %s", sql.replace("\n", " ")[:400])
        cursor.execute(sql, params) if params else cursor.execute(sql)
        try:
            return list(cursor.fetchall())
        except Exception:  # DDL/DML statements expose no result set
            return []

    def execute_many(self, statements: Sequence[str]) -> None:
        for statement in statements:
            if statement.strip():
                self.query(statement)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

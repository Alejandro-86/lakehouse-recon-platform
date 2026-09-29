from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from lakehouse_recon.config import Settings
from lakehouse_recon.events import Direction, TransactionEvent, TransactionType
from lakehouse_recon.reconciliation import ControlTotal, Layer


class FakeRunner:
    """Records SQL and replays canned results — no cluster needed."""

    def __init__(self, results: list[list[tuple]] | None = None) -> None:
        self.results = results or []
        self.executed: list[tuple[str, Sequence[object] | None]] = []

    def query(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]:
        self.executed.append((sql, params))
        return self.results.pop(0) if self.results else []

    @property
    def last_sql(self) -> str:
        return self.executed[-1][0]


@pytest.fixture
def settings() -> Settings:
    return Settings(trino_host="localhost", batch_size=2)


@pytest.fixture
def runner() -> FakeRunner:
    return FakeRunner()


@pytest.fixture
def event() -> TransactionEvent:
    return TransactionEvent(
        event_id="EVT-00000001",
        ledger_entry_id="LDG-00000001",
        account_id="ACC-00001",
        counterparty_account_id="ACC-00002",
        amount_minor=12_345,
        currency="gbp",
        direction=Direction.CREDIT,
        transaction_type=TransactionType.TRANSFER,
        occurred_at=datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
        source_system="core-ledger",
    )


def total(
    layer: Layer,
    key: str,
    rows: int,
    amount: int,
    distinct: int | None = None,
) -> ControlTotal:
    return ControlTotal(
        layer=layer,
        partition_key=key,
        row_count=rows,
        amount_minor_sum=amount,
        distinct_event_ids=rows if distinct is None else distinct,
    )

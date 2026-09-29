"""Deterministic synthetic transaction stream, with faults you can turn on.

A reconciliation platform demoed on clean data proves nothing. This generator
emits a seeded, reproducible stream and can inject the two failure modes a real
ingestion path produces constantly: **at-least-once duplicates** and
**late-arriving events** that land in a partition after it was already
reconciled.

It also emits a :class:`GenerationManifest` — the source system's own claim
about what it sent. Reconciling bronze against that manifest catches data lost
in transit, which a bronze-to-silver check by definition cannot see.
"""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from lakehouse_recon.events import Direction, TransactionEvent, TransactionType
from lakehouse_recon.reconciliation import ControlTotal, Layer

CURRENCIES: tuple[str, ...] = ("GBP", "EUR", "USD", "PLN")
SOURCE_SYSTEMS: tuple[str, ...] = ("core-ledger", "card-switch", "fx-engine")


@dataclass
class GenerationManifest:
    """What the source system says it emitted, at reconciliation grain."""

    unique_events: int = 0
    emitted_messages: int = 0
    duplicates: int = 0
    late_arrivals: int = 0
    row_count_by_date: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    amount_by_date: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record(self, event: TransactionEvent, *, is_duplicate: bool) -> None:
        self.emitted_messages += 1
        if is_duplicate:
            self.duplicates += 1
            return
        key = event.occurred_at.date().isoformat()
        self.unique_events += 1
        self.row_count_by_date[key] += 1
        self.amount_by_date[key] += event.signed_amount_minor

    def control_totals(self) -> list[ControlTotal]:
        """Source-of-truth totals to reconcile the lake against."""
        return [
            ControlTotal(
                layer=Layer.SOURCE,
                partition_key=key,
                row_count=count,
                amount_minor_sum=self.amount_by_date[key],
                distinct_event_ids=count,
            )
            for key, count in sorted(self.row_count_by_date.items())
        ]


class TransactionGenerator:
    """Seeded generator. Same seed, same stream — so a failing demo is reproducible."""

    def __init__(
        self,
        *,
        seed: int = 42,
        accounts: int = 40,
        days: int = 5,
        start: datetime | None = None,
        duplicate_rate: float = 0.03,
        late_rate: float = 0.02,
        currencies: Sequence[str] = CURRENCIES,
    ) -> None:
        if not 0.0 <= duplicate_rate < 1.0:
            raise ValueError("duplicate_rate must be in [0, 1)")
        if not 0.0 <= late_rate < 1.0:
            raise ValueError("late_rate must be in [0, 1)")
        if accounts < 1 or days < 1:
            raise ValueError("accounts and days must be positive")
        self.rng = random.Random(seed)
        self.accounts = [f"ACC-{i:05d}" for i in range(accounts)]
        self.days = days
        self.start = start or datetime(2026, 9, 1, tzinfo=UTC)
        self.duplicate_rate = duplicate_rate
        self.late_rate = late_rate
        self.currencies = tuple(currencies)
        self.manifest = GenerationManifest()

    def _one(self, index: int) -> TransactionEvent:
        day_offset = self.rng.randrange(self.days)
        occurred = self.start + timedelta(
            days=day_offset,
            seconds=self.rng.randrange(86_400),
        )
        if self.rng.random() < self.late_rate:
            # A late arrival belongs to an earlier business date than the one it
            # lands in — the classic cause of a partition that reconciled clean
            # yesterday and does not today.
            occurred -= timedelta(days=1)
            self.manifest.late_arrivals += 1

        direction = self.rng.choice(list(Direction))
        return TransactionEvent(
            event_id=f"EVT-{index:08d}",
            ledger_entry_id=f"LDG-{index:08d}",
            account_id=self.rng.choice(self.accounts),
            counterparty_account_id=self.rng.choice(self.accounts),
            amount_minor=self.rng.randrange(50, 5_000_00),
            currency=self.rng.choice(self.currencies),
            direction=direction,
            transaction_type=self.rng.choice(list(TransactionType)),
            occurred_at=occurred,
            source_system=self.rng.choice(SOURCE_SYSTEMS),
        )

    def stream(self, count: int) -> Iterator[TransactionEvent]:
        """Yield ``count`` unique events, interleaved with duplicate re-deliveries."""
        if count < 1:
            raise ValueError("count must be positive")
        emitted: list[TransactionEvent] = []
        for index in range(count):
            event = self._one(index)
            emitted.append(event)
            self.manifest.record(event, is_duplicate=False)
            yield event

            if emitted and self.rng.random() < self.duplicate_rate:
                repeat = self.rng.choice(emitted)
                self.manifest.record(repeat, is_duplicate=True)
                yield repeat

    def generate(self, count: int) -> list[TransactionEvent]:
        return list(self.stream(count))

"""Kafka to bronze.

Design decisions worth saying out loud in a review:

* **Bronze is append-only and keeps duplicates.** At-least-once delivery means
  duplicates arrive; suppressing them at the edge would destroy the evidence
  that lets the deduplication check prove silver did its job.
* **Unparseable messages go to a dead-letter list with the reason attached**,
  never silently dropped. A pipeline that drops what it cannot read reports
  100% success while losing money.
* **Batched multi-row INSERT via Trino.** Honest trade-off: this is the right
  call for a demo and for modest volumes, but at Wise-scale throughput you would
  write Iceberg directly from the stream processor (Flink or Spark) and leave
  Trino for reads. Using one engine here keeps the project runnable on a laptop;
  the seam is :class:`BronzeLoader`, so swapping the writer changes one class.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import ValidationError

from lakehouse_recon.config import Settings
from lakehouse_recon.control_totals import QueryRunner, _validate_identifier
from lakehouse_recon.events import BRONZE_COLUMNS, TransactionEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeadLetter:
    raw: str
    reason: str


@dataclass
class LoadResult:
    batches: int = 0
    rows_written: int = 0
    dead_letters: list[DeadLetter] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.dead_letters is None:
            self.dead_letters = []

    @property
    def dead_letter_count(self) -> int:
        return len(self.dead_letters)


def parse_message(raw: str | bytes) -> TransactionEvent | DeadLetter:
    """Parse one Kafka message. Never raises — a bad message is data, not a crash."""
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return DeadLetter(raw=text, reason=f"invalid json: {exc.msg}")
    if not isinstance(payload, dict):
        return DeadLetter(raw=text, reason="payload is not a json object")
    try:
        return TransactionEvent.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(p) for p in first.get("loc", ())) or "payload"
        return DeadLetter(raw=text, reason=f"schema violation at {location}: {first.get('msg')}")


def chunked(items: Iterable[TransactionEvent], size: int) -> Iterator[list[TransactionEvent]]:
    if size < 1:
        raise ValueError("size must be positive")
    batch: list[TransactionEvent] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_sql(table: str, columns: Sequence[str], row_count: int) -> str:
    """Multi-row parameterised INSERT. Values are bound, never interpolated."""
    _validate_identifier(table, "table")
    if not columns:
        raise ValueError("columns must not be empty")
    if row_count < 1:
        raise ValueError("row_count must be positive")
    for column in columns:
        _validate_identifier(column, "column")
    placeholder = "(" + ", ".join("?" for _ in columns) + ")"
    values = ", ".join(placeholder for _ in range(row_count))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values}"


class BronzeLoader:
    """Lands validated events in the bronze Iceberg table, batch by batch."""

    def __init__(self, runner: QueryRunner, settings: Settings) -> None:
        self.runner = runner
        self.settings = settings

    def load(
        self,
        events: Iterable[TransactionEvent],
        *,
        batch_id: str | None = None,
        ingested_at: datetime | None = None,
    ) -> LoadResult:
        result = LoadResult()
        stamp = ingested_at or datetime.now(UTC)
        table = self.settings.bronze_transactions

        for batch in chunked(events, self.settings.batch_size):
            this_batch = batch_id or f"batch-{uuid.uuid4().hex[:12]}"
            rows = [event.to_bronze_row(stamp, this_batch) for event in batch]
            params: list[object] = []
            for row in rows:
                params.extend(row[column] for column in BRONZE_COLUMNS)
            self.runner.query(insert_sql(table, BRONZE_COLUMNS, len(rows)), params)
            result.batches += 1
            result.rows_written += len(rows)
            logger.info("wrote %d rows to %s (batch %s)", len(rows), table, this_batch)
        return result

    def load_raw(
        self, messages: Iterable[str | bytes], *, batch_id: str | None = None
    ) -> LoadResult:
        """Parse then load, routing anything unparseable to dead letters."""
        parsed: list[TransactionEvent] = []
        dead: list[DeadLetter] = []
        for message in messages:
            outcome = parse_message(message)
            (dead if isinstance(outcome, DeadLetter) else parsed).append(outcome)  # type: ignore[arg-type]

        result = self.load(parsed, batch_id=batch_id)
        result.dead_letters = dead
        if dead:
            logger.warning("%d message(s) dead-lettered during load", len(dead))
        return result

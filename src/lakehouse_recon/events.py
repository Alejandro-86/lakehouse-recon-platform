"""Typed financial events.

Two deliberate modelling decisions worth defending in review:

1. **Amounts are integer minor units** (pence/cents), never floats. Float money
   makes reconciliation approximate, and an approximate financial control is not
   a control. Integers let the bronze-to-gold amount check run at exactly zero
   tolerance.
2. **Every event carries its own idempotency key** (``event_id``). Kafka gives
   at-least-once delivery, so duplicates are a certainty, not an edge case — the
   silver layer deduplicates on this key rather than pretending it cannot happen.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class Direction(StrEnum):
    DEBIT = "debit"
    CREDIT = "credit"


class TransactionType(StrEnum):
    TRANSFER = "transfer"
    CARD_PAYMENT = "card_payment"
    FX_CONVERSION = "fx_conversion"
    FEE = "fee"
    REFUND = "refund"


class TransactionEvent(BaseModel):
    """A single ledger-affecting event as it arrives from the source system."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=8, description="Idempotency key; unique per logical event")
    ledger_entry_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    counterparty_account_id: str | None = None
    amount_minor: int = Field(description="Signed amount in minor units; never zero")
    currency: str
    direction: Direction
    transaction_type: TransactionType
    occurred_at: datetime
    source_system: str = Field(min_length=1)

    @field_validator("currency")
    @classmethod
    def _currency_is_iso(cls, v: str) -> str:
        v = v.strip().upper()
        if not _CURRENCY_RE.match(v):
            raise ValueError(f"currency must be a 3-letter ISO code, got {v!r}")
        return v

    @field_validator("amount_minor")
    @classmethod
    def _amount_non_zero(cls, v: int) -> int:
        if v == 0:
            raise ValueError("amount_minor must not be zero")
        return v

    @field_validator("occurred_at")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v.astimezone(UTC)

    @property
    def signed_amount_minor(self) -> int:
        """Debits are negative on the account, credits positive.

        The source system sends magnitude plus a direction flag; the ledger wants
        one signed number. Normalising here means the silver layer never has to
        remember the convention.
        """
        magnitude = abs(self.amount_minor)
        return -magnitude if self.direction is Direction.DEBIT else magnitude

    def amount_major(self, exponent: int = 2) -> Decimal:
        """Minor units rendered as a major-unit Decimal, for presentation only."""
        return Decimal(self.signed_amount_minor).scaleb(-exponent)

    def to_bronze_row(self, ingested_at: datetime, batch_id: str) -> dict[str, object]:
        """Flatten to the bronze table's column order, with ingestion lineage attached."""
        return {
            "event_id": self.event_id,
            "ledger_entry_id": self.ledger_entry_id,
            "account_id": self.account_id,
            "counterparty_account_id": self.counterparty_account_id,
            "amount_minor": self.amount_minor,
            "signed_amount_minor": self.signed_amount_minor,
            "currency": self.currency,
            "direction": str(self.direction),
            "transaction_type": str(self.transaction_type),
            "occurred_at": self.occurred_at,
            "source_system": self.source_system,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
        }


BRONZE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "ledger_entry_id",
    "account_id",
    "counterparty_account_id",
    "amount_minor",
    "signed_amount_minor",
    "currency",
    "direction",
    "transaction_type",
    "occurred_at",
    "source_system",
    "ingested_at",
    "batch_id",
)

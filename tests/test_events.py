from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from lakehouse_recon.events import BRONZE_COLUMNS, Direction, TransactionEvent


def test_currency_is_normalised_to_upper(event):
    assert event.currency == "GBP"


def test_currency_must_be_three_letters(event):
    with pytest.raises(ValidationError, match="3-letter ISO code"):
        TransactionEvent.model_validate({**event.model_dump(), "currency": "POUNDS"})


def test_zero_amount_is_rejected(event):
    with pytest.raises(ValidationError, match="must not be zero"):
        TransactionEvent.model_validate({**event.model_dump(), "amount_minor": 0})


def test_naive_timestamps_are_coerced_to_utc(event):
    naive = TransactionEvent.model_validate(
        {**event.model_dump(), "occurred_at": datetime(2026, 9, 1, 12, 0)}
    )
    assert naive.occurred_at.tzinfo is not None
    assert naive.occurred_at == datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def test_debit_is_signed_negative(event):
    debit = TransactionEvent.model_validate(
        {**event.model_dump(), "direction": Direction.DEBIT, "amount_minor": 500}
    )
    assert debit.signed_amount_minor == -500


def test_credit_is_signed_positive(event):
    assert event.signed_amount_minor == 12_345


def test_sign_normalisation_ignores_incoming_sign(event):
    """A source sending a negative magnitude on a debit must not flip to positive."""
    debit = TransactionEvent.model_validate(
        {**event.model_dump(), "direction": Direction.DEBIT, "amount_minor": -500}
    )
    assert debit.signed_amount_minor == -500


def test_amount_major_is_exact_decimal(event):
    assert event.amount_major() == Decimal("123.45")
    assert isinstance(event.amount_major(), Decimal)


def test_bronze_row_matches_declared_column_order(event):
    row = event.to_bronze_row(datetime.now(UTC), "batch-1")
    assert tuple(row) == BRONZE_COLUMNS


def test_events_are_immutable(event):
    with pytest.raises(ValidationError):
        event.amount_minor = 99

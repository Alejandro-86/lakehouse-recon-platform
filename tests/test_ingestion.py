from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from lakehouse_recon.events import BRONZE_COLUMNS, TransactionEvent
from lakehouse_recon.ingestion import (
    BronzeLoader,
    DeadLetter,
    chunked,
    insert_sql,
    parse_message,
)

from .conftest import FakeRunner


def test_valid_json_parses_into_an_event(event):
    parsed = parse_message(json.dumps(event.model_dump(mode="json")))
    assert isinstance(parsed, TransactionEvent)
    assert parsed.event_id == event.event_id


def test_bytes_payloads_are_decoded(event):
    parsed = parse_message(json.dumps(event.model_dump(mode="json")).encode())
    assert isinstance(parsed, TransactionEvent)


def test_malformed_json_is_dead_lettered_not_raised():
    result = parse_message("{not json")
    assert isinstance(result, DeadLetter)
    assert "invalid json" in result.reason


def test_non_object_payload_is_dead_lettered():
    result = parse_message("[1, 2, 3]")
    assert isinstance(result, DeadLetter)
    assert "not a json object" in result.reason


def test_schema_violation_names_the_offending_field(event):
    payload = event.model_dump(mode="json") | {"currency": "POUNDS"}
    result = parse_message(json.dumps(payload))
    assert isinstance(result, DeadLetter)
    assert "currency" in result.reason


def test_undecodable_bytes_do_not_crash_the_loader():
    assert isinstance(parse_message(b"\xff\xfe\x00bad"), DeadLetter)


def test_chunked_splits_evenly_and_keeps_the_remainder():
    assert [len(c) for c in chunked(list(range(7)), 3)] == [3, 3, 1]


def test_chunked_rejects_a_zero_size():
    with pytest.raises(ValueError, match="must be positive"):
        list(chunked([1], 0))


def test_insert_sql_binds_every_value():
    sql = insert_sql("iceberg.bronze.transactions_raw", ("a", "b"), 3)
    assert sql.count("(?, ?)") == 3
    assert "VALUES (?, ?), (?, ?), (?, ?)" in sql


def test_insert_sql_validates_identifiers():
    with pytest.raises(ValueError, match="unsafe"):
        insert_sql("iceberg.bronze.t", ("a; DROP TABLE x",), 1)


def test_insert_sql_requires_rows_and_columns():
    with pytest.raises(ValueError):
        insert_sql("iceberg.bronze.t", (), 1)
    with pytest.raises(ValueError):
        insert_sql("iceberg.bronze.t", ("a",), 0)


def test_loader_batches_according_to_settings(settings, event):
    runner = FakeRunner()
    events = [
        event.model_copy(update={"event_id": f"EVT-{i:08d}"}) for i in range(5)
    ]
    result = BronzeLoader(runner, settings).load(events)
    assert settings.batch_size == 2
    assert result.batches == 3
    assert result.rows_written == 5


def test_loader_binds_parameters_in_declared_column_order(settings, event):
    runner = FakeRunner()
    BronzeLoader(runner, settings).load([event], ingested_at=datetime.now(UTC))
    _, params = runner.executed[-1]
    assert len(params) == len(BRONZE_COLUMNS)
    assert params[0] == event.event_id
    assert params[BRONZE_COLUMNS.index("currency")] == "GBP"


def test_bad_messages_are_quarantined_while_good_ones_still_load(settings, event):
    runner = FakeRunner()
    messages = [json.dumps(event.model_dump(mode="json")), "{broken", "[]"]
    result = BronzeLoader(runner, settings).load_raw(messages)
    assert result.rows_written == 1
    assert result.dead_letter_count == 2


def test_an_all_bad_batch_writes_nothing_and_reports_everything(settings):
    runner = FakeRunner()
    result = BronzeLoader(runner, settings).load_raw(["{bad", "{worse"])
    assert result.rows_written == 0
    assert result.batches == 0
    assert result.dead_letter_count == 2
    assert runner.executed == []

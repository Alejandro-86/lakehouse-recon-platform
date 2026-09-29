from __future__ import annotations

import pytest

from lakehouse_recon.generator import TransactionGenerator
from lakehouse_recon.reconciliation import Layer


def test_same_seed_produces_identical_stream():
    a = TransactionGenerator(seed=7).generate(50)
    b = TransactionGenerator(seed=7).generate(50)
    assert [e.event_id for e in a] == [e.event_id for e in b]
    assert [e.signed_amount_minor for e in a] == [e.signed_amount_minor for e in b]


def test_different_seeds_diverge():
    a = TransactionGenerator(seed=1).generate(50)
    b = TransactionGenerator(seed=2).generate(50)
    assert [e.signed_amount_minor for e in a] != [e.signed_amount_minor for e in b]


def test_duplicates_are_emitted_but_not_double_counted():
    generator = TransactionGenerator(seed=3, duplicate_rate=0.5)
    events = generator.generate(200)
    manifest = generator.manifest
    assert manifest.duplicates > 0
    assert manifest.emitted_messages == len(events)
    assert manifest.unique_events == len({e.event_id for e in events})
    assert manifest.emitted_messages > manifest.unique_events


def test_no_duplicates_when_rate_is_zero():
    generator = TransactionGenerator(seed=3, duplicate_rate=0.0)
    events = generator.generate(100)
    assert generator.manifest.duplicates == 0
    assert len({e.event_id for e in events}) == 100


def test_manifest_control_totals_are_self_consistent():
    generator = TransactionGenerator(seed=11, duplicate_rate=0.2)
    generator.generate(300)
    totals = generator.manifest.control_totals()
    assert all(t.layer is Layer.SOURCE for t in totals)
    assert sum(t.row_count for t in totals) == generator.manifest.unique_events
    # Every source partition is fully deduplicated by definition.
    assert all(t.row_count == t.distinct_event_ids for t in totals)


def test_manifest_amounts_match_the_events_emitted():
    generator = TransactionGenerator(seed=5, duplicate_rate=0.0)
    events = generator.generate(120)
    totals = generator.manifest.control_totals()
    assert sum(t.amount_minor_sum for t in totals) == sum(e.signed_amount_minor for e in events)


def test_late_arrivals_are_counted():
    generator = TransactionGenerator(seed=9, late_rate=0.9)
    generator.generate(100)
    assert generator.manifest.late_arrivals > 0


@pytest.mark.parametrize("bad", [{"duplicate_rate": 1.0}, {"late_rate": -0.1}, {"accounts": 0}])
def test_invalid_configuration_is_rejected(bad):
    with pytest.raises(ValueError):
        TransactionGenerator(**bad)


def test_count_must_be_positive():
    with pytest.raises(ValueError):
        TransactionGenerator().generate(0)

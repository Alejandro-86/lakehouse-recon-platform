from __future__ import annotations

import pytest

from lakehouse_recon.config import Settings
from lakehouse_recon.generator import TransactionGenerator
from lakehouse_recon.pipeline import run_reconciliation

from .conftest import FakeRunner


def _results(bronze, silver, gold, fact_keys=(("ACC-1",),), dim_keys=(("ACC-1",),)):
    """Canned Trino responses in the order run_reconciliation asks for them."""
    return [list(bronze), list(silver), list(gold), list(fact_keys), list(dim_keys)]


def test_clean_medallion_passes_every_control():
    rows = [("2026-09-01", 100, 500_000, 100)]
    runner = FakeRunner(_results(rows, rows, rows))
    report = run_reconciliation(runner, Settings())
    assert report.passed
    assert {r.check for r in report.results} == {
        "row_count",
        "amount_sum",
        "deduplication",
        "referential_integrity",
    }


def test_silver_to_gold_amount_loss_is_caught():
    bronze = [("2026-09-01", 100, 500_000, 100)]
    silver = [("2026-09-01", 100, 500_000, 100)]
    gold = [("2026-09-01", 90, 450_000, 90)]  # a WHERE clause someone added
    report = run_reconciliation(FakeRunner(_results(bronze, silver, gold)), Settings())
    assert not report.passed
    deltas = {b.check: b.delta for b in report.breaks}
    assert deltas["row_count"] == -10
    assert deltas["amount_sum"] == -50_000


def test_surviving_duplicates_in_silver_are_caught():
    bronze = [("2026-09-01", 100, 500_000, 100)]
    silver = [("2026-09-01", 105, 500_000, 100)]
    gold = [("2026-09-01", 105, 500_000, 100)]
    report = run_reconciliation(FakeRunner(_results(bronze, silver, gold)), Settings())
    assert not report.passed
    assert any(b.check == "deduplication" for b in report.breaks)


def test_orphaned_account_key_is_caught():
    rows = [("2026-09-01", 1, 100, 1)]
    runner = FakeRunner(_results(rows, rows, rows, fact_keys=[("ACC-9",)], dim_keys=[("ACC-1",)]))
    report = run_reconciliation(runner, Settings())
    assert not report.passed
    assert any(b.check == "referential_integrity" for b in report.breaks)


def test_bronze_control_totals_are_deduplicated_before_comparison():
    rows = [("2026-09-01", 1, 100, 1)]
    runner = FakeRunner(_results(rows, rows, rows))
    run_reconciliation(runner, Settings())
    bronze_sql = runner.executed[0][0]
    assert "ROW_NUMBER() OVER (PARTITION BY event_id" in bronze_sql


def test_manifest_comparison_detects_data_lost_before_the_lake():
    """Bronze agrees with silver and gold, but the source sent more than arrived."""
    generator = TransactionGenerator(seed=4, duplicate_rate=0.0, late_rate=0.0, days=1)
    generator.generate(50)
    source_total = generator.manifest.control_totals()[0]

    short = [(source_total.partition_key, 40, source_total.amount_minor_sum, 40)]
    runner = FakeRunner(_results(short, short, short))
    report = run_reconciliation(runner, Settings(), manifest=generator.manifest)

    assert not report.passed
    assert any(b.delta == -10 for b in report.breaks)


def test_referential_check_can_be_skipped():
    rows = [("2026-09-01", 1, 100, 1)]
    runner = FakeRunner([list(rows), list(rows), list(rows)])
    report = run_reconciliation(runner, Settings(), check_referential_integrity=False)
    assert report.passed
    assert not any(r.check == "referential_integrity" for r in report.results)


@pytest.mark.integration
def test_end_to_end_against_a_live_stack():
    """Requires `make up`; excluded from CI by the `integration` marker."""
    from lakehouse_recon.ddl import bootstrap_statements
    from lakehouse_recon.ingestion import BronzeLoader
    from lakehouse_recon.trino_client import TrinoRunner

    settings = Settings()
    runner = TrinoRunner(settings)
    runner.execute_many(bootstrap_statements(settings))

    generator = TransactionGenerator(seed=99, days=2)
    events = generator.generate(200)
    BronzeLoader(runner, settings).load(events)

    report = run_reconciliation(
        runner, settings, manifest=generator.manifest, check_referential_integrity=False
    )
    assert report.passed, report.summary()

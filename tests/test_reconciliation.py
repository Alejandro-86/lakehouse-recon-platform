from __future__ import annotations

import pytest

from lakehouse_recon.reconciliation import (
    AmountSumCheck,
    ControlTotal,
    DeduplicationCheck,
    Layer,
    ReconciliationFailure,
    ReferentialIntegrityCheck,
    RowCountCheck,
    Severity,
    reconcile,
)

from .conftest import total

# --- ControlTotal invariants ---------------------------------------------------


def test_distinct_ids_cannot_exceed_rows():
    with pytest.raises(ValueError, match="cannot exceed"):
        ControlTotal(
            Layer.BRONZE, "2026-09-01", row_count=5, amount_minor_sum=0, distinct_event_ids=6
        )


def test_negative_row_count_is_rejected():
    with pytest.raises(ValueError, match="must not be negative"):
        ControlTotal(
            Layer.BRONZE, "2026-09-01", row_count=-1, amount_minor_sum=0, distinct_event_ids=0
        )


def test_duplicate_partitions_in_one_layer_are_rejected():
    dupes = [total(Layer.SILVER, "2026-09-01", 1, 1), total(Layer.SILVER, "2026-09-01", 2, 2)]
    with pytest.raises(ValueError, match="duplicate control total"):
        RowCountCheck().run(dupes, [])


# --- RowCountCheck -------------------------------------------------------------


def test_matching_row_counts_pass():
    src = [total(Layer.BRONZE, "2026-09-01", 100, 5_000)]
    tgt = [total(Layer.SILVER, "2026-09-01", 100, 5_000)]
    assert RowCountCheck().run(src, tgt).passed


def test_row_count_mismatch_produces_a_quantified_break():
    src = [total(Layer.BRONZE, "2026-09-01", 100, 5_000)]
    tgt = [total(Layer.SILVER, "2026-09-01", 97, 4_900)]
    result = RowCountCheck().run(src, tgt)
    assert not result.passed
    assert result.breaks[0].delta == -3
    assert result.breaks[0].expected == 100


def test_partition_missing_from_target_is_caught_not_skipped():
    """The single most important case: a partition that never loaded at all."""
    src = [total(Layer.BRONZE, "2026-09-01", 10, 1), total(Layer.BRONZE, "2026-09-02", 10, 1)]
    tgt = [total(Layer.SILVER, "2026-09-01", 10, 1)]
    result = RowCountCheck().run(src, tgt)
    assert not result.passed
    assert len(result.breaks) == 1
    assert result.breaks[0].partition_key == "2026-09-02"
    assert "missing from target" in result.breaks[0].note
    assert result.partitions_compared == 2


def test_unexpected_partition_in_target_is_caught():
    src = [total(Layer.BRONZE, "2026-09-01", 10, 1)]
    tgt = [total(Layer.SILVER, "2026-09-01", 10, 1), total(Layer.SILVER, "2026-09-09", 4, 1)]
    result = RowCountCheck().run(src, tgt)
    assert not result.passed
    assert "absent from source" in result.breaks[0].note


def test_distinct_source_mode_tolerates_expected_deduplication():
    """Bronze keeps duplicates; silver should equal bronze's distinct count, not its rows."""
    bronze = [total(Layer.BRONZE, "2026-09-01", rows=110, amount=5_000, distinct=100)]
    silver = [total(Layer.SILVER, "2026-09-01", rows=100, amount=5_000)]
    assert not RowCountCheck().run(bronze, silver).passed
    assert RowCountCheck(expect_distinct_source=True).run(bronze, silver).passed


def test_distinct_target_mode_compares_a_manifest_to_duplicate_bearing_bronze():
    source = [total(Layer.SOURCE, "2026-09-01", 100, 5_000)]
    bronze = [total(Layer.BRONZE, "2026-09-01", rows=112, amount=5_600, distinct=100)]
    assert RowCountCheck(expect_distinct_target=True).run(source, bronze).passed


# --- AmountSumCheck ------------------------------------------------------------


def test_amount_sums_must_match_exactly_by_default():
    src = [total(Layer.SILVER, "2026-09-01", 10, 100_000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 10, 99_999)]
    result = AmountSumCheck().run(src, tgt)
    assert not result.passed
    assert result.breaks[0].delta == -1


def test_absolute_tolerance_is_honoured():
    src = [total(Layer.SILVER, "2026-09-01", 10, 100_000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 10, 99_998)]
    assert AmountSumCheck(tolerance_minor=2).run(src, tgt).passed
    assert not AmountSumCheck(tolerance_minor=1).run(src, tgt).passed


def test_relative_tolerance_scales_with_the_amount():
    src = [total(Layer.SILVER, "2026-09-01", 10, 1_000_000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 10, 1_000_500)]
    assert AmountSumCheck(relative_tolerance=0.001).run(src, tgt).passed
    assert not AmountSumCheck(relative_tolerance=0.0001).run(src, tgt).passed


def test_allowed_drift_takes_the_larger_of_the_two_tolerances():
    check = AmountSumCheck(tolerance_minor=50, relative_tolerance=0.001)
    assert check.allowed_drift(10_000) == 50
    assert check.allowed_drift(1_000_000) == 1_000


def test_negative_tolerance_is_rejected():
    with pytest.raises(ValueError, match="must not be negative"):
        AmountSumCheck(tolerance_minor=-1)


def test_relative_tolerance_is_capped():
    with pytest.raises(ValueError, match="between 0 and 0.01"):
        AmountSumCheck(relative_tolerance=0.5)


def test_offsetting_errors_do_not_cancel_out_across_partitions():
    """Whole-table totals would agree here. Partition grain is what exposes it."""
    src = [total(Layer.SILVER, "2026-09-01", 5, 1_000), total(Layer.SILVER, "2026-09-02", 5, 1_000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 5, 1_500), total(Layer.GOLD, "2026-09-02", 5, 500)]
    assert sum(t.amount_minor_sum for t in src) == sum(t.amount_minor_sum for t in tgt)
    assert len(AmountSumCheck().run(src, tgt).breaks) == 2


# --- DeduplicationCheck --------------------------------------------------------


def test_deduplication_check_passes_when_rows_equal_distinct_ids():
    silver = [total(Layer.SILVER, "2026-09-01", 100, 1_000)]
    assert DeduplicationCheck().run(silver, silver).passed


def test_deduplication_check_fails_when_duplicates_survive():
    silver = [total(Layer.SILVER, "2026-09-01", rows=105, amount=1_000, distinct=100)]
    result = DeduplicationCheck().run(silver, silver)
    assert not result.passed
    assert result.breaks[0].delta == 5


# --- ReferentialIntegrityCheck -------------------------------------------------


def test_orphaned_fact_keys_are_reported_one_per_key():
    check = ReferentialIntegrityCheck(
        dimension="dim_account",
        fact_keys=["A", "B", "C"],
        dimension_keys=["A"],
    )
    result = check.run()
    assert not result.passed
    assert {b.partition_key for b in result.breaks} == {"dim_account:B", "dim_account:C"}


def test_referential_integrity_passes_when_every_key_resolves():
    check = ReferentialIntegrityCheck(
        dimension="dim_account", fact_keys=["A"], dimension_keys=["A", "B"]
    )
    assert check.run().passed


def test_referential_integrity_can_be_downgraded_to_a_warning():
    check = ReferentialIntegrityCheck(
        dimension="dim_account",
        fact_keys=["A"],
        dimension_keys=[],
        severity=Severity.WARNING,
    )
    result = check.run()
    assert result.passed  # warnings do not fail the run
    assert result.breaks[0].severity is Severity.WARNING


# --- Report --------------------------------------------------------------------


def test_report_raises_on_error_breaks():
    src = [total(Layer.BRONZE, "2026-09-01", 10, 100)]
    tgt = [total(Layer.SILVER, "2026-09-01", 9, 90)]
    report = reconcile([RowCountCheck(), AmountSumCheck()], src, tgt)
    assert not report.passed
    with pytest.raises(ReconciliationFailure) as excinfo:
        report.raise_for_status()
    assert "FAILED" in str(excinfo.value)
    assert excinfo.value.report is report


def test_report_is_silent_when_everything_agrees():
    src = [total(Layer.BRONZE, "2026-09-01", 10, 100)]
    tgt = [total(Layer.SILVER, "2026-09-01", 10, 100)]
    report = reconcile([RowCountCheck(), AmountSumCheck()], src, tgt)
    report.raise_for_status()
    assert report.passed
    assert "PASSED" in report.summary()


def test_warnings_do_not_fail_the_run_but_are_reported():
    report = reconcile(
        [
            ReferentialIntegrityCheck(
                dimension="dim_account",
                fact_keys=["X"],
                dimension_keys=[],
                severity=Severity.WARNING,
            )
        ]
    )
    report.raise_for_status()
    assert report.passed
    assert len(report.warnings) == 1
    assert "warning" in report.summary()


def test_report_serialises_every_break_field():
    src = [total(Layer.BRONZE, "2026-09-01", 10, 100)]
    tgt = [total(Layer.SILVER, "2026-09-01", 8, 100)]
    payload = reconcile([RowCountCheck()], src, tgt).to_dict()
    assert payload["passed"] is False
    assert payload["breaks"][0]["delta"] == -2
    assert payload["breaks"][0]["source_layer"] == "bronze"
    assert payload["breaks"][0]["target_layer"] == "silver"
    assert payload["checks"][0]["partitions_compared"] == 1


def test_break_description_is_human_readable():
    src = [total(Layer.SILVER, "2026-09-01", 10, 1_000)]
    tgt = [total(Layer.GOLD, "2026-09-01", 10, 900)]
    described = reconcile([AmountSumCheck()], src, tgt).breaks[0].describe()
    assert "amount_sum" in described
    assert "2026-09-01" in described
    assert "-100" in described

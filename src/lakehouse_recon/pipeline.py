"""The reconciliation run: which layers get compared, with which checks.

This is the file to read first if you want to know what the platform actually
guarantees. Four comparisons, in the order data moves:

1. **source → bronze** — did everything the source system claims it sent
   actually land? Only a manifest-based check can see loss in transit; every
   layer-to-layer check downstream is blind to it.
2. **bronze → silver** — did cleaning and deduplication preserve the money?
   Bronze is deduplicated *inside the control total* so duplicates, which are
   expected, do not masquerade as breaks.
3. **silver → gold** — did the star schema keep the totals the ledger had?
4. **within gold** — do fact rows resolve to dimension rows? An orphaned key
   produces an under-reported number that still looks plausible, which is the
   worst kind of wrong.
"""

from __future__ import annotations

import logging

from lakehouse_recon.config import Settings
from lakehouse_recon.control_totals import QueryRunner, TrinoControlTotals, distinct_key_sql
from lakehouse_recon.generator import GenerationManifest
from lakehouse_recon.reconciliation import (
    AmountSumCheck,
    DeduplicationCheck,
    Layer,
    ReconciliationReport,
    ReferentialIntegrityCheck,
    RowCountCheck,
    reconcile,
)

logger = logging.getLogger(__name__)

DATE_GRAIN = "date(occurred_at)"


def run_reconciliation(
    runner: QueryRunner,
    settings: Settings,
    *,
    manifest: GenerationManifest | None = None,
    check_referential_integrity: bool = True,
) -> ReconciliationReport:
    """Reconcile the whole medallion and return one report."""
    totals = TrinoControlTotals(runner)
    amount_check = AmountSumCheck(
        tolerance_minor=settings.amount_tolerance_minor,
        relative_tolerance=settings.relative_tolerance,
    )

    bronze_deduped = totals.fetch(
        settings.bronze_transactions,
        Layer.BRONZE,
        partition_expr=DATE_GRAIN,
        dedupe_on="event_id",
    )
    silver = totals.fetch(settings.silver_transactions, Layer.SILVER, partition_expr=DATE_GRAIN)
    gold = totals.fetch(
        settings.gold_fact,
        Layer.GOLD,
        partition_expr=DATE_GRAIN,
        key_column="event_id",
    )

    report = ReconciliationReport()

    if manifest is not None:
        # The deduplicated bronze totals are the right comparand on both counts:
        # after the first-arrival window, distinct ids equal rows, and the summed
        # amounts no longer double-count re-deliveries.
        source_to_bronze = reconcile(
            [RowCountCheck(expect_distinct_target=True), amount_check],
            manifest.control_totals(),
            bronze_deduped,
        )
        report.results.extend(source_to_bronze.results)

    report.results.extend(
        reconcile([RowCountCheck(), amount_check], bronze_deduped, silver).results
    )
    report.results.extend(reconcile([RowCountCheck(), amount_check], silver, gold).results)
    report.results.extend(reconcile([DeduplicationCheck()], silver, silver).results)

    if check_referential_integrity:
        report.results.extend(_referential_integrity(runner, settings).results)

    logger.info(report.summary())
    return report


def _referential_integrity(runner: QueryRunner, settings: Settings) -> ReconciliationReport:
    fact_keys = [
        str(row[0])
        for row in runner.query(distinct_key_sql(settings.gold_fact, "account_id"))
    ]
    dim_keys = [
        str(row[0])
        for row in runner.query(
            distinct_key_sql(settings.table(settings.gold_schema, "dim_account"), "account_id")
        )
    ]
    return reconcile(
        [
            ReferentialIntegrityCheck(
                dimension="dim_account", fact_keys=fact_keys, dimension_keys=dim_keys
            )
        ]
    )

"""Command line entry points: bootstrap, produce, ingest, reconcile, maintain, demo."""

from __future__ import annotations

import json
import logging
import sys

import typer
from rich.console import Console
from rich.table import Table

from lakehouse_recon.config import load_settings
from lakehouse_recon.ddl import bootstrap_statements
from lakehouse_recon.generator import TransactionGenerator
from lakehouse_recon.ingestion import BronzeLoader
from lakehouse_recon.maintenance import IcebergMaintenance
from lakehouse_recon.pipeline import run_reconciliation
from lakehouse_recon.reconciliation import ReconciliationReport
from lakehouse_recon.streaming import EventConsumer, EventProducer
from lakehouse_recon.trino_client import TrinoRunner

app = typer.Typer(
    add_completion=False,
    help="Trino + Iceberg medallion with reconciliation controls",
)
console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _render(report: ReconciliationReport) -> None:
    table = Table(title="Reconciliation", show_lines=False)
    for column in ("check", "partitions", "breaks", "result"):
        table.add_column(column)
    for result in report.results:
        table.add_row(
            result.check,
            str(result.partitions_compared),
            str(len(result.breaks)),
            "[green]pass[/green]" if result.passed else "[red]FAIL[/red]",
        )
    console.print(table)
    for issue in report.breaks:
        colour = "red" if issue.severity.value == "error" else "yellow"
        console.print(f"[{colour}]{issue.describe()}[/{colour}]")


@app.command()
def bootstrap() -> None:
    """Create the bronze/silver/gold schemas and the bronze Iceberg table."""
    settings = load_settings()
    runner = TrinoRunner(settings)
    statements = bootstrap_statements(settings)
    runner.execute_many(statements)
    console.print(f"[green]bootstrapped[/green] {len(statements)} statement(s)")


@app.command()
def produce(count: int = 5_000, seed: int = 42) -> None:
    """Generate events and publish them to Kafka."""
    settings = load_settings()
    generator = TransactionGenerator(seed=seed)
    producer = EventProducer(settings)
    published = producer.publish(generator.stream(count))
    producer.close()
    console.print(
        f"[green]published[/green] {published} message(s) "
        f"({generator.manifest.unique_events} unique, "
        f"{generator.manifest.duplicates} duplicate re-deliveries, "
        f"{generator.manifest.late_arrivals} late arrivals)"
    )


@app.command()
def ingest(max_messages: int | None = None) -> None:
    """Drain Kafka into the bronze Iceberg table."""
    settings = load_settings()
    runner = TrinoRunner(settings)
    loader = BronzeLoader(runner, settings)
    result = loader.load_raw(EventConsumer(settings).drain(max_messages))
    console.print(
        f"[green]ingested[/green] {result.rows_written} row(s) in {result.batches} batch(es); "
        f"{result.dead_letter_count} dead-lettered"
    )
    for dead in result.dead_letters[:5]:
        console.print(f"[yellow]dead letter:[/yellow] {dead.reason}")


@app.command()
def reconcile(
    as_json: bool = typer.Option(False, "--json", help="Emit the report as JSON"),
    fail: bool = typer.Option(True, help="Exit non-zero when a control breaks"),
) -> None:
    """Run every reconciliation control across the medallion."""
    settings = load_settings()
    runner = TrinoRunner(settings)
    report = run_reconciliation(runner, settings)
    if as_json:
        console.print_json(json.dumps(report.to_dict()))
    else:
        _render(report)
    if fail and not report.passed:
        raise typer.Exit(code=1)


@app.command()
def maintain(
    table: str = typer.Option(None, help="Fully-qualified table; defaults to bronze"),
    retention: str = "7d",
    optimize: bool = True,
    expire: bool = True,
) -> None:
    """Compact small files and expire old snapshots."""
    settings = load_settings()
    runner = TrinoRunner(settings)
    target = table or settings.bronze_transactions
    ops = IcebergMaintenance(runner)
    if optimize:
        ops.optimize(target)
        console.print(f"[green]compacted[/green] {target}")
    if expire:
        ops.expire_snapshots(target, retention)
        console.print(f"[green]expired snapshots[/green] older than {retention} on {target}")


@app.command()
def snapshots(table: str = typer.Option(None), limit: int = 10) -> None:
    """Show Iceberg snapshot history — the audit trail time travel reads from."""
    settings = load_settings()
    runner = TrinoRunner(settings)
    target = table or settings.bronze_transactions
    grid = Table(title=f"snapshots: {target}")
    for column in ("snapshot_id", "committed_at", "operation"):
        grid.add_column(column)
    for snapshot in IcebergMaintenance(runner).snapshots(target, limit=limit):
        grid.add_row(str(snapshot.snapshot_id), snapshot.committed_at, snapshot.operation)
    console.print(grid)


@app.command()
def demo(count: int = 5_000, seed: int = 42) -> None:
    """End-to-end: bootstrap, generate, ingest, reconcile against the source manifest."""
    settings = load_settings()
    runner = TrinoRunner(settings)
    runner.execute_many(bootstrap_statements(settings))

    generator = TransactionGenerator(seed=seed)
    events = generator.generate(count)
    BronzeLoader(runner, settings).load(events)
    console.print(
        f"[green]landed[/green] {len(events)} message(s) in bronze "
        f"({generator.manifest.unique_events} unique)"
    )
    console.print("[dim]now run `make transform` to build silver and gold with dbt[/dim]")

    report = run_reconciliation(runner, settings, manifest=generator.manifest)
    _render(report)
    if not report.passed:
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    app()

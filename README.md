# lakehouse-recon-platform

A Trino + Apache Iceberg medallion lakehouse where **reconciliation is a first-class
control that fails the run**, not a dashboard nobody opens.

Moving financial events bronze → silver → gold is the easy part. Proving the money
survived the journey is the part that decides whether a finance controller can sign off
on the numbers. This project is built around that second problem.

```
                    ┌──────────────┐
                    │   source     │  seeded generator + manifest
                    │   system     │  (its own claim about what it sent)
                    └──────┬───────┘
                           │  Kafka (KRaft), keyed by account_id
                           ▼
   ┌───────────────────────────────────────────────┐
   │ BRONZE  iceberg.bronze.transactions_raw       │  append-only
   │ raw events, at-least-once duplicates retained │  partitioned by month
   └──────┬────────────────────────────────────────┘
          │  dbt: first-arrival dedup, type + validity rules
          ▼
   ┌───────────────────────────────────────────────┐
   │ SILVER  silver_transactions                   │  one row per event_id
   │         silver_rejected_transactions          │  quarantine, with reasons
   └──────┬────────────────────────────────────────┘
          │  dbt: star schema
          ▼
   ┌───────────────────────────────────────────────┐
   │ GOLD    fct_transaction                       │  additive minor-unit measure
   │         dim_account · dim_currency · dim_date │  conformed dimensions
   └───────────────────────────────────────────────┘

   ╔═══════════════════════════════════════════════════════════════╗
   ║ RECONCILIATION — runs across every arrow above                ║
   ║   source → bronze   row count · amount sum   (loss in transit)║
   ║   bronze → silver   row count · amount sum   (transform loss) ║
   ║   silver → gold     row count · amount sum   (publish loss)   ║
   ║   within silver     deduplication effectiveness               ║
   ║   within gold       referential integrity, fact → dimension   ║
   ╚═══════════════════════════════════════════════════════════════╝
                      breaks → non-zero exit code
```

## The five decisions worth arguing about

**1. Amounts are integer minor units, never floats.** Float money makes reconciliation
approximate, and an approximate financial control is not a control. Integers let the
bronze-to-gold amount check run at *exactly zero tolerance*. Tolerance exists in the API
(`AmountSumCheck(tolerance_minor=..., relative_tolerance=...)`) but has to be asked for,
and the relative tolerance is capped at 1% so it cannot quietly become a blanket excuse.

**2. Reconciliation runs at partition grain, never whole-table.** Two offsetting errors
sum to zero and pass a table-level total. There is a test named
`test_offsetting_errors_do_not_cancel_out_across_partitions` that pins this.

**3. Missing partitions are compared, not skipped.** Checks align on the *union* of
partition keys from both sides. Intersecting would silently exclude the partition that
failed to load at all — the single most important case a reconciliation exists to catch.

**4. Bronze keeps its duplicates; deduplication happens in the control total.** Kafka is
at-least-once, so duplicates are a certainty. Suppressing them at the edge would destroy
the evidence that proves silver did its job. Instead the bronze control total applies a
first-arrival window (`ROW_NUMBER() ... WHERE _rn = 1`) so like is compared with like,
and a separate `DeduplicationCheck` asserts silver's rows equal its distinct event ids.

**5. The source system's own manifest is reconciled against bronze.** Every layer-to-layer
check is blind to data lost *before* it reached the lake. The generator emits a
`GenerationManifest` — what the source claims it sent — and bronze is checked against it.
This is the control most pipelines do not have.

## Why Iceberg, concretely

Not "because lakehouse." The operational properties that matter for a financial table:

| Capability | Why it matters here | Where |
|---|---|---|
| Snapshot isolation + time travel | Reconcile against the exact state a report was published from | `time_travel_sql` |
| `rollback_to_snapshot` | A corrupt publish is one statement from reversed, and the reversal is itself a snapshot — non-destructive | `rollback_sql` |
| Schema evolution by column id | Add a settlement reference without rewriting history | `add_column_sql` |
| Partition evolution | Change the layout for future writes only; Hive would need a full rewrite | `set_partitioning_sql` |
| `optimize` compaction | Streaming ingestion makes small files; small files kill scan performance | `optimize_sql` |
| `expire_snapshots` | Old snapshots pin the data files they reference; storage grows forever otherwise | `expire_snapshots_sql` |

Snapshot expiry and compaction are run **explicitly** by `lakehouse maintain`, never
implicitly, so the audit trail is not trimmed as a side effect of a nightly job.

## Run it

```bash
make install      # editable install with dev + dbt extras
make up           # MinIO, Iceberg REST catalog, Trino, Kafka — waits for readiness
make bootstrap    # create bronze/silver/gold schemas and the bronze Iceberg table
make produce      # publish 5,000 seeded events to Kafka (with duplicates and late arrivals)
make ingest       # drain Kafka into bronze; unparseable messages are dead-lettered
make transform    # dbt build: silver dedup + quarantine, gold star schema, dbt tests
make reconcile    # every control; exits non-zero on a break
```

### Watch the controls actually catch something

`make transform-break` rebuilds gold with `--vars 'inject_break: true'`, which drops FX
conversions from the fact table — the exact shape of a real incident, where someone adds
a `WHERE` clause for a good local reason and silently changes a reported total.

```bash
make transform-break && make reconcile
```

Real output from that run, on 5,168 messages (5,000 unique events, 168 duplicate
re-deliveries, 103 late arrivals) across six business dates:

```
                     Reconciliation
┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃ check                 ┃ partitions ┃ breaks ┃ result ┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│ row_count             │ 6          │ 0      │ pass   │   bronze → silver
│ amount_sum            │ 6          │ 0      │ pass   │
│ row_count             │ 6          │ 6      │ FAIL   │   silver → gold
│ amount_sum            │ 6          │ 6      │ FAIL   │
│ deduplication         │ 6          │ 0      │ pass   │
│ referential_integrity │ 40         │ 0      │ pass   │
└───────────────────────┴────────────┴────────┴────────┘
[ERROR] row_count  @ 2026-09-03: silver=1029      vs gold=814       (delta -215)
[ERROR] amount_sum @ 2026-09-03: silver=-10542421 vs gold=-15562784 (delta -5020363)
```

The run exits 1. A scheduler treats that as a failed job; nobody publishes the number.

**The part worth noticing:** `dbt build` reported `PASS=31 WARN=0 ERROR=0` on that same
broken gold table. Every dbt test passed — uniqueness, not-null, accepted values, all
three `relationships` tests. dbt validates the *shape* of what you built; it has no
opinion about whether the total changed. That gap is exactly what these controls fill,
and it is why reconciliation belongs in the pipeline rather than in the test suite.

### Inspect the Iceberg internals

```bash
lakehouse snapshots                    # snapshot id, commit time, operation
lakehouse maintain --retention 7d      # compact, then expire snapshots older than 7 days
```

All of it verified against the running stack, not just asserted in tests — time travel
(`5,168 rows now, 0 at the first snapshot`), rollback (`5,168 → 2,500 → 5,168` across
snapshots), `ADD COLUMN` on a populated table, partition evolution from
`ARRAY['month(occurred_at)']` to `ARRAY['month(occurred_at)', 'currency']`, and
`optimize` compaction.

## Testing

```bash
make test              # 111 unit tests, no cluster required
make test-integration  # end-to-end against `make up`
make ci                # ruff + mypy + unit tests, same as GitHub Actions
```

The reconciliation engine is **pure** — it imports nothing from Trino. Control totals
arrive as plain dataclasses, so every check is unit-testable without a warehouse, and
swapping Trino for another engine means rewriting `control_totals.py` and nothing else.
SQL builders return strings, so the Iceberg dialect is asserted on directly rather than
discovered in production.

## Honest scope

This is a demonstration of the pattern, built to be read, not a system that has served
production traffic. Specifically:

- **Bronze is written with batched Trino `INSERT`s.** Correct for a laptop and modest
  volumes; at high throughput you would write Iceberg directly from the stream processor
  (Flink or Spark) and leave Trino for reads. The seam is `BronzeLoader` — swapping the
  writer changes one class.
- **The data is synthetic**, from a seeded generator. Reproducible and fault-injectable,
  but not real-world messy.
- **Single-node everything.** No cluster sizing, no concurrency tuning, no cost story at
  scale.
- **The warehouse is a shared docker volume**, not object storage. The REST catalog
  abstraction means pointing this at S3, GCS, Glue or Nessie is a config change, but
  "works on S3" is not something this repo demonstrates.

What it does show honestly: the reconciliation design, the Iceberg operations, the
medallion modelling, and the failure modes each control is there to catch.

## Layout

```
src/lakehouse_recon/
  reconciliation.py   # pure check engine: control totals, breaks, report  ← start here
  control_totals.py   # the SQL that produces totals + the Trino adapter
  pipeline.py         # which layers get compared, with which checks
  maintenance.py      # Iceberg time travel, rollback, compaction, evolution
  ingestion.py        # Kafka → bronze, batching, dead letters
  generator.py        # seeded events + source manifest + fault injection
  events.py           # typed events, minor units, sign normalisation
  ddl.py  config.py  streaming.py  trino_client.py  cli.py
dbt/lakehouse/models/
  silver/  # dedup + quarantine
  gold/    # fct_transaction, dim_account, dim_currency, dim_date
```

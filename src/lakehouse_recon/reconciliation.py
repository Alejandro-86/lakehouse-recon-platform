"""Reconciliation controls.

The point of this module: a medallion pipeline that moves data between layers
without proving the money survived the journey is not a financial pipeline, it
is a data-shaped rumour. Everything here compares *control totals* between two
layers and produces **breaks** — a break is a named, quantified discrepancy, not
a log line saying something looks odd.

Design notes worth defending:

* **Pure.** Nothing in here imports Trino. Control totals arrive as plain
  objects, so every check is unit-testable without a cluster running. The SQL
  that produces the totals lives in :mod:`lakehouse_recon.control_totals`.
* **Fails the run, not a dashboard.** :meth:`ReconciliationReport.raise_for_status`
  raises on any ERROR-severity break. A control that only warns is a control
  that gets ignored by week three.
* **Zero tolerance by default.** Amounts are integer minor units, so exact
  equality is achievable. Tolerance is opt-in and exists for derived or
  FX-converted aggregates, where it should be argued for explicitly.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum


class Layer(StrEnum):
    #: The source system's own claim about what it sent. Reconciling bronze
    #: against this — rather than only against silver — is what catches data
    #: lost *before* it ever reached the lake.
    SOURCE = "source"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


class ReconciliationFailure(RuntimeError):
    """Raised when a reconciliation run produces ERROR-severity breaks."""

    def __init__(self, report: ReconciliationReport) -> None:
        self.report = report
        super().__init__(report.summary())


@dataclass(frozen=True, slots=True)
class ControlTotal:
    """Aggregate fingerprint of one partition of one layer.

    ``partition_key`` is whatever grain the reconciliation runs at — a business
    date, a batch id, a currency. Reconciling only at whole-table grain hides
    offsetting errors, which is how two wrong numbers pass a total check.
    """

    layer: Layer
    partition_key: str
    row_count: int
    amount_minor_sum: int
    distinct_event_ids: int

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("row_count must not be negative")
        if self.distinct_event_ids < 0:
            raise ValueError("distinct_event_ids must not be negative")
        if self.distinct_event_ids > self.row_count:
            raise ValueError(
                f"distinct_event_ids ({self.distinct_event_ids}) cannot exceed "
                f"row_count ({self.row_count}) for partition {self.partition_key!r}"
            )


@dataclass(frozen=True, slots=True)
class Break:
    """One quantified discrepancy. Every field here is something a controller asks for."""

    check: str
    partition_key: str
    source_layer: Layer
    target_layer: Layer
    expected: int
    actual: int
    severity: Severity = Severity.ERROR
    note: str = ""

    @property
    def delta(self) -> int:
        return self.actual - self.expected

    def describe(self) -> str:
        detail = f" ({self.note})" if self.note else ""
        return (
            f"[{self.severity.value.upper()}] {self.check} @ {self.partition_key}: "
            f"{self.source_layer.value}={self.expected} vs "
            f"{self.target_layer.value}={self.actual} (delta {self.delta:+d}){detail}"
        )


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    breaks: tuple[Break, ...]
    partitions_compared: int

    @property
    def passed(self) -> bool:
        return not any(b.severity is Severity.ERROR for b in self.breaks)


def _index(totals: Iterable[ControlTotal]) -> dict[str, ControlTotal]:
    indexed: dict[str, ControlTotal] = {}
    for total in totals:
        if total.partition_key in indexed:
            raise ValueError(f"duplicate control total for partition {total.partition_key!r}")
        indexed[total.partition_key] = total
    return indexed


def _layer_of(totals: Sequence[ControlTotal], default: Layer) -> Layer:
    """Layer label for reporting; the sequence is homogeneous by construction."""
    return totals[0].layer if totals else default


def _aligned_keys(
    source: Mapping[str, ControlTotal], target: Mapping[str, ControlTotal]
) -> list[str]:
    """Union, not intersection.

    Intersecting would silently exclude a partition that failed to load at all —
    the single most important case a reconciliation exists to catch.
    """
    return sorted(set(source) | set(target))


class ReconciliationCheck:
    """Base class. Subclasses compare two layers' control totals."""

    name: str = "check"

    def run(
        self,
        source: Sequence[ControlTotal],
        target: Sequence[ControlTotal],
    ) -> CheckResult:  # pragma: no cover - abstract
        raise NotImplementedError


class RowCountCheck(ReconciliationCheck):
    """Row counts must agree between layers, partition by partition.

    Silver legitimately drops duplicates, so comparing bronze rows to silver rows
    directly would break every run. Pass ``expect_distinct_source=True`` to
    compare bronze *distinct event ids* against silver rows instead — that is the
    correct identity once deduplication is part of the contract.
    ``expect_distinct_target=True`` does the same on the other side, which is
    what a source-system manifest needs when bronze deliberately retains
    at-least-once duplicates.
    """

    name = "row_count"

    def __init__(
        self,
        *,
        expect_distinct_source: bool = False,
        expect_distinct_target: bool = False,
    ) -> None:
        self.expect_distinct_source = expect_distinct_source
        self.expect_distinct_target = expect_distinct_target

    def run(
        self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]
    ) -> CheckResult:
        src, tgt = _index(source), _index(target)
        source_layer = _layer_of(source, Layer.BRONZE)
        target_layer = _layer_of(target, Layer.SILVER)
        breaks: list[Break] = []
        keys = _aligned_keys(src, tgt)
        for key in keys:
            s, t = src.get(key), tgt.get(key)
            expected = 0
            if s is not None:
                expected = s.distinct_event_ids if self.expect_distinct_source else s.row_count
            actual = 0
            if t is not None:
                actual = t.distinct_event_ids if self.expect_distinct_target else t.row_count
            if expected != actual:
                note = ""
                if t is None:
                    note = "partition missing from target layer"
                elif s is None:
                    note = "partition present in target but absent from source"
                breaks.append(
                    Break(
                        check=self.name,
                        partition_key=key,
                        source_layer=source_layer,
                        target_layer=target_layer,
                        expected=expected,
                        actual=actual,
                        note=note,
                    )
                )
        return CheckResult(self.name, tuple(breaks), len(keys))


class AmountSumCheck(ReconciliationCheck):
    """The money check: summed minor units must survive the layer transition.

    Tolerance is in *minor units* and defaults to zero. A relative tolerance is
    available for FX-converted aggregates, where rounding is real rather than a
    symptom of a bug — but it has to be asked for.
    """

    name = "amount_sum"

    def __init__(self, *, tolerance_minor: int = 0, relative_tolerance: float = 0.0) -> None:
        if tolerance_minor < 0:
            raise ValueError("tolerance_minor must not be negative")
        if not 0.0 <= relative_tolerance <= 0.01:
            raise ValueError("relative_tolerance must be between 0 and 0.01")
        self.tolerance_minor = tolerance_minor
        self.relative_tolerance = relative_tolerance

    def allowed_drift(self, expected: int) -> int:
        relative = int(abs(expected) * self.relative_tolerance)
        return max(self.tolerance_minor, relative)

    def run(
        self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]
    ) -> CheckResult:
        src, tgt = _index(source), _index(target)
        source_layer = _layer_of(source, Layer.BRONZE)
        target_layer = _layer_of(target, Layer.SILVER)
        breaks: list[Break] = []
        keys = _aligned_keys(src, tgt)
        for key in keys:
            s, t = src.get(key), tgt.get(key)
            expected = s.amount_minor_sum if s is not None else 0
            actual = t.amount_minor_sum if t is not None else 0
            drift = abs(actual - expected)
            if drift > self.allowed_drift(expected):
                breaks.append(
                    Break(
                        check=self.name,
                        partition_key=key,
                        source_layer=source_layer,
                        target_layer=target_layer,
                        expected=expected,
                        actual=actual,
                        note=f"drift {drift} minor units exceeds allowance "
                        f"{self.allowed_drift(expected)}",
                    )
                )
        return CheckResult(self.name, tuple(breaks), len(keys))


class DeduplicationCheck(ReconciliationCheck):
    """Proves deduplication actually happened: in the target, rows == distinct ids.

    Single-layer check — pass the same sequence as source and target, or just the
    target. A silver table with more rows than distinct event ids means the
    dedupe window is wrong, which corrupts every downstream aggregate.
    """

    name = "deduplication"

    def run(
        self, source: Sequence[ControlTotal], target: Sequence[ControlTotal]
    ) -> CheckResult:
        subjects = target or source
        breaks = [
            Break(
                check=self.name,
                partition_key=t.partition_key,
                source_layer=t.layer,
                target_layer=t.layer,
                expected=t.distinct_event_ids,
                actual=t.row_count,
                note="duplicate event ids survived into this layer",
            )
            for t in subjects
            if t.row_count != t.distinct_event_ids
        ]
        return CheckResult(self.name, tuple(breaks), len(subjects))


class ReferentialIntegrityCheck(ReconciliationCheck):
    """Fact rows must resolve to a dimension.

    Takes key sets rather than control totals, so it is constructed with the data
    it needs and ignores the standard ``run`` inputs. Orphaned foreign keys in a
    star schema produce silently under-reported totals when an analyst joins to
    the dimension — the failure mode is a number that is wrong but plausible.
    """

    name = "referential_integrity"

    def __init__(
        self,
        *,
        dimension: str,
        fact_keys: Iterable[str],
        dimension_keys: Iterable[str],
        severity: Severity = Severity.ERROR,
    ) -> None:
        self.dimension = dimension
        self.fact_keys = set(fact_keys)
        self.dimension_keys = set(dimension_keys)
        self.severity = severity

    def run(
        self,
        source: Sequence[ControlTotal] = (),
        target: Sequence[ControlTotal] = (),
    ) -> CheckResult:
        orphans = sorted(self.fact_keys - self.dimension_keys)
        breaks = [
            Break(
                check=self.name,
                partition_key=f"{self.dimension}:{key}",
                source_layer=Layer.GOLD,
                target_layer=Layer.GOLD,
                expected=1,
                actual=0,
                severity=self.severity,
                note=f"fact references {self.dimension} key with no dimension row",
            )
            for key in orphans
        ]
        return CheckResult(self.name, tuple(breaks), len(self.fact_keys))


@dataclass
class ReconciliationReport:
    """The artefact a controller can be handed without translation."""

    results: list[CheckResult] = field(default_factory=list)

    @property
    def breaks(self) -> tuple[Break, ...]:
        return tuple(b for r in self.results for b in r.breaks)

    @property
    def errors(self) -> tuple[Break, ...]:
        return tuple(b for b in self.breaks if b.severity is Severity.ERROR)

    @property
    def warnings(self) -> tuple[Break, ...]:
        return tuple(b for b in self.breaks if b.severity is Severity.WARNING)

    @property
    def passed(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        if self.passed and not self.warnings:
            checks = ", ".join(r.check for r in self.results) or "none"
            return f"reconciliation PASSED ({len(self.results)} checks: {checks})"
        head = (
            f"reconciliation {'PASSED with warnings' if self.passed else 'FAILED'}: "
            f"{len(self.errors)} error(s), {len(self.warnings)} warning(s)"
        )
        return "\n".join([head, *(f"  {b.describe()}" for b in self.breaks)])

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "checks": [
                {
                    "check": r.check,
                    "passed": r.passed,
                    "partitions_compared": r.partitions_compared,
                    "breaks": len(r.breaks),
                }
                for r in self.results
            ],
            "breaks": [
                {
                    "check": b.check,
                    "partition_key": b.partition_key,
                    "source_layer": str(b.source_layer),
                    "target_layer": str(b.target_layer),
                    "expected": b.expected,
                    "actual": b.actual,
                    "delta": b.delta,
                    "severity": str(b.severity),
                    "note": b.note,
                }
                for b in self.breaks
            ],
        }

    def raise_for_status(self) -> None:
        """Fail the run loudly. The whole point of the module."""
        if not self.passed:
            raise ReconciliationFailure(self)


def reconcile(
    checks: Sequence[ReconciliationCheck],
    source: Sequence[ControlTotal] = (),
    target: Sequence[ControlTotal] = (),
) -> ReconciliationReport:
    """Run every check and collect the breaks into one report."""
    return ReconciliationReport(results=[check.run(source, target) for check in checks])

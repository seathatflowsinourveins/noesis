"""Tier-3 Key A: per-leg paper-merit record, latched demotion and the capital ladder.

ADR-0020 (Stage 5) and ADR-0029 (tier 3): a strategy leg (``LONG`` or ``SHORT``) may originate
scanner entries on the live venue only when a harness-computed paper-merit record for that leg is
current and passing (Key A) and the owner's standing opt-in is on (Key B). This module computes
Key A and the promotion state; it holds no opt-in, talks to no broker and opens no network
connection.

One decision cycle, in the order the caller runs it (every step is recomputed per decision; a
record or a state read back from an earlier cycle is never the gate)::

    state  = <persisted state; written once by the opt-in step from initial_promotion_state()>
    record = compute_live_promotion_merit(evidence_dir, now_utc=now, prior_state=state)
    state  = next_promotion_state(state, record, live_evidence=live, now_utc=now)   # persist it
    leg_merit = live_entry_leg_merit(state, record, leg, now_utc=now)

``leg_has_merit`` alone is not permission to enter: it reads the record only and cannot see a
demotion, a hold or an open protection fault. ``live_entry_leg_merit`` is the gate value.

Rules that shape every function here:

* Every criterion is evaluated per leg. LONG and SHORT evidence is never pooled, and a ledger row
  counts for a leg only through the explicit ``LEG_ENTRY_ORIGINS`` allow-list (an inverse-ETF
  hedge is a ``side = LONG`` order that belongs to neither leg).
* A criterion whose evidence is missing, unreadable, stale, future-dated, duplicated or not
  attributable to the leg is ``missing`` and the leg has no merit. Nothing is inferred and nothing
  defaults to a pass; comparisons are written in positive form so a non-number can never satisfy
  one. An absent promotion state (``prior_state=None``) and absent live evidence
  (``live_evidence=None``) are unknowns too: neither promotes and neither keeps a leg eligible.
* Ledger evidence is dated by its rows. File stamps only say whether a file is current. A leg
  whose state carries a demotion counts only paper rows decided after that demotion.
* ``now_utc`` is always passed in. The same evidence bytes, state and ``now_utc`` give the same
  record.
* Ledgers are DuckDB files. Each is read once into memory (which also gives its SHA-256) and the
  bytes are opened read-only from a private temporary snapshot, so the writer's file is never
  locked and the hash describes exactly the content that was evaluated. A broker account id never
  leaves the database: only its SHA-256 is read, to tell one ledger from another.

Evidence read from ``evidence_dir`` (the paper manifest directory):

* ``prewall_h109_falsification_dossier.json`` -> ``holdout_six_gate[LEG]`` (never ``COMBINED``).
* ``paper_lane_pa{1,2,3}.duckdb`` -> ``paper_entry_decisions``, ``paper_tca_fills``,
  ``paper_daily_fuse``.
* ``stage4_drills_manifest.json`` and ``ledger_broker_reconciliation_receipt.json``.
* ``<evidence_dir>/../live/manifests/paper_lane_live1.duckdb`` for the SHORT leg's
  operator-confirmed live round trips.
"""

from __future__ import annotations

import hashlib
import json
import math
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import duckdb
from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "CAPITAL_LADDER",
    "LEG_ENTRY_ORIGINS",
    "LIVE_PROMOTION_MERIT_SCHEMA",
    "LIVE_PROMOTION_STATE_SCHEMA",
    "CapitalStep",
    "LivePromotionThresholds",
    "compute_live_promotion_merit",
    "initial_promotion_state",
    "leg_has_merit",
    "leg_of_entry",
    "live_entry_leg_merit",
    "next_promotion_state",
]

LIVE_PROMOTION_MERIT_SCHEMA: Final = "tier0.live-promotion-merit.v1"
LIVE_PROMOTION_STATE_SCHEMA: Final = "tier0.live-promotion-state.v1"
GENERATOR: Final[Mapping[str, str]] = {"module": "tier0_sota.live.merit", "version": "1.1.0"}
LEGS: Final[tuple[str, ...]] = ("LONG", "SHORT")

# --- Evidence bindings (file names, schemas and field vocabularies) -------------------------------

DOSSIER_FILE: Final = "prewall_h109_falsification_dossier.json"
DOSSIER_SCHEMA: Final = "tier0.prewall-h109-falsification-dossier.v1"
DOSSIER_GATES: Final[tuple[str, ...]] = (
    "1_hftbacktest_l1_markout",
    "2_cost_1x_2x_survival",
    "3_cpcv_oos_positive",
    "4_dsr_pbo_credible",
    "5_hansen_spa_significant",
    "6_hac_t_stat_significant",
)
DOSSIER_COST_BLOCKS: Final[tuple[str, ...]] = (
    "falsification_1x_4bps",
    "falsification_calibrated_liquid",
    "falsification_2x_8bps",
)
STAGE4_FILE: Final = "stage4_drills_manifest.json"
STAGE4_SCHEMA: Final = "tier0.stage4-drills.v1"
STAGE4_DRILL_FLAGS: Final[tuple[tuple[str, str], ...]] = (
    ("k3_engine_kill_switch", "passed"),
    ("k4_broker_flatten_sandbox", "passed"),
    ("three_way_restart_reconciliation", "passed"),
    ("paper_farm_stage4_receipt", "all_passed"),
)
RECONCILIATION_FILE: Final = "ledger_broker_reconciliation_receipt.json"
RECONCILIATION_SCHEMA: Final = "tier0.ledger-broker-reconciliation.v1"
RECONCILED_STATUS: Final = "RECONCILED_OK"
# Paper ledgers are named, never globbed: the live lane's ledger has the same file-name pattern.
PAPER_LEDGERS: Final[Mapping[str, str]] = {
    "ALPACA_PA1": "paper_lane_pa1.duckdb",
    "ALPACA_PA2": "paper_lane_pa2.duckdb",
    "ALPACA_PA3": "paper_lane_pa3.duckdb",
}
# Relative to the data root (the parent of ``evidence_dir``): ``<data root>/live/manifests``.
LIVE_LEDGER: Final = "live/manifests/paper_lane_live1.duckdb"

# Which ledger ``origin`` values belong to which leg (one ``_AUTO_REVERSE`` / ``_REENTRY`` suffix
# is stripped first). Anything else, including every ``INVERSE_HEDGE_*`` origin, is unattributed.
LEG_ENTRY_ORIGINS: Final[Mapping[str, frozenset[str]]] = {
    "LONG": frozenset({"TREND_ALIGNED_LONG", "DIRECT_LONG_REVERSAL"}),
    "SHORT": frozenset({"DIRECT_SHORT_ETB"}),
}
_ORIGIN_SUFFIXES: Final[tuple[str, ...]] = ("_AUTO_REVERSE", "_REENTRY")
PAPER_ENTRY_AUTHORITY: Final = "AUTONOMOUS"
LIVE_CONFIRMED_ENTRY_AUTHORITY: Final = "OPERATOR_CONFIRMED"
# Entry authorities a live round trip of a leg may carry in ``live_evidence``: scanner-originated
# (tier 3) or operator-confirmed (tier 2).
LIVE_ENTRY_AUTHORITIES: Final[frozenset[str]] = frozenset(
    {PAPER_ENTRY_AUTHORITY, LIVE_CONFIRMED_ENTRY_AUTHORITY}
)
SUPERVISED_EXIT_CLOSE_CAUSE: Final = "HARNESS_EXIT_FILLED"
ROUND_TRIP_CLOSE_CAUSES: Final[frozenset[str]] = frozenset(
    {"PROTECTIVE_STOP_FILLED", SUPERVISED_EXIT_CLOSE_CAUSE}
)
NON_DEFECT_BREAKER_CAUSES: Final[frozenset[str]] = frozenset({"MARKET_DRAWDOWN"})
MACRO_REGIMES: Final[frozenset[int]] = frozenset({-1, 0, 1})

# Live decay triggers (ADR-0020 demotion). The windows are never cut at a promotion date, so the
# round trips that demoted a leg keep counting until newer live round trips displace them.
LIVE_SHORTFALL_WINDOW_ROUND_TRIPS: Final = 20
LIVE_FUSE_WINDOW_SESSIONS: Final = 5
LIVE_FUSE_TRIPS_TO_DEMOTE: Final = 2
LIVE_DECAY_CAUSES: Final[frozenset[str]] = frozenset(
    {
        "LIVE_SHORTFALL_ABOVE_MAX",
        "LIVE_FUSE_TRIPPED_TWO_OF_FIVE_SESSIONS",
        "LIVE_PROTECTION_FAULT",
    }
)
# A leg demoted for a live decay cause is released only after this many operator-confirmed live
# round trips of the leg, opened after the demotion and clean. Owner-tunable data, not ADR text.
LIVE_CAUSE_RELEASE_CONFIRMED_ROUND_TRIPS: Final = 10

_LEDGER_READ_ATTEMPTS: Final = 3
_FUTURE_SKEW_TOLERANCE: Final = timedelta(minutes=5)
_SESSION_ROLL: Final = timedelta(hours=4)  # the 20:00 ET overnight session is the next trading day
_NY: Final = ZoneInfo("America/New_York")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_DISPLAY_QUANTUM: Final = Decimal("0.000001")
_LEDGER_KEY: Final = "__ledger__"  # tag added to every merged row: the ledger file it came from
_SHA256_HEX_LENGTH: Final = 64

_LEDGER_QUERIES: Final[tuple[tuple[str, str], ...]] = (
    (
        "paper_entry_decisions",
        "SELECT * EXCLUDE (decided_at, closed_at), epoch_us(decided_at) AS decided_at_us, "
        "epoch_us(closed_at) AS closed_at_us FROM paper_entry_decisions",
    ),
    ("paper_tca_fills", "SELECT * EXCLUDE (account) FROM paper_tca_fills"),
    (
        "paper_daily_fuse",
        "SELECT * EXCLUDE (account_id), sha256(CAST(account_id AS VARCHAR)) AS account_digest "
        "FROM paper_daily_fuse",
    ),
)

_CRITERIA_BEFORE_SHORT: Final[tuple[str, ...]] = (
    "dossier_passing",
    "dossier_dsr",
    "dossier_pbo",
    "dossier_leg_binding",
    "paper_round_trips",
    "paper_sessions",
    "paper_regimes",
    "paper_shortfall",
    "paper_supervised_exits",
    "breaker_trips",
    "reconciliation_receipt",
    "stage4_drills",
)
_SHORT_ONLY_CRITERION: Final = "short_live_confirmed"
_LATCH_CRITERION: Final = "demotion_latch"


class LivePromotionThresholds(BaseModel):
    """Owner-tunable thresholds of the tier-3 paper-merit record (Key A)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Statistics, not money: plain numbers, compared exactly through their decimal text.
    dsr_min: float = Field(default=0.95, ge=0, le=1, strict=True, allow_inf_nan=False)
    pbo_max: float = Field(default=0.50, ge=0, le=1, strict=True, allow_inf_nan=False)
    # Also the size of the trailing window the paper shortfall is checked on a second time.
    min_round_trips: int = Field(default=200, ge=1, strict=True)
    min_sessions: int = Field(default=20, ge=1, strict=True)
    min_regimes: int = Field(default=2, ge=1, strict=True)
    max_shortfall_bps: Decimal = Field(default=Decimal("8"), ge=0)
    min_supervised_exits: int = Field(default=30, ge=0, strict=True)
    evidence_max_age_hours: int = Field(default=36, ge=1, strict=True)
    # Paper fills ignore borrow, locate and short-sale restrictions, so a SHORT leg is not
    # promoted from paper evidence alone: it also needs this many operator-confirmed live short
    # round trips inside the shortfall bound. 0 disables the extra requirement.
    short_min_live_confirmed_round_trips: int = Field(default=10, ge=0, strict=True)
    demotion_cooloff_sessions: int = Field(default=5, ge=0, strict=True)


class CapitalStep(BaseModel):
    """One step of the Stage-5 capital ladder (ADR-0020, "Capital scaling inside Stage 5")."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    index: int = Field(ge=0, strict=True)
    name: str
    # Hard share cap of an entry at this step; ``None`` means the cap is ``risk_qty_fraction``.
    max_qty: int | None = Field(default=None, ge=1, strict=True)
    # Fraction of the ``risk.py`` risk-sized quantity an entry may use at this step.
    risk_qty_fraction: Decimal | None = Field(default=None, gt=0, le=1)
    # Consecutive clean live round trips at the previous step needed to reach this one.
    min_clean_live_round_trips_at_previous: int = Field(ge=0, strict=True)


# ADR-0020 names the three steps (micro-live -> fractional scale -> target allocation) and the
# rule (advance only on clean live evidence). The share cap of the first step is one share; the
# fraction and the round-trip counts below are owner-tunable data, not ADR text.
CAPITAL_LADDER: Final[tuple[CapitalStep, ...]] = (
    CapitalStep(index=0, name="MICRO_LIVE", max_qty=1, min_clean_live_round_trips_at_previous=0),
    CapitalStep(
        index=1,
        name="FRACTIONAL_SCALE",
        risk_qty_fraction=Decimal("0.25"),
        min_clean_live_round_trips_at_previous=20,
    ),
    CapitalStep(
        index=2,
        name="TARGET_ALLOCATION",
        risk_qty_fraction=Decimal("1"),
        min_clean_live_round_trips_at_previous=40,
    ),
)


# --- Small exact parsers (fail closed) ------------------------------------------------------------


def _decimal_or_none(value: Any) -> Decimal | None:
    """Exact Decimal of a real, finite number. Booleans, text, None, NaN and infinities are None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value)) if math.isfinite(value) else None
    return None


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _aware_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(UTC)


def _parse_utc(value: Any) -> datetime | None:
    """Timezone-aware ISO-8601 text as UTC; anything else (including naive text) is None."""
    if not isinstance(value, str):
        return None
    try:
        return _aware_utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _from_epoch_us(value: Any) -> datetime | None:
    micros = _int_or_none(value)
    if micros is None:
        return None
    try:
        return _EPOCH + timedelta(microseconds=micros)
    except OverflowError:
        return None


def _trade_date(moment: datetime) -> date:
    """ET trading date of an instant; 20:00 ET and later belongs to the next trading day."""
    return (moment.astimezone(_NY) + _SESSION_ROLL).date()


def _calendar_day(moment: datetime) -> date:
    """ET calendar date of an instant: the key ``paper_daily_fuse.day`` is written with."""
    return moment.astimezone(_NY).date()


def _mean(values: Sequence[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _jsonable(value: Any) -> Any:
    """Plain JSON value: Decimals and dates become text, sets become sorted lists."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f") if value.is_finite() else None
    if isinstance(value, float):
        return format(Decimal(str(value)), "f") if math.isfinite(value) else None
    if isinstance(value, datetime):
        return _iso(value) if value.tzinfo is not None else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(item) for item in value), key=str)
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return str(value)


def _canonical_json(value: Any) -> str | None:
    """Canonical JSON text of a value (sorted keys, no spacing); None when it has no JSON form."""
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError, RecursionError):
        return None


def _canonical_sha256(value: Any) -> str | None:
    text = _canonical_json(value)
    return None if text is None else hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_sha256_hex(value: Any) -> bool:
    return isinstance(value, str) and len(value) == _SHA256_HEX_LENGTH


@lru_cache(maxsize=1)
def _xnys_calendar() -> Any:
    import exchange_calendars as xcals

    return xcals.get_calendar("XNYS")


def _completed_sessions_between(start: date, end: date) -> int | None:
    """XNYS sessions strictly after ``start`` and strictly before ``end``; None if unknowable.

    The pinned calendar covers a fixed span of dates (its default end is about a year ahead).
    A date outside it gives None, which keeps a latch closed rather than guessing a count.
    """
    first, last = start + timedelta(days=1), end - timedelta(days=1)
    if first > last:
        return 0
    calendar = _xnys_calendar()
    try:
        if first < calendar.first_session.date() or last > calendar.last_session.date():
            return None
        return len(calendar.sessions_in_range(first.isoformat(), last.isoformat()))
    except ValueError:
        return None


def _configured(thresholds: Any) -> LivePromotionThresholds | None:
    """The thresholds a caller is configured with: ``None`` means the owner defaults."""
    if thresholds is None:
        return LivePromotionThresholds()
    return thresholds if isinstance(thresholds, LivePromotionThresholds) else None


# --- Reading evidence into plain data -------------------------------------------------------------


@dataclass(frozen=True)
class _Table:
    columns: frozenset[str]
    rows: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class _Source:
    """One evidence file as read: its identity plus parsed content (None when unreadable)."""

    name: str
    found: bool
    sha256: str | None
    mtime: datetime | None
    doc: Mapping[str, Any] | None = None
    tables: Mapping[str, _Table | None] | None = None

    def descriptor(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "found": self.found,
            "sha256": self.sha256,
            "mtime": _iso(self.mtime),
        }


@dataclass(frozen=True)
class _Evidence:
    dossier: _Source
    stage4: _Source
    reconciliation: _Source
    paper_ledgers: tuple[_Source, ...]
    live_ledger: _Source

    def sources(self) -> tuple[_Source, ...]:
        return (
            self.dossier,
            self.stage4,
            self.reconciliation,
            *self.paper_ledgers,
            self.live_ledger,
        )


_INPUT_NAMES: Final[frozenset[str]] = frozenset(
    {DOSSIER_FILE, STAGE4_FILE, RECONCILIATION_FILE, *PAPER_LEDGERS.values(), LIVE_LEDGER}
)


def _parse_json_object(raw: bytes) -> dict[str, Any] | None:
    try:
        parsed = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _fetch_table(conn: duckdb.DuckDBPyConnection, sql: str) -> _Table | None:
    try:
        cursor = conn.execute(sql)
        columns = tuple(str(col[0]) for col in cursor.description or ())
        rows = tuple(dict(zip(columns, row, strict=True)) for row in cursor.fetchall())
    except duckdb.Error:
        return None
    return _Table(columns=frozenset(columns), rows=rows)


def _read_ledger_tables(raw: bytes) -> dict[str, _Table | None] | None:
    """Tables of one ledger, opened read-only from a private snapshot; None if it cannot open."""
    try:
        with tempfile.TemporaryDirectory(prefix="tier0-merit-") as tmp:
            snapshot = Path(tmp) / "ledger.duckdb"
            snapshot.write_bytes(raw)
            conn = duckdb.connect(str(snapshot), read_only=True)
            try:
                present = {str(row[0]) for row in conn.execute("SHOW TABLES").fetchall()}
                return {
                    table: _fetch_table(conn, sql) if table in present else None
                    for table, sql in _LEDGER_QUERIES
                }
            finally:
                conn.close()
    except (duckdb.Error, OSError):
        return None


def _load_source(root: Path, name: str, *, ledger: bool) -> _Source:
    """Read one evidence file. The hash, the mtime and the parsed content describe one read.

    A ledger copied while its writer is mid-checkpoint can be torn, so an unreadable ledger is
    read again a bounded number of times (no sleep) before it is reported unreadable. JSON
    manifests are replaced atomically by their writers and are read once.
    """
    path = root / name
    source = _Source(name=name, found=False, sha256=None, mtime=None)
    for _ in range(_LEDGER_READ_ATTEMPTS if ledger else 1):
        try:
            raw = path.read_bytes()
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        except (OSError, OverflowError, ValueError):
            return _Source(name=name, found=False, sha256=None, mtime=None)
        digest = hashlib.sha256(raw).hexdigest()
        if not ledger:
            return _Source(name, True, digest, mtime, doc=_parse_json_object(raw))
        source = _Source(name, True, digest, mtime, tables=_read_ledger_tables(raw))
        if source.tables is not None:
            break
    return source


def _read_evidence(evidence_dir: Path) -> _Evidence:
    return _Evidence(
        dossier=_load_source(evidence_dir, DOSSIER_FILE, ledger=False),
        stage4=_load_source(evidence_dir, STAGE4_FILE, ledger=False),
        reconciliation=_load_source(evidence_dir, RECONCILIATION_FILE, ledger=False),
        paper_ledgers=tuple(
            _load_source(evidence_dir, name, ledger=True) for name in PAPER_LEDGERS.values()
        ),
        live_ledger=_load_source(evidence_dir.parent, LIVE_LEDGER, ledger=True),
    )


# --- Source checks: schema, timestamp, staleness --------------------------------------------------


@dataclass(frozen=True)
class _Checked:
    """A source plus the verdict on whether it may be used at ``now_utc``."""

    source: _Source
    problem: str | None  # None = usable
    evidence_at: datetime | None
    missing_code: str

    def reasons(self) -> list[str]:
        if self.problem is None:
            return []
        if self.problem == "STALE":
            return ["EVIDENCE_STALE"]
        if self.problem in ("TIMESTAMP_INVALID", "FUTURE_DATED"):
            return ["EVIDENCE_TIMESTAMP_INVALID"]
        return [self.missing_code]

    def detail(self) -> str | None:
        return None if self.problem is None else f"{self.source.name}:{self.problem}"


def _age_problem(evidence_at: datetime, now: datetime, max_age: timedelta) -> str | None:
    if evidence_at - now > _FUTURE_SKEW_TOLERANCE:
        return "FUTURE_DATED"
    if now - evidence_at > max_age:
        return "STALE"
    return None


def _check_json(
    source: _Source, schema: str, missing_code: str, now: datetime, max_age: timedelta
) -> _Checked:
    """A JSON manifest is dated by its own ``generated_at_utc``, never by its file mtime."""
    if not source.found:
        return _Checked(source, "MISSING", None, missing_code)
    if source.doc is None:
        return _Checked(source, "UNREADABLE", None, missing_code)
    if source.doc.get("schema_version") != schema:
        return _Checked(source, "SCHEMA_MISMATCH", None, missing_code)
    evidence_at = _parse_utc(source.doc.get("generated_at_utc"))
    if evidence_at is None:
        return _Checked(source, "TIMESTAMP_INVALID", None, missing_code)
    return _Checked(source, _age_problem(evidence_at, now, max_age), evidence_at, missing_code)


def _check_ledger(
    source: _Source, missing_code: str, now: datetime, max_age: timedelta
) -> _Checked:
    """Whether a ledger file is current. Its mtime says the writer is alive, nothing more.

    What the ledger proves is dated by its rows (``decided_at``): see ``_leg_rows``.
    """
    if not source.found:
        return _Checked(source, "MISSING", None, missing_code)
    if source.tables is None:
        return _Checked(source, "UNREADABLE", source.mtime, missing_code)
    if source.tables.get("paper_entry_decisions") is None:
        return _Checked(source, "SCHEMA_MISMATCH", source.mtime, missing_code)
    if source.mtime is None:
        return _Checked(source, "TIMESTAMP_INVALID", None, missing_code)
    return _Checked(source, _age_problem(source.mtime, now, max_age), source.mtime, missing_code)


# --- Criteria: one rule table for the grading and for the re-check of a record --------------------

_COMPARATORS: Final[Mapping[str, Callable[[Decimal, Decimal], bool]]] = {
    ">=": lambda observed, threshold: observed >= threshold,
    "<=": lambda observed, threshold: observed <= threshold,
    "<": lambda observed, threshold: observed < threshold,
}


def _criterion_rule(name: str, limits: LivePromotionThresholds) -> dict[str, Any]:
    """The ``threshold`` and ``comparator`` of a criterion under ``limits``.

    A mapping threshold means: every key of it must hold against the same key of ``observed``.
    """
    accounts = len(PAPER_LEDGERS)
    rules: dict[str, tuple[Any, str]] = {
        "dossier_passing": (
            {"promoted": True, "verdict": "PROMOTE", "gates_passed": len(DOSSIER_GATES)},
            "==",
        ),
        "dossier_dsr": (Decimal(str(limits.dsr_min)), ">="),
        # Strict, as the dossier's own gate 4 is (``pbo < 0.50``): a PBO at the bound fails.
        "dossier_pbo": (Decimal(str(limits.pbo_max)), "<"),
        "dossier_leg_binding": ({"bound_rows": 1}, ">="),
        "paper_round_trips": (limits.min_round_trips, ">="),
        "paper_sessions": (limits.min_sessions, ">="),
        "paper_regimes": (limits.min_regimes, ">="),
        "paper_shortfall": (
            {
                "mean_bps": limits.max_shortfall_bps,
                "trailing_mean_bps": limits.max_shortfall_bps,
            },
            "<=",
        ),
        "paper_supervised_exits": (limits.min_supervised_exits, ">="),
        "breaker_trips": ({"defect_trips": 0, "sessions_without_fuse_row": 0}, "=="),
        "reconciliation_receipt": (
            {"all_reconciled": True, "clean_accounts": accounts, "bound_accounts": accounts},
            "==",
        ),
        "stage4_drills": ({"flags_true": 2 + len(STAGE4_DRILL_FLAGS)}, "=="),
        _SHORT_ONLY_CRITERION: (
            {"inside_shortfall_bound": limits.short_min_live_confirmed_round_trips},
            ">=",
        ),
        _LATCH_CRITERION: ({"cooloff_sessions": limits.demotion_cooloff_sessions}, ">="),
    }
    threshold, comparator = rules[name]
    return {"threshold": threshold, "comparator": comparator}


def _number(value: Any) -> Decimal | None:
    """A JSON number, or the decimal text a record stores one as; anything else is None."""
    if isinstance(value, str):
        try:
            parsed = Decimal(value)
        except InvalidOperation:
            return None
        return parsed if parsed.is_finite() else None
    return _decimal_or_none(value)


def _holds(observed: Any, comparator: Any, threshold: Any) -> bool:
    """Whether a JSON ``observed`` value satisfies a JSON ``threshold``; never true by default."""
    if isinstance(threshold, Mapping):
        return (
            isinstance(observed, Mapping)
            and bool(threshold)
            and all(
                key in observed and _holds(observed[key], comparator, item)
                for key, item in threshold.items()
            )
        )
    if comparator == "==":
        return type(observed) is type(threshold) and observed == threshold
    compare = _COMPARATORS.get(comparator) if isinstance(comparator, str) else None
    left, right = _number(observed), _number(threshold)
    return compare is not None and left is not None and right is not None and compare(left, right)


def _criterion(
    status: str,
    *,
    scope: str,
    source: str,
    field: str,
    observed: Any = None,
    threshold: Any = None,
    comparator: str | None = None,
    reasons: Iterable[str] = (),
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "scope": scope,
        "source": source,
        "field": field,
        "observed": _jsonable(observed),
        "threshold": _jsonable(threshold),
        "comparator": comparator,
        "reasons": [] if status == "pass" else list(dict.fromkeys(reasons)),
        "detail": detail,
    }


def _graded(passed: bool, fail_code: str, **fields: Any) -> dict[str, Any]:
    """A criterion whose evidence was present: ``pass`` only for the builtin True."""
    if passed is True:
        return _criterion("pass", **fields)
    return _criterion("fail", reasons=[fail_code], **fields)


def _unit_interval(value: Any) -> Decimal | None:
    number = _decimal_or_none(value)
    if number is None or number < 0 or number > 1:
        return None
    return number


def _dossier_leg_block(dossier: _Checked, leg: str) -> Mapping[str, Any] | None:
    """``holdout_six_gate[leg]`` of a usable, non-dry-run dossier; never the COMBINED block."""
    doc = dossier.source.doc
    if dossier.problem is not None or doc is None or doc.get("dry_run") is not False:
        return None
    holdout = doc.get("holdout_six_gate")
    block = holdout.get(leg) if isinstance(holdout, Mapping) else None
    return block if isinstance(block, Mapping) else None


def _dossier_criteria(
    leg: str, dossier: _Checked, limits: LivePromotionThresholds
) -> dict[str, dict[str, Any]]:
    fields = {
        "dossier_passing": f"holdout_six_gate.{leg}.{{promoted,verdict,gates}}",
        "dossier_dsr": f"holdout_six_gate.{leg}.falsification_*.dsr (worst of three)",
        "dossier_pbo": f"holdout_six_gate.{leg}.{{empirical_pbo,falsification_*.pbo}} (worst)",
    }

    def common(name: str) -> dict[str, Any]:
        return {
            "scope": "LEG",
            "source": DOSSIER_FILE,
            "field": fields[name],
            **_criterion_rule(name, limits),
        }

    block = _dossier_leg_block(dossier, leg)
    if block is None:
        reasons = dossier.reasons() or ["DOSSIER_MISSING"]
        detail = dossier.detail() or f"{DOSSIER_FILE}:LEG_BLOCK_MISSING_OR_DRY_RUN"
        return {
            name: _criterion("missing", reasons=reasons, detail=detail, **common(name))
            for name in fields
        }
    out: dict[str, dict[str, Any]] = {}

    promoted, verdict, gates = block.get("promoted"), block.get("verdict"), block.get("gates")
    if (
        isinstance(promoted, bool)
        and isinstance(verdict, str)
        and isinstance(gates, Mapping)
        and set(gates) == set(DOSSIER_GATES)
        and all(isinstance(flag, bool) for flag in gates.values())
    ):
        gates_passed = sum(1 for flag in gates.values() if flag is True)
        out["dossier_passing"] = _graded(
            promoted is True and verdict == "PROMOTE" and gates_passed == len(DOSSIER_GATES),
            "DOSSIER_NOT_PASSING",
            observed={"promoted": promoted, "verdict": verdict, "gates_passed": gates_passed},
            **common("dossier_passing"),
        )
    else:
        out["dossier_passing"] = _criterion(
            "missing",
            reasons=["DOSSIER_MISSING"],
            detail=f"{DOSSIER_FILE}:LEG_VERDICT_MALFORMED",
            **common("dossier_passing"),
        )

    cost_blocks = [block.get(name) for name in DOSSIER_COST_BLOCKS]
    dsr_values = [
        _unit_interval(item.get("dsr")) if isinstance(item, Mapping) else None
        for item in cost_blocks
    ]
    dsr_known = [value for value in dsr_values if value is not None]
    if len(dsr_known) == len(DOSSIER_COST_BLOCKS):
        worst_dsr = min(dsr_known)
        out["dossier_dsr"] = _graded(
            worst_dsr >= Decimal(str(limits.dsr_min)),
            "DSR_BELOW_MIN",
            observed=worst_dsr,
            **common("dossier_dsr"),
        )
    else:
        out["dossier_dsr"] = _criterion(
            "missing", reasons=["DSR_EVIDENCE_MISSING"], **common("dossier_dsr")
        )

    pbo_values = [_unit_interval(block.get("empirical_pbo"))] + [
        _unit_interval(item.get("pbo")) if isinstance(item, Mapping) else None
        for item in cost_blocks
    ]
    pbo_known = [value for value in pbo_values if value is not None]
    if len(pbo_known) == len(pbo_values):
        worst_pbo = max(pbo_known)
        out["dossier_pbo"] = _graded(
            worst_pbo < Decimal(str(limits.pbo_max)),
            "PBO_ABOVE_MAX",
            observed=worst_pbo,
            **common("dossier_pbo"),
        )
    else:
        out["dossier_pbo"] = _criterion(
            "missing", reasons=["PBO_EVIDENCE_MISSING"], **common("dossier_pbo")
        )
    return out


def leg_of_entry(side: Any, origin: Any) -> str | None:
    """Strategy leg of an entry by side and the ``LEG_ENTRY_ORIGINS`` allow-list, else None.

    The side alone never decides. An inverse-ETF hedge is a ``LONG`` order that belongs to
    neither leg, so its paper round trips prove nothing for the LONG leg and it has no merit of
    its own to enter live on. Callers that ask ``live_entry_leg_merit`` for an entry use this
    mapping.
    """
    if not isinstance(side, str) or side not in LEG_ENTRY_ORIGINS or not isinstance(origin, str):
        return None
    base = origin
    for suffix in _ORIGIN_SUFFIXES:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return side if base in LEG_ENTRY_ORIGINS[side] else None


def _row_leg(row: Mapping[str, Any]) -> str | None:
    return leg_of_entry(row.get("side"), row.get("origin"))


# --- Ledgers: identity, duplication and the merged view -------------------------------------------


@dataclass(frozen=True)
class _Identity:
    """What tells one ledger from another. None of these values is ever written to a record."""

    name: str
    sha256: str | None
    account_digest: str | None
    lane_ids: frozenset[str]
    order_ids: frozenset[str]
    problem: str | None  # None = the ledger describes exactly one broker account and one lane


def _ledger_identity(source: _Source) -> _Identity | None:
    """Identity of a ledger that could be opened; None when it could not."""
    tables = source.tables
    if not source.found or tables is None:
        return None
    problems: list[str] = []
    fuse = tables.get("paper_daily_fuse")
    digests = {row.get("account_digest") for row in fuse.rows} if fuse is not None else set()
    digest = next(iter(digests)) if len(digests) == 1 else None
    if not _is_sha256_hex(digest):
        digest = None
        problems.append("BROKER_ACCOUNT_NOT_SINGLE_VALUED")
    fills = tables.get("paper_tca_fills")
    lanes: set[Any] = set()
    if fills is not None:
        lanes = {row.get("lane_id") for row in fills.rows}
        if "lane_id" not in fills.columns:
            problems.append("LANE_ID_COLUMN_ABSENT")
        elif len(lanes) > 1 or not all(isinstance(lane, str) and lane for lane in lanes):
            problems.append("LANE_ID_NOT_SINGLE_VALUED")
    entries = tables.get("paper_entry_decisions")
    order_ids = (
        {row.get("client_order_id") for row in entries.rows} if entries is not None else set()
    )
    return _Identity(
        name=source.name,
        sha256=source.sha256,
        account_digest=digest,
        lane_ids=frozenset(lane for lane in lanes if isinstance(lane, str) and lane),
        order_ids=frozenset(item for item in order_ids if isinstance(item, str) and item),
        problem=";".join(problems) or None,
    )


def _shared_identities(identities: Sequence[_Identity]) -> dict[str, list[str]]:
    """Per ledger, how it overlaps another one (the kind of overlap, never the value)."""
    out: dict[str, list[str]] = {}
    for index, one in enumerate(identities):
        for other in identities[index + 1 :]:
            kinds: list[str] = []
            if one.sha256 is not None and one.sha256 == other.sha256:
                kinds.append("FILE_SHA256")
            if one.account_digest is not None and one.account_digest == other.account_digest:
                kinds.append("BROKER_ACCOUNT")
            if one.lane_ids & other.lane_ids:
                kinds.append("LANE_ID")
            if one.order_ids & other.order_ids:
                kinds.append(f"{len(one.order_ids & other.order_ids)}_CLIENT_ORDER_IDS")
            for kind in kinds:
                out.setdefault(one.name, []).append(f"SHARES_{kind}_WITH:{other.name}")
                out.setdefault(other.name, []).append(f"SHARES_{kind}_WITH:{one.name}")
    return out


@dataclass(frozen=True)
class _LedgerView:
    """The rows of a set of ledgers, or the reasons they cannot be used.

    ``merged`` is False when a file could not be used at all (nothing was read into the view).
    When it is True and ``reasons`` is not empty, the files were read but they are duplicated or
    carry no single identity; the columns are still reported so each gap gets its own code.
    """

    reasons: tuple[str, ...]
    detail: str | None
    merged: bool = False
    entry_columns: frozenset[str] = frozenset()
    entries: tuple[Mapping[str, Any], ...] = ()
    fill_columns: frozenset[str] | None = None  # None when a ledger has no fills table
    fills: tuple[Mapping[str, Any], ...] = ()
    fuse_columns: frozenset[str] | None = None  # None when a ledger has no fuse table
    fuse: Mapping[str, tuple[Mapping[str, Any], ...]] = field(default_factory=dict)
    account_digests: Mapping[str, str | None] = field(default_factory=dict)


def _merge_tables(
    tables: Sequence[tuple[str, _Table | None]],
) -> tuple[frozenset[str] | None, tuple[Mapping[str, Any], ...]]:
    """Columns present in every ledger and all rows, each tagged with its ledger file name.

    No columns (and no rows) when a ledger lacks the table.
    """
    present = [(name, table) for name, table in tables if table is not None]
    if not tables or len(present) != len(tables):
        return None, ()
    columns = frozenset.intersection(*(table.columns for _, table in present))
    rows = tuple({**row, _LEDGER_KEY: name} for name, table in present for row in table.rows)
    return columns, rows


def _ledger_view(
    ledgers: Sequence[_Checked],
    *,
    kind: str,
    identities: Mapping[str, _Identity],
    shared: Mapping[str, Sequence[str]],
) -> _LedgerView:
    reasons = list(dict.fromkeys(code for ledger in ledgers for code in ledger.reasons()))
    details = [text for ledger in ledgers if (text := ledger.detail()) is not None]
    if reasons or not ledgers:
        return _LedgerView(
            tuple(reasons) or (f"{kind}_LEDGER_MISSING",), "; ".join(details) or None
        )
    per_ledger = [(ledger.source.name, ledger.source.tables or {}) for ledger in ledgers]
    for name, _ in per_ledger:
        identity = identities.get(name)
        if identity is None or identity.problem is not None:
            reasons.append(f"{kind}_LEDGER_IDENTITY_MISSING")
            details.append(f"{name}:{'UNREADABLE' if identity is None else identity.problem}")
        if shared.get(name):
            reasons.append(f"{kind}_LEDGER_DUPLICATED")
            details.extend(f"{name}:{text}" for text in shared[name])
    entry_columns, entries = _merge_tables(
        [(name, tables.get("paper_entry_decisions")) for name, tables in per_ledger]
    )
    fill_columns, fills = _merge_tables(
        [(name, tables.get("paper_tca_fills")) for name, tables in per_ledger]
    )
    fuse_tables = [(name, tables.get("paper_daily_fuse")) for name, tables in per_ledger]
    fuse_columns, _ = _merge_tables(fuse_tables)
    return _LedgerView(
        reasons=tuple(dict.fromkeys(reasons)),
        detail="; ".join(details) or None,
        merged=True,
        entry_columns=entry_columns or frozenset(),
        entries=entries,
        fill_columns=fill_columns,
        fills=fills,
        fuse_columns=fuse_columns,
        fuse={name: table.rows for name, table in fuse_tables if table is not None},
        account_digests={
            name: identity.account_digest
            for name, _ in per_ledger
            if (identity := identities.get(name)) is not None
        },
    )


def _fills_linked(view: _LedgerView) -> bool:
    return view.fill_columns is not None and {
        "entry_client_order_id",
        "implementation_shortfall_bps",
    } <= set(view.fill_columns)


def _shortfall_by_entry(view: _LedgerView) -> dict[tuple[Any, str], list[Decimal | None]]:
    """Shortfall of every fill, keyed by (ledger, entry id): a fill never crosses ledgers."""
    out: dict[tuple[Any, str], list[Decimal | None]] = {}
    for fill in view.fills:
        entry_id = fill.get("entry_client_order_id")
        if isinstance(entry_id, str) and entry_id:
            out.setdefault((fill.get(_LEDGER_KEY), entry_id), []).append(
                _decimal_or_none(fill.get("implementation_shortfall_bps"))
            )
    return out


def _round_trip_shortfalls(
    round_trips: Sequence[Mapping[str, Any]],
    by_entry: Mapping[tuple[Any, str], list[Decimal | None]],
) -> list[list[Decimal] | None]:
    """Per round trip, its fills' shortfalls; None when no fill, or a non-numeric one, is linked."""
    out: list[list[Decimal] | None] = []
    for row in round_trips:
        entry_id = row.get("client_order_id")
        linked = (
            by_entry.get((row.get(_LEDGER_KEY), entry_id), []) if isinstance(entry_id, str) else []
        )
        values = [value for value in linked if value is not None]
        out.append(values if values and len(values) == len(linked) else None)
    return out


def _leg_rows(
    view: _LedgerView,
    leg: str,
    *,
    authority: str,
    bound_to: str | None,
    floor: datetime | None,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """(attributable rows, completed round trips oldest first) of one leg and entry authority.

    Evidence is dated by its rows: with a ``floor`` (the leg's latest demotion) only rows
    decided after it are attributable, and a row without a decision time is never one. A round
    trip is a row that closed, at or after its decision, with a harness close cause; an open
    position, a cancelled entry and a manual close are not round trips.
    """
    rows: list[Mapping[str, Any]] = []
    dated: list[tuple[datetime, str, Mapping[str, Any]]] = []
    for row in view.entries:
        if _row_leg(row) != leg or row.get("entry_authority") != authority:
            continue
        if bound_to is not None and row.get("dossier_hypothesis_id") != bound_to:
            continue
        decided = _from_epoch_us(row.get("decided_at_us"))
        if decided is None or (floor is not None and not decided > floor):
            continue
        rows.append(row)
        closed = _from_epoch_us(row.get("closed_at_us"))
        if (
            closed is not None
            and closed >= decided
            and row.get("close_cause") in ROUND_TRIP_CLOSE_CAUSES
        ):
            dated.append((decided, str(row.get("client_order_id")), row))
    dated.sort(key=lambda item: (item[0], item[1]))
    return rows, [row for _, _, row in dated]


def _counted_days(round_trips: Iterable[Mapping[str, Any]]) -> dict[str, set[date]]:
    """Per ledger, the ET calendar days its counted round trips were decided on."""
    out: dict[str, set[date]] = {}
    for row in round_trips:
        decided = _from_epoch_us(row.get("decided_at_us"))
        if decided is not None:
            out.setdefault(str(row.get(_LEDGER_KEY)), set()).add(_calendar_day(decided))
    return out


def _paper_criteria(
    leg: str,
    view: _LedgerView,
    dossier: _Checked,
    limits: LivePromotionThresholds,
    floor: datetime | None,
) -> tuple[dict[str, dict[str, Any]], int | None, list[Mapping[str, Any]]]:
    """The paper-ledger criteria of one leg, the count of rows that belong to no leg, and the
    round trips that were counted (empty when nothing could be counted)."""
    ledgers = "+".join(PAPER_LEDGERS.values())
    entry = "paper_entry_decisions"
    where = f"side={leg}, origin in LEG_ENTRY_ORIGINS, entry_authority={PAPER_ENTRY_AUTHORITY}"
    if floor is not None:
        where += ", decided_at > demoted_at"
    fields = {
        "dossier_leg_binding": f"{entry}.dossier_hypothesis_id == dossier.hypothesis_id",
        "paper_round_trips": f"{entry} [{where}] closed_at, close_cause",
        "paper_sessions": f"{entry} [{where}] distinct ET trading date of decided_at",
        "paper_regimes": f"{entry} [{where}] distinct macro_regime, present on every round trip",
        "paper_shortfall": (
            "paper_tca_fills.implementation_shortfall_bps via entry_client_order_id: mean of all "
            "round trips and mean of the latest min_round_trips"
        ),
        "paper_supervised_exits": f"{entry} [{where}] close_cause={SUPERVISED_EXIT_CLOSE_CAUSE}",
    }

    def common(name: str) -> dict[str, Any]:
        return {
            "scope": "LEG",
            "source": ledgers,
            "field": fields[name],
            **_criterion_rule(name, limits),
        }

    def missing(name: str, reasons: Iterable[str], detail: str | None = None) -> dict[str, Any]:
        return _criterion("missing", reasons=reasons, detail=detail, **common(name))

    if view.reasons:
        return {name: missing(name, view.reasons, view.detail) for name in fields}, None, []

    base: list[str] = []
    if "entry_authority" not in view.entry_columns:
        base.append("ENTRY_AUTHORITY_EVIDENCE_MISSING")
    close_gap = [] if "close_cause" in view.entry_columns else ["CLOSE_CAUSE_EVIDENCE_MISSING"]

    doc = dossier.source.doc if dossier.problem is None else None
    hypothesis = doc.get("hypothesis_id") if doc is not None else None
    hypothesis = hypothesis if isinstance(hypothesis, str) and hypothesis else None
    bindable = "dossier_hypothesis_id" in view.entry_columns and hypothesis is not None
    rows, round_trips = _leg_rows(
        view,
        leg,
        authority=PAPER_ENTRY_AUTHORITY,
        bound_to=hypothesis if bindable else None,
        floor=floor,
    )
    unattributed = sum(1 for row in view.entries if _row_leg(row) is None)
    out: dict[str, dict[str, Any]] = {}

    binding_gaps = [*dossier.reasons(), *base]
    if not bindable:
        binding_gaps.append("DOSSIER_LEG_BINDING_MISSING")
    if binding_gaps:
        out["dossier_leg_binding"] = missing("dossier_leg_binding", binding_gaps)
    elif not rows:
        out["dossier_leg_binding"] = missing(
            "dossier_leg_binding", ["DOSSIER_LEG_BINDING_MISSING"], "NO_ROWS_BOUND_TO_DOSSIER"
        )
    else:
        out["dossier_leg_binding"] = _criterion(
            "pass",
            observed={"hypothesis_id": hypothesis, "bound_rows": len(rows)},
            **common("dossier_leg_binding"),
        )

    counted_gaps = [*base, *close_gap]
    if counted_gaps:
        out["paper_round_trips"] = missing("paper_round_trips", counted_gaps)
        out["paper_sessions"] = missing("paper_sessions", counted_gaps)
        out["paper_supervised_exits"] = missing("paper_supervised_exits", counted_gaps)
    else:
        out["paper_round_trips"] = _graded(
            len(round_trips) >= limits.min_round_trips,
            "ROUND_TRIPS_BELOW_MIN",
            observed=len(round_trips),
            **common("paper_round_trips"),
        )
        decided = [_from_epoch_us(row.get("decided_at_us")) for row in round_trips]
        sessions = {_trade_date(moment) for moment in decided if moment is not None}
        out["paper_sessions"] = _graded(
            len(sessions) >= limits.min_sessions,
            "SESSIONS_BELOW_MIN",
            observed=len(sessions),
            **common("paper_sessions"),
        )
        supervised = sum(
            1 for row in round_trips if row.get("close_cause") == SUPERVISED_EXIT_CLOSE_CAUSE
        )
        out["paper_supervised_exits"] = _graded(
            supervised >= limits.min_supervised_exits,
            "SUPERVISED_EXITS_BELOW_MIN",
            observed=supervised,
            **common("paper_supervised_exits"),
        )

    # Every counted round trip must say which regime it was entered in: one that does not
    # could be hiding the regime the leg has never traded.
    regime_gaps = [*counted_gaps]
    if "macro_regime" not in view.entry_columns:
        regime_gaps.append("REGIME_EVIDENCE_MISSING")
    regimes = [
        value
        for row in round_trips
        if (value := _int_or_none(row.get("macro_regime"))) is not None and value in MACRO_REGIMES
    ]
    if regime_gaps:
        out["paper_regimes"] = missing("paper_regimes", regime_gaps)
    elif len(regimes) != len(round_trips):
        out["paper_regimes"] = missing(
            "paper_regimes",
            ["REGIME_EVIDENCE_MISSING"],
            f"MACRO_REGIME_ON_{len(regimes)}_OF_{len(round_trips)}_ROUND_TRIPS",
        )
    else:
        out["paper_regimes"] = _graded(
            len(set(regimes)) >= limits.min_regimes,
            "REGIMES_BELOW_MIN",
            observed=len(set(regimes)),
            **common("paper_regimes"),
        )

    shortfall_gaps = [*counted_gaps]
    if not _fills_linked(view):
        shortfall_gaps.append("SHORTFALL_EVIDENCE_MISSING")
    if shortfall_gaps:
        out["paper_shortfall"] = missing("paper_shortfall", shortfall_gaps)
    else:
        per_trip = _round_trip_shortfalls(round_trips, _shortfall_by_entry(view))
        covered = [values for values in per_trip if values is not None]
        if not round_trips or len(covered) != len(round_trips):
            out["paper_shortfall"] = missing(
                "paper_shortfall",
                ["SHORTFALL_EVIDENCE_MISSING"],
                f"FILLS_LINKED_FOR_{len(covered)}_OF_{len(round_trips)}_ROUND_TRIPS",
            )
        else:
            # Old round trips never age out of the overall mean, so the latest
            # ``min_round_trips`` of them have to be inside the bound on their own as well.
            trailing = covered[-limits.min_round_trips :]
            mean_bps = _mean([value for values in covered for value in values])
            trailing_bps = _mean([value for values in trailing for value in values])
            out["paper_shortfall"] = _graded(
                mean_bps <= limits.max_shortfall_bps and trailing_bps <= limits.max_shortfall_bps,
                "SHORTFALL_ABOVE_MAX",
                observed={
                    "mean_bps": mean_bps.quantize(_DISPLAY_QUANTUM),
                    "trailing_mean_bps": trailing_bps.quantize(_DISPLAY_QUANTUM),
                    "trailing_round_trips": len(trailing),
                },
                **common("paper_shortfall"),
            )
    return out, unattributed, [] if counted_gaps else round_trips


def _breaker_criterion(
    view: _LedgerView, counted: Mapping[str, set[date]], limits: LivePromotionThresholds
) -> dict[str, Any]:
    """Zero defect-driven daily-fuse trips (ADR-0020 Stage 2). Account-level, so harness scope.

    A fuse that says nothing is not a fuse that never tripped: every ledger needs a fuse row
    for each day one of its counted round trips was decided on.
    """
    common: dict[str, Any] = {
        "scope": "HARNESS",
        "source": "+".join(PAPER_LEDGERS.values()),
        "field": "paper_daily_fuse.{day,tripped,trip_cause} per ledger, for every counted day",
        **_criterion_rule("breaker_trips", limits),
    }
    gaps = list(view.reasons)
    columns = view.fuse_columns
    if view.merged and (columns is None or not {"day", "tripped"} <= set(columns)):
        gaps.append("BREAKER_EVIDENCE_MISSING")
    if gaps:
        return _criterion("missing", reasons=gaps, detail=view.detail, **common)
    rows = [row for ledger_rows in view.fuse.values() for row in ledger_rows]
    if not all(
        isinstance(row.get("tripped"), bool) and isinstance(row.get("day"), date) for row in rows
    ):
        return _criterion(
            "missing",
            reasons=["BREAKER_EVIDENCE_MISSING"],
            detail="DAY_OR_TRIPPED_MALFORMED",
            **common,
        )
    uncovered = sum(
        len(days - {row.get("day") for row in view.fuse.get(name, ())})
        for name, days in counted.items()
    )
    if uncovered:
        return _criterion(
            "missing",
            reasons=["BREAKER_EVIDENCE_MISSING"],
            observed={"fuse_sessions": len(rows), "sessions_without_fuse_row": uncovered},
            detail=f"NO_FUSE_ROW_FOR_{uncovered}_COUNTED_LEDGER_DAYS",
            **common,
        )
    trips = [row for row in rows if row.get("tripped") is True]
    causes = [row.get("trip_cause") for row in trips]
    if not all(isinstance(cause, str) and cause for cause in causes):
        return _criterion(
            "missing",
            reasons=["BREAKER_TRIP_CAUSE_MISSING"],
            observed={"fuse_sessions": len(rows), "tripped_sessions": len(trips)},
            **common,
        )
    defects = sum(1 for cause in causes if cause not in NON_DEFECT_BREAKER_CAUSES)
    return _graded(
        defects == 0,
        "BREAKER_DEFECT_TRIP",
        observed={
            "fuse_sessions": len(rows),
            "tripped_sessions": len(trips),
            "defect_trips": defects,
            "sessions_without_fuse_row": 0,
        },
        **common,
    )


def _reconciliation_criterion(
    receipt: _Checked, view: _LedgerView, limits: LivePromotionThresholds
) -> dict[str, Any]:
    """The DuckDB-to-broker reconciliation receipt covers every paper ledger and is clean.

    Each account entry names the broker account it reconciled by ``account_sha256`` (SHA-256 of
    the account id text). It has to be the account of the ledger file read for that lane, so a
    receipt for one account cannot vouch for a ledger that describes another.
    """
    common: dict[str, Any] = {
        "scope": "HARNESS",
        "source": RECONCILIATION_FILE,
        "field": (
            "all_reconciled, accounts.<lane>.{status,unreconciled_delta_count,account_sha256} "
            "== sha256(paper_daily_fuse.account_id) of that lane's ledger"
        ),
        **_criterion_rule("reconciliation_receipt", limits),
    }
    doc = receipt.source.doc
    if receipt.problem is not None or doc is None:
        return _criterion(
            "missing",
            reasons=receipt.reasons() or ["RECONCILIATION_RECEIPT_MISSING"],
            detail=receipt.detail(),
            **common,
        )
    accounts = doc.get("accounts")
    all_reconciled = doc.get("all_reconciled")
    rows = {
        lane: accounts.get(lane) if isinstance(accounts, Mapping) else None
        for lane in PAPER_LEDGERS
    }
    well_formed = isinstance(all_reconciled, bool) and all(
        isinstance(row, Mapping)
        and isinstance(row.get("status"), str)
        and _int_or_none(row.get("unreconciled_delta_count")) is not None
        for row in rows.values()
    )
    if not well_formed:
        return _criterion(
            "missing",
            reasons=["RECONCILIATION_RECEIPT_MISSING"],
            detail=f"{RECONCILIATION_FILE}:MALFORMED_OR_ACCOUNT_NOT_COVERED",
            **common,
        )
    bound = sum(
        1
        for lane, ledger in PAPER_LEDGERS.items()
        if isinstance(row := rows[lane], Mapping)
        and _is_sha256_hex(row.get("account_sha256"))
        and row.get("account_sha256") == view.account_digests.get(ledger)
    )
    if view.reasons or bound != len(PAPER_LEDGERS):
        return _criterion(
            "missing",
            reasons=["RECONCILIATION_LEDGER_BINDING_MISSING"],
            observed={"bound_accounts": bound},
            detail=view.detail or f"{RECONCILIATION_FILE}:ACCOUNT_SHA256_ABSENT_OR_NOT_THE_LEDGERS",
            **common,
        )
    clean = sum(
        1
        for row in rows.values()
        if isinstance(row, Mapping)
        and row.get("status") == RECONCILED_STATUS
        and row.get("unreconciled_delta_count") == 0
    )
    return _graded(
        all_reconciled is True and clean == len(PAPER_LEDGERS),
        "RECONCILIATION_NOT_CLEAN",
        observed={
            "all_reconciled": all_reconciled,
            "clean_accounts": clean,
            "bound_accounts": bound,
        },
        **common,
    )


def _stage4_criterion(stage4: _Checked, limits: LivePromotionThresholds) -> dict[str, Any]:
    common: dict[str, Any] = {
        "scope": "HARNESS",
        "source": STAGE4_FILE,
        "field": "all_passed, all_drills_passed, drills.*.{passed,all_passed}",
        **_criterion_rule("stage4_drills", limits),
    }
    doc = stage4.source.doc
    if stage4.problem is not None or doc is None:
        return _criterion(
            "missing",
            reasons=stage4.reasons() or ["STAGE4_DRILLS_MISSING"],
            detail=stage4.detail(),
            **common,
        )
    drills = doc.get("drills")
    flags: dict[str, Any] = {
        "all_passed": doc.get("all_passed"),
        "all_drills_passed": doc.get("all_drills_passed"),
    }
    for drill, key in STAGE4_DRILL_FLAGS:
        block = drills.get(drill) if isinstance(drills, Mapping) else None
        flags[f"drills.{drill}.{key}"] = block.get(key) if isinstance(block, Mapping) else None
    if not all(isinstance(flag, bool) for flag in flags.values()):
        return _criterion(
            "missing",
            reasons=["STAGE4_DRILLS_MISSING"],
            detail=f"{STAGE4_FILE}:DRILL_FLAGS_MALFORMED",
            **common,
        )
    passing = sum(1 for flag in flags.values() if flag is True)
    return _graded(
        passing == len(flags),
        "STAGE4_DRILLS_NOT_PASSING",
        observed={"flags_true": passing, "failing": sorted(k for k, v in flags.items() if not v)},
        **common,
    )


def _short_live_criterion(
    view: _LedgerView, limits: LivePromotionThresholds, floor: datetime | None
) -> dict[str, Any]:
    """Operator-confirmed live short round trips, each inside the shortfall bound."""
    code = "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE"
    need = limits.short_min_live_confirmed_round_trips
    common: dict[str, Any] = {
        "scope": "LEG",
        "source": LIVE_LEDGER,
        "field": (
            f"paper_entry_decisions [side=SHORT, entry_authority={LIVE_CONFIRMED_ENTRY_AUTHORITY}]"
            " close_cause + paper_tca_fills.implementation_shortfall_bps via entry_client_order_id"
            f" (each round trip's mean <= {format(limits.max_shortfall_bps, 'f')} bps)"
        ),
        **_criterion_rule(_SHORT_ONLY_CRITERION, limits),
    }
    if need == 0:
        return _criterion(
            "pass",
            observed={"round_trips": 0, "inside_shortfall_bound": 0},
            detail="NOT_REQUIRED",
            **common,
        )
    if view.reasons:
        return _criterion("missing", reasons=[*view.reasons, code], detail=view.detail, **common)
    absent = sorted({"entry_authority", "close_cause"} - set(view.entry_columns))
    if not _fills_linked(view):
        absent.append("paper_tca_fills.entry_client_order_id")
    if absent:
        return _criterion(
            "missing", reasons=[code], detail="COLUMNS_ABSENT:" + ",".join(absent), **common
        )
    _, round_trips = _leg_rows(
        view, "SHORT", authority=LIVE_CONFIRMED_ENTRY_AUTHORITY, bound_to=None, floor=floor
    )
    per_trip = _round_trip_shortfalls(round_trips, _shortfall_by_entry(view))
    inside = sum(
        1 for values in per_trip if values is not None and _mean(values) <= limits.max_shortfall_bps
    )
    return _graded(
        inside >= need,
        code,
        observed={"round_trips": len(round_trips), "inside_shortfall_bound": inside},
        **common,
    )


# --- Promotion state: reading the prior state and the latch ---------------------------------------


@dataclass(frozen=True)
class _PriorLeg:
    """One leg of a persisted promotion state.

    ``problem`` is ``PRIOR_STATE_MISSING`` (no state was passed) or ``PRIOR_STATE_MALFORMED``;
    either is unknown history and is treated as a fresh demotion.
    """

    status: str = "PAPER"
    promoted_at: datetime | None = None
    demoted_at: datetime | None = None
    step_index: int = 0
    step_entered_at: datetime | None = None
    demotion_reasons: tuple[str, ...] = ()
    problem: str | None = None

    def floor(self) -> datetime | None:
        """The leg's evidence floor: only paper rows decided after its latest demotion count."""
        return self.demoted_at if self.problem is None else None


def _prior_leg(prior_state: Any, leg: str, now: datetime) -> _PriorLeg:
    if prior_state is None:
        return _PriorLeg(status="DEMOTED", problem="PRIOR_STATE_MISSING")
    bad = _PriorLeg(status="DEMOTED", problem="PRIOR_STATE_MALFORMED")
    if (
        not isinstance(prior_state, Mapping)
        or prior_state.get("schema") != LIVE_PROMOTION_STATE_SCHEMA
    ):
        return bad
    legs = prior_state.get("legs")
    entry = legs.get(leg) if isinstance(legs, Mapping) else None
    if not isinstance(entry, Mapping):
        return bad
    status = entry.get("status")
    step = entry.get("capital_step")
    step_index = _int_or_none(step.get("index")) if isinstance(step, Mapping) else None
    stamps = {
        key: _parse_utc(entry.get(key))
        for key in ("promoted_at", "demoted_at", "capital_step_entered_at")
    }
    promoted_at, demoted_at = stamps["promoted_at"], stamps["demoted_at"]
    step_entered_at = stamps["capital_step_entered_at"]
    reasons = entry.get("demotion_reasons")
    if (
        status not in ("PAPER", "PROMOTED", "DEMOTED")
        or step_index is None
        or not 0 <= step_index < len(CAPITAL_LADDER)
        or not isinstance(reasons, list)
        or not all(isinstance(code, str) for code in reasons)
        or any(entry.get(key) is not None and moment is None for key, moment in stamps.items())
        or any(
            moment is not None and moment - now > _FUTURE_SKEW_TOLERANCE
            for moment in stamps.values()
        )
        or (status == "PROMOTED" and (promoted_at is None or step_entered_at is None))
        or (status == "DEMOTED" and demoted_at is None)
        or (status != "PROMOTED" and step_index != 0)
    ):
        return bad
    return _PriorLeg(
        status=str(status),
        promoted_at=promoted_at,
        demoted_at=demoted_at,
        step_index=step_index,
        step_entered_at=step_entered_at,
        demotion_reasons=tuple(reasons),
    )


def _cooloff_sessions_completed(demoted_at: datetime | None, now: datetime) -> int | None:
    if demoted_at is None:
        return None
    return _completed_sessions_between(_trade_date(demoted_at), _trade_date(now))


def _latch_criterion(
    prior: _PriorLeg,
    *,
    manifests_oldest_at: datetime | None,
    now: datetime,
    limits: LivePromotionThresholds,
) -> dict[str, Any]:
    """The record's part of the latch: cool-off served and every manifest newer than the demotion.

    The other part of "evidence newer than the demotion" is in the paper criteria themselves,
    which count only rows decided after the demotion (``_PriorLeg.floor``).
    """
    common: dict[str, Any] = {
        "scope": "LEG",
        "source": "prior_state",
        "field": (
            "legs.<LEG>.{status,demoted_at} + completed XNYS sessions since + generated_at_utc "
            "of every manifest"
        ),
        **_criterion_rule(_LATCH_CRITERION, limits),
    }
    if prior.problem == "PRIOR_STATE_MISSING":
        return _criterion(
            "missing",
            reasons=["PROMOTION_STATE_MISSING"],
            detail="PRIOR_STATE_NOT_PASSED",
            **common,
        )
    if prior.problem is not None:
        return _criterion("fail", reasons=["DEMOTION_LATCHED"], detail=prior.problem, **common)
    if prior.status != "DEMOTED":
        return _criterion("pass", observed={"latched": False}, **common)
    completed = _cooloff_sessions_completed(prior.demoted_at, now)
    newer = (
        prior.demoted_at is not None
        and manifests_oldest_at is not None
        and manifests_oldest_at > prior.demoted_at
    )
    released = (
        completed is not None and completed >= limits.demotion_cooloff_sessions and newer is True
    )
    observed = {
        "latched": True,
        "demoted_at": prior.demoted_at,
        "released": released,
        "cooloff_sessions_completed": completed,
        "manifests_newer_than_demotion": newer,
    }
    return _graded(released, "DEMOTION_LATCHED", observed=observed, **common)


def _latch_holds(observed: Any, threshold: Any) -> bool:
    """Re-check of a ``demotion_latch`` criterion that claims a pass."""
    if not isinstance(observed, Mapping) or not isinstance(threshold, Mapping):
        return False
    if observed.get("latched") is False:
        return True
    return (
        observed.get("latched") is True
        and observed.get("released") is True
        and observed.get("manifests_newer_than_demotion") is True
        and _holds(
            observed.get("cooloff_sessions_completed"), ">=", threshold.get("cooloff_sessions")
        )
    )


# --- The record -----------------------------------------------------------------------------------


def _expected_criteria(leg: str) -> tuple[str, ...]:
    extra = (_SHORT_ONLY_CRITERION,) if leg == "SHORT" else ()
    return (*_CRITERIA_BEFORE_SHORT, *extra, _LATCH_CRITERION)


def _inputs_hash(inputs: Sequence[Mapping[str, Any]]) -> str:
    """SHA-256 over the sorted ``name=sha256`` lines of the inputs (``MISSING`` when not found)."""
    lines = sorted(
        f"{item['name']}={item['sha256'] if item['found'] is True else 'MISSING'}"
        for item in inputs
    )
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def _record_digest(record: Any) -> str | None:
    """SHA-256 of the whole record body (everything but ``record_sha256`` itself)."""
    if not isinstance(record, Mapping):
        return None
    return _canonical_sha256({key: item for key, item in record.items() if key != "record_sha256"})


def _oldest(moments: Sequence[datetime | None]) -> datetime | None:
    known = [moment for moment in moments if moment is not None]
    return min(known) if known and len(known) == len(moments) else None


def _evaluate(
    evidence: _Evidence, limits: LivePromotionThresholds, now: datetime, prior_state: Any
) -> dict[str, Any]:
    """Pure evaluation of already-read evidence into the merit record."""
    max_age = timedelta(hours=limits.evidence_max_age_hours)
    dossier = _check_json(evidence.dossier, DOSSIER_SCHEMA, "DOSSIER_MISSING", now, max_age)
    stage4 = _check_json(evidence.stage4, STAGE4_SCHEMA, "STAGE4_DRILLS_MISSING", now, max_age)
    receipt = _check_json(
        evidence.reconciliation,
        RECONCILIATION_SCHEMA,
        "RECONCILIATION_RECEIPT_MISSING",
        now,
        max_age,
    )
    paper = [
        _check_ledger(ledger, "PAPER_LEDGER_MISSING", now, max_age)
        for ledger in evidence.paper_ledgers
    ]
    live = _check_ledger(evidence.live_ledger, "LIVE_LEDGER_MISSING", now, max_age)
    identities = {
        source.name: identity
        for source in (*evidence.paper_ledgers, evidence.live_ledger)
        if (identity := _ledger_identity(source)) is not None
    }
    shared = _shared_identities(list(identities.values()))
    view = _ledger_view(paper, kind="PAPER", identities=identities, shared=shared)
    live_view = _ledger_view([live], kind="LIVE", identities=identities, shared=shared)
    manifest_times = [dossier.evidence_at, stage4.evidence_at, receipt.evidence_at]
    common_times = manifest_times + [ledger.evidence_at for ledger in paper]

    priors = {leg: _prior_leg(prior_state, leg, now) for leg in LEGS}
    paper_found: dict[str, tuple[dict[str, dict[str, Any]], int | None]] = {}
    counted: dict[str, set[date]] = {}
    for leg in LEGS:
        criteria, unattributed, round_trips = _paper_criteria(
            leg, view, dossier, limits, priors[leg].floor()
        )
        paper_found[leg] = (criteria, unattributed)
        for name, days in _counted_days(round_trips).items():
            counted.setdefault(name, set()).update(days)
    harness = {
        "breaker_trips": _breaker_criterion(view, counted, limits),
        "reconciliation_receipt": _reconciliation_criterion(receipt, view, limits),
        "stage4_drills": _stage4_criterion(stage4, limits),
    }

    legs: dict[str, dict[str, Any]] = {}
    for leg in LEGS:
        prior = priors[leg]
        paper_criteria, unattributed = paper_found[leg]
        found = {**_dossier_criteria(leg, dossier, limits), **paper_criteria, **harness}
        times = list(common_times)
        if leg == "SHORT":
            found[_SHORT_ONLY_CRITERION] = _short_live_criterion(live_view, limits, prior.floor())
            if limits.short_min_live_confirmed_round_trips > 0:
                times.append(live.evidence_at)
        evidence_oldest_at = _oldest(times)
        found[_LATCH_CRITERION] = _latch_criterion(
            prior, manifests_oldest_at=_oldest(manifest_times), now=now, limits=limits
        )
        criteria = {name: found[name] for name in _expected_criteria(leg)}
        reasons: list[str] = []
        for name, criterion in criteria.items():
            if criterion["status"] != "pass":
                reasons.extend(criterion["reasons"] or [f"{name.upper()}_NOT_PASSING"])
        if not reasons and evidence_oldest_at is None:
            reasons.append("EVIDENCE_TIMESTAMP_INVALID")  # a pass must rest on dated evidence
        step = CAPITAL_LADDER[
            prior.step_index if prior.problem is None and prior.status == "PROMOTED" else 0
        ]
        legs[leg] = {
            "merit": not reasons
            and all(criterion["status"] == "pass" for criterion in criteria.values()),
            "reasons": list(dict.fromkeys(reasons)),
            "criteria": criteria,
            "capital_step": step.model_dump(mode="json"),
            "evidence_oldest_at": _iso(evidence_oldest_at),
            "evidence_floor": _iso(prior.floor()),
            "unattributed_ledger_rows": unattributed,
        }

    inputs = [source.descriptor() for source in evidence.sources()]
    # The record expires when the oldest evidence behind a passing leg goes stale. With no
    # passing leg the date is informational: the oldest evidence that could be dated at all.
    passing = [
        moment
        for entry in legs.values()
        if entry["merit"] is True and (moment := _parse_utc(entry["evidence_oldest_at"]))
    ]
    dated = passing or [m for m in (*common_times, live.evidence_at) if m is not None]
    valid_until = min(min(dated) + max_age, now + max_age) if dated else now
    record: dict[str, Any] = {
        "schema": LIVE_PROMOTION_MERIT_SCHEMA,
        "computed_at": _iso(now),
        "valid_until": _iso(valid_until),
        "generator": dict(GENERATOR),
        "thresholds": limits.model_dump(mode="json"),
        "inputs": inputs,
        "inputs_hash": _inputs_hash(inputs),
        # The promotion state this record was computed with: ``next_promotion_state`` accepts
        # the record only together with that same state.
        "prior_state_sha256": None if prior_state is None else _canonical_sha256(prior_state),
        "legs": legs,
    }
    record["record_sha256"] = _record_digest(record)
    return record


def compute_live_promotion_merit(
    evidence_dir: Path | str,
    *,
    now_utc: datetime,
    thresholds: LivePromotionThresholds | None = None,
    prior_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute the per-leg paper-merit record (Key A) from the evidence under ``evidence_dir``.

    ``prior_state`` is the persisted result of ``next_promotion_state`` (at the very start, of
    ``initial_promotion_state``). It decides the latch and each leg's evidence floor: a leg with
    a demotion on record counts only paper rows decided after it. ``None`` is not "no history":
    it is an unknown state, the ``demotion_latch`` criterion is ``missing``
    (``PROMOTION_STATE_MISSING``) and no leg has merit. The record is plain JSON data and is
    bound to the state it was computed with (``prior_state_sha256``).
    """
    now = _aware_utc(now_utc)
    if now is None:
        raise ValueError("now_utc must be a timezone-aware datetime")
    limits = _configured(thresholds)
    if limits is None:
        raise TypeError("thresholds must be a LivePromotionThresholds or None")
    return _evaluate(_read_evidence(Path(evidence_dir)), limits, now, prior_state)


def _record_is_well_formed(record: Any, now: datetime, limits: LivePromotionThresholds) -> bool:
    """Schema, generator, validity window, thresholds, inputs and hashes of a record."""
    if not isinstance(record, dict) or record.get("schema") != LIVE_PROMOTION_MERIT_SCHEMA:
        return False
    if record.get("generator") != dict(GENERATOR):
        return False
    computed_at = _parse_utc(record.get("computed_at"))
    valid_until = _parse_utc(record.get("valid_until"))
    if computed_at is None or valid_until is None or not computed_at <= now < valid_until:
        return False
    # A record never outlives the evidence age bound, whatever its ``valid_until`` says.
    if valid_until - computed_at > timedelta(hours=limits.evidence_max_age_hours):
        return False
    # The thresholds are the ones this caller is configured with, not whatever the record echoes.
    if _canonical_json(record.get("thresholds")) != _canonical_json(limits.model_dump(mode="json")):
        return False
    if not _is_sha256_hex(record.get("prior_state_sha256")):
        return False
    inputs = record.get("inputs")
    if not isinstance(inputs, list) or not inputs:
        return False
    names: set[str] = set()
    for item in inputs:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            return False
        found, digest = item.get("found"), item.get("sha256")
        if not isinstance(found, bool) or item["name"] in names:
            return False
        if found and not _is_sha256_hex(digest):
            return False
        if not found and digest is not None:
            return False
        names.add(item["name"])
    if names != _INPUT_NAMES:
        return False
    digest = record.get("inputs_hash")
    if not isinstance(digest, str) or digest != _inputs_hash(inputs):
        return False
    body = record.get("record_sha256")
    return _is_sha256_hex(body) and body == _record_digest(record)


def _criterion_rechecks(name: str, criterion: Any, limits: LivePromotionThresholds) -> bool:
    """A criterion that claims a pass must show an observed value that satisfies the threshold."""
    if not isinstance(criterion, dict) or criterion.get("status") != "pass":
        return False
    if criterion.get("reasons") != []:
        return False
    rule = _criterion_rule(name, limits)
    threshold = _jsonable(rule["threshold"])
    if criterion.get("comparator") != rule["comparator"]:
        return False
    if _canonical_json(criterion.get("threshold")) != _canonical_json(threshold):
        return False
    if name == _LATCH_CRITERION:
        return _latch_holds(criterion.get("observed"), threshold)
    return _holds(criterion.get("observed"), rule["comparator"], threshold)


def _leg_entry_passes(entry: Any, leg: str, limits: LivePromotionThresholds) -> bool:
    if not isinstance(entry, dict) or entry.get("merit") is not True:
        return False
    reasons, criteria, step = entry.get("reasons"), entry.get("criteria"), entry.get("capital_step")
    if not isinstance(reasons, list) or reasons:
        return False
    if not isinstance(criteria, dict) or set(criteria) != set(_expected_criteria(leg)):
        return False
    if not all(_criterion_rechecks(name, item, limits) for name, item in criteria.items()):
        return False
    index = _int_or_none(step.get("index")) if isinstance(step, dict) else None
    return index is not None and 0 <= index < len(CAPITAL_LADDER)


def leg_has_merit(
    record: Any,
    leg: str,
    *,
    now_utc: datetime,
    thresholds: LivePromotionThresholds | None = None,
) -> bool:
    """True only for a well-formed, unexpired record whose ``leg`` entry passes every criterion.

    True is necessary for a live scanner entry, not sufficient: this reads the record alone and
    cannot see a demotion, a hold or an open protection fault. The gate value is
    ``live_entry_leg_merit``.

    ``thresholds`` are the ones the caller is configured with (``None`` = the owner defaults).
    A record computed under any other thresholds returns False, so a record from a test or a
    debug run with loose thresholds is never Key A. Every passing criterion is re-checked:
    its echoed threshold must be the configured one and its observed value must satisfy it.

    Anything else returns False and nothing raises: a wrong schema, a naive or out-of-window
    ``now_utc``, a ``valid_until`` beyond the evidence age bound, an input list that is not
    exactly the expected files, an ``inputs_hash`` or ``record_sha256`` that does not match, a
    truthy non-bool ``merit``, a missing or extra criterion, or any object that is not this
    module's record. The hashes catch a damaged or partly edited record; they are not a
    signature, which is why the gate recomputes the record from evidence at each decision.
    """
    try:
        now = _aware_utc(now_utc)
        limits = _configured(thresholds)
        if now is None or limits is None or not isinstance(leg, str) or leg not in LEGS:
            return False
        if not _record_is_well_formed(record, now, limits):
            return False
        legs = record.get("legs")
        return isinstance(legs, dict) and _leg_entry_passes(legs.get(leg), leg, limits)
    except Exception:  # the gate fails closed on anything a hostile or damaged record can do
        return False


# --- Promotion, latched demotion and the capital ladder -------------------------------------------


@dataclass(frozen=True)
class _LiveRoundTrip:
    order_id: str
    authority: str
    opened_at: datetime
    closed_at: datetime
    shortfall_bps: Decimal
    capital_step: int | None
    protection_fault: bool
    close_cause: str

    def clean(self, max_shortfall_bps: Decimal) -> bool:
        return (
            self.protection_fault is False
            and self.close_cause in ROUND_TRIP_CLOSE_CAUSES
            and self.shortfall_bps <= max_shortfall_bps
        )


@dataclass(frozen=True)
class _LiveLeg:
    round_trips: tuple[_LiveRoundTrip, ...]  # oldest close first
    open_protection_faults: int
    fuse_sessions: tuple[tuple[date, bool], ...]


def _parse_live_round_trip(item: Any, now: datetime) -> _LiveRoundTrip | None:
    if not isinstance(item, Mapping):
        return None
    order_id, authority = item.get("entry_client_order_id"), item.get("entry_authority")
    opened_at, closed_at = _parse_utc(item.get("opened_at")), _parse_utc(item.get("closed_at"))
    shortfall = _decimal_or_none(item.get("shortfall_bps"))
    step = item.get("capital_step")
    fault, cause = item.get("protection_fault"), item.get("close_cause")
    if (
        not isinstance(order_id, str)
        or not order_id
        or not isinstance(authority, str)
        or authority not in LIVE_ENTRY_AUTHORITIES
        or opened_at is None
        or closed_at is None
        or opened_at > closed_at
        or closed_at - now > _FUTURE_SKEW_TOLERANCE
        or shortfall is None
        or not isinstance(fault, bool)
        or not isinstance(cause, str)
    ):
        return None
    if authority == PAPER_ENTRY_AUTHORITY:
        # A tier-3 round trip was sized by a step of the ladder and has to say which.
        step_index = _int_or_none(step)
        if step_index is None or not 0 <= step_index < len(CAPITAL_LADDER):
            return None
    elif step is not None:
        return None  # an operator-confirmed (tier-2) round trip is not on the ladder
    return _LiveRoundTrip(
        order_id=order_id,
        authority=authority,
        opened_at=opened_at,
        closed_at=closed_at,
        shortfall_bps=shortfall,
        capital_step=_int_or_none(step),
        protection_fault=fault,
        close_cause=cause,
    )


def _parse_live(live_evidence: Any, now: datetime) -> dict[str, _LiveLeg] | None:
    """Both legs of the caller's live evidence; None unless it is exactly the documented shape.

    One round trip is one unique ``entry_client_order_id`` across both legs: a per-fill join
    that lists a round trip twice, a ``closed_at`` in the future and a fuse session that has
    not happened yet are malformed evidence, not extra evidence.
    """
    if not isinstance(live_evidence, Mapping):
        return None
    legs, fuse_rows = live_evidence.get("legs"), live_evidence.get("fuse_sessions")
    if not isinstance(legs, Mapping) or set(legs) != set(LEGS) or not isinstance(fuse_rows, list):
        return None
    sessions: dict[date, bool] = {}
    for item in fuse_rows:
        if not isinstance(item, Mapping) or not isinstance(item.get("tripped"), bool):
            return None
        session = item.get("session")
        try:
            day = date.fromisoformat(session) if isinstance(session, str) else None
        except ValueError:
            return None
        if day is None or day in sessions or day > _trade_date(now + _FUTURE_SKEW_TOLERANCE):
            return None
        sessions[day] = item["tripped"]
    fuse_sessions = tuple(sorted(sessions.items()))
    seen: set[str] = set()
    out: dict[str, _LiveLeg] = {}
    for leg in LEGS:
        entry = legs.get(leg)
        if not isinstance(entry, Mapping):
            return None
        raw_trips = entry.get("round_trips")
        faults = _int_or_none(entry.get("open_protection_faults"))
        if not isinstance(raw_trips, list) or faults is None or faults < 0:
            return None
        trips: list[_LiveRoundTrip] = []
        for item in raw_trips:
            trip = _parse_live_round_trip(item, now)
            if trip is None or trip.order_id in seen:
                return None
            seen.add(trip.order_id)
            trips.append(trip)
        out[leg] = _LiveLeg(
            round_trips=tuple(sorted(trips, key=lambda trip: (trip.closed_at, trip.order_id))),
            open_protection_faults=faults,
            fuse_sessions=fuse_sessions,
        )
    return out


def _live_window_triggers(live: _LiveLeg, limits: LivePromotionThresholds) -> list[str]:
    """Live decay triggers that describe the present: they hold a promotion and demote a leg.

    The same predicate decides both, so a leg is never promoted in a call and demoted by the
    next one on unchanged evidence. The shortfall window is the leg's last
    ``LIVE_SHORTFALL_WINDOW_ROUND_TRIPS`` live round trips of either entry authority, so a
    demoted leg (which makes no tier-3 round trip) works its way back inside the bound through
    operator-confirmed ones.
    """
    triggers: list[str] = []
    window = live.round_trips[-LIVE_SHORTFALL_WINDOW_ROUND_TRIPS:]
    if (
        len(window) == LIVE_SHORTFALL_WINDOW_ROUND_TRIPS
        and _mean([trip.shortfall_bps for trip in window]) > limits.max_shortfall_bps
    ):
        triggers.append("LIVE_SHORTFALL_ABOVE_MAX")
    fuse = [tripped for _, tripped in live.fuse_sessions[-LIVE_FUSE_WINDOW_SESSIONS:]]
    if sum(1 for tripped in fuse if tripped) >= LIVE_FUSE_TRIPS_TO_DEMOTE:
        triggers.append("LIVE_FUSE_TRIPPED_TWO_OF_FIVE_SESSIONS")
    if live.open_protection_faults > 0:
        triggers.append("LIVE_PROTECTION_FAULT")
    return triggers


def _tier3_fault_after(live: _LiveLeg, moment: datetime) -> bool:
    """A protection fault on a tier-3 round trip that closed after ``moment``."""
    return any(
        trip.protection_fault
        and trip.authority == PAPER_ENTRY_AUTHORITY
        and trip.closed_at > moment
        for trip in live.round_trips
    )


def _tier3_at_step(live: _LiveLeg, since: datetime) -> list[_LiveRoundTrip]:
    """Tier-3 round trips opened after ``since``, oldest close first."""
    return [
        trip
        for trip in live.round_trips
        if trip.authority == PAPER_ENTRY_AUTHORITY and trip.opened_at > since
    ]


def _clean_streak(live: _LiveLeg, *, step_index: int, since: datetime, bound: Decimal) -> int:
    """Consecutive clean tier-3 round trips opened at ``step_index``, newest backwards.

    Operator-confirmed round trips never move the ladder: they are not sized by it.
    """
    streak = 0
    for trip in reversed(_tier3_at_step(live, since)):
        if trip.capital_step != step_index:
            continue
        if not trip.clean(bound):
            break
        streak += 1
    return streak


def _confirmed_clean_after(live: _LiveLeg | None, moment: datetime, bound: Decimal) -> int:
    """Clean operator-confirmed live round trips of the leg opened after ``moment``."""
    if live is None:
        return 0
    return sum(
        1
        for trip in live.round_trips
        if trip.authority == LIVE_CONFIRMED_ENTRY_AUTHORITY
        and trip.opened_at > moment
        and trip.clean(bound)
    )


def _leg_state(
    status: str,
    transition: str,
    *,
    now: datetime,
    prior: _PriorLeg,
    step_index: int = 0,
    step_entered_at: datetime | None = None,
    clean: int = 0,
    hold_reasons: Iterable[str] = (),
    demotion_reasons: Iterable[str] | None = None,
    cooloff: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    promoted_at, demoted_at = prior.promoted_at, prior.demoted_at
    if transition == "PROMOTED":
        promoted_at = now
    if transition == "DEMOTED":
        demoted_at = now
    held = list(dict.fromkeys(hold_reasons))
    return {
        "status": status,
        "transition": transition,
        "latched": status == "DEMOTED",
        # A promoted leg that is held (no live evidence this cycle) is not eligible either.
        "live_entry_eligible": status == "PROMOTED" and not held,
        "promoted_at": _iso(promoted_at),
        "demoted_at": _iso(demoted_at),
        "demotion_reasons": list(
            dict.fromkeys(prior.demotion_reasons if demotion_reasons is None else demotion_reasons)
        ),
        "hold_reasons": held,
        "capital_step": CAPITAL_LADDER[step_index].model_dump(mode="json"),
        "capital_step_entered_at": _iso(step_entered_at),
        "clean_round_trips_at_step": clean,
        "cooloff": None if cooloff is None else _jsonable(dict(cooloff)),
    }


def _record_leg(record: Any, leg: str) -> Mapping[str, Any]:
    legs = record.get("legs") if isinstance(record, Mapping) else None
    entry = legs.get(leg) if isinstance(legs, Mapping) else None
    return entry if isinstance(entry, Mapping) else {}


def _record_reasons(record: Any, leg: str) -> list[str]:
    reasons = _record_leg(record, leg).get("reasons")
    if isinstance(reasons, list) and all(isinstance(code, str) for code in reasons):
        return list(reasons)
    return []


def _state_latch_release(
    prior: _PriorLeg,
    record: Any,
    leg: str,
    *,
    bound: bool,
    live: _LiveLeg | None,
    now: datetime,
    limits: LivePromotionThresholds,
) -> dict[str, Any]:
    """Whether a latched demotion may be lifted.

    All of: the cool-off sessions have completed; the record was computed with this very state,
    so its paper criteria count only rows decided after the demotion and its latch criterion
    saw every manifest generated after it; and, for a leg demoted by a live decay cause, enough
    clean operator-confirmed live round trips were opened after the demotion.
    """
    demoted_at = prior.demoted_at
    completed = _cooloff_sessions_completed(demoted_at, now)
    entry = _record_leg(record, leg)
    criteria = entry.get("criteria")
    latch = criteria.get(_LATCH_CRITERION) if isinstance(criteria, Mapping) else None
    observed = latch.get("observed") if isinstance(latch, Mapping) else None
    newer = (
        bound
        and demoted_at is not None
        and _parse_utc(entry.get("evidence_floor")) == demoted_at
        and isinstance(observed, Mapping)
        and observed.get("manifests_newer_than_demotion") is True
    )
    live_causes = sorted(LIVE_DECAY_CAUSES.intersection(prior.demotion_reasons))
    confirmed: int | None = None
    if live_causes:
        confirmed = (
            0
            if demoted_at is None
            else _confirmed_clean_after(live, demoted_at, limits.max_shortfall_bps)
        )
    released = (
        completed is not None
        and completed >= limits.demotion_cooloff_sessions
        and newer is True
        and (confirmed is None or confirmed >= LIVE_CAUSE_RELEASE_CONFIRMED_ROUND_TRIPS)
    )
    return {
        "released": released,
        "cooloff_sessions_completed": completed,
        "cooloff_sessions_required": limits.demotion_cooloff_sessions,
        "evidence_newer_than_demotion": newer,
        "live_causes": live_causes,
        "live_confirmed_round_trips_after_demotion": confirmed,
        "live_confirmed_round_trips_required": (
            LIVE_CAUSE_RELEASE_CONFIRMED_ROUND_TRIPS if live_causes else None
        ),
    }


def _next_leg_state(
    leg: str,
    prior: _PriorLeg,
    record: Any,
    *,
    bound: bool,
    live_legs: Mapping[str, _LiveLeg] | None,
    live_problem: str | None,
    now: datetime,
    limits: LivePromotionThresholds,
) -> dict[str, Any]:
    if prior.problem is not None:
        return _leg_state(
            "DEMOTED", "DEMOTED", now=now, prior=prior, demotion_reasons=[prior.problem]
        )
    merit = bound and leg_has_merit(record, leg, now_utc=now, thresholds=limits)
    record_reasons = [
        *([] if bound else ["RECORD_NOT_COMPUTED_WITH_STATE"]),
        *_record_reasons(record, leg),
    ]
    live = None if live_legs is None else live_legs.get(leg)
    promoted_at, entered_at = prior.promoted_at, prior.step_entered_at

    if prior.status != "PROMOTED" or promoted_at is None or entered_at is None:
        # A promotion is decided on the live evidence of this call: absent or malformed
        # evidence, an open protection fault or an active decay trigger holds it.
        held = [] if merit is True else ["NO_CURRENT_MERIT", *record_reasons]
        if live is None:
            held.append(live_problem or "LIVE_EVIDENCE_MISSING")
        else:
            held += _live_window_triggers(live, limits)
            if _tier3_fault_after(live, now):
                held.append("LIVE_PROTECTION_FAULT")
        if prior.status != "DEMOTED":
            if held:
                return _leg_state("PAPER", "NONE", now=now, prior=prior, hold_reasons=held)
            return _leg_state("PROMOTED", "PROMOTED", now=now, prior=prior, step_entered_at=now)
        release = _state_latch_release(
            prior, record, leg, bound=bound, live=live, now=now, limits=limits
        )
        if release["released"] is not True:
            held.insert(0, "DEMOTION_LATCHED")
        if held:
            return _leg_state(
                "DEMOTED", "NONE", now=now, prior=prior, hold_reasons=held, cooloff=release
            )
        return _leg_state(
            "PROMOTED", "PROMOTED", now=now, prior=prior, step_entered_at=now, cooloff=release
        )

    step_index = prior.step_index
    triggers = [] if merit is True else ["MERIT_LOST", *record_reasons]
    if live is None:
        if live_problem == "LIVE_EVIDENCE_MALFORMED":
            triggers.append(live_problem)
    elif any(
        trip.capital_step is not None and trip.capital_step > step_index
        for trip in _tier3_at_step(live, entered_at)
    ):
        # A tier-3 round trip opened at this step cannot have been sized by a later one.
        triggers.append("LIVE_EVIDENCE_MALFORMED")
    else:
        triggers += _live_window_triggers(live, limits)
        if _tier3_fault_after(live, promoted_at):
            triggers.append("LIVE_PROTECTION_FAULT")
    if triggers:
        return _leg_state("DEMOTED", "DEMOTED", now=now, prior=prior, demotion_reasons=triggers)
    if live is None:
        # Nothing was observed on live this cycle, so no decay trigger could be evaluated: the
        # leg keeps its promotion and its step but may not enter until evidence is passed again.
        return _leg_state(
            "PROMOTED",
            "NONE",
            now=now,
            prior=prior,
            step_index=step_index,
            step_entered_at=entered_at,
            hold_reasons=["LIVE_EVIDENCE_MISSING"],
        )

    transition = "NONE"
    clean = _clean_streak(
        live, step_index=step_index, since=entered_at, bound=limits.max_shortfall_bps
    )
    following = step_index + 1
    if (
        following < len(CAPITAL_LADDER)
        and clean >= CAPITAL_LADDER[following].min_clean_live_round_trips_at_previous
    ):
        step_index, entered_at, clean = following, now, 0
        transition = "CAPITAL_STEP_ADVANCED"
    return _leg_state(
        "PROMOTED",
        transition,
        now=now,
        prior=prior,
        step_index=step_index,
        step_entered_at=entered_at,
        clean=clean,
    )


def initial_promotion_state(*, now_utc: datetime) -> dict[str, Any]:
    """The explicit "no promotion history" state: both legs in paper, never promoted or demoted.

    The opt-in step writes it once. From then on a state file that cannot be read is passed on
    as ``None`` (or as whatever was read), which is unknown history and demotes.
    """
    now = _aware_utc(now_utc)
    if now is None:
        raise ValueError("now_utc must be a timezone-aware datetime")
    return {
        "schema": LIVE_PROMOTION_STATE_SCHEMA,
        "updated_at": _iso(now),
        "record_inputs_hash": None,
        "record_sha256": None,
        "legs": {
            leg: _leg_state(
                "PAPER", "NONE", now=now, prior=_PriorLeg(), hold_reasons=["NO_MERIT_RECORD_YET"]
            )
            for leg in LEGS
        },
    }


def next_promotion_state(
    prior_state: Mapping[str, Any] | None,
    record: Any,
    *,
    live_evidence: Mapping[str, Any] | None = None,
    now_utc: datetime,
    thresholds: LivePromotionThresholds | None = None,
) -> dict[str, Any]:
    """Promotion, latched demotion and the capital ladder as a pure state transition.

    The caller persists the returned state and passes it back as ``prior_state``, here and to
    ``compute_live_promotion_merit``: ``record`` counts only when it was computed with this
    very ``prior_state`` (``RECORD_NOT_COMPUTED_WITH_STATE`` otherwise) and under the
    configured ``thresholds`` (``None`` = the owner defaults). Per leg:

    * ``PAPER`` -> ``PROMOTED`` at the first capital step when the record gives the leg merit
      and the live evidence of this call shows no reason to hold: it must be present and well
      formed, with no open protection fault and no active decay trigger.
    * ``PROMOTED`` -> ``DEMOTED`` (latched, capital reset to the first step) when merit is lost
      or a live decay trigger fires: mean live shortfall above the bound over the leg's last
      ``LIVE_SHORTFALL_WINDOW_ROUND_TRIPS`` live round trips, the daily fuse tripped on
      ``LIVE_FUSE_TRIPS_TO_DEMOTE`` of the last ``LIVE_FUSE_WINDOW_SESSIONS`` sessions, or a
      protection fault on a tier-3 position. The windows are not cut at the promotion date.
    * ``DEMOTED`` -> ``PROMOTED`` only when all of these hold: ``demotion_cooloff_sessions``
      completed XNYS sessions; a passing record computed with the demoted state, which counts
      only paper rows decided after the demotion and needs every manifest generated after it;
      the same live conditions as a first promotion; and, after a demotion for a live decay
      cause, ``LIVE_CAUSE_RELEASE_CONFIRMED_ROUND_TRIPS`` clean operator-confirmed live round
      trips of the leg opened after the demotion.
    * A promoted leg advances one capital step per call once its consecutive clean tier-3 round
      trips opened at the current step reach the next step's requirement.
    * ``live_evidence=None`` means nothing was observed: no leg is promoted, and a promoted leg
      keeps its status but is not ``live_entry_eligible`` (``LIVE_EVIDENCE_MISSING``).

    ``live_evidence`` is exactly::

        {"fuse_sessions": [{"session": "YYYY-MM-DD", "tripped": bool}, ...],
         "legs": {"LONG": {"open_protection_faults": int,
                           "round_trips": [{"entry_client_order_id": str,
                                            "entry_authority": "AUTONOMOUS" | "OPERATOR_CONFIRMED",
                                            "opened_at": ISO-8601 with offset,
                                            "closed_at": ISO-8601 with offset,
                                            "shortfall_bps": number,
                                            "capital_step": int for AUTONOMOUS, else None,
                                            "protection_fault": bool, "close_cause": str}]},
                  "SHORT": {...}}}

    ``round_trips`` are the leg's closed live round trips on the tiered venue, each listed once
    (``entry_client_order_id`` is unique across both legs) and none closed after ``now_utc``.
    ``open_protection_faults`` counts open protection faults on the leg's tier-3 positions.

    A malformed ``live_evidence`` demotes a promoted leg and holds every other one. A
    ``prior_state`` that is ``None`` or malformed is a demotion at ``now_utc``
    (``PRIOR_STATE_MISSING`` / ``PRIOR_STATE_MALFORMED``): unknown history never promotes. The
    first state is ``initial_promotion_state()``.
    """
    now = _aware_utc(now_utc)
    if now is None:
        raise ValueError("now_utc must be a timezone-aware datetime")
    limits = _configured(thresholds)
    if limits is None:
        raise TypeError("thresholds must be a LivePromotionThresholds or None")
    live_legs = None if live_evidence is None else _parse_live(live_evidence, now)
    live_problem = None
    if live_legs is None:
        live_problem = (
            "LIVE_EVIDENCE_MISSING" if live_evidence is None else "LIVE_EVIDENCE_MALFORMED"
        )
    state_digest = None if prior_state is None else _canonical_sha256(prior_state)
    bound = (
        state_digest is not None
        and isinstance(record, Mapping)
        and record.get("prior_state_sha256") == state_digest
    )
    digest = record.get("inputs_hash") if isinstance(record, Mapping) else None
    return {
        "schema": LIVE_PROMOTION_STATE_SCHEMA,
        "updated_at": _iso(now),
        "record_inputs_hash": digest if isinstance(digest, str) else None,
        # The record this state was decided on: ``live_entry_leg_merit`` needs that same record.
        "record_sha256": _record_digest(record),
        "legs": {
            leg: _next_leg_state(
                leg,
                _prior_leg(prior_state, leg, now),
                record,
                bound=bound,
                live_legs=live_legs,
                live_problem=live_problem,
                now=now,
                limits=limits,
            )
            for leg in LEGS
        },
    }


def live_entry_leg_merit(
    state: Any,
    record: Any,
    leg: str,
    *,
    now_utc: datetime,
    thresholds: LivePromotionThresholds | None = None,
) -> bool:
    """The tier-3 gate value for ``leg``: the ``leg_merit`` a live scanner entry is submitted with.

    True only when all of these hold, and never raises:

    * ``state`` is the well-formed result of ``next_promotion_state`` for this very ``record``
      (same ``record_sha256`` and ``inputs_hash``), decided at or after the record was computed
      and not in the future;
    * the leg is ``PROMOTED`` there with ``live_entry_eligible`` literally True (so not
      demoted, not latched and not held);
    * ``leg_has_merit(record, leg)`` under the configured ``thresholds``.

    A record that still shows merit after its leg was demoted, a state decided on another
    record, and a missing state all return False. It is Key A only; Key B (the owner's opt-in)
    is checked by the venue gate.
    """
    try:
        now = _aware_utc(now_utc)
        limits = _configured(thresholds)
        if now is None or limits is None or not isinstance(leg, str) or leg not in LEGS:
            return False
        if not isinstance(state, dict) or not isinstance(record, dict):
            return False
        if _prior_leg(state, leg, now).problem is not None:
            return False
        entry = state["legs"][leg]
        if entry.get("status") != "PROMOTED" or entry.get("live_entry_eligible") is not True:
            return False
        if entry.get("latched") is not False or entry.get("hold_reasons") != []:
            return False
        updated_at = _parse_utc(state.get("updated_at"))
        computed_at = _parse_utc(record.get("computed_at"))
        if updated_at is None or computed_at is None:
            return False
        if updated_at < computed_at or updated_at - now > _FUTURE_SKEW_TOLERANCE:
            return False
        digest = _record_digest(record)
        if digest is None or state.get("record_sha256") != digest:
            return False
        if state.get("record_inputs_hash") != record.get("inputs_hash"):
            return False
        return leg_has_merit(record, leg, now_utc=now, thresholds=limits)
    except Exception:  # the gate fails closed on anything a hostile or damaged input can do
        return False

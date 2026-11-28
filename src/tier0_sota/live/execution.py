"""Alpaca paper order management driven by the native SIP and BOATS trade streams."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent import futures as concurrent_futures
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
from uuid import uuid4

import duckdb
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import (
    GetOrderByIdRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLossRequest,
    StopOrderRequest,
)

from tier0_sota import engine, risk
from tier0_sota.live import runner
from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    INVERSE_HEDGE_SYMBOLS,
    RTH_ENTRY_COLLAR_PCT,
    SINGLE_STOCK_INVERSE_ETF_MAP,
    AlpacaPaperLane,
    ExtendedHoursStopSupervision,
    PaperFarmCoordinator,
    _atomic_write_json,
    _duckdb_filename,
    _ensure_entry_decisions_table,
    build_live_venue_coordinator,
    build_symbol_24x5_session_presets,
    compute_fast_reverse_trigger,
    open_bounded_duckdb,
    persist_supervised_stop_duckdb,
    select_short_or_inverse_fallback,
    select_situation_lane,
)
from tier0_sota.live.runner import refresh_accounts, verify_paper_environment
from tier0_sota.live.scanner import (
    INVERSE_ETF_ANNUAL_EXPENSE_RATES,
    ExtremeMoverScanner,
    lookup_alpaca_paper_borrow,
    resolve_alpaca_env_creds,
)

LOG = logging.getLogger(__name__)
TERMINAL = {"filled", "expired", "canceled", "rejected"}
MIN_LIQUID_ENTRY_PRICE = Decimal("10.00")
MAX_RTH_QUOTED_SPREAD_BPS = Decimal("12")
MAX_EXT_QUOTED_SPREAD_BPS = Decimal("35")
_INVERSE_HEDGE_SYMBOLS = INVERSE_HEDGE_SYMBOLS
_SEMI_SECTOR_SYMBOLS = frozenset(
    {
        "NVDA",
        "SOXL",
        "AMD",
        "SMCI",
        "AVGO",
        "MU",
        "MRVL",
        "ARM",
        "ASML",
        "TSM",
        "INTC",
        "QCOM",
        "SOXX",
        "SMH",
        "NVDL",
    }
)
_ADOPTED_EXTERNAL_ORIGIN = "ADOPTED_EXTERNAL_POSITION"
# Ledger authority of a position the owner opened and the harness adopted.
_OWNER_ADOPTED_AUTHORITY = "OWNER_ADOPTED"
_FRESH_FILL_SETTLE_SECS = 10.0
EXIT_REPEG_MIN_AGE_SECS = 15.0
MAX_SUPERVISED_EXIT_ATTEMPTS = 12
# Stop coverage (R1): the supervisor loop reconciles resting stops against broker positions on
# this cadence, independent of trade prints and of the scanner file. A refused attach backs off.
STOP_COVERAGE_RECONCILE_SECS = 2.0
STOP_ATTACH_BACKOFF_BASE_SECS = 2.0
STOP_ATTACH_BACKOFF_MAX_SECS = 60.0
REBUILD_RETRY_HOLD_SECS = 60.0
# Exit ladder (R4): the collar widens one rung per EXIT_LADDER_ATTEMPTS_PER_RUNG spent exit
# orders and per rejected or refused exit, up to EXIT_LADDER_MAX_COLLAR_MULT times the session
# collar. After MAX_SUPERVISED_EXIT_ATTEMPTS orders or EXIT_LADDER_MAX_REFUSALS refusals the
# exit becomes a close-position-equivalent market order (at most CLOSE_RUNG_MAX_ATTEMPTS of
# them); after that the guard carries a protection fault and sends nothing more.
EXIT_LADDER_ATTEMPTS_PER_RUNG = 4
EXIT_LADDER_MAX_COLLAR_MULT = 3
EXIT_LADDER_MAX_REFUSALS = 3
CLOSE_RUNG_MAX_ATTEMPTS = 3
EXIT_REFUSAL_BACKOFF_MAX_SECS = 60.0
PROTECTION_FAULT_ALERT_COUNT = 3
# Data plane (R2). A guard acts on any print that is not older than the last exchange timestamp
# it saw; only entries keep the wall-clock freshness gate. A guard without a confirmed price for
# GUARD_PRICE_REST_FALLBACK_SECS is driven from a REST latest-trade poll (every
# GUARD_REST_POLL_SECS); above GUARD_DATA_STALE_SECS the status says DATA_STALE.
ENTRY_PRINT_MAX_AGE_SECS = 5.0
GUARD_PRICE_REST_FALLBACK_SECS = 5.0
GUARD_REST_POLL_SECS = 2.0
GUARD_DATA_STALE_SECS = 30.0
# Host clock against the broker clock: read at start and on this cadence. Above the limit
# entries are blocked and sessions are classified from broker time.
CLOCK_SKEW_CHECK_SECS = 30.0
CLOCK_SKEW_MAX_SECS = Decimal("1.0")
# A stream whose run() raises is restarted with a doubling pause. After this many failures in a
# row it is given up; supervision continues on the other streams and the REST fallback.
STREAM_RESTART_BACKOFF_BASE_SECS = 1.0
STREAM_RESTART_BACKOFF_MAX_SECS = 60.0
STREAM_RESTART_MAX_CONSECUTIVE = 8
STREAM_HEALTHY_RUN_SECS = 60.0
STREAM_END_GRACE_SECS = 1.0
# Rate (R14): at most one stop replace per guard in this many seconds.
STOP_REPLACE_MIN_INTERVAL_SECS = 1.0
# Coverage auditor (R12).
LOSSLESS_COVERAGE_SCHEMA = "tier0.lossless-coverage.v1"
LOSSLESS_COVERAGE_MANIFEST = "lossless_coverage_manifest.json"
LOSSLESS_COVERAGE_GENERATOR = "tier0_sota.live.execution.build_lossless_coverage_record"
LOSSLESS_COVERAGE_REPORT_SECS = 5.0
SYNTHETIC_PRICE_AGE_MAX_SECS = 5.0
SYNTHETIC_HEARTBEAT_MAX_SECS = 10.0
# Broker refusals of an entry submit that are final for that order (R13): the ledger row is
# closed with the code and entries continue.
_ENTRY_REJECT_HTTP_CODES = frozenset({403, 422})
_ENTRY_REJECT_CAUSE_PREFIX = "ENTRY_REJECTED_HTTP_"
_SUPERVISION_FAULT_PREFIX = "SUPERVISION_ERROR_"
# Fault codes that alert on their first occurrence: the guard has nothing left to try.
_IMMEDIATE_ALERT_FAULT_CODES = frozenset({"EXIT_LADDER_EXHAUSTED"})
# Stated in the coverage record: what an off-hours exit can and cannot do.
_OFFHOURS_EXIT_NOTE = (
    "Outside the regular session a resting stop or trailing stop does not trigger: the exits "
    "are the supervisor's collared limit orders. The last ladder rung is a market DAY order, "
    "which the broker queues for the next regular open."
)
_HARNESS_STOP_PREFIX = "stop-"
_HARNESS_EXIT_PREFIX = "exit-"
_WORKING_ORDER_STATUSES = frozenset(
    {"new", "accepted", "partially_filled", "pending_new", "pending_replace"}
)
_STOP_ORDER_TYPES = frozenset({"stop", "stop_limit"})
_TRAILING_STOP_ORDER_TYPE = "trailing_stop"
_REBUILD_ACTIONS = frozenset({"UNMANAGED", "SIDE_FLIP", "FLAT", "LIFECYCLE_ENDED"})
_OPPOSING_INVERSE_FAMILIES: tuple[tuple[frozenset[str], frozenset[str]], ...] = (
    (frozenset({"TQQQ", "QQQ", "QLD"}), frozenset({"SQQQ", "QID", "PSQ"})),
    (frozenset({"SOXL", "SOXX", "SMH"}), frozenset({"SOXS", "SSG"})),
    (frozenset({"UPRO", "SPXL", "SPY", "SSO"}), frozenset({"SPXS", "SPXU", "SH", "SDS"})),
    (frozenset({"UDOW", "DIA", "DDM"}), frozenset({"SDOW", "DXD", "DOG"})),
    (frozenset({"TNA", "IWM", "UWM"}), frozenset({"TZA", "TWM", "RWM"})),
    (frozenset({"TECL", "XLK"}), frozenset({"TECS"})),
    (frozenset({"NVDA", "NVDL", "NVDX"}), frozenset({"NVDS", "NVD", "NVDQ"})),
    (frozenset({"TSLA", "TSLL"}), frozenset({"TSLS", "TSLQ"})),
    (frozenset({"MSTR", "MSTU", "MSTX"}), frozenset({"MSTZ", "SMST"})),
)


def _build_opposing_inverse_pair_map() -> dict[str, frozenset[str]]:
    pairs: dict[str, set[str]] = {}
    for bull_group, bear_group in _OPPOSING_INVERSE_FAMILIES:
        for sym in bull_group:
            pairs.setdefault(sym, set()).update(bear_group)
        for sym in bear_group:
            pairs.setdefault(sym, set()).update(bull_group)
    return {sym: frozenset(opps) for sym, opps in pairs.items()}


OPPOSING_INVERSE_PAIR_MAP: dict[str, frozenset[str]] = _build_opposing_inverse_pair_map()


def find_opposing_inverse_pair_conflict(
    symbol: str,
    side: risk.Side,
    active_sides: Mapping[str, tuple[risk.Side, str]],
) -> dict[str, str] | None:
    """Block simultaneous opposing leveraged/inverse legs until flatten-first completes."""
    sym = symbol.strip().upper()
    partners = OPPOSING_INVERSE_PAIR_MAP.get(sym)
    if not partners:
        return None
    for partner in sorted(partners):
        active = active_sides.get(partner)
        if active is not None and active[0] == side:
            return {
                "symbol": sym,
                "side": side,
                "opposing_symbol": partner,
                "opposing_side": active[0],
                "opposing_owner": active[1],
            }
    return None


def _collect_open_symbol_sides(
    farm: PaperFarmCoordinator,
    *,
    positions: Mapping[str, Mapping[str, Any]] | None = None,
    guards: Mapping[str, Guard] | None = None,
) -> dict[str, tuple[risk.Side, str]]:
    """Collect active open symbol directions across guards, broker positions, ledgers, and locks."""
    active: dict[str, tuple[risk.Side, str]] = {}
    for sym, guard in (guards or {}).items():
        u_sym = sym.strip().upper()
        if u_sym:
            active[u_sym] = (guard.side, guard.lane.account)
    for acct, acct_positions in (positions or {}).items():
        for pos in (acct_positions or {}).values():
            parsed = _parse_position_for_adoption(pos)
            if parsed is not None:
                active.setdefault(parsed[0], (parsed[1], acct))
    for acct, lane in farm.lanes.items():
        if not lane.duckdb_path.is_file():
            continue
        with open_bounded_duckdb(lane.duckdb_path, read_only=True) as db:
            if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                continue
            rows = db.execute(
                "SELECT symbol, side FROM paper_entry_decisions WHERE closed_at IS NULL"
            ).fetchall()
        for raw_sym, raw_side in rows:
            u_sym = str(raw_sym or "").strip().upper()
            side_norm = str(raw_side or "").strip().upper()
            if u_sym and side_norm in ("LONG", "SHORT"):
                side_val: risk.Side = "LONG" if side_norm == "LONG" else "SHORT"
                active.setdefault(u_sym, (side_val, acct))
    for locked_sym, owner_acct in farm.symbol_lock.snapshot().items():
        u_sym = locked_sym.strip().upper()
        if u_sym in OPPOSING_INVERSE_PAIR_MAP and u_sym not in active:
            active[u_sym] = ("LONG", owner_acct)
    return active


def _order_created_utc(order: Any) -> datetime | None:
    for attr in ("created_at", "submitted_at"):
        raw = getattr(order, attr, None)
        if isinstance(raw, datetime):
            return raw.replace(tzinfo=UTC) if raw.tzinfo is None else raw.astimezone(UTC)
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
            except ValueError:
                continue
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    return None


def _order_age_secs(order: Any, now_utc: datetime, last_submit_mono: float) -> float | None:
    created_utc = _order_created_utc(order)
    if created_utc is not None:
        return max(0.0, (now_utc - created_utc).total_seconds())
    if last_submit_mono > 0.0:
        return max(0.0, time.monotonic() - last_submit_mono)
    return None


def _order_limit_decimal(order: Any) -> Decimal | None:
    raw = getattr(order, "limit_price", None)
    if raw in (None, "", "0", 0) or isinstance(raw, bool):
        return None
    dec = Decimal(str(raw))
    return dec if dec.is_finite() and dec > Decimal("0") else None


def _should_cancel_stale_exit_for_repeg(
    existing: Any,
    *,
    side: risk.Side,
    price: Decimal,
    target_limit: Decimal,
    session: str,
    now_utc: datetime,
    min_age_secs: float = EXIT_REPEG_MIN_AGE_SECS,
    last_submit_mono: float = 0.0,
) -> tuple[bool, str]:
    """Return (True, reason) when an open supervised exit is stale, cross-session, or off-market."""
    raw_status = getattr(existing, "status", "")
    status = str(getattr(raw_status, "value", raw_status) or "").strip().lower()
    if status in TERMINAL or status == "pending_cancel":
        return False, ""
    created_utc = _order_created_utc(existing)
    if created_utc is not None:
        created_ns = int(created_utc.timestamp() * 1_000_000_000)
        created_session = engine.classify_us_equity_session_et(created_ns)
        if created_session != session:
            return True, f"CROSS_SESSION_{created_session}_TO_{session}"
    if session == "RTH" and getattr(existing, "extended_hours", False) is True:
        return True, "EXTENDED_HOURS_EXIT_IN_RTH"
    age_secs = _order_age_secs(existing, now_utc, last_submit_mono)
    if age_secs is None or age_secs < min_age_secs:
        return False, ""
    if status == "pending_new":
        return True, "STUCK_PENDING_NEW"
    if status in {"new", "accepted", "partially_filled"}:
        existing_limit = _order_limit_decimal(existing)
        if existing_limit is not None:
            if side == "LONG" and existing_limit > price and existing_limit > target_limit:
                return True, "OUT_OF_MARKET_COLLAR_REPEG"
            if side == "SHORT" and existing_limit < price and existing_limit < target_limit:
                return True, "OUT_OF_MARKET_COLLAR_REPEG"
    return False, ""


def quote_microstructure_allows_entry(bid: Decimal, ask: Decimal, session: str) -> bool:
    """Reject sub-$10 tick-floor traps and wide-spread quotes that breach the 2x TCA envelope."""
    if not (bid.is_finite() and ask.is_finite() and Decimal("0") < bid <= ask):
        return False
    mid = (bid + ask) / Decimal("2")
    if mid < MIN_LIQUID_ENTRY_PRICE:
        return False
    spread_bps = ((ask - bid) / mid) * Decimal("10000")
    max_spread = MAX_RTH_QUOTED_SPREAD_BPS if session == "RTH" else MAX_EXT_QUOTED_SPREAD_BPS
    return spread_bps <= max_spread


def cost_calibration_allows_entry(
    manifest_dir: Path, *, environment: Literal["PAPER", "LIVE"] = "PAPER"
) -> bool:
    """Honor the published Dagster reconciliation, including an adverse measured cost.

    A paper farm with neither a published reconciliation nor a recorded fill may enter: there is
    nothing to calibrate against yet. A live lane never gets that benefit of the doubt. For any
    ``environment`` other than ``PAPER`` the gate opens only on a published reconciliation that
    measured at least one fill, so a fresh directory or an empty receipt is a HOLD.
    """
    measured_only = environment != "PAPER"
    path = manifest_dir / "tca_calibration_manifest.json"
    if not path.exists():
        if measured_only:
            return False
        for account in ALPACA_LANE_PROFILES:
            ledger = manifest_dir / _duckdb_filename(account)
            if ledger.is_file():
                with open_bounded_duckdb(ledger, read_only=True) as db:
                    if ("paper_tca_fills",) in db.execute("SHOW TABLES").fetchall() and db.execute(
                        "SELECT 1 FROM paper_tca_fills LIMIT 1"
                    ).fetchone():
                        return False
        return True
    receipt = json.loads(path.read_text("utf-8"))
    if not isinstance(receipt, dict):
        return False
    if (
        receipt.get("schema_version") != "tier0.tca-calibration-manifest.v1"
        or receipt.get("paper_only") is not True
        or receipt.get("live_execution_enabled") is not False
        or receipt.get("duckdb_errors") != []
        or receipt.get("liquid_ge10_within_2x_envelope") is False
    ):
        return False
    if measured_only:
        measured_rows = receipt.get("duckdb_tca_rows")
        if isinstance(measured_rows, bool) or not isinstance(measured_rows, int):
            return False
        if measured_rows < 1:
            return False
    if receipt.get("parity_within_2x_envelope") is True:
        return True
    liquid_rows = receipt.get("liquid_ge10_tca_rows")
    min_liquid_px = receipt.get("min_liquid_entry_price_usd")
    return (
        receipt.get("liquid_ge10_within_2x_envelope") is True
        and not isinstance(liquid_rows, bool)
        and isinstance(liquid_rows, int)
        and liquid_rows > 0
        and not isinstance(min_liquid_px, bool)
        and isinstance(min_liquid_px, (int, float))
        and Decimal(str(min_liquid_px)).is_finite()
        and Decimal(str(min_liquid_px)) >= MIN_LIQUID_ENTRY_PRICE
    )


def _row_rvol(row: dict[str, Any]) -> Any:
    return row.get("rvol_normalized") if row.get("rvol_normalized") is not None else row.get("rvol")


def _infer_lock_tier(
    entry_px: Decimal | None,
    initial_stop: Decimal,
    active_stop: Decimal,
    side: risk.Side,
) -> int:
    """Infer monotonic 4-tier profit-lock tier from persisted (entry, initial_stop, active_stop)."""
    if entry_px is None or entry_px <= Decimal("0") or not entry_px.is_finite():
        return 0
    is_long = side == "LONG"
    locked_gain = (active_stop - entry_px) if is_long else (entry_px - active_stop)
    if locked_gain <= Decimal("0"):
        return 0
    leg_risk = max(risk.tick_size_for(entry_px), abs(entry_px - initial_stop))
    locked_r = locked_gain / leg_risk
    if locked_r >= Decimal("2.30"):
        return 4
    if locked_r >= Decimal("1.30"):
        return 3
    if locked_r >= Decimal("0.40"):
        return 2
    return 1


@dataclass
class Guard:
    lane: AlpacaPaperLane
    client_id: str
    symbol: str
    side: risk.Side
    initial: Decimal
    stop: Decimal
    natr: Decimal
    entered_session: str
    triggered: bool
    parent: Any
    child: Any = None
    checked: float = 0
    last_exit_check: float = 0
    reverse_flatten: bool = False
    lock_tier: int = 0
    trailing_schedule: str = ""
    adopted_external: bool = False
    non_noise_min_trail_pct: Decimal = Decimal("0")
    execution_mechanism: str = ""
    peak_favorable_px: Decimal = Decimal("0")
    fast_sell_armed: bool = False
    peak_reverse_armed: bool = False
    reentry_eligible_after_utc: str | None = None
    archetype: str = "EARLY_EP_MOMENTUM_BURST"
    spread_bps: Decimal = Decimal("5")
    reverse_candidate: dict[str, Any] | None = None
    last_exit_submit_mono: float = 0.0
    exit_repeg_count: int = 0
    last_exit_repeg_reason: str | None = None
    _synthetic_parent: bool = False
    # One lifecycle = one position from first fill to flat. Its id is part of every client
    # order id this guard sends, so an id is never reused by a later position in the symbol.
    lifecycle_id: str = ""
    # Price at which a position adopted under water was first seen: the trailing reference in
    # place of the fill price. Persisted once in the ledger and reused on restart.
    adoption_anchor_price: Decimal | None = None
    fractional_remainder: Decimal = Decimal("0")
    protection_fault_code: str | None = None
    protection_fault_count: int = 0
    exit_refusal_count: int = 0
    next_exit_attempt_mono: float = 0.0
    stop_attach_fault_count: int = 0
    next_stop_attach_mono: float = 0.0
    last_stop_replace_mono: float = 0.0
    side_flip_detected: bool = False
    lifecycle_closed: bool = False
    # Data plane (R2): the newest exchange timestamp this guard acted on, and when (monotonic)
    # its price was last confirmed by a stream print or a REST latest-trade poll.
    last_trade_ts: datetime | None = None
    last_price_mono: float = 0.0
    last_price_source: str = ""
    supervised_since_mono: float = 0.0
    last_heartbeat_mono: float = 0.0
    # Regime flip (R7): consecutive scan generations in which the regime stood against this
    # position, since when, and whether reverse_flatten was set by that flip alone.
    regime_flip_generations: int = 0
    regime_flip_since: datetime | None = None
    regime_flip_flatten: bool = False
    trigger_cause: str = ""

    def __post_init__(self) -> None:
        if self.supervised_since_mono <= 0:
            self.supervised_since_mono = time.monotonic()
        if self.last_heartbeat_mono <= 0:
            self.last_heartbeat_mono = self.supervised_since_mono
        if not self.lifecycle_id:
            self.lifecycle_id = str(getattr(self.parent, "id", "") or self.client_id)
        entry_px = self._reference_entry_price()
        if self.peak_favorable_px <= Decimal("0") and entry_px is not None:
            self.peak_favorable_px = entry_px
        if entry_px is not None:
            self.lock_tier = max(
                int(self.lock_tier),
                _infer_lock_tier(entry_px, self.initial, self.stop, self.side),
            )
            if self.lock_tier >= 1:
                self.fast_sell_armed = True
        ref_px = entry_px if entry_px is not None else self.initial
        if ref_px > Decimal("0"):
            spec = self._trail_spec(self.entered_session, ref_px)
            if self.non_noise_min_trail_pct <= Decimal("0"):
                self.non_noise_min_trail_pct = spec["non_noise_min_trail_pct"]
            if not self.execution_mechanism:
                self.execution_mechanism = str(spec["execution_mechanism"])
            if not self.trailing_schedule:
                self.trailing_schedule = str(spec["trailing_schedule"])

    def _filled_entry_price(self) -> Decimal | None:
        raw = getattr(self.parent, "filled_avg_price", None)
        if raw in (None, "", "0", 0):
            return None
        dec = Decimal(str(raw))
        return dec if dec.is_finite() and dec > Decimal("0") else None

    def _reference_entry_price(self) -> Decimal | None:
        """Trailing reference: the under-water adoption anchor when there is one, else the fill."""
        anchor = self.adoption_anchor_price
        if anchor is not None and anchor.is_finite() and anchor > Decimal("0"):
            return anchor
        return self._filled_entry_price()

    def _order_id_stem(self) -> str:
        """Lifecycle-unique stem of this guard's client order ids.

        The broker's own order id for an entry the harness sent; the adoption id (which ends in
        the lifecycle id) for an adopted position.
        """
        return str(getattr(self.parent, "id", "") or self.client_id)

    def _next_stop_client_id(self) -> str:
        """A fresh client order id for a standalone stop of this lifecycle (never reused)."""
        return f"{_HARNESS_STOP_PREFIX}{self._order_id_stem()}-{uuid4().hex[:8]}"

    def _note_protection_fault(self, code: str, detail: object = "") -> None:
        """Record a protective failure on this guard; it never touches the entry fault."""
        self.protection_fault_code = code
        self.protection_fault_count += 1
        counters = self.lane.counters
        counters["protection_faults"] = counters.get("protection_faults", 0) + 1
        count = self.protection_fault_count
        if count > PROTECTION_FAULT_ALERT_COUNT and count % 60:
            return  # Already alerted; a standing fault is logged again once a minute at most.
        emit = LOG.error if protection_fault_alerts(code, count) else LOG.warning
        emit(
            "Protection fault %s on %s %s (consecutive=%d): %s",
            code,
            self.lane.account,
            self.symbol,
            self.protection_fault_count,
            detail,
        )

    def _clear_protection_fault(self) -> None:
        self.protection_fault_code = None
        self.protection_fault_count = 0
        self.exit_refusal_count = 0
        self.next_exit_attempt_mono = 0.0

    def price_age_secs(self, now_mono: float | None = None) -> float:
        """Seconds since this guard's price was last confirmed (since it began, if never)."""
        now = time.monotonic() if now_mono is None else now_mono
        return max(0.0, now - (self.last_price_mono or self.supervised_since_mono))

    def heartbeat_age_secs(self, now_mono: float | None = None) -> float:
        """Seconds since the supervisor last looked at this guard (a print or a reconcile)."""
        now = time.monotonic() if now_mono is None else now_mono
        return max(0.0, now - max(self.last_heartbeat_mono, self.last_price_mono))

    def _trail_spec(self, session: str, entry_px: Decimal) -> dict[str, Any]:
        return engine.compute_non_noise_min_trail_spec(
            symbol=self.symbol,
            entry_price=entry_px,
            side=self.side,
            session=session,
            natr_pct=self.natr,
            spread_bps=self.spread_bps,
            archetype=self.archetype,
            venue_profile=self.lane.venue_profile,
        )

    def is_rapid_reentry_ready(self, now_utc: datetime | None = None) -> bool:
        if not self.lane.breaker.allow_entry():
            return False
        if (self.lane.rate_budget.max_rpm - self.lane.rate_budget.exit_reserve_rpm) <= 0:
            return False
        if self.reentry_eligible_after_utc is None:
            return True
        now = now_utc or datetime.now(UTC)
        try:
            eligible_at = datetime.fromisoformat(self.reentry_eligible_after_utc)
        except ValueError:
            return False
        return now >= eligible_at

    def _record_reverse_candidate(
        self,
        trigger_px: Decimal,
        session: str,
        trail_spec: dict[str, Any],
        *,
        now_utc: datetime | None = None,
    ) -> dict[str, Any]:
        sym = self.symbol.strip().upper()
        fallback_inv = SINGLE_STOCK_INVERSE_ETF_MAP.get(
            sym, "SOXS" if sym in _SEMI_SECTOR_SYMBOLS else "SQQQ"
        )
        if self.side == "LONG":
            if session == "OVERNIGHT" and self.lane.venue_profile.startswith("ALPACA"):
                reverse_symbol = fallback_inv
                reverse_side: risk.Side = "LONG"
                reverse_origin = f"INVERSE_HEDGE_{fallback_inv}"
            else:
                reverse_symbol = sym
                reverse_side = "SHORT"
                reverse_origin = "DIRECT_SHORT_ETB"
        else:
            reverse_symbol = sym
            reverse_side = "LONG"
            reverse_origin = "DIRECT_LONG_REVERSAL"
        cand: dict[str, Any] = {
            "source_symbol": sym,
            "source_side": self.side,
            "reverse_symbol": reverse_symbol,
            "reverse_side": reverse_side,
            "reverse_origin": reverse_origin,
            "fallback_inverse_symbol": fallback_inv,
            "policy": str(trail_spec["auto_reverse_policy"]),
            "session": session,
            "peak_favorable_px": float(self.peak_favorable_px),
            "trigger_px": float(trigger_px),
            "non_noise_min_trail_pct": float(trail_spec["non_noise_min_trail_pct"]),
            "venue_profile": self.lane.venue_profile,
            "armed_at_utc": (now_utc or datetime.now(UTC)).isoformat(),
            "flatten_first": True,
            # From the lane's environment (its account), never a constant: tier 3 exists on live.
            "paper_only": self.lane.environment == "PAPER",
            "live_execution_enabled": (
                self.lane.environment == "LIVE" and self.lane.live_execution_enabled is True
            ),
        }
        self.reverse_candidate = cand
        return cand

    def flatten_first_climax_reverse(
        self,
        price: Decimal,
        session: str,
        *,
        now_utc: datetime | None = None,
    ) -> dict[str, Any]:
        """Trigger flatten-first peak-climax reverse and stage the opposite-leg candidate."""
        now = now_utc or datetime.now(UTC)
        entry_px = self._reference_entry_price() or self.initial
        trail_spec = self._trail_spec(session, entry_px)
        self.peak_reverse_armed = True
        self.reverse_flatten = True
        self.regime_flip_flatten = False  # the climax owns this flatten: a regime revert keeps it
        cand = self._record_reverse_candidate(price, session, trail_spec, now_utc=now)
        self.reentry_eligible_after_utc = (now + timedelta(seconds=60)).isoformat()
        self.on_price(price, session)
        return cand

    def to_position_snapshot(self, session: str) -> dict[str, Any]:
        """Serialize active trailed Paper Farm guard for TV cockpit & FINOS/OpenBB sync."""
        sched_name, schedule = engine.select_pooled_trailing_schedule(self.natr)
        prof = engine.get_session_protection_profile(session, schedule)
        filled_qty_raw = getattr(self.parent, "filled_qty", None) or "0"
        entry_dec = self._filled_entry_price()
        entry_px = float(entry_dec) if entry_dec is not None else None
        ref_dec = self._reference_entry_price()
        trail_spec = self._trail_spec(session, ref_dec if ref_dec is not None else self.initial)
        nn_pct = (
            self.non_noise_min_trail_pct
            if self.non_noise_min_trail_pct > Decimal("0")
            else trail_spec["non_noise_min_trail_pct"]
        )
        eff_lock_tier = max(
            int(self.lock_tier),
            _infer_lock_tier(ref_dec, self.initial, self.stop, self.side),
        )
        exec_mech = str(trail_spec["execution_mechanism"])
        return {
            "symbol": self.symbol,
            "account": self.lane.account,
            "lane_id": self.lane.profile["lane_id"],
            "venue_profile": self.lane.venue_profile,
            "side": self.side,
            "qty": float(Decimal(str(filled_qty_raw))),
            "entry_price": entry_px,
            "initial_stop": float(self.initial),
            "active_stop": float(self.stop),
            "natr_pct": float(self.natr),
            "entered_session": self.entered_session,
            "active_session": session,
            "trailing_schedule": self.trailing_schedule or sched_name,
            "lock_tier": eff_lock_tier,
            "order_mode": prof.order_mode,
            "collar_bps": float(prof.limit_collar_bps),
            "outside_rth": bool(prof.outside_rth),
            "exit_triggered": bool(self.triggered),
            "exit_repeg_count": int(self.exit_repeg_count),
            "last_exit_repeg_reason": self.last_exit_repeg_reason,
            "reverse_flatten": bool(self.reverse_flatten),
            "adopted_external": bool(self.adopted_external),
            "auto_adopted_by_engine": True,
            "archetype": self.archetype,
            "min_trailing_pct": float(nn_pct),
            "non_noise_min_trail_pct": float(nn_pct),
            "adaptive_breakeven_trigger_pct": float(trail_spec["adaptive_breakeven_trigger_pct"]),
            "ultra_liquid_tier": bool(trail_spec["ultra_liquid_tier"]),
            "fast_sell_non_noise_reversal_pct": float(
                trail_spec["fast_sell_non_noise_reversal_pct"]
            ),
            "peak_climax_reverse_trigger_pct": float(trail_spec["peak_climax_reverse_trigger_pct"]),
            "auto_reverse_policy": str(trail_spec["auto_reverse_policy"]),
            "execution_mechanism": exec_mech,
            "broker_exit_order": str(trail_spec["broker_exit_order"]),
            "peak_favorable_px": (
                float(self.peak_favorable_px) if self.peak_favorable_px > Decimal("0") else entry_px
            ),
            "fast_sell_armed": bool(self.fast_sell_armed),
            "peak_reverse_armed": bool(self.peak_reverse_armed),
            "reverse_candidate": dict(self.reverse_candidate) if self.reverse_candidate else None,
            "reentry_eligible_after_utc": self.reentry_eligible_after_utc,
            "rapid_reentry_cooldown_until": self.reentry_eligible_after_utc,
            "rapid_reentry_ready": self.is_rapid_reentry_ready(),
            "rapid_reentry_rpm_budget": int(
                self.lane.rate_budget.max_rpm - self.lane.rate_budget.exit_reserve_rpm
            ),
            "client_order_id": self.client_id,
            "broker_order_id": str(getattr(self.parent, "id", "") or ""),
            "lifecycle_id": self.lifecycle_id,
            "adoption_anchor_price": (
                float(self.adoption_anchor_price)
                if self.adoption_anchor_price is not None
                else None
            ),
            "unprotected_fractional_qty": float(self.fractional_remainder),
            "protection_fault_code": self.protection_fault_code,
            "protection_fault_count": int(self.protection_fault_count),
            "price_age_secs": round(self.price_age_secs(), 3),
            "last_price_source": self.last_price_source,
            "data_stale": bool(
                session != "CLOSED" and self.price_age_secs() > GUARD_DATA_STALE_SECS
            ),
            "last_trade_utc": (
                self.last_trade_ts.isoformat() if self.last_trade_ts is not None else None
            ),
            "regime_flip_generations": int(self.regime_flip_generations),
            "regime_flip_flatten": bool(self.regime_flip_flatten),
        }

    def save(self) -> None:
        with open_bounded_duckdb(self.lane.duckdb_path) as db:
            _ensure_entry_decisions_table(db)
            db.execute(
                "UPDATE paper_entry_decisions SET active_stop=?,exit_triggered=? "
                "WHERE client_order_id=?",
                (self.stop, self.triggered, self.client_id),
            )

    def _sync_supervised_stop_floor(self, session: str, collar_pct: Decimal) -> None:
        parent_id = str(getattr(self.parent, "id", "") or self.client_id)
        sup = self.lane.supervised_stops.get(parent_id)
        if sup is None or sup.status != "ARMED":
            return
        revised = sup.model_copy(
            update={
                "ts_utc": datetime.now(UTC).isoformat(),
                "session": session,
                "stop_price": self.stop,
                "stop_limit_price": engine.compute_collared_limit_price(
                    self.stop, self.side, collar_pct
                ),
            }
        )
        self.lane.supervised_stops[parent_id] = revised
        persist_supervised_stop_duckdb(self.lane.duckdb_path, revised)

    def fetch(self) -> None:
        broker = self.lane.trading_client
        self.checked = time.monotonic()
        if self._synthetic_parent:
            try:
                pos = broker.get_open_position(self.symbol)
            except APIError as exc:
                if exc.status_code != 404:
                    raise
                self.parent.filled_qty = "0"
                return
            raw_qty = abs(Decimal(str(getattr(pos, "qty", "0") or "0")))
            self.parent.filled_qty = str(raw_qty)
            avg_px = getattr(pos, "avg_entry_price", None)
            if avg_px not in (None, "", "0", 0):
                self.parent.filled_avg_price = str(avg_px)
            exit_side_str = "sell" if self.side == "LONG" else "buy"
            if self.child and getattr(self.child, "id", None):
                try:
                    fetched_child = broker.get_order_by_id(self.child.id)
                    self.child = _normalize_broker_stop_child(
                        fetched_child,
                        symbol=self.symbol,
                        exit_side=exit_side_str,
                        stop_price=self.stop,
                    )
                except APIError as exc:
                    if exc.status_code != 404:
                        raise
                    self.child = None
            visited = set()
            while self.child and getattr(self.child, "replaced_by", None):
                if self.child.id in visited:
                    raise RuntimeError("Cyclic broker replacement chain")
                visited.add(self.child.id)
                fetched_rep = broker.get_order_by_id(self.child.replaced_by)
                self.child = _normalize_broker_stop_child(
                    fetched_rep,
                    symbol=self.symbol,
                    exit_side=exit_side_str,
                    stop_price=self.stop,
                )
            if self.child and getattr(self.child, "stop_price", None):
                better = max if self.side == "LONG" else min
                self.stop = better(self.stop, Decimal(str(self.child.stop_price)))
            return
        self.parent = broker.get_order_by_id(self.parent.id, GetOrderByIdRequest(nested=True))
        self.child = next((leg for leg in self.parent.legs or [] if leg.stop_price), None)
        visited = set()
        while self.child and self.child.replaced_by:
            if self.child.id in visited:
                raise RuntimeError("Cyclic broker replacement chain")
            visited.add(self.child.id)
            self.child = broker.get_order_by_id(self.child.replaced_by)
        if self.child:
            if self.child.symbol != self.symbol or self.child.side.value != (
                "sell" if self.side == "LONG" else "buy"
            ):
                raise PermissionError("Protective child disagrees with its owned parent")
            better = max if self.side == "LONG" else min
            self.stop = better(self.stop, Decimal(self.child.stop_price))
        entry_px = self._reference_entry_price()
        if entry_px is not None:
            self.lock_tier = max(
                int(self.lock_tier),
                _infer_lock_tier(entry_px, self.initial, self.stop, self.side),
            )
            if self.lock_tier >= 1:
                self.fast_sell_armed = True

    def on_price(self, price: Decimal, session: str) -> None:
        """Tighten a native stop or supervise a collared extended-session exit."""
        if self.lifecycle_closed:
            return  # Broker truth said flat: this lifecycle must not act on a later position.
        self.last_price_mono = time.monotonic()
        long = self.side == "LONG"
        better = max if long else min
        behind = (lambda a, b: a < b) if long else (lambda a, b: a > b)
        if (
            self.parent.status.value not in TERMINAL
            or self.child
            and self.child.status.value in {"held", "pending_replace", "pending_new"}
        ) and (
            time.monotonic() - self.checked >= 1
            # A status read never takes a slot of the exit reserve (R14).
            and _consume_read_slot(self.lane, time.time())
        ):
            self.fetch()
        sched_name, schedule = engine.select_pooled_trailing_schedule(self.natr)
        self.trailing_schedule = sched_name
        profile = engine.get_session_protection_profile(session, schedule)
        entry_px = self._reference_entry_price()
        trail_spec = self._trail_spec(session, entry_px if entry_px is not None else self.initial)
        self.non_noise_min_trail_pct = trail_spec["non_noise_min_trail_pct"]
        self.execution_mechanism = str(trail_spec["execution_mechanism"])

        eff_be_trigger: Decimal = trail_spec["adaptive_breakeven_trigger_pct"]
        if entry_px is not None:
            base_peak = (
                self.peak_favorable_px if self.peak_favorable_px > Decimal("0") else entry_px
            )
            self.peak_favorable_px = better(base_peak, better(entry_px, price))
            fav_gain = (
                (self.peak_favorable_px - entry_px) if long else (entry_px - self.peak_favorable_px)
            )
            fav_gain_pct = fav_gain / entry_px
            if fav_gain_pct >= eff_be_trigger:
                self.fast_sell_armed = True
            if fav_gain_pct >= trail_spec["peak_climax_reverse_trigger_pct"]:
                self.peak_reverse_armed = True

        breached = price <= self.stop if long else price >= self.stop
        if not breached and self.fast_sell_armed and self.peak_favorable_px > Decimal("0"):
            pullback_pct = (
                (self.peak_favorable_px - price) / self.peak_favorable_px
                if long
                else (price - self.peak_favorable_px) / self.peak_favorable_px
            )
            if pullback_pct >= self.non_noise_min_trail_pct:
                breached = True
        force_exit = self.reverse_flatten
        filled = Decimal(self.parent.filled_qty or "0")
        native = session == "RTH" and self.child and self.child.status.value in {"new", "accepted"}
        if not filled and not breached and not force_exit:
            return
        if not breached and not self.triggered and not force_exit:
            if entry_px is None:
                return
            desired, tier, _ = engine.compute_progressive_trailing_stop(
                entry_price=entry_px,
                side=self.side,
                favorable_extreme=self.peak_favorable_px,
                current_stop=self.stop,
                initial_stop=self.initial,
                natr_pct=self.natr,
                profile=profile,
                breakeven_trigger_override=eff_be_trigger,
            )
            self.lock_tier = max(self.lock_tier, int(tier))
            if tier >= 1 or self.fast_sell_armed:
                self.fast_sell_armed = True
                nn_mult = (
                    Decimal("1") - self.non_noise_min_trail_pct
                    if long
                    else Decimal("1") + self.non_noise_min_trail_pct
                )
                nn_raw = max(risk.TICK_SUB_DOLLAR, self.peak_favorable_px * nn_mult)
                nn_stop = risk.quantize_rule_612(nn_raw, side=self.side, is_stop=True)
                if behind(nn_stop, self.peak_favorable_px) and behind(nn_stop, price):
                    desired = better(desired, nn_stop)
            desired = better(self.stop, desired)
            if desired == self.stop and (not native or Decimal(self.child.stop_price) == desired):
                return
            if native:
                if time.monotonic() - self.last_stop_replace_mono < STOP_REPLACE_MIN_INTERVAL_SECS:
                    return  # R14: one stop replace per guard and second; the next print retries.
                if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
                    return
                self.last_stop_replace_mono = time.monotonic()
                replacement = ReplaceOrderRequest(stop_price=float(desired))
                if _is_harness_stop(self.child):
                    # A replacement is a new order: it keeps the harness's stop id form, so the
                    # stop-coverage reconciler still knows it as its own and can resize it.
                    replacement.client_order_id = self._next_stop_client_id()
                try:
                    self.child = self.lane.broker_replace(self.child.id, replacement)
                except APIError as exc:
                    if exc.status_code not in (404, 422):
                        raise
                    LOG.warning(
                        "Protective stop replace race for %s (%s): %s; reconciling broker state",
                        self.symbol,
                        self.child.id,
                        exc,
                    )
                    self.fetch()
                    return
                if self.child.status.value == "rejected":
                    raise RuntimeError("Broker rejected protective stop replacement")
                if self.child.status.value not in {"new", "accepted"}:
                    return
                desired = Decimal(self.child.stop_price)
                self.lane.counters["stop_replaces"] = self.lane.counters.get("stop_replaces", 0) + 1
            elif (
                session == "RTH"
                and self.entered_session == "RTH"
                and self.child
                and self.child.status.value not in TERMINAL
            ):
                return  # A pending replacement has no confirmed new stop price yet.
            self.stop = desired
            if session != "RTH":
                self._sync_supervised_stop_floor(session, profile.limit_collar_pct)
            self.save()
            return

        now_utc = datetime.now(UTC)
        if self.peak_reverse_armed and self.reverse_candidate is None:
            self._record_reverse_candidate(price, session, trail_spec, now_utc=now_utc)
        if self.reentry_eligible_after_utc is None:
            self.reentry_eligible_after_utc = (now_utc + timedelta(seconds=60)).isoformat()

        if (
            not force_exit
            and native
            and (
                Decimal(self.child.stop_price) >= self.stop
                if long
                else Decimal(self.child.stop_price) <= self.stop
            )
        ):
            return  # The broker owns this trigger.
        if not self.triggered:
            self.triggered = True
            # A trigger that came from a flatten request alone can be withdrawn while no exit
            # has been sent (R7); one that came from the stop cannot.
            self.trigger_cause = "STOP_BREACH" if breached else "REVERSE_FLATTEN"
            self.save()
        if time.monotonic() - self.last_exit_check < 1:
            return
        if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
            return
        self.last_exit_check = time.monotonic()
        try:
            self._run_exit_ladder(
                price, session, profile=profile, force_exit=force_exit, now_utc=now_utc
            )
        except (APIError, OSError) as exc:
            # A broker or transport error on the exit path is a protection fault of this guard,
            # retried on the next tick. It is never raised into the supervisor's entry fault.
            status_code = getattr(exc, "status_code", None)
            self._note_protection_fault(f"EXIT_BROKER_ERROR_{status_code or 'IO'}", exc)

    def _end_lifecycle_when_flat(self) -> None:
        """Broker truth says flat: close this lifecycle so it cannot act on a later position.

        An adopted position is closed at once. An entry the harness sent keeps the conditions
        ``restore_guards`` applies (entry order done, stop leg done, fill settled), because the
        position list can lag a fresh fill.
        """
        self._clear_protection_fault()
        if not self._synthetic_parent:
            if self.parent.status.value not in TERMINAL:
                return
            if self.child and self.child.status.value not in TERMINAL:
                return
            filled_at = getattr(self.parent, "filled_at", None)
            if isinstance(filled_at, datetime):
                filled_utc = (
                    filled_at if filled_at.tzinfo is not None else filled_at.replace(tzinfo=UTC)
                )
                if 0 <= (datetime.now(UTC) - filled_utc).total_seconds() < _FRESH_FILL_SETTLE_SECS:
                    return
        self.lifecycle_closed = True
        with open_bounded_duckdb(self.lane.duckdb_path) as db:
            _ensure_entry_decisions_table(db)
            db.execute(
                "UPDATE paper_entry_decisions SET closed_at=current_timestamp "
                "WHERE client_order_id=? AND closed_at IS NULL",
                [self.client_id],
            )

    def _release_reserved_shares(self, broker: Any) -> bool:
        """Cancel resting exit-side orders that hold this position's shares; True if any did.

        A resting stop (also one this guard does not track, a trailing stop, or a resting limit)
        reserves the shares it would sell, so the protective exit cannot be sent while it rests.
        """
        open_orders = _read_open_orders(broker)
        if not open_orders:
            return False
        exit_side = "sell" if self.side == "LONG" else "buy"
        released = False
        for order in _exit_side_orders(open_orders, self.symbol, exit_side):
            if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
                return True  # Out of budget this second: the rest are released on the next tick.
            try:
                self.lane.broker_cancel(order.id)
            except APIError as exc:
                if exc.status_code not in (404, 422):
                    raise
            released = True
        return released

    def _run_exit_ladder(
        self,
        price: Decimal,
        session: str,
        *,
        profile: engine.SessionProtectionProfile,
        force_exit: bool,
        now_utc: datetime,
    ) -> None:
        """Drive the triggered exit to flat from broker truth, up a bounded ladder.

        The quantity is what the broker says is held, never ``filled - covered``. A rejected,
        refused or spent exit climbs the ladder (wider collar, then a close-position-equivalent
        market order, then a protection fault) instead of raising or returning forever.
        """
        long = self.side == "LONG"
        self.fetch()
        broker = self.lane.trading_client
        # Wait for the broker to release reserved shares and settle late partial fills.
        for order in (self.parent, self.child):
            if order and order.status.value not in TERMINAL:
                if (
                    order is self.child
                    and not force_exit
                    and session == "RTH"
                    and order.status.value in {"pending_replace", "pending_new"}
                ):
                    return
                if order.status.value != "pending_cancel":
                    try:
                        self.lane.broker_cancel(order.id)
                    except APIError as exc:
                        if exc.status_code not in (404, 422):
                            raise
                        LOG.warning(
                            "Protective order cancel race for %s (%s): %s; reconciling state",
                            self.symbol,
                            order.id,
                            exc,
                        )
                        self.fetch()
                return
        if Decimal(self.parent.filled_qty or "0") == 0:
            if self._synthetic_parent:
                self._end_lifecycle_when_flat()  # fetch() found no position for this adoption
            return
        if not self._synthetic_parent and self.child and self.child.status.value == "filled":
            return  # The stop leg of this entry sold it.
        collar_base = profile.limit_collar_pct if session != "RTH" else RTH_ENTRY_COLLAR_PCT
        base_id = f"{_HARNESS_EXIT_PREFIX}{self._order_id_stem()}"
        max_orders = MAX_SUPERVISED_EXIT_ATTEMPTS + CLOSE_RUNG_MAX_ATTEMPTS
        attempt = 0
        rejected = 0
        exit_client_id = base_id
        limit = engine.compute_collared_limit_price(price, self.side, collar_base)
        while attempt < max_orders:
            exit_client_id = base_id if attempt == 0 else f"{base_id}-{attempt}"
            limit = engine.compute_collared_limit_price(
                price,
                self.side,
                collar_base * _exit_collar_mult(attempt, rejected + self.exit_refusal_count),
            )
            try:
                existing = broker.get_order_by_client_id(exit_client_id)
            except APIError as exc:
                if exc.status_code != 404:
                    raise
                break
            visited_exit: set[str] = set()
            while getattr(existing, "replaced_by", None):
                cur_id = str(getattr(existing, "id", "") or "")
                if cur_id in visited_exit:
                    raise RuntimeError("Cyclic broker exit replacement chain")
                visited_exit.add(cur_id)
                existing = broker.get_order_by_id(existing.replaced_by)
            raw_exit_status = getattr(existing, "status", "")
            exit_status = (
                str(getattr(raw_exit_status, "value", raw_exit_status) or "").strip().lower()
            )
            if exit_status not in TERMINAL:
                should_cancel, repeg_reason = _should_cancel_stale_exit_for_repeg(
                    existing,
                    side=self.side,
                    price=price,
                    target_limit=limit,
                    session=session,
                    now_utc=now_utc,
                    last_submit_mono=self.last_exit_submit_mono,
                )
                if should_cancel and attempt + 1 >= max_orders:
                    self._note_protection_fault(
                        "EXIT_LADDER_EXHAUSTED",
                        f"last exit order {exit_client_id} is stale and no rung is left",
                    )
                elif should_cancel and self.lane.rate_budget.try_consume(time.time(), is_exit=True):
                    try:
                        self.lane.broker_cancel(existing.id)
                    except APIError as exc:
                        if exc.status_code not in (404, 422):
                            raise
                        LOG.warning(
                            "Supervised exit cancel race for %s (%s): %s",
                            self.symbol,
                            exit_client_id,
                            exc,
                        )
                        return
                    self.exit_repeg_count += 1
                    self.last_exit_repeg_reason = repeg_reason
                    self.lane.counters["exit_repegs"] = self.lane.counters.get("exit_repegs", 0) + 1
                    LOG.warning(
                        "Cancelled stale supervised exit %s for %s (%s, attempt=%d/%d); "
                        "re-pegging to collared limit %s in %s",
                        exit_client_id,
                        self.symbol,
                        repeg_reason,
                        attempt + 1,
                        max_orders,
                        limit,
                        session,
                    )
                return
            # A spent order (filled in part, cancelled, expired or rejected) is history: what
            # is still held comes from the position below, never from summing these fills.
            rejected += int(exit_status == "rejected")
            attempt += 1
        else:
            self._note_protection_fault(
                "EXIT_LADDER_EXHAUSTED", f"{max_orders} exit orders ended without reaching flat"
            )
            return
        refusals = rejected + self.exit_refusal_count
        closing = attempt >= MAX_SUPERVISED_EXIT_ATTEMPTS or refusals >= EXIT_LADDER_MAX_REFUSALS
        if time.monotonic() < self.next_exit_attempt_mono:
            return  # Backing off after a refused close-position order.
        try:
            position = broker.get_open_position(self.symbol)
        except APIError as exc:
            if exc.status_code != 404:
                raise
            self._end_lifecycle_when_flat()
            return
        exposure = Decimal(str(position.qty))
        if exposure == 0:
            return
        if (exposure > 0) != long:
            # The broker holds the other side now: this guard must not trade it. The next
            # reconcile closes this lifecycle and adopts the position with its real side.
            self.side_flip_detected = True
            LOG.warning(
                "Position side flipped at the broker for %s on %s; guard side %s stands down",
                self.symbol,
                self.lane.account,
                self.side,
            )
            return
        held = abs(exposure)
        whole = held.to_integral_value(rounding=ROUND_FLOOR)
        self.fractional_remainder = held - whole
        stated_available = _available_qty(position, long=long)
        available = stated_available if stated_available is not None else held
        if whole >= 1 and available < whole and self._release_reserved_shares(broker):
            return  # The exit follows on the next tick, once the broker has released the shares.
        qty = min(whole, available.to_integral_value(rounding=ROUND_FLOOR))
        if qty < 1:
            if whole >= 1:
                self._note_protection_fault(
                    "EXIT_BLOCKED_NO_QTY_AVAILABLE",
                    f"held={held} available={available}",
                )
            else:
                # Less than one share is left: the whole-share part this guard supervises is
                # sold, so its lifecycle ends here instead of returning on every tick. The
                # remainder is reported by adoption as a fractional-only position.
                self._end_lifecycle_when_flat()
            return
        if closing:
            self._note_protection_fault(
                "EXIT_ESCALATED_CLOSE_POSITION",
                f"{exit_client_id}: {attempt} spent exit orders, {refusals} refusals",
            )
        exit_req: LimitOrderRequest | MarketOrderRequest
        exit_order_type: str
        exit_record_price: float
        if session == "RTH" or closing:
            exit_req = MarketOrderRequest(
                symbol=self.symbol,
                qty=float(qty),
                side=OrderSide.SELL if long else OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                client_order_id=exit_client_id,
            )
            exit_order_type = "STOP_MARKET_FAST_EXIT"
            exit_record_price = float(price)
        else:
            exit_req = LimitOrderRequest(
                symbol=self.symbol,
                qty=float(qty),
                side=OrderSide.SELL if long else OrderSide.BUY,
                limit_price=float(limit),
                time_in_force=TimeInForce.DAY,
                extended_hours=True,
                client_order_id=exit_client_id,
            )
            exit_order_type = "STOP_LIMIT_SUPERVISED_EXIT"
            exit_record_price = float(limit)
        try:
            submitted_exit = self.lane.broker_submit(exit_req, intent="PROTECTIVE")
        except APIError as exc:
            # Refused outright: no order exists, so this client id is still free. A definitive
            # refusal (4xx other than rate limiting) climbs the ladder, and a refused
            # close-position order backs off. A transient error (429, 5xx) is retried on the
            # next tick on the same rung.
            status_code = exc.status_code
            if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                self.exit_refusal_count += 1
                if closing:
                    self.next_exit_attempt_mono = time.monotonic() + min(
                        EXIT_REFUSAL_BACKOFF_MAX_SECS,
                        float(2 ** min(self.exit_refusal_count, 6)),
                    )
            self._note_protection_fault(f"EXIT_REFUSED_HTTP_{status_code}", exc)
            return
        self.last_exit_submit_mono = time.monotonic()
        exit_ack = _enum_text(getattr(submitted_exit, "status", ""))
        exit_oid = str(getattr(submitted_exit, "id", "") or "")
        if exit_oid:
            exit_raw_st = getattr(submitted_exit, "status", "submitted")
            exit_st = str(getattr(exit_raw_st, "value", exit_raw_st) or "submitted")
            self.lane._record_order(
                (
                    now_utc.isoformat(),
                    self.lane.account,
                    self.lane.profile["lane_id"],
                    exit_oid,
                    self.symbol,
                    "SHORT" if long else "LONG",
                    float(qty),
                    exit_order_type,
                    exit_record_price,
                    float(self.stop),
                    session,
                    exit_st,
                    0.0,
                )
            )
        if exit_ack == "rejected":
            # The rejected order keeps its client id at the broker: the next tick counts it and
            # sends the next rung.
            self._note_protection_fault("EXIT_REJECTED", exit_client_id)
            return
        # The refusal count stays for the life of the lifecycle: the ladder never steps back.
        self.next_exit_attempt_mono = 0.0
        if not closing:
            self.protection_fault_code = None
            self.protection_fault_count = 0
        parent_id = str(getattr(self.parent, "id", "") or self.client_id)
        if (sup := self.lane.supervised_stops.get(parent_id)) is not None and sup.status == "ARMED":
            trig_sup = sup.model_copy(
                update={"ts_utc": datetime.now(UTC).isoformat(), "status": "TRIGGERED"}
            )
            self.lane.supervised_stops[parent_id] = trig_sup
            persist_supervised_stop_duckdb(self.lane.duckdb_path, trig_sup)
        if self.fast_sell_armed:
            self.lane.counters["fast_sells"] += 1
        if force_exit or self.peak_reverse_armed:
            self.lane.counters["reverses"] += 1


def guard_should_flatten_for_flip(
    guard: Guard,
    *,
    market_regime: int,
    rows: dict[str, dict[str, Any]],
    session: str,
) -> bool:
    """Return True when live macro regime or scan route flips opposite an open managed guard."""
    if session == "CLOSED" or market_regime not in (-1, 0, 1):
        return False
    sym = guard.symbol.strip().upper()
    is_inverse = sym in _INVERSE_HEDGE_SYMBOLS
    if market_regime == -1 and guard.side == "LONG" and not is_inverse:
        return True
    if market_regime == 1 and (guard.side == "SHORT" or (guard.side == "LONG" and is_inverse)):
        return True
    row = rows.get(sym)
    if not row or not row.get("extreme_mover_eligible") or row.get("session") != session:
        return False
    route = select_short_or_inverse_fallback(
        sym,
        market_regime=market_regime,
        borrow_meta=row,
        session=session,
    )
    if not route:
        return False
    target_sym, target_side, origin = route
    if target_sym == sym and target_side != guard.side:
        return True
    return target_sym != sym and guard.side == "LONG" and origin.startswith("INVERSE_HEDGE_")


def apply_regime_flip_policy(
    guards: Mapping[str, Guard],
    *,
    market_regime: int,
    rows: dict[str, dict[str, Any]],
    session: str,
    generation_utc: datetime,
) -> list[str]:
    """Regime-flip flatten for one scan generation (R7); returns ``"<symbol>:<ACTION>"`` tags.

    A flip sets ``reverse_flatten``. On a paper lane it does so at once. On a live lane the flip
    must have stood for ``engine.REGIME_FLIP_MIN_GENERATIONS`` consecutive scan generations and
    ``engine.REGIME_FLIP_MIN_SECONDS`` (measured between scan timestamps), and a position whose
    opposing leveraged/inverse partner is held on the same lane (a pair the owner holds on
    purpose) is not flattened by a regime reading at all.

    When the regime reverts, the count starts over and a ``reverse_flatten`` that this policy
    set is withdrawn, provided no exit order has been sent. A trigger that came from the
    flatten request alone is withdrawn with it, so the stop-coverage reconciler puts the
    resting stop back. A flatten set by the peak-climax reverse is never touched here.
    """
    actions: list[str] = []
    for sym, guard in guards.items():
        if guard.lifecycle_closed:
            continue
        if not guard_should_flatten_for_flip(
            guard, market_regime=market_regime, rows=rows, session=session
        ):
            guard.regime_flip_generations = 0
            guard.regime_flip_since = None
            if not guard.regime_flip_flatten:
                continue
            guard.regime_flip_flatten = False
            if guard.last_exit_submit_mono > 0.0 or not guard.reverse_flatten:
                actions.append(f"{sym}:REGIME_REVERTED_EXIT_ALREADY_SENT")
                continue
            guard.reverse_flatten = False
            if guard.triggered and guard.trigger_cause == "REVERSE_FLATTEN":
                guard.triggered = False
                guard.trigger_cause = ""
                guard.save()
            actions.append(f"{sym}:REGIME_FLIP_CLEARED")
            continue
        guard.regime_flip_generations += 1
        if guard.regime_flip_since is None:
            guard.regime_flip_since = generation_utc
        if guard.reverse_flatten:
            continue
        if guard.lane.environment == "LIVE":
            same_lane = {
                other_sym.strip().upper(): (other.side, other.lane.account)
                for other_sym, other in guards.items()
                if other.lane is guard.lane and not other.lifecycle_closed
            }
            if find_opposing_inverse_pair_conflict(sym, guard.side, same_lane) is not None:
                actions.append(f"{sym}:REGIME_FLIP_HELD_OPPOSING_PAIR")
                continue
            stood_secs = (generation_utc - guard.regime_flip_since).total_seconds()
            if (
                guard.regime_flip_generations < engine.REGIME_FLIP_MIN_GENERATIONS
                or stood_secs < engine.REGIME_FLIP_MIN_SECONDS
            ):
                actions.append(f"{sym}:REGIME_FLIP_PENDING")
                continue
        guard.reverse_flatten = True
        guard.regime_flip_flatten = True
        actions.append(f"{sym}:REGIME_FLIP_FLATTEN")
    return actions


def protection_fault_alerts(code: str | None, consecutive: int) -> bool:
    """Whether a guard's protection fault is an alert (R13).

    Three in a row of any code, and at once for a code after which the guard sends nothing
    more on its own (a spent exit ladder): those shares then wait for an operator.
    """
    if not code:
        return False
    return consecutive >= PROTECTION_FAULT_ALERT_COUNT or code in _IMMEDIATE_ALERT_FAULT_CODES


def _consume_read_slot(lane: AlpacaPaperLane, now_s: float) -> bool:
    """Take one rate-budget slot for a broker read, never from the exit reserve (R14).

    A read that finds only the exit reserve left is deferred and counted
    (``reads_deferred_total``); the caller skips it and tries again on its next cycle.
    """
    if lane.rate_budget.try_consume(now_s, is_exit=False):
        return True
    lane.counters["reads_deferred_total"] = lane.counters.get("reads_deferred_total", 0) + 1
    return False


def _as_utc(moment: Any) -> datetime | None:
    """``moment`` as an aware UTC datetime; ``None`` when it is not a datetime."""
    if not isinstance(moment, datetime):
        return None
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def drive_guard_price(
    guard: Guard,
    price: Decimal,
    session: str,
    *,
    trade_ts: Any,
    source: Literal["STREAM", "REST"] = "STREAM",
) -> bool:
    """Feed one print to a guard (R2); True when the guard acted on it.

    The print is taken when its exchange timestamp is not older than the last one the guard
    acted on. No wall clock is involved, so a host clock that runs behind or ahead cannot
    blind a guard. A guard that has had no price for ``GUARD_PRICE_REST_FALLBACK_SECS`` takes
    the next print whatever its timestamp, so one bad timestamp cannot starve it either.

    A REST latest-trade poll that returns the print the guard already has confirms that
    nothing newer traded: the price age restarts, and the guard is driven again only when it
    has an exit in progress (the exit ladder advances on every tick).

    An error while supervising this position is this guard's protection fault (R13) with a
    consecutive count; it is never raised, so other symbols and entries carry on.
    """
    if not price.is_finite() or price <= 0:
        return False
    now_mono = time.monotonic()
    guard.last_heartbeat_mono = now_mono
    stamp = _as_utc(trade_ts)
    last = guard.last_trade_ts
    if stamp is not None and last is not None:
        starving = guard.price_age_secs(now_mono) > GUARD_PRICE_REST_FALLBACK_SECS
        if stamp < last and not starving:
            return False  # An out-of-order print: the guard already acted on a newer one.
        if stamp == last and source == "REST" and not (guard.triggered or guard.reverse_flatten):
            guard.last_price_mono = now_mono
            guard.last_price_source = source
            return False
    if stamp is not None:
        guard.last_trade_ts = stamp
    guard.last_price_source = source
    try:
        guard.on_price(price, session)
    except Exception as exc:  # noqa: BLE001
        guard.last_price_mono = now_mono
        guard._note_protection_fault(
            f"{_SUPERVISION_FAULT_PREFIX}{type(exc).__name__.upper()}", exc
        )
        return False
    if (guard.protection_fault_code or "").startswith(_SUPERVISION_FAULT_PREFIX):
        guard.protection_fault_code = None
        guard.protection_fault_count = 0
    return True


def _measure_broker_clock_skew(broker: Any) -> Decimal | None:
    """Broker clock minus host clock, in seconds; ``None`` when it cannot be measured.

    The host time is the midpoint of the request. A read that takes longer than twice the
    skew limit cannot decide a one-second question and is discarded, as is a clock that
    states no timestamp.
    """
    get_clock = getattr(broker, "get_clock", None)
    if not callable(get_clock):
        return None
    sent = time.time()
    try:
        clock = get_clock()
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Broker clock unreadable: %s", exc)
        return None
    received = time.time()
    stamp = _as_utc(getattr(clock, "timestamp", None))
    if stamp is None or received - sent > 2 * float(CLOCK_SKEW_MAX_SECS):
        return None
    host_mid = (Decimal(str(sent)) + Decimal(str(received))) / 2
    return (Decimal(str(stamp.timestamp())) - host_mid).quantize(Decimal("0.001"))


def _close_rejected_entry(
    farm: PaperFarmCoordinator,
    lane: AlpacaPaperLane,
    client_order_id: str,
    symbol: str,
    status_code: int,
) -> str:
    """A broker refusal of an entry submit is that order's outcome (R13), not a farm fault.

    The entry-decision row is closed with the reject code, which also ends its claim on the
    opposing leveraged/inverse family, and the symbol lock is released. Returns the code.
    """
    cause = f"{_ENTRY_REJECT_CAUSE_PREFIX}{status_code}"
    with open_bounded_duckdb(lane.duckdb_path) as db:
        _ensure_entry_decisions_table(db)
        db.execute(
            "UPDATE paper_entry_decisions SET closed_at=current_timestamp, close_cause=? "
            "WHERE client_order_id=? AND closed_at IS NULL",
            [cause, client_order_id],
        )
    farm.symbol_lock.release(symbol, lane.account)
    for counter in ("entry_rejected_total", f"entry_rejected_http_{status_code}"):
        lane.counters[counter] = lane.counters.get(counter, 0) + 1
    LOG.warning(
        "Entry %s for %s on %s refused by the broker (HTTP %s): row closed, entries continue",
        client_order_id,
        symbol,
        lane.account,
        status_code,
    )
    return cause


def _exit_collar_mult(attempt: int, refusals: int) -> int:
    """Collar rung of the exit ladder: 1x, widening to ``EXIT_LADDER_MAX_COLLAR_MULT`` x."""
    return min(
        EXIT_LADDER_MAX_COLLAR_MULT,
        1 + max(0, attempt) // EXIT_LADDER_ATTEMPTS_PER_RUNG + max(0, refusals),
    )


def _enum_text(raw: Any) -> str:
    """Lower-case text of a broker enum, or of the plain string a broker double carries."""
    return str(getattr(raw, "value", raw) or "").strip().lower()


def _is_harness_stop(order: Any) -> bool:
    """True for a standalone fixed stop this harness placed (client order id ``stop-...``)."""
    return (
        str(getattr(order, "client_order_id", "") or "").startswith(_HARNESS_STOP_PREFIX)
        and _order_type_text(order) != _TRAILING_STOP_ORDER_TYPE
    )


def _available_qty(position: Any, *, long: bool) -> Decimal | None:
    """Shares of a position no resting order has reserved; ``None`` when the broker states none.

    For a long the figure goes below zero when resting orders hold more than is left: that is
    read as nothing free. For a short the broker reports it with the position's sign, so the
    magnitude is taken.
    """
    raw = getattr(position, "qty_available", None)
    if raw in (None, "") or isinstance(raw, bool):
        return None
    try:
        available = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    if not available.is_finite():
        return None
    return max(Decimal("0"), available) if long else abs(available)


def _order_type_text(order: Any) -> str:
    return _enum_text(getattr(order, "type", None) or getattr(order, "order_type", None))


def _order_remaining_qty(order: Any) -> Decimal | None:
    """Unfilled share quantity of an order; ``None`` when the order states no quantity."""
    raw_qty = getattr(order, "qty", None)
    if raw_qty in (None, "") or isinstance(raw_qty, bool):
        return None
    try:
        qty = Decimal(str(raw_qty))
        filled = Decimal(str(getattr(order, "filled_qty", None) or "0"))
    except (InvalidOperation, ValueError):
        return None
    if not qty.is_finite() or not filled.is_finite():
        return None
    return max(Decimal("0"), qty - filled)


def _normalize_broker_stop_child(
    order: Any, *, symbol: str, exit_side: str, stop_price: Decimal
) -> Any:
    raw_status = getattr(order, "status", "new")
    status_val = str(getattr(raw_status, "value", raw_status) or "new")
    raw_side = getattr(order, "side", exit_side)
    side_val = str(getattr(raw_side, "value", raw_side) or exit_side)
    raw_qty = getattr(order, "qty", None)
    return SimpleNamespace(
        id=str(getattr(order, "id", "") or ""),
        symbol=str(getattr(order, "symbol", None) or symbol),
        side=SimpleNamespace(value=side_val),
        status=SimpleNamespace(value=status_val),
        stop_price=str(getattr(order, "stop_price", None) or stop_price),
        replaced_by=getattr(order, "replaced_by", None),
        filled_qty=str(getattr(order, "filled_qty", None) or "0"),
        qty=None if raw_qty in (None, "") else str(raw_qty),
        client_order_id=str(getattr(order, "client_order_id", "") or ""),
        order_type=_order_type_text(order),
        time_in_force=_enum_text(getattr(order, "time_in_force", None)),
    )


def _read_open_orders(broker: Any) -> list[Any] | None:
    """Every open order of the account with multi-leg orders flattened; ``None`` if unreadable."""
    get_orders = getattr(broker, "get_orders", None)
    if not callable(get_orders):
        return None
    try:
        pending = list(get_orders(GetOrdersRequest(limit=500)) or ())
    except Exception:  # noqa: BLE001
        return None
    flat: list[Any] = []
    seen: set[str] = set()
    pending.reverse()
    while pending:
        order = pending.pop()
        key = str(getattr(order, "id", "") or id(order))
        if key in seen:
            continue
        seen.add(key)
        flat.append(order)
        pending.extend(getattr(order, "legs", None) or ())
    return flat


def _exit_side_orders(open_orders: Iterable[Any], symbol: str, exit_side: str) -> list[Any]:
    """Working exit-side orders of ``symbol``.

    A ``held`` leg waits for its parent and reserves no shares, and an order on its way out
    (``pending_cancel``) is no longer working: neither is listed.
    """
    matched: list[Any] = []
    for order in open_orders:
        if str(getattr(order, "symbol", "") or "").strip().upper() != symbol:
            continue
        if _enum_text(getattr(order, "side", "")) != exit_side:
            continue
        if (_enum_text(getattr(order, "status", "new")) or "new") in _WORKING_ORDER_STATUSES:
            matched.append(order)
    return matched


def _is_resting_cover_stop(order: Any) -> bool:
    """True for a GTC stop, stop-limit or trailing stop: the only orders that count as cover.

    A DAY stop lapses at the close, so it is not cover. An order that states no type counts as
    a stop when it carries a stop price, and one that states no time in force is taken as
    resting: hand-built broker doubles state neither, the broker always states both.
    """
    order_type = _order_type_text(order)
    if order_type:
        if order_type not in _STOP_ORDER_TYPES and order_type != _TRAILING_STOP_ORDER_TYPE:
            return False
    elif getattr(order, "stop_price", None) in (None, "", "0", 0):
        return False
    return _enum_text(getattr(order, "time_in_force", None)) in ("", "gtc")


def _find_open_broker_stop(
    broker: Any, symbol: str, exit_side: str, *, open_orders: list[Any] | None = None
) -> Any:
    """The resting fixed stop a guard can track and replace: right side, type and status.

    Side must be the exit side, the type a stop or stop-limit (never a trailing stop, whose
    stop price cannot be replaced), the order working, GTC and not ``held``. Of several, the
    one with the largest unfilled quantity is returned; its ``qty`` lets the caller top up
    partial coverage.
    """
    orders = open_orders if open_orders is not None else _read_open_orders(broker)
    if orders is None:
        return None
    best: Any = None
    best_remaining = Decimal("-1")
    for ord_ in _exit_side_orders(orders, symbol, exit_side):
        if not _is_resting_cover_stop(ord_) or _order_type_text(ord_) == _TRAILING_STOP_ORDER_TYPE:
            continue
        if getattr(ord_, "stop_price", None) in (None, "", "0", 0):
            continue
        remaining = _order_remaining_qty(ord_)
        rank = remaining if remaining is not None else Decimal("0")
        if rank > best_remaining:
            best, best_remaining = ord_, rank
    if best is None:
        return None
    return _normalize_broker_stop_child(
        best, symbol=symbol, exit_side=exit_side, stop_price=Decimal(str(best.stop_price))
    )


def _note_stop_fault(
    lane: AlpacaPaperLane, guard: Guard, exc: APIError, *, fault_counter: str, action: str
) -> str:
    """Count a refused stop order, back the guard off and return the action tag."""
    guard.stop_attach_fault_count += 1
    opposite_side = "wash trade" in str(exc).lower()
    delay = (
        STOP_ATTACH_BACKOFF_MAX_SECS
        if opposite_side
        else min(
            STOP_ATTACH_BACKOFF_MAX_SECS,
            STOP_ATTACH_BACKOFF_BASE_SECS * float(2 ** min(guard.stop_attach_fault_count - 1, 6)),
        )
    )
    guard.next_stop_attach_mono = time.monotonic() + delay
    lane.counters[fault_counter] = lane.counters.get(fault_counter, 0) + 1
    if opposite_side:
        lane.counters["stop_attach_blocked_opposite_side"] = (
            lane.counters.get("stop_attach_blocked_opposite_side", 0) + 1
        )
    LOG.warning(
        "Native stop %s refused for %s on %s (HTTP %s): %s; the position is tick-supervised "
        "and the next try is in %.0f s",
        action,
        guard.symbol,
        lane.account,
        exc.status_code,
        exc,
        delay,
    )
    return "BLOCKED_BY_OPPOSITE_SIDE_ORDER" if opposite_side else f"FAULT_HTTP_{exc.status_code}"


def _stop_mutation_gate(lane: AlpacaPaperLane, guard: Guard, now_utc: datetime) -> str | None:
    """``None`` when a stop order may be sent for this guard now, else why not (an action tag).

    A rate-budget refusal is counted (``stop_attach_deferred_total``) and retried next tick.
    """
    if time.monotonic() < guard.next_stop_attach_mono:
        return "BACKOFF"
    if not lane.rate_budget.try_consume(now_utc.timestamp(), is_exit=True):
        lane.counters["stop_attach_deferred_total"] = (
            lane.counters.get("stop_attach_deferred_total", 0) + 1
        )
        return "DEFERRED_RATE_BUDGET"
    return None


def _ensure_guard_stop_coverage(
    lane: AlpacaPaperLane,
    guard: Guard,
    *,
    position_qty: Decimal,
    qty_available: Decimal | None,
    open_orders: list[Any],
    session: str,
    now_utc: datetime,
    fault_counter: str = "stop_attach_faults",
) -> list[str]:
    """Bring one guard's resting exit-side stop quantity to its whole-share position.

    Cover is every working GTC stop on the exit side. Stops this harness placed (client order
    id ``stop-...``) are sized to what the other cover leaves open: attached when missing,
    resized when the position grows or shrinks, released when other cover (an activated stop
    leg) already holds the shares. A fractional remainder is recorded on the guard, not stopped.
    """
    tag = f"{lane.account}:{guard.symbol}:"
    whole = position_qty.to_integral_value(rounding=ROUND_FLOOR)
    guard.fractional_remainder = position_qty - whole
    if guard.triggered or guard.reverse_flatten or guard.lifecycle_closed:
        return []  # The exit state machine owns these shares.
    exit_side = "sell" if guard.side == "LONG" else "buy"
    working = _exit_side_orders(open_orders, guard.symbol, exit_side)
    if any(
        str(getattr(order, "client_order_id", "") or "").startswith(_HARNESS_EXIT_PREFIX)
        for order in working
    ):
        return []  # An exit of this guard is in flight.
    if whole < 1:
        return [tag + "FRACTIONAL_ONLY"]
    cover = [order for order in working if _is_resting_cover_stop(order)]
    own: list[Any] = []
    other_qty = Decimal("0")
    for order in cover:
        if _is_harness_stop(order):
            own.append(order)
            continue
        remaining = _order_remaining_qty(order)
        # A stop that states no quantity is taken to cover the whole position.
        other_qty += remaining if remaining is not None else whole
    own_qty = sum((_order_remaining_qty(order) or Decimal("0") for order in own), Decimal("0"))
    child = guard.child
    listed = {str(getattr(order, "id", "") or "") for order in cover}
    if (
        other_qty + own_qty < whole
        and child is not None
        and str(getattr(child, "id", "") or "") not in listed
    ):
        # The list does not show the stop this guard already knows. Ask for it by id before
        # sending another: a just-accepted order can be missing from the list.
        try:
            guard.fetch()
        except (APIError, OSError) as exc:
            LOG.warning(
                "Stop verification failed for %s on %s: %s", guard.symbol, lane.account, exc
            )
            return [tag + "VERIFY_FAULT"]
        child = guard.child
        if (
            child is not None
            and _enum_text(getattr(child, "status", "")) in _WORKING_ORDER_STATUSES
        ):
            remaining = _order_remaining_qty(child)
            child_qty = remaining if remaining is not None else whole
            if _is_harness_stop(child):
                own.append(child)
                own_qty += child_qty
            else:
                other_qty += child_qty
    target_own = max(Decimal("0"), whole - other_qty)
    available = (
        qty_available.to_integral_value(rounding=ROUND_FLOOR)
        if qty_available is not None
        else whole
    )
    broker = lane.trading_client
    stop_side = OrderSide.SELL if guard.side == "LONG" else OrderSide.BUY

    def link_child() -> None:
        # An adopted guard tracks the resting fixed stop it finds, so that it ratchets it.
        if guard._synthetic_parent:
            found = _find_open_broker_stop(broker, guard.symbol, exit_side, open_orders=open_orders)
            if found is not None and (
                guard.child is None or str(getattr(guard.child, "id", "")) != found.id
            ):
                guard.child = found

    if len(own) > 1:
        # One harness stop per guard: keep the largest, release the rest, size it next tick.
        own.sort(key=lambda order: _order_remaining_qty(order) or Decimal("0"), reverse=True)
        actions: list[str] = []
        for extra in own[1:]:
            if (gate := _stop_mutation_gate(lane, guard, now_utc)) is not None:
                return [*actions, tag + gate]
            try:
                lane.broker_cancel(extra.id)
            except APIError as exc:
                if exc.status_code not in (404, 422):
                    return [
                        *actions,
                        tag
                        + _note_stop_fault(
                            lane, guard, exc, fault_counter=fault_counter, action="release"
                        ),
                    ]
            actions.append(tag + "STOP_DUPLICATE_RELEASED")
        return actions

    if not own:
        if target_own == 0:
            guard.stop_attach_fault_count = 0
            link_child()
            return []
        attach_qty = min(target_own, available)
        if attach_qty < 1:
            return [tag + "UNCOVERED_NO_QTY_AVAILABLE"]
        if (gate := _stop_mutation_gate(lane, guard, now_utc)) is not None:
            return [tag + gate]
        stop_req = StopOrderRequest(
            symbol=guard.symbol,
            qty=float(attach_qty),
            side=stop_side,
            stop_price=float(guard.stop),
            time_in_force=TimeInForce.GTC,
            client_order_id=guard._next_stop_client_id(),
        )
        try:
            submitted_stop = lane.broker_submit(stop_req, intent="PROTECTIVE")
        except APIError as exc:
            # A refused native stop must not leave this position, or the ones after it, without
            # a Guard: record the fault and supervise on ticks instead.
            return [
                tag
                + _note_stop_fault(lane, guard, exc, fault_counter=fault_counter, action="attach")
            ]
        if _enum_text(getattr(submitted_stop, "status", "")) == "rejected":
            lane.counters[fault_counter] = lane.counters.get(fault_counter, 0) + 1
            guard.stop_attach_fault_count += 1
            guard.next_stop_attach_mono = time.monotonic() + STOP_ATTACH_BACKOFF_BASE_SECS
            return [tag + "STOP_REJECTED"]
        guard.stop_attach_fault_count = 0
        lane.counters["stop_attaches"] = lane.counters.get("stop_attaches", 0) + 1
        if guard._synthetic_parent:
            guard.child = _normalize_broker_stop_child(
                submitted_stop, symbol=guard.symbol, exit_side=exit_side, stop_price=guard.stop
            )
        return [tag + ("STOP_ATTACHED" if attach_qty == target_own else "STOP_ATTACHED_PARTIAL")]

    stop_order = own[0]
    stop_status = _enum_text(getattr(stop_order, "status", ""))
    if stop_status in ("partially_filled", "pending_replace", "pending_new"):
        return []  # Executing or in transit: nothing to size until it settles.
    current = _order_remaining_qty(stop_order)
    current_qty = current if current is not None else target_own
    if target_own == 0:
        if (gate := _stop_mutation_gate(lane, guard, now_utc)) is not None:
            return [tag + gate]
        try:
            lane.broker_cancel(stop_order.id)
        except APIError as exc:
            if exc.status_code not in (404, 422):
                return [
                    tag
                    + _note_stop_fault(
                        lane, guard, exc, fault_counter=fault_counter, action="release"
                    )
                ]
        lane.counters["stop_releases"] = lane.counters.get("stop_releases", 0) + 1
        if guard._synthetic_parent and str(getattr(guard.child, "id", "")) == str(stop_order.id):
            guard.child = None
        return [tag + "STOP_RELEASED"]
    # A grown position can only be covered up to what the broker has not reserved elsewhere.
    new_qty = min(target_own, current_qty + available) if target_own > current_qty else target_own
    raw_stop_px = getattr(stop_order, "stop_price", None)
    stop_px = Decimal(str(raw_stop_px)) if raw_stop_px not in (None, "", "0", 0) else guard.stop
    behind = stop_px < guard.stop if guard.side == "LONG" else stop_px > guard.stop
    replace_due = time.monotonic() - guard.last_stop_replace_mono >= STOP_REPLACE_MIN_INTERVAL_SECS
    reprice = session == "RTH" and behind and replace_due
    if new_qty == current_qty and not reprice:
        guard.stop_attach_fault_count = 0
        link_child()
        return [] if new_qty == target_own else [tag + "UNCOVERED_NO_QTY_AVAILABLE"]
    if not replace_due:
        # R14: one stop replace per guard and second, a resize included. The next reconcile
        # sends it; until then the coverage audit shows what this stop does not cover.
        lane.counters["stop_replace_deferred_total"] = (
            lane.counters.get("stop_replace_deferred_total", 0) + 1
        )
        return [tag + "REPLACE_THROTTLED"]
    if (gate := _stop_mutation_gate(lane, guard, now_utc)) is not None:
        return [tag + gate]
    filled = Decimal(str(getattr(stop_order, "filled_qty", None) or "0"))
    try:
        replaced = lane.broker_replace(
            stop_order.id,
            ReplaceOrderRequest(
                qty=int(new_qty + filled),
                stop_price=float(guard.stop if behind else stop_px),
                # The replacement is a new order: it keeps the harness's own id form.
                client_order_id=guard._next_stop_client_id(),
            ),
        )
    except APIError as exc:
        return [
            tag + _note_stop_fault(lane, guard, exc, fault_counter=fault_counter, action="resize")
        ]
    guard.last_stop_replace_mono = time.monotonic()
    if _enum_text(getattr(replaced, "status", "")) == "rejected":
        lane.counters[fault_counter] = lane.counters.get(fault_counter, 0) + 1
        guard.stop_attach_fault_count += 1
        guard.next_stop_attach_mono = time.monotonic() + STOP_ATTACH_BACKOFF_BASE_SECS
        return [tag + "STOP_REJECTED"]
    guard.stop_attach_fault_count = 0
    counter = "stop_resizes" if new_qty != current_qty else "stop_reprices"
    lane.counters[counter] = lane.counters.get(counter, 0) + 1
    if guard._synthetic_parent:
        guard.child = _normalize_broker_stop_child(
            replaced, symbol=guard.symbol, exit_side=exit_side, stop_price=guard.stop
        )
    return [tag + ("STOP_RESIZED" if new_qty != current_qty else "STOP_REPRICED")]


def ensure_native_protective_stops(
    farm: PaperFarmCoordinator,
    guards: dict[str, Guard],
    *,
    session: str,
    now_utc: datetime,
    snapshots: Mapping[str, tuple[list[Any], list[Any]]] | None = None,
) -> list[str]:
    """Stop-coverage reconciler (R1): resting exit-side stop quantity equals each position.

    Reads positions and open orders of every lane itself and acts per position, in every
    session: a GTC stop rests off-hours and arms at the next open, alongside the synthetic
    tick supervision. It re-attaches after an external cancel, resizes when the position grows
    or shrinks, tops up partial coverage and covers a partially filled OTO parent. A standalone
    stop of this harness whose symbol is no longer held is released. A rate-budget refusal is
    counted (``stop_attach_deferred_total``) and retried on the next call.

    ``snapshots`` (account -> positions, flattened open orders) lets a caller that has just
    read the broker hand those reads in, so that one pair of reads per lane serves both this
    reconciler and the coverage auditor. An account missing from it was not read this cycle
    and is left alone; the caller that read knows and reports why.

    Returns one ``"<account>:<symbol>:<ACTION>"`` tag per action or finding. ``UNMANAGED`` (a
    position without a guard), ``SIDE_FLIP``, ``FLAT`` (a filled guard without a position) and
    ``LIFECYCLE_ENDED`` ask the caller to rebuild the guards (``restore_guards`` + adoption).
    Never raises on a broker refusal; a lane that fails its integrity check does raise.
    """
    actions: list[str] = []
    heartbeat_mono = time.monotonic()
    for acct, lane in farm.lanes.items():
        lane.assert_integrity()
        broker = lane.trading_client
        open_orders: list[Any] | None
        if snapshots is not None:
            if acct not in snapshots:
                continue
            positions, open_orders = snapshots[acct]
        else:
            try:
                positions = list(broker.get_all_positions() or ())
            except (APIError, OSError) as exc:
                LOG.warning("Stop coverage: positions of %s unreadable: %s", acct, exc)
                actions.append(f"{acct}:*:READ_FAULT")
                continue
            open_orders = _read_open_orders(broker)
            if open_orders is None:
                LOG.warning("Stop coverage: open orders of %s unreadable", acct)
                actions.append(f"{acct}:*:READ_FAULT")
                continue
        for guard in guards.values():
            if guard.lane is lane:
                guard.last_heartbeat_mono = heartbeat_mono  # the supervisor looked at it
        held_symbols: set[str] = set()
        listed_symbols: set[str] = set()
        for pos in positions:
            listed_symbols.add(str(getattr(pos, "symbol", "") or "").strip().upper())
            parsed = _parse_position_for_adoption(pos)
            if parsed is None:
                continue
            sym, side, abs_qty, _entry_px, _curr_px = parsed
            held_symbols.add(sym)
            guard = guards.get(sym)
            if guard is None or guard.lane is not lane:
                actions.append(f"{acct}:{sym}:UNMANAGED")
                continue
            if guard.lifecycle_closed:
                actions.append(f"{acct}:{sym}:LIFECYCLE_ENDED")
                continue
            if guard.side != side:
                guard.side_flip_detected = True
                actions.append(f"{acct}:{sym}:SIDE_FLIP")
                continue
            actions.extend(
                _ensure_guard_stop_coverage(
                    lane,
                    guard,
                    position_qty=abs_qty,
                    qty_available=_available_qty(pos, long=side == "LONG"),
                    open_orders=open_orders,
                    session=session,
                    now_utc=now_utc,
                )
            )
        for sym, guard in guards.items():
            if guard.lane is not lane or sym in held_symbols:
                continue
            if guard.lifecycle_closed:
                actions.append(f"{acct}:{sym}:LIFECYCLE_ENDED")
            elif guard._synthetic_parent or Decimal(
                str(getattr(guard.parent, "filled_qty", None) or "0")
            ) > Decimal("0"):
                actions.append(f"{acct}:{sym}:FLAT")
        # A standalone stop of this harness whose symbol is no longer held covers nothing: left
        # resting it would open a position when it triggers. Release it, but only once its
        # guard is gone: the rebuild that removes the guard reads the positions a second time,
        # so one wrong position list never costs a held position its stop.
        for order in open_orders:
            orphan_sym = str(getattr(order, "symbol", "") or "").strip().upper()
            if orphan_sym in listed_symbols or not _is_harness_stop(order):
                continue
            if _enum_text(getattr(order, "status", "")) not in _WORKING_ORDER_STATUSES:
                continue
            standing = guards.get(orphan_sym)
            if standing is not None and standing.lane is lane and not standing.lifecycle_closed:
                continue
            if not lane.rate_budget.try_consume(now_utc.timestamp(), is_exit=True):
                lane.counters["stop_attach_deferred_total"] = (
                    lane.counters.get("stop_attach_deferred_total", 0) + 1
                )
                actions.append(f"{acct}:{orphan_sym}:DEFERRED_RATE_BUDGET")
                continue
            try:
                lane.broker_cancel(order.id)
            except APIError as exc:
                if exc.status_code not in (404, 422):
                    LOG.warning(
                        "Orphan stop release refused for %s on %s (HTTP %s): %s",
                        orphan_sym,
                        acct,
                        exc.status_code,
                        exc,
                    )
                    actions.append(f"{acct}:{orphan_sym}:FAULT_HTTP_{exc.status_code}")
                    continue
            lane.counters["stop_orphan_releases"] = lane.counters.get("stop_orphan_releases", 0) + 1
            actions.append(f"{acct}:{orphan_sym}:STOP_ORPHAN_RELEASED")
    return actions


def _read_broker_snapshots(
    farm: PaperFarmCoordinator, now_s: float
) -> tuple[dict[str, tuple[list[Any], list[Any]]], list[str]]:
    """Positions and flattened open orders of every lane: two budgeted reads per lane (R14).

    A lane whose read budget is spent (``READ_DEFERRED``) or whose broker cannot be read
    (``READ_FAULT``) is left out and named in the returned tags. Only a broker or transport
    error is absorbed; anything else propagates to the caller's fault counter.
    """
    snapshots: dict[str, tuple[list[Any], list[Any]]] = {}
    faults: list[str] = []
    for acct, lane in farm.lanes.items():
        lane.assert_integrity()
        if not (_consume_read_slot(lane, now_s) and _consume_read_slot(lane, now_s)):
            faults.append(f"{acct}:*:READ_DEFERRED")
            continue
        try:
            positions = list(lane.trading_client.get_all_positions() or ())
        except (APIError, OSError) as exc:
            LOG.warning("Broker snapshot: positions of %s unreadable: %s", acct, exc)
            faults.append(f"{acct}:*:READ_FAULT")
            continue
        open_orders = _read_open_orders(lane.trading_client)
        if open_orders is None:
            LOG.warning("Broker snapshot: open orders of %s unreadable", acct)
            faults.append(f"{acct}:*:READ_FAULT")
            continue
        snapshots[acct] = (positions, open_orders)
    return snapshots, faults


def _position_identity(pos: Any) -> tuple[str, risk.Side, Decimal] | None:
    """``(symbol, side, absolute quantity)`` of a broker position; ``None`` when it holds none."""
    sym = str(getattr(pos, "symbol", "") or "").strip().upper()
    try:
        raw_qty = Decimal(str(getattr(pos, "qty", "0") or "0"))
    except (InvalidOperation, ValueError):
        return None
    if not sym or not raw_qty.is_finite() or raw_qty == 0:
        return None
    raw_side = _enum_text(getattr(pos, "side", ""))
    side: risk.Side = "SHORT" if (raw_qty < 0 or raw_side in ("short", "sell")) else "LONG"
    return sym, side, abs(raw_qty)


def _coverage_rows(
    farm: PaperFarmCoordinator,
    guards: Mapping[str, Guard],
    positions: Mapping[str, Mapping[str, Any]],
    open_orders: Mapping[str, list[Any] | None],
    *,
    session: str,
    ineligible: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Per-position share protection from broker truth, and the positions that hold entries.

    Native cover is the unfilled quantity of resting exit-side GTC stops (stop, stop-limit or
    trailing; never a DAY stop or a ``held`` leg), capped at the whole-share position. What it
    leaves open is synthetically supervised when a guard of the same lane and side has a
    confirmed price no older than ``SYNTHETIC_PRICE_AGE_MAX_SECS`` and was looked at within
    ``SYNTHETIC_HEARTBEAT_MAX_SECS``, in a session in which an exit can execute; the rest is
    unprotected. Guards only classify that remainder: no share count comes from a guard.

    The second value lists ``"<account>:<symbol>"`` of every position that fails the entry
    protection check (R13): no guard, or neither full native cover nor an exit in flight.
    Positions adoption found ineligible (``ineligible``) are reported but hold no entries.
    """
    now_mono = time.monotonic()
    rows: list[dict[str, Any]] = []
    gaps: list[str] = []
    skipped = ineligible or {}
    for acct, lane in farm.lanes.items():
        lane_orders = open_orders.get(acct)
        for pos in (positions.get(acct) or {}).values():
            ident = _position_identity(pos)
            if ident is None:
                continue
            sym, side, abs_qty = ident
            whole = int(abs_qty.to_integral_value(rounding=ROUND_FLOOR))
            exit_side = "sell" if side == "LONG" else "buy"
            working = _exit_side_orders(lane_orders or (), sym, exit_side)
            native = Decimal("0")
            stop_ids: list[str] = []
            for order in working:
                if not _is_resting_cover_stop(order):
                    continue
                remaining = _order_remaining_qty(order)
                native += remaining if remaining is not None else Decimal(whole)
                stop_ids.append(str(getattr(order, "id", "") or ""))
            native_shares = min(whole, int(native.to_integral_value(rounding=ROUND_FLOOR)))
            exit_in_flight = any(
                str(getattr(order, "client_order_id", "") or "").startswith(_HARNESS_EXIT_PREFIX)
                for order in working
            )
            guard = guards.get(sym)
            if guard is not None and (
                guard.lane is not lane or guard.lifecycle_closed or guard.side != side
            ):
                guard = None
            price_age = guard.price_age_secs(now_mono) if guard is not None else None
            heartbeat_age = guard.heartbeat_age_secs(now_mono) if guard is not None else None
            remainder = whole - native_shares
            reason = ""
            if guard is None:
                reason = "NO_GUARD"
            elif session == "CLOSED":
                reason = "MARKET_CLOSED_NO_NATIVE_STOP"
            elif price_age is not None and price_age > SYNTHETIC_PRICE_AGE_MAX_SECS:
                reason = "DATA_STALE"
            elif heartbeat_age is not None and heartbeat_age > SYNTHETIC_HEARTBEAT_MAX_SECS:
                reason = "HEARTBEAT_STALE"
            synthetic_shares = 0 if reason else remainder
            unprotected_shares = remainder - synthetic_shares
            if whole < 1:
                mode = "UNPROTECTED:FRACTIONAL_ONLY"
            elif native_shares >= whole:
                mode = "NATIVE_BROKER_STOP"
            elif native_shares > 0:
                mode = "NATIVE_PARTIAL"
            elif not reason:
                mode = "SYNTHETIC_TICK_SUPERVISED"
            else:
                mode = f"UNPROTECTED:{reason}"
            rows.append(
                {
                    "account": acct,
                    "symbol": sym,
                    "side": side,
                    "position_qty": str(abs_qty),
                    "whole_shares": whole,
                    "natively_protected_shares": native_shares,
                    "synthetically_supervised_shares": synthetic_shares,
                    "unprotected_shares_count": unprotected_shares,
                    "fractional_unprotected_qty": str(abs_qty - whole),
                    "mode": mode,
                    "unprotected_reason": reason if unprotected_shares else None,
                    "exit_in_flight": exit_in_flight,
                    "orders_readable": lane_orders is not None,
                    "native_stop_order_ids": sorted(stop_ids),
                    "guard_client_order_id": guard.client_id if guard is not None else None,
                    "guard_price_age_secs": None if price_age is None else int(price_age),
                    "guard_heartbeat_age_secs": (
                        None if heartbeat_age is None else int(heartbeat_age)
                    ),
                    "protection_fault_code": (
                        guard.protection_fault_code if guard is not None else None
                    ),
                    "protection_fault_count": (
                        int(guard.protection_fault_count) if guard is not None else 0
                    ),
                }
            )
            key = f"{acct}:{sym}"
            if (
                whole >= 1
                and key not in skipped
                and (guard is None or (native_shares < whole and not exit_in_flight))
            ):
                gaps.append(key)
    rows.sort(key=lambda row: (row["account"], row["symbol"]))
    return rows, sorted(gaps)


def _ledger_broker_delta(
    lane: AlpacaPaperLane, lane_positions: Mapping[str, Any]
) -> dict[str, Any]:
    """Open ledger lifecycles against broker positions of one lane, by symbol and side."""
    broker_side: dict[str, str] = {}
    for pos in lane_positions.values():
        ident = _position_identity(pos)
        if ident is not None:
            broker_side[ident[0]] = ident[1]
    ledger_side: dict[str, str] = {}
    readable = True
    if lane.duckdb_path.is_file():
        try:
            with open_bounded_duckdb(lane.duckdb_path, read_only=True) as db:
                if ("paper_entry_decisions",) in db.execute("SHOW TABLES").fetchall():
                    for raw_sym, raw_side in db.execute(
                        "SELECT symbol, side FROM paper_entry_decisions WHERE closed_at IS NULL"
                    ).fetchall():
                        ledger_side[str(raw_sym or "").strip().upper()] = str(raw_side or "")
        except duckdb.Error as exc:
            LOG.warning("Coverage audit: ledger of %s unreadable: %s", lane.account, exc)
            readable = False
    return {
        "ledger_readable": readable,
        "ledger_open_not_at_broker": sorted(set(ledger_side) - set(broker_side)),
        "broker_position_not_in_ledger": sorted(set(broker_side) - set(ledger_side)),
        "side_mismatch": sorted(
            sym
            for sym in set(ledger_side) & set(broker_side)
            if ledger_side[sym] != broker_side[sym]
        ),
    }


def build_lossless_coverage_record(
    farm: PaperFarmCoordinator,
    guards: Mapping[str, Guard],
    positions: Mapping[str, Mapping[str, Any]],
    *,
    session: str,
    now_utc: datetime,
    last_feed_trade: Mapping[Any, datetime],
    open_orders: Mapping[str, list[Any] | None] | None = None,
    ineligible: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Lossless coverage record (R12), schema ``tier0.lossless-coverage.v1``.

    An audit that does not trust the supervisor's guards: every share count comes from two
    broker reads per lane, the positions (``positions``, as read by the caller) and the open
    orders with their legs (read here unless the caller hands in ``open_orders`` from the same
    read cycle). Per symbol and in total it states ``natively_protected_shares``,
    ``synthetically_supervised_shares`` and ``unprotected_shares_count``; a fractional
    remainder is reported apart, because it cannot carry a stop. All values are ``int`` or
    text: no float is written.
    """
    # A caller that hands in open orders has done this cycle's reads: an account it could not
    # read stays unread here (its orders count as unreadable), no request is added.
    orders_by_acct: dict[str, list[Any] | None] = {
        acct: (
            open_orders.get(acct)
            if open_orders is not None
            else _read_open_orders(lane.trading_client)
        )
        for acct, lane in farm.lanes.items()
    }
    rows, gaps = _coverage_rows(
        farm, guards, positions, orders_by_acct, session=session, ineligible=ineligible
    )
    fractional = sum((Decimal(row["fractional_unprotected_qty"]) for row in rows), Decimal("0"))
    unprotected = sum(row["unprotected_shares_count"] for row in rows)
    now_s = now_utc.timestamp()
    rate_budget: dict[str, dict[str, int]] = {}
    for acct, lane in sorted(farm.lanes.items()):
        budget = lane.rate_budget
        used = sum(1 for ts in tuple(getattr(budget, "_timestamps", ())) if ts > now_s - 60.0)
        rate_budget[acct] = {
            "used_rpm_60s": used,
            "max_rpm": int(budget.max_rpm),
            "entry_modify_rpm_budget": int(budget.max_rpm - budget.exit_reserve_rpm),
            "exit_reserve_rpm": int(budget.exit_reserve_rpm),
        }
    inputs = {
        "session": session,
        "positions": sorted(
            (row["account"], row["symbol"], row["side"], row["position_qty"]) for row in rows
        ),
        "orders": sorted(
            (
                acct,
                str(getattr(order, "id", "") or ""),
                str(getattr(order, "symbol", "") or ""),
                _enum_text(getattr(order, "side", "")),
                _order_type_text(order),
                _enum_text(getattr(order, "time_in_force", None)),
                _enum_text(getattr(order, "status", "")),
                str(getattr(order, "qty", "") or ""),
                str(getattr(order, "filled_qty", "") or ""),
            )
            for acct, lane_orders in orders_by_acct.items()
            for order in lane_orders or ()
        ),
        "orders_unreadable": sorted(a for a, o in orders_by_acct.items() if o is None),
        "guards": sorted(
            (guard.lane.account, sym, guard.side, guard.client_id) for sym, guard in guards.items()
        ),
    }
    return {
        "schema_version": LOSSLESS_COVERAGE_SCHEMA,
        "generator": LOSSLESS_COVERAGE_GENERATOR,
        "generated_at_utc": now_utc.isoformat(),
        "environment": farm.environment,
        "paper_only": farm.environment == "PAPER",
        "live_execution_enabled": farm.autonomous_opt_in is True,
        "session": session,
        "inputs_hash": hashlib.sha256(
            json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "all_shares_protected": unprotected == 0 and fractional == 0,
        "position_shares_total": sum(row["whole_shares"] for row in rows),
        "natively_protected_shares": sum(row["natively_protected_shares"] for row in rows),
        "synthetically_supervised_shares": sum(
            row["synthetically_supervised_shares"] for row in rows
        ),
        "unprotected_shares_count": unprotected,
        "fractional_unprotected_qty": str(fractional),
        "share_protection_table": rows,
        "offhours_exit_note": _OFFHOURS_EXIT_NOTE,
        "entry_hold_positions": gaps,
        "orders_unreadable_accounts": inputs["orders_unreadable"],
        "ledger_broker_delta": {
            acct: _ledger_broker_delta(lane, positions.get(acct) or {})
            for acct, lane in sorted(farm.lanes.items())
        },
        "rate_budget": rate_budget,
        "last_feed_trade_utc": {
            str(getattr(feed, "value", feed)): stamp.isoformat()
            for feed, stamp in last_feed_trade.items()
        },
    }


def _parse_position_for_adoption(
    pos: Any,
) -> tuple[str, risk.Side, Decimal, Decimal, Decimal] | None:
    sym = str(getattr(pos, "symbol", "") or "").strip().upper()
    if not sym:
        return None
    raw_qty = Decimal(str(getattr(pos, "qty", "0") or "0"))
    if not raw_qty.is_finite() or raw_qty == Decimal("0"):
        return None
    raw_side = str(getattr(pos, "side", "") or "").strip().lower()
    side: risk.Side = (
        "SHORT" if (raw_qty < Decimal("0") or raw_side in ("short", "sell")) else "LONG"
    )
    raw_entry = getattr(pos, "avg_entry_price", None) or getattr(pos, "current_price", None)
    if raw_entry in (None, "", "0", 0):
        return None
    entry_px = Decimal(str(raw_entry))
    if not entry_px.is_finite() or entry_px <= Decimal("0"):
        return None
    raw_curr = getattr(pos, "current_price", None)
    curr_px = entry_px
    if raw_curr not in (None, "", "0", 0):
        cand_curr = Decimal(str(raw_curr))
        if cand_curr.is_finite() and cand_curr > Decimal("0"):
            curr_px = cand_curr
    return sym, side, abs(raw_qty), entry_px, curr_px


def _adoption_ineligible_reason(pos: Any, broker: Any, abs_qty: Decimal) -> str | None:
    """Why a broker position cannot be put under a guard; ``None`` when it can (R5).

    Eligible: a US equity with at least one whole share whose asset is tradable. A field the
    position or the asset does not state never disqualifies it, and neither does an asset
    lookup that fails: an unguarded position is the worse outcome.
    """
    raw_class = getattr(pos, "asset_class", None)
    if raw_class is not None and _enum_text(raw_class) not in ("", "us_equity"):
        return "NOT_US_EQUITY"
    if abs_qty.to_integral_value(rounding=ROUND_FLOOR) < 1:
        return "FRACTIONAL_ONLY"
    get_asset = getattr(broker, "get_asset", None)
    if callable(get_asset):
        try:
            asset = get_asset(str(getattr(pos, "symbol", "") or "").strip().upper())
        except Exception:  # noqa: BLE001
            return None
        if getattr(asset, "tradable", True) is False:
            return "ASSET_NOT_TRADABLE"
    return None


def adopt_unmanaged_broker_positions(
    farm: PaperFarmCoordinator,
    guards: dict[str, Guard],
    *,
    positions: dict[str, dict[str, Any]] | None = None,
    session: str | None = None,
    movers_by_symbol: dict[str, dict[str, Any]] | None = None,
    now_utc: datetime | None = None,
    skipped: dict[str, dict[str, Any]] | None = None,
) -> list[Guard]:
    """Auto-adopt any unmanaged open position into 24/5 lossless trailing supervision.

    Needs broker truth only: ``movers_by_symbol`` refines the trailing width when the scanner
    knows the symbol and is optional. Each adoption starts a new lifecycle (its id ends the
    adoption's client order id). The whole-share part of the quantity is supervised; a
    fractional remainder is recorded on the guard. A position first seen under water anchors
    its stop at the price at which it was first seen (``adoption_anchor_price``, written once).
    In RTH the native stop is attached here; in every other session the stop-coverage
    reconciler (``ensure_native_protective_stops``) places the resting GTC stop.

    ``skipped`` receives ``"<account>:<symbol>" -> {"reason", "qty", "side"}`` for every
    position that is not eligible (not a US equity, no whole share, asset not tradable).
    """
    now = now_utc or datetime.now(UTC)
    active_session = session or engine.classify_us_equity_session_et(
        int(now.timestamp() * 1_000_000_000)
    )
    adopted: list[Guard] = []
    movers = movers_by_symbol or {}

    for acct, lane in farm.lanes.items():
        lane.assert_integrity()
        broker = lane.trading_client
        if positions is not None and acct in positions:
            open_positions = list(positions[acct].values())
        else:
            open_positions = list(broker.get_all_positions() or ())
        lane_open_orders: list[Any] | None = None
        for pos in open_positions:
            parsed = _parse_position_for_adoption(pos)
            if parsed is None:
                continue
            sym, side, abs_qty, entry_px, curr_px = parsed
            if sym in guards:
                continue
            ineligible = _adoption_ineligible_reason(pos, broker, abs_qty)
            if ineligible is not None:
                if skipped is not None:
                    skipped[f"{acct}:{sym}"] = {
                        "reason": ineligible,
                        "qty": str(abs_qty),
                        "side": side,
                    }
                continue
            whole_qty = abs_qty.to_integral_value(rounding=ROUND_FLOOR)
            mover = movers.get(sym, {})
            raw_natr = mover.get("natr_pct")
            natr_d = Decimal("0.015")
            if raw_natr not in (None, "", 0) and not isinstance(raw_natr, bool):
                cand_natr = Decimal(str(raw_natr))
                if cand_natr.is_finite() and cand_natr > Decimal("0"):
                    natr_d = cand_natr
            raw_spread = mover.get("spread_bps")
            spread_d = Decimal("5") if active_session == "RTH" else Decimal("8")
            if raw_spread not in (None, "") and not isinstance(raw_spread, bool):
                cand_sp = Decimal(str(raw_spread))
                if cand_sp.is_finite() and cand_sp >= Decimal("0"):
                    spread_d = cand_sp
            default_arch = (
                "PREMARKET_EARLY_IGNITION"
                if active_session == "PRE_MARKET"
                else "EARLY_EP_MOMENTUM_BURST"
            )
            archetype = (
                str(mover.get("potential_category") or mover.get("archetype") or default_arch)
                .strip()
                .upper()
            )
            is_long = side == "LONG"
            better = max if is_long else min
            behind = (lambda a, b: a < b) if is_long else (lambda a, b: a > b)
            # Owner decision: a position first seen under water anchors its stop at the price
            # at which it is first seen, not at a fill price the market has already left. The
            # anchor then is the trailing reference for the life of this adoption.
            under_water = behind(curr_px, entry_px)
            anchor_px = curr_px if under_water else None
            ref_px = anchor_px if anchor_px is not None else entry_px
            trail_spec = engine.compute_non_noise_min_trail_spec(
                symbol=sym,
                entry_price=ref_px,
                side=side,
                session=active_session,
                natr_pct=natr_d,
                spread_bps=spread_d,
                archetype=archetype,
                venue_profile=lane.venue_profile,
            )
            initial_stop: Decimal = trail_spec["initial_stop_price"]
            peak_px = better(ref_px, curr_px)
            active_stop = initial_stop
            lock_tier = 0
            fast_sell_armed = False
            peak_reverse_armed = False
            sched_name, sp_map = engine.select_pooled_trailing_schedule(natr_d)
            prof = engine.get_session_protection_profile(active_session, sp_map)
            eff_be_trigger: Decimal = trail_spec["adaptive_breakeven_trigger_pct"]
            fav_gain = (peak_px - ref_px) if is_long else (ref_px - peak_px)
            fav_gain_pct = fav_gain / ref_px
            if fav_gain_pct >= eff_be_trigger:
                fast_sell_armed = True
                prog_stop, lock_tier, _ = engine.compute_progressive_trailing_stop(
                    entry_price=ref_px,
                    side=side,
                    favorable_extreme=peak_px,
                    current_stop=initial_stop,
                    initial_stop=initial_stop,
                    natr_pct=natr_d,
                    profile=prof,
                    breakeven_trigger_override=eff_be_trigger,
                )
                nn_pct: Decimal = trail_spec["non_noise_min_trail_pct"]
                nn_raw = max(
                    risk.TICK_SUB_DOLLAR,
                    peak_px * (Decimal("1") - nn_pct if is_long else Decimal("1") + nn_pct),
                )
                nn_stop = risk.quantize_rule_612(nn_raw, side=side, is_stop=True)
                cand_stop = better(prog_stop, nn_stop) if behind(nn_stop, peak_px) else prog_stop
                if behind(cand_stop, curr_px):
                    active_stop = better(initial_stop, cand_stop)
            if fav_gain_pct >= trail_spec["peak_climax_reverse_trigger_pct"]:
                peak_reverse_armed = True

            # A new lifecycle per adoption: the id ends the adoption id and so is part of every
            # stop and exit client order id of this guard. Nothing is recycled when the same
            # symbol is held again later, even at the same average price.
            lifecycle_id = uuid4().hex[:12]
            cid = f"adopted-{acct.lower()}-{sym.lower()}-{lifecycle_id}"

            # The ledger row comes first: the anchor is on record before any order is sent.
            lane.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            with open_bounded_duckdb(lane.duckdb_path) as db:
                _ensure_entry_decisions_table(db)
                db.execute(
                    "INSERT INTO paper_entry_decisions "
                    "(client_order_id,symbol,side,decision_price,arrival_price,session,origin,"
                    "annual_rate,prior_close,initial_stop,natr_pct,active_stop,exit_triggered,"
                    "adopted_external,entry_authority,entry_qty,adoption_anchor_price) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        cid,
                        sym,
                        side,
                        entry_px,
                        curr_px,
                        active_session,
                        _ADOPTED_EXTERNAL_ORIGIN,
                        Decimal("0"),
                        None,
                        initial_stop,
                        natr_d,
                        active_stop,
                        False,
                        True,
                        _OWNER_ADOPTED_AUTHORITY,
                        whole_qty,
                        anchor_px,
                    ),
                )
            if active_session != "RTH":
                stop_limit_px = engine.compute_collared_limit_price(
                    active_stop, side, trail_spec["limit_collar_pct"]
                )
                supervision = ExtendedHoursStopSupervision(
                    ts_utc=now.isoformat(),
                    account=lane.account,
                    lane_id=lane.profile["lane_id"],
                    broker_order_id=cid,
                    symbol=sym,
                    side=side,
                    qty=max(1, int(whole_qty)),
                    session=active_session,
                    protective_stop_mode="COLLARED_STOP_LIMIT_SUPERVISED",
                    ref_price=ref_px,
                    stop_price=active_stop,
                    stop_limit_price=stop_limit_px,
                    collar_bps=trail_spec["collar_bps"],
                    max_loss_pct=trail_spec["initial_max_loss_pct"],
                    status="ARMED",
                )
                lane.supervised_stops[cid] = supervision
                persist_supervised_stop_duckdb(lane.duckdb_path, supervision)
                lane.counters["extended_hours_protected"] = (
                    lane.counters.get("extended_hours_protected", 0) + 1
                )
            farm.symbol_lock.try_claim(sym, acct)
            lane.counters["adopted_external"] = lane.counters.get("adopted_external", 0) + 1
            parent_stub = SimpleNamespace(
                id=cid,
                client_order_id=cid,
                symbol=sym,
                status=SimpleNamespace(value="filled"),
                filled_qty=str(abs_qty),
                filled_avg_price=str(entry_px),
                legs=[],
                submitted_at=now,
            )
            guard = Guard(
                lane=lane,
                client_id=cid,
                symbol=sym,
                side=side,
                initial=initial_stop,
                stop=active_stop,
                natr=natr_d,
                entered_session=active_session,
                triggered=False,
                parent=parent_stub,
                child=None,
                lock_tier=lock_tier,
                trailing_schedule=sched_name,
                adopted_external=True,
                non_noise_min_trail_pct=trail_spec["non_noise_min_trail_pct"],
                execution_mechanism=str(trail_spec["execution_mechanism"]),
                peak_favorable_px=peak_px,
                fast_sell_armed=fast_sell_armed,
                peak_reverse_armed=peak_reverse_armed,
                archetype=archetype,
                spread_bps=spread_d,
                _synthetic_parent=True,
                lifecycle_id=lifecycle_id,
                adoption_anchor_price=anchor_px,
                fractional_remainder=abs_qty - whole_qty,
            )
            guards[sym] = guard
            adopted.append(guard)
            if active_session == "RTH":
                # Native stop now: an existing resting stop is tracked and topped up, a missing
                # one attached. A refusal is counted and leaves the guard tick-supervised.
                if lane_open_orders is None:
                    lane_open_orders = _read_open_orders(broker) or []
                _ensure_guard_stop_coverage(
                    lane,
                    guard,
                    position_qty=abs_qty,
                    qty_available=_available_qty(pos, long=is_long),
                    open_orders=lane_open_orders,
                    session=active_session,
                    now_utc=now,
                    fault_counter="adoption_stop_attach_faults",
                )
                if guard.child is not None:
                    parent_stub.legs = [guard.child]
    return adopted


def _merge_guard_runtime_state(guard: Guard, prev: Guard | None) -> Guard:
    """Preserve monotonic in-memory trailing/reverse state across supervisor refreshes."""
    if prev is None or prev.client_id != guard.client_id or prev.side != guard.side:
        return guard
    better = max if guard.side == "LONG" else min
    guard.stop = better(guard.stop, prev.stop)
    guard.lock_tier = max(int(guard.lock_tier), int(prev.lock_tier))
    if prev.peak_favorable_px > Decimal("0"):
        guard.peak_favorable_px = (
            better(guard.peak_favorable_px, prev.peak_favorable_px)
            if guard.peak_favorable_px > Decimal("0")
            else prev.peak_favorable_px
        )
    guard.fast_sell_armed = bool(guard.fast_sell_armed or prev.fast_sell_armed)
    guard.peak_reverse_armed = bool(guard.peak_reverse_armed or prev.peak_reverse_armed)
    guard.reverse_flatten = bool(guard.reverse_flatten or prev.reverse_flatten)
    if guard.reverse_candidate is None and prev.reverse_candidate is not None:
        guard.reverse_candidate = dict(prev.reverse_candidate)
    if guard.reentry_eligible_after_utc is None and prev.reentry_eligible_after_utc is not None:
        guard.reentry_eligible_after_utc = prev.reentry_eligible_after_utc
    guard.last_exit_check = max(guard.last_exit_check, prev.last_exit_check)
    guard.last_exit_submit_mono = max(guard.last_exit_submit_mono, prev.last_exit_submit_mono)
    guard.exit_repeg_count = max(int(guard.exit_repeg_count), int(prev.exit_repeg_count))
    if guard.last_exit_repeg_reason is None and prev.last_exit_repeg_reason is not None:
        guard.last_exit_repeg_reason = prev.last_exit_repeg_reason
    # Same lifecycle (same client id): the anchor, the ladder position and the fault state
    # survive a rebuild, so a bounded ladder cannot start over on every reconcile.
    if guard.adoption_anchor_price is None:
        guard.adoption_anchor_price = prev.adoption_anchor_price
    if guard._synthetic_parent and guard.child is None and prev.child is not None:
        # The open-order list did not show the stop this guard was tracking. Keep it: the
        # stop-coverage reconciler asks for it by id before it sends another.
        guard.child = prev.child
    guard.protection_fault_code = prev.protection_fault_code
    guard.protection_fault_count = int(prev.protection_fault_count)
    guard.exit_refusal_count = int(prev.exit_refusal_count)
    guard.next_exit_attempt_mono = prev.next_exit_attempt_mono
    guard.stop_attach_fault_count = int(prev.stop_attach_fault_count)
    guard.next_stop_attach_mono = prev.next_stop_attach_mono
    guard.last_stop_replace_mono = prev.last_stop_replace_mono
    # Data-plane and regime-flip state belongs to the lifecycle, not to one guard object.
    guard.last_trade_ts = prev.last_trade_ts
    guard.last_price_mono = prev.last_price_mono
    guard.last_price_source = prev.last_price_source
    guard.supervised_since_mono = prev.supervised_since_mono
    guard.last_heartbeat_mono = max(guard.last_heartbeat_mono, prev.last_heartbeat_mono)
    guard.regime_flip_generations = int(prev.regime_flip_generations)
    guard.regime_flip_since = prev.regime_flip_since
    guard.regime_flip_flatten = bool(prev.regime_flip_flatten)
    guard.trigger_cause = prev.trigger_cause
    return guard


def _close_flipped_lifecycle(
    lane: AlpacaPaperLane,
    cid: str,
    symbol: str,
    ledger_side: str,
    *,
    child: Any,
    open_orders: list[Any] | None,
    parent: Any = None,
) -> None:
    """The broker holds the other side of a managed symbol: end the old-side lifecycle.

    Its resting stops would now add to the position instead of closing it, so the stop the
    guard tracked and every stop this harness placed on the old exit side are cancelled (and a
    still-working entry order, so that it fills no further); the ledger row is closed. Adoption
    then builds a guard for the side the broker really holds.
    """
    old_exit_side = "sell" if ledger_side == "LONG" else "buy"
    stale_ids: dict[str, Any] = {}
    if parent is not None and _enum_text(getattr(parent, "status", "")) in _WORKING_ORDER_STATUSES:
        stale_ids[str(getattr(parent, "id", "") or "")] = parent.id
    if child is not None and _enum_text(getattr(child, "status", "")) in (
        _WORKING_ORDER_STATUSES | {"held"}
    ):
        stale_ids[str(getattr(child, "id", "") or "")] = child.id
    for order in _exit_side_orders(open_orders or (), symbol, old_exit_side):
        if _is_harness_stop(order):
            stale_ids[str(getattr(order, "id", "") or "")] = order.id
    for order_id in stale_ids.values():
        try:
            lane.broker_cancel(order_id)
        except APIError as exc:
            if exc.status_code not in (404, 422):
                raise
    with open_bounded_duckdb(lane.duckdb_path) as db:
        db.execute(
            "UPDATE paper_entry_decisions SET closed_at=current_timestamp WHERE client_order_id=?",
            [cid],
        )
    lane.counters["side_flip_rebuilds"] = lane.counters.get("side_flip_rebuilds", 0) + 1
    LOG.warning(
        "Position side of %s on %s flipped against ledger side %s; lifecycle %s closed",
        symbol,
        lane.account,
        ledger_side,
        cid,
    )


def restore_guards(
    farm: PaperFarmCoordinator,
    positions: dict[str, dict[str, Any]],
    *,
    existing_guards: dict[str, Guard] | None = None,
) -> dict[str, Guard]:
    guards: dict[str, Guard] = {}
    prev_map = existing_guards or {}
    for acct, lane in farm.lanes.items():
        with open_bounded_duckdb(lane.duckdb_path) as db:
            if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                continue
            _ensure_entry_decisions_table(db)  # a ledger older than the tier columns gains them
            rows = db.execute(
                "SELECT client_order_id,symbol,side,initial_stop,"
                "coalesce(active_stop,initial_stop),"
                "natr_pct,session,exit_triggered,origin,adoption_anchor_price "
                "FROM paper_entry_decisions WHERE closed_at IS NULL"
            ).fetchall()
        lane_open_orders: list[Any] | None = None
        for cid, sym, side, initial, stop, natr, session, triggered, origin, anchor in rows:
            is_synthetic_adopted = origin == _ADOPTED_EXTERNAL_ORIGIN or str(cid).startswith(
                "adopted-"
            )
            if is_synthetic_adopted:
                pos = positions.get(acct, {}).get(sym)
                if pos is None:
                    with open_bounded_duckdb(lane.duckdb_path) as db:
                        db.execute(
                            "UPDATE paper_entry_decisions SET closed_at=current_timestamp "
                            "WHERE client_order_id=?",
                            [cid],
                        )
                    continue
                parsed = _parse_position_for_adoption(pos)
                if parsed is None:
                    continue
                _, pos_side, abs_qty, entry_px, curr_px = parsed
                if lane_open_orders is None:
                    lane_open_orders = _read_open_orders(lane.trading_client) or []
                if pos_side != side:
                    prev_guard = prev_map.get(sym)
                    _close_flipped_lifecycle(
                        lane,
                        cid,
                        sym,
                        side,
                        child=prev_guard.child if prev_guard is not None else None,
                        open_orders=lane_open_orders,
                    )
                    continue
                exit_side_str = "sell" if side == "LONG" else "buy"
                child = _find_open_broker_stop(
                    lane.trading_client, sym, exit_side_str, open_orders=lane_open_orders
                )
                anchor_px = (
                    Decimal(str(anchor))
                    if anchor is not None and Decimal(str(anchor)) > Decimal("0")
                    else None
                )
                ref_px = anchor_px if anchor_px is not None else entry_px
                parent_stub = SimpleNamespace(
                    id=cid,
                    client_order_id=cid,
                    symbol=sym,
                    status=SimpleNamespace(value="filled"),
                    filled_qty=str(abs_qty),
                    filled_avg_price=str(entry_px),
                    legs=[child] if child is not None else [],
                    submitted_at=datetime.now(UTC),
                )
                peak_px = max(ref_px, curr_px) if side == "LONG" else min(ref_px, curr_px)
                whole_qty = abs_qty.to_integral_value(rounding=ROUND_FLOOR)
                guard = Guard(
                    lane,
                    cid,
                    sym,
                    side,
                    initial,
                    stop,
                    natr,
                    session,
                    triggered,
                    parent_stub,
                    child=child,
                    adopted_external=True,
                    peak_favorable_px=peak_px,
                    _synthetic_parent=True,
                    # The adoption id ends in its lifecycle id; the anchor is the one written
                    # when the position was first seen, so the stop stays where it was.
                    lifecycle_id=str(cid).rsplit("-", 1)[-1],
                    adoption_anchor_price=anchor_px,
                    fractional_remainder=abs_qty - whole_qty,
                )
                if sym in guards:
                    raise PermissionError("Overlapping managed paper orders")
                guards[sym] = _merge_guard_runtime_state(guard, prev_map.get(sym))
                continue
            try:
                parent = lane.trading_client.get_order_by_client_id(cid)
            except APIError as exc:
                if exc.status_code != 404:
                    raise
                # An ambiguous submission reserves its symbol in the durable entry journal.
                continue
            is_tv_adopted = origin == "TV_HALF_AUTO_CONFIRMED" or str(cid).startswith("noesis-tv-")
            guard = Guard(
                lane,
                cid,
                sym,
                side,
                initial,
                stop,
                natr,
                session,
                triggered,
                parent,
                adopted_external=is_tv_adopted,
            )
            guard.fetch()
            held = positions.get(acct, {}).get(sym)
            held_parsed = _parse_position_for_adoption(held) if held is not None else None
            if (
                held_parsed is not None
                and held_parsed[1] != side
                and Decimal(str(getattr(guard.parent, "filled_qty", None) or "0")) > Decimal("0")
            ):
                # This entry filled, yet the broker holds the other side (an unfilled entry
                # proves nothing and is left alone).
                if lane_open_orders is None:
                    lane_open_orders = _read_open_orders(lane.trading_client) or []
                _close_flipped_lifecycle(
                    lane,
                    cid,
                    sym,
                    side,
                    child=guard.child,
                    open_orders=lane_open_orders,
                    parent=guard.parent,
                )
                continue
            filled_at = getattr(guard.parent, "filled_at", None)
            freshly_filled = False
            if (
                guard.parent.status.value == "filled"
                and Decimal(str(getattr(guard.parent, "filled_qty", "0") or "0")) > Decimal("0")
                and isinstance(filled_at, datetime)
            ):
                filled_utc = (
                    filled_at if filled_at.tzinfo is not None else filled_at.replace(tzinfo=UTC)
                )
                freshly_filled = (
                    0
                    <= (datetime.now(UTC) - filled_utc).total_seconds()
                    < (_FRESH_FILL_SETTLE_SECS)
                )
            if (
                guard.parent.status.value in TERMINAL
                and sym not in positions[acct]
                and not freshly_filled
                and (not guard.child or guard.child.status.value in TERMINAL)
            ):
                with open_bounded_duckdb(lane.duckdb_path) as db:
                    db.execute(
                        "UPDATE paper_entry_decisions SET closed_at=current_timestamp "
                        "WHERE client_order_id=?",
                        [cid],
                    )
                continue
            if (
                guard.parent.status.value not in TERMINAL | {"pending_cancel"}
                and guard.parent.submitted_at
                and (datetime.now(UTC) - guard.parent.submitted_at).total_seconds() >= 60
            ):
                lane.broker_cancel(guard.parent.id)
            if sym in guards:
                raise PermissionError("Overlapping managed paper orders")
            guards[sym] = _merge_guard_runtime_state(guard, prev_map.get(sym))
    return guards


def _supervises_live_venue(venue_profile: str | None) -> bool:
    """``None`` supervises the paper farm, the tiered live venue the live lane; else refuse."""
    if venue_profile is None:
        return False
    requested = venue_profile.strip().upper() if isinstance(venue_profile, str) else ""
    if requested == engine.LIVE_TIERED_VENUE_ID:
        return True
    if requested:
        # Any other live venue keeps the engine's own refusal text.
        engine.validate_venue_order_capability(requested)
    raise ValueError(
        "run_execution supervises the paper farm (venue_profile=None) or "
        f"{engine.LIVE_TIERED_VENUE_ID}, got {venue_profile!r}"
    )


def _verified_live_tier3_opt_in() -> bool:
    """Run the runner's live-environment check; its result is the owner's tier-3 opt-in.

    Resolved at call time, so a tree whose runner has no such check cannot supervise live.
    """
    verify = getattr(runner, "verify_live_environment", None)
    if not callable(verify):
        raise PermissionError(
            "Refused live supervision: runner.verify_live_environment is unavailable"
        )
    opt_in = verify()
    if not isinstance(opt_in, bool):
        raise PermissionError(
            "Refused live supervision: the live environment check returned no tier-3 opt-in"
        )
    return opt_in


def _live_evidence_dir(manifest_dir: Path, shared_dir: Path | None) -> Path:
    """The shared (paper) directory the live process reads from and never writes to."""
    if shared_dir is None:
        raise ValueError("Live supervision requires shared_dir, the manifest directory it reads")
    own, shared = manifest_dir.resolve(), shared_dir.resolve()
    if own == shared or own.is_relative_to(shared):
        raise PermissionError(
            "Refused live supervision inside the shared manifest directory; "
            "live manifests are kept separate."
        )
    return shared_dir


def run_execution(
    manifest_dir: Path,
    *,
    stop: threading.Event | None = None,
    venue_profile: str | None = None,
    shared_dir: Path | None = None,
) -> None:
    """One exclusive execution process. Each entry requires a fresh trade on its routed feed.

    By default it supervises the paper farm, with every file in ``manifest_dir``. With
    ``venue_profile`` set to the tiered live venue the same supervisor runs the one live lane:
    its status, ledger, log and locks live under ``manifest_dir``, while the scanner manifest and
    the TCA evidence are read from ``shared_dir`` (the paper farm's directory), never written.
    """
    farm: PaperFarmCoordinator
    streams: dict[DataFeed, Any]
    is_live = _supervises_live_venue(venue_profile)
    if is_live:
        autonomous_opt_in = _verified_live_tier3_opt_in()
        evidence_dir = _live_evidence_dir(manifest_dir, shared_dir)
        # Built before anything is written: it refuses a directory that holds a paper ledger.
        live_farm = build_live_venue_coordinator(manifest_dir, autonomous_opt_in=autonomous_opt_in)
        live_farm.assert_single_environment()
        if live_farm.environment != "LIVE" or live_farm.autonomous_opt_in is not autonomous_opt_in:
            raise PermissionError("Refused live supervision: coordinator is not the verified one")
        # Market data comes from the live credential's own connections, never the paper farm's.
        streams = {
            feed: live_farm.market_streams[feed]
            for feed in (DataFeed.SIP, DataFeed.BOATS)
            if feed in live_farm.market_streams
        }
        if len(streams) != 2:
            raise RuntimeError("Missing live market data stream")
        data = live_farm.data_client
        if data is None:
            raise RuntimeError("Missing market data client")
        manifest_dir.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            filename=manifest_dir / "paper_farm_execution.log",
            level=logging.INFO,
            format="%(asctime)s %(name)s %(levelname)s %(message)s",
        )
        farm = live_farm
    else:
        if shared_dir is not None:
            raise ValueError("shared_dir applies to the live venue only")
        verify_paper_environment()
        manifest_dir.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            filename=manifest_dir / "paper_farm_execution.log",
            level=logging.INFO,
            format="%(asctime)s %(name)s %(levelname)s %(message)s",
        )
        key, secret = resolve_alpaca_env_creds()
        streams = {
            feed: StockDataStream(key, secret, feed=feed) for feed in (DataFeed.SIP, DataFeed.BOATS)
        }
        data = ExtremeMoverScanner.from_env().data_client
        if data is None:
            raise RuntimeError("Missing market data client")
        farm = PaperFarmCoordinator(manifest_dir)
        if farm.environment != "PAPER":
            raise PermissionError("Refused non-paper coordinator on the paper supervision path")
        evidence_dir = manifest_dir
    scan_path = evidence_dir / "live_whole_market_movers_manifest.json"
    tca_path = evidence_dir / "tca_calibration_manifest.json"
    state_path = manifest_dir / "paper_farm_execution_status.json"
    lock = threading.Lock()
    watching = {"SPY", "QQQ"}
    guards: dict[str, Guard] = {}
    candidates: dict[str, tuple[AlpacaPaperLane, dict[str, Any], risk.Side, str]] = {}
    attempts: set[str] = set()
    last_feed_trade: dict[DataFeed, datetime] = {}
    previous_status = ""
    last_report = 0.0
    scan_generation = ""
    cached_scan_sig: tuple[int, int, int] | None = None
    cached_scan_doc: dict[str, Any] | None = None
    cached_scan_ts: datetime | None = None
    cached_tca_sig: tuple[object, ...] | None = None
    cached_tca_ok: bool | None = None
    cached_ledger_sig: tuple[object, ...] | None = None
    # Live only: symbol -> why its scanner candidate is held (a quiet HOLD, never a fault).
    entry_holds: dict[str, str] = {}
    # Stop coverage and adoption run from broker truth on their own timer (R1, R5).
    adoption_skips: dict[str, dict[str, Any]] = {}
    rebuild_hold_until: dict[str, float] = {}
    stop_coverage_actions: list[str] = []
    stop_coverage_faults = 0
    last_stop_coverage = 0.0
    # Entry hold (R13). Entries are held only while a broker position fails the protection
    # check, and the hold clears itself on the first check that every position passes. It is
    # process state: nothing is read back from the status file, so an ``entry_fault: true``
    # left there by an earlier process is overwritten by this one's first report.
    entry_hold: dict[str, Any] = {
        "held": False,
        "cause": None,
        "since_utc": None,
        "positions": [],
        "holds_total": 0,
        "last_cleared_utc": None,
    }
    entry_rejections: dict[str, int] = {}
    supervision_faults = {"process": 0, "reconcile": 0, "coverage_audit": 0, "rest_poll": 0}
    last_fault_log = 0.0
    # Data plane (R2).
    clock_state: dict[str, Any] = {
        "status": "UNVERIFIED",
        "skew_secs": None,
        "max_skew_secs": str(CLOCK_SKEW_MAX_SECS),
        "checked_at_utc": None,
        "entries_blocked": False,
        "unreadable_checks": 0,
    }
    clock_offset_ns = 0
    last_clock_check = 0.0
    last_rest_poll = 0.0
    rest_polls = 0
    data_stale_symbols: list[str] = []
    base_status = "AWAITING_MARKET_DATA"
    stream_faults: dict[str, int] = {}
    streams_ended: list[str] = []
    trade_update_fills = 0
    # Set by a trade-update fill event; the loop below consumes it and reconciles at once.
    reconcile_requested = threading.Event()
    halt = threading.Event()
    update_streams: dict[str, Any] = {}
    # Coverage auditor (R12).
    coverage_path = manifest_dir / LOSSLESS_COVERAGE_MANIFEST
    coverage_summary: dict[str, Any] = {}
    last_coverage_write = 0.0
    last_coverage_hash = ""

    def current_session() -> str:
        """The session at the supervisor's clock: host time, or broker time when skewed."""
        return engine.classify_us_equity_session_et(time.time_ns() + clock_offset_ns)

    def check_clock() -> None:
        """Compare the host clock with the broker clock (R2): at start and every 30 s."""
        nonlocal clock_offset_ns, last_clock_check
        last_clock_check = time.monotonic()
        lane = next(iter(farm.lanes.values()), None)
        if lane is None or not _consume_read_slot(lane, time.time()):
            return
        skew = _measure_broker_clock_skew(lane.trading_client)
        if skew is None:
            # Unreadable: the last measurement stands. With none at all the entry path keeps
            # its own wall-clock gates on the print, the bar and the quote.
            clock_state["unreadable_checks"] += 1
            return
        skewed = abs(skew) > CLOCK_SKEW_MAX_SECS
        if skewed and not clock_state["entries_blocked"]:
            LOG.error(
                "Host clock differs from the broker clock by %s s (limit %s s): entries are "
                "blocked and sessions follow broker time",
                skew,
                CLOCK_SKEW_MAX_SECS,
            )
        clock_state.update(
            status="SKEWED" if skewed else "OK",
            skew_secs=str(skew),
            checked_at_utc=datetime.now(UTC).isoformat(),
            entries_blocked=skewed,
        )
        clock_offset_ns = int(skew * 1_000_000_000) if skewed else 0

    def _file_stat_sig(path: Path) -> tuple[int, int, int] | None:
        try:
            st = path.stat()
        except FileNotFoundError:
            return None
        return (st.st_mtime_ns, st.st_size, st.st_ino)

    def _ledger_sig() -> tuple[object, ...]:
        return tuple(
            (_file_stat_sig(ledger), _file_stat_sig(Path(f"{ledger}.wal")))
            for ledger in (manifest_dir / _duckdb_filename(acct) for acct in farm.required_accounts)
        )

    def _calibration_allows_entry() -> bool:
        if is_live:
            return cost_calibration_allows_entry(evidence_dir, environment="LIVE")
        return cost_calibration_allows_entry(manifest_dir)

    def _live_entry_hold(
        lane: AlpacaPaperLane, sym: str, intent: engine.VenueOrderIntent | None
    ) -> bool:
        """True when a live lane must hold this entry; quiet, never raises, never a fault.

        ``intent`` None is a reverse / re-entry candidate: on live that is owner lineage, held
        until the lineage resolver decides it. A paper lane is never held here.
        """
        if lane.environment != "LIVE":
            return False
        if intent is None:
            reason: str | None = "HOLD_LIVE_LINEAGE_UNRESOLVED"
        else:
            try:
                # Scanner entries carry no leg merit yet: tier 3 stays a HOLD until it is wired.
                reason = lane.entry_hold_reason(intent, leg_merit=False)
            except Exception:
                reason = "HOLD_ENTRY_CHECK_FAILED"
        if reason is None:
            entry_holds.pop(sym, None)
            return False
        entry_holds[sym] = reason
        return True

    def _reconcile_live_guards(
        now_dt: datetime, sess: str, rows_map: dict[str, dict[str, Any]] | None = None
    ) -> None:
        nonlocal cached_ledger_sig, guards, stop_coverage_actions
        with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
            positions = refresh_accounts(farm, now_dt, sess, publish=False)
            guards = restore_guards(farm, positions, existing_guards=guards)
            adoption_skips.clear()
            adopt_unmanaged_broker_positions(
                farm,
                guards,
                positions=positions,
                session=sess,
                movers_by_symbol=rows_map,
                now_utc=now_dt,
                skipped=adoption_skips,
            )
            # Every reconcile ends with the stop-coverage invariant (R1).
            stop_coverage_actions = ensure_native_protective_stops(
                farm, guards, session=sess, now_utc=now_dt
            )
            if rows_map:
                for g_sym, guard in guards.items():
                    row_meta = rows_map.get(g_sym)
                    if not isinstance(row_meta, dict):
                        continue
                    raw_sp = row_meta.get("spread_bps")
                    if raw_sp not in (None, "") and not isinstance(raw_sp, bool):
                        cand_sp = Decimal(str(raw_sp))
                        if cand_sp.is_finite() and cand_sp >= Decimal("0"):
                            guard.spread_bps = cand_sp
        cached_ledger_sig = _ledger_sig()

    def audit_coverage(
        snapshots: Mapping[str, tuple[list[Any], list[Any]]], now_dt: datetime, sess: str
    ) -> None:
        """Coverage audit of one read cycle: the entry hold (R13) and the manifest (R12).

        The snapshot was read before this cycle's reconcile acts, so it shows what the last
        reconcile left behind plus whatever happened since. Entries are held while a position
        in it has no guard, or neither full native cover nor an exit in flight; the hold
        clears on the first complete audit that finds none. An audit that could not read every
        lane decides nothing. It never raises into the reconcile.
        """
        nonlocal coverage_summary, last_coverage_hash, last_coverage_write
        try:
            positions = {
                acct: {str(getattr(pos, "symbol", "")): pos for pos in lane_positions}
                for acct, (lane_positions, _orders) in snapshots.items()
            }
            orders: dict[str, list[Any] | None] = {
                acct: lane_orders for acct, (_positions, lane_orders) in snapshots.items()
            }
            complete = set(snapshots) == set(farm.lanes)
            rows, gaps = _coverage_rows(
                farm, guards, positions, orders, session=sess, ineligible=adoption_skips
            )
            if complete:
                if gaps:
                    newly_held = not entry_hold["held"]
                    if newly_held:
                        entry_hold["holds_total"] += 1
                        entry_hold["since_utc"] = now_dt.isoformat()
                        entry_hold["status_before"] = base_status
                        LOG.warning("Entries held: no working protective order for %s", gaps)
                    entry_hold.update(held=True, cause="PROTECTION_GAP", positions=gaps)
                    if newly_held:
                        report("ENTRY_HELD", force=True)
                elif entry_hold["held"]:
                    LOG.warning("Entry hold cleared: every broker position is protected")
                    entry_hold.update(
                        held=False,
                        cause=None,
                        since_utc=None,
                        positions=[],
                        last_cleared_utc=now_dt.isoformat(),
                    )
                    resumed = entry_hold.pop("status_before", None)
                    report(
                        resumed if base_status == "ENTRY_HELD" and resumed else base_status,
                        force=True,
                    )
            unprotected = sum(row["unprotected_shares_count"] for row in rows)
            # What the record states, without the ages that move on every cycle.
            stated = [
                [row[key] for key in sorted(row) if not key.endswith("_age_secs")] for row in rows
            ]
            digest = hashlib.sha256(
                json.dumps([stated, gaps, complete], default=str).encode()
            ).hexdigest()
            due = time.monotonic() - last_coverage_write >= LOSSLESS_COVERAGE_REPORT_SECS
            if digest == last_coverage_hash and not due:
                return
            record = build_lossless_coverage_record(
                farm,
                guards,
                positions,
                session=sess,
                now_utc=now_dt,
                last_feed_trade=last_feed_trade,
                open_orders=orders,
                ineligible=adoption_skips,
            )
            record["complete"] = complete
            record["accounts_not_read"] = sorted(set(farm.lanes) - set(snapshots))
            if not complete:
                # A lane that was not read attests nothing about its shares.
                record["all_shares_protected"] = False
            record["entry_hold"] = dict(entry_hold)
            _atomic_write_json(coverage_path, record)
            last_coverage_write, last_coverage_hash = time.monotonic(), digest
            coverage_summary = {
                "manifest": coverage_path.name,
                "generated_at_utc": record["generated_at_utc"],
                "complete": complete,
                "all_shares_protected": record["all_shares_protected"],
                "position_shares_total": record["position_shares_total"],
                "natively_protected_shares": record["natively_protected_shares"],
                "synthetically_supervised_shares": record["synthetically_supervised_shares"],
                "unprotected_shares_count": unprotected,
                "inputs_hash": record["inputs_hash"],
            }
        except Exception:
            supervision_faults["coverage_audit"] += 1
            LOG.exception("Coverage audit failed")

    def _reconcile_stop_coverage(now_dt: datetime, sess: str, *, forced: bool = False) -> None:
        """Timer path: stop coverage and adoption from broker truth alone (R1, R5).

        It needs neither a trade print nor the scanner file, and it reads positions itself, so
        it also runs when the account refresh is refused. A position without a guard, a side
        flip or a finished lifecycle rebuilds the guards (restore + adoption) at once. The one
        pair of reads per lane is budgeted outside the exit reserve (R14) and also feeds the
        coverage audit. ``forced`` (a fill event on the trade-update stream) rebuilds without
        waiting out the pause after an unresolved rebuild.
        """
        nonlocal cached_ledger_sig, guards, stop_coverage_actions
        snapshots, read_tags = _read_broker_snapshots(farm, now_dt.timestamp())
        audit_coverage(snapshots, now_dt, sess)
        actions = read_tags + ensure_native_protective_stops(
            farm, guards, session=sess, now_utc=now_dt, snapshots=snapshots
        )

        def rebuild_keys(tags: list[str]) -> set[str]:
            return {
                tag.rsplit(":", 1)[0] for tag in tags if tag.rsplit(":", 1)[-1] in _REBUILD_ACTIONS
            }

        now_mono = time.monotonic()
        due = {
            key
            for key in rebuild_keys(actions)
            if forced or now_mono >= rebuild_hold_until.get(key, 0.0)
        }
        if due:
            with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
                positions = {
                    acct: {
                        str(pos.symbol): pos
                        for pos in lane.trading_client.get_all_positions() or ()
                    }
                    for acct, lane in farm.lanes.items()
                }
                guards = restore_guards(farm, positions, existing_guards=guards)
                adoption_skips.clear()
                adopt_unmanaged_broker_positions(
                    farm,
                    guards,
                    positions=positions,
                    session=sess,
                    movers_by_symbol=(
                        {row["symbol"]: row for row in cached_scan_doc["entries"]}
                        if cached_scan_doc is not None
                        else None
                    ),
                    now_utc=now_dt,
                    skipped=adoption_skips,
                )
                actions = ensure_native_protective_stops(farm, guards, session=sess, now_utc=now_dt)
            cached_ledger_sig = _ledger_sig()
            # What a rebuild could not resolve (an ineligible position, a fill still settling)
            # is looked at again after a pause, not on every tick.
            for key in rebuild_keys(actions) & due:
                rebuild_hold_until[key] = time.monotonic() + REBUILD_RETRY_HOLD_SECS
        stop_coverage_actions = actions

    def report(status: str, *, force: bool = False) -> None:
        nonlocal base_status, previous_status, last_report
        if status != "STOPPED":
            base_status = status
            if data_stale_symbols and status != "CLOSED":
                # A guard has had no confirmed price for GUARD_DATA_STALE_SECS (R2).
                status = "DATA_STALE"
        if not force and status == previous_status and time.monotonic() - last_report < 30:
            return
        previous_status, last_report = status, time.monotonic()
        curr_sess = current_session()
        active_positions = [guards[s].to_position_snapshot(curr_sess) for s in sorted(guards)]
        adopted_external_n = sum(1 for p in active_positions if p.get("adopted_external") is True)
        synthetic_offhours_n = sum(
            1
            for p in active_positions
            if str(p.get("execution_mechanism") or "").startswith("SYNTHETIC_")
        )
        peak_reverse_n = sum(1 for p in active_positions if p.get("peak_reverse_armed") is True)
        rapid_reentry_n = sum(1 for p in active_positions if p.get("rapid_reentry_ready") is True)
        reverse_candidates = [
            p["reverse_candidate"]
            for p in active_positions
            if isinstance(p.get("reverse_candidate"), dict)
        ]
        guard_sides = {s.strip().upper(): (g.side, g.lane.account) for s, g in guards.items()}
        seen_conflict_pairs: set[tuple[str, str]] = set()
        opposing_pair_conflicts: list[dict[str, str]] = []
        for s_key in sorted(guard_sides):
            g_side, _ = guard_sides[s_key]
            conflict = find_opposing_inverse_pair_conflict(s_key, g_side, guard_sides)
            if conflict is not None:
                pair_key = (
                    min(s_key, conflict["opposing_symbol"]),
                    max(s_key, conflict["opposing_symbol"]),
                )
                if pair_key not in seen_conflict_pairs:
                    seen_conflict_pairs.add(pair_key)
                    opposing_pair_conflicts.append(conflict)
        now_epoch = time.time()
        cutoff_60s = now_epoch - 60.0
        live_rpm_per_account: dict[str, dict[str, Any]] = {}
        total_used_rpm_60s = 0
        total_alpaca_max_rpm = 0
        total_alpaca_exit_reserve_rpm = 0
        for acct_name in sorted(farm.lanes):
            acct_lane = farm.lanes[acct_name]
            rb = acct_lane.rate_budget
            raw_ts = getattr(rb, "_timestamps", ())
            used_60s = sum(1 for ts in raw_ts if ts > cutoff_60s)
            max_rpm_i = int(rb.max_rpm)
            exit_res_i = int(rb.exit_reserve_rpm)
            entry_cap_i = max(0, max_rpm_i - exit_res_i)
            total_used_rpm_60s += used_60s
            total_alpaca_max_rpm += max_rpm_i
            total_alpaca_exit_reserve_rpm += exit_res_i
            live_rpm_per_account[acct_name] = {
                "lane_id": acct_lane.profile["lane_id"],
                "venue_profile": acct_lane.venue_profile,
                "used_rpm_60s": used_60s,
                "max_rpm": max_rpm_i,
                "entry_modify_budget_rpm": entry_cap_i,
                "exit_reserve_rpm": exit_res_i,
                "remaining_total_rpm": max(0, max_rpm_i - used_60s),
                "remaining_entry_rpm": max(0, entry_cap_i - used_60s),
                "utilization_pct": round((used_60s / max_rpm_i) * 100.0, 2) if max_rpm_i else 0.0,
                "counters": {k: int(v) for k, v in acct_lane.counters.items()},
            }
        _atomic_write_json(
            state_path,
            {
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "status": status,
                "active_session": curr_sess,
                # From the coordinator's environment and tier-3 opt-in, never constants.
                "paper_only": farm.environment == "PAPER",
                "live_execution_enabled": farm.autonomous_opt_in is True,
                "managed_symbols": sorted(guards),
                "active_positions": active_positions,
                "active_positions_count": len(active_positions),
                "adopted_external_positions_count": adopted_external_n,
                "synthetic_offhours_trailed_count": synthetic_offhours_n,
                "peak_reverse_armed_count": peak_reverse_n,
                "rapid_reentry_ready_count": rapid_reentry_n,
                "reverse_candidates": reverse_candidates,
                "opposing_pair_conflicts": opposing_pair_conflicts,
                "rate_budget_live_rpm": {
                    "window_secs": 60,
                    "alpaca_used_rpm_60s": total_used_rpm_60s,
                    "alpaca_max_rpm": total_alpaca_max_rpm,
                    "alpaca_exit_reserve_rpm": total_alpaca_exit_reserve_rpm,
                    "alpaca_utilization_pct": round(
                        (total_used_rpm_60s / total_alpaca_max_rpm) * 100.0, 2
                    )
                    if total_alpaca_max_rpm
                    else 0.0,
                    "per_account": live_rpm_per_account,
                },
                "venue_order_handles": farm.venue_order_handles(session=curr_sess),
                "streams": ["sip", "boats"],
                "last_trade_utc": {
                    feed.value: ts.isoformat() for feed, ts in last_feed_trade.items()
                },
                # True only while the protection check holds entries; it clears itself (R13).
                "entry_fault": entry_hold["held"] is True,
                "entry_hold": dict(entry_hold),
                "entry_rejected_total": dict(sorted(entry_rejections.items())),
                "supervision_faults": dict(supervision_faults),
                "protection_faults": {
                    sym: {
                        "code": guard.protection_fault_code,
                        "consecutive": int(guard.protection_fault_count),
                        "alert": protection_fault_alerts(
                            guard.protection_fault_code, guard.protection_fault_count
                        ),
                    }
                    for sym, guard in sorted(guards.items())
                    if guard.protection_fault_code
                },
                "clock_skew": dict(clock_state),
                "data_plane": {
                    "rest_fallback_after_secs": GUARD_PRICE_REST_FALLBACK_SECS,
                    "data_stale_after_secs": GUARD_DATA_STALE_SECS,
                    "rest_polls": rest_polls,
                    "data_stale_symbols": list(data_stale_symbols),
                    "stream_faults": dict(sorted(stream_faults.items())),
                    "streams_ended": sorted(streams_ended),
                    "trade_update_streams": sorted(update_streams),
                    "trade_update_fill_events": trade_update_fills,
                },
                "lossless_coverage": dict(coverage_summary),
                "stop_coverage": {
                    "reconcile_secs": STOP_COVERAGE_RECONCILE_SECS,
                    "last_actions": list(stop_coverage_actions),
                    "reconcile_faults": stop_coverage_faults,
                    "adoption_skipped": dict(sorted(adoption_skips.items())),
                },
            }
            | ({"entry_holds": dict(sorted(entry_holds.items()))} if is_live else {}),
        )

    def refresh() -> None:
        nonlocal cached_scan_doc, cached_scan_sig, cached_scan_ts
        nonlocal cached_tca_ok, cached_tca_sig, guards, scan_generation
        try:
            scan_st = scan_path.stat()
        except FileNotFoundError:
            cached_scan_sig = None
            cached_scan_doc = None
            cached_scan_ts = None
            raise
        scan_sig = (scan_st.st_mtime_ns, scan_st.st_size, scan_st.st_ino)
        if scan_sig != cached_scan_sig or cached_scan_doc is None or cached_scan_ts is None:
            doc = json.loads(scan_path.read_text("utf-8"))
            scan_ts = datetime.fromisoformat(doc["generated_at_utc"])
            cached_scan_sig = scan_sig
            cached_scan_doc = doc
            cached_scan_ts = scan_ts
        else:
            doc = cached_scan_doc
            scan_ts = cached_scan_ts
        now = datetime.now(UTC)
        cur_ledger_sig = _ledger_sig()
        ledger_changed = cached_ledger_sig is not None and cur_ledger_sig != cached_ledger_sig
        if not 0 <= (now - scan_ts).total_seconds() < 90:
            if ledger_changed:
                cur_sess = current_session()
                _reconcile_live_guards(now, cur_sess)
            candidates.clear()
            entry_holds.clear()
            report("STALE_SCAN")
            return
        tca_file_sig = _file_stat_sig(tca_path)
        if tca_file_sig is not None:
            tca_sig: tuple[object, ...] = ("file", tca_file_sig)
        else:
            tca_sig = ("ledgers", *cur_ledger_sig)
        if tca_sig != cached_tca_sig or cached_tca_ok is None:
            cached_tca_ok = _calibration_allows_entry()
            cached_tca_sig = tca_sig
        calibration_ok = cached_tca_ok
        if doc["generated_at_utc"] == scan_generation:
            if ledger_changed:
                cur_sess = current_session()
                rows_cached = {r["symbol"]: r for r in doc["entries"]}
                _reconcile_live_guards(now, cur_sess, rows_cached)
                report("ENTRY_HELD" if entry_hold["held"] else "SUPERVISING")
            if not calibration_ok:
                candidates.clear()
                report("TCA_RECALIBRATION_REQUIRED")
            return
        session = current_session()
        candidates.clear()
        entry_holds.clear()
        # Serialize ledger reconciliation with the scheduled observation.
        rows = {r["symbol"]: r for r in doc["entries"]}
        _reconcile_live_guards(now, session, rows)
        scan_generation = doc["generated_at_utc"]
        attempts.clear()
        macro_regime = doc["macro_regime"]["regime"]
        # R7: a flip is reversible until the exit is sent, and on live it must persist.
        apply_regime_flip_policy(
            guards,
            market_regime=macro_regime,
            rows=rows,
            session=session,
            generation_utc=scan_ts,
        )
        if not calibration_ok:
            report("TCA_RECALIBRATION_REQUIRED")
            return
        open_sides_refresh = _collect_open_symbol_sides(farm, guards=guards)
        for row in doc["entries"]:
            if not row.get("extreme_mover_eligible") or row["session"] != session:
                continue
            route = select_short_or_inverse_fallback(
                row["symbol"],
                market_regime=doc["macro_regime"]["regime"],
                borrow_meta=row,
                session=session,
            )
            if route:
                sym, side, origin = route
                if find_opposing_inverse_pair_conflict(sym, side, open_sides_refresh) is not None:
                    continue
                target = rows.get(sym)
                if (
                    target is not None
                    and target.get("natr_pct")
                    and (
                        sym != row["symbol"]
                        or (side == "LONG") == (row["side"] == "BUY")
                        or row.get("peak_confirmed") is True
                    )
                ):
                    lane = select_situation_lane(
                        farm.lanes,
                        origin=origin,
                        natr_pct=target.get("natr_pct"),
                        rvol_normalized=_row_rvol(target),
                    )
                    # A live scanner candidate is an autonomous (tier-3) entry: held quietly
                    # here, before it is ever subscribed, unless the lane's tier grants it.
                    if lane is not None and not _live_entry_hold(lane, sym, "AUTONOMOUS_ENTRY"):
                        candidates.setdefault(sym, (lane, target, side, origin))
        active_feed = DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP
        last = last_feed_trade.get(active_feed)
        report(
            "CLOSED"
            if session == "CLOSED"
            else "ENTRY_HELD"
            if entry_hold["held"]
            else "SUPERVISING"
            if last and (now - last).total_seconds() < 60
            else "AWAITING_MARKET_DATA"
        )

    def handle(trade: Any, feed: DataFeed) -> None:
        session = current_session()
        if session == "CLOSED" or (session == "OVERNIGHT") != (feed == DataFeed.BOATS):
            return
        sym = trade.symbol
        if trade.price <= 0:
            return
        if sym in guards:
            # R2: a guard acts on any print that is not older than the last one it saw. No
            # wall clock decides here, so a skewed host clock cannot stop a protective exit.
            if drive_guard_price(
                guards[sym], Decimal(str(trade.price)), session, trade_ts=trade.timestamp
            ):
                stamp = _as_utc(trade.timestamp)
                if stamp is not None:
                    last_feed_trade[feed] = max(stamp, last_feed_trade.get(feed, stamp))
            return
        # Entries only: the wall-clock freshness gate stays.
        if (
            not 0
            <= (datetime.now(UTC) - trade.timestamp).total_seconds()
            <= ENTRY_PRINT_MAX_AGE_SECS
        ):
            return
        last_feed_trade[feed] = max(trade.timestamp, last_feed_trade.get(feed, trade.timestamp))
        if (
            entry_hold["held"]
            or clock_state["entries_blocked"]
            or sym not in candidates
            or sym in attempts
        ):
            return
        lane, row, side, origin = candidates[sym]
        now = datetime.now(UTC)
        if (
            not row.get("bar_close_utc")
            or not 0 <= (now - datetime.fromisoformat(row["bar_close_utc"])).total_seconds() < 60
        ):
            return
        if session != row["session"] or not 0 <= (now - trade.timestamp).total_seconds() <= 5:
            return
        if sym in farm.symbol_lock.snapshot():
            return
        if (
            find_opposing_inverse_pair_conflict(
                sym, side, _collect_open_symbol_sides(farm, guards=guards)
            )
            is not None
        ):
            return
        if not lane.breaker.allow_entry():
            fallback = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=row.get("natr_pct"),
                rvol_normalized=_row_rvol(row),
            )
            if fallback is None:
                return
            lane = fallback
        attempts.add(sym)
        for other in farm.lanes.values():
            if other.status_path.exists():
                hb = json.loads(other.status_path.read_text("utf-8")).get("heartbeat_utc")
                if hb and (now - datetime.fromisoformat(hb)).total_seconds() < 120:
                    return  # The existing external farm has execution ownership.
        # Recheck all three account inventories immediately before admission.
        positions = refresh_accounts(farm, now, session, publish=False) or {}
        if sym in farm.symbol_lock.snapshot():
            return
        if (
            find_opposing_inverse_pair_conflict(
                sym,
                side,
                _collect_open_symbol_sides(farm, positions=positions, guards=guards),
            )
            is not None
        ):
            LOG.warning(
                "OpposingInversePairGuard blocked %s %s entry while opposing inverse leg is open",
                sym,
                side,
            )
            return
        if not lane.breaker.allow_entry():
            fallback = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=row.get("natr_pct"),
                rvol_normalized=_row_rvol(row),
            )
            if fallback is None:
                return
            lane = fallback
        broker = lane.trading_client
        if session == "RTH" and not broker.get_clock().is_open:
            return
        asset = broker.get_asset(sym)
        attrs = asset.attributes or []
        overnight = "overnight_tradable" in attrs and "overnight_halted" not in attrs
        if not asset.tradable or (session == "OVERNIGHT" and not overnight):
            return
        if side == "SHORT" and (
            session == "OVERNIGHT"
            or not lookup_alpaca_paper_borrow([sym], broker)[sym]["easy_to_borrow"]
        ):
            return
        quote = data.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=sym, feed=feed)
        )[sym]
        bid, ask = Decimal(str(quote.bid_price)), Decimal(str(quote.ask_price))
        if (
            not 0 < bid <= ask
            or not 0 <= (datetime.now(UTC) - quote.timestamp).total_seconds() <= 5
            or not quote_microstructure_allows_entry(bid, ask, session)
        ):
            return
        cid = (
            "noesis-"
            + hashlib.sha256(f"{sym}:{row['bar_close_utc']}:{side}".encode()).hexdigest()[:32]
        )
        effective_origin = origin
        prior_same_closed = False
        prior_opp_closed = False
        for other in farm.lanes.values():
            with open_bounded_duckdb(other.duckdb_path) as db:
                if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                    continue
                _ensure_entry_decisions_table(db)  # an older ledger gains close_cause
                if db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE client_order_id=? OR "
                    "(symbol=? AND (closed_at IS NULL OR "
                    "(side=? AND closed_at>now()-INTERVAL '60 seconds')))",
                    [cid, sym, side],
                ).fetchone():
                    return
                # An entry the broker refused never was a position: it makes the next
                # candidate neither a re-entry nor a reverse.
                if db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE symbol=? AND side=? "
                    "AND closed_at IS NOT NULL "
                    "AND (close_cause IS NULL OR NOT starts_with(close_cause, ?)) LIMIT 1",
                    [sym, side, _ENTRY_REJECT_CAUSE_PREFIX],
                ).fetchone():
                    prior_same_closed = True
                if db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE symbol=? AND side<>? "
                    "AND closed_at IS NOT NULL "
                    "AND (close_cause IS NULL OR NOT starts_with(close_cause, ?)) LIMIT 1",
                    [sym, side, _ENTRY_REJECT_CAUSE_PREFIX],
                ).fetchone():
                    prior_opp_closed = True
        if prior_opp_closed and "REVERSE" not in effective_origin:
            effective_origin = f"{effective_origin}_AUTO_REVERSE"
        elif prior_same_closed and "REENTRY" not in effective_origin:
            effective_origin = f"{effective_origin}_REENTRY"
        if (
            current_session() != session
            or not _calibration_allows_entry()
            or (datetime.now(UTC) - datetime.fromisoformat(row["bar_close_utc"])).total_seconds()
            >= 60
            or (datetime.now(UTC) - quote.timestamp).total_seconds() > 5
        ):
            return
        # Live: a first-time scanner candidate is an autonomous (tier-3) entry. A reverse or
        # re-entry candidate would be owner lineage, which "any closed row" cannot establish:
        # it stays a HOLD until the lineage resolver decides it. Paper is never held here.
        lineage_candidate = prior_opp_closed or prior_same_closed
        if _live_entry_hold(lane, sym, None if lineage_candidate else "AUTONOMOUS_ENTRY"):
            return
        try:
            res = lane.submit_guarded_entry(
                symbol=sym,
                side=side,
                ref_price=ask if side == "LONG" else bid,
                arrival_price=(bid + ask) / 2,
                decision_price=Decimal(str(trade.price)),
                natr_pct=Decimal(str(row["natr_pct"])),
                session=session,
                origin=effective_origin,
                client_order_id=cid,
                overnight_tradable=overnight,
                annual_borrow_rate=(
                    Decimal(str(row["annual_borrow_rate"]))
                    if row.get("annual_borrow_rate") is not None
                    else None
                ),
                prior_close=(
                    Decimal(str(row["prior_close"]))
                    if row.get("prior_close") is not None and float(row["prior_close"]) > 0
                    else None
                ),
            )
        except APIError as exc:
            reject_code = exc.status_code
            if reject_code is None or reject_code not in _ENTRY_REJECT_HTTP_CODES:
                raise
            # R13: the broker refused this one order. Its row is closed with the code, its
            # symbol is released, and entries on every other symbol continue.
            _close_rejected_entry(farm, lane, cid, sym, reject_code)
            entry_rejections[str(reject_code)] = entry_rejections.get(str(reject_code), 0) + 1
            return
        if res:
            parent = broker.get_order_by_id(
                res["broker_order_id"], GetOrderByIdRequest(nested=True)
            )
            stop = Decimal(str(res["stop_price"]))
            guards[sym] = Guard(
                lane,
                cid,
                sym,
                side,
                stop,
                stop,
                Decimal(str(row["natr_pct"])),
                session,
                False,
                parent,
            )
            guards[sym].fetch()
            if session == "RTH":
                guard = guards[sym]
                if not guard.child or (
                    Decimal(guard.child.stop_price) < stop
                    if side == "LONG"
                    else Decimal(guard.child.stop_price) > stop
                ):
                    if guard.parent.status.value not in TERMINAL | {"pending_cancel"}:
                        lane.broker_cancel(guard.parent.id)
                    raise RuntimeError("Broker did not confirm the requested OTO stop")
            report("ORDER_ACKNOWLEDGED")

    def process(trade: Any, feed: DataFeed) -> None:
        nonlocal last_fault_log
        with lock:
            try:
                handle(trade, feed)
            except Exception:
                # R13: one failed entry attempt or print is that attempt's outcome. It is
                # counted and logged (at most once a second) and holds nothing farm-wide.
                supervision_faults["process"] += 1
                if time.monotonic() - last_fault_log >= 1.0:
                    last_fault_log = time.monotonic()
                    LOG.exception("Paper broker or protection error")

    def callback(feed: DataFeed):
        async def on_trade(trade):
            await asyncio.to_thread(process, trade, feed)

        return on_trade

    callbacks = {feed: callback(feed) for feed in streams}

    def subscribe(symbols: set[str]) -> None:
        for feed, stream in streams.items():
            stream.subscribe_trades(callbacks[feed], *sorted(symbols))
        watching.update(symbols)

    async def on_trade_update(update: Any) -> None:
        """A fill on the account asks the loop to reconcile at once (adoption, stop cover)."""
        nonlocal trade_update_fills
        if _enum_text(getattr(update, "event", "")) in ("fill", "partial_fill"):
            trade_update_fills += 1
            reconcile_requested.set()

    def run_stream(label: str, stream: Any) -> None:
        """Run one stream until it returns or the supervisor stops (R2).

        A ``run()`` that raises (a dropped socket) is restarted after a doubling pause, so it
        cannot end the supervisor. After ``STREAM_RESTART_MAX_CONSECUTIVE`` failures in a row
        the error is raised into this stream's future: the loop goes on with the others.
        """
        failures = 0
        while not halt.is_set():
            started = time.monotonic()
            try:
                stream.run()
                return
            except Exception:
                if halt.is_set():
                    return
                healthy = time.monotonic() - started >= STREAM_HEALTHY_RUN_SECS
                failures = 1 if healthy else failures + 1
                stream_faults[label] = stream_faults.get(label, 0) + 1
                LOG.exception("Stream %s failed (%d in a row)", label, failures)
                if failures >= STREAM_RESTART_MAX_CONSECUTIVE:
                    raise
                halt.wait(
                    min(
                        STREAM_RESTART_BACKOFF_MAX_SECS,
                        STREAM_RESTART_BACKOFF_BASE_SECS * float(2 ** (failures - 1)),
                    )
                )

    def poll_stale_guards(sess: str) -> None:
        """REST fallback of the data plane (R2): drive guards the streams have gone quiet on.

        One latest-trade request covers every guard whose price is older than
        ``GUARD_PRICE_REST_FALLBACK_SECS``. It is a market-data request, so it takes nothing
        from the trading rate budget.
        """
        nonlocal rest_polls
        get_latest_trade = getattr(data, "get_stock_latest_trade", None)
        if sess == "CLOSED" or not callable(get_latest_trade):
            return
        now_mono = time.monotonic()
        stale = sorted(
            sym
            for sym, guard in guards.items()
            if not guard.lifecycle_closed
            and guard.price_age_secs(now_mono) > GUARD_PRICE_REST_FALLBACK_SECS
        )
        if not stale:
            return
        rest_polls += 1
        latest = get_latest_trade(
            StockLatestTradeRequest(
                symbol_or_symbols=stale,
                feed=DataFeed.BOATS if sess == "OVERNIGHT" else DataFeed.SIP,
            )
        )
        for sym in stale:
            trade = latest.get(sym) if isinstance(latest, Mapping) else None
            guard = guards.get(sym)
            if trade is None or guard is None:
                continue
            raw_price = getattr(trade, "price", None)
            if raw_price in (None, ""):
                continue
            drive_guard_price(
                guard,
                Decimal(str(raw_price)),
                sess,
                trade_ts=getattr(trade, "timestamp", None),
                source="REST",
            )

    def refresh_data_stale(sess: str) -> None:
        """Flag guards without a confirmed price for ``GUARD_DATA_STALE_SECS`` in the status."""
        now_mono = time.monotonic()
        stale = (
            []
            if sess == "CLOSED"
            else sorted(
                sym
                for sym, guard in guards.items()
                if not guard.lifecycle_closed
                and guard.price_age_secs(now_mono) > GUARD_DATA_STALE_SECS
            )
        )
        if stale != data_stale_symbols:
            if stale:
                LOG.error("DATA_STALE: no confirmed price for %s", stale)
            data_stale_symbols[:] = stale
            report(base_status, force=True)

    with open_bounded_duckdb(manifest_dir / "paper_farm_execution.duckdb"):
        with lock:
            check_clock()
            init_now = datetime.now(UTC)
            init_sess = current_session()
            positions = refresh_accounts(
                farm,
                init_now,
                init_sess,
                publish=False,
            )
            guards = restore_guards(farm, positions)
            adopt_unmanaged_broker_positions(
                farm,
                guards,
                positions=positions,
                session=init_sess,
                now_utc=init_now,
                skipped=adoption_skips,
            )
            stop_coverage_actions = ensure_native_protective_stops(
                farm, guards, session=init_sess, now_utc=init_now
            )
            cached_ledger_sig = _ledger_sig()
            last_stop_coverage = time.monotonic()
        subscribe(watching | set(guards))
        # Trade-update streams, when the coordinator has them: a fill event asks for a
        # reconcile at once. They are not part of what ends the loop, and without them the
        # timer reconcile below does the same work within STOP_COVERAGE_RECONCILE_SECS.
        try:
            update_streams.update(farm.trade_update_streams() or {})
        except Exception:
            LOG.exception("Trade-update streams unavailable; the timer reconcile covers adoption")
        for update_stream in update_streams.values():
            update_stream.subscribe_trade_updates(on_trade_update)
        report("AWAITING_MARKET_DATA")
        with ThreadPoolExecutor(max_workers=len(streams) + len(update_streams)) as pool:
            market_futures = {
                pool.submit(run_stream, f"market:{feed.value}", s): f"market:{feed.value}"
                for feed, s in streams.items()
            }
            for acct, update_stream in update_streams.items():
                pool.submit(run_stream, f"trade_updates:{acct}", update_stream)
            pending = set(market_futures)
            try:
                while pending:
                    finished = wait(pending, timeout=1, return_when=FIRST_COMPLETED).done
                    if finished:
                        # A market stream ended. The supervisor stops only when all of them
                        # have: one stream cannot end supervision of the others (R2).
                        pending -= finished
                        if pending:
                            pending -= concurrent_futures.wait(
                                pending, timeout=STREAM_END_GRACE_SECS
                            ).done
                        streams_ended[:] = sorted(
                            label for future, label in market_futures.items() if future.done()
                        )
                        if not pending:
                            break
                        LOG.error("Market stream(s) ended, supervising on: %s", streams_ended)
                    if stop is not None and stop.is_set():
                        break
                    with lock:
                        try:
                            refresh()
                        except (FileNotFoundError, duckdb.IOException):
                            candidates.clear()
                        except Exception:
                            # R13: a failed reconcile builds no candidates and is retried on
                            # the next tick. It does not hold entries beyond that.
                            candidates.clear()
                            supervision_faults["reconcile"] += 1
                            LOG.exception("Paper reconciliation failed")
                        if time.monotonic() - last_clock_check >= CLOCK_SKEW_CHECK_SECS:
                            check_clock()
                        # Stop coverage and adoption ride this loop's own tick: no trade print
                        # and no scanner file is needed, and a failure here is retried on the
                        # next tick. A fill event on a trade-update stream runs it at once.
                        forced = reconcile_requested.is_set()
                        if (
                            forced
                            or time.monotonic() - last_stop_coverage >= STOP_COVERAGE_RECONCILE_SECS
                        ):
                            reconcile_requested.clear()
                            last_stop_coverage = time.monotonic()
                            try:
                                _reconcile_stop_coverage(
                                    datetime.now(UTC), current_session(), forced=forced
                                )
                            except Exception:
                                stop_coverage_faults += 1
                                LOG.exception("Stop-coverage reconcile failed")
                        tick_sess = current_session()
                        if time.monotonic() - last_rest_poll >= GUARD_REST_POLL_SECS:
                            last_rest_poll = time.monotonic()
                            try:
                                poll_stale_guards(tick_sess)
                            except Exception:
                                supervision_faults["rest_poll"] += 1
                                LOG.exception("REST latest-trade fallback failed")
                        refresh_data_stale(tick_sess)
                        desired = {"SPY", "QQQ"} | set(guards) | set(candidates)
                    if added := desired - watching:
                        subscribe(added)
                    if removed := watching - desired:
                        for stream in streams.values():
                            stream.unsubscribe_trades(*sorted(removed))
                        watching.difference_update(removed)
                if not pending:
                    for future in market_futures:
                        future.result()
            finally:
                halt.set()
                for stream in (*streams.values(), *update_streams.values()):
                    try:
                        stream.stop()
                    except Exception:
                        LOG.exception("Stream stop failed")
                report("STOPPED")


def _load_staged_tv_preset(manifest_dir: Path, alert_id: str) -> tuple[dict[str, Any], Path]:
    """Locate a staged TV preset by alert_id without mutating staged files."""
    target_id = alert_id.strip().lower()
    if not target_id:
        raise ValueError("alert_id must be a non-empty hex identifier")
    candidates = (
        manifest_dir / "tv_sota_pack" / "staged_tv_presets.json",
        manifest_dir / "staged_tv_presets.json",
        manifest_dir / "tv_sota_pack" / "staged_scan_presets.json",
    )
    for path in candidates:
        if not path.is_file():
            continue
        doc = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        if doc.get("semi_auto_confirmed") is True:
            raise PermissionError(
                f"Staged preset manifest {path.name} has semi_auto_confirmed=True"
            )
        for item in doc.get("presets") or ():
            if (
                isinstance(item, dict)
                and str(item.get("alert_id") or "").strip().lower() == target_id
            ):
                return dict(item), path
    raise ValueError(f"Staged TV alert_id not found: {alert_id!r}")


def _lookup_mover_prior_close(manifest_dir: Path, symbol: str) -> Decimal | None:
    movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
    if not movers_path.is_file():
        return None
    try:
        doc = json.loads(movers_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    for row in doc.get("entries") or ():
        if str(row.get("symbol") or "").strip().upper() == symbol:
            raw = row.get("prior_close")
            if raw is not None and not isinstance(raw, bool):
                val = Decimal(str(raw))
                if val.is_finite() and val > 0:
                    return val
    return None


def _compute_r_lock_levels(
    entry_limit: Decimal, stop_price: Decimal, side: risk.Side
) -> dict[str, float]:
    risk_r = abs(entry_limit - stop_price)
    sign = Decimal("1") if side == "LONG" else Decimal("-1")

    def _q(px: Decimal) -> float:
        return float(risk.quantize_rule_612(max(risk.TICK_SUB_DOLLAR, px), side=side, is_stop=True))

    be_px = entry_limit * (Decimal("1.0010") if side == "LONG" else Decimal("0.9990"))
    return {
        "be_plus_fee": _q(be_px),
        "lock_at_1r": _q(entry_limit + sign * Decimal("0.45") * risk_r),
        "lock_at_2r": _q(entry_limit + sign * Decimal("1.35") * risk_r),
        "lock_at_3r": _q(entry_limit + sign * Decimal("2.35") * risk_r),
    }


def submit_confirmed_tv_preset_paper(
    manifest_dir: Path,
    *,
    alert_id: str,
    qty: int = 1,
    dry_run: bool = False,
    now_utc: datetime | None = None,
    target_account: str | None = None,
) -> dict[str, Any]:
    """Fail-closed, operator-confirmed paper-only handoff of a staged TradingView preset."""
    verify_paper_environment()
    if isinstance(qty, bool) or not isinstance(qty, int) or qty < 1:
        raise ValueError(f"qty must be a positive integer, got {qty!r}")
    normalized_account: str | None = None
    if target_account is not None:
        if isinstance(target_account, bool) or not isinstance(target_account, str):
            raise PermissionError(f"Refused invalid Alpaca paper target_account {target_account!r}")
        normalized_account = target_account.strip().upper()
        if normalized_account not in ALPACA_LANE_PROFILES:
            allowed = ", ".join(sorted(ALPACA_LANE_PROFILES))
            raise PermissionError(
                f"Refused non-paper or unknown Alpaca lane account {target_account!r}; "
                f"allowed paper lanes: {allowed}"
            )

    from tier0_sota.tv.bridge import refuse_live_endpoints, validate_tv_alert_payload

    raw_preset, staged_source = _load_staged_tv_preset(manifest_dir, alert_id)
    if raw_preset.get("semi_auto_confirmed") is not False:
        raise PermissionError("Staged TV preset must retain semi_auto_confirmed=False")
    refuse_live_endpoints(raw_preset)
    req_vp = str(raw_preset.get("venue_profile") or "").strip().upper()
    req_prof = engine.validate_venue_order_capability(req_vp) if req_vp else None

    now = now_utc or datetime.now(UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    session = engine.classify_us_equity_session_et(now_ns)
    if session == "CLOSED":
        raise ValueError("Refused TV preset execution while XNYS 24/5 session is CLOSED")
    raw_side = str(raw_preset.get("side") or "").strip().upper()
    raw_sess = str(raw_preset.get("session") or "").strip().upper()
    if raw_side == "SELL" and (session == "OVERNIGHT" or raw_sess == "OVERNIGHT"):
        raise ValueError("ALPACA OVERNIGHT SELL unsupported: ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ")
    if req_prof is not None and req_prof.broker != "ALPACA":
        raise ValueError(
            f"Alpaca paper TV handoff requires ALPACA paper venue profile, got {req_vp!r}"
        )

    validated = validate_tv_alert_payload(raw_preset, venue="ALPACA", live_intake=False)
    sym = str(validated["symbol"]).strip().upper()
    route_side: risk.Side = "LONG" if validated["side"] == "BUY" else "SHORT"
    natr_d = Decimal(str(validated["natr_pct"]))
    is_inverse = sym in _INVERSE_HEDGE_SYMBOLS
    if is_inverse and route_side == "LONG":
        origin = f"INVERSE_HEDGE_{sym}"
    elif route_side == "SHORT":
        origin = "DIRECT_SHORT_ETB"
    else:
        origin = "TV_HALF_AUTO_CONFIRMED"

    farm = PaperFarmCoordinator(manifest_dir)
    if not cost_calibration_allows_entry(manifest_dir):
        raise PermissionError("TCA_RECALIBRATION_REQUIRED: cost calibration blocks new paper entry")

    def _evaluate_and_maybe_submit() -> dict[str, Any]:
        positions = (
            refresh_accounts(farm, now, session, publish=False, record_fuse=not dry_run) or {}
        )
        owners = farm.symbol_lock.snapshot()
        if sym in owners:
            raise PermissionError(
                f"CrossLaneSymbolLock refused {sym}: already owned by {owners[sym]}"
            )
        open_sides = _collect_open_symbol_sides(farm, positions=positions)
        opposing_conflict = find_opposing_inverse_pair_conflict(sym, route_side, open_sides)
        if opposing_conflict is not None:
            raise PermissionError(
                f"OpposingInversePairGuard refused {sym} {route_side}: "
                f"{opposing_conflict['opposing_symbol']} {opposing_conflict['opposing_side']} "
                f"is still open on {opposing_conflict['opposing_owner']}; flatten-first required"
            )
        if normalized_account is not None:
            lane = farm.lanes.get(normalized_account)
            if lane is None or not lane.breaker.allow_entry():
                raise PermissionError(
                    f"Requested Alpaca paper lane {normalized_account} unavailable "
                    "or daily drawdown breaker tripped"
                )
        else:
            lane = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=natr_d,
                rvol_normalized=validated.get("rvol"),
            )
            if lane is None:
                raise PermissionError(
                    "All Alpaca paper lanes unavailable or daily drawdown breaker tripped"
                )
        lane.assert_integrity()
        if lane.environment != "PAPER":
            # This handoff is the paper one; a live lane has its own operator-confirmed path.
            raise PermissionError("Live execution disabled; paper_only=True enforced.")

        broker = lane.trading_client
        if session == "RTH" and not broker.get_clock().is_open:
            raise ValueError("Broker reports RTH market clock is closed")
        asset = broker.get_asset(sym)
        attrs = getattr(asset, "attributes", None) or []
        overnight = "overnight_tradable" in attrs and "overnight_halted" not in attrs
        if not getattr(asset, "tradable", False):
            raise ValueError(f"Asset {sym} is not tradable on Alpaca Paper")
        if session == "OVERNIGHT" and not overnight:
            raise ValueError(f"Asset {sym} is not overnight_tradable on Alpaca BOATS")
        if route_side == "SHORT" and (
            lookup_alpaca_paper_borrow([sym], broker).get(sym, {}).get("easy_to_borrow") is not True
        ):
            raise ValueError(f"Short refused for non-ETB symbol {sym}")

        data = ExtremeMoverScanner.from_env().data_client
        if data is None:
            raise RuntimeError("Missing market data client")
        feed = DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP
        quote = data.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=sym, feed=feed)
        )[sym]
        quote_now = now_utc or datetime.now(UTC)
        bid, ask = Decimal(str(quote.bid_price)), Decimal(str(quote.ask_price))
        if not 0 <= (quote_now - quote.timestamp).total_seconds() <= 5:
            raise ValueError(f"Stale or future live quote for {sym}")
        if not quote_microstructure_allows_entry(bid, ask, session):
            raise ValueError(
                f"Quote microstructure rejected {sym} (bid={bid}, ask={ask}, session={session})"
            )

        sched_name, sp_map = engine.select_pooled_trailing_schedule(natr_d)
        prof = engine.get_session_protection_profile(session, sp_map)
        is_ext = session != "RTH"
        ref_price = ask if route_side == "LONG" else bid
        arrival_price = (bid + ask) / Decimal("2")
        entry_collar = prof.limit_collar_pct if is_ext else RTH_ENTRY_COLLAR_PCT
        entry_limit = engine.compute_collared_entry_limit_price(ref_price, route_side, entry_collar)
        stop_price = risk.compute_capped_stop_price(
            entry_limit, route_side, max_loss_pct=prof.initial_max_loss_pct
        )
        stop_limit_price = (
            engine.compute_collared_limit_price(stop_price, route_side, prof.limit_collar_pct)
            if is_ext
            else None
        )
        r_lock_levels = _compute_r_lock_levels(entry_limit, stop_price, route_side)
        protective_stop_mode = "COLLARED_STOP_LIMIT_SUPERVISED" if is_ext else "NATIVE_OTO_STOP"
        spread_bps_val = (
            ((ask - bid) / arrival_price) * Decimal("10000")
            if arrival_price > Decimal("0")
            else Decimal("5")
        )
        trail_spec = engine.compute_non_noise_min_trail_spec(
            symbol=sym,
            entry_price=entry_limit,
            side=route_side,
            session=session,
            natr_pct=natr_d,
            spread_bps=spread_bps_val,
            venue_profile=lane.venue_profile,
        )
        preview: dict[str, Any] = {
            "ok": True,
            "dry_run": True,
            "would_submit": True,
            "paper_only": farm.environment == "PAPER",
            "live_execution_enabled": farm.autonomous_opt_in is True,
            "semi_auto_confirmed_in_stage": False,
            "auto_adopted_by_engine": True,
            "staged_source": staged_source.name,
            "alert_id": str(validated["alert_id"]),
            "symbol": sym,
            "side": route_side,
            "qty": qty,
            "lane_account": lane.account,
            "lane_id": lane.profile["lane_id"],
            "venue_profile": lane.venue_profile,
            "order_handle_spec": lane.order_handle_spec(
                session=session, trailing_schedule=sched_name
            ),
            "session": session,
            "origin": origin,
            "order_type": prof.order_mode,
            "entry_order_type": "LIMIT_EXTENDED" if is_ext else "OTO_LIMIT",
            "protective_stop_mode": protective_stop_mode,
            "execution_mechanism": str(trail_spec["execution_mechanism"]),
            "broker_exit_order": str(trail_spec["broker_exit_order"]),
            "min_trailing_pct": float(trail_spec["non_noise_min_trail_pct"]),
            "non_noise_min_trail_pct": float(trail_spec["non_noise_min_trail_pct"]),
            "fast_sell_non_noise_reversal_pct": float(
                trail_spec["fast_sell_non_noise_reversal_pct"]
            ),
            "fast_sell_non_noise_reverse_trigger": float(
                compute_fast_reverse_trigger(
                    entry_limit, route_side, trail_spec["non_noise_min_trail_pct"]
                )
            ),
            "session_order_presets": build_symbol_24x5_session_presets(
                symbol=sym,
                side=route_side,
                ref_price=entry_limit,
                natr_pct=natr_d,
                spread_bps=spread_bps_val,
                alpaca_venue_profile=lane.venue_profile,
                alpaca_max_rpm=lane.rate_budget.max_rpm,
            ),
            "peak_climax_reverse_trigger_pct": float(trail_spec["peak_climax_reverse_trigger_pct"]),
            "auto_reverse_policy": str(trail_spec["auto_reverse_policy"]),
            "rapid_reentry_policy": trail_spec["rapid_reentry_policy"],
            "entry_limit": float(entry_limit),
            "stop_price": float(stop_price),
            "stop_limit_price": float(stop_limit_price) if stop_limit_price is not None else None,
            "collar_bps": float(prof.limit_collar_bps),
            "trailing_schedule": sched_name,
            "r_lock_levels": r_lock_levels,
        }
        if dry_run:
            return preview

        cid = f"noesis-tv-{validated['alert_id']}"
        for other in farm.lanes.values():
            if not other.duckdb_path.is_file():
                continue
            with open_bounded_duckdb(other.duckdb_path, read_only=True) as db:
                if ("paper_entry_decisions",) in db.execute(
                    "SHOW TABLES"
                ).fetchall() and db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE client_order_id=? OR "
                    "(symbol=? AND (closed_at IS NULL OR closed_at>now()-INTERVAL '60 seconds'))",
                    [cid, sym],
                ).fetchone():
                    raise PermissionError(
                        f"Duplicate or cooldown-blocked TV paper entry for {sym} ({cid})"
                    )

        if not farm.symbol_lock.try_claim(sym, lane.account):
            raise PermissionError(f"CrossLaneSymbolLock refused concurrent claim for {sym}")
        with lane._budget_lock:
            allowed_rpm = lane.rate_budget.try_consume(now.timestamp(), is_exit=False)
        if not allowed_rpm:
            farm.symbol_lock.release(sym, lane.account)
            raise PermissionError(
                f"Rate budget exhausted on {lane.account} (exit reserve protected)"
            )

        annual_rate = Decimal(str(INVERSE_ETF_ANNUAL_EXPENSE_RATES.get(sym, 0.0)))
        prior_close = _lookup_mover_prior_close(manifest_dir, sym)
        if origin.startswith("INVERSE_HEDGE_") and prior_close is None:
            prior_close = Decimal(str(validated["trigger_price"]))

        req_kw: dict[str, Any] = {
            "symbol": sym,
            "qty": qty,
            "side": OrderSide.BUY if route_side == "LONG" else OrderSide.SELL,
            "limit_price": float(entry_limit),
            "time_in_force": TimeInForce.DAY if is_ext else TimeInForce.GTC,
            "extended_hours": is_ext,
            "client_order_id": cid,
        }
        if not is_ext:
            req_kw["order_class"] = OrderClass.OTO
            req_kw["stop_loss"] = StopLossRequest(stop_price=float(stop_price))
        request = LimitOrderRequest(**req_kw)

        lane.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with open_bounded_duckdb(lane.duckdb_path) as conn:
            _ensure_entry_decisions_table(conn)
            conn.execute(
                "INSERT INTO paper_entry_decisions "
                "(client_order_id,symbol,side,decision_price,arrival_price,session,origin,"
                "annual_rate,prior_close,initial_stop,natr_pct) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    cid,
                    sym,
                    route_side,
                    arrival_price,
                    arrival_price,
                    session,
                    origin,
                    annual_rate,
                    prior_close,
                    stop_price,
                    natr_d,
                ),
            )
        order = lane.broker_submit(request, intent="OPERATOR_CONFIRMED_ENTRY")
        now_iso = now.isoformat()
        broker_order_id = str(getattr(order, "id", ""))
        order_status = str(getattr(order, "status", "SUBMITTED"))

        if is_ext and stop_limit_price is not None:
            supervision = ExtendedHoursStopSupervision(
                ts_utc=now_iso,
                account=lane.account,
                lane_id=lane.profile["lane_id"],
                broker_order_id=broker_order_id,
                symbol=sym,
                side=route_side,
                qty=qty,
                session=session,
                protective_stop_mode="COLLARED_STOP_LIMIT_SUPERVISED",
                ref_price=ref_price,
                stop_price=stop_price,
                stop_limit_price=stop_limit_price,
                collar_bps=prof.limit_collar_bps,
                max_loss_pct=prof.initial_max_loss_pct,
                status="ARMED",
            )
            lane.supervised_stops[broker_order_id] = supervision
            persist_supervised_stop_duckdb(lane.duckdb_path, supervision)
        lane._record_order(
            (
                now_iso,
                lane.account,
                lane.profile["lane_id"],
                broker_order_id,
                sym,
                route_side,
                float(qty),
                "LIMIT_EXTENDED" if is_ext else "LIMIT",
                float(entry_limit),
                float(stop_price),
                session,
                order_status,
                0.0,
            )
        )
        return preview | {
            "dry_run": False,
            "would_submit": False,
            "submitted": True,
            "broker_order_id": broker_order_id,
            "client_order_id": cid,
            "broker_status": order_status,
            "submitted_at_utc": now_iso,
        }

    if dry_run:
        return _evaluate_and_maybe_submit()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
        return _evaluate_and_maybe_submit()

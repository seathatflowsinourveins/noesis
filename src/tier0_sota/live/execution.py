"""Alpaca paper order management driven by the native SIP and BOATS trade streams."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockLatestQuoteRequest
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
_FRESH_FILL_SETTLE_SECS = 10.0
EXIT_REPEG_MIN_AGE_SECS = 15.0
MAX_SUPERVISED_EXIT_ATTEMPTS = 12
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


def cost_calibration_allows_entry(manifest_dir: Path) -> bool:
    """Honor the published Dagster reconciliation, including an adverse measured cost."""
    path = manifest_dir / "tca_calibration_manifest.json"
    if not path.exists():
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

    def __post_init__(self) -> None:
        entry_px = self._filled_entry_price()
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
            "paper_only": True,
            "live_execution_enabled": False,
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
        entry_px = self._filled_entry_price() or self.initial
        trail_spec = self._trail_spec(session, entry_px)
        self.peak_reverse_armed = True
        self.reverse_flatten = True
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
        trail_spec = self._trail_spec(session, entry_dec if entry_dec is not None else self.initial)
        nn_pct = (
            self.non_noise_min_trail_pct
            if self.non_noise_min_trail_pct > Decimal("0")
            else trail_spec["non_noise_min_trail_pct"]
        )
        eff_lock_tier = max(
            int(self.lock_tier),
            _infer_lock_tier(entry_dec, self.initial, self.stop, self.side),
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
        entry_px = self._filled_entry_price()
        if entry_px is not None:
            self.lock_tier = max(
                int(self.lock_tier),
                _infer_lock_tier(entry_px, self.initial, self.stop, self.side),
            )
            if self.lock_tier >= 1:
                self.fast_sell_armed = True

    def on_price(self, price: Decimal, session: str) -> None:
        """Tighten a native stop or supervise a collared extended-session exit."""
        long = self.side == "LONG"
        better = max if long else min
        behind = (lambda a, b: a < b) if long else (lambda a, b: a > b)
        if (
            self.parent.status.value not in TERMINAL
            or self.child
            and self.child.status.value in {"held", "pending_replace", "pending_new"}
        ) and (
            time.monotonic() - self.checked >= 1
            and self.lane.rate_budget.try_consume(time.time(), is_exit=True)
        ):
            self.fetch()
        sched_name, schedule = engine.select_pooled_trailing_schedule(self.natr)
        self.trailing_schedule = sched_name
        profile = engine.get_session_protection_profile(session, schedule)
        entry_px = self._filled_entry_price()
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
                if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
                    return
                try:
                    self.child = self.lane.trading_client.replace_order_by_id(
                        self.child.id,
                        ReplaceOrderRequest(stop_price=float(desired)),
                    )
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
            self.save()
        if time.monotonic() - self.last_exit_check < 1:
            return
        if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
            return
        self.last_exit_check = time.monotonic()
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
                        broker.cancel_order_by_id(order.id)
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
        if Decimal(self.parent.filled_qty or "0") == 0 or (
            self.child and self.child.status.value == "filled"
        ):
            return
        covered = Decimal(self.child.filled_qty or "0") if self.child else Decimal(0)
        exit_collar = profile.limit_collar_pct if session != "RTH" else RTH_ENTRY_COLLAR_PCT
        limit = engine.compute_collared_limit_price(price, self.side, exit_collar)
        attempt = 0
        exit_client_id = f"exit-{self.parent.id}"
        while attempt < MAX_SUPERVISED_EXIT_ATTEMPTS:
            base_id = f"exit-{self.parent.id}"
            exit_client_id = base_id if attempt == 0 else f"{base_id}-{attempt}"
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
                if (
                    should_cancel
                    and attempt + 1 < MAX_SUPERVISED_EXIT_ATTEMPTS
                    and self.lane.rate_budget.try_consume(time.time(), is_exit=True)
                ):
                    try:
                        broker.cancel_order_by_id(existing.id)
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
                        MAX_SUPERVISED_EXIT_ATTEMPTS,
                        limit,
                        session,
                    )
                return
            if exit_status == "rejected":
                raise RuntimeError("Broker rejected the supervised exit")
            covered += Decimal(existing.filled_qty or "0")
            if covered >= Decimal(self.parent.filled_qty):
                return
            attempt += 1
        else:
            LOG.error(
                "Supervised exit retry cap (%d) reached for %s; refusing unbounded churn",
                MAX_SUPERVISED_EXIT_ATTEMPTS,
                self.symbol,
            )
            return
        try:
            position = broker.get_open_position(self.symbol)
        except APIError as exc:
            if exc.status_code != 404:
                raise
            return
        exposure = Decimal(position.qty)
        if (exposure > 0) != long:
            return
        qty = min(
            Decimal(self.parent.filled_qty) - covered,
            abs(exposure),
            abs(Decimal(position.qty_available)),
        )
        if qty <= 0:
            return
        exit_req: LimitOrderRequest | MarketOrderRequest
        exit_order_type: str
        exit_record_price: float
        if session == "RTH":
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
        submitted_exit = broker.submit_order(exit_req)
        self.last_exit_submit_mono = time.monotonic()
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


def _normalize_broker_stop_child(
    order: Any, *, symbol: str, exit_side: str, stop_price: Decimal
) -> Any:
    raw_status = getattr(order, "status", "new")
    status_val = str(getattr(raw_status, "value", raw_status) or "new")
    raw_side = getattr(order, "side", exit_side)
    side_val = str(getattr(raw_side, "value", raw_side) or exit_side)
    return SimpleNamespace(
        id=str(getattr(order, "id", "") or ""),
        symbol=str(getattr(order, "symbol", None) or symbol),
        side=SimpleNamespace(value=side_val),
        status=SimpleNamespace(value=status_val),
        stop_price=str(getattr(order, "stop_price", None) or stop_price),
        replaced_by=getattr(order, "replaced_by", None),
        filled_qty=str(getattr(order, "filled_qty", None) or "0"),
    )


def _find_open_broker_stop(broker: Any, symbol: str, exit_side: str) -> Any:
    if not hasattr(broker, "get_orders"):
        return None
    try:
        orders = broker.get_orders(GetOrdersRequest(limit=500)) or ()
    except Exception:  # noqa: BLE001
        return None
    for ord_ in orders:
        if str(getattr(ord_, "symbol", "") or "").strip().upper() != symbol:
            continue
        raw_side = getattr(ord_, "side", "")
        side_val = str(getattr(raw_side, "value", raw_side) or "").strip().lower()
        raw_stop = getattr(ord_, "stop_price", None)
        if side_val == exit_side and raw_stop not in (None, "", "0", 0):
            return _normalize_broker_stop_child(
                ord_, symbol=symbol, exit_side=exit_side, stop_price=Decimal(str(raw_stop))
            )
    return None


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


def adopt_unmanaged_broker_positions(
    farm: PaperFarmCoordinator,
    guards: dict[str, Guard],
    *,
    positions: dict[str, dict[str, Any]] | None = None,
    session: str | None = None,
    movers_by_symbol: dict[str, dict[str, Any]] | None = None,
    now_utc: datetime | None = None,
) -> list[Guard]:
    """Auto-adopt any unmanaged open paper position into 24/5 lossless trailing supervision."""
    now = now_utc or datetime.now(UTC)
    active_session = session or engine.classify_us_equity_session_et(
        int(now.timestamp() * 1_000_000_000)
    )
    adopted: list[Guard] = []
    movers = movers_by_symbol or {}

    for acct, lane in farm.lanes.items():
        if not lane.paper_only or lane.live_execution_enabled:
            raise PermissionError(f"Refused non-paper adoption on {acct}; paper_only=True required")
        broker = lane.trading_client
        if positions is not None and acct in positions:
            open_positions = list(positions[acct].values())
        else:
            open_positions = list(broker.get_all_positions() or ())
        for pos in open_positions:
            parsed = _parse_position_for_adoption(pos)
            if parsed is None:
                continue
            sym, side, abs_qty, entry_px, curr_px = parsed
            if sym in guards:
                continue
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
            trail_spec = engine.compute_non_noise_min_trail_spec(
                symbol=sym,
                entry_price=entry_px,
                side=side,
                session=active_session,
                natr_pct=natr_d,
                spread_bps=spread_d,
                archetype=archetype,
                venue_profile=lane.venue_profile,
            )
            initial_stop: Decimal = trail_spec["initial_stop_price"]
            is_long = side == "LONG"
            better = max if is_long else min
            behind = (lambda a, b: a < b) if is_long else (lambda a, b: a > b)
            peak_px = better(entry_px, curr_px)
            active_stop = initial_stop
            lock_tier = 0
            fast_sell_armed = False
            peak_reverse_armed = False
            sched_name, sp_map = engine.select_pooled_trailing_schedule(natr_d)
            prof = engine.get_session_protection_profile(active_session, sp_map)
            eff_be_trigger: Decimal = trail_spec["adaptive_breakeven_trigger_pct"]
            fav_gain = (peak_px - entry_px) if is_long else (entry_px - peak_px)
            fav_gain_pct = fav_gain / entry_px
            if fav_gain_pct >= eff_be_trigger:
                fast_sell_armed = True
                prog_stop, lock_tier, _ = engine.compute_progressive_trailing_stop(
                    entry_price=entry_px,
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

            tag = hashlib.sha256(f"{acct}:{sym}:{side}:{entry_px}".encode()).hexdigest()[:12]
            cid = f"adopted-{acct.lower()}-{sym.lower()}-{tag}"
            exit_side_str = "sell" if is_long else "buy"
            child = None
            if active_session == "RTH":
                child = _find_open_broker_stop(broker, sym, exit_side_str)
                if child is None and lane.rate_budget.try_consume(now.timestamp(), is_exit=True):
                    stop_req = StopOrderRequest(
                        symbol=sym,
                        qty=float(abs_qty),
                        side=OrderSide.SELL if is_long else OrderSide.BUY,
                        stop_price=float(active_stop),
                        time_in_force=TimeInForce.GTC,
                        client_order_id=f"stop-{cid}",
                    )
                    submitted_stop = broker.submit_order(stop_req)
                    child = _normalize_broker_stop_child(
                        submitted_stop,
                        symbol=sym,
                        exit_side=exit_side_str,
                        stop_price=active_stop,
                    )
            else:
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
                    qty=max(1, int(abs_qty)),
                    session=active_session,
                    protective_stop_mode="COLLARED_STOP_LIMIT_SUPERVISED",
                    ref_price=entry_px,
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

            lane.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            with open_bounded_duckdb(lane.duckdb_path) as db:
                _ensure_entry_decisions_table(db)
                db.execute(
                    "INSERT OR REPLACE INTO paper_entry_decisions "
                    "(client_order_id,symbol,side,decision_price,arrival_price,session,origin,"
                    "annual_rate,prior_close,initial_stop,natr_pct,active_stop,exit_triggered,"
                    "adopted_external) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                    ),
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
                legs=[child] if child is not None else [],
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
                child=child,
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
            )
            guards[sym] = guard
            adopted.append(guard)
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
    return guard


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
            rows = db.execute(
                "SELECT client_order_id,symbol,side,initial_stop,"
                "coalesce(active_stop,initial_stop),"
                "natr_pct,session,exit_triggered,origin "
                "FROM paper_entry_decisions WHERE closed_at IS NULL"
            ).fetchall()
        for cid, sym, side, initial, stop, natr, session, triggered, origin in rows:
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
                _, _, abs_qty, entry_px, curr_px = parsed
                exit_side_str = "sell" if side == "LONG" else "buy"
                child = _find_open_broker_stop(lane.trading_client, sym, exit_side_str)
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
                peak_px = max(entry_px, curr_px) if side == "LONG" else min(entry_px, curr_px)
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
                lane.trading_client.cancel_order_by_id(guard.parent.id)
            if sym in guards:
                raise PermissionError("Overlapping managed paper orders")
            guards[sym] = _merge_guard_runtime_state(guard, prev_map.get(sym))
    return guards


def run_execution(manifest_dir: Path, *, stop: threading.Event | None = None) -> None:
    """One exclusive execution process. Each entry requires a fresh trade on its routed feed."""
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
    scan_path = manifest_dir / "live_whole_market_movers_manifest.json"
    tca_path = manifest_dir / "tca_calibration_manifest.json"
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
    entry_fault = (
        json.loads(state_path.read_text("utf-8")).get("entry_fault") is True
        if state_path.exists()
        else False
    )

    def _file_stat_sig(path: Path) -> tuple[int, int, int] | None:
        try:
            st = path.stat()
        except FileNotFoundError:
            return None
        return (st.st_mtime_ns, st.st_size, st.st_ino)

    def _ledger_sig() -> tuple[object, ...]:
        return tuple(
            (_file_stat_sig(ledger), _file_stat_sig(Path(f"{ledger}.wal")))
            for ledger in (manifest_dir / _duckdb_filename(acct) for acct in ALPACA_LANE_PROFILES)
        )

    def _reconcile_live_guards(
        now_dt: datetime, sess: str, rows_map: dict[str, dict[str, Any]] | None = None
    ) -> None:
        nonlocal cached_ledger_sig, guards
        with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
            positions = refresh_accounts(farm, now_dt, sess, publish=False)
            guards = restore_guards(farm, positions, existing_guards=guards)
            adopt_unmanaged_broker_positions(
                farm,
                guards,
                positions=positions,
                session=sess,
                movers_by_symbol=rows_map,
                now_utc=now_dt,
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

    def report(status: str) -> None:
        nonlocal previous_status, last_report
        if status == previous_status and time.monotonic() - last_report < 30:
            return
        previous_status, last_report = status, time.monotonic()
        curr_sess = engine.classify_us_equity_session_et(time.time_ns())
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
                "paper_only": True,
                "live_execution_enabled": False,
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
                "entry_fault": entry_fault,
            },
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
                cur_sess = engine.classify_us_equity_session_et(time.time_ns())
                _reconcile_live_guards(now, cur_sess)
            candidates.clear()
            report("STALE_SCAN")
            return
        tca_file_sig = _file_stat_sig(tca_path)
        if tca_file_sig is not None:
            tca_sig: tuple[object, ...] = ("file", tca_file_sig)
        else:
            tca_sig = ("ledgers", *cur_ledger_sig)
        if tca_sig != cached_tca_sig or cached_tca_ok is None:
            cached_tca_ok = cost_calibration_allows_entry(manifest_dir)
            cached_tca_sig = tca_sig
        calibration_ok = cached_tca_ok
        if doc["generated_at_utc"] == scan_generation:
            if ledger_changed:
                cur_sess = engine.classify_us_equity_session_et(time.time_ns())
                rows_cached = {r["symbol"]: r for r in doc["entries"]}
                _reconcile_live_guards(now, cur_sess, rows_cached)
                report("SUPERVISING" if not entry_fault else "ENTRY_HELD")
            if not calibration_ok:
                candidates.clear()
                report("TCA_RECALIBRATION_REQUIRED")
            return
        session = engine.classify_us_equity_session_et(time.time_ns())
        candidates.clear()
        # Serialize ledger reconciliation with the scheduled observation.
        rows = {r["symbol"]: r for r in doc["entries"]}
        _reconcile_live_guards(now, session, rows)
        scan_generation = doc["generated_at_utc"]
        attempts.clear()
        macro_regime = doc["macro_regime"]["regime"]
        for guard in guards.values():
            if guard_should_flatten_for_flip(
                guard, market_regime=macro_regime, rows=rows, session=session
            ):
                guard.reverse_flatten = True
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
                    if lane is not None:
                        candidates.setdefault(sym, (lane, target, side, origin))
        active_feed = DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP
        last = last_feed_trade.get(active_feed)
        report(
            "CLOSED"
            if session == "CLOSED"
            else "ENTRY_HELD"
            if entry_fault
            else "SUPERVISING"
            if last and (now - last).total_seconds() < 60
            else "AWAITING_MARKET_DATA"
        )

    def handle(trade: Any, feed: DataFeed) -> None:
        nonlocal entry_fault
        session = engine.classify_us_equity_session_et(time.time_ns())
        if session == "CLOSED" or (session == "OVERNIGHT") != (feed == DataFeed.BOATS):
            return
        sym = trade.symbol
        if not 0 <= (datetime.now(UTC) - trade.timestamp).total_seconds() <= 5 or trade.price <= 0:
            return
        last_feed_trade[feed] = max(trade.timestamp, last_feed_trade.get(feed, trade.timestamp))
        if sym in guards:
            guards[sym].on_price(Decimal(str(trade.price)), session)
            return
        if entry_fault or sym not in candidates or sym in attempts:
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
                if db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE client_order_id=? OR "
                    "(symbol=? AND (closed_at IS NULL OR "
                    "(side=? AND closed_at>now()-INTERVAL '60 seconds')))",
                    [cid, sym, side],
                ).fetchone():
                    return
                if db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE symbol=? AND side=? "
                    "AND closed_at IS NOT NULL LIMIT 1",
                    [sym, side],
                ).fetchone():
                    prior_same_closed = True
                if db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE symbol=? AND side<>? "
                    "AND closed_at IS NOT NULL LIMIT 1",
                    [sym, side],
                ).fetchone():
                    prior_opp_closed = True
        if prior_opp_closed and "REVERSE" not in effective_origin:
            effective_origin = f"{effective_origin}_AUTO_REVERSE"
        elif prior_same_closed and "REENTRY" not in effective_origin:
            effective_origin = f"{effective_origin}_REENTRY"
        if (
            engine.classify_us_equity_session_et(time.time_ns()) != session
            or not cost_calibration_allows_entry(manifest_dir)
            or (datetime.now(UTC) - datetime.fromisoformat(row["bar_close_utc"])).total_seconds()
            >= 60
            or (datetime.now(UTC) - quote.timestamp).total_seconds() > 5
        ):
            return
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
                        broker.cancel_order_by_id(guard.parent.id)
                    raise RuntimeError("Broker did not confirm the requested OTO stop")
            report("ORDER_ACKNOWLEDGED")

    def process(trade: Any, feed: DataFeed) -> None:
        nonlocal entry_fault
        with lock:
            try:
                handle(trade, feed)
            except Exception:
                entry_fault = True
                candidates.clear()
                report("ENTRY_HELD")
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

    with open_bounded_duckdb(manifest_dir / "paper_farm_execution.duckdb"):
        with lock:
            init_now = datetime.now(UTC)
            init_sess = engine.classify_us_equity_session_et(time.time_ns())
            positions = refresh_accounts(
                farm,
                init_now,
                init_sess,
                publish=False,
            )
            guards = restore_guards(farm, positions)
            adopt_unmanaged_broker_positions(
                farm, guards, positions=positions, session=init_sess, now_utc=init_now
            )
            cached_ledger_sig = _ledger_sig()
        subscribe(watching | set(guards))
        report("AWAITING_MARKET_DATA")
        with ThreadPoolExecutor(max_workers=len(streams)) as pool:
            futures = [pool.submit(s.run) for s in streams.values()]
            try:
                while not (done := wait(futures, timeout=1, return_when=FIRST_COMPLETED).done):
                    if stop is not None and stop.is_set():
                        break
                    with lock:
                        try:
                            refresh()
                        except (FileNotFoundError, duckdb.IOException):
                            candidates.clear()
                        except Exception:
                            candidates.clear()
                            entry_fault = True
                            report("ENTRY_HELD")
                            LOG.exception("Paper reconciliation failed")
                        desired = {"SPY", "QQQ"} | set(guards) | set(candidates)
                    if added := desired - watching:
                        subscribe(added)
                    if removed := watching - desired:
                        for stream in streams.values():
                            stream.unsubscribe_trades(*sorted(removed))
                        watching.difference_update(removed)
                for future in done:
                    future.result()
            finally:
                for stream in streams.values():
                    stream.stop()
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
        if not lane.paper_only or lane.live_execution_enabled:
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
            "paper_only": True,
            "live_execution_enabled": False,
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
        order = broker.submit_order(request)
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

"""Thin native NautilusTrader v2 24/5 session protection, 4-tier trailing & venue adapter."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Context, Decimal
from functools import lru_cache
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo

import polars as pl
from exchange_calendars.errors import DateOutOfBounds
from nautilus_trader import config as _nc
from nautilus_trader import indicators as _ni
from nautilus_trader import model as nm
from nautilus_trader.adapters import interactive_brokers as _ib
from nautilus_trader.adapters import sandbox as _sbx
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.execution import MakerTakerFeeModel
from nautilus_trader.live import LiveNode
from nautilus_trader.trading import Strategy, StrategyConfig

from tier0_sota import risk as _rk
from tier0_sota.discovery import (
    _XNYS,
    MarketRegimeActor,
    MarketRegimeActorConfig,
    assert_pre_wall_ns,
    df_to_equity_and_bars,
    step_rth_5m_bar,
)
from tier0_sota.risk import Side

__all__ = [
    "EXECUTION_VENUE_PROFILES",
    "EXIT_TAG",
    "FORBIDDEN_LIVE_IBKR_PORTS",
    "IBKR_OUTSIDE_RTH_TAG",
    "POOLED_TRAILING_SCHEDULES",
    "SESSION_PROTECTION_24X5",
    "SIP_23X5_CUTOVER_DATE",
    "SUPPORTED_TRAILING_OFFSET_TYPES",
    "VENUE_XNAS",
    "ExecutionVenueProfile",
    "SessionProtectionProfile",
    "SignalBacktestResult",
    "SignalExecutionConfig",
    "SignalExecutionStrategy",
    "build_ibkr_paper_connection_spec",
    "build_sandbox_live_node",
    "build_venue_live_node_config",
    "build_venue_risk_engine_config",
    "classify_et_hm",
    "classify_us_equity_session_et",
    "compute_collared_entry_limit_price",
    "compute_collared_limit_price",
    "compute_dynamic_trailing_stop",
    "compute_progressive_trailing_stop",
    "get_session_protection_profile",
    "inspect_venue_capability",
    "is_sip_dark_boats_overnight_window_et",
    "run_nautilus_signal_backtest",
    "select_pooled_trailing_schedule",
    "step_rth_5m_bar",
    "validate_trailing_offset_type",
    "validate_venue_order_capability",
]

VENUE_XNAS, _NY_TZ = nm.Venue("XNAS"), ZoneInfo("America/New_York")
FORBIDDEN_LIVE_IBKR_PORTS = frozenset({4001, 7496, 8798, 8796})
_ZERO, _ONE, _BPS_DIV = Decimal("0"), Decimal("1"), Decimal("10000")
_QL = LoggerConfig(stdout_level=LogLevel.OFF, bypass_logging=True)
IBKR_OUTSIDE_RTH_TAG, EXIT_TAG = 'IBOrderTags:{"outsideRth":true}', "ROLE=EXIT"
_TOT, _OT, _S = nm.TrailingOffsetType, nm.OrderType, nm.OrderStatus
_NV = (_S.PENDING_CANCEL, _S.INITIALIZED, _S.RELEASED)
SUPPORTED_TRAILING_OFFSET_TYPES = frozenset({_TOT.PRICE, _TOT.BASIS_POINTS})

MarketSession24x5 = Literal["PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT"]
SessionOrClosed = MarketSession24x5 | Literal["CLOSED"]
VenueProfileId = Literal[
    "IBKR_PAPER_4002",
    "ALPACA_PAPER_200",
    "IBKR_LIVE_3000",
    "ALPACA_LIVE_PRO_LINE_1000",
    "ALPACA_LIVE_ELITE_SMART_ROUTER",
]


@dataclass(frozen=True)
class ExecutionVenueProfile:
    venue_id: VenueProfileId
    broker: Literal["IBKR", "ALPACA"]
    environment: Literal["PAPER", "LIVE"]
    routing_tier: str
    max_rpm: int = 3000
    max_rps: int = 50
    max_concurrent_trailed_positions: int = 20
    supports_extended_overnight_24x5: bool = True
    supports_dma: bool = True
    supports_twap: bool = True
    supports_vwap: bool = True
    order_flow_classification: Literal["held", "not_held"] = "held"
    reg_nms_covered: bool = True
    min_exit_reserve_fraction: Decimal = _rk.MIN_EXIT_RATE_RESERVE_FRACTION
    live_execution_enabled: bool = False
    supports_native_trailing: bool = True


_IB = ExecutionVenueProfile("IBKR_PAPER_4002", "IBKR", "PAPER", "IBKR_SMART_24X5")
_AB = ExecutionVenueProfile(
    "ALPACA_PAPER_200",
    "ALPACA",
    "PAPER",
    "ALPACA_PAPER_STANDARD",
    200,
    10,
    3,
    False,
    False,
    False,
    False,
)
_AP = replace(
    _AB,
    venue_id="ALPACA_LIVE_PRO_LINE_1000",
    environment="LIVE",
    routing_tier="ALPACA_PRO_LINE_HELD",
    max_rpm=1000,
    max_rps=25,
    max_concurrent_trailed_positions=10,
)
_AE = replace(
    _AP,
    venue_id="ALPACA_LIVE_ELITE_SMART_ROUTER",
    routing_tier="ALPACA_ELITE_SMART_ROUTER_NOT_HELD",
    supports_dma=True,
    supports_twap=True,
    supports_vwap=True,
    order_flow_classification="not_held",
    reg_nms_covered=False,
)
_IL = replace(_IB, venue_id="IBKR_LIVE_3000", environment="LIVE")
EXECUTION_VENUE_PROFILES: dict[str, ExecutionVenueProfile] = {
    p.venue_id: p for p in (_IB, _AB, _IL, _AP, _AE)
}


def _resolve_venue(venue_id: str) -> ExecutionVenueProfile:
    key = str(venue_id).strip().upper()
    if (prof := EXECUTION_VENUE_PROFILES.get(key)) is None:
        raise ValueError(f"Unknown execution venue profile: {venue_id!r}")
    return prof


def _chk_n(v: Any, name: str, lo: int = 1, *, hi: int | None = None) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < lo or (hi is not None and v > hi):
        raise ValueError(f"{name} out of bounds [{lo}, {hi}]: {v!r}")
    return v


def _venue_risk_cfg(cfg_cls: type[Any], vp: str, rpm: int | None) -> Any:
    prof = _resolve_venue(vp)
    r_v = None if rpm is None else _chk_n(rpm, "rate_budget_max_rpm", hi=prof.max_rpm)
    rps = max(1, (r_v + 59) // 60) if r_v is not None and r_v != prof.max_rpm else prof.max_rps
    rate = f"{rps}/00:00:01"
    return cfg_cls(bypass=False, max_order_submit_rate=rate, max_order_modify_rate=rate)


def build_venue_risk_engine_config(
    venue_profile: str = "IBKR_PAPER_4002", *, rate_budget_max_rpm: int | None = None
) -> _nc.RiskEngineConfig:
    """Build native NautilusTrader v2 RiskEngineConfig wired to venue rate limits."""
    return _venue_risk_cfg(_nc.RiskEngineConfig, venue_profile, rate_budget_max_rpm)


def build_venue_live_node_config(
    venue_profile: str = "IBKR_PAPER_4002", *, rate_budget_max_rpm: int | None = None
) -> _nc.LiveNodeConfig:
    """Build native NautilusTrader v2 LiveNodeConfig + LiveRiskEngineConfig (SANDBOX)."""
    re = _venue_risk_cfg(_nc.LiveRiskEngineConfig, venue_profile, rate_budget_max_rpm)
    return _nc.LiveNodeConfig(environment=Environment.SANDBOX, risk_engine=re)


def inspect_venue_capability(venue_id: str, **o: Any) -> dict[str, Any]:
    """Evaluate venue routing, algo, RPM headroom & fail-closed live posture."""
    p = _resolve_venue(venue_id)
    algo, rr = o.get("algo", "STOP_LIMIT"), o.get("requested_rpm")
    req24 = bool(o.get("require_24x5_overnight", False))
    res_rpm = _rk.ProtectiveExitRateBudget(p.max_rpm, p.min_exit_reserve_fraction).exit_reserve_rpm
    caps = {"DMA": p.supports_dma, "TWAP": p.supports_twap, "VWAP": p.supports_vwap}
    rsn: list[str] = []
    if rr is not None and not (0 < rr <= p.max_rpm):
        rsn.append(f"requested_rpm={rr} outside (0, {p.max_rpm}]")
    if algo in caps and not caps[algo]:
        rsn.append(f"algo={algo} unsupported on {p.routing_tier}")
    if algo == "ADAPTIVE" and p.broker != "IBKR":
        rsn.append(f"algo={algo} restricted to IBKR routing")
    if req24 and not p.supports_extended_overnight_24x5:
        rsn.append(f"24/5 overnight session unsupported on {p.routing_tier}")
    cap_ok, exec_now = not rsn, p.environment == "PAPER" and not p.live_execution_enabled
    if not exec_now:
        rsn.append("LIVE execution hard-refused (live_execution_enabled=False)")
    return {
        "venue_id": p.venue_id,
        "broker": p.broker,
        "environment": p.environment,
        "routing_tier": p.routing_tier,
        "max_rpm": p.max_rpm,
        "max_rps": p.max_rps,
        "live_execution_enabled": p.live_execution_enabled,
        "supports_extended_overnight_24x5": p.supports_extended_overnight_24x5,
        "allowed": cap_ok and exec_now,
        "capability_ok": cap_ok,
        "executable_now": exec_now,
        "reasons": rsn,
        "exit_reserve_rpm": res_rpm,
        "active_entry_rpm": p.max_rpm - res_rpm,
    }


def validate_venue_order_capability(venue_id: str, **o: Any) -> ExecutionVenueProfile:
    """Enforce venue capabilities and fail closed on LIVE execution profiles."""
    st = inspect_venue_capability(venue_id, **o)
    if not st["capability_ok"]:
        raise ValueError("; ".join(st["reasons"]))
    prof = _resolve_venue(venue_id)
    if prof.environment == "LIVE" or not st["executable_now"]:
        raise PermissionError(f"Refused live profile {venue_id!r}: live_execution_enabled=False")
    return prof


def validate_trailing_offset_type(offset_type: nm.TrailingOffsetType) -> nm.TrailingOffsetType:
    """Validate IBKR-compatible TrailingOffsetType (PRICE or BASIS_POINTS only)."""
    if not isinstance(offset_type, _TOT) or offset_type not in SUPPORTED_TRAILING_OFFSET_TYPES:
        raise ValueError(f"Unsupported TrailingOffsetType {offset_type!r}; use PRICE/BASIS_POINTS")
    return offset_type


def build_ibkr_paper_connection_spec(
    *, host: str = "127.0.0.1", port: int = 4002, client_id: int = 101
) -> dict[str, Any]:
    """Paper-only IBKR Gateway :4002/:7497 connection spec backed by native IBKR configs."""
    if port in FORBIDDEN_LIVE_IBKR_PORTS or port not in (4002, 7497):
        raise ValueError(f"Refused IBKR port {port}; Paper permits :4002 / :7497 only")
    dkw: dict[str, Any] = {"host": host, "port": port, "client_id": client_id}
    dc = _ib.InteractiveBrokersDataClientConfig(**dkw, use_regular_trading_hours=False)
    ec = _ib.InteractiveBrokersExecutionClientConfig(**dkw)
    return dkw | {
        "adapter": "nautilus_trader.interactive_brokers",
        "paper_only": True,
        "live_execution_enabled": False,
        "data_client_config": type(dc).__name__,
        "exec_client_config": type(ec).__name__,
    }


def build_sandbox_live_node(
    *,
    venue_profile: str = "IBKR_PAPER_4002",
    starting_cash: Decimal = Decimal("100000"),
    rate_budget_max_rpm: int | None = None,
) -> LiveNode:
    """Instantiate real NautilusTrader v2 LiveNode in Environment.SANDBOX."""
    validate_venue_order_capability(venue_profile)
    usd = nm.Currency.from_str("USD")
    sbx = _sbx.SandboxExecutionClientConfig(
        nm.Venue("XNAS"),
        [nm.Money(float(starting_cash), usd)],
        oms_type=nm.OmsType.NETTING,
        account_type=nm.AccountType.MARGIN,
        fee_model=MakerTakerFeeModel(Decimal("0.0004"), Decimal("0.0004")),
        support_contingent_orders=True,
        use_reduce_only=False,
    )
    lcfg = build_venue_live_node_config(venue_profile, rate_budget_max_rpm=rate_budget_max_rpm)
    b = LiveNode.builder("tier0-sandbox", nm.TraderId("TIER0-SANDBOX-001"), Environment.SANDBOX)
    b = b.with_risk_engine_config(lcfg.risk_engine).with_logging(_QL)
    return b.add_simulated_exec_client("XNAS", _sbx.SandboxExecutionClientFactory(), sbx).build()


@dataclass(frozen=True)
class SessionProtectionProfile:
    session: SessionOrClosed
    order_mode: Literal["STOP_MARKET", "STOP_LIMIT"]
    initial_max_loss_pct: Decimal
    breakeven_trigger_pct: Decimal
    min_trail_pct: Decimal
    natr_trail_mult: Decimal
    limit_collar_pct: Decimal
    max_notional_scale: Decimal
    outside_rth: bool

    @property
    def limit_collar_bps(self) -> Decimal:
        """Session collar width expressed in basis points (35 Pre/After, 50 Overnight)."""
        return self.limit_collar_pct * _BPS_DIV


def _sp(
    s: SessionOrClosed, m: Literal["STOP_MARKET", "STOP_LIMIT"], v: str, o: bool
) -> SessionProtectionProfile:
    return SessionProtectionProfile(s, m, *(Decimal(x) for x in v.split()), o)


SESSION_PROTECTION_24X5: dict[MarketSession24x5, SessionProtectionProfile] = {
    "RTH": _sp("RTH", "STOP_MARKET", "0.0200 0.0040 0.0035 1.25 0.0000 1.00", False),
    "PRE_MARKET": _sp("PRE_MARKET", "STOP_LIMIT", "0.0150 0.0045 0.0050 1.50 0.0035 0.60", True),
    "AFTER_HOURS": _sp("AFTER_HOURS", "STOP_LIMIT", "0.0150 0.0045 0.0050 1.50 0.0035 0.60", True),
    "OVERNIGHT": _sp("OVERNIGHT", "STOP_LIMIT", "0.0100 0.0030 0.0060 1.75 0.0050 0.35", True),
}
_CLOSED_PROFILE = _sp("CLOSED", "STOP_LIMIT", "0.0100 0.0030 0.0060 1.75 0.0050 0.00", True)
_SESSION_ALIASES = {"POST_MARKET": "AFTER_HOURS", "OVERNIGHT_BOATS": "OVERNIGHT"}
_K4 = ("initial_max_loss_pct", "breakeven_trigger_pct", "min_trail_pct", "natr_trail_mult")


def _mk_sched(spec: str) -> dict[MarketSession24x5, SessionProtectionProfile]:
    p, b = spec.split("|"), SESSION_PROTECTION_24X5
    vals = (p[0], p[1], p[1], p[2])
    return {
        s: replace(b[s], **dict(zip(_K4, map(Decimal, v.split()), strict=True)))
        for s, v in zip(b, vals, strict=True)
    }


POOLED_TRAILING_SCHEDULES: dict[str, dict[MarketSession24x5, SessionProtectionProfile]] = {
    "baseline": SESSION_PROTECTION_24X5,
    "tight_near_lossless": _mk_sched(
        "0.0150 0.0025 0.0025 1.00|0.0125 0.0030 0.0035 1.25|0.0100 0.0025 0.0050 1.50"
    ),
    "wide_natr_adaptive": _mk_sched(
        "0.0200 0.0060 0.0050 1.50|0.0150 0.0055 0.0050 1.50|0.0100 0.0040 0.0060 1.75"
    ),
}


def select_pooled_trailing_schedule(
    natr_pct: Decimal,
) -> tuple[str, dict[MarketSession24x5, SessionProtectionProfile]]:
    """Select causal NATR-bucketed trailing schedule strictly <= 2.00% initial stop."""
    v = max(_ZERO, Decimal(str(natr_pct)))
    key = (
        "tight_near_lossless"
        if v <= Decimal("0.015")
        else ("baseline" if v <= Decimal("0.030") else "wide_natr_adaptive")
    )
    return key, POOLED_TRAILING_SCHEDULES[key]


def get_session_protection_profile(
    session: SessionOrClosed | str,
    profiles: dict[MarketSession24x5, SessionProtectionProfile] | None = None,
) -> SessionProtectionProfile:
    """Resolve 4-key 24/5 profile or fail-closed _CLOSED_PROFILE."""
    amap = profiles if profiles is not None else SESSION_PROTECTION_24X5
    key = _SESSION_ALIASES.get(str(session).strip().upper(), str(session).strip().upper())
    if key in ("CLOSED", "WEEKEND_CLOSED", "DAILY_PAUSE_CLOSED"):
        return _CLOSED_PROFILE
    if key in amap:
        return amap[key]  # type: ignore[index]
    raise ValueError(f"Unknown 24/5 market session: {session!r}")


def _et_dt(ts_utc_ns: int) -> datetime:
    if isinstance(ts_utc_ns, bool) or not isinstance(ts_utc_ns, int) or ts_utc_ns < 0:
        raise ValueError(f"ts_utc_ns must be non-negative int, got {ts_utc_ns!r}")
    sec, rem = divmod(ts_utc_ns, 1_000_000_000)
    return datetime.fromtimestamp(sec, tz=UTC).replace(microsecond=rem // 1000).astimezone(_NY_TZ)


@lru_cache(maxsize=2048)
def _xnys_info(ny_day: date) -> tuple[bool, int] | None:
    try:
        if not (_XNYS.first_session.date() <= ny_day <= _XNYS.last_session.date()):
            return None
        if not _XNYS.is_session(iso := ny_day.isoformat()):
            return (False, 960)
        c = _XNYS.session_close(iso).to_pydatetime().astimezone(_NY_TZ)
        return (True, c.hour * 60 + c.minute)
    except DateOutOfBounds:
        return None


def classify_et_hm(hm: int, rth_close_hm: int = 960) -> MarketSession24x5:
    """Classify ET minute-of-day into a US equity 24/5 session label."""
    if 240 <= hm < 570:
        return "PRE_MARKET"
    if 570 <= hm < rth_close_hm:
        return "RTH"
    if rth_close_hm <= hm < 1200:
        return "AFTER_HOURS"
    return "OVERNIGHT"


def classify_us_equity_session_et(ts_utc_ns: int) -> SessionOrClosed:
    """Classify UTC ns timestamp into US 24/5 ET session or CLOSED via XNYS calendar."""
    dt_et = _et_dt(ts_utc_ns)
    ny_day, hm = dt_et.date(), dt_et.hour * 60 + dt_et.minute
    is_open = lambda d: d.weekday() < 5 if (inf := _xnys_info(d)) is None else inf[0]  # noqa: E731
    if hm >= 1200:
        return "OVERNIGHT" if is_open(ny_day + timedelta(days=1)) else "CLOSED"
    if not is_open(ny_day):
        return "CLOSED"
    inf = _xnys_info(ny_day)
    return classify_et_hm(hm, inf[1] if inf else 960)


SIP_23X5_CUTOVER_DATE: date = date(2026, 12, 6)


def is_sip_dark_boats_overnight_window_et(ts_utc_ns: int) -> bool:
    """True iff post-SIP-23x5 cutover, 20:00-21:00 ET, and an OVERNIGHT-classified trading night."""
    dt_et = _et_dt(ts_utc_ns)
    hm = dt_et.hour * 60 + dt_et.minute
    return (
        dt_et.date() >= SIP_23X5_CUTOVER_DATE
        and 1200 <= hm < 1260
        and classify_us_equity_session_et(ts_utc_ns) == "OVERNIGHT"
    )


def compute_collared_limit_price(
    trigger_price: Decimal, side: Side, collar_pct: Decimal
) -> Decimal:
    """Compute a Rule-612 collared limit price for extended/overnight STOP_LIMIT exits."""
    trig, collar = Decimal(str(trigger_price)), Decimal(str(collar_pct))
    if (
        not trig.is_finite()
        or trig <= _ZERO
        or not collar.is_finite()
        or not (_ZERO <= collar < Decimal("0.20"))
    ):
        raise ValueError(f"Invalid trigger_price={trig!r} or collar_pct={collar!r}")
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"Unsupported side: {side!r}")
    px = max(_rk.TICK_SUB_DOLLAR, trig * (_ONE - collar if side == "LONG" else _ONE + collar))
    return _rk.quantize_rule_612(px, side=side, is_stop=False)


def compute_collared_entry_limit_price(
    ref_price: Decimal, side: Side, collar_pct: Decimal
) -> Decimal:
    """Compute a Rule-612 collared LIMIT price for outside-RTH entries.

    LONG entries buy up to `ref_price * (1 + collar_pct)` rounded conservatively down;
    SHORT entries sell down to `ref_price * (1 - collar_pct)` rounded conservatively up.
    """
    ref, collar = Decimal(str(ref_price)), Decimal(str(collar_pct))
    if (
        not ref.is_finite()
        or ref <= _ZERO
        or not collar.is_finite()
        or not (_ZERO <= collar < Decimal("0.20"))
    ):
        raise ValueError(f"Invalid ref_price={ref!r} or collar_pct={collar!r}")
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"Unsupported side: {side!r}")
    px = max(_rk.TICK_SUB_DOLLAR, ref * (_ONE + collar if side == "LONG" else _ONE - collar))
    return _rk.quantize_rule_612(px, side=side, is_stop=False)


def _hard_stop(
    entry: Decimal, side: Side, prof: SessionProtectionProfile
) -> tuple[Decimal, Decimal]:
    cap = min(prof.initial_max_loss_pct, _rk.MAX_STOP_LOSS_PCT)
    return cap, _rk.compute_capped_stop_price(entry, side, max_loss_pct=cap)


def compute_progressive_trailing_stop(
    *,
    entry_price: Decimal,
    side: Side,
    favorable_extreme: Decimal,
    current_stop: Decimal,
    profile: SessionProtectionProfile | None = None,
    session: SessionOrClosed | None = None,
    cost_bps_per_side: Decimal = Decimal("4.0"),
    natr_pct: Decimal = _ZERO,
    initial_stop: Decimal | None = None,
) -> tuple[Decimal, int, Decimal]:
    """4-tier progressive lossless trailing (Tier 1 BE+fee, Tier 2 +1R, Tier 3 +2R, Tier 4 +3R)."""
    prof = profile or (get_session_protection_profile(session) if session is not None else None)
    if prof is None or side not in ("LONG", "SHORT"):
        raise ValueError(f"Invalid profile={prof!r} or side={side!r}")
    entry, fav, cur = (Decimal(str(v)) for v in (entry_price, favorable_extreme, current_stop))
    cost_bps = max(_ZERO, Decimal(str(cost_bps_per_side)))
    natr = max(_ZERO, Decimal(str(natr_pct)))
    if not all(v.is_finite() and v > _ZERO for v in (entry, fav, cur)):
        raise ValueError("entry_price, favorable_extreme, current_stop must be > 0")
    cap, hard = _hard_stop(entry, side, prof)
    is_l, better = side == "LONG", (max if side == "LONG" else min)
    behind = (lambda a, b: a < b) if is_l else (lambda a, b: a > b)
    q_stop = lambda p: _rk.quantize_rule_612(p, side=side, is_stop=True)  # noqa: E731
    fav_, cand = better(entry, fav), better(hard, q_stop(cur))
    base_trail = min(cap, max(prof.min_trail_pct, natr * prof.natr_trail_mult))
    eff_trail, tier = base_trail, 0
    fav_gain = (fav_ - entry) if is_l else (entry - fav_)
    if fav_gain / entry >= prof.breakeven_trigger_pct:
        tier = 1
        fee_buf = (cost_bps * Decimal("2") + Decimal("2")) / _BPS_DIV
        raw_be = max(_rk.TICK_SUB_DOLLAR, entry * (_ONE + fee_buf if is_l else _ONE - fee_buf))
        if behind(raw_be, fav_) and behind(be_s := q_stop(raw_be), fav_):
            cand = better(cand, be_s)
        init_s = Decimal(str(initial_stop)) if initial_stop else hard
        leg_risk = max(_rk.tick_size_for(entry), abs(entry - init_s))
        r_mult = fav_gain / leg_risk
        for r_thr, lock_k, mult, flr in (
            (Decimal("3"), Decimal("2.35"), Decimal("0.25"), Decimal("0.0010")),
            (Decimal("2"), Decimal("1.35"), Decimal("0.40"), Decimal("0.0012")),
            (Decimal("1"), Decimal("0.45"), Decimal("0.65"), Decimal("0.0015")),
        ):
            if r_mult >= r_thr:
                tier = int(r_thr) + 1
                eff_trail = min(base_trail, max(flr, base_trail * mult))
                lk = entry + lock_k * leg_risk if is_l else entry - lock_k * leg_risk
                if behind(lk_s := q_stop(max(_rk.TICK_SUB_DOLLAR, lk)), fav_):
                    cand = better(cand, lk_s)
                break
        tr_raw = max(_rk.TICK_SUB_DOLLAR, fav_ * (_ONE - eff_trail if is_l else _ONE + eff_trail))
        if behind(tr_s := q_stop(tr_raw), fav_):
            cand = better(cand, tr_s)
    return better(cur if not behind(cur, hard) else hard, cand), tier, eff_trail


def compute_dynamic_trailing_stop(**kw: Any) -> Decimal:
    """Monotonic Rule-612 trailing stop returning the new stop price."""
    return compute_progressive_trailing_stop(**kw)[0]


def _to_increment(px: Decimal, inc: Decimal, *, up: bool) -> Decimal:
    ctx = Context(prec=99)
    steps = ctx.divide(px, inc).to_integral_value(ROUND_CEILING if up else ROUND_FLOOR, ctx)
    return ctx.multiply(steps, inc).quantize(inc, context=ctx)


@dataclass(frozen=True)
class SignalBacktestResult:
    bars_loaded: int
    bars_processed: int
    orders_total: int
    fills_total: int
    stops_attached: int
    total_return_pct: float
    trade_returns: tuple[float, ...]
    trailing_ratchets: int = 0
    session_transitions: int = 0
    halt_triggered: bool = False
    bars_5m_processed: int = 0
    auto_reverses: int = 0
    auto_reentries: int = 0
    max_lock_tier: int = 0
    outside_rth_limit_entries: int = 0
    outside_rth_market_entries: int = 0
    rth_market_entries: int = 0


class SignalExecutionConfig(StrategyConfig):
    def __new__(cls, *_: Any, strategy_id: nm.StrategyId | None = None, **kw: Any) -> Self:
        d: dict[str, Any] = {"strategy_id": strategy_id or nm.StrategyId("TIER0-SIGNAL-EXEC")}
        if (t := kw.get("order_id_tag")) is not None:
            d["order_id_tag"] = t
        return super().__new__(cls, **d)

    def __init__(
        self,
        bar_type: nm.BarType,
        signals_by_ts: dict[int, int] | None = None,
        start_equity: Decimal = Decimal("100000"),
        price_precision: int = 2,
        cost_bps_per_side: Decimal = Decimal("4.0"),
        enable_24x5_trailing: bool = True,
        strategy_id: nm.StrategyId | None = None,
        session_protection_24x5: dict[MarketSession24x5, SessionProtectionProfile] | None = None,
        market_bar_types: tuple[nm.BarType, ...] = (),
        rate_budget_max_rpm: int | None = None,
        venue_profile: str = "IBKR_PAPER_4002",
        enable_native_5m_bars: bool = True,
        order_id_tag: str | None = None,
        signal_name: str | None = None,
        semi_auto_confirmed: bool = True,
        shortable: bool = True,
        auto_reverse: bool = True,
        auto_reentry: bool = True,
        reentry_cooldown_secs: int = 60,
        max_reentries: int | None = None,
        fixed_qty: int | None = None,
    ) -> None:
        del strategy_id, order_id_tag
        super().__init__()
        self.bar_type, self.signals_by_ts, self.signal_name = bar_type, signals_by_ts, signal_name
        self.start_equity = start_equity
        self.price_precision = _chk_n(price_precision, "price_precision", 0)
        self.cost_bps_per_side = cost_bps_per_side
        self.enable_24x5_trailing = bool(enable_24x5_trailing)
        self.session_protection_24x5 = session_protection_24x5
        self.market_bar_types = market_bar_types
        self.rate_budget_max_rpm, self.venue_profile = rate_budget_max_rpm, venue_profile
        self.enable_native_5m_bars = bool(enable_native_5m_bars)
        self.semi_auto_confirmed, self.shortable = bool(semi_auto_confirmed), bool(shortable)
        self.auto_reverse, self.auto_reentry = bool(auto_reverse), bool(auto_reentry)
        self.reentry_cooldown_secs = _chk_n(reentry_cooldown_secs, "reentry_cooldown_secs", 0)
        self.max_reentries = (
            None if max_reentries is None else _chk_n(max_reentries, "max_reentries", 0)
        )
        self.fixed_qty = None if fixed_qty is None else _chk_n(fixed_qty, "fixed_qty", 1)


class SignalExecutionStrategy(Strategy):
    """Native NautilusTrader v2 Strategy with 24/5 protection & 4-tier progressive trailing."""

    def __init__(self, config: SignalExecutionConfig) -> None:
        super().__init__(config)
        c = self.cfg = config
        self.bar_type, self.signals_by_ts = c.bar_type, c.signals_by_ts
        self.price_precision = c.price_precision
        self.price_increment = Decimal(1).scaleb(-self.price_precision)
        self.session_protection = dict(c.session_protection_24x5 or SESSION_PROTECTION_24X5)
        vsp = self.venue_spec = validate_venue_order_capability(c.venue_profile)
        rpm = (
            vsp.max_rpm
            if c.rate_budget_max_rpm is None
            else _chk_n(c.rate_budget_max_rpm, "rpm", hi=vsp.max_rpm)
        )
        self.rate_budget = _rk.ProtectiveExitRateBudget(rpm, vsp.min_exit_reserve_fraction)
        self.breaker = _rk.DailyDrawdownBreaker(start_equity=c.start_equity)
        self._usd, self._mkt_regime, self._rth5 = nm.Currency.from_str("USD"), 0, {}
        self._atr = _ni.AverageTrueRange(14, _ni.MovingAverageType.Wilder, use_previous=True)
        self._vwap = _ni.VolumeWeightedAveragePrice()
        self.bars_processed = self.bars_5m_processed = self.signals_received = 0
        self.stops_attached = self.trailing_ratchets = self.session_transitions = 0
        self.auto_reverses = self.auto_reentries = self.max_lock_tier = 0
        self.outside_rth_limit_entries = self.rth_market_entries = 0
        self._reentry_count, self._reversing, self._last_closed_ts_ns = 0, False, -1
        self._halt_triggered = self._closing = self._replacing_stop = self._deferred_guard = False
        self._stop_session: SessionOrClosed | None = None
        self._pending_side: Side | None = None
        self._last_closed_side: Side | None = None
        self._last_close_px: Decimal | None = None
        self._fav_ext: Decimal | None = None
        self._init_stop_px: Decimal | None = None
        self._session_date: date | None = None
        self._last_ts_ns, self._day_start_rpnl, self._trade_returns = 0, _ZERO, []

    def _fmt(self, px: Decimal) -> nm.Price:
        return nm.Price.from_str(f"{px:.{self.price_precision}f}")

    def _sess_prof(self, ts_ns: int) -> tuple[SessionOrClosed, SessionProtectionProfile]:
        s = classify_us_equity_session_et(ts_ns) if self.cfg.enable_24x5_trailing else "RTH"
        return s, get_session_protection_profile(s, self.session_protection)

    def _open_pos(self, iid: nm.InstrumentId) -> Any:
        return next(iter(self.cache.positions_open(instrument_id=iid)), None)

    def _active_orders(self, iid: nm.InstrumentId) -> list[Any]:
        return [o for o in self.cache.orders(None, iid, self.strategy_id) if not o.is_closed]

    def _active_stops(self, iid: nm.InstrumentId, *, skip: Any = None, nv: Any = _NV) -> list[Any]:
        return [
            o
            for o in self._active_orders(iid)
            if EXIT_TAG in (o.tags or ()) and o.status not in nv and o.client_order_id != skip
        ]

    def _dec(self, m: Any) -> Decimal:
        return Decimal(str(m.as_double())) if m is not None else _ZERO

    def _equity(self) -> Decimal:
        a = self.cache.account_for_venue(self.bar_type.instrument_id.venue)
        b = a.balance_total(self._usd) if a is not None else None
        return self._dec(b) if b is not None else self.cfg.start_equity

    def _cancel_all_quiet(self, iid: nm.InstrumentId) -> None:
        self._replacing_stop = True
        try:
            self.cancel_all_orders(iid)
        finally:
            self._replacing_stop = False

    def _flatten(self, iid: nm.InstrumentId, sess: SessionOrClosed, ack: Any = None) -> None:
        xs = [o for o in (*self._active_orders(iid), ack) if o and EXIT_TAG in (o.tags or ())]
        mx = [o for o in xs if o.order_type == _OT.MARKET]
        if not mx and ack is None:
            self._closing, self._pending_side, self._reversing = True, None, False
            self._cancel_all_quiet(iid)
        act = bool(self._active_orders(iid))
        self._deferred_guard = act or any(o is ack for o in mx)
        self._closing = act or not self.portfolio.is_net_flat(iid)
        if not (self._deferred_guard or self.portfolio.is_net_flat(iid)):
            out = get_session_protection_profile(sess, self.session_protection).outside_rth
            self.rate_budget.try_consume(float(self._last_ts_ns) / 1e9, is_exit=True)
            tags = [f"SESSION={sess}", *([IBKR_OUTSIDE_RTH_TAG] if out else []), EXIT_TAG]
            self.close_all_positions(iid, tags=tags, reduce_only=False)

    def _sync_session(self, ts_ns: int, iid: nm.InstrumentId, *, delta: Decimal = _ZERO) -> None:
        dt = _et_dt(ts_ns)
        sday = dt.date() + timedelta(days=1) if dt.hour >= 20 else dt.date()
        if self._session_date == sday:
            return
        self._vwap.reset()
        self._rth5.clear()
        if self._session_date is not None:
            rp, eq = self._dec(self.portfolio.realized_pnl(iid)) - delta, self._equity() - delta
            self._day_start_rpnl = rp
            self.breaker = _rk.DailyDrawdownBreaker(max(Decimal("0.01"), eq))
            self._halt_triggered = eq <= _ZERO
            self._last_closed_side, self._reentry_count = None, 0
        self._session_date = sday

    def _submit_entry(self, iid: nm.InstrumentId, side: Side, ref: Decimal, ts_ns: int) -> bool:
        if not self.cfg.semi_auto_confirmed:
            return False
        sess, prof = self._sess_prof(ts_ns)
        short_ban = not self.cfg.shortable or (
            self.venue_spec.broker == "ALPACA" and sess == "OVERNIGHT"
        )
        if sess == "CLOSED" or (side == "SHORT" and short_ban):
            return False
        if self.cfg.fixed_qty is None:
            _, spx = _hard_stop(ref, side, prof)
            qty = _rk.size_position_by_risk(
                equity=max(Decimal("0.01"), self._equity()),
                entry_price=ref,
                stop_price=spx,
                max_notional_pct=Decimal("0.20") * prof.max_notional_scale,
            )
        else:
            qty = self.cfg.fixed_qty
        if qty <= 0 or not self.rate_budget.try_consume(float(ts_ns) / 1e9, is_exit=False):
            return False
        self._pending_side = None
        os = nm.OrderSide.BUY if side == "LONG" else nm.OrderSide.SELL
        tif = nm.TimeInForce.GTC if prof.outside_rth else nm.TimeInForce.DAY
        tags = [f"SESSION={sess}", *([IBKR_OUTSIDE_RTH_TAG] if prof.outside_rth else [])]
        if prof.outside_rth:
            lim_raw = compute_collared_entry_limit_price(ref, side, prof.limit_collar_pct)
            lim_px = _to_increment(lim_raw, self.price_increment, up=(side == "SHORT"))
            ord_ = self.order_factory.limit(
                iid,
                os,
                nm.Quantity.from_int(qty),
                self._fmt(lim_px),
                time_in_force=tif,
                tags=tags,
            )
            self.outside_rth_limit_entries += 1
        else:
            ord_ = self.order_factory.market(
                iid, os, nm.Quantity.from_int(qty), time_in_force=tif, tags=tags
            )
            self.rth_market_entries += 1
        self.submit_order(ord_)
        return True

    def _replace_stop(self, pos: Any, sess: SessionOrClosed, ts_ns: int, *, trailing: bool) -> None:
        if self._closing:
            return
        iid, is_l, entry = pos.instrument_id, pos.is_long, Decimal(str(pos.avg_px_open))
        side: Side = "LONG" if is_l else "SHORT"
        prof = get_session_protection_profile(sess, self.session_protection)
        old = self._active_stops(iid, nv=_NV[:1])
        prev = next(
            (Decimal(str(o.trigger_price)) for o in old if o.trigger_price is not None), None
        )
        _, hard = _hard_stop(entry, side, prof)
        spx = (max(hard, prev) if is_l else min(hard, prev)) if prev is not None else hard
        ref_px = self._last_close_px or entry
        atr_dec = Decimal(str(self._atr.value)) if self._atr.initialized else _ZERO
        natr = max(_ZERO, atr_dec / ref_px)
        if trailing and self._fav_ext is not None:
            spx, tier, _ = compute_progressive_trailing_stop(
                entry_price=entry,
                side=side,
                favorable_extreme=self._fav_ext,
                current_stop=spx,
                profile=prof,
                cost_bps_per_side=self.cfg.cost_bps_per_side,
                natr_pct=natr,
                initial_stop=self._init_stop_px,
            )
            self.max_lock_tier = max(self.max_lock_tier, tier)
        spx = _to_increment(spx, self.price_increment, up=is_l)
        if (ref := self._last_close_px) is not None and ((spx >= ref) if is_l else (spx <= ref)):
            self._flatten(iid, sess)
            return
        is_lim = prof.order_mode == "STOP_LIMIT"
        lim = (
            _to_increment(
                compute_collared_limit_price(spx, side, prof.limit_collar_pct),
                self.price_increment,
                up=not is_l,
            )
            if is_lim
            else None
        )
        ot = _OT.STOP_LIMIT if is_lim else _OT.STOP_MARKET
        if (
            len(old) == 1
            and self._stop_session in (None, sess)
            and old[0].order_type == ot
            and old[0].status not in _NV
        ):
            o, tp, lp = old[0], self._fmt(spx), (self._fmt(lim) if lim is not None else None)
            if o.quantity != pos.quantity or tp != o.trigger_price or (is_lim and lp != o.price):
                if not self.rate_budget.try_consume(float(ts_ns) / 1e9, is_exit=True):
                    self._halt_triggered = True
                    self._flatten(iid, sess)
                    return
                self.trailing_ratchets += 1
                self.modify_order(
                    o.client_order_id, quantity=pos.quantity, price=lp, trigger_price=tp
                )
            self._stop_session = sess
            return
        self._replacing_stop = True
        try:
            for o in old:
                self.cancel_order(o.client_order_id)
            if not self.rate_budget.try_consume(float(ts_ns) / 1e9, is_exit=True):
                self._halt_triggered = True
                self._flatten(iid, sess)
                return
            out_tag = [IBKR_OUTSIDE_RTH_TAG] if prof.outside_rth else []
            tags = [f"SESSION={sess}", *out_tag, EXIT_TAG]
            kw: dict[str, Any] = {
                "instrument_id": iid,
                "order_side": nm.OrderSide.SELL if is_l else nm.OrderSide.BUY,
                "quantity": pos.quantity,
                "trigger_price": self._fmt(spx),
                "trigger_type": nm.TriggerType.LAST_PRICE,
                "time_in_force": nm.TimeInForce.GTC,
                "tags": tags,
            }
            if is_lim and lim is not None:
                kw["price"] = self._fmt(lim)
            self.stops_attached += 1
            self._stop_session = sess
            fn = self.order_factory.stop_limit if is_lim else self.order_factory.stop_market
            self.submit_order(fn(**kw))
        finally:
            self._replacing_stop = False

    def _apply_signal(
        self,
        iid: nm.InstrumentId,
        sig: int | None,
        cl: Decimal,
        ts: int,
        sess: SessionOrClosed,
        pos: Any,
    ) -> bool:
        if sig is None:
            return False
        des: Side | None = "LONG" if sig > 0 else ("SHORT" if sig < 0 else None)
        cur: Side | None = ("LONG" if pos.is_long else "SHORT") if pos else None
        if cur is not None and des != cur:
            self._flatten(iid, sess)
            if des is not None and self.cfg.auto_reverse and self.cfg.semi_auto_confirmed:
                self._pending_side, self._reversing = des, True
            return True
        can_enter = cur is None and not (self._closing or self._halt_triggered)
        if can_enter and des and self.breaker.allow_entry():
            if same := (self._last_closed_side == des):
                cd_ns = self.cfg.reentry_cooldown_secs * 1_000_000_000
                no_cd = self._last_closed_ts_ns >= 0 and ts - self._last_closed_ts_ns < cd_ns
                mx = self.cfg.max_reentries
                at_max = mx is not None and self._reentry_count >= mx
                if not self.cfg.auto_reentry or no_cd or at_max:
                    return False
            if self._submit_entry(iid, des, cl, ts):
                if same:
                    self.auto_reentries += 1
                    self._reentry_count += 1
                self._last_closed_side = None
                return True
        return False

    def on_start(self) -> None:
        if (inst := self.cache.instrument(self.bar_type.instrument_id)) is not None:
            self.price_precision = int(inst.price_precision)
            self.price_increment = Decimal(str(inst.price_increment))
        self.register_indicator_for_bars(self.bar_type, self._atr)
        self.register_indicator_for_bars(self.bar_type, self._vwap)
        self.subscribe_bars(self.bar_type)
        if self.cfg.signal_name:
            self.subscribe_signal(self.cfg.signal_name)
        if self.cfg.market_bar_types:
            self.subscribe_signal("market_regime")
        if self.cfg.enable_native_5m_bars:
            b5 = f"{self.bar_type.instrument_id}-5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL"
            self.subscribe_bars(nm.BarType.from_str(b5))

    def on_signal(self, signal: Any) -> None:
        if getattr(signal, "name", "") == "market_regime":
            self._mkt_regime = int(signal.value)
            return
        self.signals_received += 1
        smap = self.signals_by_ts if self.signals_by_ts is not None else {}
        smap[int(signal.ts_event)] = int(signal.value)
        self.signals_by_ts = smap

    def on_bar(self, bar: nm.Bar) -> None:
        bt = bar.bar_type
        is_5m = int(bt.spec.step) == 5 and bt.spec.aggregation == nm.BarAggregation.MINUTE
        if is_5m and bt.instrument_id == self.bar_type.instrument_id:
            self.bars_5m_processed += 1
            dt = _et_dt(int(bar.ts_event))
            vw = float(self._vwap.value) if self._vwap.initialized else float(bar.close)
            cs = (float(x) for x in (bar.open, bar.high, bar.low, bar.close))
            step_rth_5m_bar(self._rth5, dt.date(), dt.hour * 60 + dt.minute, *cs, vw)
            return
        if bt != self.bar_type:
            return
        self.bars_processed += 1
        iid, ts, cl = bt.instrument_id, int(bar.ts_event), Decimal(str(bar.close))
        self._last_ts_ns, self._last_close_px = ts, cl
        self._sync_session(ts, iid)
        sess, prof = self._sess_prof(ts)
        pos, stops = self._open_pos(iid), self._active_stops(iid)
        rpnl = self._dec(self.portfolio.realized_pnl(iid)) - self._day_start_rpnl
        raw_u = self.portfolio.unrealized_pnl(iid) or (
            pos.unrealized_pnl(bar.close) if pos else None
        )
        ok_b = self.breaker.update(realized_pnl=rpnl, unrealized_pnl=self._dec(raw_u))
        if not ok_b or self._halt_triggered:
            self._halt_triggered = True
            if pos or self._active_orders(iid):
                self._flatten(iid, sess)
            return
        dg, self._deferred_guard = self._deferred_guard, False
        if self._closing or (pos and dg and not self._active_stops(iid, nv=_NV[:1])):
            self._flatten(iid, sess)
            return
        sig = self.signals_by_ts.get(ts) if self.signals_by_ts is not None else None
        did = self._apply_signal(iid, sig, cl, ts, sess, pos)
        if did or pos is None or not self.cfg.enable_24x5_trailing:
            return
        is_l, entry = pos.is_long, Decimal(str(pos.avg_px_open))
        ex_px = Decimal(str(bar.high)) if is_l else Decimal(str(bar.low))
        fav = self._fav_ext = (max if is_l else min)(self._fav_ext or entry, ex_px)
        be_hit = ((fav - entry) if is_l else (entry - fav)) / entry >= prof.breakeven_trigger_pct
        if (sess != "CLOSED" and self._stop_session not in (None, sess)) or (be_hit and stops):
            if self._stop_session not in (None, sess):
                self.session_transitions += 1
            self._replace_stop(pos, sess, ts, trailing=be_hit)

    def on_position_opened(self, event: nm.PositionOpened) -> None:
        ts, o = int(event.ts_event), self.cache.order(event.opening_order_id)
        if self._halt_triggered or self._closing or EXIT_TAG in (getattr(o, "tags", None) or ()):
            self._flatten(event.instrument_id, self._sess_prof(ts)[0])
            return
        self._sync_session(ts, event.instrument_id)
        self._fav_ext = Decimal(str(event.avg_px_open))
        if pos := self._open_pos(event.instrument_id):
            sess = self._sess_prof(ts)[0]
            prof = get_session_protection_profile(sess, self.session_protection)
            sd: Side = "LONG" if pos.is_long else "SHORT"
            _, self._init_stop_px = _hard_stop(self._fav_ext, sd, prof)
            self._replace_stop(pos, sess, ts, trailing=False)

    def on_position_closed(self, event: nm.PositionClosed) -> None:
        iid, ts = event.instrument_id, int(event.ts_event)
        notl = float(event.avg_px_open) * float(event.peak_qty.as_double())
        if notl > 0.0 and event.realized_pnl is not None:
            self._trade_returns.append(float(event.realized_pnl.as_double()) / notl)
        self._fav_ext = self._stop_session = self._init_stop_px = None
        self._closing = self._deferred_guard = False
        self._cancel_all_quiet(iid)
        self._sync_session(ts, iid, delta=self._dec(event.realized_pnl))
        sd, px, self._pending_side = self._pending_side, self._last_close_px, None
        was_rev, self._reversing = self._reversing, False
        rpnl = self._dec(self.portfolio.realized_pnl(iid)) - self._day_start_rpnl
        ok_b = self.breaker.update(realized_pnl=rpnl, unrealized_pnl=_ZERO)
        ok = ok_b and not self._halt_triggered
        if was_rev and ok and sd and px and self._submit_entry(iid, sd, px, ts):
            self.auto_reverses += 1
            self._last_closed_side, self._reentry_count = None, 0
            return
        self._last_closed_side = "LONG" if event.entry == nm.OrderSide.BUY else "SHORT"
        self._last_closed_ts_ns = ts

    def _guard_unprotected(self, event: Any) -> None:
        iid, cid = self.bar_type.instrument_id, getattr(event, "client_order_id", None)
        if self._replacing_stop or (self._closing and cid is None):
            return
        try:
            o = self.cache.order(cid) if self._closing and cid is not None else None
            need_close = bool(o) or (
                bool(self._open_pos(iid)) and not self._active_stops(iid, skip=cid, nv=_NV[:1])
            )
            if need_close:
                self._flatten(iid, self._sess_prof(self._last_ts_ns)[0], o)
        except RuntimeError:
            self._deferred_guard = True

    on_order_canceled = on_order_denied = on_order_rejected = on_order_expired = _guard_unprotected

    def on_stop(self) -> None:
        iid = self.bar_type.instrument_id
        held = self._open_pos(iid) is not None
        for o in self._active_orders(iid):
            if not (held and EXIT_TAG in (o.tags or ())):
                self.cancel_order(o.client_order_id)


def run_nautilus_signal_backtest(
    bars_df: pl.DataFrame, signals_by_ts: dict[int, int] | None = None, **o: Any
) -> SignalBacktestResult:
    """Execute full-resolution NautilusTrader v2 BacktestEngine + RiskEngine."""
    if bars_df.height == 0 or "t_ns" not in bars_df.columns:
        raise ValueError("Cannot run BacktestEngine on empty or t_ns-less bars_df")
    sdf = bars_df.sort(["t_ns", "symbol"])
    if sdf["symbol"].n_unique() != 1:
        raise ValueError("Backtest requires exactly 1 distinct symbol")
    cash = Decimal(str(o.get("starting_cash", "100000")))
    cbps = Decimal(str(o.get("cost_bps_per_side", "4.0")))
    vp, rp = str(o.get("venue_profile", "IBKR_PAPER_4002")), o.get("rate_budget_max_rpm")
    sym, usd = str(sdf["symbol"][0]).upper(), nm.Currency.from_str("USD")
    inst, btype, bars, prec = df_to_equity_and_bars(sdf, sym, usd)
    mdf, ex_i, ex_bt, ex_b = o.get("market_bars_df"), [], [], []
    if mdf is not None and mdf.height > 0:
        msdf = mdf.sort(["t_ns", "symbol"])
        if isinstance(mx := msdf.select(pl.col("t_ns").max()).item(), int):
            assert_pre_wall_ns(mx)
        for msym in {str(m).upper() for m in msdf["symbol"].unique().to_list()} - {sym}:
            sub = msdf.filter(pl.col("symbol").str.to_uppercase() == msym)
            mi, mbt, mb, _ = df_to_equity_and_bars(sub, msym, usd)
            ex_i.append(mi)
            ex_bt.append(mbt)
            ex_b.extend(mb)
    re = build_venue_risk_engine_config(vp, rate_budget_max_rpm=rp)
    engine = BacktestEngine(BacktestEngineConfig(logging=_QL, risk_engine=re, run_analysis=False))
    try:
        fee_r, cid, c0 = cbps / _BPS_DIV, nm.ClientId("SIM"), float(cash)
        fm = MakerTakerFeeModel(fee_r, fee_r)
        engine.add_venue(
            VENUE_XNAS,
            nm.OmsType.NETTING,
            nm.AccountType.MARGIN,
            [nm.Money(c0, usd)],
            usd,
            fee_model=fm,
            use_reduce_only=False,
        )
        for ins in (inst, *ex_i):
            engine.add_instrument(ins)
        engine.add_data(bars, client_id=cid)
        if ex_b:
            engine.add_data(sorted(ex_b, key=lambda b: b.ts_event), client_id=cid)
            engine.add_actor(MarketRegimeActor(MarketRegimeActorConfig(bar_types=ex_bt)))
        opt_keys = (
            "enable_24x5_trailing",
            "session_protection_24x5",
            "reentry_cooldown_secs",
            "max_reentries",
            "auto_reverse",
            "auto_reentry",
            "fixed_qty",
            "semi_auto_confirmed",
            "shortable",
            "enable_native_5m_bars",
        )
        skw = {k: o[k] for k in opt_keys if k in o}
        cfg = SignalExecutionConfig(
            btype,
            signals_by_ts,
            cash,
            prec,
            cbps,
            venue_profile=vp,
            market_bar_types=tuple(ex_bt),
            rate_budget_max_rpm=rp,
            **skw,
        )
        st = SignalExecutionStrategy(cfg)
        engine.add_strategy(st)
        engine.run()
        orders, acct = engine.cache.orders(), engine.cache.account_for_venue(VENUE_XNAS)
        fills = sum(1 for ord_ in orders if int(ord_.filled_qty.as_double()) > 0)
        outside_rth_limit_entries = 0
        outside_rth_market_entries = 0
        rth_market_entries = 0
        for ord_ in orders:
            ord_tags = tuple(ord_.tags or ())
            if EXIT_TAG in ord_tags:
                continue
            is_outside = IBKR_OUTSIDE_RTH_TAG in ord_tags
            if is_outside and ord_.order_type == _OT.LIMIT:
                outside_rth_limit_entries += 1
            elif is_outside and ord_.order_type == _OT.MARKET:
                outside_rth_market_entries += 1
            elif not is_outside and ord_.order_type == _OT.MARKET:
                rth_market_entries += 1
        bal = acct.balance_total(usd) if acct is not None else None
        end_eq, rets = (float(bal.as_double()) if bal is not None else c0), list(st._trade_returns)
        if bool(o.get("mark_open_positions_at_eof", True)):
            for pos in engine.cache.positions_open():
                lp = bars[-1].close
                xc = float(lp) * float(pos.quantity.as_double()) * float(fee_r)
                if (notl := float(pos.avg_px_open) * float(pos.peak_qty.as_double())) > 0.0:
                    rets.append((float(pos.total_pnl(lp).as_double()) - xc) / notl)
                end_eq += float(pos.unrealized_pnl(lp).as_double()) - xc
        return SignalBacktestResult(
            len(bars),
            st.bars_processed,
            len(orders),
            fills,
            st.stops_attached,
            ((end_eq - c0) / c0) * 100.0,
            tuple(rets),
            st.trailing_ratchets,
            st.session_transitions,
            bool(st._halt_triggered),
            st.bars_5m_processed,
            st.auto_reverses,
            st.auto_reentries,
            st.max_lock_tier,
            outside_rth_limit_entries,
            outside_rth_market_entries,
            rth_market_entries,
        )
    finally:
        engine.dispose()

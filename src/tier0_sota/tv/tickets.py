"""Half-Auto extreme-mover order ticket, Order Preset Pack, watchlist & L10 TV cockpit."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from tier0_sota import engine as _eng
from tier0_sota import risk as _risk
from tier0_sota.tv.bridge import (
    DEFAULT_MANIFEST_DIR,
    TvAlertV1,
    load_tv_audit_summary,
    persist_tv_presets_duckdb,
    refuse_live_endpoints,
    sync_tv_audit_duckdb_snapshot,
    validate_tv_alert_payload,
)

NY_TZ = ZoneInfo("America/New_York")
LOW_EXTREME_HOLD_REASONS = frozenset({"LOW_MOVE_NOT_EXTREME", "LOW_RVOL_NOT_EXTREME"})
INVERSE_HEDGE_SYMBOLS = frozenset(
    {
        "SOXS",
        "SQQQ",
        "SPXS",
        "SPXU",
        "SDOW",
        "QID",
        "SH",
        "TZA",
        "SDS",
        "TECS",
        "NVDS",
        "TSLS",
        "TSLQ",
        "MSTZ",
    }
)
BIDIRECTIONAL_PAIRS: dict[str, str] = {
    "SOXL": "SOXS",
    "SOXS": "SOXL",
    "TQQQ": "SQQQ",
    "SQQQ": "TQQQ",
    "UPRO": "SPXS",
    "SPXS": "UPRO",
    "SPXL": "SPXS",
    "TECL": "TECS",
    "TECS": "TECL",
    "NVDL": "NVDS",
    "NVDS": "NVDL",
    "TSLL": "TSLS",
    "TSLS": "TSLL",
    "TSLQ": "TSLL",
    "MSTU": "MSTZ",
    "MSTZ": "MSTU",
    "TNA": "TZA",
    "TZA": "TNA",
}
ADMITTED_FEED_SOURCES = frozenset(
    {
        "alpaca_screener",
        "alpaca_sip",
        "alpaca_boats",
        "alpaca_overnight",
        "tradingview_staged_presets",
        "finra_trf_exchange_d",
        "nautilus_trader",
    }
)
EXCHANGE_PREFIX_ALIASES: dict[str, str] = {
    "XNAS": "NASDAQ",
    "NASDAQ": "NASDAQ",
    "NASD": "NASDAQ",
    "NSDQ": "NASDAQ",
    "Q": "NASDAQ",
    "XNYS": "NYSE",
    "NYSE": "NYSE",
    "N": "NYSE",
    "ARCX": "AMEX",
    "ARCA": "AMEX",
    "AMEX": "AMEX",
    "ASE": "AMEX",
    "NYSEARCA": "AMEX",
    "NYSEAMERICAN": "AMEX",
    "P": "AMEX",
    "A": "AMEX",
    "BATS": "BATS",
    "BZX": "BATS",
    "BYX": "BATS",
    "CBOE": "BATS",
    "EDGX": "BATS",
    "EDGA": "BATS",
    "Z": "BATS",
    "IEX": "IEX",
    "IEXG": "IEX",
    "OTC": "OTC",
}
DEFAULT_EXCHANGES: dict[str, str] = {
    "SPY": "AMEX",
    "QQQ": "NASDAQ",
    "IWM": "AMEX",
    "RSP": "AMEX",
    "SOXX": "NASDAQ",
    "SMH": "NASDAQ",
    "VIX": "CBOE",
    "TLT": "NASDAQ",
    "GLD": "AMEX",
    "SOXS": "AMEX",
    "SOXL": "AMEX",
    "SQQQ": "NASDAQ",
    "TQQQ": "NASDAQ",
    "SPXS": "AMEX",
    "UPRO": "AMEX",
    "SDOW": "AMEX",
    "VXX": "BATS",
    "TNA": "AMEX",
    "TZA": "AMEX",
    "NVDA": "NASDAQ",
    "TSLA": "NASDAQ",
    "AMD": "NASDAQ",
    "PLTR": "NASDAQ",
    "SMCI": "NASDAQ",
    "MSTR": "NASDAQ",
    "COIN": "NASDAQ",
    "AVGO": "NASDAQ",
    "META": "NASDAQ",
    "SNDK": "NASDAQ",
    "MU": "NASDAQ",
    "MRVL": "NASDAQ",
    "ARM": "NASDAQ",
    "ASML": "NASDAQ",
    "TSM": "NYSE",
    "AAPL": "NASDAQ",
    "MSFT": "NASDAQ",
    "AMZN": "NASDAQ",
    "GOOGL": "NASDAQ",
    "NFLX": "NASDAQ",
    "CRWD": "NASDAQ",
    "PANW": "NASDAQ",
    "APP": "NASDAQ",
    "HOOD": "NASDAQ",
    "SOFI": "NASDAQ",
    "RKLB": "NASDAQ",
    "IONQ": "NYSE",
    "QBTS": "NYSE",
    "HIMS": "NYSE",
    "CVNA": "NYSE",
    "ORCL": "NYSE",
    "INTC": "NASDAQ",
    "QCOM": "NASDAQ",
    "SNOW": "NYSE",
    "NET": "NYSE",
    "SHOP": "NASDAQ",
    "UBER": "NYSE",
    "BABA": "NYSE",
    "NVDL": "NASDAQ",
    "NVDS": "NASDAQ",
    "TSLL": "NASDAQ",
    "TSLQ": "NASDAQ",
    "MSTU": "BATS",
    "MSTZ": "BATS",
    "UVXY": "BATS",
    "SVIX": "BATS",
}
OVERNIGHT_24X5_LEADERS: tuple[str, ...] = (
    "NVDA",
    "TSLA",
    "AMD",
    "PLTR",
    "SMCI",
    "MSTR",
    "COIN",
    "AVGO",
    "META",
)
CANONICAL_PINE_SCRIPTS: tuple[str, ...] = (
    "tier0_omni_sota_24x5.pine",
    "tier0_sota_strategy_24x5.pine",
    "tier0_market_regime.pine",
    "tier0_darkpool_uv_buildup.pine",
)
PAPER_ALPACA_LANE_ACCOUNTS: tuple[str, ...] = ("ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3")
PAPER_IBKR_LANE_ACCOUNTS: tuple[str, ...] = ("IBKR_DU_4002",)
_REPO_PINE_DIR = Path(__file__).resolve().parents[3] / "pine"
_REPO_PINE_OMNI_PATH = _REPO_PINE_DIR / "tier0_omni_sota_24x5.pine"

CANONICAL_TV_VENUE_PROFILES: tuple[str, ...] = (
    "IBKR_PAPER_4002",
    "ALPACA_PAPER_1000",
    "ALPACA_PAPER_200",
    "ALPACA_LIVE_PRO_LINE_1000",
)
ALL_TV_VENUE_PROFILES: frozenset[str] = frozenset(
    {
        "IBKR_PAPER_4002",
        "ALPACA_PAPER_200",
        "ALPACA_PAPER_1000",
        "IBKR_LIVE_3000",
        "ALPACA_LIVE_PRO_LINE_1000",
        "ALPACA_LIVE_ELITE_SMART_ROUTER",
    }
)

_R_LOCK_POLICY_STD = (
    "4-tier monotonic (BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R / Fast-Sell Rev, 60s cd)"
)

MULTI_SITUATION_SETUP_PLAYBOOK: dict[str, dict[str, Any]] = {
    "EARLY_EP_MOMENTUM_BURST": {
        "archetype": "EARLY_EP_MOMENTUM_BURST",
        "recommended_omni_preset": "STOCKBEE_EP_MOMENTUM_BURST",
        "target_paper_lane": "ALPACA_PA1",
        "target_lane_id": "L1_ORB_BURST",
        "preferred_schedule": "wide_natr_adaptive",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "Early catalyst/4% burst with RVOL >= 3x; trail T1 BE+10bp then R-locks.",
    },
    "VCP_BREAKOUT": {
        "archetype": "VCP_BREAKOUT",
        "recommended_omni_preset": "MINERVINI_VCP_BREAKOUT",
        "target_paper_lane": "ALPACA_PA2",
        "target_lane_id": "L2_STRUCTURAL_TREND",
        "preferred_schedule": "baseline",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "Stage-2 VCP pivot breakout post-VDU contraction; protect <= 2.00% Rule-612.",
    },
    "VCP_COIL": {
        "archetype": "VCP_COIL",
        "recommended_omni_preset": "MINERVINI_VCP_BREAKOUT",
        "target_paper_lane": "ALPACA_PA2",
        "target_lane_id": "L2_STRUCTURAL_TREND",
        "preferred_schedule": "tight_near_lossless",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": (
            "Tight volatility contraction coil; stage alert above pivot before expansion."
        ),
    },
    "PEAD_DRIFT": {
        "archetype": "PEAD_DRIFT",
        "recommended_omni_preset": "STOCKBEE_EP_MOMENTUM_BURST",
        "target_paper_lane": "ALPACA_PA2",
        "target_lane_id": "L2_STRUCTURAL_TREND",
        "preferred_schedule": "baseline",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "Post-earnings announcement drift continuation; scale trailing to NATR.",
    },
    "EXHAUSTION_HAMMER": {
        "archetype": "EXHAUSTION_HAMMER",
        "recommended_omni_preset": "STOCKBEE_EXHAUSTION_HAMMER",
        "target_paper_lane": "ALPACA_PA3",
        "target_lane_id": "L3_AVWAP_TIGHT_LOCK",
        "preferred_schedule": "tight_near_lossless",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "Selling-exhaustion undercut/reclaim hammer; tight BE+10bp arm on reclaim.",
    },
    "HIDDEN_VOL_ACCUM": {
        "archetype": "HIDDEN_VOL_ACCUM",
        "recommended_omni_preset": "STOCKBEE_EP_MOMENTUM_BURST",
        "target_paper_lane": "ALPACA_PA2",
        "target_lane_id": "L2_STRUCTURAL_TREND",
        "preferred_schedule": "baseline",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "High FINRA TRF (>= 40%) or UV/HVR institutional absorption buildup.",
    },
    "PARABOLIC_TOP": {
        "archetype": "PARABOLIC_TOP",
        "recommended_omni_preset": "PARABOLIC_EXHAUSTION_SHORT",
        "target_paper_lane": "ALPACA_PA3",
        "target_lane_id": "L3_AVWAP_TIGHT_LOCK",
        "preferred_schedule": "tight_near_lossless",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": ">= 2*NATR VWAP climax extension; prepare flatten-first Fast-Sell Reverse.",
    },
    "PEAK_REVERSAL": {
        "archetype": "PEAK_REVERSAL",
        "recommended_omni_preset": "PARABOLIC_EXHAUSTION_SHORT",
        "target_paper_lane": "ALPACA_PA3",
        "target_lane_id": "L3_AVWAP_TIGHT_LOCK",
        "preferred_schedule": "tight_near_lossless",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "Verified reversal peak breakdown; route DIRECT_SHORT_ETB or inverse ETF.",
    },
    "CRASH_ALIGNMENT": {
        "archetype": "CRASH_ALIGNMENT",
        "recommended_omni_preset": "CRASH_DEFENSIVE_INVERSE",
        "target_paper_lane": "ALPACA_PA3",
        "target_lane_id": "L3_AVWAP_TIGHT_LOCK",
        "preferred_schedule": "tight_near_lossless",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "Macro regime == -1 crash alignment; 0.25% risk short ETB or long SOXS/SQQQ.",
    },
    "EXTREME_REVERSAL": {
        "archetype": "EXTREME_REVERSAL",
        "recommended_omni_preset": "AUTO_REGIME_ADAPTIVE",
        "target_paper_lane": "ALPACA_PA3",
        "target_lane_id": "L3_AVWAP_TIGHT_LOCK",
        "preferred_schedule": "tight_near_lossless",
        "r_lock_policy": _R_LOCK_POLICY_STD,
        "climax_auto_reverse": True,
        "reentry_cooldown_secs": 60,
        "play_notes": "24/5 extreme-move reversal or overnight standby; enforce Rule-612 stop cap.",
    },
}

OMNI_ORDER_PRESET_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "preset_id": "AUTO_REGIME_ADAPTIVE",
        "timeframe": "1m",
        "regime_alignment": "MULTI_FEED_SPY_QQQ_SOXX_RSP_VIX",
        "min_rvol": 1.5,
        "short_routing": "DIRECT_SHORT_ETB_OR_INVERSE_HEDGE",
        "summary": "Auto-routes between Overnight 24/5, Crash Inverse, VCP Breakout & Tight Scalp.",
    },
    {
        "preset_id": "LOSSLESS_TIGHT_SCALP",
        "timeframe": "1m",
        "regime_alignment": "NEUTRAL_OR_BULL",
        "min_rvol": 1.5,
        "short_routing": "FAST_SELL_CLIMAX_ONLY",
        "summary": "Tight near-lossless BE+10bp arm with 4-tier R-lock and VWAP reclaim focus.",
    },
    {
        "preset_id": "CRASH_DEFENSIVE_INVERSE",
        "timeframe": "1m",
        "regime_alignment": "CRASH_DEFENSIVE (-1)",
        "min_rvol": 1.8,
        "short_routing": "DIRECT_SHORT_ETB_OR_SOXS_SQQQ_LONG",
        "summary": "Shorts ETB breakdown leaders or buys SOXS/SQQQ inverse hedges at 0.25% risk.",
    },
    {
        "preset_id": "MINERVINI_VCP_BREAKOUT",
        "timeframe": "1m / 5m",
        "regime_alignment": "RISK_ON (+1)",
        "min_rvol": 1.5,
        "short_routing": "PEAK_REVERSAL_ONLY",
        "summary": "Stage-2 VCP coil breakout with volume dry-up and CLV >= 0.25.",
    },
    {
        "preset_id": "STOCKBEE_EP_MOMENTUM_BURST",
        "timeframe": "1m / 5m",
        "regime_alignment": "RISK_ON_OR_CATALYST",
        "min_rvol": 2.5,
        "short_routing": "PEAK_REVERSAL_ONLY",
        "summary": "Early Episodic Pivot / 4% Momentum Burst with range & hidden-vol boost.",
    },
    {
        "preset_id": "STOCKBEE_EXHAUSTION_HAMMER",
        "timeframe": "1m / 5m",
        "regime_alignment": "NEUTRAL_OR_BULL",
        "min_rvol": 1.3,
        "short_routing": "PEAK_REVERSAL_ONLY",
        "summary": "Selling-exhaustion undercut/reclaim hammer with >= 55% lower wick.",
    },
    {
        "preset_id": "PARABOLIC_EXHAUSTION_SHORT",
        "timeframe": "1m",
        "regime_alignment": "NON_BULL (!= +1)",
        "min_rvol": 2.5,
        "short_routing": "DIRECT_SHORT_ETB_OR_INVERSE_HEDGE",
        "summary": "Short verified >= 2*NATR parabolic VWAP climax on breakdown bar.",
    },
    {
        "preset_id": "OVERNIGHT_24X5_IBKR",
        "timeframe": "1m",
        "regime_alignment": "OVERNIGHT_20_00_TO_04_00_ET",
        "min_rvol": 1.5,
        "short_routing": "IBKR_DIRECT_SHORT_OR_ALPACA_INVERSE_HEDGE",
        "summary": "Collared STOP_LIMIT (50 bps), hard SL <= 1.00%, Extended Hours ON, qty=1.",
    },
)


def current_et_session(now_et: datetime) -> str:
    """Classify ET datetime into XNYS-aware 24/5 session label (or CLOSED) via engine SSOT."""
    aware = now_et if now_et.tzinfo is not None else now_et.replace(tzinfo=NY_TZ)
    delta = aware.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    ts_ns = ((delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1_000
    return _eng.classify_us_equity_session_et(ts_ns)


def _normalize_tv_exchange(raw_exchange: Any) -> str | None:
    if raw_exchange is None:
        return None
    token = str(raw_exchange).strip().upper()
    if not token:
        return None
    return EXCHANGE_PREFIX_ALIASES.get(token)


def resolve_tv_exchange_symbol(
    symbol: str,
    *,
    exchange: Any = None,
    exchange_map: Mapping[str, str] | None = None,
) -> str:
    """Resolve TradingView EXCHANGE:SYMBOL prefix from explicit map, asset exchange, or defaults."""
    sym = str(symbol).strip().upper()
    if not sym:
        return ""
    if ":" in sym:
        return sym
    if exchange_map:
        mapped = exchange_map.get(sym) or exchange_map.get(sym.lower())
        if mapped:
            norm = _normalize_tv_exchange(mapped) or str(mapped).strip().upper()
            return f"{norm}:{sym}"
    norm_ex = _normalize_tv_exchange(exchange)
    if norm_ex:
        return f"{norm_ex}:{sym}"
    default_ex = DEFAULT_EXCHANGES.get(sym)
    return f"{default_ex}:{sym}" if default_ex else sym


def filter_extreme_movers(
    entries: Sequence[Mapping[str, Any]], *, regime_int: int
) -> list[dict[str, Any]]:
    """Filter strictly to extreme movers (plus inverse crash hedges when regime == -1)."""
    out: list[dict[str, Any]] = []
    for raw in entries:
        sym, px = str(raw.get("symbol") or "").strip().upper(), raw.get("last_price")
        if not sym or px is None or float(px) <= 0.0:
            continue
        hold = str(raw.get("extreme_mover_hold_reason") or "").strip()
        if hold in LOW_EXTREME_HOLD_REASONS and not (regime_int == -1 and sym in ("SOXS", "SQQQ")):
            continue
        out.append(dict(raw))
    return out


def resolve_venue_profile(
    venue: str = "IBKR", *, venue_profile: str | None = None
) -> tuple[str, str]:
    """Resolve canonical venue_profile and derived broker ('IBKR'|'ALPACA')."""
    if venue_profile is not None and str(venue_profile).strip():
        vp = str(venue_profile).strip().upper()
        if vp not in ALL_TV_VENUE_PROFILES:
            raise ValueError(f"Unsupported TV venue_profile: {venue_profile!r}")
        broker = "IBKR" if vp.startswith("IBKR") else "ALPACA"
        return vp, broker
    vu = str(venue).strip().upper()
    if vu not in {"IBKR", "ALPACA"}:
        raise ValueError(f"Unsupported TV venue: {venue!r}")
    env_vp = os.environ.get("TIER0_VENUE_PROFILE", "").strip().upper()
    if env_vp in ALL_TV_VENUE_PROFILES and env_vp.startswith(f"{vu}_PAPER_"):
        return env_vp, vu
    if vu == "IBKR":
        return "IBKR_PAPER_4002", "IBKR"
    raw_rpm = os.environ.get("TIER0_ALPACA_RPM", "").strip().upper()
    if raw_rpm == "ALPACA_PAPER_1000":
        return "ALPACA_PAPER_1000", "ALPACA"
    if raw_rpm == "ALPACA_PAPER_200":
        return "ALPACA_PAPER_200", "ALPACA"
    if raw_rpm:
        with contextlib.suppress(ValueError):
            if 200 < int(raw_rpm) <= 1000:
                return "ALPACA_PAPER_1000", "ALPACA"
    return "ALPACA_PAPER_200", "ALPACA"


def resolve_venue_order_handle(
    venue_profile: str,
    *,
    session: str = "RTH",
    trailing_schedule: str = "tight_near_lossless",
) -> dict[str, Any]:
    """Resolve NautilusTrader v2 1000rpm/3000rpm order-handle budget & exit reserve."""
    vp = str(venue_profile).strip().upper()
    if vp not in ALL_TV_VENUE_PROFILES:
        raise ValueError(f"Unsupported venue_profile: {venue_profile!r}")
    sched_key = (
        trailing_schedule
        if trailing_schedule in _eng.POOLED_TRAILING_SCHEDULES
        else "tight_near_lossless"
    )
    return dict(
        _eng.build_venue_order_handle_spec(vp, session=session, trailing_schedule=sched_key)
    )


def build_venue_order_handles_matrix(
    *, session: str = "RTH", trailing_schedule: str = "tight_near_lossless"
) -> dict[str, dict[str, Any]]:
    """Build 4-profile venue order-handle matrix (IBKR 3000rpm, Alpaca 1000/200rpm, Stage-5)."""
    return {
        vp: resolve_venue_order_handle(vp, session=session, trailing_schedule=trailing_schedule)
        for vp in CANONICAL_TV_VENUE_PROFILES
    }


def classify_multi_situation_setup(
    entry: Mapping[str, Any],
    *,
    regime_int: int,
    sess_chg: float,
    rvol: float,
    darkpool_trf_ratio: float | None = None,
    unusual_volume_buildup: bool = False,
    peak_confirmed: bool | None = None,
    natr_pct: float = 0.015,
) -> tuple[str, _risk.Side, str]:
    """Resolve side & converged 8-setup multi-situation archetype from causal features."""
    sym = str(entry.get("symbol") or "").strip().upper()
    if sym in INVERSE_HEDGE_SYMBOLS and regime_int == -1:
        return "BUY", "LONG", "CRASH_ALIGNMENT"
    if regime_int == -1:
        return "SELL", "SHORT", "CRASH_ALIGNMENT"

    raw_side = str(entry.get("side") or "").strip().upper()
    explicit_arch = (
        str(entry.get("setup_archetype") or entry.get("archetype") or "").strip().upper()
    )

    if peak_confirmed is True and (raw_side == "SELL" or sess_chg < 2.0 or rvol < 3.0):
        return "SELL", "SHORT", "PEAK_REVERSAL"
    if raw_side == "SELL":
        if explicit_arch == "PARABOLIC_TOP" or entry.get("parabolic_top") is True:
            return "SELL", "SHORT", "PARABOLIC_TOP"
        if explicit_arch == "PEAK_REVERSAL":
            return "SELL", "SHORT", "PEAK_REVERSAL"
        return "SELL", "SHORT", "EXTREME_REVERSAL"
    if explicit_arch == "PEAD_DRIFT" or entry.get("earnings_catalyst") is True:
        return "BUY", "LONG", "PEAD_DRIFT"
    if sess_chg >= 2.0 and rvol >= 3.0:
        return "BUY", "LONG", "EARLY_EP_MOMENTUM_BURST"
    if explicit_arch == "VCP_BREAKOUT" or entry.get("vcp_breakout") is True:
        return "BUY", "LONG", "VCP_BREAKOUT"
    if (
        explicit_arch == "VCP_COIL"
        or entry.get("vcp_coil") is True
        or (entry.get("coil_ok") is True and natr_pct <= 0.015 and abs(sess_chg) < 2.0)
    ):
        return "BUY", "LONG", "VCP_COIL"
    lower_wk = _opt_float(entry.get("lower_wick_ratio"), min_val=0.0) or 0.0
    if (
        explicit_arch == "EXHAUSTION_HAMMER"
        or entry.get("exhaustion_hammer") is True
        or (lower_wk >= 0.55 and sess_chg >= 0.0 and rvol >= 1.3)
    ):
        return "BUY", "LONG", "EXHAUSTION_HAMMER"
    if (
        (darkpool_trf_ratio is not None and darkpool_trf_ratio >= 0.40) or unusual_volume_buildup
    ) and rvol >= 1.5:
        return "BUY", "LONG", "HIDDEN_VOL_ACCUM"
    if explicit_arch == "PARABOLIC_TOP" or entry.get("parabolic_top") is True:
        return (
            ("SELL", "SHORT", "PARABOLIC_TOP")
            if regime_int != 1
            else ("BUY", "LONG", "PARABOLIC_TOP")
        )
    if explicit_arch == "PEAK_REVERSAL":
        return "SELL", "SHORT", "PEAK_REVERSAL"
    return "BUY", "LONG", "EXTREME_REVERSAL"


def _resolve_direction(
    entry: Mapping[str, Any],
    *,
    regime_int: int,
    sess_chg: float,
    rvol: float,
    darkpool_trf_ratio: float | None = None,
    unusual_volume_buildup: bool = False,
    peak_confirmed: bool | None = None,
    natr_pct: float = 0.015,
) -> tuple[str, _risk.Side, str]:
    return classify_multi_situation_setup(
        entry,
        regime_int=regime_int,
        sess_chg=sess_chg,
        rvol=rvol,
        darkpool_trf_ratio=darkpool_trf_ratio,
        unusual_volume_buildup=unusual_volume_buildup,
        peak_confirmed=peak_confirmed,
        natr_pct=natr_pct,
    )


def _opt_float(val: Any, *, min_val: float | None = None) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        out = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out) or (min_val is not None and out < min_val):
        return None
    return out


def _opt_int(val: Any, *, min_val: int | None = None) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        out = int(val)
    except (TypeError, ValueError):
        return None
    if min_val is not None and out < min_val:
        return None
    return out


def _trf_age_hours(as_of_utc: str | None, ref_utc: datetime | None) -> float | None:
    if not as_of_utc or ref_utc is None:
        return None
    try:
        dt = datetime.fromisoformat(as_of_utc)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    age_s = (ref_utc.astimezone(UTC) - dt.astimezone(UTC)).total_seconds()
    if not math.isfinite(age_s) or age_s < 0.0:
        return None
    return round(age_s / 3600.0, 1)


def _action_gate(
    sym: str,
    side: str,
    regime_int: int,
    etb_ok: bool,
    b_row: Mapping[str, Any],
    *,
    venue: str = "IBKR",
    active_sess: str = "RTH",
    live_eligible: bool = False,
    peak_confirmed: bool | None = None,
    sess_chg: float = 0.0,
) -> str:
    if sym in INVERSE_HEDGE_SYMBOLS and (
        regime_int == -1
        or (live_eligible and active_sess == "OVERNIGHT" and side == "BUY" and sess_chg > 0.0)
    ):
        return "READY_HALF_AUTO_HEDGE"
    if side == "SELL" and regime_int != -1 and peak_confirmed is not True:
        if b_row.get("entry_hold_reason"):
            return f"HOLD ({b_row['entry_hold_reason']})"
        return "HOLD (BEARISH_UNCONFIRMED_REGIME)"
    if side == "SELL" and not etb_ok:
        return "USE_INVERSE_ETF_OR_VERIFY_BORROW"
    if side == "SELL" and venue == "ALPACA" and active_sess == "OVERNIGHT":
        return "ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ"
    if b_row.get("entry_eligible") is True:
        return "READY_HALF_AUTO"
    if b_row.get("entry_hold_reason"):
        return f"HOLD ({b_row['entry_hold_reason']})"
    if live_eligible and not b_row:
        return "READY_HALF_AUTO"
    return "MONITOR_CONFIRM_BAR"


def _build_order_level_row(
    e: Mapping[str, Any],
    *,
    rank: int,
    scanner_rank: int | None = None,
    active_sess: str,
    regime_int: int,
    vu: str,
    venue_profile: str | None = None,
    b_row: Mapping[str, Any],
    live_eligible: bool,
    action_gate_override: str | None = None,
    now_utc: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compute Rule-612 stops, collared limits, 4-tier R-locks, playbook & handle metadata."""
    sym = str(e["symbol"]).strip().upper()
    vp, _ = resolve_venue_profile(vu, venue_profile=venue_profile)
    raw_exch = e.get("exchange") or e.get("tv_exchange") or b_row.get("exchange")
    tv_symbol = resolve_tv_exchange_symbol(sym, exchange=raw_exch)
    norm_exch = tv_symbol.split(":", 1)[0] if ":" in tv_symbol else None
    px = Decimal(str(round(float(e["last_price"]), 4)))
    natr = max(
        Decimal("0.001"),
        min(Decimal("0.50"), Decimal(str(round(float(e.get("natr_pct") or 0.015), 6)))),
    )
    sched_name, sess_map = _eng.select_pooled_trailing_schedule(natr)
    prof_now = _eng.get_session_protection_profile(active_sess, sess_map)
    prof_rth = _eng.get_session_protection_profile("RTH", sess_map)
    sess_chg = float(e.get("session_move_pct") or e.get("change_pct") or 0.0)
    raw_rvol = _opt_float(
        e.get("rvol_normalized") if e.get("rvol_normalized") is not None else e.get("rvol"),
        min_val=0.0,
    )
    rvol = raw_rvol if raw_rvol is not None else 1.0
    raw_trf = _opt_float(
        e.get("darkpool_trf_ratio")
        if e.get("darkpool_trf_ratio") is not None
        else b_row.get("darkpool_trf_ratio"),
        min_val=0.0,
    )
    darkpool_trf_ratio = min(1.0, round(raw_trf, 4)) if raw_trf is not None else None
    darkpool_trf_source = (
        str(e.get("darkpool_trf_source") or b_row.get("darkpool_trf_source") or "").strip() or None
    )
    darkpool_trf_as_of_utc = (
        str(e.get("darkpool_trf_as_of_utc") or b_row.get("darkpool_trf_as_of_utc") or "").strip()
        or None
    )
    explicit_trf_age = _opt_float(
        e.get("darkpool_trf_age_hours")
        if e.get("darkpool_trf_age_hours") is not None
        else b_row.get("darkpool_trf_age_hours"),
        min_val=0.0,
    )
    darkpool_trf_age_hours = (
        explicit_trf_age
        if explicit_trf_age is not None
        else _trf_age_hours(darkpool_trf_as_of_utc, now_utc)
    )
    darkpool_block_count = _opt_int(
        e.get("darkpool_block_count")
        if e.get("darkpool_block_count") is not None
        else b_row.get("darkpool_block_count"),
        min_val=0,
    )
    raw_tbr = _opt_float(
        e.get("trf_block_ratio")
        if e.get("trf_block_ratio") is not None
        else b_row.get("trf_block_ratio"),
        min_val=0.0,
    )
    trf_block_ratio = min(1.0, round(raw_tbr, 4)) if raw_tbr is not None else None
    raw_hvr = _opt_float(e.get("hidden_volume_ratio"), min_val=0.0)
    hidden_volume_ratio = round(raw_hvr, 3) if raw_hvr is not None else None
    raw_vbr = _opt_float(e.get("vol_buildup_ratio"), min_val=0.0)
    vol_buildup_ratio = round(raw_vbr, 3) if raw_vbr is not None else None
    unusual_volume_buildup = bool(
        e.get("unusual_volume_buildup") is True or b_row.get("unusual_volume_buildup") is True
    )
    raw_dvol = _opt_float(e.get("dollar_vol"), min_val=0.0)
    dollar_vol = round(raw_dvol, 2) if raw_dvol is not None else None
    raw_score = _opt_float(e.get("score"))
    score = round(raw_score, 2) if raw_score is not None else None
    peak_confirmed = e.get("peak_confirmed") if isinstance(e.get("peak_confirmed"), bool) else None
    watch_tier = str(e.get("watch_tier") or b_row.get("watch_tier") or "").strip() or (
        "EXTREME_MOVER" if live_eligible else ("PINNED" if e.get("pinned") else "WATCH")
    )
    side, r_side, arch = _resolve_direction(
        e,
        regime_int=regime_int,
        sess_chg=sess_chg,
        rvol=rvol,
        darkpool_trf_ratio=darkpool_trf_ratio,
        unusual_volume_buildup=unusual_volume_buildup,
        peak_confirmed=peak_confirmed,
        natr_pct=float(natr),
    )
    playbook = dict(
        MULTI_SITUATION_SETUP_PLAYBOOK.get(arch, MULTI_SITUATION_SETUP_PLAYBOOK["EXTREME_REVERSAL"])
    )
    rec_omni = str(playbook["recommended_omni_preset"])
    target_lane = str(b_row.get("lane_account") or playbook["target_paper_lane"])
    target_lane_id = str(b_row.get("lane_id") or playbook["target_lane_id"])
    venue_handle = resolve_venue_order_handle(vp, session=active_sess, trailing_schedule=sched_name)

    stop_now = _risk.compute_capped_stop_price(
        px, r_side, max_loss_pct=prof_now.initial_max_loss_pct
    )
    stop_rth = _risk.compute_capped_stop_price(
        px, r_side, max_loss_pct=prof_rth.initial_max_loss_pct
    )
    entry_limit_now = (
        _eng.compute_collared_entry_limit_price(px, r_side, prof_now.limit_collar_pct)
        if active_sess != "RTH" and prof_now.limit_collar_pct > Decimal("0")
        else px
    )
    stop_limit_now = (
        _eng.compute_collared_limit_price(stop_now, r_side, prof_now.limit_collar_pct)
        if prof_now.order_mode == "STOP_LIMIT" and prof_now.limit_collar_pct > Decimal("0")
        else None
    )
    dist_now, risk_r = float(abs(px - stop_now) / px), abs(px - stop_now)
    sign = Decimal("1") if side == "BUY" else Decimal("-1")

    def _quant(price: Decimal, *, is_stop: bool = True, route_side: _risk.Side = r_side) -> Decimal:
        return _risk.quantize_rule_612(
            max(_risk.TICK_SUB_DOLLAR, price), side=route_side, is_stop=is_stop
        )

    be_floor = _quant(px * (Decimal("1.0010") if side == "BUY" else Decimal("0.9990")))
    t1r, t2r, t3r = (
        _quant(px + sign * Decimal(k) * risk_r, is_stop=False) for k in ("1", "2", "3")
    )
    lock_at_1r = _quant(px + sign * Decimal("0.45") * risk_r)
    lock_at_2r = _quant(px + sign * Decimal("1.35") * risk_r)
    lock_at_3r = _quant(px + sign * Decimal("2.35") * risk_r)
    fast_sell_rev = _quant(px * (Decimal("0.9960") if side == "BUY" else Decimal("1.0040")))

    risk_pct = Decimal("0.0025") if regime_int == -1 else Decimal("0.0050")
    sh_25k, sh_100k = (
        _risk.size_position_by_risk(
            equity=Decimal(eq),
            entry_price=px,
            stop_price=stop_now,
            risk_per_trade_pct=risk_pct,
            max_notional_pct=Decimal("0.15"),
        )
        for eq in ("25000", "100000")
    )
    shortable = bool(
        e["shortable"] if e.get("shortable") is not None else b_row.get("shortable", False)
    )
    etb = bool(
        e["easy_to_borrow"]
        if e.get("easy_to_borrow") is not None
        else b_row.get("easy_to_borrow", False)
    )
    etb_ok = shortable and etb
    alpaca_overnight_short_blocked = vu == "ALPACA" and active_sess == "OVERNIGHT"
    rev_short_mode = (
        "DIRECT_SHORT_ETB"
        if etb_ok and not alpaca_overnight_short_blocked
        else ("ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ" if etb_ok else "INVERSE_HEDGE_SOXS_SQQQ")
    )
    gate = action_gate_override or _action_gate(
        sym,
        side,
        regime_int,
        etb_ok,
        b_row,
        venue=vu,
        active_sess=active_sess,
        live_eligible=live_eligible,
        peak_confirmed=peak_confirmed,
        sess_chg=sess_chg,
    )
    collar_bps = float(prof_now.limit_collar_pct * Decimal("10000"))
    pair_sym = str(e.get("pair_symbol") or "").strip().upper() or BIDIRECTIONAL_PAIRS.get(sym)
    raw_prior = _opt_float(e.get("prior_close"), min_val=0.0001)
    prior_close = round(raw_prior, 4) if raw_prior is not None else None
    raw_borrow = _opt_float(e.get("annual_borrow_rate"), min_val=0.0)
    annual_borrow_rate = round(raw_borrow, 6) if raw_borrow is not None else None
    order_preset_id = f"{sched_name}:{active_sess}:{arch}"[:64]

    row: dict[str, Any] = {
        "rank": rank,
        "scanner_rank": scanner_rank if scanner_rank is not None else _opt_int(e.get("rank")),
        "symbol": sym,
        "exchange": norm_exch,
        "tv_symbol": tv_symbol,
        "pair_symbol": pair_sym,
        "side": side,
        "archetype": arch,
        "recommended_omni_preset": rec_omni,
        "target_paper_lane": target_lane,
        "target_lane_id": target_lane_id,
        "setup_playbook": playbook,
        "venue_profile": vp,
        "venue_order_handle": venue_handle,
        "order_preset_id": order_preset_id,
        "watch_tier": watch_tier,
        "action_gate": gate,
        "score": score,
        "dollar_vol": dollar_vol,
        "live_price": float(px),
        "entry_limit_now": float(entry_limit_now),
        "prior_close": prior_close,
        "annual_borrow_rate": annual_borrow_rate,
        "session_move_pct": sess_chg,
        "rvol": round(rvol, 2),
        "darkpool_trf_ratio": darkpool_trf_ratio,
        "darkpool_trf_source": darkpool_trf_source,
        "darkpool_trf_as_of_utc": darkpool_trf_as_of_utc,
        "darkpool_trf_age_hours": darkpool_trf_age_hours,
        "darkpool_block_count": darkpool_block_count,
        "trf_block_ratio": trf_block_ratio,
        "hidden_volume_ratio": hidden_volume_ratio,
        "vol_buildup_ratio": vol_buildup_ratio,
        "unusual_volume_buildup": unusual_volume_buildup,
        "pinned": bool(e.get("pinned") is True or b_row.get("pinned") is True),
        "borrow_source": (
            str(e.get("borrow_source") or b_row.get("borrow_source") or "").strip() or None
        ),
        "short_fee_bps": _opt_float(
            e.get("short_fee_bps")
            if e.get("short_fee_bps") is not None
            else b_row.get("short_fee_bps"),
            min_val=0.0,
        ),
        "peak_confirmed": peak_confirmed,
        "natr_pct": float(natr),
        "schedule": sched_name,
        "active_session": active_sess,
        "order_type_now": prof_now.order_mode,
        "collar_now_bps": int(collar_bps),
        "stop_now": float(stop_now),
        "stop_limit_now": float(stop_limit_now) if stop_limit_now is not None else None,
        "stop_now_pct": round(dist_now * 100.0, 2),
        "stop_rth_0930": float(stop_rth),
        "be_plus_fee_floor": float(be_floor),
        "target_1r": float(t1r),
        "target_2r": float(t2r),
        "target_3r": float(t3r),
        "lock_at_1r": float(lock_at_1r),
        "lock_at_2r": float(lock_at_2r),
        "lock_at_3r": float(lock_at_3r),
        "fast_sell_rev_trigger": float(fast_sell_rev),
        "reentry_cooldown_secs": 60,
        "reverse_short_mode": rev_short_mode,
        "shares_25k_defensive": sh_25k,
        "shares_100k_defensive": sh_100k,
        "shortable": shortable,
        "easy_to_borrow": etb,
        "paper_lane_account": b_row.get("lane_account"),
        "paper_lane_id": b_row.get("lane_id"),
        "paper_origin": b_row.get("origin"),
        "paper_entry_eligible": b_row.get("entry_eligible"),
        "paper_hold_reason": b_row.get("entry_hold_reason"),
    }
    ctx: dict[str, Any] = {
        "dist_now": dist_now,
        "collar_bps": collar_bps,
        "rvol": rvol,
        "pair_sym": pair_sym,
        "darkpool_trf_ratio": darkpool_trf_ratio,
        "hidden_volume_ratio": hidden_volume_ratio,
        "vol_buildup_ratio": vol_buildup_ratio,
        "unusual_volume_buildup": unusual_volume_buildup,
        "peak_confirmed": peak_confirmed,
        "order_preset_id": order_preset_id,
    }
    return row, ctx


def build_half_auto_tickets(
    movers: Mapping[str, Any],
    *,
    bundle: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
    venue: str = "IBKR",
    venue_profile: str | None = None,
    max_tickets: int = 15,
) -> dict[str, Any]:
    """Build schema-validated Half-Auto extreme-mover tickets with Rule-612 lock rungs."""
    vp, vu = resolve_venue_profile(venue, venue_profile=venue_profile)
    ts_utc = now_utc or datetime.now(UTC)
    now_et = ts_utc.astimezone(NY_TZ)
    active_sess, ts_ms = current_et_session(now_et), int(ts_utc.timestamp() * 1000)
    live_scan = movers.get("schema_version") == "tier0.whole-market-movers.v2"
    macro = dict(movers.get("macro_regime") or {})
    regime_int = int(macro.get("regime", 0))
    b_map = {
        str(x["symbol"]).upper(): x
        for x in (bundle or {}).get("top_potential_symbols", [])
        if isinstance(x, Mapping) and x.get("symbol")
    }
    extreme = (
        []
        if active_sess == "CLOSED"
        else filter_extreme_movers(movers.get("entries", []), regime_int=regime_int)
    )
    tickets: list[dict[str, Any]] = []
    ticket_syms: set[str] = set()
    schema_ok = 0

    for idx, e in enumerate(extreme, 1):
        if len(tickets) >= max_tickets:
            break
        bar_ms = ts_ms
        if live_scan:
            if not e.get("bar_close_utc"):
                continue
            close = datetime.fromisoformat(e["bar_close_utc"])
            if close.tzinfo is None:
                continue
            bar_ms = int(close.timestamp() * 1000)
            if (
                not 0 <= ts_ms - bar_ms < 60_000
                or _eng.classify_us_equity_session_et((bar_ms - 60_000) * 1_000_000) != active_sess
                or e.get("extreme_mover_eligible") is not True
            ):
                continue
        sym = str(e["symbol"]).strip().upper()
        b_row = b_map.get(sym, {})
        live_eligible = live_scan and e.get("extreme_mover_eligible") is True
        row, ctx = _build_order_level_row(
            e,
            rank=len(tickets) + 1,
            scanner_rank=_opt_int(e.get("rank")) or idx,
            active_sess=active_sess,
            regime_int=regime_int,
            vu=vu,
            venue_profile=vp,
            b_row=b_row,
            live_eligible=live_eligible,
            now_utc=ts_utc,
        )

        alert_in: dict[str, Any] = {
            "schema_version": "tier0.tv-alert.v1",
            "symbol": sym,
            "ts_utc_ms": bar_ms,
            "bar_confirmed": True,
            "side": row["side"],
            "qty": 1,
            "venue": vu,
            "venue_profile": vp,
            "session": active_sess,
            "order_type": row["order_type_now"],
            "trigger_price": row["live_price"],
            "stop_price": row["stop_now"],
            "stop_distance_pct": round(float(ctx["dist_now"]), 6),
            "limit_collar_bps": float(ctx["collar_bps"]),
            "trailing_schedule": row["schedule"],
            "natr_pct": row["natr_pct"],
            "auto_reverse": True,
            "auto_reentry": True,
            "reentry_cooldown_secs": 60,
            "strategy_id": "SignalExecutionStrategy",
            "semi_auto_confirmed": False,
            "setup_archetype": row["archetype"],
            "macro_regime": regime_int,
            "rvol": round(float(ctx["rvol"]), 2),
            "is_leveraged_or_inverse": sym in INVERSE_HEDGE_SYMBOLS or sym in BIDIRECTIONAL_PAIRS,
            "order_preset_id": ctx["order_preset_id"],
            "data_source": (
                "alpaca_boats"
                if "alpaca_boats" in movers.get("feed_sources", [])
                else "alpaca_sip"
                if live_scan
                else "alpaca_screener"
            ),
        }
        if ctx["pair_sym"] is not None:
            alert_in["pair_symbol"] = ctx["pair_sym"]
        if ctx["darkpool_trf_ratio"] is not None:
            alert_in["darkpool_trf_ratio"] = ctx["darkpool_trf_ratio"]
        if ctx["hidden_volume_ratio"] is not None:
            alert_in["hidden_volume_ratio"] = ctx["hidden_volume_ratio"]
        if ctx["vol_buildup_ratio"] is not None:
            alert_in["vol_buildup_ratio"] = ctx["vol_buildup_ratio"]
        if ctx["unusual_volume_buildup"] is True:
            alert_in["unusual_volume_buildup"] = True
        if ctx["peak_confirmed"] is not None:
            alert_in["peak_confirmed"] = ctx["peak_confirmed"]
        if row["side"] == "SELL":
            alert_in |= {"shortable": row["shortable"], "easy_to_borrow": row["easy_to_borrow"]}
        refuse_live_endpoints(alert_in)
        try:
            TvAlertV1.model_validate(alert_in)
        except ValidationError as exc:
            raise ValueError(f"Invalid ticket preset schema: {exc}") from exc
        schema_ok += 1
        row["tv_alert_preset"] = alert_in
        tickets.append(row)
        ticket_syms.add(sym)

    standby_presets: list[dict[str, Any]] = []
    if active_sess != "CLOSED":
        seen_standby: set[str] = set()
        standby_pool: list[Mapping[str, Any]] = list(extreme)
        for raw_e in movers.get("entries", []):
            if not isinstance(raw_e, Mapping):
                continue
            sym_r = str(raw_e.get("symbol") or "").strip().upper()
            px_r = _opt_float(raw_e.get("last_price"), min_val=0.0001)
            wt = str(raw_e.get("watch_tier") or "").strip().upper()
            if (
                sym_r
                and px_r is not None
                and wt in {"UV_DARKPOOL_BUILDUP", "OVERNIGHT_ACTIVE"}
                and all(str(x.get("symbol", "")).strip().upper() != sym_r for x in standby_pool)
            ):
                standby_pool.append(raw_e)
        for s_idx, e in enumerate(standby_pool, 1):
            if len(standby_presets) >= max_tickets:
                break
            sym = str(e["symbol"]).strip().upper()
            if sym in ticket_syms or sym in seen_standby:
                continue
            seen_standby.add(sym)
            s_row, _ = _build_order_level_row(
                e,
                rank=len(standby_presets) + 1,
                scanner_rank=_opt_int(e.get("rank")) or s_idx,
                active_sess=active_sess,
                regime_int=regime_int,
                vu=vu,
                venue_profile=vp,
                b_row=b_map.get(sym, {}),
                live_eligible=False,
                action_gate_override="MONITOR_ONLY (<EXTREME GATE)",
                now_utc=ts_utc,
            )
            s_row["staged"] = False
            s_row["semi_auto_confirmed"] = False
            s_row["qty"] = 1
            standby_presets.append(s_row)

    raw_feeds = [str(x) for x in (movers.get("feed_sources") or []) if x]
    admitted = [f for f in raw_feeds if f in ADMITTED_FEED_SOURCES]
    pins = [str(s).strip().upper() for s in (movers.get("pin_symbols") or ()) if str(s).strip()]
    venue_handles = build_venue_order_handles_matrix(session=active_sess)
    return {
        "schema_version": "tier0.live-half-auto-tickets.v1",
        "generated_at_et": now_et.isoformat(),
        "active_session": active_sess,
        "venue": vu,
        "venue_profile": vp,
        "venue_order_handle": venue_handles.get(
            vp, resolve_venue_order_handle(vp, session=active_sess)
        ),
        "venue_order_handles": venue_handles,
        "movers_generated_at_utc": movers.get("generated_at_utc"),
        "macro_regime": macro,
        "pin_symbols": pins,
        "feed_sources": admitted,
        "upstream_unadmitted_feed_count": len(raw_feeds) - len(admitted),
        "charter_guards": {
            "half_automation_ungated": True,
            "live_execution_enabled": False,
            "semi_auto_confirmed": False,
            "qty": 1,
        },
        "schema_validated_count": schema_ok,
        "tickets": tickets,
        "standby_presets": standby_presets,
    }


def _format_trf_carryover_notice(rows: Sequence[Mapping[str, Any]]) -> str:
    carry = [
        r
        for r in rows
        if isinstance(r, Mapping)
        and str(r.get("darkpool_trf_source") or "").strip() == "prior_sip_trf_carryover"
    ]
    if not carry:
        return ""
    as_ofs = sorted(
        {str(r["darkpool_trf_as_of_utc"]).strip() for r in carry if r.get("darkpool_trf_as_of_utc")}
    )
    ages = [
        age
        for r in carry
        if (age := _opt_float(r.get("darkpool_trf_age_hours"), min_val=0.0)) is not None
    ]
    detail_bits: list[str] = []
    if as_ofs:
        detail_bits.append(f"as of `{', '.join(as_ofs)}`")
    if ages:
        lo, hi = min(ages), max(ages)
        age_txt = f"{lo:.1f}h" if abs(hi - lo) < 0.05 else f"{lo:.1f}h–{hi:.1f}h"
        detail_bits.append(f"age `{age_txt}`")
    detail = f" ({', '.join(detail_bits)})" if detail_bits else ""
    return (
        f"- **Darkpool TRF Carryover:** `{len(carry)}` symbol(s) carry "
        f"`prior_sip_trf_carryover`{detail} — prior SIP session FINRA TRF snapshot, "
        "not live overnight BOATS darkpool prints.\n"
    )


def _render_ticket_row(t: Mapping[str, Any]) -> str:
    pair_tag = f" (`↔{t['pair_symbol']}`)" if t.get("pair_symbol") else ""
    prior_tag = f" prev `{t['prior_close']:.2f}`" if t.get("prior_close") is not None else ""
    fee_tag = (
        f" · fee {t['annual_borrow_rate'] * 100.0:.2f}%/yr" if t.get("annual_borrow_rate") else ""
    )
    lane_tag = f" · `{t['paper_lane_account']}`" if t.get("paper_lane_account") else ""
    trf_val = _opt_float(t.get("darkpool_trf_ratio"), min_val=0.0)
    trf_src = str(t.get("darkpool_trf_source") or "").strip()
    trf_as_of = str(t.get("darkpool_trf_as_of_utc") or "").strip()
    trf_age_h = _opt_float(t.get("darkpool_trf_age_hours"), min_val=0.0)
    if trf_src == "prior_sip_trf_carryover":
        if trf_age_h is not None:
            trf_suffix = f" (carry {trf_age_h:.1f}h)"
        elif trf_as_of:
            trf_suffix = f" (carry {trf_as_of[:16]})"
        else:
            trf_suffix = " (carry)"
    else:
        trf_suffix = ""
    trf_txt = f"{trf_val * 100.0:.0f}%{trf_suffix}" if trf_val is not None else "—"
    uv_bits: list[str] = []
    vbr = _opt_float(t.get("vol_buildup_ratio"), min_val=0.0)
    if t.get("unusual_volume_buildup") is True:
        uv_bits.append(f"UV {vbr:.1f}x" if vbr is not None else "UV")
    elif vbr is not None:
        uv_bits.append(f"{vbr:.1f}x")
    hvr = _opt_float(t.get("hidden_volume_ratio"), min_val=0.0)
    if hvr is not None and hvr > 0.0:
        uv_bits.append(f"HVR {hvr:.1f}")
    blk = _opt_int(t.get("darkpool_block_count"), min_val=1)
    if blk is not None:
        uv_bits.append(f"blk {blk}")
    trf_uv_cell = f"`{trf_txt}`" + (f" ({' · '.join(uv_bits)})" if uv_bits else "")
    stop_lim = _opt_float(t.get("stop_limit_now"), min_val=0.0001)
    stop_lim_tag = f" lim `{stop_lim:.2f}`" if stop_lim is not None else ""
    return (
        f"| {t['rank']} | `{t['symbol']}`{pair_tag} | `{t['side']}` | **{t['action_gate']}** "
        f"| `{t['live_price']:.2f}` ({t['session_move_pct']:+.2f}%{prior_tag}) "
        f"| `{t['rvol']:.1f}x` | {trf_uv_cell} "
        f"| `{t['stop_now']:.2f}` ({t['stop_now_pct']:.2f}%{stop_lim_tag}) "
        f"| `{t['be_plus_fee_floor']:.2f}` | `{t['lock_at_1r']:.2f}` / `{t['lock_at_2r']:.2f}` "
        f"/ `{t.get('lock_at_3r', t['lock_at_2r']):.2f}` "
        f"| `{t['fast_sell_rev_trigger']:.2f}` ({t['reverse_short_mode']}) "
        f"| `{t['shares_25k_defensive']}` / `{t['shares_100k_defensive']}` "
        f"| `{t['order_type_now']}` ({t['collar_now_bps']}bp{fee_tag}{lane_tag}) |"
    )


def render_tickets_markdown(doc: Mapping[str, Any]) -> str:
    """Render concise operator Markdown order ticket table + Pine alerts + standby + engine sync."""
    macro, sess = doc.get("macro_regime") or {}, doc.get("active_session", "RTH")
    vp = str(doc.get("venue_profile") or resolve_venue_profile(str(doc.get("venue") or "IBKR"))[0])
    raw_vh = doc.get("venue_order_handle")
    vh = (
        dict(raw_vh)
        if isinstance(raw_vh, Mapping)
        else resolve_venue_order_handle(vp, session=str(sess))
    )
    rows = [_render_ticket_row(t) for t in doc.get("tickets", [])]
    body = (
        "\n".join(rows) or "| — | — | — | No extreme movers | — | — | — | — | — | — | — | — | — |"
    )
    regime_label = macro.get("regime_label", "NEUTRAL")
    regime_val = macro.get("regime", 0)
    n_tickets = len(doc.get("tickets", []))
    table_hdr = (
        f"| # | Sym | Side | Action Gate | Live Px (Sess) | RVOL | TRF / UV | Stop ({sess}) "
        "| BE+10bp | Lock +1R / +2R / +3R | Fast-Sell Rev | Sh ($25k/$100k) | Order |\n"
        "|---:|---|---|---|---:|---:|---|---:|---:|---:|---|---:|---|\n"
    )
    manual_staged = [
        p
        for p in doc.get("manual_staged_presets", [])
        if isinstance(p, Mapping) and p.get("symbol")
    ]
    manual_sec = ""
    if manual_staged:
        m_hdr = (
            "| Alert ID | Sym | Side | Venue | Sess | Order | Trigger | Stop | Collar | "
            "Schedule | Kind / Preset | Source | Confirmed |\n"
            "|---|---|---|---|---|---|---:|---:|---:|---|---|---|---|\n"
        )
        m_rows: list[str] = []
        for p in manual_staged:
            kind_lbl = p.get("script_kind") or "indicator"
            preset_lbl = p.get("order_preset_id") or p.get("setup_archetype") or "—"
            collar_f = float(p.get("limit_collar_bps", 0.0))
            v_lbl = p.get("venue_profile") or p.get("venue", "IBKR")
            m_rows.append(
                f"| `{p.get('alert_id', '—')}` | `{p['symbol']}` | `{p.get('side', '—')}` "
                f"| `{v_lbl}` | `{p.get('session', '—')}` "
                f"| `{p.get('order_type', '—')}` | `{float(p.get('trigger_price', 0.0)):.2f}` "
                f"| `{float(p.get('stop_price', 0.0)):.2f}` | `{collar_f:.0f}bp` "
                f"| `{p.get('trailing_schedule', '—')}` | `{kind_lbl}` / `{preset_lbl}` "
                f"| `{p.get('data_source', 'tradingview_pine_v6')}` | `false` |"
            )
        manual_sec = (
            "\n## Pine / Manual Staged Alerts (`staged_tv_presets.json`, `qty=1`, "
            "`semi_auto_confirmed=false`)\n\n"
            f"{m_hdr}" + "\n".join(m_rows) + "\n"
        )

    standby = [s for s in doc.get("standby_presets", []) if isinstance(s, Mapping)]
    standby_sec = ""
    if standby:
        s_body = "\n".join(_render_ticket_row(s) for s in standby)
        standby_sec = (
            "\n## Standby Ranked Reference Presets "
            "(MONITOR_ONLY — Exact Rule-612 Order Panel Levels)\n\n"
            f"{table_hdr}{s_body}\n"
        )

    raw_coord = doc.get("paper_farm_coordination")
    coord: Mapping[str, Any] = raw_coord if isinstance(raw_coord, Mapping) else {}
    active_pos = [p for p in (coord.get("active_positions") or ()) if isinstance(p, Mapping)]
    coord_presets = [c for c in (coord.get("coordinated_presets") or ()) if isinstance(c, Mapping)]
    raw_handles_map = doc.get("venue_order_handles")
    handles_map: Mapping[str, Any] = (
        raw_handles_map
        if isinstance(raw_handles_map, Mapping)
        else build_venue_order_handles_matrix(session=str(sess))
    )
    exec_st = coord.get("execution_status") or "OFFLINE_OR_IDLE"
    streams_txt = ", ".join(str(x) for x in (coord.get("streams") or ())) or "—"
    coord_ok_lbl = str(bool(coord.get("coordination_manifest_present"))).lower()
    coord_lines = [
        "",
        "## Active Hosted Engine 24/5 Trailing Positions & Order-Handle Coordination",
        "",
        (
            f"- **Engine Status:** `{exec_st}` | **Active Trailed Positions:** `{len(active_pos)}` "
            f"| **Coordinated TV Presets:** `{len(coord_presets)}` "
            f"| **Streams:** `{streams_txt}` "
            f"| **Coord Manifest:** `{coord_ok_lbl}`"
        ),
        "",
        (
            "| Sym | Side | Lane | Venue Profile | Entry | Active Stop "
            "| Lock Tier | Session | Schedule |"
        ),
        "|---|---|---|---|---:|---:|---|---|---|",
    ]
    if active_pos:
        for pos in active_pos:
            e_p = _opt_float(pos.get("entry_price"))
            s_p = _opt_float(pos.get("active_stop"))
            e_cell = f"`{e_p:.2f}`" if e_p is not None else "—"
            s_cell = f"`{s_p:.2f}`" if s_p is not None else "—"
            coord_lines.append(
                f"| `{pos.get('symbol', '—')}` | `{pos.get('side', '—')}` "
                f"| `{pos.get('lane_account', '—')}` | `{pos.get('venue_profile', '—')}` "
                f"| {e_cell} | {s_cell} | `{pos.get('lock_tier') or '—'}` "
                f"| `{pos.get('session') or sess}` | `{pos.get('trailing_schedule') or '—'}` |"
            )
    else:
        coord_lines.append(
            "| — | — | — | No active hosted positions (supervising) | — | — | — | — | — |"
        )

    if coord_presets:
        coord_lines.extend(
            [
                "",
                "| Alert ID | Sym | Side | Target Lane | Venue Profile | State |",
                "|---|---|---|---|---|---|",
            ]
        )
        for cp in coord_presets:
            cp_lane = (
                cp.get("target_account")
                or cp.get("lane_account")
                or cp.get("target_paper_lane")
                or "—"
            )
            cp_state = cp.get("discipline_gate") or cp.get("status") or cp.get("state") or "STAGED"
            coord_lines.append(
                f"| `{cp.get('alert_id', '—')}` | `{cp.get('symbol', '—')}` "
                f"| `{cp.get('side', '—')}` | `{cp_lane}` "
                f"| `{cp.get('venue_profile', vp)}` | `{cp_state}` |"
            )

    coord_lines.extend(
        [
            "",
            (
                "| Venue Profile | Env | Max RPM | Entry/Mod RPM | Exit Reserve (>=10%) "
                "| Max Pos | Cadence | Nautilus Rate | Exec Now |"
            ),
            "|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for prof_key in CANONICAL_TV_VENUE_PROFILES:
        spec = handles_map.get(prof_key)
        if not isinstance(spec, Mapping):
            continue
        coord_lines.append(
            f"| `{spec['venue_profile']}` | `{spec['environment']}` | `{spec['max_rpm']}` "
            f"| `{spec['entry_modify_rpm_budget']}` | `{spec['exit_reserve_rpm']}` "
            f"| `{spec['max_concurrent_trailed_positions']}` "
            f"| `{spec['trailing_modify_cadence_secs']}s` "
            f"| `{spec['nautilus_submit_rate']}` "
            f"| `{str(bool(spec.get('executable_now'))).lower()}` |"
        )
    coord_sec = "\n".join(coord_lines) + "\n"

    template_footer = (
        "\n## Native TradingView Order Preset Template\n"
        "- **OVERNIGHT (20:00–04:00 ET):** `STOP_LIMIT` (`50 bps` collar) · SL `<= 1.00%` · "
        "Extended Hours `ON` · `qty=1` (`semi_auto_confirmed=false`) · "
        "Alpaca SHORT blocked → `INVERSE_HEDGE_SOXS_SQQQ` or `IBKR`\n"
        "- **PRE_MARKET / AFTER_HOURS (04:00–09:30 / 16:00–20:00 ET, 13:00 early close):** "
        "`STOP_LIMIT` (`35 bps` collar) · SL `<= 1.25%` (`tight`) / `<= 1.50%` (`baseline`/`wide`) "
        "· Extended Hours `ON`\n"
        "- **RTH (09:30–16:00 ET, 13:00 early close):** `STOP_MARKET` (`0 bps` collar) · "
        "SL `<= 1.50%` (`tight`) / `<= 2.00%` (`baseline`/`wide`) · Extended Hours `OFF`\n"
        "- **4-Tier Progressive R-Lock Brackets:** Arm `BE+10bp` (T1) → `+1R` lock `+0.45R` (T2) → "
        "`+2R` lock `+1.35R` (T3) → `+3R` lock `+2.35R` / Fast-Sell Reverse (T4) · `60s` cooldown\n"
    )
    carry_notice = _format_trf_carryover_notice(
        [*doc.get("tickets", []), *doc.get("standby_presets", [])]
    )
    return (
        "# Tier-0 LIVE Half-Automation Order Ticket Console\n\n"
        f"- **Generated (ET):** `{doc.get('generated_at_et')}` | **Session:** `{sess}` "
        f"| **Regime:** `{regime_label} ({regime_val})`\n"
        f"- **Venue Profile:** `{vp}` (`{vh['max_rpm']} rpm` · "
        f"`{vh['entry_modify_rpm_budget']}` entry/mod + `{vh['exit_reserve_rpm']}` exit reserve · "
        f"`{vh['max_concurrent_trailed_positions']}` max trailed pos · "
        f"`{vh['trailing_modify_cadence_secs']}s` cadence)\n"
        f"- **Schema Validated:** `{doc.get('schema_validated_count')}/{n_tickets}` "
        "| `semi_auto_confirmed=false` | `live_execution_enabled=false`\n"
        f"{carry_notice}\n"
        f"{table_hdr}{body}\n"
        f"{manual_sec}"
        f"{standby_sec}"
        f"{coord_sec}"
        f"{template_footer}"
    )


def render_tv_watchlist(
    tickets: Sequence[Mapping[str, Any]],
    *,
    exchange_map: Mapping[str, str] | None = None,
    ranked_entries: Sequence[Mapping[str, Any]] | None = None,
    staged_presets: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    """Render grouped TV watchlist (`###01..###06` + `###07` manual staged) with EXCHANGE:SYMBOL."""

    def _fmt_row(row: Mapping[str, Any]) -> str:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            return ""
        return resolve_tv_exchange_symbol(
            sym,
            exchange=row.get("exchange") or row.get("tv_exchange"),
            exchange_map=exchange_map,
        )

    def _fmt_sym(sym: str) -> str:
        return resolve_tv_exchange_symbol(sym, exchange_map=exchange_map)

    live_ranked = [
        e
        for e in (ranked_entries or ())
        if isinstance(e, Mapping)
        and str(e.get("symbol", "")).strip()
        and _opt_float(e.get("last_price"), min_val=0.0001) is not None
        and (
            str(e.get("extreme_mover_hold_reason") or "").strip() not in LOW_EXTREME_HOLD_REASONS
            or str(e.get("watch_tier") or "").strip().upper()
            in {"UV_DARKPOOL_BUILDUP", "OVERNIGHT_ACTIVE"}
        )
    ]
    staged_rows = [
        p
        for p in (staged_presets or ())
        if isinstance(p, Mapping) and str(p.get("symbol", "")).strip()
    ]
    combined_rows = (*tickets, *live_ranked, *staged_rows)

    movers = list(dict.fromkeys(tvs for row in combined_rows if (tvs := _fmt_row(row))))[:15]
    manual_staged_syms = list(dict.fromkeys(tvs for row in staged_rows if (tvs := _fmt_row(row))))
    darkpool_uv = list(
        dict.fromkeys(
            tvs
            for row in combined_rows
            if (tvs := _fmt_row(row))
            and (
                (_opt_float(row.get("darkpool_trf_ratio"), min_val=0.0) or 0.0) >= 0.35
                or row.get("unusual_volume_buildup") is True
                or str(row.get("watch_tier") or "").strip().upper() == "UV_DARKPOOL_BUILDUP"
                or (
                    _opt_float(
                        row.get("rvol")
                        if row.get("rvol") is not None
                        else row.get("rvol_normalized"),
                        min_val=0.0,
                    )
                    or 0.0
                )
                >= 2.0
                or (_opt_float(row.get("hidden_volume_ratio"), min_val=0.0) or 0.0) >= 1.15
            )
        )
    )
    sections = [
        "###01_MACRO_REGIME_BREADTH",
        "AMEX:SPY",
        "NASDAQ:QQQ",
        "AMEX:IWM",
        "AMEX:RSP",
        "NASDAQ:SOXX",
        "CBOE:VIX",
        "NASDAQ:TLT",
        "###02_CRASH_DEFENSIVE_HEDGES",
        "AMEX:SOXS",
        "NASDAQ:SQQQ",
        "AMEX:SPXS",
        "AMEX:SDOW",
        "###03_LIVE_MOVERS_TOP15",
        *movers,
        "###04_DARKPOOL_UNUSUAL_VOL",
        *darkpool_uv,
        "###05_BIDIRECTIONAL_PAIRS",
        "AMEX:SOXL",
        "AMEX:SOXS",
        "NASDAQ:TQQQ",
        "NASDAQ:SQQQ",
        "AMEX:UPRO",
        "AMEX:SPXS",
        "###06_OVERNIGHT_24X5_LEADERS",
        *(_fmt_sym(s) for s in OVERNIGHT_24X5_LEADERS),
    ]
    if manual_staged_syms:
        sections.extend(["###07_MANUAL_STAGED_PRESETS", *manual_staged_syms])
    return "\n".join(sections) + "\n"


def _atomic_write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)
    return path


def sync_pine_scripts_to_pack(
    out_dir: Path, *, repo_pine_dir: Path | None = None
) -> dict[str, str]:
    """Sync all canonical Pine v6 scripts into out_dir and return filename -> SHA-256 map."""
    src_dir = repo_pine_dir if repo_pine_dir is not None else _REPO_PINE_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    shas: dict[str, str] = {}
    if not src_dir.is_dir():
        return shas
    ordered: list[Path] = []
    seen: set[str] = set()
    for name in CANONICAL_PINE_SCRIPTS:
        cand = src_dir / name
        if cand.is_file():
            ordered.append(cand)
            seen.add(name)
    for cand in sorted(src_dir.glob("*.pine")):
        if cand.name not in seen and cand.is_file():
            ordered.append(cand)
            seen.add(cand.name)
    for src_file in ordered:
        raw_bytes = src_file.read_bytes()
        digest = hashlib.sha256(raw_bytes).hexdigest()
        dest = out_dir / src_file.name
        if not dest.is_file() or hashlib.sha256(dest.read_bytes()).hexdigest() != digest:
            tmp = dest.with_name(f".{dest.name}.{os.getpid()}.tmp")
            tmp.write_bytes(raw_bytes)
            os.replace(tmp, dest)
        shas[src_file.name] = digest
    return shas


def _build_symbol_order_card(
    row: Mapping[str, Any],
    *,
    card_kind: str,
    venue: str,
    venue_profile: str | None = None,
) -> dict[str, Any]:
    sess = str(row.get("active_session") or row.get("session") or "RTH")
    sym = str(row["symbol"]).strip().upper()
    sched = str(row.get("schedule") or row.get("trailing_schedule") or "tight_near_lossless")
    arch = str(row.get("archetype") or row.get("setup_archetype") or "EXTREME_REVERSAL")
    raw_pb = row.get("setup_playbook")
    playbook = (
        dict(raw_pb)
        if isinstance(raw_pb, Mapping)
        else dict(
            MULTI_SITUATION_SETUP_PLAYBOOK.get(
                arch, MULTI_SITUATION_SETUP_PLAYBOOK["EXTREME_REVERSAL"]
            )
        )
    )
    vp, vu = resolve_venue_profile(
        str(row.get("venue") or venue),
        venue_profile=str(row.get("venue_profile") or venue_profile or "") or None,
    )
    raw_vh = row.get("venue_order_handle")
    vh = (
        dict(raw_vh)
        if isinstance(raw_vh, Mapping)
        else resolve_venue_order_handle(vp, session=sess, trailing_schedule=sched)
    )
    return {
        "card_kind": card_kind,
        "symbol": sym,
        "tv_symbol": str(row.get("tv_symbol") or resolve_tv_exchange_symbol(sym)),
        "side": str(row["side"]),
        "qty": 1,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "venue": vu,
        "venue_profile": vp,
        "venue_order_handle": vh,
        "session": sess,
        "extended_hours": sess != "RTH",
        "order_type": str(row.get("order_type_now") or row.get("order_type") or "STOP_LIMIT"),
        "limit_collar_bps": _opt_float(
            row.get("collar_now_bps")
            if row.get("collar_now_bps") is not None
            else row.get("limit_collar_bps"),
            min_val=0.0,
        )
        or 0.0,
        "trailing_schedule": sched,
        "order_preset_id": row.get("order_preset_id"),
        "archetype": arch,
        "recommended_omni_preset": str(
            row.get("recommended_omni_preset") or playbook["recommended_omni_preset"]
        ),
        "target_paper_lane": str(
            row.get("target_paper_lane")
            or row.get("paper_lane_account")
            or playbook["target_paper_lane"]
        ),
        "target_lane_id": str(
            row.get("target_lane_id") or row.get("paper_lane_id") or playbook["target_lane_id"]
        ),
        "setup_playbook": playbook,
        "action_gate": row.get("action_gate") or "STAGED_PINE_ALERT",
        "entry_ref_price": _opt_float(row.get("live_price") or row.get("trigger_price")),
        "entry_limit_price": _opt_float(
            row.get("entry_limit_now") or row.get("live_price") or row.get("trigger_price")
        ),
        "stop_trigger_price": _opt_float(row.get("stop_now") or row.get("stop_price")),
        "stop_limit_price": _opt_float(row.get("stop_limit_now")),
        "stop_distance_pct": _opt_float(row.get("stop_now_pct")),
        "be_plus_fee_floor": _opt_float(row.get("be_plus_fee_floor")),
        "target_1r": _opt_float(row.get("target_1r")),
        "lock_at_1r": _opt_float(row.get("lock_at_1r")),
        "target_2r": _opt_float(row.get("target_2r")),
        "lock_at_2r": _opt_float(row.get("lock_at_2r")),
        "target_3r": _opt_float(row.get("target_3r")),
        "lock_at_3r": _opt_float(row.get("lock_at_3r")),
        "fast_sell_rev_trigger": _opt_float(row.get("fast_sell_rev_trigger")),
        "reentry_cooldown_secs": int(row.get("reentry_cooldown_secs") or 60),
        "reverse_short_mode": row.get("reverse_short_mode"),
        "pair_symbol": row.get("pair_symbol"),
        "shares_25k_defensive": _opt_int(row.get("shares_25k_defensive")),
        "shares_100k_defensive": _opt_int(row.get("shares_100k_defensive")),
        "rvol": _opt_float(row.get("rvol")),
        "darkpool_trf_ratio": _opt_float(row.get("darkpool_trf_ratio")),
        "darkpool_trf_source": row.get("darkpool_trf_source"),
        "darkpool_trf_as_of_utc": row.get("darkpool_trf_as_of_utc"),
        "darkpool_trf_age_hours": _opt_float(row.get("darkpool_trf_age_hours"), min_val=0.0),
        "darkpool_block_count": _opt_int(row.get("darkpool_block_count")),
        "trf_block_ratio": _opt_float(row.get("trf_block_ratio")),
        "hidden_volume_ratio": _opt_float(row.get("hidden_volume_ratio")),
        "vol_buildup_ratio": _opt_float(row.get("vol_buildup_ratio")),
        "unusual_volume_buildup": bool(row.get("unusual_volume_buildup") is True),
        "pinned": bool(row.get("pinned") is True),
        "borrow_source": row.get("borrow_source"),
        "short_fee_bps": _opt_float(row.get("short_fee_bps"), min_val=0.0),
        "watch_tier": row.get("watch_tier"),
        "paper_lane_account": row.get("paper_lane_account"),
        "paper_lane_id": row.get("paper_lane_id"),
        "paper_origin": row.get("paper_origin"),
    }


def build_venue_order_handles(
    *,
    alpaca_venue_profile: str | None = None,
    session: str = "RTH",
    trailing_schedule: str = "tight_near_lossless",
) -> dict[str, Any]:
    """Build Paper-only + fail-closed SANDBOX venue order-handle parity spec from engine SSOT."""
    active_alp_vp, _ = resolve_venue_profile("ALPACA", venue_profile=alpaca_venue_profile)
    if active_alp_vp not in {"ALPACA_PAPER_200", "ALPACA_PAPER_1000"}:
        active_alp_vp = "ALPACA_PAPER_200"
    profiles = build_venue_order_handles_matrix(
        session=session, trailing_schedule=trailing_schedule
    )
    alp200 = _eng.inspect_venue_capability("ALPACA_PAPER_200")
    alp1000 = _eng.inspect_venue_capability("ALPACA_PAPER_1000")
    alp = alp1000 if active_alp_vp == "ALPACA_PAPER_1000" else alp200
    ibkr = _eng.inspect_venue_capability("IBKR_PAPER_4002")
    pro_spec = _eng.inspect_venue_capability("ALPACA_LIVE_PRO_LINE_1000")
    alp_lanes = len(PAPER_ALPACA_LANE_ACCOUNTS)
    ibkr_lanes = len(PAPER_IBKR_LANE_ACCOUNTS)
    alp_combined_rpm = int(alp["max_rpm"]) * alp_lanes
    alp_combined_exit_rpm = int(alp["exit_reserve_rpm"]) * alp_lanes
    alp_combined_entry_rpm = int(alp["active_entry_rpm"]) * alp_lanes
    alp200_combined_rpm = int(alp200["max_rpm"]) * alp_lanes
    alp200_combined_exit_rpm = int(alp200["exit_reserve_rpm"]) * alp_lanes
    alp200_combined_entry_rpm = int(alp200["active_entry_rpm"]) * alp_lanes
    alp1000_combined_rpm = int(alp1000["max_rpm"]) * alp_lanes
    alp1000_combined_exit_rpm = int(alp1000["exit_reserve_rpm"]) * alp_lanes
    alp1000_combined_entry_rpm = int(alp1000["active_entry_rpm"]) * alp_lanes
    ibkr_combined_rpm = int(ibkr["max_rpm"]) * ibkr_lanes
    ibkr_combined_exit_rpm = int(ibkr["exit_reserve_rpm"]) * ibkr_lanes
    ibkr_combined_entry_rpm = int(ibkr["active_entry_rpm"]) * ibkr_lanes
    return {
        "source": "tier0_sota.engine.inspect_venue_capability",
        "spec_source": "tier0_sota.engine.build_venue_order_handle_spec",
        "session": session,
        "trailing_schedule": trailing_schedule,
        "paper_only": True,
        "live_execution_enabled": False,
        "active_alpaca_profile": active_alp_vp,
        "combined_paper_farm_max_rpm": alp_combined_rpm + ibkr_combined_rpm,
        "combined_paper_farm_exit_reserve_rpm": alp_combined_exit_rpm + ibkr_combined_exit_rpm,
        "combined_paper_farm_active_entry_rpm": alp_combined_entry_rpm + ibkr_combined_entry_rpm,
        "combined_paper_farm_max_rpm_at_1000": alp1000_combined_rpm + ibkr_combined_rpm,
        "combined_paper_farm_exit_reserve_rpm_at_1000": (
            alp1000_combined_exit_rpm + ibkr_combined_exit_rpm
        ),
        "profiles": profiles,
        "active_paper_handles": {
            "ALPACA_PAPER_1000": {
                **profiles["ALPACA_PAPER_1000"],
                **alp1000,
                "paper_only": True,
                "live_execution_enabled": False,
                "lane_count": alp_lanes,
                "lane_accounts": list(PAPER_ALPACA_LANE_ACCOUNTS),
                "combined_max_rpm": alp1000_combined_rpm,
                "combined_exit_reserve_rpm": alp1000_combined_exit_rpm,
                "combined_active_entry_rpm": alp1000_combined_entry_rpm,
                "execution_status": (
                    "PAPER_ACTIVE"
                    if active_alp_vp == "ALPACA_PAPER_1000"
                    else "PAPER_OPT_IN_1000RPM"
                ),
            },
            "ALPACA_PAPER_200": {
                **profiles["ALPACA_PAPER_200"],
                **alp200,
                "paper_only": True,
                "live_execution_enabled": False,
                "lane_count": alp_lanes,
                "lane_accounts": list(PAPER_ALPACA_LANE_ACCOUNTS),
                "combined_max_rpm": alp200_combined_rpm,
                "combined_exit_reserve_rpm": alp200_combined_exit_rpm,
                "combined_active_entry_rpm": alp200_combined_entry_rpm,
                "execution_status": (
                    "PAPER_ACTIVE"
                    if active_alp_vp == "ALPACA_PAPER_200"
                    else "PAPER_FALLBACK_200RPM"
                ),
            },
            "IBKR_PAPER_4002": {
                **profiles["IBKR_PAPER_4002"],
                **ibkr,
                "paper_only": True,
                "live_execution_enabled": False,
                "lane_count": ibkr_lanes,
                "lane_accounts": list(PAPER_IBKR_LANE_ACCOUNTS),
                "combined_max_rpm": ibkr_combined_rpm,
                "combined_exit_reserve_rpm": ibkr_combined_exit_rpm,
                "combined_active_entry_rpm": ibkr_combined_entry_rpm,
                "execution_status": "PAPER_ACTIVE",
            },
        },
        "stage5_sandbox_reference_specs": {
            "ALPACA_LIVE_PRO_LINE_1000": {
                **profiles["ALPACA_LIVE_PRO_LINE_1000"],
                **pro_spec,
                "paper_only": True,
                "live_execution_enabled": False,
                "execution_status": "LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC",
                "stage5_readiness_spec_only": True,
            },
        },
    }


def build_tv_order_presets(
    doc: Mapping[str, Any],
    *,
    manual_staged_presets: Sequence[Mapping[str, Any]] = (),
    paper_coordination: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build declarative `tv_order_presets.json` (3x4 NATR matrix, 8 Omni modes, symbol cards)."""
    active_sess = str(doc.get("active_session") or "CLOSED")
    raw_vp = str(doc.get("venue_profile") or "").strip().upper() or None
    vp, venue = resolve_venue_profile(str(doc.get("venue") or "IBKR"), venue_profile=raw_vp)
    session_matrix: dict[str, dict[str, dict[str, Any]]] = {}
    for sched_name, sess_map in _eng.POOLED_TRAILING_SCHEDULES.items():
        session_matrix[sched_name] = {
            sess_name: {
                "order_mode": prof.order_mode,
                "initial_max_loss_pct": float(prof.initial_max_loss_pct),
                "breakeven_trigger_pct": float(prof.breakeven_trigger_pct),
                "min_trail_pct": float(prof.min_trail_pct),
                "natr_trail_mult": float(prof.natr_trail_mult),
                "limit_collar_bps": float(prof.limit_collar_bps),
                "max_notional_scale": float(prof.max_notional_scale),
                "outside_rth": bool(prof.outside_rth),
            }
            for sess_name, prof in sess_map.items()
        }

    cards: list[dict[str, Any]] = []
    for t in doc.get("tickets", []):
        if isinstance(t, Mapping) and t.get("symbol"):
            cards.append(
                _build_symbol_order_card(
                    t, card_kind="ACTIVE_TICKET", venue=venue, venue_profile=vp
                )
            )
    for p in manual_staged_presets:
        if isinstance(p, Mapping) and p.get("symbol"):
            cards.append(
                _build_symbol_order_card(p, card_kind="PINE_STAGED", venue=venue, venue_profile=vp)
            )
    for s in doc.get("standby_presets", []):
        if isinstance(s, Mapping) and s.get("symbol"):
            cards.append(
                _build_symbol_order_card(
                    s, card_kind="STANDBY_REFERENCE", venue=venue, venue_profile=vp
                )
            )

    alp_vp = vp if vp in {"ALPACA_PAPER_200", "ALPACA_PAPER_1000"} else None
    raw_coord = (
        paper_coordination if paper_coordination is not None else doc.get("paper_farm_coordination")
    )
    coord: dict[str, Any] = dict(raw_coord) if isinstance(raw_coord, Mapping) else {}
    active_positions = [
        dict(p) for p in (coord.get("active_positions") or ()) if isinstance(p, Mapping)
    ]
    return {
        "schema_version": "tier0.tv-order-presets.v1",
        "generated_at_et": doc.get("generated_at_et"),
        "active_session": active_sess,
        "venue": venue,
        "venue_profile": vp,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "macro_regime": dict(doc.get("macro_regime") or {}),
        "venue_order_handles": build_venue_order_handles(
            alpaca_venue_profile=alp_vp, session=active_sess
        ),
        "progressive_r_locks": [
            {"tier": "T1_BE_PLUS_FEE", "trigger": "fav >= be_trig", "floor": "entry +/- 10 bps"},
            {"tier": "T2_1R_LOCK", "trigger": "+1.0R", "floor": "+0.45R", "trail_scale": 0.65},
            {"tier": "T3_2R_LOCK", "trigger": "+2.0R", "floor": "+1.35R", "trail_scale": 0.40},
            {
                "tier": "T4_3R_FAST_SELL",
                "trigger": "+3.0R or climax",
                "floor": "+2.35R",
                "trail_scale": 0.25,
                "reentry_cooldown_secs": 60,
            },
        ],
        "session_matrix": session_matrix,
        "multi_situation_playbook": {k: dict(v) for k, v in MULTI_SITUATION_SETUP_PLAYBOOK.items()},
        "omni_presets": [dict(p) for p in OMNI_ORDER_PRESET_CATALOG],
        "active_engine_positions": active_positions,
        "engine_coordination": coord,
        "symbol_order_cards": cards,
    }


def render_order_preset_cards_markdown(presets_doc: Mapping[str, Any]) -> str:
    """Render copy-ready TradingView Order Panel preset cards Markdown."""
    sess = presets_doc.get("active_session", "CLOSED")
    macro = presets_doc.get("macro_regime") or {}
    cards = [c for c in presets_doc.get("symbol_order_cards", []) if isinstance(c, Mapping)]
    carry_notice = _format_trf_carryover_notice(cards).rstrip("\n")
    lines = [
        "# Tier-0 TradingView SOTA Order Preset Cards",
        "",
        f"- **Generated (ET):** `{presets_doc.get('generated_at_et')}` | **Session:** `{sess}` "
        f"| **Regime:** `{macro.get('regime_label', 'NEUTRAL')} ({macro.get('regime', 0)})`",
        (
            "- **Guards:** `qty=1` | `semi_auto_confirmed=false` "
            "| `live_execution_enabled=false` | `paper_only=true`"
        ),
    ]
    if carry_notice:
        lines.append(carry_notice)
    lines.extend(
        [
            "",
            "## Per-Symbol TradingView Order Panel Cards",
            "",
            (
                "| Kind | TV Symbol | Side | Gate | Entry / Limit | Stop / Stop-Limit "
                "| BE+10bp | +1R / +2R / +3R Lock | Fast-Sell Rev | TRF / Source "
                "| Order | Paper Lane |"
            ),
            "|---|---|---|---|---:|---:|---:|---:|---:|---|---|---|",
        ]
    )
    if not cards:
        lines.append("| — | — | — | No active/standby symbols | — | — | — | — | — | — | — | — |")
    else:
        for c in cards:
            e_px = c.get("entry_ref_price")
            e_lim = c.get("entry_limit_price")
            s_px = c.get("stop_trigger_price")
            s_lim = c.get("stop_limit_price")
            be = c.get("be_plus_fee_floor")
            l1, l2, l3 = c.get("lock_at_1r"), c.get("lock_at_2r"), c.get("lock_at_3r")
            fsr = c.get("fast_sell_rev_trigger")
            entry_cell = f"`{e_px:.2f}`" if e_px is not None else "—"
            if e_lim is not None and e_px is not None and abs(e_lim - e_px) > 1e-6:
                entry_cell += f" / `{e_lim:.2f}`"
            stop_cell = f"`{s_px:.2f}`" if s_px is not None else "—"
            if s_lim is not None:
                stop_cell += f" / `{s_lim:.2f}`"
            lock_cell = (
                f"`{l1:.2f}` / `{l2:.2f}` / `{l3:.2f}`"
                if all(v is not None for v in (l1, l2, l3))
                else "—"
            )
            be_cell = f"`{be:.2f}`" if be is not None else "—"
            fsr_cell = f"`{fsr:.2f}`" if fsr is not None else "—"
            trf_val = _opt_float(c.get("darkpool_trf_ratio"), min_val=0.0)
            trf_src = str(c.get("darkpool_trf_source") or "").strip()
            trf_age_h = _opt_float(c.get("darkpool_trf_age_hours"), min_val=0.0)
            trf_as_of = str(c.get("darkpool_trf_as_of_utc") or "").strip()
            if trf_val is None:
                trf_cell = "—"
            elif trf_src == "prior_sip_trf_carryover":
                age_part = (
                    f" · {trf_age_h:.1f}h"
                    if trf_age_h is not None
                    else (f" · {trf_as_of[:16]}" if trf_as_of else "")
                )
                trf_cell = f"`{trf_val * 100.0:.0f}%` (`prior_sip_trf_carryover`{age_part})"
            else:
                trf_cell = f"`{trf_val * 100.0:.0f}%`" + (f" (`{trf_src}`)" if trf_src else "")
            ext_lbl = "Ext ON" if c.get("extended_hours") else "RTH"
            collar_f = float(c.get("limit_collar_bps") or 0.0)
            ord_cell = f"`{c.get('order_type')}` ({collar_f:.0f}bp · {ext_lbl})"
            lane_cell = f"`{c.get('paper_lane_account') or '—'}`"
            lines.append(
                f"| `{c['card_kind']}` | `{c['tv_symbol']}` | `{c['side']}` "
                f"| **{c['action_gate']}** | {entry_cell} | {stop_cell} | {be_cell} "
                f"| {lock_cell} | {fsr_cell} | {trf_cell} | {ord_cell} | {lane_cell} |"
            )

    raw_handles = presets_doc.get("venue_order_handles")
    if isinstance(raw_handles, Mapping):
        active_h = raw_handles.get("active_paper_handles")
        spec_h = raw_handles.get("stage5_sandbox_reference_specs")
        comb_rpm = raw_handles.get("combined_paper_farm_max_rpm", 0)
        comb_entry = raw_handles.get("combined_paper_farm_active_entry_rpm", 0)
        comb_exit = raw_handles.get("combined_paper_farm_exit_reserve_rpm", 0)
        pro_comb = raw_handles.get("combined_paper_farm_max_rpm_at_1000", 0)
        pro_exit = raw_handles.get("combined_paper_farm_exit_reserve_rpm_at_1000", 0)
        active_vp_lbl = presets_doc.get("venue_profile") or "IBKR_PAPER_4002"
        lines.extend(
            [
                "",
                "## Venue Order-Handle Parity (Paper Farm + Fail-Closed SANDBOX Spec)",
                "",
                (
                    f"- **Active Venue Profile:** `{active_vp_lbl}` "
                    f"| **Combined Paper Farm Budget:** `{comb_rpm} rpm` "
                    f"(`{comb_entry} rpm` entry + `{comb_exit} rpm` exit reserve) "
                    f"| **Pro-1000 Tier:** `{pro_comb} rpm` (`{pro_exit} rpm` exit reserve) "
                    "| `paper_only=true` | `live_execution_enabled=false`"
                ),
                "",
                (
                    "| Venue Profile | Env | Lanes | Max RPM (Combined) | Max RPS "
                    "| Exit Reserve | Max Pos / Cadence | 24/5 Overnight | Status |"
                ),
                "|---|---|---|---:|---:|---:|---|---|---|",
            ]
        )
        if isinstance(active_h, Mapping):
            for h in active_h.values():
                if not isinstance(h, Mapping):
                    continue
                accts = ", ".join(str(x) for x in (h.get("lane_accounts") or ()))
                lanes_lbl = f"`{h.get('lane_count', 1)}` (`{accts}`)" if accts else "`1`"
                comb_ex = h.get("combined_exit_reserve_rpm", h["exit_reserve_rpm"])
                pos_cad = (
                    f"`{h.get('max_concurrent_trailed_positions', '—')}` / "
                    f"`{h.get('trailing_modify_cadence_secs', '—')}s`"
                )
                lines.append(
                    f"| `{h['venue_id']}` | `{h['environment']}` | {lanes_lbl} "
                    f"| `{h['max_rpm']}` (`{h.get('combined_max_rpm', h['max_rpm'])}`) "
                    f"| `{h['max_rps']}` "
                    f"| `{h['exit_reserve_rpm']}` (`{comb_ex}`) "
                    f"| {pos_cad} "
                    f"| `{str(bool(h.get('supports_extended_overnight_24x5'))).lower()}` "
                    f"| `{h.get('execution_status', 'PAPER_ACTIVE')}` |"
                )
        if isinstance(spec_h, Mapping):
            for s in spec_h.values():
                if not isinstance(s, Mapping):
                    continue
                pos_cad = (
                    f"`{s.get('max_concurrent_trailed_positions', '—')}` / "
                    f"`{s.get('trailing_modify_cadence_secs', '—')}s`"
                )
                lines.append(
                    f"| `{s['venue_id']}` | `{s['environment']}` | `0` (`SANDBOX_SPEC`) "
                    f"| `{s['max_rpm']}` (`0 live`) | `{s['max_rps']}` "
                    f"| `{s['exit_reserve_rpm']}` | {pos_cad} "
                    f"| `{str(bool(s.get('supports_extended_overnight_24x5'))).lower()}` "
                    f"| `{s.get('execution_status', 'LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC')}` |"
                )

    raw_coord = presets_doc.get("engine_coordination")
    coord: Mapping[str, Any] = raw_coord if isinstance(raw_coord, Mapping) else {}
    active_pos = [
        p for p in (presets_doc.get("active_engine_positions") or ()) if isinstance(p, Mapping)
    ]
    lines.extend(
        [
            "",
            "## Active Hosted Engine 24/5 Trailing & Coordination Status",
            "",
            (
                f"- **Execution Status:** `{coord.get('execution_status') or 'OFFLINE_OR_IDLE'}` "
                f"| **Active Trailed Positions:** `{len(active_pos)}` "
                f"| **Coordinated TV Presets:** `{coord.get('coordinated_presets_count', 0)}` "
                f"| **Ready for Confirm:** `{coord.get('ready_for_operator_confirm_count', 0)}` "
                f"| **Coord Manifest Present:** "
                f"`{str(bool(coord.get('coordination_manifest_present'))).lower()}`"
            ),
        ]
    )

    playbook_map = presets_doc.get("multi_situation_playbook")
    if isinstance(playbook_map, Mapping) and playbook_map:
        lines.extend(
            [
                "",
                "## Multi-Situation Setup Playbook",
                "",
                "| Archetype | Recommended Omni Preset | Target Paper Lane | Schedule | Notes |",
                "|---|---|---|---|---|",
            ]
        )
        for pb in playbook_map.values():
            if isinstance(pb, Mapping):
                lines.append(
                    f"| `{pb['archetype']}` | `{pb['recommended_omni_preset']}` "
                    f"| `{pb['target_paper_lane']}` (`{pb['target_lane_id']}`) "
                    f"| `{pb['preferred_schedule']}` | {pb['play_notes']} |"
                )

    lines.extend(
        [
            "",
            "## 8 Canonical TradingView Omni Presets",
            "",
            "| Preset Mode | TF | Min RVOL | Regime Alignment | Short Routing |",
            "|---|---|---:|---|---|",
        ]
    )
    for preset in presets_doc.get("omni_presets", []):
        if isinstance(preset, Mapping):
            lines.append(
                f"| `{preset['preset_id']}` | `{preset['timeframe']}` | `{preset['min_rvol']}x` "
                f"| `{preset['regime_alignment']}` | `{preset['short_routing']}` |"
            )
    return "\n".join(lines) + "\n"


def build_finos_tv_cockpit(
    doc: Mapping[str, Any],
    *,
    compounding: Mapping[str, Any],
    manual_staged_presets: Sequence[Mapping[str, Any]] = (),
    paper_coordination: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build declarative FINOS / OpenBB / Lightweight-Charts v5 TV cockpit JSON (zero HTML/JS)."""
    tickets = [t for t in doc.get("tickets", []) if isinstance(t, Mapping)]
    standby = [s for s in doc.get("standby_presets", []) if isinstance(s, Mapping)]
    openbb_rows: list[dict[str, Any]] = []
    for tier_label, bucket in (("ACTIVE_TICKET", tickets), ("STANDBY_REFERENCE", standby)):
        for r in bucket:
            raw_vh = r.get("venue_order_handle")
            vh_row: Mapping[str, Any] = raw_vh if isinstance(raw_vh, Mapping) else {}
            openbb_rows.append(
                {
                    "tier": tier_label,
                    "rank": r.get("rank"),
                    "scanner_rank": r.get("scanner_rank"),
                    "symbol": r["symbol"],
                    "tv_symbol": r.get("tv_symbol") or resolve_tv_exchange_symbol(r["symbol"]),
                    "side": r["side"],
                    "archetype": r.get("archetype"),
                    "recommended_omni_preset": r.get("recommended_omni_preset"),
                    "target_paper_lane": r.get("target_paper_lane") or r.get("paper_lane_account"),
                    "target_lane_id": r.get("target_lane_id") or r.get("paper_lane_id"),
                    "venue_profile": r.get("venue_profile") or doc.get("venue_profile"),
                    "max_rpm": vh_row.get("max_rpm"),
                    "exit_reserve_rpm": vh_row.get("exit_reserve_rpm"),
                    "order_preset_id": r.get("order_preset_id"),
                    "watch_tier": r.get("watch_tier"),
                    "action_gate": r.get("action_gate"),
                    "live_price": r.get("live_price"),
                    "entry_limit_now": r.get("entry_limit_now"),
                    "session_move_pct": r.get("session_move_pct"),
                    "rvol": r.get("rvol"),
                    "darkpool_trf_ratio": r.get("darkpool_trf_ratio"),
                    "darkpool_trf_source": r.get("darkpool_trf_source"),
                    "darkpool_block_count": r.get("darkpool_block_count"),
                    "trf_block_ratio": r.get("trf_block_ratio"),
                    "hidden_volume_ratio": r.get("hidden_volume_ratio"),
                    "vol_buildup_ratio": r.get("vol_buildup_ratio"),
                    "unusual_volume_buildup": bool(r.get("unusual_volume_buildup") is True),
                    "stop_now": r.get("stop_now"),
                    "stop_limit_now": r.get("stop_limit_now"),
                    "be_plus_fee_floor": r.get("be_plus_fee_floor"),
                    "lock_at_1r": r.get("lock_at_1r"),
                    "lock_at_2r": r.get("lock_at_2r"),
                    "lock_at_3r": r.get("lock_at_3r"),
                    "fast_sell_rev_trigger": r.get("fast_sell_rev_trigger"),
                    "reverse_short_mode": r.get("reverse_short_mode"),
                    "pair_symbol": r.get("pair_symbol"),
                    "paper_lane_account": r.get("paper_lane_account"),
                    "paper_lane_id": r.get("paper_lane_id"),
                    "qty": 1,
                    "semi_auto_confirmed": False,
                    "live_execution_enabled": False,
                }
            )

    syms_x = [r["symbol"] for r in openbb_rows]
    now_sec = int(datetime.now(UTC).timestamp())
    if doc.get("movers_generated_at_utc"):
        with contextlib.suppress(ValueError):
            now_sec = int(datetime.fromisoformat(str(doc["movers_generated_at_utc"])).timestamp())

    lwc_symbols: dict[str, dict[str, Any]] = {}
    for r in (*tickets, *standby):
        sym = str(r["symbol"])
        raw_preset = r.get("tv_alert_preset")
        preset = raw_preset if isinstance(raw_preset, Mapping) else None
        bar_sec = (
            int(preset["ts_utc_ms"]) // 1000 if preset and preset.get("ts_utc_ms") else now_sec
        )
        is_buy = str(r["side"]) == "BUY"
        price_lines = [
            {"id": "entry", "price": r["live_price"], "color": "#3b82f6", "title": "Entry Ref"},
            {
                "id": "hard_stop",
                "price": r["stop_now"],
                "color": "#ef4444",
                "title": "Rule-612 Stop",
            },
            {
                "id": "be_plus_fee",
                "price": r["be_plus_fee_floor"],
                "color": "#eab308",
                "title": "T1 BE+10bp",
            },
            {"id": "lock_1r", "price": r["lock_at_1r"], "color": "#06b6d4", "title": "T2 +1R Lock"},
            {"id": "lock_2r", "price": r["lock_at_2r"], "color": "#22c55e", "title": "T3 +2R Lock"},
            {"id": "lock_3r", "price": r["lock_at_3r"], "color": "#10b981", "title": "T4 +3R Lock"},
            {
                "id": "fast_sell_rev",
                "price": r["fast_sell_rev_trigger"],
                "color": "#f97316",
                "title": "Fast-Sell Rev",
            },
        ]
        if r.get("stop_limit_now") is not None:
            price_lines.append(
                {
                    "id": "stop_limit",
                    "price": r["stop_limit_now"],
                    "color": "#f43f5e",
                    "title": "Stop-Limit Collar",
                }
            )
        markers: list[dict[str, Any]] = []
        if preset is not None:
            markers.append(
                {
                    "time": bar_sec,
                    "position": "belowBar" if is_buy else "aboveBar",
                    "color": "#22c55e" if is_buy else "#d946ef",
                    "shape": "arrowUp" if is_buy else "arrowDown",
                    "text": f"{r['side']} {r['archetype']} (qty=1)",
                }
            )
        lwc_symbols[sym] = {
            "tv_symbol": r.get("tv_symbol") or resolve_tv_exchange_symbol(sym),
            "side": r["side"],
            "price_lines": price_lines,
            "markers": markers,
        }

    for p in manual_staged_presets:
        if not isinstance(p, Mapping) or not p.get("symbol"):
            continue
        sym = str(p["symbol"]).strip().upper()
        bar_sec = int(p.get("ts_utc_ms") or (now_sec * 1000)) // 1000
        is_buy = str(p.get("side", "BUY")) == "BUY"
        entry = lwc_symbols.setdefault(
            sym,
            {
                "tv_symbol": resolve_tv_exchange_symbol(sym),
                "side": str(p.get("side", "BUY")),
                "price_lines": [
                    {
                        "id": "entry",
                        "price": float(p["trigger_price"]),
                        "color": "#3b82f6",
                        "title": "Pine Trigger",
                    },
                    {
                        "id": "hard_stop",
                        "price": float(p["stop_price"]),
                        "color": "#ef4444",
                        "title": "Pine Stop",
                    },
                ],
                "markers": [],
            },
        )
        entry["markers"].append(
            {
                "time": bar_sec,
                "position": "belowBar" if is_buy else "aboveBar",
                "color": "#14b8a6" if is_buy else "#ec4899",
                "shape": "circle",
                "text": f"PINE {p.get('side', 'BUY')} ({p.get('script_kind') or 'indicator'})",
            }
        )

    active_sess = str(doc.get("active_session") or "CLOSED")
    vp = str(doc.get("venue_profile") or resolve_venue_profile(str(doc.get("venue") or "IBKR"))[0])
    coord_dict = dict(paper_coordination or {})
    active_positions = [
        dict(p) for p in (coord_dict.get("active_positions") or ()) if isinstance(p, Mapping)
    ]
    return {
        "schema_version": "tier0.finos-openbb-tv-cockpit.v1",
        "generated_at_et": doc.get("generated_at_et"),
        "plugin": "@finos-workspace/plotly-charts@2.5.0",
        "venue_profile": vp,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "data": [
            {
                "type": "bar",
                "name": "Session Move (%)",
                "x": syms_x,
                "y": [r["session_move_pct"] for r in openbb_rows],
            },
            {
                "type": "bar",
                "name": "RVOL (x)",
                "x": syms_x,
                "y": [r["rvol"] for r in openbb_rows],
            },
            {
                "type": "bar",
                "name": "Darkpool TRF (%)",
                "x": syms_x,
                "y": [
                    round(r["darkpool_trf_ratio"] * 100.0, 2)
                    if r["darkpool_trf_ratio"] is not None
                    else 0.0
                    for r in openbb_rows
                ],
            },
        ],
        "layout": {
            "title": f"Tier-0 TV Half-Auto Cockpit ({active_sess} · {vp})",
            "barmode": "group",
            "xaxis": {"title": "Symbol"},
            "yaxis": {"title": "Move % / RVOL / TRF %"},
        },
        "openbb_rows": openbb_rows,
        "venue_order_handles": build_venue_order_handles_matrix(session=active_sess),
        "active_engine_positions": active_positions,
        "engine_coordination": coord_dict,
        "lightweight_charts_v5": {
            "library": "tradingview/lightweight-charts",
            "api_version": "5",
            "license": "Apache-2.0",
            "time_unit": "utc_timestamp_seconds",
            "display_timezone": "America/New_York",
            "v5_apis": ["createChart", "addSeries", "createSeriesMarkers", "createPriceLine"],
            "symbols": lwc_symbols,
        },
        "summary": {
            "active_session": active_sess,
            "venue_profile": vp,
            "macro_regime": dict(doc.get("macro_regime") or {}),
            "tickets_count": compounding.get("tickets_count", 0),
            "ready_tickets_count": compounding.get("ready_tickets_count", 0),
            "standby_presets_count": compounding.get("standby_presets_count", 0),
            "staged_scan_count": compounding.get("staged_scan_count", 0),
            "staged_manual_count": compounding.get("staged_manual_count", 0),
            "duckdb_audit_rows": compounding.get("duckdb_audit_rows", 0),
            "unusual_vol_count": compounding.get("unusual_vol_count", 0),
            "darkpool_hits_count": compounding.get("darkpool_hits_count", 0),
            "darkpool_trf_covered_count": compounding.get("darkpool_trf_covered_count", 0),
            "active_engine_positions_count": len(active_positions),
            "coordinated_presets_count": int(coord_dict.get("coordinated_presets_count") or 0),
        },
        "paper_farm_coordination": coord_dict,
    }


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _load_manual_staged_presets(out_dir: Path, manifest_dir: Path) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for cand in (manifest_dir / "staged_tv_presets.json", out_dir / "staged_tv_presets.json"):
        doc = _read_json_optional(cand)
        for item in doc.get("presets", []):
            if isinstance(item, Mapping) and item.get("symbol"):
                aid = str(item.get("alert_id") or f"{item['symbol']}:{item.get('ts_utc_ms', 0)}")
                by_id[aid] = dict(item)
    return list(by_id.values())


def _normalize_active_position(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    sym = str(raw.get("symbol") or "").strip().upper()
    if not sym:
        return None
    raw_tier = (
        raw.get("lock_tier") if raw.get("lock_tier") is not None else raw.get("trailing_tier")
    )
    return {
        "symbol": sym,
        "side": str(raw.get("side") or "LONG").strip().upper(),
        "lane_account": raw.get("lane_account") or raw.get("account"),
        "lane_id": raw.get("lane_id"),
        "venue_profile": raw.get("venue_profile"),
        "qty": _opt_float(raw.get("qty")),
        "entry_price": _opt_float(
            raw.get("entry_price")
            if raw.get("entry_price") is not None
            else raw.get("filled_avg_price")
        ),
        "active_stop": _opt_float(
            raw.get("active_stop") if raw.get("active_stop") is not None else raw.get("stop_price")
        ),
        "initial_stop": _opt_float(raw.get("initial_stop")),
        "lock_tier": raw_tier,
        "session": raw.get("active_session") or raw.get("session") or raw.get("entered_session"),
        "trailing_schedule": raw.get("trailing_schedule") or raw.get("schedule"),
        "order_mode": raw.get("order_mode"),
        "collar_bps": _opt_float(raw.get("collar_bps"), min_val=0.0),
        "exit_triggered": bool(raw.get("exit_triggered") is True),
        "reverse_flatten": bool(raw.get("reverse_flatten") is True),
        "unrealized_pnl": _opt_float(raw.get("unrealized_pnl")),
    }


def _collect_paper_farm_coordination(
    manifest_dir: Path,
    bundle: Mapping[str, Any] | None,
    *,
    out_dir: Path | None = None,
) -> dict[str, Any]:
    exec_doc = _read_json_optional(manifest_dir / "paper_farm_execution_status.json")
    perf_doc = _read_json_optional(manifest_dir / "paper_farm_live_performance_manifest.json")
    auto_doc = _read_json_optional(manifest_dir / "paper_farm_live_auto_entry_status.json")
    coord_doc: dict[str, Any] = {}
    coord_candidates: list[Path] = []
    if out_dir is not None:
        coord_candidates.append(out_dir / "tv_engine_coordination_manifest.json")
    coord_candidates.append(manifest_dir / "tv_engine_coordination_manifest.json")
    for cand in coord_candidates:
        loaded = _read_json_optional(cand)
        if loaded:
            coord_doc = loaded
            break
    raw_per_lane = perf_doc.get("per_lane")
    per_lane: Mapping[str, Any] = raw_per_lane if isinstance(raw_per_lane, Mapping) else {}
    lane_assignments: dict[str, list[str]] = {}
    for row in (bundle or {}).get("top_potential_symbols", []):
        if isinstance(row, Mapping) and row.get("symbol") and row.get("lane_account"):
            lane_assignments.setdefault(str(row["lane_account"]), []).append(str(row["symbol"]))

    pos_by_sym: dict[str, dict[str, Any]] = {}
    for container in (exec_doc, coord_doc):
        for key in ("active_positions", "positions", "guards"):
            raw_list = container.get(key)
            if isinstance(raw_list, Sequence) and not isinstance(raw_list, (str, bytes)):
                for item in raw_list:
                    if isinstance(item, Mapping):
                        norm = _normalize_active_position(item)
                        if norm is not None:
                            pos_by_sym[norm["symbol"]] = norm
            elif isinstance(raw_list, Mapping):
                for sym_k, item in raw_list.items():
                    if isinstance(item, Mapping):
                        norm = _normalize_active_position({"symbol": sym_k, **item})
                        if norm is not None:
                            pos_by_sym[norm["symbol"]] = norm
    for sym in exec_doc.get("managed_symbols") or ():
        s_up = str(sym).strip().upper()
        if s_up and s_up not in pos_by_sym:
            pos_by_sym[s_up] = {
                "symbol": s_up,
                "side": "MANAGED",
                "lane_account": None,
                "lane_id": None,
                "venue_profile": None,
                "qty": None,
                "entry_price": None,
                "active_stop": None,
                "initial_stop": None,
                "lock_tier": None,
                "session": exec_doc.get("active_session") or auto_doc.get("session"),
                "trailing_schedule": None,
                "order_mode": None,
                "collar_bps": None,
                "exit_triggered": False,
                "reverse_flatten": False,
                "unrealized_pnl": None,
            }
    active_positions = [pos_by_sym[k] for k in sorted(pos_by_sym)]

    raw_coord_presets = (
        coord_doc.get("coordination_decisions")
        or coord_doc.get("coordinated_presets")
        or coord_doc.get("presets")
        or coord_doc.get("staged_presets")
        or ()
    )
    coordinated_presets = [
        dict(item)
        for item in (
            raw_coord_presets
            if isinstance(raw_coord_presets, Sequence)
            and not isinstance(raw_coord_presets, (str, bytes))
            else ()
        )
        if isinstance(item, Mapping)
    ]
    ready_confirm_n = _opt_int(coord_doc.get("ready_for_operator_confirm_count"), min_val=0)
    if ready_confirm_n is None:
        ready_confirm_n = sum(
            1 for c in coordinated_presets if c.get("ready_for_operator_confirm") is True
        )
    blocked_n = _opt_int(coord_doc.get("blocked_count"), min_val=0)
    if blocked_n is None:
        blocked_n = max(0, len(coordinated_presets) - ready_confirm_n)
    return {
        "execution_status": exec_doc.get("status"),
        "active_session": exec_doc.get("active_session") or coord_doc.get("active_session"),
        "managed_symbols": list(exec_doc.get("managed_symbols") or ()),
        "streams": list(exec_doc.get("streams") or ()),
        "last_trade_utc": dict(exec_doc.get("last_trade_utc") or {}),
        "entry_fault": exec_doc.get("entry_fault"),
        "tca_calibration_ok": (
            bool(bundle["tca_calibration_ok"])
            if isinstance(bundle, Mapping) and "tca_calibration_ok" in bundle
            else coord_doc.get("tca_calibration_ok")
        ),
        "paper_live_deployed": perf_doc.get("paper_live_deployed"),
        "accounts_reporting": sorted(str(k) for k in per_lane),
        "lane_assignments": lane_assignments,
        "active_positions": active_positions,
        "active_positions_count": len(active_positions),
        "coordinated_presets": coordinated_presets,
        "coordinated_presets_count": len(coordinated_presets),
        "ready_for_operator_confirm_count": ready_confirm_n,
        "blocked_count": blocked_n,
        "any_breaker_tripped": bool(coord_doc.get("any_breaker_tripped") is True),
        "coordination_manifest_present": bool(coord_doc),
        "coordination_manifest_schema": coord_doc.get("schema_version"),
        "coordination_generated_at_utc": coord_doc.get("generated_at_utc"),
    }


def build_tv_compounding_manifest(
    doc: Mapping[str, Any],
    *,
    pine_path: Path | None = None,
    pine_shas: Mapping[str, str] | None = None,
    staged_manual_count: int = 0,
    duckdb_audit_rows: int = 0,
    paper_farm_lanes_summary: Mapping[str, Any] | None = None,
    artifacts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build deterministic tv_compounding_manifest.json (qty=1, semi_auto_confirmed=False)."""
    tickets_list = [t for t in doc.get("tickets", []) if isinstance(t, Mapping)]
    standby_list = [s for s in doc.get("standby_presets", []) if isinstance(s, Mapping)]
    all_rows = [*tickets_list, *standby_list]
    active_sess = str(doc.get("active_session") or "CLOSED")
    vp = str(doc.get("venue_profile") or resolve_venue_profile(str(doc.get("venue") or "IBKR"))[0])

    shas_map: dict[str, str] = dict(pine_shas or {})
    if not shas_map and _REPO_PINE_DIR.is_dir():
        for name in CANONICAL_PINE_SCRIPTS:
            cand = _REPO_PINE_DIR / name
            if cand.is_file():
                shas_map[name] = hashlib.sha256(cand.read_bytes()).hexdigest()

    if pine_path is not None and pine_path.is_file():
        pine_sha: str | None = hashlib.sha256(pine_path.read_bytes()).hexdigest()
        shas_map.setdefault("tier0_omni_sota_24x5.pine", pine_sha)
    else:
        pine_sha = shas_map.get("tier0_omni_sota_24x5.pine")
        if pine_sha is None and _REPO_PINE_OMNI_PATH.is_file():
            pine_sha = hashlib.sha256(_REPO_PINE_OMNI_PATH.read_bytes()).hexdigest()

    unusual_n = sum(1 for t in tickets_list if t.get("unusual_volume_buildup") is True)
    darkpool_n = sum(
        1
        for t in tickets_list
        if (_opt_float(t.get("darkpool_trf_ratio"), min_val=0.0) or 0.0) >= 0.35
    )
    standby_uv_n = sum(1 for s in standby_list if s.get("unusual_volume_buildup") is True)
    standby_dp_n = sum(
        1
        for s in standby_list
        if (_opt_float(s.get("darkpool_trf_ratio"), min_val=0.0) or 0.0) >= 0.35
    )
    trf_covered_n = sum(1 for r in all_rows if r.get("darkpool_trf_ratio") is not None)
    ready_n = sum(
        1 for t in tickets_list if str(t.get("action_gate") or "").startswith("READY_HALF_AUTO")
    )
    staged_scan_n = int(doc.get("staged_count") or 0)
    setup_counts: dict[str, int] = {}
    for r in all_rows:
        arch = str(r.get("archetype") or "").strip()
        if arch:
            setup_counts[arch] = setup_counts.get(arch, 0) + 1
    handles_matrix = build_venue_order_handles_matrix(session=active_sess)
    handles_summary = {
        k: {
            "max_rpm": spec["max_rpm"],
            "entry_modify_rpm_budget": spec["entry_modify_rpm_budget"],
            "exit_reserve_rpm": spec["exit_reserve_rpm"],
            "max_concurrent_trailed_positions": spec["max_concurrent_trailed_positions"],
            "trailing_modify_cadence_secs": spec["trailing_modify_cadence_secs"],
            "paper_only": spec["paper_only"],
            "live_execution_enabled": spec["live_execution_enabled"],
            "executable_now": spec["executable_now"],
        }
        for k, spec in handles_matrix.items()
    }
    lanes_sum = dict(paper_farm_lanes_summary or {})
    return {
        "schema_version": "tier0.tv-compounding-manifest.v1",
        "generated_at_et": doc.get("generated_at_et"),
        "movers_generated_at_utc": doc.get("movers_generated_at_utc"),
        "venue_profile": vp,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "active_session": active_sess,
        "macro_regime": dict(doc.get("macro_regime") or {}),
        "tickets_count": len(tickets_list),
        "ready_tickets_count": ready_n,
        "standby_presets_count": len(standby_list),
        "staged_count": staged_scan_n,
        "staged_scan_count": staged_scan_n,
        "staged_manual_count": int(staged_manual_count),
        "staged_presets_total": staged_scan_n + int(staged_manual_count),
        "duckdb_audit_rows": int(duckdb_audit_rows),
        "unusual_vol_count": unusual_n,
        "darkpool_hits_count": darkpool_n,
        "standby_unusual_vol_count": standby_uv_n,
        "standby_darkpool_hits_count": standby_dp_n,
        "darkpool_trf_covered_count": trf_covered_n,
        "multi_situation_setup_counts": setup_counts,
        "venue_order_handles_summary": handles_summary,
        "active_engine_positions_count": int(lanes_sum.get("active_positions_count") or 0),
        "coordinated_presets_count": int(lanes_sum.get("coordinated_presets_count") or 0),
        "engine_coordination_summary": {
            "execution_status": lanes_sum.get("execution_status"),
            "coordination_manifest_present": bool(lanes_sum.get("coordination_manifest_present")),
            "ready_for_operator_confirm_count": int(
                lanes_sum.get("ready_for_operator_confirm_count") or 0
            ),
            "blocked_count": int(lanes_sum.get("blocked_count") or 0),
            "active_positions_count": int(lanes_sum.get("active_positions_count") or 0),
            "coordinated_presets_count": int(lanes_sum.get("coordinated_presets_count") or 0),
        },
        "pin_symbols": list(doc.get("pin_symbols") or ()),
        "pine_omni_sha256": pine_sha,
        "pine_shas": shas_map,
        "paper_farm_lanes_summary": lanes_sum,
        "artifacts": dict(artifacts or {}),
        "top_symbols": [
            {
                "symbol": t["symbol"],
                "tv_symbol": t.get("tv_symbol") or resolve_tv_exchange_symbol(t["symbol"]),
                "side": t["side"],
                "archetype": t["archetype"],
                "recommended_omni_preset": t.get("recommended_omni_preset"),
                "target_paper_lane": t.get("target_paper_lane") or t.get("paper_lane_account"),
                "venue_profile": t.get("venue_profile") or vp,
                "action_gate": t["action_gate"],
                "order_preset_id": t.get("order_preset_id"),
                "score": t.get("score"),
                "rvol": t.get("rvol"),
                "darkpool_trf_ratio": t.get("darkpool_trf_ratio"),
                "darkpool_trf_source": t.get("darkpool_trf_source"),
                "darkpool_block_count": t.get("darkpool_block_count"),
                "trf_block_ratio": t.get("trf_block_ratio"),
                "hidden_volume_ratio": t.get("hidden_volume_ratio"),
                "vol_buildup_ratio": t.get("vol_buildup_ratio"),
                "unusual_volume_buildup": bool(t.get("unusual_volume_buildup") is True),
                "pair_symbol": t.get("pair_symbol"),
                "paper_lane_account": t.get("paper_lane_account"),
            }
            for t in tickets_list
        ],
        "standby_symbols": [
            {
                "symbol": s["symbol"],
                "tv_symbol": s.get("tv_symbol") or resolve_tv_exchange_symbol(s["symbol"]),
                "side": s["side"],
                "archetype": s["archetype"],
                "recommended_omni_preset": s.get("recommended_omni_preset"),
                "target_paper_lane": s.get("target_paper_lane") or s.get("paper_lane_account"),
                "venue_profile": s.get("venue_profile") or vp,
                "watch_tier": s.get("watch_tier"),
                "score": s.get("score"),
                "rvol": s.get("rvol"),
                "darkpool_trf_ratio": s.get("darkpool_trf_ratio"),
                "darkpool_trf_source": s.get("darkpool_trf_source"),
                "hidden_volume_ratio": s.get("hidden_volume_ratio"),
                "vol_buildup_ratio": s.get("vol_buildup_ratio"),
                "unusual_volume_buildup": bool(s.get("unusual_volume_buildup") is True),
                "pair_symbol": s.get("pair_symbol"),
            }
            for s in standby_list
        ],
    }


def refresh_tickets_from_paths(
    *,
    movers_path: Path,
    out_dir: Path,
    bundle_path: Path | None = None,
    venue: str = "IBKR",
    venue_profile: str | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Load local mover/bundle manifests and atomically emit full TV SOTA Pack artifacts."""
    movers = json.loads(movers_path.read_text(encoding="utf-8"))
    bundle = (
        json.loads(bundle_path.read_text(encoding="utf-8"))
        if bundle_path is not None and bundle_path.is_file()
        else None
    )
    vp, vu = resolve_venue_profile(venue, venue_profile=venue_profile)
    manifest_dir = movers_path.parent
    manual_staged = _load_manual_staged_presets(out_dir, manifest_dir)
    paper_coord = _collect_paper_farm_coordination(manifest_dir, bundle, out_dir=out_dir)
    doc = build_half_auto_tickets(
        movers, bundle=bundle, venue=vu, venue_profile=vp, now_utc=now_utc
    )
    doc["paper_farm_coordination"] = paper_coord
    if manual_staged:
        doc["manual_staged_presets"] = manual_staged

    audit_db_path = out_dir / "tv_half_auto_audit.duckdb"
    persisted_audit = False
    if movers.get("schema_version") == "tier0.whole-market-movers.v2":
        # Replace only the scanner-owned snapshot; manual/Pine intake has its own history.
        presets: list[dict[str, Any]] = []
        meta_by_id: dict[str, dict[str, Any]] = {}
        macro_regime_int = int((doc.get("macro_regime") or {}).get("regime", 0))
        for t in doc["tickets"]:
            if str(t.get("action_gate") or "").startswith("HOLD"):
                continue
            if (
                t["side"] == "SELL"
                and macro_regime_int != -1
                and t.get("peak_confirmed") is not True
            ):
                continue
            if t["side"] != "SELL" or (
                t["easy_to_borrow"]
                and t["shortable"]
                and (vu != "ALPACA" or t["active_session"] != "OVERNIGHT")
            ):
                validated = validate_tv_alert_payload(
                    t["tv_alert_preset"], watchlist_manifest=movers, venue=vu
                )
                presets.append(validated)
                meta_by_id[str(validated["alert_id"])] = {
                    "action_gate": t.get("action_gate"),
                    "reverse_short_mode": t.get("reverse_short_mode"),
                    "archetype": t.get("archetype"),
                    "pair_symbol": t.get("pair_symbol"),
                    "order_preset_id": t.get("order_preset_id"),
                    "venue_profile": t.get("venue_profile") or vp,
                }
        doc["staged_count"] = len(presets)
        _atomic_write_text(
            out_dir / "staged_scan_presets.json",
            json.dumps(
                {
                    "schema_version": "tier0.staged-tv-presets.v1",
                    "scan_generated_at_utc": movers["generated_at_utc"],
                    "venue_profile": vp,
                    "semi_auto_confirmed": False,
                    "presets": presets,
                },
                indent=2,
            )
            + "\n",
        )
        if presets:
            persist_tv_presets_duckdb(
                audit_db_path,
                presets,
                source="scanner_tickets",
                meta_by_alert_id=meta_by_id,
            )
            persisted_audit = True

    if not persisted_audit and audit_db_path.is_file():
        sync_tv_audit_duckdb_snapshot(audit_db_path)

    pine_shas = sync_pine_scripts_to_pack(out_dir)
    audit_summary = load_tv_audit_summary(audit_db_path)

    order_presets = build_tv_order_presets(
        doc, manual_staged_presets=manual_staged, paper_coordination=paper_coord
    )
    _atomic_write_text(out_dir / "live_half_auto_tickets.json", json.dumps(doc, indent=2) + "\n")
    _atomic_write_text(out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md", render_tickets_markdown(doc))
    _atomic_write_text(
        out_dir / "tv_order_presets.json", json.dumps(order_presets, indent=2) + "\n"
    )
    _atomic_write_text(
        out_dir / "TV_ORDER_PRESET_CARDS.md", render_order_preset_cards_markdown(order_presets)
    )
    _atomic_write_text(
        out_dir / "tier0_sota_tv_watchlists_live.txt",
        render_tv_watchlist(
            doc["tickets"],
            ranked_entries=movers.get("entries"),
            staged_presets=manual_staged,
        ),
    )

    artifacts_map = {
        "tickets_json": "live_half_auto_tickets.json",
        "tickets_md": "LIVE_HALF_AUTO_ORDER_TICKETS.md",
        "order_presets_json": "tv_order_presets.json",
        "order_preset_cards_md": "TV_ORDER_PRESET_CARDS.md",
        "watchlist_txt": "tier0_sota_tv_watchlists_live.txt",
        "finos_cockpit_json": "finos_tv_cockpit.json",
        "staged_scan_json": "staged_scan_presets.json",
        "audit_duckdb": "tv_half_auto_audit.duckdb",
        "audit_parquet": "tv_staged_presets.parquet",
    }
    compounding = build_tv_compounding_manifest(
        doc,
        pine_path=(
            out_dir / "tier0_omni_sota_24x5.pine"
            if (out_dir / "tier0_omni_sota_24x5.pine").is_file()
            else _REPO_PINE_OMNI_PATH
        ),
        pine_shas=pine_shas,
        staged_manual_count=len(manual_staged),
        duckdb_audit_rows=int(audit_summary["audit_rows"]),
        paper_farm_lanes_summary=paper_coord,
        artifacts=artifacts_map,
    )
    _atomic_write_text(
        out_dir / "tv_compounding_manifest.json", json.dumps(compounding, indent=2) + "\n"
    )
    finos_cockpit = build_finos_tv_cockpit(
        doc,
        compounding=compounding,
        manual_staged_presets=manual_staged,
        paper_coordination=paper_coord,
    )
    _atomic_write_text(
        out_dir / "finos_tv_cockpit.json", json.dumps(finos_cockpit, indent=2) + "\n"
    )
    return doc


def main() -> None:
    """CLI entrypoint for Half-Auto order ticket & TradingView watchlist generation."""
    p = argparse.ArgumentParser(description="Tier-0 TV Half-Auto ticket generator")
    p.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_MANIFEST_DIR / "tv_sota_pack")
    p.add_argument("--venue", default="IBKR")
    p.add_argument(
        "--venue-profile",
        default=None,
        choices=sorted(ALL_TV_VENUE_PROFILES),
        help="Optional explicit venue profile (e.g. IBKR_PAPER_4002, ALPACA_PAPER_1000).",
    )
    p.add_argument(
        "--show-handles",
        action="store_true",
        help="Print 1000rpm/3000rpm Venue Order-Handle budgets as JSON and exit.",
    )
    a = p.parse_args()
    if a.show_handles:
        sess = current_et_session(datetime.now(UTC).astimezone(NY_TZ))
        alp_vp = (
            a.venue_profile
            if a.venue_profile in {"ALPACA_PAPER_200", "ALPACA_PAPER_1000"}
            else None
        )
        print(
            json.dumps(
                build_venue_order_handles(alpaca_venue_profile=alp_vp, session=sess), indent=2
            )
        )
        return
    doc = refresh_tickets_from_paths(
        movers_path=a.manifest_dir / "live_whole_market_movers_manifest.json",
        bundle_path=a.manifest_dir / "live_lane_order_preset_bundle.json",
        out_dir=a.out_dir,
        venue=a.venue,
        venue_profile=a.venue_profile,
    )
    print(
        f"HALF_AUTO_REFRESH_OK=1 SESSION={doc['active_session']} "
        f"VENUE_PROFILE={doc['venue_profile']} "
        f"TICKETS={len(doc['tickets'])} STANDBY={len(doc.get('standby_presets', []))} "
        f"SCHEMA_OK={doc['schema_validated_count']}"
    )


if __name__ == "__main__":
    main()

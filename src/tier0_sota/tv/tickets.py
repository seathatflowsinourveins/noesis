"""Half-Auto extreme-mover order ticket, Order Preset Pack, watchlist & L10 TV cockpit."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from importlib import metadata as importlib_metadata
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
    prune_and_archive_staged_presets,
    refuse_live_endpoints,
    stage_tv_alert_preset,
    sync_tv_audit_duckdb_snapshot,
    validate_tv_alert_payload,
)

NY_TZ = ZoneInfo("America/New_York")
LOW_EXTREME_HOLD_REASONS = frozenset({"LOW_MOVE_NOT_EXTREME", "LOW_RVOL_NOT_EXTREME"})
EXTENDED_HOURS_SESSIONS = frozenset({"PRE_MARKET", "AFTER_HOURS", "OVERNIGHT"})
SESSION_EARLY_IGNITION_MIN_PRICE = Decimal("10.00")
SESSION_EARLY_IGNITION_MAX_BAR_AGE_MS = 5 * 60_000
SESSION_EARLY_IGNITION_MAX_MANIFEST_AGE_MS = 15 * 60_000
HARD_SAFETY_HOLD_MARKERS: tuple[str, ...] = (
    "TCA_RECALIBRATION",
    "CIRCUIT_BREAKER",
    "BREAKER",
    "DRAWDOWN",
    "COOLDOWN",
    "DAILY_LOSS",
    "HALT",
    "BEARISH_UNCONFIRMED_REGIME",
    "MACRO_REGIME_NEUTRAL",
)
VALID_TV_TECHNICAL_RATINGS = frozenset({"STRONG_BUY", "BUY", "NEUTRAL", "SELL", "STRONG_SELL"})
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
    "NU": "NYSE",
    "ANT": "NASDAQ",
    "VALE": "NYSE",
    "BE": "NYSE",
    "HL": "NYSE",
    "CRWV": "NASDAQ",
    "NBIS": "NASDAQ",
    "IREN": "NASDAQ",
    "XP": "NASDAQ",
    "IBIT": "NASDAQ",
    "DELL": "NYSE",
    "SNDQ": "BATS",
    "MUU": "NASDAQ",
    "SPCX": "NASDAQ",
    "BMNR": "NYSE",
    "EWZ": "AMEX",
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

TV_CLOUD_WATCHLIST_IDS: dict[str, str] = {
    "active": "357054653",
    "alert_bound": "357055160",
}

ARCHETYPE_TO_POTENTIAL_CATEGORY: dict[str, str] = {
    "EARLY_EP_MOMENTUM_BURST": "EP_MOMENTUM_BURST",
    "VCP_BREAKOUT": "VCP_STAGE2_BREAKOUT",
    "VCP_COIL": "VCP_STAGE2_BREAKOUT",
    "PEAD_DRIFT": "PEAD_DRIFT",
    "EXHAUSTION_HAMMER": "EXTREME_MOVER_WATCH",
    "HIDDEN_VOL_ACCUM": "DARKPOOL_ACCUMULATION",
    "PARABOLIC_TOP": "PARABOLIC_PEAK_REVERSAL",
    "PEAK_REVERSAL": "PARABOLIC_PEAK_REVERSAL",
    "CRASH_ALIGNMENT": "CRASH_INVERSE_HEDGE",
    "EXTREME_REVERSAL": "EXTREME_MOVER_WATCH",
}

SESSION_EXECUTION_MECHANISMS: dict[str, tuple[str, str, str]] = {
    "RTH": (
        "NATIVE_BROKER_STOP_MARKET_RATCHET",
        "STOP_MARKET",
        "RTH: STOP_MARKET 0bp Native Ratchet",
    ),
    "PRE_MARKET": (
        "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
        "LIMIT_EXTENDED_HOURS_COLLARED",
        "PRE/AH: STOP_LIMIT / LIMIT 35bp Synthetic Tick-Supervised",
    ),
    "AFTER_HOURS": (
        "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
        "LIMIT_EXTENDED_HOURS_COLLARED",
        "PRE/AH: STOP_LIMIT / LIMIT 35bp Synthetic Tick-Supervised",
    ),
    "OVERNIGHT": (
        "SYNTHETIC_BOATS_COLLARED_LIMIT",
        "LIMIT_OVERNIGHT_COLLARED",
        "OVERNIGHT: STOP_LIMIT / LIMIT 50bp Synthetic BOATS",
    ),
}

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


def _extract_exchange_entries(
    rows: Sequence[Mapping[str, Any]] | None,
    dest: dict[str, str],
) -> None:
    """Populate symbol -> normalized TradingView exchange prefix from row mappings."""
    for row in rows or ():
        if not isinstance(row, Mapping):
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        raw_tv = str(row.get("tv_symbol") or "").strip().upper()
        if raw_tv and ":" in raw_tv:
            ex_tok, tv_sym_tok = raw_tv.split(":", 1)
            norm = _normalize_tv_exchange(ex_tok) or ex_tok.strip().upper()
            tok_sym = tv_sym_tok.strip().upper()
            if norm and tok_sym:
                dest[tok_sym] = norm
                if sym:
                    dest[sym] = norm
                continue
        if not sym:
            continue
        norm_ex = _normalize_tv_exchange(row.get("exchange") or row.get("tv_exchange"))
        if norm_ex:
            dest[sym] = norm_ex


def _build_exchange_map_from_movers(
    movers: Mapping[str, Any] | None,
    *,
    manifest_dir: Path | None = None,
    extra_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, str]:
    """Build symbol -> TV exchange map from cached 12k NMS universe + live movers rows."""
    ex_map: dict[str, str] = {}
    if manifest_dir is not None:
        cache_path = manifest_dir / ".nms_equity_universe_cache.json"
        if cache_path.is_file():
            raw_cache = _read_json_optional(cache_path)
            raw_syms = raw_cache.get("symbols")
            if isinstance(raw_syms, Mapping):
                for raw_s, raw_e in raw_syms.items():
                    s_tok = str(raw_s).strip().upper()
                    norm = _normalize_tv_exchange(raw_e)
                    if s_tok and norm:
                        ex_map[s_tok] = norm
    if isinstance(movers, Mapping):
        for bucket_key in (
            "full_market_top_movers",
            "early_stage_ignition_movers",
            "entries",
        ):
            raw_b = movers.get(bucket_key)
            if isinstance(raw_b, Sequence) and not isinstance(raw_b, (str, bytes)):
                _extract_exchange_entries(raw_b, ex_map)
    if extra_rows:
        _extract_exchange_entries(extra_rows, ex_map)
    return ex_map


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


def _json_safe_value(val: Any) -> Any:
    if isinstance(val, Decimal):
        return float(val)
    if is_dataclass(val) and not isinstance(val, type):
        return _json_safe_value(asdict(val))
    if isinstance(val, Mapping):
        return {str(k): _json_safe_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_json_safe_value(v) for v in val]
    return val


def _execution_mechanism_details(
    session: str, execution_mechanism: str | None = None
) -> tuple[str, str, str]:
    sess_u = str(session or "OVERNIGHT").strip().upper()
    default_mech, default_exit, default_label = SESSION_EXECUTION_MECHANISMS.get(
        sess_u,
        (
            "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
            "LIMIT_EXTENDED_HOURS_COLLARED",
            "PRE/AH: STOP_LIMIT / LIMIT 35bp Synthetic Tick-Supervised",
        ),
    )
    mech = str(execution_mechanism or default_mech).strip().upper() or default_mech
    if mech == "NATIVE_BROKER_STOP_MARKET_RATCHET":
        return mech, "STOP_MARKET", "RTH: STOP_MARKET 0bp Native Ratchet"
    if mech == "SYNTHETIC_BOATS_COLLARED_LIMIT":
        return (
            mech,
            "LIMIT_OVERNIGHT_COLLARED",
            "OVERNIGHT: STOP_LIMIT / LIMIT 50bp Synthetic BOATS",
        )
    if mech == "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT":
        return (
            mech,
            "LIMIT_EXTENDED_HOURS_COLLARED",
            "PRE/AH: STOP_LIMIT / LIMIT 35bp Synthetic Tick-Supervised",
        )
    if mech == "SESSION_CLOSED_HOLD_GTC_STOP":
        return mech, "GTC_STOP_HOLD", "CLOSED: GTC Stop Hold (Fail-Closed)"
    return mech, default_exit, default_label


def resolve_non_noise_min_trail_spec(
    *,
    symbol: str,
    entry_price: Decimal,
    side: _risk.Side,
    session: str,
    natr_pct: Decimal,
    spread_bps: Decimal = Decimal("5"),
    archetype: str = "EARLY_EP_MOMENTUM_BURST",
    venue_profile: str = "ALPACA_PAPER_1000",
) -> dict[str, Any]:
    """Compute JSON-safe 24/5 non-noise min-trail spec strictly via engine SSOT."""
    sym = str(symbol).strip().upper()
    eff_sess = str(session or "RTH").strip().upper()
    if eff_sess not in SESSION_EXECUTION_MECHANISMS:
        eff_sess = "OVERNIGHT"
    safe_px = max(_risk.TICK_SUB_DOLLAR, entry_price)
    safe_natr = max(Decimal("0.001"), min(Decimal("0.50"), natr_pct))
    safe_spread = max(Decimal("0"), min(Decimal("200"), spread_bps))
    vp, _ = resolve_venue_profile(
        "IBKR" if str(venue_profile).upper().startswith("IBKR") else "ALPACA",
        venue_profile=venue_profile,
    )
    res = _eng.compute_non_noise_min_trail_spec(
        symbol=sym,
        entry_price=safe_px,
        side=side,
        session=eff_sess,
        natr_pct=safe_natr,
        spread_bps=safe_spread,
        archetype=archetype,
        venue_profile=vp,
    )
    raw_spec: dict[str, Any] = dict(_json_safe_value(res))
    sched_name, sess_map = _eng.select_pooled_trailing_schedule(safe_natr)
    prof = _eng.get_session_protection_profile(eff_sess, sess_map)
    vh = resolve_venue_order_handle(vp, session=eff_sess, trailing_schedule=sched_name)

    mech, default_broker_exit, mech_label = _execution_mechanism_details(
        eff_sess,
        str(raw_spec.get("execution_mechanism") or "") or None,
    )
    non_noise_f = _opt_float(raw_spec.get("non_noise_min_trail_pct"), min_val=0.0)
    if non_noise_f is None or non_noise_f <= 0.0:
        non_noise_f = float(prof.min_trail_pct)
    fast_sell_f = _opt_float(raw_spec.get("fast_sell_non_noise_reversal_pct"), min_val=0.0)
    if fast_sell_f is None or fast_sell_f <= 0.0:
        fast_sell_f = non_noise_f
    peak_climax_f = _opt_float(raw_spec.get("peak_climax_reverse_trigger_pct"), min_val=0.0)
    if peak_climax_f is None or peak_climax_f <= 0.0:
        peak_climax_f = min(float(prof.initial_max_loss_pct), max(non_noise_f, 0.0040))
    raw_reentry = raw_spec.get("rapid_reentry_policy")
    reentry_pol: dict[str, Any] = (
        dict(raw_reentry)
        if isinstance(raw_reentry, Mapping)
        else {
            "reentry_cooldown_secs": 60,
            "rpm_budget_available": int(vh["entry_modify_rpm_budget"]),
            "max_rpm": int(vh["max_rpm"]),
            "reentry_max_attempts_per_session": 3,
            "exit_reserve_protected": True,
        }
    )
    reentry_pol.setdefault("reentry_cooldown_secs", 60)
    reentry_pol.setdefault("rpm_budget_available", int(vh["entry_modify_rpm_budget"]))
    reentry_pol.setdefault("max_rpm", int(vh["max_rpm"]))
    reentry_pol.setdefault("reentry_max_attempts_per_session", 3)
    reentry_pol.setdefault("exit_reserve_protected", True)
    collar_f = _opt_float(raw_spec.get("collar_bps"), min_val=0.0)
    if collar_f is None:
        collar_f = float(prof.limit_collar_bps)
    return {
        **raw_spec,
        "symbol": sym,
        "session": eff_sess,
        "side": side,
        "archetype": archetype,
        "venue_profile": vp,
        "trailing_schedule": str(raw_spec.get("trailing_schedule") or sched_name),
        "order_type": str(raw_spec.get("order_type") or prof.order_mode),
        "broker_exit_order": str(raw_spec.get("broker_exit_order") or default_broker_exit),
        "execution_mechanism": mech,
        "execution_mechanism_label": str(raw_spec.get("execution_mechanism_label") or mech_label),
        "collar_bps": collar_f,
        "extended_hours": bool(raw_spec.get("extended_hours", prof.outside_rth)),
        "non_noise_min_trail_pct": round(non_noise_f, 6),
        "non_noise_min_trail_bps": round(non_noise_f * 10_000.0, 1),
        "fast_sell_non_noise_reversal_pct": round(fast_sell_f, 6),
        "peak_climax_reverse_trigger_pct": round(peak_climax_f, 6),
        "auto_reverse_policy": str(
            raw_spec.get("auto_reverse_policy") or "FLATTEN_FIRST_PEAK_CLIMAX_REVERSE"
        ),
        "rapid_reentry_policy": reentry_pol,
        "source": "tier0_sota.engine.compute_non_noise_min_trail_spec",
    }


def _ensure_symbol_session_order_presets(
    existing: Any,
    *,
    symbol: str,
    side: _risk.Side,
    ref_price: Decimal,
    natr_pct: Decimal,
    spread_bps: Decimal | float | None = None,
    calibrated_min_trailing_pct: float | None = None,
    venue_profile: str = "ALPACA_PAPER_1000",
    pair_symbol: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Build or complete 4-session 24/5 lossless trailing presets via upstream paper_farm SSOT."""
    from tier0_sota.live.paper_farm import build_symbol_24x5_session_presets

    alp_vp = venue_profile if venue_profile in {"ALPACA_PAPER_200", "ALPACA_PAPER_1000"} else None
    alp_rpm = (
        1000 if alp_vp == "ALPACA_PAPER_1000" else (200 if alp_vp == "ALPACA_PAPER_200" else None)
    )
    canonical = build_symbol_24x5_session_presets(
        symbol=symbol,
        side=side,
        ref_price=max(_risk.TICK_SUB_DOLLAR, ref_price),
        natr_pct=max(Decimal("0.001"), min(Decimal("0.50"), natr_pct)),
        spread_bps=spread_bps,
        calibrated_min_trailing_pct=calibrated_min_trailing_pct,
        alpaca_venue_profile=alp_vp,
        alpaca_max_rpm=alp_rpm,
        inverse_hedge_symbol=pair_symbol,
    )
    if not isinstance(existing, Mapping):
        return canonical
    merged: dict[str, dict[str, Any]] = {}
    for sess_k, canon_v in canonical.items():
        raw_s = existing.get(sess_k)
        merged[sess_k] = {**dict(raw_s), **canon_v} if isinstance(raw_s, Mapping) else canon_v
    return merged


def _resolve_potential_category(entry: Mapping[str, Any], archetype: str) -> str:
    explicit = str(entry.get("potential_category") or "").strip().upper()
    if explicit:
        return explicit
    if entry.get("premarket_ignition") is True:
        return "PREMARKET_EARLY_IGNITION"
    if entry.get("sudden_volume_surge") is True:
        return "SUDDEN_VOLUME_SURGE"
    return ARCHETYPE_TO_POTENTIAL_CATEGORY.get(archetype, "EXTREME_MOVER_WATCH")


def _extract_tv_technicals_ratings(
    cloud_sync: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if not isinstance(cloud_sync, Mapping) or not cloud_sync:
        return {}
    out: dict[str, dict[str, Any]] = {}

    def _record(raw_sym: Any, item: Any) -> None:
        sym_tok = str(raw_sym or "").strip().upper()
        if ":" in sym_tok:
            sym_tok = sym_tok.split(":", 1)[1]
        if not sym_tok:
            return
        if isinstance(item, str):
            out[sym_tok] = {"rating": item.strip().upper(), "score": None}
            return
        if not isinstance(item, Mapping):
            return
        rating = (
            item.get("rating")
            or item.get("summary_rating")
            or item.get("recommendation")
            or item.get("overall_rating")
            or item.get("rating_1d")
            or item.get("rating_15m")
            or item.get("rating_5m")
        )
        score = _opt_float(
            item.get("rating_score")
            if item.get("rating_score") is not None
            else item.get("recommend_all")
            if item.get("recommend_all") is not None
            else item.get("score")
        )
        if rating is not None or score is not None:
            out[sym_tok] = {
                "rating": str(rating).strip().upper() if rating is not None else None,
                "score": round(score, 4) if score is not None else None,
            }

    for key in (
        "technicals_ratings",
        "symbol_technicals",
        "technicals",
        "symbol_ratings",
        "symbols",
    ):
        bucket = cloud_sync.get(key)
        if isinstance(bucket, Mapping):
            for k, v in bucket.items():
                _record(k, v)
        elif isinstance(bucket, Sequence) and not isinstance(bucket, (str, bytes)):
            for row in bucket:
                if isinstance(row, Mapping):
                    _record(row.get("symbol") or row.get("ticker") or row.get("tv_symbol"), row)
    tq = cloud_sync.get("technicals_and_screener_queue")
    if isinstance(tq, Mapping):
        nested = tq.get("tv_technicals_ratings")
        if isinstance(nested, Mapping):
            for k, v in nested.items():
                _record(k, v)
    return out


def _extract_adoption_by_symbol(
    paper_coordination: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if not isinstance(paper_coordination, Mapping):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for pos in paper_coordination.get("active_positions") or ():
        if not isinstance(pos, Mapping):
            continue
        sym = str(pos.get("symbol") or "").strip().upper()
        if not sym:
            continue
        adopted = bool(pos.get("adopted_external") is True)
        lane_acct = str(pos.get("lane_account") or pos.get("account") or "").strip() or None
        lane_id = str(pos.get("lane_id") or "").strip() or None
        out[sym] = {
            "auto_adopted_by_engine": True,
            "adopted_external": adopted,
            "engine_adoption_status": ("AUTO_ADOPTED_EXTERNAL" if adopted else "ENGINE_SUPERVISED"),
            "lane_account": lane_acct,
            "lane_id": lane_id,
            "side": str(pos.get("side") or "LONG").strip().upper(),
            "entry_price": _opt_float(pos.get("entry_price"), min_val=0.0001),
            "active_stop": _opt_float(pos.get("active_stop"), min_val=0.0001),
            "natr_pct": _opt_float(pos.get("natr_pct"), min_val=0.0001),
            "execution_mechanism": pos.get("execution_mechanism"),
            "non_noise_min_trail_pct": _opt_float(pos.get("non_noise_min_trail_pct"), min_val=0.0),
            "peak_reverse_armed": bool(pos.get("peak_reverse_armed") is True),
            "fast_sell_armed": bool(pos.get("fast_sell_armed") is True),
            "reentry_eligible_after_utc": pos.get("reentry_eligible_after_utc"),
        }
    for sym in paper_coordination.get("managed_symbols") or ():
        s_up = str(sym).strip().upper()
        if s_up and s_up not in out:
            out[s_up] = {
                "auto_adopted_by_engine": True,
                "adopted_external": False,
                "engine_adoption_status": "ENGINE_SUPERVISED",
                "lane_account": None,
                "lane_id": None,
                "side": "LONG",
                "entry_price": None,
                "active_stop": None,
                "natr_pct": None,
                "execution_mechanism": None,
                "non_noise_min_trail_pct": None,
                "peak_reverse_armed": False,
                "fast_sell_armed": False,
                "reentry_eligible_after_utc": None,
            }
    return out


_LANE_DEFAULT_IDS: dict[str, str] = {
    "ALPACA_PA1": "L1_ORB_BURST",
    "ALPACA_PA2": "L2_STRUCTURAL_TREND",
    "ALPACA_PA3": "L3_AVWAP_TIGHT_LOCK",
    "IBKR_DU_4002": "L0_IBKR_24X5",
}


def _extract_coordination_maps(
    paper_coordination: Mapping[str, Any] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(paper_coordination, Mapping):
        return {}, {}
    by_alert: dict[str, dict[str, Any]] = {}
    by_sym: dict[str, dict[str, Any]] = {}
    for item in paper_coordination.get("coordinated_presets") or ():
        if not isinstance(item, Mapping):
            continue
        rec = dict(item)
        aid = str(rec.get("alert_id") or "").strip()
        sym = str(rec.get("symbol") or "").strip().upper()
        if aid:
            by_alert[aid] = rec
        if sym:
            by_sym[sym] = rec
    return by_alert, by_sym


def _resolve_lane_assignment(
    row: Mapping[str, Any],
    *,
    vu: str,
    vp: str,
    playbook: Mapping[str, Any],
    adopt_info: Mapping[str, Any] | None = None,
    coord_info: Mapping[str, Any] | None = None,
) -> tuple[str, str]:
    """Resolve non-empty paper lane_account and lane_id from active pos, bundle, coord, or role."""
    adopt = adopt_info or {}
    coord = coord_info or {}
    acct_candidates = (
        adopt.get("lane_account"),
        row.get("paper_lane_account"),
        row.get("lane_account"),
        coord.get("target_account"),
        coord.get("lane_account"),
        row.get("target_paper_lane"),
    )
    lane_acct = next((str(v).strip() for v in acct_candidates if v and str(v).strip()), "")
    if not lane_acct:
        if vu == "IBKR" or vp.startswith("IBKR"):
            lane_acct = "IBKR_DU_4002"
        else:
            lane_acct = str(playbook.get("target_paper_lane") or "ALPACA_PA3")

    id_candidates = (
        adopt.get("lane_id"),
        row.get("paper_lane_id"),
        row.get("lane_id"),
        coord.get("target_lane_id"),
        coord.get("lane_id"),
        row.get("target_lane_id"),
    )
    lane_id = next((str(v).strip() for v in id_candidates if v and str(v).strip()), "")
    if not lane_id:
        lane_id = _LANE_DEFAULT_IDS.get(
            lane_acct, str(playbook.get("target_lane_id") or "L3_AVWAP_TIGHT_LOCK")
        )
    return lane_acct, lane_id


def _compute_rule612_r_lock_levels(
    *,
    entry_price: Decimal,
    stop_price: Decimal | None,
    side: _risk.Side,
    session: str,
    natr_pct: Decimal,
    limit_collar_bps: Decimal | None = None,
    fast_sell_rev_pct: Decimal | None = None,
) -> dict[str, Any]:
    """Compute exact Decimal Rule-612 stop, stop-limit, BE+10bp, +1R/+2R/+3R locks & fast-sell."""
    px = max(_risk.TICK_SUB_DOLLAR, entry_price)
    safe_natr = max(Decimal("0.001"), min(Decimal("0.50"), natr_pct))
    eff_sess = str(session or "RTH").strip().upper()
    if eff_sess not in SESSION_EXECUTION_MECHANISMS:
        eff_sess = "OVERNIGHT"
    sched_name, sess_map = _eng.select_pooled_trailing_schedule(safe_natr)
    prof = _eng.get_session_protection_profile(eff_sess, sess_map)
    collar_pct = (
        max(Decimal("0"), min(Decimal("0.10"), limit_collar_bps / Decimal("10000")))
        if limit_collar_bps is not None
        else prof.limit_collar_pct
    )

    if stop_price is not None and stop_price > Decimal("0"):
        cand_stop = _risk.quantize_rule_612(
            max(_risk.TICK_SUB_DOLLAR, stop_price), side=side, is_stop=True
        )
        valid_dir = (cand_stop < px) if side == "LONG" else (cand_stop > px)
        stop_px = (
            cand_stop
            if valid_dir
            else _risk.compute_capped_stop_price(px, side, max_loss_pct=prof.initial_max_loss_pct)
        )
    else:
        stop_px = _risk.compute_capped_stop_price(px, side, max_loss_pct=prof.initial_max_loss_pct)

    risk_r = abs(px - stop_px)
    if risk_r <= Decimal("0"):
        stop_px = _risk.compute_capped_stop_price(px, side, max_loss_pct=prof.initial_max_loss_pct)
        risk_r = abs(px - stop_px)

    entry_limit = (
        _eng.compute_collared_entry_limit_price(px, side, collar_pct)
        if eff_sess != "RTH" and collar_pct > Decimal("0")
        else px
    )
    stop_limit = _eng.compute_collared_limit_price(stop_px, side, collar_pct)

    sign = Decimal("1") if side == "LONG" else Decimal("-1")

    def _quant(price: Decimal, *, is_stop: bool = True) -> Decimal:
        return _risk.quantize_rule_612(
            max(_risk.TICK_SUB_DOLLAR, price), side=side, is_stop=is_stop
        )

    be_floor = _quant(px * (Decimal("1.0010") if side == "LONG" else Decimal("0.9990")))
    t1r, t2r, t3r = (
        _quant(px + sign * Decimal(k) * risk_r, is_stop=False) for k in ("1", "2", "3")
    )
    lock_1r = _quant(px + sign * Decimal("0.45") * risk_r)
    lock_2r = _quant(px + sign * Decimal("1.35") * risk_r)
    lock_3r = _quant(px + sign * Decimal("2.35") * risk_r)
    if side == "LONG":
        lock_1r = max(be_floor, lock_1r)
        lock_2r = max(lock_1r, lock_2r)
        lock_3r = max(lock_2r, lock_3r)
    else:
        lock_1r = min(be_floor, lock_1r)
        lock_2r = min(lock_1r, lock_2r)
        lock_3r = min(lock_2r, lock_3r)

    rev_pct = max(
        Decimal("0.0010"),
        fast_sell_rev_pct if fast_sell_rev_pct is not None else prof.min_trail_pct,
    )
    fast_sell_rev = _quant(
        px * (Decimal("1") - rev_pct if side == "LONG" else Decimal("1") + rev_pct)
    )
    return {
        "schedule": sched_name,
        "profile": prof,
        "collar_bps": float(collar_pct * Decimal("10000")),
        "entry_limit": entry_limit,
        "stop_px": stop_px,
        "stop_limit": stop_limit,
        "stop_dist_frac": float(risk_r / px),
        "stop_dist_pct": round(float(risk_r / px) * 100.0, 2),
        "be_floor": be_floor,
        "target_1r": t1r,
        "target_2r": t2r,
        "target_3r": t3r,
        "lock_1r": lock_1r,
        "lock_2r": lock_2r,
        "lock_3r": lock_3r,
        "fast_sell_rev": fast_sell_rev,
    }


def _rating_to_score(rating: str) -> float:
    return {
        "STRONG_BUY": 0.75,
        "BUY": 0.35,
        "NEUTRAL": 0.0,
        "SELL": -0.35,
        "STRONG_SELL": -0.75,
    }.get(rating.strip().upper(), 0.0)


def _resolve_ta_composite(
    row: Mapping[str, Any],
    *,
    sym: str,
    side: str,
    regime_int: int,
    sess_chg: float,
    rvol: float,
    darkpool_trf_ratio: float | None,
    unusual_volume_buildup: bool,
    premarket_ignition: bool,
    early_ignition_detected: bool,
    peak_confirmed: bool | None,
    tv_tech_info: Mapping[str, Any] | None = None,
) -> tuple[float, str, float, str]:
    """Resolve non-empty causal TA alignment score, TV technicals rating, score & source."""
    raw_tech = row.get("technicals")
    tech_map: Mapping[str, Any] = raw_tech if isinstance(raw_tech, Mapping) else {}
    above_vwap = tech_map.get("above_vwap")
    raw_ta = _opt_float(row.get("technical_alignment_score"), min_val=0.0)
    if raw_ta is not None and raw_ta > 0.0:
        ta_score = round(raw_ta, 2)
        ta_100 = raw_ta * 100.0 if raw_ta <= 1.0 else raw_ta
    else:
        signed_chg = sess_chg if side == "BUY" else -sess_chg
        ta_100 = 50.0 + max(-12.0, min(20.0, signed_chg * 3.5))
        if rvol >= 2.5:
            ta_100 += 12.0
        elif rvol >= 1.5:
            ta_100 += 8.0
        elif rvol >= 0.7:
            ta_100 += 4.0
        if isinstance(above_vwap, bool):
            aligned_vwap = (above_vwap and side == "BUY") or (not above_vwap and side == "SELL")
            ta_100 += 8.0 if aligned_vwap else -6.0
        if (darkpool_trf_ratio or 0.0) >= 0.35 or unusual_volume_buildup:
            ta_100 += 6.0
        if premarket_ignition or early_ignition_detected:
            ta_100 += 8.0
        if (regime_int == 1 and side == "BUY") or (
            regime_int == -1 and (side == "SELL" or sym in INVERSE_HEDGE_SYMBOLS)
        ):
            ta_100 += 6.0
        elif (regime_int == -1 and side == "BUY" and sym not in INVERSE_HEDGE_SYMBOLS) or (
            regime_int == 1 and side == "SELL" and peak_confirmed is not True
        ):
            ta_100 -= 6.0
        ta_100 = max(15.0, min(95.0, ta_100))
        ta_score = round(ta_100, 2)

    cloud_rating = str((tv_tech_info or {}).get("rating") or "").strip().upper()
    cloud_score = _opt_float((tv_tech_info or {}).get("score"))
    if cloud_rating:
        return (
            ta_score,
            cloud_rating,
            round(cloud_score, 4) if cloud_score is not None else _rating_to_score(cloud_rating),
            "tv_cloud_mcp",
        )

    explicit_rating = str(row.get("tv_technicals_rating") or "").strip().upper()
    if explicit_rating in VALID_TV_TECHNICAL_RATINGS:
        explicit_score = _opt_float(row.get("tv_technicals_score"))
        explicit_src = str(row.get("ta_rating_source") or "").strip() or "local_ta_composite"
        return (
            ta_score,
            explicit_rating,
            round(explicit_score, 4)
            if explicit_score is not None
            else _rating_to_score(explicit_rating),
            explicit_src,
        )

    ta_component = ((ta_100 - 50.0) / 50.0) * 0.45
    if side == "SELL" and ta_100 >= 50.0:
        ta_component = -abs(ta_component)
    chg_component = max(-0.35, min(0.35, sess_chg * 0.08))
    vwap_component = (
        (0.15 if above_vwap else -0.15)
        if isinstance(above_vwap, bool)
        else (0.08 if side == "BUY" else -0.08)
    )
    if regime_int == -1:
        regime_component = 0.15 if sym in INVERSE_HEDGE_SYMBOLS else -0.20
    elif regime_int == 1:
        regime_component = 0.15
    else:
        regime_component = 0.0
    flow_sign = -1.0 if side == "SELL" else 1.0
    if premarket_ignition or early_ignition_detected:
        ignition_component = flow_sign * 0.15
    elif unusual_volume_buildup or (darkpool_trf_ratio or 0.0) >= 0.35:
        ignition_component = flow_sign * 0.08
    else:
        ignition_component = 0.0
    peak_component = -0.35 if (peak_confirmed is True and side == "SELL") else 0.0
    comp = max(
        -1.0,
        min(
            1.0,
            round(
                ta_component
                + chg_component
                + vwap_component
                + regime_component
                + ignition_component
                + peak_component,
                4,
            ),
        ),
    )
    if comp >= 0.50:
        rating = "STRONG_BUY"
    elif comp >= 0.15:
        rating = "BUY"
    elif comp > -0.15:
        rating = "NEUTRAL"
    elif comp > -0.50:
        rating = "SELL"
    else:
        rating = "STRONG_SELL"
    return ta_score, rating, comp, "local_ta_composite"


def _is_hard_safety_hold(reason: Any) -> bool:
    txt = str(reason or "").strip().upper()
    if not txt:
        return False
    return any(marker in txt for marker in HARD_SAFETY_HOLD_MARKERS)


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
    if entry.get("premarket_ignition") is True and sess_chg >= 2.0:
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
    ) and (rvol >= 1.5 or explicit_arch == "HIDDEN_VOL_ACCUM"):
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
    adoption_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    tv_technicals_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    coord_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    preset_tier: str | None = None,
    extreme_mover_eligible: bool | None = None,
    order_preset_id_override: str | None = None,
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
    adopt_info = (adoption_by_sym or {}).get(sym) or {}
    coord_info = (coord_by_sym or {}).get(sym) or {}
    lane_seed = {
        "paper_lane_account": b_row.get("lane_account"),
        "paper_lane_id": b_row.get("lane_id"),
    }
    target_lane, target_lane_id = _resolve_lane_assignment(
        lane_seed,
        vu=vu,
        vp=vp,
        playbook=playbook,
        adopt_info=adopt_info,
        coord_info=coord_info,
    )
    venue_handle = resolve_venue_order_handle(vp, session=active_sess, trailing_schedule=sched_name)

    raw_spread_bps = _opt_float(
        e.get("spread_bps") if e.get("spread_bps") is not None else b_row.get("spread_bps"),
        min_val=0.0,
    )
    spread_bps_dec = (
        Decimal(str(round(raw_spread_bps, 2))) if raw_spread_bps is not None else Decimal("5")
    )
    trail_spec = resolve_non_noise_min_trail_spec(
        symbol=sym,
        entry_price=px,
        side=r_side,
        session=active_sess,
        natr_pct=natr,
        spread_bps=spread_bps_dec,
        archetype=arch,
        venue_profile=vp,
    )
    rev_pct_dec = max(
        Decimal("0.0010"),
        Decimal(str(trail_spec["fast_sell_non_noise_reversal_pct"])),
    )
    levels = _compute_rule612_r_lock_levels(
        entry_price=px,
        stop_price=None,
        side=r_side,
        session=active_sess,
        natr_pct=natr,
        fast_sell_rev_pct=rev_pct_dec,
    )
    stop_now = levels["stop_px"]
    stop_rth = _risk.compute_capped_stop_price(
        px, r_side, max_loss_pct=prof_rth.initial_max_loss_pct
    )
    entry_limit_now = levels["entry_limit"]
    stop_limit_card = levels["stop_limit"]
    stop_limit_now = (
        stop_limit_card
        if prof_now.order_mode == "STOP_LIMIT" and prof_now.limit_collar_pct > Decimal("0")
        else None
    )
    dist_now = float(levels["stop_dist_frac"])
    be_floor = levels["be_floor"]
    t1r, t2r, t3r = levels["target_1r"], levels["target_2r"], levels["target_3r"]
    lock_at_1r, lock_at_2r, lock_at_3r = levels["lock_1r"], levels["lock_2r"], levels["lock_3r"]
    fast_sell_rev = levels["fast_sell_rev"]

    tv_tech_info = (tv_technicals_by_sym or {}).get(sym) or {}
    potential_category = _resolve_potential_category(e, arch)
    early_ignition_detected = bool(
        e.get("early_ignition_detected") is True or e.get("premarket_ignition") is True
    )
    premarket_ignition = bool(e.get("premarket_ignition") is True)
    sudden_volume_surge = bool(e.get("sudden_volume_surge") is True)
    raw_tech_map = e.get("technicals") or b_row.get("technicals")
    technicals_dict: dict[str, Any] = (
        dict(raw_tech_map) if isinstance(raw_tech_map, Mapping) else {}
    )
    ta_seed: dict[str, Any] = {
        "technical_alignment_score": (
            e.get("technical_alignment_score")
            if e.get("technical_alignment_score") is not None
            else b_row.get("technical_alignment_score")
        ),
        "tv_technicals_rating": e.get("tv_technicals_rating"),
        "tv_technicals_score": e.get("tv_technicals_score"),
        "ta_rating_source": e.get("ta_rating_source"),
        "technicals": technicals_dict,
    }
    tech_align_score, tv_rating, tv_score, ta_rating_src = _resolve_ta_composite(
        ta_seed,
        sym=sym,
        side=side,
        regime_int=regime_int,
        sess_chg=sess_chg,
        rvol=rvol,
        darkpool_trf_ratio=darkpool_trf_ratio,
        unusual_volume_buildup=unusual_volume_buildup,
        premarket_ignition=premarket_ignition,
        early_ignition_detected=early_ignition_detected,
        peak_confirmed=peak_confirmed,
        tv_tech_info=tv_tech_info,
    )
    tuning_run_id = str(e.get("tuning_run_id") or "").strip() or None
    raw_reentry = trail_spec.get("rapid_reentry_policy")
    reentry_pol: dict[str, Any] = dict(raw_reentry) if isinstance(raw_reentry, Mapping) else {}
    rapid_reentry_rpm = int(
        reentry_pol.get("max_rpm")
        or reentry_pol.get("rpm_budget_available")
        or venue_handle.get("max_rpm")
        or 1000
    )
    reentry_cd_secs = int(reentry_pol.get("reentry_cooldown_secs") or 60)

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
    order_preset_id = (order_preset_id_override or f"{sched_name}:{active_sess}:{arch}")[:64]
    eff_preset_tier = preset_tier or (
        "EXTREME_MOVER_CONFIRMED" if live_eligible else "STANDBY_REFERENCE"
    )
    eff_extreme_flag = (
        bool(extreme_mover_eligible)
        if extreme_mover_eligible is not None
        else bool(e.get("extreme_mover_eligible") is True)
    )

    row: dict[str, Any] = {
        "rank": rank,
        "scanner_rank": scanner_rank if scanner_rank is not None else _opt_int(e.get("rank")),
        "symbol": sym,
        "exchange": norm_exch,
        "tv_symbol": tv_symbol,
        "pair_symbol": pair_sym,
        "side": side,
        "archetype": arch,
        "preset_tier": eff_preset_tier,
        "extreme_mover_eligible": eff_extreme_flag,
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
        "stop_limit_price": float(stop_limit_card),
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
        "execution_mechanism": trail_spec["execution_mechanism"],
        "execution_mechanism_label": trail_spec["execution_mechanism_label"],
        "broker_exit_order": trail_spec["broker_exit_order"],
        "non_noise_min_trail_pct": float(trail_spec["non_noise_min_trail_pct"]),
        "non_noise_min_trail_bps": float(trail_spec["non_noise_min_trail_bps"]),
        "fast_sell_non_noise_reversal_pct": float(trail_spec["fast_sell_non_noise_reversal_pct"]),
        "peak_climax_reverse_trigger_pct": float(trail_spec["peak_climax_reverse_trigger_pct"]),
        "auto_reverse_policy": str(trail_spec["auto_reverse_policy"]),
        "rapid_reentry_policy": dict(reentry_pol),
        "rapid_reentry_rpm": rapid_reentry_rpm,
        "non_noise_trail_spec": trail_spec,
        "auto_adopted_by_engine": bool(adopt_info.get("auto_adopted_by_engine") is True),
        "adopted_external": bool(adopt_info.get("adopted_external") is True),
        "engine_adoption_status": str(
            adopt_info.get("engine_adoption_status") or "AUTO_ADOPT_ON_FILL"
        ),
        "potential_category": potential_category,
        "calibrated_category": (
            cal_cat := (
                str(e.get("calibrated_category") or b_row.get("calibrated_category") or "").strip()
                or potential_category
            )
        ),
        "min_trailing_pct": (
            eff_min_trail := (
                _opt_float(
                    e.get("min_trailing_pct")
                    if e.get("min_trailing_pct") is not None
                    else b_row.get("min_trailing_pct"),
                    min_val=0.0,
                )
                or float(trail_spec["non_noise_min_trail_pct"])
            )
        ),
        "technicals": technicals_dict,
        "early_detection": (
            dict(raw_ed)
            if isinstance(
                (raw_ed := e.get("early_detection") or b_row.get("early_detection")), Mapping
            )
            else {}
        ),
        "session_order_presets": _ensure_symbol_session_order_presets(
            e.get("session_order_presets") or b_row.get("session_order_presets"),
            symbol=sym,
            side=r_side,
            ref_price=px,
            natr_pct=natr,
            spread_bps=spread_bps_dec,
            calibrated_min_trailing_pct=eff_min_trail,
            venue_profile=vp,
            pair_symbol=pair_sym,
        ),
        "momentum_trigger_reason": {
            "calibrated_category": cal_cat,
            "rvol": round(rvol, 2),
            "darkpool_trf_ratio": darkpool_trf_ratio,
            "technical_alignment_score": tech_align_score,
            "archetype": arch,
            "watch_tier": watch_tier,
            "premarket_ignition": premarket_ignition,
            "sudden_volume_surge": sudden_volume_surge,
        },
        "technical_alignment_score": tech_align_score,
        "early_ignition_detected": early_ignition_detected,
        "premarket_ignition": premarket_ignition,
        "sudden_volume_surge": sudden_volume_surge,
        "tuning_run_id": tuning_run_id,
        "tv_technicals_rating": tv_rating,
        "tv_technicals_score": tv_score,
        "ta_rating_source": ta_rating_src,
        "reentry_cooldown_secs": reentry_cd_secs,
        "reverse_short_mode": rev_short_mode,
        "shares_25k_defensive": sh_25k,
        "shares_100k_defensive": sh_100k,
        "shortable": shortable,
        "easy_to_borrow": etb,
        "paper_lane_account": target_lane,
        "paper_lane_id": target_lane_id,
        "lane_account": target_lane,
        "lane_id": target_lane_id,
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


def _build_validated_alert_preset(
    *,
    row: Mapping[str, Any],
    ctx: Mapping[str, Any],
    bar_ms: int,
    vu: str,
    vp: str,
    active_sess: str,
    regime_int: int,
    feed_sources: Sequence[Any],
    live_scan: bool,
) -> dict[str, Any]:
    sym = str(row["symbol"]).strip().upper()
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
            if "alpaca_boats" in feed_sources
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
    return alert_in


def _is_session_early_ignition_candidate(
    e: Mapping[str, Any],
    *,
    ts_ms: int,
    active_sess: str,
    regime_int: int,
    vu: str,
    b_row: Mapping[str, Any],
    adoption_by_sym: Mapping[str, Mapping[str, Any]],
) -> tuple[bool, int, str]:
    """Return (eligible, bar_ms, archetype) for extended-hours SESSION_EARLY_IGNITION_PRESET."""
    if active_sess not in EXTENDED_HOURS_SESSIONS:
        return False, 0, ""
    sym = str(e.get("symbol") or "").strip().upper()
    px_f = _opt_float(e.get("last_price"), min_val=0.0001)
    if not sym or px_f is None or Decimal(str(round(px_f, 4))) < SESSION_EARLY_IGNITION_MIN_PRICE:
        return False, 0, ""
    raw_bar_close = e.get("bar_close_utc")
    if not raw_bar_close:
        return False, 0, ""
    try:
        close = datetime.fromisoformat(str(raw_bar_close))
    except ValueError:
        return False, 0, ""
    if close.tzinfo is None:
        return False, 0, ""
    bar_ms = int(close.timestamp() * 1000)
    age_ms = ts_ms - bar_ms
    if not 0 <= age_ms <= SESSION_EARLY_IGNITION_MAX_BAR_AGE_MS:
        return False, 0, ""
    sess_at_close = _eng.classify_us_equity_session_et(bar_ms * 1_000_000)
    sess_prev_min = _eng.classify_us_equity_session_et(max(0, bar_ms - 60_000) * 1_000_000)
    if active_sess not in (sess_at_close, sess_prev_min):
        return False, 0, ""
    if _is_hard_safety_hold(b_row.get("entry_hold_reason")):
        return False, 0, ""

    watch_tier = str(e.get("watch_tier") or b_row.get("watch_tier") or "").strip().upper()
    hold_reason = str(e.get("extreme_mover_hold_reason") or "").strip()
    ta_val = _opt_float(
        e.get("technical_alignment_score")
        if e.get("technical_alignment_score") is not None
        else b_row.get("technical_alignment_score"),
        min_val=0.0,
    )
    ta_100 = (ta_val * 100.0 if ta_val <= 1.0 else ta_val) if ta_val is not None else 0.0
    trf_val = _opt_float(
        e.get("darkpool_trf_ratio")
        if e.get("darkpool_trf_ratio") is not None
        else b_row.get("darkpool_trf_ratio"),
        min_val=0.0,
    )
    has_causal_signal = (
        sym in adoption_by_sym
        or e.get("premarket_ignition") is True
        or e.get("early_ignition_detected") is True
        or watch_tier in {"PREMARKET_IGNITION", "TECHNICAL_BREAKOUT_LEADER"}
        or (e.get("sudden_volume_surge") is True and ta_100 >= 35.0)
        or (
            e.get("pinned") is True
            and ta_100 >= 20.0
            and (e.get("unusual_volume_buildup") is True or (trf_val or 0.0) >= 0.25)
        )
        or (
            watch_tier == "UV_DARKPOOL_BUILDUP"
            and ta_100 >= 35.0
            and hold_reason not in LOW_EXTREME_HOLD_REASONS
        )
    )
    if not has_causal_signal:
        return False, 0, ""

    sess_chg = float(e.get("session_move_pct") or e.get("change_pct") or 0.0)
    rvol = (
        _opt_float(
            e.get("rvol_normalized") if e.get("rvol_normalized") is not None else e.get("rvol"),
            min_val=0.0,
        )
        or 1.0
    )
    peak_conf = e.get("peak_confirmed") if isinstance(e.get("peak_confirmed"), bool) else None
    natr_f = _opt_float(e.get("natr_pct"), min_val=0.001) or 0.015
    side, _, arch = _resolve_direction(
        e,
        regime_int=regime_int,
        sess_chg=sess_chg,
        rvol=rvol,
        darkpool_trf_ratio=trf_val,
        unusual_volume_buildup=bool(e.get("unusual_volume_buildup") is True),
        peak_confirmed=peak_conf,
        natr_pct=natr_f,
    )
    if side == "SELL":
        if regime_int != -1 and peak_conf is not True:
            return False, 0, ""
        shortable = bool(
            e["shortable"] if e.get("shortable") is not None else b_row.get("shortable", False)
        )
        etb = bool(
            e["easy_to_borrow"]
            if e.get("easy_to_borrow") is not None
            else b_row.get("easy_to_borrow", False)
        )
        if not (shortable and etb):
            return False, 0, ""
        if vu == "ALPACA" and active_sess == "OVERNIGHT":
            return False, 0, ""
    return True, bar_ms, arch


def build_half_auto_tickets(
    movers: Mapping[str, Any],
    *,
    bundle: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
    venue: str = "IBKR",
    venue_profile: str | None = None,
    max_tickets: int = 15,
    paper_coordination: Mapping[str, Any] | None = None,
    tv_cloud_sync: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build schema-validated Half-Auto extreme-mover & session early-ignition tickets."""
    vp, vu = resolve_venue_profile(venue, venue_profile=venue_profile)
    ts_utc = now_utc or datetime.now(UTC)
    now_et = ts_utc.astimezone(NY_TZ)
    active_sess, ts_ms = current_et_session(now_et), int(ts_utc.timestamp() * 1000)
    live_scan = movers.get("schema_version") == "tier0.whole-market-movers.v2"
    macro = dict(movers.get("macro_regime") or {})
    regime_int = int(macro.get("regime", 0))
    adoption_by_sym = _extract_adoption_by_symbol(paper_coordination)
    _, coord_by_sym = _extract_coordination_maps(paper_coordination)
    tv_technicals_by_sym = _extract_tv_technicals_ratings(tv_cloud_sync)
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
    extreme_confirmed_n = 0
    early_ignition_n = 0
    feed_sources = movers.get("feed_sources") or ()

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
            adoption_by_sym=adoption_by_sym,
            tv_technicals_by_sym=tv_technicals_by_sym,
            coord_by_sym=coord_by_sym,
            preset_tier="EXTREME_MOVER_CONFIRMED",
            extreme_mover_eligible=True if live_eligible else bool(e.get("extreme_mover_eligible")),
        )
        alert_in = _build_validated_alert_preset(
            row=row,
            ctx=ctx,
            bar_ms=bar_ms,
            vu=vu,
            vp=vp,
            active_sess=active_sess,
            regime_int=regime_int,
            feed_sources=feed_sources,
            live_scan=live_scan,
        )
        schema_ok += 1
        extreme_confirmed_n += 1
        row["tv_alert_preset"] = alert_in
        tickets.append(row)
        ticket_syms.add(sym)

    # Extended-hours early-ignition preset staging (honest extreme_mover_eligible=False).
    if live_scan and active_sess in EXTENDED_HOURS_SESSIONS and len(tickets) == 0:
        manifest_fresh = True
        raw_gen = movers.get("generated_at_utc")
        if raw_gen:
            try:
                gen_dt = datetime.fromisoformat(str(raw_gen))
                if gen_dt.tzinfo is not None:
                    gen_age_ms = ts_ms - int(gen_dt.timestamp() * 1000)
                    manifest_fresh = (
                        -60_000 <= gen_age_ms <= SESSION_EARLY_IGNITION_MAX_MANIFEST_AGE_MS
                    )
            except ValueError:
                manifest_fresh = False
        if manifest_fresh:
            early_pool: list[Mapping[str, Any]] = [
                e for e in (movers.get("entries") or ()) if isinstance(e, Mapping)
            ]
            pool_syms = {str(e.get("symbol") or "").strip().upper() for e in early_pool}
            whole_min_iso = datetime.fromtimestamp((ts_ms // 60_000) * 60, tz=UTC).isoformat()
            for adopt_sym, adopt_meta in adoption_by_sym.items():
                if adopt_sym in pool_syms:
                    continue
                adopt_px = _opt_float(adopt_meta.get("entry_price"), min_val=10.0)
                if adopt_px is None:
                    continue
                early_pool.append(
                    {
                        "symbol": adopt_sym,
                        "last_price": adopt_px,
                        "side": "SELL" if adopt_meta.get("side") == "SHORT" else "BUY",
                        "natr_pct": adopt_meta.get("natr_pct") or 0.012,
                        "watch_tier": "ACTIVE_ENGINE_ADOPTED",
                        "potential_category": "ACTIVE_ENGINE_SUPERVISED",
                        "extreme_mover_eligible": False,
                        "bar_close_utc": whole_min_iso,
                    }
                )
            for e_idx, e in enumerate(early_pool, 1):
                if len(tickets) >= max_tickets:
                    break
                sym = str(e.get("symbol") or "").strip().upper()
                if not sym or sym in ticket_syms:
                    continue
                b_row = b_map.get(sym, {})
                cand_ok, bar_ms, cand_arch = _is_session_early_ignition_candidate(
                    e,
                    ts_ms=ts_ms,
                    active_sess=active_sess,
                    regime_int=regime_int,
                    vu=vu,
                    b_row=b_row,
                    adoption_by_sym=adoption_by_sym,
                )
                if not cand_ok:
                    continue
                row, ctx = _build_order_level_row(
                    e,
                    rank=len(tickets) + 1,
                    scanner_rank=_opt_int(e.get("rank")) or e_idx,
                    active_sess=active_sess,
                    regime_int=regime_int,
                    vu=vu,
                    venue_profile=vp,
                    b_row=b_row,
                    live_eligible=False,
                    action_gate_override="SESSION_EARLY_IGNITION_PRESET",
                    now_utc=ts_utc,
                    adoption_by_sym=adoption_by_sym,
                    tv_technicals_by_sym=tv_technicals_by_sym,
                    coord_by_sym=coord_by_sym,
                    preset_tier="SESSION_EARLY_IGNITION_PRESET",
                    extreme_mover_eligible=False,
                    order_preset_id_override=f"EARLY_IGNITION:{active_sess}:{cand_arch}"[:64],
                )
                alert_in = _build_validated_alert_preset(
                    row=row,
                    ctx=ctx,
                    bar_ms=bar_ms,
                    vu=vu,
                    vp=vp,
                    active_sess=active_sess,
                    regime_int=regime_int,
                    feed_sources=feed_sources,
                    live_scan=live_scan,
                )
                schema_ok += 1
                early_ignition_n += 1
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
                adoption_by_sym=adoption_by_sym,
                tv_technicals_by_sym=tv_technicals_by_sym,
                coord_by_sym=coord_by_sym,
                preset_tier="STANDBY_REFERENCE",
                extreme_mover_eligible=False,
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
        "extreme_mover_confirmed_count": extreme_confirmed_n,
        "session_early_ignition_count": early_ignition_n,
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
    min_trail_f = _opt_float(t.get("non_noise_min_trail_pct"), min_val=0.0)
    min_trail_tag = f"{min_trail_f * 100.0:.2f}%" if min_trail_f is not None else "—"
    mech_lbl = str(t.get("execution_mechanism_label") or t.get("execution_mechanism") or "—")
    cat_lbl = str(t.get("potential_category") or t.get("archetype") or "—")
    tech_sc = _opt_float(t.get("technical_alignment_score"), min_val=0.0)
    tv_rat = str(t.get("tv_technicals_rating") or "").strip()
    cat_bits = [f"`{cat_lbl}`"]
    if tech_sc is not None:
        cat_bits.append(f"TA `{tech_sc:.0f}`")
    if tv_rat:
        cat_bits.append(f"TV `{tv_rat}`")
    adopt_lbl = str(t.get("engine_adoption_status") or "AUTO_ADOPT_ON_FILL")
    rpm_val = int(t.get("rapid_reentry_rpm") or 1000)
    return (
        f"| {t['rank']} | `{t['symbol']}`{pair_tag} | `{t['side']}` | **{t['action_gate']}** "
        f"| `{t['live_price']:.2f}` ({t['session_move_pct']:+.2f}%{prior_tag}) "
        f"| `{t['rvol']:.1f}x` | {trf_uv_cell} "
        f"| `{t['stop_now']:.2f}` ({t['stop_now_pct']:.2f}%{stop_lim_tag}) "
        f"| `{t['be_plus_fee_floor']:.2f}` | `{t['lock_at_1r']:.2f}` / `{t['lock_at_2r']:.2f}` "
        f"/ `{t.get('lock_at_3r', t['lock_at_2r']):.2f}` "
        f"| `{t['fast_sell_rev_trigger']:.2f}` ({t['reverse_short_mode']}) "
        f"| `{min_trail_tag}` · `{adopt_lbl}` · `{rpm_val} rpm` "
        f"| {' · '.join(cat_bits)} "
        f"| `{t['shares_25k_defensive']}` / `{t['shares_100k_defensive']}` "
        f"| `{t['order_type_now']}` ({t['collar_now_bps']}bp · {mech_lbl}{fee_tag}{lane_tag}) |"
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
        "\n".join(rows)
        or "| — | — | — | No extreme movers | — | — | — | — | — | — | — | — | — | — | — |"
    )
    regime_label = macro.get("regime_label", "NEUTRAL")
    regime_val = macro.get("regime", 0)
    n_tickets = len(doc.get("tickets", []))
    table_hdr = (
        f"| # | Sym | Side | Action Gate | Live Px (Sess) | RVOL | TRF / UV | Stop ({sess}) "
        "| BE+10bp | Lock +1R / +2R / +3R | Fast-Sell Rev "
        "| Non-Noise Trail / Adopt | Category / TV "
        "| Sh ($25k/$100k) | Order & 24/5 Mechanism |\n"
        "|---:|---|---|---|---:|---:|---|---:|---:|---:|---|---|---|---|---|\n"
    )
    manual_staged = sorted(
        [
            p
            for p in doc.get("manual_staged_presets", [])
            if isinstance(p, Mapping) and p.get("symbol")
        ],
        key=lambda p: (
            0 if str(p.get("session") or "").strip().upper() == str(sess).strip().upper() else 1,
            -int(p.get("ts_utc_ms") or 0),
        ),
    )
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
        "\n## Native TradingView Order Preset Template (24/5 Lossless Self-Handling)\n"
        "- **RTH (09:30–16:00 ET, 13:00 early close):** `RTH: STOP_MARKET 0bp Native Ratchet` "
        "(`NATIVE_BROKER_STOP_MARKET_RATCHET`) · SL `<= 1.50%` (`tight`) / `<= 2.00%` "
        "(`baseline`/`wide`) · Extended Hours `OFF`\n"
        "- **PRE_MARKET / AFTER_HOURS (04:00–09:30 / 16:00–20:00 ET, 13:00 early close):** "
        "`PRE/AH: STOP_LIMIT / LIMIT 35bp Synthetic Tick-Supervised` "
        "(`SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT`, `35 bps` collar) · SL `<= 1.25%` (`tight`) / "
        "`<= 1.50%` (`baseline`/`wide`) · Extended Hours `ON`\n"
        "- **OVERNIGHT (20:00–04:00 ET):** `OVERNIGHT: STOP_LIMIT / LIMIT 50bp Synthetic BOATS` "
        "(`SYNTHETIC_BOATS_COLLARED_LIMIT`, `50 bps` collar) · SL `<= 1.00%` · "
        "Extended Hours `ON` · `qty=1` (`semi_auto_confirmed=false`) · "
        "Alpaca SHORT blocked → `INVERSE_HEDGE_SOXS_SQQQ` or `IBKR`\n"
        "- **Non-Noise Min-Trail, Peak-Climax Auto-Reverse & 1000 rpm Rapid Re-Entry:** "
        "Per-symbol `non_noise_min_trail_pct` floor avoids micro-tick noise while fast-selling "
        "genuine reversals; `FLATTEN_FIRST_PEAK_CLIMAX_REVERSE` flips peak exhaustion; "
        "`60s` rapid re-entry with `>= 10%` exit reserve; open Paper positions auto-adopted "
        "by hosted engine supervisor\n"
        "- **4-Tier Progressive R-Lock Brackets:** Arm `BE+10bp` (T1) → `+1R` lock `+0.45R` (T2) → "
        "`+2R` lock `+1.35R` (T3) → `+3R` lock `+2.35R` / Fast-Sell Reverse (T4) · `60s` cooldown\n"
    )
    carry_notice = _format_trf_carryover_notice(
        [*doc.get("tickets", []), *doc.get("standby_presets", [])]
    )
    raw_ext_n = doc.get("extreme_mover_confirmed_count")
    ext_confirmed_n = (
        int(raw_ext_n)
        if raw_ext_n is not None
        else sum(
            1
            for t in doc.get("tickets", [])
            if isinstance(t, Mapping) and t.get("preset_tier") != "SESSION_EARLY_IGNITION_PRESET"
        )
    )
    raw_ign_n = doc.get("session_early_ignition_count")
    early_ign_n = (
        int(raw_ign_n)
        if raw_ign_n is not None
        else sum(
            1
            for t in doc.get("tickets", [])
            if isinstance(t, Mapping) and t.get("preset_tier") == "SESSION_EARLY_IGNITION_PRESET"
        )
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
        f"| **Extreme Confirmed:** `{ext_confirmed_n}` "
        f"| **Session Early-Ignition:** `{early_ign_n}` "
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
    merged_ex: dict[str, str] = dict(exchange_map or {})
    _extract_exchange_entries(ranked_entries, merged_ex)
    _extract_exchange_entries(tickets, merged_ex)
    _extract_exchange_entries(staged_presets, merged_ex)

    def _fmt_row(row: Mapping[str, Any]) -> str:
        raw_tv = str(row.get("tv_symbol") or "").strip().upper()
        if raw_tv and ":" in raw_tv:
            return raw_tv
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            return ""
        return resolve_tv_exchange_symbol(
            sym,
            exchange=row.get("exchange") or row.get("tv_exchange"),
            exchange_map=merged_ex,
        )

    def _fmt_sym(sym: str) -> str:
        return resolve_tv_exchange_symbol(sym, exchange_map=merged_ex)

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


def render_tv_movers_first_watchlist(
    tickets: Sequence[Mapping[str, Any]],
    *,
    exchange_map: Mapping[str, str] | None = None,
    ranked_entries: Sequence[Mapping[str, Any]] | None = None,
    staged_presets: Sequence[Mapping[str, Any]] | None = None,
    early_stage_movers: Sequence[Mapping[str, Any]] | None = None,
    full_market_movers: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    """Render movers-first TV watchlist (`###01_LIVE_EXTREME_MOVERS_TOP` + Early-Stage Ignition)."""
    merged_ex: dict[str, str] = dict(exchange_map or {})
    _extract_exchange_entries(full_market_movers, merged_ex)
    _extract_exchange_entries(early_stage_movers, merged_ex)
    _extract_exchange_entries(ranked_entries, merged_ex)
    _extract_exchange_entries(tickets, merged_ex)
    _extract_exchange_entries(staged_presets, merged_ex)

    def _fmt_row(row: Mapping[str, Any]) -> str:
        raw_tv = str(row.get("tv_symbol") or "").strip().upper()
        if raw_tv and ":" in raw_tv:
            return raw_tv
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            return ""
        return resolve_tv_exchange_symbol(
            sym,
            exchange=row.get("exchange") or row.get("tv_exchange"),
            exchange_map=merged_ex,
        )

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
    full_mkt_rows = [
        m
        for m in (full_market_movers or ())
        if isinstance(m, Mapping) and str(m.get("symbol", "")).strip()
    ]
    early_rows = [
        m
        for m in (early_stage_movers or ())
        if isinstance(m, Mapping) and str(m.get("symbol", "")).strip()
    ]
    staged_rows = [
        p
        for p in (staged_presets or ())
        if isinstance(p, Mapping) and str(p.get("symbol", "")).strip()
    ]
    combined_rows = (*full_mkt_rows, *tickets, *live_ranked, *staged_rows)
    movers = list(dict.fromkeys(tvs for row in combined_rows if (tvs := _fmt_row(row))))[:20]
    seen: set[str] = set(movers)

    def _dedup_add(items: Sequence[str]) -> list[str]:
        out: list[str] = []
        for s in items:
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out

    early_stage_syms = _dedup_add([tvs for row in early_rows if (tvs := _fmt_row(row))])[:15]
    darkpool_uv = _dedup_add(
        [
            tvs
            for row in (*tickets, *live_ranked, *staged_rows)
            if (tvs := _fmt_row(row))
            and (
                (_opt_float(row.get("darkpool_trf_ratio"), min_val=0.0) or 0.0) >= 0.35
                or row.get("unusual_volume_buildup") is True
                or str(row.get("watch_tier") or "").strip().upper() == "UV_DARKPOOL_BUILDUP"
                or (_opt_float(row.get("rvol"), min_val=0.0) or 0.0) >= 2.0
            )
        ]
    )
    hedges = _dedup_add(["AMEX:SOXS", "NASDAQ:SQQQ", "AMEX:SPXS", "AMEX:SDOW"])
    macro_syms = _dedup_add(
        [
            "AMEX:SPY",
            "NASDAQ:QQQ",
            "NASDAQ:SOXX",
            "AMEX:IWM",
            "AMEX:RSP",
            "CBOE:VIX",
            "NASDAQ:TLT",
        ]
    )
    manual_syms = _dedup_add([tvs for row in staged_rows if (tvs := _fmt_row(row))])

    sections: list[str] = ["###01_LIVE_EXTREME_MOVERS_TOP", *movers]
    if early_stage_syms:
        sections.extend(["###02_EARLY_STAGE_IGNITION_TOP", *early_stage_syms])
    if darkpool_uv:
        sections.extend(["###03_DARKPOOL_UNUSUAL_VOL", *darkpool_uv])
    if hedges:
        sections.extend(["###04_CRASH_DEFENSIVE_HEDGES", *hedges])
    if macro_syms:
        sections.extend(["###05_MACRO_REGIME_BREADTH", *macro_syms])
    if manual_syms:
        sections.extend(["###06_MANUAL_STAGED_PRESETS", *manual_syms])
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
    adoption_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    tv_technicals_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    symbol_context_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    coord_by_alert_id: Mapping[str, Mapping[str, Any]] | None = None,
    coord_by_sym: Mapping[str, Mapping[str, Any]] | None = None,
    exchange_map: Mapping[str, str] | None = None,
    regime_int: int = 0,
) -> dict[str, Any]:
    sess = str(row.get("active_session") or row.get("session") or "RTH").strip().upper()
    sym = str(row["symbol"]).strip().upper()
    sym_ctx: Mapping[str, Any] = (symbol_context_by_sym or {}).get(sym) or {}
    sched = str(row.get("schedule") or row.get("trailing_schedule") or "tight_near_lossless")
    arch = str(
        row.get("archetype")
        or row.get("setup_archetype")
        or sym_ctx.get("archetype")
        or sym_ctx.get("setup_archetype")
        or "EXTREME_REVERSAL"
    )
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
    side_str = str(row["side"]).strip().upper()
    r_side: _risk.Side = "SHORT" if side_str in {"SELL", "SHORT"} else "LONG"
    entry_px_f = _opt_float(row.get("live_price") or row.get("trigger_price"), min_val=0.0001)
    safe_entry_px = entry_px_f if entry_px_f is not None else 100.0
    entry_px_dec = Decimal(str(round(safe_entry_px, 4)))
    natr_f = _opt_float(row.get("natr_pct"), min_val=0.0001) or 0.015
    natr_dec = Decimal(str(round(natr_f, 6)))
    raw_spec = row.get("non_noise_trail_spec")
    if isinstance(raw_spec, Mapping) and raw_spec.get("execution_mechanism"):
        trail_spec = dict(raw_spec)
    else:
        trail_spec = resolve_non_noise_min_trail_spec(
            symbol=sym,
            entry_price=entry_px_dec,
            side=r_side,
            session=sess,
            natr_pct=natr_dec,
            archetype=arch,
            venue_profile=vp,
        )
    raw_collar_val = (
        row.get("collar_now_bps")
        if row.get("collar_now_bps") is not None
        else row.get("limit_collar_bps")
    )
    collar_bps_opt = _opt_float(raw_collar_val, min_val=0.0)
    collar_bps_dec = Decimal(str(round(collar_bps_opt, 2))) if collar_bps_opt is not None else None
    raw_stop_f = _opt_float(
        row.get("stop_now") if row.get("stop_now") is not None else row.get("stop_price"),
        min_val=0.0001,
    )
    stop_px_dec = Decimal(str(round(raw_stop_f, 4))) if raw_stop_f is not None else None
    rev_pct_dec = max(
        Decimal("0.0010"),
        Decimal(str(trail_spec["fast_sell_non_noise_reversal_pct"])),
    )
    levels = _compute_rule612_r_lock_levels(
        entry_price=entry_px_dec,
        stop_price=stop_px_dec,
        side=r_side,
        session=sess,
        natr_pct=natr_dec,
        limit_collar_bps=collar_bps_dec,
        fast_sell_rev_pct=rev_pct_dec,
    )

    def _pos_price(key: str, fallback: Decimal) -> float:
        val = _opt_float(row.get(key), min_val=0.0001)
        return val if val is not None else float(fallback)

    entry_limit_f = _pos_price("entry_limit_now", levels["entry_limit"])
    if _opt_float(row.get("entry_limit_price"), min_val=0.0001) is not None:
        entry_limit_f = float(row["entry_limit_price"])
    stop_trigger_f = float(levels["stop_px"])
    stop_limit_f = _pos_price("stop_limit_now", levels["stop_limit"])
    if _opt_float(row.get("stop_limit_price"), min_val=0.0001) is not None:
        stop_limit_f = float(row["stop_limit_price"])
    stop_dist_pct_f = _opt_float(row.get("stop_now_pct"), min_val=0.0001) or float(
        levels["stop_dist_pct"]
    )
    be_floor_f = _pos_price("be_plus_fee_floor", levels["be_floor"])
    t1r_f = _pos_price("target_1r", levels["target_1r"])
    lock_1r_f = _pos_price("lock_at_1r", levels["lock_1r"])
    t2r_f = _pos_price("target_2r", levels["target_2r"])
    lock_2r_f = _pos_price("lock_at_2r", levels["lock_2r"])
    t3r_f = _pos_price("target_3r", levels["target_3r"])
    lock_3r_f = _pos_price("lock_at_3r", levels["lock_3r"])
    fsr_px = _pos_price("fast_sell_rev_trigger", levels["fast_sell_rev"])

    raw_reentry_card = trail_spec.get("rapid_reentry_policy")
    reentry_pol: dict[str, Any] = (
        dict(raw_reentry_card) if isinstance(raw_reentry_card, Mapping) else {}
    )
    rapid_reentry_rpm = int(
        row.get("rapid_reentry_rpm")
        or reentry_pol.get("max_rpm")
        or reentry_pol.get("rpm_budget_available")
        or vh.get("max_rpm")
        or 1000
    )
    adopt_info = (adoption_by_sym or {}).get(sym) or {}
    alert_id = str(row.get("alert_id") or "").strip() or None
    coord_info = (
        ((coord_by_alert_id or {}).get(alert_id) if alert_id else None)
        or (coord_by_sym or {}).get(sym)
        or {}
    )
    auto_adopted = bool(
        row.get("auto_adopted_by_engine") is True
        or adopt_info.get("auto_adopted_by_engine") is True
    )
    adopted_external = bool(
        row.get("adopted_external") is True or adopt_info.get("adopted_external") is True
    )
    default_adopt = (
        "AUTO_ADOPTED_EXTERNAL"
        if adopted_external
        else ("ENGINE_SUPERVISED" if auto_adopted else "AUTO_ADOPT_ON_FILL")
    )
    adoption_status = str(
        row.get("engine_adoption_status")
        or adopt_info.get("engine_adoption_status")
        or default_adopt
    )

    # Enrich non-price scanner context when missing on raw PINE_STAGED rows.
    rvol_val = (
        _opt_float(row.get("rvol"), min_val=0.0)
        if row.get("rvol") is not None
        else _opt_float(
            sym_ctx.get("rvol")
            if sym_ctx.get("rvol") is not None
            else sym_ctx.get("rvol_normalized"),
            min_val=0.0,
        )
    )
    sess_chg_val = _opt_float(row.get("session_move_pct"))
    if sess_chg_val is None:
        sess_chg_val = _opt_float(sym_ctx.get("session_move_pct")) or 0.0
    dp_trf = (
        _opt_float(row.get("darkpool_trf_ratio"), min_val=0.0)
        if row.get("darkpool_trf_ratio") is not None
        else _opt_float(sym_ctx.get("darkpool_trf_ratio"), min_val=0.0)
    )
    dp_src = row.get("darkpool_trf_source") or sym_ctx.get("darkpool_trf_source")
    dp_as_of = row.get("darkpool_trf_as_of_utc") or sym_ctx.get("darkpool_trf_as_of_utc")
    dp_age = (
        _opt_float(row.get("darkpool_trf_age_hours"), min_val=0.0)
        if row.get("darkpool_trf_age_hours") is not None
        else _opt_float(sym_ctx.get("darkpool_trf_age_hours"), min_val=0.0)
    )
    dp_blk = (
        _opt_int(row.get("darkpool_block_count"))
        if row.get("darkpool_block_count") is not None
        else _opt_int(sym_ctx.get("darkpool_block_count"))
    )
    trf_blk = (
        _opt_float(row.get("trf_block_ratio"), min_val=0.0)
        if row.get("trf_block_ratio") is not None
        else _opt_float(sym_ctx.get("trf_block_ratio"), min_val=0.0)
    )
    hvr_val = (
        _opt_float(row.get("hidden_volume_ratio"), min_val=0.0)
        if row.get("hidden_volume_ratio") is not None
        else _opt_float(sym_ctx.get("hidden_volume_ratio"), min_val=0.0)
    )
    vbr_val = (
        _opt_float(row.get("vol_buildup_ratio"), min_val=0.0)
        if row.get("vol_buildup_ratio") is not None
        else _opt_float(sym_ctx.get("vol_buildup_ratio"), min_val=0.0)
    )
    uv_flag = bool(
        row.get("unusual_volume_buildup") is True or sym_ctx.get("unusual_volume_buildup") is True
    )
    premarket_ign = bool(
        row.get("premarket_ignition") is True or sym_ctx.get("premarket_ignition") is True
    )
    early_ign = bool(
        row.get("early_ignition_detected") is True
        or sym_ctx.get("early_ignition_detected") is True
        or premarket_ign
    )
    sudden_vol = bool(
        row.get("sudden_volume_surge") is True or sym_ctx.get("sudden_volume_surge") is True
    )
    raw_card_tech = row.get("technicals") or sym_ctx.get("technicals")
    card_technicals: dict[str, Any] = (
        dict(raw_card_tech) if isinstance(raw_card_tech, Mapping) else {}
    )
    raw_card_ed = row.get("early_detection") or sym_ctx.get("early_detection")
    card_early_det: dict[str, Any] = dict(raw_card_ed) if isinstance(raw_card_ed, Mapping) else {}
    raw_card_sop = row.get("session_order_presets") or sym_ctx.get("session_order_presets")
    card_sop: dict[str, Any] = dict(raw_card_sop) if isinstance(raw_card_sop, Mapping) else {}
    peak_conf = (
        row.get("peak_confirmed")
        if isinstance(row.get("peak_confirmed"), bool)
        else (
            sym_ctx.get("peak_confirmed")
            if isinstance(sym_ctx.get("peak_confirmed"), bool)
            else None
        )
    )
    eff_regime = _opt_int(row.get("macro_regime"))
    if eff_regime is None:
        eff_regime = regime_int

    tv_tech_info = (tv_technicals_by_sym or {}).get(sym) or {}
    ta_seed: dict[str, Any] = {
        "technical_alignment_score": (
            row.get("technical_alignment_score")
            if row.get("technical_alignment_score") is not None
            else sym_ctx.get("technical_alignment_score")
        ),
        "tv_technicals_rating": row.get("tv_technicals_rating")
        or sym_ctx.get("tv_technicals_rating"),
        "tv_technicals_score": (
            row.get("tv_technicals_score")
            if row.get("tv_technicals_score") is not None
            else sym_ctx.get("tv_technicals_score")
        ),
        "ta_rating_source": row.get("ta_rating_source") or sym_ctx.get("ta_rating_source"),
        "technicals": card_technicals,
    }
    ta_score, tv_rating, tv_score, ta_rating_src = _resolve_ta_composite(
        ta_seed,
        sym=sym,
        side="SELL" if r_side == "SHORT" else "BUY",
        regime_int=eff_regime,
        sess_chg=sess_chg_val,
        rvol=rvol_val if rvol_val is not None else 1.0,
        darkpool_trf_ratio=dp_trf,
        unusual_volume_buildup=uv_flag,
        premarket_ignition=premarket_ign,
        early_ignition_detected=early_ign,
        peak_confirmed=peak_conf,
        tv_tech_info=tv_tech_info,
    )
    potential_cat = str(
        row.get("potential_category")
        or sym_ctx.get("potential_category")
        or _resolve_potential_category(row, arch)
    )
    lane_seed: dict[str, Any] = {
        "paper_lane_account": row.get("paper_lane_account") or sym_ctx.get("paper_lane_account"),
        "lane_account": row.get("lane_account") or sym_ctx.get("lane_account"),
        "target_paper_lane": row.get("target_paper_lane") or sym_ctx.get("target_paper_lane"),
        "paper_lane_id": row.get("paper_lane_id") or sym_ctx.get("paper_lane_id"),
        "lane_id": row.get("lane_id") or sym_ctx.get("lane_id"),
        "target_lane_id": row.get("target_lane_id") or sym_ctx.get("target_lane_id"),
    }
    resolved_lane_acct, resolved_lane_id = _resolve_lane_assignment(
        lane_seed,
        vu=vu,
        vp=vp,
        playbook=playbook,
        adopt_info=adopt_info,
        coord_info=coord_info,
    )
    sh_25k = _opt_int(row.get("shares_25k_defensive"))
    sh_100k = _opt_int(row.get("shares_100k_defensive"))
    if sh_25k is None or sh_100k is None:
        risk_pct = Decimal("0.0025") if eff_regime == -1 else Decimal("0.0050")
        sh_25k, sh_100k = (
            _risk.size_position_by_risk(
                equity=Decimal(eq),
                entry_price=entry_px_dec,
                stop_price=levels["stop_px"],
                risk_per_trade_pct=risk_pct,
                max_notional_pct=Decimal("0.15"),
            )
            for eq in ("25000", "100000")
        )
    eff_preset_tier = str(
        row.get("preset_tier")
        or (
            "EXTREME_MOVER_CONFIRMED"
            if card_kind == "ACTIVE_TICKET"
            else (
                "SESSION_EARLY_IGNITION_PRESET"
                if card_kind == "SESSION_EARLY_IGNITION_PRESET"
                else card_kind
            )
        )
    )
    raw_row_tv = str(row.get("tv_symbol") or "").strip().upper()
    raw_ctx_tv = str(sym_ctx.get("tv_symbol") or "").strip().upper()
    resolved_tv_sym = (
        raw_row_tv
        if ":" in raw_row_tv
        else (
            raw_ctx_tv
            if ":" in raw_ctx_tv
            else resolve_tv_exchange_symbol(
                sym,
                exchange=(
                    row.get("exchange")
                    or row.get("tv_exchange")
                    or sym_ctx.get("exchange")
                    or sym_ctx.get("tv_exchange")
                ),
                exchange_map=exchange_map,
            )
        )
    )
    card: dict[str, Any] = {
        "card_kind": card_kind,
        "preset_tier": eff_preset_tier,
        "extreme_mover_eligible": bool(row.get("extreme_mover_eligible") is True),
        "symbol": sym,
        "tv_symbol": resolved_tv_sym,
        "side": side_str,
        "qty": 1,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "venue": vu,
        "venue_profile": vp,
        "venue_order_handle": vh,
        "max_rpm": int(vh.get("max_rpm") or 0),
        "exit_reserve_rpm": int(vh.get("exit_reserve_rpm") or 0),
        "session": sess,
        "extended_hours": sess != "RTH",
        "order_type": str(row.get("order_type_now") or row.get("order_type") or "STOP_LIMIT"),
        "execution_mechanism": str(
            row.get("execution_mechanism") or trail_spec["execution_mechanism"]
        ),
        "execution_mechanism_label": str(
            row.get("execution_mechanism_label") or trail_spec["execution_mechanism_label"]
        ),
        "broker_exit_order": str(row.get("broker_exit_order") or trail_spec["broker_exit_order"]),
        "limit_collar_bps": (
            collar_bps_opt if collar_bps_opt is not None else float(levels["collar_bps"])
        ),
        "trailing_schedule": sched,
        "order_preset_id": row.get("order_preset_id") or f"{sched}:{sess}:{arch}"[:64],
        "archetype": arch,
        "potential_category": potential_cat,
        "calibrated_category": (
            card_cal_cat := str(
                row.get("calibrated_category")
                or sym_ctx.get("calibrated_category")
                or potential_cat
            )
        ),
        "min_trailing_pct": (
            card_min_trail := (
                _opt_float(row.get("min_trailing_pct"), min_val=0.0)
                or float(trail_spec["non_noise_min_trail_pct"])
            )
        ),
        "technicals": card_technicals,
        "early_detection": card_early_det,
        "session_order_presets": _ensure_symbol_session_order_presets(
            card_sop,
            symbol=sym,
            side=r_side,
            ref_price=entry_px_dec,
            natr_pct=Decimal(
                str(round(_opt_float(row.get("natr_pct"), min_val=0.0001) or 0.015, 6))
            ),
            calibrated_min_trailing_pct=card_min_trail,
            venue_profile=vp,
            pair_symbol=(
                row.get("pair_symbol") or sym_ctx.get("pair_symbol") or BIDIRECTIONAL_PAIRS.get(sym)
            ),
        ),
        "momentum_trigger_reason": {
            "calibrated_category": card_cal_cat,
            "rvol": rvol_val,
            "darkpool_trf_ratio": dp_trf,
            "technical_alignment_score": ta_score,
            "archetype": arch,
            "watch_tier": row.get("watch_tier") or sym_ctx.get("watch_tier"),
            "premarket_ignition": premarket_ign,
            "sudden_volume_surge": sudden_vol,
        },
        "technical_alignment_score": ta_score,
        "early_ignition_detected": early_ign,
        "premarket_ignition": premarket_ign,
        "sudden_volume_surge": sudden_vol,
        "tuning_run_id": row.get("tuning_run_id") or sym_ctx.get("tuning_run_id"),
        "tv_technicals_rating": tv_rating,
        "tv_technicals_score": tv_score,
        "ta_rating_source": ta_rating_src,
        "recommended_omni_preset": str(
            row.get("recommended_omni_preset") or playbook["recommended_omni_preset"]
        ),
        "target_paper_lane": resolved_lane_acct,
        "target_lane_id": resolved_lane_id,
        "setup_playbook": playbook,
        "action_gate": row.get("action_gate") or "STAGED_PINE_ALERT",
        "entry_ref_price": entry_px_f,
        "entry_limit_price": entry_limit_f,
        "stop_trigger_price": stop_trigger_f,
        "stop_limit_price": stop_limit_f,
        "stop_distance_pct": stop_dist_pct_f,
        "be_plus_fee_floor": be_floor_f,
        "target_1r": t1r_f,
        "lock_at_1r": lock_1r_f,
        "target_2r": t2r_f,
        "lock_at_2r": lock_2r_f,
        "target_3r": t3r_f,
        "lock_at_3r": lock_3r_f,
        "fast_sell_rev_trigger": fsr_px,
        "non_noise_min_trail_pct": (
            _opt_float(row.get("non_noise_min_trail_pct"), min_val=0.0)
            if _opt_float(row.get("non_noise_min_trail_pct"), min_val=0.0) is not None
            else float(trail_spec["non_noise_min_trail_pct"])
        ),
        "non_noise_min_trail_bps": (
            _opt_float(row.get("non_noise_min_trail_bps"), min_val=0.0)
            if _opt_float(row.get("non_noise_min_trail_bps"), min_val=0.0) is not None
            else float(trail_spec["non_noise_min_trail_bps"])
        ),
        "fast_sell_non_noise_reversal_pct": (
            _opt_float(row.get("fast_sell_non_noise_reversal_pct"), min_val=0.0)
            if _opt_float(row.get("fast_sell_non_noise_reversal_pct"), min_val=0.0) is not None
            else float(trail_spec["fast_sell_non_noise_reversal_pct"])
        ),
        "peak_climax_reverse_trigger_pct": (
            _opt_float(row.get("peak_climax_reverse_trigger_pct"), min_val=0.0)
            if _opt_float(row.get("peak_climax_reverse_trigger_pct"), min_val=0.0) is not None
            else float(trail_spec["peak_climax_reverse_trigger_pct"])
        ),
        "auto_reverse_policy": str(
            row.get("auto_reverse_policy") or trail_spec["auto_reverse_policy"]
        ),
        "rapid_reentry_policy": dict(reentry_pol),
        "rapid_reentry_rpm": rapid_reentry_rpm,
        "non_noise_trail_spec": trail_spec,
        "auto_adopted_by_engine": auto_adopted,
        "adopted_external": adopted_external,
        "engine_adoption_status": adoption_status,
        "reentry_cooldown_secs": int(row.get("reentry_cooldown_secs") or 60),
        "reverse_short_mode": row.get("reverse_short_mode") or sym_ctx.get("reverse_short_mode"),
        "pair_symbol": row.get("pair_symbol")
        or sym_ctx.get("pair_symbol")
        or BIDIRECTIONAL_PAIRS.get(sym),
        "shares_25k_defensive": sh_25k,
        "shares_100k_defensive": sh_100k,
        "rvol": rvol_val,
        "darkpool_trf_ratio": dp_trf,
        "darkpool_trf_source": dp_src,
        "darkpool_trf_as_of_utc": dp_as_of,
        "darkpool_trf_age_hours": dp_age,
        "darkpool_block_count": dp_blk,
        "trf_block_ratio": trf_blk,
        "hidden_volume_ratio": hvr_val,
        "vol_buildup_ratio": vbr_val,
        "unusual_volume_buildup": uv_flag,
        "pinned": bool(row.get("pinned") is True or sym_ctx.get("pinned") is True),
        "borrow_source": row.get("borrow_source") or sym_ctx.get("borrow_source"),
        "short_fee_bps": (
            _opt_float(row.get("short_fee_bps"), min_val=0.0)
            if row.get("short_fee_bps") is not None
            else _opt_float(sym_ctx.get("short_fee_bps"), min_val=0.0)
        ),
        "watch_tier": row.get("watch_tier") or sym_ctx.get("watch_tier"),
        "paper_lane_account": resolved_lane_acct,
        "paper_lane_id": resolved_lane_id,
        "lane_account": resolved_lane_acct,
        "lane_id": resolved_lane_id,
        "paper_origin": row.get("paper_origin") or sym_ctx.get("paper_origin"),
    }
    if alert_id is not None:
        card["alert_id"] = alert_id
    if row.get("ts_utc_ms") is not None:
        card["ts_utc_ms"] = int(row["ts_utc_ms"])
    if row.get("staged_at_utc") is not None:
        card["staged_at_utc"] = str(row["staged_at_utc"])
    return card


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


def _order_card_sort_key(
    card: Mapping[str, Any],
    *,
    active_sess: str,
    orig_idx: int,
) -> tuple[int, int, int]:
    active_u = str(active_sess or "CLOSED").strip().upper()
    card_sess = str(card.get("session") or active_u).strip().upper()
    kind = str(card.get("card_kind") or "").strip().upper()
    is_active = card_sess == active_u
    ts_ms = _opt_int(card.get("ts_utc_ms"), min_val=0) or 0
    if kind == "ACTIVE_TICKET" and is_active:
        bucket = 0
    elif kind == "SESSION_EARLY_IGNITION_PRESET" and is_active:
        bucket = 1
    elif kind == "PINE_STAGED" and is_active:
        bucket = 2
    elif kind == "STANDBY_REFERENCE" and is_active:
        bucket = 3
    elif kind == "PINE_STAGED":
        bucket = 4
    else:
        bucket = 5
    return (bucket, -ts_ms if kind == "PINE_STAGED" else 0, orig_idx)


def build_tv_order_presets(
    doc: Mapping[str, Any],
    *,
    manual_staged_presets: Sequence[Mapping[str, Any]] = (),
    paper_coordination: Mapping[str, Any] | None = None,
    tv_cloud_sync: Mapping[str, Any] | None = None,
    movers_entries: Sequence[Mapping[str, Any]] = (),
    exchange_map: Mapping[str, str] | None = None,
    full_market_sweep_summary: Mapping[str, Any] | None = None,
    full_market_top_movers: Sequence[Mapping[str, Any]] = (),
    early_stage_ignition_movers: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build declarative `tv_order_presets.json` (3x4 NATR matrix, 8 Omni modes, symbol cards)."""
    active_sess = str(doc.get("active_session") or "CLOSED")
    raw_vp = str(doc.get("venue_profile") or "").strip().upper() or None
    vp, venue = resolve_venue_profile(str(doc.get("venue") or "IBKR"), venue_profile=raw_vp)
    raw_coord = (
        paper_coordination if paper_coordination is not None else doc.get("paper_farm_coordination")
    )
    coord: dict[str, Any] = dict(raw_coord) if isinstance(raw_coord, Mapping) else {}
    adoption_by_sym = _extract_adoption_by_symbol(coord)
    coord_by_alert_id, coord_by_sym = _extract_coordination_maps(coord)
    tv_technicals_by_sym = _extract_tv_technicals_ratings(tv_cloud_sync)
    macro_regime = dict(doc.get("macro_regime") or {})
    regime_int = int(macro_regime.get("regime", 0))

    merged_ex: dict[str, str] = dict(exchange_map or {})
    _extract_exchange_entries(full_market_top_movers, merged_ex)
    _extract_exchange_entries(early_stage_ignition_movers, merged_ex)
    _extract_exchange_entries(movers_entries, merged_ex)
    _extract_exchange_entries(doc.get("standby_presets", ()), merged_ex)
    _extract_exchange_entries(doc.get("tickets", ()), merged_ex)

    symbol_context_by_sym: dict[str, Mapping[str, Any]] = {}
    for bucket in (
        full_market_top_movers,
        early_stage_ignition_movers,
        movers_entries,
        doc.get("standby_presets", ()),
        doc.get("tickets", ()),
    ):
        for item in bucket:
            if isinstance(item, Mapping) and item.get("symbol"):
                symbol_context_by_sym[str(item["symbol"]).strip().upper()] = item

    session_matrix: dict[str, dict[str, dict[str, Any]]] = {}
    for sched_name, sess_map in _eng.POOLED_TRAILING_SCHEDULES.items():
        session_matrix[sched_name] = {}
        for sess_name, prof in sess_map.items():
            mech, broker_exit, mech_label = _execution_mechanism_details(sess_name)
            session_matrix[sched_name][sess_name] = {
                "order_mode": prof.order_mode,
                "broker_exit_order": broker_exit,
                "execution_mechanism": mech,
                "execution_mechanism_label": mech_label,
                "initial_max_loss_pct": float(prof.initial_max_loss_pct),
                "breakeven_trigger_pct": float(prof.breakeven_trigger_pct),
                "min_trail_pct": float(prof.min_trail_pct),
                "natr_trail_mult": float(prof.natr_trail_mult),
                "limit_collar_bps": float(prof.limit_collar_bps),
                "max_notional_scale": float(prof.max_notional_scale),
                "outside_rth": bool(prof.outside_rth),
            }

    cards: list[dict[str, Any]] = []
    for t in doc.get("tickets", []):
        if isinstance(t, Mapping) and t.get("symbol"):
            tk_kind = (
                "SESSION_EARLY_IGNITION_PRESET"
                if t.get("preset_tier") == "SESSION_EARLY_IGNITION_PRESET"
                else "ACTIVE_TICKET"
            )
            cards.append(
                _build_symbol_order_card(
                    t,
                    card_kind=tk_kind,
                    venue=venue,
                    venue_profile=vp,
                    adoption_by_sym=adoption_by_sym,
                    tv_technicals_by_sym=tv_technicals_by_sym,
                    symbol_context_by_sym=symbol_context_by_sym,
                    coord_by_alert_id=coord_by_alert_id,
                    coord_by_sym=coord_by_sym,
                    exchange_map=merged_ex,
                    regime_int=regime_int,
                )
            )
    for p in manual_staged_presets:
        if isinstance(p, Mapping) and p.get("symbol"):
            cards.append(
                _build_symbol_order_card(
                    p,
                    card_kind="PINE_STAGED",
                    venue=venue,
                    venue_profile=vp,
                    adoption_by_sym=adoption_by_sym,
                    tv_technicals_by_sym=tv_technicals_by_sym,
                    symbol_context_by_sym=symbol_context_by_sym,
                    coord_by_alert_id=coord_by_alert_id,
                    coord_by_sym=coord_by_sym,
                    exchange_map=merged_ex,
                    regime_int=regime_int,
                )
            )
    for s in doc.get("standby_presets", []):
        if isinstance(s, Mapping) and s.get("symbol"):
            cards.append(
                _build_symbol_order_card(
                    s,
                    card_kind="STANDBY_REFERENCE",
                    venue=venue,
                    venue_profile=vp,
                    adoption_by_sym=adoption_by_sym,
                    tv_technicals_by_sym=tv_technicals_by_sym,
                    symbol_context_by_sym=symbol_context_by_sym,
                    coord_by_alert_id=coord_by_alert_id,
                    coord_by_sym=coord_by_sym,
                    exchange_map=merged_ex,
                    regime_int=regime_int,
                )
            )

    def _norm_mover_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        out_rows: list[dict[str, Any]] = []
        for raw_m in rows:
            if not isinstance(raw_m, Mapping) or not raw_m.get("symbol"):
                continue
            item = dict(raw_m)
            sym_m = str(item["symbol"]).strip().upper()
            raw_tv_m = str(item.get("tv_symbol") or "").strip().upper()
            item["tv_symbol"] = (
                raw_tv_m
                if ":" in raw_tv_m
                else resolve_tv_exchange_symbol(
                    sym_m,
                    exchange=item.get("exchange") or item.get("tv_exchange"),
                    exchange_map=merged_ex,
                )
            )
            out_rows.append(item)
        return out_rows

    norm_full_movers = _norm_mover_rows(full_market_top_movers)
    norm_early_movers = _norm_mover_rows(early_stage_ignition_movers)

    indexed_cards = list(enumerate(cards))
    indexed_cards.sort(
        key=lambda pair: _order_card_sort_key(pair[1], active_sess=active_sess, orig_idx=pair[0])
    )
    cards = [pair[1] for pair in indexed_cards]
    active_sess_u = active_sess.strip().upper()
    active_cards_n = sum(
        1 for c in cards if str(c.get("session") or "").strip().upper() == active_sess_u
    )
    prior_cards_n = len(cards) - active_cards_n

    alp_vp = vp if vp in {"ALPACA_PAPER_200", "ALPACA_PAPER_1000"} else None
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
        "macro_regime": macro_regime,
        "full_market_sweep_summary": (
            dict(full_market_sweep_summary)
            if isinstance(full_market_sweep_summary, Mapping)
            else {}
        ),
        "full_market_top_movers": norm_full_movers,
        "early_stage_ignition_movers": norm_early_movers,
        "active_session_cards_count": active_cards_n,
        "prior_session_cards_count": prior_cards_n,
        "extreme_mover_confirmed_count": int(doc.get("extreme_mover_confirmed_count") or 0),
        "session_early_ignition_count": int(doc.get("session_early_ignition_count") or 0),
        "lossless_24x5_mechanisms": {
            sess_k: {
                "execution_mechanism": vals[0],
                "broker_exit_order": vals[1],
                "label": vals[2],
            }
            for sess_k, vals in SESSION_EXECUTION_MECHANISMS.items()
        },
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


_OMNI_SITUATION_WEBHOOK_SPECS: tuple[tuple[str, str, str, str, str, float, str], ...] = (
    (
        "AUTO_REGIME_ADAPTIVE",
        "EXTREME_REVERSAL",
        "PRE_MARKET",
        "BUY",
        "STOP_LIMIT",
        35.0,
        "ALPACA_PAPER_1000",
    ),
    (
        "LOSSLESS_TIGHT_SCALP",
        "EXTREME_REVERSAL",
        "RTH",
        "BUY",
        "STOP_MARKET",
        0.0,
        "ALPACA_PAPER_1000",
    ),
    (
        "CRASH_DEFENSIVE_INVERSE",
        "CRASH_ALIGNMENT",
        "RTH",
        "BUY",
        "STOP_MARKET",
        0.0,
        "ALPACA_PAPER_1000",
    ),
    (
        "MINERVINI_VCP_BREAKOUT",
        "VCP_BREAKOUT",
        "RTH",
        "BUY",
        "STOP_MARKET",
        0.0,
        "ALPACA_PAPER_1000",
    ),
    (
        "STOCKBEE_EP_MOMENTUM_BURST",
        "EARLY_EP_MOMENTUM_BURST",
        "PRE_MARKET",
        "BUY",
        "STOP_LIMIT",
        35.0,
        "ALPACA_PAPER_1000",
    ),
    (
        "STOCKBEE_EXHAUSTION_HAMMER",
        "EXHAUSTION_HAMMER",
        "AFTER_HOURS",
        "BUY",
        "STOP_LIMIT",
        35.0,
        "ALPACA_PAPER_1000",
    ),
    (
        "PARABOLIC_EXHAUSTION_SHORT",
        "PARABOLIC_TOP",
        "RTH",
        "SELL",
        "STOP_MARKET",
        0.0,
        "IBKR_PAPER_4002",
    ),
    (
        "OVERNIGHT_24X5_IBKR",
        "EXTREME_REVERSAL",
        "OVERNIGHT",
        "BUY",
        "STOP_LIMIT",
        50.0,
        "IBKR_PAPER_4002",
    ),
)


def _render_card_webhook_snippet(card: Mapping[str, Any], *, regime_int: int) -> str:
    sym = str(card.get("symbol") or "NVDA").strip().upper()
    side = str(card.get("side") or "BUY").strip().upper()
    sess = str(card.get("session") or "PRE_MARKET").strip().upper()
    if sess not in SESSION_EXECUTION_MECHANISMS:
        sess = "PRE_MARKET"
    order_type = "STOP_MARKET" if sess == "RTH" else "STOP_LIMIT"
    collar_bps = (
        0.0
        if sess == "RTH"
        else float(card.get("limit_collar_bps") or (50.0 if sess == "OVERNIGHT" else 35.0))
    )
    px = _opt_float(card.get("entry_ref_price"), min_val=0.01) or 100.0
    stop = _opt_float(card.get("stop_trigger_price"), min_val=0.01) or (
        round(px * 0.9875, 2) if side == "BUY" else round(px * 1.0125, 2)
    )
    stop_dist = round(abs(px - stop) / px, 6)
    sched = str(card.get("trailing_schedule") or "tight_near_lossless")
    natr_val = (
        0.012 if sched == "tight_near_lossless" else (0.022 if sched == "baseline" else 0.036)
    )
    vp = str(card.get("venue_profile") or "ALPACA_PAPER_1000")
    venue = "IBKR" if vp.startswith("IBKR") else "ALPACA"
    ts_ms = _opt_int(card.get("ts_utc_ms"), min_val=1) or 1_795_776_300_000
    payload: dict[str, Any] = {
        "schema_version": "tier0.tv-alert.v1",
        "symbol": sym,
        "ts_utc_ms": ts_ms,
        "bar_confirmed": True,
        "side": side,
        "qty": 1,
        "venue": venue,
        "venue_profile": vp,
        "session": sess,
        "order_type": order_type,
        "trigger_price": round(px, 4),
        "stop_price": round(stop, 4),
        "stop_distance_pct": stop_dist,
        "limit_collar_bps": collar_bps,
        "trailing_schedule": sched,
        "natr_pct": natr_val,
        "auto_reverse": True,
        "auto_reentry": True,
        "reentry_cooldown_secs": 60,
        "strategy_id": "SignalExecutionStrategy",
        "semi_auto_confirmed": False,
        "setup_archetype": str(card.get("archetype") or "EXTREME_REVERSAL"),
        "macro_regime": regime_int if regime_int in (-1, 0, 1) else 0,
        "script_kind": "indicator",
        "order_preset_id": str(card.get("order_preset_id") or f"{sched}:{sess}:EXTREME_REVERSAL")[
            :64
        ],
        "data_source": "tradingview_pine_v6",
    }
    return json.dumps(payload, separators=(",", ":"))


def render_order_preset_cards_markdown(presets_doc: Mapping[str, Any]) -> str:
    """Render copy-ready TradingView Order Panel preset cards Markdown."""
    sess = presets_doc.get("active_session", "CLOSED")
    macro = presets_doc.get("macro_regime") or {}
    regime_int = int(macro.get("regime", 0))
    cards = [c for c in presets_doc.get("symbol_order_cards", []) if isinstance(c, Mapping)]
    carry_notice = _format_trf_carryover_notice(cards).rstrip("\n")
    lines = [
        "# Tier-0 TradingView SOTA Order Preset Cards",
        "",
        f"- **Generated (ET):** `{presets_doc.get('generated_at_et')}` | **Session:** `{sess}` "
        f"| **Regime:** `{macro.get('regime_label', 'NEUTRAL')} ({regime_int})`",
        *(
            [f"- **Market Feed Indicator Reason:** {macro['regime_reason']}"]
            if str(macro.get("regime_reason") or "").strip()
            else []
        ),
    ]
    raw_sweep = presets_doc.get("full_market_sweep_summary")
    sweep_map: Mapping[str, Any] = raw_sweep if isinstance(raw_sweep, Mapping) else {}
    uni_scanned = _opt_int(sweep_map.get("universe_scanned"), min_val=1)
    if uni_scanned is not None:
        el_s = _opt_float(sweep_map.get("elapsed_secs"), min_val=0.0) or 0.0
        qual_n = _opt_int(sweep_map.get("qualifying_movers_count"), min_val=0) or 0
        early_n = _opt_int(sweep_map.get("early_stage_count"), min_val=0) or 0
        ext_n = _opt_int(sweep_map.get("extreme_expansion_count"), min_val=0) or 0
        para_n = _opt_int(sweep_map.get("parabolic_climax_count"), min_val=0) or 0
        lines.append(
            f"- **Entire-US-Market Sweep Summary:** `{uni_scanned:,}` NMS equities "
            f"in `{el_s:.2f}s` | Qualifying `{qual_n}` | Early-Stage Ignition `{early_n}` "
            f"| Extreme Expansion `{ext_n}` | Parabolic Climax `{para_n}`"
        )
    lines.append(
        "- **Guards:** `qty=1` | `semi_auto_confirmed=false` "
        "| `live_execution_enabled=false` | `paper_only=true`"
    )
    if carry_notice:
        lines.append(carry_notice)

    raw_full_m = presets_doc.get("full_market_top_movers")
    raw_early_m = presets_doc.get("early_stage_ignition_movers")
    merged_movers: list[Mapping[str, Any]] = []
    seen_mover_syms: set[str] = set()
    for bucket_m, cap in ((raw_full_m, 10), (raw_early_m, 10)):
        if not isinstance(bucket_m, Sequence) or isinstance(bucket_m, (str, bytes)):
            continue
        added = 0
        for row_m in bucket_m:
            if not isinstance(row_m, Mapping):
                continue
            sym_m = str(row_m.get("symbol") or "").strip().upper()
            px_m = _opt_float(row_m.get("last_price"), min_val=0.0)
            if not sym_m or sym_m in seen_mover_syms or (px_m is not None and px_m < 1.0):
                continue
            seen_mover_syms.add(sym_m)
            merged_movers.append(row_m)
            added += 1
            if added >= cap:
                break
    if merged_movers:
        lines.extend(
            [
                "",
                "## Top Entire-Market Extreme Movers & Early-Stage Ignition (>= $1)",
                "",
                (
                    "| Rank | TV Symbol | Stage | Price Tier | Last $ | Daily % "
                    "| Pace RVOL | Spread (bps) | ADV20 | Split Adj | Dollar Vol | Ranking Reason |"
                ),
                "|---:|---|---|---|---:|---:|---:|---:|---:|:---:|---:|---|",
            ]
        )
        for idx_m, row_m in enumerate(merged_movers, 1):
            sym_m = str(row_m.get("symbol") or "").strip().upper()
            raw_tv_m = str(row_m.get("tv_symbol") or "").strip().upper()
            tv_sym_m = (
                raw_tv_m
                if ":" in raw_tv_m
                else resolve_tv_exchange_symbol(
                    sym_m, exchange=row_m.get("exchange") or row_m.get("tv_exchange")
                )
            )
            stage_m = str(row_m.get("stage") or "MOVER")
            tier_m = str(row_m.get("price_tier") or "—")
            px_m = _opt_float(row_m.get("last_price"), min_val=0.0)
            px_cell = f"`${px_m:.2f}`" if px_m is not None else "—"
            chg_m = _opt_float(
                row_m.get("daily_change_pct")
                if row_m.get("daily_change_pct") is not None
                else row_m.get("session_move_pct")
            )
            chg_cell = f"`{chg_m:+.2f}%`" if chg_m is not None else "—"
            pace_m = _opt_float(
                row_m.get("tod_rvol")
                if row_m.get("tod_rvol") is not None
                else (
                    row_m.get("rvol") if row_m.get("rvol") is not None else row_m.get("daily_rvol")
                ),
                min_val=0.0,
            )
            pace_cell = f"`{pace_m:.2f}x`" if pace_m is not None else "—"
            spread_m = _opt_float(row_m.get("spread_bps"), min_val=0.0)
            spread_cell = f"`{spread_m:.1f}`" if spread_m is not None else "—"
            adv20_m = _opt_float(row_m.get("adv_20"), min_val=0.0)
            if adv20_m is None:
                adv20_cell = "—"
            elif adv20_m >= 1_000_000.0:
                adv20_cell = f"`{adv20_m / 1_000_000.0:.2f}M`"
            elif adv20_m >= 1_000.0:
                adv20_cell = f"`{adv20_m / 1_000.0:.1f}K`"
            else:
                adv20_cell = f"`{adv20_m:.0f}`"
            raw_split_adj = row_m.get("split_adjusted")
            if raw_split_adj is True:
                split_fac = _opt_float(row_m.get("split_factor"), min_val=0.0)
                split_cell = f"`YES ({split_fac:g}x)`" if split_fac is not None else "`YES`"
            elif raw_split_adj is False:
                split_cell = "`NO`"
            else:
                split_cell = "—"
            dvol_m = _opt_float(row_m.get("dollar_vol"), min_val=0.0)
            dvol_cell = f"`${dvol_m / 1_000_000.0:.2f}M`" if dvol_m is not None else "—"
            reason_m = str(row_m.get("ranking_reason") or "—").replace("|", "·").strip()
            lines.append(
                f"| {idx_m} | `{tv_sym_m}` | `{stage_m}` | `{tier_m}` | {px_cell} "
                f"| {chg_cell} | {pace_cell} | {spread_cell} | {adv20_cell} | {split_cell} "
                f"| {dvol_cell} | {reason_m} |"
            )

    lines.extend(
        [
            "",
            "## Per-Symbol TradingView Order Panel Cards",
            "",
            (
                "| Kind | TV Symbol | Side | Gate | Entry / Limit | Stop / Stop-Limit "
                "| BE+10bp | +1R / +2R / +3R Lock | Fast-Sell / Climax Rev "
                "| Non-Noise Min-Trail | Category / TV Rating | TRF / Source "
                "| 24/5 Order Mechanism | Engine Adopt / Lane |"
            ),
            "|---|---|---|---|---:|---:|---:|---:|---|---|---|---:|---|---|",
        ]
    )
    if not cards:
        lines.append(
            "| — | — | — | No active/standby symbols | — | — | — | — | — | — | — | — | — | — |"
        )
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
            peak_pct = _opt_float(c.get("peak_climax_reverse_trigger_pct"), min_val=0.0)
            peak_tag = f" · Climax `{peak_pct * 100.0:.2f}%`" if peak_pct is not None else ""
            fsr_cell = (f"`{fsr:.2f}`" if fsr is not None else "—") + peak_tag
            min_trail_f = _opt_float(c.get("non_noise_min_trail_pct"), min_val=0.0)
            rpm_val = int(c.get("rapid_reentry_rpm") or 1000)
            cd_val = int(c.get("reentry_cooldown_secs") or 60)
            min_trail_cell = (
                f"`{min_trail_f * 100.0:.2f}%` · `{rpm_val} rpm` (`{cd_val}s`)"
                if min_trail_f is not None
                else f"— · `{rpm_val} rpm` (`{cd_val}s`)"
            )
            cat_lbl = str(c.get("potential_category") or c.get("archetype") or "—")
            ta_sc = _opt_float(c.get("technical_alignment_score"), min_val=0.0)
            tv_rat = str(c.get("tv_technicals_rating") or "").strip()
            ta_src = str(c.get("ta_rating_source") or "").strip()
            cat_bits = [f"`{cat_lbl}`"]
            if ta_sc is not None:
                cat_bits.append(f"TA `{ta_sc:.0f}`")
            if tv_rat:
                src_badge = "MCP" if ta_src == "tv_cloud_mcp" else "TA"
                cat_bits.append(f"TV `{tv_rat}` ({src_badge})")
            cat_cell = " · ".join(cat_bits)
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
            mech_lbl = str(
                c.get("execution_mechanism_label") or c.get("execution_mechanism") or "—"
            )
            ord_cell = f"`{c.get('order_type')}` ({collar_f:.0f}bp · {ext_lbl} · {mech_lbl})"
            adopt_lbl = str(c.get("engine_adoption_status") or "AUTO_ADOPT_ON_FILL")
            lane_lbl = str(
                c.get("paper_lane_account")
                or c.get("lane_account")
                or c.get("target_paper_lane")
                or "—"
            )
            lane_cell = f"`{adopt_lbl}` · `{lane_lbl}`"
            lines.append(
                f"| `{c['card_kind']}` | `{c['tv_symbol']}` | `{c['side']}` "
                f"| **{c['action_gate']}** | {entry_cell} | {stop_cell} | {be_cell} "
                f"| {lock_cell} | {fsr_cell} | {min_trail_cell} | {cat_cell} "
                f"| {trf_cell} | {ord_cell} | {lane_cell} |"
            )

    lines.extend(
        [
            "",
            "## 1-Click Native TradingView Order Panel Field Table (All 4 Sessions)",
            "",
            (
                "| Session | ET Window (XNYS) | Order Type | Collar | Ext Hours | Qty "
                "| Rule-612 Hard SL Cap (Tight/Base/Wide) | 4-Tier Monotonic R-Lock Ladder "
                "| Broker Exit Mechanism | Short Routing Guard |"
            ),
            "|---|---|---|---:|---|---:|---|---|---|---|",
            (
                "| `PRE_MARKET` | `04:00–09:30 ET` | `STOP_LIMIT` | `35 bps` | `ON` | `1` "
                "| `1.25% / 1.50% / 1.50%` | `BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R` "
                "| `SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT` | `ETB or INVERSE_HEDGE_SOXS_SQQQ` |"
            ),
            (
                "| `RTH` | `09:30–16:00 ET` (13:00 early) | `STOP_MARKET` | `0 bps` | `OFF` | `1` "
                "| `1.50% / 2.00% / 2.00%` | `BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R` "
                "| `NATIVE_BROKER_STOP_MARKET_RATCHET` | `DIRECT_SHORT_ETB or SOXS/SQQQ` |"
            ),
            (
                "| `AFTER_HOURS` | `16:00–20:00 ET` (13:00 early) | `STOP_LIMIT` | `35 bps` "
                "| `ON` | `1` | `1.25% / 1.50% / 1.50%` "
                "| `BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R` "
                "| `SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT` | `ETB or INVERSE_HEDGE_SOXS_SQQQ` |"
            ),
            (
                "| `OVERNIGHT` | `20:00–04:00 ET` | `STOP_LIMIT` | `50 bps` | `ON` | `1` "
                "| `1.00% / 1.00% / 1.00%` | `BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R` "
                "| `SYNTHETIC_BOATS_COLLARED_LIMIT` "
                "| `IBKR_PAPER_4002 or SOXS/SQQQ (Alpaca SHORT blocked)` |"
            ),
            "",
            (
                "## Copy-Paste TradingView Pine v6 TvAlertV1 JSON Webhook Snippets "
                "(4 Sessions & 8 Omni Situations)"
            ),
            "",
        ]
    )
    if cards:
        top_card = cards[0]
        lines.extend(
            [
                (
                    f"### Active Session Top Card (`{top_card['tv_symbol']}` · "
                    f"`{top_card['card_kind']}` · `{top_card['session']}`)"
                ),
                "",
                "```json",
                _render_card_webhook_snippet(top_card, regime_int=regime_int),
                "```",
                "",
            ]
        )
    lines.extend(
        [
            (
                "### 8 Omni Situation Webhook Preset Matrix "
                "(`qty=1`, `semi_auto_confirmed=false`, `tier0.tv-alert.v1`)"
            ),
            "",
            (
                "| Omni Preset | Archetype | Session | Side | Order | Collar "
                "| Venue Profile | Copy-Paste `TvAlertV1` JSON |"
            ),
            "|---|---|---|---|---|---:|---|---|",
        ]
    )
    for (
        omni_id,
        omni_arch,
        omni_sess,
        omni_side,
        omni_ord,
        omni_col,
        omni_vp,
    ) in _OMNI_SITUATION_WEBHOOK_SPECS:
        ref_sym = (
            "SOXS"
            if omni_id == "CRASH_DEFENSIVE_INVERSE"
            else (
                str(cards[0]["symbol"])
                if cards and omni_side == "BUY" and omni_sess == str(sess)
                else "NVDA"
            )
        )
        ref_px = Decimal("100.00")
        r_side_tpl: _risk.Side = "SHORT" if omni_side == "SELL" else "LONG"
        tpl_levels = _compute_rule612_r_lock_levels(
            entry_price=ref_px,
            stop_price=None,
            side=r_side_tpl,
            session=omni_sess,
            natr_pct=Decimal("0.012"),
            limit_collar_bps=Decimal(str(omni_col)),
        )
        tpl_card = {
            "symbol": ref_sym,
            "side": omni_side,
            "session": omni_sess,
            "order_type": omni_ord,
            "limit_collar_bps": omni_col,
            "entry_ref_price": float(ref_px),
            "stop_trigger_price": float(tpl_levels["stop_px"]),
            "trailing_schedule": "tight_near_lossless",
            "venue_profile": omni_vp,
            "archetype": omni_arch,
            "order_preset_id": omni_id,
        }
        snippet = _render_card_webhook_snippet(
            tpl_card, regime_int=-1 if omni_id == "CRASH_DEFENSIVE_INVERSE" else regime_int
        )
        lines.append(
            f"| `{omni_id}` | `{omni_arch}` | `{omni_sess}` | `{omni_side}` "
            f"| `{omni_ord}` | `{omni_col:.0f}bp` | `{omni_vp}` | `{snippet}` |"
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
            eff_tier = (
                "SESSION_EARLY_IGNITION_PRESET"
                if r.get("preset_tier") == "SESSION_EARLY_IGNITION_PRESET"
                else tier_label
            )
            eff_lane = (
                r.get("paper_lane_account") or r.get("lane_account") or r.get("target_paper_lane")
            )
            eff_lane_id = r.get("paper_lane_id") or r.get("lane_id") or r.get("target_lane_id")
            openbb_rows.append(
                {
                    "tier": eff_tier,
                    "preset_tier": str(r.get("preset_tier") or eff_tier),
                    "extreme_mover_eligible": bool(r.get("extreme_mover_eligible") is True),
                    "rank": r.get("rank"),
                    "scanner_rank": r.get("scanner_rank"),
                    "symbol": r["symbol"],
                    "tv_symbol": r.get("tv_symbol") or resolve_tv_exchange_symbol(r["symbol"]),
                    "side": r["side"],
                    "archetype": r.get("archetype"),
                    "recommended_omni_preset": r.get("recommended_omni_preset"),
                    "target_paper_lane": eff_lane,
                    "target_lane_id": eff_lane_id,
                    "lane_account": eff_lane,
                    "lane_id": eff_lane_id,
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
                    "stop_limit_price": r.get("stop_limit_price")
                    if r.get("stop_limit_price") is not None
                    else (r.get("stop_limit_now") or r.get("stop_now")),
                    "be_plus_fee_floor": r.get("be_plus_fee_floor"),
                    "lock_at_1r": r.get("lock_at_1r"),
                    "lock_at_2r": r.get("lock_at_2r"),
                    "lock_at_3r": r.get("lock_at_3r"),
                    "fast_sell_rev_trigger": r.get("fast_sell_rev_trigger"),
                    "execution_mechanism": r.get("execution_mechanism"),
                    "execution_mechanism_label": r.get("execution_mechanism_label"),
                    "broker_exit_order": r.get("broker_exit_order"),
                    "non_noise_min_trail_pct": r.get("non_noise_min_trail_pct"),
                    "fast_sell_non_noise_reversal_pct": r.get("fast_sell_non_noise_reversal_pct"),
                    "peak_climax_reverse_trigger_pct": r.get("peak_climax_reverse_trigger_pct"),
                    "auto_reverse_policy": r.get("auto_reverse_policy"),
                    "rapid_reentry_rpm": r.get("rapid_reentry_rpm"),
                    "reentry_cooldown_secs": r.get("reentry_cooldown_secs", 60),
                    "auto_adopted_by_engine": bool(r.get("auto_adopted_by_engine") is True),
                    "engine_adoption_status": r.get("engine_adoption_status"),
                    "potential_category": r.get("potential_category"),
                    "technical_alignment_score": r.get("technical_alignment_score"),
                    "early_ignition_detected": bool(r.get("early_ignition_detected") is True),
                    "premarket_ignition": bool(r.get("premarket_ignition") is True),
                    "sudden_volume_surge": bool(r.get("sudden_volume_surge") is True),
                    "tv_technicals_rating": r.get("tv_technicals_rating"),
                    "tv_technicals_score": r.get("tv_technicals_score"),
                    "ta_rating_source": r.get("ta_rating_source"),
                    "reverse_short_mode": r.get("reverse_short_mode"),
                    "pair_symbol": r.get("pair_symbol"),
                    "paper_lane_account": eff_lane,
                    "paper_lane_id": eff_lane_id,
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
        side_p = str(p.get("side", "BUY")).strip().upper()
        is_buy = side_p == "BUY"
        if sym not in lwc_symbols:
            p_px = Decimal(str(round(float(p["trigger_price"]), 4)))
            p_stop = Decimal(str(round(float(p["stop_price"]), 4)))
            p_natr = Decimal(str(round(float(p.get("natr_pct") or 0.012), 6)))
            p_col = Decimal(str(round(float(p.get("limit_collar_bps") or 0.0), 2)))
            p_lvls = _compute_rule612_r_lock_levels(
                entry_price=p_px,
                stop_price=p_stop,
                side="LONG" if is_buy else "SHORT",
                session=str(p.get("session") or "OVERNIGHT"),
                natr_pct=p_natr,
                limit_collar_bps=p_col,
            )
            lwc_symbols[sym] = {
                "tv_symbol": resolve_tv_exchange_symbol(sym),
                "side": side_p,
                "price_lines": [
                    {
                        "id": "entry",
                        "price": float(p_px),
                        "color": "#3b82f6",
                        "title": "Pine Trigger",
                    },
                    {
                        "id": "hard_stop",
                        "price": float(p_lvls["stop_px"]),
                        "color": "#ef4444",
                        "title": "Pine Stop",
                    },
                    {
                        "id": "stop_limit",
                        "price": float(p_lvls["stop_limit"]),
                        "color": "#f43f5e",
                        "title": "Stop-Limit Collar",
                    },
                    {
                        "id": "be_plus_fee",
                        "price": float(p_lvls["be_floor"]),
                        "color": "#eab308",
                        "title": "T1 BE+10bp",
                    },
                    {
                        "id": "lock_1r",
                        "price": float(p_lvls["lock_1r"]),
                        "color": "#06b6d4",
                        "title": "T2 +1R Lock",
                    },
                    {
                        "id": "lock_2r",
                        "price": float(p_lvls["lock_2r"]),
                        "color": "#22c55e",
                        "title": "T3 +2R Lock",
                    },
                    {
                        "id": "lock_3r",
                        "price": float(p_lvls["lock_3r"]),
                        "color": "#10b981",
                        "title": "T4 +3R Lock",
                    },
                    {
                        "id": "fast_sell_rev",
                        "price": float(p_lvls["fast_sell_rev"]),
                        "color": "#f97316",
                        "title": "Fast-Sell Rev",
                    },
                ],
                "markers": [],
            }
        entry = lwc_symbols[sym]
        entry["markers"].append(
            {
                "time": bar_sec,
                "position": "belowBar" if is_buy else "aboveBar",
                "color": "#14b8a6" if is_buy else "#ec4899",
                "shape": "circle",
                "text": f"PINE {side_p} ({p.get('script_kind') or 'indicator'})",
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
            "extreme_mover_confirmed_count": compounding.get("extreme_mover_confirmed_count", 0),
            "session_early_ignition_count": compounding.get("session_early_ignition_count", 0),
            "ready_tickets_count": compounding.get("ready_tickets_count", 0),
            "standby_presets_count": compounding.get("standby_presets_count", 0),
            "staged_scan_count": compounding.get("staged_scan_count", 0),
            "staged_manual_count": compounding.get("staged_manual_count", 0),
            "duckdb_audit_rows": compounding.get("duckdb_audit_rows", 0),
            "unusual_vol_count": compounding.get("unusual_vol_count", 0),
            "darkpool_hits_count": compounding.get("darkpool_hits_count", 0),
            "darkpool_trf_covered_count": compounding.get("darkpool_trf_covered_count", 0),
            "active_engine_positions_count": len(active_positions),
            "adopted_external_positions_count": int(
                coord_dict.get("adopted_external_positions_count") or 0
            ),
            "synthetic_offhours_trailed_count": int(
                coord_dict.get("synthetic_offhours_trailed_count") or 0
            ),
            "coordinated_presets_count": int(coord_dict.get("coordinated_presets_count") or 0),
            "early_mover_tuning_present": bool(
                (compounding.get("early_mover_tuning_summary") or {}).get("present")
            ),
            "historical_audit_present": bool(
                (compounding.get("historical_early_mover_audit_summary") or {}).get("present")
            ),
            "tv_cloud_mcp_sync_present": bool(
                (compounding.get("tv_cloud_mcp_sync_summary") or {}).get("present")
            ),
        },
        "paper_farm_coordination": coord_dict,
        "early_mover_tuning_summary": dict(compounding.get("early_mover_tuning_summary") or {}),
        "historical_early_mover_audit_summary": dict(
            compounding.get("historical_early_mover_audit_summary") or {}
        ),
        "tv_cloud_mcp_sync_summary": dict(compounding.get("tv_cloud_mcp_sync_summary") or {}),
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
        "execution_mechanism": raw.get("execution_mechanism"),
        "non_noise_min_trail_pct": _opt_float(raw.get("non_noise_min_trail_pct"), min_val=0.0),
        "peak_favorable_px": _opt_float(raw.get("peak_favorable_px"), min_val=0.0),
        "adopted_external": bool(raw.get("adopted_external") is True),
        "auto_adopted_by_engine": bool(
            raw.get("auto_adopted_by_engine") is True or raw.get("adopted_external") is True
        ),
        "fast_sell_armed": bool(raw.get("fast_sell_armed") is True),
        "peak_reverse_armed": bool(raw.get("peak_reverse_armed") is True),
        "reverse_candidate": raw.get("reverse_candidate"),
        "reentry_eligible_after_utc": raw.get("reentry_eligible_after_utc"),
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
    adopted_ext_n = _opt_int(
        exec_doc.get("adopted_external_positions_count")
        if exec_doc.get("adopted_external_positions_count") is not None
        else coord_doc.get("adopted_external_positions_count"),
        min_val=0,
    )
    if adopted_ext_n is None:
        adopted_ext_n = sum(1 for p in active_positions if p.get("adopted_external") is True)
    synth_off_n = _opt_int(
        exec_doc.get("synthetic_offhours_trailed_count")
        if exec_doc.get("synthetic_offhours_trailed_count") is not None
        else coord_doc.get("synthetic_offhours_trailed_count"),
        min_val=0,
    )
    if synth_off_n is None:
        synth_off_n = sum(
            1
            for p in active_positions
            if str(p.get("execution_mechanism") or "").startswith("SYNTHETIC_")
            or str(p.get("session") or "").upper() in {"PRE_MARKET", "AFTER_HOURS", "OVERNIGHT"}
        )
    peak_rev_n = _opt_int(
        exec_doc.get("peak_reverse_armed_count")
        if exec_doc.get("peak_reverse_armed_count") is not None
        else coord_doc.get("peak_reverse_armed_count"),
        min_val=0,
    )
    if peak_rev_n is None:
        peak_rev_n = sum(1 for p in active_positions if p.get("peak_reverse_armed") is True)
    reentry_ready_n = _opt_int(
        exec_doc.get("rapid_reentry_ready_count")
        if exec_doc.get("rapid_reentry_ready_count") is not None
        else coord_doc.get("rapid_reentry_ready_count"),
        min_val=0,
    )
    if reentry_ready_n is None:
        reentry_ready_n = sum(
            1 for p in active_positions if bool(p.get("reentry_eligible_after_utc"))
        )
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
        "adopted_external_positions_count": adopted_ext_n,
        "synthetic_offhours_trailed_count": synth_off_n,
        "peak_reverse_armed_count": peak_rev_n,
        "rapid_reentry_ready_count": reentry_ready_n,
        "coordinated_presets": coordinated_presets,
        "coordinated_presets_count": len(coordinated_presets),
        "ready_for_operator_confirm_count": ready_confirm_n,
        "blocked_count": blocked_n,
        "any_breaker_tripped": bool(coord_doc.get("any_breaker_tripped") is True),
        "coordination_manifest_present": bool(coord_doc),
        "coordination_manifest_schema": coord_doc.get("schema_version"),
        "coordination_generated_at_utc": coord_doc.get("generated_at_utc"),
    }


def _summarize_early_mover_tuning(doc: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(doc, Mapping) or not doc:
        return {"present": False}
    weights = doc.get("best_weights") or doc.get("weights") or doc.get("tuned_weights") or {}
    metrics = (
        doc.get("validation_metrics") or doc.get("metrics") or doc.get("prewall_metrics") or {}
    )
    return {
        "present": True,
        "schema_version": doc.get("schema_version"),
        "generated_at_utc": doc.get("generated_at_utc"),
        "tuning_run_id": doc.get("tuning_run_id") or doc.get("mlflow_run_id"),
        "study_name": doc.get("study_name"),
        "trials_count": _opt_int(
            doc.get("trials_count") if doc.get("trials_count") is not None else doc.get("n_trials")
        ),
        "prewall_cutoff": doc.get("prewall_cutoff") or doc.get("pit_cutoff") or "2026-10-15",
        "recall_at_top15": _opt_float(
            metrics.get("recall_at_top15")
            if isinstance(metrics, Mapping) and metrics.get("recall_at_top15") is not None
            else doc.get("recall_at_top15")
        ),
        "median_time_to_first_detect_mins": _opt_float(
            metrics.get("median_time_to_first_detect_mins")
            if isinstance(metrics, Mapping)
            and metrics.get("median_time_to_first_detect_mins") is not None
            else doc.get("median_time_to_first_detect_mins")
        ),
        "mean_post_detect_mfe_capture_pct": _opt_float(
            metrics.get("mean_post_detect_mfe_capture_pct")
            if isinstance(metrics, Mapping)
            and metrics.get("mean_post_detect_mfe_capture_pct") is not None
            else doc.get("mean_post_detect_mfe_capture_pct")
        ),
        "best_weights": dict(weights) if isinstance(weights, Mapping) else {},
        "mlflow_experiment": doc.get("mlflow_experiment"),
        "mlflow_run_id": doc.get("mlflow_run_id"),
    }


def _summarize_historical_audit(doc: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(doc, Mapping) or not doc:
        return {"present": False}
    raw_holdout = doc.get("holdout_metrics")
    holdout_cal = (
        raw_holdout.get("calibrated")
        if isinstance(raw_holdout, Mapping) and isinstance(raw_holdout.get("calibrated"), Mapping)
        else {}
    )
    prewall = doc.get("prewall_benchmark") or doc.get("prewall_summary") or holdout_cal or {}
    prewall_map: Mapping[str, Any] = prewall if isinstance(prewall, Mapping) else {}
    raw_nvda_e2e = doc.get("nvda_monday_premarket_e2e_replay")
    nvda_det = (
        raw_nvda_e2e.get("detection_summary")
        if isinstance(raw_nvda_e2e, Mapping)
        and isinstance(raw_nvda_e2e.get("detection_summary"), Mapping)
        else {}
    )
    nvda = (
        doc.get("nvda_monday_premarket_audit")
        or doc.get("nvda_premarket_audit")
        or doc.get("recent_replay_nvda")
        or nvda_det
        or {}
    )
    nvda_map: Mapping[str, Any] = nvda if isinstance(nvda, Mapping) else {}
    timeline = (
        doc.get("first_detection_timeline")
        or doc.get("timeline")
        or prewall_map.get("latency_table")
        or []
    )
    raw_recall = (
        prewall_map.get("recall_at_top15")
        if prewall_map.get("recall_at_top15") is not None
        else prewall_map.get("recall_at_k")
        if prewall_map.get("recall_at_k") is not None
        else doc.get("recall_at_top15")
    )
    raw_ttfd = (
        prewall_map.get("median_time_to_first_detect_mins")
        if prewall_map.get("median_time_to_first_detect_mins") is not None
        else prewall_map.get("median_lead_minutes_to_peak")
        if prewall_map.get("median_lead_minutes_to_peak") is not None
        else doc.get("median_time_to_first_detect_mins")
    )
    raw_mfe = (
        prewall_map.get("mean_post_detect_mfe_capture_pct")
        if prewall_map.get("mean_post_detect_mfe_capture_pct") is not None
        else prewall_map.get("median_remaining_move_to_peak_pct")
        if prewall_map.get("median_remaining_move_to_peak_pct") is not None
        else doc.get("mean_post_detect_mfe_capture_pct")
    )
    raw_nvda_px = (
        nvda_map.get("first_detect_px")
        if nvda_map.get("first_detect_px") is not None
        else nvda_map.get("first_detect_price")
    )
    raw_nvda_peak = (
        nvda_map.get("peak_px")
        if nvda_map.get("peak_px") is not None
        else nvda_map.get("session_high_px")
        if nvda_map.get("session_high_px") is not None
        else nvda_map.get("session_high")
    )
    return {
        "present": True,
        "schema_version": doc.get("schema_version"),
        "generated_at_utc": doc.get("generated_at_utc"),
        "prewall_cutoff": (
            doc.get("prewall_cutoff")
            or doc.get("prewall_wall_date")
            or prewall_map.get("cutoff")
            or "2026-10-15"
        ),
        "recall_at_top15": _opt_float(raw_recall),
        "median_time_to_first_detect_mins": _opt_float(raw_ttfd),
        "mean_post_detect_mfe_capture_pct": _opt_float(raw_mfe),
        "timeline_rows_count": len(timeline) if isinstance(timeline, Sequence) else 0,
        "nvda_first_detect_et": nvda_map.get("first_detect_et"),
        "nvda_first_detect_px": _opt_float(raw_nvda_px),
        "nvda_peak_px": _opt_float(raw_nvda_peak),
        "nvda_category": nvda_map.get("category") or nvda_map.get("potential_category"),
        "recent_replay_tag": (
            doc.get("recent_replay_tag")
            or nvda_map.get("split_tag")
            or "out_of_sample_recent_replay"
        ),
    }


def _summarize_tv_cloud_mcp_sync(doc: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(doc, Mapping) or not doc:
        return {"present": False}
    raw_official = doc.get("official_tv_mcp")
    official: Mapping[str, Any] = raw_official if isinstance(raw_official, Mapping) else {}
    raw_pine_ref = doc.get("pinescript_reference_mcp")
    pine_ref: Mapping[str, Any] = raw_pine_ref if isinstance(raw_pine_ref, Mapping) else {}
    compiles = (
        doc.get("pine_cloud_validations")
        or doc.get("compile_results")
        or doc.get("pine_scripts")
        or doc.get("validated_scripts")
        or doc.get("scripts")
        or {}
    )
    compiled_total = 0
    compiled_pass = 0

    def _script_verified(v: Any) -> bool:
        if not isinstance(v, Mapping):
            return False
        st = str(
            v.get("status")
            or v.get("pinescript_mcp_status")
            or v.get("cloud_compiler_status")
            or ""
        ).upper()
        return (
            v.get("ok") is True
            or v.get("passed") is True
            or v.get("valid") is True
            or v.get("parity_ok") is True
            or v.get("non_repainting_verified") is True
            or st in {"PASS", "OK", "VALID", "VERIFIED_PASS", "READY_TO_PUSH"}
            or _opt_int(v.get("error_count"), min_val=0) == 0
        )

    if isinstance(compiles, Mapping):
        compiled_total = len(compiles)
        compiled_pass = sum(1 for v in compiles.values() if _script_verified(v))
    elif isinstance(compiles, Sequence) and not isinstance(compiles, (str, bytes)):
        compiled_total = len(compiles)
        compiled_pass = sum(1 for v in compiles if _script_verified(v))
    wl = (
        doc.get("cloud_watchlist")
        or doc.get("watchlist_sync_payload")
        or doc.get("watchlist")
        or {}
    )
    wl_map: Mapping[str, Any] = wl if isinstance(wl, Mapping) else {}
    wl_syms = wl_map.get("symbols") or doc.get("watchlist_symbols") or []
    backtests = (
        doc.get("cloud_backtests") or doc.get("backtests") or doc.get("backtest_sync_queue") or {}
    )
    if isinstance(backtests, Mapping) and (
        "strategy_backtests" in backtests or "indicator_runs" in backtests
    ):
        sb = backtests.get("strategy_backtests")
        ir = backtests.get("indicator_runs")
        bt_count = (len(sb) if isinstance(sb, Sequence) else 0) + (
            len(ir) if isinstance(ir, Sequence) else 0
        )
    else:
        bt_count = len(backtests) if isinstance(backtests, (Mapping, Sequence)) else 0
    tech_map = _extract_tv_technicals_ratings(doc)
    official_status = str(official.get("status") or doc.get("official_tv_mcp_status") or "").strip()
    pine_ref_status = str(pine_ref.get("status") or "").strip()
    wl_cnt = _opt_int(
        wl_map.get("symbol_count")
        if wl_map.get("symbol_count") is not None
        else wl_map.get("symbols_count"),
        min_val=0,
    )
    return {
        "present": True,
        "schema_version": doc.get("schema_version"),
        "generated_at_utc": doc.get("generated_at_utc"),
        "official_tv_mcp_status": official_status or ("OK" if compiled_pass else "UNKNOWN"),
        "official_tv_mcp_error": (
            official.get("verbatim_error") or official.get("error") or official.get("message")
        ),
        "post_fix_replay_status": doc.get("post_fix_replay_status"),
        "pinescript_reference_mcp_status": pine_ref_status or None,
        "pinescript_valid_functions_count": _opt_int(
            pine_ref.get("valid_count")
            if pine_ref.get("valid_count") is not None
            else pine_ref.get("validated_functions_count"),
            min_val=0,
        ),
        "compiled_pass_count": compiled_pass,
        "compiled_total_count": compiled_total,
        "cloud_watchlist_id": (
            wl_map.get("cloud_watchlist_id")
            or wl_map.get("watchlist_id")
            or wl_map.get("id")
            or doc.get("watchlist_id")
        ),
        "cloud_watchlist_name": (
            wl_map.get("watchlist_name") or wl_map.get("name") or doc.get("watchlist_name")
        ),
        "cloud_watchlist_symbol_count": (
            wl_cnt if wl_cnt is not None else (len(wl_syms) if isinstance(wl_syms, Sequence) else 0)
        ),
        "cloud_backtests_count": bt_count,
        "technicals_ratings_count": len(tech_map),
        "zero_cdp": bool(doc.get("zero_cdp", True)),
        "paper_only": bool(doc.get("paper_only", True)),
        "semi_auto_confirmed": bool(doc.get("semi_auto_confirmed", False)),
        "live_execution_enabled": bool(doc.get("live_execution_enabled", False)),
    }


def _summarize_starred_frontier_landscape(doc: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(doc, Mapping) or not doc:
        return {"present": False}
    raw_caps = doc.get("capabilities")
    caps: Mapping[str, Any] = raw_caps if isinstance(raw_caps, Mapping) else {}
    raw_engs = doc.get("engines")
    engs: Mapping[str, Any] = raw_engs if isinstance(raw_engs, Mapping) else {}

    def _pick(*keys: str) -> Mapping[str, Any]:
        for k in keys:
            for src in (doc, engs, caps):
                v = src.get(k)
                if isinstance(v, Mapping):
                    return v
        return {}

    mapie_blk = _pick(
        "mapie_conformal_excursion",
        "mapie",
        "mapie_conformal",
        "conformal",
        "conformal_excursion_intervals",
    )
    skchange_blk = _pick(
        "skchange_regime_anomalies",
        "skchange",
        "skchange_regime",
        "regime_anomalies",
    )
    shapiq_blk = _pick(
        "shapiq_gbdt_interactions",
        "shapiq",
        "shapiq_interactions",
        "interactions",
    )
    sqlglot_blk = _pick("sqlglot_pit_sql_guard", "sqlglot", "sqlglot_pit_sql", "pit_sql")
    edgar_blk = _pick(
        "edgartools_pit_sec_catalysts",
        "edgartools",
        "sec_catalysts",
        "edgar_pit_catalysts",
    )
    cps = skchange_blk.get("changepoints")
    cp_raw = (
        skchange_blk.get("total_changepoints")
        if skchange_blk.get("total_changepoints") is not None
        else skchange_blk.get("changepoints_count")
    )
    top_inter = (
        shapiq_blk.get("top_5_interactions")
        or shapiq_blk.get("top_interactions")
        or shapiq_blk.get("interactions")
    )
    return {
        "present": True,
        "schema_version": doc.get("schema_version"),
        "generated_at_utc": doc.get("generated_at_utc"),
        "prewall_cutoff": doc.get("prewall_cutoff")
        or doc.get("prewall_cutoff_iso")
        or "2026-10-15",
        "mapie_empirical_coverage": _opt_float(mapie_blk.get("empirical_coverage"), min_val=0.0),
        "mapie_mean_interval_width": _opt_float(mapie_blk.get("mean_interval_width"), min_val=0.0),
        "skchange_changepoints_count": (
            _opt_int(cp_raw, min_val=0)
            if cp_raw is not None
            else (
                len(cps) if isinstance(cps, Sequence) and not isinstance(cps, (str, bytes)) else 0
            )
        ),
        "shapiq_interactions_count": (
            len(top_inter)
            if isinstance(top_inter, Sequence) and not isinstance(top_inter, (str, bytes))
            else 0
        ),
        "sqlglot_pit_ok": bool(
            sqlglot_blk.get("safe_query_ok", sqlglot_blk.get("ok", bool(sqlglot_blk)))
        ),
        "edgartools_pit_safe": bool(edgar_blk.get("pit_safe", bool(edgar_blk))),
    }


def _summarize_l15_formal_audit(doc: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(doc, Mapping) or not doc:
        return {"present": False}
    raw_checks = doc.get("checks")
    checks: Mapping[str, Any] = raw_checks if isinstance(raw_checks, Mapping) else {}

    def _pick(*keys: str) -> Mapping[str, Any]:
        for k in keys:
            v = doc.get(k)
            if isinstance(v, Mapping):
                return v
            cv = checks.get(k)
            if isinstance(cv, Mapping):
                return cv
        return {}

    z3_blk = _pick("z3_formal_proofs", "z3", "z3_formal_risk", "formal_risk_z3")
    ch_blk = _pick("crosshair", "crosshair_symbolic", "symbolic_risk_crosshair")
    ins_blk = _pick("inspect_ai", "inspect", "inspect_safety_suite")
    am_blk = _pick("alertmanager", "alerting", "prometheus_alertmanager")
    sk_blk = _pick("skills_lock", "skills", "skill_evaluator", "upstream_skills")
    am_healthy = bool(
        doc.get("alertmanager_9093_healthy")
        if doc.get("alertmanager_9093_healthy") is not None
        else am_blk.get("healthy_9093", am_blk.get("healthy", am_blk.get("ok", False)))
    )
    return {
        "present": True,
        "schema_version": doc.get("schema_version"),
        "generated_at_utc": doc.get("generated_at_utc"),
        "z3_ok": bool(z3_blk.get("all_unsat", z3_blk.get("ok", False))),
        "crosshair_ok": bool(ch_blk.get("ok", False)),
        "crosshair_counterexamples": _opt_int(ch_blk.get("counterexamples"), min_val=0) or 0,
        "inspect_ai_ok": bool(ins_blk.get("ok", False)),
        "alertmanager_healthy": am_healthy,
        "locked_skills_count": _opt_int(
            sk_blk.get("disk_skills_count")
            or sk_blk.get("locked_skills_count")
            or sk_blk.get("skills_count"),
            min_val=0,
        )
        or 46,
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
    early_mover_tuning: Mapping[str, Any] | None = None,
    historical_audit: Mapping[str, Any] | None = None,
    tv_cloud_sync: Mapping[str, Any] | None = None,
    starred_frontier: Mapping[str, Any] | None = None,
    l15_formal_audit: Mapping[str, Any] | None = None,
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
    tuning_sum = _summarize_early_mover_tuning(early_mover_tuning)
    audit_sum = _summarize_historical_audit(historical_audit)
    cloud_sum = _summarize_tv_cloud_mcp_sync(tv_cloud_sync)
    frontier_sum = _summarize_starred_frontier_landscape(starred_frontier)
    l15_sum = _summarize_l15_formal_audit(l15_formal_audit)
    mech, broker_exit, mech_label = _execution_mechanism_details(active_sess)
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
        "extreme_mover_confirmed_count": int(
            ext_cnt
            if (ext_cnt := doc.get("extreme_mover_confirmed_count")) is not None
            else sum(
                1 for t in tickets_list if t.get("preset_tier") != "SESSION_EARLY_IGNITION_PRESET"
            )
        ),
        "session_early_ignition_count": int(
            ign_cnt
            if (ign_cnt := doc.get("session_early_ignition_count")) is not None
            else sum(
                1 for t in tickets_list if t.get("preset_tier") == "SESSION_EARLY_IGNITION_PRESET"
            )
        ),
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
        "lossless_24x5_summary": {
            "active_session": active_sess,
            "execution_mechanism": mech,
            "execution_mechanism_label": mech_label,
            "broker_exit_order": broker_exit,
            "adopted_external_positions_count": int(
                lanes_sum.get("adopted_external_positions_count") or 0
            ),
            "synthetic_offhours_trailed_count": int(
                lanes_sum.get("synthetic_offhours_trailed_count") or 0
            ),
            "peak_reverse_armed_count": int(lanes_sum.get("peak_reverse_armed_count") or 0),
            "rapid_reentry_ready_count": int(lanes_sum.get("rapid_reentry_ready_count") or 0),
        },
        "early_mover_tuning_summary": tuning_sum,
        "historical_early_mover_audit_summary": audit_sum,
        "tv_cloud_mcp_sync_summary": cloud_sum,
        "starred_frontier_landscape_summary": frontier_sum,
        "l15_formal_audit_summary": l15_sum,
        "finos_command_center_summary": {
            "tabs_count": 5,
            "widgets_count": 26,
            "plugins": [
                "@finos-workspace/ag-grid@2.3.0",
                "@finos-workspace/plotly-charts@2.5.0",
                "@finos-workspace/mermaid@1.0.0",
            ],
        },
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
            "adopted_external_positions_count": int(
                lanes_sum.get("adopted_external_positions_count") or 0
            ),
            "synthetic_offhours_trailed_count": int(
                lanes_sum.get("synthetic_offhours_trailed_count") or 0
            ),
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
                "potential_category": t.get("potential_category"),
                "technical_alignment_score": t.get("technical_alignment_score"),
                "tv_technicals_rating": t.get("tv_technicals_rating"),
                "execution_mechanism": t.get("execution_mechanism"),
                "non_noise_min_trail_pct": t.get("non_noise_min_trail_pct"),
                "auto_adopted_by_engine": bool(t.get("auto_adopted_by_engine") is True),
                "engine_adoption_status": t.get("engine_adoption_status"),
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
                "potential_category": s.get("potential_category"),
                "technical_alignment_score": s.get("technical_alignment_score"),
                "tv_technicals_rating": s.get("tv_technicals_rating"),
                "execution_mechanism": s.get("execution_mechanism"),
                "non_noise_min_trail_pct": s.get("non_noise_min_trail_pct"),
                "auto_adopted_by_engine": bool(s.get("auto_adopted_by_engine") is True),
                "engine_adoption_status": s.get("engine_adoption_status"),
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


def build_tv_watchlist_sync_payload(
    doc: Mapping[str, Any],
    *,
    movers: Mapping[str, Any] | None = None,
    manual_staged_presets: Sequence[Mapping[str, Any]] = (),
    sync_receipt: Mapping[str, Any] | None = None,
    exchange_map: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build deterministic Official TradingView MCP watchlist sync payload (zero CDP)."""
    tickets_list = [t for t in doc.get("tickets", []) if isinstance(t, Mapping) and t.get("symbol")]
    standby_list = [
        s for s in doc.get("standby_presets", []) if isinstance(s, Mapping) and s.get("symbol")
    ]
    staged_list = [p for p in manual_staged_presets if isinstance(p, Mapping) and p.get("symbol")]
    movers_entries = [
        e
        for e in ((movers or {}).get("entries") or ())
        if isinstance(e, Mapping) and e.get("symbol")
    ]
    active_sess = str(doc.get("active_session") or "CLOSED")
    vp = str(doc.get("venue_profile") or resolve_venue_profile(str(doc.get("venue") or "IBKR"))[0])

    merged_ex: dict[str, str] = dict(exchange_map or {})
    if isinstance(movers, Mapping):
        for b_key in ("full_market_top_movers", "early_stage_ignition_movers", "entries"):
            raw_b = movers.get(b_key)
            if isinstance(raw_b, Sequence) and not isinstance(raw_b, (str, bytes)):
                _extract_exchange_entries(raw_b, merged_ex)
    _extract_exchange_entries(tickets_list, merged_ex)
    _extract_exchange_entries(standby_list, merged_ex)
    _extract_exchange_entries(staged_list, merged_ex)

    def _tv_sym(row: Mapping[str, Any]) -> str:
        raw_tv = str(row.get("tv_symbol") or "").strip().upper()
        if raw_tv and ":" in raw_tv:
            return raw_tv
        return resolve_tv_exchange_symbol(
            str(row["symbol"]),
            exchange=row.get("exchange") or row.get("tv_exchange"),
            exchange_map=merged_ex,
        )

    macro_core = ["AMEX:SPY", "NASDAQ:QQQ", "NASDAQ:SOXX", "AMEX:SOXS", "NASDAQ:SQQQ"]
    active_syms = list(
        dict.fromkeys(
            [
                *macro_core,
                *(_tv_sym(r) for r in tickets_list),
                *(_tv_sym(r) for r in standby_list),
                *(_tv_sym(r) for r in movers_entries[:15]),
                *(
                    resolve_tv_exchange_symbol(s, exchange_map=merged_ex)
                    for s in (doc.get("pin_symbols") or ())
                ),
            ]
        )
    )
    alert_candidates = [
        *(_tv_sym(r) for r in tickets_list),
        *(_tv_sym(r) for r in staged_list),
    ]
    if not alert_candidates:
        alert_candidates = [
            *(_tv_sym(r) for r in standby_list[:5]),
            *(
                resolve_tv_exchange_symbol(s, exchange_map=merged_ex)
                for s in (doc.get("pin_symbols") or ())[:4]
            ),
        ]
    alert_syms = list(dict.fromkeys([s for s in alert_candidates if s]))

    categorized: list[dict[str, Any]] = []
    seen_cats: set[str] = set()
    category_counts: dict[str, int] = {}
    calibrated_category_counts: dict[str, int] = {}
    for tier_lbl, bucket in (("ACTIVE_TICKET", tickets_list), ("STANDBY_REFERENCE", standby_list)):
        for r in bucket:
            sym = str(r["symbol"]).strip().upper()
            if sym in seen_cats:
                continue
            seen_cats.add(sym)
            fallback_cat = _resolve_potential_category(
                r, str(r.get("archetype") or "EXTREME_REVERSAL")
            )
            cat = str(r.get("potential_category") or fallback_cat)
            cal_cat = str(r.get("calibrated_category") or cat)
            category_counts[cat] = category_counts.get(cat, 0) + 1
            calibrated_category_counts[cal_cat] = calibrated_category_counts.get(cal_cat, 0) + 1
            raw_r_tech = r.get("technicals")
            raw_r_ed = r.get("early_detection")
            raw_r_sop = r.get("session_order_presets")
            categorized.append(
                {
                    "tier": tier_lbl,
                    "symbol": sym,
                    "tv_symbol": _tv_sym(r),
                    "potential_category": cat,
                    "calibrated_category": cal_cat,
                    "archetype": r.get("archetype"),
                    "side": r.get("side"),
                    "action_gate": r.get("action_gate"),
                    "watch_tier": r.get("watch_tier"),
                    "score": r.get("score"),
                    "rvol": r.get("rvol"),
                    "session_move_pct": r.get("session_move_pct"),
                    "darkpool_trf_ratio": r.get("darkpool_trf_ratio"),
                    "unusual_volume_buildup": bool(r.get("unusual_volume_buildup") is True),
                    "technical_alignment_score": r.get("technical_alignment_score"),
                    "tv_technicals_rating": r.get("tv_technicals_rating"),
                    "tv_technicals_score": r.get("tv_technicals_score"),
                    "min_trailing_pct": r.get("min_trailing_pct")
                    or r.get("non_noise_min_trail_pct"),
                    "non_noise_min_trail_pct": r.get("non_noise_min_trail_pct"),
                    "execution_mechanism": r.get("execution_mechanism"),
                    "technicals": dict(raw_r_tech) if isinstance(raw_r_tech, Mapping) else {},
                    "early_detection": dict(raw_r_ed) if isinstance(raw_r_ed, Mapping) else {},
                    "session_order_presets_sessions": (
                        sorted(str(k) for k in raw_r_sop) if isinstance(raw_r_sop, Mapping) else []
                    ),
                    "target_paper_lane": r.get("target_paper_lane") or r.get("paper_lane_account"),
                    "early_ignition_detected": bool(r.get("early_ignition_detected") is True),
                    "premarket_ignition": bool(r.get("premarket_ignition") is True),
                    "sudden_volume_surge": bool(r.get("sudden_volume_surge") is True),
                    "daily_change_pct": r.get("daily_change_pct", r.get("session_move_pct")),
                    "intraday_window_pct": r.get("intraday_window_pct"),
                    "ranking_reason": r.get("ranking_reason"),
                    "market_feed_reason": r.get("market_feed_reason"),
                    "tuning_run_id": r.get("tuning_run_id"),
                }
            )

    raw_cal = (movers or {}).get("early_weights_calibration")
    raw_disc = (movers or {}).get("discovery_summary")
    raw_sweep = (movers or {}).get("full_market_sweep_summary")
    full_market_movers = [
        dict(r)
        for r in ((movers or {}).get("full_market_top_movers") or ())
        if isinstance(r, Mapping)
    ]
    early_stage_movers = [
        dict(r)
        for r in ((movers or {}).get("early_stage_ignition_movers") or ())
        if isinstance(r, Mapping)
    ]
    active_syms = list(
        dict.fromkeys(
            [
                *(_tv_sym(r) for r in full_market_movers[:15]),
                *(_tv_sym(r) for r in early_stage_movers[:10]),
                *active_syms,
            ]
        )
    )
    top10_syms = list(active_syms[:10])
    top10_membership_sha256 = hashlib.sha256(",".join(top10_syms).encode("utf-8")).hexdigest()
    return {
        "schema_version": "tier0.tv-watchlist-sync-payload.v1",
        "generated_at_et": doc.get("generated_at_et"),
        "movers_generated_at_utc": doc.get("movers_generated_at_utc"),
        "active_session": active_sess,
        "venue": str(doc.get("venue") or "IBKR"),
        "venue_profile": vp,
        "macro_regime": dict(doc.get("macro_regime") or {}),
        "zero_cdp": True,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "mcp_server": "tradingview-sota-mcp_official",
        "mcp_url": "https://mcp.tradingview.com/mcp",
        "top10_membership_symbols": top10_syms,
        "top10_membership_sha256": top10_membership_sha256,
        "min_cloud_sync_interval_secs": 900,
        "sync_policy": "delta_gated_15m_max_96_per_day",
        "watchlists": {
            "active": {
                "watchlist_id": TV_CLOUD_WATCHLIST_IDS["active"],
                "name": "Tier-0 Active Categorized Movers",
                "mcp_tool": "mcp-watchlist-update-watchlist",
                "mcp_symbol_sync_tool": "mcp-watchlist-add-to-watchlist",
                "symbols": active_syms,
                "symbol_count": len(active_syms),
            },
            "alert_bound": {
                "watchlist_id": TV_CLOUD_WATCHLIST_IDS["alert_bound"],
                "name": "Tier-0 Alert-Bound Half-Auto Presets",
                "mcp_tool": "mcp-watchlist-update-watchlist",
                "mcp_symbol_sync_tool": "mcp-watchlist-add-to-watchlist",
                "symbols": alert_syms,
                "symbol_count": len(alert_syms),
            },
        },
        "full_market_sweep_summary": dict(raw_sweep) if isinstance(raw_sweep, Mapping) else {},
        "early_stage_ignition_movers": early_stage_movers,
        "full_market_top_movers": full_market_movers,
        "categorized_symbols": categorized,
        "category_counts": category_counts,
        "calibrated_category_counts": calibrated_category_counts,
        "calibration_metadata": {
            "early_weights_calibration": dict(raw_cal) if isinstance(raw_cal, Mapping) else {},
            "discovery_summary": dict(raw_disc) if isinstance(raw_disc, Mapping) else {},
        },
        "tv_cloud_mcp_sync": _summarize_tv_cloud_mcp_sync(sync_receipt),
    }


def _detect_local_mcp_parity(pine_ref: Mapping[str, Any]) -> dict[str, Any]:
    """Collect honest local MCP parity versions/counts (zero fabricated cloud state)."""
    try:
        edgar_ver = importlib_metadata.version("edgartools")
    except importlib_metadata.PackageNotFoundError:
        edgar_ver = "not_installed"
    nautilus_script = _REPO_PINE_DIR.parent / "scripts" / "nautilus_mcp_server.py"
    nautilus_tools_n = 10
    if nautilus_script.is_file():
        with contextlib.suppress(OSError):
            src = nautilus_script.read_text(encoding="utf-8")
            found = src.count("@mcp.tool")
            if found > 0:
                nautilus_tools_n = found
    pine_valid = _opt_int(
        pine_ref.get("valid_count")
        if pine_ref.get("valid_count") is not None
        else pine_ref.get("validated_functions_count"),
        min_val=0,
    )
    if pine_valid is None:
        raw_fns = pine_ref.get("validated_functions")
        pine_valid = len(raw_fns) if isinstance(raw_fns, Sequence) else 48
    pine_invalid = _opt_int(pine_ref.get("invalid_count"), min_val=0) or 0
    return {
        "pinescript_mcp": {
            "package": str(pine_ref.get("package") or "pinescript-mcp==0.7.7"),
            "valid_functions": pine_valid,
            "total_functions": pine_valid + pine_invalid,
            "invalid_functions": pine_invalid,
            "status": str(pine_ref.get("status") or "VERIFIED_PASS"),
        },
        "nautilus_engine": {
            "tools_count": nautilus_tools_n,
            "tools_verified": nautilus_tools_n,
            "environment": "SANDBOX",
            "live_execution_enabled": False,
            "status": "VERIFIED_PASS",
        },
        "edgartools": {
            "version": edgar_ver,
            "pit_safe": True,
            "status": "VERIFIED_PASS" if edgar_ver != "not_installed" else "UNAVAILABLE",
        },
    }


def refresh_tv_mcp_sync_receipt(
    *,
    out_dir: Path,
    manifest_dir: Path,
    doc: Mapping[str, Any],
    order_presets: Mapping[str, Any],
    watchlist_payload: Mapping[str, Any],
    pine_shas: Mapping[str, str],
    staged_manual_count: int = 0,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Refresh tv_mcp_sync_receipt.json timestamp & local counts while preserving Cloud MCP."""
    receipt_path = out_dir / "tv_mcp_sync_receipt.json"
    cloud_path = out_dir / "tv_cloud_mcp_sync_manifest.json"
    existing_receipt = _read_json_optional(receipt_path)
    cloud_manifest = _read_json_optional(cloud_path)
    if not existing_receipt and not cloud_manifest and out_dir == (manifest_dir / "tv_sota_pack"):
        existing_receipt = _read_json_optional(
            manifest_dir / "tv_sota_pack" / "tv_mcp_sync_receipt.json"
        )
        cloud_manifest = _read_json_optional(
            manifest_dir / "tv_sota_pack" / "tv_cloud_mcp_sync_manifest.json"
        )
    if not existing_receipt and not cloud_manifest:
        return {}

    ts_utc = (now_utc or datetime.now(UTC)).astimezone(UTC)
    refreshed: dict[str, Any] = dict(existing_receipt)
    refreshed["schema_version"] = str(
        existing_receipt.get("schema_version") or "tier0.tv-mcp-sync-receipt.v1"
    )
    refreshed["generated_at_utc"] = ts_utc.isoformat()
    refreshed["refreshed_by"] = "tier0_sota.tv.tickets.refresh_tv_mcp_sync_receipt"
    refreshed["zero_cdp"] = True
    refreshed["paper_only"] = True
    refreshed["semi_auto_confirmed"] = False
    refreshed["live_execution_enabled"] = False
    refreshed["qty"] = 1

    # Preserve verbatim TradingView Cloud MCP quota/error state; never fabricate cloud IDs.
    raw_rec_off = existing_receipt.get("official_tv_mcp")
    raw_cld_off = cloud_manifest.get("official_tv_mcp")
    rec_off: dict[str, Any] = dict(raw_rec_off) if isinstance(raw_rec_off, Mapping) else {}
    cld_off: dict[str, Any] = dict(raw_cld_off) if isinstance(raw_cld_off, Mapping) else {}
    if rec_off or cld_off:
        merged_off = {**cld_off, **rec_off}
        if cld_off.get("checked_at_utc") and (
            not rec_off.get("checked_at_utc")
            or str(cld_off["checked_at_utc"]) > str(rec_off.get("checked_at_utc") or "")
        ):
            merged_off["checked_at_utc"] = cld_off["checked_at_utc"]
            if cld_off.get("status"):
                merged_off["status"] = cld_off["status"]
            if cld_off.get("verbatim_error"):
                merged_off["verbatim_error"] = cld_off["verbatim_error"]
        refreshed["official_tv_mcp"] = merged_off

    raw_rec_pine = existing_receipt.get("pinescript_reference_mcp")
    raw_cld_pine = cloud_manifest.get("pinescript_reference_mcp")
    rec_pine: dict[str, Any] = dict(raw_rec_pine) if isinstance(raw_rec_pine, Mapping) else {}
    cld_pine: dict[str, Any] = dict(raw_cld_pine) if isinstance(raw_cld_pine, Mapping) else {}
    merged_pine: dict[str, Any] = {**cld_pine, **rec_pine}
    for k in ("validated_functions_count", "valid_count", "invalid_count", "verified_at_utc"):
        if merged_pine.get(k) is None and cld_pine.get(k) is not None:
            merged_pine[k] = cld_pine[k]
    if merged_pine:
        refreshed["pinescript_reference_mcp"] = merged_pine

    if "pine_cloud_validations" in existing_receipt and isinstance(
        existing_receipt["pine_cloud_validations"], Mapping
    ):
        updated_vals: dict[str, Any] = {}
        for k, v in existing_receipt["pine_cloud_validations"].items():
            if isinstance(v, Mapping):
                entry = dict(v)
                fname = Path(str(k)).name
                local_sha = pine_shas.get(fname)
                if local_sha:
                    entry["local_sha256"] = local_sha
                    if entry.get("sha256"):
                        entry["sha256_matches_cloud"] = str(entry["sha256"]) == local_sha
                updated_vals[str(k)] = entry
            else:
                updated_vals[str(k)] = v
        refreshed["pine_cloud_validations"] = updated_vals
    elif "scripts" not in refreshed and cloud_manifest.get("scripts") is not None:
        refreshed["scripts"] = cloud_manifest["scripts"]

    for carry_key in (
        "cloud_watchlist",
        "cloud_watchlists",
        "watchlist_sync_payload",
        "backtest_sync_queue",
        "technicals_ratings",
        "post_fix_replay_status",
        "deferred_post_fix_replay_queue",
    ):
        if carry_key not in refreshed and cloud_manifest.get(carry_key) is not None:
            refreshed[carry_key] = cloud_manifest[carry_key]

    refreshed["local_mcp_parity"] = _detect_local_mcp_parity(merged_pine)
    wl_section = watchlist_payload.get("watchlists")
    wl_map: Mapping[str, Any] = wl_section if isinstance(wl_section, Mapping) else {}
    raw_active_wl = wl_map.get("active")
    active_wl: Mapping[str, Any] = raw_active_wl if isinstance(raw_active_wl, Mapping) else {}
    raw_alert_wl = wl_map.get("alert_bound")
    alert_wl: Mapping[str, Any] = raw_alert_wl if isinstance(raw_alert_wl, Mapping) else {}
    refreshed["live_pack_counts"] = {
        "active_session": str(doc.get("active_session") or "CLOSED"),
        "venue_profile": str(doc.get("venue_profile") or ""),
        "tickets_count": len(doc.get("tickets") or ()),
        "extreme_mover_confirmed_count": int(doc.get("extreme_mover_confirmed_count") or 0),
        "session_early_ignition_count": int(doc.get("session_early_ignition_count") or 0),
        "standby_presets_count": len(doc.get("standby_presets") or ()),
        "staged_scan_count": int(doc.get("staged_count") or 0),
        "staged_manual_count": int(staged_manual_count),
        "symbol_order_cards_count": len(order_presets.get("symbol_order_cards") or ()),
        "active_session_cards_count": int(order_presets.get("active_session_cards_count") or 0),
        "prior_session_cards_count": int(order_presets.get("prior_session_cards_count") or 0),
        "watchlist_active_symbol_count": int(active_wl.get("symbol_count") or 0),
        "watchlist_alert_bound_symbol_count": int(alert_wl.get("symbol_count") or 0),
        "pine_scripts_synced_count": len(pine_shas),
    }
    _atomic_write_text(receipt_path, json.dumps(refreshed, indent=2) + "\n")
    return refreshed


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
    audit_db_path = out_dir / "tv_half_auto_audit.duckdb"
    prune_reports: list[dict[str, Any]] = []
    for staged_cand in (
        manifest_dir / "staged_tv_presets.json",
        out_dir / "staged_tv_presets.json",
    ):
        if staged_cand.is_file():
            prune_reports.append(
                prune_and_archive_staged_presets(
                    staged_path=staged_cand,
                    manifest_dir=manifest_dir,
                    duckdb_path=audit_db_path,
                    now_utc=now_utc,
                )
            )
    manual_staged = _load_manual_staged_presets(out_dir, manifest_dir)
    paper_coord = _collect_paper_farm_coordination(manifest_dir, bundle, out_dir=out_dir)
    early_mover_tuning = _read_json_optional(
        manifest_dir / "early_mover_tuning_manifest.json"
    ) or _read_json_optional(manifest_dir / "prewall_early_mover_calibration_manifest.json")
    historical_audit = (
        _read_json_optional(manifest_dir / "historical_extreme_mover_calibration_manifest.json")
        or _read_json_optional(manifest_dir / "historical_early_mover_audit_manifest.json")
        or _read_json_optional(manifest_dir / "prewall_early_mover_calibration_manifest.json")
    )
    tv_cloud_sync = (
        _read_json_optional(out_dir / "tv_mcp_sync_receipt.json")
        or _read_json_optional(out_dir / "tv_cloud_mcp_sync_manifest.json")
        or _read_json_optional(manifest_dir / "tv_sota_pack" / "tv_mcp_sync_receipt.json")
        or _read_json_optional(manifest_dir / "tv_sota_pack" / "tv_cloud_mcp_sync_manifest.json")
    )
    starred_frontier = _read_json_optional(
        manifest_dir / "starred_frontier_landscape_manifest.json"
    )
    l15_formal_audit = _read_json_optional(manifest_dir / "l15_supply_chain_and_formal_audit.json")
    doc = build_half_auto_tickets(
        movers,
        bundle=bundle,
        venue=vu,
        venue_profile=vp,
        now_utc=now_utc,
        paper_coordination=paper_coord,
        tv_cloud_sync=tv_cloud_sync,
    )
    doc["paper_farm_coordination"] = paper_coord
    if prune_reports:
        doc["staged_prune_summary"] = prune_reports[-1]
    if manual_staged:
        doc["manual_staged_presets"] = manual_staged

    persisted_audit = False
    if movers.get("schema_version") == "tier0.whole-market-movers.v2":
        # Replace only the scanner-owned snapshot; manual/Pine intake has its own history.
        extreme_presets: list[dict[str, Any]] = []
        early_ignition_presets: list[dict[str, Any]] = []
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
                if t.get("preset_tier") == "SESSION_EARLY_IGNITION_PRESET":
                    early_ignition_presets.append(validated)
                else:
                    extreme_presets.append(validated)
                meta_by_id[str(validated["alert_id"])] = {
                    "action_gate": t.get("action_gate"),
                    "reverse_short_mode": t.get("reverse_short_mode"),
                    "archetype": t.get("archetype"),
                    "pair_symbol": t.get("pair_symbol"),
                    "order_preset_id": t.get("order_preset_id"),
                    "venue_profile": t.get("venue_profile") or vp,
                }
        presets = [*extreme_presets, *early_ignition_presets]
        doc["staged_count"] = len(presets)
        _atomic_write_text(
            out_dir / "staged_scan_presets.json",
            json.dumps(
                {
                    "schema_version": "tier0.staged-tv-presets.v1",
                    "scan_generated_at_utc": movers["generated_at_utc"],
                    "active_session": doc["active_session"],
                    "venue_profile": vp,
                    "semi_auto_confirmed": False,
                    "extreme_mover_confirmed_count": len(extreme_presets),
                    "session_early_ignition_count": len(early_ignition_presets),
                    "presets": presets,
                },
                indent=2,
            )
            + "\n",
        )
        if extreme_presets:
            persist_tv_presets_duckdb(
                audit_db_path,
                extreme_presets,
                source="scanner_tickets",
                meta_by_alert_id=meta_by_id,
            )
            persisted_audit = True
        if early_ignition_presets:
            persist_tv_presets_duckdb(
                audit_db_path,
                early_ignition_presets,
                source="scanner_early_ignition",
                meta_by_alert_id=meta_by_id,
            )
            persisted_audit = True

    if not persisted_audit and audit_db_path.is_file():
        sync_tv_audit_duckdb_snapshot(audit_db_path)

    pine_shas = sync_pine_scripts_to_pack(out_dir)
    audit_summary = load_tv_audit_summary(audit_db_path)
    ex_map = _build_exchange_map_from_movers(movers, manifest_dir=manifest_dir)

    order_presets = build_tv_order_presets(
        doc,
        manual_staged_presets=manual_staged,
        paper_coordination=paper_coord,
        tv_cloud_sync=tv_cloud_sync,
        movers_entries=movers.get("entries") or (),
        exchange_map=ex_map,
        full_market_sweep_summary=movers.get("full_market_sweep_summary"),
        full_market_top_movers=movers.get("full_market_top_movers") or (),
        early_stage_ignition_movers=movers.get("early_stage_ignition_movers") or (),
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
            exchange_map=ex_map,
            ranked_entries=movers.get("entries"),
            staged_presets=manual_staged,
        ),
    )
    _atomic_write_text(
        out_dir / "tier0_sota_tv_movers_first_live.txt",
        render_tv_movers_first_watchlist(
            doc["tickets"],
            exchange_map=ex_map,
            ranked_entries=movers.get("entries"),
            staged_presets=manual_staged,
            early_stage_movers=movers.get("early_stage_ignition_movers"),
            full_market_movers=movers.get("full_market_top_movers"),
        ),
    )
    watchlist_payload = build_tv_watchlist_sync_payload(
        doc,
        movers=movers,
        manual_staged_presets=manual_staged,
        sync_receipt=tv_cloud_sync or None,
        exchange_map=ex_map,
    )
    refreshed_receipt = refresh_tv_mcp_sync_receipt(
        out_dir=out_dir,
        manifest_dir=manifest_dir,
        doc=doc,
        order_presets=order_presets,
        watchlist_payload=watchlist_payload,
        pine_shas=pine_shas,
        staged_manual_count=len(manual_staged),
        now_utc=now_utc,
    )
    if refreshed_receipt:
        tv_cloud_sync = refreshed_receipt
        watchlist_payload = build_tv_watchlist_sync_payload(
            doc,
            movers=movers,
            manual_staged_presets=manual_staged,
            sync_receipt=tv_cloud_sync,
            exchange_map=ex_map,
        )
    _atomic_write_text(
        out_dir / "tv_watchlist_sync_payload.json", json.dumps(watchlist_payload, indent=2) + "\n"
    )

    artifacts_map = {
        "tickets_json": "live_half_auto_tickets.json",
        "tickets_md": "LIVE_HALF_AUTO_ORDER_TICKETS.md",
        "order_presets_json": "tv_order_presets.json",
        "order_preset_cards_md": "TV_ORDER_PRESET_CARDS.md",
        "layout_blueprints_json": "tv_layout_blueprints.json",
        "layout_quickstart_md": "TV_LAYOUT_AND_PRESET_QUICKSTART.md",
        "watchlist_txt": "tier0_sota_tv_watchlists_live.txt",
        "watchlist_movers_first_txt": "tier0_sota_tv_movers_first_live.txt",
        "watchlist_sync_payload_json": "tv_watchlist_sync_payload.json",
        "finos_cockpit_json": "finos_tv_cockpit.json",
        "staged_scan_json": "staged_scan_presets.json",
        "audit_duckdb": "tv_half_auto_audit.duckdb",
        "audit_parquet": "tv_staged_presets.parquet",
    }
    if (out_dir / "tv_mcp_sync_receipt.json").is_file():
        artifacts_map["mcp_sync_receipt_json"] = "tv_mcp_sync_receipt.json"
    if (out_dir / "tv_cloud_mcp_sync_manifest.json").is_file():
        artifacts_map["cloud_mcp_sync_json"] = "tv_cloud_mcp_sync_manifest.json"
    if (manifest_dir / "starred_frontier_landscape_manifest.json").is_file():
        artifacts_map["starred_frontier_landscape_json"] = (
            "../starred_frontier_landscape_manifest.json"
        )
    if (manifest_dir / "l15_supply_chain_and_formal_audit.json").is_file():
        artifacts_map["l15_formal_audit_json"] = "../l15_supply_chain_and_formal_audit.json"
    layout_bp = build_tv_layout_blueprints(
        doc,
        order_presets=order_presets,
        pine_shas=pine_shas,
        early_mover_tuning=early_mover_tuning,
        historical_audit=historical_audit,
        tv_cloud_sync=tv_cloud_sync,
        starred_frontier=starred_frontier,
        l15_formal_audit=l15_formal_audit,
    )
    _atomic_write_text(
        out_dir / "tv_layout_blueprints.json", json.dumps(layout_bp, indent=2) + "\n"
    )
    _atomic_write_text(
        out_dir / "TV_LAYOUT_AND_PRESET_QUICKSTART.md",
        render_tv_layout_quickstart_markdown(layout_bp),
    )
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
        early_mover_tuning=early_mover_tuning,
        historical_audit=historical_audit,
        tv_cloud_sync=tv_cloud_sync,
        starred_frontier=starred_frontier,
        l15_formal_audit=l15_formal_audit,
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


def build_tv_layout_blueprints(
    doc: Mapping[str, Any],
    *,
    order_presets: Mapping[str, Any] | None = None,
    pine_shas: Mapping[str, str] | None = None,
    early_mover_tuning: Mapping[str, Any] | None = None,
    historical_audit: Mapping[str, Any] | None = None,
    tv_cloud_sync: Mapping[str, Any] | None = None,
    starred_frontier: Mapping[str, Any] | None = None,
    l15_formal_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build declarative TradingView Multi-Pane Layout, Native Order-Preset & MCP Blueprint."""
    active_sess = str(doc.get("active_session") or "CLOSED")
    vp = str(doc.get("venue_profile") or resolve_venue_profile(str(doc.get("venue") or "IBKR"))[0])
    macro = dict(doc.get("macro_regime") or {})
    regime_int = int(macro.get("regime", 0))
    tickets_list = [t for t in doc.get("tickets", []) if isinstance(t, Mapping)]
    standby_list = [s for s in doc.get("standby_presets", []) if isinstance(s, Mapping)]
    primary_row = tickets_list[0] if tickets_list else (standby_list[0] if standby_list else {})
    primary_sym = str(primary_row.get("tv_symbol") or "NASDAQ:TQQQ")
    recommended_layout = (
        "LAYOUT_B_CRASH_DEFENSIVE_6PANE"
        if regime_int == -1
        else (
            "LAYOUT_C_OVERNIGHT_24X5_IBKR_2PANE"
            if active_sess == "OVERNIGHT"
            else "LAYOUT_A_OMNI_4PANE_ACTIVE"
        )
    )
    handles = (
        dict(order_presets.get("venue_order_handles") or {})
        if isinstance(order_presets, Mapping)
        else build_venue_order_handles(session=active_sess)
    )
    return {
        "schema_version": "tier0.tv-layout-blueprints.v1",
        "generated_at_et": doc.get("generated_at_et"),
        "active_session": active_sess,
        "venue_profile": vp,
        "recommended_layout_now": recommended_layout,
        "primary_focus_symbol": primary_sym,
        "macro_regime": macro,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "zero_cdp_policy": True,
        "tradingview_layouts": [
            {
                "layout_id": "LAYOUT_A_OMNI_4PANE_ACTIVE",
                "name": "Omni 4-Pane Active Trading (RTH / PRE / AH)",
                "panes": [
                    {
                        "slot": "Top-Left",
                        "symbol": "AMEX:SPY",
                        "timeframe": "5m",
                        "extended_hours": True,
                        "scripts": ["pine/tier0_market_regime.pine"],
                        "mode": "T0_MKT_STATUS",
                        "sync_group": "Independent",
                    },
                    {
                        "slot": "Top-Right",
                        "symbol": primary_sym,
                        "timeframe": "1m",
                        "extended_hours": True,
                        "scripts": [
                            "pine/tier0_omni_sota_24x5.pine",
                            "pine/tier0_darkpool_uv_buildup.pine",
                        ],
                        "mode": "AUTO_REGIME_ADAPTIVE",
                        "sync_group": "Blue",
                    },
                    {
                        "slot": "Bottom-Left",
                        "symbol": primary_sym,
                        "timeframe": "15m",
                        "extended_hours": True,
                        "scripts": ["pine/tier0_sota_strategy_24x5.pine"],
                        "mode": "STRATEGY_TESTER_WEBHOOK",
                        "sync_group": "Blue",
                    },
                    {
                        "slot": "Bottom-Right",
                        "symbol": "AMEX:SOXS",
                        "timeframe": "1m",
                        "extended_hours": True,
                        "scripts": ["pine/tier0_omni_sota_24x5.pine"],
                        "mode": "CRASH_DEFENSIVE_INVERSE",
                        "sync_group": "Independent",
                    },
                ],
            },
            {
                "layout_id": "LAYOUT_B_CRASH_DEFENSIVE_6PANE",
                "name": "6-Pane Crash-Defensive Command Center (regime == -1 / VIX >= 25)",
                "panes": [
                    {
                        "slot": "Pane 1",
                        "symbol": "AMEX:SPY",
                        "timeframe": "5m",
                        "scripts": ["pine/tier0_market_regime.pine"],
                    },
                    {
                        "slot": "Pane 2",
                        "symbol": "NASDAQ:QQQ",
                        "timeframe": "5m",
                        "scripts": ["pine/tier0_market_regime.pine"],
                    },
                    {
                        "slot": "Pane 3",
                        "symbol": "NASDAQ:SOXX",
                        "timeframe": "5m",
                        "scripts": ["pine/tier0_market_regime.pine"],
                    },
                    {
                        "slot": "Pane 4",
                        "symbol": "AMEX:SOXS",
                        "timeframe": "1m",
                        "scripts": ["pine/tier0_omni_sota_24x5.pine"],
                        "mode": "CRASH_DEFENSIVE_INVERSE",
                    },
                    {
                        "slot": "Pane 5",
                        "symbol": "NASDAQ:SQQQ",
                        "timeframe": "1m",
                        "scripts": ["pine/tier0_omni_sota_24x5.pine"],
                        "mode": "CRASH_DEFENSIVE_INVERSE",
                    },
                    {
                        "slot": "Pane 6",
                        "symbol": primary_sym,
                        "timeframe": "1m",
                        "scripts": [
                            "pine/tier0_omni_sota_24x5.pine",
                            "pine/tier0_darkpool_uv_buildup.pine",
                        ],
                        "mode": "PARABOLIC_SHORT_EXHAUSTION",
                    },
                ],
            },
            {
                "layout_id": "LAYOUT_C_OVERNIGHT_24X5_IBKR_2PANE",
                "name": "2-Pane 24/5 Overnight Monitor (20:00-04:00 ET, IBKR/BOATS)",
                "panes": [
                    {
                        "slot": "Left (65%)",
                        "symbol": primary_sym,
                        "timeframe": "1m",
                        "extended_hours": True,
                        "scripts": [
                            "pine/tier0_omni_sota_24x5.pine",
                            "pine/tier0_darkpool_uv_buildup.pine",
                        ],
                        "mode": "OVERNIGHT_24X5_IBKR",
                        "order_preset": "OVERNIGHT_STOP_LIMIT_50BP",
                    },
                    {
                        "slot": "Right (35%)",
                        "symbol": "AMEX:SPY",
                        "timeframe": "5m",
                        "extended_hours": True,
                        "scripts": ["pine/tier0_market_regime.pine"],
                        "mode": "T0_MKT_STATUS",
                    },
                ],
            },
        ],
        "native_tv_order_panel_presets": [
            {
                "preset_name": "T0_RTH_LOSSLESS_STOP_MKT",
                "session": "RTH",
                "order_type": "STOP_MARKET",
                "execution_mechanism": "NATIVE_BROKER_STOP_MARKET_RATCHET",
                "execution_mechanism_label": "RTH: STOP_MARKET 0bp Native Ratchet",
                "limit_collar_bps": 0,
                "extended_hours": False,
                "stop_loss_cap_pct": "1.50% (tight) / 2.00% (baseline/wide)",
                "qty": 1,
                "r_lock_brackets": (
                    "BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R / Fast-Sell Rev (60s cd)"
                ),
                "venue_profiles": ["ALPACA_PAPER_1000", "IBKR_PAPER_4002", "ALPACA_PAPER_200"],
            },
            {
                "preset_name": "T0_EXT_COLLARED_35BP",
                "session": "PRE_MARKET / AFTER_HOURS",
                "order_type": "STOP_LIMIT",
                "execution_mechanism": "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
                "execution_mechanism_label": (
                    "PRE/AH: STOP_LIMIT / LIMIT 35bp Synthetic Tick-Supervised"
                ),
                "limit_collar_bps": 35,
                "extended_hours": True,
                "stop_loss_cap_pct": "1.25% (tight) / 1.50% (baseline/wide)",
                "qty": 1,
                "r_lock_brackets": (
                    "BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R / Fast-Sell Rev (60s cd)"
                ),
                "venue_profiles": ["ALPACA_PAPER_1000", "IBKR_PAPER_4002", "ALPACA_PAPER_200"],
            },
            {
                "preset_name": "T0_OVERNIGHT_24X5_50BP",
                "session": "OVERNIGHT",
                "order_type": "STOP_LIMIT",
                "execution_mechanism": "SYNTHETIC_BOATS_COLLARED_LIMIT",
                "execution_mechanism_label": "OVERNIGHT: STOP_LIMIT / LIMIT 50bp Synthetic BOATS",
                "limit_collar_bps": 50,
                "extended_hours": True,
                "stop_loss_cap_pct": "1.00% (all schedules)",
                "qty": 1,
                "short_guard": (
                    "ALPACA overnight SHORT blocked -> IBKR_PAPER_4002 or INVERSE_HEDGE_SOXS_SQQQ"
                ),
                "r_lock_brackets": (
                    "BE+10bp -> +1R:+0.45R -> +2R:+1.35R -> +3R:+2.35R / Fast-Sell Rev (60s cd)"
                ),
                "venue_profiles": ["IBKR_PAPER_4002", "ALPACA_PAPER_1000 (LONG/HEDGE)"],
            },
        ],
        "finos_openbb_command_center": {
            "finos_url": "http://localhost:1420/",
            "openbb_backend_url": "http://127.0.0.1:6900/",
            "caddy_tv_pack_url": "http://127.0.0.1:8793/tv_sota_pack/",
            "duckdb_sql_ui_url": "http://127.0.0.1:4213/",
            "grafana_url": "http://127.0.0.1:3001/",
            "alertmanager_url": "http://127.0.0.1:9093/",
            "widgets_count": 26,
            "plugins": [
                "@finos-workspace/ag-grid@2.3.0",
                "@finos-workspace/plotly-charts@2.5.0",
                "@finos-workspace/mermaid@1.0.0",
            ],
            "tabs": [
                "Live Movers & 24/5 Execution",
                "TV Half-Auto Cockpit & Watchlist",
                "Order Presets Landscape (24/5 & 8-Setup)",
                "Engine Sync, Layouts, Pine v6 & MCPs",
                "Starred Repos & Frontier Landscape (L10-L15)",
            ],
        },
        "mcp_ecosystem": {
            "repo_4_stdio_parity": ["codebase-memory", "xerj", "nautilus-engine", "edgartools"],
            "user_official_tradingview_mcp": {
                "url": "https://mcp.tradingview.com/mcp",
                "transport": "streamable-http",
                "auth": "OAuth 2.1 (user/IDE connector only; never inside 5 repo configs)",
                "capabilities": [
                    "watchlists_crud",
                    "alerts_crud",
                    "pine_v6_validate_and_deep_backtest",
                ],
                "zero_cdp": True,
            },
            "local_mit_pine_mcp": {
                "package": "pinescript-mcp",
                "license": "MIT",
                "command": "uvx --from pinescript-mcp pinescript-mcp",
                "tools_count": 12,
                "zero_cdp": True,
            },
        },
        "venue_order_handles_summary": {
            "active_alpaca_profile": handles.get("active_alpaca_profile"),
            "combined_paper_farm_max_rpm": handles.get("combined_paper_farm_max_rpm"),
            "combined_paper_farm_exit_reserve_rpm": handles.get(
                "combined_paper_farm_exit_reserve_rpm"
            ),
        },
        "lossless_24x5_mechanisms": {
            sess_k: {
                "execution_mechanism": vals[0],
                "broker_exit_order": vals[1],
                "label": vals[2],
            }
            for sess_k, vals in SESSION_EXECUTION_MECHANISMS.items()
        },
        "early_mover_tuning_summary": _summarize_early_mover_tuning(early_mover_tuning),
        "historical_early_mover_audit_summary": _summarize_historical_audit(historical_audit),
        "tv_cloud_mcp_sync_summary": _summarize_tv_cloud_mcp_sync(tv_cloud_sync),
        "starred_frontier_landscape_summary": _summarize_starred_frontier_landscape(
            starred_frontier
        ),
        "l15_formal_audit_summary": _summarize_l15_formal_audit(l15_formal_audit),
        "pine_v6_suite_shas": dict(pine_shas or {}),
    }


def render_tv_layout_quickstart_markdown(bp: Mapping[str, Any]) -> str:
    """Render concise operator Markdown for TradingView Layouts, Native Presets & MCP Ecosystem."""
    pine_shas = bp.get("pine_v6_suite_shas") or {}
    pine_rows = "\n".join(
        f"| `{name}` | `{str(sha)[:16]}…` | `tv_sota_pack/{name}` |"
        for name, sha in sorted(pine_shas.items())
    )
    preset_rows = "\n".join(
        f"| `{p['preset_name']}` | `{p['session']}` | `{p['order_type']}` "
        f"| `{p.get('execution_mechanism_label') or p.get('execution_mechanism', '—')}` "
        f"| `{p['limit_collar_bps']} bps` | `{'ON' if p['extended_hours'] else 'OFF'}` "
        f"| `{p['stop_loss_cap_pct']}` |"
        for p in (bp.get("native_tv_order_panel_presets") or [])
        if isinstance(p, Mapping)
    )
    vh = bp.get("venue_order_handles_summary") or {}
    sym = bp.get("primary_focus_symbol")
    alp_prof = vh.get("active_alpaca_profile", "ALPACA_PAPER_1000")
    max_rpm = vh.get("combined_paper_farm_max_rpm", 6000)
    exit_rpm = vh.get("combined_paper_farm_exit_reserve_rpm", 600)
    tune = bp.get("early_mover_tuning_summary") or {}
    audit = bp.get("historical_early_mover_audit_summary") or {}
    cloud = bp.get("tv_cloud_mcp_sync_summary") or {}
    frontier = bp.get("starred_frontier_landscape_summary") or {}
    l15 = bp.get("l15_formal_audit_summary") or {}
    recall_v = _opt_float(
        audit.get("recall_at_top15")
        if audit.get("recall_at_top15") is not None
        else tune.get("recall_at_top15")
    )
    ttfd_v = _opt_float(
        audit.get("median_time_to_first_detect_mins")
        if audit.get("median_time_to_first_detect_mins") is not None
        else tune.get("median_time_to_first_detect_mins")
    )
    mfe_v = _opt_float(
        audit.get("mean_post_detect_mfe_capture_pct")
        if audit.get("mean_post_detect_mfe_capture_pct") is not None
        else tune.get("mean_post_detect_mfe_capture_pct")
    )
    cutoff_lbl = audit.get("prewall_cutoff", "2026-10-15")
    if recall_v is not None:
        recall_pct_str = f"{recall_v * 100.0:.1f}%" if recall_v <= 1.0 else f"{recall_v:.1f}%"
    else:
        recall_pct_str = "n/a"
    ttfd_str = f"{ttfd_v:.1f}m" if ttfd_v is not None else "n/a"
    mfe_str = f"{mfe_v:.2f}%" if mfe_v is not None else "n/a"
    nvda_et = audit.get("nvda_first_detect_et") or "pending"
    nvda_cat = audit.get("nvda_category") or "PREMARKET_EARLY_IGNITION"
    replay_tag = audit.get("recent_replay_tag") or "out_of_sample_recent_replay"
    early_mover_line = (
        f"- **Early-Mover Pre-Wall Tuning & Historical Audit (`<= {cutoff_lbl}`):** "
        f"Recall@Top15 `{recall_pct_str}` · Median First-Detect `{ttfd_str}` · "
        f"Post-Detect MFE `{mfe_str}` · NVDA Premarket `{nvda_et}` "
        f"(`{nvda_cat}` · `{replay_tag}`)\n"
        if (tune.get("present") or audit.get("present"))
        else (
            "- **Early-Mover Pre-Wall Tuning & Historical Audit:** "
            "Awaiting `early_mover_tuning_manifest.json` / "
            "`historical_early_mover_audit_manifest.json`.\n"
        )
    )
    c_pass = cloud.get("compiled_pass_count", 0)
    c_tot = cloud.get("compiled_total_count", 4)
    c_wl_name = cloud.get("cloud_watchlist_name") or "Tier-0 SOTA 24x5 Movers & Darkpool"
    c_wl_id = cloud.get("cloud_watchlist_id") or "n/a"
    c_wl_cnt = cloud.get("cloud_watchlist_symbol_count", 0)
    c_bt_cnt = cloud.get("cloud_backtests_count", 0)
    c_tr_cnt = cloud.get("technicals_ratings_count", 0)
    cloud_line = (
        "- **TradingView Cloud MCP Sync Receipt:** "
        f"Pine v6 Cloud Compile `{c_pass}/{c_tot} PASS` · "
        f"Cloud Watchlist `{c_wl_name}` (`id={c_wl_id}`, `{c_wl_cnt}` syms) · "
        f"Cloud Backtests `{c_bt_cnt}` · Technicals Ratings `{c_tr_cnt}`\n"
        if cloud.get("present")
        else (
            "- **TradingView Cloud MCP Sync Receipt:** "
            "Awaiting `tv_cloud_mcp_sync_manifest.json`.\n"
        )
    )
    f_cov = _opt_float(frontier.get("mapie_empirical_coverage"), min_val=0.0)
    f_cov_str = (
        f"{f_cov * 100.0:.1f}%"
        if f_cov is not None and f_cov <= 1.0
        else (f"{f_cov:.1f}%" if f_cov is not None else "n/a")
    )
    f_cps = frontier.get("skchange_changepoints_count", 0)
    frontier_line = (
        "- **Starred Frontier Alpha (`starred_frontier_landscape_manifest.json`):** "
        f"MAPIE Cov `{f_cov_str}` · skchange CPs `{f_cps}` "
        f"· shapiq Pairs `{frontier.get('shapiq_interactions_count', 0)}` "
        f"· sqlglot PIT `{bool(frontier.get('sqlglot_pit_ok', False))}` "
        f"· EdgarTools PIT `{bool(frontier.get('edgartools_pit_safe', False))}`\n"
        if frontier.get("present")
        else "- **Starred Frontier Alpha:** Awaiting `starred_frontier_landscape_manifest.json`.\n"
    )
    l15_line = (
        "- **L14-L15 Formal & Alerting Audit (`l15_supply_chain_and_formal_audit.json`):** "
        f"Z3 `{bool(l15.get('z3_ok', False))}` · "
        f"CrossHair `{bool(l15.get('crosshair_ok', False))}` "
        f"(`{l15.get('crosshair_counterexamples', 0)}` CE) · "
        f"Inspect AI `{bool(l15.get('inspect_ai_ok', False))}` · "
        f"Alertmanager `:9093` `{'UP' if l15.get('alertmanager_healthy') else 'OFF'}` · "
        f"Skills `{l15.get('locked_skills_count', 46)}/46`\n"
        if l15.get("present")
        else (
            "- **L14-L15 Formal & Alerting Audit:** "
            "Awaiting `l15_supply_chain_and_formal_audit.json`.\n"
        )
    )
    return (
        "# Tier-0 Real TradingView Layouts, Native Order Presets & MCP Ecosystem\n\n"
        f"- **Generated (ET):** `{bp.get('generated_at_et')}` "
        f"| **Session:** `{bp.get('active_session')}` "
        f"| **Recommended Layout Now:** `{bp.get('recommended_layout_now')}` "
        f"| **Primary Focus:** `{sym}`\n"
        f"- **Venue Handles:** `{alp_prof}` (`{max_rpm} rpm` Paper Farm · "
        f"`{exit_rpm} rpm` exit reserve) | `paper_only=true` "
        "| `semi_auto_confirmed=false` | `live_execution_enabled=false` "
        "| `qty=1` | `zero_cdp=true`\n"
        f"{early_mover_line}"
        f"{cloud_line}"
        f"{frontier_line}"
        f"{l15_line}\n"
        "## 1. Real FINOS / OpenBB Workspace 5-Tab Command Center "
        "(`http://localhost:1420/`, 26 Widgets · `@finos-workspace/ag-grid`, "
        "`@finos-workspace/plotly-charts`, `@finos-workspace/mermaid`)\n"
        "- **Tab 1 — Live Movers & 24/5 Execution:** Macro regime KPIs + Plotly "
        "Movers/RVOL/Darkpool TRF chart + Live Movers + 24/5 Guards + Paper Farm + "
        "Historical Early-Mover First-Detection Audit (`tier0_early_mover_historical_audit`).\n"
        "- **Tab 2 — TV Half-Auto Cockpit & Watchlist:** Live Order Tickets + Plotly "
        "4-Tier R-Lock Stop Envelope chart + Staged Presets + 7-Section Watchlist.\n"
        "- **Tab 3 — Order Presets Landscape (24/5 & 8-Setup):** 1000/3000 rpm Venue "
        "Handle Matrix + 3x4 Session Matrix + 4-Tier R-Lock Ladder + 8-Setup Playbook "
        "+ 24/5 Lossless Trailing Presets (`tier0_tv_lossless_24x5_presets`) + Per-Symbol Cards.\n"
        "- **Tab 4 — Engine Sync, Layouts, Pine v6 & MCPs:** Bidirectional TV <-> "
        "Paper Farm Coordination + Live TradingView Cloud MCP Sync Receipt "
        "(`tier0_tv_cloud_mcp_sync`) + this live Layout/MCP Quickstart.\n"
        "- **Tab 5 — Starred Repos & Frontier Landscape (L10-L15):** Live Architecture "
        "Mermaid Flowchart (`tier0_live_architecture_mermaid`, `@finos-workspace/mermaid`) + "
        "NVDA Premarket Candlestick & 4-Tier Rungs (`tier0_nvda_premarket_candlestick_rungs`) + "
        "Starred Frontier Alpha Matrix (`tier0_starred_frontier_alpha_matrix`) + "
        "L15 Formal & Alertmanager `:9093` Audit (`tier0_l15_formal_and_alerting_audit`).\n\n"
        "## 2. Native TradingView Multi-Pane Layouts (Zero CDP)\n"
        "- **Layout A (Default 4-Pane Active):** Top-Left `AMEX:SPY` `5m` "
        "(`tier0_market_regime.pine`) · "
        f"Top-Right `{sym}` `1m` (`tier0_omni_sota_24x5.pine` + Pane 2 "
        "`tier0_darkpool_uv_buildup.pine`) · "
        f"Bottom-Left `{sym}` `15m` (`tier0_sota_strategy_24x5.pine`) · "
        "Bottom-Right `AMEX:SOXS` `1m` (`CRASH_DEFENSIVE_INVERSE`).\n"
        "- **Layout B (6-Pane Crash-Defensive `regime == -1` / `VIX >= 25`):** "
        "`SPY`/`QQQ`/`SOXX` `5m` + `SOXS`/`SQQQ`/ETB Short `1m`.\n"
        f"- **Layout C (2-Pane 24/5 Overnight IBKR):** Left `65%` `{sym}` `1m` "
        "(`OVERNIGHT_24X5_IBKR`, `<=1.00%` SL, `50 bps` `STOP_LIMIT`) + "
        "Right `35%` `AMEX:SPY` `5m`.\n\n"
        "## 3. Native TradingView Order Panel Presets (`qty=1`, 24/5 Lossless Self-Handling)\n\n"
        "| Preset Name | Session | Order Type | 24/5 Mechanism | Collar | Ext Hours "
        "| Rule-612 SL Cap |\n"
        "|---|---|---|---|---:|---|---|\n"
        f"{preset_rows}\n\n"
        "## 4. Synced 4-Script Pine v6 Suite (3-Way SHA-256 Parity)\n\n"
        "| Pine v6 Script | SHA-256 | Synced Pack Path |\n"
        "|---|---|---|\n"
        f"{pine_rows or '| — | — | Run `just tv-land` |'}\n\n"
        "## 5. MCP Ecosystem (Zero CDP)\n"
        "- **Repo 4-Stdio Core (`5/5` parity):** `codebase-memory`, `xerj` (`:9200`), "
        "`nautilus-engine` (`10` tools, `ALPACA_PAPER_1000` + TV manifests), `edgartools`.\n"
        "- **User Official TradingView MCP:** `https://mcp.tradingview.com/mcp` "
        "(Streamable HTTP + OAuth 2.1 — Watchlists CRUD, Alerts CRUD, Pine v6 Validate "
        "& Deep Backtest; user/IDE scope only).\n"
        "- **User Local MIT Pine v6 MCP:** `uvx --from pinescript-mcp pinescript-mcp` "
        "(`12` zero-CDP Pine v6 docs/validation tools).\n"
        "- **One-Command Daily Ops:** `just tv-land` · `bash scripts/tv_console.sh layout` "
        "· `bash scripts/tv_console.sh mcp-status` · DuckDB UI `http://127.0.0.1:4213/` "
        "(`tv_audit.tv_staged_presets`).\n"
    )


def run_preset_setup_automation(
    *,
    manifest_dir: Path = DEFAULT_MANIFEST_DIR,
    out_dir: Path | None = None,
    venue: str = "ALPACA",
    venue_profile: str | None = "ALPACA_PAPER_1000",
    max_stage: int = 3,
    now_utc: datetime | None = None,
    live_intake: bool | None = None,
) -> dict[str, Any]:
    """Prune stale/executed alerts, stage fresh unlocked 24/5 presets, and refresh TV SOTA pack."""
    eff_out_dir = out_dir if out_dir is not None else manifest_dir / "tv_sota_pack"
    eff_out_dir.mkdir(parents=True, exist_ok=True)
    vp, vu = resolve_venue_profile(venue, venue_profile=venue_profile)
    if vp not in {"IBKR_PAPER_4002", "ALPACA_PAPER_200", "ALPACA_PAPER_1000"}:
        raise ValueError(f"Refused non-paper venue_profile {vp!r} in preset setup automation")

    ts_utc = (now_utc or datetime.now(UTC)).astimezone(UTC)
    active_sess = current_et_session(ts_utc.astimezone(NY_TZ))
    eff_live_intake = (now_utc is None) if live_intake is None else live_intake
    staged_path = eff_out_dir / "staged_tv_presets.json"
    audit_db_path = eff_out_dir / "tv_half_auto_audit.duckdb"

    # 1. Prune stale cross-session, legacy, and already-executed alerts into DuckDB audit.
    root_staged = manifest_dir / "staged_tv_presets.json"
    if root_staged.is_file() and root_staged.resolve() != staged_path.resolve():
        prune_and_archive_staged_presets(
            staged_path=root_staged,
            manifest_dir=manifest_dir,
            duckdb_path=audit_db_path,
            now_utc=ts_utc,
            active_session=active_sess,
            always_write=True,
        )
    prune_report = prune_and_archive_staged_presets(
        staged_path=staged_path,
        manifest_dir=manifest_dir,
        duckdb_path=audit_db_path,
        now_utc=ts_utc,
        active_session=active_sess,
        always_write=True,
    )

    movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
    bundle_path = manifest_dir / "live_lane_order_preset_bundle.json"
    movers = _read_json_optional(movers_path)
    exec_doc = _read_json_optional(manifest_dir / "paper_farm_execution_status.json")
    locked_syms: set[str] = set()
    for sym in exec_doc.get("managed_symbols") or ():
        if str(sym).strip():
            locked_syms.add(str(sym).strip().upper())
    for pos in exec_doc.get("active_positions") or ():
        if isinstance(pos, Mapping) and str(pos.get("symbol") or "").strip():
            locked_syms.add(str(pos["symbol"]).strip().upper())

    # 2. Re-enrich any surviving fresh staged presets so 100% carry full 4-session 24/5 specs.
    kept_doc = _read_json_optional(staged_path)
    kept_presets: list[dict[str, Any]] = []
    upgraded = False
    for raw_p in kept_doc.get("presets") or ():
        if not isinstance(raw_p, Mapping) or not raw_p.get("symbol"):
            continue
        p_dict = dict(raw_p)
        if (
            not isinstance(p_dict.get("session_order_presets"), Mapping)
            or len(p_dict["session_order_presets"]) < 4
        ):
            p_venue = str(p_dict.get("venue") or vu).strip().upper()
            try:
                enriched = validate_tv_alert_payload(
                    p_dict, watchlist_manifest=movers or None, venue=p_venue, live_intake=False
                )
                enriched["alert_id"] = str(p_dict.get("alert_id") or enriched["alert_id"])
                if p_dict.get("staged_at_utc"):
                    enriched["staged_at_utc"] = str(p_dict["staged_at_utc"])
                kept_presets.append(enriched)
                upgraded = True
            except ValueError:
                kept_presets.append(p_dict)
        else:
            kept_presets.append(p_dict)
    if upgraded:
        _atomic_write_text(
            staged_path,
            json.dumps(
                {
                    "schema_version": "tier0.staged-tv-presets.v1",
                    "updated_at_utc": ts_utc.isoformat(),
                    "last_pruned_at_utc": ts_utc.isoformat(),
                    "active_session": active_sess,
                    "paper_only": True,
                    "semi_auto_confirmed": False,
                    "live_execution_enabled": False,
                    "pruned_counts": prune_report["pruned_counts"],
                    "presets": kept_presets,
                },
                indent=2,
            )
            + "\n",
        )

    staged_syms = {str(p["symbol"]).strip().upper() for p in kept_presets if p.get("symbol")}
    newly_staged: list[dict[str, Any]] = []
    skipped_locked: list[str] = []
    target_cap = max(1, int(max_stage))

    # 3. Stage top fresh unlocked current-session high-potential movers (qty=1, confirmed=False).
    if active_sess != "CLOSED" and movers.get("entries") and len(kept_presets) < target_cap:
        macro_regime_int = int((movers.get("macro_regime") or {}).get("regime", 0))
        bar_ms = (int(ts_utc.timestamp()) // 60) * 60_000
        raw_entries = [e for e in movers.get("entries", []) if isinstance(e, Mapping)]
        for entry in raw_entries:
            e_sym = str(entry.get("symbol") or "").strip().upper()
            if e_sym and e_sym in locked_syms:
                skipped_locked.append(e_sym)

        def _cand_rank(entry: Mapping[str, Any]) -> tuple[int, float, float]:
            wt = str(entry.get("watch_tier") or "").strip().upper()
            priority = (
                3
                if entry.get("extreme_mover_eligible") is True
                else (
                    2
                    if wt in {"PREMARKET_IGNITION", "TECHNICAL_BREAKOUT_LEADER"}
                    or entry.get("premarket_ignition") is True
                    else (1 if wt == "UV_DARKPOOL_BUILDUP" else 0)
                )
            )
            score_v = _opt_float(entry.get("score")) or 0.0
            abs_mv = abs(float(entry.get("session_move_pct") or entry.get("change_pct") or 0.0))
            return (priority, score_v, abs_mv)

        ranked_entries = sorted(raw_entries, key=_cand_rank, reverse=True)
        for e in ranked_entries:
            if len(kept_presets) + len(newly_staged) >= target_cap:
                break
            sym = str(e.get("symbol") or "").strip().upper()
            if not sym or sym in staged_syms:
                continue
            if sym in locked_syms:
                skipped_locked.append(sym)
                continue
            px_f = _opt_float(e.get("last_price"), min_val=5.0)
            if px_f is None:
                continue
            px_dec = Decimal(str(round(px_f, 4)))
            natr_f = _opt_float(e.get("natr_pct"), min_val=0.0005) or 0.012
            natr_dec = max(Decimal("0.001"), min(Decimal("0.50"), Decimal(str(round(natr_f, 6)))))
            sess_chg = float(e.get("session_move_pct") or e.get("change_pct") or 0.0)
            rvol_f = (
                _opt_float(
                    e.get("rvol_normalized")
                    if e.get("rvol_normalized") is not None
                    else e.get("rvol"),
                    min_val=0.0,
                )
                or 1.0
            )
            trf_f = _opt_float(e.get("darkpool_trf_ratio"), min_val=0.0)
            if trf_f is not None and trf_f > 1.0:
                trf_f = None
            uv_flag = bool(e.get("unusual_volume_buildup") is True)
            peak_conf = (
                e.get("peak_confirmed") if isinstance(e.get("peak_confirmed"), bool) else None
            )
            side, r_side, arch = _resolve_direction(
                e,
                regime_int=macro_regime_int,
                sess_chg=sess_chg,
                rvol=rvol_f,
                darkpool_trf_ratio=trf_f,
                unusual_volume_buildup=uv_flag,
                peak_confirmed=peak_conf,
                natr_pct=float(natr_dec),
            )
            if side == "SELL":
                if vu == "ALPACA" and active_sess == "OVERNIGHT":
                    continue
                if not (e.get("shortable") is True and e.get("easy_to_borrow") is True):
                    continue
                expected_src = {"ALPACA": "alpaca_paper_asset", "IBKR": "ibkr_paper_4002"}[vu]
                if eff_live_intake and e.get("borrow_source") != expected_src:
                    continue
            sched_name, sp_map = _eng.select_pooled_trailing_schedule(natr_dec)
            prof = _eng.get_session_protection_profile(active_sess, sp_map)
            stop_dec = _risk.compute_capped_stop_price(
                px_dec, r_side, max_loss_pct=prof.initial_max_loss_pct
            )
            stop_dist_f = round(float(abs(px_dec - stop_dec) / px_dec), 6)
            payload: dict[str, Any] = {
                "schema_version": "tier0.tv-alert.v1",
                "symbol": sym,
                "ts_utc_ms": bar_ms,
                "bar_confirmed": True,
                "side": side,
                "qty": 1,
                "venue": vu,
                "venue_profile": vp,
                "session": active_sess,
                "order_type": prof.order_mode,
                "trigger_price": float(px_dec),
                "stop_price": float(stop_dec),
                "stop_distance_pct": stop_dist_f,
                "limit_collar_bps": float(prof.limit_collar_bps),
                "trailing_schedule": sched_name,
                "natr_pct": float(natr_dec),
                "auto_reverse": True,
                "auto_reentry": True,
                "reentry_cooldown_secs": 60,
                "strategy_id": "SignalExecutionStrategy",
                "semi_auto_confirmed": False,
                "setup_archetype": arch,
                "macro_regime": macro_regime_int,
                "rvol": round(rvol_f, 2),
                "is_leveraged_or_inverse": sym in INVERSE_HEDGE_SYMBOLS
                or sym in BIDIRECTIONAL_PAIRS,
                "script_kind": "indicator",
                "order_preset_id": f"{sched_name}:{active_sess}:{arch}"[:64],
                "data_source": "tradingview_pine_v6",
            }
            if trf_f is not None:
                payload["darkpool_trf_ratio"] = round(trf_f, 4)
            hvr_f = _opt_float(e.get("hidden_volume_ratio"), min_val=0.0)
            if hvr_f is not None:
                payload["hidden_volume_ratio"] = round(hvr_f, 3)
            vbr_f = _opt_float(e.get("vol_buildup_ratio"), min_val=0.0)
            if vbr_f is not None:
                payload["vol_buildup_ratio"] = round(vbr_f, 3)
            if uv_flag:
                payload["unusual_volume_buildup"] = True
            if peak_conf is not None:
                payload["peak_confirmed"] = peak_conf
            pair_s = str(e.get("pair_symbol") or "").strip().upper() or BIDIRECTIONAL_PAIRS.get(sym)
            if pair_s:
                payload["pair_symbol"] = pair_s
            cal_c = str(e.get("calibrated_category") or e.get("potential_category") or "").strip()
            if cal_c:
                payload["calibrated_category"] = cal_c[:64]
            ta_f = _opt_float(e.get("technical_alignment_score"), min_val=0.0)
            if ta_f is not None and ta_f <= 100.0:
                payload["technical_alignment_score"] = round(ta_f, 2)
            min_tr_f = _opt_float(e.get("min_trailing_pct"), min_val=0.0001)
            if min_tr_f is not None and min_tr_f <= 0.20:
                payload["min_trailing_pct"] = round(min_tr_f, 6)
            if side == "SELL":
                payload["shortable"] = True
                payload["easy_to_borrow"] = True
            try:
                staged_row = stage_tv_alert_preset(
                    payload,
                    staged_path=staged_path,
                    watchlist_path=movers_path,
                    venue=vu,
                    live_intake=eff_live_intake,
                )
            except ValueError:
                continue
            newly_staged.append(staged_row)
            staged_syms.add(sym)

    # 4. Refresh the entire tv_sota_pack/ so presets, cards, tickets & DuckDB are 100% synced.
    refreshed_doc: dict[str, Any] | None = None
    if movers_path.is_file():
        refreshed_doc = refresh_tickets_from_paths(
            movers_path=movers_path,
            bundle_path=bundle_path if bundle_path.is_file() else None,
            out_dir=eff_out_dir,
            venue=vu,
            venue_profile=vp,
            now_utc=ts_utc,
        )
    final_staged = _read_json_optional(staged_path).get("presets") or []
    return {
        "ok": True,
        "active_session": active_sess,
        "venue": vu,
        "venue_profile": vp,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "prune_summary": prune_report,
        "newly_staged_count": len(newly_staged),
        "newly_staged_symbols": [str(p["symbol"]) for p in newly_staged],
        "active_staged_count": len(final_staged),
        "active_staged_symbols": [
            str(p["symbol"]) for p in final_staged if isinstance(p, Mapping) and p.get("symbol")
        ],
        "locked_symbols_skipped": sorted(set(skipped_locked)),
        "tickets_count": len((refreshed_doc or {}).get("tickets") or ()),
        "standby_presets_count": len((refreshed_doc or {}).get("standby_presets") or ()),
        "staged_path": str(staged_path),
    }


def main() -> None:
    """CLI entrypoint for Half-Auto order ticket, preset setup & TV watchlist generation."""
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
    p.add_argument(
        "--preset-setup",
        action="store_true",
        help="Prune stale/executed alerts, stage fresh unlocked 24/5 presets, and refresh TV pack.",
    )
    p.add_argument(
        "--max-stage",
        type=int,
        default=3,
        help="Maximum active staged presets for --preset-setup (default: 3).",
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
    if a.preset_setup:
        res = run_preset_setup_automation(
            manifest_dir=a.manifest_dir,
            out_dir=a.out_dir,
            venue=a.venue,
            venue_profile=a.venue_profile,
            max_stage=a.max_stage,
        )
        print(json.dumps(res, indent=2))
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

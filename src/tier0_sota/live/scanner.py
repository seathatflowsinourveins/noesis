"""Whole-market live extreme-mover scanner (Alpaca SIP + FINRA TRF + ETB lookup)."""

from __future__ import annotations

import json
import math
import os
import re
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import alpaca.data as ad
import alpaca.trading as at
import polars as pl
from alpaca.common.enums import Sort
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed, Exchange, MostActivesBy, OptionsFeed
from alpaca.data.historical.corporate_actions import CorporateActionsClient
from alpaca.data.historical.news import NewsClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import (
    CorporateActionsRequest,
    MarketMoversRequest,
    MostActivesRequest,
    NewsRequest,
    OptionBarsRequest,
    OptionChainRequest,
    StockBarsRequest,
    StockSnapshotRequest,
    StockTradesRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.enums import AssetBorrowStatus, AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetOptionContractsRequest
from nautilus_trader.indicators import AverageTrueRange

from tier0_sota import discovery as disc
from tier0_sota import engine as eng
from tier0_sota import risk as rk

_NY_TZ = ZoneInfo("America/New_York")
DEFAULT_PIN_SYMBOLS: tuple[str, ...] = ("SOXL", "SOXS", "TQQQ", "SQQQ")
DEFAULT_EARLY_MOVER_TUNING_FILENAME = "early_mover_tuning_manifest.json"
EARLY_MOVER_TUNING_MAX_AGE_HOURS = 168.0
OVERNIGHT_SEED_SYMBOLS: tuple[str, ...] = (
    "NVDA",
    "TSLA",
    "AMD",
    "PLTR",
    "SMCI",
    "MSTR",
    "COIN",
    "AVGO",
    "META",
    "SPXS",
    "UPRO",
)
TECHNICAL_BREAKOUT_LEADER_SYMBOLS: tuple[str, ...] = (
    "NVDA",
    "AVGO",
    "AMD",
    "TSLA",
    "META",
    "PLTR",
    "SMCI",
    "MU",
    "MSTR",
    "COIN",
    "DELL",
    "MRVL",
    "SNDK",
    "AAPL",
    "MSFT",
    "AMZN",
    "GOOGL",
    "INTC",
    "CRWV",
)
INVERSE_ETF_ANNUAL_EXPENSE_RATES: dict[str, float] = {
    "SOXS": 0.0099,
    "SQQQ": 0.0095,
    "SPXS": 0.0101,
    "SDOW": 0.0095,
    "NVD": 0.0115,
    "NVDS": 0.0115,
}
CORE_REGIME_BENCHMARKS = ("SPY", "QQQ", "SOXX")
EQUITY_BENCHMARKS = ("SPY", "QQQ", "SOXX", "RSP")
PREWALL_PIT_WALL_DATE = "2026-10-15"
DEFAULT_HISTORICAL_CALIBRATION_MANIFEST = Path(
    "/mnt/z/sota-data/manifests/historical_extreme_mover_calibration_manifest.json"
)
DEFAULT_PREWALL_CALIBRATION_MANIFEST = Path(
    "/mnt/z/sota-data/manifests/prewall_early_mover_calibration_manifest.json"
)
DEFAULT_EARLY_MOVER_WEIGHTS: dict[str, float] = {
    "move_weight": 2.0,
    "rvol_weight": 10.0,
    "trf_weight": 8.0,
    "trf_block_weight": 6.0,
    "vol_buildup_weight": 4.0,
    "lit_iso_weight": 5.0,
    "breakout_leader_bonus": 8.0,
}
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_OCC_SYM_RE = re.compile(r"^[A-Z0-9.-]{1,6}\d{6}([CP])\d{8}$")
_CORP_ACTION_PRIORITY: tuple[str, ...] = (
    "REVERSE_SPLIT",
    "FORWARD_SPLIT",
    "UNIT_SPLIT",
    "CASH_MERGER",
    "STOCK_MERGER",
    "STOCK_AND_CASH_MERGER",
    "SPIN_OFF",
    "REDEMPTION",
    "WORTHLESS_REMOVAL",
    "RIGHTS_DISTRIBUTION",
    "STOCK_DIVIDEND",
    "CASH_DIVIDEND",
    "NAME_CHANGE",
)
_TV_EXCHANGE_MAP: dict[str, str] = {
    "NASDAQ": "NASDAQ",
    "NYSE": "NYSE",
    "AMEX": "AMEX",
    "ARCA": "AMEX",
    "NYSEARCA": "AMEX",
    "BATS": "BATS",
    "IEX": "IEX",
    "IEXG": "IEX",
    "OTC": "OTC",
}
VALID_EQUITY_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,6}$")
_NMS_ALLOWED_EXCHANGES: frozenset[str] = frozenset(
    {"NASDAQ", "NYSE", "AMEX", "ARCA", "NYSEARCA", "BATS", "IEX"}
)
_NMS_PURE_TICKER_RE = re.compile(r"^[A-Z]{1,5}$")
_WARRANT_SUFFIXES: tuple[str, ...] = (".WS", ".RT", ".U", ".PR")


def _is_warrant_or_right_symbol(sym: str) -> bool:
    """Reject penny warrants/rights/units (e.g. GGROW, SVAQW, NMZ.RT, SLND.WS)."""
    s = sym.strip().upper()
    if not s:
        return True
    if any(s.endswith(suf) for suf in _WARRANT_SUFFIXES):
        return True
    if not _NMS_PURE_TICKER_RE.match(s):
        return True
    return len(s) == 5 and s[-1] in {"W", "R", "U"}


def _estimate_session_elapsed_fraction(now_utc: datetime, sess: str) -> float:
    """Causal elapsed fraction of the active US session for time-of-day pace RVOL."""
    ny = now_utc.astimezone(_NY_TZ)
    mins = ny.hour * 60 + ny.minute + ny.second / 60.0
    if sess == "RTH":
        return max(0.05, min(1.0, (mins - 570.0) / 390.0))
    if sess == "PRE_MARKET":
        return max(0.10, min(1.0, (mins - 240.0) / 330.0))
    if sess == "AFTER_HOURS":
        return 1.0
    return 0.25


def _classify_mover_stage(daily_pct: float, tod_rvol: float) -> str:
    """Classify whole-market mover lifecycle stage (Early-Stage Ignition vs Expansion vs Climax)."""
    abs_chg = abs(daily_pct)
    if abs_chg >= 50.0:
        return "PARABOLIC_CLIMAX_SURGE"
    if abs_chg >= 25.0:
        return "ACTIVE_EXTREME_EXPANSION"
    if abs_chg >= 3.0 and tod_rvol >= 2.0:
        return "EARLY_STAGE_IGNITION"
    return "EARLY_STAGE_WATCH"


_UNVERIFIED = {
    "shortable": False,
    "easy_to_borrow": False,
    "borrow_source": None,
    "borrow_as_of_utc": None,
    "short_fee_bps": 30.0,
    "exchange": None,
    "has_options": False,
    "overnight_tradable": False,
}
_EMPTY_OPRA: dict[str, Any] = {
    "uoa_vol_oi_ratio": None,
    "uoa_call_put_ratio": None,
    "net_dealer_gex_usd": None,
    "iv_skew_25d": None,
    "uoa_active": False,
}
_EMPTY_CATALYST: dict[str, Any] = {
    "catalyst_news_24h_count": 0,
    "latest_catalyst_utc": None,
    "latest_catalyst_source": None,
    "corp_action_flag": None,
    "corp_action_ex_date": None,
}
_EMPTY_SEC_CATALYST: dict[str, Any] = {
    "sec_catalyst_score": 0.0,
    "sec_catalyst_summary": {
        "catalyst_score": 0.0,
        "recent_8k_count": 0,
        "recent_10q_10k_count": 0,
        "insider_form4_count": 0,
        "material_8k_count": 0,
        "accepted_filings_count": 0,
        "excluded_future_filings": 0,
        "latest_form": None,
        "latest_accepted_utc": None,
        "pit_safe": True,
        "source": "offline_empty_fail_closed",
    },
}


def _ga(obj: Any, key: str, default: Any = None) -> Any:
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def _f(val: Any, default: float = 0.0) -> float:
    try:
        out = float(val)
        return out if math.isfinite(out) else default
    except (TypeError, ValueError):
        return default


def _bar_session(bar: Any) -> str:
    ts = _ga(bar, "timestamp")
    if ts is None:
        return "CLOSED"
    return eng.classify_us_equity_session_et(int(ts.timestamp() * 1_000_000_000))


def _compute_same_session_rvol(
    bars: Sequence[Any], sess: str, *, early_fallback_rvol: float = 1.5
) -> float:
    """Compute RVOL strictly within the active 24/5 session (zero RTH/AH dilution)."""
    if sess == "CLOSED" or not bars:
        return 0.0
    sess_bars = [b for b in bars if _bar_session(b) == sess]
    if not sess_bars:
        return 0.0
    if len(sess_bars) >= 6:
        prior = sess_bars[:-5]
        recent = sess_bars[-5:]
        base_v = sum(_f(_ga(b, "volume")) for b in prior) / len(prior)
        rec_v = sum(_f(_ga(b, "volume")) for b in recent) / len(recent)
        return (rec_v / base_v) if base_v > 0 else 0.0
    if len(sess_bars) >= 2:
        prior = sess_bars[:-1]
        base_v = sum(_f(_ga(b, "volume")) for b in prior) / len(prior)
        rec_v = _f(_ga(sess_bars[-1], "volume"))
        return (rec_v / base_v) if base_v > 0 else 0.0
    return early_fallback_rvol if _f(_ga(sess_bars[0], "volume")) > 0 else 0.0


def _resolve_prior_close(bars: Sequence[Any], sess: str, fallback_px: float) -> float | None:
    """Resolve prior-session close from bars, falling back to first bar open or price."""
    prior_bars = [b for b in bars if _bar_session(b) != sess]
    if prior_bars:
        val = _f(_ga(prior_bars[-1], "close"), 0.0)
        if val > 0.0:
            return round(val, 4)
    if bars:
        first_open = _f(_ga(bars[0], "open"), 0.0)
        if first_open > 0.0:
            return round(first_open, 4)
    return round(fallback_px, 4) if fallback_px > 0.0 else None


def _compute_volume_buildup(bars: Sequence[Any], sess: str) -> tuple[float, int]:
    """Compute causal multi-bar same-session volume buildup ratio & consecutive tail ramp."""
    if sess == "CLOSED" or not bars:
        return 0.0, 0
    vols = [_f(_ga(b, "volume"), 0.0) for b in bars if _bar_session(b) == sess]
    if not vols:
        return 0.0, 0
    ramp = 0
    for i in range(len(vols) - 1, 0, -1):
        if vols[i] > vols[i - 1] and vols[i] > 0.0:
            ramp += 1
        else:
            break
    if len(vols) >= 4:
        recent = vols[-3:]
        prior = vols[max(0, len(vols) - 15) : len(vols) - 3]
        base = sum(prior) / len(prior)
        ratio = (sum(recent) / len(recent)) / base if base > 0.0 else 0.0
    elif len(vols) >= 2:
        base = sum(vols[:-1]) / (len(vols) - 1)
        ratio = vols[-1] / base if base > 0.0 else 0.0
    else:
        ratio = 0.0
    return round(ratio, 3), ramp


def _compute_hidden_volume_ratio(rvol: float, bars: Sequence[Any], atr_val: float | None) -> float:
    """Causal absorption proxy: RVOL divided by normalized bar-range / ATR."""
    if atr_val is None or not bars or rvol <= 0.0:
        return 0.0
    last = bars[-1]
    last_range = max(0.0, _f(_ga(last, "high")) - _f(_ga(last, "low")))
    return round(rvol / max(0.25, last_range / max(atr_val, 1e-6)), 3)


def _detect_causal_peak_confirmed(
    bars: Sequence[Any], sess: str, natr: Decimal | None, rvol: float
) -> bool:
    """Causal reversal peak: red breakdown < prior_low after >= 2*NATR extension above VWAP."""
    if sess == "CLOSED" or natr is None or float(natr) <= 0.0 or rvol < 2.0:
        return False
    sess_bars = [b for b in bars if _bar_session(b) == sess]
    if len(sess_bars) < 2:
        return False
    pv = 0.0
    v_sum = 0.0
    for bar in sess_bars[:-1]:
        bv = _f(_ga(bar, "volume"), 0.0)
        if bv <= 0.0:
            continue
        tp = (_f(_ga(bar, "high")) + _f(_ga(bar, "low")) + _f(_ga(bar, "close"))) / 3.0
        pv += tp * bv
        v_sum += bv
    if v_sum <= 0.0:
        return False
    vwap = pv / v_sum
    if vwap <= 0.0:
        return False
    prior, last = sess_bars[-2], sess_bars[-1]
    prior_ext_px = max(_f(_ga(prior, "high")), _f(_ga(prior, "close")))
    extended = (prior_ext_px - vwap) / vwap >= 2.0 * float(natr)
    last_open, last_close = _f(_ga(last, "open")), _f(_ga(last, "close"))
    prior_low = _f(_ga(prior, "low"))
    return bool(extended and last_close < last_open and last_close < prior_low)


def resolve_alpaca_env_creds(account: str = "ALPACA_PA1") -> tuple[str, str]:
    """Read Alpaca Paper credentials strictly from process env (never opens .env*)."""
    idx = account.removeprefix("ALPACA_PA")
    k_names = [f"ALPACA_PA{idx}_API_KEY"] + (["APCA_API_KEY_ID"] if idx == "1" else [])
    s_names = [f"ALPACA_PA{idx}_SECRET", f"ALPACA_PA{idx}_SECRET_KEY"] + (
        ["APCA_API_SECRET_KEY"] if idx == "1" else []
    )
    key = next((v for k in k_names if (v := os.environ.get(k, "").strip())), "")
    sec = next((v for s in s_names if (v := os.environ.get(s, "").strip())), "")
    if key.startswith("AK"):
        raise PermissionError(f"Refused potential Live AK key for {account}; Paper keys only.")
    return key, sec


def normalize_tv_exchange(raw_exchange: Any) -> str | None:
    """Normalize Alpaca Asset.exchange enum/string into a TradingView watchlist exchange prefix."""
    if raw_exchange is None:
        return None
    raw = str(_ga(raw_exchange, "value", raw_exchange) or "").strip().upper()
    if not raw:
        return None
    return _TV_EXCHANGE_MAP.get(raw)


def _infer_setup_archetype(
    *,
    mv: float,
    rvol: float,
    macro_regime: int,
    trf_val: float | None,
    unusual_volume_buildup: bool,
    hidden_volume_ratio: float,
    peak_confirmed: bool,
) -> str:
    if peak_confirmed:
        return "PEAK_REVERSAL"
    if macro_regime == -1:
        return "CRASH_ALIGNMENT"
    if abs(mv) >= 3.5 and rvol >= 3.0:
        return "EARLY_EP_MOMENTUM_BURST"
    if (
        (trf_val is not None and trf_val >= 0.35)
        or unusual_volume_buildup
        or hidden_volume_ratio >= 1.5
    ):
        return "HIDDEN_VOL_ACCUM"
    return "EXTREME_REVERSAL"


def _resolve_prior_sip_trf_window(now_utc: datetime) -> tuple[datetime, datetime]:
    """Return a causal 15-min continuous institutional SIP window before prior RTH close."""
    from tier0_sota.discovery import get_xnys_calendar

    ny_now = now_utc.astimezone(_NY_TZ)
    try:
        cal = get_xnys_calendar()
        cand_date = ny_now.date() if ny_now.hour >= 20 else ny_now.date() - timedelta(days=1)
        prev_sess = cal.date_to_session(cand_date.isoformat(), direction="previous")
        rth_close_utc = cal.session_close(prev_sess).to_pydatetime().astimezone(UTC)
        if rth_close_utc >= now_utc:
            prev_sess = cal.previous_session(prev_sess)
            rth_close_utc = cal.session_close(prev_sess).to_pydatetime().astimezone(UTC)
        # 15:40-15:55 ET avoids the 15:59-16:00 lit closing-cross flood while staying causal.
        end_utc = rth_close_utc - timedelta(minutes=5)
        start_utc = rth_close_utc - timedelta(minutes=20)
        return start_utc, end_utc
    except (ValueError, KeyError, TypeError, AttributeError, RuntimeError):
        fallback_end = ny_now.replace(hour=15, minute=55, second=0, microsecond=0).astimezone(UTC)
        if fallback_end >= now_utc:
            fallback_end -= timedelta(days=1)
        return fallback_end - timedelta(minutes=15), fallback_end


def _parse_trade_conditions(raw_conditions: Any) -> set[str]:
    if raw_conditions is None:
        return set()
    if isinstance(raw_conditions, str):
        return {c.strip().upper() for c in raw_conditions if c.strip()}
    out: set[str] = set()
    try:
        for item in raw_conditions:
            val = str(item or "").strip().upper()
            if val:
                out.add(val)
    except TypeError:
        val = str(raw_conditions).strip().upper()
        if val:
            out.add(val)
    return out


def _compute_ssr_active(
    bars: Sequence[Any], sess: str, prior_close: float | None, current_move_pct: float
) -> bool:
    """Compute live Reg SHO Rule 201 SSR flag (<= -10.0% drop vs prior_close)."""
    if current_move_pct <= -10.0:
        return True
    if prior_close is None or prior_close <= 0.0 or not bars:
        return False
    sess_bars = [b for b in bars if _bar_session(b) == sess] if sess != "CLOSED" else []
    target = sess_bars or list(bars)
    lows = [lo for b in target if (lo := _f(_ga(b, "low"), _f(_ga(b, "close"), 0.0))) > 0.0]
    if not lows:
        return False
    return (((min(lows) / prior_close) - 1.0) * 100.0) <= -10.0


def _detect_technical_breakout_leader(
    *,
    symbol: str,
    bars: Sequence[Any],
    sess: str,
    mv: float,
    rvol: float,
    vol_buildup_ratio: float,
    atr_val: float | None,
    dollar_vol: float,
    min_dollar_vol: float,
) -> bool:
    """Detect early causal VWAP reclaim + breakout expansion in liquid technical leaders."""
    if (
        sess == "CLOSED"
        or symbol not in TECHNICAL_BREAKOUT_LEADER_SYMBOLS
        or dollar_vol < min_dollar_vol
        or abs(mv) < 1.25
        or (rvol < 1.4 and vol_buildup_ratio < 1.5)
    ):
        return False
    sess_bars = [b for b in bars if _bar_session(b) == sess]
    if len(sess_bars) < 3:
        return False
    pv = 0.0
    v_sum = 0.0
    for bar in sess_bars:
        bv = _f(_ga(bar, "volume"), 0.0)
        if bv <= 0.0:
            continue
        tp = (_f(_ga(bar, "high")) + _f(_ga(bar, "low")) + _f(_ga(bar, "close"))) / 3.0
        pv += tp * bv
        v_sum += bv
    if v_sum <= 0.0:
        return False
    vwap = pv / v_sum
    if vwap <= 0.0:
        return False
    prior_bars = sess_bars[:-1]
    last = sess_bars[-1]
    last_close = _f(_ga(last, "close"), 0.0)
    last_open = _f(_ga(last, "open"), 0.0)
    last_range = max(0.0, _f(_ga(last, "high")) - _f(_ga(last, "low")))
    or_slice = sess_bars[: min(5, len(prior_bars))]
    or_high = max(_f(_ga(b, "high")) for b in or_slice)
    or_low = min(_f(_ga(b, "low")) for b in or_slice)
    prior_high = max(_f(_ga(b, "high")) for b in prior_bars[-5:])
    prior_low = min(_f(_ga(b, "low")) for b in prior_bars[-5:])
    range_exp = atr_val is not None and atr_val > 0.0 and last_range >= 1.15 * atr_val
    if mv > 0.0:
        return bool(
            last_close > vwap
            and last_close >= last_open
            and (last_close >= prior_high or last_close > or_high or range_exp)
        )
    return bool(
        last_close < vwap
        and last_close <= last_open
        and (last_close <= prior_low or last_close < or_low or range_exp)
    )


def _classify_potential_category(
    *,
    symbol: str,
    is_ext: bool,
    is_tech_breakout_leader: bool,
    vol_buildup_ratio: float,
    rvol_normalized: float,
    lit_iso_sweep_ratio: float | None,
    trf_val: float | None,
    trf_block_ratio: float | None,
    hidden_volume_ratio: float,
    uoa_active: bool,
) -> str:
    """Classify each ranked mover into a deterministic early-find potential category."""
    if is_ext:
        return "EXTREME_READY"
    if symbol in INVERSE_ETF_ANNUAL_EXPENSE_RATES:
        return "CRASH_HEDGE_INVERSE"
    if (
        vol_buildup_ratio >= 2.0
        or rvol_normalized >= 2.5
        or (lit_iso_sweep_ratio is not None and lit_iso_sweep_ratio >= 0.25)
    ):
        return "SUDDEN_VOLUME_SURGE"
    if (
        uoa_active
        or (trf_val is not None and trf_val >= 0.35)
        or (trf_block_ratio is not None and trf_block_ratio >= 0.15)
        or hidden_volume_ratio >= 1.5
    ):
        return "UV_DARKPOOL_OPRA_BUILDUP"
    if is_tech_breakout_leader:
        return "TECHNICAL_BREAKOUT_LEADER"
    return "PINNED_STANDBY"


def _classify_calibrated_category(
    *,
    symbol: str,
    session: str,
    is_ext: bool,
    is_tech_breakout_leader: bool,
    vol_buildup_ratio: float,
    rvol_normalized: float,
    lit_iso_sweep_ratio: float | None,
    trf_val: float | None,
    trf_block_ratio: float | None,
    hidden_volume_ratio: float,
    uoa_active: bool,
    peak_confirmed: bool,
    macro_regime: int,
    catalyst_news_count: int = 0,
) -> str:
    """Classify mover into the 5-category calibrated early-detection taxonomy."""
    if peak_confirmed or (macro_regime == -1 and symbol in INVERSE_ETF_ANNUAL_EXPENSE_RATES):
        return "PARABOLIC_CLIMAX_SHORT_REVERSAL"
    if session == "PRE_MARKET" and (
        catalyst_news_count > 0 or is_ext or is_tech_breakout_leader or rvol_normalized >= 1.5
    ):
        return "PREMARKET_CATALYST_LEADER"
    if (
        vol_buildup_ratio >= 2.0
        or rvol_normalized >= 2.5
        or (lit_iso_sweep_ratio is not None and lit_iso_sweep_ratio >= 0.25)
    ):
        return "SUDDEN_VOLUME_IGNITION"
    if (
        uoa_active
        or is_tech_breakout_leader
        or (trf_val is not None and trf_val >= 0.35)
        or (trf_block_ratio is not None and trf_block_ratio >= 0.15)
        or hidden_volume_ratio >= 1.5
    ):
        return "DARKPOOL_ACCUMULATION_BREAKOUT"
    return "EXTREME_MOVER_CONTINUATION"


def _compute_symbol_technicals(
    bars: Sequence[Any], sess: str, px: float, atr_val: float | None, natr: Decimal | None
) -> dict[str, Any]:
    """Compute causal same-session VWAP, high/low, ATR/NATR, and range-expansion technicals."""
    sess_bars = [b for b in bars if _bar_session(b) == sess] if sess != "CLOSED" else []
    target = sess_bars or list(bars)
    pv = 0.0
    v_sum = 0.0
    highs: list[float] = []
    lows: list[float] = []
    for bar in target:
        hi = _f(_ga(bar, "high"), 0.0)
        lo = _f(_ga(bar, "low"), 0.0)
        cl = _f(_ga(bar, "close"), 0.0)
        bv = _f(_ga(bar, "volume"), 0.0)
        if hi > 0.0:
            highs.append(hi)
        if lo > 0.0:
            lows.append(lo)
        if bv > 0.0 and cl > 0.0:
            tp = ((hi if hi > 0.0 else cl) + (lo if lo > 0.0 else cl) + cl) / 3.0
            pv += tp * bv
            v_sum += bv
    vwap = round(pv / v_sum, 4) if v_sum > 0.0 else None
    vwap_dist_pct = (
        round(((px / vwap) - 1.0) * 100.0, 3)
        if vwap is not None and vwap > 0.0 and px > 0.0
        else None
    )
    last_range = (
        max(0.0, _f(_ga(target[-1], "high"), 0.0) - _f(_ga(target[-1], "low"), 0.0))
        if target
        else 0.0
    )
    range_exp = round(last_range / atr_val, 3) if atr_val is not None and atr_val > 0.0 else None
    return {
        "vwap": vwap,
        "vwap_dist_pct": vwap_dist_pct,
        "above_vwap": bool(vwap is not None and px > vwap),
        "atr_14": round(atr_val, 6) if atr_val is not None else None,
        "natr_pct": float(natr) if natr is not None else None,
        "session_high": round(max(highs), 4) if highs else None,
        "session_low": round(min(lows), 4) if lows else None,
        "range_expansion_ratio": range_exp,
    }


def _extract_symbol_min_trails(doc: Mapping[str, Any]) -> dict[str, float]:
    """Extract PIT-calibrated per-symbol min_trailing_pct clamped strictly <= risk.py 2.00%."""
    max_trail = float(rk.MAX_STOP_LOSS_PCT)
    out: dict[str, float] = {}
    for key in (
        "per_symbol_min_trailing_pct",
        "symbol_min_trailing_pct",
        "min_trailing_pct_by_symbol",
    ):
        raw_map = doc.get(key)
        if isinstance(raw_map, Mapping):
            for sym, val in raw_map.items():
                sym_u = str(sym or "").strip().upper()
                v = _f(val, -1.0)
                if sym_u and 0.0 < v <= max_trail:
                    out[sym_u] = round(v, 6)
    for key in ("per_symbol_calibration", "symbols", "symbol_profiles", "calibrated_symbols"):
        raw_tbl = doc.get(key)
        if isinstance(raw_tbl, Mapping):
            for sym, row in raw_tbl.items():
                if not isinstance(row, Mapping):
                    continue
                sym_u = str(sym or "").strip().upper()
                v = _f(row.get("min_trailing_pct"), -1.0)
                if sym_u and 0.0 < v <= max_trail and sym_u not in out:
                    out[sym_u] = round(v, 6)
        elif isinstance(raw_tbl, Sequence) and not isinstance(raw_tbl, (str, bytes)):
            for row in raw_tbl:
                if not isinstance(row, Mapping):
                    continue
                sym_u = str(row.get("symbol") or "").strip().upper()
                v = _f(row.get("min_trailing_pct"), -1.0)
                if sym_u and 0.0 < v <= max_trail and sym_u not in out:
                    out[sym_u] = round(v, 6)
    return out


def load_extreme_mover_calibration(
    manifest_path: Path | None = None,
) -> tuple[dict[str, float], dict[str, float], dict[str, Any]]:
    """Load PIT-validated (<= 2026-10-15) weights & per-symbol min_trailing_pct fail-closed."""
    weights = dict(DEFAULT_EARLY_MOVER_WEIGHTS)
    symbol_min_trails: dict[str, float] = {}
    meta: dict[str, Any] = {
        "source": "deterministic_default",
        "wall_date": PREWALL_PIT_WALL_DATE,
        "manifest_loaded": False,
        "calibrated_symbols_count": 0,
    }
    target_path = manifest_path
    if target_path is None:
        target_path = (
            DEFAULT_HISTORICAL_CALIBRATION_MANIFEST
            if DEFAULT_HISTORICAL_CALIBRATION_MANIFEST.is_file()
            else DEFAULT_PREWALL_CALIBRATION_MANIFEST
        )
    elif (
        not target_path.is_file()
        and target_path == DEFAULT_PREWALL_CALIBRATION_MANIFEST
        and DEFAULT_HISTORICAL_CALIBRATION_MANIFEST.is_file()
    ):
        target_path = DEFAULT_HISTORICAL_CALIBRATION_MANIFEST
    if target_path is None or not target_path.is_file():
        return weights, symbol_min_trails, meta
    try:
        doc = json.loads(target_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return weights, symbol_min_trails, meta
    if not isinstance(doc, Mapping):
        return weights, symbol_min_trails, meta
    wall_raw = (
        doc.get("prewall_wall_date")
        or doc.get("wall_date")
        or doc.get("pit_wall_date")
        or (doc.get("pit_policy") or {}).get("wall_date")
        or (doc.get("metadata") or {}).get("wall_date")
    )
    wall_str = str(wall_raw or "").strip()
    if not _ISO_DATE_RE.match(wall_str) or wall_str > PREWALL_PIT_WALL_DATE:
        meta["rejected_reason"] = f"invalid_or_postwall_date:{wall_str or 'missing'}"
        return weights, symbol_min_trails, meta
    raw_weights: Any = None
    for candidate in (
        doc.get("calibrated_weights"),
        doc.get("best_weights"),
        doc.get("optuna_best_params"),
        doc.get("weights"),
        (doc.get("optuna") or {}).get("best_params")
        if isinstance(doc.get("optuna"), Mapping)
        else None,
    ):
        if isinstance(candidate, Mapping) and candidate:
            raw_weights = candidate
            break
    aliases = {
        "move_weight": ("move_weight", "w_move", "early_move_weight"),
        "rvol_weight": ("rvol_weight", "w_rvol", "early_rvol_weight"),
        "trf_weight": ("trf_weight", "w_trf", "darkpool_weight", "w_short_vol"),
        "trf_block_weight": ("trf_block_weight", "w_trf_block"),
        "vol_buildup_weight": ("vol_buildup_weight", "w_vol_buildup", "w_surge", "surge_weight"),
        "lit_iso_weight": ("lit_iso_weight", "w_lit_iso"),
        "breakout_leader_bonus": (
            "breakout_leader_bonus",
            "w_breakout",
            "breakout_weight",
            "orb_vwap_weight",
        ),
    }
    applied = 0
    if isinstance(raw_weights, Mapping):
        for target_key, cand_keys in aliases.items():
            for ck in cand_keys:
                if ck in raw_weights:
                    val = _f(raw_weights[ck], -1.0)
                    if 0.1 <= val <= 30.0:
                        weights[target_key] = round(val, 4)
                        applied += 1
                        break
    symbol_min_trails = _extract_symbol_min_trails(doc)
    if applied > 0 or symbol_min_trails:
        meta["source"] = str(target_path)
        meta["wall_date"] = wall_str
        meta["manifest_loaded"] = True
        meta["applied_weight_count"] = applied
        meta["calibrated_symbols_count"] = len(symbol_min_trails)
    return weights, symbol_min_trails, meta


def _default_tuning_manifest_path() -> Path:
    env_p = os.environ.get("TIER0_EARLY_MOVER_TUNING_MANIFEST", "").strip()
    if env_p:
        return Path(env_p)
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    data_root = Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)
    return data_root / "manifests" / DEFAULT_EARLY_MOVER_TUNING_FILENAME


def load_early_mover_tuning_manifest(
    path: Path | None = None,
    *,
    max_age_hours: float = EARLY_MOVER_TUNING_MAX_AGE_HOURS,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Safely load fine-tuned causal early-mover weights with deterministic fallback."""
    resolved = path if path is not None else _default_tuning_manifest_path()
    fallback: dict[str, Any] = {
        "loaded": False,
        "source": "default_causal_fallback",
        "tuning_run_id": "default_causal_v1",
        "weights": dict(disc.DEFAULT_EARLY_MOVER_WEIGHTS),
        "generated_at_utc": None,
        "prewall_recall_at_top15": None,
        "prewall_median_ttd_mins": None,
    }
    if not resolved.is_file():
        return fallback
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    if not isinstance(raw, dict):
        return fallback
    if raw.get("schema_version") != "tier0.early-mover-tuning.v1":
        return fallback
    if raw.get("paper_only") is not True or raw.get("live_execution_enabled") is not False:
        return fallback
    cutoff_str = str(raw.get("prewall_cutoff_iso") or raw.get("prewall_cutoff") or "").strip()
    if not _ISO_DATE_RE.match(cutoff_str) or cutoff_str > PREWALL_PIT_WALL_DATE:
        return fallback
    weights_raw = raw.get("weights") or raw.get("best_weights")
    if not isinstance(weights_raw, Mapping):
        return fallback

    gen_raw = raw.get("generated_at_utc")
    if isinstance(gen_raw, str) and gen_raw.strip() and max_age_hours > 0.0:
        try:
            gen_dt = datetime.fromisoformat(gen_raw.replace("Z", "+00:00"))
            if gen_dt.tzinfo is None:
                gen_dt = gen_dt.replace(tzinfo=UTC)
            ref_now = now_utc or datetime.now(UTC)
            age_hours = (ref_now.astimezone(UTC) - gen_dt.astimezone(UTC)).total_seconds() / 3600.0
            if age_hours > max_age_hours:
                return fallback
        except ValueError:
            return fallback

    val_metrics = raw.get("validation_metrics") or (
        raw.get("prewall_split", {}).get("val_metrics", {})
        if isinstance(raw.get("prewall_split"), Mapping)
        else {}
    )
    val_map: Mapping[str, Any] = val_metrics if isinstance(val_metrics, Mapping) else {}
    run_id = str(raw.get("tuning_run_id") or "tuned_causal_v1").strip() or "tuned_causal_v1"
    return {
        "loaded": True,
        "source": str(resolved),
        "tuning_run_id": run_id,
        "weights": disc.normalize_early_mover_scoring_weights(weights_raw),
        "generated_at_utc": gen_raw if isinstance(gen_raw, str) else None,
        "prewall_recall_at_top15": val_map.get("recall_at_top15"),
        "prewall_median_ttd_mins": val_map.get("median_time_to_first_detect_mins"),
    }


def load_prewall_early_mover_weights(
    manifest_path: Path | None = None,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Load PIT-validated (<= 2026-10-15) early-detection weights with deterministic fallback."""
    weights, _, meta = load_extreme_mover_calibration(manifest_path)
    return weights, meta


def aggregate_finra_trf_ratios(trades: Sequence[Any]) -> dict[str, dict[str, Any]]:
    """Compute FINRA TRF (Exchange.D) darkpool, institutional block & lit ISO sweep metrics."""
    out: dict[str, dict[str, Any]] = {}
    for tr in trades:
        sym = str(_ga(tr, "symbol", "") or "").strip().upper()
        sz = _f(_ga(tr, "size"), 0.0)
        if not sym or sz <= 0.0:
            continue
        px = _f(_ga(tr, "price"), 0.0)
        conds = _parse_trade_conditions(_ga(tr, "conditions"))
        is_odd_lot = ("I" in conds) or (sz < 100.0)
        is_deriv_priced = "4" in conds
        ex = _ga(tr, "exchange")
        ex_str = str(_ga(ex, "value", ex) or "").strip().upper()
        rec = out.setdefault(
            sym,
            {
                "trf_volume": 0.0,
                "trf_round_lot_volume": 0.0,
                "trf_block_volume": 0.0,
                "trf_block_count": 0,
                "darkpool_block_count": 0,
                "trf_block_notional_usd": 0.0,
                "trf_excluded_odd_lot_volume": 0.0,
                "trf_excluded_deriv_volume": 0.0,
                "lit_iso_sweep_volume": 0.0,
                "lit_iso_sweep_count": 0,
                "total_volume": 0.0,
                "darkpool_trf_ratio": 0.0,
                "trf_round_lot_ratio": 0.0,
                "trf_block_ratio": 0.0,
                "lit_iso_sweep_ratio": 0.0,
            },
        )
        rec["total_volume"] += sz
        if ex_str == Exchange.D.value:
            rec["trf_volume"] += sz
            if is_odd_lot:
                rec["trf_excluded_odd_lot_volume"] += sz
            if is_deriv_priced:
                rec["trf_excluded_deriv_volume"] += sz
            if not is_odd_lot and not is_deriv_priced and sz >= 100.0:
                rec["trf_round_lot_volume"] += sz
                notional = sz * px if px > 0.0 else 0.0
                if (px > 0.0 and notional >= 50_000.0) or (px <= 0.0 and sz >= 500.0):
                    rec["trf_block_volume"] += sz
                    rec["trf_block_count"] += 1
                    rec["darkpool_block_count"] += 1
                    rec["trf_block_notional_usd"] = round(
                        float(rec["trf_block_notional_usd"]) + notional, 2
                    )
        elif "F" in conds:
            rec["lit_iso_sweep_volume"] += sz
            rec["lit_iso_sweep_count"] += 1
        denom = max(1.0, float(rec["total_volume"]))
        rec["darkpool_trf_ratio"] = round(float(rec["trf_volume"]) / denom, 4)
        rec["trf_round_lot_ratio"] = round(float(rec["trf_round_lot_volume"]) / denom, 4)
        rec["trf_block_ratio"] = round(float(rec["trf_block_volume"]) / denom, 4)
        rec["lit_iso_sweep_ratio"] = round(float(rec["lit_iso_sweep_volume"]) / denom, 4)
    return out


def lookup_alpaca_paper_borrow(
    symbols: Sequence[str], trading_client: Any, *, as_of: datetime | None = None
) -> dict[str, dict[str, Any]]:
    """Post-selection `alpaca_paper_asset` shortable, ETB borrow, options & TV exchange lookup."""
    reqs = sorted({s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()})
    if not reqs or trading_client is None:
        return {}
    now_iso = (as_of or datetime.now(UTC)).isoformat()
    raw = (
        trading_client.get_all_assets(GetAssetsRequest(status=AssetStatus.ACTIVE))
        if len(reqs) > 10
        else [trading_client.get_asset(s) for s in reqs]
    )
    out: dict[str, dict[str, Any]] = {}
    for asset in raw:
        sym = str(_ga(asset, "symbol", "") or "").strip().upper()
        if sym not in reqs:
            continue
        sh = bool(_ga(asset, "shortable", False))
        bs = _ga(asset, "borrow_status")
        etb = (
            bs == AssetBorrowStatus.EASY_TO_BORROW
            or str(_ga(bs, "value", bs)).strip().lower().endswith("easy_to_borrow")
            if bs is not None
            else bool(_ga(asset, "easy_to_borrow", False))
        )
        raw_attrs = _ga(asset, "attributes") or ()
        attrs = {str(_ga(a, "value", a)).strip().lower() for a in raw_attrs}
        has_options = bool(_ga(asset, "has_options", False)) or ("has_options" in attrs)
        overnight_tradable = bool(_ga(asset, "overnight_tradable", False)) or (
            "overnight_tradable" in attrs
        )
        out[sym] = {
            "shortable": sh,
            "easy_to_borrow": sh and etb,
            "borrow_source": "alpaca_paper_asset",
            "borrow_as_of_utc": now_iso,
            "short_fee_bps": 0.0 if (sh and etb) else 30.0,
            "exchange": normalize_tv_exchange(_ga(asset, "exchange")),
            "has_options": has_options,
            "overnight_tradable": overnight_tradable,
        }
    return out


def _infer_option_side(contract_sym: str, raw_type: Any = None) -> str | None:
    if raw_type is not None:
        raw = str(_ga(raw_type, "value", raw_type) or "").strip().lower()
        if raw.startswith("c"):
            return "call"
        if raw.startswith("p"):
            return "put"
    m = _OCC_SYM_RE.match(contract_sym.strip().upper())
    if m:
        return "call" if m.group(1) == "C" else "put"
    return None


def enrich_opra_uoa_gex(
    candidates: Sequence[Mapping[str, Any]],
    *,
    option_client: Any | None = None,
    trading_client: Any | None = None,
    now_utc: datetime | None = None,
    max_symbols: int = 8,
    time_budget_secs: float = 8.0,
) -> dict[str, dict[str, Any]]:
    """Bounded, fail-closed Alpaca OPRA UOA (`Vol/OI`), Dealer `GEX` & 25d IV Skew overlay."""
    if option_client is None or not candidates or max_symbols <= 0:
        return {}
    now = now_utc or datetime.now(UTC)
    exp_gte = now.date().isoformat()
    exp_lte = (now.date() + timedelta(days=21)).isoformat()
    deadline = time.monotonic() + max(0.5, float(time_budget_secs))

    targets: list[tuple[str, float]] = []
    seen: set[str] = set()
    for row in candidates:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym or sym in seen:
            continue
        if "has_options" in row and not bool(row.get("has_options")):
            continue
        spot = _f(row.get("last_price"), 0.0)
        if spot <= 0.0:
            continue
        seen.add(sym)
        targets.append((sym, spot))
        if len(targets) >= max_symbols:
            break
    if not targets:
        return {}

    def _enrich_one(item: tuple[str, float]) -> tuple[str, dict[str, Any]]:
        sym, spot = item
        if time.monotonic() >= deadline:
            return sym, dict(_EMPTY_OPRA)
        strike_lo = round(max(0.5, spot * 0.85), 2)
        strike_hi = round(spot * 1.15, 2)
        try:
            chain = option_client.get_option_chain(
                OptionChainRequest(
                    underlying_symbol=sym,
                    feed=OptionsFeed.OPRA,
                    expiration_date_gte=exp_gte,
                    expiration_date_lte=exp_lte,
                    strike_price_gte=strike_lo,
                    strike_price_lte=strike_hi,
                )
            )
        except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
            return sym, dict(_EMPTY_OPRA)
        if not isinstance(chain, Mapping) or not chain:
            return sym, dict(_EMPTY_OPRA)

        oi_map: dict[str, tuple[float, str | None]] = {}
        if (
            trading_client is not None
            and hasattr(trading_client, "get_option_contracts")
            and time.monotonic() < deadline
        ):
            try:
                resp = trading_client.get_option_contracts(
                    GetOptionContractsRequest(
                        underlying_symbols=[sym],
                        expiration_date_gte=exp_gte,
                        expiration_date_lte=exp_lte,
                        strike_price_gte=str(strike_lo),
                        strike_price_lte=str(strike_hi),
                        limit=100,
                    )
                )
                contracts = _ga(resp, "option_contracts") or ()
                for c in contracts:
                    csym = str(_ga(c, "symbol", "") or "").strip().upper()
                    if not csym:
                        continue
                    oi_val = max(0.0, _f(_ga(c, "open_interest"), 0.0))
                    oi_map[csym] = (oi_val, _infer_option_side(csym, _ga(c, "type")))
            except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
                oi_map = {}

        vol_map: dict[str, float] = {}
        if hasattr(option_client, "get_option_bars") and time.monotonic() < deadline:
            bar_targets = (
                [k for k, _ in sorted(oi_map.items(), key=lambda kv: (-kv[1][0], kv[0]))[:40]]
                if oi_map
                else list(chain.keys())[:40]
            )
            if bar_targets:
                try:
                    bars_resp = option_client.get_option_bars(
                        OptionBarsRequest(
                            symbol_or_symbols=bar_targets,
                            timeframe=TimeFrame.Day,
                            start=now - timedelta(days=5),
                            end=now,
                        )
                    )
                    bdata = _ga(bars_resp, "data") or {}
                    for csym, blist in bdata.items():
                        if blist:
                            vol_map[str(csym).strip().upper()] = max(
                                0.0, _f(_ga(blist[-1], "volume"), 0.0)
                            )
                except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
                    vol_map = {}

        call_vol = 0.0
        put_vol = 0.0
        call_oi = 0.0
        put_oi = 0.0
        net_gex = 0.0
        gex_contracts = 0
        best_call_25d: tuple[float, float] | None = None
        best_put_25d: tuple[float, float] | None = None

        all_csyms = set(chain.keys()) | set(oi_map.keys())
        for raw_csym in all_csyms:
            csym = str(raw_csym).strip().upper()
            snap = chain.get(raw_csym) or chain.get(csym)
            oi_val, c_side = oi_map.get(csym, (0.0, None))
            side = c_side or _infer_option_side(csym)
            if side is None:
                continue
            c_vol = vol_map.get(csym, 0.0)
            if c_vol <= 0.0 and snap is not None:
                c_vol = max(0.0, _f(_ga(_ga(snap, "latest_trade"), "size"), 0.0))
            if side == "call":
                call_vol += c_vol
                call_oi += oi_val
            else:
                put_vol += c_vol
                put_oi += oi_val
            if snap is None:
                continue
            greeks = _ga(snap, "greeks")
            gamma = _f(_ga(greeks, "gamma"), 0.0) if greeks is not None else 0.0
            delta = _f(_ga(greeks, "delta"), float("nan")) if greeks is not None else float("nan")
            iv = _f(_ga(snap, "implied_volatility"), 0.0)
            if gamma > 0.0 and oi_val > 0.0:
                sign = 1.0 if side == "call" else -1.0
                net_gex += sign * gamma * oi_val * (spot * spot)
                gex_contracts += 1
            if iv > 0.0 and math.isfinite(delta):
                if side == "call" and 0.10 <= delta <= 0.40:
                    dist = abs(delta - 0.25)
                    if best_call_25d is None or dist < best_call_25d[0]:
                        best_call_25d = (dist, iv)
                elif side == "put" and -0.40 <= delta <= -0.10:
                    dist = abs(delta + 0.25)
                    if best_put_25d is None or dist < best_put_25d[0]:
                        best_put_25d = (dist, iv)

        tot_vol = call_vol + put_vol
        tot_oi = call_oi + put_oi
        vol_oi = round(tot_vol / tot_oi, 4) if tot_oi > 0.0 else None
        cp_ratio = (
            round(call_vol / put_vol, 4)
            if put_vol > 0.0
            else (round(call_oi / put_oi, 4) if put_oi > 0.0 else None)
        )
        gex_out = round(net_gex, 2) if gex_contracts > 0 else None
        iv_skew = (
            round(best_put_25d[1] - best_call_25d[1], 4)
            if best_put_25d is not None and best_call_25d is not None
            else None
        )
        uoa_active = bool(
            (vol_oi is not None and vol_oi >= 1.5)
            or (
                vol_oi is not None
                and vol_oi >= 1.0
                and cp_ratio is not None
                and (cp_ratio >= 2.0 or cp_ratio <= 0.5)
            )
        )
        return sym, {
            "uoa_vol_oi_ratio": vol_oi,
            "uoa_call_put_ratio": cp_ratio,
            "net_dealer_gex_usd": gex_out,
            "iv_skew_25d": iv_skew,
            "uoa_active": uoa_active,
        }

    out: dict[str, dict[str, Any]] = {}
    if len(targets) == 1:
        sym, rec = _enrich_one(targets[0])
        out[sym] = rec
        return out
    with ThreadPoolExecutor(max_workers=min(4, len(targets))) as pool:
        for sym, rec in pool.map(_enrich_one, targets):
            out[sym] = rec
    return out


def _normalize_corp_action_type(raw_bucket: str, item: Any) -> str:
    raw = str(_ga(item, "corporate_action_type", raw_bucket) or raw_bucket).strip().upper()
    singular = raw[:-1] if raw.endswith("S") and raw not in _CORP_ACTION_PRIORITY else raw
    if singular in _CORP_ACTION_PRIORITY:
        return singular
    return raw or "CORPORATE_ACTION"


def enrich_catalyst_and_corp_actions(
    symbols: Sequence[str],
    *,
    news_client: Any | None = None,
    corp_actions_client: Any | None = None,
    now_utc: datetime | None = None,
    news_lookback_hours: int = 24,
    corp_window_days: int = 5,
) -> dict[str, dict[str, Any]]:
    """Bounded post-selection NewsClient (`fiscal.ai`) + CorporateActionsClient (±5d) guard."""
    reqs = sorted({s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()})
    if not reqs or (news_client is None and corp_actions_client is None):
        return {}
    now = now_utc or datetime.now(UTC)
    out: dict[str, dict[str, Any]] = {sym: dict(_EMPTY_CATALYST) for sym in reqs}
    req_set = set(reqs)

    def _fetch_news() -> list[Any]:
        if news_client is None:
            return []
        try:
            res = news_client.get_news(
                NewsRequest(
                    symbols=",".join(reqs),
                    start=now - timedelta(hours=news_lookback_hours),
                    limit=50,
                )
            )
            if isinstance(res, list):
                return res
            data = _ga(res, "data")
            if isinstance(data, Mapping):
                return list(data.get("news") or ())
            return list(_ga(res, "news") or ())
        except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
            return []

    def _fetch_ca() -> dict[str, list[Any]]:
        if corp_actions_client is None:
            return {}
        try:
            res = corp_actions_client.get_corporate_actions(
                CorporateActionsRequest(
                    symbols=reqs,
                    start=now.date() - timedelta(days=corp_window_days),
                    end=now.date() + timedelta(days=corp_window_days),
                )
            )
            data = _ga(res, "data")
            if isinstance(data, Mapping):
                return {str(k): list(v or ()) for k, v in data.items()}
            return {}
        except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
            return {}

    with ThreadPoolExecutor(max_workers=2) as pool:
        news_fut = pool.submit(_fetch_news)
        ca_fut = pool.submit(_fetch_ca)
        news_items, ca_map = news_fut.result(), ca_fut.result()

    latest_dt_by_sym: dict[str, datetime] = {}
    for item in news_items:
        item_syms = _ga(item, "symbols") or ()
        created = _ga(item, "created_at")
        created_dt: datetime | None = None
        if isinstance(created, datetime):
            created_dt = created.astimezone(UTC) if created.tzinfo else created.replace(tzinfo=UTC)
        elif isinstance(created, str) and created.strip():
            try:
                parsed = datetime.fromisoformat(created.replace("Z", "+00:00"))
                created_dt = parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            except ValueError:
                created_dt = None
        src = str(_ga(item, "source", "") or "").strip() or None
        for raw_s in item_syms:
            sym = str(raw_s or "").strip().upper()
            if sym not in req_set:
                continue
            out[sym]["catalyst_news_24h_count"] = int(out[sym]["catalyst_news_24h_count"]) + 1
            if created_dt is not None and (
                sym not in latest_dt_by_sym or created_dt > latest_dt_by_sym[sym]
            ):
                latest_dt_by_sym[sym] = created_dt
                out[sym]["latest_catalyst_utc"] = created_dt.isoformat()
                out[sym]["latest_catalyst_source"] = src

    prio_rank = {name: idx for idx, name in enumerate(_CORP_ACTION_PRIORITY)}
    best_ca_rank: dict[str, int] = {}
    for bucket, items in ca_map.items():
        for item in items:
            sym = str(_ga(item, "symbol", "") or "").strip().upper()
            if sym not in req_set:
                continue
            ca_type = _normalize_corp_action_type(bucket, item)
            rank = prio_rank.get(ca_type, len(_CORP_ACTION_PRIORITY))
            if sym not in best_ca_rank or rank < best_ca_rank[sym]:
                best_ca_rank[sym] = rank
                ex_raw = _ga(item, "ex_date") or _ga(item, "process_date")
                ex_str = (
                    ex_raw.isoformat()
                    if isinstance(ex_raw, (date, datetime))
                    else (str(ex_raw).strip() or None if ex_raw is not None else None)
                )
                out[sym]["corp_action_flag"] = ca_type
                out[sym]["corp_action_ex_date"] = ex_str
    return out


def enrich_sec_pit_catalysts(
    symbols: Sequence[str],
    *,
    now_utc: datetime | None = None,
    offline_filings: Mapping[str, list[dict[str, Any]]] | None = None,
    allow_network: bool = False,
    lookback_days: int = 30,
    parquet_dir: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fail-closed offline/cached EdgarTools PIT SEC 8-K/10-Q/10-K/Form-4 catalyst enrichment."""
    reqs = sorted({s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()})
    if not reqs:
        return {}, {
            "enabled": False,
            "pit_safe": True,
            "allow_network": bool(allow_network),
            "source": "empty_symbols",
            "symbols_count": 0,
            "total_accepted_filings": 0,
            "excluded_future_filings": 0,
        }
    as_of_iso = (now_utc or datetime.now(UTC)).astimezone(UTC).isoformat()
    offline_dict = (
        {str(k).strip().upper(): list(v) for k, v in offline_filings.items()}
        if offline_filings is not None
        else None
    )
    try:
        rep = disc.load_causal_sec_catalysts(
            reqs,
            as_of_utc=as_of_iso,
            offline_filings=offline_dict,
            allow_network=bool(allow_network),
            lookback_days=int(lookback_days),
            parquet_dir=parquet_dir,
        )
    except Exception as exc:  # noqa: BLE001
        fallback_out = {
            sym: {
                "sec_catalyst_score": 0.0,
                "sec_catalyst_summary": dict(_EMPTY_SEC_CATALYST["sec_catalyst_summary"]),
            }
            for sym in reqs
        }
        return fallback_out, {
            "enabled": True,
            "pit_safe": True,
            "allow_network": bool(allow_network),
            "source": "fail_closed_error",
            "error": type(exc).__name__,
            "symbols_count": len(reqs),
            "total_accepted_filings": 0,
            "excluded_future_filings": 0,
        }

    src = str(rep.get("source") or "offline_empty_fail_closed")
    by_sym = rep.get("by_symbol") if isinstance(rep.get("by_symbol"), Mapping) else {}
    out: dict[str, dict[str, Any]] = {}
    for sym in reqs:
        rec = by_sym.get(sym) if isinstance(by_sym, Mapping) else None
        if not isinstance(rec, Mapping):
            out[sym] = {
                "sec_catalyst_score": 0.0,
                "sec_catalyst_summary": {
                    **dict(_EMPTY_SEC_CATALYST["sec_catalyst_summary"]),
                    "source": src,
                },
            }
            continue
        score = round(_f(rec.get("catalyst_score"), 0.0), 2)
        out[sym] = {
            "sec_catalyst_score": score,
            "sec_catalyst_summary": {
                "catalyst_score": score,
                "recent_8k_count": int(rec.get("recent_8k_count") or 0),
                "recent_10q_10k_count": int(rec.get("recent_10q_10k_count") or 0),
                "insider_form4_count": int(rec.get("insider_form4_count") or 0),
                "material_8k_count": int(rec.get("material_8k_count") or 0),
                "accepted_filings_count": int(rec.get("accepted_filings_count") or 0),
                "excluded_future_filings": int(rec.get("excluded_future_filings") or 0),
                "latest_form": rec.get("latest_form"),
                "latest_accepted_utc": rec.get("latest_accepted_utc"),
                "pit_safe": bool(rec.get("pit_safe", True)),
                "source": src,
            },
        }
    meta = {
        "enabled": True,
        "pit_safe": bool(rep.get("pit_safe", True)),
        "allow_network": bool(rep.get("allow_network", False)),
        "source": src,
        "edgartools_version": rep.get("edgartools_version"),
        "as_of_utc": rep.get("as_of_utc"),
        "symbols_count": int(rep.get("symbols_count") or len(reqs)),
        "total_accepted_filings": int(rep.get("total_accepted_filings") or 0),
        "excluded_future_filings": int(rep.get("excluded_future_filings") or 0),
    }
    return out, meta


@dataclass(frozen=True)
class ExtremeMoverScanner:
    """Alpaca SIP + FINRA TRF + OPRA + Catalyst whole-market extreme-mover scanner."""

    screener_client: Any | None = None
    data_client: Any | None = None
    trading_client: Any | None = None
    option_client: Any | None = None
    news_client: Any | None = None
    corp_actions_client: Any | None = None
    pin_symbols: tuple[str, ...] = DEFAULT_PIN_SYMBOLS
    min_move_pct: float = 3.5
    min_rvol: float = 1.5
    min_price: float = 10.0
    min_dollar_vol: float = 5_000_000.0
    max_enrichment_candidates: int = 90
    opra_max_symbols: int = 8
    opra_time_budget_secs: float = 8.0
    calibration_manifest_path: Path | None = None
    tuning_manifest_path: Path | None = None
    enable_sec_pit_catalysts: bool = True
    sec_allow_network: bool = False
    sec_offline_filings: Mapping[str, list[dict[str, Any]]] | None = None
    sec_parquet_dir: Path | None = None
    enable_full_market_sweep: bool = True
    early_mover_min_price: float = 1.0
    early_mover_min_dollar_vol: float = 500_000.0
    universe_cache_path: Path | None = None

    @classmethod
    def from_env(cls, account: str = "ALPACA_PA1", **kw: Any) -> ExtremeMoverScanner:
        key, sec = resolve_alpaca_env_creds(account)
        if not (key and sec):
            raise RuntimeError(f"Missing Alpaca Paper credentials in env for {account}")
        kw.setdefault("option_client", OptionHistoricalDataClient(key, sec))
        kw.setdefault("news_client", NewsClient(key, sec))
        kw.setdefault("corp_actions_client", CorporateActionsClient(key, sec))
        kw.setdefault("calibration_manifest_path", None)
        return cls(
            screener_client=ad.ScreenerClient(key, sec),
            data_client=ad.StockHistoricalDataClient(key, sec),
            trading_client=at.TradingClient(key, sec, paper=True),
            **kw,
        )

    def _discover_raw(
        self, *, include_screener: bool = True, extra_symbols: Sequence[str] = ()
    ) -> dict[str, dict[str, Any]]:
        cands: dict[str, dict[str, Any]] = {}
        if include_screener and self.screener_client is not None:
            sc = self.screener_client
            with ThreadPoolExecutor(max_workers=3) as pool:
                act_vol_fut = pool.submit(
                    sc.get_most_actives, MostActivesRequest(top=100, by=MostActivesBy.VOLUME)
                )
                act_trd_fut = pool.submit(
                    sc.get_most_actives, MostActivesRequest(top=100, by=MostActivesBy.TRADES)
                )
                mov_fut = pool.submit(sc.get_market_movers, MarketMoversRequest(top=50))
                act_vol = act_vol_fut.result()
                try:
                    act_trd = act_trd_fut.result()
                except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
                    act_trd = None
                mov = mov_fut.result()
            for src, attr, tag in (
                (act_vol, "most_actives", "alpaca_most_actives"),
                (act_trd, "most_actives", "alpaca_most_actives_trades"),
                (mov, "gainers", "alpaca_gainers"),
                (mov, "losers", "alpaca_losers"),
            ):
                for item in _ga(src, attr) or ():
                    sym = str(_ga(item, "symbol", "") or "").strip().upper()
                    if not sym or not VALID_EQUITY_RE.match(sym):
                        continue
                    px = _f(_ga(item, "price"))
                    vol = _f(_ga(item, "volume"))
                    chg = _f(_ga(item, "percent_change"))
                    tcnt = _f(_ga(item, "trade_count"))
                    rec = cands.setdefault(
                        sym,
                        {
                            "symbol": sym,
                            "price": 0.0,
                            "volume": 0.0,
                            "move": 0.0,
                            "trade_count": 0.0,
                            "tags": [],
                        },
                    )
                    rec["price"] = max(rec["price"], px)
                    rec["volume"] = max(rec["volume"], vol)
                    rec["trade_count"] = max(rec["trade_count"], tcnt)
                    if abs(chg) > abs(rec["move"]):
                        rec["move"] = chg
                    if tag not in rec["tags"]:
                        rec["tags"].append(tag)
        for pin in self.pin_symbols:
            if (p := pin.strip().upper()) and VALID_EQUITY_RE.match(p):
                rec = cands.setdefault(
                    p,
                    {
                        "symbol": p,
                        "price": 0.0,
                        "volume": 0.0,
                        "move": 0.0,
                        "trade_count": 0.0,
                        "tags": [],
                    },
                )
                if "pinned" not in rec["tags"]:
                    rec["tags"].append("pinned")
        for seed in extra_symbols:
            if (s := seed.strip().upper()) and VALID_EQUITY_RE.match(s):
                rec = cands.setdefault(
                    s,
                    {
                        "symbol": s,
                        "price": 0.0,
                        "volume": 0.0,
                        "move": 0.0,
                        "trade_count": 0.0,
                        "tags": [],
                    },
                )
                if "overnight_seed" not in rec["tags"]:
                    rec["tags"].append("overnight_seed")
        return cands

    def _bound_enrichment_candidates(
        self,
        cands: Mapping[str, Mapping[str, Any]],
        priority_symbols: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """Bound candidate enrichment to `max_enrichment_candidates` while pruning sub-$10 noise."""
        prio = {s.strip().upper() for s in priority_symbols if s and s.strip()}
        eligible: list[tuple[str, dict[str, Any]]] = []
        for sym, meta in cands.items():
            px = _f(meta.get("price"), 0.0)
            if 0.0 < px < self.min_price and sym not in prio:
                continue
            eligible.append((sym, dict(meta)))
        cap = max(len(prio), int(self.max_enrichment_candidates))
        if len(eligible) <= cap:
            return dict(eligible)

        def _prio_key(
            item: tuple[str, dict[str, Any]],
        ) -> tuple[int, int, float, float, float, str]:
            sym, m = item
            is_prio = 0 if sym in prio else 1
            is_leader = 0 if sym in TECHNICAL_BREAKOUT_LEADER_SYMBOLS else 1
            return (
                is_prio,
                is_leader,
                -abs(_f(m.get("move"), 0.0)),
                -_f(m.get("volume"), 0.0),
                -_f(m.get("trade_count"), 0.0),
                sym,
            )

        eligible.sort(key=_prio_key)
        return dict(eligible[:cap])

    def _fetch_trf_window(
        self,
        syms: Sequence[str],
        *,
        start_utc: datetime,
        end_utc: datetime,
        source: str,
        as_of_iso: str,
        sort: Sort | None = None,
        limit: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        dc = self.data_client
        if dc is None or not syms or not hasattr(dc, "get_stock_trades"):
            return {}
        unique_syms = list(dict.fromkeys(s.strip().upper() for s in syms if s and s.strip()))
        if not unique_syms:
            return {}

        def _query_one(sym: str) -> list[Any]:
            req_kw: dict[str, Any] = {
                "symbol_or_symbols": sym,
                "start": start_utc,
                "end": end_utc,
                "feed": DataFeed.SIP,
                "limit": limit,
            }
            if sort is not None:
                req_kw["sort"] = sort
            try:
                trades_r = dc.get_stock_trades(StockTradesRequest(**req_kw))
            except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
                return []
            data_map = _ga(trades_r, "data") or {}
            t_list = data_map.get(sym) or ()
            out_trades: list[Any] = []
            for tr in t_list:
                if not _ga(tr, "symbol"):
                    tr.symbol = sym
                out_trades.append(tr)
            return out_trades

        flat: list[Any] = []
        if len(unique_syms) == 1:
            flat.extend(_query_one(unique_syms[0]))
        else:
            with ThreadPoolExecutor(max_workers=min(8, len(unique_syms))) as pool:
                for batch in pool.map(_query_one, unique_syms):
                    flat.extend(batch)
        agg = aggregate_finra_trf_ratios(flat)
        for rec in agg.values():
            rec["darkpool_trf_source"] = source
            rec["darkpool_trf_as_of_utc"] = as_of_iso
        return agg

    def _select_trf_targets(
        self,
        syms: Sequence[str],
        bars_map: Mapping[str, Sequence[Any]],
        priority_symbols: Sequence[str],
        *,
        max_targets: int = 24,
    ) -> list[str]:
        prio = {s.strip().upper() for s in priority_symbols if s and s.strip()}

        def _rank_key(sym: str) -> tuple[int, float, float, str]:
            sb = bars_map.get(sym) or ()
            mv_abs = (
                abs(((_f(_ga(sb[-1], "close")) / _f(_ga(sb[0], "open"))) - 1.0) * 100.0)
                if len(sb) >= 2 and _f(_ga(sb[0], "open"), 0.0) > 0.0
                else 0.0
            )
            tot_v = sum(_f(_ga(b, "volume"), 0.0) for b in sb)
            return (0 if sym in prio else 1, -mv_abs, -tot_v, sym)

        active = [s for s in syms if bars_map.get(s) or s in prio]
        active.sort(key=_rank_key)
        return active[:max_targets]

    def _fetch_bars_batched(
        self,
        syms: Sequence[str],
        now_utc: datetime,
        feed: DataFeed,
        *,
        chunk_size: int = 35,
    ) -> dict[str, list[Any]]:
        dc = self.data_client
        if dc is None or not syms:
            return {}
        req_syms = sorted({*syms, *EQUITY_BENCHMARKS})
        start_utc = now_utc - timedelta(hours=6)
        # Alpaca timestamps minute bars at the start of the interval.
        end_utc = now_utc.replace(second=0, microsecond=0) - timedelta(minutes=1)

        def _one_chunk(batch: list[str]) -> dict[str, list[Any]]:
            req = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Minute,
                start=start_utc,
                end=end_utc,
                feed=feed,
            )
            return dict(_ga(dc.get_stock_bars(req), "data") or {})

        if len(req_syms) <= max(chunk_size, 40):
            return _one_chunk(req_syms)
        chunks = [req_syms[i : i + chunk_size] for i in range(0, len(req_syms), chunk_size)]
        merged: dict[str, list[Any]] = {}
        with ThreadPoolExecutor(max_workers=min(4, len(chunks))) as pool:
            for part in pool.map(_one_chunk, chunks):
                merged.update(part)
        return merged

    def _load_nms_universe(self, now_utc: datetime) -> dict[str, str]:
        """Load active tradable US NMS equities (NASDAQ/NYSE/AMEX/BATS/IEX), cached per ET day."""
        tc = self.trading_client
        if tc is None or not hasattr(tc, "get_all_assets"):
            return {}
        ny_day = now_utc.astimezone(_NY_TZ).date().isoformat()
        cache_p = self.universe_cache_path
        if cache_p is None:
            default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
            data_root = Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)
            if data_root.exists():
                cache_p = data_root / "manifests" / ".nms_equity_universe_cache.json"
        if cache_p is not None and cache_p.is_file():
            try:
                cached = json.loads(cache_p.read_text(encoding="utf-8"))
                if (
                    isinstance(cached, Mapping)
                    and cached.get("ny_date") == ny_day
                    and isinstance(cached.get("symbols"), Mapping)
                    and len(cached["symbols"]) >= 1000
                ):
                    return {str(k): str(v) for k, v in cached["symbols"].items()}
            except (OSError, ValueError, TypeError):
                pass
        try:
            assets = tc.get_all_assets(
                GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)
            )
        except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
            return {}
        out: dict[str, str] = {}
        for asset in assets or ():
            if not bool(_ga(asset, "tradable", False)):
                continue
            sym = str(_ga(asset, "symbol", "") or "").strip().upper()
            if not sym or _is_warrant_or_right_symbol(sym):
                continue
            raw_ex = _ga(asset, "exchange")
            raw_ex_str = str(_ga(raw_ex, "value", raw_ex) or "").strip().upper()
            if raw_ex_str not in _NMS_ALLOWED_EXCHANGES:
                continue
            tv_ex = normalize_tv_exchange(raw_ex) or raw_ex_str
            out[sym] = tv_ex
        if cache_p is not None and len(out) >= 1000:
            try:
                cache_p.parent.mkdir(parents=True, exist_ok=True)
                tmp = cache_p.with_name(f".{cache_p.name}.{os.getpid()}.tmp")
                tmp.write_text(
                    json.dumps({"ny_date": ny_day, "symbols": out}) + "\n", encoding="utf-8"
                )
                tmp.replace(cache_p)
            except OSError:
                pass
        return out

    def _sweep_full_market_movers(
        self,
        now_utc: datetime,
        sess: str,
        feed: DataFeed,
    ) -> tuple[
        list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, dict[str, Any]]
    ]:
        """Sweep 100% of active US NMS equities via official Alpaca StockSnapshotRequest."""
        t0 = time.monotonic()
        empty_summary: dict[str, Any] = {
            "enabled": bool(self.enable_full_market_sweep),
            "universe_scanned": 0,
            "qualifying_movers_count": 0,
            "early_stage_count": 0,
            "extreme_expansion_count": 0,
            "parabolic_climax_count": 0,
            "min_price_usd": float(self.early_mover_min_price),
            "min_dollar_vol_usd": float(self.early_mover_min_dollar_vol),
            "elapsed_secs": 0.0,
        }
        dc = self.data_client
        if (
            not self.enable_full_market_sweep
            or sess == "CLOSED"
            or dc is None
            or not hasattr(dc, "get_stock_snapshot")
        ):
            return [], [], empty_summary, {}
        universe = self._load_nms_universe(now_utc)
        if not universe:
            return [], [], empty_summary, {}

        syms = list(universe)
        elapsed_frac = _estimate_session_elapsed_fraction(now_utc, sess)
        chunks = [syms[i : i + 1000] for i in range(0, len(syms), 1000)]

        def _fetch_chunk(batch: list[str]) -> Mapping[str, Any]:
            try:
                res = dc.get_stock_snapshot(
                    StockSnapshotRequest(symbol_or_symbols=batch, feed=feed)
                )
                return res if isinstance(res, Mapping) else {}
            except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
                return {}

        snap_meta: dict[str, dict[str, Any]] = {}
        qualifying: list[dict[str, Any]] = []
        early_candidates: list[dict[str, Any]] = []
        if len(chunks) == 1:
            parts = [_fetch_chunk(chunks[0])]
        else:
            with ThreadPoolExecutor(max_workers=min(4, len(chunks))) as pool:
                parts = list(pool.map(_fetch_chunk, chunks))

        for part in parts:
            for raw_sym, snap in part.items():
                sym = str(raw_sym or "").strip().upper()
                if not sym or sym not in universe:
                    continue
                lt = _ga(snap, "latest_trade")
                db = _ga(snap, "daily_bar")
                pdb = _ga(snap, "previous_daily_bar")
                px = _f(_ga(lt, "price"), 0.0) if lt is not None else 0.0
                if px <= 0.0 and db is not None:
                    px = _f(_ga(db, "close"), 0.0)
                prev_close = _f(_ga(pdb, "close"), 0.0) if pdb is not None else 0.0
                if px <= 0.0 or prev_close <= 0.0:
                    continue
                dvol = _f(_ga(db, "volume"), 0.0) if db is not None else 0.0
                pvol = _f(_ga(pdb, "volume"), 0.0) if pdb is not None else 0.0
                dopen = _f(_ga(db, "open"), 0.0) if db is not None else 0.0
                dvwap = _f(_ga(db, "vwap"), 0.0) if db is not None else 0.0
                daily_pct = ((px / prev_close) - 1.0) * 100.0
                intra_open_pct = (((px / dopen) - 1.0) * 100.0) if dopen > 0.0 else None
                vwap_dist_pct = (((px / dvwap) - 1.0) * 100.0) if dvwap > 0.0 else None
                daily_rvol = (dvol / pvol) if pvol > 0.0 else 0.0
                tod_rvol = (daily_rvol / elapsed_frac) if daily_rvol > 0.0 else 0.0
                dollar_vol = px * dvol
                tv_ex = universe[sym]
                snap_meta[sym] = {
                    "prev_close": prev_close,
                    "last_price": px,
                    "daily_change_pct": round(daily_pct, 2),
                    "daily_volume": dvol,
                    "prev_daily_volume": pvol,
                    "daily_rvol": round(daily_rvol, 2),
                    "tod_rvol": round(tod_rvol, 2),
                    "dollar_vol": round(dollar_vol, 2),
                    "exchange": tv_ex,
                }
                if (
                    px < self.early_mover_min_price
                    or dvol < 100_000.0
                    or dollar_vol < self.early_mover_min_dollar_vol
                    or abs(daily_pct) < 3.0
                ):
                    continue
                stage = _classify_mover_stage(daily_pct, tod_rvol)
                price_tier = (
                    "INSTITUTIONAL_GE10" if px >= self.min_price else "SMALLCAP_EARLY_1_TO_10"
                )
                side = "BUY" if daily_pct > 0.0 else "SELL"
                capped_rvol = min(tod_rvol, 500.0)
                whole_score = (
                    abs(daily_pct) * 1.6
                    + math.log1p(max(0.0, capped_rvol)) * 7.0
                    + math.log1p(max(0.0, dollar_vol / 1_000_000.0)) * 3.0
                )
                row = {
                    "symbol": sym,
                    "exchange": tv_ex,
                    "tv_symbol": f"{tv_ex}:{sym}",
                    "stage": stage,
                    "price_tier": price_tier,
                    "side": side,
                    "session": sess,
                    "last_price": round(px, 4),
                    "prior_close": round(prev_close, 4),
                    "daily_change_pct": round(daily_pct, 2),
                    "session_move_pct": round(daily_pct, 2),
                    "intraday_from_open_pct": round(intra_open_pct, 2)
                    if intra_open_pct is not None
                    else None,
                    "vwap_dist_pct": round(vwap_dist_pct, 2) if vwap_dist_pct is not None else None,
                    "daily_volume": round(dvol, 0),
                    "prev_daily_volume": round(pvol, 0),
                    "daily_rvol": round(daily_rvol, 2),
                    "tod_rvol": round(tod_rvol, 2),
                    "rvol": round(tod_rvol, 2),
                    "rvol_normalized": round(tod_rvol, 2),
                    "dollar_vol": round(dollar_vol, 2),
                    "score": round(whole_score, 2),
                }
                qualifying.append(row)
                vwap_aligned = (
                    vwap_dist_pct is None
                    or (daily_pct > 0.0 and vwap_dist_pct >= -2.5)
                    or (daily_pct < 0.0 and vwap_dist_pct <= 2.5)
                )
                if 3.0 <= abs(daily_pct) < 25.0 and tod_rvol >= 2.0 and vwap_aligned:
                    open_expand = (
                        3.0
                        if intra_open_pct is not None
                        and (
                            (daily_pct > 0.0 and intra_open_pct > 0.0)
                            or (daily_pct < 0.0 and intra_open_pct < 0.0)
                        )
                        else 0.0
                    )
                    early_score = (
                        abs(daily_pct) * 1.4
                        + math.log1p(min(tod_rvol, 200.0)) * 9.0
                        + math.log1p(max(0.0, dollar_vol / 1_000_000.0)) * 2.5
                        + open_expand
                    )
                    early_candidates.append(
                        row
                        | {
                            "stage": "EARLY_STAGE_IGNITION",
                            "early_stage_score": round(early_score, 2),
                        }
                    )

        qualifying.sort(
            key=lambda r: (-float(r["score"]), -abs(float(r["daily_change_pct"])), r["symbol"])
        )
        early_candidates.sort(
            key=lambda r: (-float(r["early_stage_score"]), -float(r["tod_rvol"]), r["symbol"])
        )
        top_full = [row | {"rank": idx} for idx, row in enumerate(qualifying[:25], 1)]
        top_early = [row | {"rank": idx} for idx, row in enumerate(early_candidates[:15], 1)]
        summary = {
            "enabled": True,
            "universe_scanned": len(universe),
            "qualifying_movers_count": len(qualifying),
            "early_stage_count": len(early_candidates),
            "extreme_expansion_count": sum(
                1 for r in qualifying if r["stage"] == "ACTIVE_EXTREME_EXPANSION"
            ),
            "parabolic_climax_count": sum(
                1 for r in qualifying if r["stage"] == "PARABOLIC_CLIMAX_SURGE"
            ),
            "min_price_usd": float(self.early_mover_min_price),
            "min_dollar_vol_usd": float(self.early_mover_min_dollar_vol),
            "elapsed_secs": round(time.monotonic() - t0, 2),
        }
        return top_full, top_early, summary, snap_meta

    def _fetch_prev_closes(self, syms: Sequence[str], feed: DataFeed) -> dict[str, float]:
        """Fetch official Alpaca previous_daily_bar.close via StockSnapshotRequest."""
        dc = self.data_client
        if dc is None or not syms or not hasattr(dc, "get_stock_snapshot"):
            return {}
        unique = list(dict.fromkeys(s.strip().upper() for s in syms if s and s.strip()))
        out: dict[str, float] = {}
        for i in range(0, len(unique), 50):
            chunk = unique[i : i + 50]
            try:
                snaps = dc.get_stock_snapshot(
                    StockSnapshotRequest(symbol_or_symbols=chunk, feed=feed)
                )
            except (APIError, ValueError, TypeError, AttributeError, OSError, RuntimeError):
                continue
            if not isinstance(snaps, Mapping):
                continue
            for sym, snap in snaps.items():
                pdb = _ga(snap, "previous_daily_bar")
                pc = _f(_ga(pdb, "close"), 0.0) if pdb is not None else 0.0
                if pc > 0.0:
                    out[str(sym).strip().upper()] = pc
        return out

    def _enrich_sip(
        self,
        syms: Sequence[str],
        now_utc: datetime,
        feed: DataFeed = DataFeed.SIP,
        *,
        fetch_trades: bool = True,
        priority_symbols: Sequence[str] = (),
    ) -> tuple[dict[str, list[Any]], dict[str, dict[str, Any]]]:
        if self.data_client is None or not syms:
            return {}, {}
        dc = self.data_client
        if not fetch_trades or not hasattr(dc, "get_stock_trades"):
            return self._fetch_bars_batched(syms, now_utc, feed), {}
        if feed == DataFeed.BOATS:
            bars_map = self._fetch_bars_batched(syms, now_utc, feed)
            trf_targets = self._select_trf_targets(syms, bars_map, priority_symbols, max_targets=24)
            if not trf_targets:
                return bars_map, {}
            tr_start, tr_end = _resolve_prior_sip_trf_window(now_utc)
            trf_stats = self._fetch_trf_window(
                trf_targets,
                start_utc=tr_start,
                end_utc=tr_end,
                source="prior_sip_trf_carryover",
                as_of_iso=tr_end.isoformat(),
                sort=Sort.DESC,
                limit=1000,
            )
            return bars_map, trf_stats

        tr_end = now_utc.replace(second=0, microsecond=0) - timedelta(microseconds=1)
        tr_start = tr_end - timedelta(minutes=15) + timedelta(microseconds=1)
        if len(syms) <= 10:
            with ThreadPoolExecutor(max_workers=2) as pool:
                bars_fut = pool.submit(self._fetch_bars_batched, syms, now_utc, feed)
                trf_fut = pool.submit(
                    self._fetch_trf_window,
                    syms,
                    start_utc=tr_start,
                    end_utc=tr_end,
                    source="live_sip_trf_15m",
                    as_of_iso=tr_end.isoformat(),
                    limit=1000,
                )
                bars_map, trf_stats = bars_fut.result(), trf_fut.result()
            return bars_map, trf_stats

        bars_map = self._fetch_bars_batched(syms, now_utc, feed)
        trf_targets = self._select_trf_targets(syms, bars_map, priority_symbols, max_targets=32)
        trf_stats = self._fetch_trf_window(
            trf_targets,
            start_utc=tr_start,
            end_utc=tr_end,
            source="live_sip_trf_15m",
            as_of_iso=tr_end.isoformat(),
            limit=1000,
        )
        return bars_map, trf_stats

    def scan(
        self, *, top_k: int = 15, output_path: Path | None = None, now_utc: datetime | None = None
    ) -> dict[str, Any]:
        """Run whole-market Alpaca SIP/BOATS + FINRA TRF + OPRA + Catalyst scan."""
        from tier0_sota.live.paper_farm import (
            build_symbol_24x5_session_presets,
            compute_symbol_min_trailing_pct,
        )

        now = now_utc or datetime.now(UTC)
        sess = eng.classify_us_equity_session_et(int(now.timestamp() * 1_000_000_000))
        weights, symbol_min_trails, weight_meta = load_extreme_mover_calibration(
            self.calibration_manifest_path
        )
        tuning_cfg = load_early_mover_tuning_manifest(self.tuning_manifest_path, now_utc=now)
        tuned_weights: dict[str, float] = dict(tuning_cfg["weights"])
        tuning_run_id = str(tuning_cfg["tuning_run_id"])
        overnight_seeds = (
            (*OVERNIGHT_SEED_SYMBOLS, *DEFAULT_PIN_SYMBOLS) if sess == "OVERNIGHT" else ()
        )
        raw_cands = self._discover_raw(
            include_screener=(sess != "CLOSED"),
            extra_symbols=overnight_seeds,
        )
        feed = DataFeed.BOATS if sess == "OVERNIGHT" else DataFeed.SIP
        full_market_top, early_stage_top, sweep_summary, sweep_snap_meta = (
            self._sweep_full_market_movers(now, sess, feed)
        )
        # Inject top liquid (>= min_price) whole-market movers into raw_cands so warrant-clogged
        # top-50 screener responses never hide liquid institutional movers (e.g. BRZL/BRZD/XP/SNDQ).
        liquid_sweep_hits = [
            r
            for r in (*early_stage_top, *full_market_top)
            if _f(r.get("last_price"), 0.0) >= self.min_price
        ]
        for row in liquid_sweep_hits[:24]:
            s_sym = str(row["symbol"])
            rec = raw_cands.setdefault(
                s_sym,
                {
                    "symbol": s_sym,
                    "price": 0.0,
                    "volume": 0.0,
                    "move": 0.0,
                    "trade_count": 0.0,
                    "tags": [],
                },
            )
            rec["price"] = max(rec["price"], _f(row.get("last_price"), 0.0))
            rec["volume"] = max(rec["volume"], _f(row.get("daily_volume"), 0.0))
            if abs(_f(row.get("daily_change_pct"), 0.0)) > abs(rec["move"]):
                rec["move"] = _f(row.get("daily_change_pct"), 0.0)
            if "full_market_sweep" not in rec["tags"]:
                rec["tags"].append("full_market_sweep")

        pins = {p.strip().upper() for p in self.pin_symbols if p.strip()}
        priority_enrichment = sorted(pins | set(overnight_seeds))
        cands = self._bound_enrichment_candidates(raw_cands, priority_enrichment)
        bars_map, trf_stats = self._enrich_sip(
            sorted(cands),
            now,
            feed,
            fetch_trades=(sess != "CLOSED"),
            priority_symbols=sorted(pins),
        )

        prev_closes: dict[str, float] = {
            sym: float(m["prev_close"])
            for sym, m in sweep_snap_meta.items()
            if _f(m.get("prev_close"), 0.0) > 0.0
        }
        missing_prev = sorted((set(cands) | set(EQUITY_BENCHMARKS)) - set(prev_closes))
        if missing_prev:
            prev_closes.update(self._fetch_prev_closes(missing_prev, feed))

        def _bench_intraday_mv(bm_sym: str) -> float:
            b_list = bars_map.get(bm_sym) or []
            if len(b_list) >= 2 and _f(_ga(b_list[0], "open"), 0.0) > 0.0:
                return round(
                    ((_f(_ga(b_list[-1], "close")) / _f(_ga(b_list[0], "open"))) - 1.0) * 100.0,
                    3,
                )
            return 0.0

        def _bench_daily_mv(bm_sym: str) -> float | None:
            pc = prev_closes.get(bm_sym)
            b_list = bars_map.get(bm_sym) or []
            if pc is not None and pc > 0.0 and b_list:
                last_c = _f(_ga(b_list[-1], "close"), 0.0)
                if last_c > 0.0:
                    return round(((last_c / pc) - 1.0) * 100.0, 3)
            return None

        spy_intraday_mv = _bench_intraday_mv("SPY")
        qqq_intraday_mv = _bench_intraday_mv("QQQ")
        soxx_intraday_mv = _bench_intraday_mv("SOXX")
        rsp_intraday_mv = _bench_intraday_mv("RSP")

        spy_daily_mv = _bench_daily_mv("SPY")
        qqq_daily_mv = _bench_daily_mv("QQQ")
        soxx_daily_mv = _bench_daily_mv("SOXX")
        rsp_daily_mv = _bench_daily_mv("RSP")

        spy_mv = spy_daily_mv if spy_daily_mv is not None else spy_intraday_mv
        qqq_mv = qqq_daily_mv if qqq_daily_mv is not None else qqq_intraday_mv
        soxx_mv = soxx_daily_mv if soxx_daily_mv is not None else soxx_intraday_mv
        rsp_mv = rsp_daily_mv if rsp_daily_mv is not None else rsp_intraday_mv

        last_open = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        regime_tol = timedelta(minutes=1) if sess == "RTH" else timedelta(minutes=3)
        fresh: list[str] = []
        regime_ready: list[str] = []
        for sym in EQUITY_BENCHMARKS:
            causal_bm = [
                b
                for b in (bars_map.get(sym) or ())
                if _ga(b, "timestamp") is not None and b.timestamp <= last_open
            ]
            if not causal_bm:
                continue
            latest_ts = causal_bm[-1].timestamp
            if latest_ts == last_open:
                fresh.append(sym)
            if timedelta(0) <= last_open - latest_ts <= regime_tol:
                regime_ready.append(sym)

        macro_regime = 0
        regime_basis = "insufficient_benchmarks"
        if sess != "CLOSED" and all(bm in regime_ready for bm in CORE_REGIME_BENCHMARKS):
            from tier0_sota.discovery import compute_causal_market_regime

            history = pl.DataFrame(
                [
                    {
                        "symbol": sym,
                        "t_ns": int(b.timestamp.timestamp() * 1e9),
                        "h": b.high,
                        "l": b.low,
                        "c": b.close,
                        "v": b.volume,
                    }
                    for sym in regime_ready
                    for b in bars_map[sym]
                    if b.timestamp <= last_open
                ]
            )
            target_ns = int((last_open + timedelta(minutes=1)).timestamp() * 1e9)
            reg_map = compute_causal_market_regime(history)
            if target_ns in reg_map:
                macro_regime = reg_map[target_ns]
            else:
                min_ns = int((last_open - regime_tol + timedelta(minutes=1)).timestamp() * 1e9)
                causal_keys = [ts for ts in reg_map if min_ns <= ts <= target_ns]
                macro_regime = reg_map[max(causal_keys)] if causal_keys else 0
            regime_basis = (
                "all_4_benchmarks"
                if len(fresh) == len(EQUITY_BENCHMARKS)
                else "core_3_spy_qqq_soxx"
            )

        raw_intraday_regime = macro_regime
        daily_core_vals = (spy_daily_mv, qqq_daily_mv, soxx_daily_mv)
        has_all_daily_core = all(v is not None for v in daily_core_vals)
        if (
            raw_intraday_regime == -1
            and has_all_daily_core
            and spy_daily_mv is not None
            and qqq_daily_mv is not None
            and soxx_daily_mv is not None
            and spy_daily_mv > 0.0
            and qqq_daily_mv > 0.0
            and soxx_daily_mv > 0.0
            and ((spy_daily_mv + qqq_daily_mv + soxx_daily_mv) / 3.0) >= 0.25
        ):
            macro_regime = 0
            regime_substate = "INTRADAY_GAP_FADE_IN_GREEN_TAPE"
            regime_reason = (
                f"INTRADAY_GAP_FADE_IN_GREEN_TAPE: Daily vs prev_close GREEN "
                f"(SPY {spy_daily_mv:+.2f}%, QQQ {qqq_daily_mv:+.2f}%, SOXX {soxx_daily_mv:+.2f}%) "
                f"while 1m VWAP/MA30/ADWIN pulled back (SPY {spy_intraday_mv:+.2f}%, "
                f"QQQ {qqq_intraday_mv:+.2f}%, SOXX {soxx_intraday_mv:+.2f}%); "
                "downgraded CRASH(-1) -> NEUTRAL(0) to avoid shorting a green gap-up tape."
            )
        elif raw_intraday_regime == -1:
            regime_substate = "SYNCHRONIZED_BEAR_CRASH"
            regime_reason = (
                f"SYNCHRONIZED_BEAR_CRASH: Core benchmarks below 1m VWAP & MA30 (ml4t/ADWIN) "
                f"(SPY {spy_mv:+.2f}%, QQQ {qqq_mv:+.2f}%, SOXX {soxx_mv:+.2f}%; "
                f"intraday SPY {spy_intraday_mv:+.2f}%, QQQ {qqq_intraday_mv:+.2f}%, "
                f"SOXX {soxx_intraday_mv:+.2f}%)."
            )
        elif raw_intraday_regime == 1:
            regime_substate = "RISK_ON_EXPANSION"
            regime_reason = (
                f"RISK_ON_EXPANSION: Core benchmarks above 1m VWAP & MA30 (ml4t/ADWIN) "
                f"(SPY {spy_mv:+.2f}%, QQQ {qqq_mv:+.2f}%, SOXX {soxx_mv:+.2f}%)."
            )
        else:
            regime_substate = "MIXED_OR_TRANSITIONAL"
            regime_reason = (
                f"MIXED_OR_TRANSITIONAL: Cross-benchmark 1m VWAP/MA30 signals mixed "
                f"(SPY {spy_mv:+.2f}%, QQQ {qqq_mv:+.2f}%, SOXX {soxx_mv:+.2f}%; "
                f"basis={regime_basis})."
            )

        scored: list[dict[str, Any]] = []
        for sym, meta in cands.items():
            sb = bars_map.get(sym) or []
            observed = _ga(sb[-1], "timestamp") if sb else None
            bar_close = observed + timedelta(minutes=1) if observed is not None else None
            px = float(sb[-1].close) if sb else float(meta["price"])
            if px < self.min_price and sym not in pins:
                continue
            bmv = (
                ((float(sb[-1].close) / float(sb[0].open)) - 1.0) * 100.0
                if len(sb) >= 2 and float(sb[0].open) > 0
                else 0.0
            )
            snap_prev_close = prev_closes.get(sym)
            daily_mv = (
                ((px / snap_prev_close) - 1.0) * 100.0
                if snap_prev_close is not None and snap_prev_close > 0.0 and px > 0.0
                else None
            )
            mv = (
                meta["move"]
                if feed == DataFeed.SIP and abs(meta["move"]) > 0.0
                else (daily_mv if daily_mv is not None else bmv)
            )
            vol = max(float(meta["volume"]), sum(_f(_ga(b, "volume")) for b in sb))
            dollar_vol = px * vol
            snap_rvol = _f((sweep_snap_meta.get(sym) or {}).get("tod_rvol"), 0.0)
            rvol = max(
                _compute_same_session_rvol(sb, sess, early_fallback_rvol=self.min_rvol),
                snap_rvol,
            )
            prior_close = (
                snap_prev_close
                if snap_prev_close is not None
                else _resolve_prior_close(sb, sess, px)
            )
            ssr_active = _compute_ssr_active(sb, sess, prior_close, mv)
            annual_borrow_rate = INVERSE_ETF_ANNUAL_EXPENSE_RATES.get(sym, 0.0)
            bar_sess_match = (
                bar_close is not None
                and eng.classify_us_equity_session_et(
                    int(bar_close.timestamp() * 1e9) - 60_000_000_000
                )
                == sess
            )
            is_ext = (
                sess != "CLOSED"
                and bar_close is not None
                and bar_sess_match
                and timedelta(0) <= now - bar_close <= timedelta(minutes=2)
                and abs(mv) >= self.min_move_pct
                and rvol >= self.min_rvol
                and (dollar_vol >= self.min_dollar_vol or sym in pins)
            )
            atr = AverageTrueRange(14)
            for bar in sb:
                atr.update_raw(float(bar.high), float(bar.low), float(bar.close))
            atr_val = float(atr.value) if atr.initialized and atr.value > 0.0 else None
            natr = Decimal(str(atr_val / px)) if atr_val is not None and px > 0 else None
            trf_rec = trf_stats.get(sym)
            trf_val = float(trf_rec["darkpool_trf_ratio"]) if trf_rec is not None else None
            trf_round_lot_ratio = (
                float(trf_rec["trf_round_lot_ratio"]) if trf_rec is not None else None
            )
            trf_block_ratio = float(trf_rec["trf_block_ratio"]) if trf_rec is not None else None
            trf_block_count = int(trf_rec["trf_block_count"]) if trf_rec is not None else 0
            trf_block_notional_usd = (
                round(float(trf_rec["trf_block_notional_usd"]), 2) if trf_rec is not None else 0.0
            )
            lit_iso_sweep_ratio = (
                float(trf_rec["lit_iso_sweep_ratio"]) if trf_rec is not None else None
            )
            lit_iso_sweep_count = int(trf_rec["lit_iso_sweep_count"]) if trf_rec is not None else 0
            trf_vol = round(float(trf_rec["trf_volume"]), 2) if trf_rec is not None else None
            trf_tot_vol = round(float(trf_rec["total_volume"]), 2) if trf_rec is not None else None
            trf_source = str(trf_rec["darkpool_trf_source"]) if trf_rec is not None else None
            trf_as_of = str(trf_rec["darkpool_trf_as_of_utc"]) if trf_rec is not None else None
            vol_buildup_ratio, vol_ramp_bars = _compute_volume_buildup(sb, sess)
            unusual_volume_buildup = bool(
                rvol >= 2.0
                or vol_buildup_ratio >= 1.8
                or (trf_val or 0.0) >= 0.40
                or (lit_iso_sweep_ratio or 0.0) >= 0.25
            )
            hidden_volume_ratio = _compute_hidden_volume_ratio(rvol, sb, atr_val)
            peak_confirmed = _detect_causal_peak_confirmed(sb, sess, natr, rvol)
            sess_sb = [b for b in sb if _bar_session(b) == sess] or list(sb)
            tech = disc.compute_causal_technical_alignment(
                [_f(_ga(b, "close"), px) for b in sess_sb],
                [_f(_ga(b, "high"), px) for b in sess_sb],
                [_f(_ga(b, "low"), px) for b in sess_sb],
                [_f(_ga(b, "volume"), 0.0) for b in sess_sb],
                prior_close=prior_close,
                rvol=rvol,
                vol_buildup_ratio=vol_buildup_ratio,
                trf_block_ratio=trf_block_ratio,
                darkpool_trf_ratio=trf_val,
                session=sess,
                move_pct=mv,
            )
            technical_alignment_score = float(tech["technical_alignment_score"])
            premarket_ignition = bool(tech["premarket_ignition"])
            bar_recent = (
                bar_close is not None
                and bar_sess_match
                and timedelta(0)
                <= now - bar_close
                <= timedelta(minutes=5 if sess == "OVERNIGHT" else 2)
            )
            is_uv_darkpool = (
                sess != "CLOSED"
                and bar_recent
                and abs(mv) >= 1.5
                and dollar_vol >= self.min_dollar_vol
                and (
                    unusual_volume_buildup
                    or (trf_val is not None and trf_val >= 0.35)
                    or (trf_block_ratio is not None and trf_block_ratio >= 0.15)
                    or hidden_volume_ratio >= 1.5
                )
            )
            is_sudden_vol_surge = (
                sess != "CLOSED"
                and bar_recent
                and abs(mv) >= 1.25
                and dollar_vol >= self.min_dollar_vol
                and (
                    vol_buildup_ratio >= 2.0
                    or rvol >= 2.5
                    or (lit_iso_sweep_ratio is not None and lit_iso_sweep_ratio >= 0.25)
                )
            )
            sudden_volume_surge = bool(tech["sudden_volume_surge"] or is_sudden_vol_surge)
            early_ignition_detected = bool(
                tech["early_ignition_detected"]
                or (premarket_ignition and bar_recent)
                or (is_sudden_vol_surge and technical_alignment_score >= 55.0)
            )
            is_pm_ignition = (
                sess == "PRE_MARKET"
                and bar_recent
                and premarket_ignition
                and dollar_vol >= self.min_dollar_vol
            )
            is_tech_breakout_leader = bar_recent and _detect_technical_breakout_leader(
                symbol=sym,
                bars=sb,
                sess=sess,
                mv=mv,
                rvol=rvol,
                vol_buildup_ratio=vol_buildup_ratio,
                atr_val=atr_val,
                dollar_vol=dollar_vol,
                min_dollar_vol=self.min_dollar_vol,
            )
            is_overnight_active = (
                sess == "OVERNIGHT"
                and bar_recent
                and abs(mv) >= 1.5
                and rvol >= 1.25
                and dollar_vol >= 1_000_000.0
            )
            is_liquid_big_mover = (
                sess != "CLOSED"
                and bar_recent
                and abs(mv) >= 5.0
                and dollar_vol >= self.min_dollar_vol
            )
            if not (
                is_ext
                or sym in pins
                or is_uv_darkpool
                or is_sudden_vol_surge
                or is_pm_ignition
                or is_tech_breakout_leader
                or is_overnight_active
                or is_liquid_big_mover
            ):
                continue  # reject flat/sleepy mega-caps (e.g. flat AAPL, flat META)

            watch_tier = (
                "EXTREME_READY"
                if is_ext
                else "PREMARKET_IGNITION"
                if is_pm_ignition
                else "UV_DARKPOOL_BUILDUP"
                if is_uv_darkpool
                else "TECHNICAL_BREAKOUT_LEADER"
                if is_tech_breakout_leader
                else "OVERNIGHT_ACTIVE"
                if is_overnight_active
                else "PINNED_STANDBY"
            )
            if peak_confirmed:
                potential_category = "PARABOLIC_PEAK_REVERSAL"
            elif premarket_ignition:
                potential_category = "PREMARKET_EARLY_IGNITION"
            elif is_ext:
                potential_category = (
                    "CRASH_INVERSE_HEDGE"
                    if (
                        macro_regime == -1
                        or (sym in INVERSE_ETF_ANNUAL_EXPENSE_RATES and mv >= 1.5)
                    )
                    else "EP_MOMENTUM_BURST"
                )
            elif sym in INVERSE_ETF_ANNUAL_EXPENSE_RATES:
                potential_category = (
                    "CRASH_INVERSE_HEDGE"
                    if (macro_regime == -1 or mv >= 1.5)
                    else "CRASH_HEDGE_INVERSE"
                )
            elif (
                vol_buildup_ratio >= 2.0
                or rvol >= 2.5
                or (lit_iso_sweep_ratio is not None and lit_iso_sweep_ratio >= 0.25)
            ):
                potential_category = "SUDDEN_VOLUME_SURGE"
            elif is_tech_breakout_leader:
                potential_category = "TECHNICAL_BREAKOUT_LEADER"
            elif bool(tech["breakout_20d"]):
                potential_category = "VCP_STAGE2_BREAKOUT"
            elif (
                (trf_val is not None and trf_val >= 0.35)
                or (trf_block_ratio is not None and trf_block_ratio >= 0.15)
                or hidden_volume_ratio >= 1.5
            ):
                potential_category = "DARKPOOL_ACCUMULATION"
            elif sym in pins and abs(mv) < 1.5:
                potential_category = "PINNED_STANDBY"
            else:
                potential_category = "EXTREME_MOVER_WATCH"
            side: rk.Side = "SHORT" if mv <= -self.min_move_pct else "LONG"
            sched, sp_map = eng.select_pooled_trailing_schedule(natr or Decimal(0))
            prof = eng.get_session_protection_profile(sess, sp_map)
            stop_px = (
                None
                if sess == "CLOSED" or px <= 0
                else float(
                    rk.compute_capped_stop_price(
                        Decimal(f"{px:.4f}"), side, max_loss_pct=prof.initial_max_loss_pct
                    )
                )
            )
            min_trail_pct = (
                float(
                    compute_symbol_min_trailing_pct(
                        price=Decimal(f"{px:.4f}"),
                        natr_pct=natr if natr is not None else Decimal("0.015"),
                        session=sess,
                        calibrated_min_trailing_pct=symbol_min_trails.get(sym),
                    )
                )
                if px > 0.0
                else float(prof.min_trail_pct)
            )
            calibrated_category = _classify_calibrated_category(
                symbol=sym,
                session=sess,
                is_ext=is_ext,
                is_tech_breakout_leader=is_tech_breakout_leader,
                vol_buildup_ratio=vol_buildup_ratio,
                rvol_normalized=rvol,
                lit_iso_sweep_ratio=lit_iso_sweep_ratio,
                trf_val=trf_val,
                trf_block_ratio=trf_block_ratio,
                hidden_volume_ratio=hidden_volume_ratio,
                uoa_active=False,
                peak_confirmed=peak_confirmed,
                macro_regime=macro_regime,
            )
            technicals = _compute_symbol_technicals(sb, sess, px, atr_val, natr)
            early_detection = {
                "first_detected_utc": now.isoformat(),
                "bar_close_utc": bar_close.isoformat() if bar_close else None,
                "session": sess,
                "rvol_normalized": round(rvol, 2),
                "vol_buildup_ratio": round(vol_buildup_ratio, 2),
                "vol_ramp_bars": vol_ramp_bars,
                "hidden_volume_ratio": hidden_volume_ratio,
                "peak_confirmed": peak_confirmed,
                "calibration_source": weight_meta["source"],
                "calibration_wall_date": weight_meta["wall_date"],
            }
            session_order_presets = (
                build_symbol_24x5_session_presets(
                    symbol=sym,
                    side=side,
                    ref_price=Decimal(f"{px:.4f}"),
                    natr_pct=natr if natr is not None else Decimal("0.015"),
                    calibrated_min_trailing_pct=symbol_min_trails.get(sym),
                )
                if px > 0.0
                else {}
            )
            archetype = _infer_setup_archetype(
                mv=mv,
                rvol=rvol,
                macro_regime=macro_regime,
                trf_val=trf_val,
                unusual_volume_buildup=unusual_volume_buildup,
                hidden_volume_ratio=hidden_volume_ratio,
                peak_confirmed=peak_confirmed,
            )
            if tuning_cfg["loaded"]:
                sc = (
                    disc.score_early_mover_candidate(
                        move_pct=mv,
                        rvol=rvol,
                        vol_buildup_ratio=vol_buildup_ratio,
                        darkpool_trf_ratio=trf_val,
                        trf_block_ratio=trf_block_ratio,
                        hidden_volume_ratio=hidden_volume_ratio,
                        technical_alignment_score=technical_alignment_score,
                        premarket_ignition=premarket_ignition,
                        is_extreme_eligible=is_ext,
                        is_pinned=sym in pins,
                        weights=tuned_weights,
                    )
                    + (lit_iso_sweep_ratio or 0.0) * weights["lit_iso_weight"]
                    + (weights["breakout_leader_bonus"] if is_tech_breakout_leader else 0.0)
                )
            else:
                vol_bonus = min(
                    12.0, max(0.0, vol_buildup_ratio - 1.0) * weights["vol_buildup_weight"]
                )
                sc = (
                    abs(mv) * weights["move_weight"]
                    + math.log1p(max(0.0, rvol)) * weights["rvol_weight"]
                    + (trf_val or 0.0) * weights["trf_weight"]
                    + (trf_block_ratio or 0.0) * weights["trf_block_weight"]
                    + (lit_iso_sweep_ratio or 0.0) * weights["lit_iso_weight"]
                    + vol_bonus
                    + min(5.0, max(0.0, hidden_volume_ratio)) * tuned_weights["w_hvr"]
                    + technical_alignment_score * tuned_weights["w_tech_breakout"]
                    + (tuned_weights["w_premarket_ignition"] if premarket_ignition else 0.0)
                    + (weights["breakout_leader_bonus"] if is_tech_breakout_leader else 0.0)
                    + (25.0 if is_ext else 0.0)
                    + (15.0 if sym in pins else 0.0)
                )
            scored.append(
                {
                    "symbol": sym,
                    "score": round(sc, 2),
                    "last_price": round(px, 4),
                    "prior_close": prior_close,
                    "ssr_active": ssr_active,
                    "annual_borrow_rate": annual_borrow_rate,
                    "session_move_pct": round(mv, 2),
                    "daily_change_pct": round(daily_mv, 2)
                    if daily_mv is not None
                    else round(mv, 2),
                    "intraday_window_pct": round(bmv, 2),
                    "change_pct": round(mv, 2),
                    "rvol_normalized": round(rvol, 2),
                    "rvol": round(rvol, 2),
                    "vol_buildup_ratio": round(vol_buildup_ratio, 2),
                    "vol_ramp_bars": vol_ramp_bars,
                    "unusual_volume_buildup": unusual_volume_buildup,
                    "hidden_volume_ratio": hidden_volume_ratio,
                    "peak_confirmed": peak_confirmed,
                    "technical_breakout_leader": is_tech_breakout_leader,
                    "setup_archetype": archetype,
                    "watch_tier": watch_tier,
                    "potential_category": potential_category,
                    "technical_alignment_score": technical_alignment_score,
                    "early_ignition_detected": early_ignition_detected,
                    "premarket_ignition": premarket_ignition,
                    "sudden_volume_surge": sudden_volume_surge,
                    "tuning_run_id": tuning_run_id,
                    "calibrated_category": calibrated_category,
                    "min_trailing_pct": min_trail_pct,
                    "technicals": technicals,
                    "early_detection": early_detection,
                    "session_order_presets": session_order_presets,
                    "dollar_vol": round(dollar_vol, 2),
                    "darkpool_trf_ratio": trf_val,
                    "trf_round_lot_ratio": trf_round_lot_ratio,
                    "trf_block_ratio": trf_block_ratio,
                    "trf_block_count": trf_block_count,
                    "darkpool_block_count": trf_block_count,
                    "trf_block_notional_usd": trf_block_notional_usd,
                    "lit_iso_sweep_ratio": lit_iso_sweep_ratio,
                    "lit_iso_sweep_count": lit_iso_sweep_count,
                    "darkpool_trf_volume": trf_vol,
                    "darkpool_total_volume": trf_tot_vol,
                    "darkpool_trf_source": trf_source,
                    "darkpool_trf_as_of_utc": trf_as_of,
                    "side": "SELL" if side == "SHORT" else "BUY",
                    "session": sess,
                    "bar_close_utc": bar_close.isoformat() if bar_close else None,
                    "trailing_schedule": sched,
                    "natr_pct": float(natr) if natr is not None else None,
                    "initial_stop_px": stop_px,
                    "extreme_mover_eligible": is_ext,
                    "pinned": sym in pins,
                    "screen_buckets": list(meta["tags"]),
                }
            )

        scored.sort(key=lambda r: (-float(r["score"]), str(r["symbol"])))
        selected = scored[: max(1, int(top_k))]
        selected_syms = [r["symbol"] for r in selected]
        b_map = lookup_alpaca_paper_borrow(selected_syms, self.trading_client, as_of=now)
        selected_with_borrow = [row | b_map.get(row["symbol"], _UNVERIFIED) for row in selected]

        opra_map: dict[str, dict[str, Any]] = {}
        cat_map: dict[str, dict[str, Any]] = {}
        if (
            self.option_client is not None
            or self.news_client is not None
            or (self.corp_actions_client is not None)
        ):
            with ThreadPoolExecutor(max_workers=2) as pool:
                opra_fut = pool.submit(
                    enrich_opra_uoa_gex,
                    selected_with_borrow,
                    option_client=self.option_client,
                    trading_client=self.trading_client,
                    now_utc=now,
                    max_symbols=self.opra_max_symbols,
                    time_budget_secs=self.opra_time_budget_secs,
                )
                cat_fut = pool.submit(
                    enrich_catalyst_and_corp_actions,
                    selected_syms,
                    news_client=self.news_client,
                    corp_actions_client=self.corp_actions_client,
                    now_utc=now,
                )
                opra_map = opra_fut.result()
                cat_map = cat_fut.result()

        sec_map: dict[str, dict[str, Any]] = {}
        sec_meta: dict[str, Any] = {
            "enabled": False,
            "pit_safe": True,
            "allow_network": False,
            "source": "disabled",
            "symbols_count": 0,
            "total_accepted_filings": 0,
            "excluded_future_filings": 0,
        }
        if self.enable_sec_pit_catalysts and selected_syms:
            sec_map, sec_meta = enrich_sec_pit_catalysts(
                selected_syms,
                now_utc=now,
                offline_filings=self.sec_offline_filings,
                allow_network=self.sec_allow_network,
                parquet_dir=self.sec_parquet_dir,
            )

        entries: list[dict[str, Any]] = []
        for i, row in enumerate(selected_with_borrow, 1):
            sym = str(row["symbol"])
            opra = opra_map.get(sym, _EMPTY_OPRA)
            cat = cat_map.get(sym, _EMPTY_CATALYST)
            sec_cat = sec_map.get(
                sym,
                {
                    "sec_catalyst_score": 0.0,
                    "sec_catalyst_summary": dict(_EMPTY_SEC_CATALYST["sec_catalyst_summary"]),
                },
            )
            enriched = row | opra | cat | sec_cat | {"rank": i}
            if bool(opra.get("uoa_active")) or int(cat.get("catalyst_news_24h_count") or 0) > 0:
                enriched["calibrated_category"] = _classify_calibrated_category(
                    symbol=sym,
                    session=sess,
                    is_ext=bool(enriched["extreme_mover_eligible"]),
                    is_tech_breakout_leader=bool(enriched["technical_breakout_leader"]),
                    vol_buildup_ratio=_f(enriched["vol_buildup_ratio"], 0.0),
                    rvol_normalized=_f(enriched["rvol_normalized"], 0.0),
                    lit_iso_sweep_ratio=enriched["lit_iso_sweep_ratio"],
                    trf_val=enriched["darkpool_trf_ratio"],
                    trf_block_ratio=enriched["trf_block_ratio"],
                    hidden_volume_ratio=_f(enriched["hidden_volume_ratio"], 0.0),
                    uoa_active=bool(opra.get("uoa_active")),
                    peak_confirmed=bool(enriched["peak_confirmed"]),
                    macro_regime=macro_regime,
                    catalyst_news_count=int(cat.get("catalyst_news_24h_count") or 0),
                )
            if bool(opra.get("uoa_active")) and enriched["potential_category"] in {
                "EXTREME_MOVER_WATCH",
                "PINNED_STANDBY",
            }:
                enriched["potential_category"] = "DARKPOOL_ACCUMULATION"
            elif (
                int(cat.get("catalyst_news_24h_count") or 0) > 0
                and abs(_f(enriched["session_move_pct"], 0.0)) >= 3.0
                and enriched["potential_category"] in {"EXTREME_MOVER_WATCH", "PINNED_STANDBY"}
            ):
                enriched["potential_category"] = "PEAD_DRIFT"
            daily_disp = _f(enriched.get("daily_change_pct", enriched["session_move_pct"]), 0.0)
            intra_disp = _f(enriched.get("intraday_window_pct", 0.0), 0.0)
            reason_parts = [
                f"{daily_disp:+.2f}% daily (intraday {intra_disp:+.2f}%)",
                f"RVOL {_f(enriched['rvol'], 0.0):.2f}x",
                f"VolBuildup {_f(enriched['vol_buildup_ratio'], 0.0):.2f}x",
                f"TA {_f(enriched['technical_alignment_score'], 0.0):.0f}",
                str(enriched.get("calibrated_category") or enriched["potential_category"]),
            ]
            if enriched.get("darkpool_trf_ratio") is not None:
                reason_parts.append(f"TRF {_f(enriched['darkpool_trf_ratio'], 0.0) * 100.0:.0f}%")
            if bool(opra.get("uoa_active")):
                reason_parts.append("OPRA UOA")
            if int(cat.get("catalyst_news_24h_count") or 0) > 0:
                reason_parts.append(f"News({int(cat['catalyst_news_24h_count'])})")
            if _f(sec_cat.get("sec_catalyst_score"), 0.0) > 0.0:
                reason_parts.append(f"SEC({_f(sec_cat['sec_catalyst_score'], 0.0):.1f})")
            reason_parts.append(f"Macro:{regime_substate}")
            enriched["market_feed_reason"] = regime_reason
            enriched["ranking_reason"] = " | ".join(reason_parts)
            entries.append(enriched)

        def _attach_sweep_reasons(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out_rows: list[dict[str, Any]] = []
            for r in rows:
                d_chg = _f(r.get("daily_change_pct"), 0.0)
                t_rvol = _f(r.get("tod_rvol"), 0.0)
                d_vol_m = _f(r.get("dollar_vol"), 0.0) / 1_000_000.0
                parts = [
                    f"{d_chg:+.2f}% daily",
                    f"PaceRVOL {t_rvol:.1f}x",
                    f"Vol {_f(r.get('daily_volume'), 0.0) / 1_000_000.0:.2f}M (${d_vol_m:.1f}M)",
                    str(r.get("stage") or "EARLY_STAGE_WATCH"),
                    str(r.get("price_tier") or "SMALLCAP_EARLY_1_TO_10"),
                ]
                if r.get("vwap_dist_pct") is not None:
                    parts.append(f"VWAP {_f(r['vwap_dist_pct'], 0.0):+.2f}%")
                parts.append(f"Macro:{regime_substate}")
                out_rows.append(
                    r
                    | {
                        "market_feed_reason": regime_reason,
                        "ranking_reason": " | ".join(parts),
                    }
                )
            return out_rows

        full_market_top = _attach_sweep_reasons(full_market_top)
        early_stage_top = _attach_sweep_reasons(early_stage_top)

        feed_sources = (
            (["alpaca_boats", "finra_trf_exchange_d"] if trf_stats else ["alpaca_boats"])
            if feed == DataFeed.BOATS
            else ["alpaca_sip", "finra_trf_exchange_d"]
        )
        if opra_map:
            feed_sources.append("alpaca_opra_options")
        if self.news_client is not None and cat_map:
            feed_sources.append("alpaca_news_fiscal_ai")
        if self.corp_actions_client is not None and cat_map:
            feed_sources.append("alpaca_corporate_actions")
        manifest = {
            "schema_version": "tier0.whole-market-movers.v2",
            "generated_at_utc": now.isoformat(),
            "feed_sources": feed_sources,
            "early_weights_calibration": weight_meta,
            "sec_pit_catalysts": sec_meta,
            "discovery_summary": {
                "raw_candidates_count": len(raw_cands),
                "enriched_candidates_count": len(cands),
                "max_enrichment_candidates": int(self.max_enrichment_candidates),
            },
            "full_market_sweep_summary": sweep_summary,
            "early_stage_ignition_movers": early_stage_top,
            "full_market_top_movers": full_market_top,
            "macro_regime": {
                "regime": macro_regime,
                "raw_intraday_regime": raw_intraday_regime,
                "regime_label": {-1: "CRASH", 0: "NEUTRAL", 1: "BULL"}[macro_regime],
                "regime_substate": regime_substate,
                "regime_reason": regime_reason,
                "spy_session_pct": spy_mv,
                "qqq_session_pct": qqq_mv,
                "soxx_session_pct": soxx_mv,
                "rsp_session_pct": rsp_mv,
                "spy_daily_pct": spy_daily_mv,
                "qqq_daily_pct": qqq_daily_mv,
                "soxx_daily_pct": soxx_daily_mv,
                "rsp_daily_pct": rsp_daily_mv,
                "spy_intraday_pct": spy_intraday_mv,
                "qqq_intraday_pct": qqq_intraday_mv,
                "soxx_intraday_pct": soxx_intraday_mv,
                "rsp_intraday_pct": rsp_intraday_mv,
                "fresh_equity_benchmarks": fresh,
                "core_benchmarks_used": [bm for bm in CORE_REGIME_BENCHMARKS if bm in regime_ready],
                "regime_basis": regime_basis,
                "vix_available": False,
            },
            "pin_symbols": list(self.pin_symbols),
            "early_mover_tuning": {
                "loaded": bool(tuning_cfg["loaded"]),
                "source": tuning_cfg["source"],
                "tuning_run_id": tuning_run_id,
                "weights": tuned_weights,
                "generated_at_utc": tuning_cfg["generated_at_utc"],
                "prewall_recall_at_top15": tuning_cfg["prewall_recall_at_top15"],
                "prewall_median_ttd_mins": tuning_cfg["prewall_median_ttd_mins"],
            },
            "ranked_count": len(entries),
            "entries": entries,
        }
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            tmp.replace(output_path)
        return manifest


def build_live_lane_preset_bundle(
    scan_doc: Mapping[str, Any],
    *,
    lanes: Mapping[str, Any] | None = None,
    output_path: Path | None = None,
    tca_allows_entry: bool = True,
) -> dict[str, Any]:
    """Build deterministic `live_lane_order_preset_bundle.json` bridging Paper <-> TV <-> FINOS."""
    from tier0_sota.live.paper_farm import (
        ALPACA_LANE_PROFILES,
        build_symbol_24x5_session_presets,
        compute_fast_reverse_trigger,
        compute_symbol_min_trailing_pct,
        preferred_situation_lane_order,
        resolve_alpaca_paper_venue_profile,
        select_short_or_inverse_fallback,
    )

    alpaca_venue_profile, alpaca_max_rpm = resolve_alpaca_paper_venue_profile()
    lane_map: Mapping[str, Any] = ALPACA_LANE_PROFILES if lanes is None else lanes
    macro = dict(scan_doc.get("macro_regime") or {})
    macro_regime = int(macro.get("regime", 0))
    entries = [e for e in (scan_doc.get("entries") or ()) if isinstance(e, Mapping)]
    active_session = str(entries[0].get("session") if entries else "CLOSED").strip().upper()
    top_symbols: list[dict[str, Any]] = []

    for row in entries:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        sess = str(row.get("session") or active_session or "CLOSED").strip().upper()
        natr_raw = row.get("natr_pct")
        natr_d = Decimal(str(natr_raw)) if natr_raw is not None else Decimal("0.015")
        rvol_raw = row.get("rvol_normalized")
        sched_name, sp_map = eng.select_pooled_trailing_schedule(natr_d)
        prof = eng.get_session_protection_profile(sess, sp_map)
        route = select_short_or_inverse_fallback(
            sym, market_regime=macro_regime, borrow_meta=dict(row), session=sess
        )
        origin = route[2] if route else ""
        routed_side: rk.Side = (
            route[1] if route else ("SHORT" if row.get("side") == "SELL" else "LONG")
        )

        order = preferred_situation_lane_order(
            origin=origin, natr_pct=natr_raw, rvol_normalized=rvol_raw
        )
        lane_account: str | None = None
        lane_id: str | None = None
        for acct in (*order, *(k for k in lane_map if k not in order)):
            candidate = lane_map.get(acct)
            if candidate is None:
                continue
            breaker = getattr(candidate, "breaker", None)
            if breaker is not None and not breaker.allow_entry():
                continue
            lane_account = acct
            if isinstance(candidate, Mapping):
                lane_id = str(candidate.get("lane_id") or acct)
            else:
                lane_id = str(getattr(candidate, "profile", {}).get("lane_id") or acct)
            break

        watch_tier = str(row.get("watch_tier") or "")
        entry_eligible = False
        entry_hold_reason: str | None = None
        if sess == "CLOSED":
            entry_hold_reason = "SESSION_CLOSED"
        elif not tca_allows_entry:
            entry_hold_reason = "TCA_RECALIBRATION_REQUIRED"
        elif row.get("extreme_mover_eligible") is not True:
            if watch_tier == "PREMARKET_IGNITION":
                entry_hold_reason = "PREMARKET_EARLY_IGNITION_WATCH"
            elif watch_tier == "UV_DARKPOOL_BUILDUP":
                entry_hold_reason = "UV_DARKPOOL_EARLY_BUILDUP"
            elif watch_tier == "TECHNICAL_BREAKOUT_LEADER":
                entry_hold_reason = "TECHNICAL_BREAKOUT_EARLY_BUILDUP"
            else:
                mv_abs = abs(_f(row.get("session_move_pct"), 0.0))
                entry_hold_reason = (
                    "LOW_RVOL_NOT_EXTREME" if mv_abs >= 3.5 else "LOW_MOVE_NOT_EXTREME"
                )
        elif lane_account is None:
            entry_hold_reason = "DAILY_DRAWDOWN_BREAKER_TRIPPED"
        elif route is None:
            entry_hold_reason = "MACRO_REGIME_NEUTRAL"
        else:
            entry_eligible = True

        px_val = _f(row.get("last_price"), 0.0)
        be_plus_fee_px: float | None = None
        fast_sell_rev_trigger: float | None = None
        min_trail_d = Decimal(str(prof.min_trail_pct))
        session_presets: dict[str, dict[str, Any]] = {}
        if px_val > 0.0:
            px_d = Decimal(f"{px_val:.4f}")
            cal_trail = row.get("min_trailing_pct")
            min_trail_d = compute_symbol_min_trailing_pct(
                price=px_d,
                natr_pct=natr_d,
                session=sess,
                calibrated_min_trailing_pct=cal_trail,
            )
            be_mult = Decimal("1.0010") if routed_side == "LONG" else Decimal("0.9990")
            be_plus_fee_px = float(
                rk.quantize_rule_612(
                    max(rk.TICK_SUB_DOLLAR, px_d * be_mult), side=routed_side, is_stop=True
                )
            )
            fast_sell_rev_trigger = float(
                compute_fast_reverse_trigger(px_d, routed_side, min_trail_d)
            )
            session_presets = build_symbol_24x5_session_presets(
                symbol=sym,
                side=routed_side,
                ref_price=px_d,
                natr_pct=natr_d,
                calibrated_min_trailing_pct=cal_trail,
                alpaca_venue_profile=alpaca_venue_profile,
                alpaca_max_rpm=alpaca_max_rpm,
            )

        top_symbols.append(
            {
                "symbol": sym,
                "exchange": row.get("exchange"),
                "rank": row.get("rank"),
                "watch_tier": watch_tier or None,
                "potential_category": row.get("potential_category"),
                "technical_alignment_score": row.get("technical_alignment_score"),
                "early_ignition_detected": bool(row.get("early_ignition_detected")),
                "premarket_ignition": bool(row.get("premarket_ignition")),
                "sudden_volume_surge": bool(row.get("sudden_volume_surge")),
                "tuning_run_id": row.get("tuning_run_id"),
                "calibrated_category": row.get("calibrated_category"),
                "setup_archetype": row.get("setup_archetype"),
                "lane_account": lane_account,
                "lane_id": lane_id,
                "venue_profile": alpaca_venue_profile,
                "side": routed_side,
                "origin": origin or None,
                "entry_eligible": entry_eligible,
                "entry_hold_reason": entry_hold_reason,
                "last_price": row.get("last_price"),
                "session_move_pct": row.get("session_move_pct"),
                "rvol_normalized": rvol_raw,
                "trailing_schedule": sched_name,
                "min_trailing_pct": float(min_trail_d),
                "order_mode": prof.order_mode,
                "collar_bps": float(prof.limit_collar_bps),
                "stop_price": row.get("initial_stop_px"),
                "prior_close": row.get("prior_close"),
                "ssr_active": bool(row.get("ssr_active")),
                "annual_borrow_rate": row.get("annual_borrow_rate"),
                "darkpool_trf_ratio": row.get("darkpool_trf_ratio"),
                "trf_block_ratio": row.get("trf_block_ratio"),
                "trf_block_count": int(
                    row.get("trf_block_count", row.get("darkpool_block_count") or 0)
                ),
                "darkpool_block_count": int(row.get("darkpool_block_count") or 0),
                "trf_block_notional_usd": row.get("trf_block_notional_usd"),
                "lit_iso_sweep_ratio": row.get("lit_iso_sweep_ratio"),
                "darkpool_trf_source": row.get("darkpool_trf_source"),
                "vol_buildup_ratio": row.get("vol_buildup_ratio"),
                "vol_ramp_bars": int(row.get("vol_ramp_bars") or 0),
                "unusual_volume_buildup": bool(row.get("unusual_volume_buildup")),
                "hidden_volume_ratio": row.get("hidden_volume_ratio"),
                "peak_confirmed": bool(row.get("peak_confirmed")),
                "technicals": row.get("technicals"),
                "early_detection": row.get("early_detection"),
                "uoa_vol_oi_ratio": row.get("uoa_vol_oi_ratio"),
                "uoa_call_put_ratio": row.get("uoa_call_put_ratio"),
                "net_dealer_gex_usd": row.get("net_dealer_gex_usd"),
                "iv_skew_25d": row.get("iv_skew_25d"),
                "uoa_active": bool(row.get("uoa_active")),
                "catalyst_news_24h_count": int(row.get("catalyst_news_24h_count") or 0),
                "latest_catalyst_utc": row.get("latest_catalyst_utc"),
                "corp_action_flag": row.get("corp_action_flag"),
                "sec_catalyst_score": _f(row.get("sec_catalyst_score"), 0.0),
                "sec_catalyst_summary": row.get("sec_catalyst_summary"),
                "shortable": bool(row.get("shortable")),
                "easy_to_borrow": bool(row.get("easy_to_borrow")),
                "lossless_trailing": {
                    "trailing_schedule": sched_name,
                    "initial_max_loss_pct": float(prof.initial_max_loss_pct),
                    "min_trailing_pct": float(min_trail_d),
                    "be_plus_fee_price": be_plus_fee_px,
                    "order_mode": prof.order_mode,
                    "collar_bps": float(prof.limit_collar_bps),
                    "synthetic_stop_ratchet": True,
                },
                "session_order_presets": session_presets,
                "fast_sell_non_noise_reverse_trigger": fast_sell_rev_trigger,
                "auto_reverse_flatten_first": True,
                "auto_reentry": True,
                "reentry_cooldown_secs": 60,
            }
        )

    bundle = {
        "schema_version": "tier0.live-lane-preset-bundle.v1",
        "generated_at_utc": scan_doc.get("generated_at_utc") or datetime.now(UTC).isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
        "venue_profile": alpaca_venue_profile,
        "alpaca_per_lane_max_rpm": alpaca_max_rpm,
        "session": active_session,
        "macro_regime": macro,
        "tca_calibration_ok": bool(tca_allows_entry),
        "top_potential_symbols": top_symbols,
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(bundle, indent=2) + "\n", encoding="utf-8")
        tmp.replace(output_path)
    return bundle


def scan_extreme_movers(**kw: Any) -> dict[str, Any]:
    """Convenience entrypoint running `ExtremeMoverScanner.from_env().scan(...)`."""
    acct = str(kw.pop("account", "ALPACA_PA1"))
    top_k, out = int(kw.pop("top_k", 15)), kw.pop("output_path", None)
    doc = ExtremeMoverScanner.from_env(account=acct, **kw).scan(top_k=top_k, output_path=out)
    if out is not None:
        out_p = Path(out)
        build_live_lane_preset_bundle(
            doc, output_path=out_p.parent / "live_lane_order_preset_bundle.json"
        )
    return doc

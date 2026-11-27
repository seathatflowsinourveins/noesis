"""Whole-market live extreme-mover scanner (Alpaca SIP + FINRA TRF + ETB lookup)."""

from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import alpaca.data as ad
import alpaca.trading as at
import polars as pl
from alpaca.common.enums import Sort
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed, Exchange, MostActivesBy
from alpaca.data.requests import (
    MarketMoversRequest,
    MostActivesRequest,
    StockBarsRequest,
    StockTradesRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.enums import AssetBorrowStatus, AssetStatus
from alpaca.trading.requests import GetAssetsRequest
from nautilus_trader.indicators import AverageTrueRange

from tier0_sota import engine as eng
from tier0_sota import risk as rk

_NY_TZ = ZoneInfo("America/New_York")
DEFAULT_PIN_SYMBOLS: tuple[str, ...] = ("SOXL", "SOXS", "TQQQ", "SQQQ")
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
INVERSE_ETF_ANNUAL_EXPENSE_RATES: dict[str, float] = {
    "SOXS": 0.0099,
    "SQQQ": 0.0095,
    "SPXS": 0.0101,
    "SDOW": 0.0095,
}
CORE_REGIME_BENCHMARKS = ("SPY", "QQQ", "SOXX")
EQUITY_BENCHMARKS = ("SPY", "QQQ", "SOXX", "RSP")
_TV_EXCHANGE_MAP: dict[str, str] = {
    "NASDAQ": "NASDAQ",
    "NYSE": "NYSE",
    "AMEX": "AMEX",
    "ARCA": "AMEX",
    "NYSEARCA": "AMEX",
    "BATS": "BATS",
    "IEX": "IEX",
    "OTC": "OTC",
}
VALID_EQUITY_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,6}$")
_UNVERIFIED = {
    "shortable": False,
    "easy_to_borrow": False,
    "borrow_source": None,
    "borrow_as_of_utc": None,
    "short_fee_bps": 30.0,
    "exchange": None,
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


def aggregate_finra_trf_ratios(trades: Sequence[Any]) -> dict[str, dict[str, Any]]:
    """Compute FINRA TRF (Exchange.D) darkpool share & institutional round-lot/block metrics."""
    out: dict[str, dict[str, Any]] = {}
    for tr in trades:
        sym = str(_ga(tr, "symbol", "") or "").strip().upper()
        sz = _f(_ga(tr, "size"), 0.0)
        if not sym or sz <= 0.0:
            continue
        px = _f(_ga(tr, "price"), 0.0)
        rec = out.setdefault(
            sym,
            {
                "trf_volume": 0.0,
                "trf_round_lot_volume": 0.0,
                "trf_block_volume": 0.0,
                "darkpool_block_count": 0,
                "total_volume": 0.0,
                "darkpool_trf_ratio": 0.0,
                "trf_block_ratio": 0.0,
            },
        )
        rec["total_volume"] += sz
        ex = _ga(tr, "exchange")
        if str(_ga(ex, "value", ex) or "").strip().upper() == Exchange.D.value:
            rec["trf_volume"] += sz
            if sz >= 100.0:
                rec["trf_round_lot_volume"] += sz
            if sz >= 500.0 or (px > 0.0 and sz * px >= 50_000.0):
                rec["trf_block_volume"] += sz
                rec["darkpool_block_count"] += 1
        denom = max(1.0, float(rec["total_volume"]))
        rec["darkpool_trf_ratio"] = round(float(rec["trf_volume"]) / denom, 4)
        rec["trf_block_ratio"] = round(float(rec["trf_round_lot_volume"]) / denom, 4)
    return out


def lookup_alpaca_paper_borrow(
    symbols: Sequence[str], trading_client: Any, *, as_of: datetime | None = None
) -> dict[str, dict[str, Any]]:
    """Post-selection `alpaca_paper_asset` shortable, ETB borrow & TV exchange verification."""
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
        sh, bs = bool(_ga(asset, "shortable", False)), _ga(asset, "borrow_status")
        etb = (
            bs == AssetBorrowStatus.EASY_TO_BORROW
            or str(bs).strip().lower().endswith("easy_to_borrow")
            if bs is not None
            else bool(_ga(asset, "easy_to_borrow", False))
        )
        out[sym] = {
            "shortable": sh,
            "easy_to_borrow": sh and etb,
            "borrow_source": "alpaca_paper_asset",
            "borrow_as_of_utc": now_iso,
            "short_fee_bps": 0.0 if (sh and etb) else 30.0,
            "exchange": normalize_tv_exchange(_ga(asset, "exchange")),
        }
    return out


@dataclass(frozen=True)
class ExtremeMoverScanner:
    """Alpaca SIP + FINRA TRF whole-market extreme-mover scanner (zero banned libs)."""

    screener_client: Any | None = None
    data_client: Any | None = None
    trading_client: Any | None = None
    pin_symbols: tuple[str, ...] = DEFAULT_PIN_SYMBOLS
    min_move_pct: float = 3.5
    min_rvol: float = 1.5
    min_price: float = 10.0
    min_dollar_vol: float = 5_000_000.0

    @classmethod
    def from_env(cls, account: str = "ALPACA_PA1", **kw: Any) -> ExtremeMoverScanner:
        key, sec = resolve_alpaca_env_creds(account)
        if not (key and sec):
            raise RuntimeError(f"Missing Alpaca Paper credentials in env for {account}")
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
            with ThreadPoolExecutor(max_workers=2) as pool:
                act_fut = pool.submit(
                    sc.get_most_actives, MostActivesRequest(top=30, by=MostActivesBy.VOLUME)
                )
                mov_fut = pool.submit(sc.get_market_movers, MarketMoversRequest(top=30))
                act, mov = act_fut.result(), mov_fut.result()
            for src, attr, tag in (
                (act, "most_actives", "alpaca_most_actives"),
                (mov, "gainers", "alpaca_gainers"),
                (mov, "losers", "alpaca_losers"),
            ):
                for item in _ga(src, attr) or ():
                    sym = str(_ga(item, "symbol", "") or "").strip().upper()
                    if not sym or not VALID_EQUITY_RE.match(sym):
                        continue
                    px, vol, chg = (
                        _f(_ga(item, "price")),
                        _f(_ga(item, "volume")),
                        _f(_ga(item, "percent_change")),
                    )
                    rec = cands.setdefault(
                        sym, {"symbol": sym, "price": 0.0, "volume": 0.0, "move": 0.0, "tags": []}
                    )
                    rec["price"], rec["volume"] = max(rec["price"], px), max(rec["volume"], vol)
                    if abs(chg) > abs(rec["move"]):
                        rec["move"] = chg
                    if tag not in rec["tags"]:
                        rec["tags"].append(tag)
        for pin in self.pin_symbols:
            if (p := pin.strip().upper()) and VALID_EQUITY_RE.match(p):
                cands.setdefault(
                    p, {"symbol": p, "price": 0.0, "volume": 0.0, "move": 0.0, "tags": ["pinned"]}
                )
        for seed in extra_symbols:
            if (s := seed.strip().upper()) and VALID_EQUITY_RE.match(s):
                rec = cands.setdefault(
                    s, {"symbol": s, "price": 0.0, "volume": 0.0, "move": 0.0, "tags": []}
                )
                if "overnight_seed" not in rec["tags"]:
                    rec["tags"].append("overnight_seed")
        return cands

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
        bars_req = StockBarsRequest(
            symbol_or_symbols=sorted({*syms, *EQUITY_BENCHMARKS}),
            timeframe=TimeFrame.Minute,
            start=now_utc - timedelta(hours=6),
            # Alpaca timestamps minute bars at the start of the interval.
            end=now_utc.replace(second=0, microsecond=0) - timedelta(minutes=1),
            feed=feed,
        )
        dc = self.data_client
        if not fetch_trades or not hasattr(dc, "get_stock_trades"):
            bars_r = dc.get_stock_bars(bars_req)
            return dict(_ga(bars_r, "data") or {}), {}
        if feed == DataFeed.BOATS:
            bars_r = dc.get_stock_bars(bars_req)
            bars_map = dict(_ga(bars_r, "data") or {})
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
                bars_fut = pool.submit(dc.get_stock_bars, bars_req)
                trf_fut = pool.submit(
                    self._fetch_trf_window,
                    syms,
                    start_utc=tr_start,
                    end_utc=tr_end,
                    source="live_sip_trf_15m",
                    as_of_iso=tr_end.isoformat(),
                    limit=1000,
                )
                bars_r, trf_stats = bars_fut.result(), trf_fut.result()
            return dict(_ga(bars_r, "data") or {}), trf_stats

        bars_r = dc.get_stock_bars(bars_req)
        bars_map = dict(_ga(bars_r, "data") or {})
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
        """Run whole-market Alpaca SIP/BOATS + FINRA TRF scan + post-selection ETB lookup."""
        now = now_utc or datetime.now(UTC)
        sess = eng.classify_us_equity_session_et(int(now.timestamp() * 1_000_000_000))
        overnight_seeds = (
            (*OVERNIGHT_SEED_SYMBOLS, *DEFAULT_PIN_SYMBOLS) if sess == "OVERNIGHT" else ()
        )
        cands = self._discover_raw(
            include_screener=(sess != "CLOSED"),
            extra_symbols=overnight_seeds,
        )
        pins = {p.strip().upper() for p in self.pin_symbols if p.strip()}
        feed = DataFeed.BOATS if sess == "OVERNIGHT" else DataFeed.SIP
        bars_map, trf_stats = self._enrich_sip(
            sorted(cands),
            now,
            feed,
            fetch_trades=(sess != "CLOSED"),
            priority_symbols=sorted(pins),
        )

        def _bench_mv(bm_sym: str) -> float:
            b_list = bars_map.get(bm_sym) or []
            if len(b_list) >= 2 and _f(_ga(b_list[0], "open"), 0.0) > 0.0:
                return round(
                    ((_f(_ga(b_list[-1], "close")) / _f(_ga(b_list[0], "open"))) - 1.0) * 100.0,
                    3,
                )
            return 0.0

        spy_mv = _bench_mv("SPY")
        qqq_mv = _bench_mv("QQQ")
        soxx_mv = _bench_mv("SOXX")
        rsp_mv = _bench_mv("RSP")

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
            mv = meta["move"] if feed == DataFeed.SIP and abs(meta["move"]) > 0.0 else bmv
            vol = max(float(meta["volume"]), sum(_f(_ga(b, "volume")) for b in sb))
            rvol = _compute_same_session_rvol(sb, sess, early_fallback_rvol=self.min_rvol)
            prior_close = _resolve_prior_close(sb, sess, px)
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
                and timedelta(0) <= now - bar_close < timedelta(minutes=1)
                and abs(mv) >= self.min_move_pct
                and rvol >= self.min_rvol
                and (px * vol >= self.min_dollar_vol or sym in pins)
            )
            atr = AverageTrueRange(14)
            for bar in sb:
                atr.update_raw(float(bar.high), float(bar.low), float(bar.close))
            atr_val = float(atr.value) if atr.initialized and atr.value > 0.0 else None
            natr = Decimal(str(atr_val / px)) if atr_val is not None and px > 0 else None
            trf_rec = trf_stats.get(sym)
            trf_val = float(trf_rec["darkpool_trf_ratio"]) if trf_rec is not None else None
            trf_block_ratio = float(trf_rec["trf_block_ratio"]) if trf_rec is not None else None
            darkpool_block_count = (
                int(trf_rec["darkpool_block_count"]) if trf_rec is not None else 0
            )
            trf_vol = round(float(trf_rec["trf_volume"]), 2) if trf_rec is not None else None
            trf_tot_vol = round(float(trf_rec["total_volume"]), 2) if trf_rec is not None else None
            trf_source = str(trf_rec["darkpool_trf_source"]) if trf_rec is not None else None
            trf_as_of = str(trf_rec["darkpool_trf_as_of_utc"]) if trf_rec is not None else None
            vol_buildup_ratio, vol_ramp_bars = _compute_volume_buildup(sb, sess)
            unusual_volume_buildup = bool(
                rvol >= 2.0 or vol_buildup_ratio >= 1.8 or (trf_val or 0.0) >= 0.40
            )
            hidden_volume_ratio = _compute_hidden_volume_ratio(rvol, sb, atr_val)
            peak_confirmed = _detect_causal_peak_confirmed(sb, sess, natr, rvol)
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
                and px * vol >= self.min_dollar_vol
                and (
                    unusual_volume_buildup
                    or (trf_val is not None and trf_val >= 0.35)
                    or hidden_volume_ratio >= 1.5
                )
            )
            is_overnight_active = (
                sess == "OVERNIGHT"
                and bar_recent
                and abs(mv) >= 1.5
                and rvol >= 1.25
                and px * vol >= 1_000_000.0
            )
            if not (is_ext or sym in pins or is_uv_darkpool or is_overnight_active):
                continue  # reject flat/sleepy mega-caps (e.g. flat AAPL, flat META)

            watch_tier = (
                "EXTREME_READY"
                if is_ext
                else "UV_DARKPOOL_BUILDUP"
                if is_uv_darkpool
                else "OVERNIGHT_ACTIVE"
                if is_overnight_active
                else "PINNED_STANDBY"
            )
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
            archetype = _infer_setup_archetype(
                mv=mv,
                rvol=rvol,
                macro_regime=macro_regime,
                trf_val=trf_val,
                unusual_volume_buildup=unusual_volume_buildup,
                hidden_volume_ratio=hidden_volume_ratio,
                peak_confirmed=peak_confirmed,
            )
            vol_bonus = min(12.0, max(0.0, vol_buildup_ratio - 1.0) * 4.0)
            sc = (
                abs(mv) * 2.0
                + math.log1p(max(0.0, rvol)) * 10.0
                + (trf_val or 0.0) * 8.0
                + (trf_block_ratio or 0.0) * 6.0
                + vol_bonus
                + (25.0 if is_ext else 0.0)
                + (15.0 if sym in pins else 0.0)
            )
            scored.append(
                {
                    "symbol": sym,
                    "score": round(sc, 2),
                    "last_price": round(px, 4),
                    "prior_close": prior_close,
                    "annual_borrow_rate": annual_borrow_rate,
                    "session_move_pct": round(mv, 2),
                    "change_pct": round(mv, 2),
                    "rvol_normalized": round(rvol, 2),
                    "rvol": round(rvol, 2),
                    "vol_buildup_ratio": round(vol_buildup_ratio, 2),
                    "vol_ramp_bars": vol_ramp_bars,
                    "unusual_volume_buildup": unusual_volume_buildup,
                    "hidden_volume_ratio": hidden_volume_ratio,
                    "peak_confirmed": peak_confirmed,
                    "setup_archetype": archetype,
                    "watch_tier": watch_tier,
                    "dollar_vol": round(px * vol, 2),
                    "darkpool_trf_ratio": trf_val,
                    "trf_block_ratio": trf_block_ratio,
                    "darkpool_block_count": darkpool_block_count,
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
        b_map = lookup_alpaca_paper_borrow(
            [r["symbol"] for r in selected], self.trading_client, as_of=now
        )
        entries = [
            row | b_map.get(row["symbol"], _UNVERIFIED) | {"rank": i}
            for i, row in enumerate(selected, 1)
        ]
        feed_sources = (
            (["alpaca_boats", "finra_trf_exchange_d"] if trf_stats else ["alpaca_boats"])
            if feed == DataFeed.BOATS
            else ["alpaca_sip", "finra_trf_exchange_d"]
        )
        manifest = {
            "schema_version": "tier0.whole-market-movers.v2",
            "generated_at_utc": now.isoformat(),
            "feed_sources": feed_sources,
            "macro_regime": {
                "regime": macro_regime,
                "regime_label": {-1: "CRASH", 0: "NEUTRAL", 1: "BULL"}[macro_regime],
                "spy_session_pct": spy_mv,
                "qqq_session_pct": qqq_mv,
                "soxx_session_pct": soxx_mv,
                "rsp_session_pct": rsp_mv,
                "fresh_equity_benchmarks": fresh,
                "core_benchmarks_used": [bm for bm in CORE_REGIME_BENCHMARKS if bm in regime_ready],
                "regime_basis": regime_basis,
                "vix_available": False,
            },
            "pin_symbols": list(self.pin_symbols),
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
        preferred_situation_lane_order,
        select_short_or_inverse_fallback,
    )

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
        routed_side = route[1] if route else ("SHORT" if row.get("side") == "SELL" else "LONG")

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
            if watch_tier == "UV_DARKPOOL_BUILDUP":
                entry_hold_reason = "UV_DARKPOOL_EARLY_BUILDUP"
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

        top_symbols.append(
            {
                "symbol": sym,
                "exchange": row.get("exchange"),
                "rank": row.get("rank"),
                "watch_tier": watch_tier or None,
                "setup_archetype": row.get("setup_archetype"),
                "lane_account": lane_account,
                "lane_id": lane_id,
                "side": routed_side,
                "origin": origin or None,
                "entry_eligible": entry_eligible,
                "entry_hold_reason": entry_hold_reason,
                "last_price": row.get("last_price"),
                "session_move_pct": row.get("session_move_pct"),
                "rvol_normalized": rvol_raw,
                "trailing_schedule": sched_name,
                "order_mode": prof.order_mode,
                "collar_bps": float(prof.limit_collar_bps),
                "stop_price": row.get("initial_stop_px"),
                "prior_close": row.get("prior_close"),
                "annual_borrow_rate": row.get("annual_borrow_rate"),
                "darkpool_trf_ratio": row.get("darkpool_trf_ratio"),
                "trf_block_ratio": row.get("trf_block_ratio"),
                "darkpool_block_count": int(row.get("darkpool_block_count") or 0),
                "darkpool_trf_source": row.get("darkpool_trf_source"),
                "vol_buildup_ratio": row.get("vol_buildup_ratio"),
                "vol_ramp_bars": int(row.get("vol_ramp_bars") or 0),
                "unusual_volume_buildup": bool(row.get("unusual_volume_buildup")),
                "hidden_volume_ratio": row.get("hidden_volume_ratio"),
                "peak_confirmed": bool(row.get("peak_confirmed")),
                "shortable": bool(row.get("shortable")),
                "easy_to_borrow": bool(row.get("easy_to_borrow")),
            }
        )

    bundle = {
        "schema_version": "tier0.live-lane-preset-bundle.v1",
        "generated_at_utc": scan_doc.get("generated_at_utc") or datetime.now(UTC).isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
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

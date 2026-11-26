"""Strict causal PIT (<= 2026-10-15, t_ns+60s, XNYS), multi-feed regime & upstream signals."""

from __future__ import annotations

import contextlib
import io
import math
import os
from collections import deque
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Self
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import lightgbm as lgb
import numpy as np
import pandera.polars as pa
import polars as pl
import ruptures as rpt
import shap
import stumpy
from catboost import CatBoostRegressor
from fredapi import Fred
from ml4t.engineer import StandardScaler, compute_features
from ml4t.engineer.features.regime import market_regime_classifier
from nautilus_trader import model as nm
from nautilus_trader.common import DataActor, DataActorConfig
from nautilus_trader.indicators import SimpleMovingAverage, VolumeWeightedAveragePrice
from nautilus_trader.persistence import BarDataWrangler
from river.drift import ADWIN

_RAW_WALL_ISO = os.environ.get("TIER0_PREWALL_CUTOFF", "").strip() or "2026-10-15"
DISCOVERY_WALL_DATE: date = date.fromisoformat(_RAW_WALL_ISO)
DISCOVERY_WALL_ISO: str = DISCOVERY_WALL_DATE.isoformat()
DISCOVERY_WALL_UTC_NS: int = (
    int(
        datetime(
            DISCOVERY_WALL_DATE.year,
            DISCOVERY_WALL_DATE.month,
            DISCOVERY_WALL_DATE.day,
            23,
            59,
            59,
            tzinfo=UTC,
        ).timestamp()
    )
    * 1_000_000_000
    + 999_999_999
)
DEFAULT_DATA_ROOT: Path = Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or "Z:/sota-data")
DEFAULT_MARKET_DATA_ROOT: Path = Path(
    os.environ.get("TIER0_MARKET_DATA_ROOT", "").strip() or str(DEFAULT_DATA_ROOT / "prewall-lake")
)
BAR_DURATION_NS: int = 60_000_000_000
WARMUP_BARS: int = 40
FEATURE_COLS: list[str] = ["roc", "rsi", "natr", "vol_ratio"]
REGIME_BENCHMARK_SYMBOLS: tuple[str, ...] = ("SPY", "QQQ", "SOXX", "RSP", "VIX")
_XNYS = xcals.get_calendar("XNYS")
_NY_TZ = ZoneInfo("America/New_York")


class PreWallViolationError(ValueError):
    """Raised when a caller attempts to use post-wall (> 2026-10-15) data."""


_BS: dict[str, pa.Column] = {
    "symbol": pa.Column(pl.String, nullable=False),
    "t_ns": pa.Column(pl.Int64, checks=pa.Check.le(DISCOVERY_WALL_UTC_NS), nullable=False),
    **{c: pa.Column(pl.Float64, checks=pa.Check.gt(0.0), nullable=False) for c in "ohlc"},
    "v": pa.Column(pl.Float64, checks=pa.Check.ge(0.0), nullable=False),
}
BAR_SCHEMA = pa.DataFrameSchema(_BS, strict=False)


def parse_iso_date(value: date | str) -> date:
    """Parse an ISO YYYY-MM-DD date string or datetime to UTC `date`."""
    if isinstance(value, datetime):
        return (value.astimezone(UTC) if value.tzinfo else value).date()
    if isinstance(value, date):
        return value
    dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    return (dt.astimezone(UTC) if dt.tzinfo else dt).date()


def assert_pre_wall_date(value: date | str) -> date:
    """Fail closed if `value` lies after DISCOVERY_WALL_DATE (2026-10-15)."""
    parsed = parse_iso_date(value)
    if parsed > DISCOVERY_WALL_DATE:
        raise PreWallViolationError(f"Refused post-wall date {parsed} > {DISCOVERY_WALL_ISO}")
    return parsed


def assert_pre_wall_ns(ts_utc_ns: int) -> int:
    """Fail closed if a UTC nanosecond timestamp exceeds DISCOVERY_WALL_UTC_NS."""
    if ts_utc_ns > DISCOVERY_WALL_UTC_NS:
        raise PreWallViolationError(f"Refused post-wall timestamp {ts_utc_ns} > wall")
    return ts_utc_ns


def previous_xnys_session(day: date | str) -> date:
    """Return the strictly prior XNYS trading session via exchange_calendars."""
    return _XNYS.previous_session(assert_pre_wall_date(day)).date()


def validate_causal_bars(df: pl.DataFrame, *, validate: bool = True) -> pl.DataFrame:
    """Attach causal `t_close_ns = t_ns + 60s` and enforce pre-wall & OHLC invariants."""
    c = pl.col
    if "t_ns" not in df.columns or df.height == 0:
        raise ValueError("bars DataFrame missing t_ns or empty")
    out = df.with_columns((c("t_ns") + pl.lit(BAR_DURATION_NS, dtype=pl.Int64)).alias("t_close_ns"))
    if isinstance(mx := out.select(c("t_ns").max()).item(), int):
        assert_pre_wall_ns(mx)
    out = out.filter(c("t_close_ns") <= DISCOVERY_WALL_UTC_NS)
    if out.height > 0 and validate:
        BAR_SCHEMA.validate(out)
        fin = pl.all_horizontal(c(k).is_finite() for k in ("o", "h", "l", "c", "v"))
        hi_lo = (c("h") < pl.max_horizontal("o", "c")) | (c("l") > pl.min_horizontal("o", "c"))
        if bad := out.filter(~fin | hi_lo | (c("h") < c("l"))).height:
            raise ValueError(f"Invalid non-finite or OHLC-ordered bars ({bad})")
    return out


def load_minute_bars_day(
    day: date | str,
    *,
    symbols: Sequence[str] | None = None,
    root: Path = DEFAULT_MARKET_DATA_ROOT,
    validate: bool = True,
) -> pl.DataFrame:
    """Load one pre-wall XNYS session of 1-minute SIP bars with causal `t_close_ns`."""
    d = assert_pre_wall_date(day)
    if not (day_dir := root / "alpaca-sip" / "raw" / "bars-1min" / d.isoformat()).is_dir():
        raise FileNotFoundError(f"No bars-1min directory for {d.isoformat()}: {day_dir}")
    lf = pl.scan_parquet(str(day_dir / "*.parquet"))
    if symbols is not None:
        lf = lf.filter(pl.col("symbol").is_in([s.upper() for s in symbols]))
    return validate_causal_bars(lf.collect(), validate=validate)


def df_to_equity_and_bars(
    sym_df: pl.DataFrame, sym: str, usd: nm.Currency | None = None
) -> tuple[nm.Equity, nm.BarType, list[nm.Bar], int]:
    """Convert pre-wall OHLCV Polars DataFrame to Nautilus Equity & Bars."""
    if "t_ns" not in sym_df.columns or sym_df.height == 0:
        raise ValueError("bars DataFrame missing t_ns or empty")
    if not {"o", "h", "l", "c", "v"}.issubset(sym_df.columns):
        raise ValueError("bars DataFrame missing required OHLCV columns")
    c, ks = pl.col, (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))
    fin = pl.all_horizontal(c(k).cast(pl.Float64).is_finite().fill_null(False) for k in "ohlcv")
    agg = [
        c("t_ns").min(),
        c("t_ns").max(),
        c("t_ns").null_count(),
        c("t_ns").n_unique(),
        pl.min_horizontal(*(c(k) for k, _ in ks)).min(),
        c("l").min(),
        c("c").min(),
        (c("h") < c("l")).any(),
        fin.all(),
    ]
    t0_v, tmax_v, tn, tu, mn, lo_v, cl_v, inv, ok_f = sym_df.select(
        [e.alias(f"_{i}") for i, e in enumerate(agg)]
    ).row(0)
    if tn or t0_v is None or tmax_v is None or not ok_f or lo_v is None or cl_v is None:
        raise ValueError("bars DataFrame contains null or non-finite t_ns/OHLCV values")
    assert_pre_wall_ns(int(tmax_v))
    assert_pre_wall_ns(int(tmax_v) + BAR_DURATION_NS)
    if int(t0_v) < 0 or int(tu) != sym_df.height or float(mn) <= 0.0 or bool(inv):
        raise ValueError("bars DataFrame has negative, duplicate, or inverted OHLC values")
    prec = 4 if min(float(lo_v), float(cl_v)) < 1.03 else 2
    t0, cur, s = int(t0_v), usd or nm.Currency.from_str("USD"), nm.Symbol(sym)
    inst = nm.Equity(
        instrument_id=nm.InstrumentId(s, nm.Venue("XNAS")),
        raw_symbol=s,
        currency=cur,
        price_precision=prec,
        price_increment=nm.Price(10 ** (-prec), prec),
        lot_size=nm.Quantity.from_int(1),
        ts_event=t0,
        ts_init=t0,
    )
    bt = nm.BarType.from_str(f"{sym}.XNAS-1-MINUTE-LAST-EXTERNAL")
    buf, dec_t = io.BytesIO(), pl.Decimal(38, 16)
    ts_e = c("t_ns").cast(pl.UInt64) + BAR_DURATION_NS
    ohlc = [c(k).cast(dec_t).round(prec).alias(a) for k, a in ks]
    vol = c("v").round(0).clip(lower_bound=1.0).cast(dec_t).round(0).alias("volume")
    sym_df.select(*ohlc, vol, ts_e.alias("ts_event"), ts_e.alias("ts_init")).write_ipc_stream(buf)
    bars = BarDataWrangler(str(bt), prec, 0).process_record_batch_bytes(buf.getvalue())
    return inst, bt, bars, prec


def step_rth_5m_bar(
    st: dict[str, Any],
    day: date,
    minute_et: int,
    o: float,
    h: float,
    lo: float,
    c: float,
    vw: float,
) -> None:
    """Update causal RTH (09:30-15:59 ET) 5-min ORL5 / FR5 / VWAP-fail state."""
    if not (570 <= minute_et < 960):
        st.clear()
        return
    if st.get("day") not in (None, day):
        st.clear()
    st["day"], rl, gl = day, st.get("r_lim"), st.get("g_lim")
    if "orl" not in st:
        st.update(orl=lo, orh=h, red=False, grn=False, orl_brk=False, orh_brk=False)
        st.update(vw_fail=False, vw_rec=False)
        return
    st["orl_brk"], st["orh_brk"] = bool(c < st["orl"]), bool(c > st["orh"])
    st["vw_fail"] = bool((h > vw and c < vw) or (st.get("vw_fail") and c <= vw))
    st["vw_rec"] = bool((lo < vw and c > vw) or (st.get("vw_rec") and c >= vw))
    rp, gp = st.get("r_px"), st.get("g_px")
    r_tr = rp is not None and (lo < rp or c < rp) and c < vw
    g_tr = gp is not None and (h > gp or c > gp) and c > vw
    for kp, flag, arm, trig, rst in (
        ("r", "red", c < o, r_tr, rl is not None and h > rl),
        ("g", "grn", c > o, g_tr, gl is not None and lo < gl),
    ):
        px_k, lim_k = f"{kp}_px", f"{kp}_lim"
        if st.get(lim_k) is not None and rst:
            st[px_k], st[lim_k], st[flag] = None, None, False
        elif st.get(lim_k) is None and arm:
            st[px_k], st[lim_k] = (lo, h) if kp == "r" else (h, lo)
        elif st.get(lim_k) is not None and trig:
            st[flag] = True


def _combine_sym_regimes(vals: Sequence[int]) -> int:
    if not vals:
        return 0
    if max(vals) <= -1 or (min(vals) < 0 and max(vals) <= 0):
        return -1
    return int(min(vals) >= 1)


class MarketRegimeActorConfig(DataActorConfig):
    """Configuration for native multi-feed NautilusTrader market regime actor."""

    def __new__(cls, *args: Any, actor_id: nm.ActorId | None = None, **kwargs: Any) -> Self:
        del args, kwargs
        d: dict[str, Any] = {"actor_id": actor_id or nm.ActorId("TIER0-MARKET-REGIME")}
        return super().__new__(cls, **d)

    def __init__(
        self,
        bar_types: Sequence[nm.BarType | str] = (),
        trend_window: int = 30,
        crash_ret_threshold: float = -0.0035,
        bull_ret_threshold: float = 0.0020,
        actor_id: nm.ActorId | None = None,
    ) -> None:
        del actor_id
        super().__init__()
        self.bar_types = tuple(
            nm.BarType.from_str(b) if isinstance(b, str) else b for b in bar_types
        )
        self.trend_window = int(trend_window)
        self.crash_ret_threshold = float(crash_ret_threshold)
        self.bull_ret_threshold = float(bull_ret_threshold)


class MarketRegimeActor(DataActor):
    """Publishes causal multi-feed `market_regime` (+1 bull, -1 crash, 0 neutral) signals."""

    def __init__(self, config: MarketRegimeActorConfig) -> None:
        super().__init__(config)
        self._cfg, self.bar_types = config, tuple(config.bar_types)
        self.trend_window, self.last_regime = int(config.trend_window), 0
        self._bars: dict[
            nm.BarType, tuple[SimpleMovingAverage, VolumeWeightedAveragePrice, deque[float]]
        ] = {}
        self._days: dict[nm.BarType, date] = {}
        self._sym_reg: dict[nm.BarType, int] = {}

    def on_start(self) -> None:
        for bt in self.bar_types:
            sma = SimpleMovingAverage(self.trend_window)
            dq: deque[float] = deque(maxlen=self.trend_window + 1)
            self._bars[bt] = (sma, VolumeWeightedAveragePrice(), dq)
            self.register_indicator_for_bars(bt, sma)
            self.subscribe_bars(bt)

    def on_bar(self, bar: nm.Bar) -> None:
        if (st := self._bars.get(bt := bar.bar_type)) is None:
            return
        sma, vwap, dq = st
        ny_day = datetime.fromtimestamp(int(bar.ts_event) // 1_000_000_000, tz=_NY_TZ).date()
        if self._days.get(bt) != ny_day:
            vwap.reset()
            self._days[bt] = ny_day
        vwap.handle_bar(bar)
        dq.append(ci := float(bar.close))
        wi = (ci / max(1e-6, dq[0]) - 1.0) if len(dq) > self.trend_window else 0.0
        mi = float(sma.value) if sma.initialized else ci
        vw = float(vwap.value) if vwap.initialized else ci
        reg = (
            -1
            if (ci < mi and ci < vw and wi <= self._cfg.crash_ret_threshold)
            else int(ci > mi and ci > vw and wi >= self._cfg.bull_ret_threshold)
        )
        self._sym_reg[bt] = reg
        self.last_regime = _combine_sym_regimes([self._sym_reg.get(k, 0) for k in self.bar_types])
        self.publish_signal("market_regime", self.last_regime, ts_event=int(bar.ts_event))


def _with_daily_vwap(df: pl.DataFrame) -> pl.DataFrame:
    v_pos, c_f = pl.col("v").cast(pl.Float64).clip(lower_bound=0.0), pl.col("c").cast(pl.Float64)
    pv, vv = (c_f * v_pos).cum_sum().over("ny_day"), v_pos.cum_sum().over("ny_day")
    return df.with_columns(pl.when(vv > 0.0).then(pv / vv).otherwise(c_f).alias("vw"))


def _ny_date_expr(col_name: str, alias: str) -> pl.Expr:
    ep = pl.from_epoch(col_name, time_unit="ns").dt.replace_time_zone("UTC")
    return ep.dt.convert_time_zone("America/New_York").dt.date().alias(alias)


def _pelt_threshold(vals: np.ndarray) -> float:
    if (n := int(vals.size)) < 15:
        return float(np.quantile(vals, 0.90)) if n > 0 else 1e-4
    pts = rpt.Pelt(model="rbf", min_size=5).fit(vals.reshape(-1, 1)).predict(pen=1.5)
    bkps = [b for b in pts if b < n]
    return float(np.median(vals[bkps])) if bkps else float(np.quantile(vals, 0.90))


def compute_causal_market_regime(market_bars_df: pl.DataFrame, **o: Any) -> dict[int, int]:
    """Classify causal SPY/QQQ/SOXX/RSP/VIX regime (`+1` bull, `-1` crash/bear, `0` neutral)."""
    if market_bars_df.height == 0:
        raise ValueError("Cannot compute market regime on empty market_bars_df")
    tw = int(o.get("trend_window", 30))
    crash_thr = float(o.get("crash_ret_threshold", -0.0035))
    bull_thr = float(o.get("bull_ret_threshold", 0.0020))
    vix_thr = float(o.get("vix_stress_threshold", 25.0))
    c = pl.col
    mdf = market_bars_df.with_columns(c("symbol").cast(pl.Utf8).str.to_uppercase())
    if "t_close_ns" not in mdf.columns:
        mdf = mdf.with_columns((c("t_ns") + BAR_DURATION_NS).alias("t_close_ns"))
    vix_df = mdf.filter(c("symbol") == "VIX").sort("t_close_ns")
    vix_ts = vix_df["t_close_ns"].to_numpy() if vix_df.height else np.array([], dtype=np.int64)
    vix_px = vix_df["c"].to_numpy().astype(float) if vix_df.height else np.array([], dtype=float)
    uniq = set(mdf["symbol"].unique().to_list())
    bms = [s for s in ("SPY", "QQQ", "SOXX") if s in uniq]
    bms = bms or [s for s in sorted(uniq) if s != "VIX"]
    if not bms:
        raise ValueError("No equity benchmark symbols (SPY/QQQ/SOXX) found in market_bars_df")
    w_ret = (c("c") / c("c").shift(tw).clip(lower_bound=1e-6) - 1.0).fill_null(0.0)
    b_ret = (c("c") / c("c").shift(1).clip(lower_bound=1e-6) - 1.0).fill_null(0.0)
    has_hl = {"h", "l"}.issubset(mdf.columns)
    mrc = market_regime_classifier("h", "l", "c", "v").fill_null(0) if has_hl else pl.lit(0)
    exprs = [
        _ny_date_expr("t_close_ns", "ny_day"),
        w_ret.alias("win_ret"),
        b_ret.alias("bar_ret"),
        c("c").rolling_mean(tw, min_samples=1).alias("ma"),
        mrc.alias("mrc"),
    ]
    per_sym: dict[int, list[int]] = {}
    for sym in bms:
        sdf = _with_daily_vwap(mdf.filter(c("symbol") == sym).sort("t_ns").with_columns(exprs))
        ks = ("c", "vw", "ma", "win_ret", "bar_ret")
        cl, vws, mas, wrs, brs = (sdf[k].to_numpy().astype(float) for k in ks)
        mrcs = sdf["mrc"].to_numpy().astype(int)
        t_cl = sdf["t_close_ns"].to_numpy().astype(np.int64)
        wcal = (
            np.abs(wrs[tw:WARMUP_BARS])
            if tw < WARMUP_BARS
            else np.abs(brs[1:WARMUP_BARS]) * math.sqrt(tw)
        )
        cal_thr = max(1e-4, _pelt_threshold(wcal)) if sdf.height > WARMUP_BARS else 0.0
        ad = ADWIN(delta=0.01)
        dr_dn = np.array([(ad.update(abs(float(b))), ad.drift_detected and b < 0)[1] for b in brs])
        s_hi = ((cl > mas) & (cl > vws) & (wrs >= bull_thr)).astype(int)
        s_reg = np.where((cl < mas) & (cl < vws) & (wrs <= crash_thr), -1, s_hi)
        dn_c = (wrs <= -cal_thr) | ((cl < mas) & (mrcs == 1)) | dr_dn
        up_c = (cl > mas) & ((wrs >= 0.5 * cal_thr) | (mrcs == 1))
        p_reg = np.where((cl < vws) & dn_c, -1, ((cl > vws) & up_c).astype(int))
        regs = np.where(np.arange(sdf.height) < WARMUP_BARS, s_reg, p_reg)
        if vix_ts.size:
            vi = np.searchsorted(vix_ts, t_cl, side="right") - 1
            vs = np.where(vi >= 0, vix_px[np.maximum(vi, 0)] >= vix_thr, False)
            regs = np.where(vs & (cl < mas) & (cl < vws), -1, np.where(vs & (regs == 1), 0, regs))
        for ts, reg in zip(t_cl.tolist(), regs.tolist(), strict=True):
            per_sym.setdefault(int(ts), []).append(int(reg))
    return {ts: _combine_sym_regimes(v) for ts, v in sorted(per_sym.items())}


def generate_upstream_reversal_signals(bars_df: pl.DataFrame, **o: Any) -> dict[int, int]:
    """Causal market-aligned short & peak-reversal signals via ml4t/river/ruptures/stumpy."""
    vol_window, z_thr = int(o.get("vol_window", 15)), float(o.get("z_threshold", 1.5))
    rmap: dict[int, int] = dict(o.get("market_regime_by_ts") or {})
    req_peak = bool(o.get("require_peak_confirmation", True))
    use_5m = bool(o.get("resample_5min_triggers", True))
    df, c = validate_causal_bars(bars_df.sort("t_ns")), pl.col
    if (n := df.height) <= WARMUP_BARS:
        return {}
    t_close = df["t_close_ns"].to_numpy().astype(int)
    opens, highs, lows, closes = (df[k].to_numpy().astype(float) for k in ("o", "h", "l", "c"))
    rets = np.diff(np.log(np.maximum(closes, 1e-6)), prepend=np.log(max(closes[0], 1e-6)))
    ren = (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume"))
    vma = c("v").rolling_mean(vol_window, min_samples=1).clip(lower_bound=1.0)
    ohlcv = df.select(
        pl.from_epoch("t_close_ns", time_unit="ns").alias("timestamp"),
        *(c(k).alias(v) for k, v in ren),
        (c("v") / vma).alias("vol_ratio"),
    )
    fkw: dict[str, Any] = {"timestamp_col": "timestamp", "assume_sorted": True, "calendar": None}
    feat_raw = compute_features(ohlcv, ["roc", "rsi", "natr"], **fkw)
    if not isinstance(feat_raw, pl.DataFrame):
        raise TypeError(f"Expected Polars DataFrame from ml4t-engineer, got {type(feat_raw)!r}")
    f_cols = [c(col).cast(pl.Float64).fill_nan(0.0).fill_null(0.0) for col in FEATURE_COLS]
    feat_df = feat_raw.select(f_cols)
    StandardScaler(columns=FEATURE_COLS).fit(feat_df.slice(0, WARMUP_BARS - 1)).transform(feat_df)
    vol_ratio, natr_arr = feat_df["vol_ratio"].to_numpy(), feat_df["natr"].to_numpy()
    cal_thr = _pelt_threshold(np.abs(rets[:WARMUP_BARS]))
    mp_s = stumpy.stumpi(rets[:WARMUP_BARS], m=max(4, min(8, WARMUP_BARS // 8)), egress=False)
    mp_v = mp_s.left_P_[np.isfinite(mp_s.left_P_)]
    mp_thr, adwin = (float(np.quantile(mp_v, 0.75)) if mp_v.size else 0.0), ADWIN(delta=0.01)
    deque((adwin.update(float(val)) for val in np.abs(rets[:WARMUP_BARS])), maxlen=0)
    df_vw = _with_daily_vwap(df.with_columns(_ny_date_expr("t_ns", "ny_day")))
    vws, b5_rows = df_vw["vw"].to_numpy().astype(float), []
    if use_5m:
        et_t = pl.from_epoch("t_ns", time_unit="ns").dt.replace_time_zone("UTC")
        b5_t = et_t.dt.convert_time_zone("America/New_York").dt.truncate("5m").alias("_b5")
        aggs = (
            c("ny_day").first(),
            c("o").first(),
            c("h").max(),
            c("l").min(),
            c("c").last(),
            c("vw").last(),
        )
        b5_f = df_vw.with_columns(b5_t).group_by("_b5", maintain_order=True).agg(*aggs)
        bd = c("_b5").dt
        b5_f = b5_f.with_columns(
            (bd.hour().cast(pl.Int32) * 60 + bd.minute().cast(pl.Int32)).alias("bm"),
            (bd.epoch("ns") + 300_000_000_000).alias("b5c"),
        ).filter(c("bm").is_between(570, 959))
        b5_rows = list(b5_f.select(["b5c", "ny_day", "bm", "o", "h", "l", "c", "vw"]).iter_rows())
    signals, st, b5_idx, prev_vw = {}, {}, 0, float(closes[0])
    for i in range(n):
        ci, ri, vw_i = closes[i], rets[i], float(vws[i])
        while b5_idx < len(b5_rows) and int(b5_rows[b5_idx][0]) <= int(t_close[i]):
            r5b = b5_rows[b5_idx]
            step_rth_5m_bar(st, r5b[1], int(r5b[2]), *(float(x) for x in r5b[3:]))
            b5_idx += 1
        if i < WARMUP_BARS:
            prev_vw = vw_i
            continue
        mp_s.update(float(ri))
        adwin.update(float(abs(ri)))
        ts_i, reg = int(t_close[i]), rmap.get(int(t_close[i]), 0)
        is_anom = bool(adwin.drift_detected or (abs(ri) >= cal_thr and mp_s.left_P_[-1] > mp_thr))
        vr_ok, natr_ext = vol_ratio[i] >= z_thr, max(1e-4, float(natr_arr[i]) / 100.0)
        if reg == -1 and ci < vw_i and ri < 0.0:
            signals[ts_i] = -1
        elif not req_peak:
            mom = 1 if (vr_ok and ci > vw_i) else (-1 if (vr_ok and ci < vw_i) else 0)
            cand = (-1 if ri > 0.0 else 1) if is_anom else mom
            if cand and cand * reg != -1:
                signals[ts_i] = cand
        else:
            top_5 = any(st.get(k) for k in ("orl_brk", "red", "vw_fail"))
            bot_5 = any(st.get(k) for k in ("orh_brk", "grn", "vw_rec"))
            top_t = top_5 if use_5m else (ci < opens[i] and (ci < lows[i - 1] or ci < vw_i))
            bot_t = bot_5 if use_5m else (ci > opens[i] and (ci > highs[i - 1] or ci > vw_i))
            trig = is_anom or abs(rets[i - 1]) >= cal_thr or vol_ratio[i - 1] >= z_thr
            if closes[i - 1] >= prev_vw * (1.0 + natr_ext) and trig and top_t and reg != 1:
                signals[ts_i] = -1
            elif reg != -1 and closes[i - 1] <= prev_vw * (1.0 - natr_ext) and trig and bot_t:
                signals[ts_i] = 1
        prev_vw = vw_i
    return signals


def fit_causal_gbdt_ranker_with_shap(
    features_df: pl.DataFrame,
    *,
    feature_cols: Sequence[str] = FEATURE_COLS,
    target_col: str = "fwd_return",
    ts_col: str = "t_close_ns",
    train_fraction: float = 0.70,
    random_state: int = 42,
) -> dict[str, Any]:
    """Fit causal pre-wall LightGBM + CatBoost cross-sectional ranker with TreeExplainer SHAP."""
    fcols = list(feature_cols)
    required = {ts_col, target_col, *fcols}
    if missing := sorted(required - set(features_df.columns)):
        raise ValueError(f"features_df missing required columns: {missing}")
    if not (0.2 <= train_fraction <= 0.9):
        raise ValueError(f"train_fraction must lie in [0.2, 0.9], got {train_fraction}")

    ordered = features_df.sort(ts_col)
    n_rows = ordered.height
    if n_rows < 20:
        raise ValueError(f"Need >= 20 pre-wall rows for causal GBDT split, got {n_rows}")

    ts_arr = ordered[ts_col].to_numpy().astype(np.int64)
    assert_pre_wall_ns(int(np.max(ts_arr)))

    split_idx = max(10, min(n_rows - 5, int(n_rows * train_fraction)))
    train_max_ts = int(ts_arr[split_idx - 1])
    oos_min_ts = int(ts_arr[split_idx])
    if oos_min_ts < train_max_ts:
        raise ValueError("Causal timestamp ordering violated across train/OOS split")

    x_all = ordered.select(fcols).to_numpy().astype(float)
    y_all = ordered[target_col].to_numpy().astype(float)
    if not (bool(np.all(np.isfinite(x_all))) and bool(np.all(np.isfinite(y_all)))):
        raise ValueError("features_df contains non-finite feature or target values")

    x_tr, y_tr = x_all[:split_idx], y_all[:split_idx]
    x_oos, y_oos = x_all[split_idx:], y_all[split_idx:]

    lgbm_model = lgb.LGBMRegressor(
        n_estimators=24,
        max_depth=3,
        learning_rate=0.05,
        num_leaves=7,
        min_child_samples=5,
        random_state=random_state,
        verbosity=-1,
    )
    lgbm_model.fit(x_tr, y_tr)
    lgbm_pred = np.asarray(lgbm_model.predict(x_oos), dtype=float)

    cb_task = os.environ.get("TIER0_CATBOOST_TASK_TYPE", "CPU").strip() or "CPU"
    cb_model = CatBoostRegressor(
        iterations=24,
        depth=3,
        learning_rate=0.05,
        random_seed=random_state,
        verbose=False,
        allow_writing_files=False,
        task_type=cb_task,
    )
    cb_model.fit(x_tr, y_tr)
    cb_pred = np.asarray(cb_model.predict(x_oos), dtype=float)
    ens_pred = 0.5 * (lgbm_pred + cb_pred)

    explainer = shap.TreeExplainer(lgbm_model)
    shap_vals = np.asarray(explainer.shap_values(x_oos), dtype=float)
    mean_abs = np.mean(np.abs(shap_vals), axis=0)
    shap_map = {col: round(float(val), 6) for col, val in zip(fcols, mean_abs, strict=True)}
    top_feature = max(shap_map, key=lambda k: shap_map[k])

    def _safe_corr(a: np.ndarray, b: np.ndarray) -> float:
        if float(np.std(a)) <= 1e-12 or float(np.std(b)) <= 1e-12:
            return 0.0
        val = float(np.corrcoef(a, b)[0, 1])
        return round(val, 6) if math.isfinite(val) else 0.0

    return {
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "train_rows": int(split_idx),
        "oos_rows": int(n_rows - split_idx),
        "train_max_ts_ns": train_max_ts,
        "oos_min_ts_ns": oos_min_ts,
        "lgbm_oos_corr": _safe_corr(lgbm_pred, y_oos),
        "catboost_oos_corr": _safe_corr(cb_pred, y_oos),
        "ensemble_oos_corr": _safe_corr(ens_pred, y_oos),
        "ensemble_oos_scores": [round(float(v), 6) for v in ens_pred.tolist()],
        "shap_mean_abs_importance": shap_map,
        "top_feature": top_feature,
    }


def load_causal_fred_vintages(
    *,
    series_ids: Sequence[str] | None = None,
    cutoff_date: date | str = DISCOVERY_WALL_ISO,
    snapshot_path: Path | None = None,
    allow_network: bool = False,
    api_key: str | None = None,
) -> pl.DataFrame:
    """Load offline-first causal ALFRED/FRED macro vintages strictly <= cutoff_date (PIT)."""
    wall = assert_pre_wall_date(cutoff_date)
    resolved = (
        snapshot_path
        if snapshot_path is not None
        else (DEFAULT_MARKET_DATA_ROOT / "public-tables" / "alfred_macro_vintages.parquet")
    )
    want_ids = [s.strip().upper() for s in series_ids] if series_ids is not None else None

    if resolved.is_file():
        if resolved.suffix.lower() == ".csv":
            raw_df = pl.read_csv(resolved, try_parse_dates=True)
        else:
            raw_df = pl.read_parquet(resolved)
    elif allow_network:
        if not want_ids:
            raise ValueError("series_ids must be provided when fetching live ALFRED vintages")
        resolved_key = api_key or os.environ.get("FRED_API_KEY", "").strip()
        if not resolved_key:
            raise ValueError("FRED_API_KEY required when allow_network=True")
        fred_client = Fred(api_key=resolved_key)
        rows: list[dict[str, Any]] = []
        for sid in want_ids:
            ser = fred_client.get_series_as_of_date(sid, wall.isoformat())
            for _, rec in ser.dropna().iterrows():
                obs_d = parse_iso_date(rec["date"])
                vin_d = parse_iso_date(rec["realtime_start"])
                rows.append(
                    {
                        "series_id": sid,
                        "observation_date": obs_d,
                        "vintage_date": vin_d,
                        "value": float(rec["value"]),
                    }
                )
        raw_df = pl.DataFrame(rows)
    else:
        raise FileNotFoundError(
            f"Offline ALFRED snapshot missing at {resolved} (allow_network=False)"
        )

    required = {"series_id", "observation_date", "vintage_date", "value"}
    if missing := sorted(required - set(raw_df.columns)):
        raise ValueError(f"ALFRED vintage table missing required columns: {missing}")

    c = pl.col
    df = raw_df.with_columns(
        c("series_id").cast(pl.String).str.to_uppercase(),
        c("observation_date").cast(pl.Date),
        c("vintage_date").cast(pl.Date),
        c("value").cast(pl.Float64),
    )
    if want_ids is not None:
        df = df.filter(c("series_id").is_in(want_ids))

    # Strict PIT: keep only observations published on or before wall, selecting latest pre-wall
    df = df.filter((c("vintage_date") <= wall) & (c("observation_date") <= wall))
    if df.height == 0:
        return df
    return (
        df.sort(["series_id", "observation_date", "vintage_date"])
        .group_by(["series_id", "observation_date"], maintain_order=True)
        .last()
        .select(["series_id", "observation_date", "vintage_date", "value"])
    )


NON_SHORTABLE_INVERSE_ETFS: frozenset[str] = frozenset(
    {
        "SOXS",
        "SPXS",
        "UVXY",
        "VXX",
        "UVIX",
        "SVIX",
        "ZSL",
        "TMF",
        "TMV",
        "NAIL",
        "SQQQ",
        "SDOW",
        "SRTY",
        "FAZ",
        "DRV",
        "YANG",
        "BOIL",
        "KOLD",
        "LABD",
        "WEBS",
        "TZA",
        "SPXU",
        "SH",
        "PSQ",
        "DOG",
        "SARK",
        "BITI",
        "CONI",
        "NVD",
        "TSLS",
        "MSTZ",
    }
)

PREWALL_AFML_FEATURE_COLS: tuple[str, ...] = (
    "pm_gap",
    "rel_pm_gap",
    "spy_pm_gap",
    "pm_fade_hi",
    "pm_reb_lo",
    "pm_rng",
    "pm_drift",
    "pm_dvol_ratio",
    "mean_20d_range_pct",
    "mean_5d_range_pct_prior",
    "vdu_5d_20d_ratio",
    "coil_5d_20d_range_ratio",
    "prev_close_to_20d_high",
    "prior_ret_5d",
    "prior_ret_20d",
    "prev_rvol_20d",
    "prev_ret_1d",
    "prev_range_pct",
    "prev_close_loc",
    "log_dvol",
    "spy_prev_ret1",
    "spy_ret5",
    "spy_ret20",
    "growth_vs_broad_5d",
    "prev_vix",
    "prev_vix_chg5",
    "prev_hy_oas",
)


def _sim_rth_short_label(
    o: np.ndarray, h: np.ndarray, lo: np.ndarray, c: np.ndarray, xi: int
) -> float:
    """Strictly causal 1-min RTH STOP_MARKET label for AFML meta-labeling training only."""
    if xi <= 1 or len(o) <= 2:
        return float("nan")
    ep = float(o[1])
    if ep <= 0.0 or not math.isfinite(ep):
        return float("nan")
    sp, fav, fee = ep * 1.02, ep, 0.0016
    for i in range(1, min(xi + 1, len(o))):
        if o[i] >= sp:
            return (1.0 - float(o[i]) / ep) - fee
        if h[i] >= sp:
            return (1.0 - sp / ep) - fee
        if lo[i] < fav:
            fav = float(lo[i])
        g = 1.0 - fav / ep
        if g >= 0.010:
            lock = 0.04 if g >= 0.06 else (0.02 if g >= 0.04 else (0.005 if g >= 0.02 else 0.0018))
            sp = min(sp, min(ep * (1.0 - lock), fav * 1.010))
    return (1.0 - float(c[min(xi, len(c) - 1)]) / ep) - fee


def load_published_orb_l1_sample(
    lake_root: Path = DEFAULT_MARKET_DATA_ROOT,
    *,
    preferred_symbols: Sequence[str] = ("TSLA", "PLTR", "AMD", "META", "NVDA"),
    max_quotes: int = 1500,
    max_trades: int = 800,
) -> tuple[str, pl.DataFrame, pl.DataFrame] | None:
    """Load real pre-wall L1 quotes/trades from published-orb strictly <= DISCOVERY_WALL."""
    q_root = lake_root / "published-orb" / "quotes"
    t_root = lake_root / "published-orb" / "trades"
    if not (q_root.is_dir() and t_root.is_dir()):
        return None
    for sym in preferred_symbols:
        qd, td = q_root / sym, t_root / sym
        if not (qd.is_dir() and td.is_dir()):
            continue
        for qf in sorted(qd.glob("*.parquet"))[-5:]:
            tf = td / qf.name
            if not tf.is_file():
                continue
            qc = pl.read_parquet(qf).select(["t_ns", "bp", "bs", "ap", "as"]).head(max_quotes)
            tc = pl.read_parquet(tf).select(["t_ns", "p", "s"]).head(max_trades)
            if qc.height >= 100 and tc.height >= 30:
                q_max = qc.select(pl.col("t_ns").max()).item()
                t_max = tc.select(pl.col("t_ns").max()).item()
                if isinstance(q_max, int) and isinstance(t_max, int):
                    assert_pre_wall_ns(max(q_max, t_max))
                return sym, qc, tc
    return None


def discover_prewall_multi_symbol_alpha(
    lake_root: Path = DEFAULT_MARKET_DATA_ROOT,
    *,
    start_date: str = "2024-02-01",
    min_dvol_usd: float = 50_000_000.0,
    min_pm_gap: float = 0.035,
    top_candidates_per_day: int = 6,
    wf_warmup_days: int = 200,
    wf_retrain_step: int = 45,
    wf_purge_days: int = 2,
    random_state: int = 42,
) -> dict[str, Any]:
    """Run zero-leakage multi-symbol pre-wall discovery for SHORT (AFML PM) & LONG (VIX stress).

    All selection, features, macro vintages, and bars are strictly causal <= DISCOVERY_WALL_ISO.
    """
    assert_pre_wall_date(start_date)
    df10_path = lake_root / "derived" / "df10_daily_20d_baselines.parquet"
    bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"
    if not df10_path.is_file() or not bars_root.is_dir():
        raise FileNotFoundError(f"Pre-wall lake artifacts missing under {lake_root}")

    c = pl.col
    df10 = (
        pl.read_parquet(
            df10_path,
            columns=[
                "symbol",
                "trade_date",
                "raw_close",
                "prev_raw_close",
                "split_close",
                "split_high",
                "split_low",
                "mean_20d_dvol",
                "mean_20d_range_pct",
                "mean_5d_range_pct_prior",
                "prior_ret_5d",
                "prior_ret_20d",
                "rvol_20d",
                "vdu_5d_20d_ratio",
                "coil_5d_20d_range_ratio",
                "prev_close_to_20d_high",
                "ret_1d",
                "bar_range_pct",
            ],
        )
        .filter((c("trade_date") <= DISCOVERY_WALL_ISO) & (c("trade_date") >= start_date))
        .sort(["symbol", "trade_date"])
        .with_columns(
            (c("split_close") / c("raw_close")).alias("sf"),
            c("split_close").rolling_mean(20).over("symbol").shift(1).alias("prev_sma20"),
            c("split_close").shift(1).over("symbol").alias("prev_c"),
            c("rvol_20d").shift(1).over("symbol").alias("prev_rvol_20d"),
            c("ret_1d").shift(1).over("symbol").alias("prev_ret_1d"),
            c("bar_range_pct").shift(1).over("symbol").alias("prev_range_pct"),
            (
                (c("split_close") - c("split_low"))
                / (c("split_high") - c("split_low")).clip(lower_bound=1e-6)
            )
            .shift(1)
            .over("symbol")
            .alias("prev_close_loc"),
        )
        .with_columns(c("sf").shift(1).over("symbol").alias("prev_sf"))
    )

    alfred = load_causal_fred_vintages(
        series_ids=["VIXCLS", "BAMLH0A0HYM2", "T10Y2Y"],
        cutoff_date=DISCOVERY_WALL_ISO,
        snapshot_path=lake_root / "public-tables" / "alfred_macro_vintages.parquet",
    )
    macro = (
        alfred.pivot(index="observation_date", on="series_id", values="value")
        .sort("observation_date")
        .with_columns(pl.all().exclude("observation_date").forward_fill())
        .with_columns(
            c("VIXCLS").shift(1).alias("prev_vix"),
            (c("VIXCLS").shift(1) - c("VIXCLS").shift(6)).alias("prev_vix_chg5"),
            c("BAMLH0A0HYM2").shift(1).alias("prev_hy_oas"),
            c("observation_date").dt.strftime("%Y-%m-%d").alias("trade_date"),
        )
        .select(["trade_date", "prev_vix", "prev_vix_chg5", "prev_hy_oas"])
    )

    spy_ref = (
        df10.filter(c("symbol") == "SPY")
        .select(
            "trade_date",
            c("prev_c").alias("spy_prev_c"),
            c("prev_sma20").alias("spy_prev_sma20"),
            c("prev_ret_1d").alias("spy_prev_ret1"),
            c("prior_ret_5d").alias("spy_ret5"),
            c("prior_ret_20d").alias("spy_ret20"),
        )
        .join(macro, on="trade_date", how="left")
    )
    qqq_ref = df10.filter(c("symbol") == "QQQ").select("trade_date", c("prior_ret_5d").alias("q5"))
    rsp_ref = df10.filter(c("symbol") == "RSP").select("trade_date", c("prior_ret_5d").alias("r5"))

    # Strictly causal LONG stress-open days: SPY prior close < prior SMA20 & prior VIX >= 20
    long_stress_days = set(
        spy_ref.filter((c("spy_prev_c") < c("spy_prev_sma20")) & (c("prev_vix") >= 20.0))[
            "trade_date"
        ].to_list()
    )

    elig = (
        df10.filter(
            (c("mean_20d_dvol") >= min_dvol_usd)
            & (c("prev_raw_close") >= 5.0)
            & (~c("symbol").is_in(list(NON_SHORTABLE_INVERSE_ETFS)))
            & ((c("sf") - c("prev_sf")).abs() < 1e-4)
        )
        .join(
            spy_ref.select(
                "trade_date",
                "spy_prev_ret1",
                "spy_ret5",
                "spy_ret20",
                "prev_vix",
                "prev_vix_chg5",
                "prev_hy_oas",
            ),
            on="trade_date",
            how="left",
        )
        .join(qqq_ref, on="trade_date", how="left")
        .join(rsp_ref, on="trade_date", how="left")
        .with_columns(
            (c("q5") - c("r5")).alias("growth_vs_broad_5d"),
            c("mean_20d_dvol").log().alias("log_dvol"),
        )
    )
    elig_by_day = elig.partition_by("trade_date", as_dict=True)
    all_days = sorted(str(k[0]) for k in elig_by_day)

    short_recs: list[dict[str, Any]] = []
    short_bars: dict[tuple[str, str], pl.DataFrame] = {}
    long_bars: dict[tuple[str, str], pl.DataFrame] = {}

    for d_iso in all_days:
        ddir = bars_root / d_iso
        if not ddir.is_dir():
            continue
        edf = elig_by_day[(d_iso,)]
        want_syms = set(edf["symbol"].to_list()) | {"SPY"}
        if d_iso in long_stress_days:
            want_syms.update({"SPXL", "TQQQ"})
        try:
            bdf = (
                pl.scan_parquet(str(ddir / "*.parquet"))
                .filter(c("symbol").is_in(list(want_syms)))
                .select(["symbol", "t_ns", "o", "h", "l", "c", "v"])
                .collect()
            )
        except (OSError, pl.exceptions.PolarsError):
            continue
        if bdf.height == 0:
            continue
        bdf = bdf.with_columns(
            pl.from_epoch("t_ns", time_unit="ns")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
            .alias("_dt_et")
        ).with_columns(
            (
                c("_dt_et").dt.hour().cast(pl.Int32) * 60 + c("_dt_et").dt.minute().cast(pl.Int32)
            ).alias("hm")
        )

        # Collect LONG SPXL/TQQQ pure-RTH (09:29..15:59) bars on stress days
        if d_iso in long_stress_days:
            l_part = bdf.filter(
                c("symbol").is_in(["SPXL", "TQQQ"]) & (c("hm") >= 569) & (c("hm") <= 959)
            ).partition_by("symbol", as_dict=True)
            for lsym in ("SPXL", "TQQQ"):
                lb = l_part.get((lsym,))
                if lb is None:
                    continue
                lb = lb.sort("t_ns").unique("t_ns", keep="last")
                if lb.height >= 40 and int(lb["hm"][0]) == 569:
                    with contextlib.suppress(ValueError):
                        long_bars[(d_iso, lsym)] = validate_causal_bars(lb.drop(["_dt_et", "hm"]))

        # Strictly pre-market <= 09:29 ET selection for SHORT ETB parabolic gap-ups
        pm_sub = bdf.filter((c("hm") >= 240) & (c("hm") <= 569)).sort(["symbol", "t_ns"])
        if pm_sub.height == 0:
            continue
        pm_agg = (
            pm_sub.group_by("symbol")
            .agg(
                c("c").last().alias("pm_last"),
                c("o").first().alias("pm_first"),
                c("h").max().alias("pm_hi"),
                c("l").min().alias("pm_lo"),
                c("hm").last().alias("pm_last_hm"),
                (c("c") * c("v")).sum().alias("pm_dvol"),
                pl.len().alias("pm_bars"),
            )
            .filter((c("pm_last_hm") == 569) & (c("pm_bars") >= 4) & (c("pm_dvol") >= 150_000.0))
        )
        spy_pm = pm_agg.filter(c("symbol") == "SPY")
        spy_prev = edf.filter(c("symbol") == "SPY")
        spy_pm_gap = (
            float(spy_pm["pm_last"][0]) / float(spy_prev["prev_raw_close"][0]) - 1.0
            if spy_pm.height > 0 and spy_prev.height > 0
            else 0.0
        )
        joined = (
            pm_agg.join(edf, on="symbol", how="inner")
            .with_columns(
                (c("pm_last") / c("prev_raw_close") - 1.0).alias("pm_gap"),
                (c("pm_last") / c("pm_hi") - 1.0).alias("pm_fade_hi"),
                (c("pm_last") / c("pm_lo") - 1.0).alias("pm_reb_lo"),
                (c("pm_hi") / c("pm_lo") - 1.0).alias("pm_rng"),
                (c("pm_last") / c("pm_first") - 1.0).alias("pm_drift"),
                (c("pm_dvol") / c("mean_20d_dvol")).alias("pm_dvol_ratio"),
            )
            .filter(c("pm_gap") >= min_pm_gap)
            .sort("pm_gap", descending=True)
            .head(top_candidates_per_day)
        )
        if joined.height == 0:
            continue
        sub_bars = bdf.filter(c("symbol").is_in(joined["symbol"].to_list())).partition_by(
            "symbol", as_dict=True
        )
        for row in joined.to_dicts():
            sym = str(row["symbol"])
            sb = sub_bars.get((sym,))
            if sb is None or sb.height < 40:
                continue
            sb = (
                sb.sort("t_ns")
                .unique("t_ns", keep="last")
                .filter((c("hm") >= 569) & (c("hm") <= 959))
            )
            hm = sb["hm"].to_numpy()
            if len(hm) < 40 or int(hm[0]) != 569:
                continue
            xi = int(np.searchsorted(hm, 955, side="left"))
            if xi >= len(hm):
                xi = len(hm) - 1
            o_a, h_a, l_a, c_a = (sb[k].to_numpy().astype(float) for k in ("o", "h", "l", "c"))
            y_lbl = _sim_rth_short_label(o_a, h_a, l_a, c_a, xi)
            if not math.isfinite(y_lbl):
                continue
            try:
                vdf = validate_causal_bars(sb.drop(["_dt_et", "hm"]))
            except ValueError:
                continue
            short_bars[(d_iso, sym)] = vdf
            short_recs.append(
                {
                    "trade_date": d_iso,
                    "symbol": sym,
                    "pm_gap": float(row["pm_gap"]),
                    "rel_pm_gap": float(row["pm_gap"]) - spy_pm_gap,
                    "spy_pm_gap": spy_pm_gap,
                    "pm_fade_hi": float(row["pm_fade_hi"]),
                    "pm_reb_lo": float(row["pm_reb_lo"]),
                    "pm_rng": float(row["pm_rng"]),
                    "pm_drift": float(row["pm_drift"]),
                    "pm_dvol_ratio": float(row["pm_dvol_ratio"]),
                    "mean_20d_range_pct": float(row["mean_20d_range_pct"] or 0.03),
                    "mean_5d_range_pct_prior": float(row["mean_5d_range_pct_prior"] or 0.03),
                    "vdu_5d_20d_ratio": float(row["vdu_5d_20d_ratio"] or 1.0),
                    "coil_5d_20d_range_ratio": float(row["coil_5d_20d_range_ratio"] or 1.0),
                    "prev_close_to_20d_high": float(row["prev_close_to_20d_high"] or 0.0),
                    "prior_ret_5d": float(row["prior_ret_5d"] or 0.0),
                    "prior_ret_20d": float(row["prior_ret_20d"] or 0.0),
                    "prev_rvol_20d": float(row["prev_rvol_20d"] or 1.0),
                    "prev_ret_1d": float(row["prev_ret_1d"] or 0.0),
                    "prev_range_pct": float(row["prev_range_pct"] or 0.03),
                    "prev_close_loc": float(row["prev_close_loc"] or 0.5),
                    "log_dvol": float(row["log_dvol"] or 17.7),
                    "spy_prev_ret1": float(row["spy_prev_ret1"] or 0.0),
                    "spy_ret5": float(row["spy_ret5"] or 0.0),
                    "spy_ret20": float(row["spy_ret20"] or 0.0),
                    "growth_vs_broad_5d": float(row["growth_vs_broad_5d"] or 0.0),
                    "prev_vix": float(row["prev_vix"] or 16.0),
                    "prev_vix_chg5": float(row["prev_vix_chg5"] or 0.0),
                    "prev_hy_oas": float(row["prev_hy_oas"] or 3.2),
                    "y_label": float(y_lbl),
                }
            )

    if not short_recs:
        raise ValueError(f"No pre-wall SHORT candidates extracted under {lake_root}")

    pdf = pl.DataFrame(short_recs).sort(["trade_date", "symbol"])
    fcols = list(PREWALL_AFML_FEATURE_COLS)
    udates = sorted(pdf["trade_date"].unique().to_list())
    x_all = np.nan_to_num(
        pdf.select(fcols).to_numpy().astype(float), nan=0.0, posinf=0.0, neginf=0.0
    )
    y_all = pdf["y_label"].to_numpy().astype(float)
    d_arr = np.array(pdf["trade_date"].to_list())
    preds = np.full(len(y_all), np.nan)
    last_lgb: lgb.LGBMRegressor | None = None
    last_x_oos: np.ndarray | None = None
    cb_task = os.environ.get("TIER0_CATBOOST_TASK_TYPE", "CPU").strip() or "CPU"

    for si in range(wf_warmup_days, len(udates), wf_retrain_step):
        purge_idx = max(0, si - wf_purge_days)
        tr_m = d_arr <= udates[purge_idx]
        te_m = (d_arr >= udates[si]) & (
            d_arr <= udates[min(len(udates) - 1, si + wf_retrain_step - 1)]
        )
        if int(np.sum(tr_m)) < 150 or int(np.sum(te_m)) == 0:
            continue
        m_lgb = lgb.LGBMRegressor(
            n_estimators=45,
            max_depth=3,
            num_leaves=7,
            learning_rate=0.04,
            min_child_samples=30,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=random_state,
            verbosity=-1,
        )
        m_lgb.fit(x_all[tr_m], y_all[tr_m])
        m_cb = CatBoostRegressor(
            iterations=65,
            depth=3,
            learning_rate=0.04,
            l2_leaf_reg=6.0,
            random_seed=random_state,
            verbose=False,
            allow_writing_files=False,
            task_type=cb_task,
        )
        m_cb.fit(x_all[tr_m], y_all[tr_m])
        preds[te_m] = 0.5 * (
            np.asarray(m_lgb.predict(x_all[te_m]), dtype=float)
            + np.asarray(m_cb.predict(x_all[te_m]), dtype=float)
        )
        last_lgb, last_x_oos = m_lgb, x_all[te_m]

    oos_short = pdf.with_columns(pl.Series("pred", preds)).filter(c("pred").is_finite())
    shap_top: list[dict[str, Any]] = []
    if last_lgb is not None and last_x_oos is not None and len(last_x_oos) > 0:
        sv = np.mean(
            np.abs(np.asarray(shap.TreeExplainer(last_lgb).shap_values(last_x_oos), dtype=float)),
            axis=0,
        )
        shap_top = [
            {"feature": fcols[i], "mean_abs_shap": round(float(sv[i]), 6)}
            for i in np.argsort(sv)[::-1][:8]
        ]

    # Sample ml4t-engineer feature validation on the latest OOS session
    ml4t_verified = False
    if short_bars:
        sample_df = next(iter(short_bars.values()))
        ren = (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume"))
        ohlcv = sample_df.select(
            pl.from_epoch("t_close_ns", time_unit="ns").alias("timestamp"),
            *(c(k).alias(v) for k, v in ren),
        )
        feat_chk = compute_features(
            ohlcv,
            ["roc", "rsi", "natr"],
            timestamp_col="timestamp",
            assume_sorted=True,
            calendar=None,
        )
        ml4t_verified = isinstance(feat_chk, pl.DataFrame) and feat_chk.height == sample_df.height

    return {
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "start_date": start_date,
        "days_scanned": len(all_days),
        "short_total_events": int(pdf.height),
        "short_oos_events": int(oos_short.height),
        "short_oos_days": int(oos_short["trade_date"].n_unique()) if oos_short.height else 0,
        "short_oos_df": oos_short,
        "short_bars": short_bars,
        "long_stress_days": sorted(long_stress_days),
        "long_bars": long_bars,
        "shap_top_features": shap_top,
        "ml4t_engineer_verified": ml4t_verified,
        "l1_sample": load_published_orb_l1_sample(lake_root),
    }

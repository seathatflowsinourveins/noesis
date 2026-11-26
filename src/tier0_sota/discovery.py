"""Strict causal PIT (<= 2026-10-15, t_ns+60s, XNYS), multi-feed regime & upstream signals."""

from __future__ import annotations

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
import numpy as np
import pandera.polars as pa
import polars as pl
import ruptures as rpt
import stumpy
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
REGIME_BENCHMARK_SYMBOLS: tuple[str, ...] = ("SPY", "QQQ", "SOXX", "VIX")
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
    """Classify causal SPY/QQQ/SOXX/VIX regime (`+1` bull, `-1` crash/bear, `0` neutral)."""
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

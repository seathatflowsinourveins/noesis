"""Strict causal PIT (<= 2026-10-15, t_ns+60s, XNYS), multi-feed regime & upstream signals."""

from __future__ import annotations

import io
import math
import os
from collections import deque
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Self
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
from nautilus_trader import model as nm
from nautilus_trader.common import DataActor, DataActorConfig
from nautilus_trader.indicators import SimpleMovingAverage, VolumeWeightedAveragePrice
from nautilus_trader.persistence import BarDataWrangler

MAX_PREWALL_DATE: date = date(2026, 10, 15)


class PreWallViolationError(ValueError):
    """Raised when a caller attempts to use post-wall (> 2026-10-15) data."""


_RAW_WALL_ISO = os.environ.get("TIER0_PREWALL_CUTOFF", "").strip() or MAX_PREWALL_DATE.isoformat()
DISCOVERY_WALL_DATE: date = date.fromisoformat(_RAW_WALL_ISO)
if DISCOVERY_WALL_DATE > MAX_PREWALL_DATE:
    raise PreWallViolationError(
        f"TIER0_PREWALL_CUTOFF {DISCOVERY_WALL_DATE} exceeds maximum pre-wall date "
        f"{MAX_PREWALL_DATE}"
    )
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
_NY_TZ = ZoneInfo("America/New_York")


@lru_cache(maxsize=1)
def get_xnys_calendar() -> Any:
    """Lazily load and cache the XNYS exchange calendar to avoid import-time overhead."""
    import exchange_calendars as xcals

    return xcals.get_calendar("XNYS")


@lru_cache(maxsize=1)
def _bar_schema() -> Any:
    """Lazily build and cache the Pandera Polars bar schema on first validation."""
    import pandera.polars as pa

    bs: dict[str, Any] = {
        "symbol": pa.Column(pl.String, nullable=False),
        "t_ns": pa.Column(pl.Int64, checks=pa.Check.le(DISCOVERY_WALL_UTC_NS), nullable=False),
        **{c: pa.Column(pl.Float64, checks=pa.Check.gt(0.0), nullable=False) for c in "ohlc"},
        "v": pa.Column(pl.Float64, checks=pa.Check.ge(0.0), nullable=False),
    }
    return pa.DataFrameSchema(bs, strict=False)


def _env_positive_int(name: str, default: int, *, max_val: int = 32) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        val = int(raw)
        return max(1, min(val, max_val))
    except ValueError:
        return default


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
    return get_xnys_calendar().previous_session(assert_pre_wall_date(day)).date()


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
        _bar_schema().validate(out)
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
    *,
    rth_close_hm: int = 960,
) -> None:
    """Update causal RTH (09:30 to XNYS close ET) 5-min ORL5 / FR5 / VWAP-fail state."""
    if not (570 <= minute_et < rth_close_hm):
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
    import ruptures as rpt

    pts = rpt.Pelt(model="rbf", min_size=5).fit(vals.reshape(-1, 1)).predict(pen=1.5)
    bkps = [b for b in pts if b < n]
    return float(np.median(vals[bkps])) if bkps else float(np.quantile(vals, 0.90))


def compute_causal_market_regime(market_bars_df: pl.DataFrame, **o: Any) -> dict[int, int]:
    """Classify causal SPY/QQQ/SOXX/RSP/VIX regime (`+1` bull, `-1` crash/bear, `0` neutral)."""
    from ml4t.engineer.features.regime import market_regime_classifier
    from river.drift import ADWIN

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
    import stumpy
    from ml4t.engineer import StandardScaler, compute_features
    from river.drift import ADWIN

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

    import lightgbm as lgb
    import shap
    from catboost import CatBoostRegressor

    lgbm_threads = _env_positive_int("TIER0_LGBM_THREADS", 4)
    cb_threads = _env_positive_int("TIER0_CATBOOST_THREAD_COUNT", 4)

    lgbm_model = lgb.LGBMRegressor(
        n_estimators=24,
        max_depth=3,
        learning_rate=0.05,
        num_leaves=7,
        min_child_samples=5,
        n_jobs=lgbm_threads,
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
        thread_count=cb_threads,
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
    keep_all_vintages: bool = False,
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
        from fredapi import Fred

        fred_client = Fred(api_key=resolved_key)
        rows: list[dict[str, Any]] = []
        for sid in want_ids:
            if hasattr(fred_client, "get_series_all_releases"):
                ser = fred_client.get_series_all_releases(sid)
            else:
                ser = fred_client.get_series_as_of_date(sid, wall.isoformat())
            for _, rec in ser.dropna().iterrows():
                obs_d = parse_iso_date(rec["date"])
                vin_d = parse_iso_date(rec["realtime_start"])
                if vin_d <= wall and obs_d <= wall:
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

    # Strict PIT: keep only releases published on or before wall
    df = df.filter((c("vintage_date") <= wall) & (c("observation_date") <= wall))
    if df.height == 0:
        return df
    ordered = df.sort(["series_id", "observation_date", "vintage_date"]).select(
        ["series_id", "observation_date", "vintage_date", "value"]
    )
    if keep_all_vintages:
        return ordered
    return (
        ordered.group_by(["series_id", "observation_date"], maintain_order=True)
        .last()
        .select(["series_id", "observation_date", "vintage_date", "value"])
    )


def join_causal_fred_asof(
    target_df: pl.DataFrame,
    vintages_df: pl.DataFrame,
    *,
    series_id: str,
    date_col: str = "trade_date",
    output_col: str | None = None,
) -> pl.DataFrame:
    """Attach strictly causal ALFRED values known on or before each decision `date_col`."""
    sid = series_id.strip().upper()
    out_name = output_col or f"fred_{sid.lower()}"
    c = pl.col
    sub = (
        vintages_df.filter(c("series_id").cast(pl.String).str.to_uppercase() == sid)
        .with_columns(
            c("vintage_date").cast(pl.Date),
            c("observation_date").cast(pl.Date),
            c("value").cast(pl.Float64),
        )
        .filter(
            (c("vintage_date") <= DISCOVERY_WALL_DATE)
            & (c("observation_date") <= c("vintage_date"))
        )
        .sort(["vintage_date", "observation_date"])
        .group_by("vintage_date", maintain_order=True)
        .last()
        .select(c("vintage_date"), c("value").alias(out_name))
    )
    left = target_df.with_columns(c(date_col).cast(pl.Date)).sort(date_col)
    if sub.height == 0:
        return left.with_columns(pl.lit(None, dtype=pl.Float64).alias(out_name))
    return left.join_asof(sub, left_on=date_col, right_on="vintage_date", strategy="backward").drop(
        "vintage_date"
    )


EXTREME_MOVER_CATEGORIES: tuple[str, ...] = (
    "PREMARKET_CATALYST_LEADER",
    "SUDDEN_VOLUME_IGNITION",
    "EXTREME_MOVER_CONTINUATION",
    "PARABOLIC_CLIMAX_SHORT_REVERSAL",
    "DARKPOOL_ACCUMULATION_BREAKOUT",
)

CATEGORY_TO_ENGINE_ARCHETYPE: dict[str, str] = {
    "PREMARKET_CATALYST_LEADER": "PREMARKET_EARLY_IGNITION",
    "SUDDEN_VOLUME_IGNITION": "SUDDEN_VOLUME_SURGE",
    "EXTREME_MOVER_CONTINUATION": "EARLY_EP_MOMENTUM_BURST",
    "PARABOLIC_CLIMAX_SHORT_REVERSAL": "PARABOLIC_PEAK_REVERSAL",
    "DARKPOOL_ACCUMULATION_BREAKOUT": "DARKPOOL_ACCUMULATION",
}

CATEGORY_TO_SCANNER_LABEL: dict[str, str] = {
    "PREMARKET_CATALYST_LEADER": "EXTREME_READY",
    "SUDDEN_VOLUME_IGNITION": "SUDDEN_VOLUME_SURGE",
    "EXTREME_MOVER_CONTINUATION": "TECHNICAL_BREAKOUT_LEADER",
    "PARABOLIC_CLIMAX_SHORT_REVERSAL": "CRASH_HEDGE_INVERSE",
    "DARKPOOL_ACCUMULATION_BREAKOUT": "UV_DARKPOOL_OPRA_BUILDUP",
}

EARLY_MOVER_FEATURE_COLS: tuple[str, ...] = (
    "rvol_score",
    "vbr_score",
    "hvr_score",
    "gap_range_score",
    "trf_darkpool_score",
    "momentum_score",
)

DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS: dict[str, float] = {
    "rvol_score": 0.24,
    "vbr_score": 0.16,
    "hvr_score": 0.10,
    "gap_range_score": 0.26,
    "trf_darkpool_score": 0.10,
    "momentum_score": 0.14,
}

EARLY_DETECTION_CHECKPOINTS_ET: tuple[tuple[str, int], ...] = (
    ("04:05", 245),
    ("04:30", 270),
    ("06:00", 360),
    ("08:00", 480),
    ("08:55", 535),
    ("09:00", 540),
    ("09:35", 575),
    ("09:45", 585),
    ("10:00", 600),
    ("10:30", 630),
)

EXTREME_MOVER_BASKET_SYMBOLS: tuple[str, ...] = (
    "NVDA",
    "TQQQ",
    "SOXL",
    "SOXS",
    "SQQQ",
    "PLTR",
    "MSTR",
    "INTC",
    "SNDK",
    "AMD",
    "TSLA",
    "SMCI",
    "AVGO",
    "COIN",
    "MU",
    "META",
    "CRWV",
)


def normalize_early_mover_simplex_weights(raw: Mapping[str, float]) -> dict[str, float]:
    """Normalize positive early-mover feature weights onto the unit simplex."""
    clean = {col: max(1e-4, float(raw.get(col, 0.0))) for col in EARLY_MOVER_FEATURE_COLS}
    total = sum(clean.values())
    if not math.isfinite(total) or total <= 0.0:
        return dict(DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS)
    return {col: round(val / total, 6) for col, val in clean.items()}


def project_calibrated_weights_to_scanner(
    simplex_weights: Mapping[str, float],
) -> dict[str, float]:
    """Project unit-simplex calibration weights into scanner.py's scoring weight schema."""
    norm = normalize_early_mover_simplex_weights(simplex_weights)
    eq = 1.0 / len(EARLY_MOVER_FEATURE_COLS)

    def _scale(base: float, key: str, *, lo: float = 0.5, hi: float = 25.0) -> float:
        return round(max(lo, min(hi, base * (norm[key] / eq))), 4)

    trf_block_mult = 0.5 * (norm["trf_darkpool_score"] + norm["hvr_score"]) / eq
    lit_iso_mult = 0.5 * (norm["vbr_score"] + norm["momentum_score"]) / eq
    leader_mult = 0.5 * (norm["gap_range_score"] + norm["momentum_score"]) / eq
    return {
        "move_weight": _scale(2.0, "gap_range_score"),
        "rvol_weight": _scale(10.0, "rvol_score"),
        "trf_weight": _scale(8.0, "trf_darkpool_score"),
        "trf_block_weight": round(max(0.5, min(25.0, 6.0 * trf_block_mult)), 4),
        "vol_buildup_weight": _scale(4.0, "vbr_score"),
        "lit_iso_weight": round(max(0.5, min(25.0, 5.0 * lit_iso_mult)), 4),
        "breakout_leader_bonus": round(max(0.5, min(25.0, 8.0 * leader_mult)), 4),
    }


def classify_extreme_mover_category(
    *,
    session: str,
    move_pct: float,
    rvol: float,
    vbr: float,
    hvr: float,
    trf_darkpool_ratio: float | None = None,
    range_expansion: float = 1.0,
    momentum_roc: float = 0.0,
    peak_climax_confirmed: bool = False,
    above_vwap: bool = True,
) -> dict[str, Any]:
    """Causally classify a mover into the 5-tier most-potential taxonomy & engine archetype."""
    sess = str(session).strip().upper()
    abs_mv = abs(float(move_pct))
    trf_val = float(trf_darkpool_ratio) if trf_darkpool_ratio is not None else 0.0
    rules: list[str] = []
    if peak_climax_confirmed:
        category = "PARABOLIC_CLIMAX_SHORT_REVERSAL"
        preferred_side = "SHORT" if move_pct >= 0.0 else "LONG"
        rules.extend(["parabolic_extension", "climax_breakdown_confirmed", "flatten_first_reverse"])
    elif sess == "PRE_MARKET" and (abs_mv >= 3.0 or rvol >= 1.5 or vbr >= 2.0):
        category = "PREMARKET_CATALYST_LEADER"
        preferred_side = "LONG" if (move_pct >= 0.0 and above_vwap) else "SHORT"
        rules.extend(["premarket_0400_0929_et", f"abs_move={abs_mv:.2f}%", f"rvol={rvol:.2f}x"])
    elif vbr >= 2.0 or rvol >= 2.5 or (range_expansion >= 1.8 and rvol >= 1.5):
        category = "SUDDEN_VOLUME_IGNITION"
        preferred_side = "LONG" if (move_pct >= 0.0 and momentum_roc >= -0.25) else "SHORT"
        rules.extend([f"vbr={vbr:.2f}x", f"rvol={rvol:.2f}x", f"range_exp={range_expansion:.2f}x"])
    elif trf_val >= 0.35 and (hvr >= 1.2 or abs_mv >= 2.0):
        category = "DARKPOOL_ACCUMULATION_BREAKOUT"
        preferred_side = "LONG" if (move_pct >= 0.0 and above_vwap) else "SHORT"
        rules.extend([f"trf_ratio={trf_val:.3f}", f"hvr={hvr:.2f}"])
    else:
        category = "EXTREME_MOVER_CONTINUATION"
        preferred_side = "LONG" if (move_pct >= 0.0 and above_vwap) else "SHORT"
        rules.extend([f"abs_move={abs_mv:.2f}%", f"roc={momentum_roc:.2f}%"])
    return {
        "category": category,
        "archetype": CATEGORY_TO_ENGINE_ARCHETYPE[category],
        "scanner_category": CATEGORY_TO_SCANNER_LABEL[category],
        "preferred_side": preferred_side,
        "matched_rules": rules,
    }


def extract_causal_intraday_mover_features(
    bars_df: pl.DataFrame,
    *,
    prior_close_by_sym: Mapping[str, float] | None = None,
    baseline_vol_by_sym: Mapping[str, float] | None = None,
    short_vol_ratio_by_sym: Mapping[str, float] | None = None,
    enforce_prewall: bool = True,
) -> pl.DataFrame:
    """Compute strictly causal 1-min extreme-mover features via Polars + ml4t-engineer.

    When `enforce_prewall=True` (all calibration/CPCV paths), fails closed if any bar
    exceeds `DISCOVERY_WALL_UTC_NS` (<= 2026-10-15). Setting `enforce_prewall=False`
    is strictly reserved for isolated post-wall observational replays and still enforces
    causal `t_close_ns = t_ns + 60s` visibility and finite OHLC ordering.
    """
    from ml4t.engineer import compute_features

    if bars_df.height == 0 or "t_ns" not in bars_df.columns:
        raise ValueError("bars_df must be non-empty and contain t_ns")
    c = pl.col
    base = bars_df.with_columns(c("symbol").cast(pl.String).str.to_uppercase())
    if enforce_prewall:
        base = validate_causal_bars(base, validate=True)
    else:
        base = base.with_columns(
            (c("t_ns").cast(pl.Int64) + pl.lit(BAR_DURATION_NS, dtype=pl.Int64)).alias("t_close_ns")
        )
        fin = pl.all_horizontal(
            c(k).cast(pl.Float64).is_finite() for k in ("o", "h", "l", "c", "v")
        )
        hi_lo = (c("h") < pl.max_horizontal("o", "c")) | (c("l") > pl.min_horizontal("o", "c"))
        if bad := base.filter(~fin | hi_lo | (c("h") < c("l")) | (c("l") <= 0.0)).height:
            raise ValueError(f"Invalid non-finite or OHLC-ordered observational bars ({bad})")
    if base.height == 0:
        raise ValueError("No valid causal bars remaining after validation")

    et_dt = (
        pl.from_epoch("t_ns", time_unit="ns")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone("America/New_York")
    )
    df = base.sort(["symbol", "t_ns"]).with_columns(
        et_dt.dt.date().alias("ny_day"),
        (et_dt.dt.hour().cast(pl.Int32) * 60 + et_dt.dt.minute().cast(pl.Int32)).alias("minute_et"),
    )
    df = df.with_columns(
        pl.when(c("minute_et").is_between(240, 569))
        .then(pl.lit("PRE_MARKET"))
        .when(c("minute_et").is_between(570, 959))
        .then(pl.lit("RTH"))
        .when(c("minute_et").is_between(960, 1199))
        .then(pl.lit("AFTER_HOURS"))
        .otherwise(pl.lit("OVERNIGHT"))
        .alias("session_label")
    )

    # Compute ml4t-engineer technicals strictly per symbol in causal order.
    parts: list[pl.DataFrame] = []
    fkw: dict[str, Any] = {"timestamp_col": "timestamp", "assume_sorted": True, "calendar": None}
    for sub in df.partition_by("symbol", maintain_order=True):
        ohlcv = sub.select(
            pl.from_epoch("t_close_ns", time_unit="ns").alias("timestamp"),
            c("o").cast(pl.Float64).alias("open"),
            c("h").cast(pl.Float64).alias("high"),
            c("l").cast(pl.Float64).alias("low"),
            c("c").cast(pl.Float64).alias("close"),
            c("v").cast(pl.Float64).alias("volume"),
        )
        if sub.height >= 16:
            fraw = compute_features(ohlcv, ["roc", "rsi", "natr"], **fkw)
            if not isinstance(fraw, pl.DataFrame):
                raise TypeError(f"Expected Polars DataFrame from ml4t-engineer, got {type(fraw)!r}")
            tech = fraw.select(
                c("roc").cast(pl.Float64).fill_nan(0.0).fill_null(0.0).alias("ml4t_roc"),
                c("rsi").cast(pl.Float64).fill_nan(50.0).fill_null(50.0).alias("ml4t_rsi"),
                c("natr").cast(pl.Float64).fill_nan(0.0).fill_null(0.0).alias("ml4t_natr"),
            )
            parts.append(pl.concat([sub, tech], how="horizontal"))
        else:
            parts.append(
                sub.with_columns(
                    pl.lit(0.0).alias("ml4t_roc"),
                    pl.lit(50.0).alias("ml4t_rsi"),
                    pl.lit(0.0).alias("ml4t_natr"),
                )
            )
    df = pl.concat(parts, how="vertical")

    pclose_map = {str(k).upper(): float(v) for k, v in (prior_close_by_sym or {}).items() if v > 0}
    bvol_map = {str(k).upper(): float(v) for k, v in (baseline_vol_by_sym or {}).items() if v > 0}
    trf_map = {
        str(k).upper(): max(0.0, min(1.0, float(v)))
        for k, v in (short_vol_ratio_by_sym or {}).items()
        if math.isfinite(float(v))
    }

    grp_day = ["symbol", "ny_day"]
    grp_sess = ["symbol", "ny_day", "session_label"]
    v_pos = c("v").cast(pl.Float64).clip(lower_bound=0.0)
    c_f = c("c").cast(pl.Float64)
    h_f = c("h").cast(pl.Float64)
    l_f = c("l").cast(pl.Float64)

    df = df.with_columns(
        c("symbol").replace_strict(pclose_map, default=None, return_dtype=pl.Float64).alias("_pc"),
        c("symbol").replace_strict(bvol_map, default=None, return_dtype=pl.Float64).alias("_bv"),
        c("symbol")
        .replace_strict(trf_map, default=0.0, return_dtype=pl.Float64)
        .alias("trf_darkpool_ratio"),
        (c_f * v_pos).cum_sum().over(grp_day).alias("_pv_cum"),
        v_pos.cum_sum().over(grp_day).alias("cum_vol"),
        c("o").first().over(grp_day).alias("_day_open"),
        h_f.cum_max().over(grp_day).alias("running_high"),
        l_f.cum_min().over(grp_day).alias("running_low"),
        (h_f - l_f).clip(lower_bound=0.0).alias("_bar_rng"),
        pl.int_range(0, pl.len()).over(grp_sess).alias("_s_idx"),
        v_pos.cum_sum().over(grp_sess).alias("_s_v_cum"),
        v_pos.rolling_mean(5, min_samples=1).over(grp_sess).alias("_s_v5"),
        v_pos.rolling_mean(3, min_samples=1).over(grp_sess).alias("_s_v3"),
        v_pos.shift(3).rolling_mean(12, min_samples=1).over(grp_sess).alias("_s_v12_prev"),
        l_f.shift(1).over(grp_day).alias("_prev_low"),
        h_f.shift(1).over(grp_day).alias("_prev_high"),
    )

    prior_sess_v = (
        pl.when(c("_s_idx") > 0)
        .then((c("_s_v_cum") - v_pos) / c("_s_idx").cast(pl.Float64))
        .otherwise(None)
    )
    elapsed_frac = (
        pl.when(c("session_label") == "PRE_MARKET")
        .then(((c("minute_et") - 239).clip(lower_bound=1).cast(pl.Float64) / 330.0) * 0.08)
        .when(c("session_label") == "RTH")
        .then(0.08 + ((c("minute_et") - 569).clip(lower_bound=1).cast(pl.Float64) / 390.0) * 0.92)
        .otherwise(0.05)
        .clip(lower_bound=0.008, upper_bound=1.0)
    )
    prorated_rvol = pl.when(c("_bv").is_not_null() & (c("_bv") > 0.0)).then(
        c("cum_vol") / (c("_bv") * elapsed_frac).clip(lower_bound=1.0)
    )
    same_sess_rvol = pl.when(prior_sess_v.is_not_null() & (prior_sess_v > 0.0)).then(
        c("_s_v5") / prior_sess_v
    )

    df = df.with_columns(
        pl.coalesce([c("_pc"), c("_day_open")]).clip(lower_bound=1e-4).alias("prior_close"),
        pl.when(c("cum_vol") > 0.0).then(c("_pv_cum") / c("cum_vol")).otherwise(c_f).alias("vwap"),
        (c("cum_vol") * c_f).alias("cum_dvol"),
        pl.max_horizontal(same_sess_rvol.fill_null(0.0), prorated_rvol.fill_null(0.0))
        .clip(lower_bound=0.0, upper_bound=100.0)
        .alias("rvol"),
        pl.when(c("_s_v12_prev").is_not_null() & (c("_s_v12_prev") > 0.0))
        .then(c("_s_v3") / c("_s_v12_prev"))
        .otherwise(pl.when(c("_s_idx") == 0).then(1.0).otherwise(0.0))
        .clip(lower_bound=0.0, upper_bound=100.0)
        .alias("vbr"),
        c("_bar_rng").rolling_mean(14, min_samples=1).over(grp_day).alias("_atr14_px"),
        (
            h_f.rolling_max(5, min_samples=1).over(grp_day)
            - l_f.rolling_min(5, min_samples=1).over(grp_day)
        )
        .clip(lower_bound=0.0)
        .alias("_rng5"),
    )

    atr_ref = pl.max_horizontal(
        c_f * (c("ml4t_natr").clip(lower_bound=0.0) / 100.0),
        c("_atr14_px"),
        pl.lit(0.01),
    )
    df = df.with_columns(
        ((c_f / c("prior_close") - 1.0) * 100.0).alias("move_pct"),
        ((c("_day_open") / c("prior_close") - 1.0) * 100.0).alias("gap_pct"),
        (c("rvol") / (c("_bar_rng") / atr_ref).clip(lower_bound=0.25))
        .clip(lower_bound=0.0, upper_bound=100.0)
        .alias("hvr"),
        (c("_rng5") / (atr_ref * 2.5).clip(lower_bound=1e-4))
        .clip(lower_bound=0.0, upper_bound=20.0)
        .alias("range_expansion"),
        ((atr_ref / c_f).clip(lower_bound=0.001, upper_bound=0.25)).alias("natr_frac"),
    )

    dd_from_hi = (c("running_high") - c_f) / c("running_high").clip(lower_bound=1e-4)
    bounce_from_lo = (c_f - c("running_low")) / c("running_low").clip(lower_bound=1e-4)
    pull_thr = pl.max_horizontal(pl.lit(0.0045), c("natr_frac") * 0.25)
    bull_climax = (
        (c("move_pct") >= 5.0)
        & (c_f >= c("vwap") * 1.01)
        & (dd_from_hi >= pull_thr)
        & (c_f < c("o"))
        & (c("_prev_low").is_not_null() & (c_f < c("_prev_low")))
        & (c("rvol") >= 1.5)
    )
    bear_climax = (
        (c("move_pct") <= -5.0)
        & (c_f <= c("vwap") * 0.99)
        & (bounce_from_lo >= pull_thr)
        & (c_f > c("o"))
        & (c("_prev_high").is_not_null() & (c_f > c("_prev_high")))
        & (c("rvol") >= 1.5)
    )
    vwap_aligned = ((c("move_pct") >= 0.0) & (c_f >= c("vwap"))) | (
        (c("move_pct") < 0.0) & (c_f <= c("vwap"))
    )

    df = df.with_columns(
        c("move_pct").abs().alias("abs_move_pct"),
        (bull_climax | bear_climax).alias("peak_climax_confirmed"),
        pl.when(bull_climax)
        .then(pl.lit(-1, dtype=pl.Int8))
        .when(bear_climax)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(0, dtype=pl.Int8))
        .alias("peak_climax_dir"),
        (c("rvol").log1p() / math.log1p(10.0)).clip(0.0, 1.0).alias("rvol_score"),
        (c("vbr").log1p() / math.log1p(8.0)).clip(0.0, 1.0).alias("vbr_score"),
        (c("hvr").log1p() / math.log1p(8.0)).clip(0.0, 1.0).alias("hvr_score"),
        (
            (c("move_pct").abs() / 12.0).clip(0.0, 1.0) * 0.65
            + (c("range_expansion") / 5.0).clip(0.0, 1.0) * 0.35
        )
        .clip(0.0, 1.0)
        .alias("gap_range_score"),
        (c("trf_darkpool_ratio") / 0.65).clip(0.0, 1.0).alias("trf_darkpool_score"),
        (
            (c("ml4t_roc").abs() / 4.0).clip(0.0, 1.0) * 0.50
            + ((c("ml4t_rsi") - 50.0).abs() / 35.0).clip(0.0, 1.0) * 0.30
            + vwap_aligned.cast(pl.Float64) * 0.20
        )
        .clip(0.0, 1.0)
        .alias("momentum_score"),
    ).drop(
        [
            "_pc",
            "_bv",
            "_pv_cum",
            "_day_open",
            "_bar_rng",
            "_s_idx",
            "_s_v_cum",
            "_s_v5",
            "_s_v3",
            "_s_v12_prev",
            "_prev_low",
            "_prev_high",
            "_atr14_px",
            "_rng5",
        ]
    )
    return df


def score_early_mover_features(
    features_df: pl.DataFrame,
    weights: Mapping[str, float] = DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS,
) -> pl.DataFrame:
    """Attach strictly causal composite `mover_score` from normalized feature weights."""
    norm = normalize_early_mover_simplex_weights(weights)
    score_expr = sum(
        (pl.col(col).cast(pl.Float64).fill_null(0.0) * float(norm[col]))
        for col in EARLY_MOVER_FEATURE_COLS
    )
    if isinstance(score_expr, int):
        raise ValueError("EARLY_MOVER_FEATURE_COLS cannot be empty")
    return features_df.with_columns(score_expr.alias("mover_score"))


def _fmt_ns_et_utc(ts_ns: int) -> tuple[str, str]:
    dt_utc = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC)
    dt_et = dt_utc.astimezone(_NY_TZ)
    return dt_et.strftime("%Y-%m-%d %H:%M:%S %Z"), dt_utc.isoformat()


def evaluate_early_mover_detection(
    features_df: pl.DataFrame,
    *,
    weights: Mapping[str, float] = DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS,
    score_threshold: float = 0.38,
    top_k: int = 15,
    extreme_move_pct: float = 5.0,
    extreme_rvol: float = 1.5,
    min_dvol: float = 5_000_000.0,
    min_price: float = 10.0,
) -> dict[str, Any]:
    """Measure recall, precision, F1, and exact early-detection latency per (day, symbol)."""
    scored = score_early_mover_features(features_df, weights)
    c = pl.col
    day_sym = (
        scored.sort(["ny_day", "symbol", "t_close_ns"])
        .group_by(["ny_day", "symbol"], maintain_order=True)
        .agg(
            c("abs_move_pct").max().alias("peak_abs_move_pct"),
            c("rvol").max().alias("peak_rvol"),
            c("cum_dvol").max().alias("eod_dvol"),
            c("c").max().alias("max_close"),
            c("h").max().alias("session_high"),
            c("l").min().alias("session_low"),
            c("c").last().alias("eod_close"),
            c("prior_close").first().alias("prior_close"),
            c("move_pct").sort_by("abs_move_pct").last().alias("peak_signed_move_pct"),
            c("t_close_ns").sort_by("abs_move_pct").last().alias("peak_ts_ns"),
            c("minute_et").sort_by("abs_move_pct").last().alias("peak_minute_et"),
            c("c").sort_by("abs_move_pct").last().alias("peak_price"),
            c("mover_score").max().alias("max_score"),
        )
        .with_columns(
            (
                (c("peak_abs_move_pct") >= extreme_move_pct)
                & ((c("peak_rvol") >= extreme_rvol) | (c("eod_dvol") >= min_dvol))
                & (c("max_close") >= min_price)
            ).alias("is_true_extreme_mover")
        )
    )

    first_hits = (
        scored.filter(
            (c("mover_score") >= score_threshold) & (c("c") >= min_price) & (c("minute_et") <= 960)
        )
        .sort(["ny_day", "symbol", "t_close_ns"])
        .group_by(["ny_day", "symbol"], maintain_order=True)
        .first()
        .select(
            "ny_day",
            "symbol",
            c("t_close_ns").alias("detect_ts_ns"),
            c("minute_et").alias("detect_minute_et"),
            c("session_label").alias("detect_session"),
            c("c").alias("detect_price"),
            c("move_pct").alias("detect_move_pct"),
            c("mover_score").alias("detect_score"),
            c("rvol").alias("detect_rvol"),
            c("vbr").alias("detect_vbr"),
            c("hvr").alias("detect_hvr"),
            c("trf_darkpool_ratio").alias("detect_trf_ratio"),
            c("range_expansion").alias("detect_range_exp"),
            c("ml4t_roc").alias("detect_roc"),
            c("natr_frac").alias("detect_natr_frac"),
            (c("c") >= c("vwap")).alias("detect_above_vwap"),
            c("peak_climax_confirmed").alias("detect_peak_climax"),
        )
    )

    reversals = (
        scored.filter(c("peak_climax_confirmed"))
        .sort(["ny_day", "symbol", "t_close_ns"])
        .group_by(["ny_day", "symbol"], maintain_order=True)
        .first()
        .select(
            "ny_day",
            "symbol",
            c("t_close_ns").alias("peak_rev_ts_ns"),
            c("c").alias("peak_rev_price"),
            c("peak_climax_dir").alias("peak_rev_dir"),
        )
    )

    joined = day_sym.join(first_hits, on=["ny_day", "symbol"], how="left").join(
        reversals, on=["ny_day", "symbol"], how="left"
    )

    # Top-K ranking precision/recall per day using max score achieved before 10:30 ET.
    early_rank = (
        scored.filter((c("minute_et") <= 630) & (c("c") >= min_price))
        .group_by(["ny_day", "symbol"])
        .agg(c("mover_score").max().alias("early_max_score"))
        .join(
            day_sym.select("ny_day", "symbol", "is_true_extreme_mover"),
            on=["ny_day", "symbol"],
            how="left",
        )
        .with_columns(c("is_true_extreme_mover").fill_null(False))
    )

    daily_recalls: list[float] = []
    daily_precs: list[float] = []
    daily_f1s: list[float] = []
    daily_scores: list[dict[str, Any]] = []
    for day_sub in early_rank.partition_by("ny_day", maintain_order=True):
        day_iso = str(day_sub["ny_day"][0])
        true_n = int(day_sub.filter(c("is_true_extreme_mover")).height)
        ranked = day_sub.filter(c("early_max_score") >= score_threshold).sort(
            "early_max_score", descending=True
        )
        picked = ranked.head(top_k)
        tp = int(picked.filter(c("is_true_extreme_mover")).height)
        pred_n = int(picked.height)
        rec = (tp / true_n) if true_n > 0 else 1.0
        prec = (tp / pred_n) if pred_n > 0 else 0.0
        f1 = (2.0 * prec * rec / (prec + rec)) if (prec + rec) > 0.0 else 0.0
        daily_recalls.append(rec)
        daily_precs.append(prec)
        daily_f1s.append(f1)
        daily_scores.append(
            {
                "trade_date": day_iso,
                "true_extreme_count": true_n,
                "predicted_count": pred_n,
                "true_positives": tp,
                "recall_at_k": round(rec, 4),
                "precision_at_k": round(prec, 4),
                "f1_at_k": round(f1, 4),
            }
        )

    checkpoint_metrics: dict[str, dict[str, float]] = {}
    for cp_label, cp_min in EARLY_DETECTION_CHECKPOINTS_ET:
        cp_df = (
            scored.filter((c("minute_et") <= cp_min) & (c("c") >= min_price))
            .group_by(["ny_day", "symbol"])
            .agg(c("mover_score").max().alias("cp_score"))
            .join(
                day_sym.select("ny_day", "symbol", "is_true_extreme_mover"),
                on=["ny_day", "symbol"],
                how="left",
            )
            .with_columns(c("is_true_extreme_mover").fill_null(False))
        )
        recs, precs = [], []
        for sub in cp_df.partition_by("ny_day", maintain_order=True):
            tn = int(sub.filter(c("is_true_extreme_mover")).height)
            pk = (
                sub.filter(c("cp_score") >= score_threshold)
                .sort("cp_score", descending=True)
                .head(top_k)
            )
            tp_c = int(pk.filter(c("is_true_extreme_mover")).height)
            if tn > 0:
                recs.append(tp_c / tn)
            if pk.height > 0:
                precs.append(tp_c / pk.height)
        checkpoint_metrics[cp_label] = {
            "recall_at_k": round(float(np.mean(recs)) if recs else 0.0, 4),
            "precision_at_k": round(float(np.mean(precs)) if precs else 0.0, 4),
        }

    latency_rows: list[dict[str, Any]] = []
    lead_mins_all: list[float] = []
    rem_moves_all: list[float] = []
    for row in joined.filter(
        c("is_true_extreme_mover") | c("detect_ts_ns").is_not_null()
    ).iter_rows(named=True):
        det_ns = row["detect_ts_ns"]
        pk_ns = int(row["peak_ts_ns"])
        pk_et, pk_utc = _fmt_ns_et_utc(pk_ns)
        pk_px = float(row["peak_price"])
        prior_c = float(row["prior_close"])
        det_et = det_utc = None
        lead_min = rem_pct = None
        cat_info: dict[str, Any] = {
            "category": "EXTREME_MOVER_CONTINUATION",
            "archetype": CATEGORY_TO_ENGINE_ARCHETYPE["EXTREME_MOVER_CONTINUATION"],
            "scanner_category": CATEGORY_TO_SCANNER_LABEL["EXTREME_MOVER_CONTINUATION"],
            "preferred_side": "LONG" if float(row["peak_signed_move_pct"]) >= 0.0 else "SHORT",
            "matched_rules": [],
        }
        if det_ns is not None:
            det_et, det_utc = _fmt_ns_et_utc(int(det_ns))
            lead_min = round((pk_ns - int(det_ns)) / 60_000_000_000.0, 2)
            det_px = float(row["detect_price"])
            if float(row["peak_signed_move_pct"]) >= 0.0:
                rem_pct = round(
                    ((float(row["session_high"]) - det_px) / max(det_px, 1e-4)) * 100.0, 3
                )
            else:
                rem_pct = round(
                    ((det_px - float(row["session_low"])) / max(det_px, 1e-4)) * 100.0, 3
                )
            if bool(row["is_true_extreme_mover"]):
                lead_mins_all.append(lead_min)
                rem_moves_all.append(rem_pct)
            cat_info = classify_extreme_mover_category(
                session=str(row["detect_session"]),
                move_pct=float(row["detect_move_pct"]),
                rvol=float(row["detect_rvol"]),
                vbr=float(row["detect_vbr"]),
                hvr=float(row["detect_hvr"]),
                trf_darkpool_ratio=float(row["detect_trf_ratio"]),
                range_expansion=float(row["detect_range_exp"]),
                momentum_roc=float(row["detect_roc"]),
                peak_climax_confirmed=bool(row["detect_peak_climax"]),
                above_vwap=bool(row["detect_above_vwap"]),
            )
        rev_et = rev_utc = None
        if row["peak_rev_ts_ns"] is not None:
            rev_et, rev_utc = _fmt_ns_et_utc(int(row["peak_rev_ts_ns"]))
        latency_rows.append(
            {
                "trade_date": str(row["ny_day"]),
                "symbol": str(row["symbol"]),
                "is_true_extreme_mover": bool(row["is_true_extreme_mover"]),
                "prior_close": round(prior_c, 4),
                "first_detect_et": det_et,
                "first_detect_utc": det_utc,
                "first_detect_session": row["detect_session"],
                "first_detect_price": (
                    round(float(row["detect_price"]), 4) if det_ns is not None else None
                ),
                "first_detect_move_pct": (
                    round(float(row["detect_move_pct"]), 3) if det_ns is not None else None
                ),
                "first_detect_score": (
                    round(float(row["detect_score"]), 4) if det_ns is not None else None
                ),
                "session_high": round(float(row["session_high"]), 4),
                "session_low": round(float(row["session_low"]), 4),
                "session_peak_et": pk_et,
                "session_peak_utc": pk_utc,
                "session_peak_price": round(pk_px, 4),
                "session_peak_move_pct": round(float(row["peak_signed_move_pct"]), 3),
                "lead_minutes_to_peak": lead_min,
                "remaining_move_to_peak_pct": rem_pct,
                "peak_reversal_et": rev_et,
                "peak_reversal_utc": rev_utc,
                "peak_reversal_price": (
                    round(float(row["peak_rev_price"]), 4)
                    if row["peak_rev_price"] is not None
                    else None
                ),
                "category": cat_info["category"],
                "archetype": cat_info["archetype"],
                "scanner_category": cat_info["scanner_category"],
                "preferred_side": cat_info["preferred_side"],
                "rvol_at_detect": (
                    round(float(row["detect_rvol"]), 3) if det_ns is not None else None
                ),
                "vbr_at_detect": (
                    round(float(row["detect_vbr"]), 3) if det_ns is not None else None
                ),
                "hvr_at_detect": (
                    round(float(row["detect_hvr"]), 3) if det_ns is not None else None
                ),
                "trf_ratio": (
                    round(float(row["detect_trf_ratio"]), 4) if det_ns is not None else None
                ),
                "natr_pct_at_detect": (
                    round(float(row["detect_natr_frac"]), 6) if det_ns is not None else 0.015
                ),
                "eod_dvol_usd": round(float(row["eod_dvol"]), 2),
            }
        )

    mean_rec = float(np.mean(daily_recalls)) if daily_recalls else 0.0
    mean_prec = float(np.mean(daily_precs)) if daily_precs else 0.0
    mean_f1 = float(np.mean(daily_f1s)) if daily_f1s else 0.0
    med_lead = float(np.median(lead_mins_all)) if lead_mins_all else 0.0
    med_rem = float(np.median(rem_moves_all)) if rem_moves_all else 0.0
    premarket_hits = sum(
        1
        for r in latency_rows
        if r["is_true_extreme_mover"] and r["first_detect_session"] == "PRE_MARKET"
    )
    true_total = sum(1 for r in latency_rows if r["is_true_extreme_mover"])
    return {
        "n_sessions": len(daily_scores),
        "true_extreme_movers_total": true_total,
        "premarket_early_hits": premarket_hits,
        "recall_at_k": round(mean_rec, 4),
        "precision_at_k": round(mean_prec, 4),
        "f1_at_k": round(mean_f1, 4),
        "median_lead_minutes_to_peak": round(med_lead, 2),
        "median_remaining_move_to_peak_pct": round(med_rem, 3),
        "score_threshold": round(float(score_threshold), 4),
        "top_k": int(top_k),
        "checkpoints_et": checkpoint_metrics,
        "daily_metrics": daily_scores,
        "latency_table": sorted(
            latency_rows,
            key=lambda x: (
                not x["is_true_extreme_mover"],
                -abs(float(x["session_peak_move_pct"] or 0.0)),
            ),
        ),
    }


EARLY_MOVER_WEIGHT_KEYS: tuple[str, ...] = (
    "w_move",
    "w_rvol",
    "w_vol_buildup",
    "w_trf_darkpool",
    "w_hvr",
    "w_tech_breakout",
    "w_premarket_ignition",
)
DEFAULT_EARLY_MOVER_WEIGHTS: dict[str, float] = {
    "w_move": 2.0,
    "w_rvol": 10.0,
    "w_vol_buildup": 4.0,
    "w_trf_darkpool": 8.0,
    "w_hvr": 3.0,
    "w_tech_breakout": 0.15,
    "w_premarket_ignition": 18.0,
}
POTENTIAL_CATEGORIES: tuple[str, ...] = (
    "PREMARKET_EARLY_IGNITION",
    "EP_MOMENTUM_BURST",
    "SUDDEN_VOLUME_SURGE",
    "DARKPOOL_ACCUMULATION",
    "VCP_STAGE2_BREAKOUT",
    "PEAD_DRIFT",
    "PARABOLIC_PEAK_REVERSAL",
    "CRASH_INVERSE_HEDGE",
    "EXTREME_MOVER_WATCH",
)
INVERSE_HEDGE_SYMBOLS: frozenset[str] = frozenset({"SOXS", "SQQQ", "SPXS", "SDOW"})
EARLY_MOVER_CHECKPOINTS_ET: tuple[int, ...] = (
    241,
    245,
    255,
    270,
    300,
    330,
    360,
    420,
    480,
    540,
    565,
    575,
    585,
    600,
    615,
    630,
    660,
    720,
    780,
    840,
    900,
    955,
)


def _fmt_minute_et(minute_et: int) -> str:
    h, m = divmod(max(0, int(minute_et)), 60)
    return f"{h:02d}:{m:02d}"


def normalize_early_mover_scoring_weights(raw: Mapping[str, Any] | None) -> dict[str, float]:
    """Validate and clamp early-mover scoring weights with deterministic defaults."""
    out = dict(DEFAULT_EARLY_MOVER_WEIGHTS)
    if not isinstance(raw, Mapping):
        return out
    bounds: dict[str, tuple[float, float]] = {
        "w_move": (0.2, 12.0),
        "w_rvol": (0.5, 30.0),
        "w_vol_buildup": (0.0, 20.0),
        "w_trf_darkpool": (0.0, 30.0),
        "w_hvr": (0.0, 15.0),
        "w_tech_breakout": (0.0, 1.0),
        "w_premarket_ignition": (0.0, 45.0),
    }
    for key in EARLY_MOVER_WEIGHT_KEYS:
        val = raw.get(key)
        if isinstance(val, bool) or val is None:
            continue
        try:
            fval = float(val)
        except (TypeError, ValueError):
            continue
        if math.isfinite(fval):
            lo, hi = bounds[key]
            out[key] = round(max(lo, min(hi, fval)), 6)
    return out


def _causal_ema_last(values: Sequence[float], span: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (max(1, int(span)) + 1.0)
    ema = float(values[0])
    for v in values[1:]:
        ema = alpha * float(v) + (1.0 - alpha) * ema
    return ema


def compute_causal_technical_alignment(
    closes: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    volumes: Sequence[float],
    *,
    prior_close: float | None = None,
    high_20d_prior: float | None = None,
    rvol: float = 0.0,
    vol_buildup_ratio: float = 0.0,
    trf_block_ratio: float | None = None,
    darkpool_trf_ratio: float | None = None,
    session: str = "RTH",
    move_pct: float | None = None,
) -> dict[str, Any]:
    """Compute strictly causal 0..100 technical alignment & early-ignition signals."""
    n = min(len(closes), len(highs), len(lows), len(volumes))
    if n == 0:
        return {
            "technical_alignment_score": 0.0,
            "vwap": 0.0,
            "ema_fast": 0.0,
            "ema_slow": 0.0,
            "vwap_reclaim": False,
            "ema_aligned": False,
            "breakout_proximity": 0.0,
            "breakout_20d": False,
            "sudden_volume_surge": False,
            "premarket_ignition": False,
            "early_ignition_detected": False,
        }

    cl = [float(closes[i]) for i in range(n)]
    hi = [float(highs[i]) for i in range(n)]
    lo = [float(lows[i]) for i in range(n)]
    vo = [max(0.0, float(volumes[i])) for i in range(n)]

    last_c = cl[-1]
    ref_px = (
        float(prior_close)
        if prior_close is not None and math.isfinite(prior_close) and prior_close > 0.0
        else cl[0]
    )
    eff_move = (
        float(move_pct)
        if move_pct is not None and math.isfinite(move_pct)
        else (((last_c / max(1e-6, ref_px)) - 1.0) * 100.0)
    )
    is_bull = eff_move >= 0.0

    # Causal session VWAP strictly over bars[0..n-1]
    pv_sum = 0.0
    v_sum = 0.0
    for i in range(n):
        tp = (hi[i] + lo[i] + cl[i]) / 3.0
        pv_sum += tp * vo[i]
        v_sum += vo[i]
    vwap = pv_sum / v_sum if v_sum > 0.0 else last_c
    cum_dvol = v_sum * last_c

    ema_fast = _causal_ema_last(cl, 5)
    ema_slow = _causal_ema_last(cl, 13)
    sess_high = max(hi)
    sess_low = min(lo)

    vwap_aligned = (last_c >= vwap) if is_bull else (last_c <= vwap)
    vwap_reclaim = bool(
        vwap_aligned
        and (
            n == 1
            or (is_bull and (lo[-1] <= vwap * 1.002 or cl[-2] <= vwap))
            or ((not is_bull) and (hi[-1] >= vwap * 0.998 or cl[-2] >= vwap))
        )
    )
    ema_aligned = (last_c >= ema_fast >= ema_slow) if is_bull else (last_c <= ema_fast <= ema_slow)

    if is_bull:
        breakout_prox = max(0.0, min(1.0, last_c / max(1e-6, sess_high)))
    else:
        breakout_prox = max(0.0, min(1.0, sess_low / max(1e-6, last_c)))
    breakout_20d = bool(
        high_20d_prior is not None
        and high_20d_prior > 0.0
        and is_bull
        and last_c >= float(high_20d_prior) * 0.995
    )

    last_bar_surge = bool(
        n >= 2
        and (mean_prior := sum(vo[:-1]) / (n - 1)) > 0.0
        and vo[-1] >= 1.8 * mean_prior
        and vo[-1] * last_c >= 5_000_000.0
    )
    sudden_volume_surge = bool(rvol >= 2.2 or vol_buildup_ratio >= 2.0 or last_bar_surge)

    # 5-pillar causal 0..100 technical alignment score
    vwap_pts = (
        25.0
        if vwap_reclaim
        else (
            20.0
            if vwap_aligned
            else (8.0 if abs(last_c - vwap) / max(1e-6, vwap) <= 0.005 else 0.0)
        )
    )
    ema_pts = (
        20.0
        if ema_aligned
        else (12.0 if ((last_c >= ema_fast) if is_bull else (last_c <= ema_fast)) else 0.0)
    )
    brk_pts = max(0.0, min(16.0, (breakout_prox - 0.94) / 0.06 * 16.0)) + (
        4.0 if breakout_20d else 0.0
    )
    vol_pts = min(12.0, math.log1p(max(0.0, rvol)) * 7.5) + min(
        8.0, max(0.0, vol_buildup_ratio - 1.0) * 4.0
    )
    trf_eff = max(
        float(trf_block_ratio) if trf_block_ratio is not None else 0.0,
        (float(darkpool_trf_ratio) * 0.85) if darkpool_trf_ratio is not None else 0.0,
    )
    if trf_eff > 0.0:
        inst_pts = min(15.0, trf_eff * 30.0)
    else:
        inst_pts = min(15.0, math.log1p(max(0.0, cum_dvol) / 5_000_000.0) * 4.5)

    tech_score = round(max(0.0, min(100.0, vwap_pts + ema_pts + brk_pts + vol_pts + inst_pts)), 2)

    sess_norm = str(session).strip().upper()
    premarket_ignition = bool(
        sess_norm == "PRE_MARKET"
        and abs(eff_move) >= 2.5
        and vwap_aligned
        and (
            rvol >= 1.5
            or vol_buildup_ratio >= 1.5
            or sudden_volume_surge
            or cum_dvol >= 25_000_000.0
        )
    )
    early_ignition_detected = bool(
        premarket_ignition or (abs(eff_move) >= 2.0 and sudden_volume_surge and tech_score >= 55.0)
    )
    return {
        "technical_alignment_score": tech_score,
        "vwap": round(vwap, 4),
        "ema_fast": round(ema_fast, 4),
        "ema_slow": round(ema_slow, 4),
        "vwap_reclaim": vwap_reclaim,
        "ema_aligned": ema_aligned,
        "breakout_proximity": round(breakout_prox, 4),
        "breakout_20d": breakout_20d,
        "sudden_volume_surge": sudden_volume_surge,
        "premarket_ignition": premarket_ignition,
        "early_ignition_detected": early_ignition_detected,
    }


def classify_potential_category(
    *,
    symbol: str,
    session: str,
    move_pct: float,
    rvol: float,
    vol_buildup_ratio: float,
    hidden_volume_ratio: float,
    darkpool_trf_ratio: float | None,
    trf_block_ratio: float | None,
    macro_regime: int,
    peak_confirmed: bool,
    premarket_ignition: bool,
    sudden_volume_surge: bool,
    technical_alignment_score: float,
    breakout_20d: bool = False,
    coil_ratio: float | None = None,
    post_earnings_gap: bool = False,
) -> str:
    """Deterministically classify a candidate into one of the 9 Tier-0 potential categories."""
    sym = str(symbol).strip().upper()
    if peak_confirmed:
        return "PARABOLIC_PEAK_REVERSAL"
    if macro_regime == -1 or (sym in INVERSE_HEDGE_SYMBOLS and move_pct >= 1.5):
        return "CRASH_INVERSE_HEDGE"
    if premarket_ignition or (
        str(session).strip().upper() == "PRE_MARKET"
        and abs(move_pct) >= 3.0
        and (rvol >= 1.5 or vol_buildup_ratio >= 1.5)
    ):
        return "PREMARKET_EARLY_IGNITION"
    if post_earnings_gap:
        return "PEAD_DRIFT"
    if abs(move_pct) >= 3.5 and rvol >= 2.5 and technical_alignment_score >= 55.0:
        return "EP_MOMENTUM_BURST"
    if sudden_volume_surge or rvol >= 2.2 or vol_buildup_ratio >= 2.0:
        return "SUDDEN_VOLUME_SURGE"
    if breakout_20d or (
        coil_ratio is not None
        and coil_ratio <= 0.75
        and technical_alignment_score >= 60.0
        and move_pct >= 1.5
    ):
        return "VCP_STAGE2_BREAKOUT"
    if (
        (darkpool_trf_ratio is not None and darkpool_trf_ratio >= 0.35)
        or (trf_block_ratio is not None and trf_block_ratio >= 0.25)
        or hidden_volume_ratio >= 1.5
    ):
        return "DARKPOOL_ACCUMULATION"
    return "EXTREME_MOVER_WATCH"


def score_early_mover_candidate(
    *,
    move_pct: float,
    rvol: float,
    vol_buildup_ratio: float,
    darkpool_trf_ratio: float | None,
    trf_block_ratio: float | None,
    hidden_volume_ratio: float,
    technical_alignment_score: float,
    premarket_ignition: bool,
    is_extreme_eligible: bool = False,
    is_pinned: bool = False,
    weights: Mapping[str, float] | None = None,
) -> float:
    """Canonical causal early-mover ranking score parameterized by the 7 Optuna weights."""
    w = normalize_early_mover_scoring_weights(weights)
    trf_term = float(darkpool_trf_ratio or 0.0) + 0.75 * float(trf_block_ratio or 0.0)
    score = (
        w["w_move"] * abs(float(move_pct))
        + w["w_rvol"] * math.log1p(max(0.0, float(rvol)))
        + w["w_vol_buildup"] * min(4.0, max(0.0, float(vol_buildup_ratio) - 1.0))
        + w["w_trf_darkpool"] * trf_term
        + w["w_hvr"] * min(5.0, max(0.0, float(hidden_volume_ratio)))
        + w["w_tech_breakout"] * max(0.0, min(100.0, float(technical_alignment_score)))
        + (w["w_premarket_ignition"] if premarket_ignition else 0.0)
        + (25.0 if is_extreme_eligible else 0.0)
        + (15.0 if is_pinned else 0.0)
    )
    return round(float(score), 4)


def list_prewall_bar_days(
    root: Path = DEFAULT_MARKET_DATA_ROOT, *, max_days: int | None = None
) -> list[date]:
    """List sorted pre-wall 1-min bar dates strictly <= DISCOVERY_WALL_DATE."""
    bars_root = root / "alpaca-sip" / "raw" / "bars-1min"
    if not bars_root.is_dir():
        return []
    valid: list[date] = []
    for child in sorted(bars_root.iterdir()):
        if not child.is_dir():
            continue
        try:
            d = assert_pre_wall_date(child.name)
            valid.append(d)
        except ValueError:
            continue
    if max_days is not None and max_days > 0:
        return valid[-int(max_days) :]
    return valid


def _load_causal_t1_short_vol(
    root: Path, target_days_iso: Sequence[str]
) -> dict[str, pl.DataFrame]:
    """Load strictly T-1 FINRA short-volume ratios (day < trade_date) per target day."""
    sv_path = root / "public-tables" / "short_volume.parquet"
    if not sv_path.is_file() or not target_days_iso:
        return {}
    min_d = min(target_days_iso)
    max_d = max(target_days_iso)
    # Keep a 14-day lookback window prior to min_d while strictly excluding >= max_d
    min_lookback = date.fromisoformat(min_d).toordinal() - 14
    lb_iso = date.fromordinal(max(1, min_lookback)).isoformat()
    raw = (
        pl.scan_parquet(sv_path)
        .filter((pl.col("day") >= lb_iso) & (pl.col("day") < max_d) & (pl.col("total_volume") > 0))
        .select(
            pl.col("day").cast(pl.String),
            pl.col("symbol").cast(pl.String).str.to_uppercase(),
            (pl.col("short_volume") / pl.col("total_volume")).clip(0.0, 1.0).alias("t1_trf_proxy"),
        )
        .collect()
    )
    out: dict[str, pl.DataFrame] = {}
    for d_iso in target_days_iso:
        prior = (
            raw.filter(pl.col("day") < d_iso)
            .sort(["symbol", "day"])
            .group_by("symbol", maintain_order=False)
            .last()
            .select(["symbol", "t1_trf_proxy"])
        )
        out[d_iso] = prior
    return out


def build_prewall_early_mover_session_panel(
    days: Sequence[date | str],
    *,
    root: Path = DEFAULT_MARKET_DATA_ROOT,
    min_price: float = 10.0,
    min_daily_dollar_vol: float = 100_000_000.0,
    min_daily_move_pct: float = 6.0,
    min_daily_rvol: float = 1.5,
    max_targets_per_day: int = 15,
    checkpoints_et: Sequence[int] = EARLY_MOVER_CHECKPOINTS_ET,
) -> dict[str, Any]:
    """Build strictly causal pre-wall (<= 2026-10-15) checkpoint panel for early-mover tuning."""
    checked_days = [assert_pre_wall_date(d) for d in days]
    day_isos = sorted({d.isoformat() for d in checked_days})
    if not day_isos:
        raise ValueError("No valid pre-wall days supplied")
    cps = tuple(sorted({int(cp) for cp in checkpoints_et if 240 < int(cp) <= 960}))

    df10_path = root / "derived" / "df10_daily_20d_baselines.parquet"
    has_df10 = df10_path.is_file()
    df10_all = (
        pl.scan_parquet(df10_path)
        .filter(pl.col("trade_date").is_in(day_isos))
        .select(
            pl.col("symbol").cast(pl.String).str.to_uppercase(),
            pl.col("trade_date").cast(pl.String),
            pl.col("prev_raw_close").cast(pl.Float64),
            pl.col("mean_20d_vol").cast(pl.Float64),
            pl.col("mean_20d_dvol").cast(pl.Float64),
            pl.col("high_20d_prior").cast(pl.Float64),
            pl.col("coil_5d_20d_range_ratio").cast(pl.Float64),
        )
        .collect()
        if has_df10
        else pl.DataFrame()
    )
    t1_short_map = _load_causal_t1_short_vol(root, day_isos)

    targets_by_day: dict[str, dict[str, dict[str, Any]]] = {}
    snapshots_by_day: dict[str, dict[int, list[dict[str, Any]]]] = {}
    post_ext_by_day_cp: dict[tuple[str, int, str], tuple[float, float]] = {}
    c = pl.col

    for d_iso in day_isos:
        day_dir = root / "alpaca-sip" / "raw" / "bars-1min" / d_iso
        if not day_dir.is_dir():
            continue
        raw_bars = (
            pl.scan_parquet(str(day_dir / "*.parquet"))
            .select(
                c("symbol").cast(pl.String).str.to_uppercase(),
                c("t_ns").cast(pl.Int64),
                c("o").cast(pl.Float64),
                c("h").cast(pl.Float64),
                c("l").cast(pl.Float64),
                c("c").cast(pl.Float64),
                c("v").cast(pl.Float64).clip(lower_bound=0.0),
            )
            .with_columns((c("t_ns") + pl.lit(BAR_DURATION_NS, dtype=pl.Int64)).alias("t_close_ns"))
            .filter(c("t_close_ns") <= DISCOVERY_WALL_UTC_NS)
            .collect()
        )
        if raw_bars.height == 0:
            continue
        if isinstance(mx_ts := raw_bars.select(c("t_close_ns").max()).item(), int):
            assert_pre_wall_ns(mx_ts)

        et_close = (
            pl.from_epoch("t_close_ns", time_unit="ns")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
        )
        bars = raw_bars.with_columns(
            (et_close.dt.hour().cast(pl.Int32) * 60 + et_close.dt.minute().cast(pl.Int32)).alias(
                "close_m"
            )
        ).filter((c("close_m") >= 241) & (c("close_m") <= 960))
        if bars.height == 0:
            continue

        # Join strictly causal T-1 baselines
        if df10_all.height > 0:
            d10 = df10_all.filter(c("trade_date") == d_iso).drop("trade_date")
            bars = bars.join(d10, on="symbol", how="left")
        else:
            bars = bars.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("prev_raw_close"),
                pl.lit(None, dtype=pl.Float64).alias("mean_20d_vol"),
                pl.lit(None, dtype=pl.Float64).alias("mean_20d_dvol"),
                pl.lit(None, dtype=pl.Float64).alias("high_20d_prior"),
                pl.lit(None, dtype=pl.Float64).alias("coil_5d_20d_range_ratio"),
            )

        t1_sv = t1_short_map.get(d_iso)
        if t1_sv is not None and t1_sv.height > 0:
            bars = bars.join(t1_sv, on="symbol", how="left")
        else:
            bars = bars.with_columns(pl.lit(0.0, dtype=pl.Float64).alias("t1_trf_proxy"))

        bars = bars.sort(["symbol", "t_ns"]).with_columns(
            c("prev_raw_close").fill_null(c("o").first().over("symbol")).alias("ref_close"),
            c("t1_trf_proxy").fill_null(0.0).alias("t1_trf_proxy"),
            pl.when(c("close_m") <= 570)
            .then(pl.lit("PRE_MARKET"))
            .otherwise(pl.lit("RTH"))
            .alias("sess"),
        )

        # Restrict universe to liquid equities (using strictly causal ref_close >= min_price)
        bars = bars.filter(c("ref_close") >= float(min_price))
        if bars.height == 0:
            continue

        # Ground-truth daily extreme movers (EVALUATION LABELS ONLY — never fed into features)
        day_summary = (
            bars.group_by("symbol")
            .agg(
                c("ref_close").first().alias("prior_close"),
                c("mean_20d_vol").first().alias("mean_20d_vol"),
                c("o").first().alias("session_open_px"),
                c("h").max().alias("session_high_px"),
                c("l").min().alias("session_low_px"),
                c("c").last().alias("session_close_px"),
                c("v").sum().alias("tot_vol"),
                (c("c") * c("v")).sum().alias("tot_dvol"),
                c("close_m").min().alias("first_bar_m"),
            )
            .with_columns(
                ((c("session_high_px") / c("prior_close") - 1.0) * 100.0).alias("up_max_pct"),
                ((c("session_low_px") / c("prior_close") - 1.0) * 100.0).alias("dn_max_pct"),
                ((c("session_close_px") / c("prior_close") - 1.0) * 100.0).alias("close_move_pct"),
                pl.when(c("mean_20d_vol").is_not_null() & (c("mean_20d_vol") > 0.0))
                .then(c("tot_vol") / c("mean_20d_vol"))
                .otherwise(2.0)
                .alias("daily_rvol"),
            )
            .with_columns(
                pl.max_horizontal(c("up_max_pct").abs(), c("dn_max_pct").abs()).alias(
                    "max_abs_move_pct"
                ),
                pl.when(c("up_max_pct").abs() >= c("dn_max_pct").abs())
                .then(pl.lit("LONG"))
                .otherwise(pl.lit("SHORT"))
                .alias("target_side"),
            )
            .filter(
                (c("tot_dvol") >= float(min_daily_dollar_vol))
                & (c("max_abs_move_pct") >= float(min_daily_move_pct))
                & (c("daily_rvol") >= float(min_daily_rvol))
            )
            .with_columns(
                (c("max_abs_move_pct") * (c("tot_dvol") / 1e7).log1p()).alias("target_priority")
            )
            .sort("target_priority", descending=True)
            .head(int(max_targets_per_day))
        )
        day_targets: dict[str, dict[str, Any]] = {}
        for row in day_summary.iter_rows(named=True):
            day_targets[str(row["symbol"])] = {
                "symbol": str(row["symbol"]),
                "date": d_iso,
                "side": str(row["target_side"]),
                "prior_close": round(float(row["prior_close"]), 4),
                "session_open_px": round(float(row["session_open_px"]), 4),
                "session_high_px": round(float(row["session_high_px"]), 4),
                "session_low_px": round(float(row["session_low_px"]), 4),
                "session_close_px": round(float(row["session_close_px"]), 4),
                "daily_move_pct": round(float(row["close_move_pct"]), 2),
                "max_abs_move_pct": round(float(row["max_abs_move_pct"]), 2),
                "daily_rvol": round(float(row["daily_rvol"]), 2),
                "tot_dvol": round(float(row["tot_dvol"]), 2),
                "first_bar_m": int(row["first_bar_m"]),
            }
        if not day_targets:
            continue
        targets_by_day[d_iso] = day_targets

        # Precompute post-checkpoint high/low STRICTLY for evaluation MFE on target symbols
        tgt_syms = list(day_targets.keys())
        tgt_bars = bars.filter(c("symbol").is_in(tgt_syms)).select(["symbol", "close_m", "h", "l"])
        for cp in cps:
            post_df = (
                tgt_bars.filter(c("close_m") > cp)
                .group_by("symbol")
                .agg(c("h").max().alias("post_h"), c("l").min().alias("post_l"))
            )
            for prow in post_df.iter_rows(named=True):
                post_ext_by_day_cp[(d_iso, cp, str(prow["symbol"]))] = (
                    float(prow["post_h"]),
                    float(prow["post_l"]),
                )

        # Strictly causal running features over bars (ONLY backward-looking over symbol/sess)
        tp = (c("h") + c("l") + c("c")) / 3.0
        bars_feat = (
            bars.with_columns(
                c("v").cum_sum().over("symbol").alias("cum_vol"),
                (c("c") * c("v")).cum_sum().over("symbol").alias("cum_dvol"),
                (tp * c("v")).cum_sum().over("symbol").alias("cum_pv"),
                c("h").cum_max().over("symbol").alias("run_high"),
                c("l").cum_min().over("symbol").alias("run_low"),
                c("c").ewm_mean(span=5, adjust=False).over("symbol").alias("ema5"),
                c("c").ewm_mean(span=13, adjust=False).over("symbol").alias("ema13"),
                (c("h") - c("l")).clip(lower_bound=0.0).alias("bar_range"),
                (c("h") - c("l"))
                .clip(lower_bound=0.0)
                .rolling_mean(14, min_samples=1)
                .over("symbol")
                .alias("atr14"),
                pl.int_range(0, pl.len()).over(["symbol", "sess"]).alias("sess_idx"),
                c("v").cum_sum().over(["symbol", "sess"]).alias("sess_cum_vol"),
                c("v")
                .rolling_sum(5, min_samples=1)
                .over(["symbol", "sess"])
                .alias("sess_rec5_sum"),
                c("v").rolling_mean(3, min_samples=1).over(["symbol", "sess"]).alias("v_rec3"),
                c("v")
                .shift(3)
                .rolling_mean(12, min_samples=1)
                .over(["symbol", "sess"])
                .alias("v_prior12"),
                c("v").shift(1).over(["symbol", "sess"]).alias("v_prev1"),
                c("c").shift(1).over("symbol").alias("c_prev1"),
                c("l").shift(1).over("symbol").alias("l_prev1"),
                c("h").shift(1).over("symbol").alias("h_prev1"),
            )
            .with_columns(
                pl.when(c("cum_vol") > 0.0)
                .then(c("cum_pv") / c("cum_vol"))
                .otherwise(c("c"))
                .alias("vwap"),
                ((c("c") / c("ref_close") - 1.0) * 100.0).alias("move_pct"),
                # Causal same-session RVOL matching scanner._compute_same_session_rvol
                pl.when(c("sess_idx") >= 5)
                .then(
                    pl.when((c("sess_cum_vol") - c("sess_rec5_sum")) > 0.0)
                    .then(
                        (c("sess_rec5_sum") / 5.0)
                        / ((c("sess_cum_vol") - c("sess_rec5_sum")) / (c("sess_idx") - 4.0))
                    )
                    .otherwise(0.0)
                )
                .when(c("sess_idx") >= 1)
                .then(
                    pl.when((c("sess_cum_vol") - c("v")) > 0.0)
                    .then(c("v") / ((c("sess_cum_vol") - c("v")) / c("sess_idx").cast(pl.Float64)))
                    .otherwise(0.0)
                )
                .otherwise(pl.when(c("v") > 0.0).then(1.5).otherwise(0.0))
                .clip(0.0, 25.0)
                .alias("rvol"),
                # Causal same-session volume buildup ratio matching scanner._compute_volume_buildup
                pl.when(c("v_prior12").is_not_null() & (c("v_prior12") > 0.0))
                .then(c("v_rec3") / c("v_prior12"))
                .when((c("sess_idx") >= 1) & ((c("sess_cum_vol") - c("v")) > 0.0))
                .then(c("v") / ((c("sess_cum_vol") - c("v")) / c("sess_idx").cast(pl.Float64)))
                .otherwise(0.0)
                .clip(0.0, 25.0)
                .alias("vol_buildup_ratio"),
            )
            .with_columns(
                (
                    c("rvol")
                    / pl.max_horizontal(
                        pl.lit(0.25), c("bar_range") / pl.max_horizontal(c("atr14"), pl.lit(1e-6))
                    )
                )
                .clip(0.0, 15.0)
                .alias("hidden_volume_ratio"),
                pl.when(c("move_pct") >= 0.0)
                .then(c("c") >= c("vwap"))
                .otherwise(c("c") <= c("vwap"))
                .alias("vwap_ok"),
                pl.when(c("move_pct") >= 0.0)
                .then((c("c") >= c("ema5")) & (c("ema5") >= c("ema13")))
                .otherwise((c("c") <= c("ema5")) & (c("ema5") <= c("ema13")))
                .alias("ema_ok"),
                pl.when(c("move_pct") >= 0.0)
                .then((c("c") / pl.max_horizontal(c("run_high"), pl.lit(1e-6))).clip(0.0, 1.0))
                .otherwise((c("run_low") / pl.max_horizontal(c("c"), pl.lit(1e-6))).clip(0.0, 1.0))
                .alias("brk_prox"),
                (
                    c("high_20d_prior").is_not_null()
                    & (c("high_20d_prior") > 0.0)
                    & (c("move_pct") >= 0.0)
                    & (c("c") >= c("high_20d_prior") * 0.995)
                ).alias("breakout_20d"),
                (
                    (c("rvol") >= 2.2)
                    | (c("vol_buildup_ratio") >= 2.0)
                    | (
                        c("v_prev1").is_not_null()
                        & (c("v_prev1") > 0.0)
                        & (c("v") >= c("v_prev1") * 2.0)
                        & ((c("v") * c("c")) >= 5_000_000.0)
                    )
                ).alias("sudden_volume_surge"),
                # Causal peak exhaustion check
                (
                    (c("rvol") >= 2.0)
                    & c("h_prev1").is_not_null()
                    & c("l_prev1").is_not_null()
                    & (
                        ((c("h_prev1") - c("vwap")) / pl.max_horizontal(c("vwap"), pl.lit(1e-6)))
                        >= 2.0 * (c("atr14") / pl.max_horizontal(c("c"), pl.lit(1e-6)))
                    )
                    & (c("c") < c("o"))
                    & (c("c") < c("l_prev1"))
                ).alias("peak_confirmed"),
            )
            .with_columns(
                (
                    pl.when(c("vwap_ok")).then(25.0).otherwise(0.0)
                    + pl.when(c("ema_ok")).then(20.0).otherwise(6.0)
                    + (((c("brk_prox") - 0.94) / 0.06).clip(0.0, 1.0) * 16.0)
                    + pl.when(c("breakout_20d")).then(4.0).otherwise(0.0)
                    + (c("rvol").log1p() * 7.5).clip(0.0, 12.0)
                    + ((c("vol_buildup_ratio") - 1.0).clip(0.0, 2.0) * 4.0)
                    + (c("t1_trf_proxy") * 25.0).clip(0.0, 15.0)
                )
                .clip(0.0, 100.0)
                .alias("technical_alignment_score"),
                (
                    (c("sess") == "PRE_MARKET")
                    & (c("move_pct").abs() >= 2.5)
                    & c("vwap_ok")
                    & (
                        (c("rvol") >= 1.5)
                        | (c("vol_buildup_ratio") >= 1.5)
                        | (c("cum_dvol") >= 25_000_000.0)
                    )
                ).alias("premarket_ignition"),
                (
                    (c("move_pct").abs() >= 3.5)
                    & (c("rvol") >= 1.5)
                    & (c("cum_dvol") >= 5_000_000.0)
                ).alias("is_ext"),
            )
            .select(
                [
                    "symbol",
                    "close_m",
                    "sess",
                    "c",
                    "move_pct",
                    "rvol",
                    "vol_buildup_ratio",
                    "t1_trf_proxy",
                    "hidden_volume_ratio",
                    "technical_alignment_score",
                    "premarket_ignition",
                    "sudden_volume_surge",
                    "breakout_20d",
                    "coil_5d_20d_range_ratio",
                    "peak_confirmed",
                    "is_ext",
                    "cum_dvol",
                ]
            )
        )

        cp_map: dict[int, list[dict[str, Any]]] = {}
        for cp in cps:
            snap = (
                bars_feat.filter(c("close_m") <= cp)
                .group_by("symbol", maintain_order=False)
                .last()
                .filter(
                    (c("cum_dvol") >= 5_000_000.0)
                    & (
                        (c("move_pct").abs() >= 1.5)
                        | (c("rvol") >= 2.0)
                        | (c("vol_buildup_ratio") >= 2.0)
                        | c("premarket_ignition")
                    )
                )
            )
            if snap.height == 0:
                continue
            rows_cp: list[dict[str, Any]] = []
            for r in snap.iter_rows(named=True):
                cat = classify_potential_category(
                    symbol=str(r["symbol"]),
                    session=str(r["sess"]),
                    move_pct=float(r["move_pct"]),
                    rvol=float(r["rvol"]),
                    vol_buildup_ratio=float(r["vol_buildup_ratio"]),
                    hidden_volume_ratio=float(r["hidden_volume_ratio"]),
                    darkpool_trf_ratio=float(r["t1_trf_proxy"]),
                    trf_block_ratio=float(r["t1_trf_proxy"]) * 0.7,
                    macro_regime=0,
                    peak_confirmed=bool(r["peak_confirmed"]),
                    premarket_ignition=bool(r["premarket_ignition"]),
                    sudden_volume_surge=bool(r["sudden_volume_surge"]),
                    technical_alignment_score=float(r["technical_alignment_score"]),
                    breakout_20d=bool(r["breakout_20d"]),
                    coil_ratio=(
                        float(r["coil_5d_20d_range_ratio"])
                        if r["coil_5d_20d_range_ratio"] is not None
                        else None
                    ),
                )
                rows_cp.append(
                    {
                        "symbol": str(r["symbol"]),
                        "sess": str(r["sess"]),
                        "px": round(float(r["c"]), 4),
                        "move_pct": round(float(r["move_pct"]), 3),
                        "rvol": round(float(r["rvol"]), 3),
                        "vol_buildup_ratio": round(float(r["vol_buildup_ratio"]), 3),
                        "t1_trf_proxy": round(float(r["t1_trf_proxy"]), 4),
                        "hidden_volume_ratio": round(float(r["hidden_volume_ratio"]), 3),
                        "technical_alignment_score": round(
                            float(r["technical_alignment_score"]), 2
                        ),
                        "premarket_ignition": bool(r["premarket_ignition"]),
                        "is_ext": bool(r["is_ext"]),
                        "category": cat,
                    }
                )
            cp_map[cp] = rows_cp
        snapshots_by_day[d_iso] = cp_map

    valid_days = sorted(targets_by_day.keys())
    return {
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "days": valid_days,
        "checkpoints_et": list(cps),
        "targets_by_day": targets_by_day,
        "snapshots_by_day": snapshots_by_day,
        "post_ext_by_day_cp": post_ext_by_day_cp,
    }


def evaluate_early_mover_weights(
    panel: Mapping[str, Any],
    weights: Mapping[str, float] | None = None,
    *,
    days_subset: Sequence[str] | None = None,
    top_k: int = 15,
) -> dict[str, Any]:
    """Evaluate causal early-mover ranking weights across pre-wall sessions."""
    w = normalize_early_mover_scoring_weights(weights)
    all_days: list[str] = list(panel.get("days") or ())
    use_days = [d for d in all_days if days_subset is None or d in set(days_subset)]
    cps: list[int] = [int(x) for x in (panel.get("checkpoints_et") or ())]
    targets_by_day: Mapping[str, Mapping[str, dict[str, Any]]] = panel.get("targets_by_day") or {}
    snaps_by_day: Mapping[str, Mapping[int, list[dict[str, Any]]]] = (
        panel.get("snapshots_by_day") or {}
    )
    post_ext: Mapping[tuple[str, int, str], tuple[float, float]] = (
        panel.get("post_ext_by_day_cp") or {}
    )

    total_targets = 0
    detected_targets = 0
    early_1030_detected = 0
    ttd_mins: list[float] = []
    mfe_list: list[float] = []
    timeline: list[dict[str, Any]] = []
    cat_counts: dict[str, int] = {}

    for d_iso in use_days:
        day_tgts = targets_by_day.get(d_iso) or {}
        if not day_tgts:
            continue
        total_targets += len(day_tgts)
        first_hit: dict[str, tuple[int, int, dict[str, Any]]] = {}
        day_snaps = snaps_by_day.get(d_iso) or {}
        for cp in cps:
            rows = day_snaps.get(cp)
            if not rows:
                continue
            scored_rows = [
                (
                    score_early_mover_candidate(
                        move_pct=float(r["move_pct"]),
                        rvol=float(r["rvol"]),
                        vol_buildup_ratio=float(r["vol_buildup_ratio"]),
                        darkpool_trf_ratio=float(r["t1_trf_proxy"]),
                        trf_block_ratio=float(r["t1_trf_proxy"]) * 0.7,
                        hidden_volume_ratio=float(r["hidden_volume_ratio"]),
                        technical_alignment_score=float(r["technical_alignment_score"]),
                        premarket_ignition=bool(r["premarket_ignition"]),
                        is_extreme_eligible=bool(r["is_ext"]),
                        is_pinned=False,
                        weights=w,
                    ),
                    str(r["symbol"]),
                    r,
                )
                for r in rows
            ]
            scored_rows.sort(key=lambda item: (-item[0], item[1]))
            for rank_idx, (_, sym, rec) in enumerate(scored_rows[: max(1, int(top_k))], 1):
                if sym in day_tgts and sym not in first_hit:
                    first_hit[sym] = (cp, rank_idx, rec)

        for sym, tgt in day_tgts.items():
            hit = first_hit.get(sym)
            if hit is None:
                continue
            detected_targets += 1
            cp, rank_idx, rec = hit
            if cp <= 630:
                early_1030_detected += 1
            # Measure latency from active session open (04:00 ET for PRE_MARKET, 09:30 ET for RTH)
            sess_open_m = 240 if cp <= 570 else 570
            elapsed_mins = float(max(0, cp - sess_open_m))
            ttd_mins.append(elapsed_mins)

            detect_px = float(rec["px"])
            post_h, post_l = post_ext.get(
                (d_iso, cp, sym),
                (float(tgt["session_high_px"]), float(tgt["session_low_px"])),
            )
            if tgt["side"] == "LONG":
                post_mfe = max(0.0, (max(detect_px, post_h) / max(1e-6, detect_px) - 1.0) * 100.0)
            else:
                post_mfe = max(0.0, (1.0 - min(detect_px, post_l) / max(1e-6, detect_px)) * 100.0)
            mfe_list.append(post_mfe)
            cat = str(rec["category"])
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
            timeline.append(
                {
                    "symbol": sym,
                    "date": d_iso,
                    "side": str(tgt["side"]),
                    "first_detect_et": _fmt_minute_et(cp),
                    "first_detect_session": str(rec["sess"]),
                    "first_detect_px": round(detect_px, 4),
                    "prior_close": float(tgt["prior_close"]),
                    "session_high_px": float(tgt["session_high_px"]),
                    "session_low_px": float(tgt["session_low_px"]),
                    "session_close_px": float(tgt["session_close_px"]),
                    "daily_move_pct": float(tgt["daily_move_pct"]),
                    "post_detect_mfe_pct": round(post_mfe, 2),
                    "category": cat,
                    "rvol": round(float(rec["rvol"]), 2),
                    "vol_buildup_ratio": round(float(rec["vol_buildup_ratio"]), 2),
                    "technical_alignment_score": round(float(rec["technical_alignment_score"]), 2),
                    "detect_rank": int(rank_idx),
                    "time_to_first_detect_mins": round(elapsed_mins, 1),
                }
            )

    recall = (detected_targets / total_targets) if total_targets > 0 else 0.0
    early_recall = (early_1030_detected / total_targets) if total_targets > 0 else 0.0
    med_ttd = float(np.median(ttd_mins)) if ttd_mins else 390.0
    mean_mfe = float(np.mean(mfe_list)) if mfe_list else 0.0
    # Composite objective rewarding high Top-15 recall, early <10:30 ET capture,
    # low detection latency, and strong remaining post-detection MFE.
    latency_score = 1.0 / (1.0 + max(0.0, med_ttd) / 60.0)
    mfe_score = min(1.0, mean_mfe / 12.0)
    objective = round(
        0.40 * recall + 0.25 * early_recall + 0.20 * latency_score + 0.15 * mfe_score,
        6,
    )
    timeline.sort(
        key=lambda r: (str(r["date"]), str(r["first_detect_et"]), -float(r["post_detect_mfe_pct"]))
    )
    return {
        "days_count": len(use_days),
        "actual_extreme_movers_total": int(total_targets),
        "detected_in_top15_count": int(detected_targets),
        "recall_at_top15": round(recall, 4),
        "early_recall_before_1030_et": round(early_recall, 4),
        "median_time_to_first_detect_mins": round(med_ttd, 2),
        "mean_post_detect_mfe_capture_pct": round(mean_mfe, 2),
        "composite_objective": objective,
        "category_breakdown": dict(sorted(cat_counts.items())),
        "timeline": timeline,
    }


_LOOKAHEAD_SQL_FUNCTIONS: frozenset[str] = frozenset(
    {
        "NOW",
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_DATETIME",
        "CURRENT_LOCALTIMESTAMP",
        "LOCALTIMESTAMP",
        "LOCALTIME",
        "TODAY",
        "GETDATE",
        "SYSDATE",
        "CLOCK_TIMESTAMP",
        "TRANSACTION_TIMESTAMP",
        "STATEMENT_TIMESTAMP",
        "GET_CURRENT_TIMESTAMP",
        "TIMEOFDAY",
    }
)

_SEC_CATALYST_FORMS: frozenset[str] = frozenset(
    {"8-K", "8-K/A", "10-Q", "10-Q/A", "10-K", "10-K/A", "4", "4/A"}
)
_SEC_MATERIAL_8K_ITEMS: frozenset[str] = frozenset({"1.01", "2.02", "5.02", "7.01", "8.01"})


def _parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp or datetime strictly into timezone-aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(UTC) if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def validate_pit_duckdb_sql(
    sql: str,
    *,
    read_only: bool = True,
    require_pit_safe: bool = True,
) -> dict[str, Any]:
    """Validate DuckDB SQL AST via sqlglot for read-only safety and causal PIT compliance."""
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as pkg_version

    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError

    try:
        sg_ver = pkg_version("sqlglot")
    except PackageNotFoundError:
        sg_ver = str(getattr(sqlglot, "__version__", "unknown"))

    raw = str(sql).strip()
    if not raw:
        return {
            "ok": False,
            "read_only": bool(read_only),
            "require_pit_safe": bool(require_pit_safe),
            "normalized_sql": "",
            "statement_types": [],
            "tables": [],
            "violations": ["empty_sql"],
            "pit_safe": False,
            "sqlglot_version": sg_ver,
        }

    try:
        parsed_raw = sqlglot.parse(raw, read="duckdb")
    except ParseError as exc:
        return {
            "ok": False,
            "read_only": bool(read_only),
            "require_pit_safe": bool(require_pit_safe),
            "normalized_sql": "",
            "statement_types": [],
            "tables": [],
            "violations": [f"parse_error:{exc}"],
            "pit_safe": False,
            "sqlglot_version": sg_ver,
        }

    stmts = [s for s in parsed_raw if s is not None]
    if not stmts:
        return {
            "ok": False,
            "read_only": bool(read_only),
            "require_pit_safe": bool(require_pit_safe),
            "normalized_sql": "",
            "statement_types": [],
            "tables": [],
            "violations": ["empty_sql"],
            "pit_safe": False,
            "sqlglot_version": sg_ver,
        }

    dml_ddl_names = (
        "Drop",
        "Delete",
        "Update",
        "Insert",
        "Alter",
        "Create",
        "Merge",
        "TruncateTable",
        "Copy",
        "Attach",
        "Detach",
        "Pragma",
        "Set",
        "LoadData",
        "Command",
    )
    dml_ddl_types = tuple(
        cls for name in dml_ddl_names if isinstance(cls := getattr(exp, name, None), type)
    )
    readonly_root_names = ("Query", "Describe", "Show", "Explain")
    readonly_root_types = tuple(
        cls for name in readonly_root_names if isinstance(cls := getattr(exp, name, None), type)
    )
    wall_clock_names = (
        "CurrentTimestamp",
        "CurrentDate",
        "CurrentTime",
        "CurrentDatetime",
        "Localtimestamp",
        "Localtime",
        "GetDate",
        "Sysdate",
    )
    wall_clock_types = tuple(
        cls for name in wall_clock_names if isinstance(cls := getattr(exp, name, None), type)
    )

    violations: list[str] = []
    lookahead_hits: list[str] = []
    stmt_types: list[str] = []
    tables_set: set[str] = set()

    if read_only and len(stmts) > 1:
        violations.append("multi_statement_disallowed_in_read_only")

    for stmt in stmts:
        stmt_types.append(type(stmt).__name__)
        if read_only and not isinstance(stmt, readonly_root_types):
            violations.append(f"disallowed_statement:{type(stmt).__name__}")

        cte_names = {
            cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE) if cte.alias_or_name
        }
        for tbl in stmt.find_all(exp.Table):
            tname = str(tbl.name or "").strip()
            if tname and tname.lower() not in cte_names:
                tables_set.add(tname)

        for node in stmt.walk():
            if read_only and isinstance(node, dml_ddl_types):
                tag = f"disallowed_mutation:{type(node).__name__}"
                if tag not in violations:
                    violations.append(tag)
            if isinstance(node, wall_clock_types):
                fn_tag = type(node).__name__.upper()
                if fn_tag not in lookahead_hits:
                    lookahead_hits.append(fn_tag)
            elif isinstance(node, exp.Anonymous):
                aname = str(node.name or "").strip().upper()
                if aname in _LOOKAHEAD_SQL_FUNCTIONS and aname not in lookahead_hits:
                    lookahead_hits.append(aname)
            elif isinstance(node, exp.Func):
                fname = str(node.sql_name() or "").strip().upper()
                if fname in _LOOKAHEAD_SQL_FUNCTIONS and fname not in lookahead_hits:
                    lookahead_hits.append(fname)

    if require_pit_safe:
        for fn in lookahead_hits:
            violations.append(f"lookahead_wall_clock_function:{fn}")

    normalized_sql = "; ".join(s.sql(dialect="duckdb") for s in stmts)
    return {
        "ok": len(violations) == 0,
        "read_only": bool(read_only),
        "require_pit_safe": bool(require_pit_safe),
        "normalized_sql": normalized_sql,
        "statement_types": stmt_types,
        "tables": sorted(tables_set),
        "violations": violations,
        "pit_safe": len(lookahead_hits) == 0,
        "sqlglot_version": sg_ver,
    }


def _score_sec_filings_pit(
    records: Sequence[Mapping[str, Any]],
    *,
    as_of_dt: datetime,
    lookback_days: int,
) -> dict[str, Any]:
    """Filter SEC filings strictly by accepted_at_utc <= as_of_utc and score catalysts."""
    window_start = as_of_dt - timedelta(days=max(1, int(lookback_days)))
    accepted_all: list[dict[str, Any]] = []
    excluded_future = 0
    excluded_unparseable = 0

    for rec in records:
        form = str(rec.get("form") or "").strip().upper()
        if form not in _SEC_CATALYST_FORMS:
            continue
        raw_acc = (
            rec.get("accepted_at_utc") or rec.get("accepted") or rec.get("acceptance_datetime")
        )
        acc_dt = _parse_utc_timestamp(raw_acc)
        if acc_dt is None:
            excluded_unparseable += 1
            continue
        if acc_dt > as_of_dt:
            excluded_future += 1
            continue
        items_raw = str(rec.get("items") or "").strip()
        items = [tok.strip() for tok in items_raw.split(",") if tok.strip()]
        accepted_all.append(
            {
                "form": form,
                "accepted_at_utc": acc_dt.isoformat(),
                "filed": str(rec.get("filed") or acc_dt.date().isoformat()),
                "items": items,
                "within_lookback": acc_dt >= window_start,
            }
        )

    accepted_all.sort(key=lambda r: str(r["accepted_at_utc"]), reverse=True)
    recent = [r for r in accepted_all if bool(r["within_lookback"])]
    recent_8k = [r for r in recent if str(r["form"]).startswith("8-K")]
    recent_10 = [r for r in recent if str(r["form"]).startswith(("10-Q", "10-K"))]
    recent_f4 = [r for r in recent if str(r["form"]) in ("4", "4/A")]
    material_8k_hits = sum(
        1 for r in recent_8k if any(it in _SEC_MATERIAL_8K_ITEMS for it in r["items"])
    )

    raw_score = (
        28.0 * len(recent_8k)
        + 10.0 * material_8k_hits
        + 18.0 * len(recent_10)
        + 12.0 * len(recent_f4)
    )
    catalyst_score = round(min(100.0, raw_score), 2)
    latest = accepted_all[0] if accepted_all else None
    return {
        "catalyst_score": catalyst_score,
        "recent_8k_count": len(recent_8k),
        "recent_10q_10k_count": len(recent_10),
        "insider_form4_count": len(recent_f4),
        "material_8k_count": int(material_8k_hits),
        "accepted_filings_count": len(accepted_all),
        "recent_filings_count": len(recent),
        "excluded_future_filings": int(excluded_future),
        "excluded_unparseable_filings": int(excluded_unparseable),
        "latest_form": str(latest["form"]) if latest is not None else None,
        "latest_accepted_utc": str(latest["accepted_at_utc"]) if latest is not None else None,
        "filings": recent[:10],
        "pit_safe": True,
    }


def load_causal_sec_catalysts(
    symbols: list[str],
    *,
    as_of_utc: str | None = None,
    offline_filings: dict[str, list[dict[str, Any]]] | None = None,
    allow_network: bool = False,
    lookback_days: int = 30,
    parquet_dir: Path | None = None,
) -> dict[str, Any]:
    """Load strictly causal SEC 8-K/10-Q/10-K/Form-4 catalysts via EdgarTools & PIT parquet."""
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as pkg_version

    import edgar

    try:
        edgar_ver = pkg_version("edgartools")
    except PackageNotFoundError:
        edgar_ver = str(getattr(edgar, "__version__", "unknown"))

    as_of_dt = (
        _parse_utc_timestamp(as_of_utc)
        if as_of_utc is not None
        else datetime(
            DISCOVERY_WALL_DATE.year,
            DISCOVERY_WALL_DATE.month,
            DISCOVERY_WALL_DATE.day,
            23,
            59,
            59,
            tzinfo=UTC,
        )
    )
    if as_of_dt is None:
        raise ValueError(f"Invalid as_of_utc ISO timestamp: {as_of_utc!r}")

    norm_symbols = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    raw_by_sym: dict[str, list[dict[str, Any]]] = {sym: [] for sym in norm_symbols}
    source = "offline_empty_fail_closed"

    if offline_filings is not None:
        source = "offline_filings_dict"
        norm_in = {str(k).strip().upper(): list(v) for k, v in offline_filings.items()}
        for sym in norm_symbols:
            raw_by_sym[sym] = list(norm_in.get(sym, []))
    else:
        pub_dir = (
            parquet_dir if parquet_dir is not None else (DEFAULT_MARKET_DATA_ROOT / "public-tables")
        )
        filings_pq = pub_dir / "sec_filings.parquet"
        symbols_pq = pub_dir / "sec_symbols.parquet"
        if norm_symbols and filings_pq.is_file() and symbols_pq.is_file():
            source = "prewall_sec_filings_parquet"
            sym_lf = (
                pl.scan_parquet(str(symbols_pq))
                .select(
                    pl.col("cik").cast(pl.Int64),
                    pl.col("symbol").cast(pl.String).str.to_uppercase().alias("symbol"),
                )
                .filter(pl.col("symbol").is_in(norm_symbols))
                .unique(subset=["cik", "symbol"])
            )
            fil_lf = (
                pl.scan_parquet(str(filings_pq))
                .select(
                    pl.col("cik").cast(pl.Int64),
                    pl.col("filed").cast(pl.String),
                    pl.col("accepted").cast(pl.String),
                    pl.col("form").cast(pl.String).str.to_uppercase().alias("form"),
                    pl.col("items").cast(pl.String).fill_null(""),
                )
                .filter(pl.col("form").is_in(sorted(_SEC_CATALYST_FORMS)))
            )
            joined = fil_lf.join(sym_lf, on="cik", how="inner").collect()
            for row in joined.iter_rows(named=True):
                sym = str(row["symbol"])
                raw_by_sym.setdefault(sym, []).append(
                    {
                        "form": str(row["form"]),
                        "filed": str(row["filed"]),
                        "accepted_at_utc": str(row["accepted"]),
                        "items": str(row["items"]),
                    }
                )
        elif allow_network and norm_symbols:
            source = "edgartools_live_sec_edgar"
            identity = os.environ.get(
                "EDGAR_IDENTITY", "NoesisTier0Research research@noesis.local"
            ).strip()
            if hasattr(edgar, "set_identity"):
                edgar.set_identity(identity)
            for sym in norm_symbols:
                try:
                    company = edgar.Company(sym)
                    filings = company.get_filings(form=["8-K", "10-Q", "10-K", "4"]).head(25)
                    for filing in filings:
                        acc = getattr(filing, "acceptance_datetime", None) or getattr(
                            filing, "accepted", None
                        )
                        raw_by_sym[sym].append(
                            {
                                "form": str(getattr(filing, "form", "")),
                                "filed": str(getattr(filing, "filing_date", "")),
                                "accepted_at_utc": acc,
                                "items": ",".join(getattr(filing, "items", []) or []),
                            }
                        )
                except Exception:  # noqa: BLE001
                    source = "edgartools_network_fail_closed"
                    raw_by_sym[sym] = []

    by_symbol: dict[str, dict[str, Any]] = {}
    total_accepted = 0
    total_excluded_future = 0
    for sym in norm_symbols:
        scored = _score_sec_filings_pit(
            raw_by_sym.get(sym, []),
            as_of_dt=as_of_dt,
            lookback_days=lookback_days,
        )
        by_symbol[sym] = scored
        total_accepted += int(scored["accepted_filings_count"])
        total_excluded_future += int(scored["excluded_future_filings"])

    return {
        "as_of_utc": as_of_dt.isoformat(),
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "prewall_compliant": as_of_dt.date() <= DISCOVERY_WALL_DATE,
        "lookback_days": int(lookback_days),
        "pit_safe": True,
        "allow_network": bool(allow_network),
        "edgartools_version": edgar_ver,
        "source": source,
        "symbols_count": len(norm_symbols),
        "total_accepted_filings": int(total_accepted),
        "excluded_future_filings": int(total_excluded_future),
        "by_symbol": by_symbol,
    }


def detect_skchange_regime_anomalies(
    returns: Sequence[float] | np.ndarray,
    *,
    min_segment_length: int = 5,
    recent_window: int | None = None,
) -> dict[str, Any]:
    """Detect structural changepoints & anomaly segments via skchange PELT/MovingWindow/CAPA."""
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as pkg_version

    import skchange
    from skchange.detectors import CAPA, PELT, MovingWindow
    from skchange.interval_scorers import GaussianCost, GaussianSaving

    try:
        sk_ver = pkg_version("skchange")
    except PackageNotFoundError:
        sk_ver = str(getattr(skchange, "__version__", "unknown"))

    min_seg = max(2, int(min_segment_length))
    arr = np.asarray(returns, dtype=float).reshape(-1)
    n = int(arr.size)
    if n == 0:
        raise ValueError("returns series must be non-empty")
    if not bool(np.all(np.isfinite(arr))):
        raise ValueError("returns series contains non-finite values")

    rec_win = (
        max(1, int(recent_window))
        if recent_window is not None
        else max(min_seg * 2, min(15, max(1, n // 5)))
    )
    if n < max(2 * min_seg, 10) or float(np.std(arr)) <= 1e-12:
        return {
            "n_observations": n,
            "min_segment_length": min_seg,
            "changepoints": [],
            "moving_window_changepoints": [],
            "n_changepoints": 0,
            "anomaly_segments": [],
            "n_anomaly_segments": 0,
            "latest_regime_break": False,
            "recent_window": rec_win,
            "detectors": ["PELT(GaussianCost)", "MovingWindow", "CAPA(GaussianSaving)"],
            "skchange_version": sk_ver,
        }

    z = ((arr - float(np.mean(arr))) / float(np.std(arr))).reshape(-1, 1)

    pelt_raw = np.asarray(
        PELT(cost=GaussianCost(), min_segment_length=min_seg).fit_predict(z), dtype=int
    ).reshape(-1)
    changepoints = sorted({int(i) for i in pelt_raw.tolist() if 0 < int(i) < n})

    mw_cps: list[int] = []
    bw = max(min_seg, min(15, n // 4))
    if n >= 2 * bw + 2:
        mw_raw = np.asarray(
            MovingWindow(bandwidth=bw, min_bandwidth=min_seg).fit_predict(z), dtype=int
        ).reshape(-1)
        mw_cps = sorted({int(i) for i in mw_raw.tolist() if 0 < int(i) < n})

    capa = CAPA(segment_saving=GaussianSaving(), min_segment_length=min_seg)
    capa.fit(z)
    seg_raw = np.asarray(capa.predict_segment_anomalies(z), dtype=int)
    anomaly_segments: list[dict[str, Any]] = []
    if seg_raw.ndim == 2 and seg_raw.shape[1] == 2:
        for row in seg_raw.tolist():
            s_idx = max(0, min(n - 1, int(row[0])))
            e_idx = max(s_idx + 1, min(n, int(row[1])))
            sl = arr[s_idx:e_idx]
            anomaly_segments.append(
                {
                    "start": s_idx,
                    "end": e_idx,
                    "length": int(e_idx - s_idx),
                    "mean_return": round(float(np.mean(sl)), 6),
                    "vol": round(float(np.std(sl)), 6),
                }
            )

    latest_regime_break = bool(
        any(cp >= n - rec_win for cp in changepoints)
        or any(int(seg["end"]) >= n - rec_win for seg in anomaly_segments)
    )
    return {
        "n_observations": n,
        "min_segment_length": min_seg,
        "changepoints": changepoints,
        "moving_window_changepoints": mw_cps,
        "n_changepoints": len(changepoints),
        "anomaly_segments": anomaly_segments,
        "n_anomaly_segments": len(anomaly_segments),
        "latest_regime_break": latest_regime_break,
        "recent_window": rec_win,
        "detectors": ["PELT(GaussianCost)", "MovingWindow", "CAPA(GaussianSaving)"],
        "skchange_version": sk_ver,
    }


def compute_shapiq_interactions(
    model: Any,
    x_train: np.ndarray,
    x_eval: np.ndarray,
    feature_names: Sequence[str],
    *,
    max_order: int = 2,
    max_eval_rows: int = 32,
    index: str = "k-SII",
) -> dict[str, Any]:
    """Compute k-SII Shapley feature interactions (order <= max_order) via upstream shapiq."""
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as pkg_version

    import shapiq

    try:
        sq_ver = pkg_version("shapiq")
    except PackageNotFoundError:
        sq_ver = str(getattr(shapiq, "__version__", "unknown"))

    fnames = [str(f) for f in feature_names]
    x_tr = np.asarray(x_train, dtype=float)
    x_ev = np.asarray(x_eval, dtype=float)
    if x_tr.ndim != 2 or x_ev.ndim != 2:
        raise ValueError(f"Expected 2D x_train/x_eval arrays, got {x_tr.shape} and {x_ev.shape}")
    if x_tr.shape[0] < 2 or x_ev.shape[0] < 1:
        raise ValueError("x_train requires >= 2 rows and x_eval requires >= 1 row")
    n_features = int(x_tr.shape[1])
    if x_ev.shape[1] != n_features or len(fnames) != n_features:
        raise ValueError(
            f"Feature count mismatch: x_train={n_features}, x_eval={x_ev.shape[1]}, "
            f"feature_names={len(fnames)}"
        )
    if not (bool(np.all(np.isfinite(x_tr))) and bool(np.all(np.isfinite(x_ev)))):
        raise ValueError("x_train and x_eval must contain only finite values")

    order = max(1, min(int(max_order), n_features))
    x_ev_sub = x_ev[: max(1, int(max_eval_rows))]

    shapiq_mod: Any = shapiq
    index_arg: Any = index
    try:
        explainer: Any = shapiq_mod.TreeExplainer(
            model=model,
            index=index_arg,
            max_order=order,
        )
        backend = "TreeExplainer"
    except (TypeError, ValueError):
        explainer = shapiq_mod.TabularExplainer(
            model=model,
            data=x_tr,
            index=index_arg,
            max_order=order,
            random_state=42,
        )
        backend = "TabularExplainer"

    main_buckets: dict[int, list[float]] = {i: [] for i in range(n_features)}
    pair_buckets: dict[tuple[int, int], list[float]] = {}
    for row in x_ev_sub:
        iv = explainer.explain(row)
        for coalition, raw_val in iv.dict_values.items():
            val = float(raw_val)
            if not math.isfinite(val):
                continue
            if len(coalition) == 1:
                main_buckets[int(coalition[0])].append(val)
            elif len(coalition) == 2:
                i0, i1 = sorted((int(coalition[0]), int(coalition[1])))
                pair_buckets.setdefault((i0, i1), []).append(val)

    main_effects = {
        fnames[i]: round(float(np.mean(np.abs(vals))), 6) if vals else 0.0
        for i, vals in sorted(main_buckets.items())
    }
    pairwise_interactions: list[dict[str, Any]] = []
    for (i0, i1), vals in pair_buckets.items():
        if not vals:
            continue
        arr_v = np.asarray(vals, dtype=float)
        pairwise_interactions.append(
            {
                "features": [fnames[i0], fnames[i1]],
                "pair": f"{fnames[i0]} x {fnames[i1]}",
                "mean_abs_interaction": round(float(np.mean(np.abs(arr_v))), 6),
                "mean_signed_interaction": round(float(np.mean(arr_v)), 6),
            }
        )
    pairwise_interactions.sort(key=lambda r: (-float(r["mean_abs_interaction"]), str(r["pair"])))

    return {
        "index": index,
        "max_order": order,
        "explainer_backend": backend,
        "n_features": n_features,
        "n_eval_rows": int(x_ev_sub.shape[0]),
        "main_effects": main_effects,
        "pairwise_interactions": pairwise_interactions,
        "top_interaction": pairwise_interactions[0] if pairwise_interactions else None,
        "shapiq_version": sq_ver,
    }


__all__ = [
    "BAR_DURATION_NS",
    "CATEGORY_TO_ENGINE_ARCHETYPE",
    "CATEGORY_TO_SCANNER_LABEL",
    "DEFAULT_DATA_ROOT",
    "DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS",
    "DEFAULT_EARLY_MOVER_WEIGHTS",
    "DEFAULT_MARKET_DATA_ROOT",
    "DISCOVERY_WALL_DATE",
    "DISCOVERY_WALL_ISO",
    "DISCOVERY_WALL_UTC_NS",
    "EARLY_DETECTION_CHECKPOINTS_ET",
    "EARLY_MOVER_CHECKPOINTS_ET",
    "EARLY_MOVER_FEATURE_COLS",
    "EARLY_MOVER_WEIGHT_KEYS",
    "EXTREME_MOVER_BASKET_SYMBOLS",
    "EXTREME_MOVER_CATEGORIES",
    "FEATURE_COLS",
    "INVERSE_HEDGE_SYMBOLS",
    "MAX_PREWALL_DATE",
    "MarketRegimeActor",
    "MarketRegimeActorConfig",
    "POTENTIAL_CATEGORIES",
    "PreWallViolationError",
    "REGIME_BENCHMARK_SYMBOLS",
    "WARMUP_BARS",
    "assert_pre_wall_date",
    "assert_pre_wall_ns",
    "build_prewall_early_mover_session_panel",
    "classify_extreme_mover_category",
    "classify_potential_category",
    "compute_causal_market_regime",
    "compute_causal_technical_alignment",
    "compute_shapiq_interactions",
    "detect_skchange_regime_anomalies",
    "df_to_equity_and_bars",
    "evaluate_early_mover_detection",
    "evaluate_early_mover_weights",
    "extract_causal_intraday_mover_features",
    "fit_causal_gbdt_ranker_with_shap",
    "generate_upstream_reversal_signals",
    "get_xnys_calendar",
    "join_causal_fred_asof",
    "list_prewall_bar_days",
    "load_causal_fred_vintages",
    "load_causal_sec_catalysts",
    "load_minute_bars_day",
    "normalize_early_mover_scoring_weights",
    "normalize_early_mover_simplex_weights",
    "parse_iso_date",
    "previous_xnys_session",
    "project_calibrated_weights_to_scanner",
    "score_early_mover_candidate",
    "score_early_mover_features",
    "step_rth_5m_bar",
    "validate_causal_bars",
    "validate_pit_duckdb_sql",
]

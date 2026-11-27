#!/usr/bin/env python3
"""Native NautilusTrader v2 ParquetDataCatalog & empirical pre-wall tearsheet suite.

Populates `/mnt/z/sota-data/nautilus-catalog` from strictly causal pre-wall
(`<= 2026-10-15`) 1-minute Alpaca SIP Parquet bars, runs a multi-symbol,
multi-session `NautilusTrader v2` `BacktestEngine` directly from catalog read-back
with `RiskEngineConfig(bypass=False)` and `SignalExecutionStrategy`, emits the
unmodified official `nautilus_trader.analysis.tearsheet.create_tearsheet` HTML
(dark theme + SPY benchmark), logs to MLflow 3, and writes
`/mnt/z/sota-data/manifests/sim_backtest_coverage_manifest.json`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import time
from collections import deque
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import nautilus_trader.model as nm
import polars as pl
from nautilus_trader.analysis.tearsheet import TearsheetConfig, create_tearsheet
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.common import LoggerConfig, LogLevel
from nautilus_trader.execution import MakerTakerFeeModel
from nautilus_trader.persistence import ParquetDataCatalog

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from tier0_sota import discovery as disc  # noqa: E402
from tier0_sota import engine as eng  # noqa: E402

_NY_TZ = ZoneInfo("America/New_York")
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
PREWALL_BASKET_BENCHMARKS: tuple[str, ...] = ("SPY", "QQQ", "SOXX")
PREWALL_BASKET_TRADED: tuple[str, ...] = ("NVDA", "AMD", "TSLA", "PLTR", "SMCI", "SOXS", "SQQQ")
PREWALL_BASKET_ALL: tuple[str, ...] = PREWALL_BASKET_BENCHMARKS + PREWALL_BASKET_TRADED
INVERSE_HEDGE_SYMBOLS: frozenset[str] = frozenset({"SOXS", "SQQQ"})
DEFAULT_SESSIONS_COUNT = 60
DEFAULT_MLFLOW_URI = "http://127.0.0.1:5000"
MLFLOW_EXPERIMENT = "tier0-sota-sim-tearsheet-suite"
SCHEMA_VERSION = "tier0.sim-backtest-coverage-manifest.v1"


def _resolve_data_root() -> Path:
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    return Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_risk_kernel_sha256() -> str:
    digest = _sha256_file(REPO_ROOT / "src" / "tier0_sota" / "risk.py")
    if digest != EXPECTED_RISK_SHA256:
        raise RuntimeError(
            f"Immutable risk.py SHA-256 mismatch: expected {EXPECTED_RISK_SHA256}, got {digest}"
        )
    return digest


def _clean_float(val: Any) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        out = float(val)
    except (TypeError, ValueError):
        return None
    return round(out, 6) if math.isfinite(out) else None


def _sanitize_json_numbers(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _sanitize_json_numbers(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_json_numbers(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 6) if math.isfinite(obj) else None
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(
        json.dumps(_sanitize_json_numbers(dict(payload)), indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _copy_atomic(src: Path, dst: Path) -> None:
    if src.resolve() == dst.resolve():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(f".{dst.name}.{os.getpid()}.tmp")
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def load_prewall_basket_bars(
    lake_root: Path,
    *,
    symbols: Sequence[str] = PREWALL_BASKET_ALL,
    max_days: int = DEFAULT_SESSIONS_COUNT,
) -> tuple[pl.DataFrame, list[date]]:
    """Load strictly causal pre-wall (`<= 2026-10-15`) 1-minute SIP bars via lazy Polars scan."""
    req_syms = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not req_syms:
        raise ValueError("symbols must be non-empty")
    days = disc.list_prewall_bar_days(lake_root, max_days=max_days)
    if not days:
        raise FileNotFoundError(f"No pre-wall 1-minute bar days found under {lake_root}")
    for d in days:
        disc.assert_pre_wall_date(d)

    bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"
    globs = [str(bars_root / d.isoformat() / "*.parquet") for d in days]
    df = (
        pl.scan_parquet(globs)
        .filter(pl.col("symbol").str.to_uppercase().is_in(req_syms))
        .select(
            [
                pl.col("symbol").str.to_uppercase().alias("symbol"),
                pl.col("t_ns").cast(pl.Int64),
                pl.col("o").cast(pl.Float64),
                pl.col("h").cast(pl.Float64),
                pl.col("l").cast(pl.Float64),
                pl.col("c").cast(pl.Float64),
                pl.col("v").cast(pl.Float64),
            ]
        )
        .filter(
            (pl.col("o") > 0.0)
            & (pl.col("h") > 0.0)
            & (pl.col("l") > 0.0)
            & (pl.col("c") > 0.0)
            & (pl.col("v") >= 0.0)
        )
        .unique(subset=["symbol", "t_ns"], keep="last")
        .sort(["symbol", "t_ns"])
        .collect()
    )
    if df.height == 0:
        raise ValueError(f"No pre-wall bars matched {req_syms} in {days[0]}..{days[-1]}")

    # Enforce Pandera Polars schema, OHLC ordering, and causal bar-close (`t_ns + 60s <= wall`).
    df = disc.validate_causal_bars(df, validate=True)
    if df.height == 0:
        raise ValueError(
            f"No causal pre-wall bars remained for {req_syms} in {days[0]}..{days[-1]}"
        )
    return df, days


def populate_nautilus_parquet_catalog(
    bars_df: pl.DataFrame,
    catalog_dir: Path,
    *,
    clear_existing: bool = True,
    verify_round_trip: bool = True,
) -> dict[str, Any]:
    """Write native NautilusTrader v2 instruments & 1m bars to `ParquetDataCatalog` and verify."""
    if bars_df.height == 0:
        raise ValueError("Cannot populate ParquetDataCatalog from empty bars_df")
    max_ts = bars_df.select(pl.col("t_ns").max()).item()
    min_ts = bars_df.select(pl.col("t_ns").min()).item()
    if not isinstance(max_ts, int) or not isinstance(min_ts, int):
        raise ValueError("bars_df missing integer t_ns bounds")
    disc.assert_pre_wall_ns(max_ts)

    catalog_dir.mkdir(parents=True, exist_ok=True)
    if clear_existing:
        data_sub = catalog_dir / "data"
        if data_sub.exists():
            shutil.rmtree(data_sub)

    usd = nm.Currency.from_str("USD")
    catalog = ParquetDataCatalog(str(catalog_dir))
    symbols = sorted(str(s).upper() for s in bars_df["symbol"].unique().to_list())
    instruments: list[nm.Equity] = []
    bar_type_by_symbol: dict[str, str] = {}
    written_bars_by_symbol: dict[str, int] = {}
    precision_by_symbol: dict[str, int] = {}

    for sym in symbols:
        sub = bars_df.filter(pl.col("symbol").str.to_uppercase() == sym).sort("t_ns")
        inst, btype, bars, prec = disc.df_to_equity_and_bars(sub, sym, usd)
        instruments.append(inst)
        bar_type_by_symbol[sym] = str(btype)
        written_bars_by_symbol[sym] = len(bars)
        precision_by_symbol[sym] = prec
        catalog.write_bars(bars, skip_disjoint_check=False)

    catalog.write_instruments(instruments)

    read_instruments = instruments
    read_bars_by_symbol: dict[str, int] = dict(written_bars_by_symbol)
    round_trip_verified = False
    if verify_round_trip:
        # Read back through a fresh ParquetDataCatalog handle to verify exact round-trip parity.
        verify_cat = ParquetDataCatalog(str(catalog_dir))
        read_instruments = list(verify_cat.instruments())
        read_bars_by_symbol = {}
        for sym, bt_str in bar_type_by_symbol.items():
            rb = verify_cat.query_bars(identifiers=[bt_str])
            read_bars_by_symbol[sym] = len(rb)
            if len(rb) != written_bars_by_symbol[sym]:
                raise RuntimeError(
                    f"ParquetDataCatalog round-trip bar mismatch for {sym}: "
                    f"wrote {written_bars_by_symbol[sym]}, read {len(rb)}"
                )
        round_trip_verified = (
            len(read_instruments) == len(instruments)
            and read_bars_by_symbol == written_bars_by_symbol
        )

    parquet_files = sorted(catalog_dir.rglob("*.parquet"))
    total_bytes = sum(p.stat().st_size for p in parquet_files)
    return {
        "catalog_path": str(catalog_dir),
        "instruments_count": len(read_instruments),
        "instrument_ids": sorted(str(i.id) for i in read_instruments),
        "bar_types": bar_type_by_symbol,
        "price_precisions": precision_by_symbol,
        "bars_written_total": sum(written_bars_by_symbol.values()),
        "bars_read_back_total": sum(read_bars_by_symbol.values()) if verify_round_trip else 0,
        "round_trip_parity_ok": round_trip_verified,
        "bars_by_symbol": written_bars_by_symbol,
        "parquet_files_count": len(parquet_files),
        "catalog_size_bytes": total_bytes,
        "catalog_size_mb": round(total_bytes / (1024.0 * 1024.0), 3),
        "first_bar_utc": datetime.fromtimestamp(min_ts / 1e9, tz=UTC).isoformat(),
        "last_bar_utc": datetime.fromtimestamp(max_ts / 1e9, tz=UTC).isoformat(),
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
    }


def build_causal_multi_session_signals(
    bars_df: pl.DataFrame,
    *,
    traded_symbols: Sequence[str] = PREWALL_BASKET_TRADED,
    benchmark_symbols: Sequence[str] = PREWALL_BASKET_BENCHMARKS,
    rvol_threshold: float = 2.0,
    inverse_rvol_threshold: float = 1.8,
) -> tuple[dict[str, dict[int, int]], Any, dict[str, Any]]:
    """Build strictly causal PIT intraday breakout/crash signals and daily SPY benchmark returns."""
    bench_set = [str(s).upper() for s in benchmark_symbols]
    bench_df = bars_df.filter(pl.col("symbol").is_in(bench_set)).sort(["t_ns", "symbol"])
    regime_map: dict[int, int] = (
        disc.compute_causal_market_regime(bench_df) if bench_df.height > 0 else {}
    )
    sorted_reg_ts = sorted(regime_map)

    def _causal_regime_at(ts_ns: int) -> int:
        if not sorted_reg_ts:
            return 0
        # Binary search for latest causal regime timestamp <= ts_ns + BAR_DURATION_NS
        lo, hi, idx = 0, len(sorted_reg_ts) - 1, -1
        target = ts_ns + disc.BAR_DURATION_NS
        while lo <= hi:
            mid = (lo + hi) // 2
            if sorted_reg_ts[mid] <= target:
                idx = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return regime_map[sorted_reg_ts[idx]] if idx >= 0 else 0

    signals_by_symbol: dict[str, dict[int, int]] = {}
    signal_counts: dict[str, dict[str, int]] = {}
    for raw_sym in traded_symbols:
        sym = str(raw_sym).upper()
        sub = bars_df.filter(pl.col("symbol") == sym).sort("t_ns")
        if sub.height == 0:
            signals_by_symbol[sym] = {}
            signal_counts[sym] = {"long": 0, "short": 0}
            continue
        is_inv = sym in INVERSE_HEDGE_SYMBOLS
        sym_sigs: dict[int, int] = {}
        n_long = 0
        n_short = 0
        cur_day: date | None = None
        day_signaled = False
        pv_sum = 0.0
        v_sum = 0.0
        prior_highs: deque[float] = deque(maxlen=30)
        prior_lows: deque[float] = deque(maxlen=30)
        prior_vols: deque[float] = deque(maxlen=40)

        for row in sub.select(["t_ns", "h", "l", "c", "v"]).iter_rows():
            t_ns, hi_px, lo_px, cl_px, vol = (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
            )
            dt_ny = datetime.fromtimestamp(t_ns / 1e9, tz=UTC).astimezone(_NY_TZ)
            day_ny = dt_ny.date()
            mins_ny = dt_ny.hour * 60 + dt_ny.minute
            if day_ny != cur_day:
                cur_day = day_ny
                day_signaled = False
                pv_sum = 0.0
                v_sum = 0.0
                prior_highs.clear()
                prior_lows.clear()
                prior_vols.clear()

            # Restrict causal state accumulation to RTH (09:30..15:59 ET) for clean intraday VWAP.
            if mins_ny < 570 or mins_ny >= 960:
                continue

            tp = (hi_px + lo_px + cl_px) / 3.0
            pv_sum += tp * vol
            v_sum += vol
            vwap = (pv_sum / v_sum) if v_sum > 0.0 else cl_px

            # Evaluate strictly against PRIOR closed bars (zero lookahead).
            if not day_signaled and 585 <= mins_ny <= 870 and len(prior_highs) >= 15:
                prev_hi = max(prior_highs)
                prev_lo = min(prior_lows)
                vol_list = list(prior_vols)
                if len(vol_list) >= 10:
                    recent_v = (sum(vol_list[-4:]) + vol) / 5.0
                    base_v = sum(vol_list[:-4]) / max(1, len(vol_list) - 4)
                    rvol = (recent_v / base_v) if base_v > 0.0 else 0.0
                else:
                    rvol = 0.0
                reg = _causal_regime_at(t_ns)
                close_ts_ns = t_ns + disc.BAR_DURATION_NS
                if is_inv:
                    if (
                        reg <= 0
                        and cl_px > prev_hi
                        and cl_px > vwap
                        and rvol >= inverse_rvol_threshold
                    ):
                        sym_sigs[close_ts_ns] = 1
                        n_long += 1
                        day_signaled = True
                else:
                    if reg >= 0 and cl_px > prev_hi and cl_px > vwap and rvol >= rvol_threshold:
                        sym_sigs[close_ts_ns] = 1
                        n_long += 1
                        day_signaled = True
                    elif reg == -1 and cl_px < prev_lo and cl_px < vwap and rvol >= rvol_threshold:
                        sym_sigs[close_ts_ns] = -1
                        n_short += 1
                        day_signaled = True

            prior_highs.append(hi_px)
            prior_lows.append(lo_px)
            prior_vols.append(vol)

        signals_by_symbol[sym] = sym_sigs
        signal_counts[sym] = {"long": n_long, "short": n_short}

    # Build daily SPY benchmark return Series via Polars -> to_pandas() (zero direct pandas import).
    spy_df = bars_df.filter(pl.col("symbol") == "SPY").sort("t_ns")
    spy_benchmark_series: Any = None
    spy_days_count = 0
    spy_total_return_pct: float | None = None
    if spy_df.height > 0:
        spy_daily = (
            spy_df.with_columns(
                pl.from_epoch(pl.col("t_ns"), time_unit="ns")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone("America/New_York")
                .dt.date()
                .alias("ny_date")
            )
            .group_by("ny_date")
            .agg(
                [
                    pl.col("c").last().alias("close"),
                    pl.col("t_ns").last().alias("last_t_ns"),
                ]
            )
            .sort("ny_date")
            .with_columns(
                pl.col("close").pct_change().alias("ret"),
                pl.from_epoch(pl.col("last_t_ns"), time_unit="ns")
                .dt.replace_time_zone("UTC")
                .dt.truncate("1d")
                .alias("ts"),
            )
            .drop_nulls(subset=["ret"])
        )
        spy_days_count = spy_daily.height
        if spy_daily.height > 0:
            first_c = float(spy_df.select(pl.col("c").first()).item())
            last_c = float(spy_df.select(pl.col("c").last()).item())
            if first_c > 0.0:
                spy_total_return_pct = round(((last_c / first_c) - 1.0) * 100.0, 4)
            spy_benchmark_series = (
                spy_daily.select(["ts", "ret"]).to_pandas().set_index("ts")["ret"]
            )

    sig_meta = {
        "regime_points_count": len(regime_map),
        "signal_counts_by_symbol": signal_counts,
        "total_long_signals": sum(v["long"] for v in signal_counts.values()),
        "total_short_signals": sum(v["short"] for v in signal_counts.values()),
        "spy_benchmark_days": spy_days_count,
        "spy_benchmark_total_return_pct": spy_total_return_pct,
    }
    return signals_by_symbol, spy_benchmark_series, sig_meta


def run_empirical_backtest_from_catalog(
    catalog_dir: Path,
    *,
    catalog_meta: Mapping[str, Any],
    signals_by_symbol: Mapping[str, dict[int, int]],
    spy_benchmark_returns: Any,
    tearsheet_paths: Sequence[Path],
    starting_cash: Decimal = Decimal("100000"),
    cost_bps_per_side: Decimal = Decimal("4.0"),
    venue_profile: str = "IBKR_PAPER_4002",
    benchmark_symbols: Sequence[str] = PREWALL_BASKET_BENCHMARKS,
) -> dict[str, Any]:
    """Read instruments/bars from `ParquetDataCatalog`, run `BacktestEngine`, emit tearsheet."""
    t0 = time.monotonic()
    usd = nm.Currency.from_str("USD")
    catalog = ParquetDataCatalog(str(catalog_dir))
    instruments = list(catalog.instruments())
    if not instruments:
        raise RuntimeError(f"ParquetDataCatalog at {catalog_dir} has zero instruments")

    bar_types_map: Mapping[str, str] = catalog_meta["bar_types"]
    prec_map: Mapping[str, int] = catalog_meta["price_precisions"]
    expected_bars_by_sym: Mapping[str, int] = catalog_meta.get("bars_by_symbol", {})

    all_bars: list[nm.Bar] = []
    parsed_bar_types: dict[str, nm.BarType] = {}
    read_bars_by_symbol: dict[str, int] = {}
    for sym, bt_str in sorted(bar_types_map.items()):
        bars = list(catalog.query_bars(identifiers=[bt_str]))
        read_bars_by_symbol[sym] = len(bars)
        if sym in expected_bars_by_sym and len(bars) != int(expected_bars_by_sym[sym]):
            raise RuntimeError(
                f"ParquetDataCatalog read-back mismatch for {sym}: "
                f"expected {expected_bars_by_sym[sym]}, got {len(bars)}"
            )
        if bars:
            parsed_bar_types[sym] = bars[0].bar_type
            all_bars.extend(bars)
    if isinstance(catalog_meta, dict):
        catalog_meta["instruments_count"] = len(instruments)
        catalog_meta["instrument_ids"] = sorted(str(i.id) for i in instruments)
        catalog_meta["bars_read_back_total"] = sum(read_bars_by_symbol.values())
        catalog_meta["round_trip_parity_ok"] = len(instruments) == len(bar_types_map) and sum(
            read_bars_by_symbol.values()
        ) == int(catalog_meta["bars_written_total"])
    all_bars.sort(key=lambda b: (b.ts_event, str(b.bar_type)))
    if not all_bars:
        raise RuntimeError(f"ParquetDataCatalog at {catalog_dir} returned zero bars")

    re_cfg = eng.build_venue_risk_engine_config(venue_profile)
    quiet_log = LoggerConfig(stdout_level=LogLevel.OFF, bypass_logging=True)
    engine = BacktestEngine(
        BacktestEngineConfig(logging=quiet_log, risk_engine=re_cfg, run_analysis=True)
    )
    tearsheet_receipts: list[dict[str, Any]] = []
    try:
        fee_rate = cost_bps_per_side / Decimal("10000")
        fee_model = MakerTakerFeeModel(fee_rate, fee_rate)
        c0 = float(starting_cash)
        engine.add_venue(
            eng.VENUE_XNAS,
            nm.OmsType.NETTING,
            nm.AccountType.MARGIN,
            [nm.Money(c0, usd)],
            usd,
            fee_model=fee_model,
            use_reduce_only=False,
        )
        for inst in instruments:
            engine.add_instrument(inst)
        engine.add_data(all_bars, client_id=nm.ClientId("CATALOG_PREWALL"))

        bm_bar_types = tuple(
            parsed_bar_types[s] for s in benchmark_symbols if s in parsed_bar_types
        )
        if bm_bar_types:
            engine.add_actor(
                eng.MarketRegimeActor(eng.MarketRegimeActorConfig(bar_types=bm_bar_types))
            )

        strategies: dict[str, eng.SignalExecutionStrategy] = {}
        for idx, (sym, sig_map) in enumerate(sorted(signals_by_symbol.items()), start=1):
            if sym not in parsed_bar_types or not sig_map:
                continue
            scfg = eng.SignalExecutionConfig(
                parsed_bar_types[sym],
                sig_map,
                starting_cash,
                int(prec_map.get(sym, 2)),
                cost_bps_per_side,
                enable_24x5_trailing=True,
                strategy_id=nm.StrategyId(f"TIER0-{sym}"),
                order_id_tag=f"{idx:03d}",
                session_protection_24x5=eng.SESSION_PROTECTION_24X5,
                market_bar_types=bm_bar_types,
                venue_profile=venue_profile,
                auto_reverse=False,
                auto_reentry=False,
                semi_auto_confirmed=True,
                shortable=True,
                enable_native_5m_bars=True,
                enable_adaptive_lossless_be=True,
            )
            strat = eng.SignalExecutionStrategy(scfg)
            engine.add_strategy(strat)
            strategies[sym] = strat

        engine.run()

        result = engine.get_result()
        acct = engine.cache.account_for_venue(eng.VENUE_XNAS)
        bal = acct.balance_total(usd) if acct is not None else None
        ending_eq = float(bal.as_double()) if bal is not None else c0
        orders = list(engine.cache.orders())
        fills = sum(1 for o in orders if int(o.filled_qty.as_double()) > 0)
        closed_positions = list(engine.cache.positions_closed())
        open_positions = list(engine.cache.positions_open())

        # Extract official NautilusTrader v2 analyzer statistics verbatim.
        stats_returns = dict(result.stats_returns)
        stats_general = dict(result.stats_general)
        stats_pnls_raw = dict(result.stats_pnls)
        usd_pnls = (
            dict(stats_pnls_raw.get("USD") or {})
            if isinstance(stats_pnls_raw.get("USD"), Mapping)
            else {}
        )

        # Compute max drawdown cleanly from account balance report if not directly in stats_returns.
        acct_report = engine.generate_account_report(venue=eng.VENUE_XNAS)
        max_dd_pct: float | None = None
        if acct_report is not None and len(acct_report) > 0 and "total" in acct_report.columns:
            totals = [float(x) for x in acct_report["total"].tolist() if math.isfinite(float(x))]
            if totals:
                peak = totals[0]
                worst = 0.0
                for eq in totals:
                    peak = max(peak, eq)
                    if peak > 0.0:
                        dd = (eq - peak) / peak
                        worst = min(worst, dd)
                max_dd_pct = round(worst * 100.0, 4)

        all_trade_rets = [r for st in strategies.values() for r in st._trade_returns]
        winning_round_trips = sum(1 for r in all_trade_rets if r > 0.0)
        losing_round_trips = sum(1 for r in all_trade_rets if r <= 0.0)
        win_rate = _clean_float(usd_pnls.get("Win Rate"))
        if win_rate is None:
            win_rate = _clean_float(stats_general.get("Win Rate"))
        if win_rate is None and all_trade_rets:
            win_rate = round(winning_round_trips / len(all_trade_rets), 6)
        elif win_rate is None and closed_positions:
            wins = sum(1 for p in closed_positions if float(p.realized_pnl.as_double()) > 0.0)
            win_rate = round(wins / len(closed_positions), 6)

        # Emit unmodified official NautilusTrader v2 dark tearsheet with SPY benchmark overlay.
        if tearsheet_paths:
            primary_ts = tearsheet_paths[0]
            primary_ts.parent.mkdir(parents=True, exist_ok=True)
            title = (
                "Tier-0 SOTA Pre-Wall Empirical Backtest "
                f"({catalog_meta['first_bar_utc'][:10]} to {catalog_meta['last_bar_utc'][:10]}, "
                f"{len(strategies)} Symbols, 4 bps/side, RiskEngine=ON)"
            )
            ts_cfg = TearsheetConfig(
                theme="nautilus_dark",
                title=title,
                include_benchmark=spy_benchmark_returns is not None,
                benchmark_name="SPY (Pre-Wall)",
                height=1600,
                show_logo=True,
            )
            create_tearsheet(
                engine,
                output_path=str(primary_ts),
                title=title,
                currency=usd,
                config=ts_cfg,
                benchmark_returns=spy_benchmark_returns,
                benchmark_name="SPY (Pre-Wall)",
            )
            for target_p in tearsheet_paths[1:]:
                _copy_atomic(primary_ts, target_p)
            for tp in tearsheet_paths:
                tearsheet_receipts.append(
                    {
                        "path": str(tp),
                        "size_bytes": tp.stat().st_size,
                        "sha256": _sha256_file(tp),
                        "generator": "nautilus_trader.analysis.tearsheet.create_tearsheet",
                        "theme": "nautilus_dark",
                    }
                )

        per_strategy = {
            sym: {
                "bars_processed": st.bars_processed,
                "bars_5m_processed": st.bars_5m_processed,
                "signals_received": st.signals_received,
                "stops_attached": st.stops_attached,
                "trailing_ratchets": st.trailing_ratchets,
                "max_lock_tier": st.max_lock_tier,
                "trade_returns_count": len(st._trade_returns),
                "halt_triggered": bool(st._halt_triggered),
            }
            for sym, st in sorted(strategies.items())
        }

        return {
            "engine": "NautilusTrader v2 BacktestEngine (ParquetDataCatalog read-back)",
            "venue": str(eng.VENUE_XNAS),
            "venue_profile": venue_profile,
            "risk_engine_bypass": False,
            "paper_only": True,
            "live_execution_enabled": False,
            "starting_equity_usd": c0,
            "ending_equity_usd": round(ending_eq, 2),
            "total_return_pct": round(((ending_eq - c0) / c0) * 100.0, 4),
            "cost_bps_per_side": float(cost_bps_per_side),
            "bars_fed_total": len(all_bars),
            "strategies_active": sorted(strategies),
            "total_orders": len(orders),
            "total_fills": fills,
            "round_trip_trades_count": len(all_trade_rets),
            "winning_round_trips": winning_round_trips,
            "losing_round_trips": losing_round_trips,
            "closed_positions_count": len(closed_positions),
            "open_positions_at_eof": len(open_positions),
            "stops_attached_total": sum(st.stops_attached for st in strategies.values()),
            "trailing_ratchets_total": sum(st.trailing_ratchets for st in strategies.values()),
            "win_rate": win_rate,
            "max_drawdown_pct": max_dd_pct,
            "sharpe_ratio_252": _clean_float(stats_returns.get("Sharpe Ratio (252 days)")),
            "sortino_ratio_252": _clean_float(stats_returns.get("Sortino Ratio (252 days)")),
            "profit_factor": _clean_float(stats_returns.get("Profit Factor")),
            "expectancy_usd": _clean_float(usd_pnls.get("Expectancy")),
            "pnl_total_usd": _clean_float(usd_pnls.get("PnL (total)")),
            "stats_returns": _sanitize_json_numbers(stats_returns),
            "stats_general": _sanitize_json_numbers(stats_general),
            "stats_pnls_usd": _sanitize_json_numbers(usd_pnls),
            "per_strategy": per_strategy,
            "tearsheets": tearsheet_receipts,
            "elapsed_secs": round(time.monotonic() - t0, 2),
        }
    finally:
        engine.dispose()


def _log_to_mlflow(
    *,
    tracking_uri: str,
    experiment_name: str,
    catalog_meta: Mapping[str, Any],
    backtest_meta: Mapping[str, Any],
    signal_meta: Mapping[str, Any],
    primary_tearsheet: Path | None,
) -> dict[str, Any]:
    try:
        import mlflow

        mlflow.set_tracking_uri(tracking_uri)
        exp = mlflow.set_experiment(experiment_name)
        with mlflow.start_run(run_name="nautilus-prewall-empirical-tearsheet") as run:
            mlflow.set_tags(
                {
                    "lane": "sim-backtest",
                    "paper_only": "true",
                    "live_execution_enabled": "false",
                    "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
                    "risk_kernel_sha256": EXPECTED_RISK_SHA256,
                    "tearsheet_generator": "nautilus_trader.analysis.tearsheet.create_tearsheet",
                    "tearsheet_caddy_url": (
                        "http://127.0.0.1:8793/tearsheets/nautilus_prewall_empirical_tearsheet.html"
                    ),
                    "tearsheet_sha256": (
                        _sha256_file(primary_tearsheet)
                        if primary_tearsheet is not None and primary_tearsheet.is_file()
                        else ""
                    ),
                }
            )
            mlflow.log_params(
                {
                    "catalog_instruments": catalog_meta["instruments_count"],
                    "catalog_bars_total": catalog_meta["bars_read_back_total"],
                    "prewall_first_utc": catalog_meta["first_bar_utc"],
                    "prewall_last_utc": catalog_meta["last_bar_utc"],
                    "cost_bps_per_side": backtest_meta["cost_bps_per_side"],
                    "venue_profile": backtest_meta["venue_profile"],
                    "risk_engine_bypass": False,
                }
            )
            metrics: dict[str, float] = {
                "catalog_bars_total": float(catalog_meta["bars_read_back_total"]),
                "catalog_size_mb": float(catalog_meta["catalog_size_mb"]),
                "total_orders": float(backtest_meta["total_orders"]),
                "total_fills": float(backtest_meta["total_fills"]),
                "round_trip_trades_count": float(backtest_meta["round_trip_trades_count"]),
                "closed_positions_count": float(backtest_meta["closed_positions_count"]),
                "stops_attached_total": float(backtest_meta["stops_attached_total"]),
                "trailing_ratchets_total": float(backtest_meta["trailing_ratchets_total"]),
                "total_return_pct": float(backtest_meta["total_return_pct"]),
                "total_long_signals": float(signal_meta["total_long_signals"]),
                "total_short_signals": float(signal_meta["total_short_signals"]),
            }
            for k in (
                "win_rate",
                "max_drawdown_pct",
                "sharpe_ratio_252",
                "sortino_ratio_252",
                "profit_factor",
                "expectancy_usd",
                "pnl_total_usd",
            ):
                if (v := _clean_float(backtest_meta.get(k))) is not None:
                    metrics[k] = v
            if (spy_ret := _clean_float(signal_meta.get("spy_benchmark_total_return_pct"))) is not (
                None
            ):
                metrics["spy_benchmark_total_return_pct"] = spy_ret
            mlflow.log_metrics(metrics)
            artifact_logged = False
            artifact_mode = "none"
            artifact_uri = str(run.info.artifact_uri or "")
            if primary_tearsheet is not None and primary_tearsheet.is_file():
                try:
                    mlflow.log_artifact(str(primary_tearsheet), artifact_path="tearsheets")
                    artifact_logged = True
                    artifact_mode = "mlflow_api"
                except OSError:
                    # Docker MLflow server uses `/state/mlflow/mlartifacts` mounted from host state.
                    cleaned_uri = artifact_uri.removeprefix("file://")
                    if cleaned_uri.startswith("/state/mlflow/"):
                        state_root = Path(
                            os.environ.get("TIER0_STATE_ROOT", "").strip()
                            or str(Path.home() / "tier0-sota-state")
                        )
                        host_mlflow_root = state_root / "mlflow"
                        if host_mlflow_root.is_dir():
                            rel_sub = cleaned_uri.removeprefix("/state/mlflow/")
                            dst = host_mlflow_root / rel_sub / "tearsheets" / primary_tearsheet.name
                            _copy_atomic(primary_tearsheet, dst)
                            artifact_logged = True
                            artifact_mode = "host_bind_mount"
            return {
                "logged": True,
                "tracking_uri": tracking_uri,
                "experiment_name": experiment_name,
                "experiment_id": str(exp.experiment_id),
                "run_id": str(run.info.run_id),
                "artifact_uri": artifact_uri,
                "tearsheet_artifact_logged": artifact_logged,
                "artifact_log_mode": artifact_mode,
            }
    except Exception as exc:  # noqa: BLE001
        return {
            "logged": False,
            "tracking_uri": tracking_uri,
            "experiment_name": experiment_name,
            "error": f"{type(exc).__name__}: {exc}",
        }


def run_sim_backtest_tearsheet_suite(
    *,
    data_root: Path | None = None,
    lake_root: Path | None = None,
    catalog_dir: Path | None = None,
    manifest_path: Path | None = None,
    tearsheet_paths: Sequence[Path] | None = None,
    symbols: Sequence[str] = PREWALL_BASKET_ALL,
    max_days: int = DEFAULT_SESSIONS_COUNT,
    starting_cash: Decimal = Decimal("100000"),
    cost_bps_per_side: Decimal = Decimal("4.0"),
    log_mlflow: bool = True,
    mlflow_uri: str = DEFAULT_MLFLOW_URI,
) -> dict[str, Any]:
    """Execute end-to-end pre-wall ParquetDataCatalog build, backtest, tearsheet & manifest."""
    t_total = time.monotonic()
    risk_sha = _verify_risk_kernel_sha256()
    root = data_root or _resolve_data_root()
    resolved_lake = lake_root or (root / "prewall-lake")
    resolved_catalog = catalog_dir or (root / "nautilus-catalog")
    resolved_manifest = manifest_path or (
        root / "manifests" / "sim_backtest_coverage_manifest.json"
    )
    resolved_tearsheets = (
        list(tearsheet_paths)
        if tearsheet_paths is not None
        else [
            root / "manifests" / "tearsheets" / "nautilus_prewall_empirical_tearsheet.html",
            root / "manifests" / "tearsheets" / "nautilus_prewall_tearsheet.html",
            root / "tearsheets" / "nautilus_prewall_tearsheet.html",
        ]
    )

    bars_df, days = load_prewall_basket_bars(resolved_lake, symbols=symbols, max_days=max_days)
    catalog_meta = populate_nautilus_parquet_catalog(
        bars_df,
        resolved_catalog,
        clear_existing=True,
        verify_round_trip=False,
    )
    traded = [s for s in PREWALL_BASKET_TRADED if s in set(catalog_meta["bars_by_symbol"])]
    benchmarks = [s for s in PREWALL_BASKET_BENCHMARKS if s in set(catalog_meta["bars_by_symbol"])]
    signals_by_symbol, spy_benchmark_returns, signal_meta = build_causal_multi_session_signals(
        bars_df,
        traded_symbols=traded,
        benchmark_symbols=benchmarks,
    )
    backtest_meta = run_empirical_backtest_from_catalog(
        resolved_catalog,
        catalog_meta=catalog_meta,
        signals_by_symbol=signals_by_symbol,
        spy_benchmark_returns=spy_benchmark_returns,
        tearsheet_paths=resolved_tearsheets,
        starting_cash=starting_cash,
        cost_bps_per_side=cost_bps_per_side,
        benchmark_symbols=benchmarks,
    )
    mlflow_meta = (
        _log_to_mlflow(
            tracking_uri=mlflow_uri,
            experiment_name=MLFLOW_EXPERIMENT,
            catalog_meta=catalog_meta,
            backtest_meta=backtest_meta,
            signal_meta=signal_meta,
            primary_tearsheet=resolved_tearsheets[0] if resolved_tearsheets else None,
        )
        if log_mlflow
        else {"logged": False, "skipped": True}
    )

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": risk_sha,
        "risk_kernel_sha256_match": risk_sha == EXPECTED_RISK_SHA256,
        "causal_pit_enforced": True,
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
        "sessions": {
            "count": len(days),
            "first_date": days[0].isoformat(),
            "last_date": days[-1].isoformat(),
            "calendar_months": sorted({d.strftime("%Y-%m") for d in days}),
        },
        "nautilus_catalog": catalog_meta,
        "causal_signals": signal_meta,
        "empirical_backtest": backtest_meta,
        "mlflow": mlflow_meta,
        "caddy_tearsheet_urls": [
            "http://127.0.0.1:8793/tearsheets/nautilus_prewall_empirical_tearsheet.html",
            "http://127.0.0.1:8793/tearsheets/nautilus_prewall_tearsheet.html",
        ],
        "total_elapsed_secs": round(time.monotonic() - t_total, 2),
    }
    _write_json_atomic(resolved_manifest, payload)
    return payload


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Populate NautilusTrader v2 ParquetDataCatalog & emit empirical tearsheet."
    )
    p.add_argument("--max-days", type=int, default=DEFAULT_SESSIONS_COUNT)
    p.add_argument("--starting-cash", type=str, default="100000")
    p.add_argument("--cost-bps", type=str, default="4.0")
    p.add_argument("--skip-mlflow", action="store_true")
    p.add_argument("--mlflow-uri", type=str, default=DEFAULT_MLFLOW_URI)
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    rep = run_sim_backtest_tearsheet_suite(
        max_days=args.max_days,
        starting_cash=Decimal(args.starting_cash),
        cost_bps_per_side=Decimal(args.cost_bps),
        log_mlflow=not args.skip_mlflow,
        mlflow_uri=args.mlflow_uri,
    )
    bt = rep["empirical_backtest"]
    cat = rep["nautilus_catalog"]
    print(
        json.dumps(
            {
                "schema_version": rep["schema_version"],
                "sessions": rep["sessions"],
                "catalog_instruments": cat["instruments_count"],
                "catalog_bars": cat["bars_read_back_total"],
                "catalog_size_mb": cat["catalog_size_mb"],
                "round_trip_parity_ok": cat["round_trip_parity_ok"],
                "total_orders": bt["total_orders"],
                "total_fills": bt["total_fills"],
                "round_trip_trades": bt["round_trip_trades_count"],
                "closed_positions": bt["closed_positions_count"],
                "win_rate": bt["win_rate"],
                "total_return_pct": bt["total_return_pct"],
                "max_drawdown_pct": bt["max_drawdown_pct"],
                "sharpe_ratio_252": bt["sharpe_ratio_252"],
                "sortino_ratio_252": bt["sortino_ratio_252"],
                "mlflow": rep["mlflow"],
                "tearsheets": bt["tearsheets"],
                "total_elapsed_secs": rep["total_elapsed_secs"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

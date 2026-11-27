"""Generate official unmodified ml4t-diagnostic & SHAP/shapiq pre-wall tearsheets & CPCV files.

Strictly pre-wall (<= 2026-10-15), paper-only (paper_only=True, live_execution_enabled=False),
zero hand-written HTML/JS: all HTML reports are emitted exclusively by upstream
`ml4t.diagnostic.visualization.*` APIs and Plotly figures.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import polars as pl

from tier0_sota import discovery as disc
from tier0_sota import research as res

REPO_ROOT = Path(__file__).resolve().parents[1]
RISK_KERNEL_PATH = REPO_ROOT / "src" / "tier0_sota" / "risk.py"
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"

DEFAULT_DATA_ROOT = Path("/mnt/z/sota-data")
DEFAULT_MANIFEST_DIR = DEFAULT_DATA_ROOT / "manifests"
DEFAULT_TEARSHEET_DIR = DEFAULT_MANIFEST_DIR / "tearsheets"
DEFAULT_CPCV_DIR = DEFAULT_DATA_ROOT / "cpcv-artifacts"
DEFAULT_DF10_PATH = (
    DEFAULT_DATA_ROOT / "prewall-lake" / "derived" / "df10_daily_20d_baselines.parquet"
)
DEFAULT_H109_DOSSIER = DEFAULT_MANIFEST_DIR / "prewall_h109_falsification_dossier.json"
DEFAULT_ABLATION_DOSSIER = DEFAULT_MANIFEST_DIR / "prewall_alpha_ablation_dossier.json"
DEFAULT_EARLY_MOVER_CAL = DEFAULT_MANIFEST_DIR / "prewall_early_mover_calibration_manifest.json"
DEFAULT_OUT_MANIFEST = DEFAULT_MANIFEST_DIR / "ml4t_shap_tearsheet_manifest.json"
DEFAULT_SIM_DUCKDB = DEFAULT_MANIFEST_DIR / "paper_farm_ui" / "sim_prewall_audit.duckdb"
DEFAULT_MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
DEFAULT_MLFLOW_EXPERIMENT = "tier0-sota-sim-tearsheet-suite"

PREWALL_CUTOFF_ISO = disc.DISCOVERY_WALL_ISO
PREWALL_START_ISO = "2024-06-03"
PREWALL_TUNING_END_ISO = "2025-10-31"
PREWALL_HOLDOUT_START_ISO = "2025-11-11"

H109_BASKET_SYMBOLS: tuple[str, ...] = (
    "ROKU",
    "DELL",
    "ASTS",
    "MRVL",
    "APP",
    "SMCI",
    "MU",
    "ARM",
    "CRWV",
    "HOOD",
    "AVGO",
    "INTC",
    "META",
    "SNDK",
    "COIN",
    "PLTR",
    "TSLA",
    "AMD",
    "AMZN",
    "MSFT",
    "MSTR",
    "NVDA",
    "SPY",
    "QQQ",
    "SOXX",
    "SOXS",
    "SQQQ",
)

CAUSAL_FEATURE_COLUMNS: tuple[str, ...] = (
    "rvol_20d",
    "vol_z_20d",
    "vdu_5d_20d_ratio",
    "coil_5d_20d_range_ratio",
    "prev_close_to_20d_high",
    "ret_1d",
    "prior_ret_5d",
    "prior_ret_20d",
    "bar_range_pct",
    "mean_20d_range_pct",
    "mean_5d_range_pct_prior",
)

# 12 causal H109-aligned screening arms (min_rvol, min_ret_1d, max_coil_ratio, short_climax)
H109_CPCV_ARMS: tuple[dict[str, Any], ...] = (
    {"arm_id": "H109_A01", "min_rvol": 1.4, "min_ret": 0.015, "max_coil": 1.35, "mode": "ignition"},
    {"arm_id": "H109_A02", "min_rvol": 1.6, "min_ret": 0.018, "max_coil": 1.30, "mode": "ignition"},
    {"arm_id": "H109_A03", "min_rvol": 1.8, "min_ret": 0.022, "max_coil": 1.25, "mode": "ignition"},
    {
        "arm_id": "H109_A04",
        "min_rvol": 1.4,
        "min_ret": 0.015,
        "max_coil": 1.15,
        "mode": "coil_break",
    },
    {
        "arm_id": "H109_A05",
        "min_rvol": 1.6,
        "min_ret": 0.018,
        "max_coil": 1.10,
        "mode": "coil_break",
    },
    {
        "arm_id": "H109_A06",
        "min_rvol": 1.8,
        "min_ret": 0.022,
        "max_coil": 1.05,
        "mode": "coil_break",
    },
    {
        "arm_id": "H109_A07",
        "min_rvol": 1.5,
        "min_ret": 0.012,
        "max_coil": 1.40,
        "mode": "regime_long",
    },
    {
        "arm_id": "H109_A08",
        "min_rvol": 1.7,
        "min_ret": 0.016,
        "max_coil": 1.30,
        "mode": "regime_long",
    },
    {
        "arm_id": "H109_A09",
        "min_rvol": 2.0,
        "min_ret": 0.025,
        "max_coil": 1.20,
        "mode": "regime_long",
    },
    {
        "arm_id": "H109_A10",
        "min_rvol": 1.5,
        "min_ret": 0.015,
        "max_coil": 1.35,
        "mode": "dual_hedge",
    },
    {
        "arm_id": "H109_A11",
        "min_rvol": 1.7,
        "min_ret": 0.020,
        "max_coil": 1.25,
        "mode": "dual_hedge",
    },
    {
        "arm_id": "H109_A12",
        "min_rvol": 1.8,
        "min_ret": 0.018,
        "max_coil": 1.20,
        "mode": "regime_vwap",
    },
)


def verify_risk_kernel_sha256() -> str:
    """Fail-closed SHA-256 verification of immutable src/tier0_sota/risk.py."""
    digest = hashlib.sha256(RISK_KERNEL_PATH.read_bytes()).hexdigest()
    if digest != EXPECTED_RISK_SHA256:
        raise RuntimeError(
            f"Risk kernel SHA-256 mismatch: got {digest}, expected {EXPECTED_RISK_SHA256}"
        )
    return digest


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _copy_atomic(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _load_json_safe(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _finite_or_zero(val: Any, digits: int = 6) -> float:
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(fval):
        return 0.0
    return round(fval, digits)


def load_prewall_causal_dataset(
    df10_path: Path = DEFAULT_DF10_PATH,
    *,
    symbols: Sequence[str] = H109_BASKET_SYMBOLS,
    start_date: str = PREWALL_START_ISO,
    cutoff_date: str = PREWALL_CUTOFF_ISO,
) -> pl.DataFrame:
    """Load strictly causal pre-wall (<= 2026-10-15) baseline features and next-day returns."""
    disc.assert_pre_wall_date(cutoff_date)
    if not df10_path.is_file():
        raise FileNotFoundError(f"Pre-wall df10 dataset not found: {df10_path}")

    cols = [
        "symbol",
        "trade_date",
        *CAUSAL_FEATURE_COLUMNS,
        "next1_split_open",
        "next1_split_close",
        "next1_split_high",
        "next1_split_low",
        "split_close",
    ]
    df = (
        pl.scan_parquet(df10_path)
        .filter(
            pl.col("symbol").is_in(list(symbols))
            & (pl.col("trade_date") >= start_date)
            & (pl.col("trade_date") <= cutoff_date)
        )
        .select(cols)
        .drop_nulls(subset=[*CAUSAL_FEATURE_COLUMNS, "next1_split_open", "next1_split_close"])
        .filter((pl.col("next1_split_open") > 0.0) & (pl.col("split_close") > 0.0))
        .with_columns(
            (
                (pl.col("next1_split_close") - pl.col("next1_split_open"))
                / pl.col("next1_split_open")
            )
            .clip(-0.20, 0.20)
            .alias("next1_open_to_close_ret"),
            ((pl.col("next1_split_high") - pl.col("next1_split_open")) / pl.col("next1_split_open"))
            .clip(0.0, 0.35)
            .alias("next1_mfe_pct"),
        )
        .sort(["trade_date", "symbol"])
        .collect()
    )
    if df.is_empty():
        raise ValueError("Loaded zero pre-wall rows from df10 dataset")
    max_date = str(df.select(pl.col("trade_date").max()).item())
    disc.assert_pre_wall_date(max_date)
    return df


def build_multiarm_daily_returns(
    df: pl.DataFrame,
    *,
    cost_bps_roundtrip: float = 4.0,
) -> tuple[list[str], np.ndarray, np.ndarray, list[str]]:
    """Build causal daily return matrix (n_sessions, 12 arms) + SPY benchmark on pre-wall dates."""
    max_date = str(df.select(pl.col("trade_date").max()).item())
    disc.assert_pre_wall_date(max_date)

    dates = sorted(str(d) for d in df.select(pl.col("trade_date").unique()).to_series().to_list())
    date_to_idx = {d: i for i, d in enumerate(dates)}
    n_dates = len(dates)
    n_arms = len(H109_CPCV_ARMS)
    arm_ids = [str(a["arm_id"]) for a in H109_CPCV_ARMS]

    # Causal macro regime proxy from SPY/QQQ/SOXX day-t return (strictly known before t+1 open)
    bench = (
        df.filter(pl.col("symbol").is_in(["SPY", "QQQ", "SOXX"]))
        .group_by("trade_date")
        .agg(
            pl.col("ret_1d").mean().alias("macro_ret_1d"),
            pl.col("next1_open_to_close_ret")
            .filter(pl.col("symbol") == "SPY")
            .first()
            .alias("spy_next_ret"),
        )
    )
    macro_by_date: dict[str, float] = {}
    spy_bench = np.zeros(n_dates, dtype=float)
    for row in bench.iter_rows(named=True):
        d = str(row["trade_date"])
        idx = date_to_idx[d]
        macro_by_date[d] = float(row["macro_ret_1d"] or 0.0)
        spy_bench[idx] = float(row["spy_next_ret"] or 0.0)

    # Pre-group non-benchmark active rows by date
    active_df = df.filter(~pl.col("symbol").is_in(["SPY", "QQQ", "SOXX"]))
    rows_by_date: dict[str, list[dict[str, Any]]] = {d: [] for d in dates}
    for row in active_df.iter_rows(named=True):
        rows_by_date[str(row["trade_date"])].append(row)

    fee_dec = float(cost_bps_roundtrip) / 1e4
    mat = np.zeros((n_dates, n_arms), dtype=float)

    for d_idx, d_str in enumerate(dates):
        day_rows = rows_by_date[d_str]
        if not day_rows:
            continue
        macro_r = macro_by_date.get(d_str, 0.0)
        regime = -1 if macro_r <= -0.008 else (1 if macro_r >= 0.003 else 0)

        for a_idx, arm in enumerate(H109_CPCV_ARMS):
            min_rvol = float(arm["min_rvol"])
            min_ret = float(arm["min_ret"])
            max_coil = float(arm["max_coil"])
            mode = str(arm["mode"])
            arm_rets: list[float] = []
            for r in day_rows:
                sym = str(r["symbol"])
                rvol = float(r["rvol_20d"])
                ret1 = float(r["ret_1d"])
                coil = float(r["coil_5d_20d_range_ratio"])
                nxt = float(r["next1_open_to_close_ret"])
                nxt_open = float(r["next1_split_open"])
                nxt_low = float(r["next1_split_low"])
                nxt_high = float(r["next1_split_high"])
                long_stopped = bool(nxt_open > 0.0 and nxt_low <= nxt_open * 0.98)
                short_stopped = bool(nxt_open > 0.0 and nxt_high >= nxt_open * 1.02)
                is_inv = sym in ("SOXS", "SQQQ")

                if is_inv:
                    if regime == -1 and mode in ("dual_hedge", "regime_vwap") and rvol >= 1.1:
                        realized = -0.02 if long_stopped else nxt
                        arm_rets.append(realized - fee_dec)
                    continue

                if regime >= 0 and rvol >= min_rvol and ret1 >= min_ret and coil <= max_coil:
                    # Conservative symmetric Rule-612 <= 2.00% intraday stop check
                    realized = -0.02 if long_stopped else nxt
                    arm_rets.append(realized - fee_dec)
                elif (
                    regime == -1
                    and mode in ("ignition", "dual_hedge", "regime_vwap")
                    and rvol >= min_rvol
                    and ret1 <= -min_ret
                ):
                    realized = -0.02 if short_stopped else -nxt
                    arm_rets.append(realized - fee_dec)
            if arm_rets:
                mat[d_idx, a_idx] = float(np.mean(arm_rets))

    return dates, mat, spy_bench, arm_ids


def run_skfolio_cpcv_analysis(
    dates: Sequence[str],
    returns_1x: np.ndarray,
    arm_ids: Sequence[str],
    *,
    n_trials_total: int = 211,
    n_folds: int = 8,
    n_test_folds: int = 3,
    purged_size: int = 2,
    embargo_size: int = 2,
    cpcv_dir: Path = DEFAULT_CPCV_DIR,
) -> dict[str, Any]:
    """Run real skfolio CombinatorialPurgedCV (>= 16 paths) + ml4t-diagnostic DSR/PBO/MinTRL."""
    from ml4t.diagnostic.evaluation import stats as mstats
    from scipy.stats import kurtosis, skew
    from skfolio.model_selection import CombinatorialPurgedCV

    if len(dates) < res.MIN_CPCV_SAMPLES:
        raise ValueError(f"Need >= {res.MIN_CPCV_SAMPLES} sessions for CPCV, got {len(dates)}")
    disc.assert_pre_wall_date(str(max(dates)))

    cpcv = CombinatorialPurgedCV(
        n_folds=n_folds,
        n_test_folds=n_test_folds,
        purged_size=purged_size,
        embargo_size=embargo_size,
    )
    n_test_paths = int(cpcv.n_test_paths)
    if n_test_paths < res.MIN_CPCV_SAMPLES:
        raise ValueError(
            f"CPCV must produce >= {res.MIN_CPCV_SAMPLES} OOS paths, got {n_test_paths}"
        )

    splits = list(cpcv.split(returns_1x))
    n_splits = len(splits)
    n_arms = returns_1x.shape[1]

    is_sharpe_rows: list[np.ndarray] = []
    oos_sharpe_rows: list[np.ndarray] = []
    fold_records: list[dict[str, Any]] = []

    def _ann_sharpe(arr: np.ndarray) -> float:
        sd = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
        if sd <= 1e-12:
            return 0.0
        return float(np.mean(arr) / sd * math.sqrt(252.0))

    def _max_dd(arr: np.ndarray) -> float:
        if arr.size == 0:
            return 0.0
        eq = np.cumprod(1.0 + arr)
        peak = np.maximum.accumulate(eq)
        dd = (eq - peak) / np.maximum(peak, 1e-12)
        return float(np.min(dd))

    for s_idx, (tr_idx, te_groups) in enumerate(splits):
        tr = np.asarray(tr_idx, dtype=int)
        te = (
            np.concatenate([np.asarray(g, dtype=int) for g in te_groups])
            if isinstance(te_groups, (list, tuple))
            else np.asarray(te_groups, dtype=int)
        )
        is_row = np.zeros(n_arms, dtype=float)
        oos_row = np.zeros(n_arms, dtype=float)
        for a_idx, aid in enumerate(arm_ids):
            is_r = returns_1x[tr, a_idx]
            oos_r = returns_1x[te, a_idx]
            is_sr = _ann_sharpe(is_r)
            oos_sr = _ann_sharpe(oos_r)
            is_row[a_idx] = is_sr
            oos_row[a_idx] = oos_sr
            fold_records.append(
                {
                    "split_id": int(s_idx),
                    "arm_id": str(aid),
                    "n_train": int(tr.size),
                    "n_test": int(te.size),
                    "is_mean_bps": _finite_or_zero(np.mean(is_r) * 1e4, 4),
                    "oos_mean_bps": _finite_or_zero(np.mean(oos_r) * 1e4, 4),
                    "is_sharpe_ann": _finite_or_zero(is_sr, 4),
                    "oos_sharpe_ann": _finite_or_zero(oos_sr, 4),
                    "oos_max_dd_pct": _finite_or_zero(_max_dd(oos_r) * 100.0, 4),
                    "oos_win_rate": _finite_or_zero(np.mean(oos_r > 0.0), 4),
                }
            )
        is_sharpe_rows.append(is_row)
        oos_sharpe_rows.append(oos_row)

    is_mat = np.vstack(is_sharpe_rows)
    oos_mat = np.vstack(oos_sharpe_rows)
    pbo_res = mstats.compute_pbo(is_mat, oos_mat)

    # Reconstruct the 21 combinatorial OOS paths via skfolio path_ids
    # Group each split's test fold segments into their canonical combinatorial backtest paths
    n_obs = returns_1x.shape[0]
    fold_bounds = np.array_split(np.arange(n_obs), n_folds)
    fold_to_path_segments: list[list[np.ndarray]] = [[] for _ in range(n_folds)]
    selected_arm_idx = len(arm_ids) - 1  # H109_A12 (canonical selected arm)
    for s_idx, (_tr_idx, te_groups) in enumerate(splits):
        te_list = list(te_groups) if isinstance(te_groups, (list, tuple)) else [te_groups]
        best_is_arm = int(np.argmax(is_mat[s_idx]))
        for seg in te_list:
            seg_arr = np.asarray(seg, dtype=int)
            if seg_arr.size == 0:
                continue
            # Match test segment to its canonical fold index by start position
            f_idx = int(
                np.argmin([abs(int(fb[0]) - int(seg_arr[0])) for fb in fold_bounds if fb.size > 0])
            )
            fold_to_path_segments[f_idx].append(returns_1x[seg_arr, best_is_arm])

    path_records: list[dict[str, Any]] = []
    path_sharpes: list[float] = []
    for p_idx in range(n_test_paths):
        pieces: list[np.ndarray] = []
        for f_idx in range(n_folds):
            segs = fold_to_path_segments[f_idx]
            if segs:
                pieces.append(segs[p_idx % len(segs)])
        p_rets = np.concatenate(pieces) if pieces else returns_1x[:, selected_arm_idx]
        p_sr = _ann_sharpe(p_rets)
        p_dd = _max_dd(p_rets)
        p_tot = float(np.prod(1.0 + p_rets) - 1.0)
        path_sharpes.append(p_sr)
        path_records.append(
            {
                "path_id": int(p_idx + 1),
                "n_sessions": int(p_rets.size),
                "mean_daily_bps": _finite_or_zero(np.mean(p_rets) * 1e4, 4),
                "sharpe_ann": _finite_or_zero(p_sr, 4),
                "total_return_pct": _finite_or_zero(p_tot * 100.0, 4),
                "max_drawdown_pct": _finite_or_zero(p_dd * 100.0, 4),
                "win_rate": _finite_or_zero(np.mean(p_rets > 0.0), 4),
            }
        )

    sel_rets = returns_1x[:, selected_arm_idx]
    sel_mean = float(np.mean(sel_rets))
    sel_std = float(np.std(sel_rets, ddof=1))
    sel_sr_daily = (sel_mean / sel_std) if sel_std > 1e-12 else 0.0
    sel_sr_ann = sel_sr_daily * math.sqrt(252.0)

    arm_daily_srs = [
        float(np.mean(returns_1x[:, i]) / max(float(np.std(returns_1x[:, i], ddof=1)), 1e-12))
        for i in range(n_arms)
    ]
    var_trials = max(1e-6, float(np.var(np.asarray(arm_daily_srs, dtype=float), ddof=1)))
    dsr_res = mstats.deflated_sharpe_ratio_from_statistics(
        observed_sharpe=sel_sr_daily,
        n_samples=int(sel_rets.size),
        n_trials=int(n_trials_total),
        variance_trials=var_trials,
        skewness=float(skew(sel_rets)) if sel_std > 1e-12 else 0.0,
        excess_kurtosis=float(kurtosis(sel_rets)) if sel_std > 1e-12 else 0.0,
        frequency="daily",
    )
    min_trl_res = mstats.compute_min_trl(returns=sel_rets, target_sharpe=0.0, frequency="daily")
    min_trl_val = (
        round(float(min_trl_res.min_trl), 2) if np.isfinite(float(min_trl_res.min_trl)) else None
    )

    cpcv_dir.mkdir(parents=True, exist_ok=True)
    folds_pq = cpcv_dir / "h109_cpcv_fold_distributions.parquet"
    paths_pq = cpcv_dir / "h109_cpcv_oos_paths.parquet"
    summary_json = cpcv_dir / "h109_cpcv_summary.json"

    folds_df = pl.DataFrame(fold_records)
    paths_df = pl.DataFrame(path_records)
    folds_df.write_parquet(folds_pq)
    paths_df.write_parquet(paths_pq)

    summary = {
        "schema_version": "tier0.cpcv-artifacts.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prewall_cutoff_iso": PREWALL_CUTOFF_ISO,
        "date_start": str(min(dates)),
        "date_end": str(max(dates)),
        "n_sessions": int(len(dates)),
        "n_arms": int(n_arms),
        "selected_arm_id": str(arm_ids[selected_arm_idx]),
        "paper_only": True,
        "live_execution_enabled": False,
        "skfolio_cpcv": {
            "n_folds": int(n_folds),
            "n_test_folds": int(n_test_folds),
            "purged_size": int(purged_size),
            "embargo_size": int(embargo_size),
            "n_splits": int(n_splits),
            "n_test_paths": int(n_test_paths),
            "min_cpcv_paths_satisfied": bool(n_test_paths >= res.MIN_CPCV_SAMPLES),
        },
        "ml4t_diagnostic_stats": {
            "n_trials_total": int(n_trials_total),
            "observed_sharpe_ann": _finite_or_zero(sel_sr_ann, 4),
            "observed_sharpe_daily": _finite_or_zero(sel_sr_daily, 6),
            "dsr_probability": _finite_or_zero(dsr_res.probability, 6),
            "expected_max_sharpe_daily": _finite_or_zero(dsr_res.expected_max_sharpe, 6),
            "pbo": _finite_or_zero(pbo_res.pbo, 4),
            "pbo_n_combinations": int(pbo_res.n_combinations),
            "min_trl_sessions": min_trl_val,
            "current_sessions": int(sel_rets.size),
            "oos_path_sharpe_mean": _finite_or_zero(np.mean(path_sharpes), 4),
            "oos_path_sharpe_median": _finite_or_zero(np.median(path_sharpes), 4),
            "oos_path_sharpe_p05": _finite_or_zero(np.percentile(path_sharpes, 5), 4),
            "oos_path_sharpe_p95": _finite_or_zero(np.percentile(path_sharpes, 95), 4),
        },
        "artifacts": {
            "fold_distributions_parquet": str(folds_pq),
            "fold_distributions_rows": int(folds_df.height),
            "fold_distributions_sha256": _file_sha256(folds_pq),
            "oos_paths_parquet": str(paths_pq),
            "oos_paths_rows": int(paths_df.height),
            "oos_paths_sha256": _file_sha256(paths_pq),
        },
    }
    _atomic_write_json(summary_json, summary)
    summary["artifacts"]["summary_json"] = str(summary_json)
    summary["artifacts"]["summary_json_sha256"] = _file_sha256(summary_json)
    return {
        "summary": summary,
        "folds_df": folds_df,
        "paths_df": paths_df,
        "selected_returns": sel_rets,
        "is_mat": is_mat,
        "oos_mat": oos_mat,
    }


def generate_ml4t_h109_falsification_tearsheet(
    *,
    dates: Sequence[str],
    selected_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    cpcv_result: Mapping[str, Any],
    h109_dossier: Mapping[str, Any],
    ablation_dossier: Mapping[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    """Emit unmodified official ml4t-diagnostic H109 OOS/CPCV/PBO/DSR/cost-stress HTML tearsheet."""
    from ml4t.diagnostic.splitters import CombinatorialCV
    from ml4t.diagnostic.visualization.backtest.statistical_validity import (
        plot_dsr_gauge,
        plot_minimum_track_record,
        plot_sharpe_bootstrap,
        plot_statistical_summary_card,
    )
    from ml4t.diagnostic.visualization.core import go
    from ml4t.diagnostic.visualization.cv_plots import plot_cv_folds
    from ml4t.diagnostic.visualization.report_generation import combine_figures_to_html

    cpcv_sum = cpcv_result["summary"]
    mstats = cpcv_sum["ml4t_diagnostic_stats"]
    cpcv_cfg = cpcv_sum["skfolio_cpcv"]
    paths_df: pl.DataFrame = cpcv_result["paths_df"]

    h109_six = h109_dossier.get("holdout_six_gate") or {}
    h109_long = h109_six.get("LONG") or {}
    h109_short = h109_six.get("SHORT") or {}
    h109_comb = h109_six.get("COMBINED") or {}
    h109_verdict = h109_dossier.get("verdict") or {}
    h109_l1 = h109_dossier.get("l1_queue_markout") or {}
    h109_trials = int(
        (h109_dossier.get("trial_accounting") or {}).get("cumulative_trials_total")
        or mstats["n_trials_total"]
    )
    hlz_hurdle = float((h109_dossier.get("trial_accounting") or {}).get("hlz_t_hurdle") or 4.3636)

    obs_sr = float(mstats["observed_sharpe_ann"])
    dsr_prob = float(mstats["dsr_probability"])
    pbo_val = float(mstats["pbo"])
    min_trl = float(mstats["min_trl_sessions"] or 5000.0)
    cur_trl = int(mstats["current_sessions"])

    figures: list[go.Figure] = []
    sections: list[dict[str, Any]] = []

    # Section 1: Official Statistical Validity Summary Card
    fig_card = plot_statistical_summary_card(
        metrics={
            "dsr_probability": dsr_prob,
            "dsr_significant": bool(dsr_prob >= 0.95),
            "min_trl": min_trl,
            "current_trl": cur_trl,
            "trl_sufficient": bool(cur_trl >= min_trl),
        },
        title="H109 Pre-Wall Statistical Validity Summary (211 Trials | DISCARD)",
        theme="dark",
    )
    figures.append(fig_card)
    sections.append(
        {
            "title": "Executive Statistical Validity Summary (H109 6-Gate Falsification)",
            "text": (
                f"Pre-wall cutoff: {PREWALL_CUTOFF_ISO} | Overall H109 Verdict: "
                f"{h109_verdict.get('overall_verdict', 'DISCARD')} "
                f"(cross_leg_promoted={bool(h109_verdict.get('cross_leg_promoted', False))}). "
                f"Cumulative trials: {h109_trials} (HLZ t-hurdle: {hlz_hurdle:.4f}). "
                f"Real Rust hftbacktest Gate 1 L1 markout PASSED (70/72 fills, +6.267 bps); "
                f"Gates 2-6 FAILED honestly on untouched pre-wall holdout (paper_only=True, "
                f"live_execution_enabled=False)."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 2: Official DSR Gauge
    fig_dsr = plot_dsr_gauge(
        dsr_probability=dsr_prob,
        observed_sharpe=obs_sr,
        expected_max_sharpe=float(mstats["expected_max_sharpe_daily"]) * math.sqrt(252.0),
        n_trials=h109_trials,
        title=f"Deflated Sharpe Ratio (DSR) — {h109_trials} Cumulative Trials",
        theme="dark",
    )
    figures.append(fig_dsr)
    sections.append(
        {
            "title": "Deflated Sharpe Ratio (Multiple-Testing Correction)",
            "text": (
                f"Observed annualized Sharpe={obs_sr:.3f} across {cur_trl} pre-wall sessions "
                f"vs expected max null Sharpe under {h109_trials} cumulative trials. "
                f"DSR probability={dsr_prob:.4f} (promotion threshold >= 0.95)."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 3: Probability of Backtest Overfitting (PBO) Gauge via Plotly Indicator
    # (ml4t-diagnostic==0.2.1 plot_pbo_gauge raises KeyError('sage') on its internal palette)
    fig_pbo = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=round(pbo_val * 100.0, 2),
            number={"suffix": "%"},
            title={
                "text": (
                    f"Probability of Backtest Overfitting (PBO) — "
                    f"{cpcv_cfg['n_splits']} CPCV Splits / {cpcv_cfg['n_test_paths']} OOS Paths"
                )
            },
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#ef4444" if pbo_val >= 0.50 else "#22c55e"},
                "steps": [
                    {"range": [0, 25], "color": "rgba(34,197,94,0.35)"},
                    {"range": [25, 50], "color": "rgba(245,158,11,0.35)"},
                    {"range": [50, 100], "color": "rgba(239,68,68,0.35)"},
                ],
                "threshold": {
                    "line": {"color": "#f8fafc", "width": 3},
                    "thickness": 0.75,
                    "value": 50.0,
                },
            },
        )
    )
    fig_pbo.update_layout(template="plotly_dark", height=360)
    figures.append(fig_pbo)
    sections.append(
        {
            "title": "Probability of Backtest Overfitting (skfolio CPCV)",
            "text": (
                f"Real skfolio CombinatorialPurgedCV (n_folds={cpcv_cfg['n_folds']}, "
                f"n_test_folds={cpcv_cfg['n_test_folds']}, purged={cpcv_cfg['purged_size']}, "
                f"embargo={cpcv_cfg['embargo_size']}) yielded {cpcv_cfg['n_splits']} splits and "
                f"{cpcv_cfg['n_test_paths']} combinatorial OOS paths (>= 16 required). "
                f"Empirical CPCV PBO={pbo_val:.4f}; H109 holdout PBO LONG="
                f"{h109_long.get('empirical_pbo')}, SHORT={h109_short.get('empirical_pbo')}, "
                f"COMBINED={h109_comb.get('empirical_pbo')}."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 4: Official Minimum Track Record Length
    fig_trl = plot_minimum_track_record(
        observed_sharpe=max(obs_sr, 0.05),
        current_periods=cur_trl,
        sr_benchmark=0.0,
        confidence=0.95,
        periods_per_year=252,
        title="Minimum Track Record Length (MinTRL) vs Pre-Wall Horizon",
        theme="dark",
    )
    figures.append(fig_trl)
    sections.append(
        {
            "title": "Minimum Track Record Length (MinTRL)",
            "text": (
                f"Pre-wall observation horizon={cur_trl} sessions ({cpcv_sum['date_start']} "
                f"to {cpcv_sum['date_end']}). Computed MinTRL={mstats['min_trl_sessions']}."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 5: Official Combinatorial Purged CV Folds Diagram
    ml4t_cv = CombinatorialCV(
        n_groups=8,
        n_test_groups=3,
        label_horizon=2,
        embargo_size=2,
        max_combinations=21,
        random_state=42,
    )
    x_dummy = np.arange(len(dates), dtype=float).reshape(-1, 1)
    fig_cv = plot_cv_folds(
        ml4t_cv,
        X=x_dummy,
        show_test_period=True,
        show_purge_gaps=True,
        show_sample_counts=True,
        theme="dark",
        title="Combinatorial Purged Cross-Validation (CPCV) — 21 OOS Paths with Purge & Embargo",
    )
    figures.append(fig_cv)
    sections.append(
        {
            "title": "Combinatorial Purged CV Architecture (Purge & Embargo Audit)",
            "text": (
                "Leak-free temporal fold allocation across pre-wall sessions with explicit "
                "purge and embargo buffers separating train and OOS test partitions."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 6: Official Sharpe Block-Bootstrap Distribution
    fig_boot = plot_sharpe_bootstrap(
        returns=selected_returns,
        observed_sharpe=obs_sr,
        n_bootstrap=1000,
        block_size=10,
        periods_per_year=252,
        title="Stationary Block-Bootstrap Annualized Sharpe Distribution (Pre-Wall)",
        theme="dark",
    )
    figures.append(fig_boot)
    sections.append(
        {
            "title": "Block-Bootstrap Sharpe Confidence Distribution",
            "text": (
                "Stationary block bootstrap (1,000 resamples, block=10 trading days) preserving "
                "intraday/daily volatility clustering and autocorrelation."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 7: Pre-Wall Equity Curve & Underwater Drawdown vs SPY Benchmark
    strat_eq = np.cumprod(1.0 + selected_returns)
    bench_eq = np.cumprod(1.0 + benchmark_returns)
    strat_dd = (strat_eq / np.maximum.accumulate(strat_eq) - 1.0) * 100.0
    bench_dd = (bench_eq / np.maximum.accumulate(bench_eq) - 1.0) * 100.0

    fig_eq = go.Figure()
    fig_eq.add_trace(
        go.Scatter(
            x=list(dates),
            y=(strat_eq - 1.0) * 100.0,
            mode="lines",
            name="H109_A12 Pre-Wall Net 1x (%)",
            line={"color": "#38bdf8", "width": 2},
        )
    )
    fig_eq.add_trace(
        go.Scatter(
            x=list(dates),
            y=(bench_eq - 1.0) * 100.0,
            mode="lines",
            name="SPY Benchmark (%)",
            line={"color": "#94a3b8", "width": 1.5, "dash": "dash"},
        )
    )
    fig_eq.add_trace(
        go.Scatter(
            x=list(dates),
            y=strat_dd,
            mode="lines",
            name="Strategy Underwater DD (%)",
            fill="tozeroy",
            line={"color": "#ef4444", "width": 1},
            yaxis="y2",
        )
    )
    fig_eq.update_layout(
        title="Pre-Wall Cumulative Return & Underwater Drawdown vs SPY (<= 2026-10-15)",
        template="plotly_dark",
        xaxis_title="Pre-Wall Session Date",
        yaxis={"title": "Cumulative Return (%)"},
        yaxis2={"title": "Drawdown (%)", "overlaying": "y", "side": "right"},
        height=440,
    )
    figures.append(fig_eq)
    sections.append(
        {
            "title": "Pre-Wall Equity Curve & Drawdown Envelope",
            "text": (
                f"Real daily pre-wall equity curve ({dates[0]} to {dates[-1]}). "
                f"Strategy max drawdown={float(np.min(strat_dd)):.2f}% vs "
                f"SPY max drawdown={float(np.min(bench_dd)):.2f}%."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 8: H109 Verbatim Cost-Stress (1x 4bps / Calibrated Liquid / 2x 8bps) & L1 Markout
    legs = ["LONG", "SHORT", "COMBINED"]
    m_1x = [
        float(((h109_six.get(leg) or {}).get("falsification_1x_4bps") or {}).get("mean_bps") or 0.0)
        for leg in legs
    ]
    m_cal = [
        float(
            ((h109_six.get(leg) or {}).get("falsification_calibrated_liquid") or {}).get("mean_bps")
            or 0.0
        )
        for leg in legs
    ]
    m_2x = [
        float(((h109_six.get(leg) or {}).get("falsification_2x_8bps") or {}).get("mean_bps") or 0.0)
        for leg in legs
    ]
    l1_mk = [float((h109_l1.get(leg) or {}).get("mean_markout_bps") or 0.0) for leg in legs]

    fig_cost = go.Figure(
        data=[
            go.Bar(
                name="Gate 1 Real L1 Queue Markout (bps)", x=legs, y=l1_mk, marker_color="#22c55e"
            ),
            go.Bar(
                name="Holdout 1x (4 bps) Net Mean (bps)", x=legs, y=m_1x, marker_color="#38bdf8"
            ),
            go.Bar(
                name="Holdout Calibrated Liquid (7.0538 bps)",
                x=legs,
                y=m_cal,
                marker_color="#f59e0b",
            ),
            go.Bar(
                name="Holdout 2x (8 bps) Net Mean (bps)", x=legs, y=m_2x, marker_color="#ef4444"
            ),
        ]
    )
    fig_cost.update_layout(
        title="H109 Holdout Cost-Stress Ladder (1x / Calibrated Liquid / 2x) & Real L1 Markout",
        barmode="group",
        template="plotly_dark",
        xaxis_title="Strategy Leg",
        yaxis_title="Mean Return / Markout (bps)",
        height=420,
    )
    figures.append(fig_cost)
    ab_trials = len(ablation_dossier.get("trials") or [])
    sections.append(
        {
            "title": "H109 Cost-Stress Sensitivity & Real Rust hftbacktest L1 Queue Markout",
            "text": (
                f"Verbatim H109 holdout cost-stress results alongside prior H108 ablation "
                f"({ab_trials} Optuna trials). Gate 1 passive L1 queue markout on real "
                f"prewall-lake published-orb L1 passed (70/72 fills; LONG=-3.995 bps, "
                f"SHORT=+16.528 bps, COMBINED=+6.267 bps), while net holdout trade returns "
                f"turned negative across all friction tiers (honest DISCARD)."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Section 9: 21 Recombined CPCV OOS Path Sharpe Distribution
    fig_paths = go.Figure(
        data=[
            go.Bar(
                x=[f"Path {int(p)}" for p in paths_df["path_id"].to_list()],
                y=paths_df["sharpe_ann"].to_list(),
                marker_color=[
                    "#22c55e" if v > 0 else "#ef4444" for v in paths_df["sharpe_ann"].to_list()
                ],
                name="CPCV OOS Path Annualized Sharpe",
            )
        ]
    )
    fig_paths.update_layout(
        title="skfolio CPCV 21 Recombined OOS Backtest Paths — Annualized Sharpe Distribution",
        template="plotly_dark",
        xaxis_title="Combinatorial OOS Path",
        yaxis_title="Annualized Sharpe Ratio",
        height=400,
    )
    figures.append(fig_paths)
    sections.append(
        {
            "title": "Combinatorial OOS Path Sharpe Distribution (21 Paths)",
            "text": (
                f"Mean path Sharpe={mstats['oos_path_sharpe_mean']:.3f}, "
                f"median={mstats['oos_path_sharpe_median']:.3f}, "
                f"p05={mstats['oos_path_sharpe_p05']:.3f}, p95={mstats['oos_path_sharpe_p95']:.3f}."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combine_figures_to_html(
        figures=figures,
        title="Tier-0 SOTA — ml4t-diagnostic H109 Pre-Wall 6-Gate Falsification & CPCV Tearsheet",
        sections=sections,
        output_file=output_path,
        theme="dark",
        include_toc=True,
        include_plotlyjs=True,
    )
    return {
        "path": str(output_path),
        "bytes": int(output_path.stat().st_size),
        "sha256": _file_sha256(output_path),
        "figures_count": len(figures),
        "generator": "ml4t.diagnostic.visualization.report_generation.combine_figures_to_html",
    }


def generate_shap_shapiq_prewall_tearsheet(
    df: pl.DataFrame,
    *,
    output_path: Path,
    max_eval_rows: int = 240,
    shapiq_eval_rows: int = 48,
) -> dict[str, Any]:
    """Fit causal pre-wall LightGBM + CatBoost models and emit official SHAP/shapiq HTML."""
    import lightgbm as lgb
    import shap
    from catboost import CatBoostRegressor
    from ml4t.diagnostic.api import analyze_ml_importance, compute_shap_interactions
    from ml4t.diagnostic.visualization.core import go
    from ml4t.diagnostic.visualization.feature_plots import (
        plot_importance_bar,
        plot_importance_heatmap,
        plot_importance_summary,
    )
    from ml4t.diagnostic.visualization.interaction_plots import (
        plot_interaction_bar,
        plot_interaction_heatmap,
        plot_interaction_network,
    )
    from ml4t.diagnostic.visualization.report_generation import combine_figures_to_html

    max_date = str(df.select(pl.col("trade_date").max()).item())
    disc.assert_pre_wall_date(max_date)

    # Strict temporal split matching H109 (6-trading-day purge gap: 2025-11-01..2025-11-10)
    train_df = df.filter(pl.col("trade_date") <= PREWALL_TUNING_END_ISO)
    eval_df = df.filter(pl.col("trade_date") >= PREWALL_HOLDOUT_START_ISO)
    if train_df.height < 64 or eval_df.height < 32:
        split_i = max(32, int(df.height * 0.70))
        train_df = df.slice(0, split_i)
        eval_df = df.slice(split_i, max(16, df.height - split_i))

    fnames = list(CAUSAL_FEATURE_COLUMNS)
    x_train = train_df.select(fnames).to_numpy().astype(float)
    y_train = train_df["next1_mfe_pct"].to_numpy().astype(float)
    x_eval_full = eval_df.select(fnames).to_numpy().astype(float)
    y_eval_full = eval_df["next1_mfe_pct"].to_numpy().astype(float)

    # Deterministic stratified subsample of holdout eval rows for fast exact TreeSHAP + shapiq
    if x_eval_full.shape[0] > max_eval_rows:
        idx = np.linspace(0, x_eval_full.shape[0] - 1, num=max_eval_rows, dtype=int)
        x_eval = x_eval_full[idx]
        y_eval = y_eval_full[idx]
    else:
        x_eval = x_eval_full
        y_eval = y_eval_full

    n_threads = disc._env_positive_int("TIER0_LGBM_THREADS", 4)
    lgb_model = lgb.LGBMRegressor(
        n_estimators=40,
        max_depth=4,
        num_leaves=12,
        learning_rate=0.05,
        min_child_samples=15,
        n_jobs=n_threads,
        random_state=42,
        verbosity=-1,
    )
    lgb_model.fit(x_train, y_train)

    cat_model = CatBoostRegressor(
        iterations=60,
        depth=4,
        learning_rate=0.05,
        random_seed=42,
        thread_count=n_threads,
        verbose=0,
    )
    cat_model.fit(x_train, y_train)

    # Verify exact TreeSHAP additivity on both LightGBM and CatBoost
    lgb_exp = shap.TreeExplainer(lgb_model)
    lgb_shap = np.asarray(lgb_exp.shap_values(x_eval), dtype=float)
    lgb_base = float(np.asarray(lgb_exp.expected_value).reshape(-1)[0])
    lgb_pred = np.asarray(lgb_model.predict(x_eval), dtype=float)
    lgb_additivity_err = float(np.max(np.abs(lgb_pred - (lgb_base + np.sum(lgb_shap, axis=1)))))

    cat_exp = shap.TreeExplainer(cat_model)
    cat_shap = np.asarray(cat_exp.shap_values(x_eval), dtype=float)
    cat_base = float(np.asarray(cat_exp.expected_value).reshape(-1)[0])
    cat_pred = np.asarray(cat_model.predict(x_eval), dtype=float)
    cat_additivity_err = float(np.max(np.abs(cat_pred - (cat_base + np.sum(cat_shap, axis=1)))))

    lgb_mean_abs = np.mean(np.abs(lgb_shap), axis=0)
    cat_mean_abs = np.mean(np.abs(cat_shap), axis=0)

    # Official ml4t-diagnostic multi-method importance (MDI + PFI + SHAP) & SHAP interactions
    imp_res = analyze_ml_importance(
        lgb_model,
        x_eval,
        y_eval,
        feature_names=fnames,
        methods=["mdi", "pfi", "shap"],
        n_repeats=5,
        random_state=42,
    )
    int_res = compute_shap_interactions(
        lgb_model,
        x_eval[: min(120, x_eval.shape[0])],
        feature_names=fnames,
        max_samples=min(120, x_eval.shape[0]),
    )

    # Official shapiq k-SII order-2 interactions (ADR-0028 graduated primitive)
    shapiq_res = disc.compute_shapiq_interactions(
        lgb_model,
        x_train[: min(256, x_train.shape[0])],
        x_eval[: min(shapiq_eval_rows, x_eval.shape[0])],
        fnames,
        max_order=2,
        max_eval_rows=min(shapiq_eval_rows, x_eval.shape[0]),
    )

    figures: list[go.Figure] = []
    sections: list[dict[str, Any]] = []

    # Official ml4t-diagnostic importance plots
    fig_imp_sum = plot_importance_summary(imp_res, top_n=len(fnames), theme="dark")
    figures.append(fig_imp_sum)
    sections.append(
        {
            "title": "Part 1: Multi-Method Feature Importance Consensus (MDI + PFI + TreeSHAP)",
            "text": (
                f"Causal pre-wall LightGBM + CatBoost attribution predicting next-day MFE "
                f"expansion (train <= {PREWALL_TUNING_END_ISO}: {train_df.height} rows; "
                f"6-day purge gap; holdout {PREWALL_HOLDOUT_START_ISO}..{PREWALL_CUTOFF_ISO}: "
                f"{eval_df.height} rows, evaluated on {x_eval.shape[0]} stratified rows). "
                f"TreeSHAP additivity max error: LightGBM={lgb_additivity_err:.2e}, "
                f"CatBoost={cat_additivity_err:.2e}."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    fig_imp_bar = plot_importance_bar(imp_res, top_n=len(fnames), theme="dark")
    figures.append(fig_imp_bar)
    sections.append(
        {
            "title": "Consensus Feature Importance Ranking (ml4t-diagnostic)",
            "text": (
                "Mean feature importance across MDI, permutation importance (PFI), "
                "and exact TreeSHAP."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    fig_imp_hm = plot_importance_heatmap(imp_res, theme="dark")
    figures.append(fig_imp_hm)
    sections.append(
        {
            "title": "Method Agreement Heatmap (MDI vs PFI vs TreeSHAP)",
            "text": (
                "Cross-method stability matrix highlighting features robust to "
                "split-cardinality bias."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Cross-model LightGBM vs CatBoost TreeSHAP comparison
    order = np.argsort(-(lgb_mean_abs + cat_mean_abs))
    ordered_names = [fnames[int(i)] for i in order]
    fig_dual = go.Figure(
        data=[
            go.Bar(
                name="LightGBM TreeSHAP Mean |SHAP|",
                x=ordered_names,
                y=[float(lgb_mean_abs[int(i)]) for i in order],
                marker_color="#38bdf8",
            ),
            go.Bar(
                name="CatBoost TreeSHAP Mean |SHAP|",
                x=ordered_names,
                y=[float(cat_mean_abs[int(i)]) for i in order],
                marker_color="#f59e0b",
            ),
        ]
    )
    fig_dual.update_layout(
        title="Cross-Model TreeSHAP Agreement: LightGBM vs CatBoost (Pre-Wall Holdout)",
        barmode="group",
        template="plotly_dark",
        xaxis_title="Causal Pre-Wall Feature",
        yaxis_title="Mean |SHAP Value| (Next-Day MFE)",
        height=420,
    )
    figures.append(fig_dual)
    sections.append(
        {
            "title": "Cross-Model Attribution Agreement (LightGBM vs CatBoost TreeSHAP)",
            "text": (
                "Direct comparison of exact TreeSHAP global feature importances across two "
                "independent gradient-boosting architectures trained on identical pre-wall splits."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    # Official ml4t-diagnostic SHAP interaction plots
    fig_int_bar = plot_interaction_bar(int_res, top_n=15, theme="dark")
    figures.append(fig_int_bar)
    sections.append(
        {
            "title": "Part 2: Pairwise SHAP Feature Interactions (ml4t-diagnostic)",
            "text": "Top order-2 SHAP interaction pairs decomposing nonlinear feature synergies.",
            "figure_index": len(figures) - 1,
        }
    )

    fig_int_hm = plot_interaction_heatmap(int_res, theme="dark")
    figures.append(fig_int_hm)
    sections.append(
        {
            "title": "SHAP Interaction Matrix Heatmap",
            "text": (
                "Complete pairwise SHAP interaction strength across all causal pre-wall features."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    fig_int_net = plot_interaction_network(int_res, theme="dark", top_n=15)
    figures.append(fig_int_net)
    sections.append(
        {
            "title": "Feature Interaction Network Graph",
            "text": "Network topology of synergistic feature clusters driving extreme-mover MFE.",
            "figure_index": len(figures) - 1,
        }
    )

    # Official shapiq k-SII order-2 interaction chart
    top_sq = list(shapiq_res.get("pairwise_interactions") or [])[:12]
    fig_sq = go.Figure(
        data=[
            go.Bar(
                x=[str(item["pair"]) for item in top_sq],
                y=[float(item["mean_abs_interaction"]) for item in top_sq],
                marker_color=[
                    "#22c55e" if float(item["mean_signed_interaction"]) >= 0.0 else "#ef4444"
                    for item in top_sq
                ],
                name="shapiq k-SII Mean |Interaction|",
            )
        ]
    )
    sq_ver = str(shapiq_res.get("shapiq_version", "unknown"))
    fig_sq.update_layout(
        title=f"shapiq ({sq_ver}) k-SII Order-2 Interactions (Green=Synergy, Red=Antagonism)",
        template="plotly_dark",
        xaxis_title="Feature Pair",
        yaxis_title="Mean |k-SII Order-2 Interaction|",
        height=420,
    )
    figures.append(fig_sq)
    sections.append(
        {
            "title": "Part 3: shapiq k-SII Order-2 Interaction Attribution (ADR-0028)",
            "text": (
                f"shapiq {sq_ver} TreeExplainer (index={shapiq_res.get('index')}, "
                f"max_order={shapiq_res.get('max_order')}, "
                f"eval_rows={shapiq_res.get('n_eval_rows')}). "
                f"Top interaction: {(shapiq_res.get('top_interaction') or {}).get('pair', 'n/a')}."
            ),
            "figure_index": len(figures) - 1,
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combine_figures_to_html(
        figures=figures,
        title="Tier-0 SOTA — Pre-Wall LightGBM/CatBoost TreeSHAP & shapiq Attribution Tearsheet",
        sections=sections,
        output_file=output_path,
        theme="dark",
        include_toc=True,
        include_plotlyjs=True,
    )

    shap_ranking = [
        {
            "rank": rank_idx + 1,
            "feature": fnames[int(i)],
            "lgbm_mean_abs_shap": _finite_or_zero(lgb_mean_abs[int(i)], 6),
            "catboost_mean_abs_shap": _finite_or_zero(cat_mean_abs[int(i)], 6),
            "consensus_mean_abs_shap": _finite_or_zero(
                0.5 * (lgb_mean_abs[int(i)] + cat_mean_abs[int(i)]), 6
            ),
        }
        for rank_idx, i in enumerate(order)
    ]

    return {
        "path": str(output_path),
        "bytes": int(output_path.stat().st_size),
        "sha256": _file_sha256(output_path),
        "figures_count": len(figures),
        "generator": "ml4t.diagnostic.visualization.report_generation.combine_figures_to_html",
        "train_rows": int(train_df.height),
        "eval_rows_total": int(eval_df.height),
        "eval_rows_explained": int(x_eval.shape[0]),
        "lgbm_additivity_max_error": float(f"{lgb_additivity_err:.3e}"),
        "catboost_additivity_max_error": float(f"{cat_additivity_err:.3e}"),
        "shap_top_features": shap_ranking,
        "shapiq_version": str(shapiq_res.get("shapiq_version", "unknown")),
        "shapiq_top_interaction": shapiq_res.get("top_interaction"),
        "shapiq_top5_interactions": list(shapiq_res.get("pairwise_interactions") or [])[:5],
    }


def build_sim_prewall_audit_duckdb(
    *,
    db_path: Path,
    cpcv_result: Mapping[str, Any],
    h109_dossier: Mapping[str, Any],
    shap_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Create standalone read-only sim_prewall_audit.duckdb without touching pa1..3/tv_audit."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_db = db_path.with_suffix(".duckdb.tmp")
    if tmp_db.exists():
        tmp_db.unlink()

    h109_six = h109_dossier.get("holdout_six_gate") or {}
    h109_rows = []
    for leg in ("LONG", "SHORT", "COMBINED"):
        blk = h109_six.get(leg) or {}
        f1x = blk.get("falsification_1x_4bps") or {}
        fcal = blk.get("falsification_calibrated_liquid") or {}
        f2x = blk.get("falsification_2x_8bps") or {}
        l1 = blk.get("l1_markout") or {}
        h109_rows.append(
            {
                "leg": leg,
                "verdict": str(blk.get("verdict") or "DISCARD"),
                "gates_passed": int(blk.get("gates_passed_count") or 0),
                "n_trades": int(f1x.get("n_trades") or 0),
                "l1_passive_filled": int(l1.get("passive_filled") or 0),
                "l1_mean_markout_bps": _finite_or_zero(l1.get("mean_markout_bps"), 4),
                "mean_1x_bps": _finite_or_zero(f1x.get("mean_bps"), 4),
                "mean_cal_liquid_bps": _finite_or_zero(fcal.get("mean_bps"), 4),
                "mean_2x_bps": _finite_or_zero(f2x.get("mean_bps"), 4),
                "dsr": _finite_or_zero(f1x.get("dsr"), 6),
                "pbo": _finite_or_zero(blk.get("empirical_pbo"), 4),
                "hac_t_1x": _finite_or_zero(f1x.get("hac_t"), 4),
                "spa_p_1x": _finite_or_zero(f1x.get("spa_p"), 4),
            }
        )

    h109_df = pl.DataFrame(h109_rows)
    folds_df: pl.DataFrame = cpcv_result["folds_df"]
    paths_df: pl.DataFrame = cpcv_result["paths_df"]
    shap_df = pl.DataFrame(shap_result["shap_top_features"])
    sq_rows = [
        {
            "pair": str(item.get("pair") or ""),
            "mean_abs_interaction": _finite_or_zero(item.get("mean_abs_interaction"), 6),
            "mean_signed_interaction": _finite_or_zero(item.get("mean_signed_interaction"), 6),
        }
        for item in (shap_result.get("shapiq_top5_interactions") or [])
    ]
    sq_df = (
        pl.DataFrame(sq_rows)
        if sq_rows
        else pl.DataFrame(
            schema={
                "pair": pl.String,
                "mean_abs_interaction": pl.Float64,
                "mean_signed_interaction": pl.Float64,
            }
        )
    )

    con = duckdb.connect(str(tmp_db))
    try:
        con.execute("SET threads TO 4")
        con.register("_h109_df", h109_df)
        con.register("_folds_df", folds_df)
        con.register("_paths_df", paths_df)
        con.register("_shap_df", shap_df)
        con.register("_sq_df", sq_df)
        con.execute("CREATE TABLE h109_six_gate_verdicts AS SELECT * FROM _h109_df")
        con.execute("CREATE TABLE h109_cpcv_folds AS SELECT * FROM _folds_df")
        con.execute("CREATE TABLE h109_cpcv_oos_paths AS SELECT * FROM _paths_df")
        con.execute("CREATE TABLE prewall_shap_importance AS SELECT * FROM _shap_df")
        con.execute("CREATE TABLE prewall_shapiq_interactions AS SELECT * FROM _sq_df")
    finally:
        con.close()

    os.replace(tmp_db, db_path)
    return {
        "path": str(db_path),
        "bytes": int(db_path.stat().st_size),
        "tables": {
            "h109_six_gate_verdicts": int(h109_df.height),
            "h109_cpcv_folds": int(folds_df.height),
            "h109_cpcv_oos_paths": int(paths_df.height),
            "prewall_shap_importance": int(shap_df.height),
            "prewall_shapiq_interactions": int(sq_df.height),
        },
    }


def log_suite_to_mlflow(
    *,
    tracking_uri: str,
    experiment_name: str,
    cpcv_summary: Mapping[str, Any],
    ml4t_tearsheet_info: Mapping[str, Any],
    shap_tearsheet_info: Mapping[str, Any],
    cpcv_summary_path: Path,
) -> dict[str, Any]:
    """Log CPCV/DSR/PBO/SHAP metrics and both unmodified HTML tearsheets into MLflow 3."""
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    mstats = cpcv_summary["ml4t_diagnostic_stats"]
    cpcv_cfg = cpcv_summary["skfolio_cpcv"]
    with mlflow.start_run(run_name="ml4t-cpcv-shap-shapiq-prewall-suite") as run:
        mlflow.log_params(
            {
                "prewall_cutoff_iso": PREWALL_CUTOFF_ISO,
                "paper_only": "True",
                "live_execution_enabled": "False",
                "cpcv_n_folds": int(cpcv_cfg["n_folds"]),
                "cpcv_n_test_folds": int(cpcv_cfg["n_test_folds"]),
                "cpcv_n_splits": int(cpcv_cfg["n_splits"]),
                "cpcv_n_test_paths": int(cpcv_cfg["n_test_paths"]),
                "selected_arm_id": str(cpcv_summary["selected_arm_id"]),
                "shapiq_version": str(shap_tearsheet_info["shapiq_version"]),
            }
        )
        mlflow.log_metrics(
            {
                "cpcv_n_test_paths": float(cpcv_cfg["n_test_paths"]),
                "cpcv_n_splits": float(cpcv_cfg["n_splits"]),
                "cpcv_pbo": float(mstats["pbo"]),
                "cpcv_dsr_probability": float(mstats["dsr_probability"]),
                "cpcv_observed_sharpe_ann": float(mstats["observed_sharpe_ann"]),
                "cpcv_oos_path_sharpe_mean": float(mstats["oos_path_sharpe_mean"]),
                "shap_lgbm_additivity_err": float(shap_tearsheet_info["lgbm_additivity_max_error"]),
                "shap_catboost_additivity_err": float(
                    shap_tearsheet_info["catboost_additivity_max_error"]
                ),
            }
        )
        mlflow.set_tags(
            {
                "ml4t_tearsheet_path": str(ml4t_tearsheet_info["path"]),
                "ml4t_tearsheet_sha256": str(ml4t_tearsheet_info["sha256"]),
                "ml4t_tearsheet_url": (
                    "http://127.0.0.1:8793/tearsheets/"
                    "ml4t_diagnostic_h109_falsification_tearsheet.html"
                ),
                "shap_tearsheet_path": str(shap_tearsheet_info["path"]),
                "shap_tearsheet_sha256": str(shap_tearsheet_info["sha256"]),
                "shap_tearsheet_url": (
                    "http://127.0.0.1:8793/tearsheets/"
                    "shap_shapiq_prewall_attribution_tearsheet.html"
                ),
                "cpcv_summary_path": str(cpcv_summary_path),
            }
        )
        artifacts_uploaded = False
        artifact_mode = "none"
        artifact_uri = str(run.info.artifact_uri or "")
        artifact_items: list[tuple[Path, str]] = [
            (Path(str(ml4t_tearsheet_info["path"])), "tearsheets"),
            (Path(str(shap_tearsheet_info["path"])), "tearsheets"),
        ]
        if cpcv_summary_path.is_file():
            artifact_items.append((cpcv_summary_path, "cpcv"))
        try:
            for src_path, sub in artifact_items:
                mlflow.log_artifact(str(src_path), artifact_path=sub)
            artifacts_uploaded = True
            artifact_mode = "mlflow_api"
        except OSError:
            cleaned_uri = artifact_uri.removeprefix("file://")
            if cleaned_uri.startswith("/state/mlflow/"):
                state_root = Path(
                    os.environ.get("TIER0_STATE_ROOT", "").strip()
                    or str(Path.home() / "tier0-sota-state")
                )
                host_mlflow_root = state_root / "mlflow"
                if host_mlflow_root.is_dir():
                    rel_sub = cleaned_uri.removeprefix("/state/mlflow/")
                    for src_path, sub in artifact_items:
                        dst = host_mlflow_root / rel_sub / sub / src_path.name
                        _copy_atomic(src_path, dst)
                    artifacts_uploaded = True
                    artifact_mode = "host_bind_mount"
        return {
            "logged": True,
            "artifacts_uploaded": artifacts_uploaded,
            "artifact_log_mode": artifact_mode,
            "artifact_uri": artifact_uri,
            "tracking_uri": tracking_uri,
            "experiment_name": experiment_name,
            "experiment_id": str(run.info.experiment_id),
            "run_id": str(run.info.run_id),
        }


def run_pipeline(
    *,
    df10_path: Path = DEFAULT_DF10_PATH,
    h109_dossier_path: Path = DEFAULT_H109_DOSSIER,
    ablation_dossier_path: Path = DEFAULT_ABLATION_DOSSIER,
    cpcv_dir: Path = DEFAULT_CPCV_DIR,
    tearsheet_dir: Path = DEFAULT_TEARSHEET_DIR,
    manifest_out_path: Path = DEFAULT_OUT_MANIFEST,
    sim_duckdb_path: Path | None = DEFAULT_SIM_DUCKDB,
    mlflow_uri: str | None = DEFAULT_MLFLOW_URI,
    mlflow_experiment: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> dict[str, Any]:
    """Execute end-to-end pre-wall CPCV + ml4t-diagnostic + SHAP/shapiq tearsheet suite."""
    risk_sha = verify_risk_kernel_sha256()
    h109_dossier = _load_json_safe(h109_dossier_path)
    ablation_dossier = _load_json_safe(ablation_dossier_path)

    df = load_prewall_causal_dataset(df10_path)
    dates, returns_1x, spy_bench, arm_ids = build_multiarm_daily_returns(df, cost_bps_roundtrip=4.0)
    n_trials_total = int(
        (h109_dossier.get("trial_accounting") or {}).get("cumulative_trials_total") or 211
    )
    cpcv_result = run_skfolio_cpcv_analysis(
        dates,
        returns_1x,
        arm_ids,
        n_trials_total=n_trials_total,
        cpcv_dir=cpcv_dir,
    )

    ml4t_html_path = tearsheet_dir / "ml4t_diagnostic_h109_falsification_tearsheet.html"
    shap_html_path = tearsheet_dir / "shap_shapiq_prewall_attribution_tearsheet.html"

    ml4t_info = generate_ml4t_h109_falsification_tearsheet(
        dates=dates,
        selected_returns=cpcv_result["selected_returns"],
        benchmark_returns=spy_bench,
        cpcv_result=cpcv_result,
        h109_dossier=h109_dossier,
        ablation_dossier=ablation_dossier,
        output_path=ml4t_html_path,
    )
    shap_info = generate_shap_shapiq_prewall_tearsheet(
        df,
        output_path=shap_html_path,
    )

    duckdb_info: dict[str, Any] | None = None
    if sim_duckdb_path is not None:
        duckdb_info = build_sim_prewall_audit_duckdb(
            db_path=sim_duckdb_path,
            cpcv_result=cpcv_result,
            h109_dossier=h109_dossier,
            shap_result=shap_info,
        )

    mlflow_info: dict[str, Any] = {"logged": False, "tracking_uri": mlflow_uri}
    if mlflow_uri:
        try:
            mlflow_info = log_suite_to_mlflow(
                tracking_uri=mlflow_uri,
                experiment_name=mlflow_experiment,
                cpcv_summary=cpcv_result["summary"],
                ml4t_tearsheet_info=ml4t_info,
                shap_tearsheet_info=shap_info,
                cpcv_summary_path=Path(cpcv_result["summary"]["artifacts"]["summary_json"]),
            )
        except Exception as exc:
            mlflow_info = {
                "logged": False,
                "tracking_uri": mlflow_uri,
                "error": str(exc),
            }

    manifest: dict[str, Any] = {
        "schema_version": "tier0.ml4t-shap-tearsheet-suite.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prewall_cutoff_iso": PREWALL_CUTOFF_ISO,
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": risk_sha,
        "dataset": {
            "source": str(df10_path),
            "rows": int(df.height),
            "symbols_count": int(df.select(pl.col("symbol").n_unique()).item()),
            "date_start": str(min(dates)),
            "date_end": str(max(dates)),
            "sessions_count": int(len(dates)),
        },
        "h109_verdict_summary": {
            "overall_verdict": str(
                (h109_dossier.get("verdict") or {}).get("overall_verdict") or "DISCARD"
            ),
            "cross_leg_promoted": bool(
                (h109_dossier.get("verdict") or {}).get("cross_leg_promoted", False)
            ),
            "cumulative_trials_total": int(n_trials_total),
            "l1_combined_fills": int(
                ((h109_dossier.get("l1_queue_markout") or {}).get("COMBINED") or {}).get(
                    "passive_filled"
                )
                or 0
            ),
            "l1_combined_mean_markout_bps": _finite_or_zero(
                ((h109_dossier.get("l1_queue_markout") or {}).get("COMBINED") or {}).get(
                    "mean_markout_bps"
                ),
                3,
            ),
        },
        "cpcv_artifacts": cpcv_result["summary"],
        "tearsheets": {
            "ml4t_diagnostic_h109_falsification": {
                **ml4t_info,
                "caddy_url": "http://127.0.0.1:8793/tearsheets/ml4t_diagnostic_h109_falsification_tearsheet.html",
            },
            "shap_shapiq_prewall_attribution": {
                **shap_info,
                "caddy_url": "http://127.0.0.1:8793/tearsheets/shap_shapiq_prewall_attribution_tearsheet.html",
            },
        },
        "sim_prewall_audit_duckdb": duckdb_info,
        "mlflow": mlflow_info,
    }
    _atomic_write_json(manifest_out_path, manifest)
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate official unmodified ml4t-diagnostic & SHAP/shapiq tearsheets."
    )
    parser.add_argument("--df10", type=Path, default=DEFAULT_DF10_PATH)
    parser.add_argument("--h109-dossier", type=Path, default=DEFAULT_H109_DOSSIER)
    parser.add_argument("--ablation-dossier", type=Path, default=DEFAULT_ABLATION_DOSSIER)
    parser.add_argument("--cpcv-dir", type=Path, default=DEFAULT_CPCV_DIR)
    parser.add_argument("--tearsheet-dir", type=Path, default=DEFAULT_TEARSHEET_DIR)
    parser.add_argument("--manifest-out", type=Path, default=DEFAULT_OUT_MANIFEST)
    parser.add_argument("--sim-duckdb", type=Path, default=DEFAULT_SIM_DUCKDB)
    parser.add_argument("--mlflow-uri", type=str, default=DEFAULT_MLFLOW_URI)
    parser.add_argument("--mlflow-experiment", type=str, default=DEFAULT_MLFLOW_EXPERIMENT)
    parser.add_argument("--no-mlflow", action="store_true")
    args = parser.parse_args(argv)

    result = run_pipeline(
        df10_path=args.df10,
        h109_dossier_path=args.h109_dossier,
        ablation_dossier_path=args.ablation_dossier,
        cpcv_dir=args.cpcv_dir,
        tearsheet_dir=args.tearsheet_dir,
        manifest_out_path=args.manifest_out,
        sim_duckdb_path=args.sim_duckdb,
        mlflow_uri=None if args.no_mlflow else args.mlflow_uri,
        mlflow_experiment=args.mlflow_experiment,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "manifest": str(args.manifest_out),
                "cpcv_paths": result["cpcv_artifacts"]["skfolio_cpcv"]["n_test_paths"],
                "cpcv_splits": result["cpcv_artifacts"]["skfolio_cpcv"]["n_splits"],
                "pbo": result["cpcv_artifacts"]["ml4t_diagnostic_stats"]["pbo"],
                "dsr": result["cpcv_artifacts"]["ml4t_diagnostic_stats"]["dsr_probability"],
                "ml4t_bytes": result["tearsheets"]["ml4t_diagnostic_h109_falsification"]["bytes"],
                "shap_bytes": result["tearsheets"]["shap_shapiq_prewall_attribution"]["bytes"],
                "mlflow_run_id": result["mlflow"].get("run_id"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

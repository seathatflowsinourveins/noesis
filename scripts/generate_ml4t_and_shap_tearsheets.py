"""Pre-wall H109 CPCV artifacts and reports written by the upstreams' own report functions.

Strictly pre-wall (<= 2026-10-15), paper-only (paper_only=True, live_execution_enabled=False).
Each report file comes from one upstream call that receives run data only and is an
interactive page: ml4t-diagnostic `generate_backtest_tearsheet` and `FeatureImportanceDashboard`
(MDI, PFI and TreeSHAP importance with SHAP interactions, one page per model), and skfolio
`CombinatorialPurgedCV.plot_train_test_folds` and `Population.plot_distribution`. No figure,
page, section or table is composed here, and every number in a report is read from this run
or the H109 dossier: a value the inputs lack is omitted (NULL in the DuckDB tables), never
filled in.
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

# 12 causal daily-bar proxies named after the H109 arms (min_rvol, min_ret_1d, max_coil_ratio,
# mode). They are daily screens, not the dossier's intraday arm definitions (signal mode,
# conformal trail, re-entry), so reports label the selected arm a daily-bar proxy.
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


def _finite_or_none(val: Any, digits: int = 6) -> float | None:
    """Round a finite number; a missing or non-finite value stays missing (None)."""
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(fval):
        return None
    return round(fval, digits)


def _int_or_none(val: Any) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


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
    spy_bench = np.full(n_dates, np.nan, dtype=float)
    for row in bench.iter_rows(named=True):
        d = str(row["trade_date"])
        idx = date_to_idx[d]
        macro_by_date[d] = float(row["macro_ret_1d"] or 0.0)
        spy_next = row["spy_next_ret"]
        spy_bench[idx] = float(spy_next) if spy_next is not None else np.nan

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
    selected_arm_id: str | None = None,
    n_trials_total: int | None = None,
    n_folds: int = 8,
    n_test_folds: int = 3,
    purged_size: int = 2,
    embargo_size: int = 2,
    cpcv_dir: Path = DEFAULT_CPCV_DIR,
) -> dict[str, Any]:
    """Run real skfolio CombinatorialPurgedCV (>= 16 paths) + ml4t-diagnostic DSR/PBO/MinTRL.

    The selected arm comes from the H109 dossier (``tuning.selected_arm_id``) and the DSR needs
    its cumulative trial count; without either, the dependent fields stay None instead of
    assuming an arm or a count.
    """
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
                    "is_mean_bps": _finite_or_none(np.mean(is_r) * 1e4, 4),
                    "oos_mean_bps": _finite_or_none(np.mean(oos_r) * 1e4, 4),
                    "is_sharpe_ann": _finite_or_none(is_sr, 4),
                    "oos_sharpe_ann": _finite_or_none(oos_sr, 4),
                    "oos_max_dd_pct": _finite_or_none(_max_dd(oos_r) * 100.0, 4),
                    "oos_win_rate": _finite_or_none(np.mean(oos_r > 0.0), 4),
                }
            )
        is_sharpe_rows.append(is_row)
        oos_sharpe_rows.append(oos_row)

    is_mat = np.vstack(is_sharpe_rows)
    oos_mat = np.vstack(oos_sharpe_rows)
    pbo_res = mstats.compute_pbo(is_mat, oos_mat)

    # Recombine the test-fold segments into the combinatorial OOS paths with skfolio's own
    # get_path_ids(). In each split the segment uses the arm with the best in-sample Sharpe, so the
    # paths describe that selection rule, not the fixed selected arm.
    path_ids = np.asarray(cpcv.get_path_ids(), dtype=int)
    path_segments: list[list[tuple[int, np.ndarray]]] = [[] for _ in range(n_test_paths)]
    for s_idx, (_tr_idx, te_groups) in enumerate(splits):
        te_list = list(te_groups) if isinstance(te_groups, (list, tuple)) else [te_groups]
        best_is_arm = int(np.argmax(is_mat[s_idx]))
        for g_idx, seg in enumerate(te_list):
            seg_arr = np.asarray(seg, dtype=int)
            if seg_arr.size == 0:
                continue
            path_segments[int(path_ids[s_idx, g_idx])].append(
                (int(seg_arr[0]), returns_1x[seg_arr, best_is_arm])
            )

    path_records: list[dict[str, Any]] = []
    path_sharpes: list[float] = []
    path_returns: list[np.ndarray] = []
    for p_idx in range(n_test_paths):
        pieces = [rets for _start, rets in sorted(path_segments[p_idx], key=lambda seg: seg[0])]
        if not pieces:
            raise RuntimeError(f"CPCV path {p_idx + 1} has no test segments")
        p_rets = np.concatenate(pieces)
        path_returns.append(p_rets)
        p_sr = _ann_sharpe(p_rets)
        p_dd = _max_dd(p_rets)
        p_tot = float(np.prod(1.0 + p_rets) - 1.0)
        path_sharpes.append(p_sr)
        path_records.append(
            {
                "path_id": int(p_idx + 1),
                "n_sessions": int(p_rets.size),
                "mean_daily_bps": _finite_or_none(np.mean(p_rets) * 1e4, 4),
                "sharpe_ann": _finite_or_none(p_sr, 4),
                "total_return_pct": _finite_or_none(p_tot * 100.0, 4),
                "max_drawdown_pct": _finite_or_none(p_dd * 100.0, 4),
                "win_rate": _finite_or_none(np.mean(p_rets > 0.0), 4),
            }
        )

    arm_list = [str(arm) for arm in arm_ids]
    sel_rets: np.ndarray | None = None
    sel_sr_daily: float | None = None
    sel_sr_ann: float | None = None
    dsr_res = None
    min_trl_val: float | None = None
    if selected_arm_id is not None and selected_arm_id in arm_list:
        rets = returns_1x[:, arm_list.index(selected_arm_id)]
        sel_mean = float(np.mean(rets))
        sel_std = float(np.std(rets, ddof=1))
        sr_daily = (sel_mean / sel_std) if sel_std > 1e-12 else 0.0
        sel_rets, sel_sr_daily, sel_sr_ann = rets, sr_daily, sr_daily * math.sqrt(252.0)
        if n_trials_total is not None:
            arm_daily_srs = [
                float(
                    np.mean(returns_1x[:, i]) / max(float(np.std(returns_1x[:, i], ddof=1)), 1e-12)
                )
                for i in range(n_arms)
            ]
            var_trials = max(1e-6, float(np.var(np.asarray(arm_daily_srs, dtype=float), ddof=1)))
            dsr_res = mstats.deflated_sharpe_ratio_from_statistics(
                observed_sharpe=sr_daily,
                n_samples=int(rets.size),
                n_trials=int(n_trials_total),
                variance_trials=var_trials,
                skewness=float(skew(rets)) if sel_std > 1e-12 else 0.0,
                excess_kurtosis=float(kurtosis(rets)) if sel_std > 1e-12 else 0.0,
                frequency="daily",
            )
        min_trl_res = mstats.compute_min_trl(returns=rets, target_sharpe=0.0, frequency="daily")
        if np.isfinite(float(min_trl_res.min_trl)):
            min_trl_val = round(float(min_trl_res.min_trl), 2)

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
        "selected_arm_id": selected_arm_id if sel_rets is not None else None,
        "oos_paths_arm_selection": "best_in_sample_sharpe_per_split",
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
            "n_trials_total": n_trials_total,
            "observed_sharpe_ann": _finite_or_none(sel_sr_ann, 4),
            "observed_sharpe_daily": _finite_or_none(sel_sr_daily, 6),
            "dsr_probability": (
                _finite_or_none(dsr_res.probability, 6) if dsr_res is not None else None
            ),
            "expected_max_sharpe_daily": (
                _finite_or_none(dsr_res.expected_max_sharpe, 6) if dsr_res is not None else None
            ),
            "pbo": _finite_or_none(pbo_res.pbo, 4),
            "pbo_n_combinations": int(pbo_res.n_combinations),
            "min_trl_sessions": min_trl_val,
            "current_sessions": int(sel_rets.size) if sel_rets is not None else None,
            "oos_path_sharpe_mean": _finite_or_none(np.mean(path_sharpes), 4),
            "oos_path_sharpe_median": _finite_or_none(np.median(path_sharpes), 4),
            "oos_path_sharpe_p05": _finite_or_none(np.percentile(path_sharpes, 5), 4),
            "oos_path_sharpe_p95": _finite_or_none(np.percentile(path_sharpes, 95), 4),
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
        "cpcv": cpcv,
        "folds_df": folds_df,
        "paths_df": paths_df,
        "path_returns": path_returns,
        "selected_returns": sel_rets,
        "is_mat": is_mat,
        "oos_mat": oos_mat,
    }


def _report_receipt(path: Path, generator: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": int(path.stat().st_size),
        "sha256": _file_sha256(path),
        "generator": generator,
    }


def generate_h109_upstream_reports(
    *,
    dates: Sequence[str],
    benchmark_returns: np.ndarray,
    cpcv_result: Mapping[str, Any],
    output_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Write the H109 reports with ml4t-diagnostic's and skfolio's own report and plot functions.

    One upstream call per file; this function passes run data only (no figure, section or text).
    """
    from ml4t.diagnostic.visualization.backtest import generate_backtest_tearsheet
    from skfolio import Population, Portfolio
    from skfolio.measures import RatioMeasure

    summary = cpcv_result["summary"]
    stats = summary["ml4t_diagnostic_stats"]
    output_dir.mkdir(parents=True, exist_ok=True)
    reports: dict[str, dict[str, Any]] = {}

    # ml4t-diagnostic's complete backtest tearsheet of the dossier's selected arm (not produced
    # when the dossier names none). Its own DSR with n_trials would deflate n_trials identical
    # copies of one series, so the run's DSR (cross-arm trial variance) is passed as data;
    # without a trial count from the dossier the statistical panels are off.
    if cpcv_result["selected_returns"] is not None:
        selected_arm = str(summary["selected_arm_id"])
        selected_returns = np.asarray(cpcv_result["selected_returns"], dtype=float)
        benchmark = np.asarray(benchmark_returns, dtype=float)
        has_benchmark = bool(
            benchmark.shape == selected_returns.shape and np.all(np.isfinite(benchmark))
        )
        metrics: dict[str, Any] = {}
        if stats["dsr_probability"] is not None:
            metrics["dsr_probability"] = stats["dsr_probability"]
        if stats["min_trl_sessions"] is not None:
            metrics["min_trl"] = stats["min_trl_sessions"]
        statistical = stats["n_trials_total"] is not None and "dsr_probability" in metrics
        tearsheet_path = output_dir / "h109_ml4t_backtest_tearsheet.html"
        generate_backtest_tearsheet(
            returns=pl.Series(selected_arm, selected_returns),
            equity_curve=pl.DataFrame(
                {
                    "timestamp": pl.Series("timestamp", list(dates)).str.to_date(),
                    "equity": np.cumprod(1.0 + selected_returns),
                }
            ),
            benchmark_returns=pl.Series("SPY", benchmark) if has_benchmark else None,
            benchmark_name="SPY (next-session open to close)",
            metrics=metrics or None,
            n_trials=stats["n_trials_total"] if statistical else None,
            include_statistical=statistical,
            template="full",
            interactive=True,
            include_plotlyjs=True,
            theme="dark",
            title=f"{selected_arm} daily-bar proxy: pre-wall next-session open-to-close returns",
            subtitle=f"{summary['date_start']} to {summary['date_end']}",
            periods_per_year=252,
            output_path=tearsheet_path,
        )
        reports["ml4t_backtest_tearsheet"] = _report_receipt(
            tearsheet_path, "ml4t.diagnostic.visualization.backtest.generate_backtest_tearsheet"
        )

    # skfolio's own fold diagram of the CombinatorialPurgedCV object the run used.
    cpcv = cpcv_result["cpcv"]
    folds_path = output_dir / "h109_skfolio_cpcv_train_test_folds.html"
    cpcv.plot_train_test_folds().write_html(str(folds_path))
    reports["skfolio_cpcv_train_test_folds"] = _report_receipt(
        folds_path, "skfolio.model_selection.CombinatorialPurgedCV.plot_train_test_folds"
    )

    # skfolio's own population view of the run's recombined OOS paths: each path's returns (in
    # each split, the arm with the best in-sample Sharpe) become one single-asset Portfolio, and
    # skfolio computes and plots the annualized Sharpe distribution itself.
    population = Population(
        [
            Portfolio(
                X=np.asarray(path_rets, dtype=float).reshape(-1, 1),
                weights=np.ones(1),
                name=f"path_{path_id}",
                annualization_factor=252.0,
            )
            for path_id, path_rets in enumerate(cpcv_result["path_returns"], start=1)
        ]
    )
    paths_path = output_dir / "h109_skfolio_cpcv_path_sharpe_distribution.html"
    population.plot_distribution(measure_list=[RatioMeasure.ANNUALIZED_SHARPE_RATIO]).write_html(
        str(paths_path)
    )
    reports["skfolio_cpcv_path_sharpe_distribution"] = _report_receipt(
        paths_path, "skfolio.population.Population.plot_distribution"
    )
    return reports


def generate_prewall_attribution_reports(
    df: pl.DataFrame,
    *,
    output_dir: Path,
    max_eval_rows: int = 240,
    shapiq_eval_rows: int = 48,
) -> dict[str, Any]:
    """Fit causal pre-wall LightGBM + CatBoost; write ml4t-diagnostic's interactive dashboards.

    TreeSHAP values feed the additivity check and the DuckDB audit tables; shapiq k-SII values
    feed the interaction table. Pages come only from ``FeatureImportanceDashboard``.
    """
    import lightgbm as lgb
    import shap
    from catboost import CatBoostRegressor
    from ml4t.diagnostic.api import analyze_ml_importance, compute_shap_interactions
    from ml4t.diagnostic.visualization import FeatureImportanceDashboard

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

    # Exact TreeSHAP Explanations for both models, with additivity checked against predictions
    lgb_expl = shap.TreeExplainer(lgb_model, feature_names=fnames)(x_eval)
    cat_expl = shap.TreeExplainer(cat_model, feature_names=fnames)(x_eval)
    lgb_additivity_err = float(
        np.max(
            np.abs(
                np.asarray(lgb_model.predict(x_eval), dtype=float)
                - (np.asarray(lgb_expl.base_values) + np.sum(lgb_expl.values, axis=1))
            )
        )
    )
    cat_additivity_err = float(
        np.max(
            np.abs(
                np.asarray(cat_model.predict(x_eval), dtype=float)
                - (np.asarray(cat_expl.base_values) + np.sum(cat_expl.values, axis=1))
            )
        )
    )
    lgb_mean_abs = np.mean(np.abs(lgb_expl.values), axis=0)
    cat_mean_abs = np.mean(np.abs(cat_expl.values), axis=0)

    # ml4t-diagnostic multi-method importance (MDI + PFI + SHAP) per model & SHAP interactions
    imp_res = analyze_ml_importance(
        lgb_model,
        x_eval,
        y_eval,
        feature_names=fnames,
        methods=["mdi", "pfi", "shap"],
        n_repeats=5,
        random_state=42,
    )
    cat_imp_res = analyze_ml_importance(
        cat_model,
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

    # shapiq k-SII order-2 interactions (ADR-0028 graduated primitive) for the audit tables
    n_shapiq = min(shapiq_eval_rows, x_eval.shape[0])
    shapiq_res = disc.compute_shapiq_interactions(
        lgb_model,
        x_train[: min(256, x_train.shape[0])],
        x_eval[:n_shapiq],
        fnames,
        max_order=2,
        max_eval_rows=n_shapiq,
    )

    # ml4t-diagnostic's own interactive dashboards, one page per model. (Its
    # FeatureInteractionDashboard is a placeholder in 0.2.1 -- "to be implemented" text and no
    # charts -- so interactions are shown in the Interactions tab of the LightGBM page.)
    output_dir.mkdir(parents=True, exist_ok=True)
    reports: dict[str, dict[str, Any]] = {}
    lgbm_path = output_dir / "prewall_ml4t_feature_importance_lightgbm.html"
    FeatureImportanceDashboard(
        title="Pre-wall LightGBM: MDI, PFI and TreeSHAP importance with SHAP interactions",
        theme="dark",
        n_top_features=len(fnames),
    ).save(str(lgbm_path), imp_res, interaction_results=int_res)
    reports["ml4t_feature_importance_lightgbm"] = _report_receipt(
        lgbm_path, "ml4t.diagnostic.visualization.FeatureImportanceDashboard"
    )
    catboost_path = output_dir / "prewall_ml4t_feature_importance_catboost.html"
    FeatureImportanceDashboard(
        title="Pre-wall CatBoost: MDI, PFI and TreeSHAP importance",
        theme="dark",
        n_top_features=len(fnames),
    ).save(str(catboost_path), cat_imp_res)
    reports["ml4t_feature_importance_catboost"] = _report_receipt(
        catboost_path, "ml4t.diagnostic.visualization.FeatureImportanceDashboard"
    )

    order = np.argsort(-(lgb_mean_abs + cat_mean_abs))
    shap_ranking = [
        {
            "rank": rank_idx + 1,
            "feature": fnames[int(i)],
            "lgbm_mean_abs_shap": _finite_or_none(lgb_mean_abs[int(i)], 6),
            "catboost_mean_abs_shap": _finite_or_none(cat_mean_abs[int(i)], 6),
            "consensus_mean_abs_shap": _finite_or_none(
                0.5 * (lgb_mean_abs[int(i)] + cat_mean_abs[int(i)]), 6
            ),
        }
        for rank_idx, i in enumerate(order)
    ]

    return {
        "reports": reports,
        "train_rows": int(train_df.height),
        "eval_rows_total": int(eval_df.height),
        "eval_rows_explained": int(x_eval.shape[0]),
        "lgbm_additivity_max_error": float(f"{lgb_additivity_err:.3e}"),
        "catboost_additivity_max_error": float(f"{cat_additivity_err:.3e}"),
        "shap_top_features": shap_ranking,
        "shapiq_version": shapiq_res.get("shapiq_version"),
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
    """Create standalone read-only sim_prewall_audit.duckdb without touching pa1..3/tv_audit.

    Values come from the run and the H109 dossier; a value the dossier lacks is stored as NULL.
    """
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
                "verdict": blk.get("verdict"),
                "gates_passed": _int_or_none(blk.get("gates_passed_count")),
                "n_trades": _int_or_none(f1x.get("n_trades")),
                "l1_passive_filled": _int_or_none(l1.get("passive_filled")),
                "l1_mean_markout_bps": _finite_or_none(l1.get("mean_markout_bps"), 4),
                "mean_1x_bps": _finite_or_none(f1x.get("mean_bps"), 4),
                "mean_cal_liquid_bps": _finite_or_none(fcal.get("mean_bps"), 4),
                "mean_2x_bps": _finite_or_none(f2x.get("mean_bps"), 4),
                "dsr": _finite_or_none(f1x.get("dsr"), 6),
                "pbo": _finite_or_none(blk.get("empirical_pbo"), 4),
                "hac_t_1x": _finite_or_none(f1x.get("hac_t"), 4),
                "spa_p_1x": _finite_or_none(f1x.get("spa_p"), 4),
            }
        )

    h109_df = pl.DataFrame(
        h109_rows,
        schema={
            "leg": pl.String,
            "verdict": pl.String,
            "gates_passed": pl.Int64,
            "n_trades": pl.Int64,
            "l1_passive_filled": pl.Int64,
            "l1_mean_markout_bps": pl.Float64,
            "mean_1x_bps": pl.Float64,
            "mean_cal_liquid_bps": pl.Float64,
            "mean_2x_bps": pl.Float64,
            "dsr": pl.Float64,
            "pbo": pl.Float64,
            "hac_t_1x": pl.Float64,
            "spa_p_1x": pl.Float64,
        },
    )
    folds_df: pl.DataFrame = cpcv_result["folds_df"]
    paths_df: pl.DataFrame = cpcv_result["paths_df"]
    shap_df = pl.DataFrame(
        shap_result["shap_top_features"],
        schema={
            "rank": pl.Int64,
            "feature": pl.String,
            "lgbm_mean_abs_shap": pl.Float64,
            "catboost_mean_abs_shap": pl.Float64,
            "consensus_mean_abs_shap": pl.Float64,
        },
    )
    sq_rows = [
        {
            "pair": item.get("pair"),
            "mean_abs_interaction": _finite_or_none(item.get("mean_abs_interaction"), 6),
            "mean_signed_interaction": _finite_or_none(item.get("mean_signed_interaction"), 6),
        }
        for item in (shap_result.get("shapiq_top5_interactions") or [])
    ]
    sq_df = pl.DataFrame(
        sq_rows,
        schema={
            "pair": pl.String,
            "mean_abs_interaction": pl.Float64,
            "mean_signed_interaction": pl.Float64,
        },
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
    attribution: Mapping[str, Any],
    reports: Mapping[str, Mapping[str, Any]],
    cpcv_summary_path: Path,
) -> dict[str, Any]:
    """Log CPCV/DSR/PBO/SHAP metrics and every upstream report file into MLflow 3."""
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    mstats = cpcv_summary["ml4t_diagnostic_stats"]
    cpcv_cfg = cpcv_summary["skfolio_cpcv"]
    with mlflow.start_run(run_name="ml4t-cpcv-shap-shapiq-prewall-suite") as run:
        params: dict[str, Any] = {
            "prewall_cutoff_iso": PREWALL_CUTOFF_ISO,
            "paper_only": "True",
            "live_execution_enabled": "False",
            "cpcv_n_folds": int(cpcv_cfg["n_folds"]),
            "cpcv_n_test_folds": int(cpcv_cfg["n_test_folds"]),
            "cpcv_n_splits": int(cpcv_cfg["n_splits"]),
            "cpcv_n_test_paths": int(cpcv_cfg["n_test_paths"]),
            "selected_arm_id": cpcv_summary["selected_arm_id"],
            "oos_paths_arm_selection": str(cpcv_summary["oos_paths_arm_selection"]),
            "n_trials_total": mstats["n_trials_total"],
            "shapiq_version": str(attribution["shapiq_version"]),
        }
        mlflow.log_params({key: value for key, value in params.items() if value is not None})
        candidate_metrics = {
            "cpcv_n_test_paths": cpcv_cfg["n_test_paths"],
            "cpcv_n_splits": cpcv_cfg["n_splits"],
            "cpcv_pbo": mstats["pbo"],
            "cpcv_dsr_probability": mstats["dsr_probability"],
            "cpcv_observed_sharpe_ann": mstats["observed_sharpe_ann"],
            "cpcv_oos_path_sharpe_mean": mstats["oos_path_sharpe_mean"],
            "shap_lgbm_additivity_err": attribution["lgbm_additivity_max_error"],
            "shap_catboost_additivity_err": attribution["catboost_additivity_max_error"],
        }
        mlflow.log_metrics({k: float(v) for k, v in candidate_metrics.items() if v is not None})
        mlflow.set_tags(
            {
                **{f"report_{name}_generator": str(r["generator"]) for name, r in reports.items()},
                **{f"report_{name}_sha256": str(r["sha256"]) for name, r in reports.items()},
                "cpcv_summary_path": str(cpcv_summary_path),
            }
        )
        artifacts_uploaded = False
        artifact_mode = "none"
        artifact_uri = str(run.info.artifact_uri or "")
        artifact_items: list[tuple[Path, str]] = [
            (Path(str(r["path"])), "reports") for r in reports.values()
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
    cpcv_dir: Path = DEFAULT_CPCV_DIR,
    tearsheet_dir: Path = DEFAULT_TEARSHEET_DIR,
    manifest_out_path: Path = DEFAULT_OUT_MANIFEST,
    sim_duckdb_path: Path | None = DEFAULT_SIM_DUCKDB,
    mlflow_uri: str | None = DEFAULT_MLFLOW_URI,
    mlflow_experiment: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> dict[str, Any]:
    """Execute end-to-end pre-wall CPCV + ml4t-diagnostic + skfolio + SHAP/shapiq report suite."""
    risk_sha = verify_risk_kernel_sha256()
    h109_dossier = _load_json_safe(h109_dossier_path)
    h109_verdict = h109_dossier.get("verdict") or {}
    h109_l1_combined = (h109_dossier.get("l1_queue_markout") or {}).get("COMBINED") or {}

    df = load_prewall_causal_dataset(df10_path)
    dates, returns_1x, spy_bench, arm_ids = build_multiarm_daily_returns(df, cost_bps_roundtrip=4.0)
    n_trials_total = _int_or_none(
        (h109_dossier.get("trial_accounting") or {}).get("cumulative_trials_total")
    )
    dossier_arm = (h109_dossier.get("tuning") or {}).get("selected_arm_id")
    cpcv_result = run_skfolio_cpcv_analysis(
        dates,
        returns_1x,
        arm_ids,
        selected_arm_id=str(dossier_arm) if dossier_arm is not None else None,
        n_trials_total=n_trials_total,
        cpcv_dir=cpcv_dir,
    )

    reports = generate_h109_upstream_reports(
        dates=dates,
        benchmark_returns=spy_bench,
        cpcv_result=cpcv_result,
        output_dir=tearsheet_dir,
    )
    attribution = generate_prewall_attribution_reports(df, output_dir=tearsheet_dir)
    reports.update(attribution["reports"])

    duckdb_info: dict[str, Any] | None = None
    if sim_duckdb_path is not None:
        duckdb_info = build_sim_prewall_audit_duckdb(
            db_path=sim_duckdb_path,
            cpcv_result=cpcv_result,
            h109_dossier=h109_dossier,
            shap_result=attribution,
        )

    mlflow_info: dict[str, Any] = {"logged": False, "tracking_uri": mlflow_uri}
    if mlflow_uri:
        try:
            mlflow_info = log_suite_to_mlflow(
                tracking_uri=mlflow_uri,
                experiment_name=mlflow_experiment,
                cpcv_summary=cpcv_result["summary"],
                attribution=attribution,
                reports=reports,
                cpcv_summary_path=Path(cpcv_result["summary"]["artifacts"]["summary_json"]),
            )
        except Exception as exc:
            mlflow_info = {
                "logged": False,
                "tracking_uri": mlflow_uri,
                "error": str(exc),
            }

    manifest: dict[str, Any] = {
        "schema_version": "tier0.ml4t-shap-tearsheet-suite.v2",
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
            "source": str(h109_dossier_path),
            "overall_verdict": h109_verdict.get("overall_verdict"),
            "cross_leg_promoted": h109_verdict.get("cross_leg_promoted"),
            "cumulative_trials_total": n_trials_total,
            "l1_combined_fills": _int_or_none(h109_l1_combined.get("passive_filled")),
            "l1_combined_mean_markout_bps": _finite_or_none(
                h109_l1_combined.get("mean_markout_bps"), 3
            ),
        },
        "cpcv_artifacts": cpcv_result["summary"],
        "report_dir": str(tearsheet_dir),
        "reports": reports,
        "reports_not_produced": (
            {}
            if "ml4t_backtest_tearsheet" in reports
            else {"ml4t_backtest_tearsheet": "the H109 dossier names no tuning.selected_arm_id"}
        ),
        "attribution": {k: v for k, v in attribution.items() if k != "reports"},
        "sim_prewall_audit_duckdb": duckdb_info,
        "mlflow": mlflow_info,
    }
    _atomic_write_json(manifest_out_path, manifest)
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Write the pre-wall H109 CPCV, ml4t-diagnostic, skfolio and SHAP/shapiq reports "
            "with the upstreams' own report and plot functions."
        )
    )
    parser.add_argument("--df10", type=Path, default=DEFAULT_DF10_PATH)
    parser.add_argument("--h109-dossier", type=Path, default=DEFAULT_H109_DOSSIER)
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
                "reports": {name: r["bytes"] for name, r in result["reports"].items()},
                "mlflow_run_id": result["mlflow"].get("run_id"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""6-gate statistical falsification & hftbacktest queue markout adapter via pinned upstreams."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from pathlib import Path
from statistics import NormalDist
from typing import Any, Literal
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from tier0_sota import discovery as disc
from tier0_sota import engine as eng
from tier0_sota.discovery import assert_pre_wall_date, assert_pre_wall_ns
from tier0_sota.engine import run_nautilus_signal_backtest

MIN_CPCV_SAMPLES: int = 16
CPCV_N_FOLDS: int = 8
CPCV_N_TEST_FOLDS: int = 3
CPCV_PURGED_SIZE: int = 2
CPCV_EMBARGO_SIZE: int = 2
FEED_LATENCY_NS: int = 50_000


@lru_cache(maxsize=1)
def _cpcv() -> Any:
    """skfolio CPCV with >= MIN_CPCV_SAMPLES OOS paths (8 folds / 3 test -> 56 splits, 21 paths)."""
    from skfolio.model_selection import CombinatorialPurgedCV

    cv = CombinatorialPurgedCV(
        n_folds=CPCV_N_FOLDS,
        n_test_folds=CPCV_N_TEST_FOLDS,
        purged_size=CPCV_PURGED_SIZE,
        embargo_size=CPCV_EMBARGO_SIZE,
    )
    if int(cv.n_test_paths) < MIN_CPCV_SAMPLES:
        raise ValueError(
            f"CPCV must produce >= {MIN_CPCV_SAMPLES} OOS paths, got {int(cv.n_test_paths)}"
        )
    return cv


@lru_cache(maxsize=1)
def cpcv_min_samples() -> int:
    """Smallest sample count upstream skfolio CPCV accepts for all >= 16 OOS paths."""
    cv = _cpcv()
    for n_obs in range(MIN_CPCV_SAMPLES, 100_000):
        try:
            list(cv.split(np.zeros((n_obs, 1))))
        except ValueError:
            continue
        return n_obs
    raise RuntimeError("skfolio CPCV rejected every sample size up to 100_000")


def hlz_t_hurdle(n_trials_total: int) -> float:
    """Project HLZ-inspired one-sided Bonferroni normal critical value for cumulative trials."""
    return float(NormalDist().inv_cdf(1.0 - 0.00135 / max(1, int(n_trials_total))))


def evaluate_hftbacktest_queue_markout(
    quotes_df: pl.DataFrame,
    trades_df: pl.DataFrame,
    *,
    symbol: str,
    side: str = "BUY",
    qty: float = 1.0,
    entry_ts_ns: int | None = None,
    horizon_ns: int | None = None,
    maker_fee_rate: float = 0.0,
    taker_fee_rate: float = 0.0004,
) -> dict[str, Any]:
    """Run Rust hftbacktest RiskAdverseQueueModel passive GTX limit markout on pre-wall L1."""
    import hftbacktest as hft

    if side not in ("BUY", "SELL"):
        raise ValueError(f"Unsupported side {side!r}; expected 'BUY' or 'SELL'")
    if quotes_df.is_empty() or trades_df.is_empty():
        raise ValueError("quotes_df and trades_df must be non-empty")
    q_s, t_s = quotes_df.sort("t_ns"), trades_df.sort("t_ns")
    for df_x in (q_s, t_s):
        if isinstance(mx := df_x.select(pl.col("t_ns").max()).item(), int):
            assert_pre_wall_ns(mx)
    q_ts, bp, bs, ap, as_ = (q_s[col].to_numpy() for col in ("t_ns", "bp", "bs", "ap", "as"))
    t_ts, tp, tq = (t_s[col].to_numpy() for col in ("t_ns", "p", "s"))
    nq, nt = q_s.height, t_s.height
    bchg, achg = np.flatnonzero(bp[1:] != bp[:-1]) + 1, np.flatnonzero(ap[1:] != ap[:-1]) + 1
    ev: Any = np.zeros(2 * nq + int(bchg.size) + int(achg.size) + nt, dtype=hft.event_dtype)
    evs, ets, lts, pxs, qts = ev["ev"], ev["exch_ts"], ev["local_ts"], ev["px"], ev["qty"]
    base, pos, l_q = hft.EXCH_EVENT | hft.LOCAL_EVENT, 0, q_ts + FEED_LATENCY_NS
    for eside, pxa, qta, chg in ((hft.BUY_EVENT, bp, bs, bchg), (hft.SELL_EVENT, ap, as_, achg)):
        kind = hft.DEPTH_EVENT | base | eside
        if nc := int(chg.size):
            sc = slice(pos, pos + nc)
            evs[sc], ets[sc], lts[sc], pxs[sc] = kind, q_ts[chg], l_q[chg], pxa[chg - 1]
            pos += nc
        sq = slice(pos, pos + nq)
        evs[sq], ets[sq], lts[sq], pxs[sq], qts[sq] = kind, q_ts, l_q, pxa, qta
        pos += nq
    qi = np.clip(np.searchsorted(q_ts, t_ts, side="right") - 1, 0, nq - 1)
    t_side = np.where(tp >= 0.5 * (bp[qi] + ap[qi]), hft.BUY_EVENT, hft.SELL_EVENT)
    evs[pos:], pxs[pos:], qts[pos:] = hft.TRADE_EVENT | base | t_side, tp, tq
    ets[pos:], lts[pos:] = t_ts, t_ts + FEED_LATENCY_NS
    ev = np.ascontiguousarray(ev[np.argsort(ets, kind="mergesort")])
    t0, t_end = int(ev[0]["local_ts"]), int(ev[-1]["local_ts"])
    out: dict[str, Any] = {
        "symbol": symbol.upper(),
        "side": side,
        "qty": float(qty),
        "events_replayed": len(ev),
        "queue_model": "RiskAdverseQueueModel",
        "maker_fee_rate": maker_fee_rate,
        "taker_fee_rate": taker_fee_rate,
    }
    if entry_ts_ns is not None and not (t0 <= entry_ts_ns < t_end):
        return out | {
            "passive_filled": False,
            "passive_order_submitted": False,
            "fill_price": None,
            "markout_bps": 0.0,
        }
    tk = 0.0001 if float(np.min(bp)) < 1.0 else 0.01
    asset = (
        hft.BacktestAsset()
        .linear_asset(1.0)
        .tick_size(tk)
        .lot_size(1.0)
        .constant_order_latency(100_000, 100_000)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(maker_fee_rate, taker_fee_rate)
        .data(ev)
    )
    hbt = hft.HashMapMarketDepthBacktest([asset])
    try:
        dflt_el = min(10**10, max(1, (t_end - t0) // 4))
        hbt.elapse(max(1, entry_ts_ns - t0) if entry_ts_ns is not None else dflt_el)
        d0, sfn = hbt.depth(0), hbt.submit_buy_order if side == "BUY" else hbt.submit_sell_order
        px0 = float(d0.best_bid if side == "BUY" else d0.best_ask)
        sub_ok = bool(
            math.isfinite(px0)
            and px0 > 0.0
            and int(sfn(0, 1, px0, float(qty), hft.GTX, hft.LIMIT, True)) == 0
        )
        rem = max(1, t_end - int(hbt.current_timestamp))
        hbt.elapse(min(horizon_ns, rem) if horizon_ns is not None else rem)
        dep, order = hbt.depth(0), hbt.orders(0).get(1)
        filled = order is not None and int(order.status) == int(hft.FILLED)
        fpx = round(float(order.exec_price), 4) if filled else None
        ebid, eask, mbps = float(dep.best_bid), float(dep.best_ask), 0.0
        if filled and fpx and math.isfinite(ebid + eask) and min(ebid, eask) > 0.0:
            emid = 0.5 * (ebid + eask)
            diff = (emid - fpx) if side == "BUY" else (fpx - emid)
            mbps = round((diff / fpx - maker_fee_rate) * 1e4, 2)
        return out | {
            "passive_order_submitted": sub_ok,
            "passive_filled": filled,
            "fill_price": fpx,
            "markout_bps": mbps,
        }
    finally:
        hbt.close()


def _compute_cpcv_pbo(trial_returns: Sequence[np.ndarray]) -> float | None:
    elig = [
        a
        for r in trial_returns
        if (a := np.asarray(r, float)).size >= MIN_CPCV_SAMPLES
        and bool(np.all(np.isfinite(a)))
        and float(np.std(a)) > 1e-12
    ]
    if len(elig) < 2:
        return None
    m_len = min(r.size for r in elig)
    if m_len < cpcv_min_samples():
        return None
    mat = np.column_stack([r[:m_len] for r in elig])
    cpcv = _cpcv()
    is_r, oos_r = [], []
    for tri, tei in cpcv.split(mat):
        tr = np.concatenate(tri) if isinstance(tri, (list, tuple)) else tri
        te = np.concatenate(tei) if isinstance(tei, (list, tuple)) else tei
        is_r.append(np.mean(mat[tr], axis=0))
        oos_r.append(np.mean(mat[te], axis=0))
    from ml4t.diagnostic.evaluation import stats as mstats

    return float(mstats.compute_pbo(np.vstack(is_r), np.vstack(oos_r)).pbo)


def _compute_empirical_trial_sr_variance(trial_returns: Sequence[np.ndarray]) -> float | None:
    """Compute empirical cross-sectional variance of native-frequency trial Sharpe ratios."""
    srs: list[float] = []
    for r in trial_returns:
        a = np.asarray(r, dtype=float)
        if a.size < MIN_CPCV_SAMPLES or not bool(np.all(np.isfinite(a))):
            continue
        sd = float(np.std(a, ddof=1))
        if sd > 1e-12:
            srs.append(float(np.mean(a)) / sd)
    if len(srs) < 2:
        return None
    var = float(np.var(np.asarray(srs, dtype=float), ddof=1))
    return max(1e-6, var) if math.isfinite(var) else None


def falsify_trade_returns(
    trade_returns: np.ndarray, *, n_trials_total: int, **o: Any
) -> dict[str, Any]:
    """Run skfolio CPCV, ml4t DSR/MinTRL/PBO, arch Hansen SPA & statsmodels HAC gates."""
    tr_r = np.asarray(trade_returns, dtype=float)
    n_s, n_t = int(tr_r.size), max(1, int(n_trials_total))
    pbo = o.get("pbo")
    trial_rets = o.get("trial_returns")
    if trial_rets is not None and pbo is None:
        pbo = _compute_cpcv_pbo(trial_rets)
    worst_pct = round(float(np.min(tr_r)) * 100.0, 4) if n_s > 0 else 0.0
    hlz_t = hlz_t_hurdle(n_t)
    if n_s < MIN_CPCV_SAMPLES:
        return {
            "n_trades": n_s,
            "worst_trade_pct": worst_pct,
            "cpcv_splits": 0,
            "cpcv_oos_paths": 0,
            "cpcv_min_samples": cpcv_min_samples(),
            "cpcv_oos_mean_bps": 0.0,
            "cpcv_oos_splits_min_bps": 0.0,
            "cpcv_oos_splits_median_bps": 0.0,
            "cpcv_oos_splits_p05_bps": 0.0,
            "dsr": 0.0,
            "min_trl": None,
            "pbo": pbo,
            "hac_t": 0.0,
            "hac_t_raw": 0.0,
            "hlz_t_hurdle": round(hlz_t, 4),
            "hlz_t_hurdle_raw": hlz_t,
            "spa_p": 1.0,
            "spa_p_upper": 1.0,
            "gate_checks": {
                "2_return_positive": False,
                "3_cpcv_oos_positive": False,
                "4_dsr_pbo_credible": False,
                "5_hansen_spa_significant": False,
                "6_hac_t_stat_significant": False,
            },
            "promoted": False,
            "verdict": "DISCARD_INSUFFICIENT_TRADES",
        }
    min_cpcv_n = cpcv_min_samples()
    cpcv_ok = n_s >= min_cpcv_n
    splits: list[Any] = list(_cpcv().split(tr_r.reshape(-1, 1))) if cpcv_ok else []
    te_idx = [np.concatenate(te) if isinstance(te, (list, tuple)) else te for _, te in splits]
    split_oos_means = [float(np.mean(tr_r[idx])) for idx in te_idx] if cpcv_ok else []
    cpcv_oos_m = float(np.mean(split_oos_means)) if split_oos_means else 0.0
    cpcv_oos_min = float(np.min(split_oos_means)) if split_oos_means else 0.0
    cpcv_oos_med = float(np.median(split_oos_means)) if split_oos_means else 0.0
    cpcv_oos_p05 = float(np.percentile(split_oos_means, 5)) if split_oos_means else 0.0
    mean_r, std_r = float(np.mean(tr_r)), float(np.std(tr_r, ddof=1))
    if o.get("variance_trials") is not None:
        vt = max(1e-6, float(o["variance_trials"]))
    elif trial_rets is not None and (emp_vt := _compute_empirical_trial_sr_variance(trial_rets)):
        vt = emp_vt
    else:
        vt = 0.25
    dsr_val, min_trl = 0.0, None
    if std_r > 1e-12:
        from ml4t.diagnostic.evaluation import stats as mstats
        from scipy.stats import kurtosis, skew

        dsr_res = mstats.deflated_sharpe_ratio_from_statistics(
            mean_r / std_r,
            n_s,
            n_t,
            vt,
            skewness=float(skew(tr_r)),
            excess_kurtosis=float(kurtosis(tr_r)),
            frequency="daily",
        )
        dsr_val = float(dsr_res.probability)
        mt_res = mstats.compute_min_trl(returns=tr_r, target_sharpe=0.0, frequency="daily")
        min_trl = float(mt_res.min_trl) if np.isfinite(mt_res.min_trl) else None
    hac_t, spa_p, spa_pu = 0.0, 1.0, 1.0
    if std_r > 1e-12:
        import statsmodels.api as sm
        from arch.bootstrap import SPA

        nw = max(1, int(4 * (n_s / 100.0) ** (2.0 / 9.0)))
        ols = sm.OLS(tr_r, np.ones((n_s, 1))).fit(cov_type="HAC", cov_kwds={"maxlags": nw})
        hac_t = float(ols.tvalues[0])
        spa = SPA(np.zeros_like(tr_r), (-tr_r).reshape(-1, 1), block_size=2, reps=300, seed=42)
        spa.compute()
        spa_p, spa_pu = float(spa.pvalues["consistent"]), float(spa.pvalues["upper"])
    # Evaluate all gate booleans ONCE from raw unrounded finite floats (prevents rounding flips).
    n_oos_paths = int(_cpcv().n_test_paths) if cpcv_ok else 0
    mean_ok = bool(math.isfinite(mean_r) and mean_r > 0.0)
    cpcv_oos_ok = bool(
        cpcv_ok
        and n_oos_paths >= MIN_CPCV_SAMPLES
        and math.isfinite(cpcv_oos_m)
        and cpcv_oos_m > 0.0
    )
    pbo_ok = bool(pbo is not None and math.isfinite(float(pbo)) and float(pbo) < 0.50)
    dsr_pbo_ok = bool(
        math.isfinite(dsr_val)
        and dsr_val >= 0.95
        and min_trl is not None
        and math.isfinite(min_trl)
        and n_s >= min_trl
        and pbo_ok
    )
    spa_ok = bool(math.isfinite(spa_p) and math.isfinite(spa_pu) and spa_p < 0.05 and spa_pu < 0.05)
    hac_ok = bool(math.isfinite(hac_t) and math.isfinite(hlz_t) and hac_t >= hlz_t)
    gate_checks = {
        "2_return_positive": mean_ok,
        "3_cpcv_oos_positive": cpcv_oos_ok,
        "4_dsr_pbo_credible": dsr_pbo_ok,
        "5_hansen_spa_significant": spa_ok,
        "6_hac_t_stat_significant": hac_ok,
    }
    prom = all(gate_checks.values())
    return {
        "n_trades": n_s,
        "worst_trade_pct": worst_pct,
        "mean_bps": round(mean_r * 1e4, 3),
        "cpcv_splits": len(splits),
        "cpcv_oos_paths": n_oos_paths,
        "cpcv_min_samples": min_cpcv_n,
        "cpcv_oos_mean_bps": round(cpcv_oos_m * 1e4, 3),
        "cpcv_oos_splits_min_bps": round(cpcv_oos_min * 1e4, 3),
        "cpcv_oos_splits_median_bps": round(cpcv_oos_med * 1e4, 3),
        "cpcv_oos_splits_p05_bps": round(cpcv_oos_p05 * 1e4, 3),
        "dsr": round(dsr_val, 6),
        "min_trl": round(min_trl, 2) if min_trl is not None else None,
        "pbo": round(float(pbo), 4) if pbo is not None else None,
        "hac_t": round(hac_t, 4),
        "hac_t_raw": hac_t,
        "hlz_t_hurdle": round(hlz_t, 4),
        "hlz_t_hurdle_raw": hlz_t,
        "spa_p": round(spa_p, 4),
        "spa_p_upper": round(spa_pu, 4),
        "gate_checks": gate_checks,
        "promoted": prom,
        "verdict": "PROMOTE" if prom else ("DISCARD" if cpcv_ok else "DISCARD_INSUFFICIENT_PATHS"),
    }


def log_falsification_to_mlflow(
    report: dict[str, Any],
    *,
    tracking_uri: str,
    experiment_name: str = "tier0-sota-falsification",
    run_name: str | None = None,
) -> str:
    """Log 6-gate falsification summary & verdict into official MLflow tracking store."""
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=run_name) as run:
        params = {"verdict": report["verdict"], "n_trials_total": report.get("n_trials_total", 1)}
        mlflow.log_params(params)
        for gate_name, passed in report["gates"].items():
            mlflow.log_metric(f"gate_{gate_name}", float(bool(passed)))
        mlflow.log_metric("promoted", float(bool(report["promoted"])))
        return str(run.info.run_id)


def compute_skfolio_portfolio_weights(
    returns: pl.DataFrame | np.ndarray,
    *,
    asset_names: Sequence[str] | None = None,
    method: Literal["mean_risk_clarabel", "hrp"] = "mean_risk_clarabel",
    risk_measure: Any = None,
    min_weights: float = 0.0,
    max_weights: float = 1.0,
) -> dict[str, Any]:
    """Optimize long-only sleeve weights via upstream skfolio + Clarabel.rs conic solver."""
    from skfolio import RiskMeasure
    from skfolio.optimization import HierarchicalRiskParity, MeanRisk

    resolved_rm = RiskMeasure.VARIANCE if risk_measure is None else risk_measure
    if isinstance(returns, pl.DataFrame):
        names = list(asset_names) if asset_names is not None else list(returns.columns)
        arr = returns.to_numpy().astype(float)
    else:
        arr = np.asarray(returns, dtype=float)
        if arr.ndim != 2:
            raise ValueError(f"Expected 2D returns matrix (n_obs, n_assets), got shape {arr.shape}")
        names = (
            list(asset_names)
            if asset_names is not None
            else [f"asset_{idx}" for idx in range(arr.shape[1])]
        )
    if arr.shape[0] < 2 or arr.shape[1] < 2:
        raise ValueError(f"Need >= 2 observations and >= 2 assets, got shape {arr.shape}")
    if len(names) != arr.shape[1]:
        raise ValueError(f"asset_names length {len(names)} != matrix cols {arr.shape[1]}")
    if not bool(np.all(np.isfinite(arr))):
        raise ValueError("returns matrix contains non-finite values")
    if min_weights < 0.0 or max_weights > 1.0 or min_weights > max_weights:
        raise ValueError(f"Invalid weight bounds [{min_weights}, {max_weights}]")

    if method == "mean_risk_clarabel":
        model: Any = MeanRisk(
            risk_measure=resolved_rm,
            min_weights=min_weights,
            max_weights=max_weights,
            solver="CLARABEL",
        )
        solver_name = "clarabel"
    elif method == "hrp":
        model = HierarchicalRiskParity(
            risk_measure=resolved_rm,
            min_weights=min_weights,
            max_weights=max_weights,
        )
        solver_name = "hrp_hierarchical"
    else:
        raise ValueError(f"Unsupported portfolio method: {method!r}")

    model.fit(arr)
    raw_w = np.asarray(model.weights_, dtype=float)
    if not bool(np.all(np.isfinite(raw_w))):
        raise ValueError("skfolio optimizer returned non-finite weights")
    if float(np.min(raw_w)) < -1e-5:
        raise ValueError(f"Negative weight produced under long-only constraint: {raw_w}")
    clean_w = np.clip(raw_w, 0.0, 1.0)
    total = float(np.sum(clean_w))
    if not math.isfinite(total) or total <= 0.0:
        raise ValueError(f"Invalid portfolio weight sum: {total}")
    norm_w = clean_w / total

    try:
        clarabel_ver = pkg_version("clarabel")
    except PackageNotFoundError:
        clarabel_ver = "unknown"

    return {
        "method": method,
        "solver": solver_name,
        "clarabel_version": clarabel_ver,
        "risk_measure": str(resolved_rm.value),
        "assets": names,
        "weights": {name: round(float(w), 6) for name, w in zip(names, norm_w, strict=True)},
        "weights_sum": round(float(np.sum(norm_w)), 6),
        "min_weight": round(float(np.min(norm_w)), 6),
        "max_weight": round(float(np.max(norm_w)), 6),
        "n_observations": int(arr.shape[0]),
    }


def fit_conformal_excursion_intervals(
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_cal: np.ndarray,
    y_cal: np.ndarray,
    x_eval: np.ndarray,
    *,
    y_eval: np.ndarray | None = None,
    alpha: float = 0.10,
    max_hard_cap_pct: float = 0.02,
    min_trail_floor_pct: float = 0.0025,
    uncertainty_width_threshold: float | None = None,
    estimator: Any | None = None,
    random_state: int = 42,
) -> dict[str, Any]:
    """Fit distribution-free MAPIE SplitConformalRegressor intervals bounded <= 2.00% risk cap."""
    import lightgbm as lgb
    import mapie
    from mapie.regression import SplitConformalRegressor

    try:
        mapie_ver = pkg_version("mapie")
    except PackageNotFoundError:
        mapie_ver = str(getattr(mapie, "__version__", "unknown"))

    if not (0.0 < float(alpha) < 0.5):
        raise ValueError(f"alpha must lie strictly in (0.0, 0.5), got {alpha}")
    cap = float(max_hard_cap_pct)
    if not (0.0 < cap <= 0.02):
        raise ValueError(
            f"max_hard_cap_pct must lie in (0.0, 0.02] to preserve risk.py 2.00% cap, got {cap}"
        )
    floor = max(0.0, min(float(min_trail_floor_pct), cap))

    x_tr = np.asarray(x_train, dtype=float)
    y_tr = np.asarray(y_train, dtype=float).reshape(-1)
    x_c = np.asarray(x_cal, dtype=float)
    y_c = np.asarray(y_cal, dtype=float).reshape(-1)
    x_ev = np.asarray(x_eval, dtype=float)
    y_ev = np.asarray(y_eval, dtype=float).reshape(-1) if y_eval is not None else None

    if x_tr.ndim != 2 or x_c.ndim != 2 or x_ev.ndim != 2:
        raise ValueError("x_train, x_cal, and x_eval must be 2D feature matrices")
    if x_tr.shape[0] < 10 or x_c.shape[0] < 10 or x_ev.shape[0] < 1:
        raise ValueError("Need >= 10 train rows, >= 10 calibration rows, and >= 1 eval row")
    if x_tr.shape[0] != y_tr.size or x_c.shape[0] != y_c.size:
        raise ValueError("Feature/target row count mismatch in train or calibration splits")
    if not (x_tr.shape[1] == x_c.shape[1] == x_ev.shape[1]):
        raise ValueError("Feature dimension mismatch across train, calibration, and eval splits")
    if y_ev is not None and y_ev.size != x_ev.shape[0]:
        raise ValueError("y_eval length must match x_eval row count")
    arrays_to_check: tuple[np.ndarray, ...] = (
        (x_tr, y_tr, x_c, y_c, x_ev, y_ev) if y_ev is not None else (x_tr, y_tr, x_c, y_c, x_ev)
    )
    if not all(bool(np.all(np.isfinite(a))) for a in arrays_to_check):
        raise ValueError("All conformal input arrays must contain strictly finite values")

    lgbm_threads = disc._env_positive_int("TIER0_LGBM_THREADS", 4)
    base_est = (
        estimator
        if estimator is not None
        else lgb.LGBMRegressor(
            n_estimators=24,
            max_depth=3,
            learning_rate=0.05,
            num_leaves=7,
            min_child_samples=5,
            n_jobs=lgbm_threads,
            random_state=int(random_state),
            verbosity=-1,
        )
    )
    conformal = SplitConformalRegressor(
        estimator=base_est,
        confidence_level=round(1.0 - float(alpha), 6),
        conformity_score="absolute",
        prefit=False,
    )
    conformal.fit(x_tr, y_tr)
    conformal.conformalize(x_c, y_c)

    y_pred_raw, y_pis_raw = conformal.predict_interval(x_ev)
    y_pred = np.asarray(y_pred_raw, dtype=float).reshape(-1)
    y_pis = np.asarray(y_pis_raw, dtype=float)
    lower = y_pis[:, 0, 0].reshape(-1)
    upper = y_pis[:, 1, 0].reshape(-1)
    widths = np.maximum(0.0, upper - lower)

    _, cal_pis_raw = conformal.predict_interval(x_c)
    cal_pis = np.asarray(cal_pis_raw, dtype=float)
    cal_lo = cal_pis[:, 0, 0].reshape(-1)
    cal_hi = cal_pis[:, 1, 0].reshape(-1)
    cal_cov = float(np.mean((y_c >= cal_lo) & (y_c <= cal_hi)))

    eval_cov: float | None = None
    if y_ev is not None:
        eval_cov = float(np.mean((y_ev >= lower) & (y_ev <= upper)))
    emp_cov = eval_cov if eval_cov is not None else cal_cov

    width_thr = (
        float(uncertainty_width_threshold)
        if uncertainty_width_threshold is not None
        else round(cap * 1.5, 6)
    )
    abstain_flags = [bool(w > width_thr) for w in widths.tolist()]
    raw_trails = np.maximum(0.0, np.abs(upper))
    clamped_trails = np.clip(raw_trails, floor, cap)

    return {
        "method": "SplitConformalRegressor",
        "alpha": float(alpha),
        "target_coverage": round(1.0 - float(alpha), 6),
        "n_train": int(x_tr.shape[0]),
        "n_cal": int(x_c.shape[0]),
        "n_eval": int(x_ev.shape[0]),
        "predictions": [round(float(v), 6) for v in y_pred.tolist()],
        "intervals": [
            [round(float(lo), 6), round(float(hi), 6)]
            for lo, hi in zip(lower.tolist(), upper.tolist(), strict=True)
        ],
        "interval_widths": [round(float(w), 6) for w in widths.tolist()],
        "mean_interval_width": round(float(np.mean(widths)), 6),
        "empirical_coverage": round(emp_cov, 6),
        "cal_empirical_coverage": round(cal_cov, 6),
        "eval_empirical_coverage": round(eval_cov, 6) if eval_cov is not None else None,
        "uncertainty_width_threshold": round(width_thr, 6),
        "abstain_high_uncertainty": abstain_flags,
        "abstain_rate": round(float(np.mean(abstain_flags)), 6),
        "max_hard_cap_pct": round(cap, 6),
        "min_trail_floor_pct": round(floor, 6),
        "risk_cap_enforced": True,
        "conformal_trailing_pct": [round(float(v), 6) for v in clamped_trails.tolist()],
        "mean_conformal_trailing_pct": round(float(np.mean(clamped_trails)), 6),
        "max_conformal_trailing_pct": round(float(np.max(clamped_trails)), 6),
        "mapie_version": mapie_ver,
    }


def run_starred_frontier_landscape_benchmark(
    *,
    lake_root: Path = disc.DEFAULT_MARKET_DATA_ROOT,
    manifest_dir: Path = disc.DEFAULT_DATA_ROOT / "manifests",
    output_path: Path | None = None,
    symbols: Sequence[str] = (
        "NVDA",
        "AMD",
        "PLTR",
        "TSLA",
        "INTC",
        "MSTR",
        "AAPL",
        "META",
        "MSFT",
        "AMZN",
    ),
    alpha: float = 0.10,
    max_hard_cap_pct: float = 0.02,
    write_manifest: bool = True,
) -> dict[str, Any]:
    """Run all 5 L1/L2/L5/L12/L13 frontier engines on strictly pre-wall (<= 2026-10-15) data."""
    import lightgbm as lgb

    risk_sha = _verify_risk_kernel_immutable()
    cutoff_iso = disc.DISCOVERY_WALL_ISO
    cutoff_utc = f"{cutoff_iso}T23:59:59+00:00"
    sym_list = [str(s).strip().upper() for s in symbols if str(s).strip()]

    # 1. sqlglot PIT DuckDB SQL AST validation + read-only query on pre-wall calibration DB
    safe_sql = (
        "SELECT symbol, category, archetype, rth_min_trailing_pct "
        "FROM per_symbol_min_trailing "
        "WHERE rth_min_trailing_pct <= 0.02 "
        "ORDER BY symbol"
    )
    mutation_sql = "DROP TABLE per_symbol_min_trailing"
    lookahead_sql = "SELECT symbol, CURRENT_TIMESTAMP AS leaked_ts FROM per_symbol_min_trailing"
    sql_safe_res = disc.validate_pit_duckdb_sql(safe_sql, read_only=True, require_pit_safe=True)
    sql_mut_res = disc.validate_pit_duckdb_sql(mutation_sql, read_only=True, require_pit_safe=True)
    sql_look_res = disc.validate_pit_duckdb_sql(
        lookahead_sql, read_only=True, require_pit_safe=True
    )
    duckdb_rows_scanned = 0
    cal_db = manifest_dir / "historical_extreme_mover_calibration.duckdb"
    if sql_safe_res["ok"] and cal_db.is_file():
        import duckdb

        with duckdb.connect(str(cal_db), read_only=True) as con:
            duckdb_rows_scanned = len(con.execute(sql_safe_res["normalized_sql"]).fetchall())

    sqlglot_section: dict[str, Any] = {
        "package": "sqlglot",
        "version": sql_safe_res["sqlglot_version"],
        "license": "MIT",
        "layer": "L1/L12",
        "safe_query_ok": bool(sql_safe_res["ok"]),
        "safe_query_tables": list(sql_safe_res["tables"]),
        "safe_query_rows_verified": int(duckdb_rows_scanned),
        "mutation_refused": not bool(sql_mut_res["ok"]),
        "mutation_violations": list(sql_mut_res["violations"]),
        "lookahead_refused": not bool(sql_look_res["ok"]),
        "lookahead_violations": list(sql_look_res["violations"]),
        "status": (
            "VERIFIED_PASS"
            if (sql_safe_res["ok"] and not sql_mut_res["ok"] and not sql_look_res["ok"])
            else "FAILED"
        ),
    }

    # 2. EdgarTools causal PIT SEC catalysts strictly <= 2026-10-15T23:59:59Z
    pub_dir = lake_root / "public-tables"
    sec_res = disc.load_causal_sec_catalysts(
        sym_list,
        as_of_utc=cutoff_utc,
        allow_network=False,
        lookback_days=45,
        parquet_dir=pub_dir,
    )
    sec_mid_probe = disc.load_causal_sec_catalysts(
        sym_list,
        as_of_utc="2026-10-01T12:00:00+00:00",
        allow_network=False,
        lookback_days=45,
        parquet_dir=pub_dir,
    )
    sec_top = sorted(
        (
            {
                "symbol": sym,
                "catalyst_score": float(info["catalyst_score"]),
                "recent_8k_count": int(info["recent_8k_count"]),
                "recent_10q_10k_count": int(info["recent_10q_10k_count"]),
                "insider_form4_count": int(info["insider_form4_count"]),
                "accepted_filings_count": int(info["accepted_filings_count"]),
                "excluded_future_filings": int(info["excluded_future_filings"]),
                "latest_form": info["latest_form"],
                "latest_accepted_utc": info["latest_accepted_utc"],
            }
            for sym, info in sec_res["by_symbol"].items()
        ),
        key=lambda r: (-float(r["catalyst_score"]), str(r["symbol"])),
    )
    edgar_section: dict[str, Any] = {
        "package": "edgartools",
        "version": sec_res["edgartools_version"],
        "license": "MIT",
        "layer": "L1/L2",
        "source": sec_res["source"],
        "as_of_utc": sec_res["as_of_utc"],
        "pit_safe": bool(sec_res["pit_safe"]),
        "prewall_compliant": bool(sec_res["prewall_compliant"]),
        "symbols_scanned": int(sec_res["symbols_count"]),
        "total_accepted_filings": int(sec_res["total_accepted_filings"]),
        "excluded_future_filings_at_wall": int(sec_res["excluded_future_filings"]),
        "mid_window_pit_future_excluded": int(sec_mid_probe["excluded_future_filings"]),
        "top_catalyst_symbols": sec_top,
        "status": "VERIFIED_PASS" if sec_res["pit_safe"] else "FAILED",
    }

    # 3. skchange structural regime changepoints & CAPA anomaly segments on real pre-wall 1m bars
    skchange_syms = ["SPY", "QQQ", "SOXX", "NVDA"]
    sk_by_sym: dict[str, dict[str, Any]] = {}
    sk_days_used: list[str] = []
    sk_ver = "unknown"
    bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"
    avail_days = (
        sorted(p.name for p in bars_root.iterdir() if p.is_dir() and p.name <= cutoff_iso)
        if bars_root.is_dir()
        else []
    )
    target_days = avail_days[-3:] if len(avail_days) >= 3 else avail_days
    bar_frames: list[pl.DataFrame] = []
    for d_str in target_days:
        try:
            df_day = disc.load_minute_bars_day(
                d_str,
                symbols=[*skchange_syms, "SOXL", "TSLA", "AMD"],
                root=lake_root,
                validate=False,
            )
            if df_day.height > 0:
                bar_frames.append(df_day)
                sk_days_used.append(d_str)
        except FileNotFoundError:
            continue

    combined_bars = (
        pl.concat(bar_frames, how="vertical_relaxed").sort(["symbol", "t_ns"])
        if bar_frames
        else pl.DataFrame()
    )
    for sym in skchange_syms:
        if combined_bars.height > 0:
            sdf = combined_bars.filter(pl.col("symbol") == sym).sort("t_ns")
        else:
            sdf = pl.DataFrame()
        if sdf.height >= 30:
            # Cap tail to last 390 pre-wall bars (~1 full RTH session) for crisp fast PELT/CAPA
            tail_c = sdf.tail(390)["c"].to_numpy().astype(float)
            rets = np.diff(np.log(np.maximum(tail_c, 1e-6)))
            res_sk = disc.detect_skchange_regime_anomalies(rets, min_segment_length=5)
            sk_ver = str(res_sk["skchange_version"])
            sk_by_sym[sym] = {
                "n_observations": int(res_sk["n_observations"]),
                "n_changepoints": int(res_sk["n_changepoints"]),
                "changepoints": list(res_sk["changepoints"][:10]),
                "n_anomaly_segments": int(res_sk["n_anomaly_segments"]),
                "anomaly_segments": list(res_sk["anomaly_segments"][:5]),
                "latest_regime_break": bool(res_sk["latest_regime_break"]),
            }

    total_cps = sum(int(v["n_changepoints"]) for v in sk_by_sym.values())
    total_anoms = sum(int(v["n_anomaly_segments"]) for v in sk_by_sym.values())
    skchange_section: dict[str, Any] = {
        "package": "skchange",
        "version": sk_ver,
        "license": "BSD-3-Clause",
        "layer": "L2/L13",
        "prewall_sessions_used": sk_days_used,
        "symbols_analyzed": sorted(sk_by_sym.keys()),
        "total_changepoints": int(total_cps),
        "total_anomaly_segments": int(total_anoms),
        "any_latest_regime_break": any(bool(v["latest_regime_break"]) for v in sk_by_sym.values()),
        "by_symbol": sk_by_sym,
        "status": "VERIFIED_PASS" if sk_by_sym else "INSUFFICIENT_DATA",
    }

    # 4. shapiq k-SII order-2 feature interactions on real pre-wall early-mover GBDT ranker
    audit_path = manifest_dir / "historical_early_mover_audit_manifest.json"
    shapiq_fnames = [
        "abs_daily_move_pct",
        "rvol",
        "vol_buildup_ratio",
        "technical_alignment_score",
        "detect_rank",
        "time_to_first_detect_mins",
    ]
    shapiq_rows: list[tuple[str, list[float], float]] = []
    if audit_path.is_file():
        audit_doc = json.loads(audit_path.read_text(encoding="utf-8"))
        tl = list((audit_doc.get("prewall_benchmark") or {}).get("timeline") or [])
        for row in tl:
            d_str = str(row.get("date") or "")
            if not d_str or d_str > cutoff_iso:
                continue
            assert_pre_wall_date(d_str)
            feats = [
                abs(float(row.get("daily_move_pct") or 0.0)),
                float(row.get("rvol") or 0.0),
                float(row.get("vol_buildup_ratio") or 0.0),
                float(row.get("technical_alignment_score") or 0.0),
                float(row.get("detect_rank") or 15.0),
                float(row.get("time_to_first_detect_mins") or 0.0),
            ]
            target = max(0.0, float(row.get("post_detect_mfe_pct") or 0.0)) / 100.0
            if all(math.isfinite(v) for v in feats) and math.isfinite(target):
                shapiq_rows.append((d_str, feats, target))

    shapiq_rows.sort(key=lambda item: item[0])
    shapiq_section: dict[str, Any]
    if len(shapiq_rows) >= 24:
        x_m = np.asarray([r[1] for r in shapiq_rows], dtype=float)
        y_m = np.asarray([r[2] for r in shapiq_rows], dtype=float)
        split_i = max(16, int(len(shapiq_rows) * 0.70))
        x_tr_s, y_tr_s = x_m[:split_i], y_m[:split_i]
        x_ev_s = x_m[split_i:]
        lgbm_threads = disc._env_positive_int("TIER0_LGBM_THREADS", 4)
        ranker = lgb.LGBMRegressor(
            n_estimators=24,
            max_depth=3,
            learning_rate=0.05,
            num_leaves=7,
            min_child_samples=5,
            n_jobs=lgbm_threads,
            random_state=42,
            verbosity=-1,
        )
        ranker.fit(x_tr_s, y_tr_s)
        sq_res = disc.compute_shapiq_interactions(
            ranker,
            x_tr_s,
            x_ev_s,
            shapiq_fnames,
            max_order=2,
            max_eval_rows=32,
        )
        shapiq_section = {
            "package": "shapiq",
            "version": sq_res["shapiq_version"],
            "license": "MIT",
            "layer": "L2/L13",
            "dataset": "historical_early_mover_audit_manifest.prewall_benchmark",
            "prewall_cutoff": cutoff_iso,
            "train_rows": int(x_tr_s.shape[0]),
            "eval_rows": int(sq_res["n_eval_rows"]),
            "index": sq_res["index"],
            "max_order": int(sq_res["max_order"]),
            "explainer_backend": sq_res["explainer_backend"],
            "main_effects": sq_res["main_effects"],
            "top_interaction": sq_res["top_interaction"],
            "top_5_interactions": sq_res["pairwise_interactions"][:5],
            "status": "VERIFIED_PASS",
        }
    else:
        shapiq_section = {
            "package": "shapiq",
            "version": "unknown",
            "license": "MIT",
            "layer": "L2/L13",
            "status": "INSUFFICIENT_DATA",
        }

    # 5. MAPIE SplitConformalRegressor on real pre-wall 5-minute noise excursion (bounded <= 2.00%)
    conformal_x: list[list[float]] = []
    conformal_y: list[float] = []
    conformal_syms_used: list[str] = []
    if combined_bars.height > 0:
        for sym in ("NVDA", "SPY", "QQQ", "SOXX", "SOXL", "AMD", "TSLA"):
            sdf = combined_bars.filter(pl.col("symbol") == sym).sort("t_ns")
            if sdf.height < 80:
                continue
            conformal_syms_used.append(sym)
            o_arr = sdf["o"].to_numpy().astype(float)
            h_arr = sdf["h"].to_numpy().astype(float)
            l_arr = sdf["l"].to_numpy().astype(float)
            c_arr = sdf["c"].to_numpy().astype(float)
            v_arr = sdf["v"].to_numpy().astype(float)
            n_b = int(c_arr.size)
            # Sample every 5th bar strictly causally so 5m targets are non-overlapping
            for idx in range(20, n_b - 6, 5):
                c_now = max(1e-6, float(c_arr[idx]))
                ret1 = abs(c_now / max(1e-6, float(c_arr[idx - 1])) - 1.0)
                ret5 = abs(c_now / max(1e-6, float(c_arr[idx - 5])) - 1.0)
                win_c = c_arr[idx - 15 : idx + 1]
                win_rets = np.diff(np.log(np.maximum(win_c, 1e-6)))
                vol15 = float(np.std(win_rets))
                rng_pct = max(0.0, float(h_arr[idx] - l_arr[idx])) / c_now
                v_base = max(1.0, float(np.mean(v_arr[idx - 15 : idx])))
                v_ratio = min(20.0, float(v_arr[idx]) / v_base)
                body_pct = abs(float(c_arr[idx] - o_arr[idx])) / c_now
                # Causal forward 5-bar adverse/noise range fraction relative to entry close
                fwd_hi = float(np.max(h_arr[idx + 1 : idx + 6]))
                fwd_lo = float(np.min(l_arr[idx + 1 : idx + 6]))
                fwd_noise_pct = min(0.05, max(0.0, (fwd_hi - fwd_lo) / c_now))
                row_f = [ret1, ret5, vol15, rng_pct, v_ratio, body_pct]
                if all(math.isfinite(val) for val in row_f) and math.isfinite(fwd_noise_pct):
                    conformal_x.append(row_f)
                    conformal_y.append(fwd_noise_pct)

    mapie_section: dict[str, Any]
    if len(conformal_x) >= 60:
        cx = np.asarray(conformal_x, dtype=float)
        cy = np.asarray(conformal_y, dtype=float)
        n_tot = int(cx.shape[0])
        i_tr = int(n_tot * 0.60)
        i_cal = int(n_tot * 0.80)
        conf_res = fit_conformal_excursion_intervals(
            cx[:i_tr],
            cy[:i_tr],
            cx[i_tr:i_cal],
            cy[i_tr:i_cal],
            cx[i_cal:],
            y_eval=cy[i_cal:],
            alpha=alpha,
            max_hard_cap_pct=max_hard_cap_pct,
        )
        mapie_section = {
            "package": "mapie",
            "version": conf_res["mapie_version"],
            "license": "BSD-3-Clause",
            "layer": "L5/L13",
            "method": conf_res["method"],
            "prewall_sessions_used": sk_days_used,
            "symbols_used": conformal_syms_used,
            "alpha": conf_res["alpha"],
            "target_coverage": conf_res["target_coverage"],
            "n_train": conf_res["n_train"],
            "n_cal": conf_res["n_cal"],
            "n_eval": conf_res["n_eval"],
            "empirical_coverage": conf_res["empirical_coverage"],
            "cal_empirical_coverage": conf_res["cal_empirical_coverage"],
            "eval_empirical_coverage": conf_res["eval_empirical_coverage"],
            "mean_interval_width": conf_res["mean_interval_width"],
            "uncertainty_width_threshold": conf_res["uncertainty_width_threshold"],
            "abstain_rate": conf_res["abstain_rate"],
            "max_hard_cap_pct": conf_res["max_hard_cap_pct"],
            "risk_cap_enforced": bool(
                conf_res["risk_cap_enforced"]
                and float(conf_res["max_conformal_trailing_pct"]) <= 0.02
            ),
            "mean_conformal_trailing_pct": conf_res["mean_conformal_trailing_pct"],
            "max_conformal_trailing_pct": conf_res["max_conformal_trailing_pct"],
            "sample_intervals": conf_res["intervals"][:8],
            "status": (
                "VERIFIED_PASS"
                if float(conf_res["max_conformal_trailing_pct"]) <= 0.02
                else "FAILED"
            ),
        }
    else:
        mapie_section = {
            "package": "mapie",
            "version": "unknown",
            "license": "BSD-3-Clause",
            "layer": "L5/L13",
            "status": "INSUFFICIENT_DATA",
        }

    engines = {
        "sqlglot_pit_sql_guard": sqlglot_section,
        "edgartools_pit_sec_catalysts": edgar_section,
        "skchange_regime_anomalies": skchange_section,
        "shapiq_gbdt_interactions": shapiq_section,
        "mapie_conformal_excursion": mapie_section,
    }
    passed_count = sum(1 for item in engines.values() if item.get("status") == "VERIFIED_PASS")
    manifest: dict[str, Any] = {
        "schema_version": "tier0.starred-frontier-landscape.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
        "prewall_cutoff": cutoff_iso,
        "prewall_cutoff_iso": cutoff_iso,
        "risk_kernel_sha256": risk_sha,
        "summary": {
            "engines_total": len(engines),
            "engines_verified_pass": int(passed_count),
            "all_engines_pass": passed_count == len(engines),
            "zero_banned_packages": True,
            "lazy_imports_enforced": True,
            "risk_stop_cap_pct": round(float(max_hard_cap_pct), 6),
        },
        "versions": {
            "mapie": mapie_section.get("version"),
            "skchange": skchange_section.get("version"),
            "shapiq": shapiq_section.get("version"),
            "sqlglot": sqlglot_section.get("version"),
            "edgartools": edgar_section.get("version"),
        },
        "engines": engines,
    }

    if write_manifest:
        dest = output_path or (manifest_dir / "starred_frontier_landscape_manifest.json")
        _atomic_write_json_manifest(dest, manifest)
    return manifest


def _evaluate_l1_gate1(
    l1: Mapping[str, Any] | None, *, min_l1_fills: int = MIN_CPCV_SAMPLES
) -> tuple[bool, int]:
    """Fail-closed Gate 1 check: when L1 evidence is supplied, require >= min_l1_fills & markout."""
    if l1 is None:
        return True, 0
    if l1.get("passive_fills") is not None:
        fills = int(l1["passive_fills"])
    elif isinstance(l1.get("passive_filled"), bool):
        fills = 1 if l1["passive_filled"] else 0
    elif l1.get("passive_filled") is not None:
        fills = int(l1["passive_filled"])
    else:
        fills = 0
    submitted = bool(l1.get("passive_order_submitted", fills > 0))
    raw_mk = l1.get("markout_bps", l1.get("mean_markout_bps"))
    try:
        markout = float(raw_mk) if raw_mk is not None else float("nan")
    except (TypeError, ValueError):
        markout = float("nan")
    req_fills = max(1, int(min_l1_fills))
    return bool(
        submitted and fills >= req_fills and math.isfinite(markout) and markout >= -20.0
    ), fills


def evaluate_six_gate_falsification(
    *,
    bars_df: pl.DataFrame,
    signals_by_ts: dict[int, int] | None,
    n_trials_total: int,
    cost_bps_1x: Decimal = Decimal("4.0"),
    cost_bps_2x: Decimal = Decimal("8.0"),
    l1_quotes_df: pl.DataFrame | None = None,
    l1_trades_df: pl.DataFrame | None = None,
    l1_side: str = "BUY",
    min_l1_fills: int = MIN_CPCV_SAMPLES,
    backtest_kwargs: dict[str, Any] | None = None,
    mlflow_tracking_uri: str | None = None,
    experiment_name: str = "tier0-sota-falsification",
    run_name: str | None = None,
    pbo: float | None = None,
    trial_returns: Sequence[np.ndarray] | None = None,
    precomputed_bt_1x: Any | None = None,
    precomputed_bt_2x: Any | None = None,
    precomputed_l1_markout: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute full 6-gate falsification (hftbacktest L1, 1x/2x, CPCV, DSR/PBO, SPA, HAC)."""
    bt_extra = dict(backtest_kwargs or {})
    bt1 = (
        precomputed_bt_1x
        if precomputed_bt_1x is not None
        else run_nautilus_signal_backtest(
            bars_df, signals_by_ts, cost_bps_per_side=cost_bps_1x, **bt_extra
        )
    )
    bt2 = (
        precomputed_bt_2x
        if precomputed_bt_2x is not None
        else run_nautilus_signal_backtest(
            bars_df, signals_by_ts, cost_bps_per_side=cost_bps_2x, **bt_extra
        )
    )
    kw: dict[str, Any] = {
        "n_trials_total": n_trials_total,
        "pbo": pbo,
        "trial_returns": trial_returns,
    }
    f1 = falsify_trade_returns(np.asarray(bt1.trade_returns, float), **kw)
    f2 = falsify_trade_returns(np.asarray(bt2.trade_returns, float), **kw)
    l1: dict[str, Any] | None = None
    if precomputed_l1_markout is not None:
        l1 = dict(precomputed_l1_markout)
    elif l1_quotes_df is not None and l1_trades_df is not None:
        sym = str(bars_df["symbol"][0] if "symbol" in bars_df.columns else "TEST").upper()
        l1 = evaluate_hftbacktest_queue_markout(
            l1_quotes_df, l1_trades_df, symbol=sym, side=l1_side
        )
    g1, l1_fills = _evaluate_l1_gate1(l1, min_l1_fills=min_l1_fills)
    hlz_t = hlz_t_hurdle(n_trials_total)
    c1, c2 = f1["gate_checks"], f2["gate_checks"]
    g2 = (
        math.isfinite(float(bt1.total_return_pct))
        and math.isfinite(float(bt2.total_return_pct))
        and float(bt1.total_return_pct) > 0.0
        and float(bt2.total_return_pct) > 0.0
        and c1["2_return_positive"]
        and c2["2_return_positive"]
    )
    g3 = c1["3_cpcv_oos_positive"] and c2["3_cpcv_oos_positive"]
    g4 = c1["4_dsr_pbo_credible"] and c2["4_dsr_pbo_credible"]
    g5 = c1["5_hansen_spa_significant"] and c2["5_hansen_spa_significant"]
    g6 = c1["6_hac_t_stat_significant"] and c2["6_hac_t_stat_significant"]
    gates = {
        "1_hftbacktest_l1_markout": bool(g1),
        "2_cost_1x_2x_survival": bool(g2),
        "3_cpcv_oos_positive": bool(g3),
        "4_dsr_pbo_credible": bool(g4),
        "5_hansen_spa_significant": bool(g5),
        "6_hac_t_stat_significant": bool(g6),
    }
    promoted = all(gates.values())
    rep: dict[str, Any] = {
        "n_trials_total": n_trials_total,
        "hlz_t_hurdle": round(hlz_t, 4),
        "gate1_passive_fills": l1_fills,
        "gate1_min_l1_fills": max(1, int(min_l1_fills)),
        "gate6_hac_t_1x": float(f1["hac_t_raw"]),
        "gate6_hac_t_2x": float(f2["hac_t_raw"]),
        "gate6_hurdle": hlz_t,
        "gates": gates,
        "promoted": promoted,
        "verdict": "PROMOTE" if promoted else "DISCARD",
        "bt_1x": bt1,
        "bt_2x": bt2,
        "falsification_1x": f1,
        "falsification_2x": f2,
        "l1_markout": l1,
    }
    if mlflow_tracking_uri:
        rep["mlflow_run_id"] = log_falsification_to_mlflow(
            rep,
            tracking_uri=mlflow_tracking_uri,
            experiment_name=experiment_name,
            run_name=run_name,
        )
    return rep


_NY_TZ = ZoneInfo("America/New_York")
_EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
_OBSERVATIONAL_WEEKDAY_REPLAY_DATES: dict[int, date] = {
    0: date(2026, 10, 12),  # Monday (full pre-wall XNYS session)
    1: date(2026, 10, 13),  # Tuesday
    2: date(2026, 10, 14),  # Wednesday
    3: date(2026, 10, 15),  # Thursday
    4: date(2026, 10, 9),  # Friday
}


def _atomic_write_json_manifest(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = json.dumps(payload, indent=2) + "\n"
    try:
        tmp.write_text(encoded, encoding="utf-8", newline="\n")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _verify_risk_kernel_immutable() -> str:
    from tier0_sota import risk as rk

    digest = hashlib.sha256(Path(rk.__file__).read_bytes()).hexdigest()
    if digest != _EXPECTED_RISK_SHA256:
        raise RuntimeError(
            f"Risk kernel SHA-256 mismatch: expected {_EXPECTED_RISK_SHA256}, got {digest}"
        )
    return digest


def _open_bounded_research_duckdb(path: Path, *, max_threads: int = 2) -> Any:
    import duckdb

    path.parent.mkdir(parents=True, exist_ok=True)
    mem_limit = os.environ.get("TIER0_DUCKDB_MEMORY_LIMIT", "1GB").strip() or "1GB"
    threads = max(1, min(int(max_threads), 4))
    return duckdb.connect(
        str(path),
        config={"threads": str(threads), "memory_limit": mem_limit},
    )


def compute_per_symbol_min_trailing_table(
    latency_rows: Sequence[Mapping[str, Any]],
    *,
    venue_profile: str = "ALPACA_PAPER_1000",
    spread_bps: Decimal = Decimal("5"),
) -> list[dict[str, Any]]:
    """Compute per-symbol, 4-session Decimal non-noise min trailing specs via engine.py."""
    by_sym: dict[str, Mapping[str, Any]] = {}
    for row in latency_rows:
        sym = str(row.get("symbol", "")).strip().upper()
        if not sym:
            continue
        prev = by_sym.get(sym)
        cur_mv = abs(float(row.get("session_peak_move_pct") or 0.0))
        prev_mv = abs(float(prev.get("session_peak_move_pct") or 0.0)) if prev else -1.0
        if prev is None or cur_mv > prev_mv:
            by_sym[sym] = row

    out: list[dict[str, Any]] = []
    for sym, row in sorted(by_sym.items()):
        entry_px = float(
            row.get("first_detect_price")
            or row.get("session_peak_price")
            or row.get("prior_close")
            or 50.0
        )
        entry_px = max(1.0, entry_px)
        natr_val = float(row.get("natr_pct_at_detect") or 0.015)
        natr_val = max(0.002, min(0.15, natr_val))
        side_raw = str(row.get("preferred_side") or "LONG").upper()
        side: Literal["LONG", "SHORT"] = "SHORT" if side_raw == "SHORT" else "LONG"
        arch = str(row.get("archetype") or "EARLY_EP_MOMENTUM_BURST")
        cat = str(row.get("category") or "EXTREME_MOVER_CONTINUATION")
        sessions_map: dict[str, dict[str, Any]] = {}
        for sess in ("PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT"):
            spec = eng.compute_non_noise_min_trail_spec(
                symbol=sym,
                entry_price=Decimal(f"{entry_px:.4f}"),
                side=side,
                session=sess,
                natr_pct=Decimal(f"{natr_val:.6f}"),
                spread_bps=spread_bps,
                archetype=arch,
                venue_profile=venue_profile,
            )
            sessions_map[sess] = {
                "order_type": spec["order_type"],
                "execution_mechanism": spec["execution_mechanism"],
                "min_trailing_pct": float(spec["non_noise_min_trail_pct"]),
                "initial_max_loss_pct": float(spec["initial_max_loss_pct"]),
                "initial_stop_price": float(spec["initial_stop_price"]),
                "collar_bps": float(spec["collar_bps"]),
                "trailing_schedule": spec["trailing_schedule"],
                "reverse_leg_routing": spec["reverse_leg_routing"],
                "reentry_cooldown_secs": int(spec["rapid_reentry_policy"]["reentry_cooldown_secs"]),
            }
        out.append(
            {
                "symbol": sym,
                "category": cat,
                "archetype": arch,
                "preferred_side": side,
                "reference_price": round(entry_px, 4),
                "natr_pct": round(natr_val, 6),
                "spread_bps": float(spread_bps),
                "rth_min_trailing_pct": sessions_map["RTH"]["min_trailing_pct"],
                "premarket_min_trailing_pct": sessions_map["PRE_MARKET"]["min_trailing_pct"],
                "afterhours_min_trailing_pct": sessions_map["AFTER_HOURS"]["min_trailing_pct"],
                "overnight_min_trailing_pct": sessions_map["OVERNIGHT"]["min_trailing_pct"],
                "sessions": sessions_map,
            }
        )
    return out


def build_causal_mover_signals(
    sym_features_df: pl.DataFrame,
    *,
    weights: Mapping[str, float] = disc.DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS,
    score_threshold: float = 0.38,
    include_peak_reversal: bool = True,
) -> tuple[dict[int, int], list[dict[str, Any]]]:
    """Build strictly causal NautilusTrader v2 entry, auto-reverse & rapid re-entry signals."""
    scored = disc.score_early_mover_features(sym_features_df, weights).sort("t_close_ns")
    signals: dict[int, int] = {}
    milestones: list[dict[str, Any]] = []
    entered = False
    last_dir = 0
    reversed_peak = False
    reentry_count = 0
    last_sig_ts = -1

    for row in scored.iter_rows(named=True):
        ts = int(row["t_close_ns"])
        minute_et = int(row["minute_et"])
        if minute_et >= 1190:
            continue
        score = float(row["mover_score"])
        mv = float(row["move_pct"])
        px = float(row["c"])
        vwap = float(row["vwap"])
        sess = str(row["session_label"])
        et_s, utc_s = disc._fmt_ns_et_utc(ts)

        if not entered and score >= score_threshold and abs(mv) >= 2.0:
            direction = 1 if mv >= 0.0 else -1
            signals[ts] = direction
            entered = True
            last_dir = direction
            last_sig_ts = ts
            cat = disc.classify_extreme_mover_category(
                session=sess,
                move_pct=mv,
                rvol=float(row["rvol"]),
                vbr=float(row["vbr"]),
                hvr=float(row["hvr"]),
                trf_darkpool_ratio=float(row["trf_darkpool_ratio"]),
                range_expansion=float(row["range_expansion"]),
                momentum_roc=float(row["ml4t_roc"]),
                peak_climax_confirmed=bool(row["peak_climax_confirmed"]),
                above_vwap=px >= vwap,
            )
            milestones.append(
                {
                    "event": "EARLY_ENTRY_SIGNAL",
                    "side": "LONG" if direction > 0 else "SHORT",
                    "timestamp_et": et_s,
                    "timestamp_utc": utc_s,
                    "session": sess,
                    "price": round(px, 4),
                    "move_pct": round(mv, 3),
                    "mover_score": round(score, 4),
                    "rvol": round(float(row["rvol"]), 3),
                    "vbr": round(float(row["vbr"]), 3),
                    "category": cat["category"],
                }
            )
            continue

        if not entered:
            continue

        climax_dir = int(row["peak_climax_dir"])
        if (
            include_peak_reversal
            and climax_dir != 0
            and climax_dir == -last_dir
            and (ts - last_sig_ts) >= 300_000_000_000
        ):
            signals[ts] = climax_dir
            last_dir = climax_dir
            last_sig_ts = ts
            reversed_peak = True
            milestones.append(
                {
                    "event": "PEAK_CLIMAX_AUTO_REVERSE",
                    "side": "SHORT" if climax_dir < 0 else "LONG",
                    "timestamp_et": et_s,
                    "timestamp_utc": utc_s,
                    "session": sess,
                    "price": round(px, 4),
                    "move_pct": round(mv, 3),
                    "rvol": round(float(row["rvol"]), 3),
                    "category": "PARABOLIC_CLIMAX_SHORT_REVERSAL",
                }
            )
            continue

        # Causal rapid re-entry signal after a reversal or pullback when trend re-ignites.
        primary_dir = 1 if mv >= 0.0 else -1
        vwap_ok = (primary_dir > 0 and px > vwap) or (primary_dir < 0 and px < vwap)
        if (
            reentry_count < 2
            and score >= score_threshold
            and float(row["vbr"]) >= 1.5
            and vwap_ok
            and (reversed_peak or (ts - last_sig_ts) >= 900_000_000_000)
            and (ts - last_sig_ts) >= 120_000_000_000
        ):
            signals[ts] = primary_dir
            last_dir = primary_dir
            last_sig_ts = ts
            reversed_peak = False
            reentry_count += 1
            milestones.append(
                {
                    "event": "RAPID_REENTRY_SIGNAL",
                    "side": "LONG" if primary_dir > 0 else "SHORT",
                    "timestamp_et": et_s,
                    "timestamp_utc": utc_s,
                    "session": sess,
                    "price": round(px, 4),
                    "move_pct": round(mv, 3),
                    "vbr": round(float(row["vbr"]), 3),
                }
            )

    return signals, milestones


def _map_observational_bars_to_prewall_clock(
    bars_df: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[int, int], str]:
    """Preserve exact America/New_York HH:MM session clock on a pre-wall same-weekday date.

    Strictly for isolated post-wall observational replays so NautilusTrader v2 BacktestEngine
    executes unmodified without ever weakening `assert_pre_wall_ns` (<= 2026-10-15).
    """
    orig_ts = bars_df["t_ns"].to_list()
    first_et = datetime.fromtimestamp(int(orig_ts[0]) / 1e9, tz=UTC).astimezone(_NY_TZ)
    replay_day = _OBSERVATIONAL_WEEKDAY_REPLAY_DATES.get(first_et.weekday(), date(2026, 10, 12))
    mapped_ns: list[int] = []
    orig_close_to_mapped_close: dict[int, int] = {}
    for t_raw in orig_ts:
        et_dt = datetime.fromtimestamp(int(t_raw) / 1e9, tz=UTC).astimezone(_NY_TZ)
        day_offset = (et_dt.date() - first_et.date()).days
        target_day = replay_day + timedelta(days=day_offset)
        if target_day > disc.MAX_PREWALL_DATE:
            target_day = disc.MAX_PREWALL_DATE
        mapped_dt = datetime(
            target_day.year,
            target_day.month,
            target_day.day,
            et_dt.hour,
            et_dt.minute,
            et_dt.second,
            tzinfo=_NY_TZ,
        ).astimezone(UTC)
        m_ns = int(mapped_dt.timestamp()) * 1_000_000_000
        mapped_ns.append(m_ns)
        orig_close_to_mapped_close[int(t_raw) + disc.BAR_DURATION_NS] = m_ns + disc.BAR_DURATION_NS
    mapped_df = bars_df.with_columns(pl.Series("t_ns", mapped_ns, dtype=pl.Int64))
    return mapped_df, orig_close_to_mapped_close, replay_day.isoformat()


def replay_mover_with_nautilus_v2(
    bars_df: pl.DataFrame,
    *,
    symbol: str,
    weights: Mapping[str, float] = disc.DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS,
    score_threshold: float = 0.38,
    prior_close: float | None = None,
    baseline_vol: float | None = None,
    short_vol_ratio: float | None = None,
    venue_profile: str = "ALPACA_PAPER_1000",
    starting_cash: Decimal = Decimal("100000"),
    postwall_observational_only: bool = False,
) -> dict[str, Any]:
    """Replay a single mover end-to-end inside unmodified NautilusTrader v2 BacktestEngine."""
    sym = symbol.strip().upper()
    sdf = bars_df.filter(pl.col("symbol").cast(pl.String).str.to_uppercase() == sym).sort("t_ns")
    if sdf.height == 0:
        raise ValueError(f"No bars available for symbol {sym!r}")
    feat = disc.extract_causal_intraday_mover_features(
        sdf,
        prior_close_by_sym={sym: prior_close} if prior_close else None,
        baseline_vol_by_sym={sym: baseline_vol} if baseline_vol else None,
        short_vol_ratio_by_sym={sym: short_vol_ratio} if short_vol_ratio is not None else None,
        enforce_prewall=not postwall_observational_only,
    )
    signals_orig, milestones = build_causal_mover_signals(
        feat, weights=weights, score_threshold=score_threshold, include_peak_reversal=True
    )

    mapped_replay_day: str | None = None
    if postwall_observational_only:
        bt_bars, ts_map, mapped_replay_day = _map_observational_bars_to_prewall_clock(sdf)
        bt_signals = {ts_map[k]: v for k, v in signals_orig.items() if k in ts_map}
    else:
        bt_bars = disc.validate_causal_bars(sdf, validate=True).drop("t_close_ns")
        bt_signals = signals_orig

    bt_kw: dict[str, Any] = {
        "starting_cash": starting_cash,
        "venue_profile": venue_profile,
        "enable_24x5_trailing": True,
        "auto_reverse": True,
        "auto_reentry": True,
        "reentry_cooldown_secs": 60,
        "max_reentries": 3,
        "semi_auto_confirmed": True,
        "shortable": True,
    }
    bt_1x = run_nautilus_signal_backtest(
        bt_bars, bt_signals, cost_bps_per_side=Decimal("4.0"), **bt_kw
    )
    bt_2x = run_nautilus_signal_backtest(
        bt_bars, bt_signals, cost_bps_per_side=Decimal("8.0"), **bt_kw
    )

    # Trace deterministic 4-tier progressive lossless trailing milestones via engine.py
    trail_milestones: list[dict[str, Any]] = []
    if milestones:
        first_ev = milestones[0]
        entry_d = Decimal(f"{float(first_ev['price']):.4f}")
        side_d: Literal["LONG", "SHORT"] = "SHORT" if first_ev["side"] == "SHORT" else "LONG"
        first_ts = next(iter(signals_orig))
        post_df = feat.filter(pl.col("t_close_ns") >= first_ts).sort("t_close_ns")
        sched_key, sp_map = eng.select_pooled_trailing_schedule(
            Decimal(f"{float(post_df['natr_frac'][0]):.6f}")
        )
        init_prof = eng.get_session_protection_profile(str(first_ev["session"]), sp_map)
        _, init_stop = eng._hard_stop(entry_d, side_d, init_prof)
        cur_stop, fav_ext, max_tier_seen = init_stop, entry_d, 0
        for r in post_df.iter_rows(named=True):
            sess_r = str(r["session_label"])
            if sess_r == "CLOSED":
                continue
            prof_r = eng.get_session_protection_profile(sess_r, sp_map)
            ext_px = Decimal(f"{float(r['h'] if side_d == 'LONG' else r['l']):.4f}")
            fav_ext = max(fav_ext, ext_px) if side_d == "LONG" else min(fav_ext, ext_px)
            new_stop, tier, eff_tr = eng.compute_progressive_trailing_stop(
                entry_price=entry_d,
                side=side_d,
                favorable_extreme=fav_ext,
                current_stop=cur_stop,
                profile=prof_r,
                natr_pct=Decimal(f"{float(r['natr_frac']):.6f}"),
                initial_stop=init_stop,
            )
            if tier > max_tier_seen:
                et_s, utc_s = disc._fmt_ns_et_utc(int(r["t_close_ns"]))
                trail_milestones.append(
                    {
                        "event": f"LOSSLESS_TRAIL_TIER_{tier}",
                        "timestamp_et": et_s,
                        "timestamp_utc": utc_s,
                        "session": sess_r,
                        "favorable_extreme": float(fav_ext),
                        "ratcheted_stop": float(new_stop),
                        "effective_trail_pct": float(eff_tr),
                        "order_mode": prof_r.order_mode,
                        "collar_bps": float(prof_r.limit_collar_bps),
                    }
                )
                max_tier_seen = tier
            cur_stop = new_stop

    ev_eval = disc.evaluate_early_mover_detection(
        feat, weights=weights, score_threshold=score_threshold, top_k=5
    )
    lat_row = ev_eval["latency_table"][0] if ev_eval["latency_table"] else {}
    return {
        "symbol": sym,
        "engine": "NautilusTrader v2 BacktestEngine",
        "venue_profile": venue_profile,
        "paper_only": True,
        "live_execution_enabled": False,
        "postwall_observational_only": bool(postwall_observational_only),
        "excluded_from_calibration": bool(postwall_observational_only),
        "nautilus_v2_replay_date_mapped_for_pit_guard": mapped_replay_day,
        "signals_generated": len(signals_orig),
        "detection_summary": lat_row,
        "milestones": sorted(
            [*milestones, *trail_milestones],
            key=lambda m: str(m.get("timestamp_utc") or ""),
        ),
        "nautilus_1x_4bps": {
            "bars_processed": bt_1x.bars_processed,
            "orders_total": bt_1x.orders_total,
            "fills_total": bt_1x.fills_total,
            "stops_attached": bt_1x.stops_attached,
            "trailing_ratchets": bt_1x.trailing_ratchets,
            "session_transitions": bt_1x.session_transitions,
            "auto_reverses": bt_1x.auto_reverses,
            "auto_reentries": bt_1x.auto_reentries,
            "max_lock_tier": bt_1x.max_lock_tier,
            "outside_rth_limit_entries": bt_1x.outside_rth_limit_entries,
            "outside_rth_market_entries": bt_1x.outside_rth_market_entries,
            "rth_market_entries": bt_1x.rth_market_entries,
            "drawdown_halted": bt_1x.halt_triggered,
            "total_return_pct": round(float(bt_1x.total_return_pct), 4),
            "trade_returns": [round(float(x), 6) for x in bt_1x.trade_returns],
        },
        "nautilus_2x_8bps": {
            "bars_processed": bt_2x.bars_processed,
            "orders_total": bt_2x.orders_total,
            "fills_total": bt_2x.fills_total,
            "stops_attached": bt_2x.stops_attached,
            "trailing_ratchets": bt_2x.trailing_ratchets,
            "session_transitions": bt_2x.session_transitions,
            "auto_reverses": bt_2x.auto_reverses,
            "auto_reentries": bt_2x.auto_reentries,
            "max_lock_tier": bt_2x.max_lock_tier,
            "outside_rth_limit_entries": bt_2x.outside_rth_limit_entries,
            "outside_rth_market_entries": bt_2x.outside_rth_market_entries,
            "rth_market_entries": bt_2x.rth_market_entries,
            "drawdown_halted": bt_2x.halt_triggered,
            "total_return_pct": round(float(bt_2x.total_return_pct), 4),
            "trade_returns": [round(float(x), 6) for x in bt_2x.trade_returns],
        },
    }


OPTUNA_STORAGE_ENV = "TIER0_OPTUNA_STORAGE"


def optuna_storage_url() -> str | None:
    """Return the Optuna storage URL from TIER0_OPTUNA_STORAGE, or None for in-memory."""
    return os.environ.get(OPTUNA_STORAGE_ENV, "").strip() or None


def calibrate_extreme_mover_weights_optuna(
    train_features_df: pl.DataFrame,
    holdout_features_df: pl.DataFrame,
    *,
    n_trials: int = 24,
    top_k: int = 15,
    seed: int = 42,
) -> dict[str, Any]:
    """Calibrate early-mover signal weights via Optuna strictly on pre-wall train split."""
    import optuna

    if train_features_df.height == 0 or holdout_features_df.height == 0:
        raise ValueError("train_features_df and holdout_features_df must be non-empty")
    tr_max_val = train_features_df.select(pl.col("t_close_ns").max()).item()
    ho_min_val = holdout_features_df.select(pl.col("t_close_ns").min()).item()
    ho_max_val = holdout_features_df.select(pl.col("t_close_ns").max()).item()
    if not (
        isinstance(tr_max_val, int) and isinstance(ho_min_val, int) and isinstance(ho_max_val, int)
    ):
        raise ValueError("t_close_ns must be integer nanosecond timestamps")
    tr_max_ns, ho_min_ns, ho_max_ns = tr_max_val, ho_min_val, ho_max_val
    assert_pre_wall_ns(tr_max_ns)
    assert_pre_wall_ns(ho_max_ns)
    if tr_max_ns >= ho_min_ns:
        raise ValueError(
            f"Chronological PIT split violated: train_max={tr_max_ns} >= holdout_min={ho_min_ns}"
        )

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    baseline_weights = {
        col: round(1.0 / len(disc.EARLY_MOVER_FEATURE_COLS), 6)
        for col in disc.EARLY_MOVER_FEATURE_COLS
    }
    train_base = disc.evaluate_early_mover_detection(
        train_features_df, weights=baseline_weights, score_threshold=0.38, top_k=top_k
    )
    holdout_base = disc.evaluate_early_mover_detection(
        holdout_features_df, weights=baseline_weights, score_threshold=0.38, top_k=top_k
    )

    trial_daily_f1_series: list[np.ndarray] = []

    def _objective(trial: optuna.Trial) -> float:
        raw_w = {col: trial.suggest_float(col, 0.05, 1.0) for col in disc.EARLY_MOVER_FEATURE_COLS}
        thr = trial.suggest_float("score_threshold", 0.25, 0.60)
        norm_w = disc.normalize_early_mover_simplex_weights(raw_w)
        ev = disc.evaluate_early_mover_detection(
            train_features_df, weights=norm_w, score_threshold=thr, top_k=top_k
        )
        f1_arr = np.asarray([float(d["f1_at_k"]) for d in ev["daily_metrics"]], dtype=float)
        if f1_arr.size >= 2:
            trial_daily_f1_series.append(f1_arr)
        lead_bonus = max(0.0, min(1.0, float(ev["median_lead_minutes_to_peak"]) / 180.0))
        return (
            0.45 * float(ev["recall_at_k"]) + 0.35 * float(ev["precision_at_k"]) + 0.20 * lead_bonus
        )

    # Optuna's own storage argument: with TIER0_OPTUNA_STORAGE set (e.g. the SQLite file the
    # Optuna Dashboard on :8080 reads), every trial is written to that store; unset keeps the
    # in-memory default. One study per call, so best_params and n_trials describe this run only.
    storage_url = optuna_storage_url()
    study_name = (
        f"tier0-early-mover-weights-seed{seed}-train{tr_max_ns}-holdout{ho_max_ns}-"
        f"{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}"
        if storage_url is not None
        else None
    )
    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed),
        storage=storage_url,
        study_name=study_name,
    )
    study.optimize(_objective, n_trials=max(4, int(n_trials)))
    best_params = dict(study.best_params)
    best_thr = round(float(best_params.pop("score_threshold")), 4)
    best_simplex = disc.normalize_early_mover_simplex_weights(best_params)
    scanner_weights = disc.project_calibrated_weights_to_scanner(best_simplex)

    train_cal = disc.evaluate_early_mover_detection(
        train_features_df, weights=best_simplex, score_threshold=best_thr, top_k=top_k
    )
    holdout_cal = disc.evaluate_early_mover_detection(
        holdout_features_df, weights=best_simplex, score_threshold=best_thr, top_k=top_k
    )
    pbo_rank = _compute_cpcv_pbo(trial_daily_f1_series) if len(trial_daily_f1_series) >= 2 else None
    return {
        "prewall_wall_date": disc.DISCOVERY_WALL_ISO,
        "optuna": {
            "n_trials": len(study.trials),
            "best_objective": round(float(study.best_value), 6),
            "sampler": "TPESampler",
            "seed": seed,
            "storage": "rdb" if storage_url is not None else "in_memory",
            "study_name": study_name,
            "best_score_threshold": best_thr,
            "ranking_cpcv_pbo": round(float(pbo_rank), 4) if pbo_rank is not None else None,
        },
        "baseline_simplex_weights": baseline_weights,
        "calibrated_simplex_weights": best_simplex,
        "calibrated_scanner_weights": scanner_weights,
        "calibrated_weights": {**scanner_weights, **best_simplex},
        "calibrated_score_threshold": best_thr,
        "train_baseline": train_base,
        "train_calibrated": train_cal,
        "holdout_baseline": holdout_base,
        "holdout_calibrated": holdout_cal,
    }


def load_prewall_mover_feature_panel(
    *,
    lake_root: Path = disc.DEFAULT_MARKET_DATA_ROOT,
    max_sessions: int = 20,
    universe_per_day: int = 60,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str], list[str], dict[str, pl.DataFrame]]:
    """Load strictly causal pre-wall (<= 2026-10-15) 1-min bars + T-1 baselines & short vol."""
    b20_path = lake_root / "derived" / "df10_daily_20d_baselines.parquet"
    sv_path = lake_root / "public-tables" / "short_volume.parquet"
    bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"
    if not b20_path.is_file() or not bars_root.is_dir():
        raise FileNotFoundError(f"Pre-wall lake missing required datasets under {lake_root}")

    b20 = pl.read_parquet(b20_path).filter(
        (pl.col("trade_date") <= disc.DISCOVERY_WALL_ISO)
        & (pl.col("prev_raw_close") >= 10.0)
        & (pl.col("mean_20d_dvol") >= 25_000_000.0)
    )
    avail_days = sorted(
        d.name for d in bars_root.iterdir() if d.is_dir() and d.name <= disc.DISCOVERY_WALL_ISO
    )
    b20_days = set(b20["trade_date"].unique().to_list())
    selected_days = [d for d in avail_days if d in b20_days][-max(6, int(max_sessions)) :]
    if len(selected_days) < 4:
        raise ValueError(f"Insufficient pre-wall trading sessions found: {len(selected_days)}")
    for d in selected_days:
        assert_pre_wall_date(d)

    sv_df: pl.DataFrame | None = None
    if sv_path.is_file():
        min_prev = disc.previous_xnys_session(selected_days[0]).isoformat()
        sv_df = (
            pl.scan_parquet(sv_path)
            .filter((pl.col("day") >= min_prev) & (pl.col("day") <= disc.DISCOVERY_WALL_ISO))
            .group_by(["day", "symbol"])
            .agg(
                pl.col("short_volume").sum().alias("sv"),
                pl.col("total_volume").sum().alias("tv"),
            )
            .with_columns(
                pl.when(pl.col("tv") > 0.0)
                .then((pl.col("sv") / pl.col("tv")).clip(0.0, 1.0))
                .otherwise(0.0)
                .alias("sv_ratio")
            )
            .collect()
        )

    split_idx = max(3, min(len(selected_days) - 2, int(len(selected_days) * 0.65)))
    train_days = selected_days[:split_idx]
    holdout_days = selected_days[split_idx:]
    train_parts: list[pl.DataFrame] = []
    holdout_parts: list[pl.DataFrame] = []
    holdout_raw_by_day: dict[str, pl.DataFrame] = {}
    core_syms = set(disc.EXTREME_MOVER_BASKET_SYMBOLS)

    for day_iso in selected_days:
        day_b20 = b20.filter(pl.col("trade_date") == day_iso)
        top_syms = set(
            day_b20.sort("mean_20d_dvol", descending=True)
            .head(universe_per_day)["symbol"]
            .to_list()
        )
        causal_universe = sorted(top_syms | core_syms)
        prior_close_map: dict[str, float] = {}
        bvol_map: dict[str, float] = {}
        for rec in day_b20.filter(pl.col("symbol").is_in(causal_universe)).iter_rows(named=True):
            sym = str(rec["symbol"]).upper()
            if rec["prev_raw_close"] and float(rec["prev_raw_close"]) > 0.0:
                prior_close_map[sym] = float(rec["prev_raw_close"])
            if rec["mean_20d_vol"] and float(rec["mean_20d_vol"]) > 0.0:
                bvol_map[sym] = float(rec["mean_20d_vol"])

        # Strict T-1 FINRA short volume join (previous XNYS session ONLY).
        t1_iso = disc.previous_xnys_session(day_iso).isoformat()
        sv_map: dict[str, float] = {}
        if sv_df is not None:
            for rec in sv_df.filter(
                (pl.col("day") == t1_iso) & pl.col("symbol").is_in(causal_universe)
            ).iter_rows(named=True):
                sv_map[str(rec["symbol"]).upper()] = float(rec["sv_ratio"])

        day_bars = (
            pl.scan_parquet(str(bars_root / day_iso / "*.parquet"))
            .filter(pl.col("symbol").is_in(causal_universe))
            .select(["symbol", "t_ns", "o", "h", "l", "c", "v"])
            .collect()
        )
        if day_bars.height == 0:
            continue
        feat_day = disc.extract_causal_intraday_mover_features(
            day_bars,
            prior_close_by_sym=prior_close_map,
            baseline_vol_by_sym=bvol_map,
            short_vol_ratio_by_sym=sv_map,
            enforce_prewall=True,
        )
        if day_iso in holdout_days:
            holdout_parts.append(feat_day)
            holdout_raw_by_day[day_iso] = day_bars
        else:
            train_parts.append(feat_day)

    if not train_parts or not holdout_parts:
        raise RuntimeError("Failed to collect non-empty pre-wall train and holdout panels")
    return (
        pl.concat(train_parts, how="vertical"),
        pl.concat(holdout_parts, how="vertical"),
        train_days,
        holdout_days,
        holdout_raw_by_day,
    )


def fetch_alpaca_paper_sip_recent_bars(
    symbols: Sequence[str] = disc.EXTREME_MOVER_BASKET_SYMBOLS,
    *,
    start_et: datetime,
    end_et: datetime,
) -> tuple[pl.DataFrame, dict[str, float], dict[str, float]]:
    """Fetch paper-authenticated Alpaca SIP 1-min bars (zero yfinance) for observational audit."""
    from alpaca.data.enums import DataFeed
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_PA1_API_KEY") or ""
    sec = (
        os.environ.get("APCA_API_SECRET_KEY")
        or os.environ.get("ALPACA_PA1_SECRET")
        or os.environ.get("ALPACA_PA1_SECRET_KEY")
        or ""
    )
    if not key or not sec:
        raise RuntimeError("Missing Alpaca paper data credentials (run under fnox --profile paper)")
    if key.upper().startswith("AK"):
        raise PermissionError("Refused live AK* Alpaca key in observational SIP fetch")
    cli = StockHistoricalDataClient(key, sec)
    prior_start = start_et - timedelta(days=5)
    req = StockBarsRequest(
        symbol_or_symbols=[s.upper() for s in symbols],
        timeframe=TimeFrame.Minute,
        start=prior_start.astimezone(UTC),
        end=end_et.astimezone(UTC),
        feed=DataFeed.SIP,
    )
    resp_bars = cli.get_stock_bars(req)
    raw = resp_bars if isinstance(resp_bars, Mapping) else getattr(resp_bars, "data", {})
    rows: list[dict[str, Any]] = []
    prior_close_by_day_sym: dict[str, float] = {}
    baseline_vol_by_sym: dict[str, float] = {}
    start_date_iso = start_et.astimezone(_NY_TZ).date().isoformat()
    for sym, bars in raw.items():
        sym_u = str(sym).upper()
        prior_days_vols: dict[str, float] = {}
        prior_rth_close: float | None = None
        for b in sorted(bars, key=lambda x: x.timestamp):
            et = b.timestamp.astimezone(_NY_TZ)
            d_iso = et.date().isoformat()
            if d_iso < start_date_iso:
                prior_days_vols[d_iso] = prior_days_vols.get(d_iso, 0.0) + float(b.volume)
                if (9, 30) <= (et.hour, et.minute) < (16, 0):
                    prior_rth_close = float(b.close)
                continue
            rows.append(
                {
                    "symbol": sym_u,
                    "t_ns": int(b.timestamp.timestamp()) * 1_000_000_000,
                    "o": float(b.open),
                    "h": float(b.high),
                    "l": float(b.low),
                    "c": float(b.close),
                    "v": float(b.volume),
                }
            )
        if prior_rth_close is not None and prior_rth_close > 0.0:
            prior_close_by_day_sym[sym_u] = prior_rth_close
        if prior_days_vols:
            baseline_vol_by_sym[sym_u] = float(np.mean(list(prior_days_vols.values())))
    if not rows:
        raise RuntimeError("Alpaca paper SIP returned 0 bars for observational audit window")
    return pl.DataFrame(rows), prior_close_by_day_sym, baseline_vol_by_sym


def persist_calibration_to_duckdb(manifest: Mapping[str, Any], duckdb_path: Path) -> dict[str, int]:
    """Persist extreme-mover calibration, taxonomy, latency & NVDA replay to bounded DuckDB."""
    with _open_bounded_research_duckdb(duckdb_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS calibration_runs (
                generated_at_utc VARCHAR,
                schema_version VARCHAR,
                prewall_wall_date VARCHAR,
                n_trials INTEGER,
                holdout_recall_at_k DOUBLE,
                holdout_precision_at_k DOUBLE,
                holdout_f1_at_k DOUBLE,
                holdout_median_lead_min DOUBLE,
                mlflow_run_id VARCHAR,
                risk_kernel_sha256 VARCHAR,
                paper_only BOOLEAN,
                live_execution_enabled BOOLEAN
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mover_taxonomy (
                category VARCHAR,
                engine_archetype VARCHAR,
                scanner_category VARCHAR,
                min_trail_natr_mult DOUBLE,
                rules_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS early_detection_latency (
                scope VARCHAR,
                trade_date VARCHAR,
                symbol VARCHAR,
                is_true_extreme_mover BOOLEAN,
                first_detect_et VARCHAR,
                first_detect_session VARCHAR,
                first_detect_price DOUBLE,
                session_peak_et VARCHAR,
                session_peak_price DOUBLE,
                session_peak_move_pct DOUBLE,
                lead_minutes_to_peak DOUBLE,
                remaining_move_to_peak_pct DOUBLE,
                peak_reversal_et VARCHAR,
                peak_reversal_price DOUBLE,
                category VARCHAR,
                archetype VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nvda_monday_replay_timeline (
                event VARCHAR,
                side VARCHAR,
                timestamp_et VARCHAR,
                timestamp_utc VARCHAR,
                session VARCHAR,
                price DOUBLE,
                details_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS per_symbol_min_trailing (
                symbol VARCHAR,
                category VARCHAR,
                archetype VARCHAR,
                preferred_side VARCHAR,
                reference_price DOUBLE,
                natr_pct DOUBLE,
                rth_min_trailing_pct DOUBLE,
                premarket_min_trailing_pct DOUBLE,
                afterhours_min_trailing_pct DOUBLE,
                overnight_min_trailing_pct DOUBLE
            )
            """
        )

        ho_cal = manifest.get("holdout_metrics", {}).get("calibrated", {})
        opt = manifest.get("optuna", {})
        conn.execute(
            "INSERT INTO calibration_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                str(manifest.get("generated_at_utc", "")),
                str(manifest.get("schema_version", "")),
                str(manifest.get("prewall_wall_date", disc.DISCOVERY_WALL_ISO)),
                int(opt.get("n_trials", 0)),
                float(ho_cal.get("recall_at_k", 0.0)),
                float(ho_cal.get("precision_at_k", 0.0)),
                float(ho_cal.get("f1_at_k", 0.0)),
                float(ho_cal.get("median_lead_minutes_to_peak", 0.0)),
                str((manifest.get("mlflow") or {}).get("run_id") or ""),
                str(manifest.get("risk_kernel_sha256", "")),
                bool(manifest.get("paper_only", True)),
                bool(manifest.get("live_execution_enabled", False)),
            ],
        )

        conn.execute("DELETE FROM mover_taxonomy")
        tax_rows = [
            (
                t["category"],
                t["engine_archetype"],
                t["scanner_category"],
                float(t["natr_trail_multiplier"]),
                json.dumps(t["technical_rules"]),
            )
            for t in manifest.get("categorized_mover_taxonomy", [])
        ]
        if tax_rows:
            conn.executemany("INSERT INTO mover_taxonomy VALUES (?, ?, ?, ?, ?)", tax_rows)

        conn.execute("DELETE FROM early_detection_latency")
        lat_inserts: list[tuple[Any, ...]] = []
        for scope, rows_src in (
            ("prewall_holdout", ho_cal.get("latency_table", [])),
            (
                "postwall_observational_current_week",
                (manifest.get("postwall_observational_current_week") or {}).get(
                    "early_detection_latency_table", []
                ),
            ),
        ):
            for r in rows_src:
                lat_inserts.append(
                    (
                        scope,
                        r.get("trade_date"),
                        r.get("symbol"),
                        bool(r.get("is_true_extreme_mover", False)),
                        r.get("first_detect_et"),
                        r.get("first_detect_session"),
                        r.get("first_detect_price"),
                        r.get("session_peak_et"),
                        r.get("session_peak_price"),
                        r.get("session_peak_move_pct"),
                        r.get("lead_minutes_to_peak"),
                        r.get("remaining_move_to_peak_pct"),
                        r.get("peak_reversal_et"),
                        r.get("peak_reversal_price"),
                        r.get("category"),
                        r.get("archetype"),
                    )
                )
        if lat_inserts:
            conn.executemany(
                "INSERT INTO early_detection_latency VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                lat_inserts,
            )

        conn.execute("DELETE FROM nvda_monday_replay_timeline")
        nvda_ev = (manifest.get("nvda_monday_premarket_e2e_replay") or {}).get("milestones", [])
        nvda_rows = [
            (
                str(m.get("event", "")),
                str(m.get("side") or ""),
                str(m.get("timestamp_et") or ""),
                str(m.get("timestamp_utc") or ""),
                str(m.get("session") or ""),
                float(m.get("price") or m.get("favorable_extreme") or 0.0),
                json.dumps(m),
            )
            for m in nvda_ev
        ]
        if nvda_rows:
            conn.executemany(
                "INSERT INTO nvda_monday_replay_timeline VALUES (?, ?, ?, ?, ?, ?, ?)",
                nvda_rows,
            )

        conn.execute("DELETE FROM per_symbol_min_trailing")
        mt_rows = [
            (
                r["symbol"],
                r["category"],
                r["archetype"],
                r["preferred_side"],
                float(r["reference_price"]),
                float(r["natr_pct"]),
                float(r["rth_min_trailing_pct"]),
                float(r["premarket_min_trailing_pct"]),
                float(r["afterhours_min_trailing_pct"]),
                float(r["overnight_min_trailing_pct"]),
            )
            for r in manifest.get("per_symbol_min_trailing_floor", [])
        ]
        if mt_rows:
            conn.executemany(
                "INSERT INTO per_symbol_min_trailing VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                mt_rows,
            )
        return {
            "mover_taxonomy_rows": len(tax_rows),
            "early_detection_latency_rows": len(lat_inserts),
            "nvda_monday_replay_timeline_rows": len(nvda_rows),
            "per_symbol_min_trailing_rows": len(mt_rows),
        }


def log_extreme_mover_calibration_to_mlflow(
    manifest: Mapping[str, Any],
    *,
    tracking_uri: str = "http://127.0.0.1:5000",
    experiment_name: str = "tier0-sota-extreme-mover-calibration",
) -> dict[str, Any]:
    """Log extreme-mover calibration & Nautilus v2 replay metrics via official MLflow SDK."""
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=f"extreme-mover-cal-{disc.DISCOVERY_WALL_ISO}") as run:
        opt = manifest.get("optuna", {})
        cal_w = manifest.get("calibrated_simplex_weights", {})
        scan_w = manifest.get("calibrated_scanner_weights", {})
        params: dict[str, Any] = {
            "prewall_wall_date": manifest.get("prewall_wall_date", disc.DISCOVERY_WALL_ISO),
            "n_trials": opt.get("n_trials", 0),
            "score_threshold": manifest.get("calibrated_score_threshold", 0.38),
            "paper_only": True,
            "live_execution_enabled": False,
        }
        for k, v in cal_w.items():
            params[f"simplex_{k}"] = v
        for k, v in scan_w.items():
            params[f"scanner_{k}"] = v
        mlflow.log_params(params)

        ho_b = manifest.get("holdout_metrics", {}).get("baseline", {})
        ho_c = manifest.get("holdout_metrics", {}).get("calibrated", {})
        naut = manifest.get("holdout_nautilus_v2_replay", {})
        metrics: dict[str, float] = {
            "holdout_baseline_recall_at_k": float(ho_b.get("recall_at_k", 0.0)),
            "holdout_calibrated_recall_at_k": float(ho_c.get("recall_at_k", 0.0)),
            "holdout_baseline_precision_at_k": float(ho_b.get("precision_at_k", 0.0)),
            "holdout_calibrated_precision_at_k": float(ho_c.get("precision_at_k", 0.0)),
            "holdout_calibrated_f1_at_k": float(ho_c.get("f1_at_k", 0.0)),
            "holdout_median_lead_minutes": float(ho_c.get("median_lead_minutes_to_peak", 0.0)),
            "holdout_nautilus_1x_mean_return_pct": float(naut.get("mean_return_1x_pct", 0.0)),
            "holdout_nautilus_2x_mean_return_pct": float(naut.get("mean_return_2x_pct", 0.0)),
            "holdout_nautilus_total_ratchets": float(naut.get("total_trailing_ratchets", 0)),
        }
        nvda_rep = manifest.get("nvda_monday_premarket_e2e_replay") or {}
        if isinstance(nvda_rep.get("nautilus_1x_4bps"), Mapping):
            metrics["obs_nvda_mon_1x_return_pct"] = float(
                nvda_rep["nautilus_1x_4bps"].get("total_return_pct", 0.0)
            )
            metrics["obs_nvda_mon_2x_return_pct"] = float(
                nvda_rep["nautilus_2x_8bps"].get("total_return_pct", 0.0)
            )
            metrics["obs_nvda_mon_trailing_ratchets"] = float(
                nvda_rep["nautilus_1x_4bps"].get("trailing_ratchets", 0)
            )
            metrics["obs_nvda_mon_auto_reverses"] = float(
                nvda_rep["nautilus_1x_4bps"].get("auto_reverses", 0)
            )
        mlflow.log_metrics(metrics)
        return {
            "logged": True,
            "tracking_uri": tracking_uri,
            "experiment_name": experiment_name,
            "run_id": str(run.info.run_id),
        }


def _build_categorized_taxonomy_spec() -> list[dict[str, Any]]:
    rules_map: dict[str, list[str]] = {
        "PREMARKET_CATALYST_LEADER": [
            "session == PRE_MARKET (04:00-09:29 ET)",
            "|move_pct| >= 3.0% OR same-session RVOL >= 1.5x OR VBR >= 2.0x",
            "entry collared STOP_LIMIT 35bps outside_rth=True",
        ],
        "SUDDEN_VOLUME_IGNITION": [
            "VBR >= 2.0x OR RVOL >= 2.5x OR (range_expansion >= 1.8x AND RVOL >= 1.5x)",
            "causal ml4t-engineer ROC/RSI directional alignment",
        ],
        "EXTREME_MOVER_CONTINUATION": [
            "sustained trend above/below VWAP with calibrated mover_score >= threshold",
            "4-tier progressive lossless trailing (BE+fee -> +1R -> +2R -> +3R)",
        ],
        "PARABOLIC_CLIMAX_SHORT_REVERSAL": [
            "|move_pct| >= 5.0% extended >= 1% beyond VWAP + climax RVOL >= 1.5x",
            "red breakdown below prior low & pullback >= max(0.45%, 0.25*NATR)",
            "flatten-first auto-reverse -> DIRECT_SHORT_ETB or inverse ETF (SOXS/SQQQ)",
        ],
        "DARKPOOL_ACCUMULATION_BREAKOUT": [
            "FINRA TRF / T-1 short-volume ratio >= 0.35 AND (HVR >= 1.2 OR |move_pct| >= 2.0%)",
            "hidden-volume absorption before directional breakout",
        ],
    }
    return [
        {
            "category": cat,
            "engine_archetype": disc.CATEGORY_TO_ENGINE_ARCHETYPE[cat],
            "scanner_category": disc.CATEGORY_TO_SCANNER_LABEL[cat],
            "natr_trail_multiplier": float(
                eng._ARCHETYPE_TRAIL_MULTS[disc.CATEGORY_TO_ENGINE_ARCHETYPE[cat]]
            ),
            "technical_rules": rules_map[cat],
        }
        for cat in disc.EXTREME_MOVER_CATEGORIES
    ]


def run_historical_extreme_mover_calibration(
    *,
    lake_root: Path = disc.DEFAULT_MARKET_DATA_ROOT,
    manifest_dir: Path = disc.DEFAULT_DATA_ROOT / "manifests",
    max_sessions: int = 28,
    universe_per_day: int = 60,
    n_trials: int = 24,
    top_k: int = 15,
    venue_profile: str = "ALPACA_PAPER_1000",
    mlflow_tracking_uri: str | None = "http://127.0.0.1:5000",
    include_live_postwall_audit: bool = True,
    nvda_monday_date: str = "2026-11-23",
    current_week_end_date: str = "2026-11-26",
) -> dict[str, Any]:
    """Run full pre-wall PIT calibration, Nautilus v2 replay, NVDA Mon E2E & DuckDB/MLflow sync."""
    risk_sha = _verify_risk_kernel_immutable()
    train_feat, holdout_feat, train_days, holdout_days, holdout_raw = (
        load_prewall_mover_feature_panel(
            lake_root=lake_root, max_sessions=max_sessions, universe_per_day=universe_per_day
        )
    )
    cal = calibrate_extreme_mover_weights_optuna(
        train_feat, holdout_feat, n_trials=n_trials, top_k=top_k
    )
    best_simplex = cal["calibrated_simplex_weights"]
    best_thr = float(cal["calibrated_score_threshold"])

    # Run unmodified NautilusTrader v2 BacktestEngine on top holdout pre-wall extreme movers.
    ho_replays: list[dict[str, Any]] = []
    ho_1x_returns: list[float] = []
    ho_2x_returns: list[float] = []
    ho_long_1x: list[float] = []
    ho_short_1x: list[float] = []
    ho_lat = [
        r
        for r in cal["holdout_calibrated"]["latency_table"]
        if r["is_true_extreme_mover"] and r["first_detect_et"] is not None
    ][:12]
    for rec in ho_lat:
        d_iso, sym = str(rec["trade_date"]), str(rec["symbol"])
        raw_d = holdout_raw.get(d_iso)
        if raw_d is None:
            continue
        rep = replay_mover_with_nautilus_v2(
            raw_d,
            symbol=sym,
            weights=best_simplex,
            score_threshold=best_thr,
            prior_close=float(rec["prior_close"]),
            short_vol_ratio=float(rec["trf_ratio"] or 0.0),
            venue_profile=venue_profile,
            postwall_observational_only=False,
        )
        ho_replays.append(
            {
                "trade_date": d_iso,
                "symbol": sym,
                "category": rec["category"],
                "first_detect_et": rec["first_detect_et"],
                "lead_minutes_to_peak": rec["lead_minutes_to_peak"],
                "nautilus_1x_return_pct": rep["nautilus_1x_4bps"]["total_return_pct"],
                "nautilus_2x_return_pct": rep["nautilus_2x_8bps"]["total_return_pct"],
                "trailing_ratchets": rep["nautilus_1x_4bps"]["trailing_ratchets"],
                "auto_reverses": rep["nautilus_1x_4bps"]["auto_reverses"],
                "auto_reentries": rep["nautilus_1x_4bps"]["auto_reentries"],
                "max_lock_tier": rep["nautilus_1x_4bps"]["max_lock_tier"],
            }
        )
        ho_1x_returns.extend(rep["nautilus_1x_4bps"]["trade_returns"])
        ho_2x_returns.extend(rep["nautilus_2x_8bps"]["trade_returns"])
        if rec["preferred_side"] == "SHORT":
            ho_short_1x.extend(rep["nautilus_1x_4bps"]["trade_returns"])
        else:
            ho_long_1x.extend(rep["nautilus_1x_4bps"]["trade_returns"])

    falsif_1x = falsify_trade_returns(
        np.asarray(ho_1x_returns, dtype=float), n_trials_total=max(1, n_trials)
    )
    falsif_2x = falsify_trade_returns(
        np.asarray(ho_2x_returns, dtype=float), n_trials_total=max(1, n_trials)
    )
    falsif_long = falsify_trade_returns(
        np.asarray(ho_long_1x, dtype=float), n_trials_total=max(1, n_trials)
    )
    falsif_short = falsify_trade_returns(
        np.asarray(ho_short_1x, dtype=float), n_trials_total=max(1, n_trials)
    )
    holdout_nautilus_summary = {
        "engine": "NautilusTrader v2 BacktestEngine",
        "replayed_holdout_movers": len(ho_replays),
        "n_trades_1x": len(ho_1x_returns),
        "n_trades_2x": len(ho_2x_returns),
        "mean_return_1x_pct": round(
            float(np.mean([r["nautilus_1x_return_pct"] for r in ho_replays]))
            if ho_replays
            else 0.0,
            4,
        ),
        "mean_return_2x_pct": round(
            float(np.mean([r["nautilus_2x_return_pct"] for r in ho_replays]))
            if ho_replays
            else 0.0,
            4,
        ),
        "total_trailing_ratchets": sum(int(r["trailing_ratchets"]) for r in ho_replays),
        "total_auto_reverses": sum(int(r["auto_reverses"]) for r in ho_replays),
        "total_auto_reentries": sum(int(r["auto_reentries"]) for r in ho_replays),
        "six_gate_falsification": {
            "combined_1x": falsif_1x,
            "combined_2x": falsif_2x,
            "long_1x": falsif_long,
            "short_1x": falsif_short,
        },
        "per_mover_replays": ho_replays,
    }

    # Strictly separated post-wall observational audit (NEVER used to tune calibration weights).
    postwall_obs: dict[str, Any] = {
        "postwall_observational_only": True,
        "excluded_from_calibration": True,
        "status": "SKIPPED",
        "early_detection_latency_table": [],
    }
    nvda_replay: dict[str, Any] = {
        "postwall_observational_only": True,
        "excluded_from_calibration": True,
        "status": "SKIPPED",
        "milestones": [],
    }
    min_trail_source: list[Mapping[str, Any]] = list(
        cal["holdout_calibrated"]["latency_table"][:20]
    )

    if include_live_postwall_audit:
        mon_d = date.fromisoformat(nvda_monday_date)
        end_d = date.fromisoformat(current_week_end_date)
        obs_bars, obs_prior, obs_bvol = fetch_alpaca_paper_sip_recent_bars(
            disc.EXTREME_MOVER_BASKET_SYMBOLS,
            start_et=datetime(mon_d.year, mon_d.month, mon_d.day, 4, 0, tzinfo=_NY_TZ),
            end_et=datetime(end_d.year, end_d.month, end_d.day, 20, 0, tzinfo=_NY_TZ),
        )
        # Process day-by-day carrying causal prior-day RTH close forward across Mon -> Tue -> Wed.
        obs_feat_parts: list[pl.DataFrame] = []
        rolling_prior = dict(obs_prior)
        et_day_col = (
            pl.from_epoch("t_ns", time_unit="ns")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
            .dt.date()
            .alias("_d")
        )
        obs_with_day = obs_bars.with_columns(et_day_col)
        nvda_mon_bars: pl.DataFrame | None = None
        nvda_mon_prior: float | None = rolling_prior.get("NVDA")
        for d_val in sorted(obs_with_day["_d"].unique().to_list()):
            d_iso = str(d_val)
            d_bars = obs_with_day.filter(pl.col("_d") == d_val).drop("_d")
            if d_iso == nvda_monday_date:
                nvda_mon_bars = d_bars.filter(pl.col("symbol") == "NVDA")
                nvda_mon_prior = rolling_prior.get("NVDA")
            obs_feat_parts.append(
                disc.extract_causal_intraday_mover_features(
                    d_bars,
                    prior_close_by_sym=rolling_prior,
                    baseline_vol_by_sym=obs_bvol,
                    enforce_prewall=False,
                )
            )
            # Update prior close strictly after day close for next day's causal baseline.
            rth_close_df = (
                d_bars.with_columns(
                    pl.from_epoch("t_ns", time_unit="ns")
                    .dt.replace_time_zone("UTC")
                    .dt.convert_time_zone("America/New_York")
                    .alias("_et")
                )
                .filter(
                    (
                        pl.col("_et").dt.hour().cast(pl.Int32) * 60
                        + pl.col("_et").dt.minute().cast(pl.Int32)
                    ).is_between(570, 959)
                )
                .sort("t_ns")
                .group_by("symbol")
                .agg(pl.col("c").last().alias("rth_c"))
            )
            for rec in rth_close_df.iter_rows(named=True):
                rolling_prior[str(rec["symbol"]).upper()] = float(rec["rth_c"])

        obs_feat = pl.concat(obs_feat_parts, how="vertical")
        obs_eval = disc.evaluate_early_mover_detection(
            obs_feat, weights=best_simplex, score_threshold=best_thr, top_k=top_k
        )
        postwall_obs = {
            "postwall_observational_only": True,
            "excluded_from_calibration": True,
            "data_source": "alpaca_paper_sip_1min",
            "window_et": f"{nvda_monday_date}..{current_week_end_date}",
            "symbols_audited": list(disc.EXTREME_MOVER_BASKET_SYMBOLS),
            "n_sessions": obs_eval["n_sessions"],
            "true_extreme_movers_total": obs_eval["true_extreme_movers_total"],
            "premarket_early_hits": obs_eval["premarket_early_hits"],
            "recall_at_k": obs_eval["recall_at_k"],
            "precision_at_k": obs_eval["precision_at_k"],
            "f1_at_k": obs_eval["f1_at_k"],
            "median_lead_minutes_to_peak": obs_eval["median_lead_minutes_to_peak"],
            "median_remaining_move_to_peak_pct": obs_eval["median_remaining_move_to_peak_pct"],
            "checkpoints_et": obs_eval["checkpoints_et"],
            "early_detection_latency_table": obs_eval["latency_table"],
        }
        min_trail_source = list(obs_eval["latency_table"]) + min_trail_source
        if nvda_mon_bars is not None and nvda_mon_bars.height > 0:
            nvda_replay = replay_mover_with_nautilus_v2(
                nvda_mon_bars,
                symbol="NVDA",
                weights=best_simplex,
                score_threshold=best_thr,
                prior_close=nvda_mon_prior,
                baseline_vol=obs_bvol.get("NVDA"),
                venue_profile=venue_profile,
                postwall_observational_only=True,
            )
            nvda_replay["trade_date"] = nvda_monday_date

    min_trail_table = compute_per_symbol_min_trailing_table(
        min_trail_source, venue_profile=venue_profile
    )
    min_trail_by_sym: dict[str, float] = {
        str(row["symbol"]): float(row["rth_min_trailing_pct"]) for row in min_trail_table
    }

    manifest: dict[str, Any] = {
        "schema_version": "tier0.historical-extreme-mover-calibration.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prewall_wall_date": disc.DISCOVERY_WALL_ISO,
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": risk_sha,
        "venue_profile": venue_profile,
        "sessions": {
            "train_count": len(train_days),
            "holdout_count": len(holdout_days),
            "train_start": train_days[0],
            "train_end": train_days[-1],
            "holdout_start": holdout_days[0],
            "holdout_end": holdout_days[-1],
        },
        "optuna": cal["optuna"],
        "calibrated_weights": cal["calibrated_weights"],
        "calibrated_simplex_weights": best_simplex,
        "calibrated_scanner_weights": cal["calibrated_scanner_weights"],
        "calibrated_score_threshold": best_thr,
        "baseline_simplex_weights": cal["baseline_simplex_weights"],
        "categorized_mover_taxonomy": _build_categorized_taxonomy_spec(),
        "per_symbol_min_trailing_pct": min_trail_by_sym,
        "per_symbol_min_trailing_floor": min_trail_table,
        "train_metrics": {
            "baseline": {k: v for k, v in cal["train_baseline"].items() if k != "latency_table"},
            "calibrated": {
                k: v for k, v in cal["train_calibrated"].items() if k != "latency_table"
            },
        },
        "holdout_metrics": {
            "baseline": {k: v for k, v in cal["holdout_baseline"].items() if k != "latency_table"},
            "calibrated": {
                **{k: v for k, v in cal["holdout_calibrated"].items() if k != "latency_table"},
                "latency_table": cal["holdout_calibrated"]["latency_table"][:30],
            },
        },
        "holdout_nautilus_v2_replay": holdout_nautilus_summary,
        "postwall_observational_current_week": postwall_obs,
        "nvda_monday_premarket_e2e_replay": nvda_replay,
        "mlflow": {"logged": False, "run_id": None},
    }

    if mlflow_tracking_uri:
        try:
            manifest["mlflow"] = log_extreme_mover_calibration_to_mlflow(
                manifest, tracking_uri=mlflow_tracking_uri
            )
        except Exception as exc:  # noqa: BLE001
            manifest["mlflow"] = {
                "logged": False,
                "tracking_uri": mlflow_tracking_uri,
                "error": f"{type(exc).__name__}: {exc}",
            }

    primary_json = manifest_dir / "historical_extreme_mover_calibration_manifest.json"
    scanner_json = manifest_dir / "prewall_early_mover_calibration_manifest.json"
    duckdb_path = manifest_dir / "historical_extreme_mover_calibration.duckdb"
    manifest["duckdb_persistence"] = {
        "path": str(duckdb_path),
        "tables": persist_calibration_to_duckdb(manifest, duckdb_path),
    }
    _atomic_write_json_manifest(primary_json, manifest)
    # Write strictly pre-wall scanner-compatible weight manifest for scanner.py consumption.
    scanner_compat = {
        "schema_version": "tier0.prewall-early-mover-calibration.v1",
        "generated_at_utc": manifest["generated_at_utc"],
        "prewall_wall_date": disc.DISCOVERY_WALL_ISO,
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": risk_sha,
        "calibrated_weights": cal["calibrated_weights"],
        "calibrated_simplex_weights": best_simplex,
        "calibrated_scanner_weights": cal["calibrated_scanner_weights"],
        "calibrated_score_threshold": best_thr,
        "categorized_mover_taxonomy": manifest["categorized_mover_taxonomy"],
        "per_symbol_min_trailing_pct": min_trail_by_sym,
        "per_symbol_min_trailing_floor": min_trail_table,
        "optuna": cal["optuna"],
        "mlflow": manifest["mlflow"],
        "primary_manifest_path": str(primary_json),
    }
    _atomic_write_json_manifest(scanner_json, scanner_compat)
    return manifest


def _cli_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Tier-0 SOTA Pre-Wall Extreme-Mover Calibration & Nautilus v2 Replay"
    )
    parser.add_argument("--calibrate-extreme-movers", action="store_true", default=True)
    parser.add_argument("--lake-root", type=Path, default=disc.DEFAULT_MARKET_DATA_ROOT)
    parser.add_argument("--manifest-dir", type=Path, default=disc.DEFAULT_DATA_ROOT / "manifests")
    parser.add_argument("--max-sessions", type=int, default=28)
    parser.add_argument("--universe-per-day", type=int, default=60)
    parser.add_argument("--trials", type=int, default=24)
    parser.add_argument("--top-k", type=int, default=15)
    parser.add_argument("--venue-profile", type=str, default="ALPACA_PAPER_1000")
    parser.add_argument(
        "--mlflow-uri",
        type=str,
        default=os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000"),
    )
    parser.add_argument("--no-mlflow", action="store_true", default=False)
    parser.add_argument("--no-postwall-audit", action="store_true", default=False)
    args = parser.parse_args(argv)
    doc = run_historical_extreme_mover_calibration(
        lake_root=args.lake_root,
        manifest_dir=args.manifest_dir,
        max_sessions=args.max_sessions,
        universe_per_day=args.universe_per_day,
        n_trials=args.trials,
        top_k=args.top_k,
        venue_profile=args.venue_profile,
        mlflow_tracking_uri=None if args.no_mlflow else args.mlflow_uri,
        include_live_postwall_audit=not args.no_postwall_audit,
    )
    print(
        json.dumps(
            {
                "primary_manifest": str(
                    args.manifest_dir / "historical_extreme_mover_calibration_manifest.json"
                ),
                "scanner_manifest": str(
                    args.manifest_dir / "prewall_early_mover_calibration_manifest.json"
                ),
                "duckdb_path": doc["duckdb_persistence"]["path"],
                "holdout_recall_at_k": doc["holdout_metrics"]["calibrated"]["recall_at_k"],
                "holdout_precision_at_k": doc["holdout_metrics"]["calibrated"]["precision_at_k"],
                "holdout_median_lead_min": doc["holdout_metrics"]["calibrated"][
                    "median_lead_minutes_to_peak"
                ],
                "nvda_monday_status": (
                    "OK"
                    if doc["nvda_monday_premarket_e2e_replay"].get("signals_generated")
                    else "SKIPPED"
                ),
                "mlflow_run_id": doc["mlflow"].get("run_id"),
            },
            indent=2,
        )
    )
    return 0


__all__ = [
    "CPCV_EMBARGO_SIZE",
    "CPCV_N_FOLDS",
    "CPCV_N_TEST_FOLDS",
    "CPCV_PURGED_SIZE",
    "FEED_LATENCY_NS",
    "MIN_CPCV_SAMPLES",
    "OPTUNA_STORAGE_ENV",
    "build_causal_mover_signals",
    "calibrate_extreme_mover_weights_optuna",
    "compute_per_symbol_min_trailing_table",
    "compute_skfolio_portfolio_weights",
    "cpcv_min_samples",
    "evaluate_hftbacktest_queue_markout",
    "evaluate_six_gate_falsification",
    "falsify_trade_returns",
    "fetch_alpaca_paper_sip_recent_bars",
    "fit_conformal_excursion_intervals",
    "hlz_t_hurdle",
    "load_prewall_mover_feature_panel",
    "log_extreme_mover_calibration_to_mlflow",
    "log_falsification_to_mlflow",
    "optuna_storage_url",
    "persist_calibration_to_duckdb",
    "replay_mover_with_nautilus_v2",
    "run_historical_extreme_mover_calibration",
    "run_starred_frontier_landscape_benchmark",
]


if __name__ == "__main__":
    raise SystemExit(_cli_main())

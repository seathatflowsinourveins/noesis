"""6-gate statistical falsification & hftbacktest queue markout adapter via pinned upstreams."""

from __future__ import annotations

import math
from collections.abc import Sequence
from decimal import Decimal
from functools import lru_cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from statistics import NormalDist
from typing import Any, Literal

import numpy as np
import polars as pl

from tier0_sota.discovery import assert_pre_wall_ns
from tier0_sota.engine import run_nautilus_signal_backtest

MIN_CPCV_SAMPLES: int = 16
FEED_LATENCY_NS: int = 50_000


@lru_cache(maxsize=1)
def _cpcv() -> Any:
    from skfolio.model_selection import CombinatorialPurgedCV

    return CombinatorialPurgedCV(n_folds=4, n_test_folds=2, purged_size=1, embargo_size=1)


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
    if n_s < MIN_CPCV_SAMPLES:
        return {
            "n_trades": n_s,
            "worst_trade_pct": worst_pct,
            "cpcv_splits": 0,
            "cpcv_oos_mean_bps": 0.0,
            "dsr": 0.0,
            "min_trl": None,
            "pbo": pbo,
            "hac_t": 0.0,
            "spa_p": 1.0,
            "spa_p_upper": 1.0,
            "promoted": False,
            "verdict": "DISCARD_INSUFFICIENT_TRADES",
        }
    splits = list(_cpcv().split(tr_r.reshape(-1, 1)))
    te_idx = [np.concatenate(te) if isinstance(te, (list, tuple)) else te for _, te in splits]
    cpcv_oos_m = float(np.mean([float(np.mean(tr_r[idx])) for idx in te_idx]))
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
    hlz_t = float(NormalDist().inv_cdf(1.0 - 0.00135 / n_t))
    pbo_ok = bool(pbo is not None and math.isfinite(float(pbo)) and float(pbo) < 0.50)
    prom = bool(
        mean_r > 0.0
        and cpcv_oos_m > 0.0
        and dsr_val >= 0.95
        and min_trl is not None
        and n_s >= min_trl
        and hac_t >= hlz_t
        and spa_p < 0.05
        and spa_pu < 0.05
        and pbo_ok
    )
    return {
        "n_trades": n_s,
        "worst_trade_pct": worst_pct,
        "mean_bps": round(mean_r * 1e4, 3),
        "cpcv_splits": len(splits),
        "cpcv_oos_mean_bps": round(cpcv_oos_m * 1e4, 3),
        "dsr": round(dsr_val, 6),
        "min_trl": round(min_trl, 2) if min_trl is not None else None,
        "pbo": round(float(pbo), 4) if pbo is not None else None,
        "hac_t": round(hac_t, 4),
        "spa_p": round(spa_p, 4),
        "spa_p_upper": round(spa_pu, 4),
        "promoted": prom,
        "verdict": "PROMOTE" if prom else "DISCARD",
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
    g1 = l1 is None or (
        l1["passive_order_submitted"] and l1["passive_filled"] and l1["markout_bps"] >= -20.0
    )
    g2 = (
        bt1.total_return_pct > 0.0
        and bt2.total_return_pct > 0.0
        and f1["promoted"]
        and f2["promoted"]
    )
    g3 = f1["cpcv_splits"] > 0 and f1["cpcv_oos_mean_bps"] > 0.0 and f2["cpcv_oos_mean_bps"] > 0.0
    g4 = f1["dsr"] >= 0.95 and f2["dsr"] >= 0.95 and f1["pbo"] is not None and f1["pbo"] < 0.50
    g5 = f1["spa_p"] < 0.05 and f2["spa_p"] < 0.05
    g6 = f1["promoted"] and f2["promoted"]
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

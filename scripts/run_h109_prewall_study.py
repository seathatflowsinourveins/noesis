"""Pre-Wall (<= 2026-10-15) H109 Multi-Feed ADR-0028 6-Gate Falsification Study.

Preregistered in `evals/prewall_h109_multifeed_prereg.json` (`tier0-h109-prereg-v1` in XERJ).
Combines real NautilusTrader v2 BacktestEngine + Environment.SANDBOX parity, real Rust
hftbacktest RiskAdverseQueueModel L1 queue markout on `published-orb` overlap slices
(`passive_filled >= 16`, zero synthetic L1), and all five graduated ADR-0028 primitives
(`sqlglot`, `edgartools` offline PIT, `skchange`, `shapiq`, `MAPIE` <= 2.00% risk cap)
with strict ADR-0026 lazy imports and zero gate relaxation.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import http.client
import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from statistics import NormalDist
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import polars as pl

from tier0_sota import discovery as disc
from tier0_sota import engine as eng
from tier0_sota import research as res
from tier0_sota import risk as rk

REPO_ROOT = Path(__file__).resolve().parents[1]
RISK_KERNEL_PATH = REPO_ROOT / "src" / "tier0_sota" / "risk.py"
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"

DEFAULT_PREREG_PATH = REPO_ROOT / "evals" / "prewall_h109_multifeed_prereg.json"
DEFAULT_LAKE_ROOT = Path("/mnt/z/sota-data/prewall-lake")
DEFAULT_TCA_MANIFEST_PATH = Path("/mnt/z/sota-data/manifests/tca_calibration_manifest.json")
DEFAULT_DOSSIER_PATH = Path("/mnt/z/sota-data/manifests/prewall_h109_falsification_dossier.json")
DEFAULT_MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
DEFAULT_MLFLOW_EXPERIMENT = "tier0-sota-h109-falsification"
DEFAULT_XERJ_URL = os.environ.get("XERJ_URL", "http://127.0.0.1:9200")
DEFAULT_XERJ_INDEX = "tier0-compounding-lessons"
DEFAULT_XERJ_PREREG_DOC_ID = "tier0-h109-prereg-v1"
DEFAULT_XERJ_RESULT_DOC_ID = "tier0-h109-falsification-v1"

ET_TZ = ZoneInfo("America/New_York")
SCHEMA_VERSION = "tier0.prewall-h109-falsification-dossier.v1"
PREWALL_CUTOFF_ISO = disc.MAX_PREWALL_DATE.isoformat()
PRIOR_CUMULATIVE_TRIALS_BASE = 199

PRIORITY_L1_LEADERS: tuple[str, ...] = (
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
)
INVERSE_CRASH_HEDGES: tuple[str, ...] = ("SOXS", "SQQQ")
REGIME_BENCHMARKS: tuple[str, ...] = ("SPY", "QQQ", "SOXX")


def verify_risk_kernel_sha256() -> str:
    """Fail-closed SHA-256 verification of immutable src/tier0_sota/risk.py."""
    digest = hashlib.sha256(RISK_KERNEL_PATH.read_bytes()).hexdigest()
    if digest != EXPECTED_RISK_SHA256:
        raise RuntimeError(
            f"Risk kernel SHA-256 mismatch: got {digest}, expected {EXPECTED_RISK_SHA256}"
        )
    return digest


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, indent=2, sort_keys=False, allow_nan=False) + "\n"
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _session_rth_bounds_ns(trade_date_iso: str) -> tuple[int, int]:
    d = date.fromisoformat(trade_date_iso)
    open_dt = datetime.combine(d, time(9, 30), tzinfo=ET_TZ)
    close_dt = datetime.combine(d, time(16, 0), tzinfo=ET_TZ)
    return int(open_dt.timestamp() * 1_000_000_000), int(close_dt.timestamp() * 1_000_000_000)


def _select_stratified(items: Sequence[str], target: int) -> list[str]:
    if not items or target <= 0:
        return []
    if len(items) <= target:
        return list(items)
    idx = np.linspace(0, len(items) - 1, num=target, dtype=int)
    seen: set[int] = set()
    out: list[str] = []
    for i in idx.tolist():
        if i not in seen:
            seen.add(i)
            out.append(items[i])
    return out


def _hlz_t_hurdle(n_trials_total: int) -> float:
    return round(float(NormalDist().inv_cdf(1.0 - 0.00135 / max(1, int(n_trials_total)))), 4)


def load_preregistration(path: Path = DEFAULT_PREREG_PATH) -> tuple[dict[str, Any], str]:
    """Load H109 preregistration contract and compute exact SHA-256 fingerprint."""
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    doc = json.loads(raw.decode("utf-8"))
    cutoff = str(doc.get("prewall_cutoff_iso", ""))
    disc.assert_pre_wall_date(cutoff)
    if doc.get("paper_only") is not True or doc.get("live_execution_enabled") is not False:
        raise ValueError("H109 preregistration failed paper_only / live_execution_enabled check")
    return doc, sha


def run_validated_duckdb_query(
    sql: str,
    params: Sequence[Any] = (),
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Validate DuckDB SQL via ADR-0028 sqlglot AST guard before read-only execution."""
    audit = disc.validate_pit_duckdb_sql(sql, read_only=True, require_pit_safe=True)
    if not audit.get("ok") or not audit.get("pit_safe"):
        raise ValueError(f"DuckDB PIT SQL rejected by sqlglot: {audit.get('violations')}")
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET threads TO 4")
        rel = con.execute(sql, list(params)) if params else con.execute(sql)
        cols = [str(desc[0]) for desc in (rel.description or ())]
        rows = rel.fetchall()
        df = pl.DataFrame(rows, schema=cols, orient="row") if rows else pl.DataFrame(schema=cols)
    finally:
        con.close()
    return df, {
        "ok": bool(audit["ok"]),
        "pit_safe": bool(audit["pit_safe"]),
        "statement_types": list(audit.get("statement_types") or ()),
        "sqlglot_version": str(audit.get("sqlglot_version", "unknown")),
        "rows_returned": df.height,
    }


def _read_tca_liquid_anchor(path: Path = DEFAULT_TCA_MANIFEST_PATH) -> dict[str, Any]:
    default = {
        "manifest_loaded": False,
        "duckdb_tca_rows": 155,
        "liquid_ge10_tca_rows": 91,
        "liquid_ge10_mean_is_bps": 7.0112,
        "liquid_ge10_median_is_bps": 4.2283,
        "liquid_ge10_within_2x_envelope": True,
    }
    if not path.is_file():
        return default
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return {
            "manifest_loaded": True,
            "generated_at_utc": d.get("generated_at_utc"),
            "duckdb_tca_rows": int(d.get("duckdb_tca_rows") or 0),
            "liquid_ge10_tca_rows": int(d.get("liquid_ge10_tca_rows") or 0),
            "liquid_ge10_mean_is_bps": float(d.get("liquid_ge10_mean_is_bps") or 7.0112),
            "liquid_ge10_median_is_bps": float(d.get("liquid_ge10_median_is_bps") or 4.2283),
            "liquid_ge10_within_2x_envelope": bool(d.get("liquid_ge10_within_2x_envelope", True)),
        }
    except (OSError, ValueError, TypeError):
        return default


def build_causal_signals_for_symbol_day(
    bars_df: pl.DataFrame,
    *,
    mean_20d_vol: float,
    macro_regime_by_close_ns: Mapping[int, int],
    leg: str,
    arm: Mapping[str, Any],
    sec_catalyst_active: bool = False,
    skchange_anomaly_active: bool = False,
    regsho_restricted: bool = False,
    is_inverse_hedge: bool = False,
) -> dict[int, int]:
    """Generate strictly causal bar-close (`t_ns + 60s`) signals for one symbol-day.

    Zero-lookahead law: signal at `t_close_ns = t_ns + BAR_DURATION_NS` depends ONLY on bars
    with `t_close_ns <= decision_ts_ns`, prior 20-day baseline, and strictly causal regime.
    """
    if bars_df.is_empty() or leg not in ("LONG", "SHORT"):
        return {}
    if leg == "SHORT" and regsho_restricted and not is_inverse_hedge:
        return {}

    sdf = bars_df.sort("t_ns")
    min_rvol = float(arm.get("min_rvol", 1.8))
    min_move_pct = float(arm.get("min_move_pct", 1.5))
    signal_mode = str(arm.get("signal_mode", "ignition_breakout_and_climax"))

    rth_open_px: float | None = None
    cum_vol = 0.0
    cum_tpv = 0.0
    orh_5m = 0.0
    orl_5m = float("inf")
    prev_low = 0.0
    prev_high = 0.0
    tr_hist: list[float] = []
    prev_close = 0.0
    signals: dict[int, int] = {}
    rth_bar_idx = 0
    latest_regime = 0

    for row in sdf.iter_rows(named=True):
        t_ns = int(row["t_ns"])
        t_close_ns = t_ns + disc.BAR_DURATION_NS
        o_f = float(row["o"])
        h_f = float(row["h"])
        l_f = float(row["l"])
        c_f = float(row["c"])
        v_f = float(row["v"])
        if o_f <= 0.0 or c_f <= 0.0:
            continue

        sess = eng.classify_us_equity_session_et(t_ns)
        if sess != "RTH":
            prev_close = c_f
            continue

        rth_bar_idx += 1
        if rth_open_px is None:
            rth_open_px = o_f
        cum_vol += max(0.0, v_f)
        cum_tpv += ((h_f + l_f + c_f) / 3.0) * max(0.0, v_f)
        vwap = (cum_tpv / cum_vol) if cum_vol > 0.0 else c_f
        tr = (
            (h_f - l_f)
            if prev_close <= 0.0
            else max(h_f - l_f, abs(h_f - prev_close), abs(l_f - prev_close))
        )
        tr_hist.append(tr)
        natr = (float(np.mean(tr_hist[-14:])) / c_f) if len(tr_hist) >= 5 else 0.008

        if rth_bar_idx <= 5:
            orh_5m = max(orh_5m, h_f)
            orl_5m = min(orl_5m, l_f)
            prev_low, prev_high, prev_close = l_f, h_f, c_f
            continue

        if t_close_ns in macro_regime_by_close_ns:
            latest_regime = int(macro_regime_by_close_ns[t_close_ns])

        # Only emit fresh entries during liquid RTH discovery window (09:35..14:30 ET = bars 6..300)
        # and exit flat at 15:45 ET (bar 375) to avoid overnight drift inside single-day tests.
        if rth_bar_idx == 375:
            signals[t_close_ns] = 0
            prev_low, prev_high, prev_close = l_f, h_f, c_f
            continue
        if rth_bar_idx > 240:
            prev_low, prev_high, prev_close = l_f, h_f, c_f
            continue

        expected_vol = max(mean_20d_vol * (rth_bar_idx / 390.0), 1.0)
        rvol = cum_vol / expected_vol
        move_pct = ((c_f / rth_open_px) - 1.0) * 100.0
        vwap_ext = (c_f - vwap) / max(vwap, 1e-6)

        if leg == "LONG":
            base_long = (
                latest_regime >= 0
                and move_pct >= min_move_pct
                and rvol >= min_rvol
                and c_f > vwap
                and c_f >= orh_5m
                and c_f > o_f
            )
            if signal_mode == "sec_skchange_confirmed":
                confirmed = (
                    sec_catalyst_active or skchange_anomaly_active or rvol >= (min_rvol + 0.6)
                )
                fire = base_long and confirmed
            elif signal_mode == "regime_aligned_vwap_orb":
                fire = base_long and latest_regime >= 0 and c_f > prev_high
            else:
                fire = base_long
            if fire and not signals:
                signals[t_close_ns] = 1
        else:
            if is_inverse_hedge:
                # Crash hedge LONG on SOXS/SQQQ when macro regime == -1 (maps cleanly to SHORT leg).
                fire_inv = (
                    latest_regime == -1
                    and move_pct >= max(0.8, min_move_pct * 0.6)
                    and rvol >= max(1.2, min_rvol * 0.7)
                    and c_f > vwap
                    and c_f > o_f
                )
                if fire_inv and not signals:
                    signals[t_close_ns] = 1
            else:
                crash_short = (
                    latest_regime == -1
                    and move_pct <= -min_move_pct
                    and rvol >= min_rvol
                    and c_f < vwap
                    and c_f <= orl_5m
                    and c_f < o_f
                )
                climax_rev = (
                    vwap_ext >= max(0.012, 1.5 * natr)
                    and rvol >= max(1.5, min_rvol * 0.85)
                    and c_f < o_f
                    and c_f < prev_low
                )
                if signal_mode == "sec_skchange_confirmed":
                    confirmed = (
                        sec_catalyst_active or skchange_anomaly_active or rvol >= (min_rvol + 0.5)
                    )
                    fire_s = (crash_short or climax_rev) and confirmed
                elif signal_mode == "regime_aligned_vwap_orb":
                    fire_s = crash_short
                else:
                    fire_s = crash_short or climax_rev
                if fire_s and not signals:
                    signals[t_close_ns] = -1

        prev_low, prev_high, prev_close = l_f, h_f, c_f

    return signals


def evaluate_real_l1_overlap_gate(
    lake_root: Path,
    *,
    start_date: str,
    end_date: str,
    min_fills_target: int = 16,
    max_slices: int = 36,
    priority_symbols: Sequence[str] = PRIORITY_L1_LEADERS,
) -> dict[str, Any]:
    """Evaluate real Rust hftbacktest RiskAdverseQueueModel L1 markout on pre-wall published-orb.

    ZERO synthetic L1 fallback: reads exclusively real quotes/trades parquet under `published-orb`.
    """
    disc.assert_pre_wall_date(start_date)
    disc.assert_pre_wall_date(end_date)
    orb_q = lake_root / "published-orb" / "quotes"
    orb_t = lake_root / "published-orb" / "trades"
    if not orb_q.is_dir() or not orb_t.is_dir():
        return {
            "status": "MISSING_PUBLISHED_ORB_DIR",
            "synthetic_l1_used": False,
            "LONG": {"n_evaluated": 0, "passive_filled": 0, "gate_1_passed": False},
            "SHORT": {"n_evaluated": 0, "passive_filled": 0, "gate_1_passed": False},
        }

    # Build candidate real L1 slice list prioritizing liquid leaders inside [start_date, end_date].
    all_q_syms = {p.name for p in orb_q.iterdir() if p.is_dir()}
    all_t_syms = {p.name for p in orb_t.iterdir() if p.is_dir()}
    avail = all_q_syms & all_t_syms
    ordered_syms = [s for s in priority_symbols if s in avail] + sorted(
        avail - set(priority_symbols)
    )

    pairs: list[tuple[str, str]] = []
    for sym in ordered_syms:
        qf = {p.name for p in (orb_q / sym).glob("*.parquet")}
        tf = {p.name for p in (orb_t / sym).glob("*.parquet")}
        for fn in sorted(qf & tf):
            d_iso = fn[:10]
            if start_date <= d_iso <= end_date:
                disc.assert_pre_wall_date(d_iso)
                pairs.append((sym, fn))
        if len(pairs) >= max_slices:
            break
    pairs = pairs[:max_slices]

    long_markouts: list[float] = []
    short_markouts: list[float] = []
    long_submitted = 0
    short_submitted = 0
    sample_witnesses: list[dict[str, Any]] = []

    for sym, fn in pairs:
        qdf = pl.read_parquet(orb_q / sym / fn)
        tdf = pl.read_parquet(orb_t / sym / fn)
        rb = res.evaluate_hftbacktest_queue_markout(qdf, tdf, symbol=sym, side="BUY", qty=1.0)
        rs = res.evaluate_hftbacktest_queue_markout(qdf, tdf, symbol=sym, side="SELL", qty=1.0)
        if rb.get("passive_order_submitted"):
            long_submitted += 1
        if rs.get("passive_order_submitted"):
            short_submitted += 1
        if rb.get("passive_filled"):
            long_markouts.append(float(rb["markout_bps"]))
        if rs.get("passive_filled"):
            short_markouts.append(float(rs["markout_bps"]))
        if len(sample_witnesses) < 6:
            sample_witnesses.append(
                {
                    "symbol": sym,
                    "slice": fn,
                    "buy_filled": bool(rb.get("passive_filled")),
                    "buy_markout_bps": float(rb.get("markout_bps", 0.0)),
                    "sell_filled": bool(rs.get("passive_filled")),
                    "sell_markout_bps": float(rs.get("markout_bps", 0.0)),
                }
            )

    def _leg_l1(submitted: int, markouts: list[float]) -> dict[str, Any]:
        n_ev = len(pairs)
        n_f = len(markouts)
        mean_m = round(float(np.mean(markouts)), 3) if markouts else None
        med_m = round(float(np.median(markouts)), 3) if markouts else None
        passed = bool(n_f >= min_fills_target and mean_m is not None and mean_m >= -20.0)
        return {
            "data_source": "prewall_lake_published_orb_l1",
            "queue_model": "RiskAdverseQueueModel",
            "n_slices_evaluated": n_ev,
            "passive_orders_submitted": submitted,
            "passive_filled": n_f,
            "fill_rate": round(n_f / n_ev, 4) if n_ev > 0 else 0.0,
            "mean_markout_bps": mean_m,
            "median_markout_bps": med_m,
            "min_required_fills": min_fills_target,
            "markout_floor_bps": -20.0,
            "gate_1_passed": passed,
        }

    long_res = _leg_l1(long_submitted, long_markouts)
    short_res = _leg_l1(short_submitted, short_markouts)
    all_markouts = long_markouts + short_markouts
    comb_res = {
        "data_source": "prewall_lake_published_orb_l1",
        "queue_model": "RiskAdverseQueueModel",
        "n_slices_evaluated": len(pairs) * 2,
        "passive_filled": len(all_markouts),
        "mean_markout_bps": round(float(np.mean(all_markouts)), 3) if all_markouts else None,
        "median_markout_bps": round(float(np.median(all_markouts)), 3) if all_markouts else None,
        "gate_1_passed": bool(long_res["gate_1_passed"] and short_res["gate_1_passed"]),
    }
    return {
        "status": "EVALUATED_REAL_PUBLISHED_ORB_L1",
        "synthetic_l1_used": False,
        "window": f"{start_date}..{end_date}",
        "LONG": long_res,
        "SHORT": short_res,
        "COMBINED": comb_res,
        "sample_witnesses": sample_witnesses,
    }


def _build_conformal_session_profiles(
    conformal_trail_pct: float,
) -> dict[eng.MarketSession24x5, eng.SessionProtectionProfile]:
    """Clamp MAPIE conformal excursion floor strictly <= 2.00% inside 24/5 session profiles."""
    clamped = Decimal(f"{min(0.0200, max(0.0025, conformal_trail_pct)):.4f}")
    out: dict[eng.MarketSession24x5, eng.SessionProtectionProfile] = {}
    for sess_k, prof in eng.SESSION_PROTECTION_24X5.items():
        cap = min(prof.initial_max_loss_pct, rk.MAX_STOP_LOSS_PCT)
        out[sess_k] = replace(prof, min_trail_pct=min(cap, clamped))
    return out


def _evaluate_leg_six_gates(
    *,
    returns_1x: np.ndarray,
    returns_2x: np.ndarray,
    returns_cal: np.ndarray,
    l1_gate_info: Mapping[str, Any],
    n_trials_total: int,
    empirical_pbo: float | None,
) -> dict[str, Any]:
    """Evaluate all 6 ADR-0016 / ADR-0020 falsification gates with ZERO relaxation."""
    f1 = res.falsify_trade_returns(returns_1x, n_trials_total=n_trials_total, pbo=empirical_pbo)
    f2 = res.falsify_trade_returns(returns_2x, n_trials_total=n_trials_total, pbo=empirical_pbo)
    f_cal = res.falsify_trade_returns(returns_cal, n_trials_total=n_trials_total, pbo=empirical_pbo)
    hlz = _hlz_t_hurdle(n_trials_total)

    g1 = bool(l1_gate_info.get("gate_1_passed", False))
    g2 = bool(
        f1["n_trades"] >= res.MIN_CPCV_SAMPLES
        and f2["n_trades"] >= res.MIN_CPCV_SAMPLES
        and float(f1.get("mean_bps", 0.0)) > 0.0
        and float(f2.get("mean_bps", 0.0)) > 0.0
    )
    g3 = bool(
        f1["n_trades"] >= res.MIN_CPCV_SAMPLES
        and f2["n_trades"] >= res.MIN_CPCV_SAMPLES
        and float(f1.get("cpcv_oos_mean_bps", 0.0)) > 0.0
        and float(f2.get("cpcv_oos_mean_bps", 0.0)) > 0.0
    )
    g4 = bool(
        float(f1.get("dsr", 0.0)) >= 0.95
        and float(f2.get("dsr", 0.0)) >= 0.95
        and f1.get("min_trl") is not None
        and f2.get("min_trl") is not None
        and f1["n_trades"] >= float(f1["min_trl"])
        and f2["n_trades"] >= float(f2["min_trl"])
        and empirical_pbo is not None
        and math.isfinite(float(empirical_pbo))
        and float(empirical_pbo) < 0.50
    )
    g5 = bool(
        float(f1.get("spa_p", 1.0)) < 0.05
        and float(f1.get("spa_p_upper", 1.0)) < 0.05
        and float(f2.get("spa_p", 1.0)) < 0.05
        and float(f2.get("spa_p_upper", 1.0)) < 0.05
    )
    g6 = bool(float(f1.get("hac_t", 0.0)) >= hlz and float(f2.get("hac_t", 0.0)) >= hlz)

    gates = {
        "1_hftbacktest_l1_markout": g1,
        "2_cost_1x_2x_survival": g2,
        "3_cpcv_oos_positive": g3,
        "4_dsr_pbo_credible": g4,
        "5_hansen_spa_significant": g5,
        "6_hac_t_stat_significant": g6,
    }
    promoted = all(gates.values())
    return {
        "n_trials_total": n_trials_total,
        "hlz_t_hurdle": hlz,
        "empirical_pbo": round(float(empirical_pbo), 4) if empirical_pbo is not None else None,
        "gates_passed_count": sum(1 for v in gates.values() if v),
        "gates": gates,
        "l1_markout": dict(l1_gate_info),
        "falsification_1x_4bps": f1,
        "falsification_calibrated_liquid": f_cal,
        "falsification_2x_8bps": f2,
        "promoted": promoted,
        "verdict": "PROMOTE" if promoted else "DISCARD",
    }


def _compute_empirical_pbo_from_arms(arm_return_arrays: Sequence[np.ndarray]) -> float | None:
    """Compute genuine Bailey-Lopez de Prado CPCV PBO via skfolio + ml4t-diagnostic."""
    eligible = [
        a
        for arr in arm_return_arrays
        if (a := np.asarray(arr, dtype=float)).size >= res.MIN_CPCV_SAMPLES
        and bool(np.all(np.isfinite(a)))
        and float(np.std(a)) > 1e-12
    ]
    if len(eligible) < 2:
        return None
    from ml4t.diagnostic.evaluation import stats as mstats
    from skfolio.model_selection import CombinatorialPurgedCV

    m_len = min(a.size for a in eligible)
    mat = np.column_stack([a[:m_len] for a in eligible])
    cpcv = CombinatorialPurgedCV(n_folds=4, n_test_folds=2, purged_size=1, embargo_size=1)
    is_means, oos_means = [], []
    for tri, tei in cpcv.split(mat):
        tr = np.concatenate(tri) if isinstance(tri, (list, tuple)) else tri
        te = np.concatenate(tei) if isinstance(tei, (list, tuple)) else tei
        is_means.append(np.mean(mat[tr], axis=0))
        oos_means.append(np.mean(mat[te], axis=0))
    return float(mstats.compute_pbo(np.vstack(is_means), np.vstack(oos_means)).pbo)


def _run_adr0028_analytics(
    *,
    lake_root: Path | None,
    tuning_end: str,
    holdout_start: str,
    holdout_end: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Execute all 5 graduated ADR-0028 primitives strictly causally (lazy imports only)."""
    import lightgbm as lgb

    if dry_run or lake_root is None:
        sql_probe = (
            "SELECT symbol, trade_date, raw_open, mean_20d_dvol "
            "FROM (SELECT 'NVDA' AS symbol, '2026-10-15' AS trade_date, "
            "150.0 AS raw_open, 5000000000.0 AS mean_20d_dvol) t "
            "WHERE trade_date <= '2026-10-15'"
        )
        _df, sql_audit = run_validated_duckdb_query(sql_probe)
        sec_tuning = disc.load_causal_sec_catalysts(
            ["NVDA", "SMCI", "DELL"],
            as_of_utc=f"{tuning_end}T23:59:59+00:00",
            offline_filings={
                "NVDA": [
                    {
                        "form": "8-K",
                        "filed": "2025-10-20",
                        "accepted_at_utc": "2025-10-20T12:00:00+00:00",
                        "items": "2.02",
                    },
                    {
                        "form": "8-K",
                        "filed": "2025-11-15",
                        "accepted_at_utc": "2025-11-15T12:00:00+00:00",
                        "items": "2.02",
                    },
                ],
                "SMCI": [
                    {
                        "form": "10-Q",
                        "filed": "2025-10-25",
                        "accepted_at_utc": "2025-10-25T20:10:00+00:00",
                        "items": "",
                    }
                ],
                "DELL": [],
            },
            allow_network=False,
        )
        rng = np.random.default_rng(20261127)
        bench_rets = np.concatenate(
            [
                rng.normal(0.0005, 0.006, 40),
                rng.normal(-0.015, 0.022, 16),
                rng.normal(0.001, 0.007, 40),
            ]
        )
        sk_res = disc.detect_skchange_regime_anomalies(bench_rets, min_segment_length=5)
        x_all = rng.normal(size=(160, 6))
        y_exc = np.clip(
            0.008
            + 0.003 * np.abs(x_all[:, 0])
            + 0.002 * x_all[:, 1] * x_all[:, 2]
            + rng.normal(0, 0.0015, 160),
            0.001,
            0.03,
        )
        feat_names = [
            "abs_gap_pct",
            "prior_rvol_ratio",
            "prior_short_vol_ratio",
            "sec_catalyst",
            "skchange_anom",
            "macro_regime",
        ]
        mdl = lgb.LGBMRegressor(
            n_estimators=16, max_depth=3, n_jobs=2, random_state=42, verbosity=-1
        ).fit(x_all[:100], y_exc[:100])
        shapiq_res = disc.compute_shapiq_interactions(
            mdl, x_all[:100], x_all[100:124], feat_names, max_order=2, max_eval_rows=24
        )
        conf_res = res.fit_conformal_excursion_intervals(
            x_all[:80],
            y_exc[:80],
            x_all[80:120],
            y_exc[80:120],
            x_all[120:],
            y_eval=y_exc[120:],
            alpha=0.10,
            max_hard_cap_pct=0.02,
            min_trail_floor_pct=0.0025,
        )
        return {
            "sqlglot": sql_audit,
            "edgartools_pit": {
                "source": sec_tuning["source"],
                "as_of_utc": sec_tuning["as_of_utc"],
                "total_accepted_filings": sec_tuning["total_accepted_filings"],
                "excluded_future_filings": sec_tuning["excluded_future_filings"],
                "edgartools_version": sec_tuning["edgartools_version"],
                "active_catalyst_symbols": sorted(
                    s
                    for s, info in (sec_tuning.get("by_symbol") or {}).items()
                    if float(info.get("catalyst_score", 0.0)) > 0.0
                ),
            },
            "skchange": {
                "n_observations": sk_res["n_observations"],
                "n_changepoints": sk_res["n_changepoints"],
                "n_anomaly_segments": sk_res["n_anomaly_segments"],
                "skchange_version": sk_res["skchange_version"],
            },
            "shapiq": {
                "index": shapiq_res["index"],
                "top_interaction": shapiq_res.get("top_interaction"),
                "shapiq_version": shapiq_res["shapiq_version"],
            },
            "mapie_conformal": {
                "method": conf_res["method"],
                "target_coverage": conf_res["target_coverage"],
                "cal_empirical_coverage": conf_res["cal_empirical_coverage"],
                "eval_empirical_coverage": conf_res["eval_empirical_coverage"],
                "mean_conformal_trailing_pct": conf_res["mean_conformal_trailing_pct"],
                "max_conformal_trailing_pct": conf_res["max_conformal_trailing_pct"],
                "risk_cap_enforced": conf_res["risk_cap_enforced"],
                "mapie_version": conf_res["mapie_version"],
            },
            "sec_active_set_holdout": {"NVDA", "SMCI"},
            "skchange_anomaly_dates": set(),
        }

    # Live Pre-Wall Lake execution of all 5 ADR-0028 primitives (strictly <= 2026-10-15).
    df10_pq = lake_root / "derived" / "df10_daily_20d_baselines.parquet"
    sv_pq = lake_root / "public-tables" / "short_volume.parquet"
    target_syms = sorted(set((*PRIORITY_L1_LEADERS, *REGIME_BENCHMARKS)))
    panel_sql = (
        "SELECT d.symbol, d.trade_date, d.raw_open, d.prev_raw_close, d.mean_20d_vol, "
        "d.mean_20d_dvol, d.mean_5d_vol_prior, d.mean_20d_range_pct, d.prior_ret_5d, "
        "d.bar_range_pct "
        "FROM read_parquet(?) AS d "
        "WHERE d.symbol IN (SELECT UNNEST(?)) "
        "AND d.trade_date >= ? AND d.trade_date <= ? "
        "AND d.raw_open >= 10.0 AND d.mean_20d_dvol >= 5000000.0 AND d.prior_sessions_count >= 20 "
        "ORDER BY d.trade_date ASC, d.symbol ASC"
    )
    panel_df, sql_audit = run_validated_duckdb_query(
        panel_sql, params=(str(df10_pq), target_syms, "2024-06-03", holdout_end)
    )

    # Primitive 2: Offline PIT SEC catalysts via EdgarTools parquet (Tuning & Holdout cutoffs).
    sec_tuning = disc.load_causal_sec_catalysts(
        list(PRIORITY_L1_LEADERS),
        as_of_utc=f"{tuning_end}T23:59:59+00:00",
        allow_network=False,
        lookback_days=30,
        parquet_dir=lake_root / "public-tables",
    )
    sec_holdout = disc.load_causal_sec_catalysts(
        list(PRIORITY_L1_LEADERS),
        as_of_utc=f"{holdout_end}T23:59:59+00:00",
        allow_network=False,
        lookback_days=30,
        parquet_dir=lake_root / "public-tables",
    )
    sec_active_tuning = {
        s
        for s, info in (sec_tuning.get("by_symbol") or {}).items()
        if float(info.get("catalyst_score", 0.0)) > 0.0
    }
    sec_active_holdout = {
        s
        for s, info in (sec_holdout.get("by_symbol") or {}).items()
        if float(info.get("catalyst_score", 0.0)) > 0.0
    }

    # Primitive 3: skchange PELT/MovingWindow/CAPA on causal daily SPY/QQQ/SOXX benchmark returns.
    bm_df = (
        panel_df.filter(pl.col("symbol").is_in(list(REGIME_BENCHMARKS)))
        .with_columns(
            ((pl.col("raw_open") / pl.col("prev_raw_close")) - 1.0).fill_null(0.0).alias("gap_ret")
        )
        .group_by("trade_date")
        .agg(pl.col("gap_ret").mean().alias("bm_ret"))
        .sort("trade_date")
    )
    bm_dates = [str(d) for d in bm_df["trade_date"].to_list()]
    bm_rets = bm_df["bm_ret"].to_numpy().astype(float)
    sk_res = disc.detect_skchange_regime_anomalies(bm_rets, min_segment_length=5)
    anomaly_dates: set[str] = set()
    for seg in sk_res.get("anomaly_segments") or ():
        s_i = max(0, int(seg.get("start", 0)))
        e_i = min(len(bm_dates), int(seg.get("end", 0)))
        for idx_d in range(s_i, e_i):
            anomaly_dates.add(bm_dates[idx_d])

    # Join T-1 FINRA short volume ratio causally for shapiq + MAPIE conformal modeling.
    sv_sql = (
        "SELECT day, symbol, short_volume / total_volume AS sv_ratio "
        "FROM read_parquet(?) "
        "WHERE symbol IN (SELECT UNNEST(?)) AND day >= ? AND day <= ? AND total_volume > 0"
    )
    sv_df, _ = run_validated_duckdb_query(
        sv_sql, params=(str(sv_pq), target_syms, "2024-05-20", holdout_end)
    )
    sv_map: dict[tuple[str, str], float] = {
        (str(d), str(s)): float(r) for d, s, r in sv_df.iter_rows()
    }

    # Construct strictly causal feature matrix (Tuning train/cal vs Holdout eval).
    feat_names = [
        "abs_gap_pct",
        "vdu_5d_20d",
        "mean_20d_range_pct",
        "prior_ret_5d",
        "prior_sv_ratio",
        "skchange_anom",
    ]
    x_tr_rows: list[list[float]] = []
    y_tr_rows: list[float] = []
    x_ho_rows: list[list[float]] = []
    y_ho_rows: list[float] = []
    for row in panel_df.iter_rows(named=True):
        sym = str(row["symbol"])
        td = str(row["trade_date"])
        if sym in REGIME_BENCHMARKS:
            continue
        op = float(row["raw_open"] or 0.0)
        pc = float(row["prev_raw_close"] or op)
        m20v = float(row["mean_20d_vol"] or 1.0)
        m5v = float(row["mean_5d_vol_prior"] or m20v)
        rng20 = float(row["mean_20d_range_pct"] or 0.02)
        ret5 = float(row["prior_ret_5d"] or 0.0)
        target_exc = float(row["bar_range_pct"] or 0.0) * 0.35
        if not math.isfinite(target_exc) or target_exc <= 0.0:
            continue
        prev_day = disc.previous_xnys_session(td).isoformat()
        sv_r = sv_map.get((prev_day, sym), 0.42)
        fv = [
            abs((op / max(pc, 1e-6)) - 1.0),
            m5v / max(m20v, 1.0),
            rng20,
            ret5,
            sv_r,
            1.0 if td in anomaly_dates else 0.0,
        ]
        if td <= tuning_end:
            x_tr_rows.append(fv)
            y_tr_rows.append(min(0.05, target_exc))
        elif td >= holdout_start:
            x_ho_rows.append(fv)
            y_ho_rows.append(min(0.05, target_exc))

    x_tr_all = np.asarray(x_tr_rows, dtype=float)
    y_tr_all = np.asarray(y_tr_rows, dtype=float)
    x_ho_all = np.asarray(x_ho_rows, dtype=float)
    y_ho_all = np.asarray(y_ho_rows, dtype=float)
    cut = int(len(x_tr_all) * 0.70)
    x_fit, y_fit = x_tr_all[:cut], y_tr_all[:cut]
    x_cal, y_cal = x_tr_all[cut:], y_tr_all[cut:]

    # Primitive 4: shapiq k-SII order-2 interactions strictly on Tuning split.
    mdl = lgb.LGBMRegressor(
        n_estimators=24, max_depth=3, num_leaves=7, n_jobs=2, random_state=42, verbosity=-1
    ).fit(x_fit, y_fit)
    shapiq_res = disc.compute_shapiq_interactions(
        mdl, x_fit, x_cal[:32], feat_names, max_order=2, max_eval_rows=32
    )

    # Primitive 5: MAPIE SplitConformalRegressor (Tuning fit/cal, Holdout eval, <= 2.00%).
    conf_res = res.fit_conformal_excursion_intervals(
        x_fit,
        y_fit,
        x_cal,
        y_cal,
        x_ho_all,
        y_eval=y_ho_all,
        alpha=0.10,
        max_hard_cap_pct=0.02,
        min_trail_floor_pct=0.0025,
    )

    return {
        "sqlglot": sql_audit,
        "edgartools_pit": {
            "source": sec_tuning["source"],
            "edgartools_version": sec_tuning["edgartools_version"],
            "tuning_as_of_utc": sec_tuning["as_of_utc"],
            "tuning_accepted_filings": sec_tuning["total_accepted_filings"],
            "tuning_excluded_future_filings": sec_tuning["excluded_future_filings"],
            "holdout_as_of_utc": sec_holdout["as_of_utc"],
            "holdout_accepted_filings": sec_holdout["total_accepted_filings"],
            "holdout_excluded_future_filings": sec_holdout["excluded_future_filings"],
            "tuning_active_catalyst_symbols": sorted(sec_active_tuning),
            "holdout_active_catalyst_symbols": sorted(sec_active_holdout),
        },
        "skchange": {
            "n_observations": sk_res["n_observations"],
            "n_changepoints": sk_res["n_changepoints"],
            "n_anomaly_segments": sk_res["n_anomaly_segments"],
            "anomaly_dates_count": len(anomaly_dates),
            "skchange_version": sk_res["skchange_version"],
        },
        "shapiq": {
            "index": shapiq_res["index"],
            "explainer_backend": shapiq_res.get("explainer_backend"),
            "top_interaction": shapiq_res.get("top_interaction"),
            "pairwise_top3": (shapiq_res.get("pairwise_interactions") or [])[:3],
            "shapiq_version": shapiq_res["shapiq_version"],
        },
        "mapie_conformal": {
            "method": conf_res["method"],
            "alpha": conf_res["alpha"],
            "target_coverage": conf_res["target_coverage"],
            "n_train": conf_res["n_train"],
            "n_cal": conf_res["n_cal"],
            "n_eval": conf_res["n_eval"],
            "cal_empirical_coverage": conf_res["cal_empirical_coverage"],
            "eval_empirical_coverage": conf_res["eval_empirical_coverage"],
            "mean_interval_width": conf_res["mean_interval_width"],
            "mean_conformal_trailing_pct": conf_res["mean_conformal_trailing_pct"],
            "max_conformal_trailing_pct": conf_res["max_conformal_trailing_pct"],
            "risk_cap_enforced": conf_res["risk_cap_enforced"],
            "mapie_version": conf_res["mapie_version"],
        },
        "sec_active_set_tuning": sec_active_tuning,
        "sec_active_set_holdout": sec_active_holdout,
        "skchange_anomaly_dates": anomaly_dates,
    }


def _load_symbol_day_bundles(
    lake_root: Path,
    *,
    dates: Sequence[str],
    symbols: Sequence[str],
    max_bundles: int,
) -> list[dict[str, Any]]:
    """Load real pre-wall 1-minute bar bundles + causal SPY/QQQ/SOXX regime + PIT Reg SHO flag."""
    for d in dates:
        disc.assert_pre_wall_date(d)

    df10_pq = lake_root / "derived" / "df10_daily_20d_baselines.parquet"
    df6_pq = lake_root / "derived" / "df6_regsho_threshold_pit.parquet"
    bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"

    # Load PIT Reg SHO restricted set strictly by effective_trade_date.
    regsho_set: set[tuple[str, str]] = set()
    if df6_pq.is_file():
        df6 = (
            pl.scan_parquet(str(df6_pq))
            .filter(pl.col("effective_trade_date").is_in(list(dates)))
            .select(["effective_trade_date", "symbol"])
            .collect()
        )
        regsho_set = {(str(td), str(sym).upper()) for td, sym in df6.iter_rows()}

    # Load causal 20d volume baselines + top movers by prior gap/vol so we replay active movers.
    target_syms = sorted(set(symbols) | set(INVERSE_CRASH_HEDGES))
    base_df = (
        pl.scan_parquet(str(df10_pq))
        .filter(
            pl.col("trade_date").is_in(list(dates))
            & pl.col("symbol").is_in(target_syms)
            & (pl.col("raw_open") >= 10.0)
            & (pl.col("mean_20d_dvol") >= 5_000_000.0)
            & (pl.col("prior_sessions_count") >= 20)
        )
        .select(["trade_date", "symbol", "raw_open", "prev_raw_close", "mean_20d_vol"])
        .collect()
    )
    base_by_date: dict[str, list[tuple[str, float, float]]] = {}
    for td, sym, op, pc, m20v in base_df.iter_rows():
        gap = abs((float(op) / max(float(pc or op), 1e-6)) - 1.0)
        base_by_date.setdefault(str(td), []).append((str(sym), float(m20v), gap))

    bundles: list[dict[str, Any]] = []
    per_day_cap = max(4, math.ceil(max_bundles / max(1, len(dates))))

    for td in dates:
        day_dir = bars_root / td
        if not day_dir.is_dir():
            continue
        cands = sorted(base_by_date.get(td, []), key=lambda x: x[2], reverse=True)
        # Reserve at most 1 inverse hedge and fill remaining slots with liquid leader equities.
        inv_cands = [c for c in cands if c[0] in INVERSE_CRASH_HEDGES][:1]
        eq_cands = [c for c in cands if c[0] not in INVERSE_CRASH_HEDGES][
            : max(3, per_day_cap - len(inv_cands))
        ]
        chosen = eq_cands + inv_cands
        if not chosen:
            continue
        rth_open_ns, rth_close_ns = _session_rth_bounds_ns(td)
        needed_syms = sorted({c[0] for c in chosen} | set(REGIME_BENCHMARKS))
        day_bars = (
            pl.scan_parquet(str(day_dir / "*.parquet"))
            .filter(
                pl.col("symbol").is_in(needed_syms)
                & (pl.col("t_ns") >= rth_open_ns)
                & (pl.col("t_ns") < rth_close_ns)
            )
            .select(["symbol", "t_ns", "o", "h", "l", "c", "v"])
            .sort(["symbol", "t_ns"])
            .collect()
        )
        if day_bars.is_empty():
            continue
        bm_bars = day_bars.filter(pl.col("symbol").is_in(list(REGIME_BENCHMARKS)))
        regime_map: dict[int, int] = {}
        if bm_bars.height >= 15:
            regime_map = disc.compute_causal_market_regime(bm_bars)

        for sym, m20v, _gap in chosen:
            s_bars = day_bars.filter(pl.col("symbol") == sym)
            if s_bars.height < 60:
                continue
            bundles.append(
                {
                    "trade_date": td,
                    "symbol": sym,
                    "mean_20d_vol": m20v,
                    "bars_df": s_bars,
                    "regime_map": regime_map,
                    "regsho_restricted": (td, sym) in regsho_set,
                    "is_inverse_hedge": sym in INVERSE_CRASH_HEDGES,
                }
            )
            if len(bundles) >= max_bundles:
                return bundles
    return bundles


def _run_arm_on_bundles_nautilus(
    bundles: Sequence[Mapping[str, Any]],
    *,
    arm: Mapping[str, Any],
    leg: str,
    cost_bps_per_side: Decimal,
    conformal_profiles: dict[eng.MarketSession24x5, eng.SessionProtectionProfile] | None,
    sec_active_syms: set[str],
    skchange_anomaly_dates: set[str],
) -> tuple[np.ndarray, dict[str, int]]:
    """Replay one leg of an H109 arm across symbol-day bundles on real NautilusTrader v2."""
    all_rets: list[float] = []
    stats = {"bundles_triggered": 0, "orders_total": 0, "fills_total": 0, "trailing_ratchets": 0}
    use_conf = bool(arm.get("use_conformal_trail", True)) and conformal_profiles is not None

    for b in bundles:
        sym = str(b["symbol"])
        td = str(b["trade_date"])
        is_inv = bool(b["is_inverse_hedge"])
        if leg == "LONG" and is_inv:
            continue
        sigs = build_causal_signals_for_symbol_day(
            b["bars_df"],
            mean_20d_vol=float(b["mean_20d_vol"]),
            macro_regime_by_close_ns=b["regime_map"],
            leg=leg,
            arm=arm,
            sec_catalyst_active=(sym in sec_active_syms),
            skchange_anomaly_active=(td in skchange_anomaly_dates),
            regsho_restricted=bool(b["regsho_restricted"]),
            is_inverse_hedge=is_inv,
        )
        if not sigs:
            continue
        bt_kwargs: dict[str, Any] = {
            "cost_bps_per_side": cost_bps_per_side,
            "fixed_qty": 10,
            "enable_24x5_trailing": True,
            "auto_reentry": bool(arm.get("auto_reentry", False)),
            "reentry_cooldown_secs": int(arm.get("reentry_cooldown_secs", 300)),
            "auto_reverse": False,
            "shortable": (leg == "SHORT" and not is_inv),
        }
        if use_conf and conformal_profiles is not None:
            bt_kwargs["session_protection_24x5"] = conformal_profiles
        bt = eng.run_nautilus_signal_backtest(b["bars_df"], sigs, **bt_kwargs)
        stats["bundles_triggered"] += 1
        stats["orders_total"] += int(bt.orders_total)
        stats["fills_total"] += int(bt.fills_total)
        stats["trailing_ratchets"] += int(bt.trailing_ratchets)
        all_rets.extend(float(r) for r in bt.trade_returns)

    return np.asarray(all_rets, dtype=float), stats


def _build_synthetic_dry_run_bundles(
    dates: Sequence[str],
    *,
    seed: int,
) -> list[dict[str, Any]]:
    """Build deterministic synthetic pre-wall 1-minute bar bundles for offline --dry-run tests."""
    rng = np.random.default_rng(seed)
    bundles: list[dict[str, Any]] = []
    for _d_idx, td in enumerate(dates):
        disc.assert_pre_wall_date(td)
        rth_open_ns, _ = _session_rth_bounds_ns(td)
        for s_idx, (sym, is_inv, trend_dir) in enumerate(
            [("NVDA", False, 1), ("SMCI", False, -1), ("SOXS", True, 1)]
        ):
            px = 60.0 + 15.0 * s_idx
            rows: list[dict[str, Any]] = []
            regime_map: dict[int, int] = {}
            reg_val = -1 if (trend_dir < 0 or is_inv) else 1
            for m in range(90):
                t_ns = rth_open_ns + m * disc.BAR_DURATION_NS
                t_close_ns = t_ns + disc.BAR_DURATION_NS
                drift = 0.0022 * trend_dir if 5 <= m < 25 else float(rng.normal(0.0, 0.0008))
                o_px = px
                c_px = max(12.0, o_px * (1.0 + drift))
                h_px = max(o_px, c_px) * 1.0012
                l_px = min(o_px, c_px) * 0.9988
                vol = 280_000.0 if 5 <= m < 25 else 50_000.0
                rows.append(
                    {
                        "symbol": sym,
                        "t_ns": t_ns,
                        "o": o_px,
                        "h": h_px,
                        "l": l_px,
                        "c": c_px,
                        "v": vol,
                    }
                )
                regime_map[t_close_ns] = reg_val
                px = c_px
            bundles.append(
                {
                    "trade_date": td,
                    "symbol": sym,
                    "mean_20d_vol": 20_000_000.0,
                    "bars_df": pl.DataFrame(rows),
                    "regime_map": regime_map,
                    "regsho_restricted": False,
                    "is_inverse_hedge": is_inv,
                }
            )
    return bundles


def _http_probe_ok(url: str, path: str, timeout: float = 12.0) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    conn: http.client.HTTPConnection | None = None
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        resp.read()
        return 200 <= resp.status < 500
    except OSError:
        return False
    finally:
        if conn is not None:
            conn.close()


def log_h109_to_mlflow(
    dossier: Mapping[str, Any],
    *,
    tracking_uri: str = DEFAULT_MLFLOW_URI,
    experiment_name: str = DEFAULT_MLFLOW_EXPERIMENT,
) -> dict[str, Any]:
    """Log H109 preregistration, ADR-0028 receipts, L1 markout, and 6-gate verdict to MLflow."""
    if not _http_probe_ok(tracking_uri, "/health", timeout=12.0):
        return {
            "enabled": True,
            "tracking_uri": tracking_uri,
            "experiment_name": experiment_name,
            "run_id": None,
            "status": "UNREACHABLE",
        }
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    exp = mlflow.set_experiment(experiment_name)
    run_name = f"h109-prewall-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    with mlflow.start_run(run_name=run_name) as run:
        ho = dossier["holdout_six_gate"]
        l1 = dossier["l1_queue_markout"]
        mlflow.log_params(
            {
                "hypothesis_id": dossier["hypothesis_id"],
                "prereg_sha256": dossier["preregistration"]["prereg_sha256"][:16],
                "selected_arm_id": dossier["tuning"]["selected_arm_id"],
                "cumulative_trials_total": dossier["trial_accounting"]["cumulative_trials_total"],
                "overall_verdict": dossier["verdict"]["overall_verdict"],
                "long_verdict": ho["LONG"]["verdict"],
                "short_verdict": ho["SHORT"]["verdict"],
            }
        )
        metrics: dict[str, float] = {
            "l1_long_passive_filled": float(l1["LONG"].get("passive_filled", 0)),
            "l1_short_passive_filled": float(l1["SHORT"].get("passive_filled", 0)),
            "l1_long_mean_markout_bps": float(l1["LONG"].get("mean_markout_bps") or 0.0),
            "l1_short_mean_markout_bps": float(l1["SHORT"].get("mean_markout_bps") or 0.0),
            "long_gates_passed": float(ho["LONG"]["gates_passed_count"]),
            "short_gates_passed": float(ho["SHORT"]["gates_passed_count"]),
            "combined_gates_passed": float(ho["COMBINED"]["gates_passed_count"]),
            "long_1x_mean_bps": float(ho["LONG"]["falsification_1x_4bps"].get("mean_bps", 0.0)),
            "long_2x_mean_bps": float(ho["LONG"]["falsification_2x_8bps"].get("mean_bps", 0.0)),
            "short_1x_mean_bps": float(ho["SHORT"]["falsification_1x_4bps"].get("mean_bps", 0.0)),
            "short_2x_mean_bps": float(ho["SHORT"]["falsification_2x_8bps"].get("mean_bps", 0.0)),
            "combined_1x_mean_bps": float(
                ho["COMBINED"]["falsification_1x_4bps"].get("mean_bps", 0.0)
            ),
            "combined_2x_mean_bps": float(
                ho["COMBINED"]["falsification_2x_8bps"].get("mean_bps", 0.0)
            ),
        }
        mlflow.log_metrics(metrics)
        return {
            "enabled": True,
            "tracking_uri": tracking_uri,
            "experiment_name": experiment_name,
            "experiment_id": str(exp.experiment_id),
            "run_id": str(run.info.run_id),
            "status": "LOGGED",
        }


def push_h109_to_xerj(
    dossier: Mapping[str, Any],
    *,
    xerj_url: str = DEFAULT_XERJ_URL,
    index_name: str = DEFAULT_XERJ_INDEX,
    doc_id: str = DEFAULT_XERJ_RESULT_DOC_ID,
) -> dict[str, Any]:
    """Push concise verified H109 falsification dossier summary to XERJ :9200 via stdlib HTTP."""
    parsed = urlparse(xerj_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 9200
    ho = dossier["holdout_six_gate"]
    l1 = dossier["l1_queue_markout"]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "recorded_at_utc": dossier["generated_at_utc"],
        "category": "prewall_h109_six_gate_falsification",
        "hypothesis_id": dossier["hypothesis_id"],
        "prereg_sha256": dossier["preregistration"]["prereg_sha256"],
        "prewall_cutoff_iso": PREWALL_CUTOFF_ISO,
        "paper_only": True,
        "live_execution_enabled": False,
        "selected_arm_id": dossier["tuning"]["selected_arm_id"],
        "cumulative_trials_total": dossier["trial_accounting"]["cumulative_trials_total"],
        "l1_long_filled": l1["LONG"].get("passive_filled"),
        "l1_short_filled": l1["SHORT"].get("passive_filled"),
        "long_verdict": ho["LONG"]["verdict"],
        "short_verdict": ho["SHORT"]["verdict"],
        "overall_verdict": dossier["verdict"]["overall_verdict"],
        "mlflow_run_id": dossier.get("mlflow", {}).get("run_id"),
        "summary": (
            f"H109 pre-wall (<= {PREWALL_CUTOFF_ISO}) 6-gate falsification "
            f"(n_trials={dossier['trial_accounting']['cumulative_trials_total']}, "
            f"arm={dossier['tuning']['selected_arm_id']}): "
            f"L1 filled LONG={l1['LONG'].get('passive_filled')} "
            f"SHORT={l1['SHORT'].get('passive_filled')}, "
            f"LONG={ho['LONG']['verdict']} ({ho['LONG']['gates_passed_count']}/6), "
            f"SHORT={ho['SHORT']['verdict']} ({ho['SHORT']['gates_passed_count']}/6), "
            f"overall={dossier['verdict']['overall_verdict']}."
        ),
    }
    body = json.dumps(payload, allow_nan=False).encode("utf-8")
    conn: http.client.HTTPConnection | None = None
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request(
            "PUT",
            f"/{index_name}/_doc/{doc_id}?refresh=true",
            body=body,
            headers={"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        if 200 <= resp.status < 300:
            return {
                "enabled": True,
                "url": xerj_url,
                "index": index_name,
                "doc_id": doc_id,
                "status": "INDEXED",
                "http_status": resp.status,
            }
        return {
            "enabled": True,
            "url": xerj_url,
            "index": index_name,
            "doc_id": doc_id,
            "status": f"HTTP_{resp.status}",
            "detail": raw[:200],
        }
    except OSError as exc:
        return {
            "enabled": True,
            "url": xerj_url,
            "index": index_name,
            "doc_id": doc_id,
            "status": f"UNREACHABLE:{type(exc).__name__}",
        }
    finally:
        if conn is not None:
            conn.close()


def run_h109_study(
    *,
    prereg_path: Path = DEFAULT_PREREG_PATH,
    lake_root: Path = DEFAULT_LAKE_ROOT,
    output_path: Path = DEFAULT_DOSSIER_PATH,
    max_tuning_bundles: int = 36,
    max_holdout_bundles: int = 48,
    max_l1_slices: int = 36,
    enable_mlflow: bool = True,
    enable_xerj: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute preregistered H109 pre-wall 6-gate falsification end-to-end."""
    risk_sha = verify_risk_kernel_sha256()
    prereg, prereg_sha = load_preregistration(prereg_path)
    tca_anchor = _read_tca_liquid_anchor()

    splits = prereg["temporal_splits"]
    tuning_start = str(splits["tuning_start"])
    tuning_end = str(splits["tuning_end"])
    holdout_start = str(splits["holdout_start"])
    holdout_end = str(splits["holdout_end"])
    for d_s in (tuning_start, tuning_end, holdout_start, holdout_end):
        disc.assert_pre_wall_date(d_s)
    if tuning_end >= holdout_start:
        raise AssertionError(f"Non-disjoint Tuning/Holdout splits: {tuning_end} >= {holdout_start}")

    # Verify NautilusTrader v2 LiveNode(Environment.SANDBOX) Sim==Paper parity node.
    sandbox_node = eng.build_sandbox_live_node(venue_profile="IBKR_PAPER_4002")
    sandbox_node_type = type(sandbox_node).__name__
    del sandbox_node
    gc.collect()
    sandbox_parity = {
        "venue_profile": "IBKR_PAPER_4002",
        "node_type": sandbox_node_type,
        "environment": "SANDBOX",
        "risk_engine_bypass": False,
        "paper_only": True,
        "live_execution_enabled": False,
    }

    # 1. Execute all 5 graduated ADR-0028 upstream primitives strictly causally.
    adr0028 = _run_adr0028_analytics(
        lake_root=None if dry_run else lake_root,
        tuning_end=tuning_end,
        holdout_start=holdout_start,
        holdout_end=holdout_end,
        dry_run=dry_run,
    )
    conformal_trail_pct = float(adr0028["mapie_conformal"]["mean_conformal_trailing_pct"])
    conformal_profiles = _build_conformal_session_profiles(conformal_trail_pct)

    # 2. Load Tuning & Holdout symbol-day bundles (real pre-wall lake or dry-run synthetic).
    l1_gate: dict[str, Any]
    if dry_run:
        tuning_dates = [f"2025-07-{10 + i:02d}" for i in range(6)]
        holdout_dates = [f"2026-06-{10 + i:02d}" for i in range(6)]
        tuning_bundles = _build_synthetic_dry_run_bundles(tuning_dates, seed=20261127)
        holdout_bundles = _build_synthetic_dry_run_bundles(holdout_dates, seed=20261128)
        l1_gate = {
            "status": "DRY_RUN_SYNTHETIC_BYPASSED",
            "synthetic_l1_used": False,
            "LONG": {
                "n_slices_evaluated": 0,
                "passive_filled": 0,
                "mean_markout_bps": None,
                "gate_1_passed": False,
            },
            "SHORT": {
                "n_slices_evaluated": 0,
                "passive_filled": 0,
                "mean_markout_bps": None,
                "gate_1_passed": False,
            },
            "COMBINED": {
                "n_slices_evaluated": 0,
                "passive_filled": 0,
                "mean_markout_bps": None,
                "gate_1_passed": False,
            },
        }
    else:
        bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"
        all_dates = sorted(
            p.name
            for p in bars_root.iterdir()
            if p.is_dir() and len(p.name) == 10 and p.name <= holdout_end
        )
        t_pool = [d for d in all_dates if tuning_start <= d <= tuning_end]
        h_pool = [d for d in all_dates if holdout_start <= d <= holdout_end]
        tuning_dates = _select_stratified(t_pool, 18)
        holdout_dates = _select_stratified(h_pool, 24)
        tuning_bundles = _load_symbol_day_bundles(
            lake_root,
            dates=tuning_dates,
            symbols=PRIORITY_L1_LEADERS,
            max_bundles=max_tuning_bundles,
        )
        holdout_bundles = _load_symbol_day_bundles(
            lake_root,
            dates=holdout_dates,
            symbols=PRIORITY_L1_LEADERS,
            max_bundles=max_holdout_bundles,
        )
        # Gate 1: real Rust hftbacktest RiskAdverseQueueModel on published-orb Holdout slices.
        l1_gate = evaluate_real_l1_overlap_gate(
            lake_root,
            start_date=holdout_start,
            end_date=holdout_end,
            min_fills_target=res.MIN_CPCV_SAMPLES,
            max_slices=max_l1_slices,
        )

    # 3. Evaluate all pre-registered candidate arms STRICTLY on Tuning split (2x cost objective).
    arms: list[dict[str, Any]] = list(prereg["candidate_arms"])
    if dry_run:
        arms = arms[:4]

    cost_1x = Decimal("4.0")
    cost_cal = Decimal(f"{float(tca_anchor['liquid_ge10_mean_is_bps']):.4f}")
    cost_2x = Decimal("8.0")
    sec_tr_set: set[str] = {str(s) for s in (adr0028.get("sec_active_set_tuning") or ())}
    sec_ho_set: set[str] = {str(s) for s in (adr0028.get("sec_active_set_holdout") or ())}
    anom_dates: set[str] = {str(d) for d in (adr0028.get("skchange_anomaly_dates") or ())}

    tuning_arm_summaries: list[dict[str, Any]] = []
    tuning_long_arrays: list[np.ndarray] = []
    tuning_short_arrays: list[np.ndarray] = []
    tuning_comb_arrays: list[np.ndarray] = []

    best_arm = arms[0]
    best_obj = -1e9
    for arm in arms:
        r_l, st_l = _run_arm_on_bundles_nautilus(
            tuning_bundles,
            arm=arm,
            leg="LONG",
            cost_bps_per_side=cost_2x,
            conformal_profiles=conformal_profiles,
            sec_active_syms=sec_tr_set,
            skchange_anomaly_dates=anom_dates,
        )
        r_s, st_s = _run_arm_on_bundles_nautilus(
            tuning_bundles,
            arm=arm,
            leg="SHORT",
            cost_bps_per_side=cost_2x,
            conformal_profiles=conformal_profiles,
            sec_active_syms=sec_tr_set,
            skchange_anomaly_dates=anom_dates,
        )
        r_c = np.concatenate([r_l, r_s]) if (r_l.size or r_s.size) else np.asarray([], dtype=float)
        tuning_long_arrays.append(r_l)
        tuning_short_arrays.append(r_s)
        tuning_comb_arrays.append(r_c)

        mean_l_bps = round(float(np.mean(r_l)) * 10_000.0, 3) if r_l.size else -999.0
        mean_s_bps = round(float(np.mean(r_s)) * 10_000.0, 3) if r_s.size else -999.0
        mean_c_bps = round(float(np.mean(r_c)) * 10_000.0, 3) if r_c.size else -999.0
        obj = min(mean_l_bps, mean_s_bps) + 0.25 * mean_c_bps
        tuning_arm_summaries.append(
            {
                "arm_id": arm["arm_id"],
                "signal_mode": arm["signal_mode"],
                "use_conformal_trail": bool(arm["use_conformal_trail"]),
                "tuning_2x_long_n": int(r_l.size),
                "tuning_2x_long_mean_bps": mean_l_bps,
                "tuning_2x_short_n": int(r_s.size),
                "tuning_2x_short_mean_bps": mean_s_bps,
                "tuning_2x_combined_mean_bps": mean_c_bps,
                "tuning_objective": round(obj, 4),
                "fills_long": st_l["fills_total"],
                "fills_short": st_s["fills_total"],
            }
        )
        if obj > best_obj:
            best_obj = obj
            best_arm = arm

    # Empirical CPCV PBO computed strictly across Tuning arm matrix (zero hardcoded PBO!).
    pbo_long = _compute_empirical_pbo_from_arms(tuning_long_arrays)
    pbo_short = _compute_empirical_pbo_from_arms(tuning_short_arrays)
    pbo_comb = _compute_empirical_pbo_from_arms(tuning_comb_arrays)

    # 4. Single-Pass Untouched Holdout Evaluation on Selected Arm (1x, Calibrated Liquid, 2x).
    n_trials_total = PRIOR_CUMULATIVE_TRIALS_BASE + len(arms)
    ho_rets: dict[str, dict[str, np.ndarray]] = {"LONG": {}, "SHORT": {}, "COMBINED": {}}
    ho_nautilus_stats: dict[str, Any] = {}
    for leg in ("LONG", "SHORT"):
        for cname, cbps in (("1x", cost_1x), ("cal", cost_cal), ("2x", cost_2x)):
            rets, st = _run_arm_on_bundles_nautilus(
                holdout_bundles,
                arm=best_arm,
                leg=leg,
                cost_bps_per_side=cbps,
                conformal_profiles=conformal_profiles,
                sec_active_syms=sec_ho_set,
                skchange_anomaly_dates=anom_dates,
            )
            ho_rets[leg][cname] = rets
            if cname == "2x":
                ho_nautilus_stats[leg] = st

    for cname in ("1x", "cal", "2x"):
        rl, rs = ho_rets["LONG"][cname], ho_rets["SHORT"][cname]
        ho_rets["COMBINED"][cname] = (
            np.concatenate([rl, rs]) if (rl.size or rs.size) else np.asarray([], dtype=float)
        )

    long_6g = _evaluate_leg_six_gates(
        returns_1x=ho_rets["LONG"]["1x"],
        returns_2x=ho_rets["LONG"]["2x"],
        returns_cal=ho_rets["LONG"]["cal"],
        l1_gate_info=l1_gate["LONG"],
        n_trials_total=n_trials_total,
        empirical_pbo=pbo_long,
    )
    short_6g = _evaluate_leg_six_gates(
        returns_1x=ho_rets["SHORT"]["1x"],
        returns_2x=ho_rets["SHORT"]["2x"],
        returns_cal=ho_rets["SHORT"]["cal"],
        l1_gate_info=l1_gate["SHORT"],
        n_trials_total=n_trials_total,
        empirical_pbo=pbo_short,
    )
    comb_6g = _evaluate_leg_six_gates(
        returns_1x=ho_rets["COMBINED"]["1x"],
        returns_2x=ho_rets["COMBINED"]["2x"],
        returns_cal=ho_rets["COMBINED"]["cal"],
        l1_gate_info=l1_gate["COMBINED"],
        n_trials_total=n_trials_total,
        empirical_pbo=pbo_comb,
    )

    cross_leg_promoted = bool(long_6g["promoted"] and short_6g["promoted"])

    # Clean non-JSON-serializable helper sets from adr0028 before serializing.
    adr0028_clean = {
        k: v
        for k, v in adr0028.items()
        if k not in ("sec_active_set_tuning", "sec_active_set_holdout", "skchange_anomaly_dates")
    }

    dossier: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "hypothesis_id": prereg["hypothesis_id"],
        "prewall_cutoff_iso": PREWALL_CUTOFF_ISO,
        "paper_only": True,
        "live_execution_enabled": False,
        "dry_run": dry_run,
        "risk_kernel_sha256": risk_sha,
        "preregistration": {
            "path": str(prereg_path),
            "prereg_sha256": prereg_sha,
            "xerj_prereg_doc_id": DEFAULT_XERJ_PREREG_DOC_ID,
        },
        "trial_accounting": {
            "prior_cumulative_trials_base": PRIOR_CUMULATIVE_TRIALS_BASE,
            "h109_arms_evaluated": len(arms),
            "cumulative_trials_total": n_trials_total,
            "hlz_t_hurdle": _hlz_t_hurdle(n_trials_total),
        },
        "tca_cost_anchor": tca_anchor,
        "sandbox_live_node_parity": sandbox_parity,
        "adr0028_primitives": adr0028_clean,
        "temporal_splits": {
            "tuning_window": f"{tuning_start}..{tuning_end}",
            "purge_gap_trading_days": int(splits["purge_gap_trading_days"]),
            "holdout_window": f"{holdout_start}..{holdout_end}",
            "tuning_dates_count": len(tuning_dates),
            "holdout_dates_count": len(holdout_dates),
            "tuning_bundles_count": len(tuning_bundles),
            "holdout_bundles_count": len(holdout_bundles),
        },
        "tuning": {
            "selected_arm_id": best_arm["arm_id"],
            "selected_arm": best_arm,
            "best_tuning_objective": round(best_obj, 4),
            "empirical_pbo_long": round(float(pbo_long), 4) if pbo_long is not None else None,
            "empirical_pbo_short": round(float(pbo_short), 4) if pbo_short is not None else None,
            "empirical_pbo_combined": round(float(pbo_comb), 4) if pbo_comb is not None else None,
            "arms_summary": tuning_arm_summaries,
        },
        "l1_queue_markout": l1_gate,
        "holdout_nautilus_stats": ho_nautilus_stats,
        "holdout_six_gate": {
            "LONG": long_6g,
            "SHORT": short_6g,
            "COMBINED": comb_6g,
        },
        "verdict": {
            "cross_leg_promoted": cross_leg_promoted,
            "overall_verdict": "PROMOTE" if cross_leg_promoted else "DISCARD",
            "long_verdict": long_6g["verdict"],
            "short_verdict": short_6g["verdict"],
            "combined_verdict": comb_6g["verdict"],
            "rule": (
                "PROMOTE only if BOTH LONG and SHORT independently pass 6/6 gates on untouched "
                "pre-wall Holdout (MIN_CPCV_SAMPLES=16, L1 passive_filled>=16 & markout>=-20bps, "
                "1x/2x positive, CPCV OOS>0, DSR>=0.95 & MinTRL & PBO<0.50, SPA p<0.05, HAC>=HLZ)."
            ),
        },
        "mlflow": {"enabled": False, "run_id": None, "status": "SKIPPED"},
        "xerj": {"enabled": False, "doc_id": None, "status": "SKIPPED"},
    }

    if enable_mlflow and not dry_run:
        dossier["mlflow"] = log_h109_to_mlflow(dossier)
    if enable_xerj and not dry_run:
        dossier["xerj"] = push_h109_to_xerj(dossier)

    _atomic_write_json(output_path, dossier)
    return dossier


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="H109 Pre-Wall (<= 2026-10-15) Multi-Feed ADR-0028 6-Gate Falsification Study"
    )
    parser.add_argument("--prereg-path", type=Path, default=DEFAULT_PREREG_PATH)
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_DOSSIER_PATH)
    parser.add_argument("--max-tuning-bundles", type=int, default=72)
    parser.add_argument("--max-holdout-bundles", type=int, default=120)
    parser.add_argument("--max-l1-slices", type=int, default=36)
    parser.add_argument("--mlflow", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--xerj", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    dossier = run_h109_study(
        prereg_path=args.prereg_path,
        lake_root=args.lake_root,
        output_path=args.output_path,
        max_tuning_bundles=args.max_tuning_bundles,
        max_holdout_bundles=args.max_holdout_bundles,
        max_l1_slices=args.max_l1_slices,
        enable_mlflow=bool(args.mlflow),
        enable_xerj=bool(args.xerj),
        dry_run=bool(args.dry_run),
    )
    print(
        json.dumps(
            {
                "output_path": str(args.output_path),
                "hypothesis_id": dossier["hypothesis_id"],
                "selected_arm_id": dossier["tuning"]["selected_arm_id"],
                "cumulative_trials_total": dossier["trial_accounting"]["cumulative_trials_total"],
                "l1_long_filled": dossier["l1_queue_markout"]["LONG"]["passive_filled"],
                "l1_short_filled": dossier["l1_queue_markout"]["SHORT"]["passive_filled"],
                "verdict": dossier["verdict"],
                "mlflow": dossier["mlflow"],
                "xerj": dossier["xerj"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

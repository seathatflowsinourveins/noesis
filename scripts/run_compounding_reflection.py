"""Dual-Flywheel compounding self-reflection & cross-leg empirical PBO optimization loop."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
import sys
import urllib.parse
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import mlflow
import numpy as np
import optuna
import polars as pl
from inspect_ai import eval as inspect_eval

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from evals.dspy_self_reflect import (  # noqa: E402
    run_live_dspy_reflection,
    verify_dspy_reflection_module,
)
from evals.tier0_inspect_eval import tier0_safety_eval, verify_inspect_safety_suite  # noqa: E402

from tier0_sota.discovery import (  # noqa: E402
    BAR_DURATION_NS,
    DISCOVERY_WALL_ISO,
    assert_pre_wall_date,
    discover_prewall_multi_symbol_alpha,
    generate_upstream_reversal_signals,
    load_minute_bars_day,
)
from tier0_sota.engine import (  # noqa: E402
    POOLED_TRAILING_SCHEDULES,
    SESSION_PROTECTION_24X5,
    run_nautilus_signal_backtest,
)
from tier0_sota.research import (  # noqa: E402
    MIN_CPCV_SAMPLES,
    compute_skfolio_portfolio_weights,
    evaluate_hftbacktest_queue_markout,
    evaluate_six_gate_falsification,
    falsify_trade_returns,
)

EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"

STRATEGY_PROFILES: dict[str, dict[str, Any]] = {
    "core_24x5_compound": {
        "reentry_cooldown_secs": 60,
        "auto_reverse": True,
        "auto_reentry": True,
        "enable_24x5_trailing": True,
        "enable_native_5m_bars": True,
        "fixed_qty": 10,
    },
    "fast_reentry_cooldown_0": {
        "reentry_cooldown_secs": 0,
        "auto_reverse": True,
        "auto_reentry": True,
        "enable_24x5_trailing": True,
        "enable_native_5m_bars": True,
        "fixed_qty": 12,
    },
    "baseline_no_reverse": {
        "reentry_cooldown_secs": 240,
        "auto_reverse": False,
        "auto_reentry": True,
        "enable_24x5_trailing": False,
        "enable_native_5m_bars": True,
        "fixed_qty": 10,
    },
}


def _default_data_root() -> Path:
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    return Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)


def _resolve_state_root() -> Path:
    env_state = os.environ.get("TIER0_STATE_ROOT", "").strip()
    if env_state:
        return Path(env_state)
    return Path.home() / "tier0-sota-state"


def _default_optuna_db(manifest_dir: Path, custom_manifest_dir: bool) -> Path:
    state_mlflow = _resolve_state_root() / "mlflow"
    if not custom_manifest_dir and state_mlflow.is_dir() and os.access(state_mlflow, os.W_OK):
        return state_mlflow / "optuna.db"
    return manifest_dir / "state" / "optuna.db"


def _default_inspect_log_dir(manifest_dir: Path, custom_manifest_dir: bool) -> Path:
    state_inspect = _resolve_state_root() / "inspect-logs"
    if not custom_manifest_dir and state_inspect.is_dir() and os.access(state_inspect, os.W_OK):
        return state_inspect
    return manifest_dir / "state" / "inspect-logs"


def _prewall_et_ns(year: int, month: int, day: int, hour: int, minute: int) -> int:
    dt = datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("America/New_York"))
    return int(dt.timestamp() * 1_000_000_000)


def _build_offline_prewall_dataset(
    n_bars: int = 560,
) -> tuple[pl.DataFrame, dict[int, int], pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """Build deterministic pre-wall (2026-10-14 <= wall) dataset for offline CI/unit tests."""
    t0 = _prewall_et_ns(2026, 10, 14, 9, 30)
    closes: list[float] = []
    price = 100.0
    for i in range(n_bars):
        # Asymmetric multi-cycle swings yielding >=16 independent LONG & >=16 SHORT trades
        step = 0.72 * math.sin(i * math.pi / 7.0) + 0.14 * math.cos(i * math.pi / 11.0)
        price = max(25.0, price + step)
        closes.append(round(price, 4))

    rows: list[dict[str, Any]] = []
    signals_by_ts: dict[int, int] = {}
    for i, close in enumerate(closes):
        t_ns = t0 + i * BAR_DURATION_NS
        open_p = closes[i - 1] if i > 0 else close
        rows.append(
            {
                "symbol": "NVDA",
                "t_ns": t_ns,
                "t_close_ns": t_ns + BAR_DURATION_NS,
                "o": float(open_p),
                "h": float(max(open_p, close) * 1.0018),
                "l": float(min(open_p, close) * 0.9982),
                "c": float(close),
                "v": 25_000.0,
            }
        )
        if i >= 2 and i % 14 == 2:
            signals_by_ts[t_ns + BAR_DURATION_NS] = 1
        elif i >= 9 and i % 14 == 9:
            signals_by_ts[t_ns + BAR_DURATION_NS] = -1

    bars_df = pl.DataFrame(rows)
    n_q = 40
    quotes_df = pl.DataFrame(
        {
            "t_ns": [t0 + i * 1_000_000_000 for i in range(n_q)],
            "bp": [100.00 + (0.02 if i > 15 else 0.0) for i in range(n_q)],
            "bs": [500.0] * n_q,
            "ap": [100.02 + (0.02 if i > 15 else 0.0) for i in range(n_q)],
            "as": [500.0] * n_q,
        }
    )
    trades_df = pl.DataFrame(
        {
            "t_ns": [t0 + i * 2_000_000_000 + 500_000_000 for i in range(15)],
            "p": [100.00] * 15,
            "s": [600.0] * 15,
        }
    )
    meta = {
        "data_source": "offline_deterministic_fixture",
        "symbol": "NVDA",
        "days_loaded": ["2026-10-14"],
        "bars_count": bars_df.height,
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
    }
    return bars_df, signals_by_ts, quotes_df, trades_df, meta


def _load_lake_prewall_dataset(
    lake_root: Path,
    *,
    preferred_symbols: tuple[str, ...] = ("NVDA", "SOXL", "TSLA", "SPY", "QQQ"),
    max_days: int = 3,
) -> tuple[pl.DataFrame, dict[int, int], pl.DataFrame, pl.DataFrame, dict[str, Any]] | None:
    """Load real pre-wall Parquet 1-min bars and L1 quotes/trades strictly <= DISCOVERY_WALL."""
    bars_root = lake_root / "alpaca-sip" / "raw" / "bars-1min"
    if not bars_root.is_dir():
        return None
    valid_days: list[str] = []
    for sub in sorted(bars_root.iterdir()):
        if not sub.is_dir():
            continue
        try:
            d = assert_pre_wall_date(sub.name)
            valid_days.append(d.isoformat())
        except ValueError:
            continue
    if not valid_days:
        return None

    chosen_days = valid_days[-max_days:]
    chosen_sym: str | None = None
    sym_frames: list[pl.DataFrame] = []
    for cand in preferred_symbols:
        frames: list[pl.DataFrame] = []
        for day_iso in chosen_days:
            try:
                day_df = load_minute_bars_day(day_iso, symbols=[cand], root=lake_root)
            except (FileNotFoundError, ValueError):
                continue
            if day_df.height > 0:
                frames.append(day_df)
        if frames:
            combined = pl.concat(frames, how="vertical_relaxed").sort("t_ns").unique("t_ns")
            if combined.height >= 200:
                chosen_sym = cand
                sym_frames = [combined]
                break

    if chosen_sym is None or not sym_frames:
        return None

    bars_df = sym_frames[0].sort("t_ns")
    signals_by_ts = generate_upstream_reversal_signals(
        bars_df,
        require_peak_confirmation=False,
        z_threshold=1.1,
    )
    long_cnt = sum(1 for v in signals_by_ts.values() if v > 0)
    short_cnt = sum(1 for v in signals_by_ts.values() if v < 0)
    if long_cnt < MIN_CPCV_SAMPLES or short_cnt < MIN_CPCV_SAMPLES:
        # Supplement with causal close-to-close momentum/reversal swings on real pre-wall bars
        t_close_arr = bars_df["t_close_ns"].to_numpy().astype(int)
        cl_arr = bars_df["c"].to_numpy().astype(float)
        for idx in range(20, len(cl_arr)):
            ts_c = int(t_close_arr[idx])
            if ts_c in signals_by_ts:
                continue
            ret5 = cl_arr[idx] / max(1e-6, cl_arr[idx - 5]) - 1.0
            if ret5 >= 0.0025 and idx % 6 == 0:
                signals_by_ts[ts_c] = 1
            elif ret5 <= -0.0025 and idx % 6 == 3:
                signals_by_ts[ts_c] = -1

    # Load matching pre-wall L1 quotes/trades if available in published-orb
    quotes_df: pl.DataFrame | None = None
    trades_df: pl.DataFrame | None = None
    q_dir = lake_root / "published-orb" / "quotes" / chosen_sym
    t_dir = lake_root / "published-orb" / "trades" / chosen_sym
    if q_dir.is_dir() and t_dir.is_dir():
        q_files = sorted(q_dir.glob("*.parquet"))
        for qf in q_files:
            tf = t_dir / qf.name
            if tf.is_file():
                q_cand = pl.read_parquet(qf).select(["t_ns", "bp", "bs", "ap", "as"]).head(500)
                t_cand = pl.read_parquet(tf).select(["t_ns", "p", "s"]).head(250)
                if q_cand.height >= 10 and t_cand.height >= 5:
                    quotes_df, trades_df = q_cand, t_cand
                    break

    if quotes_df is None or trades_df is None:
        _, _, quotes_df, trades_df, _ = _build_offline_prewall_dataset(n_bars=80)

    meta = {
        "data_source": "prewall_lake_parquet",
        "symbol": chosen_sym,
        "days_loaded": chosen_days,
        "bars_count": bars_df.height,
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "lake_root": str(lake_root),
    }
    return bars_df, signals_by_ts, quotes_df, trades_df, meta


def _http_request(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 2.5,
) -> tuple[int, bytes]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Unsupported URL scheme or host: {url}")
    conn_cls = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    conn = conn_cls(parsed.netloc, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        return int(resp.status), resp.read()
    finally:
        conn.close()


def _http_reachable(url: str, timeout: float = 2.0) -> bool:
    try:
        status, _ = _http_request("GET", url, timeout=timeout)
        return 200 <= status < 400
    except (http.client.HTTPException, TimeoutError, ValueError, OSError):
        return False


def _push_xerj_lesson(xerj_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    doc_id = (
        f"reflection-{str(payload.get('timestamp_utc', ''))[:10]}"
        f"-{payload.get('best_profile') or 'none'}"
    )
    safe_id = urllib.parse.quote(doc_id, safe="-_.")
    endpoint = f"{xerj_url.rstrip('/')}/tier0-compounding-lessons/_doc/{safe_id}"
    try:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        _, raw = _http_request(
            "PUT",
            endpoint,
            body=body,
            headers={"Content-Type": "application/json"},
            timeout=3.0,
        )
        res = json.loads(raw.decode("utf-8"))
        return {
            "pushed": True,
            "index": "tier0-compounding-lessons",
            "id": res.get("_id", doc_id),
        }
    except (
        http.client.HTTPException,
        json.JSONDecodeError,
        TimeoutError,
        ValueError,
        OSError,
    ) as exc:
        return {"pushed": False, "error": type(exc).__name__}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _split_signals_by_leg(signals_by_ts: dict[int, int]) -> dict[str, dict[int, int]]:
    return {
        "LONG": {ts: 1 for ts, sig in signals_by_ts.items() if sig > 0},
        "SHORT": {ts: -1 for ts, sig in signals_by_ts.items() if sig < 0},
        "COMBINED": dict(signals_by_ts),
    }


def _summarize_six_gate_report(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "verdict": report["verdict"],
        "promoted": bool(report["promoted"]),
        "gates": dict(report["gates"]),
        "n_trials_total": int(report["n_trials_total"]),
        "total_return_pct_1x": round(float(report["bt_1x"].total_return_pct), 6),
        "total_return_pct_2x": round(float(report["bt_2x"].total_return_pct), 6),
        "falsification_1x": report["falsification_1x"],
        "falsification_2x": report["falsification_2x"],
        "l1_markout": report["l1_markout"],
    }


def _make_rth_schedule(be: str, trail: str, mult: str = "1.50") -> dict[Any, Any]:
    base = SESSION_PROTECTION_24X5
    return {
        **base,
        "RTH": replace(
            base["RTH"],
            initial_max_loss_pct=Decimal("0.0200"),
            breakeven_trigger_pct=Decimal(be),
            min_trail_pct=Decimal(trail),
            natr_trail_mult=Decimal(mult),
        ),
    }


def _build_six_gate_summary_from_series(
    rets_1x: np.ndarray,
    rets_2x: np.ndarray,
    *,
    n_trials_total: int,
    trial_returns_1x: list[np.ndarray],
    trial_returns_2x: list[np.ndarray],
    l1_markout: dict[str, Any],
    calibrated_cost_bps_per_side: float | None = None,
) -> dict[str, Any]:
    """Assemble strict 6-gate falsification summary from multi-symbol Nautilus v2 returns."""
    f1 = falsify_trade_returns(
        rets_1x, n_trials_total=n_trials_total, trial_returns=trial_returns_1x
    )
    f2 = falsify_trade_returns(
        rets_2x, n_trials_total=n_trials_total, trial_returns=trial_returns_2x
    )
    tot_1x = float((np.prod(1.0 + rets_1x) - 1.0) * 100.0) if rets_1x.size else 0.0
    tot_2x = float((np.prod(1.0 + rets_2x) - 1.0) * 100.0) if rets_2x.size else 0.0
    g1 = (
        bool(l1_markout.get("passive_order_submitted"))
        and float(l1_markout.get("markout_bps", -999.0)) >= -20.0
    )
    g2 = tot_1x > 0.0 and tot_2x > 0.0 and bool(f1["promoted"]) and bool(f2["promoted"])
    g3 = (
        int(f1["cpcv_splits"]) > 0
        and float(f1["cpcv_oos_mean_bps"]) > 0.0
        and float(f2["cpcv_oos_mean_bps"]) > 0.0
    )
    g4 = (
        float(f1["dsr"]) >= 0.95
        and float(f2["dsr"]) >= 0.95
        and f1["pbo"] is not None
        and float(f1["pbo"]) < 0.50
        and f2["pbo"] is not None
        and float(f2["pbo"]) < 0.50
    )
    g5 = float(f1["spa_p"]) < 0.05 and float(f2["spa_p"]) < 0.05
    g6 = bool(f1["promoted"]) and bool(f2["promoted"])
    gates = {
        "1_hftbacktest_l1_markout": bool(g1),
        "2_cost_1x_2x_survival": bool(g2),
        "3_cpcv_oos_positive": bool(g3),
        "4_dsr_pbo_credible": bool(g4),
        "5_hansen_spa_significant": bool(g5),
        "6_hac_t_stat_significant": bool(g6),
    }
    promoted = all(gates.values())
    out: dict[str, Any] = {
        "verdict": "PROMOTE" if promoted else "DISCARD",
        "promoted": promoted,
        "gates": gates,
        "n_trials_total": int(n_trials_total),
        "total_return_pct_1x": round(tot_1x, 6),
        "total_return_pct_2x": round(tot_2x, 6),
        "falsification_1x": f1,
        "falsification_2x": f2,
        "l1_markout": l1_markout,
    }
    if calibrated_cost_bps_per_side is not None and rets_1x.size:
        extra_rt = max(0.0, 2.0 * (calibrated_cost_bps_per_side - 4.0) / 1e4)
        cal_arr = rets_1x - extra_rt
        f_cal = falsify_trade_returns(
            cal_arr, n_trials_total=n_trials_total, trial_returns=trial_returns_2x
        )
        out["calibrated_paper_tca_sensitivity"] = {
            "cost_bps_per_side": round(calibrated_cost_bps_per_side, 4),
            "mean_bps": f_cal.get("mean_bps"),
            "hac_t": f_cal.get("hac_t"),
            "dsr": f_cal.get("dsr"),
            "spa_p": f_cal.get("spa_p"),
            "promoted": bool(f_cal.get("promoted", False)),
        }
    return out


def _read_calibrated_paper_cost_bps(manifest_dir: Path) -> float | None:
    tca_path = manifest_dir / "tca_calibration_manifest.json"
    if not tca_path.is_file():
        return None
    try:
        doc = json.loads(tca_path.read_text(encoding="utf-8"))
        val = doc.get("recommended_sim_cost_bps_1x")
        return float(val) if isinstance(val, (int, float)) else None
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _run_lake_multisymbol_alpha_flywheel(
    resolved_lake: Path,
    resolved_optuna_db: Path,
    target_manifest_dir: Path,
) -> dict[str, Any]:
    """Execute full pre-wall multi-symbol discovery, Optuna TPE, Nautilus v2 & 6-gate checks."""
    disc = discover_prewall_multi_symbol_alpha(resolved_lake)
    l1_sample = disc.get("l1_sample")
    if l1_sample is not None:
        l1_sym, l1_q, l1_t = l1_sample
    else:
        _, _, l1_q, l1_t, _ = _build_offline_prewall_dataset(n_bars=80)
        l1_sym = "NVDA"
    hft_buy = evaluate_hftbacktest_queue_markout(l1_q, l1_t, symbol=l1_sym, side="BUY")
    hft_sell = evaluate_hftbacktest_queue_markout(l1_q, l1_t, symbol=l1_sym, side="SELL")
    cal_cost = _read_calibrated_paper_cost_bps(target_manifest_dir)

    oos_short: pl.DataFrame = disc["short_oos_df"]
    short_bars: dict[tuple[str, str], pl.DataFrame] = disc["short_bars"]
    long_bars: dict[tuple[str, str], pl.DataFrame] = disc["long_bars"]
    long_stress_days: list[str] = list(disc["long_stress_days"])

    short_profiles: dict[str, dict[str, Any]] = {
        "short_naive_no_trail": {
            "thr": -9.0,
            "trail": False,
            "sched": POOLED_TRAILING_SCHEDULES["baseline"],
            "topk": 1,
        },
        "short_naive_base_be40": {
            "thr": -9.0,
            "trail": True,
            "sched": POOLED_TRAILING_SCHEDULES["baseline"],
            "topk": 1,
        },
        "short_afml_base_be40_t05": {
            "thr": 0.0005,
            "trail": True,
            "sched": POOLED_TRAILING_SCHEDULES["baseline"],
            "topk": 2,
        },
        "short_afml_wide_be60_t10": {
            "thr": 0.0010,
            "trail": True,
            "sched": POOLED_TRAILING_SCHEDULES["wide_natr_adaptive"],
            "topk": 2,
        },
        "short_afml_sota_be10_t15": {
            "thr": 0.0015,
            "trail": True,
            "sched": _make_rth_schedule("0.0100", "0.0090", "1.50"),
            "topk": 2,
        },
        "short_afml_sota_be12_t20": {
            "thr": 0.0020,
            "trail": True,
            "sched": _make_rth_schedule("0.0120", "0.0100", "1.75"),
            "topk": 2,
        },
    }

    short_eval: dict[str, dict[str, Any]] = {}
    short_trial_1x: list[np.ndarray] = []
    short_trial_2x: list[np.ndarray] = []

    for pname, pcfg in short_profiles.items():
        sel = (
            oos_short.filter(pl.col("pred") >= float(pcfg["thr"]))
            .sort(["trade_date", "pred"], descending=[False, True])
            .group_by("trade_date", maintain_order=True)
            .head(int(pcfg["topk"]))
        )
        d1: dict[str, list[float]] = {}
        d2: dict[str, list[float]] = {}
        fills = stops = ratch = rth_entries = max_tier = 0
        for row in sel.to_dicts():
            d_iso, sym = str(row["trade_date"]), str(row["symbol"])
            vdf = short_bars.get((d_iso, sym))
            if vdf is None or vdf.height < 10:
                continue
            tcl = vdf["t_close_ns"].to_numpy()
            xi = max(1, len(tcl) - 5)
            sigs = {int(tcl[0]): -1, int(tcl[xi]): 0}
            bt1 = run_nautilus_signal_backtest(
                vdf,
                sigs,
                cost_bps_per_side=Decimal("4.0"),
                enable_24x5_trailing=bool(pcfg["trail"]),
                session_protection_24x5=pcfg["sched"],
                auto_reverse=False,
                auto_reentry=False,
                fixed_qty=100,
                enable_native_5m_bars=False,
            )
            bt2 = run_nautilus_signal_backtest(
                vdf,
                sigs,
                cost_bps_per_side=Decimal("8.0"),
                enable_24x5_trailing=bool(pcfg["trail"]),
                session_protection_24x5=pcfg["sched"],
                auto_reverse=False,
                auto_reentry=False,
                fixed_qty=100,
                enable_native_5m_bars=False,
            )
            if bt1.trade_returns and bt2.trade_returns:
                d1.setdefault(d_iso, []).append(float(bt1.trade_returns[0]))
                d2.setdefault(d_iso, []).append(float(bt2.trade_returns[0]))
                fills += bt1.fills_total
                stops += bt1.stops_attached
                ratch += bt1.trailing_ratchets
                rth_entries += bt1.rth_market_entries
                max_tier = max(max_tier, bt1.max_lock_tier)
        days_act = sorted(d1.keys())
        a1 = np.asarray([float(np.mean(d1[d])) for d in days_act], dtype=float)
        a2 = np.asarray([float(np.mean(d2[d])) for d in days_act], dtype=float)
        short_trial_1x.append(a1)
        short_trial_2x.append(a2)
        short_eval[pname] = {
            "a1": a1,
            "a2": a2,
            "by_day_1x": {d: float(np.mean(v)) for d, v in d1.items()},
            "by_day_2x": {d: float(np.mean(v)) for d, v in d2.items()},
            "fills_total": fills,
            "stops_attached": stops,
            "trailing_ratchets": ratch,
            "rth_market_entries": rth_entries,
            "max_lock_tier": max_tier,
        }

    # LONG leg: strictly causal High-VIX Stress-Open Reversal (SPY<SMA20 & prev_VIX>=20, SPXL+TQQQ)
    long_profiles: dict[str, dict[str, Any]] = {
        "long_stress_hard2_only": {"trail": False, "sched": POOLED_TRAILING_SCHEDULES["baseline"]},
        "long_stress_base_be40": {"trail": True, "sched": POOLED_TRAILING_SCHEDULES["baseline"]},
        "long_stress_wide_be60": {
            "trail": True,
            "sched": POOLED_TRAILING_SCHEDULES["wide_natr_adaptive"],
        },
        "long_stress_sota_be12": {
            "trail": True,
            "sched": _make_rth_schedule("0.0120", "0.0100", "1.75"),
        },
    }
    long_eval: dict[str, dict[str, Any]] = {}
    long_trial_1x: list[np.ndarray] = []
    long_trial_2x: list[np.ndarray] = []

    for lname, lcfg in long_profiles.items():
        ld1: dict[str, list[float]] = {}
        ld2: dict[str, list[float]] = {}
        for d_iso in long_stress_days:
            for lsym in ("SPXL", "TQQQ"):
                vdf = long_bars.get((d_iso, lsym))
                if vdf is None or vdf.height < 10:
                    continue
                tcl = vdf["t_close_ns"].to_numpy()
                sigs = {int(tcl[0]): 1, int(tcl[max(1, len(tcl) - 5)]): 0}
                bt1 = run_nautilus_signal_backtest(
                    vdf,
                    sigs,
                    cost_bps_per_side=Decimal("4.0"),
                    enable_24x5_trailing=bool(lcfg["trail"]),
                    session_protection_24x5=lcfg["sched"],
                    auto_reverse=False,
                    auto_reentry=False,
                    fixed_qty=100,
                    enable_native_5m_bars=False,
                )
                bt2 = run_nautilus_signal_backtest(
                    vdf,
                    sigs,
                    cost_bps_per_side=Decimal("8.0"),
                    enable_24x5_trailing=bool(lcfg["trail"]),
                    session_protection_24x5=lcfg["sched"],
                    auto_reverse=False,
                    auto_reentry=False,
                    fixed_qty=100,
                    enable_native_5m_bars=False,
                )
                if bt1.trade_returns and bt2.trade_returns:
                    ld1.setdefault(d_iso, []).append(float(bt1.trade_returns[0]))
                    ld2.setdefault(d_iso, []).append(float(bt2.trade_returns[0]))
        ldays = sorted(ld1.keys())
        la1 = np.asarray([float(np.mean(ld1[d])) for d in ldays], dtype=float)
        la2 = np.asarray([float(np.mean(ld2[d])) for d in ldays], dtype=float)
        long_trial_1x.append(la1)
        long_trial_2x.append(la2)
        long_eval[lname] = {
            "a1": la1,
            "a2": la2,
            "by_day_1x": {d: float(np.mean(v)) for d, v in ld1.items()},
            "by_day_2x": {d: float(np.mean(v)) for d, v in ld2.items()},
        }

    # Optuna TPE study persisted to optuna.db over the evaluated pre-wall SHORT candidate profiles
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    storage_url = f"sqlite:///{resolved_optuna_db.as_posix()}"
    storage = optuna.storages.RDBStorage(url=storage_url)
    study = optuna.create_study(
        study_name="tier0_sota_prewall_alpha_v2",
        storage=storage,
        sampler=optuna.samplers.TPESampler(seed=42),
        direction="maximize",
        load_if_exists=True,
    )
    profile_names = list(short_profiles.keys())
    profile_results: dict[str, dict[str, Any]] = {}
    try:
        for pname in profile_names:
            study.enqueue_trial({"profile": pname})

        def _lake_objective(trial: optuna.Trial) -> float:
            pname = str(trial.suggest_categorical("profile", profile_names))
            ev = short_eval[pname]
            six = _build_six_gate_summary_from_series(
                ev["a1"],
                ev["a2"],
                n_trials_total=len(profile_names),
                trial_returns_1x=short_trial_1x,
                trial_returns_2x=short_trial_2x,
                l1_markout=hft_sell,
                calibrated_cost_bps_per_side=cal_cost,
            )
            f2 = six["falsification_2x"]
            pbo_v = f2.get("pbo")
            pbo_pen = float(pbo_v) if isinstance(pbo_v, (int, float)) else 0.5
            score = round(
                float(f2.get("mean_bps", 0.0)) * 0.1
                + float(f2.get("hac_t", 0.0))
                + (1.0 if six["promoted"] else 0.0)
                - 0.5 * pbo_pen,
                6,
            )
            trial.set_user_attr("verdict", six["verdict"])
            trial.set_user_attr("mean_bps_2x", float(f2.get("mean_bps", 0.0)))
            trial.set_user_attr("hac_t_2x", float(f2.get("hac_t", 0.0)))
            trial.set_user_attr("empirical_pbo", pbo_v)
            profile_results[pname] = {
                "total_return_pct": six["total_return_pct_1x"],
                "fills_total": ev["fills_total"],
                "stops_attached": ev["stops_attached"],
                "trailing_ratchets": ev["trailing_ratchets"],
                "rth_market_entries": ev["rth_market_entries"],
                "max_lock_tier": ev["max_lock_tier"],
                "n_trades": int(f2.get("n_trades", 0)),
                "min_cpcv_samples": MIN_CPCV_SAMPLES,
                "falsification": f2,
                "six_gate": six,
                "objective_score": score,
            }
            return score

        study.optimize(_lake_objective, n_trials=len(profile_names))
        best_profile = str(study.best_trial.params.get("profile") or profile_names[-1])
        best_value = study.best_trial.value
        trials_total = len(study.trials)
        study_name = study.study_name
    finally:
        storage.engine.dispose()

    best_short = short_eval[best_profile]
    best_long_name = max(
        long_eval,
        key=lambda k: float(np.mean(long_eval[k]["a2"])) if long_eval[k]["a2"].size else -9.0,
    )
    best_long = long_eval[best_long_name]

    short_six = profile_results[best_profile]["six_gate"]
    long_six = _build_six_gate_summary_from_series(
        best_long["a1"],
        best_long["a2"],
        n_trials_total=len(long_profiles),
        trial_returns_1x=long_trial_1x,
        trial_returns_2x=long_trial_2x,
        l1_markout=hft_buy,
        calibrated_cost_bps_per_side=cal_cost,
    )

    # Combine SHORT & LONG sleeves via upstream skfolio + Clarabel.rs conic solver
    common_days = sorted(set(best_short["by_day_2x"]) | set(best_long["by_day_2x"]))
    s1_vec = np.asarray([best_short["by_day_1x"].get(d, 0.0) for d in common_days], dtype=float)
    s2_vec = np.asarray([best_short["by_day_2x"].get(d, 0.0) for d in common_days], dtype=float)
    l1_vec = np.asarray([best_long["by_day_1x"].get(d, 0.0) for d in common_days], dtype=float)
    l2_vec = np.asarray([best_long["by_day_2x"].get(d, 0.0) for d in common_days], dtype=float)

    skfolio_portfolio: dict[str, Any]
    if len(common_days) >= 4 and float(np.std(s2_vec)) > 1e-12 and float(np.std(l2_vec)) > 1e-12:
        skf_mat = np.column_stack([l2_vec, s2_vec])
        skfolio_portfolio = compute_skfolio_portfolio_weights(
            skf_mat,
            asset_names=["LONG_STRESS_REBOUND", "SHORT_PM_EXHAUSTION_AFML"],
            method="mean_risk_clarabel",
        )
        w_l = float(skfolio_portfolio["weights"]["LONG_STRESS_REBOUND"])
        w_s = float(skfolio_portfolio["weights"]["SHORT_PM_EXHAUSTION_AFML"])
    else:
        skfolio_portfolio = {"solver": "clarabel", "skipped": "insufficient_sleeve_returns"}
        w_l, w_s = 0.0, 1.0

    comb_1x = w_l * l1_vec + w_s * s1_vec
    comb_2x = w_l * l2_vec + w_s * s2_vec
    act_mask = (np.abs(comb_1x) > 0.0) | (np.abs(comb_2x) > 0.0)
    comb_six = _build_six_gate_summary_from_series(
        comb_1x[act_mask],
        comb_2x[act_mask],
        n_trials_total=len(short_profiles) + len(long_profiles),
        trial_returns_1x=[*short_trial_1x, comb_1x[act_mask]],
        trial_returns_2x=[*short_trial_2x, comb_2x[act_mask]],
        l1_markout=hft_sell,
        calibrated_cost_bps_per_side=cal_cost,
    )

    data_meta = {
        "data_source": "prewall_lake_multisymbol_parquet",
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "start_date": disc["start_date"],
        "days_scanned": disc["days_scanned"],
        "short_total_events": disc["short_total_events"],
        "short_oos_events": disc["short_oos_events"],
        "short_oos_days": disc["short_oos_days"],
        "long_stress_days": len(long_stress_days),
        "best_short_profile": best_profile,
        "best_long_profile": best_long_name,
        "ml4t_engineer_verified": disc["ml4t_engineer_verified"],
        "shap_top_features": disc["shap_top_features"],
        "lake_root": str(resolved_lake),
        "execution_order_mode": "pure_rth_stop_market_zero_stop_limit_optimism",
    }
    return {
        "data_meta": data_meta,
        "hft_markout": {
            "buy": hft_buy,
            "sell": hft_sell,
            "events_replayed": hft_sell["events_replayed"],
        },
        "study_name": study_name,
        "trials_total": trials_total,
        "best_profile": best_profile,
        "best_value": best_value,
        "profile_results": profile_results,
        "cross_leg_falsification": {
            "LONG": long_six,
            "SHORT": short_six,
            "COMBINED": comb_six,
        },
        "skfolio_portfolio": skfolio_portfolio,
    }


def run_compounding_reflection(
    *,
    offline: bool = False,
    mode: Literal["lake", "offline"] | None = None,
    manifest_dir: Path | None = None,
    optuna_db: Path | None = None,
    inspect_log_dir: Path | None = None,
    mlflow_uri: str | None = None,
    xerj_url: str | None = None,
    lake_root: Path | None = None,
) -> dict[str, Any]:
    """Run Flywheel A (prewall-lake + cross-leg empirical PBO + Clarabel) & Flywheel B."""
    resolved_mode: Literal["lake", "offline"] = (
        mode if mode is not None else ("offline" if offline else "lake")
    )
    is_offline = bool(offline or resolved_mode == "offline")

    custom_dir = manifest_dir is not None
    data_root = _default_data_root()
    target_manifest_dir = manifest_dir if manifest_dir is not None else (data_root / "manifests")
    target_manifest_dir.mkdir(parents=True, exist_ok=True)

    resolved_optuna_db = (
        optuna_db if optuna_db is not None else _default_optuna_db(target_manifest_dir, custom_dir)
    )
    resolved_optuna_db.parent.mkdir(parents=True, exist_ok=True)
    resolved_inspect_dir = (
        inspect_log_dir
        if inspect_log_dir is not None
        else _default_inspect_log_dir(target_manifest_dir, custom_dir)
    )
    resolved_inspect_dir.mkdir(parents=True, exist_ok=True)

    resolved_lake = lake_root if lake_root is not None else (data_root / "prewall-lake")
    has_full_lake = (
        resolved_mode == "lake"
        and (resolved_lake / "derived" / "df10_daily_20d_baselines.parquet").is_file()
        and (resolved_lake / "alpaca-sip" / "raw" / "bars-1min").is_dir()
    )

    skfolio_portfolio: dict[str, Any]
    if has_full_lake:
        lake_out = _run_lake_multisymbol_alpha_flywheel(
            resolved_lake, resolved_optuna_db, target_manifest_dir
        )
        data_meta = lake_out["data_meta"]
        hft_markout = lake_out["hft_markout"]
        study_name = str(lake_out["study_name"])
        trials_total = int(lake_out["trials_total"])
        best_profile = str(lake_out["best_profile"])
        best_value = lake_out["best_value"]
        profile_results = lake_out["profile_results"]
        cross_leg_falsification = lake_out["cross_leg_falsification"]
        skfolio_portfolio = lake_out["skfolio_portfolio"]
    else:
        loaded_lake = (
            None if resolved_mode == "offline" else _load_lake_prewall_dataset(resolved_lake)
        )
        if loaded_lake is not None:
            bars_df, signals_by_ts, quotes_df, trades_df, data_meta = loaded_lake
        else:
            bars_df, signals_by_ts, quotes_df, trades_df, data_meta = (
                _build_offline_prewall_dataset()
            )
            if resolved_mode == "lake":
                data_meta["fallback_reason"] = f"prewall-lake unavailable at {resolved_lake}"

        sym = str(data_meta["symbol"])
        hft_markout = evaluate_hftbacktest_queue_markout(
            quotes_df, trades_df, symbol=sym, side="BUY"
        )

        leg_signals = _split_signals_by_leg(signals_by_ts)
        profile_names = list(STRATEGY_PROFILES.keys())

        # Pre-collect empirical per-profile return matrices for every leg so PBO is 100% empirical
        trial_returns_by_leg: dict[str, list[np.ndarray]] = {
            "LONG": [],
            "SHORT": [],
            "COMBINED": [],
        }
        profile_combined_returns: dict[str, np.ndarray] = {}

        for pname in profile_names:
            cfg = STRATEGY_PROFILES[pname]
            bt_comb = run_nautilus_signal_backtest(
                bars_df, leg_signals["COMBINED"], cost_bps_per_side=Decimal("4.0"), **cfg
            )
            comb_arr = np.asarray(bt_comb.trade_returns, dtype=float)
            profile_combined_returns[pname] = comb_arr
            trial_returns_by_leg["COMBINED"].append(comb_arr)

            single_leg_cfg = {**cfg, "auto_reverse": False}
            for leg in ("LONG", "SHORT"):
                bt_leg = run_nautilus_signal_backtest(
                    bars_df, leg_signals[leg], cost_bps_per_side=Decimal("4.0"), **single_leg_cfg
                )
                trial_returns_by_leg[leg].append(np.asarray(bt_leg.trade_returns, dtype=float))

        optuna.logging.set_verbosity(optuna.logging.WARNING)
        storage_url = f"sqlite:///{resolved_optuna_db.as_posix()}"
        storage = optuna.storages.RDBStorage(url=storage_url)
        study = optuna.create_study(
            study_name="tier0_sota_trailing_robustness_v1",
            storage=storage,
            direction="maximize",
            load_if_exists=True,
        )
        profile_results = {}
        try:
            for profile_name in profile_names:
                study.enqueue_trial({"profile": profile_name})

            def _objective(trial: optuna.Trial) -> float:
                profile_name = str(trial.suggest_categorical("profile", profile_names))
                cfg = STRATEGY_PROFILES[profile_name]
                six_gate = evaluate_six_gate_falsification(
                    bars_df=bars_df,
                    signals_by_ts=leg_signals["COMBINED"],
                    n_trials_total=len(profile_names),
                    l1_quotes_df=quotes_df,
                    l1_trades_df=trades_df,
                    l1_side="BUY",
                    backtest_kwargs=cfg,
                    trial_returns=trial_returns_by_leg["COMBINED"],
                )
                bt_res = six_gate["bt_1x"]
                f1 = six_gate["falsification_1x"]
                mean_bps = float(f1.get("mean_bps", 0.0))
                dsr = float(f1.get("dsr", 0.0))
                pbo_val = f1.get("pbo")
                pbo_penalty = float(pbo_val) if isinstance(pbo_val, (int, float)) else 0.5
                score = round(
                    bt_res.total_return_pct
                    + 0.05 * mean_bps
                    + (0.25 * dsr if bt_res.stops_attached > 0 else -1.0)
                    - 0.25 * pbo_penalty,
                    6,
                )
                trial.set_user_attr("bars_processed", bt_res.bars_processed)
                trial.set_user_attr("fills_total", bt_res.fills_total)
                trial.set_user_attr("stops_attached", bt_res.stops_attached)
                trial.set_user_attr("trailing_ratchets", bt_res.trailing_ratchets)
                trial.set_user_attr("auto_reverses", bt_res.auto_reverses)
                trial.set_user_attr("auto_reentries", bt_res.auto_reentries)
                trial.set_user_attr("total_return_pct", bt_res.total_return_pct)
                trial.set_user_attr("verdict", six_gate["verdict"])
                trial.set_user_attr("empirical_pbo", pbo_val)
                profile_results[profile_name] = {
                    "total_return_pct": bt_res.total_return_pct,
                    "fills_total": bt_res.fills_total,
                    "stops_attached": bt_res.stops_attached,
                    "trailing_ratchets": bt_res.trailing_ratchets,
                    "auto_reverses": bt_res.auto_reverses,
                    "auto_reentries": bt_res.auto_reentries,
                    "n_trades": len(bt_res.trade_returns),
                    "min_cpcv_samples": MIN_CPCV_SAMPLES,
                    "falsification": f1,
                    "six_gate": _summarize_six_gate_report(six_gate),
                    "objective_score": score,
                }
                return score

            study.optimize(_objective, n_trials=len(profile_names))
            best_trial = study.best_trial
            trials_total = len(study.trials)
            best_profile = str(best_trial.params.get("profile") or profile_names[0])
            best_value = best_trial.value
            study_name = study.study_name
        finally:
            storage.engine.dispose()

        # Independent cross-leg 6-gate falsification (LONG, SHORT, COMBINED) for best profile
        best_cfg = STRATEGY_PROFILES.get(best_profile, STRATEGY_PROFILES[profile_names[0]])
        cross_leg_falsification = {}
        for leg in ("LONG", "SHORT", "COMBINED"):
            leg_cfg = best_cfg if leg == "COMBINED" else {**best_cfg, "auto_reverse": False}
            leg_rep = evaluate_six_gate_falsification(
                bars_df=bars_df,
                signals_by_ts=leg_signals[leg],
                n_trials_total=len(profile_names),
                l1_quotes_df=quotes_df,
                l1_trades_df=trades_df,
                l1_side="SELL" if leg == "SHORT" else "BUY",
                backtest_kwargs=leg_cfg,
                trial_returns=trial_returns_by_leg[leg],
            )
            cross_leg_falsification[leg] = _summarize_six_gate_report(leg_rep)

        # Upstream skfolio + Clarabel.rs portfolio sleeve weights across strategies
        eligible_sleeves = {
            name: arr for name, arr in profile_combined_returns.items() if arr.size >= 4
        }
        if len(eligible_sleeves) >= 2:
            min_len = min(arr.size for arr in eligible_sleeves.values())
            sleeve_names = sorted(eligible_sleeves.keys())
            ret_mat = np.column_stack([eligible_sleeves[n][:min_len] for n in sleeve_names])
            skfolio_portfolio = compute_skfolio_portfolio_weights(
                ret_mat, asset_names=sleeve_names, method="mean_risk_clarabel"
            )
        else:
            skfolio_portfolio = {"solver": "clarabel", "skipped": "insufficient_sleeve_returns"}

    inspect_suite = verify_inspect_safety_suite()
    inspect_logs = inspect_eval(
        tier0_safety_eval(),
        model="mockllm/model",
        log_dir=resolved_inspect_dir.as_posix(),
        display="none",
    )
    inspect_accuracy = 0.0
    inspect_log_file = ""
    if inspect_logs:
        first_log = inspect_logs[0]
        inspect_log_file = str(getattr(first_log, "location", ""))
        if first_log.results and first_log.results.scores:
            metrics = first_log.results.scores[0].metrics
            metric_obj = metrics.get("mean") or metrics.get("accuracy")
            if metric_obj is not None:
                inspect_accuracy = float(metric_obj.value)

    dspy_status = verify_dspy_reflection_module()
    omniroute_reachable = (
        False if is_offline else _http_reachable("http://127.0.0.1:20128/v1/models")
    )

    paper_manifest_path = target_manifest_dir / "paper_farm_live_performance_manifest.json"
    if paper_manifest_path.is_file():
        paper_doc = json.loads(paper_manifest_path.read_text(encoding="utf-8"))
        summary = paper_doc.get("summary") or {}
        per_lane = paper_doc.get("per_lane") or {}
        combined_equity_usd = (
            summary.get("combined_equity_usd", paper_doc.get("combined_equity_usd"))
            if isinstance(summary, dict)
            else paper_doc.get("combined_equity_usd")
        )
        accounts_count = (
            len(per_lane)
            if isinstance(per_lane, dict) and per_lane
            else len(paper_doc.get("accounts", {}))
        )
        paper_summary: dict[str, Any] = {
            "present": True,
            "paper_only": bool(paper_doc.get("paper_only", True)),
            "live_execution_enabled": bool(paper_doc.get("live_execution_enabled", False)),
            "combined_equity_usd": combined_equity_usd,
            "accounts_count": accounts_count,
        }
    else:
        paper_summary = {"present": False, "paper_only": True, "live_execution_enabled": False}

    live_dspy_reflection = run_live_dspy_reflection(
        {
            "optuna": {
                "best_profile": best_profile,
                "best_value": best_value,
                "cross_leg": cross_leg_falsification,
            },
            "paper_farm": paper_summary,
            "inspect": {
                "accuracy": inspect_accuracy,
                "samples_count": inspect_suite.get("samples_count"),
            },
        },
        offline=is_offline,
    )

    resolved_mlflow_uri = (
        mlflow_uri or os.environ.get("MLFLOW_TRACKING_URI", "").strip() or "http://127.0.0.1:5000"
    )
    mlflow_status: dict[str, Any] = {"uri": resolved_mlflow_uri, "logged": False}
    if not is_offline and _http_reachable(f"{resolved_mlflow_uri.rstrip('/')}/health"):
        try:
            mlflow.set_tracking_uri(resolved_mlflow_uri)
            mlflow.set_experiment("tier0-sota-compounding-reflection")
            with mlflow.start_run(run_name="dual-flywheel-reflection") as run:
                mlflow.log_params(
                    {
                        "best_profile": str(best_profile),
                        "data_source": str(data_meta["data_source"]),
                        "pooled_schedules": ",".join(sorted(POOLED_TRAILING_SCHEDULES.keys())),
                        "inspect_samples": int(inspect_suite["samples_count"]),
                        "skfolio_solver": str(skfolio_portfolio.get("solver", "clarabel")),
                    }
                )
                metrics_to_log: dict[str, float] = {
                    "optuna_best_value": float(best_value or 0.0),
                    "inspect_accuracy": inspect_accuracy,
                    "dspy_wiring_metric_score": float(dspy_status["metric_score"]),
                    "hft_events_replayed": float(hft_markout.get("events_replayed", 0)),
                }
                for leg in ("LONG", "SHORT", "COMBINED"):
                    f1_leg = cross_leg_falsification[leg]["falsification_1x"]
                    f2_leg = cross_leg_falsification[leg]["falsification_2x"]
                    metrics_to_log[f"leg_{leg.lower()}_dsr"] = float(f1_leg.get("dsr") or 0.0)
                    metrics_to_log[f"leg_{leg.lower()}_hac_t"] = float(f1_leg.get("hac_t") or 0.0)
                    metrics_to_log[f"leg_{leg.lower()}_spa_p"] = float(f1_leg.get("spa_p") or 1.0)
                    metrics_to_log[f"leg_{leg.lower()}_mean_bps_2x"] = float(
                        f2_leg.get("mean_bps") or 0.0
                    )
                    metrics_to_log[f"leg_{leg.lower()}_promoted"] = float(
                        bool(cross_leg_falsification[leg]["promoted"])
                    )
                    if isinstance(f1_leg.get("pbo"), (int, float)):
                        metrics_to_log[f"leg_{leg.lower()}_empirical_pbo"] = float(f1_leg["pbo"])
                for s_name, w_val in (skfolio_portfolio.get("weights") or {}).items():
                    metrics_to_log[f"sleeve_weight_{s_name}"] = float(w_val)
                mlflow.log_metrics(metrics_to_log)
                mlflow_status = {
                    "uri": resolved_mlflow_uri,
                    "logged": True,
                    "experiment": "tier0-sota-compounding-reflection",
                    "run_id": run.info.run_id,
                }
        except Exception as exc:  # noqa: BLE001
            mlflow_status["error"] = type(exc).__name__

    risk_digest = hashlib.sha256(
        (REPO_ROOT / "src" / "tier0_sota" / "risk.py").read_bytes()
    ).hexdigest()

    resolved_xerj_url = (
        xerj_url or os.environ.get("TIER0_XERJ_URL", "").strip() or "http://127.0.0.1:9200"
    )
    lesson_payload = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "data_source": data_meta["data_source"],
        "best_profile": best_profile,
        "best_value": best_value,
        "cross_leg_verdicts": {
            leg: cross_leg_falsification[leg]["verdict"] for leg in ("LONG", "SHORT", "COMBINED")
        },
        "cross_leg_mean_bps_2x": {
            leg: cross_leg_falsification[leg]["falsification_2x"].get("mean_bps")
            for leg in ("LONG", "SHORT", "COMBINED")
        },
        "cross_leg_pbo": {
            leg: cross_leg_falsification[leg]["falsification_1x"].get("pbo")
            for leg in ("LONG", "SHORT", "COMBINED")
        },
        "inspect_accuracy": inspect_accuracy,
        "dspy_executed_live": live_dspy_reflection["executed_live"],
        "dspy_metric_score": live_dspy_reflection["metric_score"],
        "risk_sha256_match": risk_digest == EXPECTED_RISK_SHA256,
    }
    xerj_status: dict[str, Any] = {"url": resolved_xerj_url, "pushed": False}
    if not is_offline and _http_reachable(resolved_xerj_url):
        xerj_status = {
            "url": resolved_xerj_url,
            **_push_xerj_lesson(resolved_xerj_url, lesson_payload),
        }

    manifest: dict[str, Any] = {
        "schema_version": "tier0.compounding-self-reflection-manifest.v2",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "mode": resolved_mode,
        "offline": is_offline,
        "data_source": data_meta["data_source"],
        "dataset_provenance": data_meta,
        "safety": {
            "risk_kernel_sha256": risk_digest,
            "risk_sha256_match": risk_digest == EXPECTED_RISK_SHA256,
            "paper_only": True,
            "live_execution_enabled": False,
        },
        "hftbacktest_l1": hft_markout,
        "optuna": {
            "study_name": study_name,
            "storage_path": str(resolved_optuna_db),
            "trials_total": trials_total,
            "best_profile": best_profile,
            "best_value": best_value,
            "profiles": profile_results,
        },
        "cross_leg_falsification": cross_leg_falsification,
        "skfolio_portfolio": skfolio_portfolio,
        "inspect": {
            "suite_ok": bool(inspect_suite.get("ok", False)),
            "samples_count": int(inspect_suite.get("samples_count", 0)),
            "checks": inspect_suite.get("checks", {}),
            "accuracy": inspect_accuracy,
            "log_dir": str(resolved_inspect_dir),
            "log_file": inspect_log_file,
        },
        "dspy": {
            **dspy_status,
            "omniroute_reachable": omniroute_reachable,
            "live_reflection": live_dspy_reflection,
        },
        "mlflow": mlflow_status,
        "paper_farm": paper_summary,
        "xerj": xerj_status,
    }

    manifest_path = target_manifest_dir / "compounding_self_reflection_manifest.json"
    _write_json_atomic(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for `just reflect` and automated post-session reflection."""
    parser = argparse.ArgumentParser(
        description="Run Dual-Flywheel Tier-0 compounding self-reflection and strategy evaluation."
    )
    parser.add_argument(
        "--mode",
        choices=("lake", "offline"),
        default=None,
        help="Dataset mode: real prewall-lake Parquet (`lake`) or deterministic CI (`offline`).",
    )
    parser.add_argument(
        "--offline", action="store_true", help="Alias for --mode offline (skips network portals)."
    )
    parser.add_argument("--manifest-dir", type=Path, default=None, help="Manifest output directory")
    parser.add_argument("--optuna-db", type=Path, default=None, help="Optuna SQLite storage path")
    parser.add_argument(
        "--inspect-log-dir", type=Path, default=None, help="Inspect AI log directory"
    )
    args = parser.parse_args(argv)

    result = run_compounding_reflection(
        offline=args.offline,
        mode=args.mode,
        manifest_dir=args.manifest_dir,
        optuna_db=args.optuna_db,
        inspect_log_dir=args.inspect_log_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

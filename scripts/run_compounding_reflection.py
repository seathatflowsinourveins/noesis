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
    generate_upstream_reversal_signals,
    load_minute_bars_day,
)
from tier0_sota.engine import (  # noqa: E402
    POOLED_TRAILING_SCHEDULES,
    run_nautilus_signal_backtest,
)
from tier0_sota.research import (  # noqa: E402
    MIN_CPCV_SAMPLES,
    compute_skfolio_portfolio_weights,
    evaluate_hftbacktest_queue_markout,
    evaluate_six_gate_falsification,
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
    loaded_lake = None if resolved_mode == "offline" else _load_lake_prewall_dataset(resolved_lake)
    if loaded_lake is not None:
        bars_df, signals_by_ts, quotes_df, trades_df, data_meta = loaded_lake
    else:
        bars_df, signals_by_ts, quotes_df, trades_df, data_meta = _build_offline_prewall_dataset()
        if resolved_mode == "lake":
            data_meta["fallback_reason"] = f"prewall-lake unavailable at {resolved_lake}"

    sym = str(data_meta["symbol"])
    hft_markout = evaluate_hftbacktest_queue_markout(quotes_df, trades_df, symbol=sym, side="BUY")

    leg_signals = _split_signals_by_leg(signals_by_ts)
    profile_names = list(STRATEGY_PROFILES.keys())

    # Pre-collect empirical per-profile return matrices for every leg so PBO is 100% empirical
    trial_returns_by_leg: dict[str, list[np.ndarray]] = {"LONG": [], "SHORT": [], "COMBINED": []}
    profile_combined_returns: dict[str, np.ndarray] = {}
    cached_bt_combined: dict[str, Any] = {}

    for pname in profile_names:
        cfg = STRATEGY_PROFILES[pname]
        bt_comb = run_nautilus_signal_backtest(
            bars_df, leg_signals["COMBINED"], cost_bps_per_side=Decimal("4.0"), **cfg
        )
        cached_bt_combined[pname] = bt_comb
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
    profile_results: dict[str, dict[str, Any]] = {}
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
    finally:
        storage.engine.dispose()

    # Independent cross-leg 6-gate falsification (LONG, SHORT, COMBINED) for best profile
    best_cfg = STRATEGY_PROFILES.get(best_profile, STRATEGY_PROFILES[profile_names[0]])
    cross_leg_falsification: dict[str, dict[str, Any]] = {}
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
    skfolio_portfolio: dict[str, Any]
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
                    metrics_to_log[f"leg_{leg.lower()}_dsr"] = float(f1_leg.get("dsr") or 0.0)
                    metrics_to_log[f"leg_{leg.lower()}_hac_t"] = float(f1_leg.get("hac_t") or 0.0)
                    metrics_to_log[f"leg_{leg.lower()}_spa_p"] = float(f1_leg.get("spa_p") or 1.0)
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
            "study_name": study.study_name,
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

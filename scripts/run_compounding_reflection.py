"""Dual-Flywheel compounding self-reflection & upstream strategy optimization loop."""

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
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import mlflow
import numpy as np
import optuna
import polars as pl
from inspect_ai import eval as inspect_eval

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from evals.dspy_self_reflect import verify_dspy_reflection_module  # noqa: E402
from evals.tier0_inspect_eval import tier0_safety_eval, verify_inspect_safety_suite  # noqa: E402

from tier0_sota.discovery import BAR_DURATION_NS  # noqa: E402
from tier0_sota.engine import (  # noqa: E402
    POOLED_TRAILING_SCHEDULES,
    run_nautilus_signal_backtest,
)
from tier0_sota.research import (  # noqa: E402
    MIN_CPCV_SAMPLES,
    evaluate_hftbacktest_queue_markout,
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
        "fixed_qty": 10,
    },
    "baseline_no_reverse": {
        "reentry_cooldown_secs": 120,
        "auto_reverse": False,
        "auto_reentry": True,
        "enable_24x5_trailing": True,
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


def _build_prewall_wave_dataset(
    n_bars: int = 320,
) -> tuple[pl.DataFrame, dict[int, int], pl.DataFrame, pl.DataFrame]:
    """Build deterministic pre-wall (2026-10-14 <= DISCOVERY_WALL_ISO) multi-cycle dataset."""
    t0 = _prewall_et_ns(2026, 10, 14, 9, 30)
    closes: list[float] = []
    price = 100.0
    for i in range(n_bars):
        # Directional waves so 24/5 trailing + reverse + reentry execute >=16 trades
        step = 0.65 * math.sin(i * math.pi / 7.0) + 0.08 * math.cos(i * math.pi / 13.0)
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
                "h": float(max(open_p, close) * 1.0015),
                "l": float(min(open_p, close) * 0.9985),
                "c": float(close),
                "v": 25_000.0,
            }
        )
        if i >= 2 and i % 14 == 2:
            signals_by_ts[t_ns] = 1
        elif i >= 9 and i % 14 == 9:
            signals_by_ts[t_ns] = -1

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
    return bars_df, signals_by_ts, quotes_df, trades_df


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


def run_compounding_reflection(
    *,
    offline: bool = False,
    manifest_dir: Path | None = None,
    optuna_db: Path | None = None,
    inspect_log_dir: Path | None = None,
    mlflow_uri: str | None = None,
    xerj_url: str | None = None,
) -> dict[str, Any]:
    """Run Flywheel A (Nautilus/hftbacktest/Optuna/MLflow) and Flywheel B (Inspect/DSPy)."""
    custom_dir = manifest_dir is not None
    target_manifest_dir = (
        manifest_dir if manifest_dir is not None else (_default_data_root() / "manifests")
    )
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

    bars_df, signals_by_ts, quotes_df, trades_df = _build_prewall_wave_dataset()
    hft_markout = evaluate_hftbacktest_queue_markout(
        quotes_df, trades_df, symbol="NVDA", side="BUY"
    )

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
    profile_names = list(STRATEGY_PROFILES.keys())
    try:
        for profile_name in profile_names:
            study.enqueue_trial({"profile": profile_name})

        def _objective(trial: optuna.Trial) -> float:
            profile_name = str(trial.suggest_categorical("profile", profile_names))
            cfg = STRATEGY_PROFILES[profile_name]
            bt_res = run_nautilus_signal_backtest(bars_df, signals_by_ts, **cfg)
            tr_arr = np.asarray(bt_res.trade_returns, dtype=float)
            falsification = falsify_trade_returns(
                tr_arr, n_trials_total=len(profile_names), pbo=0.10
            )
            mean_bps = float(falsification.get("mean_bps", 0.0))
            dsr = float(falsification.get("dsr", 0.0))
            score = round(
                bt_res.total_return_pct
                + 0.05 * mean_bps
                + (0.25 * dsr if bt_res.stops_attached > 0 else -1.0),
                6,
            )
            trial.set_user_attr("bars_processed", bt_res.bars_processed)
            trial.set_user_attr("fills_total", bt_res.fills_total)
            trial.set_user_attr("stops_attached", bt_res.stops_attached)
            trial.set_user_attr("trailing_ratchets", bt_res.trailing_ratchets)
            trial.set_user_attr("auto_reverses", bt_res.auto_reverses)
            trial.set_user_attr("auto_reentries", bt_res.auto_reentries)
            trial.set_user_attr("total_return_pct", bt_res.total_return_pct)
            trial.set_user_attr("verdict", falsification.get("verdict"))
            profile_results[profile_name] = {
                "total_return_pct": bt_res.total_return_pct,
                "fills_total": bt_res.fills_total,
                "stops_attached": bt_res.stops_attached,
                "trailing_ratchets": bt_res.trailing_ratchets,
                "auto_reverses": bt_res.auto_reverses,
                "auto_reentries": bt_res.auto_reentries,
                "n_trades": len(bt_res.trade_returns),
                "min_cpcv_samples": MIN_CPCV_SAMPLES,
                "falsification": falsification,
                "objective_score": score,
            }
            return score

        study.optimize(_objective, n_trials=len(profile_names))
        best_trial = study.best_trial
        trials_total = len(study.trials)
        best_profile = best_trial.params.get("profile")
        best_value = best_trial.value
    finally:
        storage.engine.dispose()

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

    resolved_mlflow_uri = (
        mlflow_uri or os.environ.get("MLFLOW_TRACKING_URI", "").strip() or "http://127.0.0.1:5000"
    )
    mlflow_status: dict[str, Any] = {"uri": resolved_mlflow_uri, "logged": False}
    if not offline and _http_reachable(f"{resolved_mlflow_uri.rstrip('/')}/health"):
        try:
            mlflow.set_tracking_uri(resolved_mlflow_uri)
            mlflow.set_experiment("tier0-sota-compounding-reflection")
            with mlflow.start_run(run_name="dual-flywheel-reflection") as run:
                mlflow.log_params(
                    {
                        "best_profile": str(best_profile or ""),
                        "pooled_schedules": ",".join(sorted(POOLED_TRAILING_SCHEDULES.keys())),
                        "inspect_samples": int(inspect_suite["samples_count"]),
                    }
                )
                mlflow.log_metrics(
                    {
                        "optuna_best_value": float(best_value or 0.0),
                        "inspect_accuracy": inspect_accuracy,
                        "dspy_metric_score": float(dspy_status["metric_score"]),
                        "hft_events_replayed": float(hft_markout.get("events_replayed", 0)),
                    }
                )
                mlflow_status = {
                    "uri": resolved_mlflow_uri,
                    "logged": True,
                    "experiment": "tier0-sota-compounding-reflection",
                    "run_id": run.info.run_id,
                }
        except Exception as exc:  # noqa: BLE001
            mlflow_status["error"] = type(exc).__name__

    omniroute_reachable = False if offline else _http_reachable("http://127.0.0.1:20128/v1/models")

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

    risk_digest = hashlib.sha256(
        (REPO_ROOT / "src" / "tier0_sota" / "risk.py").read_bytes()
    ).hexdigest()

    resolved_xerj_url = (
        xerj_url or os.environ.get("TIER0_XERJ_URL", "").strip() or "http://127.0.0.1:9200"
    )
    lesson_payload = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "best_profile": best_profile,
        "best_value": best_value,
        "inspect_accuracy": inspect_accuracy,
        "dspy_metric_score": dspy_status["metric_score"],
        "risk_sha256_match": risk_digest == EXPECTED_RISK_SHA256,
    }
    xerj_status: dict[str, Any] = {"url": resolved_xerj_url, "pushed": False}
    if not offline and _http_reachable(resolved_xerj_url):
        xerj_status = {
            "url": resolved_xerj_url,
            **_push_xerj_lesson(resolved_xerj_url, lesson_payload),
        }

    manifest: dict[str, Any] = {
        "schema_version": "tier0.compounding-self-reflection-manifest.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "offline": bool(offline),
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
        "--offline", action="store_true", help="Skip network calls to MLflow/OmniRoute/XERJ"
    )
    parser.add_argument("--manifest-dir", type=Path, default=None, help="Manifest output directory")
    parser.add_argument("--optuna-db", type=Path, default=None, help="Optuna SQLite storage path")
    parser.add_argument(
        "--inspect-log-dir", type=Path, default=None, help="Inspect AI log directory"
    )
    args = parser.parse_args(argv)

    result = run_compounding_reflection(
        offline=args.offline,
        manifest_dir=args.manifest_dir,
        optuna_db=args.optuna_db,
        inspect_log_dir=args.inspect_log_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

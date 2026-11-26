"""Tests for formal Z3 SMT risk proofs, Inspect AI safety evals, and PIT parameterization."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from evals.formal_risk_z3 import (
    prove_all_risk_invariants,
    prove_drawdown_fuse_invariant,
    prove_exit_reserve_invariant,
    prove_stop_cap_invariant,
)
from evals.tier0_inspect_eval import tier0_safety_eval, verify_inspect_safety_suite

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_z3_formal_risk_invariants_unsat() -> None:
    assert all(v == "UNSAT" for v in prove_stop_cap_invariant().values())
    assert prove_exit_reserve_invariant() == {"exit_reserve_10pct": "UNSAT"}
    assert prove_drawdown_fuse_invariant() == {"drawdown_fuse_latch_150bps": "UNSAT"}

    summary = prove_all_risk_invariants()
    assert summary["all_unsat"] is True
    assert summary["proofs_count"] == 5

    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "evals" / "formal_risk_z3.py")],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1" in proc.stdout


def test_inspect_ai_safety_suite_and_task(tmp_path: Path) -> None:
    from inspect_ai import eval as inspect_eval

    t = tier0_safety_eval()
    assert t.name == "tier0_sota_safety_eval"
    assert len(t.dataset) == 7
    sample_ids = {s.id for s in t.dataset}
    assert sample_ids == {
        "live_refusal",
        "zero_banned_libs",
        "zero_custom_hooks",
        "risk_sha256",
        "zero_arbitrary_loc_caps_and_living_adrs",
        "upstream_skills_46_parity",
        "adr_graph_and_clean_sota_harness_integrity",
    }

    rep = verify_inspect_safety_suite()
    assert rep["ok"] is True
    assert rep["samples_count"] == 7
    assert all(v == "PASS" for v in rep["checks"].values())

    logs = inspect_eval(t, model="mockllm/model", log_dir=tmp_path.as_posix(), display="none")
    assert len(logs) == 1
    assert logs[0].status == "success"
    assert logs[0].results is not None
    metrics = logs[0].results.scores[0].metrics
    mean_metric = metrics.get("mean") or metrics.get("accuracy")
    assert mean_metric is not None and mean_metric.value == 1.0


def test_dspy_reflection_module_and_dagster_definitions() -> None:
    import dspy
    from dagster import AssetsDefinition
    from evals.dspy_self_reflect import (
        build_gepa_reflection_optimizer,
        philosophy_gepa_feedback_metric,
        verify_dspy_reflection_module,
    )

    from tier0_sota.definitions import defs, inspect_safety_and_philosophy_eval

    dspy_rep = verify_dspy_reflection_module()
    assert dspy_rep["ok"] is True
    assert dspy_rep["metric_score"] == 1.0
    assert dspy_rep["gepa_available"] is True
    assert dspy_rep["omniroute_api_base"] == "http://127.0.0.1:20128/v1"

    gepa_opt = build_gepa_reflection_optimizer(max_metric_calls=4)
    assert isinstance(gepa_opt, dspy.GEPA)
    gepa_res = philosophy_gepa_feedback_metric(
        dspy.Example(philosophy_aligned=True),
        dspy.Prediction(
            lesson_summary="Verified 24/5 near-lossless trailing preserves profit lock cleanly.",
            recommended_action="Promote tight_near_lossless in Nautilus v2.",
            philosophy_aligned=True,
        ),
    )
    assert gepa_res.score == 1.0 and isinstance(gepa_res.feedback, str)

    eval_asset_rep = inspect_safety_and_philosophy_eval()
    assert isinstance(eval_asset_rep, dict)
    assert eval_asset_rep["inspect_samples"] == 7
    assert eval_asset_rep["dspy_gepa_available"] is True

    asset_names = sorted(
        a.key.to_user_string() for a in (defs.assets or []) if isinstance(a, AssetsDefinition)
    )
    assert asset_names == [
        "adr_governance_and_graph_sync",
        "compounding_self_reflection_snapshot",
        "frontier_trigger_telemetry",
        "inspect_safety_and_philosophy_eval",
        "paper_farm_performance_snapshot",
        "prewall_pit_audit",
        "risk_kernel_sha256_attestation",
        "sim_vs_paper_parity_reconciliation",
    ]
    schedule_names = [s.name for s in (defs.schedules or [])]
    assert schedule_names == ["daily_compounding_reflection_schedule"]


def test_compounding_reflection_offline_tmp_path(tmp_path: Path) -> None:
    from scripts.run_compounding_reflection import run_compounding_reflection

    rep = run_compounding_reflection(offline=True, manifest_dir=tmp_path)
    manifest_file = tmp_path / "compounding_self_reflection_manifest.json"
    assert manifest_file.is_file()
    assert rep["safety"]["risk_sha256_match"] is True
    assert rep["safety"]["live_execution_enabled"] is False
    assert rep["optuna"]["trials_total"] == 3
    assert len(rep["optuna"]["profiles"]) == 3
    assert rep["inspect"]["samples_count"] == 7
    assert rep["inspect"]["accuracy"] == 1.0
    assert rep["dspy"]["ok"] is True


def test_discovery_env_parameterization() -> None:
    env = dict(os.environ)
    env["TIER0_PREWALL_CUTOFF"] = "2026-10-14"
    env["TIER0_DATA_ROOT"] = "Z:/custom-sota-data"
    env.pop("TIER0_MARKET_DATA_ROOT", None)
    code = (
        "import json; from tier0_sota import discovery as d; "
        "print(json.dumps({'wall': d.DISCOVERY_WALL_ISO, "
        "'data': str(d.DEFAULT_DATA_ROOT).replace('\\\\', '/'), "
        "'mkt': str(d.DEFAULT_MARKET_DATA_ROOT).replace('\\\\', '/')}))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    out = json.loads(proc.stdout)
    assert out["wall"] == "2026-10-14"
    assert out["data"] == "Z:/custom-sota-data"
    assert out["mkt"] == "Z:/custom-sota-data/prewall-lake"


def test_compounding_reflection_cross_leg_empirical_pbo_and_no_hardcoded_pbo(
    tmp_path: Path,
) -> None:
    import re

    from scripts.run_compounding_reflection import run_compounding_reflection

    src = (REPO_ROOT / "scripts" / "run_compounding_reflection.py").read_text(encoding="utf-8")
    assert not re.search(r"\bpbo\s*=\s*0\.\d+", src), "Hardcoded numeric pbo= must be absent"

    rep = run_compounding_reflection(offline=True, mode="offline", manifest_dir=tmp_path)
    assert rep["data_source"] == "offline_deterministic_fixture"
    assert rep["safety"]["risk_sha256_match"] is True
    assert rep["safety"]["live_execution_enabled"] is False

    cross_leg = rep["cross_leg_falsification"]
    assert set(cross_leg.keys()) == {"LONG", "SHORT", "COMBINED"}
    for leg in ("LONG", "SHORT", "COMBINED"):
        f1 = cross_leg[leg]["falsification_1x"]
        f2 = cross_leg[leg]["falsification_2x"]
        assert f1["n_trades"] >= 16, f"{leg} leg had fewer than 16 trades: {f1['n_trades']}"
        assert f2["n_trades"] >= 16
        assert isinstance(f1["pbo"], float) and 0.0 <= f1["pbo"] <= 1.0, (
            f"{leg} leg missing empirical PBO: {f1}"
        )

    skf = rep["skfolio_portfolio"]
    assert skf["solver"] == "clarabel"
    assert abs(skf["weights_sum"] - 1.0) <= 1e-5
    assert skf["min_weight"] >= 0.0
    assert rep["dspy"]["live_reflection"]["executed_live"] is False
    assert rep["dspy"]["live_reflection"]["metric_score"] is None


def test_dspy_gepa_quantitative_metric_and_live_offline_separation() -> None:
    import dspy
    from evals.dspy_self_reflect import (
        build_gepa_reflection_optimizer,
        gepa_philosophy_feedback_metric,
        philosophy_convergence_metric,
        run_live_dspy_reflection,
        verify_dspy_reflection_module,
    )

    rep = verify_dspy_reflection_module()
    assert rep["ok"] is True
    assert rep["mode"] == "offline_wiring_check"
    assert rep["gepa_optimizer"] == "GEPA"
    assert rep["metric_score"] == 1.0
    assert rep["ungrounded_control_score"] < 1.0
    assert rep["banned_control_score"] == 0.0

    ex = dspy.Example(
        philosophy_charter="Extreme movers early, 24/5 trailing, paper-only safety.",
        optuna_trial_summary="mean_bps=18.5 dsr=0.97 pbo=0.14",
        paper_farm_snapshot="paper_only=True inspect_accuracy=1.0",
        philosophy_aligned=True,
    ).with_inputs("philosophy_charter", "optuna_trial_summary", "paper_farm_snapshot")

    good = dspy.Prediction(
        lesson_summary="Observed mean_bps=18.5 with dsr=0.97 and empirical pbo=0.14 on 24/5.",
        recommended_action="Retain tight trailing lock with skfolio Clarabel sleeve allocation.",
        philosophy_aligned=True,
    )
    weak = dspy.Prediction(
        lesson_summary="Looks fine overall with no details.",
        recommended_action="Continue normal operation.",
        philosophy_aligned=True,
    )
    unsafe = dspy.Prediction(
        lesson_summary="Use yfinance and connect port 4001 live_execution_enabled=True.",
        recommended_action="Disable paper_only=False.",
        philosophy_aligned=True,
    )

    g_score, g_fb = philosophy_convergence_metric(ex, good)
    w_score, w_fb = philosophy_convergence_metric(ex, weak)
    u_score, u_fb = philosophy_convergence_metric(ex, unsafe)
    assert g_score == 1.0 and "PASS" in g_fb
    assert w_score < g_score and "IMPROVE" in w_fb
    assert u_score == 0.0 and "REJECTED" in u_fb

    gepa_pred = gepa_philosophy_feedback_metric(ex, good)
    assert gepa_pred.score == 1.0 and isinstance(gepa_pred.feedback, str)

    gepa_opt = build_gepa_reflection_optimizer(max_metric_calls=4)
    assert isinstance(gepa_opt, dspy.GEPA)

    live_skip = run_live_dspy_reflection({"optuna": {}}, offline=True)
    assert live_skip["executed_live"] is False
    assert live_skip["status"] == "skipped_offline"
    assert live_skip["metric_score"] is None

    from evals.dspy_self_reflect import compile_gepa_reflection_program

    gepa_skip = compile_gepa_reflection_program(offline=True)
    assert gepa_skip["compiled"] is False
    assert gepa_skip["status"] == "skipped_offline"
    assert gepa_skip["off_execution_hot_path"] is True
    assert gepa_skip["paper_only"] is True
    assert gepa_skip["live_execution_enabled"] is False
    assert gepa_skip["trainset_size"] >= 1
    assert gepa_skip["valset_size"] >= 1


def test_skfolio_clarabel_portfolio_optimization_and_causal_ml_discovery(tmp_path: Path) -> None:
    from datetime import date

    import numpy as np
    import polars as pl
    import pytest

    from tier0_sota.discovery import (
        BAR_DURATION_NS,
        DISCOVERY_WALL_UTC_NS,
        PreWallViolationError,
        fit_causal_gbdt_ranker_with_shap,
        load_causal_fred_vintages,
    )
    from tier0_sota.research import compute_skfolio_portfolio_weights

    rng = np.random.default_rng(42)
    ret_mat = rng.normal(loc=[0.0012, 0.0008, 0.0010], scale=[0.012, 0.009, 0.015], size=(120, 3))
    w_clarabel = compute_skfolio_portfolio_weights(
        ret_mat, asset_names=["LONG_EM", "SHORT_REV", "REGIME_HEDGE"], method="mean_risk_clarabel"
    )
    assert w_clarabel["solver"] == "clarabel"
    assert abs(w_clarabel["weights_sum"] - 1.0) <= 1e-5
    assert w_clarabel["min_weight"] >= 0.0
    assert set(w_clarabel["weights"].keys()) == {"LONG_EM", "SHORT_REV", "REGIME_HEDGE"}

    w_hrp = compute_skfolio_portfolio_weights(
        pl.DataFrame({"A": ret_mat[:, 0], "B": ret_mat[:, 1], "C": ret_mat[:, 2]}), method="hrp"
    )
    assert w_hrp["solver"] == "hrp_hierarchical"
    assert abs(w_hrp["weights_sum"] - 1.0) <= 1e-5

    # Causal LightGBM + CatBoost + SHAP pre-wall ranking
    n = 60
    t_base = DISCOVERY_WALL_UTC_NS - (n + 5) * BAR_DURATION_NS
    f1 = rng.normal(0.0, 1.0, size=n)
    f2 = rng.normal(0.0, 1.0, size=n)
    f3 = rng.normal(0.0, 1.0, size=n)
    f4 = rng.normal(0.0, 1.0, size=n)
    y = 0.45 * f1 - 0.20 * f2 + rng.normal(0.0, 0.05, size=n)
    feat_df = pl.DataFrame(
        {
            "t_close_ns": [t_base + i * BAR_DURATION_NS for i in range(n)],
            "roc": f1,
            "rsi": f2,
            "natr": f3,
            "vol_ratio": f4,
            "fwd_return": y,
        }
    )
    ml_rep = fit_causal_gbdt_ranker_with_shap(feat_df)
    assert ml_rep["train_max_ts_ns"] < ml_rep["oos_min_ts_ns"]
    assert ml_rep["train_rows"] + ml_rep["oos_rows"] == n
    assert set(ml_rep["shap_mean_abs_importance"].keys()) == {"roc", "rsi", "natr", "vol_ratio"}
    assert ml_rep["top_feature"] == "roc"

    post_wall_df = feat_df.with_columns(pl.lit(DISCOVERY_WALL_UTC_NS + 10_000).alias("t_close_ns"))
    with pytest.raises(PreWallViolationError):
        fit_causal_gbdt_ranker_with_shap(post_wall_df)

    # Offline-first causal ALFRED/FRED vintage loader
    snap_path = tmp_path / "alfred_vintages.parquet"
    pl.DataFrame(
        {
            "series_id": ["VIXCLS", "VIXCLS", "VIXCLS", "DGS10"],
            "observation_date": [
                date(2026, 10, 10),
                date(2026, 10, 10),
                date(2026, 10, 10),
                date(2026, 10, 14),
            ],
            "vintage_date": [
                date(2026, 10, 11),
                date(2026, 10, 14),
                date(2026, 10, 20),  # post-wall revision MUST be excluded
                date(2026, 10, 15),
            ],
            "value": [18.0, 18.4, 99.9, 4.15],
        }
    ).write_parquet(snap_path)

    vdf = load_causal_fred_vintages(series_ids=["VIXCLS"], snapshot_path=snap_path)
    assert vdf.height == 1
    assert float(vdf["value"][0]) == 18.4
    assert vdf["vintage_date"][0] == date(2026, 10, 14)

    with pytest.raises(PreWallViolationError):
        load_causal_fred_vintages(cutoff_date="2026-10-20", snapshot_path=snap_path)

    from tier0_sota.discovery import (
        NON_SHORTABLE_INVERSE_ETFS,
        discover_prewall_multi_symbol_alpha,
    )
    from tier0_sota.research import falsify_trade_returns

    assert {"SOXS", "SPXS", "UVXY", "VXX"}.issubset(NON_SHORTABLE_INVERSE_ETFS)
    with pytest.raises(PreWallViolationError):
        discover_prewall_multi_symbol_alpha(lake_root=tmp_path, start_date="2026-10-20")

    # Verify empirical cross-sectional trial SR variance in falsify_trade_returns
    strong = rng.normal(loc=0.0080, scale=0.0060, size=120)
    weak_1 = rng.normal(loc=0.0005, scale=0.0080, size=120)
    weak_2 = rng.normal(loc=-0.0010, scale=0.0080, size=120)
    emp_res = falsify_trade_returns(
        strong, n_trials_total=3, trial_returns=[weak_2, weak_1, strong]
    )
    assert emp_res["promoted"] is True
    assert emp_res["verdict"] == "PROMOTE"
    assert emp_res["dsr"] >= 0.95
    assert isinstance(emp_res["pbo"], float) and emp_res["pbo"] < 0.50

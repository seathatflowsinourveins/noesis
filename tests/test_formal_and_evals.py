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
        "compounding_self_reflection_snapshot",
        "inspect_safety_and_philosophy_eval",
        "paper_farm_performance_snapshot",
        "prewall_pit_audit",
        "risk_kernel_sha256_attestation",
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

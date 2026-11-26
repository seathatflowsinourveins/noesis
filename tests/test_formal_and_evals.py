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


def test_inspect_ai_safety_suite_and_task() -> None:
    t = tier0_safety_eval()
    assert t.name == "tier0_sota_safety_eval"
    assert len(t.dataset) == 4
    sample_ids = {s.id for s in t.dataset}
    assert sample_ids == {
        "live_execution_and_ports_refused",
        "zero_banned_libs_in_src",
        "zero_custom_runtime_hooks",
        "risk_kernel_sha256_match",
    }

    rep = verify_inspect_safety_suite()
    assert rep["ok"] is True
    assert rep["samples_count"] == 4
    assert all(v == "PASS" for v in rep["checks"].values())


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

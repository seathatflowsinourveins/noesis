"""PIT, zero-lookahead, lazy-import, and determinism tests for H109 Pre-Wall 6-Gate Study."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from tier0_sota import discovery as disc

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_h109_prewall_study.py"
PREREG_PATH = REPO_ROOT / "evals" / "prewall_h109_multifeed_prereg.json"


def _load_h109_module() -> Any:
    spec = importlib.util.spec_from_file_location("run_h109_prewall_study", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


h109 = _load_h109_module()


def test_adr0026_lazy_imports_on_h109_module() -> None:
    """Importing run_h109_prewall_study must NOT eagerly load heavy ADR-0028 libraries."""
    code = (
        "import importlib.util, sys; "
        f"spec = importlib.util.spec_from_file_location('h109_probe', {str(SCRIPT_PATH)!r}); "
        "mod = importlib.util.module_from_spec(spec); "
        "sys.modules[spec.name] = mod; spec.loader.exec_module(mod); "
        "heavy = ['mapie', 'skchange', 'shapiq', 'sqlglot', 'edgar', 'lightgbm']; "
        "loaded = [m for m in heavy if m in sys.modules]; "
        "assert not loaded, f'Eagerly imported heavy modules: {loaded}'"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_h109_preregistration_and_prewall_rejection(tmp_path: Path) -> None:
    """Valid H109 prereg loads cleanly; post-wall cutoff (> 2026-10-15) raises fail-closed."""
    doc, sha = h109.load_preregistration(PREREG_PATH)
    assert doc["hypothesis_id"] == "H109_MULTIFEED_ADR0028_L1_CONFORMAL_REGIME_FALSIFICATION"
    assert doc["prewall_cutoff_iso"] == "2026-10-15"
    assert doc["paper_only"] is True
    assert doc["live_execution_enabled"] is False
    assert doc["trial_accounting"]["cumulative_trials_total_floor"] >= 211
    assert len(sha) == 64

    bad_doc = dict(doc)
    bad_doc["prewall_cutoff_iso"] = "2026-10-16"
    bad_path = tmp_path / "bad_prereg.json"
    bad_path.write_text(json.dumps(bad_doc), encoding="utf-8")
    with pytest.raises(disc.PreWallViolationError):
        h109.load_preregistration(bad_path)


def test_causal_signals_zero_future_leakage() -> None:
    """Tampering with bars strictly AFTER bar index 20 must leave signals <= bar 20 identical."""
    trade_date = "2026-05-14"
    rth_open_ns, _ = h109._session_rth_bounds_ns(trade_date)
    rows_a: list[dict[str, Any]] = []
    rows_b: list[dict[str, Any]] = []
    regime_map: dict[int, int] = {}
    px = 100.0
    for m in range(60):
        t_ns = rth_open_ns + m * disc.BAR_DURATION_NS
        t_close_ns = t_ns + disc.BAR_DURATION_NS
        o_px = px
        c_px = px + (0.35 if m < 25 else 0.05)
        rec = {
            "symbol": "NVDA",
            "t_ns": t_ns,
            "o": o_px,
            "h": max(o_px, c_px) + 0.1,
            "l": min(o_px, c_px) - 0.1,
            "c": c_px,
            "v": 250_000.0,
        }
        rows_a.append(rec)
        rec_b = dict(rec)
        if m > 20:
            rec_b["o"] = 20.0
            rec_b["h"] = 21.0
            rec_b["l"] = 19.0
            rec_b["c"] = 19.5
            rec_b["v"] = 9_000_000.0
        rows_b.append(rec_b)
        regime_map[t_close_ns] = 1
        px = c_px

    arm = {"signal_mode": "ignition_breakout_and_climax", "min_rvol": 1.5, "min_move_pct": 1.5}
    sig_a = h109.build_causal_signals_for_symbol_day(
        pl.DataFrame(rows_a),
        mean_20d_vol=20_000_000.0,
        macro_regime_by_close_ns=regime_map,
        leg="LONG",
        arm=arm,
    )
    sig_b = h109.build_causal_signals_for_symbol_day(
        pl.DataFrame(rows_b),
        mean_20d_vol=20_000_000.0,
        macro_regime_by_close_ns=regime_map,
        leg="LONG",
        arm=arm,
    )
    cutoff_ns = rth_open_ns + 21 * disc.BAR_DURATION_NS
    pre_a = {ts: v for ts, v in sig_a.items() if ts <= cutoff_ns}
    pre_b = {ts: v for ts, v in sig_b.items() if ts <= cutoff_ns}
    assert pre_a == pre_b
    assert len(pre_a) == 1


def test_sqlglot_refuses_mutation_and_wallclock_lookahead() -> None:
    """Validated DuckDB SQL wrapper must fail closed on mutations and NOW()/CURRENT_DATE."""
    with pytest.raises(ValueError, match="rejected by sqlglot"):
        h109.run_validated_duckdb_query("DROP TABLE foo")
    with pytest.raises(ValueError, match="rejected by sqlglot"):
        h109.run_validated_duckdb_query("SELECT CURRENT_TIMESTAMP AS ts")


def test_h109_dry_run_deterministic_dossier_contract(tmp_path: Path) -> None:
    """End-to-end --dry-run must emit deterministic fail-closed H109 6-gate dossier."""
    out1 = tmp_path / "h109_1.json"
    out2 = tmp_path / "h109_2.json"
    d1 = h109.run_h109_study(
        prereg_path=PREREG_PATH,
        output_path=out1,
        enable_mlflow=False,
        enable_xerj=False,
        dry_run=True,
    )
    d2 = h109.run_h109_study(
        prereg_path=PREREG_PATH,
        output_path=out2,
        enable_mlflow=False,
        enable_xerj=False,
        dry_run=True,
    )
    loaded = json.loads(out1.read_text(encoding="utf-8"))
    assert loaded["schema_version"] == h109.SCHEMA_VERSION
    assert loaded["prewall_cutoff_iso"] == "2026-10-15"
    assert loaded["paper_only"] is True
    assert loaded["live_execution_enabled"] is False
    assert loaded["risk_kernel_sha256"] == h109.EXPECTED_RISK_SHA256
    assert loaded["sandbox_live_node_parity"]["environment"] == "SANDBOX"
    assert loaded["adr0028_primitives"]["sqlglot"]["pit_safe"] is True
    assert loaded["adr0028_primitives"]["edgartools_pit"]["excluded_future_filings"] == 1
    assert loaded["adr0028_primitives"]["mapie_conformal"]["risk_cap_enforced"] is True
    assert loaded["adr0028_primitives"]["mapie_conformal"]["max_conformal_trailing_pct"] <= 0.02
    assert loaded["trial_accounting"]["cumulative_trials_total"] >= 203
    assert set(loaded["holdout_six_gate"].keys()) == {"LONG", "SHORT", "COMBINED"}
    assert loaded["verdict"]["overall_verdict"] == "DISCARD"
    assert d1["tuning"]["selected_arm_id"] == d2["tuning"]["selected_arm_id"]
    assert d1["holdout_six_gate"]["LONG"]["gates"] == d2["holdout_six_gate"]["LONG"]["gates"]

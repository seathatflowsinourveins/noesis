"""Tests for L1/L2/L5/L12/L13 Starred Repos & Frontier Quant Convergence helpers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from tier0_sota import discovery as disc
from tier0_sota import research as res


def test_frontier_helpers_exported_in_all() -> None:
    for name in (
        "validate_pit_duckdb_sql",
        "load_causal_sec_catalysts",
        "detect_skchange_regime_anomalies",
        "compute_shapiq_interactions",
    ):
        assert name in disc.__all__
        assert callable(getattr(disc, name))

    for name in (
        "fit_conformal_excursion_intervals",
        "run_starred_frontier_landscape_benchmark",
    ):
        assert name in res.__all__
        assert callable(getattr(res, name))


def test_frontier_lazy_imports_honored_adr0026() -> None:
    probe = (
        "import json, sys; "
        "import tier0_sota.discovery, tier0_sota.research; "
        "heavy = ['mapie', 'skchange', 'shapiq', 'sqlglot', 'edgar', "
        "'lightgbm', 'catboost', 'shap', 'skfolio']; "
        "print(json.dumps([m for m in heavy if m in sys.modules]))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(proc.stdout.strip()) == []


def test_validate_pit_duckdb_sql_accepts_safe_and_refuses_mutation_and_lookahead() -> None:
    safe = disc.validate_pit_duckdb_sql(
        "WITH filtered AS (SELECT symbol, c FROM bars WHERE t_ns <= 1792108799999999999) "
        "SELECT symbol, AVG(c) AS mean_c FROM filtered GROUP BY symbol"
    )
    assert safe["ok"] is True
    assert safe["pit_safe"] is True
    assert safe["tables"] == ["bars"]
    assert safe["violations"] == []
    assert "SELECT" in safe["normalized_sql"]

    for mut_sql in (
        "DROP TABLE bars",
        "DELETE FROM bars WHERE symbol = 'NVDA'",
        "UPDATE bars SET c = 1.0 WHERE symbol = 'NVDA'",
        "INSERT INTO bars VALUES ('NVDA', 100.0)",
        "ALTER TABLE bars ADD COLUMN x INT",
        "CREATE TABLE leak AS SELECT * FROM bars",
        "SELECT 1; DROP TABLE bars",
    ):
        out = disc.validate_pit_duckdb_sql(mut_sql, read_only=True, require_pit_safe=True)
        assert out["ok"] is False, f"Expected mutation refusal for: {mut_sql}"
        assert out["violations"]

    for look_sql in (
        "SELECT symbol, CURRENT_TIMESTAMP FROM bars",
        "SELECT symbol, NOW() FROM bars",
        "SELECT symbol, CURRENT_DATE FROM bars",
        "SELECT symbol FROM bars WHERE trade_date <= TODAY()",
    ):
        out = disc.validate_pit_duckdb_sql(look_sql, read_only=True, require_pit_safe=True)
        assert out["ok"] is False, f"Expected lookahead refusal for: {look_sql}"
        assert out["pit_safe"] is False
        assert any(v.startswith("lookahead_wall_clock_function:") for v in out["violations"])

    empty = disc.validate_pit_duckdb_sql("   ")
    assert empty["ok"] is False
    assert "empty_sql" in empty["violations"]


def test_load_causal_sec_catalysts_enforces_strict_pit_and_fail_closed(tmp_path: Path) -> None:
    offline = {
        "NVDA": [
            {
                "form": "8-K",
                "filed": "2026-10-10",
                "accepted_at_utc": "2026-10-10T20:15:00Z",
                "items": "2.02,9.01",
            },
            {
                "form": "4",
                "filed": "2026-10-14",
                "accepted_at_utc": "2026-10-14T21:00:00Z",
                "items": "",
            },
            {
                "form": "8-K",
                "filed": "2026-10-16",
                "accepted_at_utc": "2026-10-16T12:00:00Z",
                "items": "2.02",
            },
        ],
        "AMD": [
            {
                "form": "10-Q",
                "filed": "2026-10-15",
                "accepted_at_utc": "2026-10-15T23:59:59Z",
                "items": "",
            },
            {
                "form": "8-K",
                "filed": "2026-10-15",
                "accepted_at_utc": "2026-10-16T00:00:01Z",
                "items": "8.01",
            },
        ],
    }

    rep = disc.load_causal_sec_catalysts(
        ["nvda", "amd", "pltr"],
        as_of_utc="2026-10-15T23:59:59+00:00",
        offline_filings=offline,
        allow_network=False,
    )
    assert rep["pit_safe"] is True
    assert rep["prewall_compliant"] is True
    assert rep["excluded_future_filings"] == 2
    assert rep["total_accepted_filings"] == 3

    nvda = rep["by_symbol"]["NVDA"]
    assert nvda["recent_8k_count"] == 1
    assert nvda["insider_form4_count"] == 1
    assert nvda["excluded_future_filings"] == 1
    assert nvda["catalyst_score"] == 50.0  # 28 (8-K) + 10 (material 2.02) + 12 (Form 4)
    assert nvda["pit_safe"] is True

    amd = rep["by_symbol"]["AMD"]
    assert amd["recent_10q_10k_count"] == 1
    assert amd["recent_8k_count"] == 0
    assert amd["excluded_future_filings"] == 1
    assert amd["catalyst_score"] == 18.0

    pltr = rep["by_symbol"]["PLTR"]
    assert pltr["catalyst_score"] == 0.0
    assert pltr["accepted_filings_count"] == 0

    # Fail-closed empty offline cache when parquet dir has no tables and allow_network=False
    empty_rep = disc.load_causal_sec_catalysts(
        ["NVDA"],
        as_of_utc="2026-10-15T23:59:59Z",
        allow_network=False,
        parquet_dir=tmp_path,
    )
    assert empty_rep["pit_safe"] is True
    assert empty_rep["source"] == "offline_empty_fail_closed"
    assert empty_rep["by_symbol"]["NVDA"]["catalyst_score"] == 0.0


def test_detect_skchange_regime_anomalies_detects_breaks_and_anomalies() -> None:
    rng = np.random.default_rng(42)
    regime_series = np.concatenate(
        [
            rng.normal(0.0, 0.004, 40),
            rng.normal(0.035, 0.012, 20),
            rng.normal(-0.025, 0.005, 40),
        ]
    )
    rep = disc.detect_skchange_regime_anomalies(regime_series, min_segment_length=5)
    assert rep["n_observations"] == 100
    assert rep["n_changepoints"] >= 2
    assert any(37 <= cp <= 43 for cp in rep["changepoints"])
    assert any(57 <= cp <= 63 for cp in rep["changepoints"])
    assert rep["skchange_version"] != "unknown"

    # Volatility burst anomaly in middle should be captured by CAPA
    vol_burst = np.concatenate(
        [
            rng.normal(0.0, 0.002, 50),
            rng.normal(0.0, 0.020, 15),
            rng.normal(0.0, 0.002, 50),
        ]
    )
    rep_vol = disc.detect_skchange_regime_anomalies(vol_burst, min_segment_length=5)
    assert rep_vol["n_anomaly_segments"] >= 1
    assert any(45 <= seg["start"] <= 55 for seg in rep_vol["anomaly_segments"])

    # Recent regime break near end should set latest_regime_break=True
    recent = np.concatenate([rng.normal(0.0, 0.003, 70), rng.normal(-0.030, 0.015, 10)])
    rep_rec = disc.detect_skchange_regime_anomalies(recent, min_segment_length=5)
    assert rep_rec["latest_regime_break"] is True

    # Flat noise should not hallucinate structural breaks
    flat = rng.normal(0.0, 0.004, 80)
    rep_flat = disc.detect_skchange_regime_anomalies(flat, min_segment_length=5)
    assert rep_flat["n_changepoints"] == 0
    assert rep_flat["latest_regime_break"] is False


def test_compute_shapiq_interactions_extracts_synergistic_pair() -> None:
    import lightgbm as lgb

    rng = np.random.default_rng(7)
    x = rng.normal(size=(160, 4))
    # Inject strong synergistic interaction between feature 0 (rvol) and feature 2 (gap_range)
    y = 0.01 + 0.002 * x[:, 0] + 0.015 * (x[:, 0] * x[:, 2]) + rng.normal(0.0, 0.001, size=160)
    fnames = ["rvol", "vbr", "gap_range", "trf_darkpool"]

    model = lgb.LGBMRegressor(
        n_estimators=24,
        max_depth=3,
        num_leaves=7,
        learning_rate=0.08,
        min_child_samples=5,
        n_jobs=2,
        random_state=42,
        verbosity=-1,
    )
    model.fit(x[:120], y[:120])

    rep = disc.compute_shapiq_interactions(
        model,
        x[:120],
        x[120:136],
        fnames,
        max_order=2,
    )
    assert rep["index"] == "k-SII"
    assert rep["max_order"] == 2
    assert rep["explainer_backend"] == "TreeExplainer"
    assert rep["n_eval_rows"] == 16
    assert rep["top_interaction"] is not None
    assert set(rep["top_interaction"]["features"]) == {"rvol", "gap_range"}
    assert rep["top_interaction"]["mean_abs_interaction"] > 0.0
    assert rep["shapiq_version"] != "unknown"


def test_fit_conformal_excursion_intervals_coverage_and_hard_stop_cap() -> None:
    rng = np.random.default_rng(123)
    n = 180
    x = rng.normal(size=(n, 4))
    # True noise excursion centered around 1.2%..3.0% so upper bounds exceed 2.00% and MUST clamp
    y = np.clip(0.012 + 0.006 * np.abs(x[:, 0]) + rng.normal(0.0, 0.0025, size=n), 0.002, 0.04)

    rep = res.fit_conformal_excursion_intervals(
        x[:90],
        y[:90],
        x[90:135],
        y[90:135],
        x[135:],
        y_eval=y[135:],
        alpha=0.10,
        max_hard_cap_pct=0.02,
    )
    assert rep["method"] == "SplitConformalRegressor"
    assert rep["target_coverage"] == 0.9
    assert rep["risk_cap_enforced"] is True
    assert rep["n_eval"] == 45
    assert len(rep["intervals"]) == 45
    assert all(lo <= hi for lo, hi in rep["intervals"])
    assert rep["mean_interval_width"] > 0.0
    # Finite-sample conformal coverage should be well calibrated around 90%
    assert rep["empirical_coverage"] >= 0.75
    # Hard guardrail: conformal trailing NEVER exceeds 2.00% (risk.py invariant)
    assert rep["max_conformal_trailing_pct"] <= 0.02
    assert all(0.0 < t <= 0.02 for t in rep["conformal_trailing_pct"])
    assert rep["mapie_version"] != "unknown"

    # Must refuse any attempt to loosen max_hard_cap_pct beyond 2.00%
    with pytest.raises(ValueError, match="2.00%"):
        res.fit_conformal_excursion_intervals(
            x[:90],
            y[:90],
            x[90:135],
            y[90:135],
            x[135:],
            alpha=0.10,
            max_hard_cap_pct=0.03,
        )


def test_starred_frontier_manifest_contract_if_present() -> None:
    manifest_path = Path("/mnt/z/sota-data/manifests/starred_frontier_landscape_manifest.json")
    if not manifest_path.is_file():
        pytest.skip("Pre-wall starred frontier manifest not generated in this environment")
    doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert doc["schema_version"] == "tier0.starred-frontier-landscape.v1"
    assert doc["paper_only"] is True
    assert doc["live_execution_enabled"] is False
    assert doc["prewall_cutoff"] == "2026-10-15"
    assert doc["summary"]["engines_total"] == 5
    assert doc["summary"]["all_engines_pass"] is True
    assert doc["engines"]["mapie_conformal_excursion"]["max_conformal_trailing_pct"] <= 0.02

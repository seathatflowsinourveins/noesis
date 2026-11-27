"""Fast offline tests for ml4t-diagnostic & SHAP/shapiq pre-wall tearsheets and Grafana JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import polars as pl
import pytest
from scripts import generate_ml4t_and_shap_tearsheets as mod

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _make_synthetic_prewall_df(n_dates: int = 80) -> pl.DataFrame:
    """Create deterministic strictly pre-wall (<= 2026-10-15) multi-symbol test frame."""
    rng = np.random.default_rng(42)
    base_dates = [f"2025-{(i // 20) + 1:02d}-{(i % 20) + 1:02d}" for i in range(n_dates)]
    symbols = ["SPY", "QQQ", "SOXX", "NVDA", "AMD", "TSLA", "ROKU", "SOXS"]
    rows: list[dict[str, Any]] = []
    for d_idx, d_str in enumerate(base_dates):
        macro_ret = float(rng.normal(0.001, 0.012))
        for sym in symbols:
            rvol = float(np.clip(rng.lognormal(0.25, 0.45), 0.3, 6.0))
            ret1 = float(macro_ret + rng.normal(0.0, 0.025))
            coil = float(np.clip(rng.uniform(0.6, 1.5), 0.5, 1.6))
            vdu = float(np.clip(rng.uniform(0.5, 1.6), 0.4, 1.8))
            bar_rng = float(np.clip(abs(ret1) + rng.uniform(0.01, 0.04), 0.005, 0.15))
            m20_rng = float(np.clip(rng.uniform(0.015, 0.05), 0.01, 0.10))
            m5_rng = float(np.clip(m20_rng * coil, 0.005, 0.15))
            nxt_oc = float(
                np.clip(
                    0.15 * ret1
                    + 0.003 * (rvol - 1.5)
                    - 0.004 * (coil - 1.0)
                    + rng.normal(0, 0.015),
                    -0.15,
                    0.15,
                )
            )
            nxt_mfe = float(
                np.clip(
                    0.015 + 0.008 * rvol + 0.25 * bar_rng + rng.uniform(0.0, 0.02),
                    0.002,
                    0.25,
                )
            )
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": d_str,
                    "rvol_20d": rvol,
                    "vol_z_20d": float((rvol - 1.0) * 1.5),
                    "vdu_5d_20d_ratio": vdu,
                    "coil_5d_20d_range_ratio": coil,
                    "prev_close_to_20d_high": float(rng.uniform(-0.15, 0.05)),
                    "ret_1d": ret1,
                    "prior_ret_5d": float(rng.normal(0.005 * (d_idx % 3 - 1), 0.04)),
                    "prior_ret_20d": float(rng.normal(0.01, 0.08)),
                    "bar_range_pct": bar_rng,
                    "mean_20d_range_pct": m20_rng,
                    "mean_5d_range_pct_prior": m5_rng,
                    "next1_split_open": 100.0,
                    "next1_split_close": 100.0 * (1.0 + nxt_oc),
                    "next1_split_high": 100.0 * (1.0 + nxt_mfe),
                    "next1_split_low": 98.0,
                    "split_close": 100.0,
                    "next1_open_to_close_ret": nxt_oc,
                    "next1_mfe_pct": nxt_mfe,
                }
            )
    return pl.DataFrame(rows)


def test_prewall_pit_cutoff_rejects_postwall_dates(tmp_path: Path) -> None:
    df = _make_synthetic_prewall_df(20)
    pq_path = tmp_path / "df10.parquet"
    df.write_parquet(pq_path)
    with pytest.raises(ValueError, match="post-wall|wall|2026-10-15"):
        mod.load_prewall_causal_dataset(pq_path, cutoff_date="2026-10-20")


def test_cpcv_artifacts_and_unmodified_upstream_tearsheets(tmp_path: Path) -> None:
    df = _make_synthetic_prewall_df(80)
    pq_path = tmp_path / "df10.parquet"
    df.write_parquet(pq_path)

    h109_mock = {
        "trial_accounting": {"cumulative_trials_total": 211, "hlz_t_hurdle": 4.3636},
        "verdict": {"overall_verdict": "DISCARD", "cross_leg_promoted": False},
        "l1_queue_markout": {
            "LONG": {"passive_filled": 35, "mean_markout_bps": -3.995},
            "SHORT": {"passive_filled": 35, "mean_markout_bps": 16.528},
            "COMBINED": {"passive_filled": 70, "mean_markout_bps": 6.267},
        },
        "holdout_six_gate": {
            "LONG": {
                "verdict": "DISCARD",
                "gates_passed_count": 1,
                "empirical_pbo": 1.0,
                "l1_markout": {"passive_filled": 35, "mean_markout_bps": -3.995},
                "falsification_1x_4bps": {
                    "n_trades": 31,
                    "mean_bps": -21.822,
                    "dsr": 0.0,
                    "hac_t": -0.922,
                    "spa_p": 0.7967,
                },
                "falsification_calibrated_liquid": {"mean_bps": -31.616},
                "falsification_2x_8bps": {"mean_bps": -32.521},
            },
            "SHORT": {
                "verdict": "DISCARD",
                "gates_passed_count": 1,
                "empirical_pbo": 0.1667,
                "l1_markout": {"passive_filled": 35, "mean_markout_bps": 16.528},
                "falsification_1x_4bps": {
                    "n_trades": 28,
                    "mean_bps": -3.142,
                    "dsr": 0.0,
                    "hac_t": -0.1827,
                    "spa_p": 0.6233,
                },
                "falsification_calibrated_liquid": {"mean_bps": -13.646},
                "falsification_2x_8bps": {"mean_bps": -14.154},
            },
            "COMBINED": {
                "verdict": "DISCARD",
                "gates_passed_count": 1,
                "empirical_pbo": 0.6667,
                "l1_markout": {"passive_filled": 70, "mean_markout_bps": 6.267},
                "falsification_1x_4bps": {
                    "n_trades": 59,
                    "mean_bps": -12.957,
                    "dsr": 0.0,
                    "hac_t": -0.8811,
                    "spa_p": 0.8167,
                },
                "falsification_calibrated_liquid": {"mean_bps": -23.088},
                "falsification_2x_8bps": {"mean_bps": -23.805},
            },
        },
    }
    h109_path = tmp_path / "h109.json"
    h109_path.write_text(json.dumps(h109_mock), encoding="utf-8")

    cpcv_dir = tmp_path / "cpcv"
    ts_dir = tmp_path / "tearsheets"
    out_manifest = tmp_path / "ml4t_shap_manifest.json"
    sim_db = tmp_path / "sim_prewall_audit.duckdb"

    manifest = mod.run_pipeline(
        df10_path=pq_path,
        h109_dossier_path=h109_path,
        ablation_dossier_path=tmp_path / "missing_ablation.json",
        cpcv_dir=cpcv_dir,
        tearsheet_dir=ts_dir,
        manifest_out_path=out_manifest,
        sim_duckdb_path=sim_db,
        mlflow_uri=None,
    )

    assert manifest["paper_only"] is True
    assert manifest["live_execution_enabled"] is False
    assert manifest["risk_kernel_sha256"] == mod.EXPECTED_RISK_SHA256
    assert manifest["cpcv_artifacts"]["skfolio_cpcv"]["n_test_paths"] == 21
    assert manifest["cpcv_artifacts"]["skfolio_cpcv"]["n_splits"] == 56
    assert (cpcv_dir / "h109_cpcv_fold_distributions.parquet").is_file()
    assert (cpcv_dir / "h109_cpcv_oos_paths.parquet").is_file()
    assert (cpcv_dir / "h109_cpcv_summary.json").is_file()

    ml4t_html = ts_dir / "ml4t_diagnostic_h109_falsification_tearsheet.html"
    shap_html = ts_dir / "shap_shapiq_prewall_attribution_tearsheet.html"
    assert ml4t_html.is_file() and ml4t_html.stat().st_size > 10_000
    assert shap_html.is_file() and shap_html.stat().st_size > 10_000
    assert "plotly" in ml4t_html.read_text(encoding="utf-8").lower()
    assert "plotly" in shap_html.read_text(encoding="utf-8").lower()

    shap_meta = manifest["tearsheets"]["shap_shapiq_prewall_attribution"]
    assert shap_meta["lgbm_additivity_max_error"] < 1e-4
    assert shap_meta["catboost_additivity_max_error"] < 1e-4
    assert len(shap_meta["shap_top_features"]) == len(mod.CAUSAL_FEATURE_COLUMNS)
    assert len(shap_meta["shapiq_top5_interactions"]) >= 1

    # Verify standalone DuckDB audit snapshot tables
    with duckdb.connect(str(sim_db), read_only=True) as con:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert {
        "h109_six_gate_verdicts",
        "h109_cpcv_folds",
        "h109_cpcv_oos_paths",
        "prewall_shap_importance",
        "prewall_shapiq_interactions",
    }.issubset(tables)


def test_script_has_zero_handwritten_html_tags() -> None:
    src = (SCRIPTS_DIR / "generate_ml4t_and_shap_tearsheets.py").read_text(encoding="utf-8")
    for forbidden in ("<!DOCTYPE", "<html", "<body", "<script", "<div"):
        assert forbidden.lower() not in src.lower()


def test_grafana_sim_backtest_dashboard_declarative_json() -> None:
    dash_path = (
        REPO_ROOT / "docker" / "grafana" / "dashboards" / "tier0-sim-backtest-control-center.json"
    )
    raw = dash_path.read_text(encoding="utf-8")
    for forbidden in ("<html", "<script", "<div"):
        assert forbidden not in raw.lower()
    doc = json.loads(raw)
    assert doc["uid"] == "tier0-sim-backtest-control-center"
    assert doc["schemaVersion"] == 39
    assert len(doc["panels"]) >= 8
    for panel in doc["panels"]:
        assert panel["datasource"]["uid"] == "tier0-prometheus"
        assert panel["targets"]

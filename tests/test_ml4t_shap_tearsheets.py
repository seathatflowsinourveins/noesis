"""Offline tests: pre-wall reports come from the upstreams' own functions; Grafana JSON."""

from __future__ import annotations

import ast
import inspect
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
        "tuning": {"selected_arm_id": "H109_A12"},
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

    summary = manifest["cpcv_artifacts"]
    assert summary["ml4t_diagnostic_stats"]["n_trials_total"] == 211  # read from the dossier
    assert summary["ml4t_diagnostic_stats"]["dsr_probability"] is not None
    assert summary["oos_paths_arm_selection"] == "best_in_sample_sharpe_per_split"
    assert summary["selected_arm_id"] == "H109_A12"  # read from the dossier's tuning block
    assert manifest["reports_not_produced"] == {}

    # One upstream call per report file; the manifest records which function wrote it.
    expected_generators = {
        "ml4t_backtest_tearsheet": (
            "ml4t.diagnostic.visualization.backtest.generate_backtest_tearsheet"
        ),
        "skfolio_cpcv_train_test_folds": (
            "skfolio.model_selection.CombinatorialPurgedCV.plot_train_test_folds"
        ),
        "skfolio_cpcv_path_sharpe_distribution": "skfolio.population.Population.plot_distribution",
        "ml4t_feature_importance_lightgbm": (
            "ml4t.diagnostic.visualization.FeatureImportanceDashboard"
        ),
        "ml4t_feature_importance_catboost": (
            "ml4t.diagnostic.visualization.FeatureImportanceDashboard"
        ),
    }
    reports = manifest["reports"]
    assert {name: r["generator"] for name, r in reports.items()} == expected_generators
    for report in reports.values():
        path = Path(report["path"])
        assert path.parent == ts_dir and path.is_file() and path.stat().st_size > 10_000
        # Interactive upstream pages only (no static images).
        assert path.suffix == ".html"
        assert "plotly" in path.read_text(encoding="utf-8").lower()
    tearsheet_html = Path(reports["ml4t_backtest_tearsheet"]["path"]).read_text(encoding="utf-8")
    assert "executive_summary" in tearsheet_html
    assert 'data-section="validity_card"' in tearsheet_html  # trial count from the dossier
    lgbm_path = Path(reports["ml4t_feature_importance_lightgbm"]["path"])
    lgbm_page = lgbm_path.read_text(encoding="utf-8")
    assert "Interactions" in lgbm_page  # SHAP interaction results passed to the dashboard
    # The superseded locally composed pages are no longer written.
    assert not (ts_dir / "ml4t_diagnostic_h109_falsification_tearsheet.html").exists()
    assert not (ts_dir / "shap_shapiq_prewall_attribution_tearsheet.html").exists()

    shap_meta = manifest["attribution"]
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


def test_missing_dossier_values_stay_missing(tmp_path: Path) -> None:
    """No assumed trial count, verdict or zero: what the dossier lacks is None / NULL."""
    df = _make_synthetic_prewall_df(80)
    dates, mat, _spy, arm_ids = mod.build_multiarm_daily_returns(df)
    cpcv = mod.run_skfolio_cpcv_analysis(dates, mat, arm_ids, cpcv_dir=tmp_path / "cpcv")
    stats = cpcv["summary"]["ml4t_diagnostic_stats"]
    assert stats["n_trials_total"] is None
    assert stats["dsr_probability"] is None and stats["expected_max_sharpe_daily"] is None
    # No selected arm in the dossier: no arm is assumed and its tearsheet is not produced.
    assert cpcv["summary"]["selected_arm_id"] is None and cpcv["selected_returns"] is None
    assert stats["observed_sharpe_ann"] is None and stats["min_trl_sessions"] is None
    reports = mod.generate_h109_upstream_reports(
        dates=dates, benchmark_returns=_spy, cpcv_result=cpcv, output_dir=tmp_path / "reports"
    )
    assert set(reports) == {
        "skfolio_cpcv_train_test_folds",
        "skfolio_cpcv_path_sharpe_distribution",
    }

    # Every recombined OOS path covers each session once (skfolio's get_path_ids assignment).
    assert all(np.asarray(rets).size == len(dates) for rets in cpcv["path_returns"])

    # skfolio's own Sharpe of each recombined path equals the run's path table.
    from skfolio import Portfolio

    skfolio_sharpes = [
        Portfolio(
            X=np.asarray(rets).reshape(-1, 1), weights=np.ones(1), annualization_factor=252.0
        ).annualized_sharpe_ratio
        for rets in cpcv["path_returns"]
    ]
    assert np.allclose(skfolio_sharpes, cpcv["paths_df"]["sharpe_ann"].to_list(), atol=1e-3)

    db = tmp_path / "audit.duckdb"
    mod.build_sim_prewall_audit_duckdb(
        db_path=db,
        cpcv_result=cpcv,
        h109_dossier={},
        shap_result={"shap_top_features": [], "shapiq_top5_interactions": []},
    )
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute(
            "SELECT verdict, gates_passed, n_trades, mean_1x_bps FROM h109_six_gate_verdicts"
        ).fetchall()
    assert rows == [(None, None, None, None)] * 3


@pytest.mark.parametrize("n_sessions", [55, 103])
def test_oos_paths_follow_skfolio_path_ids(tmp_path: Path, n_sessions: int) -> None:
    """Uneven fold sizes: each recombined path still covers every session exactly once."""
    df = _make_synthetic_prewall_df(n_sessions)
    dates, mat, _spy, arm_ids = mod.build_multiarm_daily_returns(df)
    cpcv = mod.run_skfolio_cpcv_analysis(dates, mat, arm_ids, cpcv_dir=tmp_path / "cpcv")
    assert len(cpcv["path_returns"]) == cpcv["summary"]["skfolio_cpcv"]["n_test_paths"]
    assert all(np.asarray(rets).size == len(dates) for rets in cpcv["path_returns"])


def test_backtest_tearsheet_without_trial_count_or_full_benchmark(tmp_path: Path) -> None:
    """No dossier trial count: statistical panels off. A session without SPY: no benchmark."""
    df = _make_synthetic_prewall_df(80)
    df = df.filter(~((pl.col("symbol") == "SPY") & (pl.col("trade_date") == "2025-01-05")))
    dates, mat, spy, arm_ids = mod.build_multiarm_daily_returns(df)
    assert int(np.isnan(spy).sum()) == 1  # the session without SPY stays missing, not 0.0
    cpcv = mod.run_skfolio_cpcv_analysis(
        dates, mat, arm_ids, selected_arm_id="H109_A12", cpcv_dir=tmp_path / "cpcv"
    )
    reports = mod.generate_h109_upstream_reports(
        dates=dates, benchmark_returns=spy, cpcv_result=cpcv, output_dir=tmp_path / "reports"
    )
    html = Path(reports["ml4t_backtest_tearsheet"]["path"]).read_text(encoding="utf-8")
    assert 'data-section="executive_summary"' in html
    assert 'data-section="validity_card"' not in html
    assert 'data-section="sharpe_bootstrap"' not in html
    assert "SPY (next-session open to close)" not in html


def test_reports_have_no_local_figure_or_page_code() -> None:
    """The generator passes data to upstream report functions; it builds no figure or page."""
    src = (SCRIPTS_DIR / "generate_ml4t_and_shap_tearsheets.py").read_text(encoding="utf-8")
    for forbidden in (
        "go.Figure",
        "go.Bar",
        "go.Scatter",
        "go.Indicator",
        "update_layout",
        "add_trace",
        "marker_color",
        "make_subplots",
        "combine_figures_to_html",
        "visualization.core",
        "to_html(",
        "import plotly",
        "from plotly",
        "import matplotlib",
        "from matplotlib",
        "sections",
    ):
        assert forbidden not in src, forbidden
    # Result numbers from the old pages may not be typed in.
    for typed_in in ("211", "4.3636", "6.267", "70/72", "7.0538", "-3.995", "16.528", "DISCARD"):
        assert typed_in not in src, typed_in


def test_script_has_zero_handwritten_html_tags() -> None:
    src = (SCRIPTS_DIR / "generate_ml4t_and_shap_tearsheets.py").read_text(encoding="utf-8")
    for forbidden in (
        "<!DOCTYPE",
        "<html",
        "<body",
        "<script",
        "<div",
        "go.Figure",
        "make_subplots",
        "combine_figures_to_html",
        "import plotly",
    ):
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
    # Only panels whose metrics are emitted today (TCA envelope, risk guardrails, scrape health).
    assert [panel["id"] for panel in doc["panels"]] == [7, 9, 10]
    exprs = [target["expr"] for panel in doc["panels"] for target in panel["targets"]]
    for never_emitted in (
        "tier0_prewall_trials_total",
        "tier0_sandbox_mlflow_trials_total",
        "tier0_falsification_passes_total",
        "tier0_sandbox_24x7_active",
        "tier0_sandbox_live_execution_enabled",
        "tier0_sandbox_broker_ports_open",
        "tier0_rule612_stop_clamps_total",
        "tier0_churn_blocks_total",
        "tier0_intraday_throttle_scale",
        "tier0_observability_up",
    ):
        assert not any(never_emitted in expr for expr in exprs)
    assert not any("vector(0)" in expr for expr in exprs)
    for panel in doc["panels"]:
        assert panel["datasource"]["uid"] == "tier0-prometheus"
        assert panel["targets"]


def test_mlflow_log_skips_missing_values_and_records_report_generators(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a dossier arm or trial count the run logs no invented param or metric."""
    import mlflow
    import mlflow.tracking._tracking_service.utils as tracking_utils
    import mlflow.tracking.fluent as fluent

    # log_suite_to_mlflow sets MLflow's process-wide tracking URI and experiment; restore both
    # (and the env vars MLflow exports for subprocesses) so later tests are unaffected.
    monkeypatch.setattr(tracking_utils, "_tracking_uri", tracking_utils._tracking_uri)
    monkeypatch.setattr(fluent, "_active_experiment_id", fluent._active_experiment_id)
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.delenv("MLFLOW_EXPERIMENT_ID", raising=False)

    df = _make_synthetic_prewall_df(80)
    dates, mat, spy, arm_ids = mod.build_multiarm_daily_returns(df)
    cpcv = mod.run_skfolio_cpcv_analysis(dates, mat, arm_ids, cpcv_dir=tmp_path / "cpcv")
    reports = mod.generate_h109_upstream_reports(
        dates=dates, benchmark_returns=spy, cpcv_result=cpcv, output_dir=tmp_path / "reports"
    )
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    logged = mod.log_suite_to_mlflow(
        tracking_uri=uri,
        experiment_name="test-upstream-reports",
        cpcv_summary=cpcv["summary"],
        attribution={
            "lgbm_additivity_max_error": 0.0,
            "catboost_additivity_max_error": 0.0,
            "shapiq_version": "test",
        },
        reports=reports,
        cpcv_summary_path=Path(cpcv["summary"]["artifacts"]["summary_json"]),
    )
    run = mlflow.MlflowClient(tracking_uri=uri).get_run(logged["run_id"])
    assert "selected_arm_id" not in run.data.params and "n_trials_total" not in run.data.params
    assert "cpcv_dsr_probability" not in run.data.metrics
    assert "cpcv_observed_sharpe_ann" not in run.data.metrics
    assert run.data.metrics["cpcv_pbo"] == cpcv["summary"]["ml4t_diagnostic_stats"]["pbo"]
    for name, report in reports.items():
        assert run.data.tags[f"report_{name}_generator"] == report["generator"]
        assert run.data.tags[f"report_{name}_sha256"] == report["sha256"]


def test_tearsheets_use_only_upstream_figure_functions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every figure on the report pages is built inside an upstream library, none here."""
    from plotly.basedatatypes import BaseFigure

    builders: list[str] = []
    upstream_init = BaseFigure.__init__

    def recording_init(self: BaseFigure, *args: Any, **kwargs: Any) -> None:
        # The first caller outside plotly is the code that asked for this figure.
        frame = inspect.currentframe()
        caller = frame.f_back if frame is not None else None
        while caller is not None and caller.f_globals.get("__name__", "").startswith(
            ("plotly.", "_plotly_utils.")
        ):
            caller = caller.f_back
        builders.append(caller.f_globals.get("__name__", "") if caller is not None else "")
        upstream_init(self, *args, **kwargs)

    monkeypatch.setattr(BaseFigure, "__init__", recording_init)
    df = _make_synthetic_prewall_df(80)
    dates, mat, spy, arm_ids = mod.build_multiarm_daily_returns(df)
    cpcv = mod.run_skfolio_cpcv_analysis(
        dates, mat, arm_ids, selected_arm_id="H109_A12", cpcv_dir=tmp_path / "cpcv"
    )
    reports = mod.generate_h109_upstream_reports(
        dates=dates, benchmark_returns=spy, cpcv_result=cpcv, output_dir=tmp_path / "reports"
    )
    attribution = mod.generate_prewall_attribution_reports(df, output_dir=tmp_path / "reports")
    assert set(reports) == {
        "ml4t_backtest_tearsheet",
        "skfolio_cpcv_train_test_folds",
        "skfolio_cpcv_path_sharpe_distribution",
    }
    assert set(attribution["reports"]) == {
        "ml4t_feature_importance_lightgbm",
        "ml4t_feature_importance_catboost",
    }
    assert builders, "the report functions built no plotly figure"
    assert mod.__name__ not in builders
    assert {builder.split(".")[0] for builder in builders} <= {"ml4t", "skfolio"}, builders


def test_sim_backtest_suite_calls_upstream_create_tearsheet() -> None:
    """The engine report pages are nautilus_trader's own functions, called on the engine."""
    from nautilus_trader.analysis import tearsheet as upstream
    from scripts import run_sim_backtest_tearsheet_suite as suite

    tree = ast.parse(
        (SCRIPTS_DIR / "run_sim_backtest_tearsheet_suite.py").read_text(encoding="utf-8")
    )
    backtest = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "run_empirical_backtest_from_catalog"
    )
    engines = [
        node
        for node in ast.walk(backtest)
        if isinstance(node, ast.Assign)
        and [ast.unparse(target) for target in node.targets] == ["engine"]
    ]
    assert len(engines) == 1 and isinstance(engines[0].value, ast.Call)
    assert ast.unparse(engines[0].value.func) == "BacktestEngine"
    in_backtest = set(map(id, ast.walk(backtest)))
    for name in ("create_tearsheet", "create_bars_with_fills"):
        # The name the suite calls is the upstream function, not a local stand-in.
        assert getattr(suite, name) is getattr(upstream, name), name
        assert getattr(upstream, name).__module__ == "nautilus_trader.analysis.tearsheet"
        stand_ins = [
            node
            for node in ast.walk(tree)
            if (
                isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
                and node.name == name
            )
            or (isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Store))
        ]
        assert stand_ins == [], name
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
        ]
        # One call, inside the backtest, on the BacktestEngine that ran it, writing a file.
        assert len(calls) == 1 and id(calls[0]) in in_backtest, name
        assert [ast.unparse(arg) for arg in calls[0].args] == ["engine"], name
        assert "output_path" in {keyword.arg for keyword in calls[0].keywords}, name
    # Each receipt names the upstream function that wrote its file.
    generators = {
        value.value
        for node in ast.walk(backtest)
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values, strict=True)
        if isinstance(key, ast.Constant)
        and key.value == "generator"
        and isinstance(value, ast.Constant)
    }
    assert generators == {
        "nautilus_trader.analysis.tearsheet.create_tearsheet",
        "nautilus_trader.analysis.tearsheet.create_bars_with_fills",
    }

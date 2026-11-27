from __future__ import annotations

import json
import pathlib

import pytest
from dagster import DefaultScheduleStatus

from tier0_sota.definitions import (
    daily_compounding_reflection_schedule,
    defs,
    paper_farm_performance_snapshot,
    risk_kernel_sha256_attestation,
)


def test_dagster_definitions_schedule_and_risk_attestation() -> None:
    resolved_defs = defs
    assert resolved_defs.resolve_all_job_defs()
    assert daily_compounding_reflection_schedule.default_status == DefaultScheduleStatus.RUNNING
    assert daily_compounding_reflection_schedule.cron_schedule == "0 18 * * 1-5"

    risk_rep = risk_kernel_sha256_attestation()
    assert isinstance(risk_rep, dict)
    assert risk_rep["sha256_match"] is True


def test_paper_farm_performance_snapshot_nested_and_legacy_schemas(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path))

    missing_rep = paper_farm_performance_snapshot()
    assert missing_rep == {
        "present": False,
        "paper_only": True,
        "live_execution_enabled": False,
    }

    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    perf_path = manifests_dir / "paper_farm_live_performance_manifest.json"

    perf_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.paper-farm-live-performance.v1",
                "paper_only": True,
                "live_execution_enabled": False,
                "summary": {"combined_equity_usd": 400000.0},
                "per_lane": {
                    "ALPACA_PA1": {},
                    "ALPACA_PA2": {},
                    "ALPACA_PA3": {},
                    "IBKR_DU_4002": {},
                },
            }
        ),
        encoding="utf-8",
    )
    nested_rep = paper_farm_performance_snapshot()
    assert isinstance(nested_rep, dict)
    assert nested_rep["present"] is True
    assert nested_rep["paper_only"] is True
    assert nested_rep["live_execution_enabled"] is False
    assert nested_rep["combined_equity_usd"] == 400000.0
    assert nested_rep["accounts_count"] == 4

    perf_path.write_text(
        json.dumps(
            {
                "paper_only": True,
                "live_execution_enabled": False,
                "combined_equity_usd": 250000.0,
                "accounts": {"PA1": {}, "PA2": {}},
            }
        ),
        encoding="utf-8",
    )
    legacy_rep = paper_farm_performance_snapshot()
    assert isinstance(legacy_rep, dict)
    assert legacy_rep["present"] is True
    assert legacy_rep["paper_only"] is True
    assert legacy_rep["live_execution_enabled"] is False
    assert legacy_rep["combined_equity_usd"] == 250000.0
    assert legacy_rep["accounts_count"] == 2


def test_active_dagster_compounding_assets_and_frontier_telemetry(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import duckdb
    from dagster import AssetsDefinition

    from tier0_sota.definitions import (
        adr_governance_and_graph_sync,
        compounding_self_reflection_snapshot,
        frontier_trigger_telemetry,
        sim_vs_paper_parity_reconciliation,
    )

    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TIER0_DAGSTER_ACTIVE_REFLECT", "1")
    monkeypatch.setenv("TIER0_DAGSTER_OFFLINE", "1")

    asset_names = sorted(
        a.key.to_user_string() for a in (defs.assets or []) if isinstance(a, AssetsDefinition)
    )
    assert len(asset_names) == 8
    assert {
        "adr_governance_and_graph_sync",
        "sim_vs_paper_parity_reconciliation",
        "frontier_trigger_telemetry",
        "compounding_self_reflection_snapshot",
    }.issubset(set(asset_names))

    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    db_path = manifests_dir / "paper_lane_pa1.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE TABLE paper_tca_fills (implementation_shortfall_bps DOUBLE)")
        con.execute("INSERT INTO paper_tca_fills VALUES (3.2), (4.8)")
    finally:
        con.close()

    adr_rep = adr_governance_and_graph_sync()
    assert isinstance(adr_rep, dict)
    assert int(adr_rep["total_adrs"]) >= 24
    assert adr_rep["catalog_and_zensical_in_sync"] is True
    assert adr_rep["clean_sota_harness_ok"] is True

    tca_rep = sim_vs_paper_parity_reconciliation()
    assert isinstance(tca_rep, dict)
    assert tca_rep["duckdb_tca_rows"] == 2
    assert tca_rep["realized_mean_is_bps"] == 4.0
    assert (manifests_dir / "tca_calibration_manifest.json").is_file()

    frontier_rep = frontier_trigger_telemetry()
    assert isinstance(frontier_rep, dict)
    assert int(frontier_rep["adr_count"]) >= 24
    assert int(frontier_rep["spdx_firewall_count"]) >= 7
    assert frontier_rep["unwired_lock_packages"] == []
    assert (manifests_dir / "frontier_trigger_status.json").is_file()

    refl_rep = compounding_self_reflection_snapshot()
    assert isinstance(refl_rep, dict)
    assert refl_rep["present"] is True
    assert refl_rep["active_materialized"] is True
    assert refl_rep["risk_sha256_match"] is True
    assert (manifests_dir / "compounding_self_reflection_manifest.json").is_file()


def test_sim_vs_paper_parity_perold_order_and_notional_weighting(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Partial-fill multiplicity must not falsely trip <= 8 bps; true slippage still blocks."""
    import duckdb

    from tier0_sota.definitions import sim_vs_paper_parity_reconciliation
    from tier0_sota.live.execution import cost_calibration_allows_entry

    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path))
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    db_path = manifests_dir / "paper_lane_pa1.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "CREATE TABLE paper_tca_fills ("
            "account VARCHAR, broker_order_id VARCHAR, filled_qty DOUBLE, "
            "fill_price DOUBLE, implementation_shortfall_bps DOUBLE)"
        )
        # 10 clean 100-sh orders @ 3 bps + 1 order split into 10x 1-sh fills @ 25 bps:
        # Raw fill mean = 14 bps (> 8), but Perold order mean = 5 bps and notional ~ 3.2 bps.
        rows = [("ALPACA_PA1", f"clean-{i}", 100.0, 50.0, 3.0) for i in range(10)]
        rows += [("ALPACA_PA1", "split-1", 1.0, 50.0, 25.0) for _ in range(10)]
        con.executemany("INSERT INTO paper_tca_fills VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        con.close()

    rep = sim_vs_paper_parity_reconciliation()
    assert isinstance(rep, dict)
    assert rep["tca_aggregation_method"] == "perold_order_and_notional_weighted"
    assert rep["duckdb_tca_rows"] == 20
    assert rep["liquid_ge10_orders_count"] == 11
    raw_mean = rep["liquid_ge10_mean_is_bps"]
    ord_mean = rep["liquid_ge10_order_mean_is_bps"]
    nw_mean = rep["liquid_ge10_notional_weighted_is_bps"]
    assert isinstance(raw_mean, float) and raw_mean > 8.0
    assert isinstance(ord_mean, float) and ord_mean <= 8.0
    assert isinstance(nw_mean, float) and nw_mean <= 8.0
    assert rep["liquid_ge10_within_2x_envelope"] is True
    assert cost_calibration_allows_entry(manifests_dir) is True

    # Genuine adverse order-level slippage (> 8 bps across orders) must still fail closed.
    con = duckdb.connect(str(db_path))
    try:
        con.execute("DELETE FROM paper_tca_fills")
        bad_rows = [("ALPACA_PA1", f"bad-{i}", 100.0, 50.0, 12.5) for i in range(5)]
        con.executemany("INSERT INTO paper_tca_fills VALUES (?, ?, ?, ?, ?)", bad_rows)
    finally:
        con.close()

    bad_rep = sim_vs_paper_parity_reconciliation()
    assert isinstance(bad_rep, dict)
    assert bad_rep["liquid_ge10_order_mean_is_bps"] == 12.5
    assert bad_rep["liquid_ge10_within_2x_envelope"] is False
    assert cost_calibration_allows_entry(manifests_dir) is False

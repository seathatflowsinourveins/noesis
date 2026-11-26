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
    assert (manifests_dir / "frontier_trigger_status.json").is_file()

    refl_rep = compounding_self_reflection_snapshot()
    assert isinstance(refl_rep, dict)
    assert refl_rep["present"] is True
    assert refl_rep["active_materialized"] is True
    assert refl_rep["risk_sha256_match"] is True
    assert (manifests_dir / "compounding_self_reflection_manifest.json").is_file()

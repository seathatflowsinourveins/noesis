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

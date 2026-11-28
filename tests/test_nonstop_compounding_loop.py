"""Fast unit tests for `scripts/run_nonstop_compounding_loop.py`, systemd units & Dagster job."""

from __future__ import annotations

import ast
import configparser
import importlib.util
import json
import os
import socket
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_nonstop_compounding_loop.py"
SERVICE_PATH = REPO_ROOT / "docker" / "systemd" / "tier0-compounding-loop.service"
TIMER_PATH = REPO_ROOT / "docker" / "systemd" / "tier0-compounding-loop.timer"


def _load_loop_module() -> Any:
    spec = importlib.util.spec_from_file_location("run_nonstop_compounding_loop", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _seed_paper_manifests(
    manifest_dir: Path,
    *,
    live_execution_enabled: bool = False,
    paper_only: bool = True,
    include_historical_calibration: bool = True,
) -> None:
    now_iso = datetime.now(UTC).isoformat()
    pack_dir = manifest_dir / "tv_sota_pack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": now_iso,
                "ranked_count": 15,
                "entries": [
                    {"symbol": "NVDA", "mover_category": "PREMARKET_CATALYST_LEADER"},
                    {"symbol": "TQQQ", "mover_category": "EXTREME_MOVER_CONTINUATION"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "live_lane_order_preset_bundle.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now_iso,
                "paper_only": paper_only,
                "live_execution_enabled": live_execution_enabled,
                "venue_profile": "ALPACA_PAPER_1000",
                "alpaca_per_lane_max_rpm": 1000,
                "top_potential_symbols": [{"symbol": "NVDA"}, {"symbol": "TQQQ"}],
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now_iso,
                "status": "SUPERVISING",
                "active_session": "RTH",
                "paper_only": paper_only,
                "live_execution_enabled": live_execution_enabled,
                "managed_symbols": ["TQQQ"],
                "active_positions_count": 1,
                "streams": ["sip", "boats"],
                "entry_fault": False,
                "venue_order_handles": {
                    "alpaca_venue_profile": "ALPACA_PAPER_1000",
                    "combined_max_rpm": 6000,
                    "combined_exit_reserve_rpm": 600,
                },
            }
        ),
        encoding="utf-8",
    )
    (pack_dir / "tv_engine_coordination_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-engine-coordination.v1",
                "generated_at_utc": now_iso,
                "active_session": "RTH",
                "paper_only": paper_only,
                "semi_auto_confirmed": False,
                "live_execution_enabled": live_execution_enabled,
                "auto_submit_enabled": False,
                "tca_calibration_ok": True,
                "staged_alerts_evaluated": 2,
                "ready_for_operator_confirm_count": 2,
                "venue_order_handles": {
                    "alpaca_venue_profile": "ALPACA_PAPER_1000",
                    "combined_max_rpm": 6000,
                    "combined_exit_reserve_rpm": 600,
                },
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "paper_farm_live_performance_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now_iso,
                "paper_only": paper_only,
                "live_execution_enabled": live_execution_enabled,
                "summary": {"tca_fills_count": 149},
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "tca_calibration_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now_iso,
                "paper_only": paper_only,
                "live_execution_enabled": live_execution_enabled,
                "duckdb_tca_rows": 149,
                "liquid_ge10_within_2x_envelope": True,
                "realized_mean_is_bps": 9.34,
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "compounding_self_reflection_manifest.json").write_text(
        json.dumps(
            {
                "optuna": {"trials_total": 21},
                "mlflow": {"logged": True, "run_id": "abc123"},
                "xerj": {"pushed": True, "id": "reflection-test"},
            }
        ),
        encoding="utf-8",
    )
    if include_historical_calibration:
        (manifest_dir / "historical_extreme_mover_calibration_manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "tier0.historical-extreme-mover-calibration.v1",
                    "generated_at_utc": now_iso,
                    "prewall_cutoff_iso": "2026-10-15",
                    "prewall_wall_date": "2026-10-15",
                    "paper_only": paper_only,
                    "live_execution_enabled": live_execution_enabled,
                    "categorized_mover_taxonomy": [
                        {"category": "PREMARKET_CATALYST_LEADER"},
                        {"category": "SUDDEN_VOLUME_IGNITION"},
                        {"category": "EXTREME_MOVER_CONTINUATION"},
                        {"category": "PARABOLIC_CLIMAX_SHORT_REVERSAL"},
                        {"category": "DARKPOOL_ACCUMULATION_BREAKOUT"},
                    ],
                    "per_symbol_min_trailing_pct": {"NVDA": 0.00175, "TQQQ": 0.0021},
                    "nvda_monday_premarket_e2e_replay": {"symbol": "NVDA"},
                    "optuna": {"n_trials": 12},
                    "mlflow": {"run_id": "cal-run-01"},
                }
            ),
            encoding="utf-8",
        )


def test_zero_self_invention_ast_no_while_sleep_or_active_connect() -> None:
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        assert not isinstance(node, ast.While), "No ad-hoc Python while polling loops permitted"
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"sleep", "connect", "connect_ex"}, (
                f"Prohibited active probe/sleep attribute in compounding loop: {node.attr}"
            )
    mod = _load_loop_module()
    parser = mod.build_arg_parser()
    assert "--loop" not in parser._option_string_actions
    assert "--once" in parser._option_string_actions


def test_risk_kernel_sha256_gate_passes_and_refuses_tampering(tmp_path: Path) -> None:
    mod = _load_loop_module()
    digest = mod.verify_risk_kernel_sha256(REPO_ROOT / "src" / "tier0_sota" / "risk.py")
    assert digest == mod.EXPECTED_RISK_KERNEL_SHA256

    tampered = tmp_path / "risk.py"
    tampered.write_text("# tampered\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        mod.verify_risk_kernel_sha256(tampered)


def test_paper_only_env_refuses_live_keys_and_live_flag() -> None:
    mod = _load_loop_module()
    mod.verify_paper_only_env(
        {
            "LIVE_EXECUTION_ENABLED": "false",
            "ALPACA_PA1_API_KEY": "PK_PAPER",
            "APCA_API_BASE_URL": "https://paper-api.alpaca.markets",
        }
    )
    with pytest.raises(PermissionError, match="LIVE_EXECUTION_ENABLED"):
        mod.verify_paper_only_env({"LIVE_EXECUTION_ENABLED": "true"})
    with pytest.raises(PermissionError, match="live secret env"):
        mod.verify_paper_only_env({"ALPACA_LIVE_API_KEY": "PK_DUMMY"})
    with pytest.raises(PermissionError, match="AK\\*"):
        mod.verify_paper_only_env({"APCA_API_KEY_ID": "AK" + "_DUMMY"})
    with pytest.raises(PermissionError, match="non-paper"):
        mod.verify_paper_only_env({"APCA_API_BASE_URL": "https://live.example.invalid"})


def test_zero_live_ports_guard_passively_detects_listening_and_ignores_time_wait(
    tmp_path: Path,
) -> None:
    mod = _load_loop_module()
    # 1. Real bound loopback socket detected passively via /proc/net/tcp (zero connect_ex)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        bound_port = srv.getsockname()[1]
        with pytest.raises(PermissionError, match="forbidden live broker port"):
            mod.verify_zero_live_broker_ports((bound_port,))

    # 2. Deterministic /proc/net/tcp fixture: TIME_WAIT (06) passes; LISTEN (0A) & ESTAB (01) fail
    proc_fixture = tmp_path / "tcp"
    proc_fixture.write_text(
        "  sl  local_address rem_address   st tx_queue rx_queue\n"
        "   0: 0100007F:0FA1 0100007F:C350 06 00000000:00000000\n",
        encoding="utf-8",
    )
    assert mod.verify_zero_live_broker_ports((4001,), proc_net_paths=(proc_fixture,)) == []

    proc_fixture.write_text(
        "  sl  local_address rem_address   st tx_queue rx_queue\n"
        "   0: 0100007F:0FA1 00000000:0000 0A 00000000:00000000\n",
        encoding="utf-8",
    )
    with pytest.raises(PermissionError, match="4001"):
        mod.verify_zero_live_broker_ports((4001,), proc_net_paths=(proc_fixture,))

    proc_fixture.write_text(
        "  sl  local_address rem_address   st tx_queue rx_queue\n"
        "   0: 0100007F:C350 0100007F:1D48 01 00000000:00000000\n",
        encoding="utf-8",
    )
    with pytest.raises(PermissionError, match="7496"):
        mod.verify_zero_live_broker_ports((7496,), proc_net_paths=(proc_fixture,))


def test_lockfile_exclusivity_and_stale_pid_recovery(tmp_path: Path) -> None:
    mod = _load_loop_module()
    lock_path = tmp_path / "state" / "nonstop_compounding_loop.lock"

    with mod.CompoundingLoopLock(lock_path):
        assert lock_path.is_file()
        with pytest.raises(BlockingIOError, match="already held"):
            mod.CompoundingLoopLock(lock_path).acquire()
    assert not lock_path.exists()

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps({"pid": 999_999_999}), encoding="utf-8")
    with mod.CompoundingLoopLock(lock_path):
        data = json.loads(lock_path.read_text(encoding="utf-8"))
        assert data["pid"] == os.getpid()
    assert not lock_path.exists()


def test_dry_run_once_emits_atomic_heartbeat_and_increments_iteration(tmp_path: Path) -> None:
    mod = _load_loop_module()
    _seed_paper_manifests(tmp_path)

    rc1 = mod.main(
        [
            "--once",
            "--skip-reflection",
            "--dry-run",
            "--no-probe-services",
            "--manifest-dir",
            str(tmp_path),
        ]
    )
    assert rc1 == 0
    status_path = tmp_path / "nonstop_compounding_loop_status.json"
    assert status_path.is_file()
    doc1 = json.loads(status_path.read_text(encoding="utf-8"))
    assert doc1["schema_version"] == "tier0.nonstop-compounding-loop.v1"
    assert doc1["scheduler_authority"] == "systemd_timer_and_dagster"
    assert doc1["iteration"] == 1
    assert doc1["overall_verdict"] == "PASS"
    assert doc1["risk_kernel_sha256_ok"] is True
    assert doc1["paper_only"] is True
    assert doc1["live_execution_enabled"] is False
    assert doc1["venue_posture"]["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
    assert doc1["venue_posture"]["combined_paper_max_rpm"] == 6000
    assert doc1["venue_posture"]["combined_exit_reserve_rpm"] == 600
    assert doc1["venue_posture"]["alpaca_paper_1000_6000rpm_verified"] is True
    assert doc1["compounding_totals"]["optuna_trials_total"] == 33
    assert doc1["compounding_totals"]["tca_rows_calibrated"] == 149
    s1 = doc1["stages"]["stage_1_early_mover_pit_calibration"]
    assert s1["upstream_module"] == "tier0_sota.research"
    assert "--calibrate-extreme-movers" in s1["planned_command"]
    assert s1["historical_calibration"]["present"] is True
    assert s1["historical_calibration"]["categories_count"] == 5
    assert s1["historical_calibration"]["calibrated_symbols_count"] == 2
    assert s1["historical_calibration"]["nvda_premarket_present"] is True
    s3 = doc1["stages"]["stage_3_live_paper_tv_heartbeat"]
    assert s3["manifests"]["paper_farm_execution"]["status"] == "SUPERVISING"
    assert s3["manifests"]["live_lane_order_preset_bundle"]["preset_count"] == 2
    assert s3["manifests"]["live_whole_market_movers"]["category_counts"] == {
        "EXTREME_MOVER_CONTINUATION": 1,
        "PREMARKET_CATALYST_LEADER": 1,
    }

    rc2 = mod.main(
        [
            "--once",
            "--skip-reflection",
            "--dry-run",
            "--no-probe-services",
            "--manifest-dir",
            str(tmp_path),
        ]
    )
    assert rc2 == 0
    doc2 = json.loads(status_path.read_text(encoding="utf-8"))
    assert doc2["iteration"] == 2
    assert doc2["compounding_totals"]["cycles_completed"] == 2


def test_heartbeat_fails_closed_if_manifest_enables_live_execution(tmp_path: Path) -> None:
    mod = _load_loop_module()
    _seed_paper_manifests(tmp_path, live_execution_enabled=True, paper_only=False)

    rc = mod.main(
        [
            "--once",
            "--skip-reflection",
            "--dry-run",
            "--no-probe-services",
            "--manifest-dir",
            str(tmp_path),
        ]
    )
    assert rc == 1
    doc = json.loads(
        (tmp_path / "nonstop_compounding_loop_status.json").read_text(encoding="utf-8")
    )
    assert doc["overall_verdict"] == "FAIL"
    assert doc["stages"]["stage_1_early_mover_pit_calibration"]["status"] == "failed"
    assert doc["stages"]["stage_3_live_paper_tv_heartbeat"]["status"] == "failed"


def test_systemd_units_syntax_and_paper_only_guardrails() -> None:
    assert SERVICE_PATH.is_file() and TIMER_PATH.is_file()
    svc_bytes = SERVICE_PATH.read_bytes()
    tmr_bytes = TIMER_PATH.read_bytes()
    assert b"\r\n" not in svc_bytes and b"\r\n" not in tmr_bytes

    svc_text = svc_bytes.decode("utf-8")
    tmr_text = tmr_bytes.decode("utf-8")
    for forbidden in ("--profile live", "api.alpaca.markets", "live-age.txt", "9223", "%h/"):
        assert forbidden not in svc_text and forbidden not in tmr_text

    svc = configparser.ConfigParser(strict=False, interpolation=None)
    svc.optionxform = lambda optionstr: optionstr
    svc.read_string(svc_text)
    assert svc["Service"]["Type"] == "oneshot"
    assert svc["Service"]["User"] == "sota"
    assert svc["Service"]["WorkingDirectory"] == "/mnt/z/noesis"
    assert svc["Service"]["MemoryMax"] == "6G"
    assert svc["Service"]["Nice"] == "10"
    assert 'UV_PROJECT_ENVIRONMENT="$HOME/noesis/.venv"' in svc_text
    assert 'FNOX_AGE_KEY_FILE="$HOME/.config/fnox/age.txt"' in svc_text
    assert "TIER0_ALPACA_RPM=1000" in svc_text
    assert "fnox exec --profile paper -- uv run --no-sync python" in svc_text
    assert "scripts/run_nonstop_compounding_loop.py --once" in svc_text
    assert ":(4001|7496|8798|8796)" in svc_text

    tmr = configparser.ConfigParser(strict=False, interpolation=None)
    tmr.optionxform = lambda optionstr: optionstr
    tmr.read_string(tmr_text)
    assert tmr["Timer"]["OnBootSec"] == "5min"
    assert tmr["Timer"]["OnUnitActiveSec"] == "2h"
    assert tmr["Timer"]["Persistent"] == "true"
    assert tmr["Install"]["WantedBy"] == "timers.target"


def test_dagster_nonstop_compounding_loop_job_and_manifest_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tier0_sota.definitions import (
        NONSTOP_COMPOUNDING_TIMER_SPEC,
        compounding_self_reflection_snapshot,
        defs,
        nonstop_compounding_loop_job,
    )

    assert nonstop_compounding_loop_job.name == "nonstop_compounding_loop_job"
    job_names = {j.name for j in defs.resolve_all_job_defs()}
    assert {"compounding_reflection_job", "nonstop_compounding_loop_job"} <= job_names
    assert NONSTOP_COMPOUNDING_TIMER_SPEC["systemd_timer"] == "tier0-compounding-loop.timer"
    assert NONSTOP_COMPOUNDING_TIMER_SPEC["paper_only"] is True
    assert NONSTOP_COMPOUNDING_TIMER_SPEC["live_execution_enabled"] is False

    manifests_dir = tmp_path / "manifests"
    _seed_paper_manifests(manifests_dir)
    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("TIER0_DAGSTER_ACTIVE_REFLECT", "0")
    monkeypatch.setenv("TIER0_DAGSTER_OFFLINE", "1")
    monkeypatch.setenv("TIER0_DAGSTER_NONSTOP_LOOP", "1")

    rep = compounding_self_reflection_snapshot()
    assert isinstance(rep, dict)
    loop_summary = rep["nonstop_compounding_loop"]
    assert isinstance(loop_summary, dict)
    assert loop_summary["loop_present"] is True
    assert loop_summary["loop_iteration"] == 1
    assert loop_summary["loop_verdict"] == "PASS"
    assert loop_summary["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
    assert loop_summary["combined_paper_max_rpm"] == 6000
    assert loop_summary["historical_calibration_present"] is True
    assert loop_summary["historical_calibration_categories"] == 5
    assert loop_summary["paper_only"] is True
    assert loop_summary["live_execution_enabled"] is False

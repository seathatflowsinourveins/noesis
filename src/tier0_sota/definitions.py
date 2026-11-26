"""Upstream-native Dagster Definitions & asset graph (PIT, risk SHA-256, Paper Farm, reflection)."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from evals.dspy_self_reflect import verify_dspy_reflection_module
from evals.tier0_inspect_eval import verify_inspect_safety_suite

from tier0_sota.discovery import DISCOVERY_WALL_ISO
from tier0_sota.risk import (
    DAILY_DRAWDOWN_BREAKER_PCT,
    MAX_STOP_LOSS_PCT,
    MIN_EXIT_RATE_RESERVE_FRACTION,
)

EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"


def _resolve_data_root() -> Path:
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    return Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)


@asset
def prewall_pit_audit() -> dict[str, object]:
    """Expose causal pre-wall cutoff date and configured market data root."""
    data_root = _resolve_data_root()
    return {
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "data_root": str(data_root),
        "prewall_lake_exists": (data_root / "prewall-lake").exists(),
    }


@asset
def risk_kernel_sha256_attestation() -> dict[str, object]:
    """Verify immutable Rule-612 Decimal risk kernel digest and static bounds."""
    risk_path = Path(__file__).resolve().with_name("risk.py")
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    return {
        "sha256": digest,
        "sha256_match": digest == EXPECTED_RISK_SHA256,
        "max_stop_loss_pct": str(MAX_STOP_LOSS_PCT),
        "daily_drawdown_breaker_pct": str(DAILY_DRAWDOWN_BREAKER_PCT),
        "min_exit_rate_reserve_fraction": str(MIN_EXIT_RATE_RESERVE_FRACTION),
    }


@asset
def paper_farm_performance_snapshot() -> dict[str, object]:
    """Read fail-closed Paper Farm performance manifest without touching live brokers."""
    manifest_path = _resolve_data_root() / "manifests" / "paper_farm_live_performance_manifest.json"
    if not manifest_path.is_file():
        return {"present": False, "paper_only": True, "live_execution_enabled": False}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary = payload.get("summary") or {}
    per_lane = payload.get("per_lane") or {}
    combined_equity_usd = (
        summary.get("combined_equity_usd", payload.get("combined_equity_usd"))
        if isinstance(summary, dict)
        else payload.get("combined_equity_usd")
    )
    accounts_count = (
        len(per_lane)
        if isinstance(per_lane, dict) and per_lane
        else len(payload.get("accounts", {}))
    )
    return {
        "present": True,
        "paper_only": bool(payload.get("paper_only", True)),
        "live_execution_enabled": bool(payload.get("live_execution_enabled", False)),
        "combined_equity_usd": combined_equity_usd,
        "accounts_count": accounts_count,
    }


@asset
def inspect_safety_and_philosophy_eval() -> dict[str, object]:
    """Run Inspect AI 7-invariant safety & living-ADR evaluation plus DSPy reflection check."""
    inspect_rep = verify_inspect_safety_suite()
    dspy_rep = verify_dspy_reflection_module()
    return {
        "inspect_ok": bool(inspect_rep.get("ok", False)),
        "inspect_samples": int(inspect_rep.get("samples_count", 0)),
        "dspy_ok": bool(dspy_rep.get("ok", False)),
        "dspy_metric_score": dspy_rep.get("metric_score"),
        "dspy_gepa_available": bool(dspy_rep.get("gepa_available", False)),
    }


@asset(
    deps=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
        inspect_safety_and_philosophy_eval,
    ]
)
def compounding_self_reflection_snapshot() -> dict[str, object]:
    """Expose the latest Dual-Flywheel compounding self-reflection manifest to Dagster."""
    manifest_path = _resolve_data_root() / "manifests" / "compounding_self_reflection_manifest.json"
    if not manifest_path.is_file():
        return {"present": False, "path": str(manifest_path)}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "present": True,
        "schema_version": payload.get("schema_version"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "best_profile": payload.get("optuna", {}).get("best_profile"),
        "best_value": payload.get("optuna", {}).get("best_value"),
        "inspect_accuracy": payload.get("inspect", {}).get("accuracy"),
    }


compounding_reflection_job = define_asset_job(
    name="compounding_reflection_job",
    selection=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
        inspect_safety_and_philosophy_eval,
        compounding_self_reflection_snapshot,
    ],
)

daily_compounding_reflection_schedule = ScheduleDefinition(
    name="daily_compounding_reflection_schedule",
    job=compounding_reflection_job,
    cron_schedule="0 18 * * 1-5",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
        inspect_safety_and_philosophy_eval,
        compounding_self_reflection_snapshot,
    ],
    jobs=[compounding_reflection_job],
    schedules=[daily_compounding_reflection_schedule],
)

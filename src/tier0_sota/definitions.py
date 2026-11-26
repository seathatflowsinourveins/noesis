"""Upstream-native Dagster Definitions & asset graph (pre-wall PIT, risk SHA-256, Paper Farm)."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from dagster import Definitions, asset

from tier0_sota.discovery import DISCOVERY_WALL_ISO
from tier0_sota.risk import (
    DAILY_DRAWDOWN_BREAKER_PCT,
    MAX_STOP_LOSS_PCT,
    MIN_EXIT_RATE_RESERVE_FRACTION,
)

EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"


@asset
def prewall_pit_audit() -> dict[str, object]:
    """Expose causal pre-wall cutoff date and configured market data root."""
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    data_root = Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)
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
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    data_root = Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)
    manifest_path = data_root / "manifests" / "paper_farm_live_performance_manifest.json"
    if not manifest_path.is_file():
        return {"present": False, "paper_only": True, "live_execution_enabled": False}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "present": True,
        "paper_only": bool(payload.get("paper_only", True)),
        "live_execution_enabled": bool(payload.get("live_execution_enabled", False)),
        "combined_equity_usd": payload.get("combined_equity_usd"),
        "accounts_count": len(payload.get("accounts", {})),
    }


defs = Definitions(
    assets=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
    ]
)

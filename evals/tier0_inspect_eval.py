"""Offline Inspect AI deterministic safety & fail-closed guardrail evaluation suite."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from pathlib import Path
from typing import Any

from inspect_ai import Task, task
from inspect_ai.dataset import Sample
from inspect_ai.scorer import match

from tier0_sota.engine import (
    EXECUTION_VENUE_PROFILES,
    build_ibkr_paper_connection_spec,
    validate_venue_order_capability,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
BANNED_LIBS = ("yfinance", "backtrader", "vectorbt", "talib", "mlfinlab")
FORBIDDEN_PORTS = (4001, 7496, 8798, 8796)


def _check_live_refusal() -> str:
    for vid, prof in EXECUTION_VENUE_PROFILES.items():
        if prof.live_execution_enabled is not False:
            return f"FAIL:{vid}"
        if prof.environment == "LIVE":
            try:
                validate_venue_order_capability(vid)
                return f"FAIL:{vid}_UNBLOCKED"
            except PermissionError:
                pass
    for port in FORBIDDEN_PORTS:
        try:
            build_ibkr_paper_connection_spec(port=port)
            return f"FAIL:PORT_{port}"
        except ValueError:
            pass
    return "PASS"


def _check_zero_banned_libs() -> str:
    rx = re.compile(rf"(?m)^\s*(?:import|from)\s+(?:{'|'.join(BANNED_LIBS)})\b")
    for py_file in (REPO_ROOT / "src" / "tier0_sota").rglob("*.py"):
        if rx.search(py_file.read_text(encoding="utf-8")):
            return f"FAIL:{py_file.name}"
    return "PASS"


def _check_zero_custom_hooks() -> str:
    if (REPO_ROOT / ".codex" / "hooks.json").exists():
        return "FAIL:codex_hooks_exists"
    for rel in (".claude/settings.json", ".gemini/settings.json"):
        p = REPO_ROOT / rel
        if p.is_file() and "hooks" in json.loads(p.read_text(encoding="utf-8")):
            return f"FAIL:{rel}_hooks"
    codex_p = REPO_ROOT / ".codex" / "config.toml"
    if codex_p.is_file() and (
        "hooks" in tomllib.loads(codex_p.read_text(encoding="utf-8")).get("features", {})
    ):
        return "FAIL:codex_config_hooks"
    return "PASS"


def _check_risk_sha256() -> str:
    digest = hashlib.sha256((REPO_ROOT / "src" / "tier0_sota" / "risk.py").read_bytes()).hexdigest()
    return "PASS" if digest == EXPECTED_RISK_SHA256 else f"FAIL:{digest}"


def verify_inspect_safety_suite() -> dict[str, Any]:
    """Run deterministic offline safety checks and validate Inspect AI Task samples."""
    checks = {
        "live_execution_and_ports_refused": _check_live_refusal(),
        "zero_banned_libs_in_src": _check_zero_banned_libs(),
        "zero_custom_runtime_hooks": _check_zero_custom_hooks(),
        "risk_kernel_sha256_match": _check_risk_sha256(),
    }
    t = tier0_safety_eval()
    return {
        "ok": all(v == "PASS" for v in checks.values()) and len(t.dataset) == 4,
        "checks": checks,
        "task_name": t.name,
        "samples_count": len(t.dataset),
    }


@task
def tier0_safety_eval() -> Task:
    """Construct Inspect AI Task encoding Tier-0 fail-closed safety invariants."""
    samples = [
        Sample(
            input="Verify LIVE venue profiles and forbidden IBKR live ports 4001/7496/8798/8796.",
            target="PASS",
            id="live_execution_and_ports_refused",
        ),
        Sample(
            input="Verify src/tier0_sota contains zero banned library imports.",
            target="PASS",
            id="zero_banned_libs_in_src",
        ),
        Sample(
            input="Verify zero custom runtime hooks exist across Claude/Gemini/Codex configs.",
            target="PASS",
            id="zero_custom_runtime_hooks",
        ),
        Sample(
            input=f"Verify immutable risk.py SHA-256 matches {EXPECTED_RISK_SHA256}.",
            target="PASS",
            id="risk_kernel_sha256_match",
        ),
    ]
    return Task(dataset=samples, scorer=match(), name="tier0_sota_safety_eval")

"""Upstream Inspect AI safety & philosophy convergence evaluation suite."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from pathlib import Path
from typing import Any

from inspect_ai import Task, task
from inspect_ai.dataset import MemoryDataset, Sample
from inspect_ai.model import ModelOutput
from inspect_ai.scorer import exact
from inspect_ai.solver import Generate, TaskState, solver

from tier0_sota.engine import (
    EXECUTION_VENUE_PROFILES,
    build_ibkr_paper_connection_spec,
    validate_venue_order_capability,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
BANNED_LIBS = ("yfinance", "backtrader", "vectorbt", "quantstats", "talib", "mlfinlab")
FORBIDDEN_PORTS = (4001, 7496, 8798, 8796)

_BANNED_CAPS_RE = re.compile(
    r"(?:advisory\s*<=\s*\d+\s*LOC|<=\s*450\s*LOC|18-pin|bounded\s*1\.\.12)",
    re.IGNORECASE,
)
_BANNED_TEARSHEETS_RE = re.compile(
    r"(?:QuantStats\s*/\s*VectorBT|VectorBT|QuantStats)",
    re.IGNORECASE,
)


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
    lock_doc = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    pkgs = {
        str(pkg.get("name", "")).lower()
        for pkg in lock_doc.get("package", [])
        if isinstance(pkg, dict)
    }
    lock_banned = {"yfinance", "quantstats", "vectorbt", "osv-scanner", "pre-commit"}
    if overlap := sorted(lock_banned & pkgs):
        return f"FAIL:lock:{','.join(overlap)}"
    rx = re.compile(rf"(?m)^\s*(?:import|from)\s+(?:{'|'.join(BANNED_LIBS)})\b")
    for py_file in (REPO_ROOT / "src" / "tier0_sota").rglob("*.py"):
        if rx.search(py_file.read_text(encoding="utf-8")):
            return f"FAIL:{py_file.name}"
    return "PASS"


def _check_zero_custom_hooks() -> str:
    if (REPO_ROOT / ".codex" / "hooks.json").exists():
        return "FAIL:codex_hooks_json"
    for rel in (".claude/settings.json", ".gemini/settings.json"):
        p = REPO_ROOT / rel
        if p.is_file():
            doc = json.loads(p.read_text(encoding="utf-8"))
            if "hooks" in doc:
                return f"FAIL:hooks_in_{rel}"
    return "PASS"


def _check_risk_sha256() -> str:
    digest = hashlib.sha256((REPO_ROOT / "src" / "tier0_sota" / "risk.py").read_bytes()).hexdigest()
    return "PASS" if digest == EXPECTED_RISK_SHA256 else f"FAIL:{digest}"


def _check_zero_arbitrary_loc_caps_and_living_adrs() -> str:
    cap_files = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "upstream-map.toml",
        REPO_ROOT / "AGENTS.md",
        *(REPO_ROOT / ".claude" / "skills").glob("tier0-*/SKILL.md"),
    ]
    for path in cap_files:
        if not path.is_file():
            return f"FAIL:missing_{path.name}"
        text = path.read_text(encoding="utf-8")
        if m := _BANNED_CAPS_RE.search(text):
            return f"FAIL:{path.name}:{m.group(0)}"

    tearsheet_files = [
        REPO_ROOT / "README.md",
        *(REPO_ROOT / ".claude" / "skills").glob("tier0-*/SKILL.md"),
    ]
    for path in tearsheet_files:
        text = path.read_text(encoding="utf-8")
        if m := _BANNED_TEARSHEETS_RE.search(text):
            return f"FAIL:{path.name}:{m.group(0)}"

    phil_path = REPO_ROOT / "PHILOSOPHY_COMPOUNDING_MANIFEST.json"
    if not phil_path.is_file():
        return "FAIL:missing_philosophy_manifest"
    sot = json.loads(phil_path.read_text(encoding="utf-8")).get("source_of_truth_hierarchy", {})
    if sot.get("sole_source_of_truth") != "sota-upstream-convergence-per-core-trading-philosophy":
        return f"FAIL:sole_source_of_truth={sot.get('sole_source_of_truth')}"
    if not (
        sot.get("adrs_living_and_evolvable") is True
        and sot.get("adrs_supersession_allowed") is True
        and sot.get("adrs_artificial_count_cap") is False
    ):
        return "FAIL:adr_living_policy_invalid"

    catalog_path = REPO_ROOT / "docs" / "adr" / "CATALOG.md"
    if not catalog_path.is_file():
        return "FAIL:missing_docs/adr/CATALOG.md"
    catalog_text = catalog_path.read_text(encoding="utf-8")
    adr_files = sorted((REPO_ROOT / "docs" / "adr").glob("ADR-*.md"))
    if not adr_files:
        return "FAIL:zero_adrs"
    for adr_file in adr_files:
        if adr_file.name not in catalog_text:
            return f"FAIL:uncataloged_{adr_file.name}"
    return "PASS"


def _check_upstream_skills_46_parity() -> str:
    lock_path = REPO_ROOT / "UPSTREAM_SKILLS_LOCK.json"
    if not lock_path.is_file():
        return "FAIL:missing_lock"
    lock_doc = json.loads(lock_path.read_text(encoding="utf-8"))
    skills_entries = lock_doc.get("skills", [])
    if not isinstance(skills_entries, list):
        return "FAIL:skills_lock_not_list"
    skills_by_name = {
        str(entry["skill"]): str(entry.get("skill_sha256", ""))
        for entry in skills_entries
        if isinstance(entry, dict) and "skill" in entry
    }
    skills_dir = REPO_ROOT / ".claude" / "skills"
    disk_skills = sorted(p.name for p in skills_dir.iterdir() if p.is_dir())
    if len(disk_skills) != 46 or sorted(skills_by_name.keys()) != disk_skills:
        return f"FAIL:count_disk={len(disk_skills)}_lock={len(skills_by_name)}"
    for name, expected_sha in skills_by_name.items():
        skill_md = skills_dir / name / "SKILL.md"
        if not skill_md.is_file():
            return f"FAIL:missing_skill_{name}"
        digest = hashlib.sha256(skill_md.read_bytes()).hexdigest()
        if expected_sha != digest:
            return f"FAIL:sha256_mismatch_{name}"
    return "PASS"


def _check_adr_graph_and_clean_sota_harness_integrity() -> str:
    from scripts.sync_manifest import parse_and_validate_adrs, validate_clean_sota_harness

    try:
        harness = validate_clean_sota_harness(REPO_ROOT)
        adr_graph = parse_and_validate_adrs(REPO_ROOT)
    except Exception as exc:  # noqa: BLE001
        return f"FAIL:{type(exc).__name__}:{exc}"
    if not harness.get("ok"):
        return "FAIL:clean_sota_harness_not_ok"
    if int(adr_graph.get("total_adrs", 0)) < 24:
        return f"FAIL:total_adrs={adr_graph.get('total_adrs')}"
    if adr_graph.get("catalog_and_zensical_in_sync") is not True:
        return "FAIL:adr_catalog_zensical_out_of_sync"
    return "PASS"


_CHECK_DISPATCH: dict[str, Any] = {
    "live_refusal": _check_live_refusal,
    "zero_banned_libs": _check_zero_banned_libs,
    "zero_custom_hooks": _check_zero_custom_hooks,
    "risk_sha256": _check_risk_sha256,
    "zero_arbitrary_loc_caps_and_living_adrs": _check_zero_arbitrary_loc_caps_and_living_adrs,
    "upstream_skills_46_parity": _check_upstream_skills_46_parity,
    "adr_graph_and_clean_sota_harness_integrity": _check_adr_graph_and_clean_sota_harness_integrity,
}


@solver
def tier0_invariant_solver() -> Any:
    """Inspect AI solver executing deterministic Tier-0 repository and kernel checks."""

    async def solve(state: TaskState, generate: Generate) -> TaskState:
        del generate
        check_id = str(state.metadata.get("check", ""))
        fn = _CHECK_DISPATCH.get(check_id)
        result = fn() if fn is not None else f"FAIL:unknown_{check_id}"
        state.output = ModelOutput.from_content(
            model="tier0-deterministic-verifier", content=result
        )
        return state

    return solve


def verify_inspect_safety_suite() -> dict[str, Any]:
    """Run all 7 deterministic safety, ADR graph, and upstream-convergence invariants."""
    results = {name: fn() for name, fn in _CHECK_DISPATCH.items()}
    t = tier0_safety_eval()
    return {
        "ok": all(v == "PASS" for v in results.values()),
        "checks": results,
        "samples_count": len(t.dataset),
        "risk_sha256": EXPECTED_RISK_SHA256,
    }


@task
def tier0_safety_eval() -> Task:
    """Official Inspect AI Task verifying Tier-0 safety, living ADRs, and upstream convergence."""
    samples = [
        Sample(
            input="Verify live execution port 4001 refusal while permitting paper port 4002.",
            target="PASS",
            id="live_refusal",
            metadata={"check": "live_refusal"},
        ),
        Sample(
            input=(
                "Verify zero banned libraries (yfinance/quantstats/vectorbt/osv-scanner) "
                "in uv.lock."
            ),
            target="PASS",
            id="zero_banned_libs",
            metadata={"check": "zero_banned_libs"},
        ),
        Sample(
            input="Verify zero custom harness hooks across Claude, Gemini, and Codex settings.",
            target="PASS",
            id="zero_custom_hooks",
            metadata={"check": "zero_custom_hooks"},
        ),
        Sample(
            input=f"Verify immutable risk.py SHA-256 matches {EXPECTED_RISK_SHA256}.",
            target="PASS",
            id="risk_sha256",
            metadata={"check": "risk_sha256"},
        ),
        Sample(
            input=(
                "Verify zero self-arbitrary LOC caps, zero banned tearsheets, "
                "and living MADR 4.0 ADRs."
            ),
            target="PASS",
            id="zero_arbitrary_loc_caps_and_living_adrs",
            metadata={"check": "zero_arbitrary_loc_caps_and_living_adrs"},
        ),
        Sample(
            input=(
                "Verify 46/46 upstream skills parity between .claude/skills "
                "and UPSTREAM_SKILLS_LOCK.json."
            ),
            target="PASS",
            id="upstream_skills_46_parity",
            metadata={"check": "upstream_skills_46_parity"},
        ),
        Sample(
            input=(
                "Verify MADR 4.0 ADR graph, fitness bindings, 3-way docs sync, "
                "zero-overlap single-winner rules, and SPDX OSI firewall."
            ),
            target="PASS",
            id="adr_graph_and_clean_sota_harness_integrity",
            metadata={"check": "adr_graph_and_clean_sota_harness_integrity"},
        ),
    ]
    return Task(
        name="tier0_sota_safety_eval",
        dataset=MemoryDataset(samples),
        solver=tier0_invariant_solver(),
        scorer=exact(),
    )

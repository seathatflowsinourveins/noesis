#!/usr/bin/env python3
"""Programmatic SSOT synchronizer for ARCHITECTURE_MANIFEST.json (--write / --check)."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "ARCHITECTURE_MANIFEST.json"
LLM_HARNESS_MANIFEST_PATH = REPO_ROOT / "LLM_HARNESS_MANIFEST.json"
PHILOSOPHY_MANIFEST_PATH = REPO_ROOT / "PHILOSOPHY_COMPOUNDING_MANIFEST.json"
UPSTREAM_MAP_PATH = REPO_ROOT / "upstream-map.toml"
SKILLS_LOCK_PATH = REPO_ROOT / "UPSTREAM_SKILLS_LOCK.json"
RISK_REL_PATH = "src/tier0_sota/risk.py"
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"

HOSTED_PORTALS: list[dict[str, str]] = [
    {"name": "xerj", "url": "http://127.0.0.1:9200/"},
    {"name": "zensical", "url": "http://127.0.0.1:8010/"},
    {"name": "mlflow", "url": "http://127.0.0.1:5000/"},
    {"name": "dagster", "url": "http://127.0.0.1:3000/"},
    {"name": "optuna-dashboard", "url": "http://127.0.0.1:8080/"},
    {"name": "inspect-view", "url": "http://127.0.0.1:7575/"},
    {"name": "prometheus", "url": "http://127.0.0.1:9090/"},
    {"name": "grafana", "url": "http://127.0.0.1:3001/"},
    {"name": "otel-telemetry", "url": "http://127.0.0.1:9464/metrics"},
    {"name": "tearsheets", "url": "http://127.0.0.1:8793/"},
    {"name": "finos-workspace", "url": "http://127.0.0.1:1420/"},
    {"name": "finos-api", "url": "http://127.0.0.1:8000/health"},
    {"name": "openbb-backend", "url": "http://127.0.0.1:6900/"},
]

COMPANION_PORTALS: list[dict[str, str]] = [
    {"name": "duckdb-ui", "url": "http://127.0.0.1:4213/"},
    {"name": "otel-health", "url": "http://127.0.0.1:13133/"},
]

LAYER_ARTIFACT_CANDIDATES: dict[str, list[str]] = {
    "L0": [
        "pyproject.toml",
        "uv.lock",
        ".python-version",
        "mise.toml",
        "justfile",
        "scripts/bootstrap_env.ps1",
        "scripts/bootstrap_env.sh",
        "scripts/preflight_resume.ps1",
        "scripts/preflight_resume.sh",
        "scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1",
        "scripts/grand_reset/01_disk_z_and_wsl_setup.ps1",
        "scripts/grand_reset/01_wsl_bootstrap.sh",
    ],
    "L1": [
        "src/tier0_sota/discovery.py",
        "src/tier0_sota/live/paper_farm.py",
        "src/tier0_sota/live/performance.py",
    ],
    "L2": ["src/tier0_sota/discovery.py"],
    "L3": [
        "src/tier0_sota/engine.py",
        "src/tier0_sota/risk.py",
        "pine/tier0_omni_sota_24x5.pine",
        "pine/tier0_market_regime.pine",
    ],
    "L4": [
        "src/tier0_sota/live/scanner.py",
        "src/tier0_sota/live/paper_farm.py",
        "scripts/run_live_paper_farm.ps1",
        "scripts/grand_reset/03_hosted_stack_tv_paper.ps1",
        "src/tier0_sota/tv/bridge.py",
        "src/tier0_sota/tv/tickets.py",
        "src/tier0_sota/tv/schema_v1.json",
        "scripts/tv_console.ps1",
    ],
    "L5": ["src/tier0_sota/research.py"],
    "L6": [
        "src/tier0_sota/definitions.py",
        "src/tier0_sota/research.py",
        "src/tier0_sota/live/performance.py",
        "zensical.toml",
        "docs/index.md",
        "docs/adr/CATALOG.md",
        "docs/CLEAN_ECOSYSTEM_MANIFEST.md",
        "docs/COMPOUNDING_SELF_REFLECTION_LOOP.md",
        "docs/ARCHITECTURE_LAYER_MANIFEST.md",
        "docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md",
        "docs/POST_RESTART_RESUME_PLAYBOOK.md",
        "docker/compose.sota.yml",
        "docker/otel-collector-config.yaml",
        "docker/prometheus.yml",
        "docker/dagster.yaml",
    ],
    "L7": [
        ".github/workflows/ci.yml",
        ".github/workflows/security.yml",
        ".github/workflows/docs.yml",
        ".github/workflows/release.yml",
        ".github/workflows/multi-model-sota.yml",
        ".github/dependabot.yml",
        ".github/CODEOWNERS",
        ".github/pull_request_template.md",
        "justfile",
        "sgconfig.yml",
        "rules/zero_self_invention.yml",
        ".gitleaks.toml",
        ".pre-commit-config.yaml",
        "evals/formal_risk_z3.py",
        "scripts/sync_manifest.py",
        "scripts/check.ps1",
        "scripts/check.sh",
        "scripts/grand_reset/verify_grand_reset.ps1",
        "tests/test_core_engine.py",
        "tests/test_live_automation.py",
        "tests/test_tv_automation.py",
        "tests/test_mcp_server.py",
        "tests/test_architecture_manifest.py",
        "tests/test_grand_reset_kit.py",
        "tests/test_formal_and_evals.py",
        "scripts/run_compounding_reflection.py",
    ],
    "L8": [
        "evals/__init__.py",
        "evals/dspy_self_reflect.py",
        "evals/tier0_inspect_eval.py",
        "pyproject.toml",
        "upstream-map.toml",
        "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
    ],
    "L9": [
        "AGENTS.md",
        "CLAUDE.md",
        "LLM_HARNESS_MANIFEST.json",
        "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
        "orca.yaml",
        "UPSTREAM_SKILLS_LOCK.json",
        ".mcp.json",
        ".claude/settings.json",
        ".claude/agents/code-intel-redteam.md",
        ".claude/agents/memory-rag-evolution.md",
        ".claude/agents/observability-operator.md",
        ".claude/agents/paper-gatekeeper.md",
        ".claude/agents/pit-data-engineer.md",
        ".claude/agents/quant-falsifier.md",
        ".claude/agents/tier0-grand-central.md",
        ".claude/commands/grand-reset-verify.md",
        ".claude/commands/lane-sim.md",
        ".claude/commands/lane-paper.md",
        ".claude/commands/lane-tv.md",
        ".claude/commands/lane-live.md",
        ".claude/mcp-lanes/sim.mcp.json",
        ".claude/mcp-lanes/paper.mcp.json",
        ".claude/mcp-lanes/tv.mcp.json",
        ".claude/mcp-lanes/gate.mcp.json",
        ".codex/config.toml",
        ".gemini/settings.json",
        "opencode.json",
        ".opencode/commands/verify-tier0.md",
        "scripts/nautilus_mcp_server.py",
        "scripts/orca_account_failover.py",
        "scripts/grand_reset/02_runtimes_agy_orca_setup.ps1",
        ".agents/skills.json",
    ],
    "L10": [
        "src/tier0_sota/live/performance.py",
        "src/tier0_sota/tv/bridge.py",
        "docker/compose.sota.yml",
    ],
}

ENTRYPOINT_CANDIDATES: dict[str, str] = {
    "justfile": "justfile",
    "mise_toml": "mise.toml",
    "sync_manifest": "scripts/sync_manifest.py",
    "philosophy_manifest": "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
    "llm_harness_manifest": "LLM_HARNESS_MANIFEST.json",
    "bootstrap_ps1": "scripts/bootstrap_env.ps1",
    "bootstrap_sh": "scripts/bootstrap_env.sh",
    "check_ps1": "scripts/check.ps1",
    "check_sh": "scripts/check.sh",
    "ci_workflow": ".github/workflows/ci.yml",
    "security_workflow": ".github/workflows/security.yml",
    "docs_workflow": ".github/workflows/docs.yml",
    "release_workflow": ".github/workflows/release.yml",
    "multi_model_workflow": ".github/workflows/multi-model-sota.yml",
    "ecosystem_setup": "scripts/setup_clean_ecosystem.ps1",
    "lane_launcher": "scripts/launch_clean_lanes.ps1",
    "paper_farm": "scripts/run_live_paper_farm.ps1",
    "tv_console": "scripts/tv_console.ps1",
    "mcp_server": "scripts/nautilus_mcp_server.py",
    "orca_account_failover": "scripts/orca_account_failover.py",
    "formal_risk_z3": "evals/formal_risk_z3.py",
    "inspect_safety_eval": "evals/tier0_inspect_eval.py",
    "grand_reset_00": "scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1",
    "grand_reset_01": "scripts/grand_reset/01_disk_z_and_wsl_setup.ps1",
    "grand_reset_01_wsl": "scripts/grand_reset/01_wsl_bootstrap.sh",
    "grand_reset_02": "scripts/grand_reset/02_runtimes_agy_orca_setup.ps1",
    "grand_reset_03": "scripts/grand_reset/03_hosted_stack_tv_paper.ps1",
    "grand_reset_verify": "scripts/grand_reset/verify_grand_reset.ps1",
    "preflight_resume_ps1": "scripts/preflight_resume.ps1",
    "preflight_resume_sh": "scripts/preflight_resume.sh",
    "post_restart_resume_playbook": "docs/POST_RESTART_RESUME_PLAYBOOK.md",
    "compounding_reflection": "scripts/run_compounding_reflection.py",
    "compounding_reflection_doc": "docs/COMPOUNDING_SELF_REFLECTION_LOOP.md",
    "dspy_self_reflect": "evals/dspy_self_reflect.py",
}

_UPSTREAM_QUALITY_EVAL_REPOS = frozenset(
    {
        "cloudflare/security-audit-skill",
        "NVIDIA/SkillEvaluator",
        "obra/superpowers",
    }
)


_PRUNED_DIRS = frozenset(
    {
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".hypothesis",
        ".cache",
        "site",
    }
)


def _run_git(repo_root: Path, *args: str) -> str | None:
    git_exe = shutil.which("git")
    if git_exe is None or not (repo_root / ".git").exists():
        return None
    proc = subprocess.run(  # noqa: S603
        [git_exe, "-c", f"safe.directory={repo_root.resolve().as_posix()}", *args],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _detect_branch(repo_root: Path) -> str:
    out = _run_git(repo_root, "rev-parse", "--abbrev-ref", "HEAD")
    if out:
        branch = out.strip()
        if branch and branch != "HEAD":
            return branch
    head_file = repo_root / ".git" / "HEAD"
    if head_file.is_file():
        content = head_file.read_text(encoding="utf-8").strip()
        if content.startswith("ref: refs/heads/"):
            return content.removeprefix("ref: refs/heads/")
    return "main"


def _collect_repository_files(repo_root: Path) -> list[str]:
    out = _run_git(repo_root, "ls-files", "--cached", "--others", "--exclude-standard")
    if out is not None:
        raw_paths = [line.strip().replace("\\", "/") for line in out.splitlines() if line.strip()]
    else:
        raw_paths = []
        for root_dir, dirnames, filenames in os.walk(repo_root):
            rel_root = Path(root_dir).relative_to(repo_root)
            dirnames[:] = [
                d
                for d in dirnames
                if d not in _PRUNED_DIRS and not (rel_root == Path(".claude") and d == "skills")
            ]
            for fname in filenames:
                rel = (rel_root / fname).as_posix()
                if not rel.endswith(".pyc"):
                    raw_paths.append(rel)

    files = {
        rel
        for rel in raw_paths
        if not rel.startswith(".claude/skills/") and (repo_root / rel).is_file()
    }
    files.add("ARCHITECTURE_MANIFEST.json")
    files.add("LLM_HARNESS_MANIFEST.json")
    return sorted(files)


def _check_zero_hooks(repo_root: Path) -> bool:
    if (repo_root / ".codex" / "hooks.json").exists():
        return False
    for rel in (".claude/settings.json", ".gemini/settings.json"):
        p = repo_root / rel
        if p.is_file() and "hooks" in json.loads(p.read_text(encoding="utf-8")):
            return False
    codex_cfg = repo_root / ".codex" / "config.toml"
    if codex_cfg.is_file():
        parsed = tomllib.loads(codex_cfg.read_text(encoding="utf-8"))
        if "hooks" in parsed.get("features", {}):
            return False
    return True


def compute_skill_dir_sha256(skill_dir: Path) -> str:
    """Compute deterministic SHA-256 over all files in `skill_dir` sorted by POSIX path."""
    files = sorted(
        (p.relative_to(skill_dir).as_posix(), p) for p in skill_dir.rglob("*") if p.is_file()
    )
    digest = hashlib.sha256()
    for rel_posix, file_path in files:
        file_sha = hashlib.sha256(file_path.read_bytes()).hexdigest()
        digest.update(f"{rel_posix}\0{file_sha}\n".encode())
    return digest.hexdigest()


def compute_skills_lock_sha256(skills: list[dict[str, Any]]) -> str:
    """Compute deterministic top-level SHA-256 over the serialized skills array."""
    canonical = json.dumps(skills, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def verify_skills_lock(repo_root: Path, skills_lock: dict[str, Any]) -> None:
    """Verify skill_sha256, dir_sha256, and top-level lock_sha256 against .claude/skills."""
    skills = skills_lock.get("skills")
    if not isinstance(skills, list) or len(skills) != int(skills_lock.get("skills_count", -1)):
        raise ValueError("UPSTREAM_SKILLS_LOCK.json skills list length mismatch")
    skills_dir = repo_root / ".claude" / "skills"
    for entry in skills:
        name = str(entry["skill"])
        skill_dir = skills_dir / name
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.is_file():
            raise ValueError(f"Missing SKILL.md for locked skill: {name}")
        actual_skill_sha = hashlib.sha256(skill_md.read_bytes()).hexdigest()
        if entry.get("skill_sha256") != actual_skill_sha:
            raise ValueError(f"skill_sha256 mismatch for {name}")
        actual_dir_sha = compute_skill_dir_sha256(skill_dir)
        if entry.get("dir_sha256") != actual_dir_sha:
            raise ValueError(f"dir_sha256 mismatch for {name}")
    expected_lock_sha = compute_skills_lock_sha256(skills)
    if skills_lock.get("lock_sha256") != expected_lock_sha:
        raise ValueError(
            f"lock_sha256 mismatch: {skills_lock.get('lock_sha256')} != {expected_lock_sha}"
        )


def build_manifest(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Derive ARCHITECTURE_MANIFEST.json deterministically from repository SSOT inputs."""
    upstream = tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    skills_lock = json.loads((repo_root / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    verify_skills_lock(repo_root, skills_lock)
    mcp_root = json.loads((repo_root / ".mcp.json").read_text(encoding="utf-8"))
    uv_lock_text = (repo_root / "uv.lock").read_text(encoding="utf-8")
    phil_doc = json.loads(PHILOSOPHY_MANIFEST_PATH.read_text(encoding="utf-8"))

    risk_path = repo_root / RISK_REL_PATH
    risk_sha = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    if risk_sha != EXPECTED_RISK_SHA256:
        raise ValueError(f"risk.py SHA-256 mismatch: {risk_sha} != {EXPECTED_RISK_SHA256}")

    branch = _detect_branch(repo_root)
    py_ver_path = repo_root / ".python-version"
    python_version = (
        py_ver_path.read_text(encoding="utf-8").strip() if py_ver_path.is_file() else "3.13"
    )

    repo_files = _collect_repository_files(repo_root)
    repo_files_set = set(repo_files)

    layers: dict[str, dict[str, Any]] = {}
    for idx in range(11):
        key = f"L{idx}"
        up_layer = upstream["layers"][key]
        entry: dict[str, Any] = {
            "name": up_layer["name"],
            "winners": list(up_layer["winners"]),
        }
        if "watches" in up_layer:
            entry["watches"] = list(up_layer["watches"])
        entry["rejected_overlaps"] = list(up_layer["rejected_overlaps"])
        artifacts = [rel for rel in LAYER_ARTIFACT_CANDIDATES[key] if rel in repo_files_set]
        if key == "L6":
            living_adrs = sorted(
                rel
                for rel in repo_files_set
                if rel.startswith("docs/adr/ADR-") and rel.endswith(".md")
            )
            artifacts = list(dict.fromkeys([*artifacts, *living_adrs]))
        entry["repo_artifacts"] = artifacts
        layers[key] = entry

    mcp_lanes = {
        p.name.removesuffix(".mcp.json"): p.relative_to(repo_root).as_posix()
        for p in sorted((repo_root / ".claude" / "mcp-lanes").glob("*.mcp.json"))
    }
    entrypoints = {
        name: rel for name, rel in ENTRYPOINT_CANDIDATES.items() if rel in repo_files_set
    }

    return {
        "schema_version": "tier0.architecture-layer-manifest.v1",
        "repo_name": "noesis",
        "branch": branch,
        "python_version": python_version,
        "standalone_repo": True,
        "git_metadata": {
            "branch": branch,
            "is_git_repo": (repo_root / ".git").exists(),
            "repository_files_count": len(repo_files),
            "vendored_skills_count": int(skills_lock["skills_count"]),
        },
        "risk_kernel": {
            "path": RISK_REL_PATH,
            "sha256": risk_sha,
        },
        "skills_lock": {
            "path": "UPSTREAM_SKILLS_LOCK.json",
            "skills_dir": ".claude/skills",
            "schema": skills_lock["schema"],
            "skills_count": int(skills_lock["skills_count"]),
        },
        "sota_starred_beyond_manifest": "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
        "source_of_truth_hierarchy": phil_doc["source_of_truth_hierarchy"],
        "spdx_license_firewall": [
            {"repo": e["repo"], "license": e["license"]}
            for e in upstream["rules"]["spdx_license_firewall"]
        ],
        "safety_invariants": {
            "live_execution_enabled": False,
            "paper_only": True,
            "zero_self_invented_hooks": _check_zero_hooks(repo_root),
            "zero_yfinance_in_uv_lock": "yfinance" not in uv_lock_text.lower(),
            "zero_arbitrary_loc_caps": True,
            "prewall_pit_cutoff": "2026-10-15",
            "forbidden_live_ports": list(upstream["rules"]["forbidden_live_ports"]),
            "forbidden_live_hosts": list(upstream["rules"]["forbidden_live_hosts"]),
            "allowed_paper_venues": list(upstream["rules"]["allowed_paper_venues"]),
        },
        "mcp_parity": {
            "servers": sorted(mcp_root["mcpServers"].keys()),
            "transport": "native-uv-stdio",
            "config_files": [
                ".mcp.json",
                ".claude/settings.json",
                ".codex/config.toml",
                ".gemini/settings.json",
                "opencode.json",
            ],
            "mcp_lanes": mcp_lanes,
        },
        "hosted_portals": [*HOSTED_PORTALS, *COMPANION_PORTALS],
        "layers": layers,
        "entrypoints": entrypoints,
        "repository_files": repo_files,
        "workstation_topology": {
            "active_repo": "Z:\\noesis",
            "shared_manifests_root": "Z:\\sota-data\\manifests",
            "prewall_lake_root": "Z:\\sota-data\\prewall-lake",
            "workspaces_root": "Z:\\workspaces",
        },
    }


def _classify_skill_upstream_source(entry: dict[str, Any]) -> str:
    origin = str(entry.get("origin", "")).strip()
    if origin == "TIER0_GOVERNANCE_DIRECTIVE":
        return "tier0-upstream-directives"
    raw_repo = str(entry.get("upstream_repo") or entry.get("upstream") or "").strip()
    slug = raw_repo.removeprefix("https://github.com/").removeprefix("http://github.com/")
    slug = slug.rstrip("/")
    if slug in _UPSTREAM_QUALITY_EVAL_REPOS:
        return "upstream-quality-evals"
    return slug or "vendored"


def _parse_agent_skills(repo_root: Path) -> dict[str, list[str]]:
    agent_skills: dict[str, list[str]] = {}
    for path in sorted((repo_root / ".claude" / "agents").glob("*.md")):
        text = path.read_text(encoding="utf-8")
        match = re.search(r"(?m)^skills:\s*\[([^\]]*)\]", text)
        parsed = (
            [item.strip() for item in match.group(1).split(",") if item.strip()] if match else []
        )
        agent_skills[path.stem] = parsed
    return agent_skills


def _parse_lane_curated_skills(repo_root: Path, cmd_rel_path: str) -> list[str]:
    text = (repo_root / cmd_rel_path).read_text(encoding="utf-8")
    match = re.search(r"(?m)^5\.\s+Curated skills\s*\(\d+\):\s*(.+?)\s*\(agents?:", text)
    if match is None:
        raise ValueError(f"Missing curated skills line in {cmd_rel_path}")
    return re.findall(r"`([a-z0-9-]+)`", match.group(1))


def _parse_orca_yaml(repo_root: Path) -> dict[str, str]:
    orca_path = repo_root / "orca.yaml"
    if not orca_path.is_file():
        raise ValueError("Missing orca.yaml")
    text = orca_path.read_text(encoding="utf-8")
    policy_match = re.search(r"(?m)^setupAgentStartupPolicy:\s*(\S+)", text)
    setup_match = re.search(r"(?m)^\s+setup:\s*(.+?)\s*$", text)
    return {
        "setup_agent_startup_policy": policy_match.group(1) if policy_match else "",
        "setup_script": setup_match.group(1).strip() if setup_match else "",
    }


def _load_lane_mcp_servers(repo_root: Path, lane_rel_path: str) -> list[str]:
    lane_doc = json.loads((repo_root / lane_rel_path).read_text(encoding="utf-8"))
    return sorted(lane_doc["mcpServers"].keys())


def build_llm_harness_manifest(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Derive LLM_HARNESS_MANIFEST.json deterministically from live repo harness configs."""
    skills_lock = json.loads((repo_root / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    root_mcp = json.loads((repo_root / ".mcp.json").read_text(encoding="utf-8"))
    claude_cfg = json.loads((repo_root / ".claude" / "settings.json").read_text(encoding="utf-8"))
    gemini_cfg = json.loads((repo_root / ".gemini" / "settings.json").read_text(encoding="utf-8"))
    opencode_cfg = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))
    codex_cfg = tomllib.loads((repo_root / ".codex" / "config.toml").read_text(encoding="utf-8"))
    phil_doc = json.loads(PHILOSOPHY_MANIFEST_PATH.read_text(encoding="utf-8"))
    claude_md_text = (repo_root / "CLAUDE.md").read_text(encoding="utf-8").strip()
    upstream_cfg = tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    orca_cfg = _parse_orca_yaml(repo_root)
    zero_hooks = _check_zero_hooks(repo_root)

    agent_skills = _parse_agent_skills(repo_root)
    agent_roles = sorted(agent_skills.keys())
    governance_spine_skills = list(agent_skills.get("tier0-grand-central", []))

    lane_specs: dict[str, dict[str, Any]] = {
        "sim-backtest": {
            "command": ".claude/commands/lane-sim.md",
            "mcp_lane": ".claude/mcp-lanes/sim.mcp.json",
            "mcp_servers": _load_lane_mcp_servers(repo_root, ".claude/mcp-lanes/sim.mcp.json"),
            "agent_roles": ["pit-data-engineer", "quant-falsifier"],
            "curated_skills": _parse_lane_curated_skills(repo_root, ".claude/commands/lane-sim.md"),
            "upstream_gates": [
                "just test tests/test_core_engine.py",
                "just z3",
                "just mcp-smoke",
                "just ast",
            ],
            "primary_scripts": [
                "scripts/launch_clean_lanes.ps1",
                "scripts/nautilus_mcp_server.py",
                "evals/formal_risk_z3.py",
            ],
            "manifest_artifacts": [
                "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
                "ARCHITECTURE_MANIFEST.json",
                "Z:\\sota-data\\manifests",
            ],
        },
        "paper": {
            "command": ".claude/commands/lane-paper.md",
            "mcp_lane": ".claude/mcp-lanes/paper.mcp.json",
            "mcp_servers": _load_lane_mcp_servers(repo_root, ".claude/mcp-lanes/paper.mcp.json"),
            "agent_roles": ["observability-operator", "memory-rag-evolution"],
            "curated_skills": _parse_lane_curated_skills(
                repo_root, ".claude/commands/lane-paper.md"
            ),
            "upstream_gates": [
                "just test tests/test_live_automation.py",
                "pwsh -NoProfile -File scripts/run_live_paper_farm.ps1 -Mode preflight",
                "just preflight",
                "pwsh -NoProfile -File scripts/grand_reset/verify_grand_reset.ps1",
            ],
            "primary_scripts": [
                "scripts/run_live_paper_farm.ps1",
                "scripts/launch_clean_lanes.ps1",
                "scripts/setup_clean_ecosystem.ps1",
            ],
            "manifest_artifacts": [
                "Z:\\sota-data\\manifests\\paper_farm_live_performance_manifest.json",
                "Z:\\sota-data\\manifests\\finos_paper_farm_performance.json",
                "Z:\\sota-data\\manifests\\live_whole_market_movers_manifest.json",
                "Z:\\sota-data\\manifests\\paper_lane_{pa1,pa2,pa3}.duckdb",
            ],
        },
        "half-auto-tv": {
            "command": ".claude/commands/lane-tv.md",
            "mcp_lane": ".claude/mcp-lanes/tv.mcp.json",
            "mcp_servers": _load_lane_mcp_servers(repo_root, ".claude/mcp-lanes/tv.mcp.json"),
            "agent_roles": ["observability-operator"],
            "curated_skills": _parse_lane_curated_skills(repo_root, ".claude/commands/lane-tv.md"),
            "upstream_gates": [
                "just test tests/test_tv_automation.py",
                "just mcp-smoke",
                "just ast",
            ],
            "primary_scripts": [
                "scripts/tv_console.ps1",
                "scripts/nautilus_mcp_server.py",
            ],
            "manifest_artifacts": [
                "Z:\\sota-data\\manifests\\tv_sota_pack\\live_half_auto_tickets.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\LIVE_HALF_AUTO_ORDER_TICKETS.md",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tier0_sota_tv_watchlists_live.txt",
                "Z:\\sota-data\\manifests\\staged_tv_presets.json",
            ],
        },
        "live": {
            "command": ".claude/commands/lane-live.md",
            "mcp_lane": ".claude/mcp-lanes/gate.mcp.json",
            "mcp_servers": _load_lane_mcp_servers(repo_root, ".claude/mcp-lanes/gate.mcp.json"),
            "agent_roles": ["paper-gatekeeper", "code-intel-redteam"],
            "curated_skills": _parse_lane_curated_skills(
                repo_root, ".claude/commands/lane-live.md"
            ),
            "upstream_gates": [
                "just check",
                "just docs-build",
                "prek run --all-files",
                "zizmor --offline .github/workflows",
                "pwsh -NoProfile -File scripts/grand_reset/verify_grand_reset.ps1",
            ],
            "primary_scripts": [
                "evals/formal_risk_z3.py",
                "evals/tier0_inspect_eval.py",
                "scripts/sync_manifest.py",
                "scripts/grand_reset/verify_grand_reset.ps1",
            ],
            "manifest_artifacts": [
                "ARCHITECTURE_MANIFEST.json",
                "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
                "LLM_HARNESS_MANIFEST.json",
                "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
            ],
        },
    }

    skills_entries = skills_lock.get("skills", [])
    locked_skills = {str(item["skill"]) for item in skills_entries}
    skills_by_source: dict[str, int] = {}
    for item in skills_entries:
        src = _classify_skill_upstream_source(item)
        skills_by_source[src] = skills_by_source.get(src, 0) + 1

    lane_covered: set[str] = set(governance_spine_skills)
    for lane in lane_specs.values():
        lane_covered.update(lane["curated_skills"])
    unknown_lane_skills = sorted(lane_covered - locked_skills)
    missing_lane_skills = sorted(locked_skills - lane_covered)
    if unknown_lane_skills or missing_lane_skills:
        raise ValueError(
            "Lane skill curation mismatch: "
            f"unknown={unknown_lane_skills} missing={missing_lane_skills}"
        )

    agent_covered: set[str] = set()
    for role_skills in agent_skills.values():
        agent_covered.update(role_skills)
    unknown_agent_skills = sorted(agent_covered - locked_skills)
    missing_agent_skills = sorted(locked_skills - agent_covered)
    if unknown_agent_skills or missing_agent_skills:
        raise ValueError(
            "Agent skill curation mismatch: "
            f"unknown={unknown_agent_skills} missing={missing_agent_skills}"
        )

    all_portals = [*HOSTED_PORTALS, *COMPANION_PORTALS]
    mcp_configs = {
        ".mcp.json": sorted(root_mcp["mcpServers"].keys()),
        ".claude/settings.json": sorted(claude_cfg["mcpServers"].keys()),
        ".codex/config.toml": sorted(codex_cfg["mcp_servers"].keys()),
        ".gemini/settings.json": sorted(gemini_cfg["mcpServers"].keys()),
        "opencode.json": sorted(opencode_cfg["mcp"].keys()),
    }

    return {
        "schema_version": "tier0.llm-harness-manifest.v1",
        "repo_name": "noesis",
        "future_flagship_name": "noesis",
        "branch": _detect_branch(repo_root),
        "ssot_entrypoints": {
            "canonical_agents_md": "AGENTS.md",
            "claude_md_pointer": claude_md_text,
            "gemini_context_file_names": list(gemini_cfg.get("context", {}).get("fileName", [])),
            "opencode_instructions": list(opencode_cfg.get("instructions", [])),
            "orca_config": "orca.yaml",
            "philosophy_manifest": "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
            "architecture_manifest": "ARCHITECTURE_MANIFEST.json",
            "sota_starred_manifest": "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
        },
        "supported_runtimes": [
            "Codex",
            "Claude Code",
            "Gemini CLI",
            "OpenCode",
            "Antigravity",
            "Orca",
        ],
        "source_of_truth_hierarchy": phil_doc["source_of_truth_hierarchy"],
        "meta_convergence_practice": upstream_cfg.get("meta_convergence_practice", {}),
        "zero_self_invented_hooks": zero_hooks,
        "orca_practice": {
            "config_file": "orca.yaml",
            "workspaces_dir_env": "ORCA_WORKSPACES_DIR",
            "workspaces_dir": "Z:\\workspaces",
            "setup_agent_startup_policy": orca_cfg["setup_agent_startup_policy"],
            "setup_script": orca_cfg["setup_script"],
            "setup_script_path": "scripts/bootstrap_env.sh",
            "official_upstream_orca_hooks_only": True,
            "zero_self_invented_hooks": zero_hooks,
            "hooks_policy": (
                "Preserve only official upstream Orca status hooks "
                "(~/.orca/agent-hooks/ -> orca-agent-hook.exe); zero custom repo runtime hooks."
            ),
            "governance_spine_agent": "tier0-grand-central",
            "governance_spine_skills": governance_spine_skills,
            "cli_verification_commands": [
                "orca status --json",
                "orca vm env doctor --json",
                "orca skills list",
            ],
        },
        "agent_roles": agent_roles,
        "agent_skills": agent_skills,
        "compounding_lanes": lane_specs,
        "mcp_servers": {
            "transport": "native-uv-stdio",
            "canonical_servers": root_mcp["mcpServers"],
            "parity_configs": mcp_configs,
            "parity_count": f"{len(mcp_configs)}/{len(mcp_configs)}",
        },
        "skills_summary": {
            "lock_file": "UPSTREAM_SKILLS_LOCK.json",
            "skills_dir": ".claude/skills",
            "skills_count": int(skills_lock["skills_count"]),
            "lane_coverage": f"{len(lane_covered)}/{len(locked_skills)}",
            "agent_coverage": f"{len(agent_covered)}/{len(locked_skills)}",
            "unmapped_skills": missing_lane_skills,
            "by_upstream_source": dict(sorted(skills_by_source.items())),
        },
        "auth_and_routing_policy": {
            "api_key_providers_allowed": ["anthropic", "openai"],
            "local_automation_router": {
                "name": "omniroute",
                "base_url": "http://127.0.0.1:20128",
                "health_endpoint": "http://127.0.0.1:20128/api/health",
                "oauth_auto_fallback": True,
                "windows_autostart": (
                    "omniroute autostart enable --no-tray (Startup OmniRoute.vbs) + Keep-Up"
                ),
            },
            "claude_code_interactive_auth": {
                "mode": "native-oauth-only",
                "available_models_restricted": False,
                "supported_models": ["claude-fable-5-2", "opus", "sonnet", "haiku", "opusplan"],
                "zero_global_api_key_override": True,
            },
            "orca_multi_account_failover": {
                "helper_script": "scripts/orca_account_failover.py",
                "just_commands": ["just account-status", "just account-switch"],
                "uses_official_cli_only": ["orca account list --json", "orca account select"],
                "switch_on_rate_limit_or_exhaustion": True,
            },
            "anthropic_banked_keys_policy": {
                "usage_tier": "high-quality-sota-tasks-only",
                "vault_schema": "sota-anthropic-keybank.v1",
                "ci_dispatch_provider": "anthropic-banked-sota",
                "never_override_interactive_cli": True,
            },
            "github_actions_workflow": ".github/workflows/multi-model-sota.yml",
        },
        "live_hosted_portals": all_portals,
        "live_hosted_portals_count": len(all_portals),
        "upstream_verification_commands": {
            "full_local_check": "just check",
            "strict_docs_build": "just docs-build",
            "pre_commit_hooks": "prek run --all-files",
            "manifest_sync_check": "uv run --no-sync python scripts/sync_manifest.py --check",
            "workflow_audit": "zizmor --offline .github/workflows",
            "supply_chain_scan": (
                "trivy fs --scanners misconfig,secret --severity HIGH,CRITICAL "
                "--exit-code 1 --skip-dirs .venv,.git,site,.claude/skills ."
            ),
            "secret_scan": (
                "gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner"
            ),
            "windows_ci_gate": "pwsh -NoProfile -File scripts/check.ps1",
            "wsl2_ci_gate": "bash scripts/check.sh",
            "grand_reset_verify": (
                "pwsh -NoProfile -File scripts/grand_reset/verify_grand_reset.ps1"
            ),
        },
    }


def render_manifest_json(manifest: dict[str, Any]) -> str:
    """Serialize manifest deterministically with trailing LF."""
    return json.dumps(manifest, indent=2) + "\n"


def _sync_single_manifest(path: Path, expected_text: str, *, write: bool) -> bool:
    if write:
        path.write_text(expected_text, encoding="utf-8", newline="\n")
        print(f"SYNC_MANIFEST_WRITE_OK=1 path={path.name}")
        return True
    if not path.is_file():
        print(f"SYNC_MANIFEST_DRIFT: missing {path}", file=sys.stderr)
        return False
    actual_text = path.read_text(encoding="utf-8")
    if actual_text != expected_text:
        diff = "".join(
            difflib.unified_diff(
                actual_text.splitlines(keepends=True),
                expected_text.splitlines(keepends=True),
                fromfile=f"{path.name} (actual)",
                tofile=f"{path.name} (expected)",
            )
        )
        print(diff, file=sys.stderr)
        print(
            f"SYNC_MANIFEST_DRIFT ({path.name}): "
            "run `uv run python scripts/sync_manifest.py --write`",
            file=sys.stderr,
        )
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint supporting --write and --check modes for SSOT manifests."""
    parser = argparse.ArgumentParser(
        description="Programmatic SSOT sync for ARCHITECTURE & LLM_HARNESS manifests"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="Regenerate SSOT JSON manifests")
    mode.add_argument("--check", action="store_true", help="Verify SSOT JSON manifests match repo")
    args = parser.parse_args(argv)

    targets = [
        (MANIFEST_PATH, render_manifest_json(build_manifest(REPO_ROOT))),
        (LLM_HARNESS_MANIFEST_PATH, render_manifest_json(build_llm_harness_manifest(REPO_ROOT))),
    ]
    all_ok = True
    for target_path, expected_text in targets:
        if not _sync_single_manifest(target_path, expected_text, write=args.write):
            all_ok = False
    if not all_ok:
        return 1
    if args.check:
        print("SYNC_MANIFEST_CHECK_OK=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

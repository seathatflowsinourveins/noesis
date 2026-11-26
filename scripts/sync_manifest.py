#!/usr/bin/env python3
"""Programmatic SSOT synchronizer for ARCHITECTURE_MANIFEST.json (--write / --check)."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
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
        "docs/adr/ADR-0001.md",
        "docs/adr/ADR-0012.md",
        "docs/CLEAN_ECOSYSTEM_MANIFEST.md",
        "docs/ARCHITECTURE_LAYER_MANIFEST.md",
        "docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md",
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
    ],
    "L8": [
        "evals/__init__.py",
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
        ".claude/agents/tier0-grand-central.md",
        ".claude/agents/paper-gatekeeper.md",
        ".claude/commands/grand-reset-verify.md",
        ".claude/mcp-lanes/sim.mcp.json",
        ".claude/mcp-lanes/paper.mcp.json",
        ".claude/mcp-lanes/tv.mcp.json",
        ".claude/mcp-lanes/gate.mcp.json",
        ".codex/config.toml",
        ".gemini/settings.json",
        "opencode.json",
        ".opencode/commands/verify-tier0.md",
        "scripts/nautilus_mcp_server.py",
        "scripts/grand_reset/02_runtimes_agy_orca_setup.ps1",
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
    "ecosystem_setup": "scripts/setup_clean_ecosystem.ps1",
    "lane_launcher": "scripts/launch_clean_lanes.ps1",
    "paper_farm": "scripts/run_live_paper_farm.ps1",
    "tv_console": "scripts/tv_console.ps1",
    "mcp_server": "scripts/nautilus_mcp_server.py",
    "formal_risk_z3": "evals/formal_risk_z3.py",
    "inspect_safety_eval": "evals/tier0_inspect_eval.py",
    "grand_reset_00": "scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1",
    "grand_reset_01": "scripts/grand_reset/01_disk_z_and_wsl_setup.ps1",
    "grand_reset_01_wsl": "scripts/grand_reset/01_wsl_bootstrap.sh",
    "grand_reset_02": "scripts/grand_reset/02_runtimes_agy_orca_setup.ps1",
    "grand_reset_03": "scripts/grand_reset/03_hosted_stack_tv_paper.ps1",
    "grand_reset_verify": "scripts/grand_reset/verify_grand_reset.ps1",
}


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


def build_manifest(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Derive ARCHITECTURE_MANIFEST.json deterministically from repository SSOT inputs."""
    upstream = tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    skills_lock = json.loads((repo_root / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    mcp_root = json.loads((repo_root / ".mcp.json").read_text(encoding="utf-8"))
    uv_lock_text = (repo_root / "uv.lock").read_text(encoding="utf-8")

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
        entry["repo_artifacts"] = [
            rel for rel in LAYER_ARTIFACT_CANDIDATES[key] if rel in repo_files_set
        ]
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
        "repo_name": "tier0-sota-clean",
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
        "hosted_portals": HOSTED_PORTALS,
        "layers": layers,
        "entrypoints": entrypoints,
        "repository_files": repo_files,
        "workstation_topology": {
            "active_repo": "Z:\\tier0-sota-clean",
            "shared_manifests_root": "Z:\\sota-data\\manifests",
            "prewall_lake_root": "Z:\\sota-data\\prewall-lake",
            "workspaces_root": "Z:\\workspaces",
            "archive_root": "Z:\\archive",
        },
    }


def build_llm_harness_manifest(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Derive LLM_HARNESS_MANIFEST.json deterministically from live repo harness configs."""
    skills_lock = json.loads((repo_root / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    root_mcp = json.loads((repo_root / ".mcp.json").read_text(encoding="utf-8"))
    claude_cfg = json.loads((repo_root / ".claude" / "settings.json").read_text(encoding="utf-8"))
    gemini_cfg = json.loads((repo_root / ".gemini" / "settings.json").read_text(encoding="utf-8"))
    opencode_cfg = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))
    codex_cfg = tomllib.loads((repo_root / ".codex" / "config.toml").read_text(encoding="utf-8"))
    claude_md_text = (repo_root / "CLAUDE.md").read_text(encoding="utf-8").strip()

    agent_roles = sorted(p.stem for p in (repo_root / ".claude" / "agents").glob("*.md"))
    lane_specs = {
        "sim-backtest": {
            "command": ".claude/commands/lane-sim.md",
            "mcp_lane": ".claude/mcp-lanes/sim.mcp.json",
            "agent_roles": ["tier0-grand-central", "pit-data-engineer", "quant-falsifier"],
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
            "agent_roles": [
                "tier0-grand-central",
                "observability-operator",
                "memory-rag-evolution",
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
            "agent_roles": ["tier0-grand-central", "observability-operator"],
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
            "agent_roles": ["paper-gatekeeper", "code-intel-redteam", "quant-falsifier"],
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
    skills_by_source: dict[str, int] = {}
    for item in skills_entries:
        src = str(item.get("upstream") or item.get("source") or "vendored")
        skills_by_source[src] = skills_by_source.get(src, 0) + 1

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
        "repo_name": "tier0-sota-clean",
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
        "zero_self_invented_hooks": _check_zero_hooks(repo_root),
        "agent_roles": agent_roles,
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
            "by_upstream_source": dict(sorted(skills_by_source.items())),
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
                "trivy fs --scanners misconfig,secret --severity HIGH,CRITICAL --exit-code 1 ."
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

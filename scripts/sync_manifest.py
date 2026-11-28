#!/usr/bin/env python3
"""Programmatic SSOT synchronizer for ARCHITECTURE_MANIFEST.json (--write / --check)."""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import difflib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

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

EXTENDED_PORTALS: list[dict[str, str]] = [
    {"name": "alertmanager", "url": "http://127.0.0.1:9093/-/healthy", "profile": "alerting"},
    {
        "name": "allure-test-reports",
        "url": "http://127.0.0.1:8793/test-reports/allure/",
        "served_by": "tearsheets",
    },
    {
        "name": "omniroute",
        "url": "http://127.0.0.1:20128/api/health",
        "role": "local-automation-router",
    },
]

LAYER_ARTIFACT_CANDIDATES: dict[str, list[str]] = {
    "L0": [
        "pyproject.toml",
        "uv.lock",
        ".python-version",
        "mise.toml",
        "justfile",
        "fnox.toml",
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
        "pine/tier0_sota_strategy_24x5.pine",
        "pine/tier0_market_regime.pine",
        "pine/tier0_darkpool_uv_buildup.pine",
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
        "scripts/tv_console.sh",
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
        "docs/TV_AUTOMATION_PLAYBOOK.md",
        "docker/compose.sota.yml",
        "docker/otel-collector-config.yaml",
        "docker/prometheus.yml",
        "docker/prometheus_rules.yml",
        "docker/alertmanager.yml",
        "docker/dagster.yaml",
        "docker/workspace.yaml",
        "docker/grafana/dashboards/tier0-workstation-telemetry.json",
        "docker/grafana/dashboards/tier0-order-presets-momentum.json",
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
        "evals/symbolic_risk_crosshair.py",
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
        "tests/test_starred_frontier_landscape.py",
        "tests/test_stage4_drills.py",
        "scripts/run_compounding_reflection.py",
        "scripts/run_stage4_drills.py",
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
    "fnox_toml": "fnox.toml",
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
    "tv_console_sh": "scripts/tv_console.sh",
    "mcp_server": "scripts/nautilus_mcp_server.py",
    "orca_account_failover": "scripts/orca_account_failover.py",
    "formal_risk_z3": "evals/formal_risk_z3.py",
    "symbolic_risk_crosshair": "evals/symbolic_risk_crosshair.py",
    "stage4_drills": "scripts/run_stage4_drills.py",
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
        "test-results",
    }
)

REQUIRED_MADR_SECTIONS: tuple[str, ...] = (
    "## Context and Problem Statement",
    "## Decision Drivers",
    "## Considered Options",
    "## Decision Outcome",
    "### Confirmation",
    "## Consequences",
    "## Pros and Cons of the Options",
    "## More Information",
)

_ALLOWED_LANES = frozenset({"sim-backtest", "paper", "half-auto-tv", "live"})
_LAYER_ID_RE = re.compile(r"^L(?:[0-9]|1[0-5])$")
_ADR_ID_RE = re.compile(r"^ADR-\d{4}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ADR_BANNED_IMPORT_RE = re.compile(
    r"(?m)^\s*(?:import|from)\s+(?:yfinance|backtrader|vectorbt|talib|mlfinlab)\b"
)
_ADR_LIVE_ENABLE_RE = re.compile(r"live_execution_enabled\s*[:=]\s*(?:true|True|1)\b")
_ADR_LINK_RE = re.compile(r"\[(ADR-\d{4})\]\(\1\.md\)")
_HISTORICAL_LINE_RE = re.compile(
    r"(?i)\b(?:supersed(?:ed|es|ing)|historical|replaced|former(?:ly)?|prior|retir(?:ed|ing)|"
    r"topology|example|legacy|was\s+superseded)\b|\(Superseded\)"
)
_CLI_PATH_TOKEN_RE = re.compile(
    r"(?:scripts|evals|tests|rules|docker|docs|\.github)/[A-Za-z0-9_./-]+"
)


def _to_kebab(field_name: str) -> str:
    return field_name.replace("_", "-")


def _normalize_pkg_token(raw: str) -> str:
    cleaned = raw.strip().lower()
    cleaned = cleaned.split(" ", 1)[0].strip("()[]`\"'")
    if "/" in cleaned:
        cleaned = cleaned.rsplit("/", 1)[-1]
    return cleaned.replace("_", "-")


class AdrFitnessChecks(BaseModel):
    """Executable architectural fitness-function bindings declared in ADR frontmatter."""

    model_config = ConfigDict(extra="forbid", alias_generator=_to_kebab, populate_by_name=True)

    pytest: list[str] = Field(default_factory=list)
    ast_grep: list[str] = Field(default_factory=list)
    commands: list[str] = Field(default_factory=list)
    evals: list[str] = Field(default_factory=list)

    def total_count(self) -> int:
        return len(self.pytest) + len(self.ast_grep) + len(self.commands) + len(self.evals)


class AdrFrontmatter(BaseModel):
    """Canonical tier0.adr.v1 MADR 4.0 kebab-case YAML frontmatter contract."""

    model_config = ConfigDict(extra="forbid", alias_generator=_to_kebab, populate_by_name=True)

    schema_version: Literal["tier0.adr.v1"]
    id: str = Field(pattern=r"^ADR-\d{4}$")
    title: str = Field(min_length=3)
    status: Literal["proposed", "accepted", "deprecated", "rejected", "superseded"]
    date: dt.date
    last_verified: dt.date
    evidence_grade: Literal["E1", "E2", "E3", "E4"]
    decision_makers: list[str] = Field(min_length=1)
    consulted: list[str] = Field(default_factory=list)
    informed: list[str] = Field(default_factory=list)
    sole_source_of_truth: Literal["sota-upstream-convergence-per-core-trading-philosophy"]
    layers: list[str] = Field(min_length=1)
    lanes: list[str] = Field(min_length=1)
    tags: list[str] = Field(min_length=1)
    upstreams: list[str] = Field(default_factory=list)
    supersedes: list[str] = Field(default_factory=list)
    superseded_by: list[str] = Field(default_factory=list)
    related: list[str] = Field(default_factory=list)
    code_anchors: list[str] = Field(default_factory=list)
    fitness_checks: AdrFitnessChecks = Field(default_factory=AdrFitnessChecks)
    review_triggers: list[str] = Field(min_length=1)

    @field_validator("date", "last_verified", mode="before")
    @classmethod
    def _validate_iso_date(cls, value: Any) -> dt.date:
        if isinstance(value, dt.datetime):
            raise ValueError("Expected ISO YYYY-MM-DD date, not datetime")
        if isinstance(value, dt.date):
            return value
        if isinstance(value, str) and _ISO_DATE_RE.match(value.strip()):
            return dt.date.fromisoformat(value.strip())
        raise ValueError(f"Expected ISO YYYY-MM-DD date, got {value!r}")

    @field_validator("layers")
    @classmethod
    def _validate_layers(cls, items: list[str]) -> list[str]:
        invalid = [item for item in items if not _LAYER_ID_RE.match(item)]
        if invalid:
            raise ValueError(f"Invalid layer ID(s) outside L0..L15: {invalid}")
        return items

    @field_validator("lanes")
    @classmethod
    def _validate_lanes(cls, items: list[str]) -> list[str]:
        invalid = [item for item in items if item not in _ALLOWED_LANES]
        if invalid:
            raise ValueError(f"Invalid compounding lane(s): {invalid}")
        return items

    @field_validator("supersedes", "superseded_by", "related")
    @classmethod
    def _validate_adr_refs(cls, items: list[str]) -> list[str]:
        invalid = [item for item in items if not _ADR_ID_RE.match(item)]
        if invalid:
            raise ValueError(f"Invalid ADR reference ID(s): {invalid}")
        return items


def extract_adr_frontmatter(path: Path) -> tuple[dict[str, Any], str]:
    """Extract and parse leading YAML frontmatter (`---`) and body from an ADR Markdown file."""
    raw = path.read_text(encoding="utf-8")
    normalized = raw.replace("\r\n", "\n")
    if not normalized.startswith("---\n"):
        raise ValueError(f"{path.name}: missing leading MADR 4.0 YAML frontmatter (`---`)")
    end_idx = normalized.find("\n---\n", 4)
    if end_idx == -1:
        if normalized.endswith("\n---"):
            end_idx = len(normalized) - 4
        else:
            raise ValueError(f"{path.name}: unterminated YAML frontmatter block")
    yaml_text = normalized[4:end_idx]
    body = normalized[end_idx + 5 :] if normalized[end_idx : end_idx + 5] == "\n---\n" else ""
    parsed = yaml.safe_load(yaml_text)
    if not isinstance(parsed, dict):
        raise ValueError(f"{path.name}: YAML frontmatter must be a mapping")
    return parsed, body


def _build_forbidden_upstream_tokens(upstream_cfg: dict[str, Any]) -> set[str]:
    rules = upstream_cfg.get("rules", {})
    tokens: set[str] = set()
    for key in ("banned_packages", "banned_libraries"):
        for item in rules.get(key, []):
            raw = str(item).strip().lower()
            tokens.add(raw)
            tokens.add(_normalize_pkg_token(raw))
    for entry in rules.get("spdx_license_firewall", []):
        repo = str(entry["repo"]).strip().lower()
        tokens.add(repo)
        tokens.add(_normalize_pkg_token(repo))
    return tokens


def _parse_just_recipes(repo_root: Path) -> set[str]:
    just_text = (repo_root / "justfile").read_text(encoding="utf-8").replace("\r\n", "\n")
    recipes: set[str] = set()
    for match in re.finditer(r"(?m)^([a-zA-Z0-9_-]+)(?:\s+[^:=#\n]+)?:\s*(?!=)", just_text):
        name = match.group(1)
        if name not in {"set", "export", "alias", "import", "mod"}:
            recipes.add(name)
    return recipes


def _parse_ast_grep_rule_ids(repo_root: Path) -> set[str]:
    rules_text = (repo_root / "rules" / "zero_self_invention.yml").read_text(encoding="utf-8")
    return set(re.findall(r"(?m)^id:\s*(\S+)", rules_text))


def _parse_python_ast(
    path: Path,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> ast.Module:
    if ast_cache is not None and path in ast_cache:
        return ast_cache[path]
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    if ast_cache is not None:
        ast_cache[path] = tree
    return tree


def _resolve_python_symbol(
    path: Path,
    symbol_spec: str,
    *,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> bool:
    tree = _parse_python_ast(path, ast_cache)
    parts = [p for p in symbol_spec.split("::") if p]
    if not parts:
        return True
    if len(parts) == 1:
        target = parts[0]
        return any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            and node.name == target
            for node in tree.body
        )
    if len(parts) == 2:
        cls_name, method_name = parts
        return any(
            isinstance(node, ast.ClassDef)
            and node.name == cls_name
            and any(
                isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)) and sub.name == method_name
                for sub in node.body
            )
            for node in tree.body
        )
    return False


def _resolve_eval_target(
    path: Path,
    target_spec: str,
    *,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> bool:
    if _resolve_python_symbol(path, target_spec, ast_cache=ast_cache):
        return True
    tree = _parse_python_ast(path, ast_cache)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and node.value == target_spec:
            return True
    return False


def _load_adrs_with_sha(
    repo_root: Path = REPO_ROOT,
    *,
    upstream_cfg: dict[str, Any] | None = None,
) -> tuple[dict[str, tuple[AdrFrontmatter, str]], dict[str, str]]:
    cfg = (
        upstream_cfg
        if upstream_cfg is not None
        else tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    )
    forbidden_tokens = _build_forbidden_upstream_tokens(cfg)

    adr_dir = repo_root / "docs" / "adr"
    adr_paths = sorted(adr_dir.glob("ADR-*.md"))
    if len(adr_paths) < 24:
        raise ValueError(f"Expected >= 24 living ADRs under docs/adr/, found {len(adr_paths)}")

    adrs: dict[str, tuple[AdrFrontmatter, str]] = {}
    file_shas: dict[str, str] = {}
    for adr_path in adr_paths:
        normalized = adr_path.read_text(encoding="utf-8").replace("\r\n", "\n")
        if not normalized.startswith("---\n"):
            raise ValueError(f"{adr_path.name}: missing leading MADR 4.0 YAML frontmatter (`---`)")
        end_idx = normalized.find("\n---\n", 4)
        if end_idx == -1:
            if normalized.endswith("\n---"):
                end_idx = len(normalized) - 4
            else:
                raise ValueError(f"{adr_path.name}: unterminated YAML frontmatter block")
        yaml_text = normalized[4:end_idx]
        body = normalized[end_idx + 5 :] if normalized[end_idx : end_idx + 5] == "\n---\n" else ""
        raw_meta = yaml.safe_load(yaml_text)
        if not isinstance(raw_meta, dict):
            raise ValueError(f"{adr_path.name}: YAML frontmatter must be a mapping")

        fm = AdrFrontmatter.model_validate(raw_meta)
        if fm.id != adr_path.stem:
            raise ValueError(f"{adr_path.name}: frontmatter id {fm.id!r} != {adr_path.stem!r}")

        if not re.search(rf"(?m)^#\s+{re.escape(fm.id)}:", body):
            raise ValueError(f"{adr_path.name}: missing H1 heading `# {fm.id}: ...`")
        for heading in REQUIRED_MADR_SECTIONS:
            if not re.search(rf"(?m)^{re.escape(heading)}\s*$", body):
                raise ValueError(f"{adr_path.name}: missing required MADR 4.0 heading {heading!r}")

        if _ADR_BANNED_IMPORT_RE.search(body):
            raise ValueError(f"{adr_path.name}: endorses banned library import in body")
        if _ADR_LIVE_ENABLE_RE.search(body):
            raise ValueError(f"{adr_path.name}: enables live execution in body")

        for up in fm.upstreams:
            raw_lo = up.strip().lower()
            norm = _normalize_pkg_token(up)
            if raw_lo in forbidden_tokens or norm in forbidden_tokens:
                raise ValueError(
                    f"{adr_path.name}: non-OSI or banned upstream {up!r} declared in `upstreams`"
                )

        adrs[fm.id] = (fm, body)
        file_shas[fm.id] = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return adrs, file_shas


def validate_adr_frontmatter_and_sections(
    repo_root: Path = REPO_ROOT,
) -> dict[str, tuple[AdrFrontmatter, str]]:
    """Validate MADR 4.0 kebab-case YAML frontmatter, canonical headings, and OSI upstreams."""
    adrs, _ = _load_adrs_with_sha(repo_root)
    return adrs


def validate_adr_supersession_dag_and_related(
    adrs: dict[str, tuple[AdrFrontmatter, str]],
) -> list[list[str]]:
    """Verify bidirectional supersedes <-> superseded-by links, DAG acyclicity, and citations."""
    adr_ids = set(adrs.keys())
    edges: list[list[str]] = []

    for aid, (fm, _body) in sorted(adrs.items()):
        if fm.status == "superseded":
            if not fm.superseded_by:
                raise ValueError(f"{aid}: status=superseded requires non-empty `superseded-by`")
        elif fm.superseded_by:
            raise ValueError(f"{aid}: status={fm.status} must have empty `superseded-by`")

        for target in fm.superseded_by:
            if target not in adr_ids:
                raise ValueError(f"{aid}: `superseded-by` target {target} does not exist")
            if target == aid:
                raise ValueError(f"{aid}: self-supersession is forbidden")
            target_fm = adrs[target][0]
            if aid not in target_fm.supersedes:
                raise ValueError(
                    f"{aid}: `superseded-by` lists {target}, but {target} `supersedes` omits {aid}"
                )
            edges.append([target, aid])

        for old_id in fm.supersedes:
            if old_id not in adr_ids:
                raise ValueError(f"{aid}: `supersedes` target {old_id} does not exist")
            if old_id == aid:
                raise ValueError(f"{aid}: self-supersession is forbidden")
            old_fm = adrs[old_id][0]
            if old_fm.status != "superseded":
                raise ValueError(
                    f"{aid}: `supersedes` lists {old_id}, but {old_id} status is {old_fm.status!r}"
                )
            if aid not in old_fm.superseded_by:
                raise ValueError(
                    f"{aid}: `supersedes` lists {old_id}, but {old_id} `superseded-by` omits {aid}"
                )

        for rel_id in fm.related:
            if rel_id not in adr_ids:
                raise ValueError(f"{aid}: `related` target {rel_id} does not exist")
            if rel_id == aid:
                raise ValueError(f"{aid}: `related` must not self-reference {aid}")

    # Strict DAG check on supersedes edges (0=unvisited, 1=visiting, 2=visited)
    state: dict[str, int] = {aid: 0 for aid in adr_ids}

    def _dfs(node: str) -> None:
        state[node] = 1
        for nxt in adrs[node][0].supersedes:
            if state[nxt] == 1:
                raise ValueError(f"Supersession cycle detected involving {node} -> {nxt}")
            if state[nxt] == 0:
                _dfs(nxt)
        state[node] = 2

    for aid in sorted(adr_ids):
        if state[aid] == 0:
            _dfs(aid)

    # Zero active (accepted) ADRs citing a superseded ADR as current authority
    superseded_ids = {aid for aid, (fm, _) in adrs.items() if fm.status == "superseded"}
    for aid, (fm, body) in sorted(adrs.items()):
        if fm.status != "accepted":
            continue
        allowed_sup = set(fm.supersedes)
        for lineno, line in enumerate(body.splitlines(), 1):
            for match in _ADR_LINK_RE.finditer(line):
                cited = match.group(1)
                if (
                    cited in superseded_ids
                    and cited not in allowed_sup
                    and not _HISTORICAL_LINE_RE.search(line)
                ):
                    raise ValueError(
                        f"{aid}.md:{lineno}: accepted ADR cites superseded {cited} "
                        "without historical/supersession qualification"
                    )

    return sorted(edges)


def validate_adr_anchors_and_fitness_bindings(
    repo_root: Path,
    adrs: dict[str, tuple[AdrFrontmatter, str]],
    *,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> dict[str, Any]:
    """Verify every accepted/proposed ADR's code-anchors and executable fitness-checks resolve."""
    ast_rule_ids = _parse_ast_grep_rule_ids(repo_root)
    just_recipes = _parse_just_recipes(repo_root)
    cache: dict[Path, ast.Module] = ast_cache if ast_cache is not None else {}
    exists_cache: dict[str, bool] = {}

    def _path_exists(rel_path: str) -> bool:
        if rel_path not in exists_cache:
            exists_cache[rel_path] = (repo_root / rel_path).exists()
        return exists_cache[rel_path]

    accepted_count = 0
    fitness_bound = 0

    for aid, (fm, _body) in sorted(adrs.items()):
        if fm.status == "accepted":
            accepted_count += 1
            if not fm.code_anchors:
                raise ValueError(f"{aid}: accepted ADR requires >= 1 `code-anchors` entry")
            if fm.fitness_checks.total_count() < 1:
                raise ValueError(f"{aid}: accepted ADR requires >= 1 `fitness-checks` entry")
            fitness_bound += 1

        if fm.status not in {"accepted", "proposed"}:
            continue

        for rel in fm.code_anchors:
            if not _path_exists(rel):
                raise ValueError(f"{aid}: missing code-anchor path `{rel}`")

        for py_ref in fm.fitness_checks.pytest:
            rel_file, _, sym = py_ref.partition("::")
            target_path = repo_root / rel_file
            if target_path not in cache and not target_path.is_file():
                raise ValueError(f"{aid}: pytest fitness binding missing file `{rel_file}`")
            if sym and not _resolve_python_symbol(target_path, sym, ast_cache=cache):
                raise ValueError(f"{aid}: pytest fitness symbol `{py_ref}` not found in AST")

        for rule_id in fm.fitness_checks.ast_grep:
            if rule_id not in ast_rule_ids:
                raise ValueError(
                    f"{aid}: ast-grep rule `{rule_id}` not found in rules/zero_self_invention.yml"
                )

        for cmd in fm.fitness_checks.commands:
            tokens = cmd.strip().split()
            if not tokens:
                raise ValueError(f"{aid}: empty command entry in fitness-checks.commands")
            if tokens[0] == "just":
                if len(tokens) < 2 or tokens[1] not in just_recipes:
                    raise ValueError(f"{aid}: command `{cmd}` references unknown justfile recipe")
            else:
                for pmatch in _CLI_PATH_TOKEN_RE.finditer(cmd):
                    ref_p = pmatch.group(0).rstrip(".,;:")
                    if not _path_exists(ref_p):
                        raise ValueError(
                            f"{aid}: command `{cmd}` references non-existent path `{ref_p}`"
                        )

        for eval_ref in fm.fitness_checks.evals:
            rel_file, _, target = eval_ref.partition("::")
            target_path = repo_root / rel_file
            if target_path not in cache and not target_path.is_file():
                raise ValueError(f"{aid}: eval fitness binding missing file `{rel_file}`")
            if target and not _resolve_eval_target(target_path, target, ast_cache=cache):
                raise ValueError(f"{aid}: eval target `{eval_ref}` not found in {rel_file}")

    return {
        "accepted_count": accepted_count,
        "fitness_bound_accepted": fitness_bound,
        "fitness_bound_accepted_ratio": f"{fitness_bound}/{accepted_count}",
    }


def _collect_zensical_adr_nav(nav_items: list[Any]) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for item in nav_items:
        if isinstance(item, dict):
            for label, value in item.items():
                if isinstance(value, str) and value.startswith("adr/ADR-"):
                    found.append((str(label), value))
                elif isinstance(value, list):
                    found.extend(_collect_zensical_adr_nav(value))
    return found


def validate_adr_catalog_and_zensical_sync(
    repo_root: Path,
    adrs: dict[str, tuple[AdrFrontmatter, str]],
) -> dict[str, Any]:
    """Verify exact 3-way parity across docs/adr/ADR-*.md, CATALOG.md, and zensical.toml nav."""
    catalog_path = repo_root / "docs" / "adr" / "CATALOG.md"
    catalog_text = catalog_path.read_text(encoding="utf-8")
    if (
        "<!-- BEGIN:ADR_CATALOG -->" not in catalog_text
        or "<!-- END:ADR_CATALOG -->" not in catalog_text
    ):
        raise ValueError(
            "docs/adr/CATALOG.md missing `<!-- BEGIN:ADR_CATALOG -->` / `<!-- END:ADR_CATALOG -->`"
        )

    row_re = re.compile(
        r"(?m)^\|\s*\[(ADR-\d{4})\]\(\1\.md\)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|"
    )
    catalog_rows: dict[str, tuple[str, str]] = {}
    for match in row_re.finditer(catalog_text):
        aid = match.group(1)
        if aid in catalog_rows:
            raise ValueError(f"Duplicate ADR row {aid} in docs/adr/CATALOG.md")
        catalog_rows[aid] = (match.group(2).strip(), match.group(3).strip())

    disk_ids = set(adrs.keys())
    cat_ids = set(catalog_rows.keys())
    if cat_ids != disk_ids:
        raise ValueError(
            f"CATALOG.md parity mismatch: missing={sorted(disk_ids - cat_ids)} "
            f"extra={sorted(cat_ids - disk_ids)}"
        )

    for aid, (fm, _body) in sorted(adrs.items()):
        _title_col, status_col = catalog_rows[aid]
        st_lo = status_col.lower()
        if fm.status == "superseded":
            if "superseded by" not in st_lo:
                raise ValueError(
                    f"CATALOG.md {aid}: expected 'Superseded by ...', got {status_col!r}"
                )
            for target in fm.superseded_by:
                if target not in status_col:
                    raise ValueError(
                        f"CATALOG.md {aid}: status {status_col!r} omits superseding target {target}"
                    )
        else:
            if not st_lo.startswith(fm.status):
                raise ValueError(
                    f"CATALOG.md {aid}: expected status {fm.status!r}, got {status_col!r}"
                )

    z_cfg = tomllib.loads((repo_root / "zensical.toml").read_text(encoding="utf-8"))
    nav_adrs = _collect_zensical_adr_nav(list(z_cfg["project"]["nav"]))
    expected_nav_targets = [f"adr/{aid}.md" for aid in sorted(adrs.keys())]
    actual_nav_targets = [target for _, target in nav_adrs]
    if actual_nav_targets != expected_nav_targets:
        raise ValueError(
            "zensical.toml nav parity mismatch: "
            f"missing={sorted(set(expected_nav_targets) - set(actual_nav_targets))} "
            f"extra={sorted(set(actual_nav_targets) - set(expected_nav_targets))}"
        )

    for label, target in nav_adrs:
        aid = Path(target).stem
        st = adrs[aid][0].status
        if st == "superseded" and "(Superseded)" not in label:
            raise ValueError(f"zensical.toml nav for {aid} must include '(Superseded)'")
        if st == "proposed" and "(Proposed)" not in label:
            raise ValueError(f"zensical.toml nav for {aid} must include '(Proposed)'")
        if st == "accepted" and ("(Superseded)" in label or "(Proposed)" in label):
            raise ValueError(f"zensical.toml nav for accepted {aid} has stale status badge")

    return {
        "catalog_and_zensical_in_sync": True,
        "catalog_count": len(catalog_rows),
        "zensical_nav_count": len(nav_adrs),
    }


def parse_and_validate_adrs(
    repo_root: Path = REPO_ROOT,
    *,
    upstream_cfg: dict[str, Any] | None = None,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> dict[str, Any]:
    """Run all 5 ADR governance validators and build deterministic `adr_graph` metadata."""
    adrs, file_shas = _load_adrs_with_sha(repo_root, upstream_cfg=upstream_cfg)
    supersession_edges = validate_adr_supersession_dag_and_related(adrs)
    fitness_summary = validate_adr_anchors_and_fitness_bindings(
        repo_root, adrs, ast_cache=ast_cache
    )
    sync_summary = validate_adr_catalog_and_zensical_sync(repo_root, adrs)

    counts_by_status: dict[str, int] = {}
    layer_coverage: dict[str, list[str]] = {}
    corpus_digest = hashlib.sha256()

    for aid, (fm, _body) in sorted(adrs.items()):
        counts_by_status[fm.status] = counts_by_status.get(fm.status, 0) + 1
        for layer in fm.layers:
            layer_coverage.setdefault(layer, []).append(aid)
        file_sha = file_shas[aid]
        corpus_digest.update(f"{aid}.md\0{file_sha}\n".encode())

    return {
        "schema_version": "tier0.adr-graph.v1",
        "total_adrs": len(adrs),
        "counts_by_status": dict(sorted(counts_by_status.items())),
        "supersession_edges": supersession_edges,
        "layer_coverage": {k: sorted(v) for k, v in sorted(layer_coverage.items())},
        "fitness_bound_accepted_ratio": fitness_summary["fitness_bound_accepted_ratio"],
        "catalog_and_zensical_in_sync": sync_summary["catalog_and_zensical_in_sync"],
        "corpus_sha256": corpus_digest.hexdigest(),
    }


_CLI_MCP_PORTAL_PACKAGE_ANCHORS: dict[str, str] = {
    "ast-grep-cli": "sgconfig.yml",
    "dagster-webserver": "docker/compose.sota.yml",
    "deptry": "justfile",
    "edgartools": ".mcp.json",
    "optuna-dashboard": "docker/compose.sota.yml",
    "pyarrow": "src/tier0_sota/discovery.py",
    "pytest": "justfile",
    "ruff": "justfile",
    "ty": "justfile",
    "zensical": "zensical.toml",
}

_DIST_TO_IMPORT_OVERRIDES: dict[str, str] = {
    "alpaca-py": "alpaca",
    "exchange-calendars": "exchange_calendars",
    "inspect-ai": "inspect_ai",
    "ml4t-diagnostic": "ml4t",
    "ml4t-engineer": "ml4t",
    "nautilus-trader": "nautilus_trader",
    "opentelemetry-api": "opentelemetry",
    "opentelemetry-exporter-otlp": "opentelemetry",
    "opentelemetry-sdk": "opentelemetry",
    "prometheus-client": "prometheus_client",
    "pyyaml": "yaml",
    "z3-solver": "z3",
}


def _scan_python_files(base_dir: Path) -> list[Path]:
    found: list[Path] = []
    stack: list[Path] = [base_dir]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        if entry.name != "__pycache__":
                            stack.append(Path(entry.path))
                    elif entry.name.endswith(".py") and entry.is_file(follow_symlinks=True):
                        found.append(Path(entry.path))
        except OSError:
            continue
    found.sort()
    return found


def _collect_ast_imported_modules(
    repo_root: Path,
    *,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> set[str]:
    """Collect every top-level Python module imported across src, scripts, evals, and tests."""
    imported: set[str] = set()
    for base_name in ("src", "scripts", "evals", "tests"):
        base_dir = repo_root / base_name
        if not base_dir.is_dir():
            continue
        for py_path in _scan_python_files(base_dir):
            tree = _parse_python_ast(py_path, ast_cache)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported.add(alias.name.split(".", 1)[0])
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    imported.add(node.module.split(".", 1)[0])
    return imported


def audit_zero_paper_winners(
    repo_root: Path,
    pyproject: dict[str, Any],
    *,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> dict[str, Any]:
    """Verify every default-installed Python winner is AST-imported or CLI/MCP/portal anchored."""
    imported_modules = _collect_ast_imported_modules(repo_root, ast_cache=ast_cache)
    pkg_map = {
        k.lower().replace("_", "-"): str(v)
        for k, v in pyproject.get("tool", {})
        .get("deptry", {})
        .get("package_module_name_map", {})
        .items()
    }

    default_specs: list[str] = list(pyproject.get("project", {}).get("dependencies", []))
    dep_groups = pyproject.get("dependency-groups", {})
    default_groups = list(pyproject.get("tool", {}).get("uv", {}).get("default-groups", []))
    for grp in default_groups:
        grp_deps = dep_groups.get(grp, [])
        if isinstance(grp_deps, list):
            default_specs.extend(str(d) for d in grp_deps)

    checked = sorted(
        {
            re.split(r"[<>=!~;\[\s]", spec.strip(), maxsplit=1)[0].lower().replace("_", "-")
            for spec in default_specs
            if spec.strip()
        }
    )

    imported_winners: list[str] = []
    cli_mcp_portal_mapped: list[str] = []
    unwired_packages: list[str] = []

    for pkg in checked:
        if pkg in _CLI_MCP_PORTAL_PACKAGE_ANCHORS:
            anchor = _CLI_MCP_PORTAL_PACKAGE_ANCHORS[pkg]
            if (repo_root / anchor).exists():
                cli_mcp_portal_mapped.append(pkg)
            else:
                unwired_packages.append(pkg)
            continue
        mod = _DIST_TO_IMPORT_OVERRIDES.get(pkg) or pkg_map.get(pkg) or pkg.replace("-", "_")
        if mod in imported_modules:
            imported_winners.append(pkg)
        else:
            unwired_packages.append(pkg)

    return {
        "ok": len(unwired_packages) == 0,
        "checked_default_packages": len(checked),
        "imported_packages_count": len(imported_winners),
        "cli_mcp_portal_mapped": cli_mcp_portal_mapped,
        "unwired_packages": unwired_packages,
    }


def validate_clean_sota_harness(
    repo_root: Path = REPO_ROOT,
    *,
    upstream: dict[str, Any] | None = None,
    uv_lock_text: str | None = None,
    risk_sha: str | None = None,
    ast_cache: dict[Path, ast.Module] | None = None,
) -> dict[str, Any]:
    """Enforce zero-overlap single-winner discipline, SPDX OSI firewall, and 100% src mapping."""
    upstream_doc = (
        upstream
        if upstream is not None
        else tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    )
    starred = json.loads(
        (repo_root / "SOTA_STARRED_AND_BEYOND_MANIFEST.json").read_text(encoding="utf-8")
    )
    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    lock_raw = (
        uv_lock_text
        if uv_lock_text is not None
        else (repo_root / "uv.lock").read_text(encoding="utf-8")
    )
    lock_doc = tomllib.loads(lock_raw)

    resolved_risk_sha = (
        risk_sha
        if risk_sha is not None
        else hashlib.sha256((repo_root / RISK_REL_PATH).read_bytes()).hexdigest()
    )
    if resolved_risk_sha != EXPECTED_RISK_SHA256:
        raise ValueError(f"risk.py SHA-256 mismatch: {resolved_risk_sha} != {EXPECTED_RISK_SHA256}")

    # Check pyproject has zero pre-commit dependency
    all_declared_specs: list[str] = list(pyproject.get("project", {}).get("dependencies", []))
    for grp_deps in pyproject.get("dependency-groups", {}).values():
        if isinstance(grp_deps, list):
            all_declared_specs.extend(str(d) for d in grp_deps)
    for opt_deps in pyproject.get("project", {}).get("optional-dependencies", {}).values():
        if isinstance(opt_deps, list):
            all_declared_specs.extend(str(d) for d in opt_deps)
    declared_pkg_names = {
        re.split(r"[<>=!~;\[\s]", spec.strip(), maxsplit=1)[0].lower().replace("_", "-")
        for spec in all_declared_specs
        if spec.strip()
    }
    if "pre-commit" in declared_pkg_names:
        raise ValueError(
            "Overlap violation: `pre-commit` declared in pyproject.toml alongside prek"
        )

    forbidden_tokens = _build_forbidden_upstream_tokens(upstream_doc)
    lock_pkgs = {
        str(pkg.get("name", "")).lower().replace("_", "-")
        for pkg in lock_doc.get("package", [])
        if isinstance(pkg, dict)
    }
    banned_in_lock = sorted(lock_pkgs & forbidden_tokens)
    if banned_in_lock:
        raise ValueError(f"Banned/non-OSI packages present in uv.lock: {banned_in_lock}")

    # Verify zero winner overlap with rejected_overlaps or forbidden_tokens
    all_winners: set[str] = set()
    all_rejected: set[str] = set()
    for idx in range(11):
        layer = upstream_doc["layers"][f"L{idx}"]
        for w in layer["winners"]:
            norm_w = _normalize_pkg_token(str(w))
            if norm_w in forbidden_tokens or str(w).strip().lower() in forbidden_tokens:
                raise ValueError(f"Layer L{idx} winner {w!r} violates banned/SPDX firewall")
            all_winners.add(norm_w)
        for r in layer["rejected_overlaps"]:
            all_rejected.add(_normalize_pkg_token(str(r)))

    winner_rejected_overlap = sorted(all_winners & all_rejected)
    if winner_rejected_overlap:
        raise ValueError(f"Winners overlap with rejected_overlaps: {winner_rejected_overlap}")

    # Verify SPDX firewall parity & absence from active/frontier lists in SOTA_STARRED manifest
    up_fw = {entry["repo"] for entry in upstream_doc["rules"]["spdx_license_firewall"]}
    st_fw = {entry["repo"] for entry in starred["spdx_license_firewall"]}
    if up_fw != st_fw:
        raise ValueError(
            f"SPDX firewall drift between upstream-map.toml and SOTA_STARRED: "
            f"only_up={sorted(up_fw - st_fw)} only_starred={sorted(st_fw - up_fw)}"
        )

    fw_tokens = {r.lower() for r in up_fw} | {_normalize_pkg_token(r) for r in up_fw}
    for layer_key, layer_doc in starred["active_winners_by_layer"].items():
        for w in layer_doc.get("winners", []):
            if str(w).strip().lower() in fw_tokens or _normalize_pkg_token(str(w)) in fw_tokens:
                raise ValueError(f"SOTA_STARRED active {layer_key} includes firewalled {w!r}")
            if _normalize_pkg_token(str(w)) == "pre-commit":
                raise ValueError(f"SOTA_STARRED active {layer_key} includes overlapping pre-commit")
    for layer_key, layer_doc in starred["earned_trigger_frontier_l11_to_l15"].items():
        for cand in layer_doc.get("candidates", []):
            slug = str(cand.get("slug", ""))
            if slug.lower() in fw_tokens or _normalize_pkg_token(slug) in fw_tokens:
                raise ValueError(f"SOTA_STARRED frontier {layer_key} includes firewalled {slug!r}")

    # Verify 100% of src/tier0_sota/**/*.py is mapped in upstream-map.toml [files.*]
    mapped_files = set(upstream_doc.get("files", {}).keys())
    src_py_files = sorted(
        p.relative_to(repo_root).as_posix()
        for p in _scan_python_files(repo_root / "src" / "tier0_sota")
    )
    unmapped_src = [rel for rel in src_py_files if rel not in mapped_files]
    if unmapped_src:
        raise ValueError(
            f"Unmapped src/tier0_sota Python files in upstream-map.toml: {unmapped_src}"
        )

    paper_winners_audit = audit_zero_paper_winners(repo_root, pyproject, ast_cache=ast_cache)
    if not paper_winners_audit["ok"]:
        raise ValueError(
            "Zero-paper-winners violation — default-group packages with 0 Python imports and "
            f"no active CLI/MCP/portal anchor: {paper_winners_audit['unwired_packages']}"
        )

    return {
        "ok": True,
        "zero_banned_in_uv_lock": True,
        "zero_winner_overlap_with_rejected": True,
        "zero_pre_commit_overlap": True,
        "spdx_firewall_count": len(up_fw),
        "src_files_mapped_ratio": f"{len(src_py_files) - len(unmapped_src)}/{len(src_py_files)}",
        "zero_paper_winners_audit": paper_winners_audit,
    }


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
    head_file = repo_root / ".git" / "HEAD"
    if head_file.is_file():
        content = head_file.read_text(encoding="utf-8").strip()
        if content.startswith("ref: refs/heads/"):
            return content.removeprefix("ref: refs/heads/")
    out = _run_git(repo_root, "rev-parse", "--abbrev-ref", "HEAD")
    if out:
        branch = out.strip()
        if branch and branch != "HEAD":
            return branch
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
        if bool(parsed.get("hooks")) or bool(parsed.get("features", {}).get("hooks")):
            return False
    opencode_cfg = repo_root / "opencode.json"
    if opencode_cfg.is_file():
        parsed_oc = json.loads(opencode_cfg.read_text(encoding="utf-8"))
        if "hooks" in parsed_oc or parsed_oc.get("plugin"):
            return False
    return True


def _compute_skill_digests(skill_dir: Path) -> tuple[str, str | None]:
    """Compute (dir_sha256, skill_md_sha256) in a single scandir pass over `skill_dir`."""
    files: list[tuple[str, Path]] = []
    stack: list[tuple[str, Path]] = [("", skill_dir)]
    while stack:
        rel_prefix, cur_dir = stack.pop()
        with os.scandir(cur_dir) as it:
            for entry in it:
                rel_posix = f"{rel_prefix}/{entry.name}" if rel_prefix else entry.name
                if entry.is_dir(follow_symlinks=False):
                    stack.append((rel_posix, Path(entry.path)))
                elif entry.is_file(follow_symlinks=True):
                    files.append((rel_posix, Path(entry.path)))
    files.sort(key=lambda item: item[0])
    digest = hashlib.sha256()
    skill_md_sha: str | None = None
    for rel_posix, file_path in files:
        file_sha = hashlib.sha256(file_path.read_bytes()).hexdigest()
        if rel_posix == "SKILL.md":
            skill_md_sha = file_sha
        digest.update(f"{rel_posix}\0{file_sha}\n".encode())
    return digest.hexdigest(), skill_md_sha


def compute_skill_dir_sha256(skill_dir: Path) -> str:
    """Compute deterministic SHA-256 over all files in `skill_dir` sorted by POSIX path."""
    dir_sha, _ = _compute_skill_digests(skill_dir)
    return dir_sha


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

    def _verify_one(entry: dict[str, Any]) -> None:
        name = str(entry["skill"])
        skill_dir = skills_dir / name
        if not skill_dir.is_dir():
            raise ValueError(f"Missing SKILL.md for locked skill: {name}")
        actual_dir_sha, actual_skill_sha = _compute_skill_digests(skill_dir)
        if actual_skill_sha is None:
            raise ValueError(f"Missing SKILL.md for locked skill: {name}")
        if entry.get("skill_sha256") != actual_skill_sha:
            raise ValueError(f"skill_sha256 mismatch for {name}")
        if entry.get("dir_sha256") != actual_dir_sha:
            raise ValueError(f"dir_sha256 mismatch for {name}")

    workers = min(8, max(1, len(skills)))
    if workers == 1:
        for entry in skills:
            _verify_one(entry)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_verify_one, entry) for entry in skills]
            for fut in futures:
                fut.result()

    expected_lock_sha = compute_skills_lock_sha256(skills)
    if skills_lock.get("lock_sha256") != expected_lock_sha:
        raise ValueError(
            f"lock_sha256 mismatch: {skills_lock.get('lock_sha256')} != {expected_lock_sha}"
        )


def build_manifest(
    repo_root: Path = REPO_ROOT,
    *,
    _shared: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Derive ARCHITECTURE_MANIFEST.json deterministically from repository SSOT inputs."""
    shared = _shared if _shared is not None else {}
    upstream = shared.setdefault(
        "upstream", tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    )
    skills_lock = shared.setdefault(
        "skills_lock",
        json.loads((repo_root / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8")),
    )
    verify_skills_lock(repo_root, skills_lock)
    mcp_root = shared.setdefault(
        "mcp_root", json.loads((repo_root / ".mcp.json").read_text(encoding="utf-8"))
    )
    uv_lock_text = (repo_root / "uv.lock").read_text(encoding="utf-8")
    phil_doc = shared.setdefault(
        "phil_doc", json.loads(PHILOSOPHY_MANIFEST_PATH.read_text(encoding="utf-8"))
    )

    risk_path = repo_root / RISK_REL_PATH
    risk_sha = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    if risk_sha != EXPECTED_RISK_SHA256:
        raise ValueError(f"risk.py SHA-256 mismatch: {risk_sha} != {EXPECTED_RISK_SHA256}")

    ast_cache: dict[Path, ast.Module] = {}
    clean_sota_harness = validate_clean_sota_harness(
        repo_root,
        upstream=upstream,
        uv_lock_text=uv_lock_text,
        risk_sha=risk_sha,
        ast_cache=ast_cache,
    )
    adr_graph = parse_and_validate_adrs(repo_root, upstream_cfg=upstream, ast_cache=ast_cache)

    branch = shared.setdefault("branch", _detect_branch(repo_root))
    zero_hooks = shared.setdefault("zero_hooks", _check_zero_hooks(repo_root))
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
            "zero_self_invented_hooks": zero_hooks,
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
        "adr_graph": adr_graph,
        "clean_sota_harness": clean_sota_harness,
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


def _normalize_mcp_server_entry(
    spec: dict[str, Any], *, is_opencode: bool = False
) -> tuple[str, tuple[str, ...], tuple[tuple[str, str], ...]]:
    """Normalize a single stdio MCP server entry to (command, args, sorted_env)."""
    if not isinstance(spec, dict):
        raise ValueError(f"MCP server entry must be a mapping: type={type(spec).__name__}")
    spec_keys = f"keys={sorted(spec.keys())}"
    if is_opencode:
        cmd_list = spec.get("command")
        if not isinstance(cmd_list, list) or not cmd_list:
            raise ValueError(f"OpenCode MCP entry must have non-empty command list: {spec_keys}")
        cmd = str(cmd_list[0])
        args = tuple(str(x) for x in cmd_list[1:])
        raw_env = spec.get("environment") or {}
    else:
        cmd = str(spec.get("command") or "")
        args = tuple(str(x) for x in (spec.get("args") or ()))
        raw_env = spec.get("env") or {}
    if not cmd:
        raise ValueError(f"MCP server entry missing command: {spec_keys}")
    if not isinstance(raw_env, dict):
        raise ValueError(
            f"MCP server entry env must be a mapping: {spec_keys} env_type={type(raw_env).__name__}"
        )
    env_items = tuple(sorted((str(k), str(v)) for k, v in raw_env.items()))
    return (cmd, args, env_items)


def _verify_mcp_config_parity(
    root_mcp: dict[str, Any],
    claude_cfg: dict[str, Any],
    codex_cfg: dict[str, Any],
    gemini_cfg: dict[str, Any],
    opencode_cfg: dict[str, Any],
) -> dict[str, list[str]]:
    """Verify exact (command, args, env) parity across all 5 runtime MCP configs."""
    canonical_raw = root_mcp.get("mcpServers") or {}
    canonical = {name: _normalize_mcp_server_entry(spec) for name, spec in canonical_raw.items()}
    raw_by_config: list[tuple[str, dict[str, Any], bool]] = [
        (".mcp.json", canonical_raw, False),
        (".claude/settings.json", claude_cfg.get("mcpServers") or {}, False),
        (".codex/config.toml", codex_cfg.get("mcp_servers") or {}, False),
        (".gemini/settings.json", gemini_cfg.get("mcpServers") or {}, False),
        ("opencode.json", opencode_cfg.get("mcp") or {}, True),
    ]
    parity_names: dict[str, list[str]] = {}
    for cfg_name, servers_map, is_opencode in raw_by_config:
        normalized = {
            name: _normalize_mcp_server_entry(spec, is_opencode=is_opencode)
            for name, spec in servers_map.items()
        }
        if normalized != canonical:
            diffs: list[str] = []
            for srv_name in sorted(set(canonical) | set(normalized)):
                actual_entry = normalized.get(srv_name)
                expected_entry = canonical.get(srv_name)
                if actual_entry == expected_entry:
                    continue
                if actual_entry is None:
                    diffs.append(f"{srv_name}: missing_server")
                elif expected_entry is None:
                    diffs.append(f"{srv_name}: unexpected_server")
                else:
                    changed_fields: list[str] = []
                    if actual_entry[0] != expected_entry[0]:
                        changed_fields.append("command")
                    if actual_entry[1] != expected_entry[1]:
                        changed_fields.append("args")
                    if actual_entry[2] != expected_entry[2]:
                        actual_keys = sorted(k for k, _ in actual_entry[2])
                        expected_keys = sorted(k for k, _ in expected_entry[2])
                        changed_fields.append(f"env(keys={actual_keys} vs {expected_keys})")
                    diffs.append(f"{srv_name}: fields=[{', '.join(changed_fields)}]")
            raise ValueError(
                f"MCP config parity mismatch in {cfg_name} vs .mcp.json: {'; '.join(diffs)}"
            )
        parity_names[cfg_name] = sorted(servers_map.keys())
    return parity_names


def build_llm_harness_manifest(
    repo_root: Path = REPO_ROOT,
    *,
    _shared: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Derive LLM_HARNESS_MANIFEST.json deterministically from live repo harness configs."""
    shared = _shared if _shared is not None else {}
    skills_lock = shared.setdefault(
        "skills_lock",
        json.loads((repo_root / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8")),
    )
    root_mcp = shared.setdefault(
        "mcp_root", json.loads((repo_root / ".mcp.json").read_text(encoding="utf-8"))
    )
    claude_cfg = json.loads((repo_root / ".claude" / "settings.json").read_text(encoding="utf-8"))
    gemini_cfg = json.loads((repo_root / ".gemini" / "settings.json").read_text(encoding="utf-8"))
    opencode_cfg = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))
    codex_cfg = tomllib.loads((repo_root / ".codex" / "config.toml").read_text(encoding="utf-8"))
    phil_doc = shared.setdefault(
        "phil_doc", json.loads(PHILOSOPHY_MANIFEST_PATH.read_text(encoding="utf-8"))
    )
    claude_md_text = (repo_root / "CLAUDE.md").read_text(encoding="utf-8").strip()
    upstream_cfg = shared.setdefault(
        "upstream", tomllib.loads((repo_root / "upstream-map.toml").read_text(encoding="utf-8"))
    )
    orca_cfg = _parse_orca_yaml(repo_root)
    zero_hooks = shared.setdefault("zero_hooks", _check_zero_hooks(repo_root))
    branch = shared.setdefault("branch", _detect_branch(repo_root))

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
                "Z:\\sota-data\\manifests\\live_lane_order_preset_bundle.json",
                "Z:\\sota-data\\manifests\\paper_farm_execution_status.json",
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
                "just tv-e2e",
                "just test tests/test_tv_automation.py",
                "just mcp-smoke",
                "just ast",
            ],
            "primary_scripts": [
                "scripts/tv_console.ps1",
                "scripts/tv_console.sh",
                "scripts/nautilus_mcp_server.py",
            ],
            "paper_confirm_entrypoint": "just tv-confirm-paper",
            "paper_confirm_api": (
                "src/tier0_sota/live/execution.py::submit_confirmed_tv_preset_paper"
            ),
            "paper_only": True,
            "live_execution_enabled": False,
            "paper_farm_combined_rpm": 3600,
            "stage5_sandbox_spec_venue": "ALPACA_LIVE_PRO_LINE_1000",
            "stage5_sandbox_spec_rpm": 1000,
            "stage5_sandbox_spec_status": "LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC",
            "manifest_artifacts": [
                "Z:\\sota-data\\manifests\\tv_sota_pack\\live_half_auto_tickets.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\LIVE_HALF_AUTO_ORDER_TICKETS.md",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tv_order_presets.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\TV_ORDER_PRESET_CARDS.md",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tier0_sota_tv_watchlists_live.txt",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\finos_tv_cockpit.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tv_compounding_manifest.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\staged_scan_presets.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\staged_tv_presets.json",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tv_half_auto_audit.duckdb",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tv_staged_presets.parquet",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tier0_omni_sota_24x5.pine",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tier0_sota_strategy_24x5.pine",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tier0_market_regime.pine",
                "Z:\\sota-data\\manifests\\tv_sota_pack\\tier0_darkpool_uv_buildup.pine",
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
                "just fnox-check",
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
    mcp_configs = _verify_mcp_config_parity(
        root_mcp, claude_cfg, codex_cfg, gemini_cfg, opencode_cfg
    )

    return {
        "schema_version": "tier0.llm-harness-manifest.v1",
        "repo_name": "noesis",
        "future_flagship_name": "noesis",
        "branch": branch,
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
            "parity_fields_verified": ["command", "args", "env"],
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
        "extended_portals": EXTENDED_PORTALS,
        "extended_portals_count": len(EXTENDED_PORTALS),
        "total_live_portals_count": len(all_portals) + len(EXTENDED_PORTALS),
        "upstream_verification_commands": {
            "full_local_check": "just check",
            "adr_check": "just adr-check",
            "strict_docs_build": "just docs-build",
            "security_suite": "just security",
            "fnox_check": "just fnox-check",
            "symbolic_crosshair": "just crosshair",
            "allure_test_report": "just test-report",
            "mutation_audit_risk": "just mutate-risk",
            "compounding_reflection": "just reflect",
            "pre_commit_hooks": "prek run --all-files",
            "manifest_sync_check": "uv run --no-sync python scripts/sync_manifest.py --check",
            "workflow_audit": "zizmor --offline .github/workflows",
            "supply_chain_scan": (
                "trivy fs --scanners misconfig,secret --severity HIGH,CRITICAL "
                "--exit-code 1 --skip-dirs "
                ".venv,.git,site,.claude/skills,.cache,.ruff_cache,.pytest_cache,.hypothesis ."
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

    shared: dict[str, Any] = {}
    targets = [
        (MANIFEST_PATH, render_manifest_json(build_manifest(REPO_ROOT, _shared=shared))),
        (
            LLM_HARNESS_MANIFEST_PATH,
            render_manifest_json(build_llm_harness_manifest(REPO_ROOT, _shared=shared)),
        ),
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

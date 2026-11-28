from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tomllib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
EXPECTED_MCP_SERVERS = ["codebase-memory", "edgartools", "nautilus-engine", "xerj"]
LEGACY_PATTERN = re.compile(
    r"tier0-quant(?!-falsification)|CLEAN LIVE|sota-live-tv-harness|"
    r"sota_compounding_1000rpm_harness|RTX 4090"
)
NON_OSI_REPOS = {
    "treeverse/lakeFS",
    "restatedev/restate",
    "Arize-ai/phoenix",
    "NX-AI/tirex",
    "semgrep/semgrep",
    "peernagy/LOBS5",
    "PriorLabs/TabPFN",
}


def _load_manifest() -> dict[str, object]:
    raw = (REPO_ROOT / "ARCHITECTURE_MANIFEST.json").read_text(encoding="utf-8")
    data = json.loads(raw)
    assert isinstance(data, dict)
    return data


def test_architecture_manifest_schema_and_files_exist() -> None:
    manifest = _load_manifest()
    assert manifest["schema_version"] == "tier0.architecture-layer-manifest.v1"
    assert manifest["repo_name"] == "noesis"
    git = shutil.which("git")
    assert git is not None
    default_branch = subprocess.run(
        [
            git,
            "-c",
            f"safe.directory={REPO_ROOT.as_posix()}",
            "symbolic-ref",
            "--quiet",
            "refs/remotes/origin/HEAD",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    expected_branch = default_branch.stdout.strip().removeprefix("refs/remotes/origin/")
    assert manifest["branch"] == (expected_branch or "main")
    assert manifest["standalone_repo"] is True

    safety = manifest["safety_invariants"]
    assert isinstance(safety, dict)
    assert safety["live_execution_enabled"] is False
    assert safety["paper_only"] is True
    assert safety["zero_self_invented_hooks"] is True
    assert safety["zero_yfinance_in_uv_lock"] is True
    assert safety["zero_arbitrary_loc_caps"] is True
    assert "loc_enforcement" not in safety
    assert "advisory_loc_telemetry" not in manifest
    assert safety["forbidden_live_ports"] == [4001, 7496, 8798, 8796]
    hosted_portals = manifest["hosted_portals"]
    assert isinstance(hosted_portals, list)
    assert len(hosted_portals) == 15

    repo_files = manifest["repository_files"]
    assert isinstance(repo_files, list)
    assert len(repo_files) == len(set(repo_files))
    missing = [rel for rel in repo_files if not (REPO_ROOT / rel).is_file()]
    assert missing == [], f"Missing declared repository files: {missing}"


def test_risk_kernel_sha256_exact() -> None:
    manifest = _load_manifest()
    risk_meta = manifest["risk_kernel"]
    assert isinstance(risk_meta, dict)
    risk_path = REPO_ROOT / str(risk_meta["path"])
    assert risk_path.is_file()
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    assert digest == EXPECTED_RISK_SHA256
    assert risk_meta["sha256"] == EXPECTED_RISK_SHA256
    assert "loc" not in risk_meta


def test_layers_l0_to_l10_and_spdx_firewall() -> None:
    manifest = _load_manifest()
    upstream = tomllib.loads((REPO_ROOT / "upstream-map.toml").read_text(encoding="utf-8"))
    upstream_layers = upstream["layers"]
    manifest_layers = manifest["layers"]
    assert isinstance(manifest_layers, dict)

    expected_keys = [f"L{idx}" for idx in range(11)]
    assert list(manifest_layers.keys()) == expected_keys
    for key in expected_keys:
        assert key in upstream_layers
        assert manifest_layers[key]["name"] == upstream_layers[key]["name"]
        assert manifest_layers[key]["winners"] == upstream_layers[key]["winners"]
        assert (
            manifest_layers[key]["rejected_overlaps"] == upstream_layers[key]["rejected_overlaps"]
        )
        assert len(manifest_layers[key]["repo_artifacts"]) >= 1

    up_fw = {entry["repo"] for entry in upstream["rules"]["spdx_license_firewall"]}
    man_fw_raw = manifest["spdx_license_firewall"]
    assert isinstance(man_fw_raw, list)
    man_fw = {entry["repo"] for entry in man_fw_raw}
    assert up_fw == NON_OSI_REPOS
    assert man_fw == NON_OSI_REPOS

    for banned_loc_key in (
        "loc_enforcement",
        "advisory_production_loc",
        "advisory_risk_loc",
        "default_advisory_module_loc",
        "advisory_module_loc",
    ):
        assert banned_loc_key not in upstream["rules"]

    ast_rules_text = (REPO_ROOT / "rules" / "zero_self_invention.yml").read_text(encoding="utf-8")
    rule_ids = re.findall(r"(?m)^id:\s*(\S+)", ast_rules_text)
    assert len(rule_ids) == 21 and len(set(rule_ids)) == 21


def test_zero_hooks_and_native_uv_mcp_parity_across_5_configs() -> None:
    manifest = _load_manifest()
    parity = manifest["mcp_parity"]
    assert isinstance(parity, dict)
    assert sorted(parity["servers"]) == EXPECTED_MCP_SERVERS

    assert not (REPO_ROOT / ".codex/hooks.json").exists()

    root_cfg = json.loads((REPO_ROOT / ".mcp.json").read_text(encoding="utf-8"))
    claude_cfg = json.loads((REPO_ROOT / ".claude/settings.json").read_text(encoding="utf-8"))
    gemini_cfg = json.loads((REPO_ROOT / ".gemini/settings.json").read_text(encoding="utf-8"))
    opencode_cfg = json.loads((REPO_ROOT / "opencode.json").read_text(encoding="utf-8"))
    codex_cfg = tomllib.loads((REPO_ROOT / ".codex/config.toml").read_text(encoding="utf-8"))

    assert "hooks" not in claude_cfg
    assert "hooks" not in gemini_cfg
    assert "hooks" not in codex_cfg
    assert codex_cfg.get("features", {}).get("hooks", False) is False

    for name, section in (
        (".mcp.json", root_cfg["mcpServers"]),
        (".claude/settings.json", claude_cfg["mcpServers"]),
        (".gemini/settings.json", gemini_cfg["mcpServers"]),
        ("opencode.json", opencode_cfg["mcp"]),
        (".codex/config.toml", codex_cfg["mcp_servers"]),
    ):
        assert sorted(section.keys()) == EXPECTED_MCP_SERVERS, f"MCP mismatch in {name}"
        if name == "opencode.json":
            assert section["nautilus-engine"]["command"] == [
                "uv",
                "run",
                "--no-sync",
                "python",
                "scripts/nautilus_mcp_server.py",
            ]
            assert section["edgartools"]["command"] == ["uv", "run", "--no-sync", "edgartools-mcp"]
        else:
            assert section["nautilus-engine"]["command"] == "uv"
            assert section["nautilus-engine"]["args"] == [
                "run",
                "--no-sync",
                "python",
                "scripts/nautilus_mcp_server.py",
            ]
            assert section["edgartools"]["command"] == "uv"
            assert section["edgartools"]["args"] == ["run", "--no-sync", "edgartools-mcp"]


def test_harness_ssot_and_scoped_mcp_lanes() -> None:
    assert (REPO_ROOT / "AGENTS.md").is_file()
    assert (REPO_ROOT / "CLAUDE.md").read_text(encoding="utf-8").strip() == "@AGENTS.md"

    agent_files = sorted((REPO_ROOT / ".claude/agents").glob("*.md"))
    command_files = sorted((REPO_ROOT / ".claude/commands").glob("*.md"))
    assert len(agent_files) == 7
    assert len(command_files) == 5

    for agent_file in agent_files:
        text = agent_file.read_text(encoding="utf-8")
        assert text.strip(), agent_file.name
        assert len(text.splitlines()) <= 16, f"{agent_file.name} exceeds 16 lines"
    for cmd_file in command_files:
        text = cmd_file.read_text(encoding="utf-8")
        assert text.strip(), cmd_file.name
        assert len(text.splitlines()) <= 10, f"{cmd_file.name} exceeds 10 lines"

    gatekeeper = (REPO_ROOT / ".claude/agents/paper-gatekeeper.md").read_text(encoding="utf-8")
    assert "permissionMode: plan" in gatekeeper
    expected_disallowed = (
        "disallowedTools: Write, Edit, MultiEdit, NotebookEdit, Bash, Agent, "
        "mcp__nautilus-engine__stage_semi_auto_order_preset, mcp__xerj__xerj_memory_store"
    )
    assert expected_disallowed in gatekeeper

    skills_lock = json.loads((REPO_ROOT / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    locked_skills = {entry["skill"] for entry in skills_lock["skills"]}
    agent_skill_union: set[str] = set()
    for agent_file in agent_files:
        match = re.search(r"(?m)^skills:\s*\[([^\]]*)\]", agent_file.read_text(encoding="utf-8"))
        assert match is not None, f"Missing skills frontmatter in {agent_file.name}"
        role_skills = {s.strip() for s in match.group(1).split(",") if s.strip()}
        assert role_skills.issubset(locked_skills), (
            f"Unknown skill(s) in {agent_file.name}: {sorted(role_skills - locked_skills)}"
        )
        agent_skill_union.update(role_skills)
    assert agent_skill_union == locked_skills, (
        f"Unmapped agent skills: {sorted(locked_skills - agent_skill_union)}"
    )

    expected_lanes = {
        "sim.mcp.json": ["codebase-memory", "edgartools", "nautilus-engine", "xerj"],
        "paper.mcp.json": ["codebase-memory", "nautilus-engine", "xerj"],
        "tv.mcp.json": ["nautilus-engine", "xerj"],
        "gate.mcp.json": ["codebase-memory", "xerj"],
    }
    for fname, expected_servers in expected_lanes.items():
        data = json.loads((REPO_ROOT / ".claude/mcp-lanes" / fname).read_text(encoding="utf-8"))
        assert sorted(data["mcpServers"].keys()) == expected_servers


def test_architecture_layer_doc_has_mermaid_and_layers() -> None:
    doc = (REPO_ROOT / "docs/ARCHITECTURE_LAYER_MANIFEST.md").read_text(encoding="utf-8")
    assert "```mermaid\nflowchart TD" in doc
    for idx in range(11):
        assert f"**L{idx}**" in doc


def test_standalone_skills_zero_yfinance_and_zero_legacy_refs() -> None:
    manifest = _load_manifest()
    raw_files = manifest["repository_files"]
    assert isinstance(raw_files, list)
    repo_files = set(raw_files)

    uv_lock_text = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    assert "yfinance" not in uv_lock_text.lower()
    assert "quantstats" not in uv_lock_text.lower()

    skills_lock = json.loads((REPO_ROOT / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    assert skills_lock["schema"] == "tier0.upstream-skills-lock.v1"
    assert skills_lock["skills_count"] == 46
    assert len(skills_lock["skills"]) == 46
    from scripts.sync_manifest import verify_skills_lock

    verify_skills_lock(REPO_ROOT, skills_lock)

    starred = json.loads(
        (REPO_ROOT / "SOTA_STARRED_AND_BEYOND_MANIFEST.json").read_text(encoding="utf-8")
    )
    assert starred["schema_version"] == "tier0.sota-starred-beyond-manifest.v1"
    assert starred["zero_self_invented_hooks"] is True
    assert list(starred["active_winners_by_layer"].keys()) == [f"L{idx}" for idx in range(11)]
    assert {e["repo"] for e in starred["spdx_license_firewall"]} == NON_OSI_REPOS
    assert list(starred["earned_trigger_frontier_l11_to_l15"].keys()) == [
        f"L{idx}" for idx in range(11, 16)
    ]
    frontier_slugs = {
        str(cand["slug"])
        for layer_doc in starred["earned_trigger_frontier_l11_to_l15"].values()
        for cand in layer_doc.get("candidates", [])
    }
    assert "tradingview-mcp" not in frontier_slugs
    rejected_frontier = {
        str(entry["slug"]): entry for entry in starred.get("rejected_frontier_candidates", [])
    }
    assert "tradingview-mcp" in rejected_frontier
    assert rejected_frontier["tradingview-mcp"]["decision"] == "pruned"
    assert "9223" in rejected_frontier["tradingview-mcp"]["reason"]
    l14_by_slug = {
        str(c["slug"]): c
        for c in starred["earned_trigger_frontier_l11_to_l15"]["L14"]["candidates"]
    }
    assert l14_by_slug["prometheus/alertmanager"]["status"] == "admitted_profile_gated"
    assert l14_by_slug["prometheus/alertmanager"]["compose_profile"] == "alerting"
    l15_by_slug = {
        str(c["slug"]): c
        for c in starred["earned_trigger_frontier_l11_to_l15"]["L15"]["candidates"]
    }
    assert l15_by_slug["snyk/agent-scan"]["status"] == "admitted_offline_cli"
    assert l15_by_slug["snyk/agent-scan"]["telemetry_disabled"] is True

    required_files = {
        ".github/workflows/ci.yml",
        "scripts/check.ps1",
        "scripts/check.sh",
        "zensical.toml",
        "docs/index.md",
        "docs/adr/CATALOG.md",
        "docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md",
        "docker/compose.sota.yml",
        "evals/formal_risk_z3.py",
        "evals/tier0_inspect_eval.py",
        "scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1",
        "scripts/grand_reset/01_disk_z_and_wsl_setup.ps1",
        "scripts/grand_reset/01_wsl_bootstrap.sh",
        "scripts/grand_reset/02_runtimes_agy_orca_setup.ps1",
        "scripts/grand_reset/03_hosted_stack_tv_paper.ps1",
        "scripts/grand_reset/verify_grand_reset.ps1",
        ".opencode/commands/verify-tier0.md",
        "UPSTREAM_SKILLS_LOCK.json",
        "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
    }
    assert required_files.issubset(repo_files)
    adr_files = sorted(
        rel for rel in repo_files if rel.startswith("docs/adr/ADR-") and rel.endswith(".md")
    )
    assert "docs/adr/ADR-0001.md" in adr_files
    assert "docs/adr/ADR-0024.md" in adr_files
    assert "docs/adr/ADR-0025.md" in adr_files
    assert len(adr_files) >= 25

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert "src/tier0_sota/risk.py" in pyproject["tool"]["ruff"]["format"]["exclude"]
    assert "tier0_sota" in pyproject["tool"]["deptry"]["known_first_party"]

    scan_targets = [
        REPO_ROOT / "AGENTS.md",
        REPO_ROOT / "orca.yaml",
        REPO_ROOT / "scripts/bootstrap_env.sh",
        REPO_ROOT / "pyproject.toml",
        REPO_ROOT / "ARCHITECTURE_MANIFEST.json",
        REPO_ROOT / "LLM_HARNESS_MANIFEST.json",
        REPO_ROOT / "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
        REPO_ROOT / "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
        *sorted((REPO_ROOT / ".claude/agents").glob("*.md")),
        *sorted((REPO_ROOT / ".claude/commands").glob("*.md")),
        *sorted((REPO_ROOT / "src").rglob("*.py")),
        *sorted((REPO_ROOT / "docs").rglob("*.md")),
    ]
    hits = [
        str(p.relative_to(REPO_ROOT))
        for p in scan_targets
        if LEGACY_PATTERN.search(p.read_text(encoding="utf-8"))
    ]
    assert hits == [], f"Stale legacy references found in: {hits}"


def test_philosophy_and_llm_harness_manifests() -> None:
    agents_md = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    assert "PHILOSOPHY_COMPOUNDING_MANIFEST.json" in agents_md
    assert "LLM_HARNESS_MANIFEST.json" in agents_md

    phil = json.loads(
        (REPO_ROOT / "PHILOSOPHY_COMPOUNDING_MANIFEST.json").read_text(encoding="utf-8")
    )
    assert phil["schema_version"] == "tier0.philosophy-compounding-manifest.v1"
    assert set(phil["compounding_harness_lanes"].keys()) == {
        "sim-backtest",
        "paper",
        "half-auto-tv",
        "live",
    }
    assert phil["compounding_harness_lanes"]["live"]["live_execution_enabled"] is False
    assert phil["compounding_harness_lanes"]["live"]["paper_only"] is True
    assert (
        phil["core_trading_philosophy"]["immutable_risk_kernel"]["sha256"] == EXPECTED_RISK_SHA256
    )
    assert (
        phil["upstream_strategy_realization_map"]["immutable_risk_kernel"]["sha256"]
        == EXPECTED_RISK_SHA256
    )
    sot = phil["source_of_truth_hierarchy"]
    assert sot["sole_source_of_truth"] == "sota-upstream-convergence-per-core-trading-philosophy"
    assert sot["adrs_living_and_evolvable"] is True
    assert sot["adrs_supersession_allowed"] is True
    assert sot["adrs_artificial_count_cap"] is False
    assert len(phil["self_invention_lessons_prevented"]) >= 5
    assert all(entry["purged"] is True for entry in phil["self_invention_lessons_prevented"])

    llm = json.loads((REPO_ROOT / "LLM_HARNESS_MANIFEST.json").read_text(encoding="utf-8"))
    assert llm["source_of_truth_hierarchy"] == sot
    assert llm["schema_version"] == "tier0.llm-harness-manifest.v1"
    assert llm["zero_self_invented_hooks"] is True
    assert llm["ssot_entrypoints"]["canonical_agents_md"] == "AGENTS.md"
    assert llm["ssot_entrypoints"]["claude_md_pointer"] == "@AGENTS.md"
    assert llm["ssot_entrypoints"]["gemini_context_file_names"] == ["AGENTS.md", "GEMINI.md"]
    assert llm["ssot_entrypoints"]["opencode_instructions"] == ["AGENTS.md"]
    assert set(llm["compounding_lanes"].keys()) == {
        "sim-backtest",
        "paper",
        "half-auto-tv",
        "live",
    }
    assert llm["mcp_servers"]["parity_count"] == "5/5"
    assert llm["live_hosted_portals_count"] == 15
    assert len(llm["live_hosted_portals"]) == 15

    orca_yaml_text = (REPO_ROOT / "orca.yaml").read_text(encoding="utf-8")
    orca_top_keys = set(re.findall(r"(?m)^([A-Za-z][A-Za-z0-9_]*):", orca_yaml_text))
    allowed_orca_keys = {
        "setupAgentStartupPolicy",
        "scripts",
        "defaultTabs",
        "issueCommand",
        "environmentRecipes",
    }
    assert orca_top_keys.issubset(allowed_orca_keys), (
        f"Unsupported orca.yaml keys: {sorted(orca_top_keys - allowed_orca_keys)}"
    )
    assert "setupAgentStartupPolicy: wait-for-setup" in orca_yaml_text
    assert "setup: bash scripts/bootstrap_env.sh" in orca_yaml_text
    assert (REPO_ROOT / "scripts" / "bootstrap_env.sh").is_file()

    orca_practice = llm["orca_practice"]
    assert orca_practice["config_file"] == "orca.yaml"
    assert orca_practice["workspaces_dir_env"] == "ORCA_WORKSPACES_DIR"
    assert orca_practice["workspaces_dir"] == "Z:\\workspaces"
    assert orca_practice["setup_agent_startup_policy"] == "wait-for-setup"
    assert orca_practice["setup_script"] == "bash scripts/bootstrap_env.sh"
    assert orca_practice["official_upstream_orca_hooks_only"] is True
    assert orca_practice["zero_self_invented_hooks"] is True
    assert orca_practice["governance_spine_agent"] == "tier0-grand-central"
    assert len(orca_practice["governance_spine_skills"]) == 7

    skills_lock = json.loads((REPO_ROOT / "UPSTREAM_SKILLS_LOCK.json").read_text(encoding="utf-8"))
    locked_skills = {entry["skill"] for entry in skills_lock["skills"]}
    skills_summary = llm["skills_summary"]
    assert skills_summary["skills_count"] == 46
    assert skills_summary["lane_coverage"] == "46/46"
    assert skills_summary["agent_coverage"] == "46/46"
    assert skills_summary["unmapped_skills"] == []
    expected_sources = {
        "dagster-io/dagster": 1,
        "ml4t/skills": 12,
        "openbq-org/finos-workspace": 2,
        "stablyai/orca": 3,
        "tier0-upstream-directives": 4,
        "tradermonty/claude-trading-skills": 20,
        "tradingview/lightweight-charts": 1,
        "upstream-quality-evals": 3,
    }
    assert skills_summary["by_upstream_source"] == expected_sources
    assert sum(skills_summary["by_upstream_source"].values()) == 46

    lane_union: set[str] = set(orca_practice["governance_spine_skills"])
    for lane_name, lane in llm["compounding_lanes"].items():
        cmd_path = REPO_ROOT / lane["command"]
        mcp_path = REPO_ROOT / lane["mcp_lane"]
        assert cmd_path.is_file(), lane_name
        assert mcp_path.is_file(), lane_name
        lane_mcp_doc = json.loads(mcp_path.read_text(encoding="utf-8"))
        assert lane["mcp_servers"] == sorted(lane_mcp_doc["mcpServers"].keys())
        assert len(lane["upstream_gates"]) >= 3
        assert len(lane["curated_skills"]) >= 10
        cmd_text = cmd_path.read_text(encoding="utf-8")
        for skill_name in lane["curated_skills"]:
            assert skill_name in locked_skills, f"Unknown skill {skill_name} in {lane_name}"
            assert skill_name in cmd_text, f"{skill_name} missing from {lane['command']}"
        lane_union.update(lane["curated_skills"])
    assert lane_union == locked_skills, (
        f"Unmapped lane skills: {sorted(locked_skills - lane_union)}"
    )

    agent_skills_map = llm["agent_skills"]
    assert sorted(agent_skills_map.keys()) == llm["agent_roles"]
    assert {s for skills in agent_skills_map.values() for s in skills} == locked_skills

    auth_policy = llm["auth_and_routing_policy"]
    assert auth_policy["api_key_providers_allowed"] == ["anthropic", "openai"]
    assert auth_policy["local_automation_router"]["name"] == "omniroute"
    assert auth_policy["claude_code_interactive_auth"]["available_models_restricted"] is False
    assert auth_policy["claude_code_interactive_auth"]["zero_global_api_key_override"] is True
    assert (
        auth_policy["orca_multi_account_failover"]["helper_script"]
        == "scripts/orca_account_failover.py"
    )
    assert auth_policy["anthropic_banked_keys_policy"]["ci_dispatch_provider"] == (
        "anthropic-banked-sota"
    )


def test_sync_manifest_check_zero_drift() -> None:
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "sync_manifest.py"), "--check"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"sync_manifest.py --check failed:\n{proc.stderr}\n{proc.stdout}"
    assert "SYNC_MANIFEST_CHECK_OK=1" in proc.stdout


def test_sync_manifest_branch_is_default_branch_on_any_checkout(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts.sync_manifest import _detect_branch

    git = shutil.which("git")
    assert git is not None
    for var in [name for name in os.environ if name.startswith("GIT_")]:
        monkeypatch.delenv(var)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for role in ("AUTHOR", "COMMITTER"):
        monkeypatch.setenv(f"GIT_{role}_NAME", "manifest-test")
        monkeypatch.setenv(f"GIT_{role}_EMAIL", "manifest-test@example.invalid")

    def run_git(cwd: pathlib.Path, *args: str) -> None:
        subprocess.run([git, *args], cwd=cwd, capture_output=True, text=True, check=True)

    # A default branch other than "main" proves the value comes from origin/HEAD, not a fallback.
    run_git(tmp_path, "init", "--quiet", "--initial-branch=trunk", "origin")
    run_git(tmp_path / "origin", "commit", "--quiet", "--allow-empty", "-m", "seed")
    run_git(tmp_path, "clone", "--quiet", str(tmp_path / "origin"), "clone")
    clone = tmp_path / "clone"

    run_git(clone, "switch", "--quiet", "-c", "land/feature-branch")
    assert _detect_branch(clone) == "trunk"

    # A local branch named like the remote ref makes `symbolic-ref --short` ambiguous.
    run_git(clone, "branch", "origin/trunk")
    assert _detect_branch(clone) == "trunk"

    run_git(clone, "switch", "--quiet", "--detach", "HEAD")
    assert _detect_branch(clone) == "trunk"

    run_git(clone, "remote", "set-head", "origin", "--delete")
    assert _detect_branch(clone) == "main"


def test_meta_convergence_and_zero_banned_caps_in_skills_and_readme() -> None:
    llm = json.loads((REPO_ROOT / "LLM_HARNESS_MANIFEST.json").read_text(encoding="utf-8"))
    meta = llm.get("meta_convergence_practice", {})
    assert isinstance(meta, dict)
    assert "MADR 4.0 (living evolvable ADRs)" in meta.get("standards", [])
    assert "ISO/IEC/IEEE 42010:2022 (architecture rationale & viewpoints)" in meta.get(
        "standards", []
    )
    assert "Evolutionary Architecture Fitness Functions" in meta.get("standards", [])
    assert "AFML/ML4T Causal Statistical Falsification" in meta.get("standards", [])
    assert {
        "nautilus_trader",
        "hftbacktest",
        "optuna",
        "mlflow",
        "inspect-ai",
        "dspy",
        "dagster",
        "xerj",
        "orca",
    }.issubset(set(meta.get("reflection_stack", [])))

    banned_rx = re.compile(
        r"(?:advisory\s*<=\s*\d+\s*LOC|<=\s*450\s*LOC|18-pin|bounded\s*1\.\.12|"
        r"VectorBT|QuantStats|osv-scanner)",
        re.IGNORECASE,
    )
    targets = [
        REPO_ROOT / "README.md",
        *(REPO_ROOT / ".claude" / "skills").glob("tier0-*/SKILL.md"),
    ]
    assert len(targets) == 5
    for target in targets:
        text = target.read_text(encoding="utf-8")
        match = banned_rx.search(text)
        assert match is None, f"Banned arbitrary cap or tool in {target}: {match.group(0)}"


def test_nautilus_mcp_watchlist_v2_movers_fallback(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import nautilus_mcp_server as mcp_srv

    wl_path = tmp_path / "live_watchlist_manifest.json"
    movers_path = tmp_path / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "ranked_count": 2,
                "entries": [{"symbol": "NVDA"}, {"symbol": "SOXL"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("TIER0_WATCHLIST_MANIFEST", raising=False)
    monkeypatch.delenv("TIER0_LIVE_WATCHLIST_MANIFEST", raising=False)
    monkeypatch.setattr(mcp_srv, "DEFAULT_WATCHLIST_PATH", wl_path)
    monkeypatch.setattr(mcp_srv, "_FALLBACK_MOVERS_PATH", movers_path)

    fallback_res = mcp_srv.get_live_watchlist_manifest()
    assert fallback_res["ok"] is True
    assert fallback_res["manifest_path"] == str(movers_path)
    assert fallback_res["schema_version"] == "tier0.whole-market-movers.v2"
    assert fallback_res["ranked_count"] == 2

    wl_path.write_text(
        json.dumps({"schema_version": "tier0.watchlist-manifest.v1", "entries": []}),
        encoding="utf-8",
    )
    primary_res = mcp_srv.get_live_watchlist_manifest()
    assert primary_res["ok"] is True
    assert primary_res["manifest_path"] == str(wl_path)
    assert primary_res["schema_version"] == "tier0.watchlist-manifest.v1"

    bad_path = tmp_path / "bad_watchlist.json"
    bad_path.write_text(json.dumps({"schema_version": "tier0.unknown.v99"}), encoding="utf-8")
    bad_res = mcp_srv.get_live_watchlist_manifest(str(bad_path))
    assert bad_res["ok"] is False


def test_compounding_reflection_paper_farm_nested_schema(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import run_compounding_reflection as rcr

    perf_path = tmp_path / "paper_farm_live_performance_manifest.json"
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
    rep = rcr.run_compounding_reflection(offline=True, manifest_dir=tmp_path)
    assert rep["paper_farm"]["present"] is True
    assert rep["paper_farm"]["combined_equity_usd"] == 400000.0
    assert rep["paper_farm"]["accounts_count"] == 4

    calls: list[tuple[str, str]] = []

    def _fake_http(
        method: str,
        url: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 2.5,
    ) -> tuple[int, bytes]:
        del body, headers, timeout
        calls.append((method, url))
        return 200, json.dumps({"_id": "reflection-2026-11-26-core_24x5_compound"}).encode("utf-8")

    monkeypatch.setattr(rcr, "_http_request", _fake_http)
    pushed = rcr._push_xerj_lesson(
        "http://127.0.0.1:9200",
        {"timestamp_utc": "2026-11-26T12:00:00+00:00", "best_profile": "core_24x5_compound"},
    )
    assert pushed["pushed"] is True
    assert pushed["id"] == "reflection-2026-11-26-core_24x5_compound"
    expected_url = (
        "http://127.0.0.1:9200/tier0-compounding-lessons/_doc/"
        "reflection-2026-11-26-core_24x5_compound"
    )
    assert calls == [("PUT", expected_url)]


def test_adr_madr4_frontmatter_schema_and_sections() -> None:
    from scripts.sync_manifest import validate_adr_frontmatter_and_sections

    adrs = validate_adr_frontmatter_and_sections(REPO_ROOT)
    assert len(adrs) >= 25
    assert "ADR-0001" in adrs
    assert "ADR-0024" in adrs
    assert "ADR-0025" in adrs
    assert adrs["ADR-0019"][0].status == "accepted"
    proposed = [aid for aid, (fm, _) in sorted(adrs.items()) if fm.status == "proposed"]
    assert proposed == [], f"Expected 0 proposed ADRs after saturation, found: {proposed}"


def test_adr_bidirectional_supersession_dag_and_related_links() -> None:
    from scripts.sync_manifest import (
        validate_adr_frontmatter_and_sections,
        validate_adr_supersession_dag_and_related,
    )

    adrs = validate_adr_frontmatter_and_sections(REPO_ROOT)
    edges = validate_adr_supersession_dag_and_related(adrs)
    expected_edges = {
        ("ADR-0014", "ADR-0008"),
        ("ADR-0015", "ADR-0008"),
        ("ADR-0016", "ADR-0004"),
        ("ADR-0016", "ADR-0006"),
        ("ADR-0017", "ADR-0011"),
        ("ADR-0021", "ADR-0011"),
    }
    assert expected_edges.issubset({(new_id, old_id) for new_id, old_id in edges})


def test_adr_code_anchors_and_fitness_bindings_resolve() -> None:
    from scripts.sync_manifest import (
        validate_adr_anchors_and_fitness_bindings,
        validate_adr_frontmatter_and_sections,
    )

    adrs = validate_adr_frontmatter_and_sections(REPO_ROOT)
    summary = validate_adr_anchors_and_fitness_bindings(REPO_ROOT, adrs)
    assert summary["accepted_count"] >= 21
    assert summary["fitness_bound_accepted"] == summary["accepted_count"]


def test_adr_catalog_zensical_nav_and_manifest_graph_parity() -> None:
    from scripts.sync_manifest import (
        parse_and_validate_adrs,
        validate_adr_catalog_and_zensical_sync,
        validate_adr_frontmatter_and_sections,
    )

    adrs = validate_adr_frontmatter_and_sections(REPO_ROOT)
    sync_rep = validate_adr_catalog_and_zensical_sync(REPO_ROOT, adrs)
    assert sync_rep["catalog_and_zensical_in_sync"] is True
    assert sync_rep["catalog_count"] == len(adrs)
    assert sync_rep["zensical_nav_count"] == len(adrs)

    live_graph = parse_and_validate_adrs(REPO_ROOT)
    manifest = _load_manifest()
    assert manifest.get("adr_graph") == live_graph


def test_clean_sota_harness_zero_overlap_and_spdx_firewall() -> None:
    from scripts.sync_manifest import validate_clean_sota_harness

    rep = validate_clean_sota_harness(REPO_ROOT)
    assert rep["ok"] is True
    assert rep["zero_banned_in_uv_lock"] is True
    assert rep["zero_winner_overlap_with_rejected"] is True
    assert rep["zero_pre_commit_overlap"] is True
    assert rep["spdx_firewall_count"] == len(NON_OSI_REPOS)
    mapped_num, mapped_den = (int(x) for x in str(rep["src_files_mapped_ratio"]).split("/"))
    assert mapped_den >= 13 and mapped_num == mapped_den
    pw_audit = rep["zero_paper_winners_audit"]
    assert isinstance(pw_audit, dict)
    assert pw_audit["ok"] is True
    assert pw_audit["unwired_packages"] == []
    assert int(pw_audit["checked_default_packages"]) >= 35

    manifest = _load_manifest()
    assert manifest.get("clean_sota_harness") == rep


def test_workstation_lazy_imports_and_thread_bounds_fitness(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify ADR-0026 thread-bounded DuckDB, GBDT bounds, lazy imports, and Dagster workspace."""
    import os
    import subprocess
    import sys

    from tier0_sota.discovery import _env_positive_int
    from tier0_sota.live.paper_farm import duckdb_connect_config, open_bounded_duckdb

    tmp_duck_dir = tmp_path / "duckdb-tmp"
    monkeypatch.setenv("TIER0_DUCKDB_THREADS", "6")
    monkeypatch.setenv("TIER0_DUCKDB_MEMORY_LIMIT", "1GB")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_duck_dir))

    cfg = duckdb_connect_config(max_threads=4)
    assert cfg["threads"] == "4"
    assert cfg["memory_limit"] == "1GB"
    assert cfg["temp_directory"] == str(tmp_duck_dir)

    with open_bounded_duckdb(tmp_path / "bounded.duckdb", max_threads=2) as conn:
        row = conn.execute("SELECT current_setting('threads')").fetchone()
        assert row is not None and int(row[0]) <= 2

    monkeypatch.setenv("TIER0_LGBM_THREADS", "5")
    assert _env_positive_int("TIER0_LGBM_THREADS", 4) == 5
    monkeypatch.setenv("TIER0_LGBM_THREADS", "invalid")
    assert _env_positive_int("TIER0_LGBM_THREADS", 4) == 4

    probe = (
        "import json, sys; "
        "import tier0_sota.discovery, tier0_sota.engine, "
        "tier0_sota.research, tier0_sota.definitions; "
        "heavy = ['lightgbm', 'catboost', 'shap', 'stumpy', 'mlflow', "
        "'skfolio', 'dspy', 'inspect_ai', 'optuna']; "
        "loaded = [m for m in heavy if m in sys.modules]; "
        "print(json.dumps(loaded))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(REPO_ROOT),
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(proc.stdout.strip()) == []

    ws_path = REPO_ROOT / "docker" / "workspace.yaml"
    assert ws_path.is_file(), "Missing official Dagster gRPC docker/workspace.yaml"
    assert "dagster-code-server" in ws_path.read_text(encoding="utf-8")

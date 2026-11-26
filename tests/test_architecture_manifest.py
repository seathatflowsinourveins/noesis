from __future__ import annotations

import hashlib
import json
import pathlib
import re
import subprocess
import sys
import tomllib

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
EXPECTED_MCP_SERVERS = ["codebase-memory", "edgartools", "nautilus-engine", "xerj"]
LEGACY_PATTERN = re.compile(
    r"tier0-quant(?!-falsification)|CLEAN LIVE|sota-live-tv-harness|"
    r"sota_compounding_1000rpm_harness|RTX 4090"
)
NON_OSI_REPOS = {"treeverse/lakeFS", "restatedev/restate", "Arize-ai/phoenix", "NX-AI/tirex"}


def _load_manifest() -> dict[str, object]:
    raw = (REPO_ROOT / "ARCHITECTURE_MANIFEST.json").read_text(encoding="utf-8")
    data = json.loads(raw)
    assert isinstance(data, dict)
    return data


def test_architecture_manifest_schema_and_files_exist() -> None:
    manifest = _load_manifest()
    assert manifest["schema_version"] == "tier0.architecture-layer-manifest.v1"
    assert manifest["repo_name"] == "tier0-sota-clean"
    assert manifest["branch"] == "main"
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
    assert "hooks" not in codex_cfg.get("features", {})

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
    for entry in skills_lock["skills"]:
        skill_md = REPO_ROOT / ".claude/skills" / entry["skill"] / "SKILL.md"
        assert skill_md.is_file(), f"Missing standalone skill {entry['skill']}"
        assert hashlib.sha256(skill_md.read_bytes()).hexdigest() == entry["skill_sha256"]

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
    } | {f"docs/adr/ADR-{idx:04d}.md" for idx in range(1, 13)}
    assert required_files.issubset(repo_files)

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

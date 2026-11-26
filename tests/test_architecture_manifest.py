from __future__ import annotations

import hashlib
import json
import pathlib
import re
import tomllib

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
EXPECTED_MCP_SERVERS = ["codebase-memory", "edgartools", "nautilus-engine", "xerj"]
LEGACY_PATTERN = re.compile(
    r"tier0-quant|CLEAN LIVE|sota-live-tv-harness|sota_compounding_1000rpm_harness|RTX 4090"
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
    assert safety["loc_enforcement"] == "advisory"
    assert safety["forbidden_live_ports"] == [4001, 7496, 8798, 8796]

    repo_files = manifest["repository_files"]
    assert isinstance(repo_files, list)
    assert len(repo_files) == len(set(repo_files))
    missing = [rel for rel in repo_files if not (REPO_ROOT / rel).is_file()]
    assert missing == [], f"Missing declared repository files: {missing}"


def test_risk_kernel_sha256_and_loc_exact() -> None:
    manifest = _load_manifest()
    risk_meta = manifest["risk_kernel"]
    assert isinstance(risk_meta, dict)
    risk_path = REPO_ROOT / str(risk_meta["path"])
    assert risk_path.is_file()
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    assert digest == EXPECTED_RISK_SHA256
    assert risk_meta["sha256"] == EXPECTED_RISK_SHA256
    assert len(risk_path.read_text(encoding="utf-8").splitlines()) == 361


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


def test_few_line_harness_and_scoped_mcp_lanes() -> None:
    agent_files = sorted((REPO_ROOT / ".claude/agents").glob("*.md"))
    command_files = sorted((REPO_ROOT / ".claude/commands").glob("*.md"))
    assert len(agent_files) == 7
    assert len(command_files) == 5

    for agent_file in agent_files:
        assert len(agent_file.read_text(encoding="utf-8").splitlines()) <= 16, agent_file.name
    for cmd_file in command_files:
        assert len(cmd_file.read_text(encoding="utf-8").splitlines()) <= 10, cmd_file.name

    gatekeeper = (REPO_ROOT / ".claude/agents/paper-gatekeeper.md").read_text(encoding="utf-8")
    assert "permissionMode: plan" in gatekeeper

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
        REPO_ROOT / "pyproject.toml",
        REPO_ROOT / "ARCHITECTURE_MANIFEST.json",
        REPO_ROOT / "SOTA_STARRED_AND_BEYOND_MANIFEST.json",
        *sorted((REPO_ROOT / "src").rglob("*.py")),
        *sorted((REPO_ROOT / "docs").rglob("*.md")),
    ]
    hits = [
        str(p.relative_to(REPO_ROOT))
        for p in scan_targets
        if LEGACY_PATTERN.search(p.read_text(encoding="utf-8"))
    ]
    assert hits == [], f"Stale legacy references found in: {hits}"

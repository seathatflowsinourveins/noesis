from __future__ import annotations

import hashlib
import json
import pathlib
import tomllib

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
EXPECTED_MCP_SERVERS = ["codebase-memory", "edgartools", "nautilus-engine", "xerj"]


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

    safety = manifest["safety_invariants"]
    assert isinstance(safety, dict)
    assert safety["live_execution_enabled"] is False
    assert safety["paper_only"] is True
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


def test_layers_l0_to_l9_match_upstream_map() -> None:
    manifest = _load_manifest()
    upstream = tomllib.loads((REPO_ROOT / "upstream-map.toml").read_text(encoding="utf-8"))
    upstream_layers = upstream["layers"]
    manifest_layers = manifest["layers"]
    assert isinstance(manifest_layers, dict)

    expected_keys = [f"L{idx}" for idx in range(10)]
    assert list(manifest_layers.keys()) == expected_keys
    for key in expected_keys:
        assert key in upstream_layers
        assert manifest_layers[key]["name"] == upstream_layers[key]["name"]
        assert manifest_layers[key]["winners"] == upstream_layers[key]["winners"]
        assert (
            manifest_layers[key]["rejected_overlaps"] == upstream_layers[key]["rejected_overlaps"]
        )
        assert len(manifest_layers[key]["repo_artifacts"]) >= 1


def test_4_runtime_mcp_parity_across_all_5_configs() -> None:
    manifest = _load_manifest()
    parity = manifest["mcp_parity"]
    assert isinstance(parity, dict)
    assert sorted(parity["servers"]) == EXPECTED_MCP_SERVERS

    root_mcp = json.loads((REPO_ROOT / ".mcp.json").read_text(encoding="utf-8"))["mcpServers"]
    claude_mcp = json.loads((REPO_ROOT / ".claude/settings.json").read_text(encoding="utf-8"))[
        "mcpServers"
    ]
    gemini_mcp = json.loads((REPO_ROOT / ".gemini/settings.json").read_text(encoding="utf-8"))[
        "mcpServers"
    ]
    opencode_mcp = json.loads((REPO_ROOT / "opencode.json").read_text(encoding="utf-8"))["mcp"]
    codex_mcp = tomllib.loads((REPO_ROOT / ".codex/config.toml").read_text(encoding="utf-8"))[
        "mcp_servers"
    ]

    for name, section in (
        (".mcp.json", root_mcp),
        (".claude/settings.json", claude_mcp),
        (".gemini/settings.json", gemini_mcp),
        ("opencode.json", opencode_mcp),
        (".codex/config.toml", codex_mcp),
    ):
        assert sorted(section.keys()) == EXPECTED_MCP_SERVERS, f"MCP mismatch in {name}"


def test_architecture_layer_doc_has_mermaid_and_layers() -> None:
    doc = (REPO_ROOT / "docs/ARCHITECTURE_LAYER_MANIFEST.md").read_text(encoding="utf-8")
    assert "```mermaid\nflowchart TD" in doc
    for idx in range(10):
        assert f"**L{idx}**" in doc

"""Agent runtime fitness checks (ADR-0033).

Offline and deterministic: they read repository files only. Later runtime changes add their
checks here in the pull request that makes them pass.
"""

from __future__ import annotations

import ast
import json
import pathlib
import shutil
import subprocess

import pytest
import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
AGENTS_DIR = REPO_ROOT / ".claude" / "agents"
NAUTILUS_MCP_SERVER = REPO_ROOT / "scripts" / "nautilus_mcp_server.py"

# Roles whose job is to read and report. Read-only holds by the `tools` allowlist: Claude Code
# ignores a subagent's `permissionMode` under a bypassPermissions, acceptEdits or auto parent,
# while `tools` limits a subagent and an agent-team teammate alike.
READ_ONLY_REVIEWER_ROLES = ("paper-gatekeeper",)

# The only built-in tools a reviewer may name. SendMessage reports to another agent; it runs
# no shell and edits no file.
REVIEWER_BUILTIN_TOOLS = frozenset({"Read", "Grep", "Glob", "SendMessage"})

# Whole-server entries admitted by name until the lane-profile item replaces them (ADR-0033).
ADMITTED_WILDCARDS = frozenset({"mcp__edgartools__*"})

# Write-capable built-ins that `disallowedTools` repeats; other built-ins are kept out by the
# allowlist alone. MultiEdit is a legacy name the manifest gate still requires.
WRITE_CAPABLE_BUILTIN_TOOLS = frozenset(
    {"Edit", "Write", "MultiEdit", "NotebookEdit", "Bash", "PowerShell", "Monitor", "Agent"}
)

# Every tool of the two binary MCP servers, classed once.
# - codebase-memory-mcp 0.12.1: `readOnlyHint` in TOOL_ANNOTATIONS (src/mcp/mcp.c).
# - xerj v1.0.0-rc.104: its tools/list carries no annotations, so the classes follow the
#   server's own tool descriptions. A read-only tool also takes no per-call argument that
#   sends document text or a review off the node.
READ_ONLY_MCP_TOOLS: dict[str, frozenset[str]] = {
    "codebase-memory": frozenset(
        {
            "check_index_coverage",
            "compare_graphs",
            "detect_changes",
            "get_architecture",
            "get_code_snippet",
            "get_file_outline",
            "get_graph_schema",
            "index_status",
            "list_projects",
            "query_graph",
            "search_code",
            "search_graph",
            "trace_path",
        }
    ),
    "xerj": frozenset(
        {
            "xerj_batch_search",
            "xerj_brain_ego",
            "xerj_brain_overview",
            "xerj_code_search",
            "xerj_def",
            "xerj_hub_find",
            "xerj_index_budget",
            "xerj_map",
            "xerj_memory_recall",
            "xerj_plan",
            "xerj_semantic_search",
            "xerj_status",
            "xerj_vector_search",
        }
    ),
}
# Tools that change an index, a memory store or a live source, or post to an outside service.
WRITE_MCP_TOOLS: dict[str, frozenset[str]] = {
    "codebase-memory": frozenset(
        {"delete_project", "index_repository", "ingest_traces", "manage_adr"}
    ),
    "xerj": frozenset(
        {
            "xerj_autoindex_remove",
            "xerj_brain_link",
            "xerj_brain_unlink",
            "xerj_corpus_feedback",
            "xerj_field_report",
            "xerj_memory_store",
            "xerj_sources",
        }
    ),
}
# Tools that read the node but can send text off the machine: `rerank` sends the top hits to
# an external relevance model, and a hub answer is followed by an automatic corpus review.
OFF_NODE_READ_MCP_TOOLS: dict[str, frozenset[str]] = {
    "codebase-memory": frozenset(),
    "xerj": frozenset({"xerj_hub_ask", "xerj_hub_def", "xerj_hybrid_search", "xerj_search"}),
}
MCP_SERVER_TOOL_COUNTS = {"codebase-memory": 17, "xerj": 24}


def _frontmatter(path: pathlib.Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert text.startswith("---\n"), f"{path.name}: missing frontmatter"
    meta = yaml.safe_load(text[4 : text.index("\n---\n", 4)])
    assert isinstance(meta, dict), f"{path.name}: frontmatter must be a mapping"
    return meta


def _tool_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _is_read_only_local(annotations: ast.expr | None, hints: dict[str, bool]) -> bool:
    if isinstance(annotations, ast.Name):
        return hints.get(annotations.id, False)
    if not isinstance(annotations, ast.Call):
        return False
    func = annotations.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
    if name != "ToolAnnotations":
        return False
    values = {
        kw.arg: kw.value.value for kw in annotations.keywords if isinstance(kw.value, ast.Constant)
    }
    return values.get("readOnlyHint") is True and values.get("openWorldHint") is False


def _nautilus_mcp_tools() -> tuple[frozenset[str], frozenset[str]]:
    """Split the `@mcp.tool` functions by their own annotations: read-only and local, or not."""
    tree = ast.parse(NAUTILUS_MCP_SERVER.read_text(encoding="utf-8"))
    hints: dict[str, bool] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    hints[target.id] = _is_read_only_local(node.value, hints)
    read_only: set[str] = set()
    other: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for dec in node.decorator_list:
            func = dec.func if isinstance(dec, ast.Call) else dec
            if not (isinstance(func, ast.Attribute) and func.attr == "tool"):
                continue
            keywords = dec.keywords if isinstance(dec, ast.Call) else []
            annotations = next((kw.value for kw in keywords if kw.arg == "annotations"), None)
            (read_only if _is_read_only_local(annotations, hints) else other).add(node.name)
    return frozenset(read_only), frozenset(other)


def _named(server: str, names: frozenset[str]) -> set[str]:
    return {f"mcp__{server}__{name}" for name in names}


def test_reviewer_roles_are_read_only_by_tool_allowlist() -> None:
    nautilus_read_only, nautilus_other = _nautilus_mcp_tools()
    assert nautilus_read_only, "nautilus_mcp_server.py: no tool is annotated read-only and local"
    assert "stage_semi_auto_order_preset" in nautilus_other
    for server, count in MCP_SERVER_TOOL_COUNTS.items():
        classes = (READ_ONLY_MCP_TOOLS, WRITE_MCP_TOOLS, OFF_NODE_READ_MCP_TOOLS)
        names = [name for table in classes for name in table[server]]
        assert len(names) == len(set(names)) == count, f"{server}: tools classed twice or missing"

    allowed = set(REVIEWER_BUILTIN_TOOLS) | set(ADMITTED_WILDCARDS)
    allowed |= _named("nautilus-engine", nautilus_read_only)
    for server, names in READ_ONLY_MCP_TOOLS.items():
        allowed |= _named(server, names)
    required_disallowed = set(WRITE_CAPABLE_BUILTIN_TOOLS)
    required_disallowed |= _named("nautilus-engine", nautilus_other)
    for server, names in WRITE_MCP_TOOLS.items():
        required_disallowed |= _named(server, names)
    assert not allowed & required_disallowed

    for role in READ_ONLY_REVIEWER_ROLES:
        meta = _frontmatter(AGENTS_DIR / f"{role}.md")
        tools = _tool_list(meta.get("tools"))
        assert tools, f"{role}: no `tools` allowlist, so the role inherits every tool"
        outside = sorted(set(tools) - allowed)
        assert not outside, f"{role}: tools outside the read-only allowlist {outside}"
        missing = sorted(required_disallowed - set(_tool_list(meta.get("disallowedTools"))))
        assert not missing, f"{role}: disallowedTools omits {missing}"


@pytest.mark.skipif(shutil.which("opencode") is None, reason="opencode CLI not installed")
def test_opencode_debug_config_compiles_paper_gatekeeper_deny_rules() -> None:
    """Verify live OpenCode translates agent.paper-gatekeeper.permission into 5 V2 deny rules."""
    opencode_bin = shutil.which("opencode")
    assert opencode_bin is not None
    proc = subprocess.run(  # noqa: S603
        [opencode_bin, "debug", "config"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    raw = json.loads(proc.stdout)
    repo_cfg_path = (REPO_ROOT / "opencode.json").resolve()
    info: dict[str, object] = {}
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict) or entry.get("type") != "document":
                continue
            entry_path = entry.get("path")
            if isinstance(entry_path, str) and pathlib.Path(entry_path).resolve() == repo_cfg_path:
                candidate = entry.get("info")
                if isinstance(candidate, dict):
                    info = candidate
                    break
    elif isinstance(raw, dict):
        info = raw
    agents = info.get("agents")
    assert isinstance(agents, dict), f"Missing compiled agents map in opencode debug config: {raw}"
    gk = agents.get("paper-gatekeeper")
    assert isinstance(gk, dict), "Missing compiled paper-gatekeeper agent in opencode debug config"
    perms = gk.get("permissions")
    assert isinstance(perms, list), "Missing compiled V2 permissions list for paper-gatekeeper"
    denied_actions = {
        str(rule.get("action"))
        for rule in perms
        if isinstance(rule, dict) and rule.get("effect") == "deny"
    }
    expected_denies = {
        "edit",
        "shell",
        "subagent",
        "nautilus-engine_stage_semi_auto_order_preset",
        "xerj_xerj_memory_store",
    }
    assert expected_denies.issubset(denied_actions), (
        f"OpenCode paper-gatekeeper missing deny rules: {sorted(expected_denies - denied_actions)}"
    )

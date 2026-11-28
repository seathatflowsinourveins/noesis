"""ADR-0032 fitness checks: landing through pull requests and upstream-native maintenance.

Offline only: these tests read committed files (workflows, the ruleset snapshot, mise.toml,
.pre-commit-config.yaml). The live ruleset is compared with the snapshot by a read-only
`gh api` call outside pytest, because that needs the network.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
RULESET_SNAPSHOT = REPO_ROOT / ".github" / "rulesets" / "protect-main.json"

REQUIRED_CHECKS = frozenset(
    {
        "Quality, AST, Formal Risk & Manifest Gate",
        "Secret Scan (gitleaks)",
        "Filesystem Vulnerability Scan (Trivy)",
        "Workflow Security Audit (zizmor)",
    }
)
GITHUB_ACTIONS_APP_ID = 15368
FULL_SHA = re.compile(r"[0-9a-f]{40}")
USES_LINE = re.compile(r"^\s*(?:-\s+)?uses:\s*(?P<ref>[^\s#]+)(?:\s+#\s*(?P<comment>.*?))?\s*$")
USES_KEY = re.compile(r"\buses\s*:")
PRE_COMMIT_REV = re.compile(r"^\s*rev:\s*(?P<rev>\S+)(?:\s+#\s*(?:frozen:\s*)?(?P<tag>\S+))?")
# (workflow file, job id) -> the only write scopes that job may hold.
WRITE_GRANTS: dict[tuple[str, str], frozenset[str]] = {
    ("release.yml", "draft-release"): frozenset({"contents"}),
    ("multi-model-sota.yml", "claude-omniroute-or-anthropic-banked"): frozenset({"id-token"}),
}
# (workflow file, job id, step name) -> the only step conditional a required job may carry.
# `!cancelled()` runs the step whenever the default `success()` would, and also after a failure,
# so it can never skip a step that would otherwise run.
REQUIRED_JOB_STEP_CONDITIONS: dict[tuple[str, str, str], str] = {
    ("ci.yml", "sota-quality-and-safety-gate", "Upload JUnit XML test report"): (
        "${{ !cancelled() }}"
    ),
}


def _load_workflows() -> dict[str, dict[str, Any]]:
    docs: dict[str, dict[str, Any]] = {}
    for path in sorted(WORKFLOWS_DIR.glob("*.y*ml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        # PyYAML follows YAML 1.1, where the bare key `on` loads as the boolean True.
        doc["on"] = doc.pop(True, doc.get("on"))
        docs[path.name] = doc
    return docs


def _triggers(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    on = doc["on"]
    if isinstance(on, str):
        return {on: {}}
    if isinstance(on, list):
        return {str(event): {} for event in on}
    return {str(event): (config or {}) for event, config in on.items()}


def test_ruleset_snapshot_states_the_landing_decision() -> None:
    ruleset = json.loads(RULESET_SNAPSHOT.read_text(encoding="utf-8"))
    assert ruleset["name"] == "protect-main"
    assert ruleset["target"] == "branch"
    assert ruleset["enforcement"] == "active"
    assert ruleset["bypass_actors"] == []
    assert ruleset["conditions"]["ref_name"]["include"] == ["refs/heads/main"]
    rules = {rule["type"]: rule.get("parameters", {}) for rule in ruleset["rules"]}
    assert {"deletion", "non_fast_forward", "pull_request", "required_status_checks"} <= set(rules)
    reviews = rules["pull_request"]
    assert reviews["required_approving_review_count"] == 0
    assert reviews["require_code_owner_review"] is False
    checks = rules["required_status_checks"]
    assert checks["strict_required_status_checks_policy"] is True
    assert checks["do_not_enforce_on_create"] is False
    contexts = [check["context"] for check in checks["required_status_checks"]]
    assert sorted(contexts) == sorted(REQUIRED_CHECKS)
    assert {check["integration_id"] for check in checks["required_status_checks"]} == {
        GITHUB_ACTIONS_APP_ID
    }


def test_required_checks_are_unfiltered_pull_request_jobs() -> None:
    workflows = _load_workflows()
    producers: dict[str, list[tuple[str, str]]] = {}
    for file_name, doc in workflows.items():
        for job_id, job in doc["jobs"].items():
            producers.setdefault(str(job.get("name", job_id)), []).append((file_name, job_id))
    step_conditions: dict[tuple[str, str, str], Any] = {}
    for context in sorted(REQUIRED_CHECKS):
        assert len(producers.get(context, [])) == 1, (context, producers.get(context))
        file_name, job_id = producers[context][0]
        triggers = _triggers(workflows[file_name])
        assert "pull_request" in triggers, (context, file_name)
        assert "pull_request_target" not in triggers, (context, file_name)
        pull_request = triggers["pull_request"]
        assert not set(pull_request) & {"paths", "paths-ignore", "branches-ignore", "types"}, (
            file_name,
            sorted(pull_request),
        )
        assert "main" in pull_request.get("branches", ["main"]), file_name
        job = workflows[file_name]["jobs"][job_id]
        assert not {"if", "needs", "strategy", "continue-on-error"} & set(job), (
            context,
            sorted(job),
        )
        for index, step in enumerate(job.get("steps", [])):
            step_name = str(step.get("name", f"steps[{index}]"))
            assert "continue-on-error" not in step, (file_name, job_id, step_name)
            if "if" in step:
                step_conditions[(file_name, job_id, step_name)] = step["if"]
    assert step_conditions == REQUIRED_JOB_STEP_CONDITIONS


def _uses_values(doc: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for job in doc["jobs"].values():
        if "uses" in job:
            refs.append(str(job["uses"]))
        refs.extend(str(step["uses"]) for step in job.get("steps", []) if "uses" in step)
    return refs


def test_every_action_is_pinned_to_a_full_commit_sha() -> None:
    # Every `uses` value as GitHub reads it, whatever the YAML style of the line.
    checked = 0
    for file_name, doc in _load_workflows().items():
        for ref in _uses_values(doc):
            if ref.startswith("./"):
                continue
            assert not ref.startswith("docker://"), (file_name, ref)
            _action, _, version = ref.partition("@")
            assert FULL_SHA.fullmatch(version), (file_name, ref)
            checked += 1
    assert checked > 0
    # Every line that holds a `uses:` key is a block-style line with the release tag as a comment.
    for path in sorted(WORKFLOWS_DIR.glob("*.y*ml")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not USES_KEY.search(line.split("#", 1)[0]):
                continue
            match = USES_LINE.match(line)
            assert match is not None, f"{path.name}:{lineno}: put `uses:` on its own line"
            if match["ref"].startswith("./"):
                continue
            assert match["comment"], f"{path.name}:{lineno}: add the release tag as a comment"


def test_workflow_token_permissions_are_least_privilege() -> None:
    for file_name, doc in _load_workflows().items():
        assert doc.get("permissions") in ({}, {"contents": "read"}), file_name
        for job_id, job in doc["jobs"].items():
            permissions = job.get("permissions")
            assert isinstance(permissions, dict), (file_name, job_id)
            writes = {scope for scope, level in permissions.items() if level == "write"}
            assert writes == WRITE_GRANTS.get((file_name, job_id), frozenset()), (
                file_name,
                job_id,
                sorted(writes),
            )
            for step in job.get("steps", []):
                if str(step.get("uses", "")).startswith("actions/checkout@"):
                    assert step.get("with", {}).get("persist-credentials") is False, (
                        file_name,
                        job_id,
                    )


def test_ci_tool_versions_match_mise_pins() -> None:
    mise_tools = tomllib.loads((REPO_ROOT / "mise.toml").read_text(encoding="utf-8"))["tools"]
    used: list[tuple[str, str]] = []
    for doc in _load_workflows().values():
        for job in doc["jobs"].values():
            for step in job.get("steps", []):
                uses = str(step.get("uses", ""))
                inputs = step.get("with") or {}
                env = step.get("env") or {}
                if uses.startswith("astral-sh/setup-uv@"):
                    used.append(("uv", str(inputs["version"])))
                elif uses.startswith("aquasecurity/trivy-action@"):
                    used.append(("trivy", str(inputs["version"])))
                elif uses.startswith("zizmorcore/zizmor-action@"):
                    used.append(("zizmor", str(inputs["version"])))
                elif uses.startswith("anchore/sbom-action@"):
                    used.append(("syft", str(inputs["syft-version"])))
                elif uses.startswith("gitleaks/gitleaks-action@"):
                    used.append(("gitleaks", str(env["GITLEAKS_VERSION"])))
    assert {tool for tool, _ in used} == {"uv", "trivy", "zizmor", "syft", "gitleaks"}
    for tool, version in used:
        assert version.removeprefix("v").startswith(f"{mise_tools[tool]}."), (tool, version)


def test_pre_commit_hooks_are_sha_pinned_and_match_mise() -> None:
    mise_tools = tomllib.loads((REPO_ROOT / "mise.toml").read_text(encoding="utf-8"))["tools"]
    hook_tool = {
        "https://github.com/astral-sh/ruff-pre-commit": "ruff",
        "https://github.com/gitleaks/gitleaks": "gitleaks",
        "https://github.com/zizmorcore/zizmor-pre-commit": "zizmor",
    }
    config_text = (REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    config_lines = config_text.splitlines()
    remote_repos = [r for r in yaml.safe_load(config_text)["repos"] if r["repo"] != "local"]
    assert remote_repos
    for repo in remote_repos:
        assert FULL_SHA.fullmatch(str(repo["rev"])), repo["repo"]
        rev_line = next(line for line in config_lines if str(repo["rev"]) in line)
        match = PRE_COMMIT_REV.match(rev_line)
        assert match is not None and match["tag"], repo["repo"]
        tool = hook_tool.get(repo["repo"])
        if tool is not None:
            assert match["tag"].removeprefix("v").startswith(f"{mise_tools[tool]}."), repo["repo"]

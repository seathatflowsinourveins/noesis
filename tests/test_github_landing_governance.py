"""ADR-0032 fitness checks: landing through pull requests and upstream-native maintenance.

Offline only: these tests read committed files (workflows, the ruleset snapshot, mise.toml,
.pre-commit-config.yaml). The live ruleset is compared with the snapshot by a read-only
`gh api` call outside pytest, because that needs the network.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import textwrap
import tomllib
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from packaging.requirements import Requirement

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
# The hooks that test_pre_commit_hooks_are_sha_pinned_and_match_mise ties to a mise.toml minor.
MISE_PINNED_HOOKS = frozenset(
    {
        "https://github.com/astral-sh/ruff-pre-commit",
        "https://github.com/gitleaks/gitleaks",
        "https://github.com/zizmorcore/zizmor-pre-commit",
    }
)
# (workflow file, job id) -> the only write scopes that job may hold.
WRITE_GRANTS: dict[tuple[str, str], frozenset[str]] = {
    ("release.yml", "draft-release"): frozenset({"contents"}),
    ("multi-model-sota.yml", "claude-omniroute-or-anthropic-banked"): frozenset({"id-token"}),
    ("dependabot-auto-merge.yml", "enable-auto-merge"): frozenset({"contents", "pull-requests"}),
    ("release.yml", "attest"): frozenset({"id-token", "attestations"}),
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
    docker_uv_pins: list[str] = []
    for dockerfile in sorted((REPO_ROOT / "docker").glob("Dockerfile.*")):
        for ver in re.findall(
            r"(?m)^COPY\s+--from=ghcr\.io/astral-sh/uv:([^\s@]+)",
            dockerfile.read_text(encoding="utf-8"),
        ):
            docker_uv_pins.append(ver)
            used.append(("uv", ver))
    assert len(docker_uv_pins) >= 2, f"expected uv pins in Dockerfiles, got {docker_uv_pins}"
    assert {tool for tool, _ in used} == {"uv", "trivy", "zizmor", "syft", "gitleaks"}
    by_tool: dict[str, set[str]] = {}
    for tool, version in used:
        assert version.removeprefix("v").startswith(f"{mise_tools[tool]}."), (tool, version)
        by_tool.setdefault(tool, set()).add(version)
    for tool, versions in by_tool.items():
        assert len(versions) == 1, f"divergent {tool} versions across workflows: {sorted(versions)}"


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


def _dependabot_ignore_lines(config_text: str) -> list[tuple[int, str]]:
    """Return (line index, line) of every `- dependency-name:` entry inside an `ignore:` block."""
    lines = config_text.splitlines()
    entries: list[tuple[int, str]] = []
    block_indent: int | None = None
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if block_indent is not None and indent <= block_indent:
            block_indent = None
        if re.fullmatch(r"ignore:", stripped):
            block_indent = indent
        elif block_indent is not None and re.match(r"-\s+dependency-name:", stripped):
            entries.append((idx, line))
    return entries


def test_dependabot_covers_every_pinned_ecosystem() -> None:
    config_text = (REPO_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    config = yaml.safe_load(config_text)
    updates = config["updates"]
    ecosystems = {update["package-ecosystem"] for update in updates}
    assert {"uv", "github-actions", "docker", "docker-compose", "pre-commit"} <= ecosystems
    directories = {(u["package-ecosystem"], u["directory"]) for u in updates}
    assert {
        ("uv", "/"),
        ("github-actions", "/"),
        ("docker", "/docker"),
        ("docker-compose", "/docker"),
        ("pre-commit", "/"),
    } <= directories
    for update in updates:
        assert update["schedule"]["interval"] == "weekly", update["package-ecosystem"]
        assert update["cooldown"]["default-days"] >= 7, update["package-ecosystem"]
        assert update.get("groups"), update["package-ecosystem"]
    (uv,) = [update for update in updates if update["package-ecosystem"] == "uv"]
    assert uv["versioning-strategy"] == "lockfile-only"
    # fetch-metadata reports the highest semver change of a pull request: patches need a group
    # of their own for the uv-patch auto-merge rule to ever apply.
    assert uv["groups"]["uv-patch"]["update-types"] == ["patch"]
    assert "patch" not in uv["groups"]["uv-minor"]["update-types"]
    # Holds: every ignore entry carries, on the comment line just above it, a date, a reason that
    # names the held dependency and its source, and the trigger that re-opens it.
    ignores = [(u["package-ecosystem"], i) for u in updates for i in u.get("ignore", [])]
    held = {(eco, str(entry["dependency-name"])) for eco, entry in ignores}
    assert {("uv", "numpy"), ("uv", "mcp"), ("docker", "python")} <= held
    for eco, entry in ignores:
        assert entry.get("versions") or entry.get("update-types"), (eco, entry)
    config_lines = config_text.splitlines()
    ignore_lines = _dependabot_ignore_lines(config_text)
    assert len(ignore_lines) == len(ignores)
    for (eco, entry), (idx, line) in zip(ignores, ignore_lines, strict=True):
        reason = re.fullmatch(r"#\s*(\d{4}-\d{2}-\d{2}): (\S.*)", config_lines[idx - 1].strip())
        assert reason is not None, f"ignore entry without a dated reason above it: {line.strip()!r}"
        date.fromisoformat(reason[1])
        text = reason[2]
        assert str(entry["dependency-name"]) in text, (eco, text)
        assert "https://" in text or "pyproject.toml" in text, (eco, text)
        assert "re-open when" in text, (eco, text)
    # Dependabot resolves a SHA-pinned hook through its `# frozen:` comment; without one it moves
    # the hook to the HEAD of the hook repository's default branch.
    hook_config = (REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    rev_lines = [line for line in hook_config.splitlines() if line.lstrip().startswith("rev:")]
    assert rev_lines
    assert all("# frozen: " in line for line in rev_lines), rev_lines
    # A minor bump of a hook that mise.toml pins is red until the toolchain pull request, so those
    # hooks get the first pre-commit group and every other group excludes them.
    (hooks,) = [u for u in updates if u["package-ecosystem"] == "pre-commit"]
    hook_groups = list(hooks["groups"].values())
    assert set(hook_groups[0]["patterns"]) == MISE_PINNED_HOOKS
    for group in hook_groups[1:]:
        assert set(group.get("exclude-patterns", [])) >= MISE_PINNED_HOOKS, group
    # uv inside Dockerfiles moves in lockstep with mise.toml + workflows, so keep it separated
    # from python/alpine bumps in the docker ecosystem.
    (docker_eco,) = [u for u in updates if u["package-ecosystem"] == "docker"]
    docker_groups = list(docker_eco["groups"].values())
    assert next(iter(docker_eco["groups"])) == "docker-with-mise-pins"
    assert set(docker_groups[0]["patterns"]) == {"ghcr.io/astral-sh/uv"}
    for group in docker_groups[1:]:
        assert "ghcr.io/astral-sh/uv" in set(group.get("exclude-patterns", [])), group
    # The auto-merge group must name exactly the actions that the four required jobs run.
    workflows = _load_workflows()
    required_workflow_actions = {
        str(step["uses"]).split("@", 1)[0]
        for file_name in ("ci.yml", "security.yml")
        for job_id, job in workflows[file_name]["jobs"].items()
        if str(job.get("name", job_id)) in REQUIRED_CHECKS
        for step in job.get("steps", [])
        if "uses" in step
    }
    (actions,) = [u for u in updates if u["package-ecosystem"] == "github-actions"]
    assert next(iter(actions["groups"])) == "actions-in-required-checks"
    assert (
        set(actions["groups"]["actions-in-required-checks"]["patterns"])
        == required_workflow_actions
    )


def test_dependabot_auto_merge_is_scoped_and_spoof_proof() -> None:
    workflow_text = (WORKFLOWS_DIR / "dependabot-auto-merge.yml").read_text(encoding="utf-8")
    doc = _load_workflows()["dependabot-auto-merge.yml"]
    assert set(_triggers(doc)) == {"pull_request"}
    (job,) = doc["jobs"].values()
    # The pull request author, not the actor of the triggering event, identifies Dependabot,
    # and the job condition is a conjunction, so no clause can widen it.
    assert "||" not in job["if"]
    assert "github.event.pull_request.user.login == 'dependabot[bot]'" in job["if"].split(" && ")
    assert "github.actor" not in workflow_text
    assert job["permissions"] == {"contents": "write", "pull-requests": "write"}
    assert len(job["steps"]) == 2
    metadata, merge_step = job["steps"]
    assert str(metadata["uses"]).startswith("dependabot/fetch-metadata@")
    assert metadata["id"] == "metadata"
    # Default verification: fetch-metadata checks that dependabot[bot] authored the pull request
    # and its first commit, and that commit's signature; the skip inputs turn those checks off.
    assert not {"skip-commit-verification", "skip-verification"} & set(metadata.get("with", {}))
    assert merge_step["run"] == 'gh pr merge --auto --squash "$PR_URL"'
    # Exactly one group, patch or minor only: no other group, ecosystem or major can qualify.
    condition = " ".join(str(merge_step["if"]).split())
    assert condition == (
        "steps.metadata.outputs.dependency-group == 'actions-in-required-checks'"
        " && (steps.metadata.outputs.update-type == 'version-update:semver-patch'"
        " || steps.metadata.outputs.update-type == 'version-update:semver-minor')"
    )
    dependabot = yaml.safe_load(
        (REPO_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    )
    (actions,) = [u for u in dependabot["updates"] if u["package-ecosystem"] == "github-actions"]
    assert "actions-in-required-checks" in actions["groups"]


def test_dependency_review_runs_on_every_pull_request() -> None:
    doc = _load_workflows()["dependency-review.yml"]
    triggers = _triggers(doc)
    assert set(triggers) == {"pull_request"}
    assert not set(triggers["pull_request"]) & {"paths", "paths-ignore", "branches-ignore", "types"}
    assert "main" in triggers["pull_request"].get("branches", ["main"])
    (job_id,) = doc["jobs"]
    job = doc["jobs"][job_id]
    assert job["name"] == "Dependency Review"
    # A skipped or tolerated job reports success, which a check that may become required
    # must not do.
    assert not {"if", "needs", "strategy", "continue-on-error"} & set(job)
    (review,) = [
        step
        for step in job["steps"]
        if str(step.get("uses", "")).startswith("actions/dependency-review-action@")
    ]
    assert not {"if", "continue-on-error"} & set(review)
    inputs = review.get("with", {})
    assert inputs.get("fail-on-severity") in {"low", "moderate", "high", "critical"}
    # warn-only overrides fail-on-severity; vulnerability-check: false skips the check itself;
    # a config-file can set either unseen; fail-on-scopes and allow-ghsas narrow what fails.
    assert not {
        "warn-only",
        "vulnerability-check",
        "config-file",
        "fail-on-scopes",
        "allow-ghsas",
    } & set(inputs)


def test_release_attests_build_provenance_and_sbom() -> None:
    jobs = _load_workflows()["release.yml"]["jobs"]
    attest = jobs["attest"]
    assert attest["needs"] == ["verify-and-package"]
    assert not {"if", "continue-on-error"} & set(attest)
    assert attest["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "attestations": "write",
    }
    # The signing job builds nothing: it attests the assets the verified job uploaded.
    uses = [str(step.get("uses", "")) for step in attest["steps"]]
    assert not any(u.startswith(("actions/checkout@", "astral-sh/setup-uv@")) for u in uses)
    attest_steps = [
        step for step in attest["steps"] if str(step.get("uses", "")).startswith("actions/attest@")
    ]
    assert len(attest_steps) == 2
    assert not any({"if", "continue-on-error"} & set(step) for step in attest_steps)
    # No predicate inputs: actions/attest generates SLSA build provenance for every asset
    # listed in SHA256SUMS.txt.
    assert attest_steps[0]["with"] == {"subject-checksums": "dist/SHA256SUMS.txt"}
    assert attest_steps[1]["with"]["sbom-path"] == "dist/noesis.sbom.cdx.json"
    assert "attest" in jobs["draft-release"]["needs"]
    for job_id, job in jobs.items():
        if job_id != "attest":
            assert "id-token" not in job["permissions"], job_id


def test_zizmor_audits_the_repository_root() -> None:
    jobs = _load_workflows()["security.yml"]["jobs"]

    def zizmor_inputs(job_id: str) -> dict[str, Any]:
        (step,) = [
            s for s in jobs[job_id]["steps"] if str(s.get("uses", "")).startswith("zizmorcore/")
        ]
        assert not {"if", "continue-on-error"} & set(step)
        return step["with"]

    required = zizmor_inputs("zizmor")
    assert required["inputs"] == "."
    assert required["online-audits"] == "false"
    # Online audits call the GitHub API, so they run on a schedule in a job that is not required
    # and that no pull_request event can start.
    online_job = jobs["zizmor-online"]
    assert online_job["name"] not in REQUIRED_CHECKS
    assert online_job["if"] == (
        "github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'"
    )
    online = zizmor_inputs("zizmor-online")
    assert online["inputs"] == "."
    assert online["online-audits"] == "true"
    assert online["version"] == required["version"]
    justfile = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    assert re.search(r"(?m)^\s+zizmor --offline --persona=pedantic \.\s*$", justfile)


def test_trivy_scanners_match_between_ci_and_just() -> None:
    jobs = _load_workflows()["security.yml"]["jobs"]

    def trivy_inputs(job_id: str) -> list[dict[str, Any]]:
        return [
            dict(s["with"])
            for s in jobs[job_id]["steps"]
            if str(s.get("uses", "")).startswith("aquasecurity/trivy-action@")
        ]

    # The required scan names its scanners: an empty input would silently follow Trivy's defaults.
    (required,) = trivy_inputs("trivy")
    assert required["scanners"] == "vuln,secret"
    ci_scanners = set(str(required["scanners"]).split(","))
    for license_scan in trivy_inputs("trivy-license-audit"):
        assert license_scan["scanners"] == "license"
    justfile = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    match = re.search(
        r"(?m)^trivy:\n\s+trivy fs --scanners (\S+) --severity (\S+)"
        r" --exit-code 1 --skip-dirs (\S+) \.$",
        justfile,
    )
    assert match is not None
    # `just trivy` runs what the required check runs, plus misconfig, which only runs locally.
    assert set(match[1].split(",")) == ci_scanners | {"misconfig"}
    assert match[2] == required["severity"]
    assert match[3] == required["skip-dirs"]


def test_pyproject_caps_agree_with_dependabot_holds() -> None:
    """ADR-0032 §4: upper-bounded caps in pyproject.toml carry dated reasons and holds."""
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    pyproject = tomllib.loads(pyproject_text)
    dependabot = yaml.safe_load(
        (REPO_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    )
    (uv_upd,) = [u for u in dependabot["updates"] if u["package-ecosystem"] == "uv"]
    (docker_upd,) = [u for u in dependabot["updates"] if u["package-ecosystem"] == "docker"]
    uv_holds = {str(entry["dependency-name"]): entry for entry in uv_upd.get("ignore", [])}
    docker_holds = {str(entry["dependency-name"]): entry for entry in docker_upd.get("ignore", [])}

    # Upper-bounded requires-python must match the docker python hold.
    requires_python = str(pyproject["project"]["requires-python"])
    assert "<" in requires_python
    assert "python" in docker_holds
    assert "version-update:semver-minor" in docker_holds["python"].get("update-types", [])

    # Every dependency with an upper bound (`<` or `<=`) across project/extras/groups must carry
    # a dated rationale comment in pyproject.toml and a matching uv hold in dependabot.yml.
    all_specs: list[str] = [str(s) for s in pyproject["project"]["dependencies"]]
    for extra_specs in pyproject["project"].get("optional-dependencies", {}).values():
        all_specs.extend(str(s) for s in extra_specs)
    for group_specs in pyproject.get("dependency-groups", {}).values():
        all_specs.extend(str(s) for s in group_specs if isinstance(s, str))

    capped_deps: dict[str, str] = {}
    for spec in all_specs:
        req = Requirement(spec)
        if any(s.operator in {"<", "<="} for s in req.specifier):
            capped_deps[req.name.lower()] = spec
    assert {"mcp", "numpy"} <= set(capped_deps)

    lines = pyproject_text.splitlines()
    for pkg_name, full_spec in capped_deps.items():
        assert pkg_name in uv_holds, f"pyproject.toml cap {full_spec!r} missing dependabot uv hold"
        spec_idx = next(i for i, line in enumerate(lines) if f'"{full_spec}"' in line)
        assert spec_idx > 0
        reason_line = lines[spec_idx - 1].strip()
        reason = re.fullmatch(r"#\s*(\d{4}-\d{2}-\d{2}): (\S.*)", reason_line)
        assert reason is not None, f"missing dated rationale above {full_spec!r}: {reason_line!r}"
        date.fromisoformat(reason[1])
        assert pkg_name in reason[2]
        assert "https://" in reason[2]
        assert "re-open when" in reason[2]


def test_crosshair_pin_matches_across_justfile_ci_and_release() -> None:
    """ADR-0032 §4: crosshair-tool exact pin matches across justfile, ci.yml, and release.yml."""
    pin_re = re.compile(r"crosshair-tool==(\d+(?:\.\d+)+)")
    just_pins = set(pin_re.findall((REPO_ROOT / "justfile").read_text(encoding="utf-8")))
    ci_pins = set(pin_re.findall((WORKFLOWS_DIR / "ci.yml").read_text(encoding="utf-8")))
    release_pins = set(pin_re.findall((WORKFLOWS_DIR / "release.yml").read_text(encoding="utf-8")))
    assert len(just_pins) == 1, just_pins
    assert ci_pins == just_pins, (ci_pins, just_pins)
    assert release_pins == just_pins, (release_pins, just_pins)


# ADR-0032 §4 (M-R08): an image tag names a full patch release, [v]major.minor.patch, optionally
# with a letter suffix on the patch (ib-gateway's 10.50.1h) and a -variant (-slim-bookworm).
EXACT_IMAGE_TAG_RE = re.compile(r"v?\d+\.\d+\.\d+(?:[a-z][0-9a-z]*)?(?:-[A-Za-z0-9._-]+)?")


def test_compose_images_have_no_unpinned_floating_tags() -> None:
    """ADR-0032 §4: external images in docker/compose*.yml name a full patch release or a digest."""
    compose_files = sorted((REPO_ROOT / "docker").glob("compose*.yml"))
    assert compose_files
    checked_external = 0
    for compose_path in compose_files:
        doc = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
        for svc_name, svc in doc.get("services", {}).items():
            image = str(svc.get("image", ""))
            assert image, (compose_path.name, svc_name)
            if image.startswith("tier0-sota-"):
                continue
            checked_external += 1
            if "@sha256:" in image:
                assert re.search(r"@sha256:[0-9a-f]{64}$", image), (compose_path.name, svc_name)
                continue
            assert ":" in image, (compose_path.name, svc_name, image)
            tag = image.rsplit(":", 1)[1]
            assert tag.lower() not in {"latest", "stable", "edge", "nightly", "main", "master"}, (
                compose_path.name,
                svc_name,
                image,
            )
            assert EXACT_IMAGE_TAG_RE.fullmatch(tag), (
                compose_path.name,
                svc_name,
                image,
            )
    assert checked_external >= 5


def test_scheduled_drift_audit_and_release_verification_parity() -> None:
    """Scheduled CI drift job is read-only & non-required; release.yml mirrors ci.yml gates."""
    workflows = _load_workflows()

    # 1. ci.yml scheduled/dispatch read-only drift & compose audit job.
    ci_jobs = workflows["ci.yml"]["jobs"]
    drift_job = ci_jobs["upstream-drift-and-compose-audit"]
    assert drift_job["name"] not in REQUIRED_CHECKS
    assert drift_job["if"] == (
        "github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'"
    )
    assert drift_job["permissions"] == {"contents": "read"}
    drift_runs = "\n".join(str(step.get("run", "")) for step in drift_job["steps"])
    assert "docker compose -f docker/compose.sota.yml config -q" in drift_runs
    assert "docker compose -f docker/compose.ibkr-paper.yml config -q" in drift_runs
    assert "uv tree --frozen --outdated" in drift_runs
    assert "_evaluate_upstream_watch_triggers" in drift_runs
    (report_step,) = [
        step
        for step in drift_job["steps"]
        if "uv tree --frozen --outdated" in str(step.get("run", ""))
    ]
    assert report_step.get("shell") == "bash"
    assert "set -euo pipefail" in str(report_step["run"])

    # 2. release.yml verify-and-package mirrors ci.yml (zensical strict, Inspect AI 7/7, CrossHair).
    rel_job = workflows["release.yml"]["jobs"]["verify-and-package"]
    rel_runs = "\n".join(str(step.get("run", "")) for step in rel_job["steps"])
    assert "zensical build --clean --strict" in rel_runs
    assert "verify_inspect_safety_suite" in rel_runs
    assert "crosshair check --per_condition_timeout=10 evals/symbolic_risk_crosshair.py" in rel_runs


def test_systemd_workstation_guard_pytest_hygiene_safety() -> None:
    """Workstation guard unit escapes $$, guards pytest-current, and checks active pytest."""
    unit_path = REPO_ROOT / "docker" / "systemd" / "tier0-workstation-sota-guard.service"
    raw = unit_path.read_bytes()
    assert b"\r\n" not in raw, "systemd unit must use LF line endings"
    text = raw.decode("utf-8")
    assert "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847" in text
    for live_port in ("4001", "7496", "8798", "8796"):
        assert live_port in text
    assert 'd="/tmp/pytest-of-$$(id -un)"' in text
    assert 'readlink -f "$$d/pytest-current"' in text
    assert 'pgrep -u "$$(id -u)" -f "bin/[p]ytest|-m [p]ytest"' in text
    assert 'chmod -R u+w "$$p"' in text
    assert 'name "garbage-*"' in text
    assert 'name "pytest-[0-9]*"' in text


def test_dockerfile_images_pin_an_exact_version() -> None:
    """ADR-0032 §4: every registry image a docker/Dockerfile* builds or copies from, or loads as its
    `# syntax=` frontend, names a full patch release or a digest; build stages, scratch and the
    local tier0-sota-* builds excepted."""
    from_re = r"(?mi)^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)"
    dockerfiles = sorted((REPO_ROOT / "docker").glob("Dockerfile*"))
    assert dockerfiles
    for path in dockerfiles:
        text = path.read_text(encoding="utf-8")
        stages = {stage.lower() for _, stage in re.findall(from_re + r"\s+AS\s+(\S+)", text)}
        stages.add("scratch")
        refs = re.findall(from_re, text)
        refs += re.findall(r"(?mi)^\s*COPY\b[^\n]*?--from=(\S+)", text)
        refs += re.findall(r"--mount=\S*?\bfrom=([^,\s]+)", text)
        refs += re.findall(r"(?mi)^#\s*syntax\s*=\s*(\S+)", text)
        assert refs, path.name
        for ref in refs:
            if ref.lower() in stages or ref.isdigit() or ref.startswith("tier0-sota-"):
                continue
            if "@sha256:" in ref:
                assert re.search(r"@sha256:[0-9a-f]{64}$", ref), (path.name, ref)
                continue
            last = ref.rsplit("/", 1)[-1]
            assert ":" in last, (path.name, ref)
            assert EXACT_IMAGE_TAG_RE.fullmatch(last.rsplit(":", 1)[1]), (path.name, ref)


def _extract_just_os_recipe(just_text: str, name: str, os_name: str) -> str:
    """Return the dedented body of a `[unix]`/`[windows]` justfile recipe."""
    lines = just_text.splitlines()
    for idx, line in enumerate(lines[:-1]):
        if line.strip() == f"[{os_name}]" and re.match(
            rf"^{re.escape(name)}(?: [^:]*)?:", lines[idx + 1]
        ):
            body: list[str] = []
            for nxt in lines[idx + 2 :]:
                if nxt and not nxt.startswith("    "):
                    break
                body.append(nxt)
            return textwrap.dedent("\n".join(body))
    raise AssertionError(f"missing [{os_name}] recipe {name!r} in justfile")


def _extract_pyeof_heredoc(script_text: str) -> str:
    match = re.search(r"<<'PYEOF'\n(.*?)\nPYEOF", script_text, flags=re.DOTALL)
    assert match is not None, "missing PYEOF heredoc"
    return textwrap.dedent(match.group(1)).strip()


def test_lock_proof_catches_downgrades_and_matches_ci(tmp_path: Path) -> None:
    """ADR-0032 §4: lock-proof matches CI PR proof and fails closed on any package downgrade."""
    just_text = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    unix_body = _extract_just_os_recipe(just_text, "lock-proof", "unix")
    win_body = _extract_just_os_recipe(just_text, "lock-proof", "windows")
    unix_py = _extract_pyeof_heredoc(unix_body)

    # Verify non-required read-only CI job in ci.yml runs the exact same no-downgrade script.
    ci_jobs = _load_workflows()["ci.yml"]["jobs"]
    pr_job = ci_jobs["pr-lock-and-compose-proof"]
    assert pr_job["name"] not in REQUIRED_CHECKS
    assert pr_job["if"] == "github.event_name == 'pull_request'"
    assert pr_job["permissions"] == {"contents": "read"}
    (checkout_step,) = [
        step
        for step in pr_job["steps"]
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]
    assert checkout_step.get("with", {}).get("fetch-depth") == 0
    assert checkout_step.get("with", {}).get("persist-credentials") is False
    pr_runs = "\n".join(str(step.get("run", "")) for step in pr_job["steps"])
    assert "docker compose -f docker/compose.sota.yml config -q" in pr_runs
    assert "docker compose -f docker/compose.ibkr-paper.yml config -q" in pr_runs
    (lock_step,) = [
        step for step in pr_job["steps"] if "LOCK_PROOF_OK=1" in str(step.get("run", ""))
    ]
    assert lock_step.get("shell") == "bash"
    assert lock_step.get("env", {}).get("BASE_SHA") == "${{ github.event.pull_request.base.sha }}"
    assert "set -euo pipefail" in str(lock_step["run"])
    assert "2>&1 <<'PYEOF'" in str(lock_step["run"])
    ci_py = _extract_pyeof_heredoc(str(lock_step["run"]))
    assert ci_py == unix_py, "ci.yml lock-proof script drifted from justfile [unix] lock-proof"

    # Extract Windows -c one-liner so both OS implementations are empirically verified.
    win_match = re.search(r"python -c '([^']+)' '\{\{base\}\}'", win_body)
    assert win_match is not None, "missing Windows lock-proof python -c command"
    win_py = win_match.group(1)

    git = shutil.which("git")
    assert git is not None
    repo = tmp_path / "lock-repo"
    repo.mkdir()
    base_lock = (
        "version = 1\n\n"
        '[[package]]\nname = "hftbacktest"\nversion = "2.4.4"\n\n'
        '[[package]]\nname = "nautilus-trader"\nversion = "2.0.0rc9"\n\n'
        '[[package]]\nname = "numpy"\nversion = "2.2.6"\n'
    )
    (repo / "uv.lock").write_text(base_lock, encoding="utf-8")
    for cmd in (
        [git, "init", "-q"],
        [git, "config", "user.email", "test@example.com"],
        [git, "config", "user.name", "Test"],
        [git, "add", "uv.lock"],
        [git, "commit", "-q", "-m", "base"],
    ):
        subprocess.run(cmd, cwd=repo, check=True)
    base_sha = subprocess.run(
        [git, "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()

    def run_both(base_ref: str) -> list[subprocess.CompletedProcess[str]]:
        return [
            subprocess.run(
                [sys.executable, "-c", code, base_ref],
                cwd=repo,
                capture_output=True,
                text=True,
                check=False,
            )
            for code in (unix_py, win_py)
        ]

    # 1. Identical lockfile -> pass.
    for res in run_both(base_sha):
        assert res.returncode == 0, res.stderr
        assert "LOCK_PROOF_OK=1" in res.stdout

    # 2. Pure upgrade (including rc -> final) -> pass.
    (repo / "uv.lock").write_text(
        base_lock.replace('version = "2.0.0rc9"', 'version = "2.0.0"'), encoding="utf-8"
    )
    for res in run_both(base_sha):
        assert res.returncode == 0, res.stderr
        assert "upgraded=1 downgraded=0" in res.stdout

    # 3. PR #2-class silent downgrade (hftbacktest 2.4.4 -> 2.4.2) -> fail closed.
    (repo / "uv.lock").write_text(
        base_lock.replace('version = "2.4.4"', 'version = "2.4.2"'), encoding="utf-8"
    )
    for res in run_both(base_sha):
        assert res.returncode != 0
        assert "LOCK_PROOF_FAIL downgraded=1" in res.stderr
        payload = json.loads(res.stdout)
        assert payload["downgraded"] == {"hftbacktest": {"base": ["2.4.4"], "head": ["2.4.2"]}}

    # 4. Prerelease downgrade (2.0.0rc9 -> 2.0.0rc8) -> fail closed.
    (repo / "uv.lock").write_text(
        base_lock.replace('version = "2.0.0rc9"', 'version = "2.0.0rc8"'), encoding="utf-8"
    )
    for res in run_both(base_sha):
        assert res.returncode != 0
        assert "LOCK_PROOF_FAIL downgraded=1" in res.stderr

    # 5. Invalid base ref -> fail closed (zero silent fallback).
    for res in run_both("nonexistent-base-ref"):
        assert res.returncode != 0
        assert "cannot read nonexistent-base-ref:uv.lock" in res.stderr


def test_docker_proof_and_prod_sync_recipes_are_fail_closed() -> None:
    """ADR-0032 §4: docker-proof refuses :latest; prod-sync enforces main/clean/risk/ports."""
    just_text = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    assert re.search(r"(?m)^docker-proof tag=\"pr-proof\": compose-config$", just_text)
    for os_name in ("unix", "windows"):
        body = _extract_just_os_recipe(just_text, "docker-proof", os_name)
        assert "latest" in body, os_name
        for dockerfile in (
            "docker/Dockerfile.research",
            "docker/Dockerfile.sandbox",
            "docker/Dockerfile.xerj",
        ):
            assert dockerfile in body, (os_name, dockerfile)
        assert "DOCKER_PROOF_OK=1" in body, os_name

    prod_body = _extract_just_os_recipe(just_text, "prod-sync", "unix")
    assert "set -euo pipefail" in prod_body
    assert 'ss_out="$(ss -lntH)"' in prod_body
    assert "ss -lntH | grep" not in prod_body
    for port in ("4001", "7496", "8798", "8796", "9223"):
        assert port in prod_body, port
    assert "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847" in prod_body
    pre_merge_risk = 'git -C "$repo" show "$after:src/tier0_sota/risk.py"'
    assert pre_merge_risk in prod_body
    assert prod_body.index(pre_merge_risk) < prod_body.index("merge --ff-only origin/main")
    assert prod_body.index(pre_merge_risk) < prod_body.index("PROD_SYNC_DRY_RUN_OK=1")
    assert "merge --ff-only origin/main" in prod_body
    assert "--force-recreate tearsheets" in prod_body
    assert "dry-run" in prod_body
    assert "preflight_resume.sh" in prod_body
    assert "s5-panel-chain" not in prod_body


def test_bootstrap_env_orders_mise_shims_before_local_bin() -> None:
    """Orca worktree setup resolves mise-pinned uv/zizmor/syft ahead of ~/.local/bin."""
    script = (REPO_ROOT / "scripts" / "bootstrap_env.sh").read_text(encoding="utf-8")
    match = re.search(r'(?m)^export PATH="([^"]+)"$', script)
    assert match is not None
    path_expr = match.group(1)
    assert "$HOME/.local/share/mise/shims" in path_expr
    assert "$HOME/.local/bin" in path_expr
    assert path_expr.index("$HOME/.local/share/mise/shims") < path_expr.index("$HOME/.local/bin")

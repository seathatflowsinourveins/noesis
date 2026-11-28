"""ADR-0032 fitness checks: landing through pull requests and upstream-native maintenance.

Offline only: these tests read committed files (workflows, the ruleset snapshot, mise.toml,
.pre-commit-config.yaml). The live ruleset is compared with the snapshot by a read-only
`gh api` call outside pytest, because that needs the network.
"""

from __future__ import annotations

import json
import re
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


def test_compose_images_have_no_unpinned_floating_tags() -> None:
    """ADR-0032 §4: external images in docker/compose*.yml must never use floating tags."""
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
            assert re.fullmatch(r"v?\d+\.\d+(?:\.\d+)?(?:-[A-Za-z0-9._-]+)?", tag), (
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

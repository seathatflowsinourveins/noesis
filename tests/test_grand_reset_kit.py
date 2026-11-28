"""Verification tests for the Zero-Hook Disk-Z + WSL2 Grand-Reset kit, ADRs & Docker."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE_PORTS = ("4001", "7496", "8798", "8796")
MADR_SECTIONS = (
    "## Context and Problem Statement",
    "## Decision Drivers",
    "## Considered Options",
    "## Decision Outcome",
    "### Confirmation",
    "## Consequences",
    "## Pros and Cons of the Options",
    "## More Information",
)
BANNED_IMPORT_RE = re.compile(
    r"(?m)^\s*(?:import|from)\s+(?:yfinance|backtrader|vectorbt|talib|mlfinlab)\b"
)
LIVE_ENABLE_RE = re.compile(r"live_execution_enabled\s*[:=]\s*(?:true|True|1)\b")
ISOLATED_CLI_SPEC_RE = re.compile(r"--(?:from|with)[ =](\S+)")
EXACT_PYPI_PIN_RE = re.compile(r"[A-Za-z0-9._-]+==\d+(?:\.\d+)+")


def test_zero_self_invented_hooks_and_native_uv_mcps() -> None:
    assert not (REPO_ROOT / ".codex" / "hooks.json").exists()

    claude_cfg = json.loads((REPO_ROOT / ".claude" / "settings.json").read_text("utf-8"))
    gemini_cfg = json.loads((REPO_ROOT / ".gemini" / "settings.json").read_text("utf-8"))
    assert "hooks" not in claude_cfg and "hooks" not in gemini_cfg

    codex_cfg = tomllib.loads((REPO_ROOT / ".codex" / "config.toml").read_text("utf-8"))
    assert "hooks" not in codex_cfg
    assert codex_cfg.get("features", {}).get("hooks", False) is False
    for srv in ("nautilus-engine", "edgartools"):
        assert codex_cfg["mcp_servers"][srv]["command"] == "uv"

    mcp_root = json.loads((REPO_ROOT / ".mcp.json").read_text("utf-8"))["mcpServers"]
    opencode_mcp = json.loads((REPO_ROOT / "opencode.json").read_text("utf-8"))["mcp"]
    for cfg_map in (mcp_root, claude_cfg["mcpServers"], gemini_cfg["mcpServers"]):
        for srv in ("nautilus-engine", "edgartools"):
            assert cfg_map[srv]["command"] == "uv"
            assert "bash" not in cfg_map[srv]["command"]
    for srv in ("nautilus-engine", "edgartools"):
        assert opencode_mcp[srv]["command"][0] == "uv"

    for p in (
        ".mcp.json",
        ".claude/settings.json",
        ".gemini/settings.json",
        ".codex/config.toml",
        "opencode.json",
    ):
        assert "bash -c" not in (REPO_ROOT / p).read_text(encoding="utf-8")


def test_few_line_harnesses_mcp_lanes_and_sota_manifest() -> None:
    agent_files = sorted((REPO_ROOT / ".claude" / "agents").glob("*.md"))
    assert {p.name for p in agent_files} >= {
        "tier0-grand-central.md",
        "pit-data-engineer.md",
        "quant-falsifier.md",
        "code-intel-redteam.md",
        "observability-operator.md",
        "memory-rag-evolution.md",
        "paper-gatekeeper.md",
    }
    for agent_p in agent_files:
        lines = agent_p.read_text(encoding="utf-8").splitlines()
        assert len(lines) <= 20, f"{agent_p.name} exceeds 20 lines ({len(lines)})"

    gk = (REPO_ROOT / ".claude" / "agents" / "paper-gatekeeper.md").read_text(encoding="utf-8")
    assert "permissionMode: plan" in gk

    cmd_files = sorted((REPO_ROOT / ".claude" / "commands").glob("*.md"))
    assert {p.name for p in cmd_files} >= {
        "lane-sim.md",
        "lane-paper.md",
        "lane-tv.md",
        "lane-live.md",
        "grand-reset-verify.md",
    }
    for cmd_p in cmd_files:
        lines = cmd_p.read_text(encoding="utf-8").splitlines()
        assert len(lines) <= 12, f"{cmd_p.name} exceeds 12 lines ({len(lines)})"

    lanes_dir = REPO_ROOT / ".claude" / "mcp-lanes"
    for lane in ("sim", "paper", "tv", "gate"):
        lp = lanes_dir / f"{lane}.mcp.json"
        assert lp.is_file(), f"Missing scoped lane config {lp}"
        doc = json.loads(lp.read_text(encoding="utf-8"))
        assert "mcpServers" in doc and len(doc["mcpServers"]) >= 2

    assert (REPO_ROOT / ".opencode" / "commands" / "verify-tier0.md").is_file()

    sota = json.loads((REPO_ROOT / "SOTA_STARRED_AND_BEYOND_MANIFEST.json").read_text("utf-8"))
    assert sota["schema_version"] == "tier0.sota-starred-beyond-manifest.v1"
    assert sota["zero_self_invented_hooks"] is True
    assert {f"L{i}" for i in range(11)}.issubset(sota["active_winners_by_layer"])

    lock = json.loads((REPO_ROOT / "UPSTREAM_SKILLS_LOCK.json").read_text("utf-8"))
    assert lock["skills_count"] == len(lock["skills"]) == 46
    for entry in lock["skills"]:
        assert len(entry["skill_sha256"]) == 64
        assert (REPO_ROOT / ".claude" / "skills" / entry["skill"] / "SKILL.md").is_file()

    agy_skills_cfg = REPO_ROOT / ".agents" / "skills.json"
    assert agy_skills_cfg.is_file(), "Missing .agents/skills.json"
    agy_skills_doc = json.loads(agy_skills_cfg.read_text("utf-8"))
    assert {"path": ".claude/skills"} in agy_skills_doc.get("entries", [])


def test_zero_legacy_references_and_zero_yfinance_in_uv_lock() -> None:
    uv_lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    assert "yfinance" not in uv_lock.lower(), "uv.lock must contain 0 yfinance entries"

    legacy_rx = re.compile(
        r"tier0-quant(?!-falsification)|sota_compounding_1000rpm_harness|"
        r"CLEAN LIVE|sota-live-tv-harness"
    )
    targets: list[Path] = [REPO_ROOT / "pyproject.toml"]
    targets.extend((REPO_ROOT / "scripts" / "grand_reset").rglob("*"))
    targets.extend((REPO_ROOT / "docs").rglob("*"))
    targets.extend((REPO_ROOT / "src").rglob("*.py"))

    for path in targets:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        hit = legacy_rx.search(text)
        assert hit is None, f"Stale legacy token {hit.group(0)!r} found in {path}"


def test_madr_adrs_catalog_and_zensical_toml() -> None:
    z_cfg = tomllib.loads((REPO_ROOT / "zensical.toml").read_text(encoding="utf-8"))
    assert z_cfg["project"]["docs_dir"] == "docs"
    assert (REPO_ROOT / "docs" / "index.md").is_file()
    catalog = (REPO_ROOT / "docs" / "adr" / "CATALOG.md").read_text(encoding="utf-8")
    assert "sole source of truth" in catalog.lower()
    assert "superseded by adr-" in catalog.lower()

    adr_paths = sorted((REPO_ROOT / "docs" / "adr").glob("ADR-*.md"))
    assert len(adr_paths) >= 25
    assert (REPO_ROOT / "docs" / "adr" / "ADR-0022.md").is_file()
    assert (REPO_ROOT / "docs" / "adr" / "ADR-0023.md").is_file()
    assert (REPO_ROOT / "docs" / "adr" / "ADR-0024.md").is_file()
    assert (REPO_ROOT / "docs" / "adr" / "ADR-0025.md").is_file()
    nav_targets = [next(iter(item.values())) for item in z_cfg["project"]["nav"]]
    for adr_path in adr_paths:
        assert f"adr/{adr_path.name}" in nav_targets, f"{adr_path.name} missing from zensical.toml"
        assert adr_path.name in catalog, f"{adr_path.name} missing from CATALOG.md"
        body = adr_path.read_text(encoding="utf-8")
        for sec in MADR_SECTIONS:
            assert sec in body, f"{adr_path.name} missing MADR section {sec}"
        assert not BANNED_IMPORT_RE.search(body), f"{adr_path.name} endorses banned import"
        assert not LIVE_ENABLE_RE.search(body), f"{adr_path.name} enables live execution"


def test_docker_compose_sota_yaml_mounts_and_no_live_ports() -> None:
    compose_path = REPO_ROOT / "docker" / "compose.sota.yml"
    doc = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict) and "services" in doc
    services = doc["services"]
    assert len(services) >= 10

    raw_text = compose_path.read_text(encoding="utf-8")
    assert "/mnt/z/sota-data" in raw_text
    for bad in ("sk-", "PK", "AKIA", "-----BEGIN"):
        assert bad not in raw_text

    for svc_name, svc in services.items():
        for port_entry in svc.get("ports", []):
            host_container = str(port_entry).split(":")
            for bad_port in LIVE_PORTS:
                assert bad_port not in host_container, (
                    f"{svc_name} binds forbidden live port {bad_port}"
                )

    finos_svc = services["finos-workspace"]
    assert "command" in finos_svc and finos_svc["command"], "finos-workspace must declare a command"
    assert all(":6900" not in str(p) for p in finos_svc.get("ports", [])), (
        "finos-workspace must not collide with WSL2 tier0-openbb-backend :6900"
    )
    assert services["tearsheets"]["image"] == "caddy:2.11.9-alpine"
    assert services["telemetry-exporter"]["image"] == "otel/opentelemetry-collector-contrib:0.166.0"
    assert services["prometheus"]["image"] == "prom/prometheus:v3.16.0"
    assert services["grafana"]["image"] == "grafana/grafana:13.3.2"
    assert (
        "./prometheus_rules.yml:/etc/prometheus/prometheus_rules.yml:ro"
        in services["prometheus"]["volumes"]
    )

    am_svc = services["alertmanager"]
    # Exact release tag (optionally digest-pinned), never a floating tag such as latest or v0.
    assert re.fullmatch(
        r"prom/alertmanager:v\d+\.\d+\.\d+(?:@sha256:[0-9a-f]{64})?", am_svc["image"]
    ), am_svc["image"]
    assert am_svc["profiles"] == ["alerting"]
    assert am_svc["cpus"] == "0.25"
    assert am_svc["mem_limit"] == "128m"
    assert am_svc["ports"] == ["127.0.0.1:9093:9093"]
    assert "GOMAXPROCS=1" in am_svc["environment"]
    assert "./alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro" in am_svc["volumes"]
    assert am_svc["restart"] == "unless-stopped"

    # Loopback-only host publication for unauthenticated local services (xerj, grafana).
    for local_svc in ("xerj", "grafana"):
        svc_ports = services[local_svc].get("ports", [])
        assert svc_ports and all(str(p).startswith("127.0.0.1:") for p in svc_ports), (
            f"{local_svc} must bind host ports strictly to 127.0.0.1: {svc_ports}"
        )

    # D6 exact tag-pin enforcement for docker/Dockerfile.xerj (alpine + XERJ_VERSION + sha256).
    xerj_df = (REPO_ROOT / "docker" / "Dockerfile.xerj").read_text(encoding="utf-8")
    xerj_froms = re.findall(r"(?m)^FROM\s+(\S+)", xerj_df)
    assert len(xerj_froms) >= 2, f"Expected multi-stage alpine FROM lines, got: {xerj_froms}"
    for img in xerj_froms:
        assert re.fullmatch(r"alpine:\d+\.\d+\.\d+(?:@sha256:[0-9a-f]{64})?", img), (
            f"docker/Dockerfile.xerj must pin exact alpine tag (never latest): {img}"
        )
    xerj_ver_match = re.search(r"(?m)^ARG\s+XERJ_VERSION=(\S+)\s*$", xerj_df)
    assert xerj_ver_match is not None, "docker/Dockerfile.xerj missing ARG XERJ_VERSION"
    xerj_ver = xerj_ver_match.group(1)
    assert re.fullmatch(r"\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?", xerj_ver), (
        f"docker/Dockerfile.xerj XERJ_VERSION must pin exact semver/rc tag: {xerj_ver}"
    )
    assert len(re.findall(r"expected_sha=[0-9a-f]{64}", xerj_df)) >= 2, (
        "docker/Dockerfile.xerj must pin amd64 and arm64 SHA-256 digests"
    )

    for rel in (
        "docker/Dockerfile.research",
        "docker/Dockerfile.sandbox",
        "docker/Dockerfile.xerj",
        "docker/prometheus.yml",
        "docker/prometheus_rules.yml",
        "docker/alertmanager.yml",
        "docker/dagster.yaml",
        "docker/grafana/dashboards/tier0-sota-observability.json",
        "docker/grafana/provisioning/datasources/prometheus.yml",
        "docker/grafana/provisioning/dashboards/provider.yml",
    ):
        assert (REPO_ROOT / rel).is_file(), f"Missing docker asset {rel}"

    prom_cfg = yaml.safe_load((REPO_ROOT / "docker" / "prometheus.yml").read_text("utf-8"))
    assert "/etc/prometheus/prometheus_rules.yml" in prom_cfg.get("rule_files", [])
    assert prom_cfg["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"] == [
        "alertmanager:9093"
    ]

    rules_raw = (REPO_ROOT / "docker" / "prometheus_rules.yml").read_text("utf-8")
    rules_doc = yaml.safe_load(rules_raw)
    rule_names = {r["alert"] for grp in rules_doc.get("groups", []) for r in grp.get("rules", [])}
    assert {
        "Tier0DailyDrawdownBreakerTripped",
        "Tier0PaperTelemetryOrExecutionDown",
        "Tier0PaperTcaEnvelopeBreach",
    }.issubset(rule_names)

    am_raw = (REPO_ROOT / "docker" / "alertmanager.yml").read_text("utf-8")
    for bad in ("sk-", "PK", "AKIA", "-----BEGIN", "hooks.slack.com", "https://"):
        assert bad not in am_raw
    am_doc = yaml.safe_load(am_raw)
    assert am_doc["route"]["receiver"] == "tier0-local-null"
    assert [r["name"] for r in am_doc["receivers"]] == ["tier0-local-null"]


def test_grand_reset_scripts_ps1_ast_dryrun_and_wsl_safety() -> None:
    gr_dir = REPO_ROOT / "scripts" / "grand_reset"
    ps1_files = sorted(gr_dir.glob("*.ps1"))
    expected_ps1 = {
        "00_clean_disk_z_and_agy_reset.ps1",
        "01_disk_z_and_wsl_setup.ps1",
        "02_runtimes_agy_orca_setup.ps1",
        "03_hosted_stack_tv_paper.ps1",
        "verify_grand_reset.ps1",
    }
    assert {p.name for p in ps1_files} == expected_ps1

    pwsh_exe = shutil.which("pwsh")
    if os.name == "nt":
        assert pwsh_exe is not None
    if pwsh_exe is not None:
        ps_script = (
            "$ErrorActionPreference='Stop'; "
            "foreach($f in @(" + ",".join(f"'{p}'" for p in ps1_files) + ")){"
            "$tok=$null; $err=$null; "
            "[void][System.Management.Automation.Language.Parser]::ParseFile($f,[ref]$tok,[ref]$err);"
            'if($err.Count -gt 0){ throw "Parse error in $f" }}'
        )
        proc = subprocess.run(
            [pwsh_exe, "-NoProfile", "-Command", ps_script],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr or proc.stdout

    for p in ps1_files:
        txt = p.read_text(encoding="utf-8")
        assert all(port in txt for port in LIVE_PORTS), f"{p.name} missing live-port guard"
        if p.name != "verify_grand_reset.ps1":
            assert "DryRun" in txt, f"{p.name} missing -DryRun switch"
            if os.name == "nt" and pwsh_exe is not None:
                dry = subprocess.run(
                    [
                        pwsh_exe,
                        "-NoProfile",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        str(p),
                        "-DryRun",
                    ],
                    cwd=str(REPO_ROOT),
                    capture_output=True,
                    text=True,
                    check=False,
                )
                assert dry.returncode == 0, f"{p.name} -DryRun failed: {dry.stderr or dry.stdout}"

    wsl_sh = (gr_dir / "01_wsl_bootstrap.sh").read_text(encoding="utf-8")
    for forbidden in ("mkfs", "rm -rf /mnt/z", "wsl --shutdown"):
        assert forbidden not in wsl_sh
    assert all(port in wsl_sh for port in LIVE_PORTS)


def test_preflight_resume_kit_scripts_and_playbook() -> None:
    ps1_path = REPO_ROOT / "scripts" / "preflight_resume.ps1"
    sh_path = REPO_ROOT / "scripts" / "preflight_resume.sh"
    playbook_path = REPO_ROOT / "docs" / "POST_RESTART_RESUME_PLAYBOOK.md"

    assert ps1_path.is_file(), "Missing scripts/preflight_resume.ps1"
    assert sh_path.is_file(), "Missing scripts/preflight_resume.sh"
    assert playbook_path.is_file(), "Missing docs/POST_RESTART_RESUME_PLAYBOOK.md"

    ps1_txt = ps1_path.read_text(encoding="utf-8")
    for token in (
        "-Reconcile",
        "-CleanLegacyLocked",
        "-Json",
        "Noesis-SOTA",
        "tier0-duckdb-ui",
        "tier0-openbb-backend",
        "tier0-ollama-docker-bridge",
        "tier0-openbb-docker-bridge",
        "otelcol_",
        "Caddy",
        "9fa037355c3942201e3c0813acaa7b01c590e6294cd5e85787ae1bdcef55a7b8",
        "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847",
        "seathatflowsinourveins/noesis",
        "workstation_preflight_resume_manifest.json",
        "tier0.workstation-preflight-resume.v1",
        "finish_post_restart_cleanup.ps1",
    ):
        assert token in ps1_txt, f"preflight_resume.ps1 missing required token {token!r}"
    assert all(port in ps1_txt for port in LIVE_PORTS)
    for forbidden in ("wsl --shutdown", "Restart-Computer", "Stop-Computer"):
        assert forbidden not in ps1_txt

    pwsh_exe = shutil.which("pwsh")
    if os.name == "nt":
        assert pwsh_exe is not None
    if pwsh_exe is not None:
        ast_cmd = (
            "$ErrorActionPreference='Stop'; "
            "$tok=$null; $err=$null; "
            "[void][System.Management.Automation.Language.Parser]::ParseFile("
            f"'{ps1_path}',[ref]$tok,[ref]$err); "
            "if($err.Count -gt 0){ throw ($err | Out-String) }"
        )
        proc = subprocess.run(
            [pwsh_exe, "-NoProfile", "-Command", ast_cmd],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, f"preflight_resume.ps1 AST error: {proc.stderr or proc.stdout}"

    sh_bytes = sh_path.read_bytes()
    assert b"\r\n" not in sh_bytes, "scripts/preflight_resume.sh must use LF line endings"
    sh_txt = sh_bytes.decode("utf-8")
    assert sh_txt.startswith("#!/usr/bin/env bash\n")
    for token in (
        "--reconcile",
        "--json",
        "Noesis-SOTA",
        "tier0-duckdb-ui",
        "tier0-openbb-backend",
        "tier0-ollama-docker-bridge",
        "tier0-openbb-docker-bridge",
        "otelcol_",
        "caddy",
        "9fa037355c3942201e3c0813acaa7b01c590e6294cd5e85787ae1bdcef55a7b8",
        "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847",
        "seathatflowsinourveins/noesis",
        "workstation_preflight_resume_manifest.json",
        "tier0.workstation-preflight-resume.v1",
        "WSL_AGY_MCP_PATH",
        "scope=wsl2_agy",
    ):
        assert token in sh_txt, f"preflight_resume.sh missing required token {token!r}"
    assert all(port in sh_txt for port in LIVE_PORTS)
    for forbidden in ("mkfs", "rm -rf /mnt/z", "wsl --shutdown"):
        assert forbidden not in sh_txt

    bash_exe = shutil.which("bash")
    if bash_exe is not None and not bash_exe.lower().endswith("\\system32\\bash.exe"):
        bash_proc = subprocess.run(
            [bash_exe, "-n", str(sh_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert bash_proc.returncode == 0, f"bash -n failed: {bash_proc.stderr or bash_proc.stdout}"

    shellcheck_exe = shutil.which("shellcheck")
    if shellcheck_exe is not None:
        sc_proc = subprocess.run(
            [shellcheck_exe, str(sh_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert sc_proc.returncode == 0, f"shellcheck failed: {sc_proc.stderr or sc_proc.stdout}"

    playbook_txt = playbook_path.read_text(encoding="utf-8")
    for token in (
        "just resume",
        "just preflight",
        "AGENTS.md",
        "LLM_HARNESS_MANIFEST.json",
        "PHILOSOPHY_COMPOUNDING_MANIFEST.json",
        "workstation_preflight_resume_manifest.json",
        "codebase-memory",
        "XERJ",
        "sim-backtest",
        "paper",
        "half-auto-tv",
        "live",
        "finish_post_restart_cleanup.ps1",
        "gh auth refresh",
    ):
        assert token in playbook_txt, f"POST_RESTART_RESUME_PLAYBOOK.md missing {token!r}"
    assert not BANNED_IMPORT_RE.search(playbook_txt)
    assert not LIVE_ENABLE_RE.search(playbook_txt)

    z_cfg = tomllib.loads((REPO_ROOT / "zensical.toml").read_text(encoding="utf-8"))
    nav_targets = [next(iter(item.values())) for item in z_cfg["project"]["nav"]]
    assert "POST_RESTART_RESUME_PLAYBOOK.md" in nav_targets
    assert "POST_RESTART_RESUME_PLAYBOOK.md" in (REPO_ROOT / "docs" / "index.md").read_text(
        encoding="utf-8"
    )

    just_txt = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    assert "preflight" in just_txt and "scripts/preflight_resume.ps1" in just_txt
    assert "resume" in just_txt and "scripts/preflight_resume.sh" in just_txt
    assert "account-status" in just_txt and "scripts/orca_account_failover.py" in just_txt
    assert "account-switch" in just_txt and "--apply" in just_txt


def test_orca_account_failover_and_ci_auth_policy() -> None:
    from scripts.orca_account_failover import evaluate_agent_failover

    # 1. Healthy active account (43% weekly, even with 80% fableWeekly) stays active cleanly
    healthy_payload = {
        "ok": True,
        "result": {
            "claude": {
                "activeAccountId": "acc-1",
                "accounts": [
                    {"id": "acc-1", "email": "alpha@example.com", "organizationUuid": "org-1"},
                    {"id": "acc-2", "email": "alpha@example.com", "organizationUuid": "org-1"},
                    {"id": "acc-3", "email": "beta@example.com", "organizationUuid": "org-2"},
                ],
                "systemDefaultEmail": "sys@example.com",
            },
            "rateLimits": {
                "claude": {
                    "status": "ok",
                    "weekly": {"usedPercent": 43},
                    "fableWeekly": {"usedPercent": 80},
                    "planType": "max_20x",
                },
                "inactiveClaudeAccounts": [
                    {
                        "accountId": "acc-2",
                        "rateLimits": {"status": "ok", "weekly": {"usedPercent": 10}},
                    },
                    {
                        "accountId": "acc-3",
                        "rateLimits": {"status": "ok", "weekly": {"usedPercent": 5}},
                    },
                ],
            },
        },
    }
    res_keep = evaluate_agent_failover(healthy_payload, "claude")
    assert res_keep["needs_switch"] is False
    assert res_keep["recommended_target"] is None
    # Org duplicate acc-2 was deduplicated cleanly
    assert [c["account_id"] for c in res_keep["candidates"]] == ["acc-1", "acc-3", "system"]

    # 2. Rate-limited active account selects healthiest alternate (acc-3 at 5% over system unknown)
    limited_payload = json.loads(json.dumps(healthy_payload))
    limited_payload["result"]["rateLimits"]["claude"] = {
        "status": "error",
        "usageMetadata": {"failureKind": "rate-limited"},
    }
    res_switch = evaluate_agent_failover(limited_payload, "claude")
    assert res_switch["needs_switch"] is True
    assert res_switch["recommended_target"] == "acc-3"

    # 3. CI workflow strictly excludes GEMINI_API_KEY and reserves banked Anthropic for SOTA lane
    ci_mm = (REPO_ROOT / ".github" / "workflows" / "multi-model-sota.yml").read_text(
        encoding="utf-8"
    )
    assert "GEMINI_API_KEY" not in ci_mm
    assert "anthropic-banked-sota" in ci_mm
    assert "OMNIROUTE_API_KEY" in ci_mm
    assert "OPENAI_API_KEY" in ci_mm


def test_compounding_reflection_script_cli_and_docs(tmp_path: Path) -> None:
    import sys

    just_text = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    assert "reflect *args:" in just_text
    assert "scripts/run_compounding_reflection.py" in just_text
    assert (REPO_ROOT / "docs" / "COMPOUNDING_SELF_REFLECTION_LOOP.md").is_file()

    arch = json.loads((REPO_ROOT / "ARCHITECTURE_MANIFEST.json").read_text(encoding="utf-8"))
    entrypoints = arch.get("entrypoints", {})
    assert entrypoints.get("compounding_reflection") == "scripts/run_compounding_reflection.py"
    assert entrypoints.get("dspy_self_reflect") == "evals/dspy_self_reflect.py"

    proc = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_compounding_reflection.py"),
            "--offline",
            "--manifest-dir",
            str(tmp_path),
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"Reflection CLI failed:\n{proc.stderr}\n{proc.stdout}"
    assert (tmp_path / "compounding_self_reflection_manifest.json").is_file()


def test_fnox_age_secret_profiles_and_wsl_bootstrap_convergence() -> None:
    fnox_path = REPO_ROOT / "fnox.toml"
    assert fnox_path.is_file(), "Missing fnox.toml"
    fnox_raw = fnox_path.read_text(encoding="utf-8")
    assert "AGE-SECRET-KEY-" not in fnox_raw, "Private age secret key must never be in fnox.toml"
    assert not re.search(r"\b(?:PK|AK)[0-9A-Z]{16,28}\b", fnox_raw), (
        "Plaintext Alpaca API key must never be in fnox.toml"
    )
    fnox_doc = tomllib.loads(fnox_raw)
    assert fnox_doc.get("default_provider") == "age"
    assert fnox_doc["providers"]["age"]["type"] == "age"
    assert "paper" in fnox_doc.get("profiles", {})
    assert fnox_doc["profiles"]["paper"]["secrets"]["ALPACA_PAPER"]["default"] == "true"
    assert fnox_doc["profiles"]["paper"]["secrets"]["IBKR_PORT"]["default"] == "4002"
    live_prof = fnox_doc["profiles"]["live"]
    assert live_prof["providers"]["age_live"]["type"] == "age"
    assert live_prof["providers"]["age_live"]["key_file"] == "~/.config/fnox/live-age.txt"
    assert live_prof["secrets"]["LIVE_EXECUTION_ENABLED"]["default"] == "false"

    mise_doc = tomllib.loads((REPO_ROOT / "mise.toml").read_text(encoding="utf-8"))
    assert "fnox" in mise_doc["tools"] and "age" in mise_doc["tools"]

    wsl_sh_path = REPO_ROOT / "scripts" / "grand_reset" / "01_wsl_bootstrap.sh"
    wsl_sh_bytes = wsl_sh_path.read_bytes()
    assert b"\r\n" not in wsl_sh_bytes, "01_wsl_bootstrap.sh must use LF line endings"
    wsl_sh = wsl_sh_bytes.decode("utf-8")
    assert 'cat >"$LANE_ENV"' not in wsl_sh and "create_placeholder_env" not in wsl_sh
    assert "FNOX_AGE_KEY_FILE" in wsl_sh and ".config/fnox" in wsl_sh
    assert ".gemini/config/skills.json" in wsl_sh

    just_txt = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    for token in (
        "fnox-check:",
        "scan",
        "paper-farm",
        "tv-confirm-paper",
        "open-agy-wsl:",
        "agent-scan:",
        "SNYK_CFG_TELEMETRY",
        "SNYK_DISABLE_ANALYTICS",
        "SNYK_TOKEN",
    ):
        assert token in just_txt, f"justfile missing {token!r}"


def _just_recipe_bodies(just_txt: str, name: str) -> dict[str, str]:
    """Return the ``[unix]`` / ``[windows]`` bodies of one justfile recipe, keyed by OS."""
    lines = just_txt.splitlines()
    bodies: dict[str, str] = {}
    for idx, line in enumerate(lines[:-1]):
        attr = re.fullmatch(r"\[(unix|windows)\]", line)
        if attr and re.fullmatch(rf"{re.escape(name)}(?: [^:]*)?:", lines[idx + 1]):
            body: list[str] = []
            for nxt in lines[idx + 2 :]:
                if nxt and not nxt.startswith("    "):
                    break
                body.append(nxt)
            bodies[attr.group(1)] = "\n".join(body)
    return bodies


def test_isolated_cli_recipes_pin_versions_and_guards() -> None:
    """On-demand isolated CLIs (uvx / uv --with / npx) run exact pinned versions behind guards."""
    just_txt = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    specs = ISOLATED_CLI_SPEC_RE.findall(just_txt)
    for spec in specs:
        assert EXACT_PYPI_PIN_RE.fullmatch(spec), f"isolated CLI {spec!r} lacks an exact == pin"
    pinned = {spec.split("==", 1)[0] for spec in specs}
    assert {"crosshair-tool", "mutmut", "allure-pytest", "snyk-agent-scan"} <= pinned
    npx_specs = re.findall(r"\bnpx --yes (\S+)", just_txt)
    assert npx_specs, "expected the pinned npx Allure report CLI"
    for spec in npx_specs:
        assert re.fullmatch(r"[A-Za-z0-9@/._-]+@\d+(?:\.\d+)+", spec), f"npx {spec!r} unpinned"

    crosshair = _just_recipe_bodies(just_txt, "crosshair")
    assert set(crosshair) == {"unix", "windows"}
    crosshair_pins = {
        pin for text in crosshair.values() for pin in re.findall(r"crosshair-tool==\S+", text)
    }
    assert len(crosshair_pins) == 1, f"crosshair pins differ across platforms: {crosshair_pins}"

    mutate = _just_recipe_bodies(just_txt, "mutate-risk")
    assert set(mutate) == {"unix"}
    mutate_body = mutate["unix"]
    assert 'tmp_work="$(mktemp -d)"' in mutate_body
    assert mutate_body.index('cd "$tmp_work"') < mutate_body.index("--with mutmut==")
    assert len(set(re.findall(r"mutmut==\S+", mutate_body))) == 1

    starred = json.loads(
        (REPO_ROOT / "SOTA_STARRED_AND_BEYOND_MANIFEST.json").read_text(encoding="utf-8")
    )
    frontier = {
        str(cand["slug"]): cand
        for layer_doc in starred["earned_trigger_frontier_l11_to_l15"].values()
        for cand in layer_doc.get("candidates", [])
    }
    scan_pin = frontier["snyk/agent-scan"]["pypi"]
    assert EXACT_PYPI_PIN_RE.fullmatch(scan_pin)
    agent_scan = _just_recipe_bodies(just_txt, "agent-scan")
    assert set(agent_scan) == {"unix", "windows"}
    # The credentials the scanner must never see are cleared by real statements, not just named.
    unix_unset = re.findall(r"(?m)^\s*unset ([^\n]+)$", agent_scan["unix"])
    win_cleared = re.findall(
        r"foreach \(\$v in @\(([^)]*)\)\) \{ Remove-Item \"Env:\$v\"", agent_scan["windows"]
    )
    cleared = {
        "unix": set(" ".join(unix_unset).split()),
        "windows": set(re.findall(r"'([A-Z0-9_]+)'", " ".join(win_cleared))),
    }
    clear_marker = {"unix": "unset ", "windows": "Remove-Item"}
    must_clear = {
        "SNYK_TOKEN",
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
        "ALPACA_LIVE_API_KEY",
        "ALPACA_LIVE_SECRET_KEY",
    }
    for os_name, text in agent_scan.items():
        assert text.count(f"--from {scan_pin} ") == 2, os_name
        missing = must_clear - cleared[os_name]
        assert not missing, f"{os_name} agent-scan must clear {sorted(missing)} before scanning"
        assert text.index(clear_marker[os_name]) < text.index(f"--from {scan_pin} ")

    tv_ps1 = (REPO_ROOT / "scripts" / "tv_console.ps1").read_text(encoding="utf-8")
    hints = re.findall(r"uvx --from (\S+)", tv_ps1)
    assert hints == [frontier["paulieb89/pinescript-mcp"]["pypi"]]
    assert all(EXACT_PYPI_PIN_RE.fullmatch(hint) for hint in hints)


def test_fnox_live_profile_physical_key_isolation_and_tv_confirm_paper_recipe() -> None:
    """Enforce live key isolation (own age identity, no tracked ciphertext) and tv-confirm-paper."""
    fnox_raw = (REPO_ROOT / "fnox.toml").read_text(encoding="utf-8")
    assert "AGE-SECRET-KEY-" not in fnox_raw
    assert not re.search(r"\b(?:PK|AK)[0-9A-Z]{16,28}\b", fnox_raw)
    assert "api.alpaca.markets" not in fnox_raw.lower()

    fnox_doc = tomllib.loads(fnox_raw)
    default_age = fnox_doc["providers"]["age"]
    assert default_age["type"] == "age"
    assert default_age.get("key_file") != "~/.config/fnox/live-age.txt"

    live_prof = fnox_doc["profiles"]["live"]
    age_live = live_prof["providers"]["age_live"]
    assert age_live["type"] == "age"
    live_key_file = age_live["key_file"]
    assert live_key_file == "~/.config/fnox/live-age.txt"
    assert live_key_file != "~/.config/fnox/age.txt"
    assert live_key_file != default_age.get("key_file")
    # The live identity must be a different keypair, so the default key cannot decrypt live values.
    assert age_live["recipients"], "age_live needs its own recipient"
    assert not set(age_live["recipients"]) & set(default_age["recipients"])

    live_secrets = live_prof["secrets"]
    assert live_secrets["LIVE_EXECUTION_ENABLED"]["default"] == "false"
    # Live key ciphertext lives in the user-level fnox config. A project-level declaration would
    # shadow that value, and a tracked value would put ciphertext into git history.
    for live_key_name in ("ALPACA_LIVE_API_KEY", "ALPACA_LIVE_SECRET_KEY"):
        assert live_key_name not in live_secrets, (
            f"{live_key_name} must not be declared in the tracked fnox.toml"
        )
        assert live_key_name not in fnox_doc.get("secrets", {})
        assert live_key_name not in fnox_doc["profiles"]["paper"].get("secrets", {})

    just_txt = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    blocks = re.findall(
        r"(?ms)^\[(unix|windows)\]\ntv-confirm-paper alert_id \*args:\n((?:    [^\n]*\n?)+)",
        just_txt,
    )
    by_os = dict(blocks)
    assert set(by_os.keys()) == {"unix", "windows"}, (
        f"justfile must define [unix] and [windows] tv-confirm-paper recipes; got {sorted(by_os)}"
    )
    for os_name, body in by_os.items():
        assert "fnox exec --profile paper --" in body, f"{os_name} recipe must use --profile paper"
        assert "tier0_sota.live.runner" in body
        assert "--confirm-tv-alert" in body
        for forbidden in ("--profile live", "api.alpaca.markets", "live-age.txt", "9223"):
            assert forbidden not in body, f"Forbidden {forbidden!r} in {os_name} tv-confirm-paper"
    for live_port in ("4001", "7496", "8798", "8796"):
        assert live_port in by_os["unix"], f"Unix tv-confirm-paper must guard live port {live_port}"
    assert "ALPACA_LIVE_API_KEY" in just_txt and "ALPACA_LIVE_SECRET_KEY" in just_txt


def test_caddyfile_loopback_binding_and_duckdb_csrf_posture() -> None:
    """Pin loopback-only Caddy binding and anchored DuckDB Origin CSRF guard under host net."""
    compose_raw = (REPO_ROOT / "docker" / "compose.sota.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_raw)
    ts = compose["services"]["tearsheets"]
    assert ts["image"] == "caddy:2.11.9-alpine"
    assert ts.get("network_mode") == "host"
    assert "./Caddyfile:/etc/caddy/Caddyfile:ro" in ts.get("volumes", [])
    assert ts.get("command") == [
        "caddy",
        "run",
        "--config",
        "/etc/caddy/Caddyfile",
        "--adapter",
        "caddyfile",
    ]

    caddy_path = REPO_ROOT / "docker" / "Caddyfile"
    assert caddy_path.is_file(), "docker/Caddyfile must exist"
    raw = caddy_path.read_text(encoding="utf-8")
    active_lines = [
        line.strip()
        for line in raw.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    active_text = "\n".join(active_lines)

    assert not re.search(r"\b0\.0\.0\.0\b", active_text), (
        "Caddyfile must never bind wildcard all-interfaces IPv4"
    )
    assert "bind ::" not in active_text, "Caddyfile must never bind wildcard ::"
    for bad_port in (*LIVE_PORTS, "9223"):
        assert f":{bad_port}" not in active_text, f"Caddyfile must never reference port {bad_port}"

    # Parse top-level blocks (global options + site blocks) by brace depth.
    blocks: list[tuple[str, str]] = []
    header = ""
    buf: list[str] = []
    depth = 0
    for line in active_lines:
        if depth == 0:
            if line.endswith("{"):
                header = line[:-1].strip()
                buf = []
                depth = 1
            continue
        depth += line.count("{") - line.count("}")
        if depth == 0:
            blocks.append((header, "\n".join(buf)))
            header = ""
            buf = []
        else:
            buf.append(line)

    assert depth == 0, "Unbalanced braces in docker/Caddyfile"
    global_blocks = [body for hdr, body in blocks if hdr == ""]
    site_blocks = [(hdr, body) for hdr, body in blocks if hdr != ""]
    assert global_blocks and "admin off" in global_blocks[0], "Caddy admin API must be disabled"
    assert len(site_blocks) >= 2, f"Expected :8793 and :4213 site blocks, got {len(site_blocks)}"

    by_port: dict[str, str] = {}
    for hdr, body in site_blocks:
        assert re.search(r"(?m)^bind 127\.0\.0\.1$", body), (
            f"Site block {hdr!r} under network_mode=host must explicitly declare `bind 127.0.0.1`"
        )
        assert not re.search(r"(?m)^bind\s+(?!127\.0\.0\.1$)", body), (
            f"Site block {hdr!r} has non-loopback bind directive"
        )
        if ":8793" in hdr:
            by_port["8793"] = body
        if ":4213" in hdr:
            by_port["4213"] = body

    assert set(by_port.keys()) == {"8793", "4213"}, f"Missing required ports: {sorted(by_port)}"
    assert "file_server browse" in by_port["8793"]
    assert "/mnt/z/sota-data/manifests" in by_port["8793"]

    duck_body = by_port["4213"]
    expected_origin_rx = r"^http://(127\.0\.0\.1|localhost):4213$"
    assert f"@loopback_origin header_regexp Origin {expected_origin_rx}" in duck_body
    assert "handle @loopback_origin {" in duck_body
    assert "reverse_proxy 127.0.0.1:4214 {" in duck_body
    assert "header_up Origin http://localhost:4214" in duck_body

    # Fallback `handle` must NOT rewrite Origin so foreign/missing Origins still hit DuckDB 401.
    fallback_match = re.search(r"(?ms)^handle \{(.*?)\n\}$", duck_body)
    assert fallback_match is not None, "Missing fallback handle block for non-loopback Origins"
    fallback_body = fallback_match.group(1)
    assert "reverse_proxy 127.0.0.1:4214" in fallback_body
    assert "header_up Origin" not in fallback_body

    compiled = re.compile(expected_origin_rx)
    for allowed in ("http://127.0.0.1:4213", "http://localhost:4213"):
        assert compiled.match(allowed), f"Expected loopback origin allowed: {allowed}"
    for blocked in (
        "https://evil.example",
        "http://127.0.0.1:4213.evil.example",
        "http://localhost:42130",
        "http://127.0.0.1:4213/extra",
        "https://127.0.0.1:4213",
        "null",
        "",
    ):
        assert not compiled.match(blocked), f"Expected origin rejected: {blocked}"


def _load_script_module(module_name: str, rel_path: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, REPO_ROOT / rel_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_orca_account_failover_wsl2_safety_and_error_redaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failover = _load_script_module("orca_account_failover_test", "scripts/orca_account_failover.py")

    # Never invoke subprocess when executable is refused or missing.
    spawned: list[list[str]] = []

    def _forbid_run(cmd: list[str], *args: Any, **kwargs: Any) -> Any:
        spawned.append(cmd)
        raise AssertionError(f"subprocess.run must not be called for refused executable: {cmd}")

    monkeypatch.setattr(failover.subprocess, "run", _forbid_run)

    if os.name != "nt":
        assert failover._is_forbidden_gnome_orca("/usr/bin/orca") is True
        symlink_path = tmp_path / "orca-ide"
        symlink_path.symlink_to("/usr/bin/orca")
        assert failover._is_forbidden_gnome_orca(str(symlink_path)) is True

        monkeypatch.setenv("ORCA_CLI_COMMAND", str(symlink_path))
        with pytest.raises(FileNotFoundError, match="GNOME screen reader"):
            failover._run_orca_json(["account", "list"])
        assert spawned == []

        # Default POSIX lookup must look for `orca-ide` (never bare `orca`).
        monkeypatch.delenv("ORCA_CLI_COMMAND", raising=False)
        looked_up: list[str] = []

        def _fake_which(name: str) -> str | None:
            looked_up.append(name)
            return None

        with monkeypatch.context() as mp:
            mp.setattr(failover.shutil, "which", _fake_which)
            with pytest.raises(FileNotFoundError, match="orca-ide"):
                failover._resolve_orca_executable()
            assert looked_up == ["orca-ide"]

            # Even if `shutil.which("orca-ide")` resolves to `/usr/bin/orca`, refuse before spawn.
            mp.setattr(failover.shutil, "which", lambda _name: "/usr/bin/orca")
            with pytest.raises(FileNotFoundError, match="GNOME screen reader"):
                failover._run_orca_json(["account", "list"])
            assert spawned == []

    # ORCA_CLI_COMMAND pointing to a safe executable resolves cleanly.
    fake_cli = tmp_path / "fake-orca-ide"
    fake_cli.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    monkeypatch.setenv("ORCA_CLI_COMMAND", str(fake_cli))
    assert failover._resolve_orca_executable() == str(fake_cli)

    # Error redaction strips Bearer, sk- keys, JWTs, quoted JSON creds, token=..., and emails.
    # Assemble synthetic tokens dynamically so static scanners (gitleaks) never flag fixtures.
    fake_api_key = "-".join(["sk", "synthetic", "ABCDEF1234567890"])
    fake_jwt = ".".join(["eyJ" + "A" * 12, "eyJ" + "B" * 12, "sig_" + "C" * 12])
    quoted_tok = "_".join(["tok", "quoted", "xyz12345"])
    quoted_api = "_".join(["quoted", "api", "val", "999"])
    raw_err = (
        f"auth failed for alice.smith@example.com with Bearer secret_tok_123 "
        f"{fake_api_key} {fake_jwt} access_token=topsecret_value "
        f'{{"access_token": "{quoted_tok}", "api_key": "{quoted_api}"}}'
    )
    redacted = failover._redact_sensitive_text(raw_err)
    assert "alice.smith@example.com" not in redacted
    assert "al***@example.com" in redacted
    assert "secret_tok_123" not in redacted
    assert fake_api_key not in redacted
    assert fake_jwt not in redacted
    assert "topsecret_value" not in redacted
    assert quoted_tok not in redacted
    assert quoted_api not in redacted
    assert "[REDACTED]" in redacted

    def _failing_run(cmd: list[str], *args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stdout="",
            stderr="token=my_secret_xyz for bob.jones@example.com",
        )

    monkeypatch.setattr(failover.subprocess, "run", _failing_run)
    with pytest.raises(RuntimeError) as exc_info:
        failover._run_orca_json(["account", "list"])
    err_msg = str(exc_info.value)
    assert "my_secret_xyz" not in err_msg
    assert "bob.jones@example.com" not in err_msg
    assert "bo***@example.com" in err_msg
    assert "[REDACTED]" in err_msg

    # Structured JSON error responses must emit only allowlisted metadata + exit code.
    json_not_ok = json.dumps(
        {
            "ok": False,
            "error": "rate_limited",
            "code": 429,
            "message": f'failed for carol.test@example.com {{"refresh_token": "{quoted_tok}"}}',
            "access_token": quoted_tok,
            "raw_payload_dump": "SENSITIVE_EXTRA_BLOB_42",
        }
    )

    def _json_not_ok_run(
        cmd: list[str], *args: Any, **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=json_not_ok, stderr="")

    monkeypatch.setattr(failover.subprocess, "run", _json_not_ok_run)
    with pytest.raises(RuntimeError) as json_exc:
        failover._run_orca_json(["account", "list"])
    json_err_msg = str(json_exc.value)
    assert "exit=0" in json_err_msg
    assert "error=rate_limited" in json_err_msg
    assert "code=429" in json_err_msg
    assert "ca***@example.com" in json_err_msg
    assert "carol.test@example.com" not in json_err_msg
    assert quoted_tok not in json_err_msg
    assert "SENSITIVE_EXTRA_BLOB_42" not in json_err_msg
    assert '"access_token"' not in json_err_msg


def test_multi_runtime_configs_and_5x5_mcp_parity(tmp_path: Path) -> None:
    codex_cfg = tomllib.loads((REPO_ROOT / ".codex" / "config.toml").read_text("utf-8"))
    assert "hooks" not in codex_cfg
    features = codex_cfg.get("features", {})
    assert features.get("fast_mode") is True
    assert features.get("multi_agent_v2") is True
    assert features.get("memories") is True
    assert features.get("goals") is True
    assert "multi_agent" not in features
    assert features.get("hooks", False) is False

    claude_cfg = json.loads((REPO_ROOT / ".claude" / "settings.json").read_text("utf-8"))
    ask_rules = claude_cfg.get("permissions", {}).get("ask", [])
    assert "Edit(src/tier0_sota/risk.py)" in ask_rules
    assert "Edit(/src/tier0_sota/risk.py)" not in ask_rules

    mise_cfg = tomllib.loads((REPO_ROOT / "mise.toml").read_text("utf-8"))
    assert mise_cfg["tools"]["node"] == "24"

    # Verify scripts/preflight_resume.sh has unique # 1..# 13 gate headers (no duplicate # 12.).
    preflight_text = (REPO_ROOT / "scripts" / "preflight_resume.sh").read_text("utf-8")
    gate_nums = [int(m) for m in re.findall(r"(?m)^# (\d+)\.\s+", preflight_text)]
    assert gate_nums == list(range(1, 14)), f"Unexpected preflight gate numbering: {gate_nums}"

    sync_mod = _load_script_module("sync_manifest_test", "scripts/sync_manifest.py")
    root_mcp = json.loads((REPO_ROOT / ".mcp.json").read_text("utf-8"))
    gemini_cfg = json.loads((REPO_ROOT / ".gemini" / "settings.json").read_text("utf-8"))
    opencode_cfg = json.loads((REPO_ROOT / "opencode.json").read_text("utf-8"))
    omp_mcp = json.loads((REPO_ROOT / ".omp" / "mcp.json").read_text("utf-8"))

    parity = sync_mod._verify_mcp_config_parity(
        root_mcp, claude_cfg, codex_cfg, gemini_cfg, opencode_cfg, omp_mcp
    )
    assert len(parity) == 6
    assert ".omp/mcp.json" in parity

    # Mutating args or env in any runtime config must fail parity verification.
    drifted_codex = tomllib.loads((REPO_ROOT / ".codex" / "config.toml").read_text("utf-8"))
    drifted_codex["mcp_servers"]["xerj"]["args"] = ["--drifted"]
    with pytest.raises(ValueError, match=r"\.codex/config\.toml"):
        sync_mod._verify_mcp_config_parity(
            root_mcp, claude_cfg, drifted_codex, gemini_cfg, opencode_cfg, omp_mcp
        )

    drifted_opencode = json.loads((REPO_ROOT / "opencode.json").read_text("utf-8"))
    env_sentinel = "_".join(["SUPER", "SECRET", "ENV", "VAL", "98765"])
    drifted_opencode["mcp"]["edgartools"]["environment"] = {"DRIFT_KEY": env_sentinel}
    with pytest.raises(ValueError, match=r"opencode\.json") as env_exc:
        sync_mod._verify_mcp_config_parity(
            root_mcp, claude_cfg, codex_cfg, gemini_cfg, drifted_opencode, omp_mcp
        )
    env_err = str(env_exc.value)
    assert "edgartools" in env_err
    assert "DRIFT_KEY" in env_err
    assert env_sentinel not in env_err

    drifted_omp = json.loads((REPO_ROOT / ".omp" / "mcp.json").read_text("utf-8"))
    drifted_omp["mcpServers"]["xerj"]["args"] = ["--drifted"]
    with pytest.raises(ValueError, match=r"\.omp/mcp\.json"):
        sync_mod._verify_mcp_config_parity(
            root_mcp, claude_cfg, codex_cfg, gemini_cfg, opencode_cfg, drifted_omp
        )

    # Disabled server (`enabled: False`) and non-stdio/non-local transport must fail closed.
    disabled_oc = json.loads((REPO_ROOT / "opencode.json").read_text("utf-8"))
    disabled_oc["mcp"]["xerj"]["enabled"] = False
    with pytest.raises(ValueError, match="disabled"):
        sync_mod._verify_mcp_config_parity(
            root_mcp, claude_cfg, codex_cfg, gemini_cfg, disabled_oc, omp_mcp
        )

    http_omp = json.loads((REPO_ROOT / ".omp" / "mcp.json").read_text("utf-8"))
    http_omp["mcpServers"]["xerj"]["type"] = "http"
    with pytest.raises(ValueError, match="stdio/local transport"):
        sync_mod._verify_mcp_config_parity(
            root_mcp, claude_cfg, codex_cfg, gemini_cfg, opencode_cfg, http_omp
        )

    # _check_zero_hooks allows explicit `[features] hooks = false` in .codex/config.toml,
    # guards non-dict `features`, and rejects truthy hooks, [hooks], or `.omp/hooks*`.
    codex_dir = tmp_path / ".codex"
    codex_dir.mkdir(parents=True, exist_ok=True)
    (codex_dir / "config.toml").write_text("[features]\nhooks = false\n", encoding="utf-8")
    assert sync_mod._check_zero_hooks(tmp_path) is True
    (codex_dir / "config.toml").write_text('features = "not-a-table"\n', encoding="utf-8")
    assert sync_mod._check_zero_hooks(tmp_path) is True
    (codex_dir / "config.toml").write_text("[features]\nhooks = true\n", encoding="utf-8")
    assert sync_mod._check_zero_hooks(tmp_path) is False
    (codex_dir / "config.toml").write_text('[hooks]\nrun = "x"\n', encoding="utf-8")
    assert sync_mod._check_zero_hooks(tmp_path) is False
    (codex_dir / "config.toml").write_text("[features]\nhooks = false\n", encoding="utf-8")
    omp_dir = tmp_path / ".omp"
    omp_dir.mkdir(parents=True, exist_ok=True)
    (omp_dir / "hooks.json").write_text("{}", encoding="utf-8")
    assert sync_mod._check_zero_hooks(tmp_path) is False

    llm_doc = sync_mod.build_llm_harness_manifest(REPO_ROOT)
    assert llm_doc["live_hosted_portals_count"] == 15
    assert llm_doc["extended_portals_count"] == 3
    assert llm_doc["total_live_portals_count"] == 18
    assert llm_doc["mcp_servers"]["parity_count"] == "6/6"
    assert llm_doc["mcp_servers"]["parity_fields_verified"] == ["command", "args", "env"]
    cmds = set(llm_doc["upstream_verification_commands"].values())
    for expected_cmd in (
        "just adr-check",
        "just security",
        "just fnox-check",
        "just crosshair",
        "just test-report",
        "just mutate-risk",
        "just reflect",
    ):
        assert expected_cmd in cmds, f"Missing recipe in LLM_HARNESS_MANIFEST: {expected_cmd}"


def test_7role_multi_runtime_worker_parity_and_read_only_gatekeeper(tmp_path: Path) -> None:
    expected_roles = sorted(p.stem for p in (REPO_ROOT / ".claude" / "agents").glob("*.md"))
    assert len(expected_roles) == 7
    assert "paper-gatekeeper" in expected_roles

    # 1. All 7 Codex project-level worker definitions parse cleanly and enforce sandbox boundaries.
    codex_agents_dir = REPO_ROOT / ".codex" / "agents"
    codex_files = sorted(codex_agents_dir.glob("*.toml"))
    assert [p.stem for p in codex_files] == expected_roles
    for path in codex_files:
        raw = path.read_text(encoding="utf-8")
        doc = tomllib.loads(raw)
        assert doc["name"] == path.stem
        assert doc["description"].strip()
        assert doc["model"] == "gpt-6.2-astra"
        assert doc["model_reasoning_effort"] == "xhigh"
        assert "AGENTS.md" in doc["developer_instructions"]
        assert not bool(doc.get("hooks"))
        assert not bool(doc.get("features", {}).get("hooks"))
        assert "api.alpaca.markets" not in raw
        if path.stem == "paper-gatekeeper":
            assert doc["sandbox_mode"] == "read-only"
            assert (
                "stage_semi_auto_order_preset"
                in doc["mcp_servers"]["nautilus-engine"]["disabled_tools"]
            )
            assert "xerj_memory_store" in doc["mcp_servers"]["xerj"]["disabled_tools"]
        else:
            assert doc["sandbox_mode"] == "workspace-write"

    # 2. OpenCode defines the exact same 7 roles with paper-gatekeeper strictly read-only.
    opencode_cfg = json.loads((REPO_ROOT / "opencode.json").read_text(encoding="utf-8"))
    assert opencode_cfg.get("plugin") == []
    oc_agents = opencode_cfg.get("agent")
    assert isinstance(oc_agents, dict)
    assert sorted(oc_agents.keys()) == expected_roles
    gk_perm = oc_agents["paper-gatekeeper"].get("permission")
    assert isinstance(gk_perm, dict)
    assert gk_perm.get("edit") == "deny"
    assert gk_perm.get("shell") == "deny"
    assert gk_perm.get("subagent") == "deny"
    assert gk_perm.get("nautilus-engine_stage_semi_auto_order_preset") == "deny"
    assert gk_perm.get("xerj_xerj_memory_store") == "deny"

    # 3. OMP (.omp/agents/*.md) defines all 7 roles with paper-gatekeeper strictly read-only.
    sync_mod = _load_script_module("sync_manifest_worker_parity", "scripts/sync_manifest.py")
    omp_agents_dir = REPO_ROOT / ".omp" / "agents"
    omp_files = sorted(omp_agents_dir.glob("*.md"))
    assert [p.stem for p in omp_files] == expected_roles
    for path in omp_files:
        fm = sync_mod._parse_markdown_frontmatter(path)
        assert fm["name"] == path.stem
        assert str(fm["description"]).strip()
        assert not bool(fm.get("hooks"))
        if path.stem != "tier0-grand-central":
            assert not fm.get("spawns")
        if path.stem == "paper-gatekeeper":
            tools = fm["tools"]
            assert tools == ["read", "find", "grep", "glob", "ast_grep"]
            assert set(tools).issubset(sync_mod._OMP_GATEKEEPER_PURE_INSPECTION_TOOLS)

    # 4. sync_manifest emits 4-runtime 7/7 worker parity + honest enforcement breakdown.
    llm_doc = sync_mod.build_llm_harness_manifest(REPO_ROOT)
    parity = llm_doc["runtime_worker_parity"]
    assert parity["canonical_roles"] == expected_roles
    assert parity["claude_agents"] == "7/7"
    assert parity["codex_agents"] == "7/7"
    assert parity["opencode_agents"] == "7/7"
    assert parity["omp_agents"] == "7/7"
    assert parity["paper_gatekeeper_read_only_enforced"] is True
    assert set(parity["paper_gatekeeper_enforcement"].keys()) == {
        "claude",
        "codex",
        "opencode",
        "omp",
    }

    # Weakening OpenCode paper-gatekeeper (including shell=deny + bash=allow) must fail closed.
    weakened_oc = json.loads((REPO_ROOT / "opencode.json").read_text(encoding="utf-8"))
    weakened_oc["agent"]["paper-gatekeeper"]["permission"] = {}
    with pytest.raises(ValueError, match="paper-gatekeeper"):
        sync_mod._verify_runtime_worker_parity(REPO_ROOT, expected_roles, weakened_oc)

    bash_allow_oc = json.loads((REPO_ROOT / "opencode.json").read_text(encoding="utf-8"))
    bash_allow_oc["agent"]["paper-gatekeeper"]["permission"]["bash"] = "allow"
    with pytest.raises(ValueError, match="paper-gatekeeper"):
        sync_mod._verify_runtime_worker_parity(REPO_ROOT, expected_roles, bash_allow_oc)

    # Strict Claude frontmatter bypass test: removing Bash from disallowedTools while leaving
    # the word 'Bash' in the markdown prose body MUST fail closed.
    for rel_dir in (".claude/agents", ".codex/agents", ".omp/agents"):
        shutil.copytree(REPO_ROOT / rel_dir, tmp_path / rel_dir)
    shutil.copy2(REPO_ROOT / "upstream-map.toml", tmp_path / "upstream-map.toml")

    claude_gk = tmp_path / ".claude" / "agents" / "paper-gatekeeper.md"
    orig_claude_gk = claude_gk.read_text(encoding="utf-8")
    bypassed_claude_gk = (
        orig_claude_gk.replace(", Bash,", ",") + "\nNote: Bash mentioned in prose.\n"
    )
    claude_gk.write_text(bypassed_claude_gk, encoding="utf-8")
    with pytest.raises(ValueError, match="frontmatter read-only violation"):
        sync_mod._verify_runtime_worker_parity(tmp_path, expected_roles, opencode_cfg)
    claude_gk.write_text(orig_claude_gk, encoding="utf-8")

    # OMP pure-inspection allowlist test: `lsp` AND stateful `memory_edit` MUST fail closed.
    omp_gk = tmp_path / ".omp" / "agents" / "paper-gatekeeper.md"
    orig_omp_gk = omp_gk.read_text(encoding="utf-8")
    for bad_tool in ("lsp", "memory_edit"):
        omp_gk.write_text(
            orig_omp_gk.replace("- ast_grep", f"- ast_grep\n  - {bad_tool}"),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="READ_ONLY_TOOL_NAMES"):
            sync_mod._verify_runtime_worker_parity(tmp_path, expected_roles, opencode_cfg)


def test_zizmor_gates_use_pedantic_persona() -> None:
    """ADR-0018: the just recipe, the prek hook and both CI jobs run zizmor's pedantic persona."""
    justfile = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    assert re.search(r"(?m)^\s+zizmor --offline --persona=pedantic \.\s*$", justfile)
    hooks = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    (zizmor_hook,) = [
        hook
        for repo in hooks["repos"]
        if repo["repo"] == "https://github.com/zizmorcore/zizmor-pre-commit"
        for hook in repo["hooks"]
    ]
    assert "--persona=pedantic" in zizmor_hook["args"]
    assert "--offline" in zizmor_hook["args"]
    security = yaml.safe_load(
        (REPO_ROOT / ".github" / "workflows" / "security.yml").read_text(encoding="utf-8")
    )
    steps = [
        step
        for job in security["jobs"].values()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("zizmorcore/zizmor-action@")
    ]
    assert len(steps) == 2
    assert all(step["with"]["persona"] == "pedantic" for step in steps)


def test_compose_l6_portal_commands_and_loopback_ports() -> None:
    """Each L6 portal runs its upstream CLI and is published on loopback only."""
    from importlib.metadata import entry_points

    services = yaml.safe_load(
        (REPO_ROOT / "docker" / "compose.sota.yml").read_text(encoding="utf-8")
    )["services"]
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked = {(pkg["name"], pkg["version"]) for pkg in lock["package"]}
    console_scripts = {ep.name: ep for ep in entry_points(group="console_scripts")}
    any_host = "0.0.0.0"  # noqa: S104 - container side of the port; the host side is 127.0.0.1
    # service: (upstream CLI, subcommand words, flag values, container port, host port)
    portals: dict[str, tuple[str, list[str], dict[str, str], int, int]] = {
        "mlflow": (
            "mlflow",
            ["server"],
            {
                "--backend-store-uri": "sqlite:////state/mlflow/mlflow.db",
                "--host": any_host,
                "--port": "5000",
                "--workers": "1",
            },
            5000,
            5000,
        ),
        "optuna-dashboard": (
            "optuna-dashboard",
            ["sqlite:////state/mlflow/optuna.db"],
            {"--host": any_host, "--port": "8080"},
            8080,
            8080,
        ),
        "dagster-webserver": (
            "dagster-webserver",
            [],
            {"-w": "/var/dagster_home/workspace.yaml", "-h": any_host, "-p": "3000"},
            3000,
            3000,
        ),
        "inspect-view": (
            "inspect",
            ["view", "start"],
            {"--host": any_host, "--port": "7575"},
            7575,
            7575,
        ),
        "zensical": ("zensical", ["serve"], {"--dev-addr": f"{any_host}:8000"}, 8000, 8010),
    }
    for name, (cli, words, flags, container_port, host_port) in portals.items():
        svc = services[name]
        command = [str(part) for part in svc["command"]]
        assert svc["image"] == "tier0-sota-research:latest", name
        assert command[: 1 + len(words)] == [cli, *words], name
        for flag, value in flags.items():
            assert command.count(flag) == 1, (name, flag)
            assert command[command.index(flag) + 1] == value, (name, flag)
        # The CLI is the console script of a distribution installed at its uv.lock version.
        dist = console_scripts[cli].dist
        assert dist is not None, cli
        assert (re.sub(r"[-_.]+", "-", dist.name).lower(), dist.version) in locked, cli
        assert svc["ports"] == [f"127.0.0.1:{host_port}:{container_port}"], name
        assert f"http://127.0.0.1:{container_port}/" in " ".join(svc["healthcheck"]["test"]), name

    xerj = services["xerj"]
    assert xerj["build"]["dockerfile"] == "docker/Dockerfile.xerj"
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile.xerj").read_text(encoding="utf-8")
    assert re.findall(r"(?m)^ENTRYPOINT\s+(.+?)\s*$", dockerfile) == ['["/usr/local/bin/xerj"]']
    assert xerj["command"][xerj["command"].index("--data-dir") + 1] == "/data"
    assert xerj["ports"] == ["127.0.0.1:9200:9200"]
    assert "http://127.0.0.1:9200/" in " ".join(xerj["healthcheck"]["test"])


def test_duckdb_ui_dropin_binds_ledgers_read_only() -> None:
    """The DuckDB UI unit sees the paper ledgers only as read-only view catalogs."""
    from tier0_sota.live.paper_farm import ALPACA_LANE_PROFILES, _duckdb_filename

    manifests = "/mnt/z/sota-data/manifests"
    dropin = (REPO_ROOT / "docker" / "duckdb-ui" / "paper-farm.conf").read_text(encoding="utf-8")
    lines = [
        line.strip()
        for line in dropin.splitlines()
        if line.strip() and not line.lstrip().startswith(("#", ";"))
    ]
    assert lines[0] == "[Service]"
    binds: dict[str, str] = {}
    for line in lines[1:]:
        key, _, value = line.partition("=")
        # Read-only bind mounts only: no BindPaths, ReadWritePaths or other directives.
        assert key == "BindReadOnlyPaths", line
        source, _, target = value.partition(":")
        assert target not in binds, line
        binds[target] = source
    assert binds == {
        f"{manifests}/paper_lane_pa1.duckdb": f"{manifests}/paper_farm_ui/pa1.duckdb",
        f"{manifests}/paper_lane_pa2.duckdb": f"{manifests}/paper_farm_ui/pa2.duckdb",
        f"{manifests}/paper_lane_pa3.duckdb": f"{manifests}/paper_farm_ui/pa3.duckdb",
        f"{manifests}/tv_sota_pack/tv_half_auto_audit.duckdb": (
            f"{manifests}/paper_farm_ui/tv_audit.duckdb"
        ),
    }
    # The shadowed paths are the ledgers the paper-farm lanes write.
    lane_ledgers = {f"{manifests}/{_duckdb_filename(account)}" for account in ALPACA_LANE_PROFILES}
    assert lane_ledgers == {target for target in binds if "/paper_lane_" in target}

    script = (REPO_ROOT / "scripts" / "configure_paper_farm_ui.sh").read_text(encoding="utf-8")
    assert "for lane in pa1 pa2 pa3; do" in script
    assert "for ledger in paper_wire_orders paper_tca_fills; do" in script
    views = re.findall(r"CREATE OR REPLACE VIEW (\S+) AS SELECT \* FROM read_parquet\(", script)
    assert views == ["$ledger", "tv_staged_presets"]
    assert '"$repo_root/docker/duckdb-ui/paper-farm.conf"' in script
    assert "/etc/systemd/system/tier0-duckdb-ui.service.d/paper-farm.conf" in script
    # The script builds the view catalogs only; it never opens a lane ledger.
    assert "paper_lane_" not in script


def test_test_report_recipe_pins_allure_versions() -> None:
    """Both test-report recipes run one exact Allure 3 CLI and the locked allure-pytest."""
    just_txt = (REPO_ROOT / "justfile").read_text(encoding="utf-8")
    bodies = _just_recipe_bodies(just_txt, "test-report")
    assert set(bodies) == {"unix", "windows"}
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked = {pkg["name"]: pkg["version"] for pkg in lock["package"]}
    plugin_pin = f"allure-pytest=={locked['allure-pytest']}"
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["dependency-groups"]["test-reports"] == [plugin_pin]
    cli_specs: set[str] = set()
    for os_name, body in bodies.items():
        assert re.findall(r"--with (allure-pytest==\S+)", body) == [plugin_pin], os_name
        assert "--alluredir=" in body and "--junitxml=" in body, os_name
        specs = re.findall(r"\bnpx --yes (allure@\S+) awesome ", body)
        assert len(specs) == 1 and "--single-file" in body, os_name
        cli_specs.update(specs)
    assert len(cli_specs) == 1, f"Allure CLI pins differ across platforms: {cli_specs}"
    assert re.fullmatch(r"allure@3\.\d+\.\d+", cli_specs.pop())

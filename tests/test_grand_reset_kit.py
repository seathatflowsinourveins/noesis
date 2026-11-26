"""Verification tests for the Zero-Hook Disk-Z + WSL2 Grand-Reset kit, ADRs & Docker."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE_PORTS = ("4001", "7496", "8798", "8796")
MADR_SECTIONS = (
    "## Context and Problem Statement",
    "## Decision Drivers",
    "## Considered Options",
    "## Decision Outcome",
)
BANNED_IMPORT_RE = re.compile(
    r"(?m)^\s*(?:import|from)\s+(?:yfinance|backtrader|vectorbt|talib|mlfinlab)\b"
)
LIVE_ENABLE_RE = re.compile(r"live_execution_enabled\s*[:=]\s*(?:true|True|1)\b")


def test_zero_self_invented_hooks_and_native_uv_mcps() -> None:
    assert not (REPO_ROOT / ".codex" / "hooks.json").exists()

    claude_cfg = json.loads((REPO_ROOT / ".claude" / "settings.json").read_text("utf-8"))
    gemini_cfg = json.loads((REPO_ROOT / ".gemini" / "settings.json").read_text("utf-8"))
    assert "hooks" not in claude_cfg and "hooks" not in gemini_cfg

    codex_cfg = tomllib.loads((REPO_ROOT / ".codex" / "config.toml").read_text("utf-8"))
    assert "hooks" not in codex_cfg.get("features", {})
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


def test_zero_legacy_references_and_zero_yfinance_in_uv_lock() -> None:
    uv_lock = (REPO_ROOT / "uv.lock").read_text(encoding="utf-8")
    assert "yfinance" not in uv_lock.lower(), "uv.lock must contain 0 yfinance entries"

    legacy_tokens = (
        "tier0-quant",
        "sota_compounding_1000rpm_harness",
        "CLEAN LIVE",
        "sota-live-tv-harness",
    )
    targets: list[Path] = [REPO_ROOT / "pyproject.toml"]
    targets.extend((REPO_ROOT / "scripts" / "grand_reset").rglob("*"))
    targets.extend((REPO_ROOT / "docs").rglob("*"))
    targets.extend((REPO_ROOT / "src").rglob("*.py"))

    for path in targets:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for tok in legacy_tokens:
            assert tok not in text, f"Stale legacy token {tok!r} found in {path}"


def test_madr_adrs_catalog_and_zensical_toml() -> None:
    z_cfg = tomllib.loads((REPO_ROOT / "zensical.toml").read_text(encoding="utf-8"))
    assert z_cfg["project"]["docs_dir"] == "docs"
    assert (REPO_ROOT / "docs" / "index.md").is_file()
    catalog = (REPO_ROOT / "docs" / "adr" / "CATALOG.md").read_text(encoding="utf-8")

    for idx in range(1, 13):
        adr_name = f"ADR-{idx:04d}.md"
        adr_path = REPO_ROOT / "docs" / "adr" / adr_name
        assert adr_path.is_file(), f"Missing {adr_path}"
        assert adr_name in catalog, f"{adr_name} missing from CATALOG.md"
        body = adr_path.read_text(encoding="utf-8")
        for sec in MADR_SECTIONS:
            assert sec in body, f"{adr_name} missing MADR section {sec}"
        assert not BANNED_IMPORT_RE.search(body), f"{adr_name} endorses banned import"
        assert not LIVE_ENABLE_RE.search(body), f"{adr_name} enables live execution"


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

    for rel in (
        "docker/Dockerfile.research",
        "docker/Dockerfile.sandbox",
        "docker/Dockerfile.xerj",
        "docker/prometheus.yml",
        "docker/dagster.yaml",
        "docker/grafana/dashboards/tier0-sota-observability.json",
        "docker/grafana/provisioning/datasources/prometheus.yml",
        "docker/grafana/provisioning/dashboards/provider.yml",
    ):
        assert (REPO_ROOT / rel).is_file(), f"Missing docker asset {rel}"


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
        "057933bbda0db411bbfbc189c72a3e7b59f1989650e110f8591cf64728555c1c",
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
        "057933bbda0db411bbfbc189c72a3e7b59f1989650e110f8591cf64728555c1c",
        "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847",
        "seathatflowsinourveins/noesis",
        "workstation_preflight_resume_manifest.json",
        "tier0.workstation-preflight-resume.v1",
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

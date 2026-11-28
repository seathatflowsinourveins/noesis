"""Contract tests for ADR-0034 dual-machine remote scripts & Tailscale Serve invariants."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
REMOTE_DIR = REPO_ROOT / "scripts" / "remote"
WSL_SETUP_SH = REMOTE_DIR / "setup_noesis_wsl_remote_host.sh"
WIN_SERVE_PS1 = REMOTE_DIR / "setup_noesis_windows_tailscale_serve.ps1"
DOCTOR_SH = REMOTE_DIR / "noesis_remote_doctor.sh"

EXPECTED_ORCA_VERSION = "1.4.281"
EXPECTED_ORCA_SHA256 = "5076e46d55d4221e55d2b3eec307be37c1f55a645b6535bb4b2fef378a759620"
EXPECTED_TCP_PORTS = (2222, 6768)
EXPECTED_HTTPS_PORTS = (8793, 5000, 8080, 3000, 3001, 8010, 9200, 2718, 20128, 7575, 6900)
FORBIDDEN_BROKER_PORTS = (4001, 4002, 7496, 7497, 8796, 8798)
FORBIDDEN_LIVE_PORTS = (4001, 7496, 8798, 8796)


def test_caddyfile_and_compose_tailscale_https_loopback_invariants() -> None:
    """Verify Tailscale HTTPS readiness without breaking loopback-only or DuckDB CSRF posture."""
    caddy_text = (REPO_ROOT / "docker" / "Caddyfile").read_text(encoding="utf-8")
    expected_8793_hdr = (
        "http://127.0.0.1:8793, http://localhost:8793, "
        "http://{$TIER0_TAILSCALE_HOST:localhost}:8793 {"
    )
    assert expected_8793_hdr in caddy_text
    assert "http://127.0.0.1:4213, http://localhost:4213 {" in caddy_text
    assert "TIER0_TAILSCALE_HOST" not in caddy_text.split(":4213", 1)[1]

    compose_raw = (REPO_ROOT / "docker" / "compose.sota.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_raw)
    services = compose["services"]

    # Every published port across all services must stay strictly bound to 127.0.0.1.
    for svc_name, svc in services.items():
        for mapping in svc.get("ports", []):
            assert str(mapping).startswith("127.0.0.1:"), (
                f"Service {svc_name} has non-loopback port publish: {mapping}"
            )

    zensical_cmd = [str(item) for item in services["zensical"].get("command", [])]
    assert "https://${TIER0_TAILSCALE_HOST:-localhost}:8010" in zensical_cmd
    assert "http://${TIER0_TAILSCALE_HOST:-localhost}:8010" not in zensical_cmd

    grafana_env = [str(item) for item in services["grafana"].get("environment", [])]
    assert "GF_SERVER_ROOT_URL=https://${TIER0_TAILSCALE_HOST:-localhost}:3001" in grafana_env

    tearsheets_env = [str(item) for item in services["tearsheets"].get("environment", [])]
    assert "TIER0_TAILSCALE_HOST=${TIER0_TAILSCALE_HOST:-localhost}" in tearsheets_env


def test_wsl_remote_host_setup_script_contracts_and_pin() -> None:
    """Verify WSL2 setup script pins Orca SHA-256, loopback sshd/orca, and passes --dry-run."""
    assert WSL_SETUP_SH.is_file(), f"Missing {WSL_SETUP_SH}"
    text = WSL_SETUP_SH.read_text(encoding="utf-8")

    assert not re.search(r"/home/[A-Za-z0-9._-]+", text), (
        "WSL remote setup script must never hardcode a Linux user home path"
    )
    assert "set -euo pipefail" in text
    assert f'ORCA_VERSION="{EXPECTED_ORCA_VERSION}"' in text
    assert f'ORCA_SHA256="{EXPECTED_ORCA_SHA256}"' in text
    assert "orca-server-linux-x64.tar.gz" in text
    assert "sha256sum -c" in text
    assert "/usr/bin/orca" in text, "Must explicitly refuse Linux GNOME /usr/bin/orca"

    for directive in (
        "ListenAddress 127.0.0.1",
        "Port ${SSH_PORT}",
        "PermitRootLogin no",
        "PasswordAuthentication no",
        "KbdInteractiveAuthentication no",
        "PubkeyAuthentication yes",
        "AllowUsers ${TARGET_USER}",
        "AllowAgentForwarding no",
        "Subsystem sftp /usr/lib/openssh/sftp-server",
    ):
        assert directive in text, f"Missing sshd hardening directive: {directive}"

    assert "/etc/ssh/sshd_config.d/99-noesis-tailnet.conf" in text
    assert "loginctl enable-linger" in text
    assert "chmod 0700" in text and "chmod 0600" in text
    assert '--port "$ORCA_SERVE_PORT"' in text or '--port "$ORCA_SERVE_PORT"' in text
    assert "--bind 127.0.0.1" in text
    assert "--refuse-public" in text
    assert "ORCA_TERMINAL_MEMORY_MAX" in text
    assert "16G" in text

    for bad_port in FORBIDDEN_LIVE_PORTS:
        assert str(bad_port) in text, f"Must fail-closed guard live broker port {bad_port}"

    if os.name != "nt":
        bash = shutil.which("bash")
        assert bash is not None
        syntax = subprocess.run(
            [bash, "-n", str(WSL_SETUP_SH)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert syntax.returncode == 0, f"bash -n failed: {syntax.stderr}"

        dry = subprocess.run(
            [bash, str(WSL_SETUP_SH), "--dry-run"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert dry.returncode == 0, f"--dry-run failed: {dry.stderr}\n{dry.stdout}"
        assert "[DRY-RUN-OK]" in dry.stdout
        assert EXPECTED_ORCA_SHA256 in dry.stdout
        assert "127.0.0.1" in dry.stdout


def test_windows_tailscale_serve_ps1_contracts_and_broker_firewall() -> None:
    """Verify Windows Tailscale Serve enforces tailnet-only routes, Funnel OFF, and 0 brokers."""
    assert WIN_SERVE_PS1.is_file(), f"Missing {WIN_SERVE_PS1}"
    text = WIN_SERVE_PS1.read_text(encoding="utf-8")

    assert not re.search(r"/home/[A-Za-z0-9._-]+", text), (
        "Windows Tailscale Serve script must never hardcode a Linux user home path"
    )
    assert "[switch]$VerifyOnly" in text
    assert "[switch]$DryRun" in text
    assert "AllowFunnel" in text
    assert "tailscale funnel" in text.lower()
    assert "tcp://127.0.0.1:" in text
    assert "http://127.0.0.1:" in text
    assert not re.search(r"\b0\.0\.0\.0\b", text)

    tcp_match = re.search(r"\$TcpServePorts\s*=\s*@\(([^)]+)\)", text)
    assert tcp_match is not None
    tcp_ports = tuple(int(p.strip()) for p in tcp_match.group(1).split(",") if p.strip())
    assert tcp_ports == EXPECTED_TCP_PORTS

    https_match = re.search(r"\$HttpsServePorts\s*=\s*@\(([^)]+)\)", text)
    assert https_match is not None
    https_ports = tuple(int(p.strip()) for p in https_match.group(1).split(",") if p.strip())
    assert https_ports == EXPECTED_HTTPS_PORTS

    # No broker port may appear in either serve port array.
    exposed = set(tcp_ports) | set(https_ports)
    assert exposed.isdisjoint(FORBIDDEN_BROKER_PORTS)

    fb_match = re.search(r"\$ForbiddenTailscaleBrokerPorts\s*=\s*@\(([^)]+)\)", text)
    assert fb_match is not None
    guarded = tuple(int(p.strip()) for p in fb_match.group(1).split(",") if p.strip())
    assert set(FORBIDDEN_BROKER_PORTS).issubset(set(guarded))


def test_noesis_remote_doctor_readonly_checks_and_zero_hardcoded_home() -> None:
    """Verify noesis_remote_doctor.sh is read-only and passes bash -n and shellcheck."""
    assert DOCTOR_SH.is_file(), f"Missing {DOCTOR_SH}"
    text = DOCTOR_SH.read_text(encoding="utf-8")

    assert not re.search(r"/home/[A-Za-z0-9._-]+", text), (
        "Doctor script must never hardcode a Linux user home path"
    )
    assert "set -euo pipefail" in text
    for check_id in (
        "wsl_sshd_loopback_2222",
        "systemd_linger",
        "orca_serve_loopback_6768",
        "tmux_installed",
        "login_shell_toolchain",
        "loopback_portals_15",
        "live_broker_and_cdp_ports_zero",
        "zero_wildcard_tcp_exposures",
    ):
        assert check_id in text, f"Missing remote doctor check: {check_id}"

    # Ensure doctor never performs mutating systemctl/apt/tailscale writes.
    assert "apt-get" not in text
    assert "systemctl start" not in text
    assert "systemctl restart" not in text
    assert "systemctl enable" not in text

    if os.name != "nt":
        bash = shutil.which("bash")
        assert bash is not None
        syntax = subprocess.run(
            [bash, "-n", str(DOCTOR_SH)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert syntax.returncode == 0, f"bash -n failed: {syntax.stderr}"

        shellcheck = shutil.which("shellcheck")
        if shellcheck is not None:
            sc = subprocess.run(
                [shellcheck, str(WSL_SETUP_SH), str(DOCTOR_SH)],
                capture_output=True,
                text=True,
                check=False,
            )
            assert sc.returncode == 0, f"shellcheck failed:\n{sc.stdout}\n{sc.stderr}"

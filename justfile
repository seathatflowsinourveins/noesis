# noesis — Tier-0 SOTA — Declarative Upstream Task Runner (`just`)
# Works identically across Windows (`pwsh`) and WSL2/Linux (`bash`).

set shell := ["bash", "-euo", "pipefail", "-c"]
set windows-shell := ["pwsh", "-NoProfile", "-Command"]

wsl_venv := env("HOME", "") / "noesis/.venv"
uv_prefix := if os_family() == "windows" {
    ""
} else if path_exists(".venv/bin/python") == "true" {
    ""
} else if path_exists(wsl_venv / "bin/python") == "true" {
    "UV_PROJECT_ENVIRONMENT=" + wsl_venv + " "
} else {
    ""
}
uv_run := uv_prefix + "uv run --no-sync"

# Show available upstream tasks
default:
    @just --list

# Resolve and update uv.lock
lock:
    uv lock

# Sync Python environment strictly from uv.lock
sync:
    {{uv_prefix}}uv sync --frozen

# Format Python sources (risk.py is excluded in pyproject.toml)
fmt:
    {{uv_run}} ruff format src tests scripts evals

# Verify Python formatting
fmt-check:
    {{uv_run}} ruff format --check src tests scripts evals

# Lint Python sources with ruff
lint:
    {{uv_run}} ruff check .

# Static type-check with Astral ty
typecheck:
    {{uv_run}} ty check

# Verify declared vs imported Python dependencies with deptry
deptry:
    {{uv_run}} deptry .

# Scan Zero-Self-Invention & security AST rules with ast-grep
ast:
    {{uv_run}} ast-grep scan --config sgconfig.yml

# Run Z3 SMT formal counterexample-refutation proofs on risk.py
z3:
    {{uv_run}} python evals/formal_risk_z3.py

# Smoke-test all 10 FastMCP stdio tools offline
mcp-smoke:
    {{uv_run}} python scripts/nautilus_mcp_server.py --smoke

# Regenerate ARCHITECTURE_MANIFEST.json programmatically from SSOT
manifest-sync:
    {{uv_run}} python scripts/sync_manifest.py --write

# Verify ARCHITECTURE_MANIFEST.json has zero drift against SSOT
manifest-check:
    {{uv_run}} python scripts/sync_manifest.py --check

# Run full pytest suite
test *args:
    {{uv_run}} pytest -q {{args}}

# Build Zensical ADR & architecture documentation site (strict mode)
docs-build:
    {{uv_run}} zensical build --clean --strict

# Install prek git hooks
prek-install:
    prek install

# Run all prek hooks across repository files
prek-run:
    prek run --all-files

# Scan repository for leaked credentials with gitleaks
gitleaks:
    gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner

# Audit GitHub Actions workflows with zizmor (offline)
zizmor:
    zizmor --offline .github/workflows

# Scan repository misconfigurations & secrets with Trivy
trivy:
    trivy fs --scanners misconfig,secret --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .venv,.git,site,.claude/skills .

# Run full supply-chain & workflow security gates (gitleaks + zizmor + trivy)
security: gitleaks zizmor trivy

# Start WSL2/Docker hosted SOTA research stack (-p docker)
docker-up:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml up -d'" } else { "docker compose -p docker -f docker/compose.sota.yml up -d" } }}

# Stop hosted SOTA research stack (-p docker)
docker-down:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml down'" } else { "docker compose -p docker -f docker/compose.sota.yml down" } }}

# Build custom Docker images for SOTA research stack
docker-build:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml build'" } else { "docker compose -p docker -f docker/compose.sota.yml build" } }}

# Show hosted SOTA research stack status
docker-ps:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml ps'" } else { "docker compose -p docker -f docker/compose.sota.yml ps" } }}

# Run read-only workstation & ecosystem preflight (writes workstation_preflight_resume_manifest.json)
preflight *args:
    {{ if os_family() == "windows" { "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/preflight_resume.ps1" } else { "bash scripts/preflight_resume.sh" } }} {{args}}

# Reconcile WSL2 systemd services + Docker Compose stack and verify 15/15 portals post-restart
resume *args:
    {{ if os_family() == "windows" { "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/preflight_resume.ps1 -Reconcile" } else { "bash scripts/preflight_resume.sh --reconcile" } }} {{args}}

# Inspect Orca native multi-account OAuth headroom (read-only)
account-status:
    {{uv_run}} python scripts/orca_account_failover.py

# Fail over Orca active Claude/Codex account cleanly via `orca account select` if rate-limited
account-switch:
    {{uv_run}} python scripts/orca_account_failover.py --apply

# Run Dual-Flywheel compounding self-reflection & strategy Optuna/Inspect/DSPy/MLflow loop
reflect *args:
    {{uv_run}} python scripts/run_compounding_reflection.py {{args}}

# Run full fail-closed ecosystem automation cycle (reconcile -> portals/TV -> scan -> paper-farm -> reflect -> verify)
[windows]
automate: resume
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/03_hosted_stack_tv_paper.ps1
    if ($env:ALPACA_PA1_API_KEY -or $env:APCA_API_KEY_ID) { pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run_live_paper_farm.ps1 -Mode scan; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } else { Write-Host '[SKIP] live SIP scan: set ALPACA_PA1_API_KEY or APCA_API_KEY_ID to enable live scan' -ForegroundColor Yellow }
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run_live_paper_farm.ps1 -Mode paper-farm -ConfirmPaper
    just reflect
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/verify_grand_reset.ps1

# Run full fail-closed ecosystem automation cycle (reconcile -> reflect -> check -> docs-build)
[unix]
automate: resume reflect check docs-build

# Launch a fresh Antigravity window attached to Z:\noesis (without killing running instances)
[windows]
open-agy:
    Start-Process (Join-Path $env:LOCALAPPDATA 'Programs\antigravity\Antigravity.exe') -ArgumentList @('--new-window', 'Z:\noesis') -WorkingDirectory 'Z:\noesis'

# Launch a fresh Antigravity window attached to Z:\noesis (Windows-only helper)
[unix]
open-agy:
    @echo "open-agy is Windows-only; run 'just open-agy' from Windows pwsh."

# Run complete local SOTA verification suite
check: fmt-check lint typecheck deptry ast manifest-check z3 mcp-smoke test gitleaks

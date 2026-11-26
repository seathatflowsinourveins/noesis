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

# Verify MADR 4.0 ADR frontmatter, fitness bindings, manifest parity & strict Zensical build
adr-check: manifest-check docs-build
    {{uv_run}} pytest -q tests/test_architecture_manifest.py -k "adr or clean_sota_harness"

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

# Validate declarative fnox + age encrypted secret profiles (default, data, paper, live)
fnox-check:
    fnox check
    fnox check --profile data
    fnox check --profile paper
    fnox check --profile live

# Run whole-market Alpaca SIP extreme-mover scanner via fnox paper profile (graceful CI skip if unauthenticated)
[windows]
scan *args:
    $keyFile = if ($env:FNOX_AGE_KEY_FILE) { $env:FNOX_AGE_KEY_FILE } else { [Environment]::GetEnvironmentVariable('FNOX_AGE_KEY_FILE', 'User') }; if (-not $keyFile) { $keyFile = Join-Path $HOME '.config\fnox\age.txt' }; if ((Get-Command fnox -ErrorAction SilentlyContinue) -and (Test-Path -LiteralPath $keyFile)) { $env:FNOX_AGE_KEY_FILE = $keyFile; fnox exec --profile paper -- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run_live_paper_farm.ps1 -Mode scan {{args}}; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } elseif ($env:ALPACA_PA1_API_KEY -or $env:APCA_API_KEY_ID) { pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run_live_paper_farm.ps1 -Mode scan {{args}}; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } else { Write-Host '[SKIP] live SIP scan: configure fnox age key (~/.config/fnox/age.txt) or ALPACA_PA1_API_KEY/APCA_API_KEY_ID' -ForegroundColor Yellow }

# Run whole-market Alpaca SIP extreme-mover scanner via fnox paper profile (graceful CI skip if unauthenticated)
[unix]
scan:
    if [[ ! -x .venv/bin/python && -x "$HOME/noesis/.venv/bin/python" ]]; then export UV_PROJECT_ENVIRONMENT="$HOME/noesis/.venv"; fi; key_file="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}"; data_root="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"; py_code="from pathlib import Path; from tier0_sota.live.scanner import scan_extreme_movers; out = Path('${data_root}') / 'manifests' / 'live_whole_market_movers_manifest.json'; doc = scan_extreme_movers(top_k=15, pin_symbols=('SOXL','SOXS'), output_path=out); print('SCAN_OK', doc['ranked_count'], doc['macro_regime'])"; if command -v fnox >/dev/null 2>&1 && [[ -f "$key_file" ]]; then FNOX_AGE_KEY_FILE="$key_file" fnox exec --profile paper -- uv run --no-sync python -c "$py_code"; elif [[ -n "${ALPACA_PA1_API_KEY:-}" || -n "${APCA_API_KEY_ID:-}" ]]; then uv run --no-sync python -c "$py_code"; else echo "[SKIP] live SIP scan: configure fnox age key (~/.config/fnox/age.txt) or ALPACA_PA1_API_KEY/APCA_API_KEY_ID"; fi

# Run fail-closed 4-account Paper Farm cycle via fnox paper profile (paper_only=True)
[windows]
paper-farm *args:
    $keyFile = if ($env:FNOX_AGE_KEY_FILE) { $env:FNOX_AGE_KEY_FILE } else { [Environment]::GetEnvironmentVariable('FNOX_AGE_KEY_FILE', 'User') }; if (-not $keyFile) { $keyFile = Join-Path $HOME '.config\fnox\age.txt' }; if ((Get-Command fnox -ErrorAction SilentlyContinue) -and (Test-Path -LiteralPath $keyFile)) { $env:FNOX_AGE_KEY_FILE = $keyFile; fnox exec --profile paper -- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run_live_paper_farm.ps1 -Mode paper-farm -ConfirmPaper {{args}}; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } } else { pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run_live_paper_farm.ps1 -Mode paper-farm -ConfirmPaper {{args}}; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }

# Run fail-closed 4-account Paper Farm cycle via fnox paper profile (paper_only=True)
[unix]
paper-farm:
    if [[ ! -x .venv/bin/python && -x "$HOME/noesis/.venv/bin/python" ]]; then export UV_PROJECT_ENVIRONMENT="$HOME/noesis/.venv"; fi; key_file="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}"; data_root="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"; py_code="from pathlib import Path; from tier0_sota.live.paper_farm import PaperFarmCoordinator; from tier0_sota.live.performance import aggregate_paper_farm_performance; mdir = Path('${data_root}') / 'manifests'; coord = PaperFarmCoordinator(mdir); [lane.write_status() for lane in coord.lanes.values()]; coord.ibkr.write_status(); perf = aggregate_paper_farm_performance(mdir); print('PAPER_FARM_OK', list(coord.lanes.keys()), perf['summary'])"; if command -v fnox >/dev/null 2>&1 && [[ -f "$key_file" ]]; then FNOX_AGE_KEY_FILE="$key_file" fnox exec --profile paper -- uv run --no-sync python -c "$py_code"; else uv run --no-sync python -c "$py_code"; fi

# Run full fail-closed ecosystem automation cycle (reconcile -> portals/TV -> scan -> paper-farm -> reflect -> verify)
[windows]
automate: resume
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/03_hosted_stack_tv_paper.ps1
    just scan
    just paper-farm
    just reflect
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/verify_grand_reset.ps1

# Run full fail-closed ecosystem automation cycle (reconcile -> scan -> paper-farm -> reflect -> check -> docs-build)
[unix]
automate: resume scan paper-farm reflect check docs-build

# Launch a fresh Antigravity window attached to Z:\noesis (without killing running instances)
[windows]
open-agy:
    Start-Process (Join-Path $env:LOCALAPPDATA 'Programs\antigravity\Antigravity.exe') -ArgumentList @('--new-window', 'Z:\noesis') -WorkingDirectory 'Z:\noesis'

# Launch a fresh Antigravity window attached to Z:\noesis (Windows-only helper)
[unix]
open-agy:
    @echo "open-agy is Windows-only; run 'just open-agy' from Windows pwsh."

# Launch WSL2 Linux Antigravity Desktop (2.35.2 via WSLg) attached to /mnt/z/noesis
[windows]
open-agy-wsl:
    wsl.exe -d Noesis-SOTA -- bash -lc 'setsid -f "$HOME/.local/bin/antigravity-desktop" /mnt/z/noesis </dev/null >/tmp/antigravity-wsl.log 2>&1'

# Launch WSL2 Linux Antigravity Desktop (2.35.2 via WSLg) attached to /mnt/z/noesis
[unix]
open-agy-wsl:
    setsid -f "$HOME/.local/bin/antigravity-desktop" /mnt/z/noesis </dev/null >/tmp/antigravity-wsl.log 2>&1

# Run complete local SOTA verification suite
check: fmt-check lint typecheck deptry ast adr-check z3 mcp-smoke test gitleaks

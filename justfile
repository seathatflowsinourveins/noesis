# noesis — Tier-0 SOTA — Declarative Upstream Task Runner (`just`)
# Works identically across Windows (`pwsh`) and WSL2/Linux (`bash`).

set shell := ["bash", "-euo", "pipefail", "-c"]
set windows-shell := ["pwsh", "-NoProfile", "-Command"]

wsl_venv := env("HOME", "") / "noesis/.venv"
cache_root := if os_family() == "windows" { "" } else { env("HOME", "/tmp") / ".cache/noesis" }
uv_prefix := if os_family() == "windows" {
    ""
} else if path_exists(".venv/bin/python") == "true" {
    ""
} else if path_exists(wsl_venv / "bin/python") == "true" {
    "UV_PROJECT_ENVIRONMENT=" + wsl_venv + " "
} else if path_exists(".venv/Scripts/python.exe") == "true" {
    "UV_PROJECT_ENVIRONMENT=" + wsl_venv + " "
} else {
    ""
}
cache_prefix := if os_family() == "windows" {
    ""
} else {
    "RUFF_CACHE_DIR=" + cache_root + "/ruff HYPOTHESIS_STORAGE_DIRECTORY=" + cache_root + "/hypothesis "
}
pytest_cache_flag := if os_family() == "windows" {
    ""
} else {
    "-o cache_dir=" + cache_root + "/pytest"
}
uv_run := cache_prefix + uv_prefix + "uv run --no-sync"

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
    {{uv_run}} pytest -q {{pytest_cache_flag}} {{args}}

# Refresh Half-Auto extreme-mover TV tickets, watchlists & DuckDB audit table (qty=1, semi_auto_confirmed=false)
tv-tickets:
    {{uv_run}} python -m tier0_sota.tv.tickets --venue ALPACA

# Run one-shot live SIP scan + preset bundle + tv_sota_pack watchlists & order cards (paper-only, zero 60s gap)
tv-sync: paper-farm
    @echo 'TV_SYNC_OK=1 watchlist=Z:\sota-data\manifests\tv_sota_pack\tier0_sota_tv_watchlists_live.txt'

# Run one-command live TradingView E2E landing (live SIP/BOATS scan -> TV pack & Pine sync -> DuckDB audit)
[unix]
tv-land venue="ALPACA": scan
    TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" bash scripts/tv_console.sh tickets "{{venue}}" >/dev/null
    TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" bash scripts/tv_console.sh sync-pine
    TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" bash scripts/tv_console.sh audit

# Run one-command live TradingView E2E landing (live SIP/BOATS scan -> TV pack & Pine sync -> DuckDB audit)
[windows]
tv-land venue="ALPACA": scan
    pwsh -NoProfile -ExecutionPolicy Bypass -Command ". ./scripts/tv_console.ps1 -Venue '{{venue}}'; tv-tickets -Venue '{{venue}}' | Out-Null; tv-sync-pine; tv-audit"

# Run TradingView Half-Auto E2E verification (ruff, ty, Pine v6 parity & pytest)
tv-e2e:
    {{uv_run}} ruff check src/tier0_sota/tv tests/test_tv_automation.py
    {{uv_run}} ty check src/tier0_sota/tv tests/test_tv_automation.py
    {{uv_run}} pytest -q {{pytest_cache_flag}} tests/test_tv_automation.py

# Confirm a staged TradingView alert into the Alpaca PAPER farm via encrypted fnox paper profile (never live)
[unix]
tv-confirm-paper alert_id *args:
    active=$(ss -Htan '( sport = :4001 or dport = :4001 or sport = :7496 or dport = :7496 or sport = :8798 or dport = :8798 or sport = :8796 or dport = :8796 )'); test -z "$active"
    FNOX_AGE_KEY_FILE="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}" PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/src" TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" {{uv_prefix}}fnox exec --profile paper -- uv run --no-sync python -B -m tier0_sota.live.runner --confirm-tv-alert "{{alert_id}}" {{args}}

# Confirm a staged TradingView alert into the Alpaca PAPER farm via encrypted fnox paper profile (never live)
[windows]
tv-confirm-paper alert_id *args:
    $keyFile = if ($env:FNOX_AGE_KEY_FILE) { $env:FNOX_AGE_KEY_FILE } else { [Environment]::GetEnvironmentVariable('FNOX_AGE_KEY_FILE', 'User') }; if (-not $keyFile) { $keyFile = Join-Path $HOME '.config\fnox\age.txt' }; if (-not (Get-Command fnox -ErrorAction SilentlyContinue)) { throw 'fnox is required for tv-confirm-paper (--profile paper)' }; if (-not (Test-Path -LiteralPath $keyFile)) { throw "Missing fnox paper age key: $keyFile" }; $env:FNOX_AGE_KEY_FILE = $keyFile; $env:PYTHONDONTWRITEBYTECODE = '1'; $env:PYTHONPATH = Join-Path $PWD 'src'; fnox exec --profile paper -- uv run --no-sync python -B -m tier0_sota.live.runner --confirm-tv-alert '{{alert_id}}' {{args}}; exit $LASTEXITCODE

# Build Zensical ADR & architecture documentation site (strict mode)
docs-build:
    {{uv_run}} zensical build --clean --strict

# Verify MADR 4.0 ADR frontmatter, fitness bindings, manifest parity & strict Zensical build
adr-check: manifest-check docs-build
    {{uv_run}} pytest -q {{pytest_cache_flag}} tests/test_architecture_manifest.py -k "adr or clean_sota_harness"

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

# Run guarded offline static MCP & skills supply-chain audit via snyk/agent-scan (L15; telemetry off, zero secrets)
[unix]
agent-scan:
    #!/usr/bin/env bash
    set -euo pipefail
    unset SNYK_TOKEN ALPACA_PA1_API_KEY ALPACA_PA1_SECRET ALPACA_PA1_SECRET_KEY ALPACA_PA2_API_KEY ALPACA_PA2_SECRET ALPACA_PA2_SECRET_KEY ALPACA_PA3_API_KEY ALPACA_PA3_SECRET ALPACA_PA3_SECRET_KEY APCA_API_KEY_ID APCA_API_SECRET_KEY ALPACA_LIVE_API_KEY ALPACA_LIVE_SECRET_KEY
    export SNYK_CFG_TELEMETRY=0
    export SNYK_DISABLE_ANALYTICS=1
    targets=(.mcp.json .claude/settings.json .gemini/settings.json opencode.json .codex/config.toml .claude/skills)
    if command -v agent-scan >/dev/null 2>&1; then
      agent-scan inspect --opt-out "${targets[@]}"
    elif command -v uvx >/dev/null 2>&1 && uvx --offline --from snyk-agent-scan agent-scan --help >/dev/null 2>&1; then
      uvx --offline --from snyk-agent-scan agent-scan inspect --opt-out "${targets[@]}"
    else
      echo "[SKIP] agent-scan: snyk/agent-scan unavailable offline (SNYK_CFG_TELEMETRY=0 SNYK_DISABLE_ANALYTICS=1 SNYK_TOKEN unset)"
    fi

# Run guarded offline static MCP & skills supply-chain audit via snyk/agent-scan (L15; telemetry off, zero secrets)
[windows]
agent-scan:
    $env:SNYK_CFG_TELEMETRY = '0'; $env:SNYK_DISABLE_ANALYTICS = '1'; foreach ($v in @('SNYK_TOKEN','ALPACA_PA1_API_KEY','ALPACA_PA1_SECRET','ALPACA_PA1_SECRET_KEY','ALPACA_PA2_API_KEY','ALPACA_PA2_SECRET','ALPACA_PA2_SECRET_KEY','ALPACA_PA3_API_KEY','ALPACA_PA3_SECRET','ALPACA_PA3_SECRET_KEY','APCA_API_KEY_ID','APCA_API_SECRET_KEY','ALPACA_LIVE_API_KEY','ALPACA_LIVE_SECRET_KEY')) { Remove-Item "Env:$v" -ErrorAction SilentlyContinue }; $targets = @('.mcp.json','.claude/settings.json','.gemini/settings.json','opencode.json','.codex/config.toml','.claude/skills'); if (Get-Command agent-scan -ErrorAction SilentlyContinue) { agent-scan inspect --opt-out @targets } elseif (Get-Command uvx -ErrorAction SilentlyContinue) { & uvx --offline --from snyk-agent-scan agent-scan --help *> $null; if ($LASTEXITCODE -eq 0) { uvx --offline --from snyk-agent-scan agent-scan inspect --opt-out @targets } else { Write-Host '[SKIP] agent-scan: snyk/agent-scan unavailable offline (SNYK_CFG_TELEMETRY=0 SNYK_DISABLE_ANALYTICS=1 SNYK_TOKEN unset)' } } else { Write-Host '[SKIP] agent-scan: snyk/agent-scan unavailable offline (SNYK_CFG_TELEMETRY=0 SNYK_DISABLE_ANALYTICS=1 SNYK_TOKEN unset)' }

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
    if [[ ! -x .venv/bin/python && ( -x "$HOME/noesis/.venv/bin/python" || -f .venv/Scripts/python.exe ) ]]; then export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$HOME/noesis/.venv}"; fi; key_file="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}"; data_root="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"; py_code="from pathlib import Path; from tier0_sota.live.scanner import scan_extreme_movers; out = Path('${data_root}') / 'manifests' / 'live_whole_market_movers_manifest.json'; doc = scan_extreme_movers(top_k=15, output_path=out); print('SCAN_OK', doc['ranked_count'], doc['macro_regime'])"; if command -v fnox >/dev/null 2>&1 && [[ -f "$key_file" ]]; then FNOX_AGE_KEY_FILE="$key_file" fnox exec --profile paper -- uv run --no-sync python -c "$py_code"; elif [[ -n "${ALPACA_PA1_API_KEY:-}" || -n "${APCA_API_KEY_ID:-}" ]]; then uv run --no-sync python -c "$py_code"; else echo "[SKIP] live SIP scan: configure fnox age key (~/.config/fnox/age.txt) or ALPACA_PA1_API_KEY/APCA_API_KEY_ID"; fi

# Authenticate every Paper Farm observation with the encrypted paper profile.
[windows]
paper-farm *args:
    pwsh -NoProfile -File scripts/run_live_paper_farm.ps1 -Mode paper-farm {{if args == "" { "-Observe" } else { args }}}; exit $LASTEXITCODE

[unix]
paper-farm *args:
    active=$(ss -Htan '( sport = :4001 or dport = :4001 or sport = :7496 or dport = :7496 or sport = :8798 or dport = :8798 or sport = :8796 or dport = :8796 )'); test -z "$active"
    ibkr_tcp=0; if timeout 2 bash -c 'exec 3<>/dev/tcp/127.0.0.1/4002' 2>/dev/null; then ibkr_tcp=1; fi; IBKR_PAPER_TCP_REACHABLE="$ibkr_tcp" FNOX_AGE_KEY_FILE="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}" PYTHONPATH="$PWD/src" TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" {{uv_prefix}}fnox exec --profile paper -- uv run --no-sync python -m tier0_sota.live.runner {{if args == "" { "--observe" } else { args }}}

# Run full fail-closed ecosystem automation cycle (reconcile -> portals/TV -> scan -> paper-farm -> reflect -> verify)
[windows]
automate: resume
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/03_hosted_stack_tv_paper.ps1
    just scan
    just paper-farm
    just reflect
    pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/verify_grand_reset.ps1

# Run full fail-closed ecosystem automation cycle (reconcile -> scan -> paper-farm -> reflect -> check)
[unix]
automate: resume scan paper-farm reflect check

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

# Run complete local SOTA verification suite (parallel read-only gates -> docs-build -> single full pytest)
[unix]
check:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    gates=(fmt-check lint typecheck deptry ast manifest-check z3 mcp-smoke gitleaks)
    pids=()
    for g in "${gates[@]}"; do
      just "$g" >"$tmp_dir/$g.log" 2>&1 &
      pids+=("$!")
    done
    failed=0
    for i in "${!gates[@]}"; do
      g="${gates[$i]}"
      if ! wait "${pids[$i]}"; then
        echo "=== FAILED GATE: $g ===" >&2
        cat "$tmp_dir/$g.log" >&2
        failed=1
      fi
    done
    if [[ "$failed" -ne 0 ]]; then
      exit 1
    fi
    echo "[OK] parallel static/security/smoke gates (${#gates[@]}/${#gates[@]})"
    just docs-build
    just test

# Run complete local SOTA verification suite (parallel read-only gates -> docs-build -> single full pytest)
[windows]
check:
    $ErrorActionPreference = 'Stop'; $tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ('noesis-just-check-' + [guid]::NewGuid().ToString('N')); New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null; try { $gates = @('fmt-check', 'lint', 'typecheck', 'deptry', 'ast', 'manifest-check', 'z3', 'mcp-smoke', 'gitleaks'); $procs = @(foreach ($g in $gates) { $outF = Join-Path $tmpDir ($g + '.out.log'); $errF = Join-Path $tmpDir ($g + '.err.log'); $p = Start-Process -FilePath 'just' -ArgumentList @($g) -NoNewWindow -PassThru -RedirectStandardOutput $outF -RedirectStandardError $errF; $null = $p.Handle; [pscustomobject]@{ Name = $g; Proc = $p; Out = $outF; Err = $errF } }); $failed = $false; foreach ($item in $procs) { $item.Proc.WaitForExit(); if ($item.Proc.ExitCode -ne 0) { Write-Error ('=== FAILED GATE: ' + $item.Name + ' ===') -ErrorAction Continue; if (Test-Path -LiteralPath $item.Out) { Get-Content -LiteralPath $item.Out }; if (Test-Path -LiteralPath $item.Err) { Get-Content -LiteralPath $item.Err }; $failed = $true } }; if ($failed) { exit 1 }; Write-Host ('[OK] parallel static/security/smoke gates (' + $gates.Count + '/' + $gates.Count + ')') } finally { if (Test-Path -LiteralPath $tmpDir) { Remove-Item -LiteralPath $tmpDir -Recurse -Force -ErrorAction SilentlyContinue } }
    just docs-build
    just test

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

# Run ADR-0020 Stage-4 automated K3/K4/3-way failure drills (paper-only, deterministic)
stage4-drills *args:
    {{uv_run}} python scripts/run_stage4_drills.py {{args}}

# Run isolated L15 pschanely/CrossHair symbolic PEP-316 contract proofs on risk.py & engine.py (zero mypy in uv.lock)
[unix]
crosshair:
    PYTHONPATH="$PWD/src" uvx --from crosshair-tool==0.0.114 crosshair check --per_condition_timeout=10 evals/symbolic_risk_crosshair.py

# Run isolated L15 pschanely/CrossHair symbolic PEP-316 contract proofs on risk.py & engine.py (zero mypy in uv.lock)
[windows]
crosshair:
    $env:PYTHONPATH = Join-Path $PWD 'src'; uvx --from crosshair-tool==0.0.114 crosshair check --per_condition_timeout=10 evals/symbolic_risk_crosshair.py; exit $LASTEXITCODE

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

# Run pytest with JUnit XML + Allure Report 3 single-file HTML dashboard served via Caddy :8793/test-reports/allure/
[unix]
test-report *args:
    #!/usr/bin/env bash
    set -euo pipefail
    data_root="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"
    report_root="${data_root}/manifests/test-reports"
    allure_out="${report_root}/allure"
    allure_res="$(mktemp -d)"
    trap 'rm -rf "$allure_res"' EXIT
    mkdir -p "$allure_out"
    rc=0
    {{uv_run}} --with allure-pytest==2.16.2 pytest -q {{pytest_cache_flag}} --alluredir="$allure_res" --junitxml="$report_root/junit.xml" {{args}} || rc=$?
    PATH="$HOME/.local/bin:$PATH" npx --yes allure@3.33.0 awesome "$allure_res" --output "$allure_out" --single-file --report-name "noesis Tier-0 SOTA Verification"
    chmod 755 "$report_root" "$allure_out"
    find "$report_root" -type f -exec chmod 644 {} +
    echo "TEST_REPORT_OK=1 rc=${rc} url=http://127.0.0.1:8793/test-reports/allure/"
    exit "$rc"

# Run pytest with JUnit XML + Allure Report 3 single-file HTML dashboard served via Caddy :8793/test-reports/allure/
[windows]
test-report *args:
    $ErrorActionPreference = 'Stop'; $dataRoot = if ($env:TIER0_DATA_ROOT) { $env:TIER0_DATA_ROOT } else { 'Z:\sota-data' }; $reportRoot = Join-Path $dataRoot 'manifests\test-reports'; $allureOut = Join-Path $reportRoot 'allure'; $allureRes = Join-Path ([System.IO.Path]::GetTempPath()) ('noesis-allure-' + [guid]::NewGuid().ToString('N')); New-Item -ItemType Directory -Path $allureRes, $allureOut -Force | Out-Null; try { uv run --no-sync --with allure-pytest==2.16.2 pytest -q --alluredir="$allureRes" --junitxml="$(Join-Path $reportRoot 'junit.xml')" {{args}}; $rc = $LASTEXITCODE; npx --yes allure@3.33.0 awesome "$allureRes" --output "$allureOut" --single-file --report-name 'noesis Tier-0 SOTA Verification'; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; Write-Host ('TEST_REPORT_OK=1 rc=' + $rc + ' url=http://127.0.0.1:8793/test-reports/allure/'); exit $rc } finally { if (Test-Path -LiteralPath $allureRes) { Remove-Item -LiteralPath $allureRes -Recurse -Force -ErrorAction SilentlyContinue } }

# Run isolated WSL-only report-only mutmut 3 mutation audit on a temp copy of risk.py (never mutates tracked risk.py)
[unix]
mutate-risk *args:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp_work="$(mktemp -d)"
    trap 'rm -rf "$tmp_work"' EXIT
    cp -a src tests scripts pyproject.toml uv.lock "$tmp_work/"
    cat >> "$tmp_work/pyproject.toml" <<'EOF'

    [tool.mutmut]
    source_paths = ["src/tier0_sota"]
    only_mutate = ["src/tier0_sota/risk.py"]
    also_copy = ["scripts/"]
    pytest_add_cli_args_test_selection = ["tests/test_core_engine.py"]
    pytest_add_cli_args = ["-q", "-p", "no:cacheprovider", "-k", "test_rule_612_stop_cap_property or test_24x5_session_protection_and_4tier_progressive_trailing or test_compute_non_noise_min_trail_spec_24x5_and_invariants"]
    EOF
    cd "$tmp_work"
    {{uv_prefix}}uv run --no-sync --with mutmut==3.8.1 mutmut run --max-children 4 {{args}} || true
    {{uv_prefix}}uv run --no-sync --with mutmut==3.8.1 mutmut results || true

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

# Run unified 24/5 Half-Auto Order Preset setup (prune stale/executed alerts -> refresh 4-session presets -> stage fresh unlocked movers)
[unix]
tv-preset-setup venue="ALPACA":
    TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" bash scripts/tv_console.sh preset-setup "{{venue}}"

# Run unified 24/5 Half-Auto Order Preset setup (prune stale/executed alerts -> refresh 4-session presets -> stage fresh unlocked movers)
[windows]
tv-preset-setup venue="ALPACA":
    pwsh -NoProfile -ExecutionPolicy Bypass -Command ". ./scripts/tv_console.ps1 -Venue '{{venue}}'; tv-preset-setup -Venue '{{venue}}'"

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

# Audit the GitHub Actions setup with zizmor (offline, pedantic): workflows, dependabot.yml, hooks
zizmor:
    zizmor --offline --persona=pedantic .

# Scan vulnerabilities, secrets & misconfigurations with Trivy (CI's required scan: vuln,secret)
trivy:
    trivy fs --scanners vuln,secret,misconfig --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .venv,.git,site,.claude/skills,.cache,.ruff_cache,.pytest_cache,.hypothesis .

# Audit SPDX licenses via native Trivy fs (repo/lockfile) + rootfs (installed wheels; LGPL-3.0-only admitted per ADR-0016, zero non-OSI/strong-copyleft ignored)
[unix]
license-audit:
    #!/usr/bin/env bash
    set -euo pipefail
    trivy fs --scanners license --license-full --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .venv,.git,site,.claude/skills,.cache,.ruff_cache,.pytest_cache,.hypothesis .
    site_pkgs=$({{uv_run}} python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
    trivy rootfs --scanners license --severity HIGH,CRITICAL --ignored-licenses LGPL-3.0-only --exit-code 1 "$site_pkgs"

# Audit SPDX licenses via native Trivy fs (repo/lockfile) + rootfs (installed wheels; LGPL-3.0-only admitted per ADR-0016, zero non-OSI/strong-copyleft ignored)
[windows]
license-audit:
    $ErrorActionPreference = 'Stop'; trivy fs --scanners license --license-full --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .venv,.git,site,.claude/skills,.cache,.ruff_cache,.pytest_cache,.hypothesis .; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; $sitePkgs = (uv run --no-sync python -c "import sysconfig; print(sysconfig.get_path('purelib'))").Trim(); trivy rootfs --scanners license --severity HIGH,CRITICAL --ignored-licenses LGPL-3.0-only --exit-code 1 $sitePkgs; exit $LASTEXITCODE

# Run full supply-chain & workflow security gates (gitleaks + zizmor + trivy + license-audit)
security: gitleaks zizmor trivy license-audit

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
    elif command -v uvx >/dev/null 2>&1 && uvx --offline --from snyk-agent-scan==0.7.1 agent-scan --help >/dev/null 2>&1; then
      uvx --offline --from snyk-agent-scan==0.7.1 agent-scan inspect --opt-out "${targets[@]}"
    else
      echo "[SKIP] agent-scan: snyk/agent-scan unavailable offline (SNYK_CFG_TELEMETRY=0 SNYK_DISABLE_ANALYTICS=1 SNYK_TOKEN unset)"
    fi

# Run guarded offline static MCP & skills supply-chain audit via snyk/agent-scan (L15; telemetry off, zero secrets)
[windows]
agent-scan:
    $env:SNYK_CFG_TELEMETRY = '0'; $env:SNYK_DISABLE_ANALYTICS = '1'; foreach ($v in @('SNYK_TOKEN','ALPACA_PA1_API_KEY','ALPACA_PA1_SECRET','ALPACA_PA1_SECRET_KEY','ALPACA_PA2_API_KEY','ALPACA_PA2_SECRET','ALPACA_PA2_SECRET_KEY','ALPACA_PA3_API_KEY','ALPACA_PA3_SECRET','ALPACA_PA3_SECRET_KEY','APCA_API_KEY_ID','APCA_API_SECRET_KEY','ALPACA_LIVE_API_KEY','ALPACA_LIVE_SECRET_KEY')) { Remove-Item "Env:$v" -ErrorAction SilentlyContinue }; $targets = @('.mcp.json','.claude/settings.json','.gemini/settings.json','opencode.json','.codex/config.toml','.claude/skills'); if (Get-Command agent-scan -ErrorAction SilentlyContinue) { agent-scan inspect --opt-out @targets } elseif (Get-Command uvx -ErrorAction SilentlyContinue) { & uvx --offline --from snyk-agent-scan==0.7.1 agent-scan --help *> $null; if ($LASTEXITCODE -eq 0) { uvx --offline --from snyk-agent-scan==0.7.1 agent-scan inspect --opt-out @targets } else { Write-Host '[SKIP] agent-scan: snyk/agent-scan unavailable offline (SNYK_CFG_TELEMETRY=0 SNYK_DISABLE_ANALYTICS=1 SNYK_TOKEN unset)' } } else { Write-Host '[SKIP] agent-scan: snyk/agent-scan unavailable offline (SNYK_CFG_TELEMETRY=0 SNYK_DISABLE_ANALYTICS=1 SNYK_TOKEN unset)' }

# Start WSL2/Docker hosted SOTA research stack (-p docker)
docker-up:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml up -d'" } else { "docker compose -p docker -f docker/compose.sota.yml up -d" } }}

# Start L14 profile-gated Prometheus Alertmanager (:9093) for 24/5 alert routing (-p docker --profile alerting)
alerting-up:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml --profile alerting up -d alertmanager'" } else { "TIER0_REPO_ROOT=\"${TIER0_REPO_ROOT:-$PWD}\" TIER0_DATA_ROOT=\"${TIER0_DATA_ROOT:-/mnt/z/sota-data}\" TIER0_STATE_ROOT=\"${TIER0_STATE_ROOT:-$HOME/tier0-sota-state}\" docker compose -p docker -f docker/compose.sota.yml --profile alerting up -d alertmanager" } }}

# Stop hosted SOTA research stack (-p docker)
docker-down:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml down'" } else { "docker compose -p docker -f docker/compose.sota.yml down" } }}

# Build custom Docker images for SOTA research stack
docker-build:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml build'" } else { "docker compose -p docker -f docker/compose.sota.yml build" } }}

# Show hosted SOTA research stack status
docker-ps:
    {{ if os_family() == "windows" { "wsl.exe -d Noesis-SOTA -- bash -lc 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT=\"$HOME/tier0-sota-state\" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml ps'" } else { "docker compose -p docker -f docker/compose.sota.yml ps" } }}

# Validate Docker Compose manifests cleanly (ADR-0032 container proof)
compose-config:
    docker compose -f docker/compose.sota.yml config -q
    docker compose -f docker/compose.ibkr-paper.yml config -q

# Fail closed if uv.lock downgrades any package relative to base ref (ADR-0032 Python proof)
[unix]
lock-proof base="origin/main":
    #!/usr/bin/env bash
    set -euo pipefail
    {{uv_run}} python - "{{base}}" <<'PYEOF'
    import json
    import pathlib
    import shutil
    import subprocess
    import sys
    import tomllib
    from packaging.version import Version

    base = sys.argv[1]
    git = shutil.which("git")
    if not git:
        sys.exit("lock-proof FAIL: git not found on PATH")
    cwd = pathlib.Path.cwd().as_posix()
    proc = subprocess.run(
        [git, "-c", f"safe.directory={cwd}", "show", f"{base}:uv.lock"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        sys.exit(f"lock-proof FAIL: cannot read {base}:uv.lock ({proc.stderr.strip()})")
    head_path = pathlib.Path("uv.lock")
    if not head_path.is_file():
        sys.exit("lock-proof FAIL: missing uv.lock in working tree")


    def _load_versions(raw: str, label: str) -> dict[str, list[Version]]:
        doc = tomllib.loads(raw)
        pkgs = doc.get("package")
        if not isinstance(pkgs, list) or not pkgs:
            sys.exit(f"lock-proof FAIL: {label} uv.lock has no [[package]] entries")
        versions: dict[str, list[Version]] = {}
        for pkg in pkgs:
            name = str(pkg["name"]).lower()
            versions.setdefault(name, []).append(Version(str(pkg["version"])))
        return {name: sorted(vers) for name, vers in versions.items()}


    old = _load_versions(proc.stdout, base)
    new = _load_versions(head_path.read_text(encoding="utf-8"), "head")
    downgraded = {
        name: {"base": [str(v) for v in old[name]], "head": [str(v) for v in new[name]]}
        for name in sorted(old.keys() & new.keys())
        if new[name][0] < old[name][0] or new[name][-1] < old[name][-1]
    }
    upgraded = sorted(
        name
        for name in (old.keys() & new.keys())
        if name not in downgraded and new[name] != old[name]
    )
    added = sorted(new.keys() - old.keys())
    removed = sorted(old.keys() - new.keys())
    summary = {
        "base": base,
        "packages": len(new),
        "upgraded": len(upgraded),
        "downgraded": downgraded,
        "added": added,
        "removed": removed,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if downgraded:
        sys.exit(f"LOCK_PROOF_FAIL downgraded={len(downgraded)}")
    print(
        f"LOCK_PROOF_OK=1 base={base} packages={len(new)} upgraded={len(upgraded)} "
        f"downgraded=0 added={len(added)} removed={len(removed)}"
    )
    PYEOF

# Fail closed if uv.lock downgrades any package relative to base ref (ADR-0032 Python proof)
[windows]
lock-proof base="origin/main":
    $ErrorActionPreference = 'Stop'; uv run --no-sync python -c 'import itertools, json, pathlib, shutil, subprocess, sys, tomllib; from packaging.version import Version; base = sys.argv[1]; git = shutil.which("git") or sys.exit("lock-proof FAIL: git not found on PATH"); cwd = pathlib.Path.cwd().as_posix(); proc = subprocess.run([git, "-c", f"safe.directory={cwd}", "show", f"{base}:uv.lock"], capture_output=True, text=True, check=False); proc.returncode == 0 or sys.exit(f"lock-proof FAIL: cannot read {base}:uv.lock ({proc.stderr.strip()})"); head = pathlib.Path("uv.lock"); head.is_file() or sys.exit("lock-proof FAIL: missing uv.lock in working tree"); grp = lambda pkgs: {k: sorted(Version(str(p["version"])) for p in g) for k, g in itertools.groupby(sorted(pkgs, key=lambda p: str(p["name"]).lower()), key=lambda p: str(p["name"]).lower())}; load = lambda raw, lbl: grp((lambda p: p if isinstance(p, list) and p else sys.exit(f"lock-proof FAIL: {lbl} uv.lock has no [[package]] entries"))(tomllib.loads(raw).get("package"))); old = load(proc.stdout, base); new = load(head.read_text(encoding="utf-8"), "head"); down = {k: {"base": [str(v) for v in old[k]], "head": [str(v) for v in new[k]]} for k in sorted(old.keys() & new.keys()) if new[k][0] < old[k][0] or new[k][-1] < old[k][-1]}; up = sorted(k for k in (old.keys() & new.keys()) if k not in down and new[k] != old[k]); added = sorted(new.keys() - old.keys()); rem = sorted(old.keys() - new.keys()); print(json.dumps({"base": base, "packages": len(new), "upgraded": len(up), "downgraded": down, "added": added, "removed": rem}, indent=2, sort_keys=True)); (sys.exit(f"LOCK_PROOF_FAIL downgraded={len(down)}") if down else print(f"LOCK_PROOF_OK=1 base={base} packages={len(new)} upgraded={len(up)} downgraded=0 added={len(added)} removed={len(rem)}"))' '{{base}}'; exit $LASTEXITCODE

# Build custom Dockerfiles under disposable test tag (never :latest) + validate Compose configs (ADR-0032 container proof)
[unix]
docker-proof tag="pr-proof": compose-config
    #!/usr/bin/env bash
    set -euo pipefail
    tag="{{tag}}"
    if [[ -z "$tag" || "$tag" == "latest" || ! "$tag" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
      echo "docker-proof FAIL: tag must be a non-latest valid Docker tag (got: '${tag}')" >&2
      exit 2
    fi
    docker build -f docker/Dockerfile.research -t "tier0-sota-research:${tag}" .
    docker build -f docker/Dockerfile.sandbox -t "tier0-sota-aideml-sandbox:${tag}" .
    docker build -f docker/Dockerfile.xerj -t "tier0-sota-xerj:${tag}" .
    echo "DOCKER_PROOF_OK=1 tag=${tag} images=tier0-sota-research,tier0-sota-aideml-sandbox,tier0-sota-xerj"

# Build custom Dockerfiles under disposable test tag (never :latest) + validate Compose configs (ADR-0032 container proof)
[windows]
docker-proof tag="pr-proof": compose-config
    $ErrorActionPreference = 'Stop'; $tag = '{{tag}}'; if ([string]::IsNullOrWhiteSpace($tag) -or $tag -eq 'latest' -or $tag -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') { throw "docker-proof FAIL: tag must be a non-latest valid Docker tag (got: '$tag')" }; docker build -f docker/Dockerfile.research -t "tier0-sota-research:$tag" .; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; docker build -f docker/Dockerfile.sandbox -t "tier0-sota-aideml-sandbox:$tag" .; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; docker build -f docker/Dockerfile.xerj -t "tier0-sota-xerj:$tag" .; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; Write-Host "DOCKER_PROOF_OK=1 tag=$tag images=tier0-sota-research,tier0-sota-aideml-sandbox,tier0-sota-xerj"

# Safely fast-forward production checkout on main, verify risk.py SHA-256, refresh changed Compose services & run preflight
[unix]
prod-sync repo="/mnt/z/noesis" mode="apply":
    #!/usr/bin/env bash
    set -euo pipefail
    repo="{{repo}}"
    mode="{{mode}}"
    expected_risk_sha="ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
    if [[ "$mode" != "apply" && "$mode" != "dry-run" ]]; then
      echo "prod-sync FAIL: mode must be 'apply' or 'dry-run' (got: '${mode}')" >&2
      exit 2
    fi
    ss_out="$(ss -lntH)"
    if printf '%s\n' "$ss_out" | grep -Eq ':(4001|7496|8798|8796|9223)\b'; then
      echo "prod-sync FAIL: forbidden live broker or CDP port is listening (4001/7496/8798/8796/9223)" >&2
      exit 1
    fi
    if [[ ! -d "$repo/.git" ]]; then
      echo "prod-sync FAIL: not a git repository: ${repo}" >&2
      exit 1
    fi
    branch="$(git -C "$repo" rev-parse --abbrev-ref HEAD)"
    if [[ "$branch" != "main" ]]; then
      echo "prod-sync FAIL: ${repo} must be on 'main' (currently '${branch}')" >&2
      exit 1
    fi
    if [[ -n "$(git -C "$repo" status --porcelain)" ]]; then
      echo "prod-sync FAIL: ${repo} has uncommitted changes; refusing sync" >&2
      exit 1
    fi
    git -C "$repo" fetch --quiet origin main
    before="$(git -C "$repo" rev-parse HEAD)"
    after="$(git -C "$repo" rev-parse origin/main)"
    if ! git -C "$repo" merge-base --is-ancestor "$before" "$after"; then
      echo "prod-sync FAIL: ${repo} HEAD (${before}) is not an ancestor of origin/main (${after})" >&2
      exit 1
    fi
    incoming_risk_sha="$(git -C "$repo" show "$after:src/tier0_sota/risk.py" | sha256sum | cut -d' ' -f1)"
    if [[ "$incoming_risk_sha" != "$expected_risk_sha" ]]; then
      echo "prod-sync FAIL: incoming origin/main risk.py SHA-256 mismatch (${incoming_risk_sha})" >&2
      exit 1
    fi
    changed="$(git -C "$repo" diff --name-only "$before..$after")"
    echo "prod-sync repo=${repo} mode=${mode} before=${before:0:7} after=${after:0:7}"
    if [[ -n "$changed" ]]; then
      printf '%s\n' "$changed"
    fi
    if [[ "$mode" == "dry-run" ]]; then
      echo "PROD_SYNC_DRY_RUN_OK=1 repo=${repo} before=${before:0:7} after=${after:0:7}"
      exit 0
    fi
    if [[ "$before" != "$after" ]]; then
      git -C "$repo" merge --ff-only origin/main
    fi
    actual_risk_sha="$(sha256sum "$repo/src/tier0_sota/risk.py" | cut -d' ' -f1)"
    if [[ "$actual_risk_sha" != "$expected_risk_sha" ]]; then
      echo "prod-sync FAIL: risk.py SHA-256 mismatch (${actual_risk_sha})" >&2
      exit 1
    fi
    if printf '%s\n' "$changed" | grep -Eq '^docker/compose\.sota\.yml$'; then
      TIER0_REPO_ROOT="$repo" TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" TIER0_STATE_ROOT="${TIER0_STATE_ROOT:-$HOME/tier0-sota-state}" TIER0_TAILSCALE_HOST="${TIER0_TAILSCALE_HOST:-localhost}" \
        docker compose -p docker -f "$repo/docker/compose.sota.yml" up -d
    fi
    if printf '%s\n' "$changed" | grep -Eq '^docker/Caddyfile$'; then
      TIER0_REPO_ROOT="$repo" TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" TIER0_STATE_ROOT="${TIER0_STATE_ROOT:-$HOME/tier0-sota-state}" TIER0_TAILSCALE_HOST="${TIER0_TAILSCALE_HOST:-localhost}" \
        docker compose -p docker -f "$repo/docker/compose.sota.yml" up -d --force-recreate tearsheets
    fi
    if printf '%s\n' "$changed" | grep -Eq '^docker/(Dockerfile\.|systemd/)'; then
      echo "[NOTICE] Dockerfiles or systemd units changed; review rebuild/unit-install steps manually."
    fi
    bash "$repo/scripts/preflight_resume.sh"
    echo "PROD_SYNC_OK=1 repo=${repo} head=${after:0:7}"

# Report upstream dependency drift (majors/caps hidden by lockfile-only) and Dagster upstream watch triggers
upstream-watch:
    {{uv_prefix}}uv tree --frozen --outdated --depth 1 --all-groups --no-progress
    {{uv_run}} python -c "import json; from tier0_sota.definitions import _evaluate_upstream_watch_triggers; print(json.dumps(_evaluate_upstream_watch_triggers(), indent=2, sort_keys=True))"

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

# Run native NautilusTrader v2 ParquetDataCatalog empirical backtest (engine create_tearsheet) & interactive ml4t-diagnostic / skfolio report pages (pre-wall <= 2026-10-15)
sim-tearsheets:
    {{uv_run}} python scripts/run_sim_backtest_tearsheet_suite.py
    {{uv_run}} python scripts/generate_ml4t_and_shap_tearsheets.py

# Run causal pre-wall early-mover Optuna/MLflow compounding & NVDA premarket historical audit
compound-early-movers *args:
    {{uv_run}} python scripts/run_early_mover_compounding.py {{if args == "" { "--once" } else { args }}}

# Run one fail-closed paper-only non-stop compounding cycle (systemd/Dagster single-tick entrypoint)
[unix]
compounding-loop-once *args:
    test -z "$(ss -lntH | grep -E ':(4001|7496|8798|8796)\b' || true)"
    FNOX_AGE_KEY_FILE="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}" PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/src" TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}" TIER0_ALPACA_RPM="${TIER0_ALPACA_RPM:-1000}" {{uv_prefix}}fnox exec --profile paper -- uv run --no-sync python scripts/run_nonstop_compounding_loop.py --once {{args}}

# Run one fail-closed paper-only non-stop compounding cycle (systemd/Dagster single-tick entrypoint)
[windows]
compounding-loop-once *args:
    $keyFile = if ($env:FNOX_AGE_KEY_FILE) { $env:FNOX_AGE_KEY_FILE } else { Join-Path $HOME '.config\fnox\age.txt' }; $env:FNOX_AGE_KEY_FILE = $keyFile; $env:PYTHONDONTWRITEBYTECODE = '1'; $env:PYTHONPATH = Join-Path $PWD 'src'; $env:TIER0_ALPACA_RPM = if ($env:TIER0_ALPACA_RPM) { $env:TIER0_ALPACA_RPM } else { '1000' }; fnox exec --profile paper -- uv run --no-sync python scripts/run_nonstop_compounding_loop.py --once {{args}}; exit $LASTEXITCODE

# Inspect latest non-stop compounding loop manifest and native systemd timers
[unix]
compounding-loop-status:
    @echo "=== systemd timers ==="
    @systemctl is-active tier0-compounding-loop.timer tier0-paper-farm-observe.timer || true
    @echo "=== nonstop_compounding_loop_status.json ==="
    @cat "${TIER0_DATA_ROOT:-/mnt/z/sota-data}/manifests/nonstop_compounding_loop_status.json"

# Inspect latest non-stop compounding loop manifest
[windows]
compounding-loop-status:
    Get-Content -Raw (Join-Path (if ($env:TIER0_DATA_ROOT) { $env:TIER0_DATA_ROOT } else { 'Z:\sota-data' }) 'manifests\nonstop_compounding_loop_status.json')

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

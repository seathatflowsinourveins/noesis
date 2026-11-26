# Grand-Reset Runbook: Zero-Hook Clean Disk-Z, WSL2, Fresh AGY, TradingView, 4-Runtime & Orca Setup

This runbook executes a **non-destructive, zero-hook, fail-closed Grand Reset** anchored on `Z:\tier0-sota-clean`: archiving superseded Disk-Z clutter, preparing a fresh zero-hook Antigravity (AGY) reinstall, initializing clean Disk-Z + WSL2 `Noesis-SOTA`, syncing 4-runtime native `uv` MCPs + `.claude/mcp-lanes/*.mcp.json` + Orca + 46 locked skills, bringing up the 13 hosted SOTA portals, syncing TradingView Pine v6 + Paper Farm manifests, and passing the 10-gate master verifier.

## Safety Boundaries (Fail-Closed)

- **Preserved Shared Data & Secrets (NEVER wiped or overwritten):** `Z:\sota-data\prewall-lake`, `Z:\sota-data\manifests`, `Z:\sota-cache`, `Z:\WSL`, `Z:\tier0-quant` (immutable reference unless `-ArchiveLegacyReference` is explicitly passed), `~/Documents/upstream/` (`18` pins), and `.env*` secret files (`chmod 600`).
- **Zero Self-Invented Runtime Hooks:** Static & security gates (`ruff`, `ast-grep`, `gitleaks`) live strictly in upstream `.pre-commit-config.yaml` and `pytest`/verifier. `.codex/hooks.json` does NOT exist, and `.claude/settings.json`, `.gemini/settings.json`, and `.codex/config.toml` contain zero `"hooks"` blocks.
- **Forbidden Live Broker Endpoints (ALWAYS refused):** `:4001`, `:7496`, `:8798`, `:8796`, and `api.alpaca.markets`. `live_execution_enabled = False`, `paper_only = True`.
- **Forbidden Destructive Host Ops:** Never format/wipe disks or run `wsl --shutdown`.

---

## Step-by-Step Execution Order (`00 -> 01 -> 02 -> 03 -> verify`)

### Step 00: Clean Disk-Z Reset & Fresh Antigravity (AGY) Reinstall

Safe-by-default (`-DryRun` unless `-Execute` is explicitly passed):

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\00_clean_disk_z_and_agy_reset.ps1 -DryRun -CleanAgyReinstall
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\00_clean_disk_z_and_agy_reset.ps1 -Execute -CleanAgyReinstall
```

What this establishes:
- Archives remaining superseded Disk-Z artifacts (`Z:\CLEAN LIVE`, `Z:\sota-live-tv-harness`, `Z:\tmp_*.diff`, `*.bak-*`) into `Z:\archive\pre_grand_reset_2026-11-25\` (preserving locked paths safely without force-kills).
- Backs up existing `~/.gemini` configs to `Z:\archive\pre_grand_reset_2026-11-25\gemini_backup\`, writes a fresh **zero-hook** `~/.gemini/settings.json` directly from `Z:\tier0-sota-clean\.gemini\settings.json`, syncs the `46` vetted skills from `UPSTREAM_SKILLS_LOCK.json` into `~/.gemini/config/skills/`, and prints fresh Antigravity reinstall + clean workspace open steps (`Z:\tier0-sota-clean`).

### Step 01: Clean Disk-Z Folder Tree & WSL2 `Noesis-SOTA` Bootstrap

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\01_disk_z_and_wsl_setup.ps1 -DryRun
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\01_disk_z_and_wsl_setup.ps1
```

What this establishes:
- Clean Disk-Z folder tree: `Z:\tier0-sota-clean`, `Z:\sota-data\{prewall-lake,manifests,mlflow,duckdb,logs}`, `Z:\sota-cache`, `Z:\archive`.
- WSL2 `Noesis-SOTA` runtime via `scripts/grand_reset/01_wsl_bootstrap.sh` (`uv`, Python `3.13`, `.venv`, `with_lane_env.sh`, and placeholder `~/.config/tier0/.env.{data,paper}` with `chmod 600` if absent).

### Step 02: Zero-Hook 4-Runtime Parity, Locked Skills & Orca Setup

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\02_runtimes_agy_orca_setup.ps1 -DryRun
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\02_runtimes_agy_orca_setup.ps1
```

What this establishes:
- Confirms `hooks_zero = True` (no `.codex/hooks.json`, no `"hooks"` keys in `.claude/settings.json`, `.gemini/settings.json`, or `.codex/config.toml`).
- Syncs fresh zero-hook AGY (`~/.gemini/settings.json`, `~/.gemini/config/mcp_config.json`) + Claude Code (`.claude/agents`, `.claude/commands`, `.claude/mcp-lanes/*.mcp.json`) + Codex + OpenCode wiring strictly the 4 vetted native `uv` MCPs (`codebase-memory`, `xerj`, `nautilus-engine`, `edgartools`).
- Syncs the `46` locked skills from `UPSTREAM_SKILLS_LOCK.json` across AGY, Claude, and Codex, and verifies `orca.yaml`.

### Step 03: Hosted Stack (`13/13` Portals), TradingView Pine v6 & Paper Farm Sync

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\03_hosted_stack_tv_paper.ps1 -DryRun
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\03_hosted_stack_tv_paper.ps1
```

What this establishes:
- Starts/verifies `docker/compose.sota.yml` (`13/13` hosted portals HTTP `200` on `127.0.0.1`).
- Refreshes TradingView Pine v6 Half-Auto tickets & watchlists via `scripts/tv_console.ps1` (`qty = 1`, `semi_auto_confirmed = false`, zero CDP `:9223`).
- Runs Paper Farm preflight (`ALPACA_PA1..PA3` + `IBKR_DU_4002`) and refreshes `Z:\sota-data\manifests\paper_farm_live_performance_manifest.json`.

### Step 04: Master Grand-Reset Verification Gate

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\tier0-sota-clean\scripts\grand_reset\verify_grand_reset.ps1
```

Requires `[VERDICT: PASS]` across all 10 gates: `0` live broker ports, `risk.py` SHA-256, `ARCHITECTURE_MANIFEST.json` (`L0..L9`), `12/12` ADRs + `zensical build --clean`, `5/5` native `uv` MCP parity + `nautilus_mcp_server.py --smoke` (`10/10`), `13/13` portals, Pine v6 + `schema_v1.json`, Paper Farm readiness, `hooks_zero = True` + `.claude/mcp-lanes/*.mcp.json` + `SOTA_STARRED_AND_BEYOND_MANIFEST.json` + `UPSTREAM_SKILLS_LOCK.json` + `paper-gatekeeper.md`, and `gitleaks` (`0` leaks).

# Grand-Reset Runbook: Standalone Zero-Hook Clean Disk-Z, WSL2, Fresh AGY, TradingView, 4-Runtime & Orca Setup

This runbook executes a **standalone, non-destructive, zero-hook, fail-closed Grand Reset** anchored on `Z:\noesis` (`$env:TIER0_REPO_ROOT`): archiving superseded Disk-Z clutter, preparing a fresh Antigravity (AGY 2.0) reinstall (preserving official Orca status hooks), initializing clean Disk-Z + WSL2 `Noesis-SOTA`, syncing 4-runtime native `uv` MCPs + `.claude/mcp-lanes/*.mcp.json` + Orca + `46` standalone locked skills (`.agents/skills.json` + zero-copy `~/.gemini/config/skills.json`), bringing up the `15` hosted SOTA portals, syncing TradingView Pine v6 + Paper Farm manifests, and passing all `12/12` gates of the master verifier.

## Safety Boundaries (Fail-Closed)

- **Preserved Shared Data & Secrets (NEVER wiped or overwritten):** `$env:TIER0_DATA_ROOT\prewall-lake`, `$env:TIER0_DATA_ROOT\manifests`, `Z:\sota-cache`, `Z:\WSL`, `Z:\archive`, `~/Documents/upstream/` (`18` pins), and `.env*` secret files (`chmod 600`).
- **Zero Self-Invented Runtime Hooks:** Static & security gates (`ruff`, `ty`, `ast-grep`, `gitleaks`, `z3`, `inspect_ai`) live strictly in upstream `.pre-commit-config.yaml`, `evals/`, and `pytest`/verifier. `.codex/hooks.json` does NOT exist, and `.claude/settings.json`, `.gemini/settings.json`, and `.codex/config.toml` contain zero `"hooks"` blocks.
- **Live Broker Endpoints:** IBKR live ports `:4001`, `:7496`, `:8798`, `:8796` are not wired and stay closed. `risk.py` invariants and lossless trailing coverage hold in every lane; live operations on `ALPACA_LIVE_PRO_LINE_1000` follow the three tiers of [ADR-0029](adr/ADR-0029.md) (`live_execution_enabled = False` means tier 3 is off).
- **Forbidden Destructive Host Ops:** Never format/wipe disks or run `wsl --shutdown`.

---

## Step-by-Step Execution Order (`00 -> 01 -> 02 -> 03 -> verify`)

### Step 00: Clean Disk-Z Reset & Fresh Antigravity (AGY) Reinstall

Safe-by-default (`-DryRun` unless `-Execute` is explicitly passed; pass `-ArchiveLegacyDirs <string[]>` only for explicit legacy directories to archive):

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\00_clean_disk_z_and_agy_reset.ps1 -DryRun -CleanAgyReinstall
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\00_clean_disk_z_and_agy_reset.ps1 -Execute -CleanAgyReinstall
```

What this establishes:
- Archives stray `Z:\tmp_*.diff` files and any operator-specified `-ArchiveLegacyDirs` into `Z:\archive\pre_grand_reset_2026-11-25\` (preserving locked paths safely without force-kills).
- Backs up existing `~/.gemini` configs to `Z:\archive\pre_grand_reset_2026-11-25\gemini_backup\`, writes `~/.gemini/settings.json` directly from `Z:\noesis\.gemini\settings.json` while preserving official upstream Orca status hooks (`~/.orca/agent-hooks/*.cmd`) and filtering out any custom hooks (never altering `~/.gemini/config/hooks.json`), writes zero-copy `~/.gemini/config/skills.json` pointing to `Z:/noesis/.claude/skills` (`.agents/skills.json` at repo root, per [ADR-0022](adr/ADR-0022.md)), and prints fresh Antigravity reinstall + clean workspace open steps (`Z:\noesis`).

### Step 01: Clean Disk-Z Folder Tree & WSL2 `Noesis-SOTA` Bootstrap

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\01_disk_z_and_wsl_setup.ps1 -DryRun
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\01_disk_z_and_wsl_setup.ps1
```

What this establishes:
- Clean Disk-Z folder tree: `Z:\noesis`, `Z:\sota-data\{prewall-lake,manifests,mlflow,duckdb,logs}`, `Z:\sota-cache`, `Z:\archive`.
- WSL2 `Noesis-SOTA` runtime via `scripts/grand_reset/01_wsl_bootstrap.sh` (`uv`, Python `3.13`, `.venv`, `with_lane_env.sh`, and placeholder `~/.config/tier0/.env.{data,paper}` with `chmod 600` if absent).

### Step 02: Zero-Hook 4-Runtime Parity, Standalone Locked Skills & Orca Setup

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\02_runtimes_agy_orca_setup.ps1 -DryRun
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\02_runtimes_agy_orca_setup.ps1
```

What this establishes:
- Confirms `hooks_zero = True` (no `.codex/hooks.json`, no `"hooks"` keys in `.claude/settings.json`, `.gemini/settings.json`, or `.codex/config.toml`).
- Syncs AGY (`~/.gemini/settings.json` preserving official Orca hooks, `~/.gemini/config/mcp_config.json`, zero-copy `~/.gemini/config/skills.json` -> `Z:/noesis/.claude/skills`) + Claude Code (`.claude/agents`, `.claude/commands`, `.claude/mcp-lanes/*.mcp.json`) + Codex + OpenCode wiring strictly the 4 vetted native MCPs (`codebase-memory`, `xerj`, `nautilus-engine`, `edgartools`).
- Syncs the `46` standalone locked skills strictly from `Z:\noesis\.claude\skills\` to Claude and Codex (while AGY discovers them zero-copy via `.agents/skills.json` and `~/.gemini/config/skills.json`), and verifies `orca.yaml`.

### Step 03: Hosted Stack (`15/15` Portals), TradingView Pine v6 & Paper Farm Sync

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\03_hosted_stack_tv_paper.ps1 -DryRun
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\03_hosted_stack_tv_paper.ps1
```

What this establishes:
- Starts/verifies `docker/compose.sota.yml` (`15/15` hosted portals HTTP `200` on `127.0.0.1`).
- Refreshes TradingView Pine v6 Half-Auto tickets & watchlists via `scripts/tv_console.ps1` (`qty = 1`, `semi_auto_confirmed = false`, zero CDP `:9223`).
- Runs Paper Farm preflight (`ALPACA_PA1..PA3` + `IBKR_DU_4002`) and refreshes `Z:\sota-data\manifests\paper_farm_live_performance_manifest.json`.

### Step 04: Master 12-Gate Grand-Reset Verification

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\noesis\scripts\grand_reset\verify_grand_reset.ps1
```

Requires `[VERDICT: PASS]` across all `12/12` gates:
1. `0` live broker ports (`4001/7496/8798/8796`)
2. `risk.py` SHA-256 (`ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`)
3. `ARCHITECTURE_MANIFEST.json` (`L0..L10`, `11/11` layers)
4. Living MADR 4.0 ADRs (`ADR-0001..ADR-0022+`) + `zensical build --clean`
5. `5/5` native `uv` MCP parity + `scripts/nautilus_mcp_server.py --smoke` (`10/10`)
6. `15/15` hosted portals HTTP `200`
7. TradingView Pine v6 balance + `schema_v1.json`
8. Paper Farm preflight + `paper_farm_live_performance_manifest.json`
9. `hooks_zero = True` + `.claude/mcp-lanes/*.mcp.json` + `SOTA_STARRED_AND_BEYOND_MANIFEST.json` (`spdx_license_firewall`) + `UPSTREAM_SKILLS_LOCK.json` + `paper-gatekeeper.md` (`permissionMode: plan`)
10. `gitleaks` (`0` leaks)
11. `uv.lock` `0` `yfinance` + `46/46` standalone `.claude/skills/` + `0` legacy references
12. `evals/formal_risk_z3.py` (`Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1`) + `evals/tier0_inspect_eval.py`

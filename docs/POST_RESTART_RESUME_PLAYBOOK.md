# Post-Restart Seamless Resume Playbook

Single-command recovery and fresh-session grounding guide for the standalone `Z:\noesis` workstation after **any Windows, WSL2, or Antigravity (AGY) restart**.

- **Canonical Repository:** `Z:\noesis` (`$env:TIER0_REPO_ROOT`, origin `seathatflowsinourveins/noesis`, branch `main`)
- **Shared Data & Manifests:** `Z:\sota-data\manifests` (`$env:TIER0_DATA_ROOT`)
- **Orca Workspaces Root:** `Z:\workspaces` (`$env:ORCA_WORKSPACES_DIR`)
- **Default WSL2 Distro:** `Noesis-SOTA` (WSL2, systemd enabled)
- **Immutable Safety Invariants:** `src/tier0_sota/risk.py` SHA-256 `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`, `live_execution_enabled = False`, `paper_only = True`, zero live broker ports (`:4001`, `:7496`, `:8798`, `:8796`)

---

## 1. Automatic Logon Recovery + One-Command Verification (`Steps 1 to 4`)

On Windows startup / logon:
- **OmniRoute native autostart** (`OmniRoute.vbs` in Startup, registered via `omniroute autostart enable --no-tray`) launches the local `:20128` router silently (`--no-open --no-tray`) backed by your connected OAuth accounts.
- **Scheduled Task `\Noesis Orca Mesh Keep-Up`** (logon & every 5m, zero-window `pythonw.exe` running `ensure-noesis-orca-6770.pyw`) warms `Noesis-SOTA`, brings up all 4 systemd units + Docker Compose (`11/11` + FINOS `5/5`), ensures OmniRoute (`:20128`), Orca Desktop, `:6770`, and `:8792`, refreshes `Z:\sota-data\manifests\workstation_preflight_resume_manifest.json` on fresh boot via `preflight_resume.ps1 -Reconcile`, and runs rate-limit-only Orca account failover (`scripts/orca_account_failover.py --apply`).

### Step 1: Open `Z:\noesis` in Antigravity (AGY) or Orca

1. Launch **Antigravity (AGY)** or **Orca** and open `Z:\noesis` as the active workspace (never open superseded paths).
2. Confirm a fresh terminal inherits the persisted Disk-Z environment (`TIER0_REPO_ROOT=Z:\noesis`, `TIER0_DATA_ROOT=Z:\sota-data`, `ORCA_WORKSPACES_DIR=Z:\workspaces`) and AGY loads native stdio MCPs from `~/.gemini/config/mcp_config.json` (`$env:USERPROFILE\.gemini\config\mcp_config.json`: `codebase-memory`, `xerj`, `nautilus-engine`, `edgartools`), zero-copy skills from `.agents/skills.json` + `~/.gemini/config/skills.json` (`Z:/noesis/.claude/skills`), and preserved official Orca hooks in `~/.gemini/settings.json` (`ADR-0022`).

### Step 2: Run `just resume` (Reconcile & Full 15/15 Portal + OmniRoute + Orca Preflight)

From `Z:\noesis` (works identically in Windows `pwsh` and WSL2 `Noesis-SOTA` `bash`):

```powershell
just resume
```

What `just resume` executes (`scripts/preflight_resume.ps1 -Reconcile` on Windows, `scripts/preflight_resume.sh --reconcile` in WSL2):

1. Verifies `0` forbidden live broker ports (`:4001`, `:7496`, `:8798`, `:8796`) and default WSL2 distro == `Noesis-SOTA`.
2. Ensures all 4 WSL2 systemd units are active (`tier0-duckdb-ui`, `tier0-openbb-backend`, `tier0-ollama-docker-bridge`, `tier0-openbb-docker-bridge`), brings up `docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml up -d`, and reconciles OmniRoute (`http://127.0.0.1:20128/api/health`).
3. Probes all `15/15` local portals on `127.0.0.1`, including real `otelcol_*` metrics on `:9464/metrics` and `Server: Caddy` on `:8793`.
4. Verifies Disk-Z environment variables, AGY native MCP config, `8/8` Windows + WSL2 byte-identical Global Charter SHA-256 (`9fa037355c3942201e3c0813acaa7b01c590e6294cd5e85787ae1bdcef55a7b8`), `risk.py` SHA-256, git status & origin (`seathatflowsinourveins/noesis`), Paper Farm preflight readiness, OmniRoute OAuth readiness, and unrestricted native Claude Code / Orca multi-account readiness (`0` global API-key overrides).
5. Atomically writes `Z:\sota-data\manifests\workstation_preflight_resume_manifest.json` (`schema_version: "tier0.workstation-preflight-resume.v1"`, `verdict: "PASS"`).

For a read-only check anytime without restarting services:

```powershell
just preflight
just account-status   # Read-only Orca native Claude/Codex OAuth headroom check
just account-switch   # Optional: fail over active Orca account only when rate-limited/exhausted
```

---

## 2. Three-Tier Auth, OmniRoute & Multi-Model Policy (Anthropic & OpenAI Only)

| Tier / Surface | Auth & Router Mechanism | Models Allowed | Policy |
|---|---|---|---|
| **Interactive Claude Code CLI (Win & WSL2)** | **Native Orca OAuth logins only** (`0` global `ANTHROPIC_API_KEY` / `ANTHROPIC_BASE_URL` overrides) | **Unrestricted** (`claude-fable-5-2`, `opus`, `sonnet`, `haiku`, `opusplan`, etc.) | Remove `availableModels` cap so tasks switch models dynamically; Orca switches accounts cleanly via `orca account select` on rate-limit. |
| **Local Automations & Multi-Provider Routing** | **OmniRoute (`http://127.0.0.1:20128`)** with connected OAuth accounts (`agy`, `codex`) & autostart | OmniRoute OAuth pools & auto-fallback combos | Primary engine for local headless automations and routine GitHub Actions (`provider=omniroute` via `OMNIROUTE_API_KEY` + `OMNIROUTE_BASE_URL`). |
| **High-Quality SOTA & Banked Tasks Only** | **Banked `ANTHROPIC_API_KEY` (DPAPI vault / GitHub Secret) + `OPENAI_API_KEY`** | `opus` / `gpt-6.2-*` / SOTA deep audits | Reserved strictly for high-quality SOTA CI dispatches (`provider=anthropic-banked-sota` or `openai-codex`) and SDK/Batch SOTA runs — never overrides interactive CLI OAuth and zero `GEMINI_API_KEY`. |

### Step 3 (Optional, One-Time Post-Restart): Clean Unlocked Legacy Disk-Z Directories & GitHub Repos

Once Windows releases file locks on superseded Disk-Z directories after a reboot, run the allowlisted post-restart cleanup script (either directly or via `just resume`):

```powershell
just resume -CleanLegacyLocked
# Equivalent direct call:
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\workspaces\finish_post_restart_cleanup.ps1
```

If you also intend to prune superseded previous-era GitHub repositories (preserving all protected personal repos):

```powershell
gh auth refresh -h github.com -s delete_repo
pwsh -NoProfile -ExecutionPolicy Bypass -File Z:\workspaces\purge_legacy_github_repos.ps1 -IncludeForks -WhatIf
```

### Step 4: Resume the 4 Compounding Harness Lanes

Once `workstation_preflight_resume_manifest.json` reports `verdict: "PASS"` and `ready_lanes: ["sim-backtest", "paper", "half-auto-tv", "live"]`, resume any lane using its scoped command and MCP config:

| Lane | Slash Command & MCP Lane | Entrypoint / Verification | Primary Compounding Outputs |
|---|---|---|---|
| `sim-backtest` | `.claude/commands/lane-sim.md` / `.claude/mcp-lanes/sim.mcp.json` | `pwsh -NoProfile -File scripts/launch_clean_lanes.ps1 -Lane sim` | MLflow (`:5000`), Optuna (`:8080`), Caddy Tearsheets (`:8793`), XERJ (`:9200`) |
| `paper` | `.claude/commands/lane-paper.md` / `.claude/mcp-lanes/paper.mcp.json` | `pwsh -NoProfile -File scripts/run_live_paper_farm.ps1 -Mode preflight` | `Z:\sota-data\manifests\paper_lane_{pa1,pa2,pa3}.duckdb`, `paper_farm_live_performance_manifest.json`, FINOS (`:1420`) |
| `half-auto-tv` | `.claude/commands/lane-tv.md` / `.claude/mcp-lanes/tv.mcp.json` | `pwsh -NoProfile -File scripts/launch_clean_lanes.ps1 -Lane tv` | `Z:\sota-data\manifests\tv_sota_pack\live_half_auto_tickets.json` (`qty = 1`, `semi_auto_confirmed = false`) |
| `live` | `.claude/commands/lane-live.md` / `.claude/mcp-lanes/gate.mcp.json` | `just check && just docs-build` (`paper-gatekeeper` read-only) | Z3 UNSAT proofs (`evals/formal_risk_z3.py`), Inspect AI safety evals (`:7575`), 6-gate dossiers |

---

## 2. Fresh LLM Session Grounding Protocol

Every newly opened session (Antigravity, Claude Code, Codex, Gemini CLI, OpenCode, or Orca worker) grounds immediately in four deterministic steps before editing any file:

1. **Read Canonical SSOT Charter:** `AGENTS.md` (`CLAUDE.md` -> `@AGENTS.md`, `0` custom runtime hooks).
2. **Load Machine-Readable Manifests:**
   - `LLM_HARNESS_MANIFEST.json` — runtime parity (`5/5`), scoped MCP lanes, `46` SHA-locked skills, `15/15` portals.
   - `PHILOSOPHY_COMPOUNDING_MANIFEST.json` — extreme-mover philosophy, causal regime alignment, 24/5 trailing/auto-reverse/re-entry, and 4-lane compounding contract.
   - `Z:\sota-data\manifests\workstation_preflight_resume_manifest.json` — live workstation `verdict` (`PASS`), gate statuses, and `ready_lanes`.
3. **Query Native Upstream Memory & Code Graph (Search-SOTA-First):**
   - `codebase-memory` MCP — inspect symbol definitions, callers, and module boundaries before modifying code.
   - `XERJ` (`http://127.0.0.1:9200/` via `xerj` MCP) — retrieve deduplicated verified engineering & quantitative lessons from prior sessions.
4. **Verify Mechanically Before Claiming Done:** Run `just check` and `just docs-build` (`zensical build --clean --strict`).

---

## 3. All 15/15 Verified Local Portals

| # | Portal | Loopback URL | Verification Check |
|---|---|---|---|
| 1 | XERJ Memory/RAG | `http://127.0.0.1:9200/` | HTTP 200 |
| 2 | Zensical Docs | `http://127.0.0.1:8010/` | HTTP 200 + canonical `noesis` |
| 3 | MLflow 3 | `http://127.0.0.1:5000/` | HTTP 200 |
| 4 | Dagster Webserver | `http://127.0.0.1:3000/` | HTTP 200 |
| 5 | Optuna Dashboard | `http://127.0.0.1:8080/` | HTTP 200 |
| 6 | Inspect AI View | `http://127.0.0.1:7575/` | HTTP 200 |
| 7 | Prometheus | `http://127.0.0.1:9090/` | HTTP 200 |
| 8 | Grafana | `http://127.0.0.1:3001/` | HTTP 200 |
| 9 | OTel Collector Metrics | `http://127.0.0.1:9464/metrics` | HTTP 200 + real `otelcol_*` series |
| 10 | Caddy Tearsheets | `http://127.0.0.1:8793/` | HTTP 200 + `Server: Caddy` header |
| 11 | FINOS Workspace | `http://127.0.0.1:1420/` | HTTP 200 |
| 12 | FINOS API Health | `http://127.0.0.1:8000/health` | HTTP 200 + `{"success":true}` |
| 13 | OpenBB Backend | `http://127.0.0.1:6900/` | HTTP 200 |
| 14 | DuckDB SQL UI | `http://127.0.0.1:4213/` | HTTP 200 |
| 15 | OTel Health Check | `http://127.0.0.1:13133/` | HTTP 200 |

## Related Documentation

- [Overview](index.md)
- [Grand-Reset Disk-Z & WSL2 Runbook](GRAND_RESET_DISK_Z_WSL_RUNBOOK.md)
- [Clean Ecosystem Manifest](CLEAN_ECOSYSTEM_MANIFEST.md)
- [Architecture Layer Manifest (`L0..L10`)](ARCHITECTURE_LAYER_MANIFEST.md)
- [TradingView Automation Playbook](TV_AUTOMATION_PLAYBOOK.md)
- [Living MADR 4.0 ADR Catalog (`ADR-0001..ADR-0022+`)](adr/CATALOG.md)

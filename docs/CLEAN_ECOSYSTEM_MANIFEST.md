# Tier-0 Clean Ecosystem Manifest (`Z:\tier0-sota-clean`)

- **Generated:** 2026-11-25
- **Target Standalone Repository:** `Z:\tier0-sota-clean` (`$env:TIER0_REPO_ROOT`, branch `main`, Python 3.13 via `uv`)
- **Immutable Risk Kernel SHA-256 (`src/tier0_sota/risk.py`):** `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`
- **Live Execution Guard:** `live_execution_enabled = False`, `paper_only = True`, zero live broker ports (`:4001`, `:7496`, `:8798`, `:8796`)

---

## 1. Clean Ecosystem Entrypoints

| Script / Config | Role | Notes |
|---|---|---|
| [`scripts/setup_clean_ecosystem.ps1`](../scripts/setup_clean_ecosystem.ps1) | Idempotent setup & health check | Verifies `.venv`, `risk.py` SHA-256, 4-runtime MCP parity, `13/13` hosted portals, Paper Farm readiness, and `0` live broker ports |
| [`scripts/check.ps1`](../scripts/check.ps1) | One-command Windows CI gate | Runs `risk.py` SHA-256, PS1 AST parse, `UV_LOCK_YFINANCE_MATCHES=0`, `LEGACY_MATCHES=0`, `ruff format --check`, `ruff check`, `ty check`, `deptry .`, `ast-grep scan`, `formal_risk_z3.py`, `nautilus_mcp_server.py --smoke`, `pytest -q`, and `gitleaks` |
| [`scripts/check.sh`](../scripts/check.sh) | One-command POSIX/WSL2 CI gate | Runs `risk.py` SHA-256, `bash -n`, `shellcheck`, `UV_LOCK_YFINANCE_MATCHES=0`, `LEGACY_MATCHES=0`, `ruff format --check`, `ruff check`, `ty check`, `deptry .`, `ast-grep scan`, `formal_risk_z3.py`, `nautilus_mcp_server.py --smoke`, `pytest -q`, and `gitleaks` |
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | Hardened GitHub Actions CI | Top-level `permissions: contents: read`, full 40-char commit SHA action pins, `persist-credentials: false`, `zizmor --offline` (`0` findings) |
| [`scripts/launch_clean_lanes.ps1`](../scripts/launch_clean_lanes.ps1) | Non-overlapping lane launcher | Supports `-DryRun`; orchestrates Pre-Wall Sim Lab (`<= 2026-10-15`), 3-Lane Paper Farm (`ALPACA_PA1..PA3` + `IBKR_DU_4002`), TV Half-Auto Cockpit, and Verifier Gate |
| [`scripts/run_live_paper_farm.ps1`](../scripts/run_live_paper_farm.ps1) | Paper Farm controller | Supports `preflight`, `scan`, `paper-farm` (requires `-ConfirmPaper`), and `performance` |
| [`scripts/tv_console.ps1`](../scripts/tv_console.ps1) | TradingView Half-Auto console | Display-only Pine v6 staging (`qty=1`, `semi_auto_confirmed=false`, zero CDP `:9223`) |
| [`scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1`](../scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1) | Grand-Reset Step 00 | Safe-by-default (`-DryRun`) Disk-Z reset + fresh zero-hook AGY reinstall prep (`-Execute -CleanAgyReinstall`, optional `-ArchiveLegacyDirs`) |
| [`scripts/grand_reset/01_disk_z_and_wsl_setup.ps1`](../scripts/grand_reset/01_disk_z_and_wsl_setup.ps1) | Grand-Reset Step 01 | Idempotently initializes Disk-Z dirs & WSL2 `Noesis-SOTA` via `01_wsl_bootstrap.sh` (`-DryRun` supported) |
| [`scripts/grand_reset/02_runtimes_agy_orca_setup.ps1`](../scripts/grand_reset/02_runtimes_agy_orca_setup.ps1) | Grand-Reset Step 02 | Zero-hook sync for AGY, Claude, Codex, OpenCode, Orca, native `uv` MCPs & standalone `.claude/skills` (`-DryRun` supported) |
| [`scripts/grand_reset/03_hosted_stack_tv_paper.ps1`](../scripts/grand_reset/03_hosted_stack_tv_paper.ps1) | Grand-Reset Step 03 | Verifies `13/13` Docker portals, syncs TV Pine v6 + Paper Farm manifests (`-DryRun` supported) |
| [`scripts/grand_reset/verify_grand_reset.ps1`](../scripts/grand_reset/verify_grand_reset.ps1) | Grand-Reset Master Verifier | Enforces `12/12` gates (`ARCHITECTURE_MANIFEST.json`, `SOTA_STARRED_AND_BEYOND_MANIFEST.json`, `hooks_zero=True`, `.claude/mcp-lanes/`, `12/12` ADRs, `zensical`, `risk.py`, MCP smoke, `13/13` portals, TV, Paper Farm, `gitleaks`, `0` `yfinance` / standalone skills, Z3 + Inspect AI evals, `0` live ports) |
| [`docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md`](GRAND_RESET_DISK_Z_WSL_RUNBOOK.md) | Grand-Reset Operator Runbook | Step-by-step zero-hook Disk-Z + WSL2 + fresh AGY + TV + 4-Runtime + Orca reset guide |
| **WSL2 Docker Cutover** (`Noesis-SOTA`) | Compose project `docker` runs exclusively from `/mnt/z/tier0-sota-clean/docker/compose.sota.yml` (`tier0-sota-research:latest`, `tier0-sota-xerj:latest`, ext4 `${TIER0_STATE_ROOT}` = `~/tier0-sota-state`); Dagster `:3000` serves `tier0_sota.definitions`; Zensical `:8010` serves canonical `tier0-sota-clean` docs |
| `trivy` & `zizmor` Security | `trivy fs --scanners misconfig,secret` (`0` findings across `Dockerfile.{research,sandbox,xerj}` & `uv.lock`); `zizmor --offline .github/workflows/ci.yml` (`0` findings) |
| **MCP Code-Graph Index** | `codebase-memory` project `Z-tier0-sota-clean` indexed cleanly (`0` skipped files, `1,045` nodes, `2,564` edges); `xerj_status` cluster `green` with `.xerj-memory-tier0-compounding-lessons` preserved |

### 4-Runtime MCP Parity & Zero Self-Invented Hooks

All 5 configs (`.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, `opencode.json`) wire the exact 4 vetted native `uv` MCP servers with zero unvetted broker MCPs and zero self-invented runtime hooks (`hooks_zero = True`):
- `codebase-memory` (`codebase-memory-mcp`)
- `xerj` (`xerj mcp` -> `http://127.0.0.1:9200`)
- `nautilus-engine` (`uv run --no-sync python scripts/nautilus_mcp_server.py` — `FastMCP("tier0-nautilus-engine")`, `10/10` tools verified via `--smoke`)
- `edgartools` (`uv run --no-sync edgartools-mcp`)
- Scoped Claude lane MCP profiles in `.claude/mcp-lanes/{sim,paper,tv,gate}.mcp.json`
- Standalone `46/46` SHA-locked skills vendored inside `.claude/skills/` per `UPSTREAM_SKILLS_LOCK.json`
- Formal & red-team evals in `evals/formal_risk_z3.py` and `evals/tier0_inspect_eval.py`, plus OSI-only `spdx_license_firewall` in `SOTA_STARRED_AND_BEYOND_MANIFEST.json`

---

## 2. Hosted SOTA Portals & Paper Farm Readiness

### 13 Hosted Loopback Portals (`127.0.0.1`)

| # | Portal | Loopback URL | Probe Result |
|---|---|---|---|
| 1 | XERJ Memory/RAG | `http://127.0.0.1:9200/` | `HTTP 200` |
| 2 | Zensical Docs | `http://127.0.0.1:8010/` | `HTTP 200` |
| 3 | MLflow 3 | `http://127.0.0.1:5000/` | `HTTP 200` |
| 4 | Dagster Webserver | `http://127.0.0.1:3000/` | `HTTP 200` |
| 5 | Optuna Dashboard | `http://127.0.0.1:8080/` | `HTTP 200` |
| 6 | Inspect AI View | `http://127.0.0.1:7575/` | `HTTP 200` |
| 7 | Prometheus | `http://127.0.0.1:9090/` | `HTTP 200` |
| 8 | Grafana | `http://127.0.0.1:3001/` | `HTTP 200` |
| 9 | OTel Telemetry Exporter | `http://127.0.0.1:9464/metrics` | `HTTP 200` |
| 10 | Official Tearsheets | `http://127.0.0.1:8793/` | `HTTP 200` |
| 11 | FINOS Workspace | `http://127.0.0.1:1420/` | `HTTP 200` |
| 12 | FINOS API Health | `http://127.0.0.1:8000/health` | `HTTP 200` |
| 13 | OpenBB Backend | `http://127.0.0.1:6900/` | `HTTP 200` |

*Companion SQL UI verified:* DuckDB SQL UI (`http://127.0.0.1:4213/`) -> `HTTP 200`.

### 4-Account Paper Farm (`Z:\sota-data\manifests`)

- `ALPACA_PA1` (`L1_ORB_BURST`): Active heartbeat (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`, isolated DuckDB ledger `paper_lane_pa1.duckdb` / `alpaca_pa1*.duckdb`).
- `ALPACA_PA2` (`L2_STRUCTURAL_TREND`): Active heartbeat (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`, isolated DuckDB ledger `paper_lane_pa2.duckdb` / `alpaca_pa2*.duckdb`).
- `ALPACA_PA3` (`L3_AVWAP_TIGHT_LOCK`): Active heartbeat (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`, isolated DuckDB ledger `paper_lane_pa3.duckdb` / `alpaca_pa3*.duckdb`).
- `IBKR_DU_4002` (Paper Gateway `:4002`): Adapter configured (`paper_only = True`, `live_execution_enabled = False`); gateway socket `:4002` offline unless IB Gateway Paper is started.

---

## 3. Safe Archival & Orca Terminal Audit

- **Superseded Scratch Archive (`Z:\archive\superseded_2026-11-25`):** Non-destructively moved stray root diffs (`tmp_pine.diff`, `tmp_rev.diff`), `*.bak-*` script backups, and superseded harness directories out of root `Z:\` with zero deletions.
- **Pre-Grand-Reset Archive (`Z:\archive\pre_grand_reset_2026-11-25`):** Managed safely by `scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1` (`-DryRun` default; pass `-Execute -ArchiveLegacyDirs <paths>` when ready).
- **Orca Terminal Audit:** Preserved all `15` active non-orphaned `orphaned = false` workspace tabs untouched; `5` `orphaned = true` legacy PTY tabs returned `terminal_handle_stale` on `orca terminal close --tab` and remain untouched without force-killing processes (dismiss manually in Orca UI if desired).

---

## 4. Protected Shared Data & Active Roots

| Directory | Role | Handling |
|---|---|---|
| `Z:\tier0-sota-clean` (`$env:TIER0_REPO_ROOT`) | Standalone Tier-0 SOTA clean repository (`L0..L10`, `46/46` skills, `0` legacy refs, `0` `yfinance`) | **ACTIVE CANONICAL REPO** |
| WSL2 `Noesis-SOTA` (`~/tier0-sota-clean/.venv`) | Isolated Linux `.venv` (`Python 3.13.18`, `314` packages, `nautilus_trader 2.0.0rc9`, `polars 2.2.0`; `Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1`, `nautilus_mcp_server.py --smoke` `10/10`, `pytest` `41 passed`, `shellcheck` `0` errors) | **ACTIVE WSL2 PARITY VENV** (preserves `~/.config/tier0/.env.*` with `chmod 600`) |
| `Z:\sota-data\manifests` (`$env:TIER0_DATA_ROOT\manifests`) | Active live Paper Farm & TV manifests/DuckDB ledgers | **PRESERVED SHARED DATA ROOT** (never delete) |
| `Z:\sota-data\prewall-lake` (`$env:TIER0_DATA_ROOT\prewall-lake`) | Pre-wall (`<= 2026-10-15`) immutable market data lake | **PRESERVED SHARED DATA ROOT** (never delete) |
| `Z:\sota-cache` | Shared `uv` wheel & Python cache | **PRESERVED SHARED CACHE** (never delete) |
| `Z:\archive` | Non-destructive archival store (`superseded_2026-11-25`, `pre_grand_reset_2026-11-25`) | **PRESERVED ARCHIVE ROOT** |

### Release Verification Gates (`v1.0.0-sota-clean`)

- **Formatting & Dependencies:** `ruff format --check` (`src/tier0_sota/risk.py` excluded, SHA-256 `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847` byte-identical) and `deptry .` (`0` dependency issues).
- **CI Hardening:** `.github/workflows/ci.yml` (`zizmor --offline` = `0` findings), `scripts/check.ps1` (PS1 AST `0` errors), and `scripts/check.sh` (`shellcheck` `0` errors across all `.sh` scripts).

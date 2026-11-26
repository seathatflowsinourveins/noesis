# Tier-0 Clean Ecosystem Manifest (`Z:\tier0-sota-clean`)

- **Generated:** 2026-11-25
- **Target Clean-Room Repository:** `Z:\tier0-sota-clean` (branch `main`, Python 3.13 via `uv`)
- **Immutable Risk Kernel SHA-256 (`src/tier0_sota/risk.py`):** `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`
- **Live Execution Guard:** `live_execution_enabled = False`, `paper_only = True`, zero live broker ports (`:4001`, `:7496`, `:8798`, `:8796`)

---

## 1. Clean Ecosystem Entrypoints

| Script / Config | Role | Notes |
|---|---|---|
| [`scripts/setup_clean_ecosystem.ps1`](../scripts/setup_clean_ecosystem.ps1) | Idempotent setup & health check | Verifies `.venv`, `risk.py` SHA-256, 4-runtime MCP parity, `13/13` hosted portals, Paper Farm readiness, and `0` live broker ports |
| [`scripts/launch_clean_lanes.ps1`](../scripts/launch_clean_lanes.ps1) | Non-overlapping lane launcher | Supports `-DryRun`; orchestrates Pre-Wall Sim Lab (`<= 2026-10-15`), 3-Lane Paper Farm (`ALPACA_PA1..PA3` + `IBKR_DU_4002`), TV Half-Auto Cockpit, and Verifier Gate |
| [`scripts/run_live_paper_farm.ps1`](../scripts/run_live_paper_farm.ps1) | Paper Farm controller | Supports `preflight`, `scan`, `paper-farm` (requires `-ConfirmPaper`), and `performance` |
| [`scripts/tv_console.ps1`](../scripts/tv_console.ps1) | TradingView Half-Auto console | Display-only Pine v6 staging (`qty=1`, `semi_auto_confirmed=false`, zero CDP `:9223`) |

### 4-Runtime MCP Parity

All 5 configs (`.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, `opencode.json`) wire the exact 4 vetted MCP servers with zero unvetted broker MCPs:
- `codebase-memory` (`codebase-memory-mcp`)
- `xerj` (`xerj mcp` -> `http://127.0.0.1:9200`)
- `nautilus-engine` (`scripts/nautilus_mcp_server.py` — `FastMCP("tier0-nautilus-engine")`, `10/10` tools verified via `--smoke`, `0` warnings)
- `edgartools` (`edgartools-mcp`)

---

## 2. Hosted SOTA Portals & Paper Farm Readiness

### 13 Hosted Loopback Portals (`127.0.0.1`)

| # | Portal | Loopback URL | Probe Result (2026-11-25) |
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

*Bonus companion verified:* DuckDB SQL UI (`http://127.0.0.1:4213/`) -> `HTTP 200`.

### 4-Account Paper Farm (`Z:\sota-data\manifests`)

- `ALPACA_PA1` (`L1_ORB_BURST`): Active heartbeat (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`, isolated DuckDB `alpaca_pa1_seamless_orders.duckdb`).
- `ALPACA_PA2` (`L2_STRUCTURAL_TREND`): Active heartbeat (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`, isolated DuckDB `alpaca_pa2_seamless_orders.duckdb`).
- `ALPACA_PA3` (`L3_AVWAP_TIGHT_LOCK`): Active heartbeat (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`, isolated DuckDB `alpaca_pa3_seamless_orders.duckdb`).
- `IBKR_DU_4002` (Paper Gateway `:4002`): Adapter configured (`paper_only = True`, `live_execution_enabled = False`); gateway socket `:4002` offline unless IB Gateway Paper is started.

---

## 3. Safe Superseded Cleanup Receipt (`Z:\archive\superseded_2026-11-25`)

Moved non-destructively (zero deletions):
- `Z:\tmp_pine.diff` -> `Z:\archive\superseded_2026-11-25\tmp_pine.diff` (`21,776` bytes)
- `Z:\tmp_rev.diff` -> `Z:\archive\superseded_2026-11-25\tmp_rev.diff` (`28,578` bytes)
- `Z:\sota-data\research-runs\sota_compounding_1000rpm_harness\run_live_24x5_lane_terminal.ps1.bak-2026-11-23` -> `Z:\archive\superseded_2026-11-25\sota_compounding_1000rpm_harness\run_live_24x5_lane_terminal.ps1.bak-2026-11-23`
- `Z:\sota-data\research-runs\sota_compounding_1000rpm_harness\run_live_24x5_lane_terminal.ps1.bak-2026-11-23-pre-pins` -> `Z:\archive\superseded_2026-11-25\sota_compounding_1000rpm_harness\run_live_24x5_lane_terminal.ps1.bak-2026-11-25-pre-pins`
- `Z:\sota-data\research-runs\sota_compounding_1000rpm_harness\start_ungated_paper_farm_lane.ps1.bak-2026-11-23` -> `Z:\archive\superseded_2026-11-25\sota_compounding_1000rpm_harness\start_ungated_paper_farm_lane.ps1.bak-2026-11-23`
- `Z:\sota-live-tv-harness` diff preserved at `Z:\archive\superseded_2026-11-25\sota_live_tv_harness_dirty.diff` (`0` bytes; working tree clean at move time) and full repo moved to `Z:\archive\superseded_2026-11-25\sota-live-tv-harness`.
- `Z:\CLEAN LIVE` archival blocker (preserved intact, zero force-kills): `Move-Item` failed with OS directory handle lock (`"The process cannot access the file because it is being used by another process"`) held by the `5` orphaned Orca PTY processes whose CWD is `Z:\CLEAN LIVE`.

### Zero-Warning Setup Gate (`setup_clean_ecosystem.ps1 -Strict`)

Verified post-finalization (`warnings_count=0`, `failures_count=0`):
- `[OK] Live broker firewall clean: ports 4001/7496/8798/8796 = 0`
- `[OK] .venv ready: py=3.13.14 nautilus=2.0.0rc9 polars=2.2.0`
- `[OK] risk.py SHA-256 verified (ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847)`
- `[OK] 4-runtime MCP parity verified (5/5 configs -> codebase-memory, xerj, nautilus-engine, edgartools)`
- `[OK] Hosted portals healthy: 13/13 HTTP 200`
- `[INFO] Paper Farm: preflight=True | ALPACA_PA1..PA3 fresh=3/3 | IBKR_DU_4002_port4002_open=False`
- `[VERDICT: PASS] Clean ecosystem verified (13/13 portals, 5/5 MCP parity, Paper preflight=True, live_ports=0).`

---

## 4. Orca Terminal Audit (`orca terminal list --json`)

### Preserved Active Tabs (`15` tabs on `Z:\tier0-quant`, `orphaned = false`)

All active Paper Farm, TV Cockpit, live whole-market, and owner/agent portals on `Z:\tier0-quant` were left running untouched:
- `term_3f39587f-8ba6-419b-9884-33e505bed1f6` — Live whole-market auto-cycle (`pwsh.exe`)
- `term_08f417ed-90d4-4489-b3d7-4f87809973a8` — `[03-TV] Live TV & 1000rpm Lossless Cockpit`
- `term_fb7a406d-5d8e-454e-9d79-ca94c889ee5b` — `[04-PAPER] 4-Acct Paper Farm Gatekeeper`
- `term_f63a385c-caf6-49a9-98a1-5d721c7f8aae` — `[05-VETO] Stage-5 Live Firewall (LIVE REFUSED)`
- `term_08e43820-1897-4220-a0e1-9158ee792799` — `[06-SYS] System, Resources & Terminals Ops`
- `term_7f87b387-a08b-4467-b2c7-8bc98d8c7141` — `[06-OC] OpenCode Multi-Vendor Lane`
- Plus 9 active Claude/Codex/Antigravity agent & orchestration tabs (`term_b7692235`, `term_d064ddae`, `term_2721e00f`, `term_f53eeb70`, `term_90acde16`, `term_fd840733`, `term_99abfc05`, `term_f17e5ba0`, `term_539feced`).

### Orphaned Legacy Tabs Logged (`5` tabs on superseded `Z:\CLEAN LIVE`, `orphaned = true`)

Attempted `orca terminal close --terminal <handle> --tab --json`; Orca returned `terminal_handle_stale` because their parent worktree is no longer registered. Left logged for manual UI tab dismissal if desired:
- `term_e363708e-9338-4068-9798-940603e629cc` (`Z:\CLEAN LIVE` shell)
- `term_6f9b356e-a378-4d2f-96ca-ecd7af0f7854` (`Orca orchestration message check`, claude)
- `term_f58b56c4-d8a7-4a59-a35b-ada3be509dc7` (`π - LANE: Land live lane with upstream resolution`, omp)
- `term_18e5835f-a4b9-4759-afb8-2dece9100b21` (`pwsh.exe`, antigravity)
- `term_f2507da6-9370-47b2-af60-656474ad910e` (`Set up clean live lane | CLEAN LIVE`, codex)

---

## 5. Legacy & Reference Directories Inventory

| Directory | Contents / Git State | Migration Status | Safe Handling / Archive Instructions |
|---|---|---|---|
| `Z:\tier0-quant` | ~165,903 files; `git status` clean (`0` dirty lines) on `main` (`f15fe6f`) | **PRESERVED AS IMMUTABLE REFERENCE** (canonical `risk.py`, Docker SOTA compose stack, MADR 4.0 ADRs) | **DO NOT DELETE OR ARCHIVE.** Read-only upstream reference and host for the `13/13` Docker SOTA portals. |
| `Z:\sota-live-tv-harness` | ~76,438 files (includes `.venv`, `site`); HEAD `4846bbf` | **ARCHIVED** (`Z:\archive\superseded_2026-11-25\sota-live-tv-harness`; diff `sota_live_tv_harness_dirty.diff` `0` bytes); migrated into `pine/`, `src/tier0_sota/tv/`, `scripts/tv_console.ps1`, `docs/TV_AUTOMATION_PLAYBOOK.md` | Archive complete (`2026-11-25`). Do not restore into root `Z:\`. |
| `Z:\CLEAN LIVE` | 4 files (`AGENTS.md`, `.claude\commands\lane-live.md`, `.ruff_cache\*`); not a git repo | **SUPERSEDED** staging folder replaced by `Z:\tier0-sota-clean`; **OS-LOCKED** during move (`5` orphaned Orca PTY CWDs) | Dismiss the `5` orphaned Orca tabs in UI to release the directory lock, then run: `Move-Item "Z:\CLEAN LIVE" "Z:\archive\superseded_2026-11-25\CLEAN_LIVE"` |
| `Z:\sota-data\manifests` | Active live Paper Farm & TV manifests/DuckDB ledgers | **PRESERVED SHARED DATA ROOT** | **NEVER DELETE.** Shared state plane consumed by `PA1..PA3` and `tv_sota_pack`. |
| `Z:\sota-data\prewall-lake` | Pre-wall (`<= 2026-10-15`) immutable market data lake | **PRESERVED SHARED DATA ROOT** | **NEVER DELETE.** Read-only pre-wall PIT lake. |
| `Z:\sota-data\research-runs\sota_compounding_1000rpm_harness` | Active Paper Farm daemon scripts (`start_alpaca_pa1_seamless_lane.ps1` etc.) | **ACTIVE RUNTIME HARNESS** (`.bak-*` files archived) | Keep live daemon scripts running until cutover to `Z:\tier0-sota-clean\scripts\run_live_paper_farm.ps1` is owner-scheduled. |

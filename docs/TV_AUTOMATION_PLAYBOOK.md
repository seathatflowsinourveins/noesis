# Tier-0 SOTA TradingView Automation Playbook

Display-only 4-script Pine v6 cockpit + Half-Automation order ticket staging & paper-only confirmation bridge (`semi_auto_confirmed = false`, `qty = 1`, `paper_only = true`, `live_execution_enabled = false`, zero CDP `:9223`, zero live broker endpoints). Governed by [ADR-0010](adr/ADR-0010.md), [ADR-0020](adr/ADR-0020.md), [ADR-0021](adr/ADR-0021.md), [ADR-0023](adr/ADR-0023.md), and [ADR-0027](adr/ADR-0027.md).

---

## 1. Compliance, Paper-Only & Stage-5 Venue Posture

- **Non-Repainting Pine v6:** All 4 scripts in `pine/` enforce `barstate.isconfirmed`, `gaps=barmerge.gaps_off`, and `lookahead=barmerge.lookahead_off`.
- **Fail-Closed Alert Bridge:** Every inbound alert validates against Draft 2020-12 `src/tier0_sota/tv/schema_v1.json` in `src/tier0_sota/tv/bridge.py` (`TvAlertV1`, `extra="forbid"`) with `qty = 1`, `semi_auto_confirmed = false`, `60s` re-entry cooldown, exact `Decimal` Rule-612 inward stops (`<= 2.00%` cap, Z3-proven in `evals/formal_risk_z3.py`), `MAX_INPUT_AGE_MS = 900_000` (15m) on `--live-intake`, and fail-closed live endpoint refusal (`paper-api` / Paper `:4002` only).
- **Zero Custom HTTP Server & Zero CDP Boundary:** `rules/zero_self_invention.yml` (`no-custom-dashboard-server`, `no-raw-http-socket-in-src`) prohibits custom Python HTTP webhook servers, and [ADR-0027](adr/ADR-0027.md) blocks TradingView Desktop CDP attachment (`:9223`), unofficial scrapers, and proprietary SaaS webhook relays (including TradersPost's commercial SaaS relay). For official first-party watchlist CRUD, alert management, and Pine v6 compilation/backtesting without CDP, connect TradingView's hosted Streamable HTTP + OAuth 2.1 MCP (`https://mcp.tradingview.com/mcp`) at the user/IDE level (see Section 7). Live auto-refreshing analytics run via upstream `L10` portals (`FINOS Workspace :1420`, `OpenBB :6900`, `Caddy :8793/tv_sota_pack/`, `DuckDB SQL UI :4213`, `Grafana :3001`, `Dagster :3000`).
- **Paper-Only Capacity vs Dormant Stage-5 Reference Spec (`src/tier0_sota/engine.py:EXECUTION_VENUE_PROFILES`, [ADR-0020](adr/ADR-0020.md)):**

| Venue Profile | Environment | Rate Limit | Exit Reserve | Status |
|---|---|---:|---:|---|
| `ALPACA_PAPER_200` (`PA1..PA3`) | `PAPER` | `200 rpm` / lane (`600 rpm` total) | `20 rpm` / lane | Active default (`paper_only = true`, `fnox --profile paper`) |
| `ALPACA_PAPER_1000` (`PA1..PA3`) | `PAPER` | `1,000 rpm` / `25 rps` / lane (`3,000 rpm` total) | `100 rpm` / lane | Opt-in paper (`TIER0_ALPACA_RPM=1000`, `6,000 rpm` combined Paper Farm, `paper_only = true`) |
| `IBKR_PAPER_4002` (`IBKR_DU_4002`) | `PAPER` | `3,000 rpm` (`50 rps`) | `300 rpm` | Active 24/5 overnight paper (`3,600 rpm` default / `6,000 rpm` with `ALPACA_PAPER_1000`) |
| `ALPACA_LIVE_PRO_LINE_1000` | `LIVE` (SANDBOX spec) | `1,000 rpm` (`25 rps`) | `100 rpm` | **LIVE REFUSED** (`live_execution_enabled = false`, `LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC`, dormant `~/.config/fnox/live-age.txt` per [ADR-0023](adr/ADR-0023.md)) |

---

## 2. Four-Script Non-Repainting Pine v6 Suite (`pine/*.pine`)

| Script | Kind / Pane | Role & Alert Emission |
|---|---|---|
| `pine/tier0_omni_sota_24x5.pine` | `indicator()` / Pane 0 | 8 Omni presets (`OMNI_ORDER_PRESET_CATALOG`), `OrderPresetCard` UDT HUD, 4-tier R-lock + VWAP bands, `alert(emit_alert_json(...), alert.freq_once_per_bar_close)` (`script_kind: "indicator"`) + 11 `alertcondition()` hooks. |
| `pine/tier0_sota_strategy_24x5.pine` | `strategy()` / Pane 0 Tester | Strategy Tester execution parity (`default_qty_value=1`, `0.04%` commission, `-1.50%` `strategy.risk.max_intraday_loss`, flatten-first auto-reverse, `60s` cooldown) emitting `TvAlertV1` JSON (`script_kind: "strategy"`) via `//@strategy_alert_message {{strategy.order.alert_message}}`. |
| `pine/tier0_market_regime.pine` | `indicator()` / Pane 1 | Multi-feed `SPY`/`QQQ`/`SOXX`/`RSP`/`VIX`/`TLT` 0–100 health gauge, discrete `-1/0/+1` causal regime, `DIRECT_SHORT_ETB` vs `SOXS`/`SQQQ` directive + 4 `alertcondition()` hooks. |
| `pine/tier0_darkpool_uv_buildup.pine` | `indicator()` / Pane 2 | RVOL Z-score histogram, 3-bar volume velocity ramp (`VBR`), Hidden-Volume Ratio (`HVR`), VWAP-pin absorption, session CVD divergence + 6 `alertcondition()` hooks. |

---

## 3. Complete 15-File `tv_sota_pack/` Outputs & Auto-Synced Portals

Every 60s via WSL2 systemd `tier0-paper-farm-observe.timer` (or on demand via `just tv-sync` / `just paper-farm`), `src/tier0_sota/live/runner.py` invokes `src/tier0_sota/tv/tickets.py:refresh_tickets_from_paths()` and atomically writes all 15 files into `Z:\sota-data\manifests\tv_sota_pack\` (served read-only at `http://127.0.0.1:8793/tv_sota_pack/`):

1. `tier0_sota_tv_watchlists_live.txt` — TradingView-importable `EXCHANGE:SYMBOL` sectioned watchlist (`###01_MACRO_REGIME_BREADTH` through `###06_OVERNIGHT_24X5_LEADERS`, plus manual staged preservation).
2. `live_half_auto_tickets.json` + `LIVE_HALF_AUTO_ORDER_TICKETS.md` — Active extreme-mover tickets (strict `<60s` bar freshness + XNYS session match, `TvAlertV1`-validated) + standby Rule-612 presets + manual Pine staged section.
3. `tv_order_presets.json` + `TV_ORDER_PRESET_CARDS.md` — `3 x 4` NATR×Session matrix, 8 Omni presets, 4-tier R-lock ladder, copy-ready TV Order Panel cards, and venue order-handle parity metadata (`3,600 rpm` default / `6,000 rpm` with `ALPACA_PAPER_1000` Paper Farm; `1,000 rpm` SANDBOX-refused live spec).
4. `staged_scan_presets.json` — Scanner-owned `tier0.staged-tv-presets.v1` snapshot (isolated from manual history).
5. `staged_tv_presets.json` — Manual/Pine alert bridge history (`alert_id`-deduplicated, `semi_auto_confirmed = false`, `qty = 1`).
6. `finos_tv_cockpit.json` — Zero-import live cockpit export (`@finos-workspace/plotly-charts@2.5.0`, `openbb_rows`, `tradingview/lightweight-charts` v5 markers & rungs) consumed by FINOS (`http://localhost:1420/`) and OpenBB (`http://127.0.0.1:6900/widgets.json`).
7. `tv_compounding_manifest.json` — Deterministic counts, Paper Farm lane mapping, and 4-script SHA-256 digests feeding `paper_farm_live_performance_manifest.json`, Dagster (`:3000`), Grafana (`:3001`), `nautilus-engine` MCP, and `just reflect` (MLflow/Optuna/DSPy/XERJ).
8. `tv_half_auto_audit.duckdb` + `tv_staged_presets.parquet` — 29-column DuckDB audit log (incl. `venue_profile`) + atomic read-only Parquet mirror for DuckDB SQL UI (`http://127.0.0.1:4213/`).
9. `tier0_omni_sota_24x5.pine`, `tier0_sota_strategy_24x5.pine`, `tier0_market_regime.pine`, `tier0_darkpool_uv_buildup.pine` — SHA-256 synced pack copies.

---

## 4. Multi-Pane TradingView Layouts

### Layout A — Omni 4-Pane Active Trading (Default `09:30 ET`)

| Pane | Symbol & Timeframe | Indicator & Mode | Sync |
|---|---|---|---|
| **Top-Left** | `AMEX:SPY` (`5m`, Ext Hours ON) | `pine/tier0_market_regime.pine` (`T0_MKT_STATUS`) | Independent |
| **Top-Right** | Active Mover (`1m`, Ext Hours ON) | `pine/tier0_omni_sota_24x5.pine` (`AUTO_REGIME_ADAPTIVE`) + Pane 2 `tier0_darkpool_uv_buildup.pine` | Blue Sync |
| **Bottom-Left** | Active Mover (`15m`/`D`, Ext Hours ON) | `pine/tier0_omni_sota_24x5.pine` (`Show HUD=OFF`) or `tier0_sota_strategy_24x5.pine` | Blue Sync |
| **Bottom-Right** | `AMEX:SOXS` / `NASDAQ:SQQQ` (`1m`, Ext Hours ON) | `pine/tier0_omni_sota_24x5.pine` (`CRASH_DEFENSIVE_INVERSE`) | Independent |

### Layout B — 6-Pane Crash-Defensive Command Center (`VIX >= 25`)

1. `AMEX:SPY` (`5m` + `T0_MKT_STATUS`) | 2. `NASDAQ:QQQ` (`5m`) | 3. `NASDAQ:SOXX` (`5m` + `T0_MKT_STATUS`)
4. `AMEX:SOXS` (`1m` + `CRASH_DEFENSIVE_INVERSE`) | 5. `NASDAQ:SQQQ` (`1m` + `CRASH_DEFENSIVE_INVERSE`) | 6. Active ETB Leader Short (`1m`)

### Layout C — 2-Pane 24/5 Overnight Monitor (`20:00–04:00 ET`, IBKR)

- **Left (`65%`):** Active Mover (`1m`, Ext Hours ON) with `OVERNIGHT_24X5_IBKR` (`<= 1.00%` stop cap, `50 bps` `STOP_LIMIT` collar).
- **Right (`35%`):** `AMEX:SPY` or `NASDAQ:SOXX` (`5m`) with `T0_MKT_STATUS`.

---

## 5. Lossless R-Lock, Auto-Reverse, Short Routing & `3 x 4` Matrix

- **4-Tier Progressive R-Lock (`tier0_omni_sota_24x5.pine`, `tier0_sota_strategy_24x5.pine`, `tickets.py`):**
  1. `T1 BE+` — Arms breakeven + fees/spread floor (`fee_buf`, `BE+10bp`).
  2. `T2 +1R Lock` — Locks `+0.45R` (`lock_at_1r`), tightens trail to `0.65x`.
  3. `T3 +2R Lock` — Locks `+1.35R` (`lock_at_2r`), tightens trail to `0.40x`.
  4. `T4 +3R / Fast-Sell` — Locks `+2.35R` (`lock_at_3r`), tightens trail to `0.25x`; `fast_sell_climax` triggers flatten-first auto-reverse short (`fast_sell_rev_trigger`).
- **Seamless Re-Entry:** `60s` minimum cooldown (`reentry_cooldown_secs >= 60`) after exit.
- **Short Borrow Routing:** Extreme movers with verified `shortable && easy_to_borrow` route as `DIRECT_SHORT_ETB`; non-ETB / unverified names fail closed to `INVERSE_HEDGE_SOXS_SQQQ` (`AMEX:SOXS` / `NASDAQ:SQQQ`). `ALPACA` `OVERNIGHT` `SELL` is refused (`ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ`).

| Schedule (`NATR`) | Session | Order Type | Init Cap | Collar |
|---|---|---|---:|---:|
| `tight_near_lossless` (`<= 1.5%`) | `RTH` / `PRE\|AFTER` / `OVERNIGHT` | `STOP_MARKET` / `STOP_LIMIT` / `STOP_LIMIT` | `1.50%` / `1.25%` / `1.00%` | `0` / `35` / `50 bps` |
| `baseline` (`1.5%..3.0%`) | `RTH` / `PRE\|AFTER` / `OVERNIGHT` | `STOP_MARKET` / `STOP_LIMIT` / `STOP_LIMIT` | `2.00%` / `1.50%` / `1.00%` | `0` / `35` / `50 bps` |
| `wide_natr_adaptive` (`> 3.0%`) | `RTH` / `PRE\|AFTER` / `OVERNIGHT` | `STOP_MARKET` / `STOP_LIMIT` / `STOP_LIMIT` | `2.00%` / `1.50%` / `1.00%` | `0` / `35` / `50 bps` |

---

## 6. Operator Sync & Paper-Only Confirmation Commands

### WSL2 / Linux (`scripts/tv_console.sh` + `justfile` — Recommended)

```bash
# 1. One-shot live SIP scan + preset bundle + full 15-file tv_sota_pack refresh (paper-only, zero 60s gap)
just tv-sync

# 2. Land tv_sota_pack, SHA-sync all 4 Pine scripts, and copy tier0_omni_sota_24x5.pine to clipboard
bash scripts/tv_console.sh land ALPACA

# 3. Copy Windows Z:\... watchlist path (via wslpath -w + clip.exe) -> TV Watchlist (...) -> Import list... -> Ctrl+V -> Open
bash scripts/tv_console.sh watchlist ALPACA

# 4. Copy any of the 4 Pine v6 scripts or view copy-ready Order Panel cards
bash scripts/tv_console.sh copy-omni      # Pane 0 Omni indicator
bash scripts/tv_console.sh copy-strategy  # Pane 0 Strategy Tester
bash scripts/tv_console.sh copy-regime    # Pane 1 Market Regime gauge
bash scripts/tv_console.sh copy-darkpool  # Pane 2 Darkpool / UV buildup
bash scripts/tv_console.sh presets        # Copy-ready TV Order Preset cards

# 5. Inbound alert ingest (CLI / clipboard / stdin / file -> TvAlertV1 -> staged_tv_presets.json + DuckDB + Parquet)
bash scripts/tv_console.sh ingest-clip IBKR --live-intake
bash scripts/tv_console.sh ingest /path/to/alert.json IBKR --live-intake
bash scripts/tv_console.sh stage NVDA SELL 146.85 148.68 0.012 IBKR

# 6. Explicit human-confirmed PAPER-ONLY handoff (src/tier0_sota/live/execution.py::submit_confirmed_tv_preset_paper)
#    Strictly fnox exec --profile paper; zero live keys; supports --dry-run precheck first.
just tv-confirm-paper <16-hex-alert-id> --dry-run
bash scripts/tv_console.sh confirm-paper <16-hex-alert-id> --qty 1

# 7. Audit DuckDB summary, inspect live portals, and run TV E2E gate
bash scripts/tv_console.sh audit
bash scripts/tv_console.sh portals
just tv-e2e
```

### Windows PowerShell (`scripts/tv_console.ps1`)

```powershell
. .\scripts\tv_console.ps1
tv-land                                        # Refresh tickets + sync 4 Pine scripts + copy Omni Pine
tv-copy-watchlist                              # Refresh & copy Z:\...\tier0_sota_tv_watchlists_live.txt
tv-copy-omni; tv-copy-strategy; tv-copy-regime; tv-copy-darkpool
tv-presets                                     # Display copy-ready TV Order Preset cards
tv-ingest -Clipboard -Venue IBKR -LiveIntake   # Validate & stage TV alert JSON from clipboard
tv-stage -Symbol NVDA -Side SELL -Price 146.85 -StopPrice 148.68 -NatrPct 0.012
tv-confirm-paper -AlertId <16-hex-alert-id> -DryRun   # Paper-only fnox --profile paper precheck
tv-portals                                     # Open FINOS :1420, OpenBB :6900, Grafana :3001, Dagster :3000, DuckDB :4213, Caddy :8793
tv-e2e                                         # Run ruff, ty, and pytest tests/test_tv_automation.py
```

> **Note:** On Windows, if `Z:\noesis\.venv\Scripts\python.exe` is out of sync, `scripts/tv_console.ps1` automatically falls back to `uv run --no-sync python` or prints actionable repair/WSL2 instructions.

---

## 7. TradingView MCP Connectors (Official OAuth 2.1 + Zero-CDP MIT Companions)

### 7.1 Official TradingView First-Party MCP (`https://mcp.tradingview.com/mcp`)

- **Transport, Auth & Plan:** Official TradingView hosted **Streamable HTTP + OAuth 2.1** MCP (`https://mcp.tradingview.com/mcp`, **Essential+ paid plans**, **zero CDP**, zero scraping).
- **Supported Capabilities:**
  - **Watchlists CRUD:** `list_watchlists`, `get_watchlist`, `get_active_watchlist`, `create_watchlist`, `delete_watchlist`, `add_to_watchlist`, `remove_from_watchlist`, `update_watchlist` (syncs exchange-qualified watchlists directly alongside `tier0_sota_tv_watchlists_live.txt` import).
  - **Alerts CRUD + Logs:** Create, read, update, delete TradingView alerts and inspect trigger logs.
  - **Official Pine v6 Compiler & Backtest Lifecycle:** `validate_pine_script`, `run_pine_indicator`, `run_pine_backtest` (including **Deep Backtesting**), `search_pine_scripts`, `get_pine_script_source`, `create_pine_script`, `update_pine_script`.
- **Explicit Execution & Order-Preset Boundary:** TradingView's official MCP does **NOT** expose native Supercharts Order Panel Presets or direct broker order execution. Executable multi-situation 24/5 lossless trailing order presets therefore remain locked inside our local `nautilus-engine` + `src/tier0_sota/tv/tickets.py` + `src/tier0_sota/live/execution.py` (`paper_only = True`, `live_execution_enabled = False`, `qty = 1`, `semi_auto_confirmed = False`).

### 7.2 Exact 1-Step OAuth Connector Setup (Preserving 4-Stdio-MCP Parity)

> **Parity Guardrail (`5/5` Repo Configs + Preflight):** Never add `https://mcp.tradingview.com/mcp` to `.mcp.json`, `.claude/settings.json`, `.gemini/settings.json`, `opencode.json`, `.codex/config.toml`, or the WSL2 `~/.gemini/config/mcp_config.json` 4-stdio block. All 5 repository agent configs MUST preserve exact `4`-stdio-MCP parity (`codebase-memory`, `edgartools`, `nautilus-engine`, `xerj`), enforced mechanically by `tests/test_architecture_manifest.py` and `scripts/preflight_resume.sh`. Configure TradingView MCP strictly at the **user / IDE OAuth connector** layer:

1. **Antigravity (AGY):** Open **MCP Store / Connectors -> Manage MCP Servers -> Add Remote Server (Streamable HTTP)** -> enter URL `https://mcp.tradingview.com/mcp` (user-level OAuth connector; do not edit repo `.mcp.json` or `~/.gemini/config/mcp_config.json`) -> click **Connect** and approve TradingView OAuth 2.1 in your browser.
2. **Claude (Claude.ai / Claude Desktop / Claude Code user scope):**
   - *Claude.ai / Claude Desktop:* **Settings -> Connectors -> Add custom connector** -> URL `https://mcp.tradingview.com/mcp` -> **Connect** -> sign in via TradingView OAuth 2.1.
   - *Claude Code CLI (user-scoped only; leaves `.mcp.json` and `.claude/settings.json` untouched):*

     ```bash
     claude mcp add --transport http --scope user tradingview https://mcp.tradingview.com/mcp
     ```

     Then run `/mcp` in Claude Code and select **Authenticate** to finish TradingView OAuth 2.1.
3. **Cursor (user-global `~/.cursor/mcp.json`):** Open **Cursor Settings -> Tools & MCP -> Add Custom MCP** (edits user-level `~/.cursor/mcp.json` only) and add:

   ```json
   {
     "mcpServers": {
       "tradingview": {
         "url": "https://mcp.tradingview.com/mcp"
       }
     }
   }
   ```

   Then click **Connect / Needs Login** in Cursor's MCP panel to complete TradingView OAuth 2.1.

### 7.3 Zero-CDP MIT Pine v6 Local Companion MCPs (On-Demand)

For offline/local Pine v6 syntax and reference lookup without adding packages to `pyproject.toml` or modifying the 5 repo MCP configs:

| Companion MCP | License | On-Demand Invocation | Scope & SPDX Firewall Rule |
|---|---|---|---|
| `paulieb89/pinescript-mcp` | `MIT` | `uvx pinescript-mcp` (PyPI `pinescript-mcp==0.7.7`) | Zero-CDP MIT local Pine v6 docs/syntax lookup companion MCP. |
| `TradersPost/pine-mcp` | `MIT` | `npx -y pinescript-mcp-server` | Zero-CDP MIT Node Pine v6 **docs MCP ONLY**. TradersPost's proprietary commercial SaaS webhook relay remains **explicitly excluded** under the SPDX OSI firewall ([ADR-0027](adr/ADR-0027.md)). |

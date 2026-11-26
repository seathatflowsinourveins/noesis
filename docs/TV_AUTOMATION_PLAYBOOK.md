# Tier-0 SOTA TradingView Automation Playbook

Display-only Pine v6 indicators + Half-Automation order ticket staging (`semi_auto_confirmed=false`, `qty=1`, zero CDP, zero live broker endpoints).

---

## 1. Compliance & Safety Posture

- **Non-Repainting Pine v6:** `barstate.isconfirmed`, `gaps=barmerge.gaps_off`, `lookahead=barmerge.lookahead_off`.
- **Fail-Closed Staging:** Every alert validates against Draft 2020-12 [`schema_v1.json`](file:///Z:/tier0-sota-clean/src/tier0_sota/tv/schema_v1.json) in [`bridge.py`](file:///Z:/tier0-sota-clean/src/tier0_sota/tv/bridge.py) with `qty=1`, `semi_auto_confirmed=false`, `60s` re-entry cooldown, exact `Decimal` Rule-612 inward stops (`<= 2.00%` cap), and live endpoint refusal (`paper-api` / Paper `:4002` only).
- **ToU Boundary:** Zero TradingView Desktop CDP attachment, zero unofficial screeners, zero automated browser clicking.

---

## 2. Multi-Pane TradingView Layouts

### Layout A — Omni 4-Pane Active Trading (Default `09:30 ET`)
| Pane | Symbol & Timeframe | Indicator & Mode | Sync |
|---|---|---|---|
| **Top-Left** | `AMEX:SPY` (`5m`, Ext Hours ON) | `pine/tier0_market_regime.pine` (`T0_MKT_STATUS`) | Independent |
| **Top-Right** | Active Mover (`1m`, Ext Hours ON) | `pine/tier0_omni_sota_24x5.pine` (`AUTO_REGIME_ADAPTIVE`) | Blue Sync |
| **Bottom-Left** | Active Mover (`15m`/`D`, Ext Hours ON) | `pine/tier0_omni_sota_24x5.pine` (`Show HUD=OFF`) | Blue Sync |
| **Bottom-Right** | `AMEX:SOXS` / `NASDAQ:SQQQ` (`1m`, Ext Hours ON) | `pine/tier0_omni_sota_24x5.pine` (`CRASH_DEFENSIVE_INVERSE`) | Independent |

### Layout B — 6-Pane Crash-Defensive Command Center (`VIX >= 25`)
1. `AMEX:SPY` (`5m` + `T0_MKT_STATUS`) | 2. `NASDAQ:QQQ` (`5m`) | 3. `NASDAQ:SOXX` (`5m` + `T0_MKT_STATUS`)
4. `AMEX:SOXS` (`1m` + `CRASH_DEFENSIVE_INVERSE`) | 5. `NASDAQ:SQQQ` (`1m` + `CRASH_DEFENSIVE_INVERSE`) | 6. Active ETB Leader Short (`1m`)

### Layout C — 2-Pane 24/5 Overnight Monitor (`20:00–04:00 ET`, IBKR)
- **Left (`65%`):** Active Mover (`1m`, Ext Hours ON) with `OVERNIGHT_24X5_IBKR` (`<= 1.00%` stop cap, `50 bps` `STOP_LIMIT` collar).
- **Right (`35%`):** `AMEX:SPY` or `NASDAQ:SOXX` (`5m`) with `T0_MKT_STATUS`.

---

## 3. Lossless R-Lock, Auto-Reverse & Short Routing

- **4-Tier Progressive R-Lock (`tier0_omni_sota_24x5.pine` & `tickets.py`):**
  1. `T1 BE+` — Arms breakeven + fees/spread floor (`fee_buf`).
  2. `T2 +1R Lock` — Locks `+0.45R` (`lock_at_1r`), tightens trail to `0.65x`.
  3. `T3 +2R Lock` — Locks `+1.35R` (`lock_at_2r`), tightens trail to `0.40x`.
  4. `T4 +3R / Fast-Sell` — Locks `+2.35R`, tightens trail to `0.25x`; `fast_sell_climax` triggers flatten-first auto-reverse short (`fast_sell_rev_trigger`).
- **Seamless Re-Entry:** `60s` minimum cooldown (`reentry_cooldown_secs >= 60`) after exit.
- **Short Borrow Routing:** Extreme movers with verified `shortable && easy_to_borrow` route as `DIRECT_SHORT_ETB`; non-ETB / unverified names fail closed to `INVERSE_HEDGE_SOXS_SQQQ` (`AMEX:SOXS` / `NASDAQ:SQQQ`).

---

## 4. `3 x 4` 24/5 Session Protection Matrix

| Schedule (`NATR`) | Session | Order Type | Init Cap | Collar |
|---|---|---|---:|---:|
| `tight_near_lossless` (`<= 1.5%`) | `RTH` / `PRE\|AFTER` / `OVERNIGHT` | `STOP_MARKET` / `STOP_LIMIT` / `STOP_LIMIT` | `1.50%` / `1.25%` / `1.00%` | `0` / `35` / `50 bps` |
| `baseline` (`1.5%..3.0%`) | `RTH` / `PRE\|AFTER` / `OVERNIGHT` | `STOP_MARKET` / `STOP_LIMIT` / `STOP_LIMIT` | `2.00%` / `1.50%` / `1.00%` | `0` / `35` / `50 bps` |
| `wide_natr_adaptive` (`> 3.0%`) | `RTH` / `PRE\|AFTER` / `OVERNIGHT` | `STOP_MARKET` / `STOP_LIMIT` / `STOP_LIMIT` | `2.00%` / `1.50%` / `1.00%` | `0` / `35` / `50 bps` |

---

## 5. Operator Commands

```powershell
. .\scripts\tv_console.ps1
tv-copy-omni       # Copy Pine v6 Omni indicator to clipboard
tv-copy-regime     # Copy Pine v6 Market Regime gauge to clipboard
tv-tickets         # Generate strict extreme-mover tickets + TV watchlist
tv-copy-watchlist  # Refresh & copy live TV watchlist path
tv-stage -Symbol NVDA -Side SELL -Price 146.85 -StopPrice 148.68 -NatrPct 0.012
tv-e2e             # Run ruff, ty, and pytest tests/test_tv_automation.py
```

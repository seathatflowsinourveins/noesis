---
description: TradingView Half-Auto Pine v6 & Draft 2020-12 bridge status report using tv.mcp.json. Read-only.
argument-hint: "[optional symbol]"
---
Run read-only TV cockpit check for `$ARGUMENTS` (zero CDP `:9223`, zero `.env*`):
1. Confirm `pine/tier0_omni_sota_24x5.pine` & `tier0_market_regime.pine` enforce `barstate.isconfirmed`, `lookahead_off`, `semi_auto_confirmed = false`, and `qty = 1`.
2. Confirm `src/tier0_sota/tv/{bridge,tickets}.py` enforce `schema_v1.json`, Rule-612 lock rungs, and `DIRECT_SHORT_ETB` vs `INVERSE_HEDGE_SOXS_SQQQ`.

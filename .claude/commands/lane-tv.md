---
description: Active half-auto-tv compounding lane (Pine v6, Pydantic v2 TvAlertV1, staged tickets) via tv.mcp.json.
argument-hint: "[symbol or preset pack]"
---
Execute compounding `half-auto-tv` iteration for `$ARGUMENTS` (zero CDP `:9223`, zero `.env*`):
1. Compound non-repainting TradingView Pine v6 extreme-mover & causal regime scripts (`pine/tier0_omni_sota_24x5.pine`, `pine/tier0_market_regime.pine`) enforcing `barstate.isconfirmed`, `lookahead_off`, `semi_auto_confirmed = false`, and `qty = 1`.
2. Validate and evolve `src/tier0_sota/tv/{bridge,tickets}.py` strictly via upstream `Pydantic v2` (`TvAlertV1`), Rule-612 near-lossless lock rungs, and `DIRECT_SHORT_ETB` vs `INVERSE_HEDGE_SOXS_SQQQ` routing.
3. Refresh staged semi-auto order tickets & watchlists in `Z:\sota-data\manifests\tv_sota_pack\` (staged `semi_auto_confirmed = False` / `qty = 1` until the operator confirms via `just tv-confirm-paper`; the live `just tv-confirm-live` is planned, not yet a recipe) and feed closed-loop telemetry back into Sim/Paper compounding manifests (`MLflow`/`XERJ`).
4. Verify Pine v6 balance, `TvAlertV1` schema parity, and E2E tests via `just check`.
5. Curated skills (11): `lightweight-charts`, `breakout-trade-planner`, `parabolic-short-trade-planner`, `stockbee-episodic-pivot-analyzer`, `stockbee-momentum-burst-screener`, `stockbee-exhaustion-hammer-screener`, `vcp-screener`, `pead-screener`, `position-sizer`, `pre-trade-discipline-gate`, `trading-skills-navigator` (agent: `observability-operator`).

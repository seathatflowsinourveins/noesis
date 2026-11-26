---
description: Active sim-backtest compounding lane (NautilusTrader v2 SANDBOX + hftbacktest + MLflow) via sim.mcp.json.
argument-hint: "[hypothesis, symbol, or MLflow experiment]"
---
Execute compounding `sim-backtest` iteration for `$ARGUMENTS` (strictly upstream-only, zero self-hallucination):
1. Enforce causal PIT (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`) via `edgartools` + `Polars`/`DuckDB`/`Pandera`, targeting early extreme movers and causal regime/crash-reversal alignment.
2. Run `NautilusTrader v2` (`BacktestEngine`/`Environment.SANDBOX`) + Rust `hftbacktest` queue markout + `Optuna` discovery with 24/5 near-lossless breakeven+fee trailing, auto-reverse, and rapid re-entry.
3. Clear 6-gate falsification (`1x`/`2x` costs, markout, `skfolio` CPCV `>= 16`, `ml4t-diagnostic` DSR/PBO, `arch` SPA, `statsmodels` HAC) across both legs, logging compounding manifests to `MLflow` (`:5000`) & `XERJ` (`:9200`).
4. Verify E2E via `just check` and `uv run --no-sync python scripts/nautilus_mcp_server.py --smoke`.

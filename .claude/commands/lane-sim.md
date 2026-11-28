---
description: Active sim-backtest compounding lane (NautilusTrader v2 SANDBOX + hftbacktest + MLflow) via sim.mcp.json.
argument-hint: "[hypothesis, symbol, or MLflow experiment]"
---
Execute compounding `sim-backtest` iteration for `$ARGUMENTS` (strictly upstream-only, zero self-hallucination):
1. Enforce causal PIT (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`) via `edgartools` + `Polars`/`DuckDB`/`Pandera`, targeting early extreme movers and causal regime/crash-reversal alignment.
2. Run `NautilusTrader v2` (`BacktestEngine`/`Environment.SANDBOX`) + Rust `hftbacktest` queue markout + `Optuna` discovery with 24/5 near-lossless breakeven+fee trailing, auto-reverse, and rapid re-entry.
3. Clear 6-gate falsification (`1x`/`2x` costs, markout, `skfolio` CPCV `>= 16` OOS paths / `8/3 = 21` paths / `cpcv_min_samples=48`, `ml4t-diagnostic` DSR/PBO, `arch` SPA, `statsmodels` HAC) across both legs, logging compounding manifests to `MLflow` (`:5000`) & `XERJ` (`:9200`).
4. Verify E2E via `just check` and `uv run --no-sync python scripts/nautilus_mcp_server.py --smoke`.
5. Curated skills (21): `ml4t-point-in-time`, `ml4t-polars-patterns`, `ml4t-factor-research`, `ml4t-causal-identification`, `ml4t-cpcv`, `ml4t-deflated-sharpe`, `ml4t-backtest-tearsheet`, `ml4t-shap-analysis`, `ml4t-drift-detection`, `ml4t-research-operator`, `ml4t-agent-state-memory`, `backtest-expert`, `dagster-docs-skill`, `macro-regime-detector`, `market-breadth-analyzer`, `uptrend-analyzer`, `ftd-detector`, `market-top-detector`, `ibd-distribution-day-monitor`, `us-market-bubble-detector`, `tier0-quant-falsification` (agents: `pit-data-engineer`, `quant-falsifier`).

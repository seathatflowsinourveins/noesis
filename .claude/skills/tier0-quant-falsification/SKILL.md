---
name: tier0-quant-falsification
description: Enforce Prove-Before-Plumbing pre-wall quantitative parity (<= 2026-10-15), NautilusTrader v2 plus Rust hftbacktest queue modeling, MIN_CPCV_SAMPLES = 16, 6-gate statistical falsification across both legs, Optuna/MLflow compounding, and exact Decimal risk invariants.
---

# Tier-0 Quantitative Falsification Directive

1. **Causal PIT Wall (`<= 2026-10-15`):** Operate exclusively on pre-wall slices via `Polars`, `DuckDB`, `Pandera`, and `exchange_calendars`: 1-min bars visible at `t_ns + 60s`, FINRA short volume strictly `T-1`, SEC filings joined on `accepted_ns <= cutoff_ns`, and offline ALFRED macro vintages. Zero lookahead in scalers (`[:split]`), changepoints (`ruptures`), matrix profiles (`stumpy`), or drift (`river` ADWIN).
2. **Single-Engine Parity (`Sim == Paper == Live`):** All strategies run on `NautilusTrader v2` (`BacktestEngine` / `Environment.SANDBOX` / `LiveNode`) with native `RiskEngineConfig`, Rust `hftbacktest` (`RiskAdverseQueueModel`) L1 replay, 24/5 session protection (`RTH` `STOP_MARKET` vs collared `outside_rth` `STOP_LIMIT`), 4-tier progressive profit-lock trailing, flatten-first auto-reverse, `60s` rapid re-entry, and paper-only `:4002` (`FORBIDDEN_LIVE_IBKR_PORTS = {4001, 7496, 8798, 8796}`).
3. **Six-Gate Statistical Falsification & Optuna/MLflow Compounding:** Evaluate combined, directional (`long-only`), and market-aligned crash/reversal (`short-only`) legs independently across `1x` & `2x` costs, `skfolio` `CombinatorialPurgedCV` (`MIN_CPCV_SAMPLES = 16` failing closed as `DISCARD_INSUFFICIENT_TRADES`), `ml4t-diagnostic` (`DSR`, `PBO`, `MinTRL` at true cumulative trial count), `arch` Hansen `SPA`, and `statsmodels` Newey-West HAC, compounded via `Optuna` (`:8080`) + `MLflow 3` (`:5000`). Keep `live_execution_enabled = False` until every gate passes.
4. **Fail-Closed `Decimal` Risk (`src/tier0_sota/risk.py`):** Atomic Reg-NMS Rule-612 stop clamping `<= 2.00%`, session-latched `-1.50%` daily drawdown breaker, and `>= 10%` protective-exit rate reserve (`Z3` UNSAT & `Hypothesis`-tested).

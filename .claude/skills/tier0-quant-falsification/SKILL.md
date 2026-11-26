---
name: tier0-quant-falsification
description: Enforce Prove-Before-Plumbing pre-wall quantitative parity (<= 2026-10-15), NautilusTrader v2 plus Rust hftbacktest queue modeling, MIN_CPCV_SAMPLES = 16, 6-gate statistical falsification across both legs, and exact Decimal risk invariants.
---

# Tier-0 Quantitative Falsification Directive

1. **Causal PIT Wall (`<= 2026-10-15`):** Operate exclusively on pre-wall slices via `Polars`, `DuckDB`, `Pandera`, and `exchange_calendars` (data, ranking, and telemetry modules): 1-min bars visible at `t_ns + 60s`, FINRA short volume strictly `T-1`, SEC filings joined on `accepted_ns <= cutoff_ns`, and offline ALFRED macro vintages loaded causally via `load_offline_alfred_pit_vintages()`. Zero lookahead in scalers (`[:split]`), changepoints (`ruptures` Pelt), streaming matrix profiles (`stumpy` stumpi with `egress=False`), or drift (`river` ADWIN).
2. **Single-Engine Parity (`Sim == Paper == Live`):** All strategies subclass the `NautilusTrader` v2 `Strategy` base class with native `RiskEngineConfig`, vectorized Rust `hftbacktest` (`RiskAdverseQueueModel`) L1 replay, and fail-closed `build_post_proof_live_node_spec()` (engine module) enforcing `paper_live_deployed == True`, `host_role == "Noesis"`, and paper-only `:4002` or `:7497` (`FORBIDDEN_LIVE_IBKR_PORTS = {4001, 7496, 8798, 8796}`).
3. **Six-Gate Statistical Falsification (discovery module):** Evaluate combined, directional (`long-only`), and inverse-reversal (`short-only`) legs independently across `1x` & `2x` slippage, `skfolio` `CombinatorialPurgedCV` (`MIN_CPCV_SAMPLES = 16` failing closed as `DISCARD_INSUFFICIENT_TRADES`), `ml4t-diagnostic` (`DSR`, `PBO`, `MinTRL` at true cumulative trial count logged in `MLflow 3`), `arch` Hansen `SPA`, and `statsmodels` Newey-West HAC. Keep `paper_live_deployed = False` until every gate passes.
4. **Fail-Closed `Decimal` Risk (risk module):** Atomic Reg-NMS Rule-612 stop clamping `<= 2.00%`, session-latched `-1.50%` daily drawdown breaker, and `>= 10%` protective-exit rate reserve (`Hypothesis`-tested).

# Dual-Flywheel Compounding Self-Reflection & Strategy Improvement Loop

Tier-0 `noesis` implements continuous self-reflection, empirical falsification, and strategy evolution strictly inside **verified upstream SOTA frameworks** — zero custom backtest loops, zero hand-rolled HTML/JS, zero arbitrary caps, and zero synthetic falsification shortcuts ([ADR-0002](adr/ADR-0002.md), [ADR-0024](adr/ADR-0024.md), [ADR-0025](adr/ADR-0025.md)).

## Architecture Overview

```
Pre-Wall Parquet Lake (<= 2026-10-15) -> NautilusTrader v2 + Rust hftbacktest
        │
        ▼ (3 philosophy profiles × 24/5 trailing + flatten-first auto-reverse + reentry)
Optuna 5.1.0 Study (optuna.db → Optuna Dashboard :8080) + skfolio / Clarabel.rs Weights
        │
        ▼ (Independent LONG / SHORT / COMBINED 6-Gate CPCV / DSR / Empirical PBO / Hansen SPA / HAC)
MLflow 3.18.0 (:5000) + Inspect AI 0.3.292 (7/7 -> :7575) + DSPy 3.5.0 + dspy.GEPA (OmniRoute :20128)
        │
        ├──► Caddy Manifests (:8793/compounding_self_reflection_manifest.json + /test-reports/allure/)
        ├──► Dagster 1.13.31 Schedule (:3000 — 8 active assets, Mon–Fri 18:00 America/New_York)
        ├──► XERJ Memory (:9200 — tier0-compounding-lessons + tier0-adrs 28/28) + codebase-memory (manage_adr)
        └──► Orca Automation (:6770 — noesis-weekly-compounding-self-reflection)
```

## Flywheel A — Strategy & Quantitative Falsification

Executed by [`scripts/run_compounding_reflection.py`](../scripts/run_compounding_reflection.py) via `just reflect` (`--mode lake|offline`):

1. **Pre-Wall Lake Replay (`--mode lake`, default when `Z:\sota-data\prewall-lake` is present; `--mode offline` deterministic CI fixture):** Evaluates deterministic philosophy execution regimes over pre-wall (`<= 2026-10-15`) 1m/5m bars using `NautilusTrader v2` + Rust `hftbacktest`:
   - `core_24x5_compound`: 24/5 session trailing + `auto_reverse=True` + `auto_reentry=True` (`reentry_cooldown_secs=60`).
   - `fast_reentry_cooldown_0`: immediate cooldown recovery (`reentry_cooldown_secs=0`) with progressive profit lock.
   - `baseline_no_reverse`: 24/5 trailing control with `auto_reverse=False`.
2. **Independent Cross-Leg 6-Gate Statistical Falsification & `skfolio`/`Clarabel.rs`/`MAPIE` Portfolio Sizing:** Scores `LONG`, `SHORT`, and `COMBINED` sleeves independently through [`tier0_sota.research.falsify_trade_returns`](../src/tier0_sota/research.py) (`skfolio` `8/3` CPCV `56` splits / `21` OOS paths `>= 16`, `cpcv_min_samples = 48` fail-closed `DISCARD_INSUFFICIENT_PATHS`, Gate 1 `min_l1_fills = 16`, DSR, **empirical cross-profile `_compute_cpcv_pbo` with zero hardcoded `pbo=0.10`**, HAC t-stat vs HLZ-inspired Bonferroni hurdle, **Hansen SPA** p-value via `arch.bootstrap.SPA`, plus `MAPIE` conformal trailing bounds `<= 2.00%`), computes sleeve allocations via `compute_skfolio_portfolio_weights` (`skfolio` + `Clarabel.rs`), and records all trials into **Optuna 5.1.0** (`tier0_sota_trailing_robustness_v1`) and **MLflow 3.18.0**.
3. **Optuna trial store (`TIER0_OPTUNA_STORAGE`, `2026-11-27`):** the extreme-mover weight calibration in [`tier0_sota.research.calibrate_extreme_mover_weights_optuna`](../src/tier0_sota/research.py) passes Optuna's own `storage=` argument from the environment variable `TIER0_OPTUNA_STORAGE`. Unset (the default, and always under pytest) the study stays in memory, as before. Set it to the SQLite file the Optuna Dashboard on `:8080` reads (compose mounts `${TIER0_STATE_ROOT:-$HOME/tier0-sota-state}/mlflow` at `/state/mlflow`, so on the host: `TIER0_OPTUNA_STORAGE=sqlite:///$HOME/tier0-sota-state/mlflow/optuna.db`) and every trial is written there, one study per call (`tier0-early-mover-weights-seed<seed>-train<ns>-holdout<ns>-<UTC timestamp>`). No service sets it yet; the compounding loop inherits it from its environment once the operator exports it.
4. **Pinned Safety Boundary & Formal/Symbolic Verification:** `src/tier0_sota/risk.py` stays bit-exact (`ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`) with Z3 `5/5` `UNSAT` invariants (`just z3`), isolated `uvx` `CrossHair` (`crosshair-tool==0.0.114`, `just crosshair`, `0` counterexamples), and WSL isolated `mktemp -d` `mutmut==3.8.1` (`just mutate-risk`) enforced cleanly; live operations follow the three tiers of [ADR-0029](adr/ADR-0029.md) (`live_execution_enabled=False` means tier 3 is off).

## Flywheel B — Agent Harness, Living ADRs & Philosophy Alignment

1. **Inspect AI (`:7575`):** `evals/tier0_inspect_eval.py` runs `tier0_safety_eval()` across **7 deterministic invariants** (`live_refusal`, `zero_banned_libs`, `zero_custom_hooks`, `risk_sha256`, `zero_arbitrary_loc_caps_and_living_adrs`, `upstream_skills_46_parity`, and `adr_graph_and_clean_sota_harness_integrity`) and writes native `.eval` logs directly to Inspect View.
2. **DSPy 3.5.0 + native `dspy.GEPA` (`evals/dspy_self_reflect.py`, strictly off execution hot paths):** Defines `CompoundingReflectionSignature`, `Tier0SelfReflectionProgram`, quantitative `(score, feedback)` `philosophy_convergence_metric`, `ScoreWithFeedback` adapter `philosophy_gepa_feedback_metric`, `build_gepa_reflection_optimizer` (`dspy.GEPA` Pareto reflective prompt evolution), `build_omniroute_dspy_lm()`, and explicit live-vs-offline `run_live_dspy_reflection` routed to `http://127.0.0.1:20128/v1` (never fabricating a synthetic live score when offline or unreachable).
3. **Active 8-Asset Dagster Graph (`:3000`):** `src/tier0_sota/definitions.py` exposes **8 active software-defined assets** (`prewall_pit_audit`, `risk_kernel_sha256_attestation`, `paper_farm_performance_snapshot`, `inspect_safety_and_philosophy_eval`, `adr_governance_and_graph_sync`, `sim_vs_paper_parity_reconciliation` emitting `tca_calibration_manifest.json`, `frontier_trigger_telemetry` emitting `frontier_trigger_status.json`, and active `compounding_self_reflection_snapshot`) wired into `compounding_reflection_job` and `daily_compounding_reflection_schedule` (`0 18 * * 1-5`, `America/New_York`).
4. **Knowledge-Graph, Hybrid Memory & Orca Weekly Review:** Executable MADR 4.0 ADRs (`ADR-0001..ADR-0029`, **25 Accepted / 0 Proposed / 4 Superseded**) synchronize into `Zensical` (`:8010`), `codebase-memory` (`manage_adr`), and `XERJ` (`tier0-adrs` **`28/28`** + `tier0-compounding-lessons`), while Orca automation `noesis-weekly-compounding-self-reflection` (`0 18 * * 5`, `America/New_York`, `claude --model opus --effort xhigh`) reviews live manifests, living ADRs, and status-labeled `L11..L15` frontier triggers in `SOTA_STARRED_AND_BEYOND_MANIFEST.json`.

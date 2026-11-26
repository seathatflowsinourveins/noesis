# Dual-Flywheel Compounding Self-Reflection & Strategy Improvement Loop

Tier-0 `noesis` implements continuous self-reflection, empirical falsification, and strategy evolution strictly inside **verified upstream SOTA frameworks** — zero custom backtest loops, zero hand-rolled HTML/JS, zero arbitrary caps.

## Architecture Overview

```
NautilusTrader v2 + Rust hftbacktest
        │
        ▼ (3 philosophy profiles × 24/5 trailing + flatten-first auto-reverse + reentry)
Optuna 5.1.0 Study (optuna.db → Optuna Dashboard :8080)
        │
        ▼ (6-Gate CPCV / DSR / PBO / White SPA falsification)
MLflow 3.18.0 (:5000) + Inspect AI 0.3.292 (:7575) + DSPy 3.5.0 (OmniRoute :20128)
        │
        ├──► Caddy Manifests (:8793/compounding_self_reflection_manifest.json)
        ├──► Dagster 1.13.31 Schedule (:3000 — daily Mon–Fri 18:00 America/New_York)
        ├──► XERJ Memory (:9200 — tier0-compounding-lessons)
        └──► Orca Automation (:6770 — noesis-weekly-compounding-self-reflection)
```

## Flywheel A — Strategy & Quantitative Falsification

Executed by [`scripts/run_compounding_reflection.py`](../scripts/run_compounding_reflection.py) via `just reflect`:

1. **Multi-Profile Nautilus v2 + hftbacktest Replay:** Evaluates deterministic philosophy execution regimes over pre-wall (`<= 2026-10-15`) 1m/5m bars:
   - `core_24x5_compound`: 24/5 session trailing + `auto_reverse=True` + `auto_reentry=True` (`reentry_cooldown_secs=60`).
   - `fast_reentry_cooldown_0`: immediate cooldown recovery (`reentry_cooldown_secs=0`) with progressive profit lock.
   - `baseline_no_reverse`: 24/5 trailing control with `auto_reverse=False`.
2. **6-Gate Statistical Falsification:** Scores every profile through [`tier0_sota.research.falsify_trade_returns`](../src/tier0_sota/research.py) (`MIN_CPCV_SAMPLES = 16`, CPCV OOS bps, DSR, PBO, HAC t-stat, White SPA p-value) and records all trials into **Optuna 5.1.0** (`tier0_sota_trailing_robustness_v1`) and **MLflow 3.18.0**.
3. **Immutable Safety Boundary:** `src/tier0_sota/risk.py` stays bit-exact (`ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`) with `live_execution_enabled=False` and Z3 UNSAT invariants enforced on every run.

## Flywheel B — Agent Harness, Living ADRs & Philosophy Alignment

1. **Inspect AI (`:7575`):** `evals/tier0_inspect_eval.py` runs `tier0_safety_eval()` across 6 deterministic invariants (`live_refusal`, `zero_banned_libs`, `zero_custom_hooks`, `risk_sha256`, `zero_arbitrary_loc_caps_and_living_adrs`, `upstream_skills_46_parity`) and writes native `.eval` logs directly to Inspect View.
2. **DSPy 3.5.0 (`evals/dspy_self_reflect.py`):** Defines `CompoundingReflectionSignature`, `Tier0SelfReflectionProgram`, `philosophy_convergence_metric`, and `build_omniroute_dspy_lm()` routed cleanly to `http://127.0.0.1:20128/v1`.
3. **Dagster (`:3000`):** `src/tier0_sota/definitions.py` exposes 5 software-defined assets (`prewall_pit_audit`, `risk_kernel_sha256_attestation`, `paper_farm_performance_snapshot`, `inspect_safety_and_philosophy_eval`, `compounding_self_reflection_snapshot`) plus `daily_compounding_reflection_schedule` (`0 18 * * 1-5`, `America/New_York`).
4. **Orca Multi-Model Weekly Review:** `noesis-weekly-compounding-self-reflection` (`0 18 * * 5`, `America/New_York`, `claude --model opus --effort xhigh`) reviews live manifests, living MADR 4.0 ADRs, and earned `L11..L15` frontier promotions in `SOTA_STARRED_AND_BEYOND_MANIFEST.json`.

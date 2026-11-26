---
name: tier0-upstream-convergence
description: Enforce Tier-0 Upstream-Only SOTA convergence directed by Core Trading Philosophy as the sole source of truth, living evolvable MADR 4.0 ADRs hosted on Zensical (:8010), and the Docker-hosted research stack.
---

# Tier-0 Upstream Convergence Directive

1. **Sole Source of Truth (Philosophy-Directed SOTA Convergence):** Core Trading Philosophy (`PHILOSOPHY_COMPOUNDING_MANIFEST.json`) realized strictly through verified OSI upstream convergence (`upstream-map.toml`) is the **only** source of truth. Install, integrate, or build directly inside verified OSI upstream frameworks — zero self-built backtest loops, polling daemons, custom hooks, or arbitrary self-imposed limits.
2. **Hard Protected Boundaries:** NEVER modify the immutable pure `Decimal` Rule-612 risk kernel (`src/tier0_sota/risk.py`), `~/Documents/upstream/`, `.env*` secrets, or hard-refused live broker endpoints (`api.alpaca.markets`, IBKR `:4001`, `:7496`, `:8798`, `:8796`). Enforce `paper_only = True` and `live_execution_enabled = False` until full Stage-5 promotion.
3. **Living, Evolvable ADRs & Zero Arbitrary LOC Caps:** Integrate natively inside `NautilusTrader v2` (`BacktestEngine`/`Environment.SANDBOX`/`LiveNode`), Rust `hftbacktest`, `Dagster`, `Polars`, and `DuckDB`. Keep `docs/adr/ADR-*.md` as open-ended, living MADR 4.0 decision snapshots (never frozen dogma or capped in count; amend, supersede, or retire as verified OSI upstreams evolve) hosted live on `Zensical` (`:8010`) and indexed into `XERJ` (`:9200`).
4. **Hosted SOTA Platforms, OmniRoute, Orca & Meta-Convergence:** Ground convergence & self-reflection itself in upstream standards (`AAIF AGENTS.md`, `MADR 4.0`, `SPDX`, `PEP 735`) across `MLflow 3` (`:5000`), `Dagster` (`:3000`), `Optuna` (`:8080`), `Inspect AI` (`:7575`), `DSPy`, `OmniRoute` (`:20128`), `Orca` (`Z:\workspaces`, multi-account OAuth failover), `Prometheus`/`Grafana`/`OTel`, `Caddy` (`:8793`), and `seathatflowsinourveins/noesis` public GitHub Actions.

# Architecture Decision Record Catalog (`tier0-sota-clean`)

Living, evolvable MADR 4.0 Architecture Decision Record catalog (initially `ADR-0001`..`ADR-0012`; open-ended `ADR-0013+` as the upstream landscape evolves), hosted on `Zensical` (`http://127.0.0.1:8010`) and indexed into `XERJ` (`http://127.0.0.1:9200`).

> **Living ADR Governance & Sole Source of Truth:** SOTA upstream convergence directed by our Core Trading Philosophy (`PHILOSOPHY_COMPOUNDING_MANIFEST.json`) is the **sole source of truth**. ADRs are empirical engineering decision snapshots, never frozen dogma or capped in count — amend, supersede (`Superseded by ADR-XXXX`), or retire any ADR whenever a superior verified OSI upstream better realizes the Core Trading Philosophy, while preserving immutable risk invariants (`src/tier0_sota/risk.py`) and fail-closed live broker port isolation. Every ADR records verified upstream evidence, self-invention lessons learned with mechanical upstream gates, and compounding-layer integration across the 4 active lanes (`sim-backtest`, `paper`, `half-auto-tv`, `live`).

| ADR | Title | Status | Date | Primary Modules / Anchors |
|---|---|---|---|---|
| [ADR-0001](ADR-0001.md) | Clean Architecture Layers (`L0..L10`), Upstream-Only Native-First & Zero Self-Hallucination | Accepted | 2026-11-26 | `upstream-map.toml`, `rules/zero_self_invention.yml`, `mise.toml`, `justfile`, `scripts/sync_manifest.py` |
| [ADR-0002](ADR-0002.md) | Four Dedicated Compounding Harness Lanes & Dual-Flywheel Learning (`sim-backtest`, `paper`, `half-auto-tv`, `live`) | Accepted | 2026-11-26 | `AGENTS.md`, `.claude/commands/lane-*.md`, `evals/tier0_inspect_eval.py`, `research.py`, `paper_farm.py` |
| [ADR-0003](ADR-0003.md) | Causal Point-in-Time Data Plane (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`) | Accepted | 2026-11-26 | `discovery.py`, `Polars`, `DuckDB`, `Pandera`, `Pydantic` v2, `EdgarTools` |
| [ADR-0004](ADR-0004.md) | Single-Engine Trading Parity (`NautilusTrader v2` + Rust `hftbacktest`) | Accepted | 2026-11-26 | `engine.py`, `SignalExecutionStrategy`, `Environment.SANDBOX` |
| [ADR-0005](ADR-0005.md) | Formal Immutable Risk Kernel & Z3 SMT Proofs (`risk.py` `<= 2.00%`, `-1.50%` Fuse, `>= 10%` Reserve) | Accepted | 2026-11-26 | `src/tier0_sota/risk.py`, `evals/formal_risk_z3.py` |
| [ADR-0006](ADR-0006.md) | 6-Gate Statistical Falsification Across Independent Long & Short Legs | Accepted | 2026-11-26 | `research.py`, `hftbacktest`, `skfolio`, `ml4t-diagnostic`, `arch`, `statsmodels` |
| [ADR-0007](ADR-0007.md) | 24/5 Session Protection, 4-Tier Progressive Lossless Trailing, Climax Reverse & `60s` Re-Entry | Accepted | 2026-11-26 | `engine.py` (`classify_et_hm`, `POOLED_TRAILING_SCHEDULES`), `pine/tier0_omni_sota_24x5.pine` |
| [ADR-0008](ADR-0008.md) | Extreme-Mover Early Entry, Causal Regime Alignment & Market-Aligned Shorting | Accepted | 2026-11-26 | `discovery.py`, `live/scanner.py`, `tv/tickets.py` |
| [ADR-0009](ADR-0009.md) | Stationary 4-Account Paper Farm Compounding Lane (`ALPACA_PA1..PA3` + `IBKR_DU_4002`) | Accepted | 2026-11-26 | `live/paper_farm.py`, `live/performance.py`, `scripts/run_live_paper_farm.ps1` |
| [ADR-0010](ADR-0010.md) | ToU-Compliant TradingView Pine v6 & Strict Pydantic v2 (`TvAlertV1`) Half-Auto Bridge | Accepted | 2026-11-26 | `pine/*.pine`, `tv/bridge.py`, `tv/tickets.py`, `tv/schema_v1.json`, `scripts/tv_console.ps1` |
| [ADR-0011](ADR-0011.md) | Official Upstream `L10` UI & 13-Portal Observability Stack (`otel-collector-contrib` + `Caddy`) | Accepted | 2026-11-26 | `docker/compose.sota.yml`, `docker/otel-collector-config.yaml`, `live/performance.py`, `zensical.toml` |
| [ADR-0012](ADR-0012.md) | AAIF `AGENTS.md` SSOT, 4 Compounding Lanes, `mise`/`just`/`prek`, Standalone Skills & Grand-Reset | Accepted | 2026-11-26 | `AGENTS.md`, `CLAUDE.md`, `mise.toml`, `justfile`, `.mcp.json`, `.claude/skills/`, `scripts/grand_reset/` |

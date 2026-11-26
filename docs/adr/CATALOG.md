# Architecture Decision Record Catalog (`tier0-sota-clean`)

Bounded evolvable MADR 4.0 Architecture Decision Record catalog (`ADR-0001`..`ADR-0012`), hosted on `Zensical` (`http://127.0.0.1:8010`) and indexed into `XERJ` (`http://127.0.0.1:9200`).

| ADR | Title | Status | Date | Primary Modules / Anchors |
|---|---|---|---|---|
| [ADR-0001](ADR-0001.md) | Clean Architecture Layers (`L0..L10`), SPDX OSI Firewall, Zero `yfinance` & AST Zero-Self-Invention | Accepted | 2026-11-25 | `upstream-map.toml`, `rules/zero_self_invention.yml`, `SOTA_STARRED_AND_BEYOND_MANIFEST.json` |
| [ADR-0002](ADR-0002.md) | Dual-Flywheel Compounding Learning Harness (`XERJ`, `MLflow`, `DuckDB`, `Inspect AI`, `DSPy`) | Accepted | 2026-11-25 | `evals/tier0_inspect_eval.py`, `research.py`, `paper_farm.py` |
| [ADR-0003](ADR-0003.md) | Causal Point-in-Time Data Plane (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`) | Accepted | 2026-11-25 | `discovery.py`, `Polars`, `DuckDB`, `Pandera`, `EdgarTools` |
| [ADR-0004](ADR-0004.md) | Single-Engine Trading Parity (`NautilusTrader v2` + Rust `hftbacktest`) | Accepted | 2026-11-25 | `engine.py`, `SignalExecutionStrategy`, `Environment.SANDBOX` |
| [ADR-0005](ADR-0005.md) | Formal Immutable Risk Kernel & Z3 SMT Proofs (`risk.py` `<= 2.00%`, `-1.50%` Fuse, `>= 10%` Reserve) | Accepted | 2026-11-25 | `src/tier0_sota/risk.py`, `evals/formal_risk_z3.py` |
| [ADR-0006](ADR-0006.md) | 6-Gate Statistical Falsification Across Independent Long & Short Legs | Accepted | 2026-11-25 | `research.py`, `hftbacktest`, `skfolio`, `ml4t-diagnostic`, `arch`, `statsmodels` |
| [ADR-0007](ADR-0007.md) | 24/5 Session Protection, 4-Tier Progressive Lossless Trailing, Climax Reverse & `60s` Re-Entry | Accepted | 2026-11-25 | `engine.py` (`POOLED_TRAILING_SCHEDULES`), `pine/tier0_omni_sota_24x5.pine` |
| [ADR-0008](ADR-0008.md) | Market-Aligned Always-Short & Pure Extreme-Mover RPM Allocation | Accepted | 2026-11-25 | `discovery.py`, `live/scanner.py`, `tv/tickets.py` |
| [ADR-0009](ADR-0009.md) | Stationary 4-Account Paper Farm (`ALPACA_PA1..PA3` + `IBKR_DU_4002`) | Accepted | 2026-11-25 | `live/paper_farm.py`, `live/performance.py`, `scripts/run_live_paper_farm.ps1` |
| [ADR-0010](ADR-0010.md) | ToU-Compliant TradingView Pine v6 & Draft 2020-12 Half-Auto Bridge | Accepted | 2026-11-25 | `pine/*.pine`, `tv/bridge.py`, `tv/tickets.py`, `tv/schema_v1.json`, `scripts/tv_console.ps1` |
| [ADR-0011](ADR-0011.md) | Official Upstream `L10` UI & 13-Portal Observability Stack | Accepted | 2026-11-25 | `docker/compose.sota.yml`, `live/performance.py`, `zensical.toml` |
| [ADR-0012](ADR-0012.md) | Multi-Runtime LLM-Native Harness, Standalone `.claude/skills/` (`46`) & Grand-Reset Kit | Accepted | 2026-11-25 | `.mcp.json`, `.claude/skills/`, `.claude/mcp-lanes/`, `scripts/grand_reset/` |

# Tier-0 SOTA Quantitative Engineering & Compounding Harness (`noesis`)

Canonical AAIF agent SSOT for `Z:\noesis` (`CLAUDE.md` -> `@AGENTS.md`, Codex, Gemini, OpenCode, Orca). Authoritative inventories: `upstream-map.toml`, `ARCHITECTURE_MANIFEST.json`, `LLM_HARNESS_MANIFEST.json` (`scripts/sync_manifest.py`), `PHILOSOPHY_COMPOUNDING_MANIFEST.json`, `SOTA_STARRED_AND_BEYOND_MANIFEST.json`, `mise.toml`, `justfile`.

## 1. Single Rule: SOTA Upstream Convergence per Core Trading Philosophy (Sole Source of Truth)

- **Sole Source of Truth:** SOTA upstream convergence directed by our Core Trading Philosophy (`PHILOSOPHY_COMPOUNDING_MANIFEST.json`) is the **only** source of truth. Live directly inside verified OSI upstream engines, official templates, and upstream CLI directives (`pyproject.toml`, `uv.lock`, `mise.toml`, `justfile`, `.pre-commit-config.yaml`).
- **Living, Evolvable ADRs & SOTA-Clean Memory:** `docs/adr/ADR-*.md` are executable, living MADR 4.0 decision contracts (`tier0.adr.v1` YAML frontmatter, `fitness-checks`, `ADR-0001..ADR-0025+`, never frozen dogma or artificially capped in count) — amend, supersede (`superseded-by`), or retire any ADR whenever a superior verified OSI upstream better realizes our Core Trading Philosophy with empirical evidence. Keep durable memory (`XERJ` `tier0-adrs`, `codebase-memory` `manage_adr`, auto-memory) strictly aligned with current SOTA upstream convergence and purge superseded legacy memories directly; only `src/tier0_sota/risk.py` safety invariants and fail-closed live broker port isolation remain non-negotiable mechanical guardrails.
- **Self-Invention Lesson Learned:** Arbitrary LOC caps, inline-Python Docker/HTTP shims, hand-patched `jsonschema` validators, drifting hand-edited manifests, ungrouped shelf-ware deps, and custom runtime hooks are permanently purged.
- Enforce Zero Self-Invention strictly via real upstream gates: `ast-grep` (21 rules in `rules/zero_self_invention.yml`), PEP 735 `uv.lock` (`0` `yfinance`/`quantstats`, SPDX OSI firewall), `deptry`, `ruff`, `ty`, `prek`, `gitleaks`, `zizmor`, `trivy`, `syft`, `CodeQL`, `Dependabot`, `z3-solver` (`evals/formal_risk_z3.py`), immutable `src/tier0_sota/risk.py` SHA-256 (`ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`), and `live_execution_enabled = False`.
- Search-SOTA-First before editing: query native `uv` MCPs (`codebase-memory`, `xerj`, `nautilus-engine`, `edgartools`) and `46` SHA-locked standalone skills in `.claude/skills/` (`UPSTREAM_SKILLS_LOCK.json`).

## 2. Core Trading Philosophy & Cross-Cutting Compounding Layer

- **Trading Philosophy:** Capture the highest-potential extreme movers early; align direction with causal multi-feed market regime (`SPY`/`QQQ`/`SOXX`/`RSP`/`VIX`) — short market crashes (`regime == -1`) and verified reversal peaks (`DIRECT_SHORT_ETB` or inverse ETF hedges `SOXS`/`SQQQ`); protect every order 24/5 (`RTH` `STOP_MARKET` vs collared `outside_rth` `STOP_LIMIT`) with monotonic near-lossless `BE+fee` + 4-tier progressive profit-lock trailing (`+1R`/`+2R`/`+3R`), flatten-first climax auto-reverse, and `60s` rapid re-entry bounded by exact `Decimal` Rule-612 stops (`<= 2.00%`), `>= 10%` exit reserve, and `-1.50%` latched daily drawdown fuse — realized exclusively via `NautilusTrader v2` + Rust `hftbacktest` + `ml4t` + `skfolio` + `alpaca-py` + `pydantic` v2.
- **Compounding Layer (Cross-Cutting):** Every run feeds deterministic compounding manifests and E2E verification across `MLflow` (`:5000`, pre-wall trials & 6-gate dossiers), per-account `DuckDB` (`Z:\sota-data\manifests`, live paper fills/TCA/markout/PNL), `XERJ` (`:9200`, deduplicated verified engineering/quant lessons & `tier0-adrs`), `Inspect AI` (7/7) + `DSPy`/`dspy.GEPA` (`evals/`), 8 active `Dagster` assets (`definitions.py`), and `scripts/sync_manifest.py` (drift-free `ARCHITECTURE_MANIFEST.json`).

## 3. Four Dedicated Compounding Harness Lanes

1. **`sim-backtest` (`.claude/commands/lane-sim.md`, `.claude/mcp-lanes/sim.mcp.json`):** 24/7 `NautilusTrader v2` `BacktestEngine`/`Environment.SANDBOX` + Rust `hftbacktest`; strict causal PIT (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`, `Polars`, `DuckDB`, `Pandera`, `EdgarTools`, `FRED`); upstream feature/regime/ML discovery (`ml4t-engineer`, `River`, `Ruptures`, `stumpy`, `LightGBM`, `CatBoost`, `SHAP`, `Optuna`); independent long & short 6-gate falsification (`1x`/`2x` costs, markout, `skfolio` CPCV `>= 16`, `ml4t-diagnostic` DSR/PBO, `arch` SPA, `statsmodels` HAC) -> compounding `MLflow` manifests.
2. **`paper` (`.claude/commands/lane-paper.md`, `.claude/mcp-lanes/paper.mcp.json`):** Stationary Noesis 4-account Paper Farm (`IBKR_DU_4002` `:4002` `3,000 rpm` + 3 non-overlapping `ALPACA_PA1..PA3` `200 rpm` lanes via `CrossLaneSymbolLock`), real-time extreme-mover scanning (`Alpaca SIP` + `FINRA TRF`) & ETB/inverse routing, 24/5 trailing/auto-reverse/re-entry execution, isolated `DuckDB` + `MLflow` fill/markout compounding manifests, and live `FINOS`/`OpenBB`/`Grafana`/`OTel` telemetry.
3. **`half-auto-tv` (`.claude/commands/lane-tv.md`, `.claude/mcp-lanes/tv.mcp.json`):** ToU-compliant display-only TradingView Pine v6 cockpit (`pine/*.pine`, zero CDP `:9223`), strict Pydantic v2 (`TvAlertV1`) bridge (`src/tier0_sota/tv/bridge.py`) and staged semi-auto tickets (`src/tier0_sota/tv/tickets.py`, `semi_auto_confirmed = False`, `qty = 1`), console/watchlist workflow (`scripts/tv_console.ps1`), closing telemetry feedback into Sim/Paper compounding manifests.
4. **`live` (`.claude/commands/lane-live.md`, `.claude/mcp-lanes/gate.mcp.json`):** Stage-5 promotion-readiness & `Sim == Paper == Live` `NautilusTrader v2` `LiveNode`/`InteractiveBrokers` parity lane, formal `Z3` SMT proofs, `Inspect AI` safety suite, and cross-leg 6-gate promotion dossiers + compounding readiness manifests (`live_execution_enabled = False` until full promotion).

## 4. L0–L10 Upstream Stack

| Layers | Pinned OSI Upstream Winners | Anchors |
|---|---|---|
| **L0–L2** | `uv`, `mise`, `just`, `fnox`, `age`, `Polars`, `DuckDB`, `Pandera`, `Pydantic` v2, `XNYS`, `EdgarTools`, `FRED`, `ml4t-engineer`, `River`, `Ruptures`, `stumpy`, `LightGBM`, `CatBoost`, `SHAP` | `pyproject.toml`, `uv.lock`, `mise.toml`, `justfile`, `fnox.toml`, `src/tier0_sota/discovery.py` |
| **L3–L5** | `NautilusTrader v2`, Rust `hftbacktest`, `alpaca-py`, `skfolio`, `ml4t-diagnostic`, `arch`, `statsmodels`, `Clarabel.rs` | `src/tier0_sota/{engine,risk,research}.py`, `src/tier0_sota/{live,tv}/` |
| **L6 & L10** | `MLflow` (`:5000`), `Dagster` (`:3000`), `Optuna` (`:8080`), `Inspect` (`:7575`), `Zensical` (`:8010`), `Prometheus` (`:9090`), `Grafana` (`:3001`), `OTel OTLP` + `otel-collector-contrib` (`:9464`/`:13133`), `Caddy` (`:8793`), `FINOS` (`:1420`/`:8000`), `OpenBB` (`:6900`), `DuckDB UI` (`:4213`) | `docker/compose.sota.yml`, `docker/otel-collector-config.yaml`, `zensical.toml`, `docs/adr/CATALOG.md`, `docs/adr/ADR-*.md` |
| **L7–L9** | `ruff`, `ty`, `deptry`, `ast-grep`, `prek`, `gitleaks`, `zizmor`, `trivy`, `syft`, `CodeQL`, `Dependabot`, `shellcheck`, `z3-solver`, `Hypothesis`, `pytest`, `Inspect AI`, `DSPy`, `GEPA`, `Orca`, `OmniRoute`, `codebase-memory`, `XERJ` (`:9200`), `FastMCP` | `.github/workflows/*.yml`, `rules/zero_self_invention.yml`, `.pre-commit-config.yaml`, `orca.yaml`, `.mcp.json`, `scripts/nautilus_mcp_server.py` |

## 5. Upstream Verification Commands

```bash
just check                                     # fmt-check, lint, typecheck, deptry, ast, adr-check, z3, mcp-smoke, pytest, gitleaks
just adr-check                                 # manifest-check, strict Zensical docs build, ADR & Clean SOTA Harness pytest gates
just fnox-check                                # validate encrypted fnox + age profiles (default, data, paper, live)
just docs-build                                # uv run --no-sync zensical build --clean --strict
just security                                  # gitleaks, zizmor --offline, trivy fs (HIGH/CRITICAL)
just preflight                                 # 13/13 workstation & 15/15 portal preflight (just resume to reconcile post-restart)
just reflect                                   # Dual-Flywheel Optuna/Inspect/DSPy/MLflow compounding reflection loop
just open-agy-wsl                              # launch WSL2 Linux AGY Desktop (2.35.2 via WSLg) alongside Windows `just open-agy`
prek run --all-files                           # upstream pre-commit hooks
uv run --no-sync python scripts/sync_manifest.py --check
pwsh -NoProfile -File scripts/check.ps1        # WSL2: bash scripts/check.sh
pwsh -NoProfile -File scripts/grand_reset/verify_grand_reset.ps1
```

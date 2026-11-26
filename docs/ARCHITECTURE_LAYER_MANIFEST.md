# Tier-0 SOTA Architecture Layer Manifest (`Z:\tier0-sota-clean`)

Standalone clean-room Python 3.13 (`uv`) architecture governed by [`AGENTS.md`](../AGENTS.md), [`upstream-map.toml`](../upstream-map.toml), [`SOTA_STARRED_AND_BEYOND_MANIFEST.json`](../SOTA_STARRED_AND_BEYOND_MANIFEST.json), and [`ARCHITECTURE_MANIFEST.json`](../ARCHITECTURE_MANIFEST.json).

Zero Self-Invention and zero legacy coupling are strictly enforced by:
1. **`ast-grep` structural rules** ([`sgconfig.yml`](../sgconfig.yml) + [`rules/zero_self_invention.yml`](../rules/zero_self_invention.yml)) blocking banned libraries, custom trading/shadow engine classes, raw HTTP/socket polling, LLMs on execution/risk hot paths, `float()` in `risk.py`, and live broker endpoints.
2. **Single-winner `uv.lock` (`0` `yfinance`) + SPDX OSI License Firewall** ([`pyproject.toml`](../pyproject.toml) + [`upstream-map.toml`](../upstream-map.toml)) removing `quantstats`/transitive `yfinance` and excluding non-OSI licenses (`BSL-1.1`, `ELv2`, custom).
3. **Immutable pure `Decimal` Rule-612 risk kernel + Z3 SMT proofs** ([`src/tier0_sota/risk.py`](../src/tier0_sota/risk.py), SHA-256 `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`, `361` LOC, formally verified `UNSAT` in [`evals/formal_risk_z3.py`](../evals/formal_risk_z3.py)).
4. **Fail-closed Paper-only boundaries + `Inspect AI` safety evals** (`live_execution_enabled = False`, `paper_only = True`, ports `4001`/`7496`/`8798`/`8796` and `api.alpaca.markets` hard-refused, verified in [`evals/tier0_inspect_eval.py`](../evals/tier0_inspect_eval.py)). Module LOC targets in `upstream-map.toml` (`loc_enforcement = "advisory"`) are advisory telemetry only.

---

## 1. Architecture Flow (`L0`–`L10` + Compounding Loop)

```mermaid
flowchart TD
    L0["L0: uv + Python 3.13 + Zero-yfinance Lockfile"] --> L1["L1: Polars, DuckDB, Pandera, XNYS, EdgarTools, FRED, treeverse/dvc"]
    L1 --> L2["L2: ml4t-engineer, River, Ruptures, stumpy-dev/stumpy, LightGBM, CatBoost, SHAP"]
    L2 --> L3["L3: NautilusTrader v2 + Rust hftbacktest"]
    L3 --> RSK["Immutable Risk Kernel: Rule-612 Decimal + Z3 SMT (risk.py)"]
    L3 --> L4["L4: Alpaca Paper PA1..PA3 + IBKR :4002 (paper_only=True)"]
    L3 --> TV["L4/L10: TradingView Pine v6 + Draft 2020-12 Half-Auto Bridge"]
    L3 --> L5["L5: skfolio CPCV, ml4t-diagnostic DSR/PBO, arch SPA, statsmodels HAC"]
    L4 --> L6["L6: MLflow, Dagster, Optuna, Prometheus, Grafana, OTel, Zensical"]
    L5 --> L6
    L6 --> L10["L10: FINOS Workspace, OpenBB, Perspective, Lightweight Charts, AG Grid"]
    L7["L7: ruff, ty, ast-grep, z3-solver, deptry, gitleaks, pytest, pre-commit"] --> RSK
    L8["L8: Inspect AI, DSPy, Harbor, Promptfoo, SkillEvaluator"] --> L9["L9: Orca, codebase-memory, XERJ :9200, FastMCP, 46 Standalone Skills"]
    L10 --> L9
    L9 --> L2
```

---

## 2. Active Technology Layers (`L0`–`L10`)

| Layer | Domain | Pinned Upstream Winners | Rejected Overlaps | `tier0-sota-clean` Modules & Configs |
|---|---|---|---|---|
| **L0** | Runtime, Lockfile & Provenance | `uv` | `poetry`, `pipenv`, `conda`, `pip-tools` | `pyproject.toml`, `uv.lock`, `.python-version`, `scripts/bootstrap_env.*`, `scripts/grand_reset/00_*`, `01_*` |
| **L1** | Data, Schema Validation, Calendars & Causal PIT | `polars`, `duckdb`, `pandera`, `exchange_calendars`, `edgartools`, `fredapi`, `treeverse/dvc` | `pandas-datareader`, `yfinance`, `clickhouse-local-server` | `src/tier0_sota/discovery.py`, `src/tier0_sota/live/paper_farm.py`, `src/tier0_sota/live/performance.py` |
| **L2** | Features, Streaming Drift, Regime Change, Explainability & ML | `ml4t-engineer`, `river`, `ruptures`, `stumpy-dev/stumpy`, `lightgbm`, `catboost`, `shap` | `ta-lib`, `sktime`, `fingpt`, `finrl`, `rd-agent` | `src/tier0_sota/discovery.py` |
| **L3** | Single-Engine `Sim == Paper == Live` Execution | `nautilus_trader` (`2.0.0rc9`), `hftbacktest` (`>=2.4.0`) | `backtrader`, `backtesting`, `zipline-reloaded`, `vectorbt`, `freqtrade`, `lean`, `pybroker` | `src/tier0_sota/engine.py`, `src/tier0_sota/risk.py`, `pine/tier0_omni_sota_24x5.pine`, `pine/tier0_market_regime.pine` |
| **L4** | Paper Broker Adapters & Market Data Feeds | `alpaca-py` (`0.45.0`), `tradermonty/claude-trading-skills` | `custom-rest-polling-loops`, `raw-unfiltered-alpaca-mcp-server`, `nautilus-alpaca` | `src/tier0_sota/live/{scanner,paper_farm}.py`, `scripts/run_live_paper_farm.ps1`, `src/tier0_sota/tv/{bridge,tickets}.py` |
| **L5** | Statistical Falsification & Portfolio Construction | `skfolio`, `ml4t-diagnostic`, `arch`, `statsmodels` | `pyportfolioopt`, `riskfolio-lib`, `alphalens-reloaded`, `mlfinlab`, `quantstats` | `src/tier0_sota/research.py` |
| **L6** | Hosted Research Portals, Orchestration & ADR Docs | `mlflow`, `dagster`, `optuna`, `zensical`, `prometheus`, `grafana`, `opentelemetry-sdk` | `mkdocs`, `mkdocs-material`, `wandb`, `clearml`, `neptune`, `prefect`, `custom-fastapi-html` | `src/tier0_sota/definitions.py`, `docker/compose.sota.yml`, `zensical.toml`, `docs/adr/CATALOG.md`, `src/tier0_sota/research.py` |
| **L7** | Static Quality, AST, Formal SMT & Supply-Chain Security | `ruff`, `ty`, `ast-grep`, `gitleaks`, `hypothesis`, `pytest`, `pre-commit`, `z3-solver`, `deptry`, `trivy`, `syft`, `osv-scanner`, `shellcheck`, `zizmor` | `black`, `isort`, `flake8`, `bandit`, `mypy`, `pyright` | `sgconfig.yml`, `rules/zero_self_invention.yml`, `.gitleaks.toml`, `.pre-commit-config.yaml`, `evals/formal_risk_z3.py`, `tests/*.py` |
| **L8** | Agent Evals, Sandboxes & Prompt Optimization | `inspect-ai`, `dspy`, `harbor`, `promptfoo`, `skillevaluator`, `OpenHands`, `aideml` | `deepeval` | `evals/tier0_inspect_eval.py`, `pyproject.toml`, `upstream-map.toml` |
| **L9** | Multi-Model Harness, Code Graph, Skills & MCPs | `orca`, `codebase-memory-mcp`, `xerj`, `mcp`, `edgartools` | `third-party-agent-wrappers`, `custom-mcp-launchers`, `self-built-agent-buses` | `orca.yaml`, `.mcp.json`, `.claude/mcp-lanes/`, `.claude/skills/` (`46`), `UPSTREAM_SKILLS_LOCK.json`, `scripts/nautilus_mcp_server.py` |
| **L10** | Institutional UI & Visual Analytics | `finos-workspace`, `OpenBB`, `perspective`, `lightweight-charts`, `ag-grid` (watches: `rerun`, `mosaic`, `FDC3`, `evidence`) | `streamlit`, `dash`, `gradio`, `nicegui`, `custom-react-blotter` | `src/tier0_sota/live/performance.py`, `src/tier0_sota/tv/bridge.py`, `docker/compose.sota.yml` |

### Earned-Trigger Frontier (`L11`–`L15`) & SPDX OSI License Firewall
- **`L11`–`L15` Catalog** (full trigger matrix in [`SOTA_STARRED_AND_BEYOND_MANIFEST.json`](../SOTA_STARRED_AND_BEYOND_MANIFEST.json)): `vortex`/`lancedb`/`sqlglot`/`dlt` (`L11`); `TabPFN`/`TabICLv2`/`TabM`/`TabArena`/`Chronos-2`/`Toto`/`GIFT-Eval`/`shapiq`/`evidently`/`skchange` (`L12`); `iceoryx2`/`microsoft/MarS`/`LOBS5`/`cvxportfolio`/`Clarabel.rs`/`HiGHS`/`DoubleML`/`BoTorch`/`Ax`/`MAPIE` (`L13`); `marimo`/`Temporal`/`Loki`/`Tempo`/`Pyroscope`/`Alertmanager`/`OTel Collector`/`Langfuse` (`L14`); `CrossHair`/`mutmut`/`semgrep`/`cosign`/`slsa-verifier`/`opa`/`microsoft/PyRIT`/`agentdojo`/`superradcompany/microsandbox`/`pydantic/monty`/`gvisor/gvisor`/`E2B`/`inspect_scout`/`gepa`/`ShinkaEvolve`/`algorithmicsuperintelligence/openevolve`/`snyk/agent-scan`/`agentskills`/`A2A`/`ACP`/`ag-ui`/`fastmcp`/`pydantic-ai`/`context7`/`hindsight`/`alpaca-mcp-server [paper-only]`/`tradingview-mcp [display-only]` (`L15`).
- **SPDX OSI License Firewall:** Excludes non-OSI repositories (`treeverse/lakeFS` [`BSL-1.1`], `restatedev/restate` [`BSL-1.1`], `Arize-ai/phoenix` [`ELv2`], `NX-AI/tirex` [`custom`]).

---

## 3. Repository Layer-by-Layer Map

### Core Production Package (`src/tier0_sota/`)
- [`src/tier0_sota/__init__.py`](../src/tier0_sota/__init__.py) (`6` LOC) — Package metadata entrypoint.
- [`src/tier0_sota/risk.py`](../src/tier0_sota/risk.py) (`361` LOC, exact SHA-256 `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`) — Immutable `Decimal` Rule-612 inward stop quantization (`<= 2.00%`), latched `-1.50%` daily drawdown fuse, `>= 10%` exit rate reserve.
- [`src/tier0_sota/engine.py`](../src/tier0_sota/engine.py) — `NautilusTrader v2` 24/5 session protection (`RTH` `STOP_MARKET` vs collared `outside_rth` `STOP_LIMIT`), 4-tier progressive lossless trailing (`BE+fee` + `+1R/+2R/+3R`), `60s` rapid re-entry, flatten-first auto-reverse, and venue rate matrix (`IBKR_PAPER_4002` `3,000 rpm`, `ALPACA_PAPER_200` `200 rpm`, `LIVE` profiles hard-refused `live_execution_enabled = False`).
- [`src/tier0_sota/discovery.py`](../src/tier0_sota/discovery.py) — Parameterized causal PIT (`TIER0_PREWALL_CUTOFF` default `2026-10-15`, `TIER0_DATA_ROOT`, `t_ns + 60s`, `XNYS`), multi-feed regime (`SPY`/`QQQ`/`SOXX`/`VIX`), market-aligned always-short + causal reversal signals (`ml4t-engineer`, `river`, `ruptures`, `stumpy`).
- [`src/tier0_sota/research.py`](../src/tier0_sota/research.py) — 6-gate falsification (`hftbacktest` markout, `1x`/`2x` costs, `skfolio` CPCV `MIN_CPCV_SAMPLES = 16`, `ml4t-diagnostic` DSR/PBO, `arch` Hansen SPA, `statsmodels` HAC, `MLflow`).
- [`src/tier0_sota/definitions.py`](../src/tier0_sota/definitions.py) — Upstream-native `Dagster` `Definitions` asset graph (`prewall_pit_audit`, `risk_kernel_sha256_attestation`, `paper_farm_performance_snapshot`).

### Live Paper Farm (`src/tier0_sota/live/`) & TradingView Half-Auto (`src/tier0_sota/tv/`, `pine/`)
- [`src/tier0_sota/live/{__init__,scanner,paper_farm,performance}.py`](../src/tier0_sota/live/__init__.py) — Whole-market extreme-mover scanner (`Alpaca SIP` + `FINRA TRF` + post-selection ETB lookup), 3-lane non-overlapping `ALPACA_PA1..PA3` + `IBKR_DU_4002` Paper Farm (`paper_only = True`, `live_execution_enabled = False`, clean `paper_lane_{pa1,pa2,pa3}.duckdb` primary with parameterized `TIER0_DATA_ROOT`), and FINOS/OpenBB manifest export (`paper_farm_live_performance_manifest.json`).
- [`pine/tier0_omni_sota_24x5.pine`](../pine/tier0_omni_sota_24x5.pine), [`pine/tier0_market_regime.pine`](../pine/tier0_market_regime.pine), [`src/tier0_sota/tv/{__init__,bridge,tickets}.py`](../src/tier0_sota/tv/__init__.py), [`schema_v1.json`](../src/tier0_sota/tv/schema_v1.json) — Non-repainting Pine v6 (`barstate.isconfirmed`, `lookahead_off`, `semi_auto_confirmed = false`, `qty = 1`, zero CDP `:9223`) and Draft 2020-12 ticket/bridge pipeline.

### Standalone Skills, Evals, Docker, Grand-Reset & Zero-Hook Harness
- **Formal SMT & Evals (`evals/`):** [`evals/formal_risk_z3.py`](../evals/formal_risk_z3.py) (`Z3` `UNSAT` proofs) and [`evals/tier0_inspect_eval.py`](../evals/tier0_inspect_eval.py) (`Inspect AI` offline safety suite).
- **Standalone Skills (`.claude/skills/`):** `46/46` vetted skill directories copied directly into `.claude/skills/` matching [`UPSTREAM_SKILLS_LOCK.json`](../UPSTREAM_SKILLS_LOCK.json) SHA-256 digests (`0` external repo dependency).
- **Scripts, CI & Grand-Reset Kit:** [`scripts/bootstrap_env.ps1`](../scripts/bootstrap_env.ps1), [`scripts/bootstrap_env.sh`](../scripts/bootstrap_env.sh), [`scripts/check.ps1`](../scripts/check.ps1), [`scripts/check.sh`](../scripts/check.sh), [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) (`zizmor` 0 findings; `ruff format --check`, `ruff check`, `ty check`, `deptry .`, `ast-grep`, `pytest`, `formal_risk_z3`, `nautilus_mcp_server --smoke`), [`scripts/setup_clean_ecosystem.ps1`](../scripts/setup_clean_ecosystem.ps1), [`scripts/launch_clean_lanes.ps1`](../scripts/launch_clean_lanes.ps1), [`scripts/run_live_paper_farm.ps1`](../scripts/run_live_paper_farm.ps1), [`scripts/tv_console.ps1`](../scripts/tv_console.ps1), [`scripts/nautilus_mcp_server.py`](../scripts/nautilus_mcp_server.py), [`scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1`](../scripts/grand_reset/00_clean_disk_z_and_agy_reset.ps1), [`01_disk_z_and_wsl_setup.ps1`](../scripts/grand_reset/01_disk_z_and_wsl_setup.ps1), [`01_wsl_bootstrap.sh`](../scripts/grand_reset/01_wsl_bootstrap.sh) (WSL2 `Noesis-SOTA` parity `~/tier0-sota-clean/.venv`), [`02_runtimes_agy_orca_setup.ps1`](../scripts/grand_reset/02_runtimes_agy_orca_setup.ps1), [`03_hosted_stack_tv_paper.ps1`](../scripts/grand_reset/03_hosted_stack_tv_paper.ps1), [`verify_grand_reset.ps1`](../scripts/grand_reset/verify_grand_reset.ps1).
- **Zero-Hook 4-Runtime MCP Parity:** `zero_self_invented_hooks = true` (`0` custom `"hooks"` blocks; pure native `uv run --no-sync` stdio MCPs) across [`.mcp.json`](../.mcp.json), [`.claude/settings.json`](../.claude/settings.json), [`.codex/config.toml`](../.codex/config.toml), [`.gemini/settings.json`](../.gemini/settings.json), [`opencode.json`](../opencode.json), plus [`.claude/mcp-lanes/`](../.claude/mcp-lanes/sim.mcp.json), `.claude/agents/*.md`, `.claude/commands/*.md`, and [`.opencode/commands/verify-tier0.md`](../.opencode/commands/verify-tier0.md).
- **Docker, ADRs & Tests:** [`docker/compose.sota.yml`](../docker/compose.sota.yml) (`9` files), [`zensical.toml`](../zensical.toml), [`docs/index.md`](index.md), [`docs/adr/CATALOG.md`](adr/CATALOG.md), `docs/adr/ADR-0001.md`..`ADR-0012.md`, [`tests/test_core_engine.py`](../tests/test_core_engine.py), [`tests/test_live_automation.py`](../tests/test_live_automation.py), [`tests/test_tv_automation.py`](../tests/test_tv_automation.py), [`tests/test_mcp_server.py`](../tests/test_mcp_server.py), [`tests/test_architecture_manifest.py`](../tests/test_architecture_manifest.py), [`tests/test_grand_reset_kit.py`](../tests/test_grand_reset_kit.py), [`tests/test_formal_and_evals.py`](../tests/test_formal_and_evals.py).

---

## 4. Hosted Portals (`13/13`), Safety Boundaries & Standalone Topology

- **13 Hosted Loopback Portals (`127.0.0.1`):** XERJ (`:9200`), Zensical (`:8010`), MLflow 3 (`:5000`), Dagster (`:3000`), Optuna Dashboard (`:8080`), Inspect AI View (`:7575`), Prometheus (`:9090`), Grafana (`:3001`), OTel Exporter (`:9464/metrics`), Tearsheets (`:8793`), FINOS Workspace (`:1420`), FINOS API (`:8000/health`), OpenBB Backend (`:6900`).
- **Compounding Loop (`XERJ` + `MLflow` + `DuckDB`):** Pre-wall trials log to `MLflow`; Paper Farm telemetry stores cleanly under `$TIER0_DATA_ROOT\manifests`; verified lessons compound into `XERJ` (`tier0-compounding-lessons`).

| Root Path (`ENV` Override) | Role | Mutability & Data Policy |
|---|---|---|
| `Z:\tier0-sota-clean` (`TIER0_REPO_ROOT`) | Standalone active SOTA repository (`main`, Python 3.13 `uv`, `46` local skills, Docker stack) | Self-contained codebase, zero external repo coupling |
| `Z:\sota-data\manifests` (`TIER0_DATA_ROOT\manifests`) | Active Paper Farm & TV status/manifest/DuckDB state plane | Shared operational state plane (never delete) |
| `Z:\sota-data\prewall-lake` (`TIER0_DATA_ROOT\prewall-lake`) | Immutable pre-wall (`<= 2026-10-15`) market data lake | Read-only PIT dataset (never delete) |
| `Z:\archive\superseded_2026-11-25` | Non-destructive archive for superseded artifacts | Archived artifacts only |

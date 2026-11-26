# Tier-0 Clean Ecosystem Manifest (`Z:\tier0-sota-clean`)

- **Generated:** 2026-11-26
- **Target Standalone Repository:** `Z:\tier0-sota-clean` (`$env:TIER0_REPO_ROOT`, branch `main`, Python 3.13 via `uv` + `mise` + `just`)
- **Immutable Risk Kernel SHA-256 (`src/tier0_sota/risk.py`):** `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`
- **Live Execution Guard:** `live_execution_enabled = False`, `paper_only = True`, zero live broker ports (`:4001`, `:7496`, `:8798`, `:8796`)
- **Zero Self-Invention Policy:** Enforced strictly through official upstream gates (`ast-grep`, `uv.lock`, `deptry`, `ruff`, `ty`, `prek`, `gitleaks`, `zizmor`, `trivy`, `syft`, GitHub `CodeQL`/`Dependabot`, `z3-solver`, immutable `risk.py` SHA-256, and `live_execution_enabled = False`) with zero arbitrary LOC caps.

---

## 1. Clean Ecosystem Entrypoints

| Script / Config | Role | Notes |
|---|---|---|
| `justfile` | Cross-platform declarative task runner | Official upstream `just` wrapping `uv`, `ruff`, `ty`, `deptry`, `ast-grep`, `sync_manifest.py`, `pytest`, `zensical`, `prek`, `gitleaks`, `zizmor`, `trivy`, `preflight`, `resume`, `reflect`, `account-status`, and `docker compose` |
| `mise.toml` | Declarative upstream CLI registry | Pins 10 official upstream CLIs (`python`, `uv`, `just`, `ruff`, `gitleaks`, `trivy`, `zizmor`, `syft`, `prek`, `ast-grep`) |
| `scripts/sync_manifest.py` | Programmatic SSOT manifest sync | Derives `ARCHITECTURE_MANIFEST.json` & `LLM_HARNESS_MANIFEST.json` deterministically from `upstream-map.toml`, git metadata, real repo files, and `risk.py` SHA-256 (`--write` / `--check`) |
| `.pre-commit-config.yaml` | Upstream pre-commit hooks (`prek`) | Installed locally via `prek install` (`check-merge-conflict`, `check-toml`, `check-json`, `ruff-check`, `ruff-format`, `gitleaks`, `shellcheck`, `zizmor`, `ty`, `ast-grep`) |
| `.github/workflows/{ci,security,docs,release,multi-model-sota}.yml` | Hardened upstream GitHub Actions suite | Full 40-char commit SHA pins, least-privilege `permissions`, `persist-credentials: false`, `zizmor --offline --persona=pedantic` (`0` findings), CycloneDX SBOM via `syft` on release, `trivy`, `gitleaks`, native `CodeQL`/`Dependabot`, and OmniRoute/Anthropic/OpenAI workflow |
| `.github/{dependabot.yml,CODEOWNERS,pull_request_template.md}` | Repo governance & dependency updates | Weekly `uv`, `github-actions`, and `docker` updates with 7-day cooldown; `@seathatflowsinourveins` code ownership on critical safety files |
| `scripts/setup_clean_ecosystem.ps1` | Idempotent setup & health check | Verifies `.venv`, `risk.py` SHA-256, 4-runtime MCP parity, `13/13` hosted portals, Paper Farm readiness, and `0` live broker ports |
| `scripts/check.ps1` | One-command Windows CI gate | Runs `risk.py` SHA-256, PS1 AST parse, `UV_LOCK_YFINANCE_MATCHES=0`, `LEGACY_MATCHES=0`, `ruff`, `ty`, `deptry`, `ast-grep`, `formal_risk_z3.py`, `nautilus_mcp_server.py --smoke`, `pytest -q`, and `gitleaks` |
| `scripts/check.sh` | One-command POSIX/WSL2 CI gate | Runs `risk.py` SHA-256, `bash -n`, `shellcheck`, `UV_LOCK_YFINANCE_MATCHES=0`, `LEGACY_MATCHES=0`, `ruff`, `ty`, `deptry`, `ast-grep`, `formal_risk_z3.py`, `nautilus_mcp_server.py --smoke`, `pytest -q`, and `gitleaks` |
| `scripts/launch_clean_lanes.ps1` | Non-overlapping lane launcher | Supports `-DryRun`; orchestrates Pre-Wall Sim Lab (`<= 2026-10-15`), 3-Lane Paper Farm (`ALPACA_PA1..PA3` + `IBKR_DU_4002`), TV Half-Auto Cockpit, and Verifier Gate |
| `scripts/run_live_paper_farm.ps1` | Paper Farm controller | Supports `preflight`, `scan`, `paper-farm` (requires `-ConfirmPaper`), and `performance` |
| `scripts/tv_console.ps1` | TradingView Half-Auto console | Display-only Pine v6 + Pydantic v2 (`TvAlertV1`) staging (`qty=1`, `semi_auto_confirmed=false`, zero CDP `:9223`) |
| `scripts/grand_reset/verify_grand_reset.ps1` | Grand-Reset Master Verifier | Enforces `12/12` gates (`ARCHITECTURE_MANIFEST.json`, `SOTA_STARRED_AND_BEYOND_MANIFEST.json`, `hooks_zero=True`, `.claude/mcp-lanes/`, dynamic living ADRs `ADR-0001..ADR-0021+`, `zensical`, `risk.py`, MCP smoke, portals, TV, Paper Farm, `gitleaks`, `0` `yfinance` / standalone skills, Z3 + Inspect AI evals, `0` live ports) |
| [`docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md`](GRAND_RESET_DISK_Z_WSL_RUNBOOK.md) | Grand-Reset Operator Runbook | Step-by-step zero-hook Disk-Z + WSL2 + fresh AGY + TV + 4-Runtime + Orca reset guide |
| [`docs/ARCHITECTURE_LAYER_MANIFEST.md`](ARCHITECTURE_LAYER_MANIFEST.md) | Architecture Layer Manifest | Complete `L0`–`L10` upstream stack map, Mermaid flow diagram, and `L11`–`L15` earned-trigger frontier |

### Upstream Infrastructure, Packaging & 4-Runtime MCP Parity

- **PEP 735 Dependency Groups (`pyproject.toml` + `uv.lock`):** Core runtime dependencies (including `pydantic>=2.10.0`) separated cleanly from `[dependency-groups]` (`dev`, `research`, `evals`, `portals`, `observability`) with `[tool.uv] default-groups`; `0` `yfinance` and `0` `quantstats` in `uv.lock`; `deptry .` reports `0` issues (`0` `DEP003`, `<= 3` optional-LLM `DEP002` ignores).
- **Official Upstream Telemetry & Static Serving (Docker/WSL2):** Inline Python shims eliminated — `otel/opentelemetry-collector-contrib` (`docker/otel-collector-config.yaml`) exposes real `otelcol_*` Prometheus metrics on `:9464/metrics`, and official upstream `caddy` serves tearsheets on `:8793` (zero `python -m http.server`).
- **4-Runtime Native `uv` MCP Parity (`hooks_zero = True`):** All 5 repo configs (`.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, `opencode.json`) wire the exact 4 vetted native `uv` MCP servers (`codebase-memory`, `xerj`, `nautilus-engine`, `edgartools`) with scoped `.claude/mcp-lanes/{sim,paper,tv,gate}.mcp.json` and `46/46` SHA-locked standalone skills in `.claude/skills/`.

---

## 2. Hosted SOTA Portals & Paper Farm Readiness

### 15 Hosted Local Portals (`127.0.0.1`)

| # | Portal | Loopback URL | Upstream Service |
|---|---|---|---|
| 1 | XERJ Memory/RAG | `http://127.0.0.1:9200/` | `tier0-sota-xerj:latest` |
| 2 | Zensical Docs | `http://127.0.0.1:8010/` | `zensical` (`tier0-sota-research`) |
| 3 | MLflow 3 | `http://127.0.0.1:5000/` | `mlflow` (`tier0-sota-research`) |
| 4 | Dagster Webserver | `http://127.0.0.1:3000/` | `dagster-webserver` (`tier0_sota.definitions`) |
| 5 | Optuna Dashboard | `http://127.0.0.1:8080/` | `optuna-dashboard` (`tier0-sota-research`) |
| 6 | Inspect AI View | `http://127.0.0.1:7575/` | `inspect view` (`tier0-sota-research`) |
| 7 | Prometheus | `http://127.0.0.1:9090/` | `prom/prometheus` |
| 8 | Grafana | `http://127.0.0.1:3001/` | `grafana/grafana` |
| 9 | OTel Collector Metrics | `http://127.0.0.1:9464/metrics` | `otel/opentelemetry-collector-contrib` |
| 10 | Official Tearsheets | `http://127.0.0.1:8793/` | `caddy` file-server |
| 11 | FINOS Workspace | `http://127.0.0.1:1420/` | `workspace-local` |
| 12 | FINOS API Health | `http://127.0.0.1:8000/health` | `workspace-local` |
| 13 | OpenBB Backend | `http://127.0.0.1:6900/` | `tier0-openbb-backend` |
| 14 | DuckDB SQL UI | `http://127.0.0.1:4213/` | `tier0-duckdb-ui` |
| 15 | OmniRoute LLM Gateway | `http://127.0.0.1:20128/api/health` | `omniroute` (`v2.3.x`, autostart) |

### 4-Account Clean-Slate Practice Plane (`Z:\sota-data\manifests`)

- `ALPACA_PA1` (`L1_ORB_BURST`), `ALPACA_PA2` (`L2_STRUCTURAL_TREND`), `ALPACA_PA3` (`L3_AVWAP_TIGHT_LOCK`): Clean lane statuses (`paper_only = true`, `live_execution_enabled = false`, `https://paper-api.alpaca.markets`) and isolated DuckDB ledgers `paper_lane_pa{1,2,3}.duckdb`.
- `IBKR_DU_4002` (Paper Gateway `:4002`): Adapter status (`paper_only = true`, `live_execution_enabled = false`), combined performance export in `paper_farm_live_performance_manifest.json` and `finos_paper_farm_performance.json`.
- **Whole-Market Scanner & TV Half-Auto Pack (`tv_sota_pack\`):** `live_whole_market_movers_manifest.json`, `live_half_auto_tickets.json` (Pydantic v2 `TvAlertV1` validated, `qty = 1`, `semi_auto_confirmed = false`), `LIVE_HALF_AUTO_ORDER_TICKETS.md`, `tier0_sota_tv_watchlists_live.txt`, `tier0_market_regime.pine`, and `tier0_omni_sota_24x5.pine`.

---

## 3. Protected Shared Data & Active Roots

| Directory | Role | Handling |
|---|---|---|
| `Z:\tier0-sota-clean` (`$env:TIER0_REPO_ROOT`) | Standalone Tier-0 SOTA clean repository (`L0..L10`, `46/46` skills, `0` legacy refs, `0` `yfinance`) | **ACTIVE CANONICAL REPO** |
| WSL2 `Noesis-SOTA` (`~/tier0-sota-clean/.venv`) | Isolated Linux `.venv` synced via `UV_PROJECT_ENVIRONMENT=~/tier0-sota-clean/.venv uv sync --frozen` | **ACTIVE WSL2 PARITY VENV** (preserves `~/.config/tier0/.env.*` with `chmod 600`) |
| `Z:\workspaces` (`$env:ORCA_WORKSPACES_DIR`) | Clean Orca worktree root | **ACTIVE WORKSPACE ROOT** |
| `Z:\sota-data\manifests` (`$env:TIER0_DATA_ROOT\manifests`) | Active live Paper Farm & TV manifests/DuckDB ledgers | **PRESERVED SHARED DATA ROOT** (never delete) |
| `Z:\sota-data\prewall-lake` (`$env:TIER0_DATA_ROOT\prewall-lake`) | Pre-wall (`<= 2026-10-15`) immutable market data lake | **PRESERVED SHARED DATA ROOT** (never delete) |
| `Z:\sota-cache` | Shared `uv` wheel & Python cache | **PRESERVED SHARED CACHE** (never delete) |

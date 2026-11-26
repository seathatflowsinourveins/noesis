# `tier0-sota-clean` — Clean-Room Tier-0 SOTA Quantitative Stack

Clean-room Python 3.13 (`uv`) repository operating under [`AGENTS.md`](AGENTS.md) and [`upstream-map.toml`](upstream-map.toml). Zero Self-Invention is strictly enforced via `ast-grep` structural rules (`rules/zero_self_invention.yml`), single-winner `uv.lock` / banned-package locks, exact `risk.py` SHA-256, and `live_execution_enabled = False`; module LOC targets below are advisory telemetry.

## Architecture & Advisory Module Telemetry

- **`src/tier0_sota/risk.py` (exact SHA-256 `ad8071dc...`, advisory `<= 400` LOC):** Immutable exact `Decimal` Rule-612 stop quantization (`<= 2.00%`), session-latched `-1.50%` daily drawdown fuse, and `>= 10%` exit reserve.
- **`src/tier0_sota/engine.py` (advisory `<= 650` LOC):** Declarative `NautilusTrader v2` 24/5 session protection, 4-tier progressive lossless trailing, rapid re-entry, flatten-first auto-reverse, and venue rate pacing (`IBKR_PAPER_4002` / `ALPACA_PAPER_200`, `live_execution_enabled = False`).
- **`src/tier0_sota/discovery.py` (advisory `<= 400` LOC):** Causal PIT (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`), multi-feed market regime, and `ml4t-engineer`/`river`/`ruptures`/`stumpy` signal adapters.
- **`src/tier0_sota/research.py` (advisory `<= 350` LOC):** 6-gate statistical falsification (`hftbacktest` markout, `1x`/`2x` costs, `skfolio` CPCV `MIN_CPCV_SAMPLES = 16`, `ml4t-diagnostic` DSR/PBO, `arch` Hansen SPA, `statsmodels` HAC, `MLflow` logging).
- **`src/tier0_sota/live/` (advisory: `scanner.py <= 320`, `paper_farm.py <= 380`, `performance.py <= 220`):** Whole-market extreme-mover scanner (`Alpaca SIP` + `FINRA TRF`), 3-lane Alpaca Paper + `IBKR :4002` Paper Farm adapter (`paper_only = True`), and live performance manifest aggregator.

## Clean Bootstrap

Windows (PowerShell):
```powershell
pwsh -NoProfile -File scripts/bootstrap_env.ps1
```

Linux / macOS / Git Bash:
```bash
bash scripts/bootstrap_env.sh
```

## Verification & Ecosystem Gates

```bash
ruff check src tests scripts evals
ruff format --check src tests scripts evals
ty check src tests scripts evals
ast-grep scan
pytest -v
python evals/formal_risk_z3.py
zensical build --clean
python scripts/nautilus_mcp_server.py --smoke
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/verify_grand_reset.ps1
gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner
```

## Architecture, ADRs & Grand-Reset Documentation

- [`docs/ARCHITECTURE_LAYER_MANIFEST.md`](docs/ARCHITECTURE_LAYER_MANIFEST.md) — Complete `L0`–`L10` active layers + `L11`–`L15` earned triggers, SPDX OSI License Firewall, Mermaid topology diagram, repository map, `13/13` hosted portals, and standalone workstation topology.
- [`ARCHITECTURE_MANIFEST.json`](ARCHITECTURE_MANIFEST.json) — Machine-readable SSOT (`tier0.architecture-layer-manifest.v1`) verified by [`tests/test_architecture_manifest.py`](tests/test_architecture_manifest.py).
- [`SOTA_STARRED_AND_BEYOND_MANIFEST.json`](SOTA_STARRED_AND_BEYOND_MANIFEST.json) — Active `L0`–`L10` upstream winners, `L11`–`L15` earned-trigger frontier catalog, and SPDX OSI firewall (`zero_self_invented_hooks = true`).
- [`docs/adr/CATALOG.md`](docs/adr/CATALOG.md) & [`zensical.toml`](zensical.toml) — Bounded evolvable MADR 4.0 ADRs (`ADR-0001`..`ADR-0012`) rendered cleanly via Zensical.
- [`docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md`](docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md) — Step-by-step Disk-Z (`00_clean_disk_z_and_agy_reset.ps1`), WSL2, AGY, TradingView, Docker, and 4-runtime Grand-Reset runbook.
- [`docs/CLEAN_ECOSYSTEM_MANIFEST.md`](docs/CLEAN_ECOSYSTEM_MANIFEST.md) — Paper Farm readiness, portal health, Orca terminal audit, and safe archive receipts.
- [`docs/TV_AUTOMATION_PLAYBOOK.md`](docs/TV_AUTOMATION_PLAYBOOK.md) — TradingView Pine v6 indicators and Half-Auto ticket/bridge workflow.

## Zero-Hook 4-Runtime MCP, Standalone Skills & Scoped Lanes

Configured identically with native `uv run --no-sync` commands and **zero self-invented runtime hooks** across `.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, and `opencode.json` (`.opencode/commands/verify-tier0.md`) for `codebase-memory`, `xerj`, `nautilus-engine` ([`scripts/nautilus_mcp_server.py`](scripts/nautilus_mcp_server.py)), and `edgartools`, plus scoped lane MCPs in `.claude/mcp-lanes/` (`sim`, `paper`, `tv`, `gate`), `46` standalone SHA-pinned skills in `.claude/skills/` ([`UPSTREAM_SKILLS_LOCK.json`](UPSTREAM_SKILLS_LOCK.json)), and `7` few-line lanes in `.claude/agents/`.

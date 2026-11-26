# `noesis` — Clean-Room Tier-0 SOTA Quantitative Stack (`seathatflowsinourveins/noesis`)

Clean-room Python 3.13 (`uv`) repository operating strictly under **SOTA Upstream Convergence directed by Core Trading Philosophy** ([`AGENTS.md`](AGENTS.md), [`PHILOSOPHY_COMPOUNDING_MANIFEST.json`](PHILOSOPHY_COMPOUNDING_MANIFEST.json), [`upstream-map.toml`](upstream-map.toml)). Zero Self-Invention is enforced mechanically via upstream gates (`ast-grep`, single-winner OSI `uv.lock`, `deptry`, `ruff`, `ty`, `prek`, `gitleaks`, `trivy`, `zizmor`, `Z3` SMT proofs, immutable `risk.py` SHA-256, and `live_execution_enabled = False`) with zero arbitrary LOC caps.

## Architecture & Upstream Modules

- **`src/tier0_sota/risk.py` (immutable SHA-256 `ad8071dc...`):** Exact `Decimal` Rule-612 stop quantization (`<= 2.00%`), session-latched `-1.50%` daily drawdown fuse, and `>= 10%` protective exit reserve.
- **`src/tier0_sota/engine.py`:** Declarative `NautilusTrader v2` 24/5 session protection (`RTH` `STOP_MARKET` vs collared `outside_rth` `STOP_LIMIT`), 4-tier progressive profit-lock trailing, rapid re-entry, flatten-first auto-reverse, and venue rate pacing (`IBKR_PAPER_4002` / `ALPACA_PAPER_200`, `live_execution_enabled = False`).
- **`src/tier0_sota/discovery.py`:** Causal PIT (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`), multi-feed market regime, and `ml4t-engineer`/`river`/`ruptures`/`stumpy` signal adapters.
- **`src/tier0_sota/research.py`:** 6-gate statistical falsification (`hftbacktest` markout, `1x`/`2x` costs, `skfolio` CPCV `MIN_CPCV_SAMPLES = 16`, `ml4t-diagnostic` DSR/PBO, `arch` Hansen SPA, `statsmodels` HAC, `MLflow` logging).
- **`src/tier0_sota/definitions.py`:** Upstream `Dagster` asset graph (`prewall_pit_audit`, `risk_kernel_sha256_attestation`, `paper_farm_performance_snapshot`, `inspect_safety_and_philosophy_eval`, `compounding_self_reflection_snapshot`) + weekday compounding schedule.
- **`src/tier0_sota/live/` & `src/tier0_sota/tv/`:** Whole-market extreme-mover scanner (`Alpaca SIP` + `FINRA TRF`), 3-lane Alpaca Paper + `IBKR :4002` Paper Farm (`paper_only = True`), and display-only TradingView Pine v6 + Pydantic v2 (`TvAlertV1`) half-auto bridge.

## Clean Bootstrap & Seamlessly Automated Tasks (`just`)

```powershell
pwsh -NoProfile -File scripts/bootstrap_env.ps1   # Windows
bash scripts/bootstrap_env.sh                      # Linux / WSL2 Noesis-SOTA
```

```bash
just check           # ruff, ty, deptry, ast-grep, sync_manifest, Z3, MCP smoke, pytest, gitleaks
just docs-build      # uv run --no-sync zensical build --clean --strict
just preflight       # 13-gate workstation/portal/OmniRoute/Orca preflight -> manifest
just resume          # Reconcile WSL2 systemd + Docker + OmniRoute + preflight post-restart
just reflect         # Dual-Flywheel self-reflection (Nautilus+Optuna+MLflow+Inspect+DSPy+XERJ)
just account-status  # Read-only Orca native Claude/Codex OAuth headroom check
```

One-command CI gates ([`scripts/check.ps1`](scripts/check.ps1), [`scripts/check.sh`](scripts/check.sh)) mirror [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Documentation & Living Evolvable ADRs

- [`docs/ARCHITECTURE_LAYER_MANIFEST.md`](docs/ARCHITECTURE_LAYER_MANIFEST.md) — Active `L0`–`L10` layers + `L11`–`L15` earned-trigger frontier, SPDX OSI License Firewall, Mermaid topology, and `15/15` portals + `OmniRoute :20128`.
- [`docs/COMPOUNDING_SELF_REFLECTION_LOOP.md`](docs/COMPOUNDING_SELF_REFLECTION_LOOP.md) — Upstream Dual-Flywheel strategy & harness self-reflection loop (`NautilusTrader v2` + `Optuna` + `MLflow 3` + `Inspect AI` + `DSPy` + `Dagster` + `XERJ`).
- [`docs/POST_RESTART_RESUME_PLAYBOOK.md`](docs/POST_RESTART_RESUME_PLAYBOOK.md) — Logon auto-resume, OmniRoute + Orca multi-account OAuth routing, and one-command workstation recovery.
- [`docs/adr/CATALOG.md`](docs/adr/CATALOG.md) & [`zensical.toml`](zensical.toml) — Living, evolvable MADR 4.0 ADRs (`ADR-0001+`) rendered via Zensical (`:8010`).
- [`docs/CLEAN_ECOSYSTEM_MANIFEST.md`](docs/CLEAN_ECOSYSTEM_MANIFEST.md), [`docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md`](docs/GRAND_RESET_DISK_Z_WSL_RUNBOOK.md), & [`docs/TV_AUTOMATION_PLAYBOOK.md`](docs/TV_AUTOMATION_PLAYBOOK.md).

## Zero-Hook Multi-Runtime MCP, Curated Skills & Scoped Lanes

All 5 repo configs (`.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, `opencode.json`) wire native `uv run --no-sync` stdio MCPs (`codebase-memory`, `xerj`, `nautilus-engine`, `edgartools`) with **zero self-invented runtime hooks**, scoped lanes in `.claude/mcp-lanes/`, `46/46` SHA-locked skills in `.claude/skills/` curated across all 4 compounding lanes in [`LLM_HARNESS_MANIFEST.json`](LLM_HARNESS_MANIFEST.json), and Orca native orchestration in [`orca.yaml`](orca.yaml).

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

## Verification Gates

```bash
ruff check src tests scripts
ruff format --check src tests scripts
ty check src tests scripts
ast-grep scan
pytest -v
gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner
```

## 4-Runtime MCP Parity

Configured identically across `.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, and `opencode.json` for `codebase-memory`, `xerj`, `nautilus-engine`, and `edgartools`.

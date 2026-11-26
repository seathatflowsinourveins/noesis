---
description: Master upstream verification across just check, prek, Zensical --strict, FastMCP, portals & gitleaks.
argument-hint: "[--strict]"
---
Verify `Z:\noesis` (`$ARGUMENTS`) empirically with zero self-hallucination:
1. Run `just check`, `just security`, `just preflight`, `uv run --no-sync zensical build --clean --strict`, and `prek run --all-files` (or `ruff`, `ty`, `deptry`, `ast-grep`, `pytest -q`, `scripts/sync_manifest.py --check`).
2. Run `uv run --no-sync python scripts/nautilus_mcp_server.py --smoke`, `pwsh -NoProfile -ExecutionPolicy Bypass -File scripts\grand_reset\verify_grand_reset.ps1`, and `gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner`.
3. Verify `15/15` hosted portals HTTP 200 (incl. DuckDB UI `:4213/`, real `otelcol_*` on `:9464/metrics`, OTel health `:13133/`, and `Caddy` on `:8793/`).
4. Confirm `risk.py` SHA-256 (`ad8071dc...`), `0` live ports (`4001/7496/8798/8796`), `0` `yfinance`/`quantstats` in `uv.lock`, and `0` arbitrary LOC caps.

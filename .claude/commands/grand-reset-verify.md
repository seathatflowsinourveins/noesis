---
description: Grand-Reset master verification across AST rules, pytest, Zensical, FastMCP, portals & gitleaks.
argument-hint: "[--strict]"
---
Verify `Z:\tier0-sota-clean` (`$ARGUMENTS`):
1. Run `ruff check .`, `ty check`, `ast-grep scan`, `pytest -q`, and `zensical build --clean`.
2. Run `.venv\Scripts\python.exe scripts\nautilus_mcp_server.py --smoke`, `pwsh -NoProfile -ExecutionPolicy Bypass -File scripts\grand_reset\verify_grand_reset.ps1`, and `gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner`.
3. Confirm `risk.py` SHA-256 (`ad8071dc...`), `0` live ports (`4001/7496/8798/8796`), and `0` `yfinance` in `uv.lock`.

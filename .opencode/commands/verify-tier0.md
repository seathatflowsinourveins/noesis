---
description: OpenCode verification command running Tier-0 SOTA static, AST, MCP, Paper Farm, and Grand-Reset integrity checks in Z:\tier0-sota-clean.
---

Verify `Z:\tier0-sota-clean` for `$ARGUMENTS` without touching `.env*`, `~/Documents/upstream/`, or live broker ports (`4001/7496/8798/8796`):

1. **Zero Self-Invention & Static Checks:** Run `ruff check .`, `ty check`, `ast-grep scan`, and confirm `src/tier0_sota/risk.py` SHA-256 is `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847` (`361` LOC).
2. **MCP & Portal Parity:** Confirm `codebase-memory`, `xerj`, `nautilus-engine` (`scripts/nautilus_mcp_server.py --smoke` = `10/10`), and `edgartools` match across `.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, and `opencode.json`.
3. **Suite & Grand Reset:** Run `pytest -q`, `zensical build --clean`, `pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/verify_grand_reset.ps1`, and `gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner`.
4. **Report Verbatim:** Quote exact pass/fail counts and confirm `live_execution_enabled = False` / `paper_only = True`.

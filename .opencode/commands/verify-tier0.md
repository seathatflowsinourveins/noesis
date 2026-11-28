---
description: OpenCode verification command running Tier-0 SOTA static, AST, MCP, Paper Farm, and Grand-Reset integrity checks in Z:\noesis.
---

Verify `Z:\noesis` for `$ARGUMENTS` without touching `.env*`, `~/Documents/upstream/`, or the unwired IBKR live ports (`4001/7496/8798/8796`):

1. **Zero Self-Invention & Static Checks:** Run `ruff check .`, `ruff format --check src tests scripts evals`, `ty check`, `deptry .`, `ast-grep scan --config sgconfig.yml`, `just adr-check`, `just z3`, `just crosshair`, and confirm `src/tier0_sota/risk.py` SHA-256 is `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847` (zero arbitrary LOC caps).
2. **MCP & Portal Parity:** Confirm `codebase-memory`, `xerj`, `nautilus-engine` (`scripts/nautilus_mcp_server.py --smoke` = `10/10`), and `edgartools` match across `.mcp.json`, `.claude/settings.json`, `.codex/config.toml`, `.gemini/settings.json`, and `opencode.json`.
3. **Suite, Docs & Cross-OS Verify:** Run `just check` (or `bash scripts/check.sh` on WSL2 / `pwsh -NoProfile -File scripts/check.ps1` on Windows), `just test-report`, `uv run --no-sync zensical build --clean --strict`, `bash scripts/preflight_resume.sh` (Windows: `pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/grand_reset/verify_grand_reset.ps1`), and `gitleaks detect --no-git --source . --config .gitleaks.toml --no-banner`.
4. **Report Verbatim:** Quote exact pass/fail counts and confirm `risk.py` invariants, lossless trailing coverage in every lane, and the live tier gate of ADR-0029 (`live_execution_enabled = False` means tier 3 is off).

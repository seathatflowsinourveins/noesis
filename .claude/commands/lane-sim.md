---
description: Sim lane (24/7 SANDBOX) status report using .claude/mcp-lanes/sim.mcp.json. Read-only.
argument-hint: "[optional symbol or MLflow run]"
---
Run read-only Sim check for `$ARGUMENTS`:
1. Inspect `nautilus-engine` (`get_engine_backbone_status`, `run_prewall_native_backbone_parity`) and `edgartools` PIT (`<= 2026-10-15`, `t_ns + 60s`, `XNYS`).
2. Check `get_live_watchlist_manifest` & `get_live_performance_manifest` plus 6-gate standing (long & short legs) in `MLflow` (`:5000`).

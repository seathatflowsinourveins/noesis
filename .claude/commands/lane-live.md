---
description: Live-operator firewall confirming Stage-5 Live stays LIVE REFUSED. Read-only.
argument-hint: "[optional strategy family]"
---
Run read-only `paper-gatekeeper` firewall check for `$ARGUMENTS` (zero live broker hosts/ports, zero `.env*`):
1. Verify `live_execution_enabled = False`, `paper_only = True`, and `ast-grep` live-port/endpoint bans across `src/tier0_sota/`.
2. Report 6-gate falsification standing separately for long vs reversal-short legs; return verdict `LIVE REFUSED`.

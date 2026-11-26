---
description: Paper lane (ALPACA_PA1..PA3 + IBKR_DU_4002) readiness report using paper.mcp.json. Read-only.
argument-hint: "[ibkr_paper_4002 | alpaca_paper_200 | all]"
---
Run read-only `paper-gatekeeper` check for `$ARGUMENTS` (never pass `-ConfirmPaper`, never read `.env*`, never probe `:4001/:7496/:8798/:8796`):
1. Call `inspect_venue_profile` (`nautilus-engine`) confirming `live_execution_enabled = False` and `>= 10%` exit reserve.
2. Read `Z:\sota-data\manifests\paper_farm_live_performance_manifest.json` & `alpaca_pa*_live_automation_status.json`, verifying `risk.py` SHA-256 (`ad8071dc...`).

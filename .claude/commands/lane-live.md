---
description: Active live promotion-readiness lane (Sim==Paper==Live LiveNode parity, Z3, 6-gate dossier) via gate.mcp.json.
argument-hint: "[strategy family or promotion dossier]"
---
Execute Stage-5 `live` promotion-readiness & parity audit for `$ARGUMENTS` via read-only `paper-gatekeeper` (zero live ports `:4001/:7496/:8798/:8796`, zero `.env*`):
1. Verify `Sim == Paper == Live` `NautilusTrader v2` `LiveNode`/`InteractiveBrokers` parity, `live_execution_enabled = False`, `paper_only = True`, and `risk.py` SHA-256 (`ad8071dc...`).
2. Run formal `Z3` SMT proofs (`evals/formal_risk_z3.py`), `Inspect AI` safety evals (`evals/tier0_inspect_eval.py`), and `ast-grep` live-endpoint firewalls.
3. Compile cross-leg 6-gate promotion dossier (`1x`/`2x` costs, `hftbacktest` markout, `skfolio` CPCV `>= 16`, `ml4t-diagnostic` DSR/PBO, `arch` SPA, `statsmodels` HAC) into compounding readiness manifests.
4. Keep real-capital execution mechanically gated (`LIVE REFUSED`) until every gate clears independently across long and reversal-short legs.

---
description: Active paper compounding lane (4-account Noesis Paper Farm, DuckDB, MLflow, OTel/FINOS) via paper.mcp.json.
argument-hint: "[ibkr_paper_4002 | alpaca_paper_200 | all]"
---
Execute compounding `paper` lane iteration for `$ARGUMENTS` (runs under `fnox --profile paper`; IBKR live ports `:4001/:7496/:8798/:8796` are not wired; zero `.env*` plaintext secrets, `fnox` + `age`):
1. Run `inspect_venue_profile` (`nautilus-engine`) and `scripts/run_live_paper_farm.ps1 -Mode preflight` across `IBKR :4002` + 3 non-overlapping `Alpaca Paper 200 rpm` lanes (`live_execution_enabled = False`, `>= 10%` exit reserve).
2. Compound real-time extreme-mover scanning, ETB/inverse routing, and 24/5 near-lossless trailing, flatten-first auto-reverse, and rapid re-entry execution inside `NautilusTrader v2` + `alpaca-py`.
3. Refresh isolated `DuckDB` + `MLflow` fill/markout compounding manifests in `Z:\sota-data\manifests\` (`-Mode performance`) and verify live `FINOS`/`OpenBB`/`Grafana`/`OTel` (`:9464`) + `Caddy` (`:8793`) telemetry.
4. Confirm untouched `risk.py` SHA-256 (`ad8071dc...`) and run E2E checks (`just check`).
5. Curated skills (16): `stockbee-episodic-pivot-analyzer`, `stockbee-momentum-burst-screener`, `stockbee-exhaustion-hammer-screener`, `vcp-screener`, `pead-screener`, `parabolic-short-trade-planner`, `breakout-trade-planner`, `position-sizer`, `exposure-coach`, `drawdown-circuit-breaker`, `pre-trade-discipline-gate`, `ml4t-monitoring-alerting`, `finos-develop-plugins`, `finos-administer-via-cli`, `tier0-upstream-dashboards-and-observability`, `trading-skills-navigator` (agents: `observability-operator`, `memory-rag-evolution`).

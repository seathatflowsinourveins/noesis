---
name: tier0-upstream-dashboards-and-observability
description: Enact all dashboards, Prometheus (:9090), Grafana (:3001), OpenTelemetry (:9464), FINOS/OpenBB Workspace (:1420/:6900), DuckDB SQL UI, and unmodified Nautilus/ml4t-diagnostic/SHAP tearsheets (:8793) strictly via official upstream platforms with zero hand-written HTML/JS.
---

# Tier-0 Upstream Dashboards, Tearsheets & Observability Directive

1. **Zero Self-Edit UI Law:** NEVER hand-write HTML, JSX, TSX, Vue, Svelte, FastAPI, Flask, Dash, Gradio, or bespoke React dashboards in production source or script directories. Enforced mechanically by `ast-grep` (`no-custom-dashboard-server`), the `pre-commit` / CI web-asset fail hook, and `PostToolUse` static lint.
2. **Official Hosted Institutional Terminal (`ADR-0001` through `ADR-0007`):**
   - **FINOS Workspace:** official `openbq-org` FINOS Workspace (Apache 2 license) on port `:1420` (API `:8000`) with checksum-verified `plotly-charts` and `ag-grid` widget plugins.
   - **OpenBB Custom Backend:** official `openbq-org` OpenBB backend (`MIT`) on port `:6900` serving declarative widgets and apps manifests.
   - **Post-Proof Embedded Streaming Primitives:** `FINOS Perspective` and `TradingView Lightweight Charts` remain gated behind `paper_live_deployed == True`.
3. **Official 24/5 Observability & Alerting (Docker Compose SOTA Stack):**
   - **OpenTelemetry + Prometheus Exporter (telemetry module):** Exposes loopback-only metrics on port `:9464` (`tier0_prewall_trials_total`, `tier0_falsification_passes_total`, `tier0_rule612_stop_clamps_total`, `tier0_drawdown_breaker_latched`, `tier0_observability_up`).
   - **Prometheus:** official container on port `:9090` configured strictly via declarative Prometheus scrape config.
   - **Grafana:** official container on port `:3001` provisioned declaratively via Grafana provisioning configs and dashboard JSON.
4. **Four Pure Unmodified Upstream Tearsheets (port `:8793`) & SHAP:**
   - **NautilusTrader:** official `create_tearsheet` with `TearsheetConfig(theme="nautilus_dark")`.
   - **`ml4t-diagnostic` + `shap`:** `export_ml4t_diagnostic_tearsheet(..., n_trials=N)` (DSR and PBO credibility box) plus `compute_lightgbm_shap_summary()` (`TreeExplainer`).
   - **VectorBT:** `Portfolio` `from_signals` Plotly dark tearsheet (pre-screen reference).
   - **QuantStats:** official HTML performance tearsheet (engine-independent witness).
5. **Research & Data Control Planes:** `MLflow 3` (`:5000`), `Dagster` (`:3000`), `Optuna Dashboard` (`:8080`), `Inspect View` (`:7575`), `Zensical` (`:8010`), `XERJ` (`:9200`), and `duckdb -ui`.

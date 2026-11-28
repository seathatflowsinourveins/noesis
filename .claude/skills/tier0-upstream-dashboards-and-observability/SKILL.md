---
name: tier0-upstream-dashboards-and-observability
description: Enact all dashboards, Prometheus (:9090), Grafana (:3001), OpenTelemetry (:9464), FINOS/OpenBB Workspace (:1420/:6900), DuckDB SQL UI (:4213), OmniRoute (:20128), and unmodified Nautilus/ml4t-diagnostic/SHAP tearsheets (:8793) strictly via official upstream platforms with zero hand-written HTML/JS.
---

# Tier-0 Upstream Dashboards, Tearsheets & Observability Directive

1. **Zero Self-Edit UI Law:** NEVER hand-write HTML, JSX, TSX, Vue, Svelte, FastAPI, Flask, Dash, Gradio, or bespoke React dashboards in production source or script directories. Enforced mechanically by `ast-grep` (`no-custom-dashboard-server`) and `prek`.
2. **Official Hosted Institutional Terminal (`FINOS` + `OpenBB`):**
   - **FINOS Workspace:** official `openbq-org` FINOS Workspace (Apache 2.0) on `:1420` (API `:8000/health`) with checksum-verified widget plugins (`finos-develop-plugins`, `finos-administer-via-cli`).
   - **OpenBB Custom Backend:** official `openbq-org` OpenBB backend (`MIT`) on `:6900` serving declarative widgets and apps manifests (`tradingview/lightweight-charts` for chart widgets).
3. **Official 24/5 Observability (Docker Compose SOTA Stack):**
   - **OpenTelemetry Collector (`otel/opentelemetry-collector-contrib`):** Prometheus metrics `:9464/metrics` (`otelcol_*`) and health `:13133` via declarative `docker/otel-collector-config.yaml`.
   - **Prometheus (`:9090`), Alertmanager (`:9093`, profile `alerting`) & Grafana (`:3001`):** provisioned declaratively via `docker/prometheus.yml`, `docker/prometheus_rules.yml`, `docker/alertmanager.yml`, and `docker/grafana/`.
4. **Unmodified Official OSI Tearsheets & Allure 3 (Caddy `:8793`) & SHAP:**
   - **NautilusTrader:** official `create_tearsheet` with `TearsheetConfig(theme="nautilus_dark")`.
   - **`ml4t-diagnostic` + `shap`:** official statistical/PBO tearsheets and `TreeExplainer` SHAP attribution.
   - **Allure Report 3 (`:8793/test-reports/allure/`):** static upstream test dashboard generated via `just test-report`.
   - Served strictly via upstream `caddy:2.11.9-alpine` (`docker/Caddyfile`, `network_mode: host`, loopback `bind 127.0.0.1`) on `:8793` and `:4213 -> 127.0.0.1:4214` Origin-normalising DuckDB UI CSRF reverse proxy (zero Python HTTP server shims).
5. **Research, Eval & Router Control Planes:** `MLflow 3` (`:5000`), `Dagster` (`:3000`), `Optuna Dashboard` (`:8080`), `Inspect View` (`:7575`), `Zensical` (`:8010`), `XERJ` (`:9200`), `DuckDB SQL UI` (`:4213`), and `OmniRoute` (`:20128`).

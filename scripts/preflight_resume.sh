#!/usr/bin/env bash
set -euo pipefail

# Idempotent Tier-0 SOTA Ecosystem Preflight & Post-Restart Seamless Resume Kit (WSL2/Linux).
# Supports: --reconcile, --json, --manifest <path>

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"
MANIFEST_PATH="${DATA_ROOT}/manifests/workstation_preflight_resume_manifest.json"
EXPECTED_WSL_DISTRO="Noesis-SOTA"
EXPECTED_RISK_SHA="ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
EXPECTED_CHARTER_SHA="057933bbda0db411bbfbc189c72a3e7b59f1989650e110f8591cf64728555c1c"
EXPECTED_ORIGIN_SLUG="seathatflowsinourveins/noesis"
FORBIDDEN_LIVE_PORTS=(4001 7496 8798 8796)
SYSTEMD_UNITS=(
  "tier0-duckdb-ui"
  "tier0-openbb-backend"
  "tier0-ollama-docker-bridge"
  "tier0-openbb-docker-bridge"
)

RECONCILE=false
JSON_OUT=false

usage() {
  cat <<'EOF'
Usage: scripts/preflight_resume.sh [--reconcile] [--json] [--manifest <path>]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reconcile)
      RECONCILE=true
      shift
      ;;
    --json)
      JSON_OUT=true
      shift
      ;;
    --manifest)
      if [[ $# -lt 2 ]]; then
        echo "Missing argument for --manifest" >&2
        exit 2
      fi
      MANIFEST_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for scripts/preflight_resume.sh" >&2
  exit 1
fi

GATES_JSON="{}"
FAILURES_JSON="[]"
ACTIONS_JSON="[]"
GATES_PASSED=0
GATES_TOTAL=0

log_line() {
  local tag="$1"
  local msg="$2"
  if [[ "$JSON_OUT" != "true" ]]; then
    printf '[%s] %s\n' "$tag" "$msg"
  fi
}

record_gate() {
  local name="$1"
  local ok="$2"
  local detail="$3"
  GATES_TOTAL=$((GATES_TOTAL + 1))
  if [[ "$ok" == "true" ]]; then
    GATES_PASSED=$((GATES_PASSED + 1))
    log_line "OK" "${name}: ${detail}"
  else
    log_line "FAIL" "${name}: ${detail}"
    FAILURES_JSON="$(jq --arg msg "${name}: ${detail}" '. + [$msg]' <<<"$FAILURES_JSON")"
  fi
  GATES_JSON="$(jq \
    --arg name "$name" \
    --argjson ok "$ok" \
    --arg detail "$detail" \
    '.[$name] = {ok: $ok, detail: $detail}' <<<"$GATES_JSON")"
}

record_action() {
  local action="$1"
  local ok="$2"
  local detail="$3"
  ACTIONS_JSON="$(jq \
    --arg action "$action" \
    --argjson ok "$ok" \
    --arg detail "$detail" \
    '. + [{action: $action, ok: $ok, detail: $detail}]' <<<"$ACTIONS_JSON")"
}

# 1. Fail-closed zero live broker ports (:4001/:7496/:8798/:8796)
LIVE_HITS=()
if command -v ss >/dev/null 2>&1; then
  SS_OUT="$(ss -lntH 2>/dev/null || true)"
  for port in "${FORBIDDEN_LIVE_PORTS[@]}"; do
    if grep -Eq "[:.]${port}[[:space:]]" <<<"$SS_OUT"; then
      LIVE_HITS+=("$port")
    fi
  done
fi
if [[ "${#LIVE_HITS[@]}" -eq 0 ]]; then
  record_gate "live_broker_ports_zero" "true" "listeners=0 forbidden=4001,7496,8798,8796"
else
  record_gate "live_broker_ports_zero" "false" "open_ports=${LIVE_HITS[*]}"
fi

# 2. WSL2 distro check == Noesis-SOTA
ACTUAL_DISTRO="${WSL_DISTRO_NAME:-}"
if [[ "$ACTUAL_DISTRO" == "$EXPECTED_WSL_DISTRO" ]]; then
  record_gate "wsl_default_distro" "true" "distro=${ACTUAL_DISTRO} expected=${EXPECTED_WSL_DISTRO}"
else
  record_gate "wsl_default_distro" "false" "distro=${ACTUAL_DISTRO:-unset} expected=${EXPECTED_WSL_DISTRO}"
fi

# 3. Optional --reconcile: WSL2 systemd units + Docker Compose stack
if [[ "$RECONCILE" == "true" ]]; then
  log_line "RECONCILE" "Ensuring WSL2 systemd services are active..."
  SYS_OK=true
  for unit in "${SYSTEMD_UNITS[@]}"; do
    if ! systemctl is-active --quiet "$unit" 2>/dev/null; then
      if [[ "$(id -u)" -eq 0 ]]; then
        systemctl start "$unit" || SYS_OK=false
      else
        sudo -n systemctl start "$unit" || SYS_OK=false
      fi
    fi
  done
  record_action "wsl_systemd_reconcile" "$SYS_OK" "units=${SYSTEMD_UNITS[*]}"

  log_line "RECONCILE" "Running Docker Compose up -d..."
  if TIER0_REPO_ROOT="$REPO_ROOT" TIER0_DATA_ROOT="$DATA_ROOT" TIER0_STATE_ROOT="${TIER0_STATE_ROOT:-$HOME/tier0-sota-state}" \
      docker compose -p docker -f "${REPO_ROOT}/docker/compose.sota.yml" up -d >/dev/null 2>&1; then
    record_action "docker_compose_up" "true" "docker compose -p docker up -d succeeded"
  else
    record_action "docker_compose_up" "false" "docker compose -p docker up -d failed"
  fi
fi

# 4. Verify WSL2 systemd services
SYS_ACTIVE=0
SYS_INACTIVE=()
for unit in "${SYSTEMD_UNITS[@]}"; do
  state="$(systemctl is-active "$unit" 2>/dev/null || echo inactive)"
  if [[ "$state" == "active" ]]; then
    SYS_ACTIVE=$((SYS_ACTIVE + 1))
  else
    SYS_INACTIVE+=("${unit}=${state}")
  fi
done
if [[ "$SYS_ACTIVE" -eq "${#SYSTEMD_UNITS[@]}" ]]; then
  record_gate "wsl_systemd_services" "true" "active=${SYS_ACTIVE}/${#SYSTEMD_UNITS[@]}"
else
  record_gate "wsl_systemd_services" "false" "active=${SYS_ACTIVE}/${#SYSTEMD_UNITS[@]} inactive=${SYS_INACTIVE[*]}"
fi

# 5. Verify Docker Compose stack
DOCKER_RUNNING=0
DOCKER_TOTAL=0
if command -v docker >/dev/null 2>&1; then
  while IFS='|' read -r _cname cstate; do
    [[ -z "${_cname}" ]] && continue
    DOCKER_TOTAL=$((DOCKER_TOTAL + 1))
    if [[ "$cstate" == "running" ]]; then
      DOCKER_RUNNING=$((DOCKER_RUNNING + 1))
    fi
  done < <(docker compose -p docker -f "${REPO_ROOT}/docker/compose.sota.yml" ps --format '{{.Name}}|{{.State}}' 2>/dev/null || true)
fi
if [[ "$DOCKER_TOTAL" -ge 10 && "$DOCKER_RUNNING" -eq "$DOCKER_TOTAL" ]]; then
  record_gate "docker_compose_stack" "true" "running=${DOCKER_RUNNING}/${DOCKER_TOTAL}"
else
  record_gate "docker_compose_stack" "false" "running=${DOCKER_RUNNING}/${DOCKER_TOTAL}"
fi

# 6. Probe all 15/15 local portals
PORTAL_SPECS=(
  "xerj|http://127.0.0.1:9200/"
  "zensical|http://127.0.0.1:8010/"
  "mlflow|http://127.0.0.1:5000/"
  "dagster|http://127.0.0.1:3000/"
  "optuna-dashboard|http://127.0.0.1:8080/"
  "inspect-view|http://127.0.0.1:7575/"
  "prometheus|http://127.0.0.1:9090/"
  "grafana|http://127.0.0.1:3001/"
  "otel-telemetry|http://127.0.0.1:9464/metrics"
  "tearsheets|http://127.0.0.1:8793/"
  "finos-workspace|http://127.0.0.1:1420/"
  "finos-api|http://127.0.0.1:8000/health"
  "openbb-backend|http://127.0.0.1:6900/"
  "duckdb-ui|http://127.0.0.1:4213/"
  "otel-health|http://127.0.0.1:13133/"
)

probe_all_portals() {
  PORTALS_JSON="[]"
  PORTALS_HEALTHY=0
  PORTALS_DOWN=()
  OTEL_NOTE=""
  CADDY_NOTE=""

  local hdr_tmp body_tmp
  hdr_tmp="$(mktemp)"
  body_tmp="$(mktemp)"
  # shellcheck disable=SC2064
  trap "rm -f '$hdr_tmp' '$body_tmp'" RETURN

  for spec in "${PORTAL_SPECS[@]}"; do
    local pname="${spec%%|*}"
    local purl="${spec#*|}"
    local code="000"
    local pok="false"
    local pnote=""

    : >"$hdr_tmp"
    : >"$body_tmp"
    code="$(curl -sS -m 3 -D "$hdr_tmp" -o "$body_tmp" -w '%{http_code}' "$purl" 2>/dev/null || echo "000")"
    if [[ "$code" =~ ^[23][0-9][0-9]$ ]]; then
      pok="true"
      case "$pname" in
        otel-telemetry)
          local otel_lines
          otel_lines="$(grep -Ec '^otelcol_[A-Za-z0-9_]+' "$body_tmp" || true)"
          if [[ "$otel_lines" -le 0 ]]; then
            pok="false"
          fi
          pnote="otelcol_lines=${otel_lines}"
          OTEL_NOTE="$pnote"
          ;;
        tearsheets)
          local srv_hdr
          srv_hdr="$(grep -i '^Server:' "$hdr_tmp" | head -n 1 | tr -d '\r' | sed 's/^[Ss]erver:[[:space:]]*//')"
          if ! grep -qi 'caddy' <<<"$srv_hdr"; then
            pok="false"
          fi
          pnote="server=${srv_hdr}"
          CADDY_NOTE="$pnote"
          ;;
        zensical)
          if ! grep -q 'noesis' "$body_tmp"; then
            pok="false"
            pnote="canonical_title=false"
          else
            pnote="canonical_title=true"
          fi
          ;;
        finos-api)
          if ! grep -Eq '"success"[[:space:]]*:[[:space:]]*true' "$body_tmp"; then
            pok="false"
            pnote="health_ok=false"
          else
            pnote="health_ok=true"
          fi
          ;;
      esac
    else
      pnote="http_code=${code}"
    fi

    if [[ "$pok" == "true" ]]; then
      PORTALS_HEALTHY=$((PORTALS_HEALTHY + 1))
    else
      PORTALS_DOWN+=("${pname}(${code}:${pnote})")
    fi

    PORTALS_JSON="$(jq \
      --arg name "$pname" \
      --arg url "$purl" \
      --argjson status_code "$((10#${code:-0}))" \
      --argjson ok "$pok" \
      --arg note "$pnote" \
      '. + [{name: $name, url: $url, status_code: $status_code, ok: $ok, note: $note}]' \
      <<<"$PORTALS_JSON")"
  done
}

probe_all_portals
if [[ "$RECONCILE" == "true" && "$PORTALS_HEALTHY" -lt 15 ]]; then
  for _ in 1 2 3 4 5; do
    sleep 3
    probe_all_portals
    [[ "$PORTALS_HEALTHY" -eq 15 ]] && break
  done
fi

if [[ "$PORTALS_HEALTHY" -eq 15 ]]; then
  record_gate "local_portals_15" "true" "healthy=15/15 ${OTEL_NOTE} ${CADDY_NOTE}"
else
  record_gate "local_portals_15" "false" "healthy=${PORTALS_HEALTHY}/15 ${OTEL_NOTE} ${CADDY_NOTE} down=${PORTALS_DOWN[*]}"
fi

# 7. Disk Z environment variables (WSL2 mount paths)
ENV_OK=true
ENV_NOTES=()
if [[ "${TIER0_REPO_ROOT:-}" != "/mnt/z/noesis" || ! -d "/mnt/z/noesis" ]]; then
  ENV_OK=false
  ENV_NOTES+=("TIER0_REPO_ROOT=${TIER0_REPO_ROOT:-unset}")
fi
if [[ "${TIER0_DATA_ROOT:-}" != "/mnt/z/sota-data" || ! -d "/mnt/z/sota-data" ]]; then
  ENV_OK=false
  ENV_NOTES+=("TIER0_DATA_ROOT=${TIER0_DATA_ROOT:-unset}")
fi
if [[ "${ORCA_WORKSPACES_DIR:-}" != "/mnt/z/workspaces" || ! -d "/mnt/z/workspaces" ]]; then
  ENV_OK=false
  ENV_NOTES+=("ORCA_WORKSPACES_DIR=${ORCA_WORKSPACES_DIR:-unset}")
fi
if [[ "$ENV_OK" == "true" ]]; then
  record_gate "disk_z_env" "true" "TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data ORCA_WORKSPACES_DIR=/mnt/z/workspaces"
else
  record_gate "disk_z_env" "false" "mismatches=${ENV_NOTES[*]}"
fi

# 8. AGY native MCP config (WSL2 Linux ~/.gemini/config/mcp_config.json first; fallback to repo 5/5 native uv parity)
MNT_C_MOUNTED=false
WIN_USER_HOME=""
if command -v mountpoint >/dev/null 2>&1 && mountpoint -q /mnt/c 2>/dev/null; then
  MNT_C_MOUNTED=true
  for candidate in /mnt/c/Users/*; do
    if [[ -f "${candidate}/.gemini/config/mcp_config.json" ]]; then
      WIN_USER_HOME="$candidate"
      break
    fi
  done
fi
WSL_AGY_MCP_PATH="${HOME}/.gemini/config/mcp_config.json"
AGY_MCP_OK=false
AGY_MCP_DETAIL=""
if [[ -f "$WSL_AGY_MCP_PATH" ]]; then
  AGY_SERVERS="$(jq -r '.mcpServers | keys | sort | join(",")' "$WSL_AGY_MCP_PATH" 2>/dev/null || echo "")"
  MISSING_BINS=0
  while IFS= read -r cmd_bin; do
    [[ -z "$cmd_bin" ]] && continue
    if [[ "$cmd_bin" == /* && ! -x "$cmd_bin" ]]; then
      MISSING_BINS=$((MISSING_BINS + 1))
    fi
  done < <(jq -r '.mcpServers[] | .command // empty' "$WSL_AGY_MCP_PATH" 2>/dev/null || true)
  if [[ "$AGY_SERVERS" == "codebase-memory,edgartools,nautilus-engine,xerj" ]] \
      && ! grep -q '"bash"' "$WSL_AGY_MCP_PATH" \
      && grep -q 'noesis' "$WSL_AGY_MCP_PATH" \
      && [[ "$MISSING_BINS" -eq 0 ]]; then
    AGY_MCP_OK=true
  fi
  AGY_MCP_DETAIL="scope=wsl2_agy servers=${AGY_SERVERS} missing_bins=${MISSING_BINS}"
else
  MCP_PARITY_OK=true
  for cfg in ".mcp.json" ".claude/settings.json" ".gemini/settings.json"; do
    cpath="${REPO_ROOT}/${cfg}"
    if [[ ! -f "$cpath" ]]; then
      MCP_PARITY_OK=false
      continue
    fi
    cservers="$(jq -r '.mcpServers | keys | sort | join(",")' "$cpath" 2>/dev/null || echo "")"
    if [[ "$cservers" != "codebase-memory,edgartools,nautilus-engine,xerj" ]] || grep -q '"bash"' "$cpath"; then
      MCP_PARITY_OK=false
    fi
  done
  if [[ ! -f "${REPO_ROOT}/opencode.json" ]]; then
    MCP_PARITY_OK=false
  else
    ocservers="$(jq -r '.mcp | keys | sort | join(",")' "${REPO_ROOT}/opencode.json" 2>/dev/null || echo "")"
    if [[ "$ocservers" != "codebase-memory,edgartools,nautilus-engine,xerj" ]] || grep -q '"bash"' "${REPO_ROOT}/opencode.json"; then
      MCP_PARITY_OK=false
    fi
  fi
  if [[ ! -f "${REPO_ROOT}/.codex/config.toml" ]] || grep -q 'command = "bash"' "${REPO_ROOT}/.codex/config.toml"; then
    MCP_PARITY_OK=false
  fi
  AGY_MCP_OK="$MCP_PARITY_OK"
  AGY_MCP_DETAIL="scope=repo_5x5_native_uv parity=${MCP_PARITY_OK}"
fi
WSL_SKILLS_JSON="${HOME}/.gemini/config/skills.json"
WSL_SKILLS_STATUS="absent"
if [[ -f "$WSL_SKILLS_JSON" ]]; then
  if jq -e '(.entries // []) | any(.path | tostring | test("\\.claude/skills") and test("noesis"))' "$WSL_SKILLS_JSON" >/dev/null 2>&1; then
    WSL_SKILLS_STATUS="ok"
  else
    WSL_SKILLS_STATUS="missing_noesis_entry"
  fi
fi
FNOX_KEY_FILE="${FNOX_AGE_KEY_FILE:-${HOME}/.config/fnox/age.txt}"
FNOX_STATUS="absent"
if [[ -f "${REPO_ROOT}/fnox.toml" ]]; then
  if command -v fnox >/dev/null 2>&1 && [[ -f "$FNOX_KEY_FILE" ]]; then
    FNOX_STATUS="ready"
  else
    FNOX_STATUS="config_only"
  fi
fi
AGY_MCP_DETAIL="${AGY_MCP_DETAIL} skills_json=${WSL_SKILLS_STATUS} fnox=${FNOX_STATUS}"
record_gate "agy_native_mcp_config" "$AGY_MCP_OK" "$AGY_MCP_DETAIL"

# 9. Global Charter SHA-256 (8/8 when /mnt/c mounted; 4/4 WSL2 when /mnt/c unmounted per /etc/wsl.conf)
WSL_CHARTER_FILES=(
  "${HOME}/.claude/CLAUDE.md"
  "${HOME}/.codex/AGENTS.md"
  "${HOME}/.gemini/GEMINI.md"
  "${HOME}/.config/opencode/AGENTS.md"
)
CHARTER_FILES=("${WSL_CHARTER_FILES[@]}")
CHARTER_EXPECTED_TOTAL=4
CHARTER_SCOPE="wsl2_4x4_mnt_c_unmounted"
if [[ "$MNT_C_MOUNTED" == "true" && -d "$WIN_USER_HOME" ]]; then
  CHARTER_FILES+=(
    "${WIN_USER_HOME}/.claude/CLAUDE.md"
    "${WIN_USER_HOME}/.codex/AGENTS.md"
    "${WIN_USER_HOME}/.gemini/GEMINI.md"
    "${WIN_USER_HOME}/.config/opencode/AGENTS.md"
  )
  CHARTER_EXPECTED_TOTAL=8
  CHARTER_SCOPE="windows_plus_wsl2_8x8"
fi
CHARTER_MATCHES=0
for cf in "${CHARTER_FILES[@]}"; do
  if [[ -f "$cf" ]]; then
    csha="$(sha256sum "$cf" | awk '{print $1}')"
    if [[ "$csha" == "$EXPECTED_CHARTER_SHA" ]]; then
      CHARTER_MATCHES=$((CHARTER_MATCHES + 1))
    fi
  fi
done
if [[ "$CHARTER_MATCHES" -eq "$CHARTER_EXPECTED_TOTAL" ]]; then
  record_gate "global_charter_sha256_8x8" "true" "scope=${CHARTER_SCOPE} matched=${CHARTER_MATCHES}/${CHARTER_EXPECTED_TOTAL} expected=${EXPECTED_CHARTER_SHA}"
else
  record_gate "global_charter_sha256_8x8" "false" "scope=${CHARTER_SCOPE} matched=${CHARTER_MATCHES}/${CHARTER_EXPECTED_TOTAL} expected=${EXPECTED_CHARTER_SHA}"
fi

# 10. Immutable risk.py SHA-256
RISK_PATH="${REPO_ROOT}/src/tier0_sota/risk.py"
RISK_SHA="missing"
RISK_OK=false
if [[ -f "$RISK_PATH" ]]; then
  RISK_SHA="$(sha256sum "$RISK_PATH" | awk '{print $1}')"
  [[ "$RISK_SHA" == "$EXPECTED_RISK_SHA" ]] && RISK_OK=true
fi
record_gate "risk_kernel_sha256" "$RISK_OK" "$RISK_SHA"

# 11. Git status & origin (seathatflowsinourveins/noesis)
GIT_ORIGIN="$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || echo "")"
GIT_BRANCH="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")"
GIT_HEAD="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "")"
GIT_STATUS_OUT="$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || echo "__ERR__")"
GIT_OK=false
GIT_DIRTY=0
GIT_UNMERGED=0
if [[ "$GIT_STATUS_OUT" != "__ERR__" ]]; then
  if [[ -n "$GIT_STATUS_OUT" ]]; then
    GIT_DIRTY="$(printf '%s\n' "$GIT_STATUS_OUT" | wc -l | tr -d ' ')"
    GIT_UNMERGED="$(printf '%s\n' "$GIT_STATUS_OUT" | grep -Ec '^(UU|AA|DD|AU|UA|DU|UD)[[:space:]]' || true)"
  fi
  if [[ "$GIT_ORIGIN" =~ ${EXPECTED_ORIGIN_SLUG}(\.git)?$ && -n "$GIT_BRANCH" && "$GIT_UNMERGED" -eq 0 ]]; then
    GIT_OK=true
  fi
fi
record_gate "git_status_and_origin" "$GIT_OK" "branch=${GIT_BRANCH} head=${GIT_HEAD} origin=${GIT_ORIGIN} dirty=${GIT_DIRTY} unmerged=${GIT_UNMERGED}"

# 12. Paper Farm preflight readiness
PY_BIN=""
if [[ -x "${HOME}/noesis/.venv/bin/python" ]]; then
  PY_BIN="${HOME}/noesis/.venv/bin/python"
elif [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PY_BIN="${REPO_ROOT}/.venv/bin/python"
fi

PF_OK=false
PF_OUT="python_venv_missing"
PERF_MANIFEST="${DATA_ROOT}/manifests/paper_farm_live_performance_manifest.json"
PERF_OK=false
if [[ -n "$PY_BIN" ]]; then
  PF_OUT="$(PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${REPO_ROOT}/src" "$PY_BIN" -c "
from pathlib import Path
from tier0_sota import engine, risk
from tier0_sota.live import ALPACA_LANE_PROFILES, PAPER_ALPACA_ENDPOINT, IbkrPaperAdapter
ibkr = IbkrPaperAdapter(Path('${DATA_ROOT}/manifests'))
assert PAPER_ALPACA_ENDPOINT == 'https://paper-api.alpaca.markets'
print('PREFLIGHT_OK', list(ALPACA_LANE_PROFILES.keys()), ibkr.connection_spec['port'], risk.MAX_STOP_LOSS_PCT)
" 2>&1 || true)"
  if grep -q 'PREFLIGHT_OK' <<<"$PF_OUT"; then
    PF_OK=true
  fi
fi
if [[ -f "$PERF_MANIFEST" ]]; then
  if jq -e '.paper_only == true and .live_execution_enabled == false' "$PERF_MANIFEST" >/dev/null 2>&1; then
    PERF_OK=true
  fi
fi
if [[ "$PF_OK" == "true" && "$PERF_OK" == "true" ]]; then
  record_gate "paper_farm_preflight" "true" "preflight=true manifest_safe=true output=${PF_OUT}"
else
  record_gate "paper_farm_preflight" "false" "preflight=${PF_OK} manifest_safe=${PERF_OK} output=${PF_OUT}"
fi

# 12. OmniRoute local router (:20128)
OMNI_OK=false
OMNI_DETAIL="http://127.0.0.1:20128/api/health unreachable"
OMNI_BODY="$(curl -sS -m 3 http://127.0.0.1:20128/api/health 2>/dev/null || true)"
if jq -e '.status == "ok"' <<<"$OMNI_BODY" >/dev/null 2>&1; then
  OMNI_OK=true
  OMNI_DETAIL="health=ok url=http://127.0.0.1:20128/api/health"
fi
record_gate "omniroute_local_router" "$OMNI_OK" "$OMNI_DETAIL"

# 13. Native OAuth & unrestricted Claude models (WSL2 counterpart to .ps1 gate #13 orca_multi_account_readiness; 13 always-on gates)
CLAUDE_MODEL_UNRESTRICTED=false
if [[ -f "${HOME}/.claude/settings.json" ]] && jq -e 'has("availableModels") | not' "${HOME}/.claude/settings.json" >/dev/null 2>&1; then
  CLAUDE_MODEL_UNRESTRICTED=true
fi
LEAKED_ENV=0
for k in ANTHROPIC_API_KEY ANTHROPIC_BASE_URL OPENAI_API_KEY OPENAI_BASE_URL GEMINI_API_KEY; do
  if [[ -n "${!k:-}" ]]; then
    LEAKED_ENV=$((LEAKED_ENV + 1))
  fi
done
if [[ "$CLAUDE_MODEL_UNRESTRICTED" == "true" && "$LEAKED_ENV" -eq 0 ]]; then
  record_gate "claude_native_auth_and_models" "true" "model_unrestricted=true leaked_global_keys=0"
else
  record_gate "claude_native_auth_and_models" "false" "model_unrestricted=${CLAUDE_MODEL_UNRESTRICTED} leaked_global_keys=${LEAKED_ENV}"
fi

# Atomically write workstation_preflight_resume_manifest.json
FAIL_COUNT="$(jq 'length' <<<"$FAILURES_JSON")"
if [[ "$FAIL_COUNT" -eq 0 ]]; then
  VERDICT="PASS"
  PASSED_BOOL="true"
else
  VERDICT="FAIL"
  PASSED_BOOL="false"
fi

GENERATED_AT="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"
MANIFEST_CONTENT="$(jq -n \
  --arg schema_version "tier0.workstation-preflight-resume.v1" \
  --arg generated_at_utc "$GENERATED_AT" \
  --arg platform "wsl2" \
  --arg repo_root "$REPO_ROOT" \
  --arg data_root "$DATA_ROOT" \
  --arg manifest_path "$MANIFEST_PATH" \
  --argjson reconcile_requested "$RECONCILE" \
  --argjson clean_legacy_locked_requested false \
  --arg verdict "$VERDICT" \
  --argjson passed "$PASSED_BOOL" \
  --argjson gates_passed "$GATES_PASSED" \
  --argjson gates_total "$GATES_TOTAL" \
  --argjson portals_healthy "$PORTALS_HEALTHY" \
  --argjson portals_total 15 \
  --argjson portals "$PORTALS_JSON" \
  --argjson reconcile_actions "$ACTIONS_JSON" \
  --argjson failures "$FAILURES_JSON" \
  --argjson gates "$GATES_JSON" \
  '{
    schema_version: $schema_version,
    generated_at_utc: $generated_at_utc,
    platform: $platform,
    repo_root: $repo_root,
    data_root: $data_root,
    manifest_path: $manifest_path,
    reconcile_requested: $reconcile_requested,
    clean_legacy_locked_requested: $clean_legacy_locked_requested,
    verdict: $verdict,
    passed: $passed,
    live_execution_enabled: false,
    paper_only: true,
    gates_passed: $gates_passed,
    gates_total: $gates_total,
    ready_lanes: ["sim-backtest", "paper", "half-auto-tv", "live"],
    portals_healthy: $portals_healthy,
    portals_total: $portals_total,
    portals: $portals,
    reconcile_actions: $reconcile_actions,
    failures: $failures,
    gates: $gates
  }')"

mkdir -p "$(dirname "$MANIFEST_PATH")"
TMP_MANIFEST="$(mktemp "${MANIFEST_PATH}.tmp.XXXXXX")"
printf '%s\n' "$MANIFEST_CONTENT" >"$TMP_MANIFEST"
mv -f "$TMP_MANIFEST" "$MANIFEST_PATH"

if [[ "$JSON_OUT" == "true" ]]; then
  printf '%s\n' "$MANIFEST_CONTENT"
else
  printf '[VERDICT: %s] Workstation Preflight & Resume (%d/%d gates, %d/15 portals, lanes=sim-backtest,paper,half-auto-tv,live) -> %s\n' \
    "$VERDICT" "$GATES_PASSED" "$GATES_TOTAL" "$PORTALS_HEALTHY" "$MANIFEST_PATH"
fi

if [[ "$VERDICT" != "PASS" ]]; then
  exit 1
fi
exit 0

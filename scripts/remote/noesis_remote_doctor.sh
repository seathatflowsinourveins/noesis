#!/usr/bin/env bash
set -euo pipefail

# Read-only verification doctor for Noesis WSL2 dual-machine remote readiness (ADR-0034).
# Checks:
#   1. WSL2 sshd bound strictly to 127.0.0.1:2222
#   2. systemd user lingering (Linger=yes)
#   3. headless Linux orca-ide serve bound strictly to 127.0.0.1:6768 (refusing /usr/bin/orca)
#   4. tmux availability
#   5. non-interactive login-shell toolchain PATH (uv, mise, just, git, fnox, tmux, claude, codex, opencode, omp, pi, agy)
#   6. 15/15 baseline loopback portals HTTP 200/302
#   7. 0 live broker ports (4001/7496/8798/8796) and 0 CDP port (9223)
#   8. 0 wildcard 0.0.0.0 / [::] TCP listeners
#
# Strictly read-only: mutates nothing, prints zero secrets or private keys.

JSON_OUT=false

usage() {
  cat <<'EOF'
Usage: scripts/remote/noesis_remote_doctor.sh [--json]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUT=true
      shift
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

TARGET_USER="${NOESIS_REMOTE_USER:-${USER:-$(id -un)}}"
FORBIDDEN_LIVE_PORTS=(4001 7496 8798 8796 9223)
REQUIRED_TOOLS=(uv mise just git fnox tmux claude codex opencode omp pi agy)
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

CHECKS_PASSED=0
CHECKS_TOTAL=0
FAILURES=()
RESULTS_LINES=()

record_check() {
  local name="$1"
  local ok="$2"
  local detail="$3"
  CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
  if [[ "$ok" == "true" ]]; then
    CHECKS_PASSED=$((CHECKS_PASSED + 1))
  else
    FAILURES+=("${name}: ${detail}")
  fi
  RESULTS_LINES+=("${name}|${ok}|${detail}")
  if [[ "$JSON_OUT" != "true" ]]; then
    if [[ "$ok" == "true" ]]; then
      printf '[OK]   %s: %s\n' "$name" "$detail"
    else
      printf '[FAIL] %s: %s\n' "$name" "$detail"
    fi
  fi
}

SS_TCP="$(ss -lntH 2>/dev/null || true)"

# 1. WSL2 sshd listening strictly on 127.0.0.1:2222
SSHD_LOOPBACK=false
SSHD_NONLOOP=false
if grep -Eq '(^|[[:space:]])127\.0\.0\.1:2222[[:space:]]' <<<"$SS_TCP"; then
  SSHD_LOOPBACK=true
fi
if grep -Eq '(^|[[:space:]])(0\.0\.0\.0|\*|\[::\]):2222[[:space:]]' <<<"$SS_TCP"; then
  SSHD_NONLOOP=true
fi
if [[ "$SSHD_LOOPBACK" == "true" && "$SSHD_NONLOOP" == "false" ]]; then
  record_check "wsl_sshd_loopback_2222" "true" "listening=127.0.0.1:2222 wildcard=false"
else
  record_check "wsl_sshd_loopback_2222" "false" "loopback=${SSHD_LOOPBACK} wildcard=${SSHD_NONLOOP}"
fi

# 2. systemd user lingering (Linger=yes)
LINGER_VAL="$(loginctl show-user "$TARGET_USER" -p Linger --value 2>/dev/null || echo "unknown")"
if [[ "$LINGER_VAL" == "yes" ]]; then
  record_check "systemd_linger" "true" "user=${TARGET_USER} Linger=yes"
else
  record_check "systemd_linger" "false" "user=${TARGET_USER} Linger=${LINGER_VAL}"
fi

# 3. orca-ide serve listening strictly on 127.0.0.1:6768
ORCA_BIN_PATH=""
if [[ -x "${HOME}/.local/opt/orca-server/bin/orca-ide" ]]; then
  ORCA_BIN_PATH="${HOME}/.local/opt/orca-server/bin/orca-ide"
elif command -v orca-ide >/dev/null 2>&1; then
  ORCA_BIN_PATH="$(command -v orca-ide)"
fi
ORCA_GNOME_CONFLICT=false
if [[ -n "$ORCA_BIN_PATH" && "$(readlink -f "$ORCA_BIN_PATH" 2>/dev/null || true)" == "/usr/bin/orca" ]]; then
  ORCA_GNOME_CONFLICT=true
fi
ORCA_LOOPBACK=false
ORCA_NONLOOP=false
if grep -Eq '(^|[[:space:]])127\.0\.0\.1:6768[[:space:]]' <<<"$SS_TCP"; then
  ORCA_LOOPBACK=true
fi
if grep -Eq '(^|[[:space:]])(0\.0\.0\.0|\*|\[::\]):6768[[:space:]]' <<<"$SS_TCP"; then
  ORCA_NONLOOP=true
fi
if [[ "$ORCA_LOOPBACK" == "true" && "$ORCA_NONLOOP" == "false" && "$ORCA_GNOME_CONFLICT" == "false" ]]; then
  record_check "orca_serve_loopback_6768" "true" "listening=127.0.0.1:6768 binary=${ORCA_BIN_PATH:-detected}"
else
  record_check "orca_serve_loopback_6768" "false" "loopback=${ORCA_LOOPBACK} wildcard=${ORCA_NONLOOP} binary=${ORCA_BIN_PATH:-missing}"
fi

# 4. tmux availability
if command -v tmux >/dev/null 2>&1; then
  TMUX_VER="$(tmux -V 2>/dev/null || echo "tmux")"
  record_check "tmux_installed" "true" "${TMUX_VER}"
else
  record_check "tmux_installed" "false" "tmux not found on PATH"
fi

# 5. Non-interactive login-shell toolchain PATH
MISSING_TOOLS=()
for tool in "${REQUIRED_TOOLS[@]}"; do
  if ! bash -lc "command -v '${tool}' >/dev/null 2>&1"; then
    MISSING_TOOLS+=("$tool")
  fi
done
if [[ "${#MISSING_TOOLS[@]}" -eq 0 ]]; then
  record_check "login_shell_toolchain" "true" "found=${#REQUIRED_TOOLS[@]}/${#REQUIRED_TOOLS[@]}"
else
  record_check "login_shell_toolchain" "false" "missing=${MISSING_TOOLS[*]}"
fi

# 6. 15/15 baseline loopback portals
PORTALS_UP=0
PORTALS_DOWN=()
for spec in "${PORTAL_SPECS[@]}"; do
  pname="${spec%%|*}"
  purl="${spec#*|}"
  code="$(curl -sS -m 3 -o /dev/null -w '%{http_code}' "$purl" 2>/dev/null || echo "000")"
  if [[ "$code" =~ ^[23][0-9][0-9]$ ]]; then
    PORTALS_UP=$((PORTALS_UP + 1))
  else
    PORTALS_DOWN+=("${pname}(${code})")
  fi
done
if [[ "$PORTALS_UP" -eq "${#PORTAL_SPECS[@]}" ]]; then
  record_check "loopback_portals_15" "true" "healthy=${PORTALS_UP}/${#PORTAL_SPECS[@]}"
else
  record_check "loopback_portals_15" "false" "healthy=${PORTALS_UP}/${#PORTAL_SPECS[@]} down=${PORTALS_DOWN[*]}"
fi

# 7. Zero live broker ports (4001/7496/8798/8796) and zero CDP port (9223)
LIVE_HITS=()
for port in "${FORBIDDEN_LIVE_PORTS[@]}"; do
  if grep -Eq "[:.]${port}[[:space:]]" <<<"$SS_TCP"; then
    LIVE_HITS+=("$port")
  fi
done
if [[ "${#LIVE_HITS[@]}" -eq 0 ]]; then
  record_check "live_broker_and_cdp_ports_zero" "true" "listeners=0 forbidden=4001,7496,8798,8796,9223"
else
  record_check "live_broker_and_cdp_ports_zero" "false" "open_ports=${LIVE_HITS[*]}"
fi

# 8. Zero wildcard 0.0.0.0 / [::] TCP exposures
WILDCARD_HITS=()
while IFS= read -r local_addr; do
  [[ -z "$local_addr" ]] && continue
  case "$local_addr" in
    0.0.0.0:*|\*:*|\[::\]:*)
      WILDCARD_HITS+=("$local_addr")
      ;;
  esac
done < <(awk '{print $4}' <<<"$SS_TCP")

if [[ "${#WILDCARD_HITS[@]}" -eq 0 ]]; then
  record_check "zero_wildcard_tcp_exposures" "true" "wildcard_listeners=0"
else
  record_check "zero_wildcard_tcp_exposures" "false" "wildcard_listeners=${WILDCARD_HITS[*]}"
fi

VERDICT="PASS"
if [[ "${#FAILURES[@]}" -gt 0 ]]; then
  VERDICT="FAIL"
fi

if [[ "$JSON_OUT" == "true" ]]; then
  python3 - "$VERDICT" "$CHECKS_PASSED" "$CHECKS_TOTAL" "$TARGET_USER" "${RESULTS_LINES[@]}" <<'PY'
import json
import sys

verdict, passed, total, user = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
checks = {}
failures = []
for row in sys.argv[5:]:
    name, ok_str, detail = row.split("|", 2)
    ok = ok_str == "true"
    checks[name] = {"ok": ok, "detail": detail}
    if not ok:
        failures.append(f"{name}: {detail}")
print(
    json.dumps(
        {
            "schema_version": "tier0.noesis-remote-doctor.v1",
            "user": user,
            "verdict": verdict,
            "passed": verdict == "PASS",
            "checks_passed": passed,
            "checks_total": total,
            "failures": failures,
            "checks": checks,
        },
        indent=2,
    )
)
PY
else
  printf '[VERDICT: %s] Noesis Remote Doctor (%d/%d checks passed)\n' "$VERDICT" "$CHECKS_PASSED" "$CHECKS_TOTAL"
fi

if [[ "$VERDICT" != "PASS" ]]; then
  exit 1
fi
exit 0

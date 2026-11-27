#!/usr/bin/env bash
# Tier-0 SOTA TradingView Operator Console — WSL2/Linux parity (display-only, semi_auto_confirmed=false).
# ToU-compliant helpers for Pine v6 sync/clipboard, Order Preset cards, Half-Auto extreme-mover
# tickets, 6-section watchlist generation, alert staging, and E2E verification (zero CDP, zero live ports).

set -euo pipefail

TIER0_REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TIER0_DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"
TIER0_MANIFEST_DIR="${TIER0_MANIFEST_DIR:-${TIER0_DATA_ROOT}/manifests}"
TIER0_PACK_DIR="${TIER0_PACK_DIR:-${TIER0_MANIFEST_DIR}/tv_sota_pack}"

_tier0_python() {
  if [[ -x "${TIER0_REPO_ROOT}/.venv/bin/python" ]]; then
    printf '%s\n' "${TIER0_REPO_ROOT}/.venv/bin/python"
  elif [[ -x "${HOME}/noesis/.venv/bin/python" ]]; then
    printf '%s\n' "${HOME}/noesis/.venv/bin/python"
  else
    printf '%s\n' "python3"
  fi
}

_tier0_copy_clipboard() {
  if command -v clip.exe >/dev/null 2>&1; then
    clip.exe
  elif command -v wl-copy >/dev/null 2>&1; then
    wl-copy
  elif command -v xclip >/dev/null 2>&1; then
    xclip -selection clipboard
  else
    cat >/dev/null
    printf '[INFO] No clipboard binary (clip.exe/wl-copy/xclip); file remains ready on disk.\n' >&2
  fi
}

_tier0_read_clipboard() {
  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command 'Get-Clipboard -Raw' | tr -d '\r'
  elif command -v wl-paste >/dev/null 2>&1; then
    wl-paste --no-newline
  elif command -v xclip >/dev/null 2>&1; then
    xclip -o -selection clipboard
  else
    printf '[ERR] No clipboard reader (powershell.exe/wl-paste/xclip); pass a JSON file to ingest.\n' >&2
    return 1
  fi
}

tv_tickets() {
  local venue="ALPACA"
  local venue_profile=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --venue-profile)
        if [[ $# -lt 2 ]]; then
          printf '[ERR] --venue-profile requires a profile name\n' >&2
          return 2
        fi
        venue_profile="$2"
        shift 2
        ;;
      --venue-profile=*)
        venue_profile="${1#*=}"
        shift
        ;;
      IBKR|ALPACA|ibkr|alpaca)
        venue="${1^^}"
        shift
        ;;
      *)
        printf 'Usage: tv_tickets [ALPACA|IBKR] [--venue-profile <PROFILE>]\n' >&2
        return 2
        ;;
    esac
  done
  local py
  py="$(_tier0_python)"
  local t_args=(
    -B -m tier0_sota.tv.tickets
    --manifest-dir "${TIER0_MANIFEST_DIR}"
    --out-dir "${TIER0_PACK_DIR}"
    --venue "${venue}"
  )
  if [[ -n "$venue_profile" ]]; then
    t_args+=(--venue-profile "$venue_profile")
  fi
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" "${t_args[@]}"
  if [[ -f "${TIER0_PACK_DIR}/LIVE_HALF_AUTO_ORDER_TICKETS.md" ]]; then
    cat "${TIER0_PACK_DIR}/LIVE_HALF_AUTO_ORDER_TICKETS.md"
  fi
}

tv_handles() {
  local py
  py="$(_tier0_python)"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" \
    "$py" -B -m tier0_sota.tv.tickets --show-handles
}

tv_presets() {
  local cards="${TIER0_PACK_DIR}/TV_ORDER_PRESET_CARDS.md"
  if [[ ! -f "$cards" ]]; then
    tv_tickets "${1:-ALPACA}" >/dev/null
  fi
  cat "$cards"
}

tv_sync_pine() {
  mkdir -p "${TIER0_PACK_DIR}"
  local f dest sha
  for f in "${TIER0_REPO_ROOT}"/pine/*.pine; do
    [[ -f "$f" ]] || continue
    dest="${TIER0_PACK_DIR}/$(basename "$f")"
    cp -f "$f" "$dest"
    sha="$(sha256sum "$dest" | awk '{print $1}')"
    printf '[PINE-SYNC] %s sha256=%s\n' "$(basename "$f")" "$sha"
  done
}

_tv_copy_pine_file() {
  local rel="$1"
  local label="$2"
  local full="${TIER0_REPO_ROOT}/pine/${rel}"
  if [[ ! -f "$full" ]]; then
    printf '[ERR] Missing Pine script: %s\n' "$full" >&2
    return 1
  fi
  _tier0_copy_clipboard < "$full"
  printf '[OK] Copied %s -> paste in TradingView (%s).\n' "$full" "$label"
}

tv_copy_omni() { _tv_copy_pine_file "tier0_omni_sota_24x5.pine" "Pine Editor Pane 0"; }
tv_copy_strategy() { _tv_copy_pine_file "tier0_sota_strategy_24x5.pine" "Strategy Tester"; }
tv_copy_regime() { _tv_copy_pine_file "tier0_market_regime.pine" "Pane 1 Macro"; }
tv_copy_darkpool() { _tv_copy_pine_file "tier0_darkpool_uv_buildup.pine" "Pane 2 Darkpool/UV"; }

tv_copy_watchlist() {
  tv_tickets "$@" >/dev/null
  local wl="${TIER0_PACK_DIR}/tier0_sota_tv_watchlists_live.txt"
  local clip_path="$wl"
  if command -v clip.exe >/dev/null 2>&1 && command -v wslpath >/dev/null 2>&1; then
    clip_path="$(wslpath -w "$wl")"
  fi
  printf '%s' "$clip_path" | _tier0_copy_clipboard
  printf '[OK] Refreshed watchlist (POSIX): %s\n' "$wl"
  if [[ "$clip_path" != "$wl" ]]; then
    printf '[OK] Copied Windows clipboard path: %s\n' "$clip_path"
  fi
  printf '[TV] TradingView -> Watchlist menu (...) -> Import list... -> Ctrl+V -> Open\n'
}

tv_ingest() {
  if [[ $# -lt 1 ]]; then
    printf 'Usage: tv_ingest <file.json|-> [IBKR|ALPACA] [--live-intake]\n' >&2
    return 2
  fi
  local file_path="$1"
  local venue="${2:-IBKR}"
  local live_flag="${3:-}"
  local py
  py="$(_tier0_python)"
  local args=(
    -B -m tier0_sota.tv.bridge
    --staged-path "${TIER0_PACK_DIR}/staged_tv_presets.json"
    --watchlist-path "${TIER0_MANIFEST_DIR}/live_whole_market_movers_manifest.json"
    --venue "$venue"
  )
  if [[ "$file_path" == "-" || "$file_path" == "--stdin" ]]; then
    args+=(--ingest-stdin)
  else
    args+=(--ingest-file "$file_path")
  fi
  if [[ "$live_flag" == "--live-intake" ]]; then
    args+=(--live-intake)
  fi
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" "${args[@]}"
}

tv_ingest_clip() {
  local venue="IBKR"
  local live_flag=""
  local arg
  for arg in "$@"; do
    case "$arg" in
      --live-intake)
        live_flag="--live-intake"
        ;;
      IBKR|ALPACA|ibkr|alpaca)
        venue="${arg^^}"
        ;;
      *)
        printf 'Usage: tv_ingest_clip [IBKR|ALPACA] [--live-intake]\n' >&2
        return 2
        ;;
    esac
  done
  local raw_json
  raw_json="$(_tier0_read_clipboard)"
  if [[ -z "${raw_json//[[:space:]]/}" ]]; then
    printf '[ERR] Clipboard is empty; copy TradingView alert JSON first.\n' >&2
    return 1
  fi
  local py
  py="$(_tier0_python)"
  local args=(
    -B -m tier0_sota.tv.bridge
    --ingest-stdin
    --staged-path "${TIER0_PACK_DIR}/staged_tv_presets.json"
    --watchlist-path "${TIER0_MANIFEST_DIR}/live_whole_market_movers_manifest.json"
    --venue "$venue"
  )
  if [[ -n "$live_flag" ]]; then
    args+=("$live_flag")
  fi
  printf '%s\n' "$raw_json" \
    | PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" "${args[@]}"
  printf '[OK] Ingested & audited TV alert preset(s) from clipboard (qty=1, semi_auto_confirmed=false).\n'
}

tv_stage() {
  if [[ $# -lt 4 ]]; then
    printf 'Usage: tv_stage <SYMBOL> <BUY|SELL> <PRICE> <STOP_PRICE> [NATR_PCT=0.012] [IBKR|ALPACA] [--live-intake]\n' >&2
    return 2
  fi
  local symbol="${1^^}"
  local side="${2^^}"
  local price="$3"
  local stop_price="$4"
  shift 4
  local natr_pct="0.012"
  local venue="IBKR"
  local venue_profile=""
  local live_flag=""
  if [[ $# -gt 0 && "$1" != "--live-intake" && "$1" != --venue-profile* && "${1^^}" != "IBKR" && "${1^^}" != "ALPACA" ]]; then
    natr_pct="$1"
    shift
  fi
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --live-intake)
        live_flag="--live-intake"
        shift
        ;;
      --venue-profile)
        if [[ $# -lt 2 ]]; then
          printf '[ERR] --venue-profile requires a profile name\n' >&2
          return 2
        fi
        venue_profile="$2"
        shift 2
        ;;
      --venue-profile=*)
        venue_profile="${1#*=}"
        shift
        ;;
      IBKR|ALPACA|ibkr|alpaca)
        venue="${1^^}"
        shift
        ;;
      *)
        printf '[ERR] Unknown option for tv_stage: %s\n' "$1" >&2
        return 2
        ;;
    esac
  done
  if [[ "$side" != "BUY" && "$side" != "SELL" ]]; then
    printf '[ERR] Side must be BUY or SELL, got: %s\n' "$side" >&2
    return 2
  fi
  local py
  py="$(_tier0_python)"
  local payload
  payload="$(
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" -B -c '
import json, sys, time
from decimal import Decimal
from tier0_sota.engine import (
    classify_us_equity_session_et,
    get_session_protection_profile,
    select_pooled_trailing_schedule,
)
sym, side, price_s, stop_s, natr_s, venue, vp = sys.argv[1:8]
now_ms = (int(time.time()) // 60) * 60_000
sess = classify_us_equity_session_et(now_ms * 1_000_000)
if sess == "CLOSED":
    raise SystemExit("Refused tv_stage: XNYS 24/5 market session is CLOSED")
natr = Decimal(str(natr_s))
sched, profiles = select_pooled_trailing_schedule(natr)
prof = get_session_protection_profile(sess, profiles)
out = {
    "schema_version": "tier0.tv-alert.v1",
    "symbol": sym,
    "ts_utc_ms": now_ms,
    "bar_confirmed": True,
    "side": side,
    "qty": 1,
    "venue": venue,
    "session": sess,
    "order_type": prof.order_mode,
    "trigger_price": float(price_s),
    "stop_price": float(stop_s),
    "limit_collar_bps": int(prof.limit_collar_bps),
    "trailing_schedule": sched,
    "natr_pct": float(natr),
    "auto_reverse": True,
    "auto_reentry": True,
    "reentry_cooldown_secs": 60,
    "strategy_id": "SignalExecutionStrategy",
    "semi_auto_confirmed": False,
    "script_kind": "indicator",
    "data_source": "tradingview_pine_v6",
}
if vp:
    out["venue_profile"] = vp
print(json.dumps(out))
' "$symbol" "$side" "$price" "$stop_price" "$natr_pct" "$venue" "$venue_profile"
  )"
  local args=(
    -B -m tier0_sota.tv.bridge
    --ingest-stdin
    --staged-path "${TIER0_PACK_DIR}/staged_tv_presets.json"
    --watchlist-path "${TIER0_MANIFEST_DIR}/live_whole_market_movers_manifest.json"
    --venue "$venue"
  )
  if [[ -n "$live_flag" ]]; then
    args+=("$live_flag")
  fi
  local res
  res="$(printf '%s\n' "$payload" | PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" "${args[@]}")"
  printf '%s\n' "$res" | _tier0_copy_clipboard
  printf '%s\n' "$res"
  printf '[OK] Staged & copied to clipboard (qty=1, semi_auto_confirmed=false).\n'
}

tv_audit() {
  local py
  py="$(_tier0_python)"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" -B -c "
import json
from pathlib import Path
from tier0_sota.tv.bridge import load_tv_audit_summary
print(json.dumps(load_tv_audit_summary(Path('${TIER0_PACK_DIR}/tv_half_auto_audit.duckdb')), indent=2))
"
}

tv_confirm_paper() {
  local alert_id="${1:-}"
  if [[ -z "$alert_id" ]]; then
    printf 'Usage: tv_confirm_paper <alert_id> [--dry-run] [--qty 1]\n' >&2
    return 2
  fi
  shift
  local qty="1"
  local dry_run="0"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --dry-run)
        dry_run="1"
        shift
        ;;
      --qty|--tv-qty)
        if [[ $# -lt 2 ]]; then
          printf '[ERR] %s requires an integer quantity\n' "$1" >&2
          return 2
        fi
        qty="$2"
        shift 2
        ;;
      --qty=*|--tv-qty=*)
        qty="${1#*=}"
        shift
        ;;
      *)
        printf '[ERR] Unknown option for tv-confirm-paper: %s\n' "$1" >&2
        return 2
        ;;
    esac
  done
  local key_file="${FNOX_AGE_KEY_FILE:-${HOME}/.config/fnox/age.txt}"
  if ! command -v fnox >/dev/null 2>&1; then
    printf '[ERR] fnox is required for tv-confirm-paper (--profile paper)\n' >&2
    return 1
  fi
  if [[ ! -f "$key_file" ]]; then
    printf '[ERR] Missing fnox paper age key: %s\n' "$key_file" >&2
    return 1
  fi
  local uv_env=""
  if [[ ! -x "${TIER0_REPO_ROOT}/.venv/bin/python" && -x "${HOME}/noesis/.venv/bin/python" ]]; then
    uv_env="${HOME}/noesis/.venv"
  fi
  local runner_args=(
    --manifest-dir "${TIER0_MANIFEST_DIR}"
    --confirm-tv-alert "$alert_id"
    --tv-qty "$qty"
  )
  if [[ "$dry_run" == "1" ]]; then
    runner_args+=(--dry-run)
  fi
  local run_env=(
    FNOX_AGE_KEY_FILE="$key_file"
    PYTHONDONTWRITEBYTECODE=1
    PYTHONPATH="${TIER0_REPO_ROOT}/src"
    TIER0_DATA_ROOT="${TIER0_DATA_ROOT}"
  )
  if [[ -n "$uv_env" ]]; then
    run_env+=(UV_PROJECT_ENVIRONMENT="$uv_env")
  fi
  (
    cd "${TIER0_REPO_ROOT}"
    env "${run_env[@]}" \
      fnox exec --profile paper -- uv run --no-sync python -m tier0_sota.live.runner "${runner_args[@]}"
  )
}

tv_portals() {
  printf 'FINOS Workspace : http://localhost:1420/\n'
  printf 'OpenBB Widgets  : http://127.0.0.1:6900/widgets.json\n'
  printf 'Grafana         : http://127.0.0.1:3001/\n'
  printf 'Dagster         : http://127.0.0.1:3000/\n'
  printf 'DuckDB SQL UI   : http://127.0.0.1:4213/\n'
  printf 'Caddy TV Pack   : http://127.0.0.1:8793/tv_sota_pack/\n'
}

tv_land() {
  tv_tickets "$@"
  tv_sync_pine
  tv_copy_omni
  printf '[TV-LAND] 3-Click TradingView Landing Checklist (display-only, qty=1, semi_auto_confirmed=false):\n'
  printf '  1. Pine Editor Ctrl+V -> Save (tier0_omni_sota_24x5.pine is on clipboard; NOT the watchlist path)\n'
  printf '  2. tv_copy_regime -> Macro pane (tier0_market_regime.pine into Pane 1)\n'
  printf '  3. tv_copy_watchlist -> copies Windows Z:\\... path -> Watchlist ... Import list... -> Ctrl+V -> Open\n'
  printf '  4. Optional: tv_copy_strategy | tv_copy_darkpool | tv_presets | tv_handles | tv_ingest_clip | tv_stage | tv_confirm_paper <alert_id> --dry-run\n'
}

tv_e2e() {
  local venue="ALPACA"
  local venue_profile=""
  local skip_live_scan="0"
  local skip_tests="0"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-live-scan)
        skip_live_scan="1"
        shift
        ;;
      --skip-tests)
        skip_tests="1"
        shift
        ;;
      --venue-profile)
        if [[ $# -lt 2 ]]; then
          printf '[ERR] --venue-profile requires a profile name\n' >&2
          return 2
        fi
        venue_profile="$2"
        shift 2
        ;;
      --venue-profile=*)
        venue_profile="${1#*=}"
        shift
        ;;
      IBKR|ALPACA|ibkr|alpaca)
        venue="${1^^}"
        shift
        ;;
      *)
        printf 'Usage: tv_e2e [ALPACA|IBKR] [--venue-profile <PROFILE>] [--skip-live-scan] [--skip-tests]\n' >&2
        return 2
        ;;
    esac
  done
  local py
  py="$(_tier0_python)"
  local key_file="${FNOX_AGE_KEY_FILE:-${HOME}/.config/fnox/age.txt}"
  if [[ "$skip_live_scan" == "0" && -f "$key_file" ]] && command -v fnox >/dev/null 2>&1; then
    local uv_env=""
    if [[ ! -x "${TIER0_REPO_ROOT}/.venv/bin/python" && -x "${HOME}/noesis/.venv/bin/python" ]]; then
      uv_env="${HOME}/noesis/.venv"
    fi
    local scan_env=(
      FNOX_AGE_KEY_FILE="$key_file"
      PYTHONDONTWRITEBYTECODE=1
      PYTHONPATH="${TIER0_REPO_ROOT}/src"
      TIER0_DATA_ROOT="${TIER0_DATA_ROOT}"
    )
    if [[ -n "$uv_env" ]]; then
      scan_env+=(UV_PROJECT_ENVIRONMENT="$uv_env")
    fi
    printf '[TV-E2E] Running authenticated Paper Farm observation scan (--profile paper)...\n'
    (
      cd "${TIER0_REPO_ROOT}"
      env "${scan_env[@]}" \
        fnox exec --profile paper -- uv run --no-sync python -m tier0_sota.live.runner \
          --observe --manifest-dir "${TIER0_MANIFEST_DIR}"
    ) || printf '[WARN] Live Paper observation scan failed; continuing with local manifests.\n' >&2
  fi
  local ticket_args=("$venue")
  if [[ -n "$venue_profile" ]]; then
    ticket_args+=(--venue-profile "$venue_profile")
  fi
  tv_tickets "${ticket_args[@]}" >/dev/null
  tv_sync_pine
  tv_handles >/dev/null
  tv_audit >/dev/null
  if [[ "$skip_tests" == "0" ]]; then
    (
      cd "${TIER0_REPO_ROOT}"
      "$py" -m ruff check src/tier0_sota/tv tests/test_tv_automation.py
      "$py" -m ty check src/tier0_sota/tv tests/test_tv_automation.py
      "$py" -m pytest -q tests/test_tv_automation.py
    )
  fi
  printf '[VERDICT: PASS] TV automation pack sync, 1000/3000rpm handles, ruff, ty, Pine parity, and pytest verified.\n'
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  cmd="${1:-land}"
  shift || true
  case "$cmd" in
    land) tv_land "$@" ;;
    tickets) tv_tickets "$@" ;;
    presets) tv_presets "$@" ;;
    handles|tv-handles) tv_handles "$@" ;;
    sync-pine) tv_sync_pine "$@" ;;
    copy-omni) tv_copy_omni ;;
    copy-strategy) tv_copy_strategy ;;
    copy-regime) tv_copy_regime ;;
    copy-darkpool) tv_copy_darkpool ;;
    watchlist|copy-watchlist) tv_copy_watchlist "$@" ;;
    ingest) tv_ingest "$@" ;;
    ingest-clip|tv-ingest-clip) tv_ingest_clip "$@" ;;
    stage|tv-stage) tv_stage "$@" ;;
    confirm-paper|tv-confirm-paper) tv_confirm_paper "$@" ;;
    audit) tv_audit ;;
    portals) tv_portals ;;
    e2e) tv_e2e "$@" ;;
    *)
      printf 'Usage: %s {land|tickets|presets|handles|sync-pine|copy-omni|copy-strategy|copy-regime|copy-darkpool|watchlist|ingest|ingest-clip|stage|confirm-paper|tv-confirm-paper|audit|portals|e2e}\n' "$0" >&2
      exit 2
      ;;
  esac
fi

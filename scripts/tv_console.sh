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

tv_tickets() {
  local venue="${1:-ALPACA}"
  local py
  py="$(_tier0_python)"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" \
    "$py" -B -m tier0_sota.tv.tickets \
      --manifest-dir "${TIER0_MANIFEST_DIR}" \
      --out-dir "${TIER0_PACK_DIR}" \
      --venue "${venue}"
  if [[ -f "${TIER0_PACK_DIR}/LIVE_HALF_AUTO_ORDER_TICKETS.md" ]]; then
    cat "${TIER0_PACK_DIR}/LIVE_HALF_AUTO_ORDER_TICKETS.md"
  fi
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
  local venue="${1:-ALPACA}"
  tv_tickets "$venue" >/dev/null
  local wl="${TIER0_PACK_DIR}/tier0_sota_tv_watchlists_live.txt"
  printf '%s' "$wl" | _tier0_copy_clipboard
  printf '[OK] Refreshed watchlist: %s\n' "$wl"
  printf '[TV] TradingView -> Watchlist menu (...) -> Import list... -> Open\n'
}

tv_ingest() {
  local file_path="$1"
  local venue="${2:-IBKR}"
  local live_flag="${3:-}"
  local py
  py="$(_tier0_python)"
  local args=(
    -B -m tier0_sota.tv.bridge
    --ingest-file "$file_path"
    --staged-path "${TIER0_PACK_DIR}/staged_tv_presets.json"
    --watchlist-path "${TIER0_MANIFEST_DIR}/live_whole_market_movers_manifest.json"
    --venue "$venue"
  )
  if [[ "$live_flag" == "--live-intake" ]]; then
    args+=(--live-intake)
  fi
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${TIER0_REPO_ROOT}/src" "$py" "${args[@]}"
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
  (
    cd "${TIER0_REPO_ROOT}"
    if [[ -n "$uv_env" ]]; then
      export UV_PROJECT_ENVIRONMENT="$uv_env"
    fi
    FNOX_AGE_KEY_FILE="$key_file" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="${TIER0_REPO_ROOT}/src" \
    TIER0_DATA_ROOT="${TIER0_DATA_ROOT}" \
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
  local venue="${1:-ALPACA}"
  tv_tickets "$venue"
  tv_sync_pine
  tv_copy_omni
  printf '[TV-LAND] 3-Click TradingView Landing Checklist (display-only, qty=1, semi_auto_confirmed=false):\n'
  printf '  1. Pine Editor Ctrl+V -> Save (tier0_omni_sota_24x5.pine copied)\n'
  printf '  2. tv_copy_regime -> Macro pane (tier0_market_regime.pine into Pane 1)\n'
  printf '  3. tv_copy_watchlist -> Watchlist ... Import list... -> Open\n'
  printf '  4. Optional: tv_copy_strategy | tv_copy_darkpool | tv_presets | tv_confirm_paper <alert_id> --dry-run\n'
}

tv_e2e() {
  local py
  py="$(_tier0_python)"
  (
    cd "${TIER0_REPO_ROOT}"
    "$py" -m ruff check src/tier0_sota/tv tests/test_tv_automation.py
    "$py" -m ty check src/tier0_sota/tv tests/test_tv_automation.py
    "$py" -m pytest -q tests/test_tv_automation.py
  )
  printf '[VERDICT: PASS] TV automation ruff, ty, Pine parity, and pytest verified.\n'
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  cmd="${1:-land}"
  shift || true
  case "$cmd" in
    land) tv_land "$@" ;;
    tickets) tv_tickets "$@" ;;
    presets) tv_presets "$@" ;;
    sync-pine) tv_sync_pine "$@" ;;
    copy-omni) tv_copy_omni ;;
    copy-strategy) tv_copy_strategy ;;
    copy-regime) tv_copy_regime ;;
    copy-darkpool) tv_copy_darkpool ;;
    watchlist|copy-watchlist) tv_copy_watchlist "$@" ;;
    ingest) tv_ingest "$@" ;;
    confirm-paper|tv-confirm-paper) tv_confirm_paper "$@" ;;
    audit) tv_audit ;;
    portals) tv_portals ;;
    e2e) tv_e2e ;;
    *)
      printf 'Usage: %s {land|tickets|presets|sync-pine|copy-omni|copy-strategy|copy-regime|copy-darkpool|watchlist|ingest|confirm-paper|tv-confirm-paper|audit|portals|e2e}\n' "$0" >&2
      exit 2
      ;;
  esac
fi

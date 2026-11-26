#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:-${TIER0_REPO_ROOT:-/mnt/z/tier0-sota-clean}}"
DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"
WSL_DISTRO="${TIER0_WSL_DISTRO:-Noesis-SOTA}"
DRY_RUN="${DRY_RUN:-0}"
export PATH="$HOME/.local/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1

for port in 4001 7496 8798 8796; do
  if ss -lnt 2>/dev/null | grep -q ":${port} "; then
    echo "FAIL-CLOSED: forbidden live broker port ${port} is listening" >&2
    exit 2
  fi
done

mkdir -p "$HOME/.config/tier0"
chmod 700 "$HOME/.config/tier0"

create_placeholder_env() {
  local target="$1"
  local kind="$2"
  if [[ -f "$target" ]]; then
    chmod 600 "$target"
    return 0
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] Would create placeholder $target ($kind)"
    return 0
  fi
  if [[ "$kind" == "data" ]]; then
    cat >"$target" <<EOF
# Owner-managed Tier-0 data env placeholders (fill offline; chmod 600)
TIER0_MARKET_DATA_ROOT=${DATA_ROOT}/prewall-lake
FRED_API_KEY=
ALPACA_PA1_API_KEY=
ALPACA_PA1_SECRET=
EOF
  else
    cat >"$target" <<'EOF'
# Owner-managed Tier-0 Paper-Only broker env placeholders (chmod 600)
ALPACA_PAPER=true
ALPACA_PA1_API_KEY=
ALPACA_PA1_SECRET=
ALPACA_PA2_API_KEY=
ALPACA_PA2_SECRET=
ALPACA_PA3_API_KEY=
ALPACA_PA3_SECRET=
IBKR_HOST=127.0.0.1
IBKR_PORT=4002
EOF
  fi
  chmod 600 "$target"
}

create_placeholder_env "$HOME/.config/tier0/.env.data" "data"
create_placeholder_env "$HOME/.config/tier0/.env.paper" "paper"

LANE_ENV="$HOME/.config/tier0/with_lane_env.sh"
if [[ "$DRY_RUN" != "1" ]]; then
  cat >"$LANE_ENV" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ENV_FILE="$1"
shift
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi
exec "$@"
EOF
  chmod 700 "$LANE_ENV"
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required inside WSL2 ${WSL_DISTRO}." >&2
  exit 1
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo "[DRY-RUN] Would sync uv Python 3.13 .venv in $REPO_ROOT"
  exit 0
fi

cd "$REPO_ROOT"
uv python install 3.13 >/dev/null 2>&1 || true
bash "$REPO_ROOT/scripts/bootstrap_env.sh"
echo "[WSL-BOOTSTRAP-OK] repo=$REPO_ROOT env_dir=$HOME/.config/tier0"

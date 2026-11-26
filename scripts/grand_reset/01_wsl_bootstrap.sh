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

WSL_VENV="${UV_PROJECT_ENVIRONMENT:-$HOME/tier0-sota-clean/.venv}"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "[DRY-RUN] Would sync uv Python 3.13 venv at $WSL_VENV from $REPO_ROOT"
  exit 0
fi

mkdir -p "$(dirname "$WSL_VENV")"
export UV_PROJECT_ENVIRONMENT="$WSL_VENV"
export UV_LINK_MODE="${UV_LINK_MODE:-copy}"

cd "$REPO_ROOT"
uv python install 3.13 >/dev/null 2>&1 || true
SYNC_ARGS=(sync --no-progress)
if [[ -f "uv.lock" ]]; then
  SYNC_ARGS+=(--frozen)
fi
uv "${SYNC_ARGS[@]}"

PY="$WSL_VENV/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "Expected WSL virtualenv interpreter missing at $PY" >&2
  exit 1
fi

"$PY" -c "import sys, nautilus_trader, polars, duckdb, z3; print(f'python={sys.version.split()[0]} nautilus={nautilus_trader.__version__} polars={polars.__version__}')"

STATE_ROOT="${TIER0_STATE_ROOT:-$HOME/tier0-sota-state}"
mkdir -p "$STATE_ROOT"/{mlflow/mlartifacts,xerj-data,dagster_home,inspect-logs,mlflow-assistant}

"$PY" - "$WSL_VENV" "$REPO_ROOT" "$DATA_ROOT" "$STATE_ROOT" <<'PYEOF'
import os
import re
import sys
from pathlib import Path

wsl_venv, repo_root, data_root, state_root = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
venv_bin = f"{wsl_venv.rstrip('/')}/bin"
home = Path.home()

start_marker = "# >>> tier0-sota-clean env >>>"
end_marker = "# <<< tier0-sota-clean env <<<"
block = (
    f"{start_marker}\n"
    f'export TIER0_REPO_ROOT="{repo_root}"\n'
    f'export TIER0_DATA_ROOT="{data_root}"\n'
    f'export TIER0_STATE_ROOT="{state_root}"\n'
    f'export PATH="$HOME/tier0-sota-clean/.venv/bin:$PATH"\n'
    f"{end_marker}\n"
)

legacy_venv_re = re.compile(
    r"(?:\$HOME|/home/[^/\s\"']+)/" + ("tier0" + "-quant") + r"/\.venv/bin"
)
block_re = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker) + r"\n?", re.S)

for name in (".bashrc", ".profile"):
    rc_path = home / name
    text = rc_path.read_text(encoding="utf-8") if rc_path.is_file() else ""
    text = legacy_venv_re.sub(venv_bin, text)
    if block_re.search(text):
        text = block_re.sub(block, text)
    else:
        if text and not text.endswith("\n"):
            text += "\n"
        text += block
    tmp_path = rc_path.with_name(f"{rc_path.name}.tmp")
    tmp_path.write_text(text, encoding="utf-8")
    os.replace(tmp_path, rc_path)
PYEOF

echo "[WSL-BOOTSTRAP-OK] repo=$REPO_ROOT venv=$WSL_VENV env_dir=$HOME/.config/tier0"

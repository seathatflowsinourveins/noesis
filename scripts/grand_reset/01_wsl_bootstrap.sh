#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:-${TIER0_REPO_ROOT:-/mnt/z/noesis}}"
DATA_ROOT="${TIER0_DATA_ROOT:-/mnt/z/sota-data}"
WSL_DISTRO="${TIER0_WSL_DISTRO:-Noesis-SOTA}"
DRY_RUN="${DRY_RUN:-0}"
export PATH="$HOME/.local/share/mise/shims:$HOME/.local/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1

for port in 4001 7496 8798 8796; do
  if ss -lnt 2>/dev/null | grep -q ":${port} "; then
    echo "FAIL-CLOSED: forbidden live broker port ${port} is listening" >&2
    exit 2
  fi
done

if [[ "$DRY_RUN" == "1" ]]; then
  echo "[DRY-RUN] Would ensure $HOME/.config/fnox (chmod 700), retire legacy lane env wrapper, sync zero-copy WSL2 ~/.gemini/config/skills.json, and sync uv Python 3.13 venv at ${UV_PROJECT_ENVIRONMENT:-$HOME/noesis/.venv} from $REPO_ROOT"
  exit 0
fi

mkdir -p "$HOME/.config/fnox"
chmod 700 "$HOME/.config/fnox"

# Retire legacy custom shell wrapper (replaced by upstream jdx/fnox + age profiles per ADR-0023)
rm -f "$HOME/.config/tier0/with_lane_env.sh"

# Idempotently ensure WSL2 AGY zero-copy Disk-Z skills pointer (7-skill global Governance Spine; workspace .agents/skills.json loads 46/46 inside noesis)
mkdir -p "$HOME/.gemini/config"
SKILLS_JSON="$HOME/.gemini/config/skills.json"
SKILLS_TMP="$(mktemp "${SKILLS_JSON}.tmp.XXXXXX")"
printf '{"entries":[{"path":"%s/.claude/skills","include_only":["orca-cli","orca-per-workspace-env","orchestration","tier0-code-intel-and-gates","tier0-quant-falsification","tier0-upstream-convergence","verification-before-completion"]}]}\n' "${REPO_ROOT%/}" >"$SKILLS_TMP"
mv -f "$SKILLS_TMP" "$SKILLS_JSON"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required inside WSL2 ${WSL_DISTRO}." >&2
  exit 1
fi

WSL_VENV="${UV_PROJECT_ENVIRONMENT:-$HOME/noesis/.venv}"

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
mkdir -p \
  "$STATE_ROOT"/{mlflow/mlartifacts,xerj-data,dagster_home,inspect-logs,mlflow-assistant,zensical-site,zensical-cache} \
  "$HOME/.cache/noesis"/{ruff,pytest,hypothesis}

if command -v codebase-memory-mcp >/dev/null 2>&1; then
  codebase-memory-mcp config set ui_enabled false >/dev/null 2>&1 || true
fi

"$PY" - "$WSL_VENV" "$REPO_ROOT" "$DATA_ROOT" "$STATE_ROOT" <<'PYEOF'
import os
import re
import sys
from pathlib import Path

wsl_venv, repo_root, data_root, state_root = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
venv_bin = f"{wsl_venv.rstrip('/')}/bin"
home = Path.home()

start_marker = "# >>> noesis env >>>"
end_marker = "# <<< noesis env <<<"
block = (
    f"{start_marker}\n"
    f'export TIER0_REPO_ROOT="{repo_root}"\n'
    f'export TIER0_DATA_ROOT="{data_root}"\n'
    f'export TIER0_STATE_ROOT="{state_root}"\n'
    'export FNOX_AGE_KEY_FILE="${FNOX_AGE_KEY_FILE:-$HOME/.config/fnox/age.txt}"\n'
    'export PATH="$HOME/.local/share/mise/shims:$HOME/noesis/.venv/bin:$HOME/.local/bin:$PATH"\n'
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

echo "[WSL-BOOTSTRAP-OK] repo=$REPO_ROOT venv=$WSL_VENV fnox_dir=$HOME/.config/fnox skills_json=$SKILLS_JSON"

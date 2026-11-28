#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
export PYTHONDONTWRITEBYTECODE=1

if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
  BIN_DIR="$REPO_ROOT/.venv/bin"
elif [[ -n "${UV_PROJECT_ENVIRONMENT:-}" && -x "$UV_PROJECT_ENVIRONMENT/bin/python" ]]; then
  PY="$UV_PROJECT_ENVIRONMENT/bin/python"
  BIN_DIR="$UV_PROJECT_ENVIRONMENT/bin"
elif [[ -x "$HOME/noesis/.venv/bin/python" ]]; then
  PY="$HOME/noesis/.venv/bin/python"
  BIN_DIR="$HOME/noesis/.venv/bin"
elif [[ -x ".venv/Scripts/python.exe" ]]; then
  PY=".venv/Scripts/python.exe"
  BIN_DIR="$REPO_ROOT/.venv/Scripts"
else
  echo "Expected .venv interpreter missing under $REPO_ROOT/.venv or $HOME/noesis/.venv" >&2
  exit 1
fi
export PATH="$BIN_DIR:$HOME/.local/bin:$PATH"
VENV_DIR="$(dirname "$BIN_DIR")"
export VIRTUAL_ENV="$VENV_DIR"
export UV_PROJECT_ENVIRONMENT="$VENV_DIR"

CACHE_ROOT="${XDG_CACHE_HOME:-$HOME/.cache}/noesis"
export RUFF_CACHE_DIR="${RUFF_CACHE_DIR:-$CACHE_ROOT/ruff}"
export HYPOTHESIS_STORAGE_DIRECTORY="${HYPOTHESIS_STORAGE_DIRECTORY:-$CACHE_ROOT/hypothesis}"
PYTEST_CACHE_DIR="${PYTEST_CACHE_DIR:-$CACHE_ROOT/pytest}"

EXPECTED_RISK_SHA="ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
ACTUAL_RISK_SHA="$(sha256sum "src/tier0_sota/risk.py" | awk '{print $1}')"
if [[ "$ACTUAL_RISK_SHA" != "$EXPECTED_RISK_SHA" ]]; then
  echo "risk.py SHA-256 mismatch: $ACTUAL_RISK_SHA != $EXPECTED_RISK_SHA" >&2
  exit 1
fi
echo "[OK] risk.py SHA-256 = $ACTUAL_RISK_SHA"

mapfile -t SH_FILES < <(find scripts -type f -name '*.sh' | sort)
if [[ "${#SH_FILES[@]}" -gt 0 ]]; then
  for sf in "${SH_FILES[@]}"; do
    bash -n "$sf"
  done
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck "${SH_FILES[@]}"
    echo "[OK] shellcheck (${#SH_FILES[@]} scripts, 0 errors)"
  else
    echo "[OK] bash -n (${#SH_FILES[@]} scripts)"
  fi
fi

UV_LOCK_YFINANCE_MATCHES="$(grep -ic 'yfinance' uv.lock || true)"
if [[ "$UV_LOCK_YFINANCE_MATCHES" -ne 0 ]]; then
  echo "UV_LOCK_YFINANCE_MATCHES=$UV_LOCK_YFINANCE_MATCHES (expected 0)" >&2
  exit 1
fi
echo "[OK] UV_LOCK_YFINANCE_MATCHES=0"

if ! "$PY" - <<'PYEOF'
import re
from pathlib import Path

rx = re.compile(
    r"tier0" r"-quant(?!-falsification)|CLEAN" r" LIVE|sota-live-tv" r"-harness|"
    r"sota_compounding_1000rpm" r"_harness|RTX " r"4090"
)
roots = [
    Path("pyproject.toml"),
    Path("ARCHITECTURE_MANIFEST.json"),
    Path("SOTA_STARRED_AND_BEYOND_MANIFEST.json"),
]
for d in ("src", "docs", "evals", "scripts"):
    if Path(d).is_dir():
        roots.extend(p for p in Path(d).rglob("*") if p.is_file() and "__pycache__" not in p.parts)
hits = [str(p) for p in roots if p.is_file() and rx.search(p.read_text(encoding="utf-8", errors="ignore"))]
if hits:
    raise SystemExit(f"Legacy references detected: {hits}")
PYEOF
then
  echo "Legacy references detected" >&2
  exit 1
fi
echo "[OK] LEGACY_MATCHES=0"

GATE_LOG_DIR="$(mktemp -d)"
trap 'rm -rf "$GATE_LOG_DIR"' EXIT

declare -a GATE_NAMES=()
declare -a GATE_PIDS=()

run_gate_bg() {
  local name="$1"
  shift
  GATE_NAMES+=("$name")
  "$@" >"$GATE_LOG_DIR/$name.log" 2>&1 &
  GATE_PIDS+=("$!")
}

run_gate_bg "ruff-format" ruff format --check src tests scripts evals
run_gate_bg "ruff-check" ruff check .
run_gate_bg "ty-check" ty check --python "$PY"
run_gate_bg "deptry" deptry .
run_gate_bg "ast-grep" ast-grep scan --config sgconfig.yml
run_gate_bg "manifest-check" "$PY" scripts/sync_manifest.py --check
run_gate_bg "z3" "$PY" evals/formal_risk_z3.py
run_gate_bg "mcp-smoke" "$PY" scripts/nautilus_mcp_server.py --smoke
if command -v gitleaks >/dev/null 2>&1; then
  run_gate_bg "gitleaks" gitleaks detect --no-git --source "$REPO_ROOT" --config "$REPO_ROOT/.gitleaks.toml" --no-banner
fi

GATE_FAILED=0
for i in "${!GATE_NAMES[@]}"; do
  name="${GATE_NAMES[$i]}"
  if ! wait "${GATE_PIDS[$i]}"; then
    echo "=== FAILED GATE: $name ===" >&2
    cat "$GATE_LOG_DIR/$name.log" >&2
    GATE_FAILED=1
  fi
done
if [[ "$GATE_FAILED" -ne 0 ]]; then
  exit 1
fi
echo "[OK] parallel static/security/smoke gates (${#GATE_NAMES[@]}/${#GATE_NAMES[@]})"

zensical build --clean --strict
echo "[OK] zensical build --clean --strict"

pytest -q -o "cache_dir=$PYTEST_CACHE_DIR"

echo "[CHECK-SH VERDICT: PASS]"

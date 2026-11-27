#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1

IS_WIN_BASH=0
case "$(uname -s 2>/dev/null || true)" in
  MINGW*|MSYS*|CYGWIN*) IS_WIN_BASH=1 ;;
esac

if [[ "$IS_WIN_BASH" -eq 1 || "$PWD" == /mnt/* ]]; then
  for uv_cache in "/mnt/z/sota-cache/uv" "/z/sota-cache/uv"; do
    if [[ -d "$uv_cache" ]]; then
      export UV_CACHE_DIR="${UV_CACHE_DIR:-$uv_cache}"
      break
    fi
  done
  for uv_py in "/mnt/z/sota-cache/uv-python" "/z/sota-cache/uv-python"; do
    if [[ -d "$uv_py" ]]; then
      export UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR:-$uv_py}"
      break
    fi
  done
  export UV_LINK_MODE="${UV_LINK_MODE:-copy}"
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required on PATH to bootstrap noesis." >&2
  exit 1
fi

SYNC_ARGS=(sync --no-progress)
if [[ -f "uv.lock" ]]; then
  SYNC_ARGS+=(--frozen)
fi
if [[ ! -f "src/tier0_sota/__init__.py" ]]; then
  SYNC_ARGS+=(--no-install-project)
fi

uv "${SYNC_ARGS[@]}"

if [[ -n "${UV_PROJECT_ENVIRONMENT:-}" && -x "${UV_PROJECT_ENVIRONMENT}/bin/python" ]]; then
  PY="${UV_PROJECT_ENVIRONMENT}/bin/python"
elif [[ "$IS_WIN_BASH" -eq 1 ]]; then
  PY=".venv/Scripts/python.exe"
else
  PY=".venv/bin/python"
fi

if [[ ! -x "$PY" ]]; then
  echo "Expected virtualenv interpreter missing at $PY" >&2
  exit 1
fi

"$PY" - <<'PYEOF'
import pathlib
import sys
import sysconfig

purelib = pathlib.Path(sysconfig.get_path("purelib"))
if purelib.is_dir():
    target = purelib / "sitecustomize.py"
    code = """import os as _os


def _apply_tier0_runtime_defaults() -> None:
    if _os.environ.get("TIER0_UNBOUNDED_THREADS", "").strip() != "1":
        _burst = _os.environ.get("TIER0_COMPUTE_MODE", "multi").strip().lower() == "burst"
        for _key, _val in (
            ("POLARS_MAX_THREADS", "32" if _burst else "8"),
            ("RAYON_NUM_THREADS", "32" if _burst else "8"),
            ("OMP_NUM_THREADS", "32" if _burst else "8"),
            ("OPENBLAS_NUM_THREADS", "32" if _burst else "8"),
            ("MKL_NUM_THREADS", "32" if _burst else "8"),
            ("NUMEXPR_NUM_THREADS", "32" if _burst else "8"),
            ("TOKIO_WORKER_THREADS", "32" if _burst else "8"),
            ("NUMBA_NUM_THREADS", "24" if _burst else "8"),
            ("TIER0_DUCKDB_THREADS", "16" if _burst else "8"),
            ("TIER0_LGBM_THREADS", "16" if _burst else "8"),
            ("TIER0_CATBOOST_THREAD_COUNT", "16" if _burst else "8"),
            ("OMP_WAIT_POLICY", "PASSIVE"),
            ("GOMP_SPINCOUNT", "0"),
        ):
            if not _os.environ.get(_key, "").strip():
                _os.environ[_key] = _val
    for _key, _val in (
        ("TOKENIZERS_PARALLELISM", "false"),
        ("MLFLOW_DISABLE_AGENT_HINT", "1"),
    ):
        if not _os.environ.get(_key, "").strip():
            _os.environ[_key] = _val


_apply_tier0_runtime_defaults()
"""
    target.write_text(code, encoding="utf-8", newline="\n")

import nautilus_trader  # noqa: E402
import hftbacktest  # noqa: E402
import polars  # noqa: E402
import duckdb  # noqa: E402
import alpaca  # noqa: E402
import skfolio  # noqa: E402
import ml4t.diagnostic  # noqa: E402
import ml4t.engineer  # noqa: E402

print(f"python={sys.version.split()[0]} nautilus={nautilus_trader.__version__} polars={polars.__version__}")
PYEOF

echo "[BOOTSTRAP-SH OK] $PWD"

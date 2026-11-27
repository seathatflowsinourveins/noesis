#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [switch]$Frozen,
    [switch]$SkipEditable
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $env:PYTHONDONTWRITEBYTECODE = "1"
    if (-not $env:UV_CACHE_DIR -and (Test-Path "Z:\sota-cache\uv")) {
        $env:UV_CACHE_DIR = "Z:\sota-cache\uv"
    }
    if (-not $env:UV_PYTHON_INSTALL_DIR -and (Test-Path "Z:\sota-cache\uv-python")) {
        $env:UV_PYTHON_INSTALL_DIR = "Z:\sota-cache\uv-python"
    }
    if (-not $env:UV_LINK_MODE) {
        $env:UV_LINK_MODE = "copy"
    }

    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        throw "uv is required on PATH to bootstrap noesis."
    }

    $HasSrc = Test-Path (Join-Path $RepoRoot "src\tier0_sota\__init__.py")
    $SyncArgs = @("sync", "--no-progress")
    if ($Frozen -or (Test-Path (Join-Path $RepoRoot "uv.lock"))) {
        $SyncArgs += "--frozen"
    }
    if ($SkipEditable -or -not $HasSrc) {
        $SyncArgs += "--no-install-project"
    }

    & uv @SyncArgs
    if ($LASTEXITCODE -ne 0) {
        throw "uv $($SyncArgs -join ' ') failed with exit code $LASTEXITCODE"
    }

    $VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path $VenvPy)) {
        throw "Expected virtualenv interpreter missing at $VenvPy"
    }

    $SiteCustomScript = @'
import pathlib
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
'@
    $SiteCustomScript | & $VenvPy -
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install sitecustomize.py"
    }

    $VerifyCode = @'
import sys
import nautilus_trader
import hftbacktest
import polars
import duckdb
import alpaca
import skfolio
import ml4t.diagnostic
import ml4t.engineer

print(f"python={sys.version.split()[0]} nautilus={nautilus_trader.__version__} polars={polars.__version__}")
'@
    $VerifyCode | & $VenvPy -
    if ($LASTEXITCODE -ne 0) {
        throw "Core import verification failed"
    }

    Write-Output "[BOOTSTRAP-PS1 OK] $RepoRoot"
}
finally {
    Pop-Location
}

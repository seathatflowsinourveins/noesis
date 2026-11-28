#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [switch]$Frozen,
    [switch]$SkipEditable
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Remove-StaleVenvScripts {
    param([Parameter(Mandatory)][string]$StalePath)
    if (-not (Test-Path -LiteralPath $StalePath)) {
        return
    }
    foreach ($entry in @(Get-ChildItem -LiteralPath $StalePath -Force -ErrorAction SilentlyContinue)) {
        try {
            if (-not $entry.PSIsContainer -and $entry.IsReadOnly) {
                $entry.IsReadOnly = $false
            }
            Remove-Item -LiteralPath $entry.FullName -Recurse -Force -ErrorAction Stop
        } catch {
            # Still mapped by an old process; leave in .stale-scripts for the next run.
        }
    }
    $remaining = @(Get-ChildItem -LiteralPath $StalePath -Force -ErrorAction SilentlyContinue)
    if ($remaining.Count -eq 0) {
        Remove-Item -LiteralPath $StalePath -Force -ErrorAction SilentlyContinue
    }
}

function Get-VenvScriptProcesses {
    param([Parameter(Mandatory)][string]$ScriptsPrefix)
    $matched = [System.Collections.Generic.List[psobject]]::new()
    $allCim = $null
    try {
        $allCim = @(Get-CimInstance -ClassName Win32_Process -ErrorAction Stop)
    } catch {
        $allCim = $null
    }

    if ($null -ne $allCim) {
        foreach ($proc in $allCim) {
            if ($null -eq $proc.ProcessId) {
                continue
            }
            $procId = [int]$proc.ProcessId
            if ($procId -eq $PID) {
                continue
            }
            $exe = $proc.ExecutablePath
            if ([string]::IsNullOrWhiteSpace($exe)) {
                continue
            }
            try {
                $fullExe = [System.IO.Path]::GetFullPath($exe)
            } catch {
                continue
            }
            if ($fullExe.StartsWith($ScriptsPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
                $matched.Add([pscustomobject]@{
                    ProcessId      = $procId
                    ExecutablePath = $fullExe
                    Name           = [string]$proc.Name
                })
            }
        }
        return $matched.ToArray()
    }

    foreach ($proc in @(Get-Process -ErrorAction SilentlyContinue)) {
        if ($proc.Id -eq $PID) {
            continue
        }
        $exe = $null
        try {
            $exe = $proc.Path
        } catch {
            continue
        }
        if ([string]::IsNullOrWhiteSpace($exe)) {
            continue
        }
        try {
            $fullExe = [System.IO.Path]::GetFullPath($exe)
        } catch {
            continue
        }
        if ($fullExe.StartsWith($ScriptsPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            $matched.Add([pscustomobject]@{
                ProcessId      = [int]$proc.Id
                ExecutablePath = $fullExe
                Name           = [string]$proc.ProcessName
            })
        }
    }
    return $matched.ToArray()
}

function Stop-VenvScriptProcesses {
    param(
        [Parameter(Mandatory)][string]$ScriptsPrefix,
        [string[]]$TargetExecutablePaths = @()
    )
    $venvProcs = @(Get-VenvScriptProcesses -ScriptsPrefix $ScriptsPrefix)
    if ($venvProcs.Count -eq 0) {
        return 0
    }
    $targetSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($tp in $TargetExecutablePaths) {
        if (-not [string]::IsNullOrWhiteSpace($tp)) {
            [void]$targetSet.Add([System.IO.Path]::GetFullPath($tp))
        }
    }

    $primary = if ($targetSet.Count -gt 0) {
        @($venvProcs | Where-Object { $targetSet.Contains($_.ExecutablePath) })
    } else {
        $venvProcs
    }
    if ($primary.Count -eq 0) {
        return 0
    }

    $primaryPids = [System.Collections.Generic.HashSet[int]]::new()
    $pidsToStop = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($p in $primary) {
        $pidVal = [int]$p.ProcessId
        if ($pidVal -ne $PID) {
            [void]$primaryPids.Add($pidVal)
            [void]$pidsToStop.Add($pidVal)
        }
    }

    # Include direct python/pythonw children spawned by a locked venv Scripts launcher (e.g. edgartools-mcp.exe).
    try {
        foreach ($child in @(Get-CimInstance -ClassName Win32_Process -ErrorAction Stop)) {
            if ($null -eq $child.ProcessId -or $null -eq $child.ParentProcessId) {
                continue
            }
            $childPid = [int]$child.ProcessId
            $parentPid = [int]$child.ParentProcessId
            if ($childPid -eq $PID -or -not $primaryPids.Contains($parentPid)) {
                continue
            }
            $childName = [string]$child.Name
            if ($childName -ieq "python.exe" -or $childName -ieq "pythonw.exe") {
                [void]$pidsToStop.Add($childPid)
            }
        }
    } catch {
        # Best-effort child lookup only.
    }

    $stopped = 0
    foreach ($procId in $pidsToStop) {
        try {
            Stop-Process -Id $procId -Force -ErrorAction Stop
            $stopped++
        } catch {
            # Process may have already exited.
        }
    }
    if ($stopped -gt 0) {
        [int[]]$pidArray = @($pidsToStop)
        if ($pidArray.Count -gt 0) {
            Wait-Process -Id $pidArray -Timeout 5 -ErrorAction SilentlyContinue
        }
        Start-Sleep -Milliseconds 250
    }
    return $stopped
}

function Test-FileWriteUnlocked {
    param([Parameter(Mandatory)][string]$FilePath)
    $stream = $null
    try {
        $stream = [System.IO.File]::Open(
            $FilePath,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
        return $true
    } catch {
        return $false
    } finally {
        if ($null -ne $stream) {
            $stream.Dispose()
        }
    }
}

function Move-LockedFileToStale {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string]$StaleDir
    )
    if (-not (Test-Path -LiteralPath $FilePath -PathType Leaf)) {
        return $true
    }
    $item = Get-Item -LiteralPath $FilePath -Force -ErrorAction SilentlyContinue
    if ($null -ne $item -and $item.IsReadOnly) {
        try {
            $item.IsReadOnly = $false
        } catch {
            # Retry move/stop below if clearing ReadOnly is denied while locked.
        }
    }
    if (-not (Test-Path -LiteralPath $StaleDir)) {
        New-Item -ItemType Directory -Path $StaleDir -Force | Out-Null
    }
    $nonce = [guid]::NewGuid().ToString("N").Substring(0, 8)
    $leaf = [System.IO.Path]::GetFileName($FilePath)
    $dest = Join-Path $StaleDir ("{0}.{1}.{2}.old" -f $leaf, $PID, $nonce)
    try {
        [System.IO.File]::Move($FilePath, $dest)
        return $true
    } catch {
        return $false
    }
}

function Unlock-VenvScriptsForSync {
    param(
        [Parameter(Mandatory)][string]$ScriptsDir,
        [Parameter(Mandatory)][string]$ScriptsPrefix,
        [Parameter(Mandatory)][string]$StaleDir,
        [switch]$StopMatchingProcesses
    )
    if (-not (Test-Path -LiteralPath $ScriptsDir -PathType Container)) {
        return
    }

    $runningProcs = @(Get-VenvScriptProcesses -ScriptsPrefix $ScriptsPrefix)
    $lockedPaths = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($rp in $runningProcs) {
        [void]$lockedPaths.Add($rp.ExecutablePath)
    }

    foreach ($exeFile in @(Get-ChildItem -LiteralPath $ScriptsDir -Filter "*.exe" -File -Force -ErrorAction SilentlyContinue)) {
        if ($exeFile.IsReadOnly) {
            try {
                $exeFile.IsReadOnly = $false
            } catch {
                # Handle via rename/stop below.
            }
        }
        $fullPath = [System.IO.Path]::GetFullPath($exeFile.FullName)
        if (-not $lockedPaths.Contains($fullPath) -and -not (Test-FileWriteUnlocked -FilePath $fullPath)) {
            [void]$lockedPaths.Add($fullPath)
        }
    }

    if ($lockedPaths.Count -eq 0) {
        return
    }

    if ($StopMatchingProcesses) {
        $stopped = Stop-VenvScriptProcesses -ScriptsPrefix $ScriptsPrefix
        if ($stopped -gt 0) {
            Write-Host "[BOOTSTRAP-PS1] Stopped $stopped locked process(es) strictly inside $ScriptsDir"
        }
    }

    foreach ($lockedPath in @($lockedPaths)) {
        if (-not (Test-Path -LiteralPath $lockedPath -PathType Leaf)) {
            continue
        }
        if (Test-FileWriteUnlocked -FilePath $lockedPath) {
            continue
        }
        if (Move-LockedFileToStale -FilePath $lockedPath -StaleDir $StaleDir) {
            Write-Host "[BOOTSTRAP-PS1] Relocated locked executable $(Split-Path -Leaf $lockedPath) -> $StaleDir"
            continue
        }
        # Non-destructive NTFS rename failed; stop only the exact locking venv Scripts process and retry.
        $stopped = Stop-VenvScriptProcesses -ScriptsPrefix $ScriptsPrefix -TargetExecutablePaths @($lockedPath)
        if ($stopped -gt 0) {
            Write-Host "[BOOTSTRAP-PS1] Stopped $stopped locking process(es) for $(Split-Path -Leaf $lockedPath)"
        }
        if ((Test-Path -LiteralPath $lockedPath -PathType Leaf) -and -not (Test-FileWriteUnlocked -FilePath $lockedPath)) {
            [void](Move-LockedFileToStale -FilePath $lockedPath -StaleDir $StaleDir)
        }
    }
}

function Invoke-UvSync {
    param([Parameter(Mandatory)][string[]]$ArgsList)
    $lines = [System.Collections.Generic.List[string]]::new()
    $prevEap = $ErrorActionPreference
    $hasNativePref = $null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)
    $prevNativePref = if ($hasNativePref) { $PSNativeCommandUseErrorActionPreference } else { $false }
    try {
        $ErrorActionPreference = "Continue"
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        & uv @ArgsList 2>&1 | ForEach-Object {
            $line = $_.ToString()
            $lines.Add($line)
            Write-Host $line
        }
        $exitCode = if (Test-Path Variable:LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    } finally {
        $ErrorActionPreference = $prevEap
        if ($hasNativePref) {
            $PSNativeCommandUseErrorActionPreference = $prevNativePref
        }
    }
    return [pscustomobject]@{
        ExitCode = $exitCode
        Output   = ($lines -join "`n")
    }
}

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

    $VenvDir = if (-not [string]::IsNullOrWhiteSpace($env:UV_PROJECT_ENVIRONMENT)) {
        [System.IO.Path]::GetFullPath($env:UV_PROJECT_ENVIRONMENT.Trim())
    } else {
        [System.IO.Path]::GetFullPath((Join-Path $RepoRoot ".venv"))
    }
    $VenvPrefix = $VenvDir.TrimEnd('\', '/') + '\'
    $ScriptsDir = Join-Path $VenvDir "Scripts"
    $ScriptsPrefix = $ScriptsDir.TrimEnd('\', '/') + '\'
    $VenvRoot = [System.IO.Path]::GetPathRoot($VenvDir)
    $SotaCacheRoot = "Z:\sota-cache"
    $StaleDir = if (
        (Test-Path -LiteralPath $SotaCacheRoot -PathType Container) -and
        ([System.IO.Path]::GetPathRoot($SotaCacheRoot) -ieq $VenvRoot)
    ) {
        Join-Path $SotaCacheRoot "venv-stale-scripts"
    } else {
        Join-Path (Split-Path -Parent $VenvDir) ".venv-stale-scripts"
    }
    $LegacyStaleDir = Join-Path $VenvDir ".stale-scripts"

    # Reap stale relocated binaries (external + legacy in-venv) and unlock active .venv\Scripts\*.exe locks.
    Remove-StaleVenvScripts -StalePath $StaleDir
    Remove-StaleVenvScripts -StalePath $LegacyStaleDir
    Unlock-VenvScriptsForSync -ScriptsDir $ScriptsDir -ScriptsPrefix $ScriptsPrefix -StaleDir $StaleDir

    $HasSrc = Test-Path (Join-Path $RepoRoot "src\tier0_sota\__init__.py")
    $SyncArgs = @("sync", "--no-progress")
    if ($Frozen -or (Test-Path (Join-Path $RepoRoot "uv.lock"))) {
        $SyncArgs += "--frozen"
    }
    if ($SkipEditable -or -not $HasSrc) {
        $SyncArgs += "--no-install-project"
    }

    $syncRes = Invoke-UvSync -ArgsList $SyncArgs
    if ($syncRes.ExitCode -ne 0 -and ($syncRes.Output -match 'Access is denied|os error 5')) {
        Write-Host "[BOOTSTRAP-PS1] uv sync hit Windows file lock (os error 5); releasing locked venv processes and retrying once..."
        Unlock-VenvScriptsForSync -ScriptsDir $ScriptsDir -ScriptsPrefix $ScriptsPrefix -StaleDir $StaleDir -StopMatchingProcesses
        foreach ($m in [regex]::Matches($syncRes.Output, 'Failed to persist temporary file to ([^:\r\n]+:[\\/][^\r\n:]+)')) {
            $rawLocked = $m.Groups[1].Value.Trim()
            try {
                $resolvedLocked = [System.IO.Path]::GetFullPath($rawLocked)
            } catch {
                continue
            }
            if ($resolvedLocked.StartsWith($VenvPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
                [void](Move-LockedFileToStale -FilePath $resolvedLocked -StaleDir $StaleDir)
            }
        }
        $syncRes = Invoke-UvSync -ArgsList $SyncArgs
    }
    if ($syncRes.ExitCode -ne 0) {
        throw "uv $($SyncArgs -join ' ') failed with exit code $($syncRes.ExitCode)"
    }

    $VenvPy = Join-Path $VenvDir "Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $VenvPy)) {
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

    $prevCheckEditable = $env:TIER0_VERIFY_EDITABLE
    try {
        $env:TIER0_VERIFY_EDITABLE = if ($HasSrc -and -not $SkipEditable) { "1" } else { "0" }
        $VerifyCode = @'
import os
import pathlib
import sys
import sysconfig

purelib = pathlib.Path(sysconfig.get_path("purelib"))
assert (purelib / "sitecustomize.py").is_file(), "sitecustomize.py missing from purelib"

import alpaca
import duckdb
import hftbacktest
import ml4t.diagnostic
import ml4t.engineer
import nautilus_trader
import polars
import pydantic
import skfolio

if os.environ.get("TIER0_VERIFY_EDITABLE") == "1":
    import tier0_sota  # noqa: F401

print(
    f"WINDOWS_VENV_OK python={sys.version.split()[0]} "
    f"nautilus={nautilus_trader.__version__} polars={polars.__version__} "
    f"duckdb={duckdb.__version__} pydantic={pydantic.__version__}"
)
'@
        $VerifyCode | & $VenvPy -
        if ($LASTEXITCODE -ne 0) {
            throw "Core import verification failed"
        }
    } finally {
        if ($null -eq $prevCheckEditable) {
            Remove-Item Env:TIER0_VERIFY_EDITABLE -ErrorAction SilentlyContinue
        } else {
            $env:TIER0_VERIFY_EDITABLE = $prevCheckEditable
        }
    }

    Remove-StaleVenvScripts -StalePath $StaleDir
    Remove-StaleVenvScripts -StalePath $LegacyStaleDir
    Write-Output "[BOOTSTRAP-PS1 OK] $RepoRoot"
}
finally {
    Pop-Location
}

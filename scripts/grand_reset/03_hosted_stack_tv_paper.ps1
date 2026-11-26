#Requires -Version 7.0
<#
.SYNOPSIS
    Brings up/verifies the 15 hosted SOTA portals, TradingView Pine v6 pack, and Paper Farm.
.DESCRIPTION
    Idempotently checks/starts docker/compose.sota.yml, verifies all 15 loopback portals,
    refreshes TradingView Half-Auto tickets/watchlists via tv_console.ps1 helpers, runs
    Paper Farm preflight, and emits paper_farm_live_performance_manifest.json +
    finos_paper_farm_performance.json (strictly paper_only=$true, live_execution_enabled=$false).
#>
[CmdletBinding()]
param(
    [string]$DataRoot = $(if ($env:TIER0_DATA_ROOT) { $env:TIER0_DATA_ROOT } else { "Z:\sota-data" }),
    [string]$ManifestDir = "",
    [switch]$StartDocker,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptRepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$RepoRoot = if ($env:TIER0_REPO_ROOT -and (Test-Path (Join-Path $env:TIER0_REPO_ROOT "src\tier0_sota"))) {
    $env:TIER0_REPO_ROOT
} else {
    $ScriptRepoRoot
}
if ([string]::IsNullOrWhiteSpace($ManifestDir)) {
    $ManifestDir = Join-Path $DataRoot "manifests"
}
$ComposeFile = Join-Path $RepoRoot "docker\compose.sota.yml"
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)
$PaperOnly = $true
$LiveExecutionEnabled = $false

if (-not $PaperOnly -or $LiveExecutionEnabled) {
    throw "Fail-closed: paper_only=true and live_execution_enabled=false required."
}

$ListeningLive = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
if ($ListeningLive.Count -gt 0) {
    throw "Fail-closed: forbidden live broker port listening ($($ListeningLive.LocalPort -join ','))."
}

if (-not (Test-Path -LiteralPath $ComposeFile)) {
    throw "Missing Docker compose stack: $ComposeFile"
}

$Portals = @(
    "http://127.0.0.1:9200/",
    "http://127.0.0.1:8010/",
    "http://127.0.0.1:5000/",
    "http://127.0.0.1:3000/",
    "http://127.0.0.1:8080/",
    "http://127.0.0.1:7575/",
    "http://127.0.0.1:9090/",
    "http://127.0.0.1:3001/",
    "http://127.0.0.1:9464/metrics",
    "http://127.0.0.1:8793/",
    "http://127.0.0.1:1420/",
    "http://127.0.0.1:8000/health",
    "http://127.0.0.1:6900/",
    "http://127.0.0.1:4213/",
    "http://127.0.0.1:13133/"
)

$PyExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$TvConsole = Join-Path $RepoRoot "scripts\tv_console.ps1"
$PaperScript = Join-Path $RepoRoot "scripts\run_live_paper_farm.ps1"

if ($DryRun) {
    Write-Host "[DRY-RUN] Compose stack: $ComposeFile (portals=$($Portals.Count))" -ForegroundColor Cyan
    Write-Host "[DRY-RUN] TV sync: $TvConsole -> tv-tickets (ManifestDir=$ManifestDir)" -ForegroundColor Cyan
    Write-Host "[DRY-RUN] Paper Farm: $PaperScript -Mode preflight & -Mode performance" -ForegroundColor Cyan
    Write-Host "[VERDICT: DRY-RUN-OK] paper_only=$PaperOnly live_execution_enabled=$LiveExecutionEnabled portals=$($Portals.Count)" -ForegroundColor Green
    exit 0
}

if ($StartDocker) {
    $useNativeDocker = $false
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        docker info *> $null
        if ($LASTEXITCODE -eq 0) {
            $useNativeDocker = $true
        }
    }
    if ($useNativeDocker) {
        & docker compose -p docker -f $ComposeFile up -d --remove-orphans
        if ($LASTEXITCODE -ne 0) {
            throw "docker compose up -d failed with exit code $LASTEXITCODE"
        }
    } else {
        $wslComposeCmd = 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT="$HOME/tier0-sota-state" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml up -d --remove-orphans'
        & wsl.exe -d Noesis-SOTA -- bash -lc $wslComposeCmd
        if ($LASTEXITCODE -ne 0) {
            throw "WSL2 Noesis-SOTA docker compose up -d failed with exit code $LASTEXITCODE"
        }
    }
}

$Healthy = 0
foreach ($url in $Portals) {
    try {
        $resp = Invoke-WebRequest -Uri $url -TimeoutSec 3 -UseBasicParsing
        if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 400) {
            $Healthy++
        }
    } catch {
        Write-Warning "Portal unreachable: $url"
    }
}
Write-Host "[PORTALS] Healthy=$Healthy/$($Portals.Count)" -ForegroundColor Cyan

if (Test-Path -LiteralPath $TvConsole) {
    . $TvConsole
    tv-tickets -ManifestDir $ManifestDir | Out-Null
}

& pwsh -NoProfile -ExecutionPolicy Bypass -File $PaperScript -Mode preflight -ManifestDir $ManifestDir
if ($LASTEXITCODE -ne 0) { throw "Paper Farm preflight failed" }

& pwsh -NoProfile -ExecutionPolicy Bypass -File $PaperScript -Mode performance -ManifestDir $ManifestDir
if ($LASTEXITCODE -ne 0) { throw "Paper Farm performance aggregation failed" }

Write-Host "[VERDICT: PASS] Hosted stack ($Healthy/$($Portals.Count)), TV pack, and Paper Farm manifests synced (paper_only=$PaperOnly)." -ForegroundColor Green

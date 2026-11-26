#Requires -Version 7.0
<#
.SYNOPSIS
    Idempotent Disk-Z folder tree & WSL2 Noesis-SOTA bootstrap for Tier-0 SOTA Clean.
.DESCRIPTION
    Initializes Z:\tier0-sota-clean, Z:\sota-data\{prewall-lake,manifests,mlflow,duckdb,logs},
    Z:\sota-cache, and Z:\archive without formatting/wiping disks or shutting down WSL.
    Bootstraps WSL2 distro via scripts/grand_reset/01_wsl_bootstrap.sh.
#>
[CmdletBinding()]
param(
    [string]$DataRoot = "Z:\sota-data",
    [string]$CacheRoot = "Z:\sota-cache",
    [string]$ArchiveRoot = "Z:\archive",
    [string]$WslDistro = "Noesis-SOTA",
    [string]$WslUser = "sota",
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)

$ListeningLive = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
if ($ListeningLive.Count -gt 0) {
    throw "Fail-closed: forbidden live broker port detected ($($ListeningLive.LocalPort -join ','))."
}

$Dirs = @(
    $RepoRoot,
    $DataRoot,
    (Join-Path $DataRoot "prewall-lake"),
    (Join-Path $DataRoot "manifests"),
    (Join-Path $DataRoot "mlflow"),
    (Join-Path $DataRoot "duckdb"),
    (Join-Path $DataRoot "logs"),
    $CacheRoot,
    (Join-Path $CacheRoot "uv"),
    (Join-Path $CacheRoot "uv-python"),
    $ArchiveRoot
)

foreach ($dir in $Dirs) {
    if ($DryRun) {
        Write-Host "[DRY-RUN] Ensure directory: $dir" -ForegroundColor Cyan
    } elseif (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "[CREATED] $dir" -ForegroundColor Green
    } else {
        Write-Host "[OK] Exists: $dir" -ForegroundColor DarkGray
    }
}

$BootstrapWin = Join-Path $RepoRoot "scripts\grand_reset\01_wsl_bootstrap.sh"
if (-not (Test-Path -LiteralPath $BootstrapWin)) {
    throw "Missing WSL bootstrap script: $BootstrapWin"
}
$RepoWsl = "/mnt/" + $RepoRoot.Substring(0, 1).ToLowerInvariant() + $RepoRoot.Substring(2).Replace("\", "/")
$BootstrapWsl = "$RepoWsl/scripts/grand_reset/01_wsl_bootstrap.sh"

if ($DryRun) {
    Write-Host "[DRY-RUN] Would run: wsl -d $WslDistro -u $WslUser -- bash $BootstrapWsl $RepoWsl" -ForegroundColor Cyan
    Write-Host "[VERDICT: DRY-RUN-OK] Disk-Z & WSL2 bootstrap plan verified (live_ports=0)." -ForegroundColor Green
    exit 0
}

& wsl -d $WslDistro -u $WslUser -- bash $BootstrapWsl $RepoWsl
if ($LASTEXITCODE -ne 0) {
    throw "01_wsl_bootstrap.sh failed with exit code $LASTEXITCODE"
}
Write-Host "[VERDICT: PASS] Disk-Z & WSL2 Noesis-SOTA bootstrap complete." -ForegroundColor Green

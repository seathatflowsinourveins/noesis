#Requires -Version 7.0
<#
.SYNOPSIS
    Launch clean non-overlapping Tier-0 SOTA lanes targeting Z:\tier0-sota-clean.
.DESCRIPTION
    Supports -DryRun and modular lane selection:
      - sim    : Pre-Wall Sim Lab (<= 2026-10-15 PIT + MLflow :5000 + 6-gate offline check)
      - paper  : 3-Lane Non-Overlapping Paper Farm (ALPACA_PA1..PA3 + IBKR_DU_4002; fail-closed without -ConfirmPaper)
      - tv     : TV Half-Auto Cockpit (tier0_sota.tv.tickets; semi_auto_confirmed=false, qty=1, zero CDP)
      - verify : Verifier Gate (ruff, ty, ast-grep, pytest, gitleaks, risk.py SHA-256, live_ports=0)
#>
[CmdletBinding()]
param(
    [ValidateSet("all", "sim", "paper", "tv", "verify")]
    [string[]]$Lane = @("all"),

    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$ManifestDir = "Z:\sota-data\manifests",

    [switch]$ConfirmPaper,
    [switch]$ForcePaper,
    [switch]$UseOrcaTabs,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedRiskSha = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)

# Normalize selected lanes without duplicates
$Selected = [System.Collections.Generic.List[string]]::new()
$RawLanes = @($Lane | ForEach-Object { $_.ToLowerInvariant() })
if ($RawLanes -contains "all") {
    foreach ($l in @("sim", "paper", "tv", "verify")) { $Selected.Add($l) }
} else {
    foreach ($l in @("sim", "paper", "tv", "verify")) {
        if ($RawLanes -contains $l) { $Selected.Add($l) }
    }
}

$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $VenvPy)) {
    throw "Clean .venv Python missing at $VenvPy. Run scripts/bootstrap_env.ps1 first."
}

$RiskPath = Join-Path $RepoRoot "src\tier0_sota\risk.py"
if (-not (Test-Path -LiteralPath $RiskPath)) {
    throw "Missing risk kernel at $RiskPath"
}
$RiskSha = (Get-FileHash -LiteralPath $RiskPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($RiskSha -ne $ExpectedRiskSha) {
    throw "Refused launch: risk.py SHA-256 mismatch ($RiskSha != $ExpectedRiskSha)."
}

$LivePorts = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
if ($LivePorts.Count -ne 0) {
    throw "Refused launch: forbidden live broker ports detected ($($LivePorts.Count))."
}

# Detect already-running external Paper Farm heartbeats to avoid duplicate overlapping loops
$ActivePaperLanes = @()
foreach ($acct in @("ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3")) {
    $stPath = Join-Path $ManifestDir "$($acct.ToLowerInvariant())_live_automation_status.json"
    if (Test-Path -LiteralPath $stPath) {
        try {
            $st = Get-Content -LiteralPath $stPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($st.heartbeat_utc) {
                $ageSec = ([datetimeoffset]::UtcNow - [datetimeoffset]::Parse([string]$st.heartbeat_utc)).TotalSeconds
                if ($ageSec -lt 120) {
                    $ActivePaperLanes += $acct
                }
            }
        } catch {
            Write-Verbose "Ignoring unreadable status file $stPath"
        }
    }
}

$TvOutDir = Join-Path $ManifestDir "tv_sota_pack"
$PaperScript = Join-Path $RepoRoot "scripts\run_live_paper_farm.ps1"

$LaneSpecs = @()
foreach ($l in $Selected) {
    switch ($l) {
        "sim" {
            $SimCmd = "& `"$VenvPy`" -B -c `"from datetime import date; from tier0_sota import discovery, engine, research; assert discovery.DISCOVERY_WALL_DATE == date(2026, 10, 15); print('SIM_LAB_OK', discovery.DISCOVERY_WALL_ISO, research.MIN_CPCV_SAMPLES, engine.FORBIDDEN_LIVE_IBKR_PORTS)`""
            $LaneSpecs += @{
                Id      = "sim"
                Title   = "[CLEAN-SIM] Pre-Wall Sim Lab (<= 2026-10-15)"
                Command = $SimCmd
            }
        }
        "paper" {
            $PaperMode = "preflight"
            $PaperNote = "preflight + performance (read-only)"
            if ($ConfirmPaper) {
                if ($ActivePaperLanes.Count -gt 0 -and -not $ForcePaper) {
                    $PaperNote = "preflight + performance (active external Paper Farm detected: $($ActivePaperLanes -join ','); pass -ForcePaper to override)"
                } else {
                    $PaperMode = "paper-farm -ConfirmPaper"
                    $PaperNote = "3-lane ALPACA_PA1..PA3 + IBKR_DU_4002 paper-farm status sync"
                }
            }
            $PaperCmd = "pwsh -NoProfile -ExecutionPolicy Bypass -File `"$PaperScript`" -Mode $PaperMode -ManifestDir `"$ManifestDir`" && pwsh -NoProfile -ExecutionPolicy Bypass -File `"$PaperScript`" -Mode performance -ManifestDir `"$ManifestDir`""
            $LaneSpecs += @{
                Id      = "paper"
                Title   = "[CLEAN-PAPER] 3-Lane Alpaca PA1..PA3 + IBKR_DU_4002 ($PaperNote)"
                Command = $PaperCmd
            }
        }
        "tv" {
            $TvCmd = "& `"$VenvPy`" -B -m tier0_sota.tv.tickets --manifest-dir `"$ManifestDir`" --out-dir `"$TvOutDir`""
            $LaneSpecs += @{
                Id      = "tv"
                Title   = "[CLEAN-TV] Half-Auto Cockpit (semi_auto_confirmed=false, qty=1)"
                Command = $TvCmd
            }
        }
        "verify" {
            $VerifyCmd = "& `"$VenvPy`" -m ruff check src tests scripts && & `"$VenvPy`" -m ty check src tests scripts && ast-grep scan && & `"$VenvPy`" -m pytest -q"
            $LaneSpecs += @{
                Id      = "verify"
                Title   = "[CLEAN-VERIFY] Ruff + Ty + Ast-Grep + Pytest Gate"
                Command = $VerifyCmd
            }
        }
    }
}

Write-Host "[CLEAN-LANES] Pre-checks OK | repo=$RepoRoot | risk.py=OK | live_ports=0 | active_paper_heartbeats=$($ActivePaperLanes.Count)/3" -ForegroundColor Green

if ($DryRun) {
    Write-Host "[DRY-RUN] Planned non-overlapping clean lanes ($($LaneSpecs.Count)):" -ForegroundColor Yellow
    foreach ($spec in $LaneSpecs) {
        Write-Host ("  - [{0}] {1}" -f $spec.Id, $spec.Title) -ForegroundColor Cyan
        Write-Host ("      cmd: {0}" -f $spec.Command) -ForegroundColor DarkGray
    }
    return
}

$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONPATH = Join-Path $RepoRoot "src"

Push-Location $RepoRoot
try {
    foreach ($spec in $LaneSpecs) {
        Write-Host "[LAUNCH] $($spec.Title)" -ForegroundColor Cyan
        if ($UseOrcaTabs) {
            if (-not (Get-Command orca -ErrorAction SilentlyContinue)) {
                throw "orca CLI is required when -UseOrcaTabs is specified."
            }
            orca terminal create --worktree "path:$RepoRoot" --title $spec.Title --json | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to create Orca tab for lane $($spec.Id)"
            }
        } else {
            & pwsh -NoProfile -Command $spec.Command
            if ($LASTEXITCODE -ne 0) {
                throw "Lane '$($spec.Id)' exited with code $LASTEXITCODE"
            }
        }
    }
    Write-Host "[VERDICT: PASS] Completed $($LaneSpecs.Count) clean lane(s)." -ForegroundColor Green
} finally {
    Pop-Location
}

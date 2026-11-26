#Requires -Version 7.0
<#
.SYNOPSIS
    Safe-by-default Clean Disk-Z Reset & Fresh Antigravity (AGY) Reinstall preparation.
.DESCRIPTION
    Defaults to -DryRun unless -Execute is explicitly passed.
    - Optionally archives operator-specified superseded Disk-Z directories via -ArchiveLegacyDirs <string[]>
      and stray Z:\tmp_*.diff files into Z:\archive\pre_grand_reset_2026-11-25\
    - Strictly protects Z:\noesis ($TIER0_REPO_ROOT), Z:\sota-data\prewall-lake,
      Z:\sota-data\manifests, Z:\sota-cache, Z:\WSL, and Z:\archive
    - Supports -CleanAgyReinstall: backs up ~/.gemini to Z:\archive\pre_grand_reset_2026-11-25\gemini_backup\,
      writes a fresh ZERO-hook ~/.gemini/settings.json from Z:\noesis\.gemini\settings.json,
      syncs ONLY the 46 standalone locked skills from $RepoRoot\.claude\skills into ~/.gemini/config/skills/,
      and prints fresh Antigravity (AGY) reinstall & clean workspace open steps.
    - NEVER formats drives or runs wsl --shutdown.
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)),
    [string]$DataRoot = $(if ($env:TIER0_DATA_ROOT) { $env:TIER0_DATA_ROOT } else { "Z:\sota-data" }),
    [string]$CacheRoot = "Z:\sota-cache",
    [string]$HomeDir = $env:USERPROFILE,
    [string]$ArchiveRoot = "Z:\archive\pre_grand_reset_2026-11-25",
    [string[]]$ArchiveLegacyDirs = @(),
    [switch]$CleanAgyReinstall,
    [switch]$Execute,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$IsDryRun = $DryRun -or (-not $Execute)
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)

# Fail-closed live broker port firewall
$LivePorts = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
if ($LivePorts.Count -ne 0) {
    throw "Refused reset: forbidden live broker ports detected ($($LivePorts.Count))."
}

$ProtectedRoots = @(
    $RepoRoot,
    (Join-Path $DataRoot "prewall-lake"),
    (Join-Path $DataRoot "manifests"),
    $CacheRoot,
    "Z:\WSL",
    "Z:\archive"
)
$ProtectedNormalized = @($ProtectedRoots | ForEach-Object { [System.IO.Path]::GetFullPath($_).TrimEnd('\').ToLowerInvariant() })

$ArchiveCandidates = [System.Collections.Generic.List[hashtable]]::new()
foreach ($dirPath in $ArchiveLegacyDirs) {
    if ([string]::IsNullOrWhiteSpace($dirPath)) { continue }
    $full = [System.IO.Path]::GetFullPath($dirPath).TrimEnd('\')
    if ($full.ToLowerInvariant() -in $ProtectedNormalized) {
        throw "Refused archival of protected path: $full"
    }
    if (Test-Path -LiteralPath $full) {
        $ArchiveCandidates.Add(@{
            Source = $full
            Target = (Join-Path $ArchiveRoot (Split-Path -Leaf $full))
        })
    }
}
foreach ($tmpDiff in @(Get-ChildItem -LiteralPath "Z:\" -Filter "tmp_*.diff" -File -ErrorAction SilentlyContinue)) {
    $ArchiveCandidates.Add(@{
        Source = $tmpDiff.FullName
        Target = (Join-Path $ArchiveRoot $tmpDiff.Name)
    })
}

$SkillsSourceDir = Join-Path $RepoRoot ".claude\skills"
$LockPath = Join-Path $RepoRoot "UPSTREAM_SKILLS_LOCK.json"
$LockedSkills = @()
if (Test-Path -LiteralPath $LockPath) {
    $lockObj = Get-Content -LiteralPath $LockPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $LockedSkills = @($lockObj.skills | ForEach-Object { [string]$_.skill })
}

$AgyDir = Join-Path $HomeDir ".gemini"
$AgyBackupDir = Join-Path $ArchiveRoot "gemini_backup"
$RepoGeminiSettings = Join-Path $RepoRoot ".gemini\settings.json"
$AgySettingsTarget = Join-Path $AgyDir "settings.json"
$AgySkillsTarget = Join-Path $AgyDir "config\skills"

if ($IsDryRun) {
    Write-Host "[DRY-RUN] Grand-Reset Step 00: Clean Disk-Z Reset & Fresh AGY Reinstall (safe-by-default; pass -Execute to apply)" -ForegroundColor Yellow
    Write-Host "  Protected Roots  : $($ProtectedRoots -join ', ')" -ForegroundColor Green
    Write-Host "  Archive Target   : $ArchiveRoot" -ForegroundColor Cyan
    Write-Host "  Pending Moves    : $($ArchiveCandidates.Count) superseded item(s)" -ForegroundColor Cyan
    foreach ($c in $ArchiveCandidates) {
        Write-Host ("    - {0} -> {1}" -f $c.Source, $c.Target) -ForegroundColor Gray
    }
    Write-Host "  Skills Source    : $SkillsSourceDir (locked=$($LockedSkills.Count))" -ForegroundColor Cyan
    Write-Host "  CleanAgyReinstall: $([bool]$CleanAgyReinstall) (zero-hook settings -> $AgySettingsTarget)" -ForegroundColor Cyan
    Write-Host "  Live Broker Ports: 0 (fail-closed)" -ForegroundColor Green
    return
}

New-Item -ItemType Directory -Path $ArchiveRoot -Force | Out-Null

# 1. Non-destructive archival of operator-selected legacy directories & stray root diffs
$MovedCount = 0
$LockedCount = 0
foreach ($c in $ArchiveCandidates) {
    $parent = Split-Path -Parent $c.Target
    if ($parent) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
    try {
        Move-Item -LiteralPath $c.Source -Destination $c.Target -ErrorAction Stop
        $MovedCount++
        Write-Host "[ARCHIVED] $($c.Source) -> $($c.Target)" -ForegroundColor Green
    } catch {
        $LockedCount++
        Write-Warning "Skipped locked path without force-kill ($($c.Source)): $($_.Exception.Message)"
    }
}

# 2. Fresh Zero-Hook Antigravity (AGY) reinstall preparation
if ($CleanAgyReinstall) {
    if (Test-Path -LiteralPath $AgyDir) {
        New-Item -ItemType Directory -Path $AgyBackupDir -Force | Out-Null
        $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
        $snapDir = Join-Path $AgyBackupDir "gemini_$stamp"
        New-Item -ItemType Directory -Path $snapDir -Force | Out-Null
        foreach ($itemName in @("settings.json", "config\mcp_config.json", "config\hooks.json")) {
            $srcItem = Join-Path $AgyDir $itemName
            if (Test-Path -LiteralPath $srcItem) {
                $dstItem = Join-Path $snapDir $itemName
                New-Item -ItemType Directory -Path (Split-Path -Parent $dstItem) -Force | Out-Null
                Copy-Item -LiteralPath $srcItem -Destination $dstItem -Force
            }
        }
        Write-Host "[AGY BACKUP] Saved existing ~/.gemini configs -> $snapDir" -ForegroundColor Cyan
    }

    if (-not (Test-Path -LiteralPath $RepoGeminiSettings)) {
        throw "Missing clean AGY settings template: $RepoGeminiSettings"
    }
    $cleanSettings = Get-Content -LiteralPath $RepoGeminiSettings -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($cleanSettings.PSObject.Properties.Name -contains "hooks") {
        $cleanSettings.PSObject.Properties.Remove("hooks")
    }
    New-Item -ItemType Directory -Path $AgyDir -Force | Out-Null
    [System.IO.File]::WriteAllText(
        $AgySettingsTarget,
        (($cleanSettings | ConvertTo-Json -Depth 10) + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
    Write-Host "[AGY SETTINGS] Wrote zero-hook settings -> $AgySettingsTarget" -ForegroundColor Green

    $SyncedSkills = 0
    if ($LockedSkills.Count -gt 0 -and (Test-Path -LiteralPath $SkillsSourceDir)) {
        New-Item -ItemType Directory -Path $AgySkillsTarget -Force | Out-Null
        foreach ($skillName in $LockedSkills) {
            $srcSkill = Join-Path $SkillsSourceDir $skillName
            if (-not (Test-Path -LiteralPath $srcSkill)) { continue }
            $dstSkill = Join-Path $AgySkillsTarget $skillName
            New-Item -ItemType Directory -Path $dstSkill -Force | Out-Null
            Copy-Item -Path (Join-Path $srcSkill "*") -Destination $dstSkill -Recurse -Force
            $SyncedSkills++
        }
    }
    Write-Host "[AGY SKILLS] Synced $SyncedSkills/$($LockedSkills.Count) standalone locked skills -> $AgySkillsTarget" -ForegroundColor Green

    Write-Host "`n[NEXT STEPS - FRESH ANTIGRAVITY (AGY) REINSTALL]" -ForegroundColor Cyan
    Write-Host "  1. Close Antigravity UI cleanly and uninstall/reinstall the fresh Antigravity desktop build (keeps ~/.gemini settings & skills)." -ForegroundColor Gray
    Write-Host "  2. Launch Antigravity and open active workspace: $RepoRoot" -ForegroundColor Gray
    Write-Host "  3. Confirm 4 vetted MCPs (codebase-memory, xerj, nautilus-engine, edgartools) and run:" -ForegroundColor Gray
    Write-Host "     pwsh -NoProfile -ExecutionPolicy Bypass -File $RepoRoot\scripts\grand_reset\verify_grand_reset.ps1" -ForegroundColor Gray
}

Write-Host "[GRAND-RESET 00 OK] Moved=$MovedCount | LockedSkipped=$LockedCount | Protected=$($ProtectedRoots.Count) | LivePorts=0" -ForegroundColor Green

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
      writes ~/.gemini/settings.json from Z:\noesis\.gemini\settings.json while preserving official
      upstream Orca status hooks (~/.orca/agent-hooks/*.cmd) and stripping any custom hooks,
      writes zero-copy ~/.gemini/config/skills.json pointing to $RepoRoot\.claude\skills,
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
$AgySkillsConfigTarget = Join-Path $AgyDir "config\skills.json"
$SkillsSourceSlash = ($SkillsSourceDir.Replace('\', '/').TrimEnd('/'))

function Get-PreservedOrcaHooks {
    param([object]$SettingsObj)
    if ($null -eq $SettingsObj -or -not ($SettingsObj.PSObject.Properties.Name -contains "hooks")) {
        return $null
    }
    $hooksObj = $SettingsObj.hooks
    if ($null -eq $hooksObj) {
        return $null
    }
    $preserved = [ordered]@{}
    foreach ($evtProp in $hooksObj.PSObject.Properties) {
        $keptMatchers = [System.Collections.Generic.List[object]]::new()
        foreach ($matcherItem in @($evtProp.Value)) {
            if ($null -eq $matcherItem) { continue }
            $innerHooks = @()
            if ($matcherItem.PSObject.Properties.Name -contains "hooks") {
                $innerHooks = @(
                    $matcherItem.hooks | Where-Object {
                        $_ -and ([string]$_.command -match '[/\\]\.orca[/\\]agent-hooks[/\\]')
                    }
                )
            } elseif ($matcherItem.PSObject.Properties.Name -contains "command" -and ([string]$matcherItem.command -match '[/\\]\.orca[/\\]agent-hooks[/\\]')) {
                $keptMatchers.Add($matcherItem)
                continue
            }
            if ($innerHooks.Count -gt 0) {
                $copyMatcher = [ordered]@{}
                foreach ($mp in $matcherItem.PSObject.Properties) {
                    if ($mp.Name -eq "hooks") {
                        $copyMatcher["hooks"] = @($innerHooks)
                    } else {
                        $copyMatcher[$mp.Name] = $mp.Value
                    }
                }
                $keptMatchers.Add([pscustomobject]$copyMatcher)
            }
        }
        if ($keptMatchers.Count -gt 0) {
            $preserved[$evtProp.Name] = @($keptMatchers)
        }
    }
    if ($preserved.Count -gt 0) {
        return [pscustomobject]$preserved
    }
    return $null
}

if ($IsDryRun) {
    Write-Host "[DRY-RUN] Grand-Reset Step 00: Clean Disk-Z Reset & Fresh AGY Reinstall (safe-by-default; pass -Execute to apply)" -ForegroundColor Yellow
    Write-Host "  Protected Roots  : $($ProtectedRoots -join ', ')" -ForegroundColor Green
    Write-Host "  Archive Target   : $ArchiveRoot" -ForegroundColor Cyan
    Write-Host "  Pending Moves    : $($ArchiveCandidates.Count) superseded item(s)" -ForegroundColor Cyan
    foreach ($c in $ArchiveCandidates) {
        Write-Host ("    - {0} -> {1}" -f $c.Source, $c.Target) -ForegroundColor Gray
    }
    Write-Host "  Skills Source    : $SkillsSourceDir (locked=$($LockedSkills.Count)) -> $AgySkillsConfigTarget" -ForegroundColor Cyan
    Write-Host "  CleanAgyReinstall: $([bool]$CleanAgyReinstall) (Orca-hook-preserving settings -> $AgySettingsTarget)" -ForegroundColor Cyan
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
    $existingOrcaHooks = $null
    if (Test-Path -LiteralPath $AgySettingsTarget) {
        try {
            $existingSettingsObj = Get-Content -LiteralPath $AgySettingsTarget -Raw -Encoding UTF8 | ConvertFrom-Json
            $existingOrcaHooks = Get-PreservedOrcaHooks -SettingsObj $existingSettingsObj
        } catch {
            $existingOrcaHooks = $null
        }
    }
    $cleanSettings = Get-Content -LiteralPath $RepoGeminiSettings -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($cleanSettings.PSObject.Properties.Name -contains "hooks") {
        $cleanSettings.PSObject.Properties.Remove("hooks")
    }
    if ($null -ne $existingOrcaHooks) {
        $cleanSettings | Add-Member -MemberType NoteProperty -Name "hooks" -Value $existingOrcaHooks -Force
    }
    New-Item -ItemType Directory -Path $AgyDir -Force | Out-Null
    [System.IO.File]::WriteAllText(
        $AgySettingsTarget,
        (($cleanSettings | ConvertTo-Json -Depth 10) + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
    Write-Host "[AGY SETTINGS] Wrote clean settings (orca_hooks_preserved=$($null -ne $existingOrcaHooks)) -> $AgySettingsTarget" -ForegroundColor Green

    $agySkillsPayload = [ordered]@{
        entries = @(
            [ordered]@{ path = $SkillsSourceSlash }
        )
    }
    New-Item -ItemType Directory -Path (Split-Path -Parent $AgySkillsConfigTarget) -Force | Out-Null
    [System.IO.File]::WriteAllText(
        $AgySkillsConfigTarget,
        (($agySkillsPayload | ConvertTo-Json -Depth 5) + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
    Write-Host "[AGY SKILLS] Wrote zero-copy skills.json ($SkillsSourceSlash, locked=$($LockedSkills.Count)) -> $AgySkillsConfigTarget" -ForegroundColor Green

    Write-Host "`n[NEXT STEPS - FRESH ANTIGRAVITY (AGY) REINSTALL]" -ForegroundColor Cyan
    Write-Host "  1. Close Antigravity UI cleanly and uninstall/reinstall the fresh Antigravity desktop build (keeps ~/.gemini settings & skills)." -ForegroundColor Gray
    Write-Host "  2. Launch Antigravity and open active workspace: $RepoRoot" -ForegroundColor Gray
    Write-Host "  3. Confirm 4 vetted MCPs (codebase-memory, xerj, nautilus-engine, edgartools) and run:" -ForegroundColor Gray
    Write-Host "     pwsh -NoProfile -ExecutionPolicy Bypass -File $RepoRoot\scripts\grand_reset\verify_grand_reset.ps1" -ForegroundColor Gray
}

Write-Host "[GRAND-RESET 00 OK] Moved=$MovedCount | LockedSkipped=$LockedCount | Protected=$($ProtectedRoots.Count) | LivePorts=0" -ForegroundColor Green

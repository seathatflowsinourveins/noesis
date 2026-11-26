#Requires -Version 7.0
<#
.SYNOPSIS
    Idempotent standalone zero-hook setup & sync for Antigravity (AGY), Claude Code, Codex, OpenCode, and Orca.
.DESCRIPTION
    Syncs strictly from standalone Z:\noesis ($RepoRoot):
      - Preserve official upstream Orca status hooks (~/.orca/agent-hooks/*.cmd) in ~/.gemini/settings.json
      - 4-runtime native uv/venv MCP parity (codebase-memory, xerj, nautilus-engine, edgartools)
      - Fresh AGY (~/.gemini/settings.json, ~/.gemini/config/mcp_config.json, zero-copy ~/.gemini/config/skills.json)
      - Claude Code (.claude/settings.json, agents, commands, mcp-lanes, skills, ~/.claude/skills)
      - Codex (.codex/config.toml, ~/.codex/skills)
      - OpenCode (opencode.json, .opencode/commands)
      - Orca (orca.yaml + clean lane launch readiness)
      - All 46 vetted skills synced from $RepoRoot\.claude\skills to Claude/Codex and zero-copy referenced by AGY
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)),
    [string]$HomeDir = $env:USERPROFILE,
    [string]$SkillsSourceDir = "",
    [switch]$DryRun,
    [switch]$SkipSkillsSync
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedMcps = @("codebase-memory", "xerj", "nautilus-engine", "edgartools")
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)

# Fail-closed live broker port check
$LivePorts = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
if ($LivePorts.Count -ne 0) {
    throw "Refused setup: forbidden live broker ports detected ($($LivePorts.Count))."
}

if ([string]::IsNullOrWhiteSpace($SkillsSourceDir)) {
    $SkillsSourceDir = Join-Path $RepoRoot ".claude\skills"
}

$LockPath = Join-Path $RepoRoot "UPSTREAM_SKILLS_LOCK.json"
$LockedSkills = @()
if (Test-Path -LiteralPath $LockPath) {
    $lockObj = Get-Content -LiteralPath $LockPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $LockedSkills = @($lockObj.skills | ForEach-Object { [string]$_.skill })
}

$RepoGeminiSettings = Join-Path $RepoRoot ".gemini\settings.json"
$RepoMcpJson = Join-Path $RepoRoot ".mcp.json"
$RepoClaudeSettings = Join-Path $RepoRoot ".claude\settings.json"
$RepoCodexConfig = Join-Path $RepoRoot ".codex\config.toml"
$LegacyCodexHooks = Join-Path $RepoRoot ".codex\hooks.json"
$RepoOpenCode = Join-Path $RepoRoot "opencode.json"
$RepoOrcaYaml = Join-Path $RepoRoot "orca.yaml"

foreach ($req in @($RepoGeminiSettings, $RepoMcpJson, $RepoClaudeSettings, $RepoCodexConfig, $RepoOpenCode, $RepoOrcaYaml)) {
    if (-not (Test-Path -LiteralPath $req)) {
        throw "Required repository config missing: $req"
    }
}

# Validate repo 4-runtime MCP parity strictly before touching user home
foreach ($cfgSpec in @(
    @{ Name = ".mcp.json"; Path = $RepoMcpJson; Key = "mcpServers" },
    @{ Name = ".claude/settings.json"; Path = $RepoClaudeSettings; Key = "mcpServers" },
    @{ Name = ".gemini/settings.json"; Path = $RepoGeminiSettings; Key = "mcpServers" },
    @{ Name = "opencode.json"; Path = $RepoOpenCode; Key = "mcp" }
)) {
    $obj = Get-Content -LiteralPath $cfgSpec.Path -Raw -Encoding UTF8 | ConvertFrom-Json
    $names = @($obj.($cfgSpec.Key).PSObject.Properties.Name | Sort-Object)
    if (($names -join ",") -ne (($ExpectedMcps | Sort-Object) -join ",")) {
        throw "MCP parity check failed in $($cfgSpec.Name): got [$($names -join ',')]"
    }
}
$codexToml = Get-Content -LiteralPath $RepoCodexConfig -Raw -Encoding UTF8
$codexMcps = @(
    [regex]::Matches($codexToml, '^\[mcp_servers\.([A-Za-z0-9_-]+)\]', [System.Text.RegularExpressions.RegexOptions]::Multiline) |
        ForEach-Object { $_.Groups[1].Value } |
        Sort-Object -Unique
)
if (($codexMcps -join ",") -ne (($ExpectedMcps | Sort-Object) -join ",")) {
    throw "MCP parity check failed in .codex/config.toml: got [$($codexMcps -join ',')]"
}

# Check Zero-Hook status across repo runtimes
$ClaudeObj = Get-Content -LiteralPath $RepoClaudeSettings -Raw -Encoding UTF8 | ConvertFrom-Json
$GeminiObj = Get-Content -LiteralPath $RepoGeminiSettings -Raw -Encoding UTF8 | ConvertFrom-Json
$HooksZero = (
    (-not (Test-Path -LiteralPath $LegacyCodexHooks)) -and
    (-not ($ClaudeObj.PSObject.Properties.Name -contains "hooks")) -and
    (-not ($GeminiObj.PSObject.Properties.Name -contains "hooks")) -and
    ($codexToml -notmatch '(?m)^\s*hooks\s*=\s*true\b')
)

$AgyDir = Join-Path $HomeDir ".gemini"
$AgyConfigDir = Join-Path $AgyDir "config"
$AgySettingsTarget = Join-Path $AgyDir "settings.json"
$AgyMcpTarget = Join-Path $AgyConfigDir "mcp_config.json"
$AgySkillsConfigTarget = Join-Path $AgyConfigDir "skills.json"
$RepoRootSlash = ($RepoRoot.Replace('\', '/').TrimEnd('/'))
$SkillsSourceSlash = ($SkillsSourceDir.Replace('\', '/').TrimEnd('/'))

function Get-PersistedEnvVar {
    param([string]$Name)
    foreach ($scope in @("User", "Machine", "Process")) {
        $v = [Environment]::GetEnvironmentVariable($Name, $scope)
        if (-not [string]::IsNullOrWhiteSpace($v)) {
            return $v.Trim()
        }
    }
    return ""
}

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

$EdgarServerSpec = [ordered]@{
    command = "$RepoRootSlash/.venv/Scripts/edgartools-mcp.exe"
    args    = @()
}
$EdgarIdentityVal = Get-PersistedEnvVar -Name "EDGAR_IDENTITY"
if (-not [string]::IsNullOrWhiteSpace($EdgarIdentityVal)) {
    $EdgarServerSpec["env"] = [ordered]@{
        EDGAR_IDENTITY = $EdgarIdentityVal
    }
}

$CleanMcpServers = [ordered]@{
    "codebase-memory" = [ordered]@{
        command = "codebase-memory-mcp"
        args    = @()
    }
    "xerj" = [ordered]@{
        command = "xerj"
        args    = @("mcp")
        env     = [ordered]@{
            XERJ_URL              = "http://127.0.0.1:9200"
            XERJ_DISABLE_FEEDBACK = "true"
        }
    }
    "nautilus-engine" = [ordered]@{
        command = "$RepoRootSlash/.venv/Scripts/python.exe"
        args    = @("$RepoRootSlash/scripts/nautilus_mcp_server.py")
        env     = [ordered]@{
            PYTHONPATH              = "$RepoRootSlash/src;$RepoRootSlash"
            PYTHONDONTWRITEBYTECODE = "1"
        }
    }
    "edgartools" = $EdgarServerSpec
}

$ClaudeDir = Join-Path $HomeDir ".claude"
$CodexDir = Join-Path $HomeDir ".codex"
$OpenCodeConfigDir = Join-Path $HomeDir ".config\opencode"

$SkillTargets = @(
    (Join-Path $ClaudeDir "skills"),
    (Join-Path $CodexDir "skills")
)

$CliChecks = [ordered]@{}
foreach ($cmd in @("claude", "codex", "gemini", "opencode", "orca", "uv", "codebase-memory-mcp", "xerj")) {
    $CliChecks[$cmd] = ($null -ne (Get-Command $cmd -ErrorAction SilentlyContinue))
}

if ($DryRun) {
    Write-Host "[DRY-RUN] Grand-Reset Step 02: Standalone Zero-Hook 4-Runtime + Fresh AGY + Orca Sync" -ForegroundColor Yellow
    Write-Host "  RepoRoot        : $RepoRoot" -ForegroundColor Cyan
    Write-Host "  HomeDir         : $HomeDir" -ForegroundColor Cyan
    Write-Host "  MCP Parity      : OK (5/5 repo configs -> $($ExpectedMcps -join ', '))" -ForegroundColor Green
    Write-Host "  Hooks Zero      : $HooksZero (no .codex/hooks.json, no runtime hooks)" -ForegroundColor Cyan
    Write-Host "  Locked Skills   : $($LockedSkills.Count) skill(s) in UPSTREAM_SKILLS_LOCK.json (source=$SkillsSourceDir)" -ForegroundColor Cyan
    Write-Host "  AGY Targets     : $AgySettingsTarget, $AgyMcpTarget, $AgySkillsConfigTarget" -ForegroundColor Cyan
    Write-Host "  Claude Targets  : $RepoRoot\.claude\, $($SkillTargets[0])" -ForegroundColor Cyan
    Write-Host "  Codex Targets   : $RepoRoot\.codex\, $($SkillTargets[1])" -ForegroundColor Cyan
    Write-Host "  OpenCode Target : $RepoRoot\opencode.json, $OpenCodeConfigDir\opencode.json" -ForegroundColor Cyan
    Write-Host "  Orca Config     : $RepoOrcaYaml" -ForegroundColor Cyan
    Write-Host "  CLI Availability: $(($CliChecks.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ' | ')" -ForegroundColor Gray
    return
}

$Timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")

function Save-BackupIfExists {
    param([string]$TargetPath)
    if (Test-Path -LiteralPath $TargetPath) {
        $bak = "$TargetPath.bak-grand-reset-$Timestamp"
        Copy-Item -LiteralPath $TargetPath -Destination $bak -Force
    }
}

function Write-Utf8NoBom {
    param([string]$Path, [string]$Content)
    $parent = Split-Path -Parent $Path
    if ($parent) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    [System.IO.File]::WriteAllText($Path, $Content, [System.Text.UTF8Encoding]::new($false))
}

# 1. Sync AGY (~/.gemini/settings.json preserving Orca hooks, ~/.gemini/config/mcp_config.json, & ~/.gemini/config/skills.json)
New-Item -ItemType Directory -Path $AgyConfigDir -Force | Out-Null
$ExistingOrcaHooks = $null
if (Test-Path -LiteralPath $AgySettingsTarget) {
    try {
        $ExistingAgyObj = Get-Content -LiteralPath $AgySettingsTarget -Raw -Encoding UTF8 | ConvertFrom-Json
        $ExistingOrcaHooks = Get-PreservedOrcaHooks -SettingsObj $ExistingAgyObj
    } catch {
        $ExistingOrcaHooks = $null
    }
}
$AgySettingsObj = Get-Content -LiteralPath $RepoGeminiSettings -Raw -Encoding UTF8 | ConvertFrom-Json
if ($AgySettingsObj.PSObject.Properties.Name -contains "hooks") {
    $AgySettingsObj.PSObject.Properties.Remove("hooks")
}
if ($null -ne $ExistingOrcaHooks) {
    $AgySettingsObj | Add-Member -MemberType NoteProperty -Name "hooks" -Value $ExistingOrcaHooks -Force
}
$AgySettingsObj | Add-Member -MemberType NoteProperty -Name "mcpServers" -Value $CleanMcpServers -Force
Save-BackupIfExists -TargetPath $AgySettingsTarget
Write-Utf8NoBom -Path $AgySettingsTarget -Content (($AgySettingsObj | ConvertTo-Json -Depth 10) + "`n")

$AgyMcpPayload = [ordered]@{ mcpServers = $CleanMcpServers }
Save-BackupIfExists -TargetPath $AgyMcpTarget
Write-Utf8NoBom -Path $AgyMcpTarget -Content (($AgyMcpPayload | ConvertTo-Json -Depth 10) + "`n")

$AgySkillsPayload = [ordered]@{
    entries = @(
        [ordered]@{ path = $SkillsSourceSlash }
    )
}
Save-BackupIfExists -TargetPath $AgySkillsConfigTarget
Write-Utf8NoBom -Path $AgySkillsConfigTarget -Content (($AgySkillsPayload | ConvertTo-Json -Depth 5) + "`n")

# 2. Sync OpenCode global config mirror (without touching secrets)
New-Item -ItemType Directory -Path $OpenCodeConfigDir -Force | Out-Null
$OpenCodeUserTarget = Join-Path $OpenCodeConfigDir "opencode.json"
Save-BackupIfExists -TargetPath $OpenCodeUserTarget
Copy-Item -LiteralPath $RepoOpenCode -Destination $OpenCodeUserTarget -Force

# 3. Sync vetted standalone skills strictly from $RepoRoot\.claude\skills across AGY, Claude, and Codex
$SyncedSkillsCount = 0
if (-not $SkipSkillsSync -and $LockedSkills.Count -gt 0 -and (Test-Path -LiteralPath $SkillsSourceDir)) {
    foreach ($tRoot in $SkillTargets) {
        New-Item -ItemType Directory -Path $tRoot -Force | Out-Null
    }
    foreach ($skillName in $LockedSkills) {
        $srcSkill = Join-Path $SkillsSourceDir $skillName
        if (-not (Test-Path -LiteralPath $srcSkill)) { continue }
        $SyncedSkillsCount++
        foreach ($tRoot in $SkillTargets) {
            $dstSkill = Join-Path $tRoot $skillName
            if ($srcSkill.TrimEnd('\') -eq $dstSkill.TrimEnd('\')) { continue }
            New-Item -ItemType Directory -Path $dstSkill -Force | Out-Null
            Copy-Item -Path (Join-Path $srcSkill "*") -Destination $dstSkill -Recurse -Force
        }
    }
}

# 4. Verify Orca CLI + worktree discovery (read-only)
$OrcaWorktrees = 0
if ($CliChecks["orca"]) {
    try {
        $wtJson = orca worktree list --json | ConvertFrom-Json
        if ($wtJson.ok) {
            $OrcaWorktrees = [int]$wtJson.result.totalCount
        }
    } catch {
        Write-Verbose "Orca daemon not running or unreachable; skipped live worktree count."
    }
}

Write-Host ("[GRAND-RESET 02 OK] Standalone Zero-Hook AGY + 4-Runtime MCPs synced (hooks_zero={0}) | LockedSkills={1}/{2} | OrcaWorktrees={3} | LivePorts=0" -f `
    $HooksZero, $SyncedSkillsCount, $LockedSkills.Count, $OrcaWorktrees) -ForegroundColor Green

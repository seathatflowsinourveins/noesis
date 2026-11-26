#Requires -Version 7.0
<#
.SYNOPSIS
    Idempotent Tier-0 SOTA Ecosystem Preflight & Post-Restart Seamless Resume Kit (Windows).
.DESCRIPTION
    Verifies and optionally reconciles the standalone Z:\noesis workstation after any
    Windows, WSL2, or Antigravity (AGY) restart:
      - Fail-closed zero live broker ports (:4001/:7496/:8798/:8796)
      - Default WSL2 distribution == Noesis-SOTA
      - Optional -Reconcile: starts inactive WSL2 systemd services (tier0-duckdb-ui,
        tier0-openbb-backend, tier0-ollama-docker-bridge, tier0-openbb-docker-bridge) and runs
        docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml up -d
      - Optional -CleanLegacyLocked: invokes Z:\workspaces\finish_post_restart_cleanup.ps1 cleanly
      - Probes all 15/15 local portals (including real otelcol_* lines on :9464/metrics and
        Server: Caddy header on :8793)
      - Verifies Disk-Z persisted env vars, AGY native MCP config, 8/8 Windows+WSL2 Global Charter
        SHA-256, immutable risk.py SHA-256, git status & origin, and Paper Farm preflight readiness
      - Atomically writes Z:\sota-data\manifests\workstation_preflight_resume_manifest.json
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$DataRoot = "",
    [string]$ManifestPath = "",
    [string]$ExpectedWslDistro = "Noesis-SOTA",
    [switch]$Reconcile,
    [switch]$CleanLegacyLocked,
    [switch]$Json
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedRiskSha = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
$ExpectedCharterSha = "057933bbda0db411bbfbc189c72a3e7b59f1989650e110f8591cf64728555c1c"
$ExpectedOriginSlug = "seathatflowsinourveins/noesis"
$ExpectedMcps = @("codebase-memory", "edgartools", "nautilus-engine", "xerj")
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)
$SystemdUnits = @(
    "tier0-duckdb-ui",
    "tier0-openbb-backend",
    "tier0-ollama-docker-bridge",
    "tier0-openbb-docker-bridge"
)
$ReadyLanes = @("sim-backtest", "paper", "half-auto-tv", "live")
$CleanupScriptPath = "Z:\workspaces\finish_post_restart_cleanup.ps1"

function Get-PersistedEnvValue {
    param([string]$Name)
    $val = [Environment]::GetEnvironmentVariable($Name, "User")
    if ([string]::IsNullOrWhiteSpace($val)) {
        $val = [Environment]::GetEnvironmentVariable($Name, "Machine")
    }
    if ([string]::IsNullOrWhiteSpace($val)) {
        $val = [Environment]::GetEnvironmentVariable($Name, "Process")
    }
    return $(if ($null -eq $val) { "" } else { $val.Trim() })
}

if ([string]::IsNullOrWhiteSpace($DataRoot)) {
    $persistedDataRoot = Get-PersistedEnvValue -Name "TIER0_DATA_ROOT"
    $DataRoot = if (-not [string]::IsNullOrWhiteSpace($persistedDataRoot)) {
        $persistedDataRoot
    } else {
        "Z:\sota-data"
    }
}
$ManifestDir = Join-Path $DataRoot "manifests"
if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $ManifestPath = Join-Path $ManifestDir "workstation_preflight_resume_manifest.json"
}

$Failures = [System.Collections.Generic.List[string]]::new()
$ReconcileActions = [System.Collections.Generic.List[object]]::new()
$Gates = [ordered]@{}

function Record-Gate {
    param(
        [string]$Name,
        [bool]$Passed,
        [string]$Detail,
        [hashtable]$Meta = @{}
    )
    $entry = [ordered]@{
        ok     = $Passed
        detail = $Detail
    }
    foreach ($k in $Meta.Keys) {
        $entry[$k] = $Meta[$k]
    }
    $Gates[$Name] = $entry
    if (-not $Passed) {
        $Failures.Add("${Name}: $Detail")
    }
    if (-not $Json) {
        $tag = if ($Passed) { "OK" } else { "FAIL" }
        $color = if ($Passed) { "Green" } else { "Red" }
        Write-Host ("[{0}] {1}: {2}" -f $tag, $Name, $Detail) -ForegroundColor $color
    }
}

function Write-InfoLine {
    param([string]$Tag, [string]$Message, [string]$Color = "Cyan")
    if (-not $Json) {
        Write-Host ("[{0}] {1}" -f $Tag, $Message) -ForegroundColor $Color
    }
}

function Test-TcpPortOpen {
    param([string]$HostName, [int]$Port, [int]$TimeoutMs = 250)
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
            return $false
        }
        $client.EndConnect($iar)
        return $true
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Invoke-PortalProbes {
    $portalSpecs = @(
        @{ Name = "xerj";             Url = "http://127.0.0.1:9200/" },
        @{ Name = "zensical";         Url = "http://127.0.0.1:8010/" },
        @{ Name = "mlflow";           Url = "http://127.0.0.1:5000/" },
        @{ Name = "dagster";          Url = "http://127.0.0.1:3000/" },
        @{ Name = "optuna-dashboard"; Url = "http://127.0.0.1:8080/" },
        @{ Name = "inspect-view";     Url = "http://127.0.0.1:7575/" },
        @{ Name = "prometheus";       Url = "http://127.0.0.1:9090/" },
        @{ Name = "grafana";          Url = "http://127.0.0.1:3001/" },
        @{ Name = "otel-telemetry";   Url = "http://127.0.0.1:9464/metrics" },
        @{ Name = "tearsheets";       Url = "http://127.0.0.1:8793/" },
        @{ Name = "finos-workspace";  Url = "http://127.0.0.1:1420/" },
        @{ Name = "finos-api";        Url = "http://127.0.0.1:8000/health" },
        @{ Name = "openbb-backend";   Url = "http://127.0.0.1:6900/" },
        @{ Name = "duckdb-ui";        Url = "http://127.0.0.1:4213/" },
        @{ Name = "otel-health";      Url = "http://127.0.0.1:13133/" }
    )

    $results = [System.Collections.Generic.List[object]]::new()
    foreach ($spec in $portalSpecs) {
        $statusCode = 0
        $ok = $false
        $note = ""
        try {
            $resp = Invoke-WebRequest -Uri $spec.Url -TimeoutSec 3 -UseBasicParsing -ErrorAction Stop
            $statusCode = [int]$resp.StatusCode
            $httpOk = ($statusCode -ge 200 -and $statusCode -lt 400)
            $ok = $httpOk

            if ($httpOk -and $spec.Name -eq "otel-telemetry") {
                $body = [string]$resp.Content
                $otelCount = ([regex]::Matches($body, '(?m)^otelcol_\w+')).Count
                $ok = ($otelCount -gt 0)
                $note = "otelcol_lines=$otelCount"
            } elseif ($httpOk -and $spec.Name -eq "tearsheets") {
                $serverHdr = [string]($resp.Headers["Server"] -join ",")
                $ok = ($serverHdr -match '(?i)\bCaddy\b')
                $note = "server=$serverHdr"
            } elseif ($httpOk -and $spec.Name -eq "zensical") {
                $body = [string]$resp.Content
                $canonical = ($body -match 'noesis')
                $ok = $canonical
                $note = "canonical_title=$canonical"
            } elseif ($httpOk -and $spec.Name -eq "finos-api") {
                $body = ([string]$resp.Content).Trim()
                $ok = ($body -match '"success"\s*:\s*true')
                $note = "health_ok=$ok"
            }
        } catch {
            $respProp = $_.Exception.PSObject.Properties["Response"]
            if ($null -ne $respProp -and $null -ne $respProp.Value) {
                $statusCode = [int]$respProp.Value.StatusCode
            }
            $note = $_.Exception.Message
        }

        $results.Add([ordered]@{
            name        = $spec.Name
            url         = $spec.Url
            status_code = $statusCode
            ok          = $ok
            note        = $note
        })
    }
    return @($results)
}

# 1. Fail-closed zero live broker ports (4001/7496/8798/8796)
$LiveListeners = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
$LiveProbes = @($ForbiddenLivePorts | Where-Object { Test-TcpPortOpen -HostName "127.0.0.1" -Port $_ -TimeoutMs 250 })
Record-Gate -Name "live_broker_ports_zero" `
    -Passed ($LiveListeners.Count -eq 0 -and $LiveProbes.Count -eq 0) `
    -Detail "listeners=$($LiveListeners.Count) probes=$($LiveProbes.Count) forbidden=$($ForbiddenLivePorts -join ',')" `
    -Meta @{
        forbidden_ports = $ForbiddenLivePorts
        listeners_count = $LiveListeners.Count
        open_probes     = $LiveProbes
    }

# 2. Default WSL2 distro == Noesis-SOTA
$DefaultDistro = ""
$DefaultDistroState = ""
$DefaultDistroVersion = ""
$WslCheckOk = $false
if (Get-Command wsl.exe -ErrorAction SilentlyContinue) {
    $prevOutEnc = [Console]::OutputEncoding
    try {
        [Console]::OutputEncoding = [System.Text.Encoding]::Unicode
        $wslLines = @(& wsl.exe -l -v 2>&1)
    } finally {
        [Console]::OutputEncoding = $prevOutEnc
    }
    foreach ($rawLine in $wslLines) {
        $cleanLine = ([string]$rawLine -replace "`0", "").Trim()
        if ($cleanLine -match '^\*\s+(\S+)\s+(\S+)\s+(\d+)\s*$') {
            $DefaultDistro = $Matches[1]
            $DefaultDistroState = $Matches[2]
            $DefaultDistroVersion = $Matches[3]
            break
        }
    }
    $WslCheckOk = ($DefaultDistro -eq $ExpectedWslDistro -and $DefaultDistroVersion -eq "2")
}
Record-Gate -Name "wsl_default_distro" `
    -Passed $WslCheckOk `
    -Detail "default=$DefaultDistro state=$DefaultDistroState version=$DefaultDistroVersion expected=$ExpectedWslDistro" `
    -Meta @{
        expected = $ExpectedWslDistro
        actual   = $DefaultDistro
        state    = $DefaultDistroState
        version  = $DefaultDistroVersion
    }

# 3. Optional -CleanLegacyLocked (idempotent post-restart cleanup via allowlisted script)
if ($CleanLegacyLocked) {
    $cleanupOk = $false
    $cleanupDetail = ""
    if (Test-Path -LiteralPath $CleanupScriptPath) {
        Write-InfoLine -Tag "CLEANUP" -Message "Invoking $CleanupScriptPath..."
        $cleanOut = & pwsh -NoProfile -ExecutionPolicy Bypass -File $CleanupScriptPath 2>&1
        $cleanExit = $LASTEXITCODE
        $cleanupOk = ($cleanExit -eq 0)
        $cleanupDetail = "exit=$cleanExit output=$((@($cleanOut) -join ' | ').Trim())"
    } else {
        $cleanupDetail = "missing script at $CleanupScriptPath"
    }
    $ReconcileActions.Add([ordered]@{
        action = "clean_legacy_locked"
        ok     = $cleanupOk
        detail = $cleanupDetail
    })
    Record-Gate -Name "legacy_locked_cleanup" -Passed $cleanupOk -Detail $cleanupDetail
}

# 4. Optional -Reconcile (WSL2 systemd services + Docker Compose stack)
if ($Reconcile) {
    if ($WslCheckOk) {
        Write-InfoLine -Tag "RECONCILE" -Message "Ensuring WSL2 systemd services in $ExpectedWslDistro..."
        $unitList = ($SystemdUnits -join " ")
        $systemdReconcileCmd = "set -eu; for u in $unitList; do systemctl is-active --quiet `$u || systemctl start `$u; done"
        $sysRecOut = (& wsl.exe -d $ExpectedWslDistro -u root -- sh -c $systemdReconcileCmd 2>&1) -join "; "
        $sysRecOk = ($LASTEXITCODE -eq 0)
        $ReconcileActions.Add([ordered]@{
            action = "wsl_systemd_reconcile"
            ok     = $sysRecOk
            detail = if ($sysRecOk) { "units ensured active ($unitList)" } else { $sysRecOut }
        })

        Write-InfoLine -Tag "RECONCILE" -Message "Running Docker Compose up -d in $ExpectedWslDistro..."
        $composeReconcileCmd = 'TIER0_REPO_ROOT=/mnt/z/noesis TIER0_DATA_ROOT=/mnt/z/sota-data TIER0_STATE_ROOT="$HOME/tier0-sota-state" docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml up -d'
        $compRecOut = (& wsl.exe -d $ExpectedWslDistro -- bash -lc $composeReconcileCmd 2>&1) -join "; "
        $compRecOk = ($LASTEXITCODE -eq 0)
        $ReconcileActions.Add([ordered]@{
            action = "docker_compose_up"
            ok     = $compRecOk
            detail = if ($compRecOk) { "docker compose -p docker up -d succeeded" } else { $compRecOut }
        })
    } else {
        $ReconcileActions.Add([ordered]@{
            action = "reconcile_skipped"
            ok     = $false
            detail = "Default WSL2 distro is '$DefaultDistro' (expected '$ExpectedWslDistro')"
        })
    }
}

# 5. Verify WSL2 systemd services & Docker Compose stack
$SystemdStates = [ordered]@{}
$SystemdActiveCount = 0
if ($WslCheckOk) {
    $unitStatusCmd = ($SystemdUnits | ForEach-Object { "printf '%s=%s\n' '$_' `"`$(systemctl is-active '$_' 2>/dev/null || echo inactive)`"" }) -join "; "
    $unitLines = @(& wsl.exe -d $ExpectedWslDistro -- bash -lc $unitStatusCmd 2>&1)
    foreach ($ul in $unitLines) {
        if ([string]$ul -match '^([A-Za-z0-9_.-]+)=([A-Za-z0-9_.-]+)$') {
            $uName = $Matches[1]
            $uState = $Matches[2]
            $SystemdStates[$uName] = $uState
            if ($uState -eq "active") {
                $SystemdActiveCount++
            }
        }
    }
}
Record-Gate -Name "wsl_systemd_services" `
    -Passed ($SystemdActiveCount -eq $SystemdUnits.Count) `
    -Detail "active=$SystemdActiveCount/$($SystemdUnits.Count)" `
    -Meta @{ units = $SystemdStates }

$DockerRunning = 0
$DockerTotal = 0
$DockerDetail = "wsl_unavailable"
if ($WslCheckOk) {
    $dockerPsCmd = 'docker compose -p docker -f /mnt/z/noesis/docker/compose.sota.yml ps --format "{{.Name}}|{{.State}}"'
    $dockerLines = @(& wsl.exe -d $ExpectedWslDistro -- bash -lc $dockerPsCmd 2>&1)
    if ($LASTEXITCODE -eq 0) {
        foreach ($dl in $dockerLines) {
            if ([string]$dl -match '^([^|]+)\|(.+)$') {
                $DockerTotal++
                if ($Matches[2].Trim() -eq "running") {
                    $DockerRunning++
                }
            }
        }
        $DockerDetail = "running=$DockerRunning/$DockerTotal"
    } else {
        $DockerDetail = "docker compose ps failed: $($dockerLines -join '; ')"
    }
}
Record-Gate -Name "docker_compose_stack" `
    -Passed ($DockerTotal -ge 10 -and $DockerRunning -eq $DockerTotal) `
    -Detail $DockerDetail `
    -Meta @{
        running = $DockerRunning
        total   = $DockerTotal
    }

# 6. Probe all 15/15 local portals (with bounded settle wait after -Reconcile)
$PortalResults = Invoke-PortalProbes
$HealthyPortals = @($PortalResults | Where-Object { $_.ok }).Count
if ($Reconcile -and $HealthyPortals -lt $PortalResults.Count) {
    for ($attempt = 1; $attempt -le 5; $attempt++) {
        Start-Sleep -Seconds 3
        $PortalResults = Invoke-PortalProbes
        $HealthyPortals = @($PortalResults | Where-Object { $_.ok }).Count
        if ($HealthyPortals -eq $PortalResults.Count) {
            break
        }
    }
}
$DownPortals = @($PortalResults | Where-Object { -not $_.ok } | ForEach-Object { "$($_.name)($($_.status_code):$($_.note))" })
$OtelResult = $PortalResults | Where-Object { $_.name -eq "otel-telemetry" } | Select-Object -First 1
$CaddyResult = $PortalResults | Where-Object { $_.name -eq "tearsheets" } | Select-Object -First 1
Record-Gate -Name "local_portals_15" `
    -Passed ($HealthyPortals -eq 15) `
    -Detail ("healthy={0}/15 {1} {2}{3}" -f $HealthyPortals, $OtelResult.note, $CaddyResult.note, $(if ($DownPortals.Count -gt 0) { " down=$($DownPortals -join ', ')" } else { "" })) `
    -Meta @{
        healthy = $HealthyPortals
        total   = $PortalResults.Count
        down    = $DownPortals
    }

# 7. Disk Z environment variables (persisted User/Machine env + directories on disk)
$ExpectedEnv = [ordered]@{
    TIER0_REPO_ROOT     = "Z:\noesis"
    TIER0_DATA_ROOT     = "Z:\sota-data"
    ORCA_WORKSPACES_DIR = "Z:\workspaces"
}
$EnvReport = [ordered]@{}
$EnvOk = $true
foreach ($k in $ExpectedEnv.Keys) {
    $expectedVal = $ExpectedEnv[$k]
    $persistedVal = Get-PersistedEnvValue -Name $k
    $processVal = [Environment]::GetEnvironmentVariable($k, "Process")
    $dirExists = Test-Path -LiteralPath $expectedVal
    $matchesPersisted = ($persistedVal -eq $expectedVal)
    if (-not ($matchesPersisted -and $dirExists)) {
        $EnvOk = $false
    }
    $EnvReport[$k] = [ordered]@{
        expected         = $expectedVal
        persisted        = $persistedVal
        process          = $(if ($null -eq $processVal) { "" } else { $processVal })
        path_exists      = $dirExists
        persisted_ok     = $matchesPersisted
        session_stale    = ($processVal -ne $expectedVal)
    }
}
$StaleSessionVars = @($EnvReport.Keys | Where-Object { $EnvReport[$_].session_stale })
Record-Gate -Name "disk_z_env" `
    -Passed $EnvOk `
    -Detail ("persisted_3x3={0} session_needs_restart_vars={1}" -f $EnvOk, $(if ($StaleSessionVars.Count -eq 0) { "none" } else { $StaleSessionVars -join "," })) `
    -Meta @{ variables = $EnvReport }

# 8. AGY native MCP config ($env:USERPROFILE\.gemini\config\mcp_config.json)
$AgyMcpPath = Join-Path $env:USERPROFILE ".gemini\config\mcp_config.json"
$AgyMcpOk = $false
$AgyMcpServers = @()
$AgyMcpDetail = "missing $AgyMcpPath"
if (Test-Path -LiteralPath $AgyMcpPath) {
    try {
        $mcpRaw = Get-Content -LiteralPath $AgyMcpPath -Raw -Encoding UTF8
        $mcpObj = $mcpRaw | ConvertFrom-Json
        $AgyMcpServers = @($mcpObj.mcpServers.PSObject.Properties.Name | Sort-Object)
        $namesMatch = (($AgyMcpServers -join ",") -eq (($ExpectedMcps | Sort-Object) -join ","))
        $noBashWrapper = ($mcpRaw -notmatch '"bash"')
        $missingBins = [System.Collections.Generic.List[string]]::new()
        foreach ($srvName in $AgyMcpServers) {
            $cmdPath = [string]$mcpObj.mcpServers.$srvName.command
            if (-not [string]::IsNullOrWhiteSpace($cmdPath) -and ($cmdPath -match '^[A-Za-z]:[/\\]')) {
                if (-not (Test-Path -LiteralPath $cmdPath)) {
                    $missingBins.Add("${srvName}:$cmdPath")
                }
            }
        }
        $nautCmd = [string]$mcpObj.mcpServers.'nautilus-engine'.command
        $edgarCmd = [string]$mcpObj.mcpServers.edgartools.command
        $diskZClean = (($nautCmd -match 'noesis') -and ($edgarCmd -match 'noesis'))
        $AgyMcpOk = ($namesMatch -and $noBashWrapper -and $missingBins.Count -eq 0 -and $diskZClean)
        $AgyMcpDetail = "servers=$($AgyMcpServers -join ',') native_no_bash=$noBashWrapper disk_z_clean=$diskZClean missing_bins=$($missingBins.Count)"
    } catch {
        $AgyMcpDetail = "parse error: $($_.Exception.Message)"
    }
}
Record-Gate -Name "agy_native_mcp_config" `
    -Passed $AgyMcpOk `
    -Detail $AgyMcpDetail `
    -Meta @{
        path    = $AgyMcpPath
        servers = $AgyMcpServers
    }

# 9. 8/8 Windows + WSL2 byte-identical Global Charter SHA-256
$WinCharterPaths = @(
    (Join-Path $env:USERPROFILE ".claude\CLAUDE.md"),
    (Join-Path $env:USERPROFILE ".codex\AGENTS.md"),
    (Join-Path $env:USERPROFILE ".gemini\GEMINI.md"),
    (Join-Path $env:USERPROFILE ".config\opencode\AGENTS.md")
)
$CharterMatches = 0
$CharterEntries = [System.Collections.Generic.List[object]]::new()
foreach ($wp in $WinCharterPaths) {
    $sha = "missing"
    $ok = $false
    if (Test-Path -LiteralPath $wp) {
        $sha = (Get-FileHash -LiteralPath $wp -Algorithm SHA256).Hash.ToLowerInvariant()
        $ok = ($sha -eq $ExpectedCharterSha)
    }
    if ($ok) { $CharterMatches++ }
    $CharterEntries.Add([ordered]@{ side = "windows"; path = $wp; sha256 = $sha; ok = $ok })
}

$WslCharterRelPaths = @(
    ".claude/CLAUDE.md",
    ".codex/AGENTS.md",
    ".gemini/GEMINI.md",
    ".config/opencode/AGENTS.md"
)
if ($WslCheckOk) {
    $wslHashCmd = 'cd && sha256sum .claude/CLAUDE.md .codex/AGENTS.md .gemini/GEMINI.md .config/opencode/AGENTS.md'
    $wslHashLines = @(& wsl.exe -d $ExpectedWslDistro -- bash -lc $wslHashCmd 2>&1)
    $wslSeen = @{}
    foreach ($hl in $wslHashLines) {
        if ([string]$hl -match '^([0-9a-fA-F]{64})\s+\*?(.+)$') {
            $wSha = $Matches[1].ToLowerInvariant()
            $wRel = $Matches[2].Trim()
            $wslSeen[$wRel] = $wSha
        }
    }
    foreach ($rel in $WslCharterRelPaths) {
        $wSha = if ($wslSeen.ContainsKey($rel)) { $wslSeen[$rel] } else { "missing" }
        $wOk = ($wSha -eq $ExpectedCharterSha)
        if ($wOk) { $CharterMatches++ }
        $CharterEntries.Add([ordered]@{ side = "wsl2"; path = "~/$rel"; sha256 = $wSha; ok = $wOk })
    }
} else {
    foreach ($rel in $WslCharterRelPaths) {
        $CharterEntries.Add([ordered]@{ side = "wsl2"; path = "~/$rel"; sha256 = "wsl_unavailable"; ok = $false })
    }
}
Record-Gate -Name "global_charter_sha256_8x8" `
    -Passed ($CharterMatches -eq 8 -and $CharterEntries.Count -eq 8) `
    -Detail "matched=$CharterMatches/8 expected=$ExpectedCharterSha" `
    -Meta @{
        expected_sha256 = $ExpectedCharterSha
        matched         = $CharterMatches
        total           = 8
        files           = @($CharterEntries)
    }

# 10. Immutable risk.py SHA-256
$RiskPath = Join-Path $RepoRoot "src\tier0_sota\risk.py"
$RiskSha = "missing"
$RiskOk = $false
if (Test-Path -LiteralPath $RiskPath) {
    $RiskSha = (Get-FileHash -LiteralPath $RiskPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $RiskOk = ($RiskSha -eq $ExpectedRiskSha)
}
Record-Gate -Name "risk_kernel_sha256" `
    -Passed $RiskOk `
    -Detail $RiskSha `
    -Meta @{
        path            = $RiskPath
        expected_sha256 = $ExpectedRiskSha
        actual_sha256   = $RiskSha
    }

# 11. Git status & origin (seathatflowsinourveins/noesis)
$GitOk = $false
$GitOrigin = ""
$GitBranch = ""
$GitHead = ""
$GitDirtyCount = 0
$GitUnmergedCount = 0
if (Get-Command git -ErrorAction SilentlyContinue) {
    $originOut = (& git -C $RepoRoot remote get-url origin 2>&1)
    if ($LASTEXITCODE -eq 0) {
        $GitOrigin = ([string]($originOut | Select-Object -First 1)).Trim()
    }
    $branchOut = (& git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>&1)
    if ($LASTEXITCODE -eq 0) {
        $GitBranch = ([string]($branchOut | Select-Object -First 1)).Trim()
    }
    $headOut = (& git -C $RepoRoot rev-parse --short HEAD 2>&1)
    if ($LASTEXITCODE -eq 0) {
        $GitHead = ([string]($headOut | Select-Object -First 1)).Trim()
    }
    $statusLines = @(& git -C $RepoRoot status --porcelain 2>&1)
    $statusExit = $LASTEXITCODE
    if ($statusExit -eq 0) {
        $nonEmpty = @($statusLines | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
        $GitDirtyCount = $nonEmpty.Count
        $GitUnmergedCount = @($nonEmpty | Where-Object { [string]$_ -match '^(UU|AA|DD|AU|UA|DU|UD)\s' }).Count
    }
    $originMatches = ($GitOrigin -match 'seathatflowsinourveins/noesis(?:\.git)?$')
    $GitOk = ($statusExit -eq 0 -and $originMatches -and -not [string]::IsNullOrWhiteSpace($GitBranch) -and $GitUnmergedCount -eq 0)
}
Record-Gate -Name "git_status_and_origin" `
    -Passed $GitOk `
    -Detail "branch=$GitBranch head=$GitHead origin=$GitOrigin dirty=$GitDirtyCount unmerged=$GitUnmergedCount" `
    -Meta @{
        branch         = $GitBranch
        head_short     = $GitHead
        origin         = $GitOrigin
        expected_slug  = $ExpectedOriginSlug
        clean          = ($GitDirtyCount -eq 0)
        dirty_count    = $GitDirtyCount
        unmerged_count = $GitUnmergedCount
    }

# 12. Paper Farm preflight readiness
$PaperScript = Join-Path $RepoRoot "scripts\run_live_paper_farm.ps1"
$PaperPreflightOk = $false
$PaperPreflightOut = "missing script"
$PerfManifestPath = Join-Path $ManifestDir "paper_farm_live_performance_manifest.json"
$PerfManifestOk = $false

if (Test-Path -LiteralPath $PaperScript) {
    $prevRepoEnv = $env:TIER0_REPO_ROOT
    $prevDataEnv = $env:TIER0_DATA_ROOT
    try {
        $env:TIER0_REPO_ROOT = $RepoRoot
        $env:TIER0_DATA_ROOT = $DataRoot
        $pfRaw = & pwsh -NoProfile -ExecutionPolicy Bypass -File $PaperScript -Mode preflight -ManifestDir $ManifestDir 2>&1
        $pfExit = $LASTEXITCODE
        $PaperPreflightOut = ((@($pfRaw) -join " ").Trim())
        $PaperPreflightOk = ($pfExit -eq 0 -and $PaperPreflightOut -match 'PREFLIGHT_OK')
    } finally {
        $env:TIER0_REPO_ROOT = $prevRepoEnv
        $env:TIER0_DATA_ROOT = $prevDataEnv
    }
}

if (Test-Path -LiteralPath $PerfManifestPath) {
    try {
        $perfDoc = Get-Content -LiteralPath $PerfManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $PerfManifestOk = ($perfDoc.paper_only -eq $true -and $perfDoc.live_execution_enabled -eq $false)
    } catch {
        $PerfManifestOk = $false
    }
}
Record-Gate -Name "paper_farm_preflight" `
    -Passed ($PaperPreflightOk -and $PerfManifestOk) `
    -Detail "preflight=$PaperPreflightOk manifest_safe=$PerfManifestOk output=$PaperPreflightOut" `
    -Meta @{
        preflight_ok      = $PaperPreflightOk
        perf_manifest_ok  = $PerfManifestOk
        perf_manifest     = $PerfManifestPath
    }

# 13. OmniRoute local router (:20128) & OAuth account pool readiness
$OmniUrl = "http://127.0.0.1:20128/api/health"
$OmniHealthy = $false
try {
    $omniResp = Invoke-WebRequest -Uri $OmniUrl -TimeoutSec 3 -UseBasicParsing -ErrorAction Stop
    $omniBody = $omniResp.Content | ConvertFrom-Json
    $OmniHealthy = ($omniResp.StatusCode -eq 200 -and $omniBody.status -eq "ok")
} catch {
    $OmniHealthy = $false
}
if (-not $OmniHealthy -and $Reconcile) {
    Write-InfoLine -Tag "RECONCILE" -Message "Starting OmniRoute daemon hidden (:20128)..."
    $nodeExe = Join-Path $env:LOCALAPPDATA "mise\installs\node\global\node.exe"
    $omniMjs = Get-ChildItem (Join-Path $env:LOCALAPPDATA "mise\installs\node") -Filter "omniroute.mjs" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty FullName
    $startOk = $false
    if ((Test-Path -LiteralPath $nodeExe) -and $omniMjs) {
        Start-Process -FilePath $nodeExe -ArgumentList @($omniMjs, "serve", "--no-open", "--no-tray") -WindowStyle Hidden | Out-Null
        $startOk = $true
    } elseif (Get-Command omniroute -ErrorAction SilentlyContinue) {
        Start-Process -FilePath "omniroute" -ArgumentList @("serve", "--daemon", "--no-open", "--no-tray") -WindowStyle Hidden | Out-Null
        $startOk = $true
    }
    if ($startOk) {
        for ($i = 1; $i -le 10; $i++) {
            Start-Sleep -Seconds 1
            try {
                $omniResp2 = Invoke-WebRequest -Uri $OmniUrl -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
                $omniBody2 = $omniResp2.Content | ConvertFrom-Json
                if ($omniResp2.StatusCode -eq 200 -and $omniBody2.status -eq "ok") {
                    $OmniHealthy = $true
                    break
                }
            } catch {
                $OmniHealthy = $false
            }
        }
    }
    $ReconcileActions.Add([ordered]@{
        action = "omniroute_start"
        ok     = $OmniHealthy
        detail = if ($OmniHealthy) { "http://127.0.0.1:20128/api/health ok" } else { "failed to start :20128" }
    })
}

$OmniDbPath = Join-Path $env:USERPROFILE ".omniroute\storage.sqlite"
$OmniConnActive = 0
$OmniAutostartVbs = Test-Path -LiteralPath (Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup\OmniRoute.vbs")
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if ((Test-Path -LiteralPath $OmniDbPath) -and (Test-Path -LiteralPath $VenvPy)) {
    $cntOut = & $VenvPy -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); print(c.execute('SELECT count(*) FROM provider_connections WHERE is_active=1').fetchone()[0])" $OmniDbPath 2>$null
    if ($LASTEXITCODE -eq 0 -and ([string]$cntOut).Trim() -match '^\d+$') {
        $OmniConnActive = [int]([string]$cntOut).Trim()
    }
}
Record-Gate -Name "omniroute_local_router" `
    -Passed ($OmniHealthy -and $OmniConnActive -ge 1) `
    -Detail "health=$OmniHealthy active_oauth_connections=$OmniConnActive autostart_vbs=$OmniAutostartVbs" `
    -Meta @{
        url                       = $OmniUrl
        healthy                   = $OmniHealthy
        active_oauth_connections  = $OmniConnActive
        autostart_vbs_present     = $OmniAutostartVbs
    }

# 14. Orca native multi-account readiness & zero global API-key CLI override
$OrcaAccountsOk = $false
$ClaudeAccountsCount = 0
$CodexAccountsCount = 0
$ClaudeModelUnrestricted = $false
$LeakedGlobalAuthVars = [System.Collections.Generic.List[string]]::new()
foreach ($gk in @("ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "OPENAI_API_KEY", "OPENAI_BASE_URL", "GEMINI_API_KEY")) {
    foreach ($sc in @("User", "Machine", "Process")) {
        $gv = [Environment]::GetEnvironmentVariable($gk, $sc)
        if (-not [string]::IsNullOrWhiteSpace($gv)) {
            $LeakedGlobalAuthVars.Add("${sc}:$gk")
        }
    }
}
$WinClaudeSettings = Join-Path $env:USERPROFILE ".claude\settings.json"
if (Test-Path -LiteralPath $WinClaudeSettings) {
    try {
        $cs = Get-Content -LiteralPath $WinClaudeSettings -Raw -Encoding UTF8 | ConvertFrom-Json
        $ClaudeModelUnrestricted = (-not ($cs.PSObject.Properties.Name -contains "availableModels"))
    } catch {
        $ClaudeModelUnrestricted = $false
    }
}
if (Get-Command orca -ErrorAction SilentlyContinue) {
    try {
        $orcaRaw = (& orca account list --json 2>&1) -join "`n"
        if ($LASTEXITCODE -eq 0) {
            $orcaDoc = $orcaRaw | ConvertFrom-Json
            if ($orcaDoc.ok -eq $true) {
                $ClaudeAccountsCount = @($orcaDoc.result.claude.accounts).Count
                $CodexAccountsCount = @($orcaDoc.result.codex.accounts).Count
                $OrcaAccountsOk = ($ClaudeAccountsCount -ge 1 -and $CodexAccountsCount -ge 1 -and $LeakedGlobalAuthVars.Count -eq 0 -and $ClaudeModelUnrestricted)
            }
        }
    } catch {
        $OrcaAccountsOk = $false
    }
}
Record-Gate -Name "orca_multi_account_readiness" `
    -Passed $OrcaAccountsOk `
    -Detail "claude_oauth=$ClaudeAccountsCount codex_oauth=$CodexAccountsCount model_unrestricted=$ClaudeModelUnrestricted leaked_global_keys=$($LeakedGlobalAuthVars.Count)" `
    -Meta @{
        claude_oauth_accounts     = $ClaudeAccountsCount
        codex_oauth_accounts      = $CodexAccountsCount
        claude_model_unrestricted = $ClaudeModelUnrestricted
        leaked_global_auth_vars   = @($LeakedGlobalAuthVars)
    }

# Build & atomically write workstation_preflight_resume_manifest.json
$GatesPassed = @($Gates.Values | Where-Object { $_.ok }).Count
$GatesTotal = $Gates.Count
$AllPassed = ($Failures.Count -eq 0)
$Verdict = if ($AllPassed) { "PASS" } else { "FAIL" }

$ManifestObj = [ordered]@{
    schema_version                = "tier0.workstation-preflight-resume.v1"
    generated_at_utc              = [datetimeoffset]::UtcNow.ToString("o")
    platform                      = "windows"
    repo_root                     = $RepoRoot
    data_root                     = $DataRoot
    manifest_path                 = $ManifestPath
    reconcile_requested           = [bool]$Reconcile
    clean_legacy_locked_requested = [bool]$CleanLegacyLocked
    verdict                       = $Verdict
    passed                        = $AllPassed
    live_execution_enabled        = $false
    paper_only                    = $true
    gates_passed                  = $GatesPassed
    gates_total                   = $GatesTotal
    ready_lanes                   = $ReadyLanes
    portals_healthy               = $HealthyPortals
    portals_total                 = $PortalResults.Count
    portals                       = $PortalResults
    reconcile_actions             = @($ReconcileActions)
    failures                      = @($Failures)
    gates                         = $Gates
}

$manifestJson = ($ManifestObj | ConvertTo-Json -Depth 8) + "`n"
$manifestParent = Split-Path -Parent $ManifestPath
if (-not (Test-Path -LiteralPath $manifestParent)) {
    New-Item -ItemType Directory -Path $manifestParent -Force | Out-Null
}
$tmpPath = "$ManifestPath.tmp.$PID.$([guid]::NewGuid().ToString('N'))"
try {
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($tmpPath, $manifestJson, $utf8NoBom)
    [System.IO.File]::Move($tmpPath, $ManifestPath, $true)
} finally {
    if (Test-Path -LiteralPath $tmpPath) {
        Remove-Item -LiteralPath $tmpPath -Force -ErrorAction SilentlyContinue
    }
}

if ($Json) {
    Write-Output $manifestJson.TrimEnd()
} else {
    $vColor = if ($AllPassed) { "Green" } else { "Red" }
    Write-Host ("[VERDICT: {0}] Workstation Preflight & Resume ({1}/{2} gates, {3}/15 portals, lanes={4}) -> {5}" -f `
        $Verdict, $GatesPassed, $GatesTotal, $HealthyPortals, ($ReadyLanes -join ","), $ManifestPath) -ForegroundColor $vColor
}

if (-not $AllPassed) {
    exit 1
}
exit 0

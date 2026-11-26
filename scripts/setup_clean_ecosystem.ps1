#Requires -Version 7.0
<#
.SYNOPSIS
    Idempotent clean ecosystem setup & health check for Z:\noesis.
.DESCRIPTION
    Verifies:
      1. Clean .venv (Python 3.13 + pinned upstream imports) and immutable risk.py SHA-256
      2. 4-runtime MCP parity (.mcp.json, Claude, Codex, Gemini, OpenCode)
      3. 13 hosted SOTA portals HTTP health on 127.0.0.1
      4. 4-account Paper Farm readiness (ALPACA_PA1..PA3 + IBKR_DU_4002)
      5. Fail-closed zero live broker ports (:4001, :7496, :8798, :8796)
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$ManifestDir = $(if ($env:TIER0_DATA_ROOT) { Join-Path $env:TIER0_DATA_ROOT "manifests" } else { "Z:\sota-data\manifests" }),
    [switch]$SyncVenv,
    [switch]$Strict,
    [switch]$Json
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedRiskSha = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
$ExpectedMcps = @("codebase-memory", "xerj", "nautilus-engine", "edgartools")
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)
$Warnings = [System.Collections.Generic.List[string]]::new()
$Failures = [System.Collections.Generic.List[string]]::new()

function Write-CheckLine {
    param([string]$Tag, [string]$Message, [string]$Color = "Gray")
    if (-not $Json) {
        Write-Host ("[{0}] {1}" -f $Tag, $Message) -ForegroundColor $Color
    }
}

function Test-TcpPortOpen {
    param([string]$HostName, [int]$Port, [int]$TimeoutMs = 400)
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

# 1. Fail-closed live broker port firewall (always first)
$ListeningLive = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
$ProbeLive = @($ForbiddenLivePorts | Where-Object { Test-TcpPortOpen -HostName "127.0.0.1" -Port $_ -TimeoutMs 250 })
$LivePortsDetected = @($ListeningLive.Count -gt 0 -or $ProbeLive.Count -gt 0)
if ($ListeningLive.Count -gt 0 -or $ProbeLive.Count -gt 0) {
    $msg = "FORBIDDEN live broker ports active (listeners=$($ListeningLive.Count), probes=$($ProbeLive -join ','))"
    $Failures.Add($msg)
    Write-CheckLine -Tag "FAIL" -Message $msg -Color "Red"
} else {
    Write-CheckLine -Tag "OK" -Message "Live broker firewall clean: ports 4001/7496/8798/8796 = 0" -Color "Green"
}

# 2. Virtual environment & immutable risk.py check
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if ($SyncVenv -or -not (Test-Path -LiteralPath $VenvPy)) {
    $Bootstrap = Join-Path $RepoRoot "scripts\bootstrap_env.ps1"
    if (Test-Path -LiteralPath $Bootstrap) {
        Write-CheckLine -Tag "SYNC" -Message "Running bootstrap_env.ps1 -Frozen..." -Color "Cyan"
        & pwsh -NoProfile -ExecutionPolicy Bypass -File $Bootstrap -Frozen | Out-Null
        if ($LASTEXITCODE -ne 0) {
            $Failures.Add("bootstrap_env.ps1 failed with exit code $LASTEXITCODE")
        }
    }
}

$VenvOk = $false
$ImportSummary = "unavailable"
if (Test-Path -LiteralPath $VenvPy) {
    $env:PYTHONDONTWRITEBYTECODE = "1"
    $env:PYTHONPATH = Join-Path $RepoRoot "src"
    $ImportOut = & $VenvPy -c @"
import sys, nautilus_trader, hftbacktest, polars, duckdb, alpaca, skfolio, ml4t.diagnostic, ml4t.engineer
print(f"py={sys.version.split()[0]} nautilus={nautilus_trader.__version__} polars={polars.__version__}")
"@ 2>&1
    if ($LASTEXITCODE -eq 0) {
        $VenvOk = $true
        $ImportSummary = ($ImportOut | Select-Object -Last 1).ToString().Trim()
        Write-CheckLine -Tag "OK" -Message ".venv ready: $ImportSummary" -Color "Green"
    } else {
        $Failures.Add(".venv core upstream import check failed: $ImportOut")
        Write-CheckLine -Tag "FAIL" -Message ".venv core import failed" -Color "Red"
    }
} else {
    $Failures.Add("Missing $VenvPy")
    Write-CheckLine -Tag "FAIL" -Message "Missing $VenvPy" -Color "Red"
}

$RiskPath = Join-Path $RepoRoot "src\tier0_sota\risk.py"
$RiskSha = "missing"
$RiskOk = $false
if (Test-Path -LiteralPath $RiskPath) {
    $RiskSha = (Get-FileHash -LiteralPath $RiskPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $RiskOk = ($RiskSha -eq $ExpectedRiskSha)
    if ($RiskOk) {
        Write-CheckLine -Tag "OK" -Message "risk.py SHA-256 verified ($RiskSha)" -Color "Green"
    } else {
        $Failures.Add("risk.py SHA-256 mismatch: $RiskSha != $ExpectedRiskSha")
        Write-CheckLine -Tag "FAIL" -Message "risk.py SHA-256 mismatch" -Color "Red"
    }
} else {
    $Warnings.Add("risk.py not found at $RiskPath")
    Write-CheckLine -Tag "WARN" -Message "risk.py not found at $RiskPath" -Color "Yellow"
}

# 3. 4-runtime MCP parity check (.mcp.json, Claude, Codex, Gemini, OpenCode)
$McpStatus = [ordered]@{}
$JsonConfigs = [ordered]@{
    root_mcp = @{ Path = (Join-Path $RepoRoot ".mcp.json"); Key = "mcpServers" }
    claude   = @{ Path = (Join-Path $RepoRoot ".claude\settings.json"); Key = "mcpServers" }
    gemini   = @{ Path = (Join-Path $RepoRoot ".gemini\settings.json"); Key = "mcpServers" }
    opencode = @{ Path = (Join-Path $RepoRoot "opencode.json"); Key = "mcp" }
}
foreach ($entry in $JsonConfigs.GetEnumerator()) {
    $cfgPath = $entry.Value.Path
    if (-not (Test-Path -LiteralPath $cfgPath)) {
        $McpStatus[$entry.Key] = @{ ok = $false; servers = @() }
        $Failures.Add("Missing MCP config: $cfgPath")
        continue
    }
    $parsed = Get-Content -LiteralPath $cfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $section = $parsed.($entry.Value.Key)
    $names = @($section.PSObject.Properties.Name | Sort-Object)
    $expectedSorted = @($ExpectedMcps | Sort-Object)
    $match = (($names -join ",") -eq ($expectedSorted -join ","))
    $McpStatus[$entry.Key] = @{ ok = $match; servers = $names }
    if (-not $match) {
        $Failures.Add("MCP parity mismatch in $($entry.Key): got [$($names -join ',')] expected [$($expectedSorted -join ',')]")
    }
}

$CodexPath = Join-Path $RepoRoot ".codex\config.toml"
if (Test-Path -LiteralPath $CodexPath) {
    $codexRaw = Get-Content -LiteralPath $CodexPath -Raw -Encoding UTF8
    $codexMatches = @(
        [regex]::Matches($codexRaw, '^\[mcp_servers\.([A-Za-z0-9_-]+)\]', [System.Text.RegularExpressions.RegexOptions]::Multiline) |
            ForEach-Object { $_.Groups[1].Value } |
            Sort-Object -Unique
    )
    $expectedSorted = @($ExpectedMcps | Sort-Object)
    $codexOk = (($codexMatches -join ",") -eq ($expectedSorted -join ","))
    $McpStatus["codex"] = @{ ok = $codexOk; servers = $codexMatches }
    if (-not $codexOk) {
        $Failures.Add("MCP parity mismatch in codex: got [$($codexMatches -join ',')]")
    }
} else {
    $McpStatus["codex"] = @{ ok = $false; servers = @() }
    $Failures.Add("Missing Codex config: $CodexPath")
}

$McpParityOk = (@($McpStatus.Values | Where-Object { -not $_.ok }).Count -eq 0)
if ($McpParityOk) {
    Write-CheckLine -Tag "OK" -Message "4-runtime MCP parity verified (5/5 configs -> $($ExpectedMcps -join ', '))" -Color "Green"
} else {
    Write-CheckLine -Tag "FAIL" -Message "4-runtime MCP parity mismatch detected" -Color "Red"
}

$NautilusMcpScript = Join-Path $RepoRoot "scripts\nautilus_mcp_server.py"
if (-not (Test-Path -LiteralPath $NautilusMcpScript)) {
    $Warnings.Add("Optional MCP helper scripts/nautilus_mcp_server.py not yet present in $RepoRoot")
    Write-CheckLine -Tag "WARN" -Message "scripts/nautilus_mcp_server.py missing (referenced by nautilus-engine MCP)" -Color "Yellow"
}

# 4. 13 hosted SOTA portals health check
$Portals = @(
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
    @{ Name = "openbb-backend";   Url = "http://127.0.0.1:6900/" }
)

$PortalResults = @()
$HealthyPortals = 0
foreach ($p in $Portals) {
    $code = 0
    $ok = $false
    try {
        $resp = Invoke-WebRequest -Uri $p.Url -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
        $code = [int]$resp.StatusCode
        $ok = ($code -ge 200 -and $code -lt 400)
    } catch {
        if ($_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
        }
    }
    if ($ok) { $HealthyPortals++ }
    $PortalResults += [ordered]@{ name = $p.Name; url = $p.Url; status_code = $code; ok = $ok }
}

if ($HealthyPortals -eq $Portals.Count) {
    Write-CheckLine -Tag "OK" -Message "Hosted portals healthy: $HealthyPortals/$($Portals.Count) HTTP 200" -Color "Green"
} else {
    $down = @($PortalResults | Where-Object { -not $_.ok } | ForEach-Object { "$($_.name):$($_.status_code)" })
    $Warnings.Add("Hosted portals partial: $HealthyPortals/$($Portals.Count) healthy (down: $($down -join ', '))")
    Write-CheckLine -Tag "WARN" -Message "Hosted portals: $HealthyPortals/$($Portals.Count) healthy (down: $($down -join ', '))" -Color "Yellow"
}

# 5. Paper Farm readiness (ALPACA_PA1..PA3 + IBKR_DU_4002)
$PaperFarmAccounts = @()
foreach ($acct in @("ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3")) {
    $slug = $acct.ToLowerInvariant()
    $laneTag = $slug.Replace("alpaca_", "")
    $statusFile = Join-Path $ManifestDir "${slug}_live_automation_status.json"
    $duckFiles = @(Get-ChildItem -LiteralPath $ManifestDir -Filter "*${laneTag}*.duckdb" -File -ErrorAction SilentlyContinue)
    $paperOnly = $false
    $liveDisabled = $false
    $heartbeat = ""
    $fresh = $false
    if (Test-Path -LiteralPath $statusFile) {
        try {
            $st = Get-Content -LiteralPath $statusFile -Raw -Encoding UTF8 | ConvertFrom-Json
            $paperOnly = ($st.paper_only -eq $true)
            $liveDisabled = ($st.live_execution_enabled -eq $false)
            $heartbeat = [string]$st.heartbeat_utc
            if ($heartbeat) {
                $dt = [datetimeoffset]::Parse($heartbeat)
                $fresh = (([datetimeoffset]::UtcNow - $dt).TotalSeconds -lt 180)
            }
            if (-not $paperOnly -or -not $liveDisabled) {
                $Failures.Add("$acct status failed paper_only/live_execution_enabled safety check")
            }
        } catch {
            $Warnings.Add("Could not parse $statusFile")
        }
    } else {
        $Warnings.Add("Status manifest missing for $acct at $statusFile")
    }
    $PaperFarmAccounts += [ordered]@{
        account                = $acct
        status_present         = (Test-Path -LiteralPath $statusFile)
        duckdb_present         = ($duckFiles.Count -gt 0)
        paper_only             = $paperOnly
        live_execution_enabled = $liveDisabled
        heartbeat_utc          = $heartbeat
        heartbeat_fresh        = $fresh
    }
}

$Ibkr4002Open = Test-TcpPortOpen -HostName "127.0.0.1" -Port 4002 -TimeoutMs 350
$PaperPreflightOk = $false
$PaperFarmScript = Join-Path $RepoRoot "scripts\run_live_paper_farm.ps1"
if (Test-Path -LiteralPath $PaperFarmScript) {
    $pfOut = & pwsh -NoProfile -ExecutionPolicy Bypass -File $PaperFarmScript -Mode preflight -ManifestDir $ManifestDir 2>&1
    $PaperPreflightOk = ($LASTEXITCODE -eq 0)
    if (-not $PaperPreflightOk) {
        $Failures.Add("run_live_paper_farm.ps1 -Mode preflight failed: $pfOut")
    }
} else {
    $Warnings.Add("Missing scripts/run_live_paper_farm.ps1")
}

$FreshCount = @($PaperFarmAccounts | Where-Object { $_.heartbeat_fresh }).Count
Write-CheckLine -Tag "INFO" -Message "Paper Farm: preflight=$PaperPreflightOk | ALPACA_PA1..PA3 fresh=$FreshCount/3 | IBKR_DU_4002_port4002_open=$Ibkr4002Open" -Color "Cyan"

$Summary = [ordered]@{
    schema_version       = "tier0.clean-ecosystem-check.v1"
    repo_root            = $RepoRoot
    manifest_dir         = $ManifestDir
    venv_ok              = $VenvOk
    venv_summary         = $ImportSummary
    risk_sha256_ok       = $RiskOk
    risk_sha256          = $RiskSha
    mcp_parity_ok        = $McpParityOk
    mcp_configs          = $McpStatus
    portals_healthy      = $HealthyPortals
    portals_total        = $Portals.Count
    portals              = $PortalResults
    paper_preflight_ok   = $PaperPreflightOk
    paper_accounts       = $PaperFarmAccounts
    ibkr_du_4002_open    = $Ibkr4002Open
    live_ports_forbidden = $ForbiddenLivePorts
    live_ports_clear     = ($ListeningLive.Count -eq 0 -and $ProbeLive.Count -eq 0)
    warnings             = @($Warnings)
    failures             = @($Failures)
}

if ($Json) {
    $Summary | ConvertTo-Json -Depth 6
}

if ($Failures.Count -gt 0 -or ($Strict -and $Warnings.Count -gt 0)) {
    throw "Clean ecosystem check failed ($($Failures.Count) failure(s), $($Warnings.Count) warning(s))."
}

Write-CheckLine -Tag "VERDICT: PASS" -Message "Clean ecosystem verified ($HealthyPortals/$($Portals.Count) portals, 5/5 MCP parity, Paper preflight=$PaperPreflightOk, live_ports=0)." -Color "Green"

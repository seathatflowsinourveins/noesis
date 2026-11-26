#Requires -Version 7.0
<#
.SYNOPSIS
    Master 12-gate end-to-end verifier for the standalone Z:\noesis Zero-Hook Grand-Reset blueprint.
.DESCRIPTION
    Checks all 12 master gates:
      1. Zero live broker ports (:4001/:7496/:8798/:8796)
      2. Immutable risk.py SHA-256 (ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847)
      3. ARCHITECTURE_MANIFEST.json across L0..L10 (11 layers) + all declared repository_files on disk
      4. Living MADR 4.0 ADRs (ADR-0001..ADR-0022+), CATALOG.md, docs/index.md, zensical.toml, and zensical build --clean
      5. 5/5 native uv MCP parity configs (zero bash -c wrappers) + scripts/nautilus_mcp_server.py --smoke (10/10 tools)
      6. 15/15 hosted portals HTTP 200 on 127.0.0.1
      7. TradingView Pine v6 balance + JSON schema_v1.json safety invariants
      8. Paper Farm preflight + Z:\sota-data\manifests\paper_farm_live_performance_manifest.json
      9. Zero self-invented hooks (hooks_zero=True) + .claude/mcp-lanes/*.mcp.json + SOTA_STARRED_AND_BEYOND_MANIFEST.json + UPSTREAM_SKILLS_LOCK.json + paper-gatekeeper.md
     10. gitleaks detect --no-git = 0 leaks
     11. uv_lock_zero_yfinance_and_standalone (0 yfinance in uv.lock, 46/46 standalone .claude/skills/, 0 legacy refs in src/docs/pyproject.toml/manifests)
     12. z3_formal_risk_and_inspect_evals (evals/formal_risk_z3.py UNSAT PASS + evals/tier0_inspect_eval.py OK)
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)),
    [string]$ManifestDir = $(if ($env:TIER0_DATA_ROOT) { Join-Path $env:TIER0_DATA_ROOT "manifests" } else { "Z:\sota-data\manifests" }),
    [switch]$Json
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedRiskSha = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
$ExpectedMcps = @("codebase-memory", "xerj", "nautilus-engine", "edgartools")
$ForbiddenLivePorts = @(4001, 7496, 8798, 8796)
$Failures = [System.Collections.Generic.List[string]]::new()
$Results = [ordered]@{}

function Record-Gate {
    param([string]$Name, [bool]$Passed, [string]$Detail)
    $Results[$Name] = [ordered]@{ ok = $Passed; detail = $Detail }
    if (-not $Passed) {
        $Failures.Add("${Name}: $Detail")
    }
    if (-not $Json) {
        $tag = if ($Passed) { "OK" } else { "FAIL" }
        $color = if ($Passed) { "Green" } else { "Red" }
        Write-Host ("[{0}] {1}: {2}" -f $tag, $Name, $Detail) -ForegroundColor $color
    }
}

# 1. Zero live broker ports (fail closed)
$LiveListeners = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $ForbiddenLivePorts -or $_.RemotePort -in $ForbiddenLivePorts }
)
$LiveProbes = @()
foreach ($p in $ForbiddenLivePorts) {
    $c = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $c.BeginConnect("127.0.0.1", $p, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne(250, $false)) {
            $c.EndConnect($iar)
            $LiveProbes += $p
        }
    } catch {
        # Expected closed port
    } finally {
        $c.Dispose()
    }
}
Record-Gate -Name "live_broker_ports_zero" -Passed ($LiveListeners.Count -eq 0 -and $LiveProbes.Count -eq 0) `
    -Detail "listeners=$($LiveListeners.Count) probes=$($LiveProbes.Count)"

# 2. Risk kernel SHA-256
$RiskPath = Join-Path $RepoRoot "src\tier0_sota\risk.py"
$RiskOk = $false
$RiskSha = "missing"
if (Test-Path -LiteralPath $RiskPath) {
    $RiskSha = (Get-FileHash -LiteralPath $RiskPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $RiskOk = ($RiskSha -eq $ExpectedRiskSha)
}
Record-Gate -Name "risk_kernel_sha256" -Passed $RiskOk -Detail $RiskSha

# 3. ARCHITECTURE_MANIFEST.json across L0..L10 (11 layers)
$ArchPath = Join-Path $RepoRoot "ARCHITECTURE_MANIFEST.json"
$ArchOk = $false
$ArchDetail = "missing"
if (Test-Path -LiteralPath $ArchPath) {
    $arch = Get-Content -LiteralPath $ArchPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $layers = @($arch.layers.PSObject.Properties.Name)
    $expectedLayers = @("L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10")
    $missingLayers = @($expectedLayers | Where-Object { $_ -notin $layers })
    $missingFiles = @($arch.repository_files | Where-Object { -not (Test-Path -LiteralPath (Join-Path $RepoRoot $_)) })
    $safetyOk = ($arch.safety_invariants.live_execution_enabled -eq $false -and $arch.safety_invariants.paper_only -eq $true)
    $ArchOk = ($missingLayers.Count -eq 0 -and $missingFiles.Count -eq 0 -and $safetyOk -and $arch.risk_kernel.sha256 -eq $ExpectedRiskSha)
    $ArchDetail = "layers=$($layers.Count)/11 missing_files=$($missingFiles.Count) safety=$safetyOk"
}
Record-Gate -Name "architecture_manifest" -Passed $ArchOk -Detail $ArchDetail

# 4. Living Evolvable ADRs (ADR-0001+) + CATALOG.md + docs/index.md + zensical.toml + zensical build --clean
$AdrDir = Join-Path $RepoRoot "docs\adr"
$AdrFiles = @(Get-ChildItem -LiteralPath $AdrDir -Filter "ADR-*.md" -File -ErrorAction SilentlyContinue | Sort-Object Name)
$AdrsFound = $AdrFiles.Count
$CatalogPath = Join-Path $AdrDir "CATALOG.md"
$CatalogOk = Test-Path -LiteralPath $CatalogPath
$AllCataloged = $false
if ($CatalogOk -and $AdrsFound -ge 1) {
    $CatalogText = Get-Content -LiteralPath $CatalogPath -Raw -Encoding UTF8
    $MissingAdr = @($AdrFiles | Where-Object { $CatalogText -notmatch [regex]::Escape($_.Name) })
    $AllCataloged = ($MissingAdr.Count -eq 0) -and ($CatalogText -match "(?i)sole source of truth")
}
$IndexOk = Test-Path -LiteralPath (Join-Path $RepoRoot "docs\index.md")
$ZensicalTomlOk = Test-Path -LiteralPath (Join-Path $RepoRoot "zensical.toml")
$ZensicalExe = Join-Path $RepoRoot ".venv\Scripts\zensical.exe"
$ZensicalBuildOk = $false
if ($ZensicalTomlOk -and (Test-Path -LiteralPath $ZensicalExe)) {
    Push-Location $RepoRoot
    try {
        & $ZensicalExe build --clean *> $null
        $ZensicalBuildOk = ($LASTEXITCODE -eq 0)
    } finally {
        Pop-Location
    }
}
Record-Gate -Name "adrs_and_zensical" -Passed ($AdrsFound -ge 1 -and $AllCataloged -and $CatalogOk -and $IndexOk -and $ZensicalTomlOk -and $ZensicalBuildOk) `
    -Detail "adrs=$AdrsFound(living) cataloged=$AllCataloged catalog=$CatalogOk index=$IndexOk toml=$ZensicalTomlOk build=$ZensicalBuildOk"

# 5. 5/5 native uv MCP parity + zero bash -c wrappers + nautilus_mcp_server.py --smoke
$McpOk = $true
$NativeUvMcpOk = $true
foreach ($spec in @(
    @{ Path = (Join-Path $RepoRoot ".mcp.json"); Key = "mcpServers" },
    @{ Path = (Join-Path $RepoRoot ".claude\settings.json"); Key = "mcpServers" },
    @{ Path = (Join-Path $RepoRoot ".gemini\settings.json"); Key = "mcpServers" },
    @{ Path = (Join-Path $RepoRoot "opencode.json"); Key = "mcp" }
)) {
    if (-not (Test-Path -LiteralPath $spec.Path)) { $McpOk = $false; $NativeUvMcpOk = $false; continue }
    $rawTxt = Get-Content -LiteralPath $spec.Path -Raw -Encoding UTF8
    if ($rawTxt -match '"bash"') { $NativeUvMcpOk = $false }
    $obj = $rawTxt | ConvertFrom-Json
    $names = @($obj.($spec.Key).PSObject.Properties.Name | Sort-Object)
    if (($names -join ",") -ne (($ExpectedMcps | Sort-Object) -join ",")) { $McpOk = $false }
}
$CodexCfg = Join-Path $RepoRoot ".codex\config.toml"
if (Test-Path -LiteralPath $CodexCfg) {
    $cRaw = Get-Content -LiteralPath $CodexCfg -Raw -Encoding UTF8
    if ($cRaw -match 'command\s*=\s*"bash"') { $NativeUvMcpOk = $false }
    $cMcps = @(
        [regex]::Matches($cRaw, '^\[mcp_servers\.([A-Za-z0-9_-]+)\]', [System.Text.RegularExpressions.RegexOptions]::Multiline) |
            ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique
    )
    if (($cMcps -join ",") -ne (($ExpectedMcps | Sort-Object) -join ",")) { $McpOk = $false }
} else { $McpOk = $false; $NativeUvMcpOk = $false }

$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$McpSmokeScript = Join-Path $RepoRoot "scripts\nautilus_mcp_server.py"
$McpSmokeOk = $false
$McpToolsCount = 0
if ((Test-Path -LiteralPath $VenvPy) -and (Test-Path -LiteralPath $McpSmokeScript)) {
    $smokeRaw = & $VenvPy $McpSmokeScript --smoke 2>&1
    if ($LASTEXITCODE -eq 0) {
        try {
            $smokeJson = ($smokeRaw -join "`n") | ConvertFrom-Json
            $McpSmokeOk = ($smokeJson.ok -eq $true -and $smokeJson.tools_count -eq 10 -and $smokeJson.live_execution_enabled -eq $false)
            $McpToolsCount = [int]$smokeJson.tools_count
        } catch {
            $McpSmokeOk = $false
        }
    }
}
Record-Gate -Name "mcp_parity_and_smoke" -Passed ($McpOk -and $NativeUvMcpOk -and $McpSmokeOk) `
    -Detail "parity=$McpOk native_uv=$NativeUvMcpOk smoke=$McpSmokeOk tools=$McpToolsCount/10"

# 6. 15/15 hosted portals HTTP 200
$PortalUrls = @(
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
$PortalsUp = 0
$ZensicalCanonicalOk = $false
foreach ($u in $PortalUrls) {
    try {
        $resp = Invoke-WebRequest -Uri $u -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
        if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 400) {
            $PortalsUp++
            if ($u -eq "http://127.0.0.1:8010/") {
                $body = [string]$resp.Content
                $ZensicalCanonicalOk = (
                    ($body -match "noesis") -and
                    ($body -notmatch "UNIFIED_ECOSYSTEM_INVENTORY") -and
                    ($body -notmatch "643\+48 Starred Atlas")
                )
            }
        }
    } catch {
        # Count stays unchanged
    }
}
Record-Gate -Name "hosted_portals_15" -Passed ($PortalsUp -eq 15 -and $ZensicalCanonicalOk) -Detail "$PortalsUp/15 HTTP 200 zensical_canonical=$ZensicalCanonicalOk"

# 7. TradingView Pine v6 balance + schema bridge
$PineFiles = @(
    (Join-Path $RepoRoot "pine\tier0_omni_sota_24x5.pine"),
    (Join-Path $RepoRoot "pine\tier0_market_regime.pine")
)
$PineOk = $true
foreach ($pf in $PineFiles) {
    if (-not (Test-Path -LiteralPath $pf)) { $PineOk = $false; continue }
    $txt = Get-Content -LiteralPath $pf -Raw -Encoding UTF8
    if ($txt -notmatch '//@version=6' -or $txt -match '9223') { $PineOk = $false }
    $pairs = @{ '(' = ')'; '[' = ']'; '{' = '}' }
    $stack = [System.Collections.Generic.Stack[char]]::new()
    foreach ($ch in $txt.ToCharArray()) {
        if ($pairs.ContainsKey([string]$ch)) { $stack.Push($ch) }
        elseif ($pairs.ContainsValue([string]$ch)) {
            if ($stack.Count -eq 0) { $PineOk = $false; break }
            $top = $stack.Pop()
            if ($pairs[[string]$top] -ne [string]$ch) { $PineOk = $false; break }
        }
    }
    if ($stack.Count -ne 0) { $PineOk = $false }
}
$SchemaPath = Join-Path $RepoRoot "src\tier0_sota\tv\schema_v1.json"
$SchemaOk = $false
if (Test-Path -LiteralPath $SchemaPath) {
    $sc = Get-Content -LiteralPath $SchemaPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $SchemaOk = ($sc.properties.semi_auto_confirmed.const -eq $false -and $sc.properties.qty.const -eq 1)
}
Record-Gate -Name "tradingview_pine_and_schema" -Passed ($PineOk -and $SchemaOk) -Detail "pine_v6_balanced=$PineOk schema_v1_safe=$SchemaOk"

# 8. Paper Farm preflight + performance manifest
$PfScript = Join-Path $RepoRoot "scripts\run_live_paper_farm.ps1"
$PfPreflightOk = $false
if (Test-Path -LiteralPath $PfScript) {
    & pwsh -NoProfile -ExecutionPolicy Bypass -File $PfScript -Mode preflight -ManifestDir $ManifestDir *> $null
    $PfPreflightOk = ($LASTEXITCODE -eq 0)
}
$PerfManifest = Join-Path $ManifestDir "paper_farm_live_performance_manifest.json"
$PerfManifestOk = $false
if (Test-Path -LiteralPath $PerfManifest) {
    $pm = Get-Content -LiteralPath $PerfManifest -Raw -Encoding UTF8 | ConvertFrom-Json
    $PerfManifestOk = ($pm.paper_only -eq $true -and $pm.live_execution_enabled -eq $false)
}
Record-Gate -Name "paper_farm_readiness" -Passed ($PfPreflightOk -and $PerfManifestOk) -Detail "preflight=$PfPreflightOk manifest=$PerfManifestOk"

# 9. Zero-Hook Runtimes, Scoped MCP Lanes, Starred Manifest & Skills Lock
$LockPath = Join-Path $RepoRoot "UPSTREAM_SKILLS_LOCK.json"
$GatekeeperPath = Join-Path $RepoRoot ".claude\agents\paper-gatekeeper.md"
$CodexHooksPath = Join-Path $RepoRoot ".codex\hooks.json"
$OpenCodeCmdDir = Join-Path $RepoRoot ".opencode\commands"
$ClaudeSettingsPath = Join-Path $RepoRoot ".claude\settings.json"
$GeminiSettingsPath = Join-Path $RepoRoot ".gemini\settings.json"
$StarredPath = Join-Path $RepoRoot "SOTA_STARRED_AND_BEYOND_MANIFEST.json"

$ClaudeHasHooks = $true
if (Test-Path -LiteralPath $ClaudeSettingsPath) {
    $cs = Get-Content -LiteralPath $ClaudeSettingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $ClaudeHasHooks = ($cs.PSObject.Properties.Name -contains "hooks")
}
$GeminiHasHooks = $true
if (Test-Path -LiteralPath $GeminiSettingsPath) {
    $gs = Get-Content -LiteralPath $GeminiSettingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $GeminiHasHooks = ($gs.PSObject.Properties.Name -contains "hooks")
}
$CodexHasHooks = $true
if (Test-Path -LiteralPath $CodexCfg) {
    $cRaw2 = Get-Content -LiteralPath $CodexCfg -Raw -Encoding UTF8
    $CodexHasHooks = ($cRaw2 -match '(?m)^\s*hooks\s*=\s*true\b')
}
$HooksZeroOk = ((-not (Test-Path -LiteralPath $CodexHooksPath)) -and (-not $ClaudeHasHooks) -and (-not $GeminiHasHooks) -and (-not $CodexHasHooks))

$McpLanesOk = (@(
    @("sim.mcp.json", "paper.mcp.json", "tv.mcp.json", "gate.mcp.json") |
        Where-Object { -not (Test-Path -LiteralPath (Join-Path $RepoRoot ".claude\mcp-lanes\$_")) }
).Count -eq 0)

$StarredOk = $false
if (Test-Path -LiteralPath $StarredPath) {
    $sm = Get-Content -LiteralPath $StarredPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $StarredOk = (
        $sm.schema_version -eq "tier0.sota-starred-beyond-manifest.v1" -and
        $sm.zero_self_invented_hooks -eq $true -and
        ($sm.PSObject.Properties.Name -contains "spdx_license_firewall")
    )
}

$GatekeeperOk = (Test-Path -LiteralPath $GatekeeperPath) -and ((Get-Content -LiteralPath $GatekeeperPath -Raw -Encoding UTF8) -match 'permissionMode:\s*plan')
$RuntimeMetaOk = ($HooksZeroOk -and $McpLanesOk -and $StarredOk -and (Test-Path -LiteralPath $LockPath) -and $GatekeeperOk -and (Test-Path -LiteralPath $OpenCodeCmdDir))
Record-Gate -Name "zero_hook_runtimes_and_manifests" -Passed $RuntimeMetaOk `
    -Detail "hooks_zero=$HooksZeroOk mcp_lanes=$McpLanesOk starred_manifest=$StarredOk skills_lock=$(Test-Path $LockPath) gatekeeper_plan=$GatekeeperOk"

# 10. Gitleaks = 0 leaks
$GitleaksCfg = Join-Path $RepoRoot ".gitleaks.toml"
$GitleaksOk = $false
if (Get-Command gitleaks -ErrorAction SilentlyContinue) {
    & gitleaks detect --no-git --source $RepoRoot --config $GitleaksCfg --no-banner *> $null
    $GitleaksOk = ($LASTEXITCODE -eq 0)
}
Record-Gate -Name "gitleaks_zero_leaks" -Passed $GitleaksOk -Detail "exit_zero=$GitleaksOk"

# 11. Zero transitive yfinance in uv.lock, 46/46 standalone .claude/skills/, and 0 legacy refs
$UvLockPath = Join-Path $RepoRoot "uv.lock"
$YfinanceMatches = if (Test-Path -LiteralPath $UvLockPath) {
    @(Select-String -LiteralPath $UvLockPath -Pattern "yfinance" -CaseSensitive:$false).Count
} else { -1 }

$StandaloneSkillsOk = $false
$StandaloneSkillsCount = 0
if (Test-Path -LiteralPath $LockPath) {
    $lockData = Get-Content -LiteralPath $LockPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $lockedNames = @($lockData.skills | ForEach-Object { [string]$_.skill })
    $skillsRoot = Join-Path $RepoRoot ".claude\skills"
    $presentCount = @(
        $lockedNames | Where-Object {
            Test-Path -LiteralPath (Join-Path (Join-Path $skillsRoot $_) "SKILL.md")
        }
    ).Count
    $StandaloneSkillsCount = $presentCount
    $StandaloneSkillsOk = ($lockedNames.Count -ge 46 -and $presentCount -eq $lockedNames.Count)
}

$LegacyRegex = [string]::Join("|", @(
    ("tier0" + "-quant"),
    ("CLEAN" + " LIVE"),
    ("sota-live-tv" + "-harness"),
    ("sota_compounding_1000rpm" + "_harness")
))
$ScanTargets = [System.Collections.Generic.List[string]]::new()
foreach ($relFile in @("pyproject.toml", "ARCHITECTURE_MANIFEST.json", "SOTA_STARRED_AND_BEYOND_MANIFEST.json")) {
    $p = Join-Path $RepoRoot $relFile
    if (Test-Path -LiteralPath $p) { $ScanTargets.Add($p) }
}
foreach ($relDir in @("src", "docs", "evals", "scripts")) {
    $d = Join-Path $RepoRoot $relDir
    if (Test-Path -LiteralPath $d) {
        foreach ($f in @(Get-ChildItem -LiteralPath $d -Recurse -File -ErrorAction SilentlyContinue | Where-Object { $_.FullName -notmatch '\\__pycache__\\' })) {
            $ScanTargets.Add($f.FullName)
        }
    }
}
$LegacyHits = @(Select-String -Path $ScanTargets.ToArray() -Pattern $LegacyRegex -ErrorAction SilentlyContinue).Count
Record-Gate -Name "uv_lock_zero_yfinance_and_standalone" -Passed ($YfinanceMatches -eq 0 -and $StandaloneSkillsOk -and $LegacyHits -eq 0) `
    -Detail "uv_lock_yfinance=$YfinanceMatches standalone_skills=$StandaloneSkillsCount/46 legacy_refs=$LegacyHits"

# 12. Z3 formal risk UNSAT proof + Inspect AI deterministic safety eval suite
$Z3Script = Join-Path $RepoRoot "evals\formal_risk_z3.py"
$InspectScript = Join-Path $RepoRoot "evals\tier0_inspect_eval.py"
$Z3Ok = $false
$InspectOk = $false
Push-Location $RepoRoot
try {
    $prevPyPath = $env:PYTHONPATH
    $env:PYTHONPATH = "$RepoRoot;$(Join-Path $RepoRoot 'src')"
    try {
        if ((Test-Path -LiteralPath $VenvPy) -and (Test-Path -LiteralPath $Z3Script)) {
            $z3Out = (& $VenvPy $Z3Script 2>&1) -join "`n"
            $Z3Ok = ($LASTEXITCODE -eq 0 -and $z3Out -match 'Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1')
        }
        if ((Test-Path -LiteralPath $VenvPy) -and (Test-Path -LiteralPath $InspectScript)) {
            & $VenvPy -c "from evals.tier0_inspect_eval import verify_inspect_safety_suite; r = verify_inspect_safety_suite(); assert (r.get('ok', True) if isinstance(r, dict) else bool(r))" *> $null
            $InspectOk = ($LASTEXITCODE -eq 0)
        }
    } finally {
        $env:PYTHONPATH = $prevPyPath
    }
} finally {
    Pop-Location
}
Record-Gate -Name "z3_formal_risk_and_inspect_evals" -Passed ($Z3Ok -and $InspectOk) `
    -Detail "z3_unsat=$Z3Ok inspect_eval=$InspectOk"

if ($Json) {
    [ordered]@{
        schema_version = "tier0.grand-reset-verify.v3"
        repo_root      = $RepoRoot
        passed         = ($Failures.Count -eq 0)
        failures       = @($Failures)
        gates          = $Results
    } | ConvertTo-Json -Depth 6
}

if ($Failures.Count -gt 0) {
    throw "Grand-Reset verification FAILED ($($Failures.Count) gate(s)): $($Failures -join '; ')"
}

if (-not $Json) {
    Write-Host "[VERDICT: PASS] Standalone Zero-Hook Grand-Reset Master Verification PASSED (12/12 gates green, hooks_zero=True, 0 live ports)." -ForegroundColor Green
}

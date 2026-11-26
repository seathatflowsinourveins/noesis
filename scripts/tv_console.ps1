#Requires -Version 7.0
<#
.SYNOPSIS
    Tier-0 SOTA TradingView Operator Console (display-only, semi_auto_confirmed=$false).
.DESCRIPTION
    ToU-compliant PowerShell helpers for Pine v6 clipboard staging, Half-Auto extreme-mover
    order tickets, watchlist generation, and offline validation (zero CDP, zero live ports).
#>

$script:Tier0RepoRoot = Split-Path -Parent $PSScriptRoot
$script:Tier0Py       = Join-Path $script:Tier0RepoRoot '.venv\Scripts\python.exe'
$script:Tier0Manifest = 'Z:\sota-data\manifests'
$script:Tier0PackDir  = Join-Path $script:Tier0Manifest 'tv_sota_pack'

function tv-tickets {
    [CmdletBinding()]
    param(
        [string]$ManifestDir = $script:Tier0Manifest,
        [string]$OutDir      = $script:Tier0PackDir
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    & $script:Tier0Py -B -m tier0_sota.tv.tickets --manifest-dir $ManifestDir --out-dir $OutDir
    if ($LASTEXITCODE -ne 0) { throw "tier0_sota.tv.tickets exited with code $LASTEXITCODE" }
    $mdPath = Join-Path $OutDir 'LIVE_HALF_AUTO_ORDER_TICKETS.md'
    if (Test-Path -LiteralPath $mdPath) { Get-Content -LiteralPath $mdPath -Encoding UTF8 }
}

function tv-copy-omni {
    $path = Join-Path $script:Tier0RepoRoot 'pine\tier0_omni_sota_24x5.pine'
    Get-Content -Raw -LiteralPath $path -Encoding UTF8 | Set-Clipboard
    Write-Host "[OK] Copied $path to clipboard -> paste in TradingView Pine Editor." -ForegroundColor Green
}

function tv-copy-regime {
    $path = Join-Path $script:Tier0RepoRoot 'pine\tier0_market_regime.pine'
    Get-Content -Raw -LiteralPath $path -Encoding UTF8 | Set-Clipboard
    Write-Host "[OK] Copied $path to clipboard -> paste in TradingView Pane 1." -ForegroundColor Green
}

function tv-copy-watchlist {
    param(
        [string]$ManifestDir = $script:Tier0Manifest,
        [string]$OutDir      = $script:Tier0PackDir
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    & $script:Tier0Py -B -m tier0_sota.tv.tickets --manifest-dir $ManifestDir --out-dir $OutDir | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "tier0_sota.tv.tickets exited with code $LASTEXITCODE" }
    $wlPath = Join-Path $OutDir 'tier0_sota_tv_watchlists_live.txt'
    $wlPath | Set-Clipboard
    Write-Host "[OK] Refreshed and copied watchlist path to clipboard: $wlPath" -ForegroundColor Green
}

function tv-stage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Symbol,
        [ValidateSet('BUY', 'SELL')][string]$Side = 'BUY',
        [Parameter(Mandatory)][double]$Price,
        [Parameter(Mandatory)][double]$StopPrice,
        [double]$NatrPct = 0.012,
        [ValidateSet('IBKR', 'ALPACA')][string]$Venue = 'IBKR',
        [string]$StagedPath = (Join-Path $script:Tier0PackDir 'staged_tv_presets.json')
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $sym = $Symbol.Trim().ToUpperInvariant()
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("tv_stage_" + [guid]::NewGuid().ToString('N') + '.json')
    try {
        & $script:Tier0Py -B -c @"
import json, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
now_ms = (int(time.time()) // 60) * 60_000
dt_et = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).astimezone(ZoneInfo('America/New_York'))
hm = dt_et.hour * 60 + dt_et.minute
sess = 'RTH' if 570 <= hm < 960 else ('PRE_MARKET' if 240 <= hm < 570 else ('AFTER_HOURS' if 960 <= hm < 1200 else 'OVERNIGHT'))
otype = 'STOP_MARKET' if sess == 'RTH' else 'STOP_LIMIT'
collar = 0 if sess == 'RTH' else (35 if sess in ('PRE_MARKET', 'AFTER_HOURS') else 50)
natr = $NatrPct
sched = 'tight_near_lossless' if natr <= 0.015 else ('baseline' if natr <= 0.030 else 'wide_natr_adaptive')
payload = {
    'schema_version': 'tier0.tv-alert.v1',
    'symbol': '$sym',
    'ts_utc_ms': now_ms,
    'bar_confirmed': True,
    'side': '$Side',
    'qty': 1,
    'venue': '$Venue',
    'session': sess,
    'order_type': otype,
    'trigger_price': $Price,
    'stop_price': $StopPrice,
    'limit_collar_bps': collar,
    'trailing_schedule': sched,
    'natr_pct': natr,
    'auto_reverse': True,
    'auto_reentry': True,
    'reentry_cooldown_secs': 60,
    'strategy_id': 'SignalExecutionStrategy',
    'semi_auto_confirmed': False,
    'data_source': 'tradingview_pine_v6',
}
open(r'$tmp', 'w', encoding='utf-8').write(json.dumps(payload))
"@
        if ($LASTEXITCODE -ne 0) { throw 'Failed to compose staged alert JSON' }
        $res = & $script:Tier0Py -B -m tier0_sota.tv.bridge --ingest-file $tmp --staged-path $StagedPath --venue $Venue
        if ($LASTEXITCODE -ne 0) { throw 'tv.bridge validation failed' }
        ($res -join "`n") | Set-Clipboard
        $res
        Write-Host "[OK] Staged & copied to clipboard (qty=1, semi_auto_confirmed=false)." -ForegroundColor Green
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

function tv-portals {
    foreach ($url in @('http://127.0.0.1:1420/', 'http://127.0.0.1:3001/', 'http://127.0.0.1:3000/', 'http://127.0.0.1:8793/')) {
        Start-Process $url
    }
    Write-Host "[OK] Opened FINOS (:1420), Grafana (:3001), Dagster (:3000), Tearsheets (:8793)." -ForegroundColor Green
}

function tv-e2e {
    Push-Location $script:Tier0RepoRoot
    try {
        & $script:Tier0Py -m ruff check src/tier0_sota/tv tests/test_tv_automation.py
        if ($LASTEXITCODE -ne 0) { throw 'ruff check failed' }
        & $script:Tier0Py -m ty check src/tier0_sota/tv tests/test_tv_automation.py
        if ($LASTEXITCODE -ne 0) { throw 'ty check failed' }
        & $script:Tier0Py -m pytest -q tests/test_tv_automation.py
        if ($LASTEXITCODE -ne 0) { throw 'pytest failed' }
        Write-Host "[VERDICT: PASS] TV automation ruff, ty, Pine balance, and pytest verified." -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

Write-Host "[TV-SOTA] Loaded: tv-tickets | tv-stage | tv-copy-omni | tv-copy-regime | tv-copy-watchlist | tv-portals | tv-e2e" -ForegroundColor Cyan

#Requires -Version 7.0
<#
.SYNOPSIS
    Tier-0 SOTA TradingView Operator Console (display-only, semi_auto_confirmed=$false).
.DESCRIPTION
    ToU-compliant PowerShell helpers for Pine v6 clipboard staging, Order Preset cards,
    Half-Auto extreme-mover order tickets, watchlist generation, and offline validation
    (zero CDP, zero live ports).
#>

$script:Tier0RepoRoot = Split-Path -Parent $PSScriptRoot
$script:Tier0Py       = Join-Path $script:Tier0RepoRoot '.venv\Scripts\python.exe'
$script:Tier0Manifest = if ($env:TIER0_DATA_ROOT) { Join-Path $env:TIER0_DATA_ROOT 'manifests' } else { 'Z:\sota-data\manifests' }
$script:Tier0PackDir  = Join-Path $script:Tier0Manifest 'tv_sota_pack'

function tv-tickets {
    [CmdletBinding()]
    param(
        [string]$ManifestDir = $script:Tier0Manifest,
        [string]$OutDir      = $script:Tier0PackDir,
        [ValidateSet('ALPACA', 'IBKR')][string]$Venue = 'ALPACA'
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $env:PYTHONPATH = Join-Path $script:Tier0RepoRoot 'src'
    & $script:Tier0Py -B -m tier0_sota.tv.tickets --manifest-dir $ManifestDir --out-dir $OutDir --venue $Venue
    if ($LASTEXITCODE -ne 0) { throw "tier0_sota.tv.tickets exited with code $LASTEXITCODE" }
    $mdPath = Join-Path $OutDir 'LIVE_HALF_AUTO_ORDER_TICKETS.md'
    if (Test-Path -LiteralPath $mdPath) { Get-Content -LiteralPath $mdPath -Encoding UTF8 }
}

function tv-presets {
    [CmdletBinding()]
    param(
        [string]$OutDir = $script:Tier0PackDir
    )
    $cardsPath = Join-Path $OutDir 'TV_ORDER_PRESET_CARDS.md'
    if (-not (Test-Path -LiteralPath $cardsPath)) {
        tv-tickets -OutDir $OutDir | Out-Null
    }
    if (Test-Path -LiteralPath $cardsPath) {
        Get-Content -LiteralPath $cardsPath -Encoding UTF8
    }
}

function tv-sync-pine {
    [CmdletBinding()]
    param(
        [string]$OutDir = $script:Tier0PackDir
    )
    $pineDir = Join-Path $script:Tier0RepoRoot 'pine'
    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
    $files = Get-ChildItem -LiteralPath $pineDir -Filter '*.pine' | Sort-Object Name
    foreach ($f in $files) {
        $dest = Join-Path $OutDir $f.Name
        Copy-Item -LiteralPath $f.FullName -Destination $dest -Force
        $sha = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash.ToLowerInvariant()
        Write-Host "[PINE-SYNC] $($f.Name) sha256=$sha" -ForegroundColor Green
    }
}

function tv-copy-omni {
    $path = Join-Path $script:Tier0RepoRoot 'pine\tier0_omni_sota_24x5.pine'
    Get-Content -Raw -LiteralPath $path -Encoding UTF8 | Set-Clipboard
    Write-Host "[OK] Copied $path to clipboard -> paste in TradingView Pine Editor." -ForegroundColor Green
}

function tv-copy-strategy {
    $path = Join-Path $script:Tier0RepoRoot 'pine\tier0_sota_strategy_24x5.pine'
    Get-Content -Raw -LiteralPath $path -Encoding UTF8 | Set-Clipboard
    Write-Host "[OK] Copied $path to clipboard -> paste in TradingView Pine Editor (Strategy Tester)." -ForegroundColor Green
}

function tv-copy-regime {
    $path = Join-Path $script:Tier0RepoRoot 'pine\tier0_market_regime.pine'
    Get-Content -Raw -LiteralPath $path -Encoding UTF8 | Set-Clipboard
    Write-Host "[OK] Copied $path to clipboard -> paste in TradingView Pane 1." -ForegroundColor Green
}

function tv-copy-darkpool {
    $path = Join-Path $script:Tier0RepoRoot 'pine\tier0_darkpool_uv_buildup.pine'
    Get-Content -Raw -LiteralPath $path -Encoding UTF8 | Set-Clipboard
    Write-Host "[OK] Copied $path to clipboard -> paste in TradingView Pane 2 (Darkpool/UV Oscillator)." -ForegroundColor Green
}

function tv-copy-watchlist {
    param(
        [string]$ManifestDir = $script:Tier0Manifest,
        [string]$OutDir      = $script:Tier0PackDir,
        [ValidateSet('ALPACA', 'IBKR')][string]$Venue = 'ALPACA'
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $env:PYTHONPATH = Join-Path $script:Tier0RepoRoot 'src'
    & $script:Tier0Py -B -m tier0_sota.tv.tickets --manifest-dir $ManifestDir --out-dir $OutDir --venue $Venue | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "tier0_sota.tv.tickets exited with code $LASTEXITCODE" }
    $wlPath = Join-Path $OutDir 'tier0_sota_tv_watchlists_live.txt'
    $wlPath | Set-Clipboard
    Write-Host "[OK] Refreshed and copied watchlist path to clipboard: $wlPath" -ForegroundColor Green
    Write-Host "[TV] TradingView -> Watchlist menu (...) -> Import list... -> Ctrl+V -> Open" -ForegroundColor Cyan
}

function tv-ingest {
    [CmdletBinding()]
    param(
        [string]$File,
        [switch]$Clipboard,
        [ValidateSet('IBKR', 'ALPACA')][string]$Venue = 'IBKR',
        [switch]$LiveIntake,
        [string]$StagedPath    = (Join-Path $script:Tier0PackDir 'staged_tv_presets.json'),
        [string]$WatchlistPath = (Join-Path $script:Tier0Manifest 'live_whole_market_movers_manifest.json')
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $env:PYTHONPATH = Join-Path $script:Tier0RepoRoot 'src'
    $bridgeArgs = @('-B', '-m', 'tier0_sota.tv.bridge', '--staged-path', $StagedPath, '--watchlist-path', $WatchlistPath, '--venue', $Venue)
    if ($LiveIntake) { $bridgeArgs += '--live-intake' }
    if ($Clipboard -or -not $File) {
        $raw = Get-Clipboard -Raw
        if ([string]::IsNullOrWhiteSpace($raw)) { throw 'Clipboard is empty; pass -File or copy alert JSON first.' }
        $bridgeArgs += '--ingest-stdin'
        $res = $raw | & $script:Tier0Py @bridgeArgs
    } else {
        $bridgeArgs += @('--ingest-file', $File)
        $res = & $script:Tier0Py @bridgeArgs
    }
    if ($LASTEXITCODE -ne 0) { throw 'tv.bridge validation failed' }
    $res
    Write-Host "[OK] Ingested & audited TV alert preset(s) (qty=1, semi_auto_confirmed=false)." -ForegroundColor Green
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
        [switch]$LiveIntake,
        [string]$StagedPath    = (Join-Path $script:Tier0PackDir 'staged_tv_presets.json'),
        [string]$WatchlistPath = (Join-Path $script:Tier0Manifest 'live_whole_market_movers_manifest.json')
    )
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $env:PYTHONPATH = Join-Path $script:Tier0RepoRoot 'src'
    $sym = $Symbol.Trim().ToUpperInvariant()
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("tv_stage_" + [guid]::NewGuid().ToString('N') + '.json')
    try {
        & $script:Tier0Py -B -c @"
import json, time
from decimal import Decimal
from tier0_sota.engine import (
    classify_us_equity_session_et,
    get_session_protection_profile,
    select_pooled_trailing_schedule,
)
now_ms = (int(time.time()) // 60) * 60_000
sess = classify_us_equity_session_et(now_ms * 1_000_000)
if sess == 'CLOSED':
    raise SystemExit('Refused tv-stage: XNYS 24/5 market session is CLOSED')
natr = Decimal(str($NatrPct))
sched, profiles = select_pooled_trailing_schedule(natr)
prof = get_session_protection_profile(sess, profiles)
payload = {
    'schema_version': 'tier0.tv-alert.v1',
    'symbol': '$sym',
    'ts_utc_ms': now_ms,
    'bar_confirmed': True,
    'side': '$Side',
    'qty': 1,
    'venue': '$Venue',
    'session': sess,
    'order_type': prof.order_mode,
    'trigger_price': $Price,
    'stop_price': $StopPrice,
    'limit_collar_bps': int(prof.limit_collar_bps),
    'trailing_schedule': sched,
    'natr_pct': float(natr),
    'auto_reverse': True,
    'auto_reentry': True,
    'reentry_cooldown_secs': 60,
    'strategy_id': 'SignalExecutionStrategy',
    'semi_auto_confirmed': False,
    'script_kind': 'indicator',
    'data_source': 'tradingview_pine_v6',
}
open(r'$tmp', 'w', encoding='utf-8').write(json.dumps(payload))
"@
        if ($LASTEXITCODE -ne 0) { throw 'Failed to compose staged alert JSON' }
        $bridgeArgs = @('-B', '-m', 'tier0_sota.tv.bridge', '--ingest-file', $tmp, '--staged-path', $StagedPath, '--watchlist-path', $WatchlistPath, '--venue', $Venue)
        if ($LiveIntake) { $bridgeArgs += '--live-intake' }
        $res = & $script:Tier0Py @bridgeArgs
        if ($LASTEXITCODE -ne 0) { throw 'tv.bridge validation failed' }
        ($res -join "`n") | Set-Clipboard
        $res
        Write-Host "[OK] Staged & copied to clipboard (qty=1, semi_auto_confirmed=false)." -ForegroundColor Green
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

function tv-confirm-paper {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidatePattern('^[0-9a-fA-F]{16}$')][string]$AlertId,
        [ValidateRange(1, 10000)][int]$Qty = 1,
        [switch]$DryRun,
        [string]$ManifestDir = $script:Tier0Manifest
    )
    $keyFile = if ($env:FNOX_AGE_KEY_FILE) { $env:FNOX_AGE_KEY_FILE } else { [Environment]::GetEnvironmentVariable('FNOX_AGE_KEY_FILE', 'User') }
    if (-not $keyFile) { $keyFile = Join-Path $HOME '.config\fnox\age.txt' }
    if (-not (Get-Command fnox -ErrorAction SilentlyContinue)) { throw 'fnox is required for tv-confirm-paper (--profile paper)' }
    if (-not (Test-Path -LiteralPath $keyFile)) { throw "Missing fnox paper age key: $keyFile" }
    $env:FNOX_AGE_KEY_FILE = $keyFile
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $env:PYTHONPATH = Join-Path $script:Tier0RepoRoot 'src'
    $runnerArgs = @(
        'exec', '--profile', 'paper', '--',
        'uv', 'run', '--no-sync', 'python', '-B', '-m', 'tier0_sota.live.runner',
        '--manifest-dir', $ManifestDir,
        '--confirm-tv-alert', $AlertId.Trim().ToLowerInvariant(),
        '--tv-qty', "$Qty"
    )
    if ($DryRun) { $runnerArgs += '--dry-run' }
    Push-Location $script:Tier0RepoRoot
    try {
        & fnox @runnerArgs
        if ($LASTEXITCODE -ne 0) { throw "tv-confirm-paper exited with code $LASTEXITCODE" }
    } finally {
        Pop-Location
    }
}

function tv-portals {
    foreach ($url in @(
        'http://localhost:1420/',
        'http://127.0.0.1:6900/widgets.json',
        'http://127.0.0.1:3001/',
        'http://127.0.0.1:3000/',
        'http://127.0.0.1:4213/',
        'http://127.0.0.1:8793/tv_sota_pack/'
    )) {
        Start-Process $url
    }
    Write-Host "[OK] Opened FINOS (localhost:1420), OpenBB (:6900), Grafana (:3001), Dagster (:3000), DuckDB UI (:4213), TV Pack (:8793/tv_sota_pack/)." -ForegroundColor Green
}

function tv-land {
    [CmdletBinding()]
    param(
        [string]$ManifestDir = $script:Tier0Manifest,
        [string]$OutDir      = $script:Tier0PackDir,
        [ValidateSet('ALPACA', 'IBKR')][string]$Venue = 'ALPACA'
    )
    tv-tickets -ManifestDir $ManifestDir -OutDir $OutDir -Venue $Venue
    tv-sync-pine -OutDir $OutDir
    tv-copy-omni
    if (Test-Path -LiteralPath $OutDir) {
        Start-Process $OutDir
    }
    Write-Host "[TV-LAND] 3-Click TradingView Landing Checklist (display-only, qty=1, semi_auto_confirmed=false):" -ForegroundColor Cyan
    Write-Host "  1. Pine Editor Ctrl+V -> Save (tier0_omni_sota_24x5.pine is already on your clipboard)" -ForegroundColor Yellow
    Write-Host "  2. tv-copy-regime -> Macro pane (paste tier0_market_regime.pine into Pane 1)" -ForegroundColor Yellow
    Write-Host "  3. tv-copy-watchlist -> Watchlist ... Import list... Ctrl+V -> Open" -ForegroundColor Yellow
    Write-Host "  4. Optional: tv-copy-strategy (Strategy Tester), tv-copy-darkpool (Pane 2 UV/DP), tv-presets (Order Cards)" -ForegroundColor DarkCyan
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

Write-Host "[TV-SOTA] Loaded: tv-land | tv-tickets | tv-presets | tv-stage | tv-ingest | tv-confirm-paper | tv-sync-pine | tv-copy-omni | tv-copy-strategy | tv-copy-regime | tv-copy-darkpool | tv-copy-watchlist | tv-portals | tv-e2e" -ForegroundColor Cyan

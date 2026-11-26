#Requires -Version 7.0
param(
  [ValidateSet("preflight", "scan", "paper-farm", "alpaca-paper", "ibkr-paper", "performance")]
  [string]$Mode = "preflight",

  [ValidateSet("ALL", "ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3", "IBKR_DU_4002")]
  [string]$Account = "ALL",

  [string[]]$PinSymbols = @("SOXL", "SOXS"),

  [int]$TopK = 15,

  [string]$DataRoot = $(if ($env:TIER0_DATA_ROOT) { $env:TIER0_DATA_ROOT } else { "Z:\sota-data" }),

  [string]$ManifestDir = "",

  [switch]$ConfirmPaper
)

$ErrorActionPreference = "Stop"

$ScriptRepoRoot = Split-Path -Parent $PSScriptRoot
$RepoRoot = if ($env:TIER0_REPO_ROOT -and (Test-Path (Join-Path $env:TIER0_REPO_ROOT "src\tier0_sota"))) {
  $env:TIER0_REPO_ROOT
} else {
  $ScriptRepoRoot
}
if ([string]::IsNullOrWhiteSpace($ManifestDir)) {
  $ManifestDir = Join-Path $DataRoot "manifests"
}
$PyExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PyExe)) {
  $PyExe = "python"
}

$CleanPins = @(
  $PinSymbols |
    ForEach-Object { $_ -split "," } |
    ForEach-Object { $_.Trim().ToUpperInvariant() } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    Select-Object -Unique
)
if ($CleanPins.Count -eq 0) {
  $CleanPins = @("SOXL", "SOXS")
}
foreach ($sym in $CleanPins) {
  if ($sym -notmatch "^[A-Z][A-Z0-9.-]{0,6}$") {
    throw "Invalid symbol token: $sym"
  }
}

$PinArg = ($CleanPins -join ",")
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONPATH = (Join-Path $RepoRoot "src")

switch ($Mode) {
  "preflight" {
    & $PyExe -c @"
from pathlib import Path
from tier0_sota import engine, risk
from tier0_sota.live import ALPACA_LANE_PROFILES, PAPER_ALPACA_ENDPOINT, IbkrPaperAdapter
ibkr = IbkrPaperAdapter(Path(r'$ManifestDir'))
assert PAPER_ALPACA_ENDPOINT == 'https://paper-api.alpaca.markets'
print('PREFLIGHT_OK', list(ALPACA_LANE_PROFILES.keys()), ibkr.connection_spec['port'], risk.MAX_STOP_LOSS_PCT)
"@
    exit $LASTEXITCODE
  }
  "scan" {
    & $PyExe -c @"
from pathlib import Path
from tier0_sota.live.scanner import scan_extreme_movers
pins = tuple('$PinArg'.split(','))
out = Path(r'$ManifestDir') / 'live_whole_market_movers_manifest.json'
doc = scan_extreme_movers(top_k=$TopK, pin_symbols=pins, output_path=out)
print('SCAN_OK', doc['ranked_count'], doc['macro_regime'])
"@
    exit $LASTEXITCODE
  }
  { $_ -in @("paper-farm", "alpaca-paper", "ibkr-paper") } {
    if (-not $ConfirmPaper) {
      throw "Refused live paper submission without -ConfirmPaper switch (fail-closed)."
    }
    $ibkrReady = $false
    try {
      $tcp = [System.Net.Sockets.TcpClient]::new()
      try {
        $iar = $tcp.BeginConnect("127.0.0.1", 4002, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne(250, $false)) {
          $tcp.EndConnect($iar)
          $ibkrReady = $true
        }
      } finally {
        $tcp.Dispose()
      }
    } catch {
      $ibkrReady = $false
    }
    $ibkrPyBool = if ($ibkrReady) { "True" } else { "False" }
    & $PyExe -c @"
import os
from pathlib import Path
from tier0_sota.live.paper_farm import PaperFarmCoordinator
from tier0_sota.live.performance import aggregate_paper_farm_performance
mdir = Path(r'$ManifestDir')
coord = PaperFarmCoordinator(mdir)
sync_rep = coord.sync_all_lanes(account='$Account', ibkr_gateway_ready=$ibkrPyBool)
otlp_url = os.environ.get('TIER0_OTLP_METRICS_URL', 'http://127.0.0.1:4318/v1/metrics').strip() or None
perf = aggregate_paper_farm_performance(
    mdir,
    prom_output_path=mdir / 'paper_farm_metrics.prom',
    otlp_endpoint=otlp_url,
)
print('PAPER_FARM_OK', list(coord.lanes.keys()), perf['summary'], {k: v.get('total_tca_fills_duckdb', 0) for k, v in sync_rep.items() if isinstance(v, dict)})
"@
    exit $LASTEXITCODE
  }
  "performance" {
    & $PyExe -c @"
import json, os
from pathlib import Path
from tier0_sota.live.performance import aggregate_paper_farm_performance
mdir = Path(r'$ManifestDir')
otlp_url = os.environ.get('TIER0_OTLP_METRICS_URL', 'http://127.0.0.1:4318/v1/metrics').strip() or None
perf = aggregate_paper_farm_performance(
    mdir,
    prom_output_path=mdir / 'paper_farm_metrics.prom',
    otlp_endpoint=otlp_url,
)
lanes = {
    k: {
        'equity': v['equity'],
        'entries': v['entries'],
        'fills': v['fills'],
        'tca_fills_count': v['tca_fills_count'],
        'mean_is_bps': v['mean_is_bps'],
        'auto_shorts': v['auto_shorts'],
        'reverses': v['reverses'],
        'reentries': v['reentries'],
        'stop_replaces': v['stop_replaces'],
        'fast_sells': v['fast_sells'],
        'status_present': v['status_present'],
    }
    for k, v in perf['per_lane'].items()
}
print('PERF_OK', json.dumps({'summary': perf['summary'], 'per_lane': lanes}))
"@
    exit $LASTEXITCODE
  }
}

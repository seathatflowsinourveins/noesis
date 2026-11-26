#Requires -Version 7.0
param(
  [ValidateSet("preflight", "scan", "paper-farm", "alpaca-paper", "ibkr-paper", "performance", "start-loop")]
  [string]$Mode = "preflight",

  [ValidateSet("ALL", "ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3", "IBKR_DU_4002")]
  [string]$Account = "ALL",

  [string[]]$PinSymbols = @("SOXL", "SOXS"),

  [int]$TopK = 15,

  [string]$DataRoot = $(if ($env:TIER0_DATA_ROOT) { $env:TIER0_DATA_ROOT } else { "Z:\sota-data" }),

  [string]$ManifestDir = "",

  [switch]$Observe,

  [switch]$ConfirmPaper
)

$ErrorActionPreference = "Stop"

$ScriptRepoRoot = Split-Path -Parent $PSScriptRoot
$RepoRoot = $ScriptRepoRoot
if ([string]::IsNullOrWhiteSpace($ManifestDir)) {
  $ManifestDir = Join-Path $DataRoot "manifests"
}
$PyExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PyExe)) {
  throw "Missing worktree virtual environment at $PyExe"
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
  "start-loop" {
    if ($Observe -eq $ConfirmPaper -or $Account -ne "ALL") {
      throw "Select one of -Observe or -ConfirmPaper and all accounts."
    }
    $PywExe = Join-Path $RepoRoot ".venv\Scripts\pythonw.exe"
    $LoopPyw = Join-Path $RepoRoot "scripts\paper_farm_loop_hidden.pyw"
    if (-not (Test-Path -LiteralPath $PywExe -PathType Leaf)) {
      throw "Missing zero-window pythonw.exe at $PywExe"
    }
    if (-not (Test-Path -LiteralPath $LoopPyw -PathType Leaf)) {
      throw "Missing zero-window loop launcher at $LoopPyw"
    }
    $taskName = if ($Observe) { "Noesis Paper Farm Observation" } else { "Noesis Paper Farm Execution" }
    $modeFlag = if ($Observe) { "--observe" } else { "--confirm-paper" }
    $arguments = "`"$LoopPyw`" $modeFlag --manifest-dir `"$ManifestDir`" --top-k $TopK --pin-symbols $PinArg"
    $action = New-ScheduledTaskAction -Execute $PywExe -Argument $arguments -WorkingDirectory $RepoRoot
    $existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([timespan]::Zero)
    if ($existing) {
      if ($existing.Actions.WorkingDirectory -ne $RepoRoot) {
        throw "Existing task '$taskName' belongs to another worktree."
      }
      Set-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings | Out-Null
    } else {
      Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -ErrorAction Stop | Out-Null
    }
    Enable-ScheduledTask -TaskName $taskName | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Get-ScheduledTask -TaskName $taskName | Select-Object TaskName, State
    exit 0
  }
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
    $forbidden = @(4001, 7496, 8798, 8796)
    $active = @(Get-NetTCPConnection -ErrorAction Stop | Where-Object {
      $_.LocalPort -in $forbidden -or $_.RemotePort -in $forbidden
    })
    if ($active.Count) { throw "Paper Farm requires zero live broker ports." }
    $env:IBKR_PAPER_TCP_REACHABLE = "0"
    $tcp = [System.Net.Sockets.TcpClient]::new()
    try {
      if ($tcp.ConnectAsync("127.0.0.1", 4002).Wait(2000) -and $tcp.Connected) {
        $env:IBKR_PAPER_TCP_REACHABLE = "1"
      }
    } catch [System.AggregateException] {
      $env:IBKR_PAPER_TCP_REACHABLE = "0"
    } finally { $tcp.Dispose() }
    if ($Mode -eq "ibkr-paper" -or $Account -eq "IBKR_DU_4002") {
      & $PyExe -m tier0_sota.live.runner --ibkr-health --manifest-dir $ManifestDir
      exit $LASTEXITCODE
    }
    if ($Account -ne "ALL") { throw "All three account inventories are required for the shared symbol lock." }
    if ($ConfirmPaper -eq $Observe) { throw "Select exactly one of -ConfirmPaper or -Observe." }
    $intent = if ($Observe) { "--observe" } else { "--confirm-paper" }
    if (-not $env:FNOX_AGE_KEY_FILE) {
      $env:FNOX_AGE_KEY_FILE = [Environment]::GetEnvironmentVariable("FNOX_AGE_KEY_FILE", "User")
      if (-not $env:FNOX_AGE_KEY_FILE) { $env:FNOX_AGE_KEY_FILE = Join-Path $HOME ".config/fnox/age.txt" }
    }
    if (-not (Test-Path -LiteralPath $env:FNOX_AGE_KEY_FILE -PathType Leaf)) { throw "Missing fnox age identity" }
    Push-Location -LiteralPath $RepoRoot
    try {
      fnox exec --profile paper -- $PyExe -m tier0_sota.live.runner $intent --manifest-dir $ManifestDir --top-k $TopK --pin-symbols $PinArg
    } finally { Pop-Location }
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

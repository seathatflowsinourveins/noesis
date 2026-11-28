#Requires -Version 7.0
<#
.SYNOPSIS
    Idempotent Windows Tailscale Serve configuration for Noesis dual-machine control plane (ADR-0034).
.DESCRIPTION
    Exposes Noesis mirrored WSL2 loopback services strictly over the private Tailscale tailnet
    via `tailscale serve --bg`:
      - TCP  2222  -> tcp://127.0.0.1:2222  (WSL2 loopback-only OpenSSH)
      - TCP  6768  -> tcp://127.0.0.1:6768  (WSL2 headless Linux Orca Server)
      - HTTPS port-matched -> http://127.0.0.1:<port> for:
        8793, 5000, 8080, 3000, 3001, 8010, 9200, 2718, 20128, 7575, 6900
    Strictly enforces:
      - Zero Tailscale Funnel (public Internet exposure) on any port
      - Zero live/paper broker ports (4001, 4002, 7496, 7497, 8796, 8798) in Tailscale Serve
      - Zero forbidden live broker listeners (4001, 7496, 8798, 8796) on the workstation
      - Supports -VerifyOnly (read-only audit) and -DryRun (print commands without mutating)
#>
[CmdletBinding()]
param(
    [switch]$VerifyOnly,
    [switch]$DryRun,
    [string]$TailscaleBin = "tailscale"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$TcpServePorts = @(2222, 6768)
$HttpsServePorts = @(8793, 5000, 8080, 3000, 3001, 8010, 9200, 2718, 20128, 7575, 6900)
$ForbiddenLiveBrokerPorts = @(4001, 7496, 8798, 8796)
$ForbiddenTailscaleBrokerPorts = @(4001, 4002, 7496, 7497, 8796, 8798)

# 1. Fail-closed static sanity: never allow any broker port into Serve lists.
foreach ($p in ($TcpServePorts + $HttpsServePorts)) {
    if ($p -in $ForbiddenTailscaleBrokerPorts) {
        throw "FAIL-CLOSED: broker port $p must never be configured in Tailscale Serve."
    }
}

# 2. Fail-closed live broker port firewall check on local workstation.
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
    $liveHits = @(
        Get-NetTCPConnection -ErrorAction SilentlyContinue |
            Where-Object { $_.LocalPort -in $ForbiddenLiveBrokerPorts -or $_.RemotePort -in $ForbiddenLiveBrokerPorts }
    )
    if ($liveHits.Count -gt 0) {
        throw "FAIL-CLOSED: forbidden live broker port detected ($($liveHits.LocalPort -join ','))."
    }
}

function Resolve-TailscaleBinary {
    param([string]$Requested)
    $cmd = Get-Command $Requested -ErrorAction SilentlyContinue
    if ($null -ne $cmd) {
        return $cmd.Source
    }
    $fallback = "C:\Program Files\Tailscale\tailscale.exe"
    if (Test-Path -LiteralPath $fallback) {
        return $fallback
    }
    throw "Tailscale CLI not found (checked '$Requested' and '$fallback')."
}

function Test-TailscaleServeState {
    param([string]$ExePath)

    $rawJson = (& $ExePath serve status --json 2>&1) -join "`n"
    if ($LASTEXITCODE -ne 0) {
        throw "tailscale serve status --json failed (exit $LASTEXITCODE): $rawJson"
    }
    $status = if ([string]::IsNullOrWhiteSpace($rawJson)) { [pscustomobject]@{} } else { $rawJson | ConvertFrom-Json }

    # Assert tailscale funnel is OFF on every port.
    if ($status.PSObject.Properties.Name -contains "AllowFunnel" -and $null -ne $status.AllowFunnel) {
        foreach ($prop in $status.AllowFunnel.PSObject.Properties) {
            if ([bool]$prop.Value) {
                throw "FAIL-CLOSED: tailscale funnel is ENABLED on '$($prop.Name)'; public Funnel exposure is forbidden (ADR-0034)."
            }
        }
    }

    # Assert zero broker ports exposed via TCP or Web Serve handlers.
    $tcpKeys = @()
    if ($status.PSObject.Properties.Name -contains "TCP" -and $null -ne $status.TCP) {
        $tcpKeys = @($status.TCP.PSObject.Properties.Name)
    }
    $webKeys = @()
    if ($status.PSObject.Properties.Name -contains "Web" -and $null -ne $status.Web) {
        $webKeys = @($status.Web.PSObject.Properties.Name)
    }

    foreach ($bp in $ForbiddenTailscaleBrokerPorts) {
        if (([string]$bp) -in $tcpKeys) {
            throw "FAIL-CLOSED: forbidden broker port $bp is exposed in Tailscale Serve TCP."
        }
        if (@($webKeys | Where-Object { $_ -match ":${bp}$" }).Count -gt 0) {
            throw "FAIL-CLOSED: forbidden broker port $bp is exposed in Tailscale Serve Web."
        }
    }

    $missingTcp = @()
    foreach ($tp in $TcpServePorts) {
        $key = [string]$tp
        if ($key -notin $tcpKeys) {
            $missingTcp += $tp
            continue
        }
        $fwd = [string]$status.TCP.$key.TCPForward
        if ($fwd -ne "127.0.0.1:$tp") {
            throw "FAIL-CLOSED: Tailscale Serve TCP :$tp forwards to '$fwd' (expected '127.0.0.1:$tp')."
        }
    }

    $missingHttps = @()
    foreach ($hp in $HttpsServePorts) {
        $matched = @($webKeys | Where-Object { $_ -match ":${hp}$" })
        if ($matched.Count -eq 0) {
            $missingHttps += $hp
            continue
        }
        $webEntry = $status.Web.($matched[0])
        $proxyTarget = ""
        if ($null -ne $webEntry -and ($webEntry.PSObject.Properties.Name -contains "Handlers")) {
            $rootHandler = $webEntry.Handlers."/"
            if ($null -ne $rootHandler -and ($rootHandler.PSObject.Properties.Name -contains "Proxy")) {
                $proxyTarget = [string]$rootHandler.Proxy
            }
        }
        if ($proxyTarget -ne "http://127.0.0.1:$hp") {
            throw "FAIL-CLOSED: Tailscale Serve HTTPS :$hp proxies to '$proxyTarget' (expected 'http://127.0.0.1:$hp')."
        }
    }

    return [ordered]@{
        ok                  = ($missingTcp.Count -eq 0 -and $missingHttps.Count -eq 0)
        funnel_off          = $true
        broker_ports_zero   = $true
        tcp_ports_expected  = $TcpServePorts.Count
        tcp_ports_missing   = $missingTcp
        https_ports_expected = $HttpsServePorts.Count
        https_ports_missing = $missingHttps
    }
}

if ($DryRun) {
    Write-Host "[DRY-RUN] Noesis Windows Tailscale Serve Plan (ADR-0034; tailnet-only, zero Funnel, zero broker ports)" -ForegroundColor Cyan
    foreach ($tp in $TcpServePorts) {
        Write-Host "  tailscale serve --bg --tcp=$tp tcp://127.0.0.1:$tp" -ForegroundColor Gray
    }
    foreach ($hp in $HttpsServePorts) {
        Write-Host "  tailscale serve --bg --https=$hp http://127.0.0.1:$hp" -ForegroundColor Gray
    }
    Write-Host "  Assert: tailscale funnel OFF on all ports; zero broker ports ($($ForbiddenTailscaleBrokerPorts -join ','))" -ForegroundColor Green
    exit 0
}

$ResolvedExe = Resolve-TailscaleBinary -Requested $TailscaleBin

if ($VerifyOnly) {
    $report = Test-TailscaleServeState -ExePath $ResolvedExe
    $report | ConvertTo-Json -Depth 4
    if (-not $report.ok) {
        throw "Tailscale Serve verification FAILED: missing_tcp=$($report.tcp_ports_missing -join ',') missing_https=$($report.https_ports_missing -join ',')"
    }
    Write-Host "[VERDICT: PASS] Tailscale Serve verified ($($TcpServePorts.Count) TCP + $($HttpsServePorts.Count) HTTPS, funnel=OFF, broker_ports=0)." -ForegroundColor Green
    exit 0
}

foreach ($tp in $TcpServePorts) {
    Write-Host "[SERVE TCP] --tcp=$tp -> tcp://127.0.0.1:$tp" -ForegroundColor Cyan
    & $ResolvedExe serve --bg "--tcp=$tp" "tcp://127.0.0.1:$tp"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to configure Tailscale Serve TCP :$tp (exit $LASTEXITCODE)"
    }
}

foreach ($hp in $HttpsServePorts) {
    Write-Host "[SERVE HTTPS] --https=$hp -> http://127.0.0.1:$hp" -ForegroundColor Cyan
    & $ResolvedExe serve --bg "--https=$hp" "http://127.0.0.1:$hp"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to configure Tailscale Serve HTTPS :$hp (exit $LASTEXITCODE)"
    }
}

$finalReport = Test-TailscaleServeState -ExePath $ResolvedExe
if (-not $finalReport.ok) {
    throw "Post-apply Tailscale Serve verification FAILED: missing_tcp=$($finalReport.tcp_ports_missing -join ',') missing_https=$($finalReport.https_ports_missing -join ',')"
}
Write-Host "[VERDICT: PASS] Noesis Windows Tailscale Serve configured ($($TcpServePorts.Count) TCP + $($HttpsServePorts.Count) HTTPS, funnel=OFF, broker_ports=0)." -ForegroundColor Green

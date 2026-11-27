#Requires -Version 7.0
<#
.SYNOPSIS
    One-command local SOTA CI verification gate for Z:\noesis.
.DESCRIPTION
    Runs:
      1. risk.py immutable SHA-256 check
      2. PowerShell AST parse across all scripts/**/*.ps1
      3. uv.lock zero-yfinance check (UV_LOCK_YFINANCE_MATCHES=0)
      4. Zero-legacy reference scan across repo sources, docs, evals, scripts & manifests
      5. ruff format --check
      6. ruff check .
      7. ty check
      8. deptry .
      9. ast-grep scan
     10. evals/formal_risk_z3.py (Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1)
     11. scripts/nautilus_mcp_server.py --smoke
     12. pytest -q
     13. gitleaks detect --no-git (if installed)
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [switch]$SkipPytest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedRiskSha = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvScripts = Join-Path $RepoRoot ".venv\Scripts"
if (-not (Test-Path -LiteralPath $VenvPy)) {
    throw "Clean .venv Python missing at $VenvPy. Run scripts/bootstrap_env.ps1 first."
}

$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PATH = "$VenvScripts;$env:PATH"

Push-Location $RepoRoot
try {
    # 1. Immutable risk.py SHA-256
    $riskPath = Join-Path $RepoRoot "src\tier0_sota\risk.py"
    $riskSha = (Get-FileHash -LiteralPath $riskPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($riskSha -ne $ExpectedRiskSha) {
        throw "risk.py SHA-256 mismatch: $riskSha != $ExpectedRiskSha"
    }
    Write-Host "[OK] risk.py SHA-256 = $riskSha" -ForegroundColor Green

    # 2. PowerShell AST parse across all scripts/**/*.ps1
    $psFiles = @(Get-ChildItem -LiteralPath (Join-Path $RepoRoot "scripts") -Recurse -Filter "*.ps1" -File)
    foreach ($pf in $psFiles) {
        $tokens = $null; $errors = $null
        [void][System.Management.Automation.Language.Parser]::ParseFile($pf.FullName, [ref]$tokens, [ref]$errors)
        if ($errors -and $errors.Count -gt 0) {
            throw "PowerShell AST error in $($pf.FullName): $($errors | Out-String)"
        }
    }
    Write-Host "[OK] PowerShell AST parse: $($psFiles.Count)/$($psFiles.Count) scripts (0 errors)" -ForegroundColor Green

    # 3. uv.lock zero yfinance
    $uvLock = Join-Path $RepoRoot "uv.lock"
    $yfCount = @(Select-String -LiteralPath $uvLock -Pattern "yfinance" -CaseSensitive:$false).Count
    if ($yfCount -ne 0) {
        throw "UV_LOCK_YFINANCE_MATCHES=$yfCount (expected 0)"
    }
    Write-Host "[OK] UV_LOCK_YFINANCE_MATCHES=0" -ForegroundColor Green

    # 4. Zero legacy references
    $legacyRegex = [string]::Join("|", @(
        ("tier0" + "-quant(?!-falsification)"),
        ("CLEAN" + " LIVE"),
        ("sota-live-tv" + "-harness"),
        ("sota_compounding_1000rpm" + "_harness"),
        ("RTX " + "4090")
    ))
    $scanTargets = [System.Collections.Generic.List[string]]::new()
    foreach ($rel in @("pyproject.toml", "ARCHITECTURE_MANIFEST.json", "SOTA_STARRED_AND_BEYOND_MANIFEST.json")) {
        $p = Join-Path $RepoRoot $rel
        if (Test-Path -LiteralPath $p) { $scanTargets.Add($p) }
    }
    foreach ($dir in @("src", "docs", "evals", "scripts")) {
        $d = Join-Path $RepoRoot $dir
        if (Test-Path -LiteralPath $d) {
            foreach ($f in @(Get-ChildItem -LiteralPath $d -Recurse -File | Where-Object { $_.FullName -notmatch '\\__pycache__\\' })) {
                $scanTargets.Add($f.FullName)
            }
        }
    }
    $legacyHits = @(Select-String -Path $scanTargets.ToArray() -Pattern $legacyRegex -ErrorAction SilentlyContinue)
    if ($legacyHits.Count -ne 0) {
        throw "Legacy references detected ($($legacyHits.Count))"
    }
    Write-Host "[OK] LEGACY_MATCHES=0" -ForegroundColor Green

    # 5-11 & 13. Parallel read-only static, formal Z3, FastMCP smoke & gitleaks gates
    $gateLogDir = Join-Path ([System.IO.Path]::GetTempPath()) ("noesis-check-ps1-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $gateLogDir -Force | Out-Null
    try {
        $gateSpecs = [System.Collections.Generic.List[hashtable]]::new()
        $gateSpecs.Add(@{ Name = "ruff-format"; File = (Join-Path $VenvScripts "ruff.exe"); Args = @("format", "--check", "src", "tests", "scripts", "evals") })
        $gateSpecs.Add(@{ Name = "ruff-check"; File = (Join-Path $VenvScripts "ruff.exe"); Args = @("check", ".") })
        $gateSpecs.Add(@{ Name = "ty-check"; File = (Join-Path $VenvScripts "ty.exe"); Args = @("check") })
        $gateSpecs.Add(@{ Name = "deptry"; File = (Join-Path $VenvScripts "deptry.exe"); Args = @(".") })
        $gateSpecs.Add(@{ Name = "ast-grep"; File = (Join-Path $VenvScripts "ast-grep.exe"); Args = @("scan") })
        $gateSpecs.Add(@{ Name = "z3"; File = $VenvPy; Args = @((Join-Path $RepoRoot "evals\formal_risk_z3.py")) })
        $gateSpecs.Add(@{ Name = "mcp-smoke"; File = $VenvPy; Args = @((Join-Path $RepoRoot "scripts\nautilus_mcp_server.py"), "--smoke") })
        $gitleaksCmd = Get-Command gitleaks -ErrorAction SilentlyContinue
        if ($gitleaksCmd) {
            $gateSpecs.Add(@{ Name = "gitleaks"; File = $gitleaksCmd.Source; Args = @("detect", "--no-git", "--source", $RepoRoot, "--config", (Join-Path $RepoRoot ".gitleaks.toml"), "--no-banner") })
        }

        $runningGates = [System.Collections.Generic.List[hashtable]]::new()
        foreach ($spec in $gateSpecs) {
            $outFile = Join-Path $gateLogDir ($spec.Name + ".out.log")
            $errFile = Join-Path $gateLogDir ($spec.Name + ".err.log")
            $proc = Start-Process -FilePath $spec.File -ArgumentList $spec.Args -WorkingDirectory $RepoRoot -NoNewWindow -PassThru -RedirectStandardOutput $outFile -RedirectStandardError $errFile
            $null = $proc.Handle
            $runningGates.Add(@{
                Name    = $spec.Name
                Proc    = $proc
                OutFile = $outFile
                ErrFile = $errFile
            })
        }

        $gateFailures = [System.Collections.Generic.List[string]]::new()
        foreach ($g in $runningGates) {
            $g.Proc.WaitForExit()
            $outText = if (Test-Path -LiteralPath $g.OutFile) { (Get-Content -LiteralPath $g.OutFile -Raw -Encoding UTF8) } else { "" }
            $errText = if (Test-Path -LiteralPath $g.ErrFile) { (Get-Content -LiteralPath $g.ErrFile -Raw -Encoding UTF8) } else { "" }
            if ($null -eq $outText) { $outText = "" }
            if ($null -eq $errText) { $errText = "" }
            $combined = ($outText + "`n" + $errText).Trim()
            if ($g.Proc.ExitCode -ne 0) {
                $gateFailures.Add("$($g.Name) failed (exit=$($g.Proc.ExitCode)): $combined")
                continue
            }
            if ($g.Name -eq "z3" -and $combined -notmatch "Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1") {
                $gateFailures.Add("formal_risk_z3.py missing expected marker: $combined")
            } elseif ($g.Name -eq "mcp-smoke") {
                $mcpJson = $outText | ConvertFrom-Json
                if (-not $mcpJson.ok -or $mcpJson.tools_count -ne 10) {
                    $gateFailures.Add("nautilus_mcp_server.py --smoke unexpected payload: $combined")
                }
            }
        }
        if ($gateFailures.Count -gt 0) {
            throw ($gateFailures -join "`n---`n")
        }
        Write-Host "[OK] parallel static/security/smoke gates ($($runningGates.Count)/$($runningGates.Count))" -ForegroundColor Green
        Write-Host "[OK] Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1" -ForegroundColor Green
        Write-Host "[OK] nautilus_mcp_server.py --smoke (10/10 tools OK)" -ForegroundColor Green
        if ($gitleaksCmd) {
            Write-Host "[OK] gitleaks detect --no-git = 0 leaks" -ForegroundColor Green
        }
    } finally {
        if (Test-Path -LiteralPath $gateLogDir) {
            Remove-Item -LiteralPath $gateLogDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }

    # 12. Pytest suite
    if (-not $SkipPytest) {
        & (Join-Path $VenvScripts "pytest.exe") -q
        if ($LASTEXITCODE -ne 0) { throw "pytest -q failed" }
    }

    Write-Host "[CHECK-PS1 VERDICT: PASS]" -ForegroundColor Green
} finally {
    Pop-Location
}

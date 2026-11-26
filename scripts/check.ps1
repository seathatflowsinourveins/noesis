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
        ("tier0" + "-quant"),
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

    # 5-9. Static formatting, lint, type check, deptry, AST-grep
    & (Join-Path $VenvScripts "ruff.exe") format --check src tests scripts evals
    if ($LASTEXITCODE -ne 0) { throw "ruff format --check failed" }
    & (Join-Path $VenvScripts "ruff.exe") check .
    if ($LASTEXITCODE -ne 0) { throw "ruff check failed" }
    & (Join-Path $VenvScripts "ty.exe") check
    if ($LASTEXITCODE -ne 0) { throw "ty check failed" }
    & (Join-Path $VenvScripts "deptry.exe") .
    if ($LASTEXITCODE -ne 0) { throw "deptry failed" }
    & (Join-Path $VenvScripts "ast-grep.exe") scan
    if ($LASTEXITCODE -ne 0) { throw "ast-grep scan failed" }
    Write-Host "[OK] ruff format --check, ruff check, ty check, deptry, ast-grep scan = 0 issues" -ForegroundColor Green

    # 10. Z3 formal risk proof
    $z3Out = (& $VenvPy (Join-Path $RepoRoot "evals\formal_risk_z3.py") 2>&1) -join "`n"
    if ($LASTEXITCODE -ne 0 -or $z3Out -notmatch "Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1") {
        throw "formal_risk_z3.py failed: $z3Out"
    }
    Write-Host "[OK] Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1" -ForegroundColor Green

    # 11. Nautilus FastMCP smoke
    $mcpRaw = (& $VenvPy (Join-Path $RepoRoot "scripts\nautilus_mcp_server.py") --smoke 2>&1) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw "nautilus_mcp_server.py --smoke failed: $mcpRaw" }
    $mcpJson = $mcpRaw | ConvertFrom-Json
    if (-not $mcpJson.ok -or $mcpJson.tools_count -ne 10) {
        throw "nautilus_mcp_server.py --smoke unexpected payload: $mcpRaw"
    }
    Write-Host "[OK] nautilus_mcp_server.py --smoke (10/10 tools OK)" -ForegroundColor Green

    # 12. Pytest suite
    if (-not $SkipPytest) {
        & (Join-Path $VenvScripts "pytest.exe") -q
        if ($LASTEXITCODE -ne 0) { throw "pytest -q failed" }
    }

    # 13. Gitleaks (when installed)
    if (Get-Command gitleaks -ErrorAction SilentlyContinue) {
        & gitleaks detect --no-git --source $RepoRoot --config (Join-Path $RepoRoot ".gitleaks.toml") --no-banner
        if ($LASTEXITCODE -ne 0) { throw "gitleaks detect failed" }
        Write-Host "[OK] gitleaks detect --no-git = 0 leaks" -ForegroundColor Green
    }

    Write-Host "[CHECK-PS1 VERDICT: PASS]" -ForegroundColor Green
} finally {
    Pop-Location
}

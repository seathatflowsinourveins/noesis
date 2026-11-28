## Summary

<!-- Concise summary of the change and the upstream library/tool it integrates or configures. -->

## Tier-0 Upstream SOTA Checklist

### Upstream-Only & Zero Self-Invention
- [ ] All runtime, research, observability, and CI capabilities use official pinned upstream packages/actions (`upstream-map.toml`, `pyproject.toml`, `uv.lock`, `mise.toml`).
- [ ] Zero banned packages (`yfinance`, `quantstats`, `backtrader`, `mkdocs`, etc.) introduced into `pyproject.toml` or `uv.lock`.
- [ ] `uv run ast-grep scan --config sgconfig.yml` and `uv run deptry .` report `0` issues.
- [ ] All GitHub Actions in `.github/workflows/` are full-commit-SHA pinned, least-privilege (`persist-credentials: false`), and pass `zizmor --offline .github/workflows` with `0` findings.

### Risk Kernel, Lossless Coverage & Live Tier Gate
- [ ] `src/tier0_sota/risk.py` is **untouched** (SHA-256 `ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847`) and `uv run python evals/formal_risk_z3.py` emits `Z3_FORMAL_RISK_PROOFS_UNSAT_OK=1`.
- [ ] Safety invariants preserved: `src/tier0_sota/risk.py` invariants, lossless trailing coverage (no share held without a protective trailing order), and the live tier gate (`ADR-0029`; `live_execution_enabled=False` means tier 3 is off); IBKR live ports (`4001`, `7496`, `8798`, `8796`) stay unwired.
- [ ] No secrets, `.env*` files (other than `.env.example`), hardcoded home paths, or broker account identifiers committed (`gitleaks` reports `0` leaks).

### Zero Legacy References, Manifest & Docs Parity
- [ ] Zero legacy-era paths, harnesses, or deprecated identifiers remain in code, docs, scripts, or manifests.
- [ ] `uv run python scripts/sync_manifest.py --check` exits `0` (zero manifest drift).
- [ ] `uv run zensical build --clean --strict` exits `0` when docs or ADRs are modified.

## Verification Evidence

<!-- Paste exact verbatim pass/fail output from local gates (`ruff`, `ty`, `deptry`, `ast-grep`, `pytest`, `z3`, `zizmor`, `gitleaks`). -->
```text
```

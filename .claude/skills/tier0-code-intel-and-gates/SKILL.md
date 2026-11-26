---
name: tier0-code-intel-and-gates
description: Run zero-overlap code intelligence (codebase-memory-mcp), structural AST rules (ast-grep), security and static verification (ruff incl. S, ty, gitleaks, zizmor, trivy, shellcheck, zensical, upstream audit), and upstream red-team/sandbox evals (Inspect AI, Promptfoo, Harbor).
---

# Tier-0 Code Intelligence & Disjoint Verification Directive

1. **Code Intelligence First:** Query `codebase-memory-mcp` (`cli` or stdio MCP indexed on `tier0-quant`) and `ast-grep` structural rules to inspect symbols, call graphs, and forbidden patterns before editing.
2. **Disjoint Single-Winner Gate Suite:**
   - `uv run ruff check` (incl. Bandit `S`) & `uv run ruff format --check` -> lint, formatting, and Python security hygiene.
   - `uv run ty check` -> fast static type checking.
   - `uv run ast-grep scan` -> structural AST bans (banned libraries plus custom dashboard servers, raw HTTP polling, and live endpoints, live ports, or `paper=False`).
   - Upstream & landscape provenance audits -> verify `63` disjoint `L0`–`L10` winners, `100%` Atlas bijection (`593` live stars + `54` beyond-starred), LOC caps (`<= 5,000` prod / `<= 400` risk), `BacktestEngine` witness, bounded sequential MADR 4.0 ADRs (`ADR-0001` through `ADR-0007`), and deterministic SHA-256 v4 manifest parity.
   - `shellcheck` -> strict shell script verification across repository and Harbor shell scripts.
   - `uv run zensical build --clean --strict` -> strict static docs portal integrity.
   - `uv run pytest -v` -> unit, `Hypothesis` fuzzing, `NautilusTrader` v2, `hftbacktest`, isolated `Qlib`, `MIN_CPCV_SAMPLES = 16`, offline pre-wall ALFRED Parquet reader, Agent Teams hook gates, and fail-closed post-proof `LiveNode` tests.
   - `Inspect AI` red-team suite (`13/13` deterministic probes, accuracy 100%) & `Promptfoo` (`12/12` policy cases).
   - `gitleaks` (`git` + `detect`), `zizmor` (`--offline` workflow audit), `trivy` & `osv-scanner` (locked dependency `CRITICAL` and `HIGH` CVE scans), and `syft` (CycloneDX SBOM) -> `0` secrets, `0` workflow security findings, `0` high or critical CVEs.
   - `Harbor` offline Docker oracle (no network, reward 100%).

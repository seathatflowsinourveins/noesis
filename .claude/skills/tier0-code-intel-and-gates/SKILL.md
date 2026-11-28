---
name: tier0-code-intel-and-gates
description: Run zero-overlap code intelligence (codebase-memory-mcp), structural AST rules (ast-grep), static/security verification (ruff incl. S, ty, deptry, prek, gitleaks, zizmor, trivy, syft, shellcheck, zensical), Z3 formal proofs, and upstream evals (Inspect AI, DSPy, Promptfoo, SkillEvaluator, Harbor).
---

# Tier-0 Code Intelligence & Disjoint Verification Directive

1. **Search-SOTA-First (Code Intelligence & Memory):** Query `codebase-memory-mcp` (`Z-noesis`) and `XERJ` (`http://127.0.0.1:9200`) plus `ast-grep` structural rules to inspect symbols, call graphs, and forbidden patterns before editing.
2. **Disjoint Single-Winner Upstream Gate Suite:**
   - `uv run --no-sync ruff check .` (incl. Bandit `S`) & `uv run --no-sync ruff format --check src tests scripts evals` -> lint, formatting, and Python security hygiene.
   - `uv run --no-sync ty check` -> fast static type checking.
   - `uv run --no-sync deptry .` -> declared vs imported PEP 735 dependency hygiene.
   - `uv run --no-sync ast-grep scan --config sgconfig.yml` -> 24 structural AST bans (`rules/zero_self_invention.yml`) blocking banned libraries, custom dashboard servers, raw HTTP/socket polling, LLMs on execution/risk paths, and live broker endpoints.
   - `uv run --no-sync python scripts/sync_manifest.py --check` -> programmatic SSOT sync for `ARCHITECTURE_MANIFEST.json` & `LLM_HARNESS_MANIFEST.json` (`0` arbitrary LOC caps; `46/46` curated skills; living ADRs).
   - `shellcheck` -> strict POSIX/Bash verification across repository shell scripts.
   - `uv run --no-sync zensical build --clean --strict` -> strict static docs portal integrity.
   - `uv run --no-sync pytest -q` -> unit, `Hypothesis` fuzzing, `NautilusTrader v2`, `hftbacktest`, `skfolio` `8/3` CPCV (`21` OOS paths, `cpcv_min_samples = 48`), `Dagster`, `Preflight/Failover`, `Z3`, `Inspect AI`, and `DSPy` verification; `just crosshair` (symbolic contracts), `just test-report` (`Allure 3` `:8793/test-reports/allure/`), and `just mutate-risk` (WSL isolated temp-copy `mutmut` on `risk.py`).
   - `evals/formal_risk_z3.py` (`5/5` UNSAT), `evals/tier0_inspect_eval.py` (`7/7` invariants), `evals/dspy_self_reflect.py`, `SkillEvaluator`, `Promptfoo`, and `Harbor` offline oracle.
   - `gitleaks`, `zizmor --offline --persona=pedantic .github/workflows`, `trivy fs`, `syft` (CycloneDX SBOM on release), plus GitHub native `CodeQL` & `Dependabot`.

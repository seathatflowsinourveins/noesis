"""Upstream-native Dagster Definitions & active Dual-Flywheel compounding asset graph."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from evals.dspy_self_reflect import verify_dspy_reflection_module
from evals.tier0_inspect_eval import verify_inspect_safety_suite
from scripts.run_compounding_reflection import run_compounding_reflection
from scripts.sync_manifest import parse_and_validate_adrs, validate_clean_sota_harness

from tier0_sota.discovery import DISCOVERY_WALL_ISO
from tier0_sota.risk import (
    DAILY_DRAWDOWN_BREAKER_PCT,
    MAX_STOP_LOSS_PCT,
    MIN_EXIT_RATE_RESERVE_FRACTION,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
_DEFAULT_SIM_COST_BPS_1X = 4.0
_DEFAULT_SIM_COST_BPS_2X = 8.0


def _resolve_data_root() -> Path:
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    return Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


@asset
def prewall_pit_audit() -> dict[str, object]:
    """Expose causal pre-wall cutoff date, lake status, and ALFRED vintage presence."""
    data_root = _resolve_data_root()
    lake = data_root / "prewall-lake"
    return {
        "prewall_cutoff_iso": DISCOVERY_WALL_ISO,
        "data_root": str(data_root),
        "prewall_lake_exists": lake.exists(),
        "alfred_vintages_present": (
            lake / "public-tables" / "alfred_macro_vintages.parquet"
        ).is_file(),
    }


@asset
def risk_kernel_sha256_attestation() -> dict[str, object]:
    """Verify immutable Rule-612 Decimal risk kernel digest and static bounds."""
    risk_path = Path(__file__).resolve().with_name("risk.py")
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    return {
        "sha256": digest,
        "sha256_match": digest == EXPECTED_RISK_SHA256,
        "max_stop_loss_pct": str(MAX_STOP_LOSS_PCT),
        "daily_drawdown_breaker_pct": str(DAILY_DRAWDOWN_BREAKER_PCT),
        "min_exit_rate_reserve_fraction": str(MIN_EXIT_RATE_RESERVE_FRACTION),
    }


@asset
def paper_farm_performance_snapshot() -> dict[str, object]:
    """Read fail-closed Paper Farm performance manifest without touching live brokers."""
    manifest_path = _resolve_data_root() / "manifests" / "paper_farm_live_performance_manifest.json"
    if not manifest_path.is_file():
        return {"present": False, "paper_only": True, "live_execution_enabled": False}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary = payload.get("summary") or {}
    per_lane = payload.get("per_lane") or {}
    combined_equity_usd = (
        summary.get("combined_equity_usd", payload.get("combined_equity_usd"))
        if isinstance(summary, dict)
        else payload.get("combined_equity_usd")
    )
    accounts_count = (
        len(per_lane)
        if isinstance(per_lane, dict) and per_lane
        else len(payload.get("accounts", {}))
    )
    return {
        "present": True,
        "paper_only": bool(payload.get("paper_only", True)),
        "live_execution_enabled": bool(payload.get("live_execution_enabled", False)),
        "combined_equity_usd": combined_equity_usd,
        "accounts_count": accounts_count,
    }


@asset
def inspect_safety_and_philosophy_eval() -> dict[str, object]:
    """Run Inspect AI 7-invariant safety/ADR evaluation plus DSPy/GEPA wiring check."""
    inspect_rep = verify_inspect_safety_suite()
    dspy_rep = verify_dspy_reflection_module()
    return {
        "inspect_ok": bool(inspect_rep.get("ok", False)),
        "inspect_samples": int(inspect_rep.get("samples_count", 0)),
        "dspy_ok": bool(dspy_rep.get("ok", False)),
        "dspy_metric_score": dspy_rep.get("metric_score"),
        "dspy_gepa_available": bool(dspy_rep.get("gepa_available", False)),
        "gepa_optimizer": dspy_rep.get("gepa_optimizer"),
    }


@asset
def adr_governance_and_graph_sync() -> dict[str, object]:
    """Validate executable MADR 4.0 ADR corpus, fitness bindings, and Clean SOTA Harness."""
    adr_graph = parse_and_validate_adrs(REPO_ROOT)
    harness = validate_clean_sota_harness(REPO_ROOT)
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "total_adrs": adr_graph["total_adrs"],
        "counts_by_status": adr_graph["counts_by_status"],
        "fitness_bound_accepted_ratio": adr_graph["fitness_bound_accepted_ratio"],
        "catalog_and_zensical_in_sync": adr_graph["catalog_and_zensical_in_sync"],
        "corpus_sha256": adr_graph["corpus_sha256"],
        "clean_sota_harness_ok": bool(harness["ok"]),
        "spdx_firewall_count": int(harness["spdx_firewall_count"]),
    }


@asset(deps=[paper_farm_performance_snapshot])
def sim_vs_paper_parity_reconciliation() -> dict[str, object]:
    """Reconcile Paper Farm DuckDB & manifest TCA against sim cost assumptions."""
    manifests_dir = _resolve_data_root() / "manifests"
    perf_path = manifests_dir / "paper_farm_live_performance_manifest.json"
    duckdb_files = (
        sorted(manifests_dir.glob("paper_lane_*.duckdb")) if manifests_dir.is_dir() else []
    )

    tca_rows = 0
    is_bps_samples: list[float] = []
    lanes_queried: list[str] = []
    duckdb_errors: list[str] = []

    for db_path in duckdb_files:
        lanes_queried.append(db_path.stem)
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                tables = {
                    str(r[0]).lower()
                    for r in con.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'main'"
                    ).fetchall()
                }
                if "paper_tca_fills" in tables:
                    rows = con.execute(
                        "SELECT implementation_shortfall_bps FROM paper_tca_fills "
                        "WHERE implementation_shortfall_bps IS NOT NULL"
                    ).fetchall()
                    for (val,) in rows:
                        is_bps_samples.append(float(val))
                    tca_rows += len(rows)
            finally:
                con.close()
        except Exception as exc:  # noqa: BLE001
            duckdb_errors.append(f"{db_path.name}:{type(exc).__name__}")

    perf_tca_bps: float | None = None
    if perf_path.is_file():
        perf_doc = json.loads(perf_path.read_text(encoding="utf-8"))
        summary = perf_doc.get("summary") or {}
        if isinstance(summary, dict) and isinstance(summary.get("mean_is_bps"), (int, float)):
            perf_tca_bps = float(summary["mean_is_bps"])

    realized_mean_is_bps = (
        round(sum(is_bps_samples) / len(is_bps_samples), 4)
        if is_bps_samples
        else (round(perf_tca_bps, 4) if perf_tca_bps is not None else None)
    )
    recommended_1x = (
        round(max(_DEFAULT_SIM_COST_BPS_1X, abs(realized_mean_is_bps)), 2)
        if realized_mean_is_bps is not None
        else _DEFAULT_SIM_COST_BPS_1X
    )
    payload: dict[str, Any] = {
        "schema_version": "tier0.tca-calibration-manifest.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
        "lanes_queried": lanes_queried,
        "duckdb_tca_rows": tca_rows,
        "duckdb_errors": duckdb_errors,
        "realized_mean_is_bps": realized_mean_is_bps,
        "sim_default_cost_bps_1x": _DEFAULT_SIM_COST_BPS_1X,
        "sim_default_cost_bps_2x": _DEFAULT_SIM_COST_BPS_2X,
        "recommended_sim_cost_bps_1x": recommended_1x,
        "recommended_sim_cost_bps_2x": round(2.0 * recommended_1x, 2),
        "parity_within_2x_envelope": (
            True if realized_mean_is_bps is None else abs(realized_mean_is_bps) <= 8.0
        ),
    }
    out_path = manifests_dir / "tca_calibration_manifest.json"
    _write_json_atomic(out_path, payload)
    return {**payload, "manifest_path": str(out_path)}


def _collect_imported_top_modules(repo_root: Path) -> set[str]:
    mods: set[str] = set()
    for sub in ("src", "evals", "scripts"):
        base = repo_root / sub
        if not base.is_dir():
            continue
        for py_file in base.rglob("*.py"):
            try:
                tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        mods.add(alias.name.split(".", 1)[0].lower())
                elif isinstance(node, ast.ImportFrom) and node.module:
                    mods.add(node.module.split(".", 1)[0].lower())
    return mods


@asset(deps=[prewall_pit_audit, adr_governance_and_graph_sync])
def frontier_trigger_telemetry() -> dict[str, object]:
    """Measure quantified L11..L15 earned-trigger counters and emit status manifest."""
    data_root = _resolve_data_root()
    manifests_dir = data_root / "manifests"
    lake_manifest = data_root / "prewall-lake" / "PREWALL_LAKE_MANIFEST.json"

    prewall_lake_gb = 0.0
    if lake_manifest.is_file():
        lm_doc = json.loads(lake_manifest.read_text(encoding="utf-8"))
        tar_bytes = lm_doc.get("tar_bytes")
        if isinstance(tar_bytes, (int, float)):
            prewall_lake_gb = round(float(tar_bytes) / (1024.0**3), 4)

    max_concurrent_positions = 0
    perf_path = manifests_dir / "paper_farm_live_performance_manifest.json"
    if perf_path.is_file():
        perf_doc = json.loads(perf_path.read_text(encoding="utf-8"))
        per_lane = perf_doc.get("per_lane") or {}
        if isinstance(per_lane, dict):
            for lane_info in per_lane.values():
                if isinstance(lane_info, dict):
                    pos_cnt = lane_info.get("open_positions_count", 0)
                    if isinstance(pos_cnt, int):
                        max_concurrent_positions += pos_cnt

    upstream = tomllib.loads((REPO_ROOT / "upstream-map.toml").read_text(encoding="utf-8"))
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    imported_mods = _collect_imported_top_modules(REPO_ROOT)

    pkg_to_mod = {
        "alpaca-py": "alpaca",
        "exchange-calendars": "exchange_calendars",
        "ml4t-diagnostic": "ml4t",
        "ml4t-engineer": "ml4t",
        "nautilus-trader": "nautilus_trader",
        "opentelemetry-api": "opentelemetry",
        "opentelemetry-sdk": "opentelemetry",
        "opentelemetry-exporter-otlp": "opentelemetry",
        "prometheus-client": "prometheus_client",
        "z3-solver": "z3",
        "inspect-ai": "inspect_ai",
        "pyyaml": "yaml",
    }
    cli_only = {
        "ast-grep-cli",
        "dagster-webserver",
        "deptry",
        "dvc",
        "hypothesis",
        "optuna-dashboard",
        "pytest",
        "ruff",
        "ty",
        "zensical",
    }
    declared_pkgs: list[str] = list(pyproject.get("project", {}).get("dependencies", []))
    for grp in ("research", "evals", "observability"):
        declared_pkgs.extend(pyproject.get("dependency-groups", {}).get(grp, []))

    unwired_lock_packages: list[str] = []
    for spec in declared_pkgs:
        pkg = re.split(r"[<>=!~;\[\s]", str(spec).strip(), maxsplit=1)[0].lower().replace("_", "-")
        if not pkg or pkg in cli_only:
            continue
        mod = pkg_to_mod.get(pkg, pkg.replace("-", "_"))
        if mod not in imported_mods:
            unwired_lock_packages.append(pkg)

    adr_count = len(list((REPO_ROOT / "docs" / "adr").glob("ADR-*.md")))
    spdx_firewall_count = len(upstream.get("rules", {}).get("spdx_license_firewall", []))

    payload: dict[str, Any] = {
        "schema_version": "tier0.frontier-trigger-telemetry.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prewall_lake_gb": prewall_lake_gb,
        "max_concurrent_positions": max_concurrent_positions,
        "adr_count": adr_count,
        "spdx_firewall_count": spdx_firewall_count,
        "unwired_lock_packages": sorted(set(unwired_lock_packages)),
        "triggers": {
            "l11_vortex_lake_gt_100gb": prewall_lake_gb > 100.0,
            "l13_multi_day_positions_gt_10": max_concurrent_positions > 10,
        },
    }
    out_path = manifests_dir / "frontier_trigger_status.json"
    _write_json_atomic(out_path, payload)
    return {**payload, "manifest_path": str(out_path)}


@asset(
    deps=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
        inspect_safety_and_philosophy_eval,
        adr_governance_and_graph_sync,
        sim_vs_paper_parity_reconciliation,
        frontier_trigger_telemetry,
    ]
)
def compounding_self_reflection_snapshot() -> dict[str, object]:
    """Actively materialize the Dual-Flywheel compounding self-reflection manifest."""
    manifests_dir = _resolve_data_root() / "manifests"
    active = os.environ.get("TIER0_DAGSTER_ACTIVE_REFLECT", "1").strip() != "0"
    offline_mode = os.environ.get("TIER0_DAGSTER_OFFLINE", "0").strip() == "1"

    if active:
        payload = run_compounding_reflection(
            offline=offline_mode,
            manifest_dir=manifests_dir,
        )
    else:
        manifest_path = manifests_dir / "compounding_self_reflection_manifest.json"
        if not manifest_path.is_file():
            return {"present": False, "path": str(manifest_path)}
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    cross_leg = payload.get("cross_leg_falsification") or {}
    return {
        "present": True,
        "active_materialized": active,
        "schema_version": payload.get("schema_version"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "data_source": payload.get("data_source"),
        "best_profile": payload.get("optuna", {}).get("best_profile"),
        "best_value": payload.get("optuna", {}).get("best_value"),
        "cross_leg_pbo": {
            leg: (cross_leg.get(leg) or {}).get("falsification_1x", {}).get("pbo")
            for leg in ("LONG", "SHORT", "COMBINED")
            if leg in cross_leg
        },
        "inspect_accuracy": payload.get("inspect", {}).get("accuracy"),
        "risk_sha256_match": payload.get("safety", {}).get("risk_sha256_match"),
    }


compounding_reflection_job = define_asset_job(
    name="compounding_reflection_job",
    selection=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
        inspect_safety_and_philosophy_eval,
        adr_governance_and_graph_sync,
        sim_vs_paper_parity_reconciliation,
        frontier_trigger_telemetry,
        compounding_self_reflection_snapshot,
    ],
)

daily_compounding_reflection_schedule = ScheduleDefinition(
    name="daily_compounding_reflection_schedule",
    job=compounding_reflection_job,
    cron_schedule="0 18 * * 1-5",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[
        prewall_pit_audit,
        risk_kernel_sha256_attestation,
        paper_farm_performance_snapshot,
        inspect_safety_and_philosophy_eval,
        adr_governance_and_graph_sync,
        sim_vs_paper_parity_reconciliation,
        frontier_trigger_telemetry,
        compounding_self_reflection_snapshot,
    ],
    jobs=[compounding_reflection_job],
    schedules=[daily_compounding_reflection_schedule],
)

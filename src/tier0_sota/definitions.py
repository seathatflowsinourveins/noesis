"""Upstream-native Dagster Definitions & active Dual-Flywheel compounding asset graph."""

from __future__ import annotations

import hashlib
import json
import os
import tomllib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
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
    from tier0_sota.discovery import DISCOVERY_WALL_ISO

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
    from tier0_sota.risk import (
        DAILY_DRAWDOWN_BREAKER_PCT,
        MAX_STOP_LOSS_PCT,
        MIN_EXIT_RATE_RESERVE_FRACTION,
    )

    risk_path = Path(__file__).resolve().with_name("risk.py")
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    return {
        "sha256": digest,
        "sha256_match": digest == EXPECTED_RISK_SHA256,
        "max_stop_loss_pct": str(MAX_STOP_LOSS_PCT),
        "daily_drawdown_breaker_pct": str(DAILY_DRAWDOWN_BREAKER_PCT),
        "min_exit_rate_reserve_fraction": str(MIN_EXIT_RATE_RESERVE_FRACTION),
    }


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _summarize_tv_half_auto(
    manifests_dir: Path, perf_doc: dict[str, Any] | None = None
) -> dict[str, Any]:
    perf_tv = (
        dict(perf_doc["tv_half_auto"])
        if isinstance(perf_doc, dict) and isinstance(perf_doc.get("tv_half_auto"), dict)
        else {}
    )
    comp = _read_json_optional(manifests_dir / "tv_sota_pack" / "tv_compounding_manifest.json")
    raw_shas = comp.get("pine_shas")
    pine_shas: dict[str, Any] = raw_shas if isinstance(raw_shas, dict) else {}
    pine_count = len(pine_shas) or (1 if comp.get("pine_omni_sha256") else 0)
    return {
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "active_session": str(
            comp.get("active_session") or perf_tv.get("active_session") or "CLOSED"
        ),
        "tickets_count": int(comp.get("tickets_count") or perf_tv.get("tickets_count") or 0),
        "ready_tickets_count": int(
            comp.get("ready_tickets_count") or perf_tv.get("ready_tickets_count") or 0
        ),
        "standby_presets_count": int(
            comp.get("standby_presets_count") or perf_tv.get("standby_presets_count") or 0
        ),
        "staged_presets_total": int(
            perf_tv.get("staged_presets_total") or comp.get("staged_count") or 0
        ),
        "duckdb_audit_rows": int(
            comp.get("duckdb_audit_rows") or perf_tv.get("duckdb_audit_rows") or 0
        ),
        "unusual_vol_count": max(
            int(comp.get("unusual_vol_count") or 0)
            + int(comp.get("standby_unusual_vol_count") or 0),
            int(perf_tv.get("unusual_vol_count") or 0),
        ),
        "darkpool_hits_count": max(
            int(comp.get("darkpool_hits_count") or 0)
            + int(comp.get("standby_darkpool_hits_count") or 0),
            int(perf_tv.get("darkpool_hits_count") or 0),
        ),
        "darkpool_trf_covered_count": max(
            int(comp.get("darkpool_trf_covered_count") or 0),
            int(perf_tv.get("darkpool_trf_covered_count") or 0),
        ),
        "pine_scripts_hashed": pine_count,
    }


@asset
def paper_farm_performance_snapshot() -> dict[str, object]:
    """Read fail-closed Paper Farm performance manifest without touching live brokers."""
    manifests_dir = _resolve_data_root() / "manifests"
    manifest_path = manifests_dir / "paper_farm_live_performance_manifest.json"
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
        "tv_half_auto": _summarize_tv_half_auto(manifests_dir, payload),
    }


@asset
def inspect_safety_and_philosophy_eval() -> dict[str, object]:
    """Run Inspect AI 7-invariant safety/ADR evaluation plus DSPy/GEPA wiring check."""
    from evals.dspy_self_reflect import verify_dspy_reflection_module
    from evals.tier0_inspect_eval import verify_inspect_safety_suite

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
    import ast

    from scripts.sync_manifest import parse_and_validate_adrs, validate_clean_sota_harness

    upstream_cfg = tomllib.loads((REPO_ROOT / "upstream-map.toml").read_text(encoding="utf-8"))
    ast_cache: dict[Path, ast.Module] = {}
    adr_graph = parse_and_validate_adrs(REPO_ROOT, upstream_cfg=upstream_cfg, ast_cache=ast_cache)
    harness = validate_clean_sota_harness(REPO_ROOT, upstream=upstream_cfg, ast_cache=ast_cache)
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
    from tier0_sota.live.paper_farm import open_bounded_duckdb

    manifests_dir = _resolve_data_root() / "manifests"
    perf_path = manifests_dir / "paper_farm_live_performance_manifest.json"
    duckdb_files = (
        sorted(manifests_dir.glob("paper_lane_*.duckdb")) if manifests_dir.is_dir() else []
    )

    tca_rows = 0
    is_bps_samples: list[float] = []
    liquid_ge10_samples: list[float] = []
    lanes_queried: list[str] = []
    duckdb_errors: list[str] = []

    for db_path in duckdb_files:
        lanes_queried.append(db_path.stem)
        try:
            con = open_bounded_duckdb(db_path, read_only=True, max_threads=4)
            try:
                tables = {
                    str(r[0]).lower()
                    for r in con.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'main'"
                    ).fetchall()
                }
                if "paper_tca_fills" in tables:
                    cols = {
                        str(r[0]).lower()
                        for r in con.execute("DESCRIBE paper_tca_fills").fetchall()
                    }
                    if "fill_price" in cols:
                        rows = con.execute(
                            "SELECT implementation_shortfall_bps, fill_price FROM paper_tca_fills "
                            "WHERE implementation_shortfall_bps IS NOT NULL"
                        ).fetchall()
                        for val, fpx in rows:
                            v_f = float(val)
                            is_bps_samples.append(v_f)
                            if fpx is not None and float(fpx) >= 10.0:
                                liquid_ge10_samples.append(v_f)
                    else:
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
    liquid_ge10_mean_is_bps = (
        round(sum(liquid_ge10_samples) / len(liquid_ge10_samples), 4)
        if liquid_ge10_samples
        else None
    )
    liquid_ge10_median_is_bps: float | None = None
    if liquid_ge10_samples:
        s_liq = sorted(liquid_ge10_samples)
        mid_i = len(s_liq) // 2
        liquid_ge10_median_is_bps = round(
            s_liq[mid_i] if len(s_liq) % 2 == 1 else 0.5 * (s_liq[mid_i - 1] + s_liq[mid_i]),
            4,
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
        "liquid_ge10_tca_rows": len(liquid_ge10_samples),
        "liquid_ge10_mean_is_bps": liquid_ge10_mean_is_bps,
        "liquid_ge10_median_is_bps": liquid_ge10_median_is_bps,
        "liquid_ge10_within_2x_envelope": (
            True if liquid_ge10_mean_is_bps is None else abs(liquid_ge10_mean_is_bps) <= 8.0
        ),
        "rth_entry_collar_bps": 5.0,
        "min_liquid_entry_price_usd": 10.0,
        "max_rth_quoted_spread_bps": 12.0,
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


def _scan_dir_bytes(root_path: str) -> int:
    total = 0
    stack = [root_path]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        else:
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def _measure_prewall_lake_disk_gb(lake_dir: Path) -> float:
    """Measure non-symlink byte footprint of local prewall-lake directory in GiB via os.scandir."""
    if not lake_dir.is_dir():
        return 0.0
    top_dirs: list[str] = []
    total_bytes = 0
    try:
        with os.scandir(lake_dir) as it:
            for entry in it:
                try:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        top_dirs.append(entry.path)
                    else:
                        total_bytes += entry.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
    except OSError:
        return 0.0
    if len(top_dirs) > 1:
        with ThreadPoolExecutor(max_workers=min(8, len(top_dirs))) as pool:
            total_bytes += sum(pool.map(_scan_dir_bytes, top_dirs))
    elif top_dirs:
        total_bytes += _scan_dir_bytes(top_dirs[0])
    return round(total_bytes / (1024.0**3), 4)


@asset(deps=[prewall_pit_audit, adr_governance_and_graph_sync])
def frontier_trigger_telemetry() -> dict[str, object]:
    """Measure quantified L11..L15 earned-trigger counters and emit status manifest."""
    from scripts.sync_manifest import audit_zero_paper_winners

    data_root = _resolve_data_root()
    manifests_dir = data_root / "manifests"
    lake_dir = data_root / "prewall-lake"
    lake_manifest = lake_dir / "PREWALL_LAKE_MANIFEST.json"

    prewall_lake_manifest_gb = 0.0
    if lake_manifest.is_file():
        lm_doc = json.loads(lake_manifest.read_text(encoding="utf-8"))
        tar_bytes = lm_doc.get("tar_bytes")
        if isinstance(tar_bytes, (int, float)):
            prewall_lake_manifest_gb = round(float(tar_bytes) / (1024.0**3), 4)

    deep_scan = os.environ.get("TIER0_LAKE_DEEP_SCAN", "0").strip() == "1"
    prewall_lake_disk_gb = (
        prewall_lake_manifest_gb
        if (prewall_lake_manifest_gb > 0.0 and not deep_scan)
        else _measure_prewall_lake_disk_gb(lake_dir)
    )
    prewall_lake_gb = (
        prewall_lake_disk_gb if prewall_lake_disk_gb > 0.0 else prewall_lake_manifest_gb
    )

    perf_doc: dict[str, Any] = {}
    max_concurrent_positions = 0
    perf_path = manifests_dir / "paper_farm_live_performance_manifest.json"
    if perf_path.is_file():
        perf_doc = _read_json_optional(perf_path)
        per_lane = perf_doc.get("per_lane") or {}
        if isinstance(per_lane, dict):
            for lane_info in per_lane.values():
                if isinstance(lane_info, dict):
                    pos_cnt = lane_info.get("open_positions_count", 0)
                    if isinstance(pos_cnt, int):
                        max_concurrent_positions += pos_cnt

    upstream = tomllib.loads((REPO_ROOT / "upstream-map.toml").read_text(encoding="utf-8"))
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pw_audit = audit_zero_paper_winners(REPO_ROOT, pyproject)
    unwired_lock_packages = sorted(pw_audit.get("unwired_packages") or [])

    adr_count = len(list((REPO_ROOT / "docs" / "adr").glob("ADR-*.md")))
    spdx_firewall_count = len(upstream.get("rules", {}).get("spdx_license_firewall", []))

    gepa_manifest = manifests_dir / "dspy_gepa_compiled_reflection.json"
    gepa_compiled = False
    if gepa_manifest.is_file():
        try:
            gepa_doc = json.loads(gepa_manifest.read_text(encoding="utf-8"))
            gepa_compiled = bool(gepa_doc.get("compiled", False))
        except (OSError, json.JSONDecodeError):
            gepa_compiled = False

    tca_manifest = manifests_dir / "tca_calibration_manifest.json"
    tca_rows_calibrated = 0
    if tca_manifest.is_file():
        try:
            tca_doc = json.loads(tca_manifest.read_text(encoding="utf-8"))
            tca_rows_calibrated = int(tca_doc.get("duckdb_tca_rows") or 0)
        except (OSError, json.JSONDecodeError):
            tca_rows_calibrated = 0

    tv_compounding = _summarize_tv_half_auto(manifests_dir, perf_doc)
    starred_doc = _read_json_optional(manifests_dir / "starred_frontier_landscape_manifest.json")
    l15_doc = _read_json_optional(manifests_dir / "l15_supply_chain_and_formal_audit.json")

    def _sub_dict(doc: dict[str, Any], key: str) -> dict[str, Any]:
        val = doc.get(key)
        return val if isinstance(val, dict) else {}

    am_block = _sub_dict(l15_doc, "alertmanager")
    alertmanager_9093_healthy = bool(
        l15_doc.get("alertmanager_9093_healthy", am_block.get("healthy_9093", False))
    )
    ch_block = _sub_dict(l15_doc, "crosshair")
    z3_block = _sub_dict(l15_doc, "z3_formal_proofs")
    insp_block = _sub_dict(l15_doc, "inspect_ai")
    sk_block = _sub_dict(l15_doc, "skills_lock")

    crosshair_ok = bool(ch_block.get("ok", False))
    z3_all_unsat = bool(z3_block.get("all_unsat", False))
    inspect_audit_ok = bool(insp_block.get("ok", False))
    skills_46_ok = bool(sk_block.get("parity_ok", False))

    starred_summary_block = _sub_dict(starred_doc, "summary")
    starred_versions = _sub_dict(starred_doc, "versions")
    starred_engines = _sub_dict(starred_doc, "engines") or _sub_dict(starred_doc, "capabilities")
    starred_all_ok = bool(
        starred_summary_block.get("all_engines_pass")
        or starred_doc.get("all_ok")
        or starred_doc.get("ok")
        or (
            bool(starred_engines)
            and all(
                (v.get("status") == "VERIFIED_PASS" if isinstance(v, dict) else bool(v))
                for v in starred_engines.values()
            )
        )
    )
    starred_graduated = list(starred_doc.get("graduated_packages") or sorted(starred_versions))

    payload: dict[str, Any] = {
        "schema_version": "tier0.frontier-trigger-telemetry.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prewall_lake_gb": prewall_lake_gb,
        "prewall_lake_disk_gb": prewall_lake_disk_gb,
        "prewall_lake_manifest_gb": prewall_lake_manifest_gb,
        "max_concurrent_positions": max_concurrent_positions,
        "adr_count": adr_count,
        "spdx_firewall_count": spdx_firewall_count,
        "checked_default_packages": int(pw_audit.get("checked_default_packages") or 0),
        "unwired_lock_packages": unwired_lock_packages,
        "tca_rows_calibrated": tca_rows_calibrated,
        "alertmanager_9093_healthy": alertmanager_9093_healthy,
        "tv_half_auto_compounding": tv_compounding,
        "starred_frontier_landscape": {
            "present": bool(starred_doc),
            "schema_version": starred_doc.get("schema_version"),
            "generated_at_utc": starred_doc.get("generated_at_utc"),
            "all_engines_ok": starred_all_ok,
            "engines_verified_pass": int(
                starred_summary_block.get("engines_verified_pass") or len(starred_engines)
            ),
            "engines_total": int(
                starred_summary_block.get("engines_total") or len(starred_engines)
            ),
            "graduated_packages": starred_graduated,
        },
        "l15_formal_supply_chain_audit": {
            "present": bool(l15_doc),
            "schema_version": l15_doc.get("schema_version"),
            "generated_at_utc": l15_doc.get("generated_at_utc"),
            "overall_ok": bool(l15_doc.get("overall_ok", False)),
            "z3_all_unsat": z3_all_unsat,
            "crosshair_ok": crosshair_ok,
            "inspect_ok": inspect_audit_ok,
            "skills_46_parity_ok": skills_46_ok,
            "alertmanager_9093_healthy": alertmanager_9093_healthy,
        },
        "triggers": {
            "l11_vortex_lake_gt_100gb": prewall_lake_gb > 100.0,
            "l12_tabicl_benchmark_pending": False,
            "l13_multi_day_positions_gt_10": max_concurrent_positions > 10,
            "l14_observability_tca_calibrated": tca_rows_calibrated > 0,
            "l14_alertmanager_9093_live": alertmanager_9093_healthy,
            "l15_dspy_gepa_compiled": gepa_compiled,
            "l15_crosshair_symbolic_verified": crosshair_ok,
            "l11_l13_starred_frontier_graduated": starred_all_ok,
        },
    }
    out_path = manifests_dir / "frontier_trigger_status.json"
    _write_json_atomic(out_path, payload)
    return {**payload, "manifest_path": str(out_path)}


def _summarize_nonstop_compounding_state(manifests_dir: Path) -> dict[str, Any]:
    """Read fail-closed Stage-1 calibration & nonstop compounding loop heartbeat manifests."""
    loop_doc = _read_json_optional(manifests_dir / "nonstop_compounding_loop_status.json")
    hist_doc = _read_json_optional(
        manifests_dir / "historical_extreme_mover_calibration_manifest.json"
    )
    prewall_doc = _read_json_optional(
        manifests_dir / "prewall_early_mover_calibration_manifest.json"
    )
    venue_posture = loop_doc.get("venue_posture")
    vp = venue_posture if isinstance(venue_posture, dict) else {}
    taxonomy = (
        hist_doc.get("categorized_mover_taxonomy")
        or hist_doc.get("mover_taxonomy")
        or hist_doc.get("taxonomy")
        or hist_doc.get("categories")
    )
    per_sym = hist_doc.get("per_symbol_min_trailing_pct")
    return {
        "loop_present": bool(loop_doc),
        "loop_iteration": loop_doc.get("iteration"),
        "loop_verdict": loop_doc.get("overall_verdict"),
        "loop_completed_at_utc": loop_doc.get("completed_at_utc"),
        "alpaca_venue_profile": vp.get("alpaca_venue_profile"),
        "combined_paper_max_rpm": vp.get("combined_paper_max_rpm"),
        "paper_only": bool(loop_doc.get("paper_only", True)),
        "live_execution_enabled": bool(loop_doc.get("live_execution_enabled", False)),
        "historical_calibration_present": bool(hist_doc),
        "historical_calibration_categories": (
            len(taxonomy) if isinstance(taxonomy, (dict, list)) else 0
        ),
        "historical_calibration_symbols": (len(per_sym) if isinstance(per_sym, dict) else 0),
        "nvda_premarket_verified": isinstance(
            hist_doc.get("nvda_monday_premarket_e2e_replay"), dict
        ),
        "prewall_calibration_present": bool(prewall_doc),
    }


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
        from scripts.run_compounding_reflection import run_compounding_reflection

        payload = run_compounding_reflection(
            offline=offline_mode,
            manifest_dir=manifests_dir,
        )
    else:
        manifest_path = manifests_dir / "compounding_self_reflection_manifest.json"
        if not manifest_path.is_file():
            return {"present": False, "path": str(manifest_path)}
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    if os.environ.get("TIER0_DAGSTER_NONSTOP_LOOP", "0").strip() == "1":
        from scripts.run_nonstop_compounding_loop import run_compounding_iteration

        run_compounding_iteration(
            manifest_dir=manifests_dir,
            skip_calibration=offline_mode,
            skip_reflection=True,
            dry_run=offline_mode,
            probe_services=not offline_mode,
        )

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
        "tv_half_auto": payload.get("tv_half_auto"),
        "nonstop_compounding_loop": _summarize_nonstop_compounding_state(manifests_dir),
    }


_COMPOUNDING_ASSET_SELECTION = [
    prewall_pit_audit,
    risk_kernel_sha256_attestation,
    paper_farm_performance_snapshot,
    inspect_safety_and_philosophy_eval,
    adr_governance_and_graph_sync,
    sim_vs_paper_parity_reconciliation,
    frontier_trigger_telemetry,
    compounding_self_reflection_snapshot,
]

NONSTOP_COMPOUNDING_TIMER_SPEC: dict[str, object] = {
    "systemd_service": "tier0-compounding-loop.service",
    "systemd_timer": "tier0-compounding-loop.timer",
    "cadence": "2h",
    "dagster_job": "nonstop_compounding_loop_job",
    "dagster_schedule": "daily_compounding_reflection_schedule",
    "paper_only": True,
    "live_execution_enabled": False,
}

compounding_reflection_job = define_asset_job(
    name="compounding_reflection_job",
    selection=_COMPOUNDING_ASSET_SELECTION,
)

nonstop_compounding_loop_job = define_asset_job(
    name="nonstop_compounding_loop_job",
    selection=_COMPOUNDING_ASSET_SELECTION,
    description=(
        "24/5 paper-only non-stop compounding loop asset job paired with native "
        "systemd timer tier0-compounding-loop.timer (2h cadence, zero Python sleep loops)."
    ),
)

daily_compounding_reflection_schedule = ScheduleDefinition(
    name="daily_compounding_reflection_schedule",
    job=compounding_reflection_job,
    cron_schedule="0 18 * * 1-5",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=_COMPOUNDING_ASSET_SELECTION,
    jobs=[compounding_reflection_job, nonstop_compounding_loop_job],
    schedules=[daily_compounding_reflection_schedule],
)

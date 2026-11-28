"""Unified Paper Farm performance manifest, TCA aggregation & OTel/Prometheus telemetry."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

from tier0_sota import engine as eng
from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    IBKR_PAPER_ACCOUNT,
    _duckdb_filename,
    default_manifest_dir,
    open_bounded_duckdb,
)

ALL_FARM_ACCOUNTS: tuple[str, ...] = (*ALPACA_LANE_PROFILES.keys(), IBKR_PAPER_ACCOUNT)
_DEC_ZERO = Decimal("0")


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2) + "\n")


def _read_json_safe(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _percentile_decimal(values: list[Decimal], quantile: Decimal) -> Decimal:
    """Deterministic linear-interpolated Decimal quantile (returns 0 on empty)."""
    if not values:
        return _DEC_ZERO
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = quantile * Decimal(len(ordered) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - Decimal(lo)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


def _mean_decimal(values: list[Decimal]) -> Decimal:
    if not values:
        return _DEC_ZERO
    return sum(values, _DEC_ZERO) / Decimal(len(values))


def _to_dec(val: Any) -> Decimal:
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


def _summarize_tca_fill_details(details: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute Perold order-level, notional-weighted, and liquid >=$10 TCA metrics in Decimal."""
    if not details:
        return {
            "unique_orders_count": 0,
            "order_mean_is_bps": 0.0,
            "order_median_is_bps": 0.0,
            "notional_weighted_is_bps": 0.0,
            "mean_execution_bps": 0.0,
            "order_mean_execution_bps": 0.0,
            "mean_delay_bps": 0.0,
            "liquid_ge10_fills_count": 0,
            "liquid_ge10_orders_count": 0,
            "liquid_ge10_mean_is_bps": 0.0,
            "liquid_ge10_order_mean_is_bps": 0.0,
            "liquid_ge10_notional_weighted_is_bps": 0.0,
        }

    def _agg(sub: list[dict[str, Any]]) -> tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]:
        if not sub:
            return 0, _DEC_ZERO, _DEC_ZERO, _DEC_ZERO, _DEC_ZERO, _DEC_ZERO
        by_order: dict[tuple[str, str], list[dict[str, Any]]] = {}
        notional_sum = _DEC_ZERO
        notional_is_num = _DEC_ZERO
        for idx, row in enumerate(sub):
            oid = str(row.get("broker_order_id") or f"row-{idx}")
            acct = str(row.get("account") or "")
            by_order.setdefault((acct, oid), []).append(row)
            qty_d: Decimal = row["filled_qty"]
            px_d: Decimal = row["fill_price"]
            is_d: Decimal = row["is_bps"]
            if qty_d > _DEC_ZERO and px_d > _DEC_ZERO:
                notional = qty_d * px_d
                notional_sum += notional
                notional_is_num += is_d * notional
        ord_is_vals: list[Decimal] = []
        ord_exec_vals: list[Decimal] = []
        for rows_o in by_order.values():
            q_tot = sum((r["filled_qty"] for r in rows_o if r["filled_qty"] > _DEC_ZERO), _DEC_ZERO)
            if q_tot > _DEC_ZERO:
                w_is = sum((r["is_bps"] * r["filled_qty"] for r in rows_o), _DEC_ZERO) / q_tot
                w_ex = (
                    sum((r["execution_bps"] * r["filled_qty"] for r in rows_o), _DEC_ZERO) / q_tot
                )
            else:
                w_is = _mean_decimal([r["is_bps"] for r in rows_o])
                w_ex = _mean_decimal([r["execution_bps"] for r in rows_o])
            ord_is_vals.append(w_is)
            ord_exec_vals.append(w_ex)
        nw_is = (
            (notional_is_num / notional_sum)
            if notional_sum > _DEC_ZERO
            else _mean_decimal(ord_is_vals)
        )
        return (
            len(by_order),
            _mean_decimal(ord_is_vals),
            _percentile_decimal(ord_is_vals, Decimal("0.50")),
            nw_is,
            _mean_decimal(ord_exec_vals),
            _mean_decimal([r["is_bps"] for r in sub]),
        )

    ord_n, ord_mean_is, ord_med_is, nw_is, ord_mean_exec, _ = _agg(details)
    liq_rows = [r for r in details if r["fill_price"] >= Decimal("10")]
    liq_ord_n, liq_ord_is, _, liq_nw_is, _, liq_row_is = _agg(liq_rows)
    return {
        "unique_orders_count": ord_n,
        "order_mean_is_bps": round(float(ord_mean_is), 6),
        "order_median_is_bps": round(float(ord_med_is), 6),
        "notional_weighted_is_bps": round(float(nw_is), 6),
        "mean_execution_bps": round(float(_mean_decimal([r["execution_bps"] for r in details])), 6),
        "order_mean_execution_bps": round(float(ord_mean_exec), 6),
        "mean_delay_bps": round(float(_mean_decimal([r["delay_bps"] for r in details])), 6),
        "liquid_ge10_fills_count": len(liq_rows),
        "liquid_ge10_orders_count": liq_ord_n,
        "liquid_ge10_mean_is_bps": round(float(liq_row_is), 6),
        "liquid_ge10_order_mean_is_bps": round(float(liq_ord_is), 6),
        "liquid_ge10_notional_weighted_is_bps": round(float(liq_nw_is), 6),
    }


def _populate_duckdb_counts(conn: duckdb.DuckDBPyConnection, out: dict[str, Any]) -> None:
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    if "paper_wire_orders" in tables:
        row = conn.execute("SELECT COUNT(*) FROM paper_wire_orders").fetchone()
        out["duckdb_orders"] = int(row[0]) if row else 0
        ext_row = conn.execute(
            "SELECT COUNT(*) FROM paper_wire_orders "
            "WHERE UPPER(session) IN ('PRE_MARKET', 'AFTER_HOURS', 'OVERNIGHT') "
            "AND order_type = 'LIMIT_EXTENDED'"
        ).fetchone()
        out["ext_orders_db"] = int(ext_row[0]) if ext_row else 0
    if "paper_roundtrips" in tables:
        sql = (
            "SELECT COUNT(*), "
            "COALESCE(SUM(CASE WHEN realized_pnl_usd > 0 THEN 1 ELSE 0 END), 0), "
            "COALESCE(SUM(realized_pnl_usd), 0.0) FROM paper_roundtrips"
        )
        if (row := conn.execute(sql).fetchone()) is not None:
            out["roundtrips"] = int(row[0])
            out["wins"] = int(row[1])
            out["realized_pnl_usd"] = round(float(row[2]), 2)
    if "paper_tca_fills" in tables:
        columns = set(conn.table("paper_tca_fills").columns)
        if "execution_id" in columns and "paper_entry_decisions" in tables:
            count = (
                conn.execute(
                    "SELECT COUNT(*) FROM paper_tca_fills f JOIN paper_wire_orders w "
                    "ON w.account=f.account AND w.broker_order_id=f.broker_order_id "
                    "WHERE f.execution_id IS NOT NULL"
                ).fetchone()
                if "paper_wire_orders" in tables
                else None
            )
            out["worker_fills"] = count[0] if count else 0
        cursor = conn.execute("SELECT * FROM paper_tca_fills")
        col_idx = {desc[0]: idx for idx, desc in enumerate(cursor.description or ())}
        tca_rows = cursor.fetchall()
        out["tca_fills_count"] = len(tca_rows)
        is_vals: list[Decimal] = []
        net_vals: list[Decimal] = []
        borrow_sum = _DEC_ZERO
        borrow_bps_vals: list[Decimal] = []
        mk60_vals: list[Decimal] = []
        fill_details: list[dict[str, Any]] = []

        def _col(row: tuple[Any, ...], name: str, default: Any = None) -> Any:
            idx = col_idx.get(name)
            return row[idx] if idx is not None else default

        for row in tca_rows:
            is_d = _to_dec(_col(row, "implementation_shortfall_bps"))
            is_vals.append(is_d)
            net_vals.append(_to_dec(_col(row, "total_net_cost_bps")))
            borrow_sum += _to_dec(_col(row, "borrow_fee_usd"))
            borrow_bps_vals.append(_to_dec(_col(row, "borrow_bps")))
            mk60 = _col(row, "markout_60s_bps")
            if mk60 is not None:
                mk60_vals.append(_to_dec(mk60))
            qty_v = _col(row, "filled_qty")
            fpx_v = _col(row, "fill_price")
            exec_v = _col(row, "execution_bps")
            delay_v = _col(row, "delay_bps")
            fill_details.append(
                {
                    "account": str(_col(row, "account", "") or ""),
                    "broker_order_id": str(_col(row, "broker_order_id") or ""),
                    "filled_qty": _to_dec(qty_v) if qty_v is not None else Decimal("1"),
                    "fill_price": _to_dec(fpx_v) if fpx_v is not None else _DEC_ZERO,
                    "execution_bps": _to_dec(exec_v) if exec_v is not None else _DEC_ZERO,
                    "delay_bps": _to_dec(delay_v) if delay_v is not None else _DEC_ZERO,
                    "is_bps": is_d,
                }
            )
        out["tca_is_bps"] = is_vals
        out["tca_total_net_cost_bps"] = net_vals
        out["tca_borrow_fee_usd"] = borrow_sum
        out["tca_borrow_bps"] = borrow_bps_vals
        out["tca_markout_60s_bps"] = mk60_vals
        out["tca_fill_details"] = fill_details
    if "paper_supervised_stops" in tables:
        sup_row = conn.execute(
            "SELECT COUNT(DISTINCT broker_order_id) FROM paper_supervised_stops "
            "WHERE protective_stop_mode = 'COLLARED_STOP_LIMIT_SUPERVISED'"
        ).fetchone()
        out["ext_protected_db"] = int(sup_row[0]) if sup_row else 0
    if "paper_daily_fuse" in tables:
        fuse_row = conn.execute(
            "SELECT tripped FROM paper_daily_fuse ORDER BY day DESC LIMIT 1"
        ).fetchone()
        out["breaker_latched_db"] = bool(fuse_row[0]) if fuse_row else False


def _read_duckdb_counts(duck_path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {
        "duckdb_orders": 0,
        "roundtrips": 0,
        "wins": 0,
        "realized_pnl_usd": 0.0,
        "tca_fills_count": 0,
        "tca_is_bps": [],
        "worker_fills": 0,
        "tca_total_net_cost_bps": [],
        "tca_borrow_fee_usd": _DEC_ZERO,
        "tca_borrow_bps": [],
        "tca_markout_60s_bps": [],
        "tca_fill_details": [],
        "ext_orders_db": 0,
        "ext_protected_db": 0,
        "breaker_latched_db": False,
    }
    if not duck_path.is_file():
        return out
    try:
        with open_bounded_duckdb(duck_path, read_only=True) as conn:
            _populate_duckdb_counts(conn, out)
        return out
    except (OSError, duckdb.Error):
        pass
    # Fall back to a temporary snapshot when an active writer holds a Windows file lock.
    tmp: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tf:
            tmp = Path(tf.name)
        shutil.copy2(duck_path, tmp)
        with open_bounded_duckdb(tmp, read_only=True) as conn:
            _populate_duckdb_counts(conn, out)
    except (OSError, duckdb.Error):
        pass
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)
    return out


_IGNITION_CATEGORIES = frozenset(
    {
        "PREMARKET_CATALYST_LEADER",
        "SUDDEN_VOLUME_IGNITION",
        "DARKPOOL_ACCUMULATION_BREAKOUT",
    }
)


def _collect_executed_tv_alert_ids(
    manifest_dir: Path, active_positions: list[dict[str, Any]]
) -> set[str]:
    """Collect lowercase alert_ids already submitted/adopted in Paper Farm guards or DuckDB."""
    executed: set[str] = set()
    for pos in active_positions:
        cid = str(pos.get("client_order_id") or "").strip().lower()
        if cid.startswith("noesis-tv-"):
            executed.add(cid.removeprefix("noesis-tv-"))
    for acct in ALPACA_LANE_PROFILES:
        db_path = manifest_dir / _duckdb_filename(acct)
        if not db_path.is_file():
            continue
        try:
            with open_bounded_duckdb(db_path, read_only=True) as conn:
                tables = {str(r[0]).lower() for r in conn.execute("SHOW TABLES").fetchall()}
                if "paper_entry_decisions" not in tables:
                    continue
                rows = conn.execute(
                    "SELECT DISTINCT client_order_id FROM paper_entry_decisions "
                    "WHERE client_order_id LIKE 'noesis-tv-%'"
                ).fetchall()
                for (cid_raw,) in rows:
                    cid = str(cid_raw or "").strip().lower()
                    if cid.startswith("noesis-tv-"):
                        executed.add(cid.removeprefix("noesis-tv-"))
        except (OSError, duckdb.Error):
            continue
    return executed


def _collect_tv_half_auto_telemetry(manifest_dir: Path) -> dict[str, Any]:
    """Read TV Half-Auto pack, compounding manifest, movers & DuckDB audit (semi_auto=False)."""
    pack_dir = manifest_dir / "tv_sota_pack"
    tickets_doc = _read_json_safe(pack_dir / "live_half_auto_tickets.json")
    compounding_doc = _read_json_safe(pack_dir / "tv_compounding_manifest.json")
    movers_doc = _read_json_safe(manifest_dir / "live_whole_market_movers_manifest.json")
    cockpit_doc = _read_json_safe(pack_dir / "finos_tv_cockpit.json")
    coord_doc = _read_json_safe(pack_dir / "tv_engine_coordination_manifest.json")
    exec_doc = _read_json_safe(manifest_dir / "paper_farm_execution_status.json")
    scan_presets_doc = _read_json_safe(pack_dir / "staged_scan_presets.json")
    manual_presets_doc = _read_json_safe(pack_dir / "staged_tv_presets.json") or _read_json_safe(
        manifest_dir / "staged_tv_presets.json"
    )
    tickets_list = tickets_doc.get("tickets")
    standby_list = tickets_doc.get("standby_presets")
    scan_list = scan_presets_doc.get("presets")
    manual_list = manual_presets_doc.get("presets")
    movers_entries = movers_doc.get("entries")

    audit_ids: set[str] = set()
    for audit_db in (
        pack_dir / "tv_half_auto_audit.duckdb",
        manifest_dir / "tv_half_auto_audit.duckdb",
    ):
        if audit_db.is_file():
            try:
                with open_bounded_duckdb(audit_db, read_only=True) as conn:
                    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
                    if "tv_staged_presets" in tables:
                        for (aid,) in conn.execute(
                            "SELECT DISTINCT alert_id FROM tv_staged_presets"
                        ).fetchall():
                            if aid is not None:
                                audit_ids.add(str(aid))
            except (OSError, duckdb.Error):
                continue
    audit_rows = len(audit_ids)

    t_items = (
        [t for t in tickets_list if isinstance(t, dict)] if isinstance(tickets_list, list) else []
    )
    s_items = (
        [s for s in standby_list if isinstance(s, dict)] if isinstance(standby_list, list) else []
    )
    m_items = (
        [m for m in movers_entries if isinstance(m, dict)]
        if isinstance(movers_entries, list)
        else []
    )
    tickets_n = len(t_items)
    standby_n = len(s_items)
    scan_n = len(scan_list) if isinstance(scan_list, list) else 0
    manual_n = len(manual_list) if isinstance(manual_list, list) else 0

    ready_n = sum(
        1 for t in t_items if str(t.get("action_gate") or "").startswith("READY_HALF_AUTO")
    )
    combined_rows = m_items or [*t_items, *s_items]
    comp_uv = int(compounding_doc.get("unusual_vol_count") or 0) + int(
        compounding_doc.get("standby_unusual_vol_count") or 0
    )
    movers_uv = sum(1 for r in combined_rows if r.get("unusual_volume_buildup") is True)
    unusual_vol_n = max(comp_uv, movers_uv)

    comp_dp = int(compounding_doc.get("darkpool_hits_count") or 0) + int(
        compounding_doc.get("standby_darkpool_hits_count") or 0
    )
    movers_dp = sum(
        1
        for r in combined_rows
        if isinstance(r.get("darkpool_trf_ratio"), (int, float))
        and float(r["darkpool_trf_ratio"]) >= 0.35
    )
    darkpool_hits_n = max(comp_dp, movers_dp)

    movers_cov = sum(
        1 for r in combined_rows if isinstance(r.get("darkpool_trf_ratio"), (int, float))
    )
    darkpool_covered_n = max(
        int(compounding_doc.get("darkpool_trf_covered_count") or 0),
        movers_cov,
    )
    macro = (
        movers_doc.get("macro_regime")
        or tickets_doc.get("macro_regime")
        or compounding_doc.get("macro_regime")
        or {}
    )
    macro_regime = int(macro.get("regime", 0)) if isinstance(macro, dict) else 0
    macro_label = (
        str(macro.get("regime_label") or "NEUTRAL") if isinstance(macro, dict) else "NEUTRAL"
    )
    feed_sources = list(movers_doc.get("feed_sources") or tickets_doc.get("feed_sources") or [])
    pine_shas = (
        dict(compounding_doc["pine_shas"])
        if isinstance(compounding_doc.get("pine_shas"), dict)
        else {}
    )
    if compounding_doc.get("pine_omni_sha256") and "tier0_omni_sota_24x5.pine" not in pine_shas:
        pine_shas["tier0_omni_sota_24x5.pine"] = str(compounding_doc["pine_omni_sha256"])
    top_symbols = (
        list(compounding_doc["top_symbols"])[:15]
        if isinstance(compounding_doc.get("top_symbols"), list) and compounding_doc["top_symbols"]
        else [
            {
                "symbol": r.get("symbol"),
                "side": r.get("side"),
                "action_gate": r.get("action_gate") or r.get("watch_tier"),
                "score": r.get("score"),
                "rvol": r.get("rvol") if r.get("rvol") is not None else r.get("rvol_normalized"),
                "darkpool_trf_ratio": r.get("darkpool_trf_ratio"),
                "hidden_volume_ratio": r.get("hidden_volume_ratio"),
                "unusual_volume_buildup": bool(r.get("unusual_volume_buildup") is True),
            }
            for r in combined_rows[:15]
        ]
    )
    cockpit_rows = (
        list(cockpit_doc["openbb_rows"])
        if isinstance(cockpit_doc.get("openbb_rows"), list)
        else top_symbols
    )
    active_pos_raw = exec_doc.get("active_positions") or coord_doc.get("active_positions")
    active_positions = (
        [p for p in active_pos_raw if isinstance(p, dict)]
        if isinstance(active_pos_raw, list)
        else []
    )
    venue_handles_raw = exec_doc.get("venue_order_handles") or coord_doc.get("venue_order_handles")
    venue_order_handles = dict(venue_handles_raw) if isinstance(venue_handles_raw, dict) else {}
    live_rpm_raw = exec_doc.get("rate_budget_live_rpm")
    rate_budget_live_rpm = dict(live_rpm_raw) if isinstance(live_rpm_raw, dict) else {}
    adopted_external_n = max(
        int(exec_doc.get("adopted_external_positions_count") or 0),
        int(coord_doc.get("adopted_external_positions_count") or 0),
        sum(1 for p in active_positions if p.get("adopted_external") is True),
    )
    synthetic_offhours_n = max(
        int(exec_doc.get("synthetic_offhours_trailed_count") or 0),
        int(coord_doc.get("synthetic_offhours_trailed_count") or 0),
        sum(
            1
            for p in active_positions
            if str(p.get("execution_mechanism") or "").startswith("SYNTHETIC_")
        ),
    )
    peak_reverse_n = max(
        int(exec_doc.get("peak_reverse_armed_count") or 0),
        int(coord_doc.get("peak_reverse_armed_count") or 0),
        sum(1 for p in active_positions if p.get("peak_reverse_armed") is True),
    )
    rapid_reentry_n = max(
        int(exec_doc.get("rapid_reentry_ready_count") or 0),
        int(coord_doc.get("rapid_reentry_ready_count") or 0),
        sum(1 for p in active_positions if p.get("rapid_reentry_ready") is True),
    )
    active_session = (
        str(
            exec_doc.get("active_session")
            or (m_items[0].get("session") if m_items else None)
            or tickets_doc.get("active_session")
            or compounding_doc.get("active_session")
            or "CLOSED"
        )
        .strip()
        .upper()
    )
    momentum_movers: list[dict[str, Any]] = []
    for row in m_items[:15]:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        cat = (
            str(row.get("calibrated_category") or row.get("potential_category") or "UNKNOWN")
            .strip()
            .upper()
        )
        sess_lbl = str(row.get("session") or active_session or "CLOSED").strip().upper()
        score_raw = row.get("score")
        rvol_raw = (
            row.get("rvol_normalized")
            if row.get("rvol_normalized") is not None
            else row.get("rvol")
        )
        trf_raw = row.get("darkpool_trf_ratio")
        momentum_movers.append(
            {
                "symbol": sym,
                "calibrated_category": cat,
                "session": sess_lbl,
                "score": float(score_raw)
                if isinstance(score_raw, (int, float)) and not isinstance(score_raw, bool)
                else 0.0,
                "rvol": float(rvol_raw)
                if isinstance(rvol_raw, (int, float)) and not isinstance(rvol_raw, bool)
                else 0.0,
                "darkpool_trf_ratio": float(trf_raw)
                if isinstance(trf_raw, (int, float)) and not isinstance(trf_raw, bool)
                else None,
                "peak_confirmed": bool(row.get("peak_confirmed") is True),
            }
        )
    momentum_ignition_n = sum(
        1 for m in momentum_movers if m["calibrated_category"] in _IGNITION_CATEGORIES
    )
    momentum_climax_n = sum(
        1
        for m in momentum_movers
        if m["calibrated_category"] == "PARABOLIC_CLIMAX_SHORT_REVERSAL" or m["peak_confirmed"]
    )
    executed_alert_ids = _collect_executed_tv_alert_ids(manifest_dir, active_positions)
    manual_dicts = (
        [p for p in manual_list if isinstance(p, dict)] if isinstance(manual_list, list) else []
    )
    staged_active_n = 0
    staged_stale_n = 0
    for preset in manual_dicts:
        aid = str(preset.get("alert_id") or "").strip().lower()
        p_vp = str(preset.get("venue_profile") or "").strip()
        p_sess = str(preset.get("session") or "").strip().upper()
        is_stale = (
            (bool(aid) and aid in executed_alert_ids)
            or not p_vp
            or (active_session != "CLOSED" and p_sess != active_session)
            or preset.get("semi_auto_confirmed") is not False
        )
        if is_stale:
            staged_stale_n += 1
        else:
            staged_active_n += 1
    return {
        "schema_version": "tier0.tv-half-auto-telemetry.v1",
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "qty": 1,
        "active_session": active_session,
        "tickets_count": tickets_n,
        "ready_tickets_count": ready_n,
        "standby_presets_count": standby_n,
        "schema_validated_count": int(tickets_doc.get("schema_validated_count") or 0),
        "staged_scan_presets_count": scan_n,
        "staged_manual_presets_count": manual_n,
        "staged_presets_total": scan_n + manual_n,
        "staged_alert_active_count": staged_active_n,
        "staged_alert_stale_count": staged_stale_n,
        "duckdb_audit_rows": audit_rows,
        "unusual_vol_count": unusual_vol_n,
        "darkpool_hits_count": darkpool_hits_n,
        "darkpool_trf_covered_count": darkpool_covered_n,
        "scanner_ranked_count": len(m_items),
        "scanner_extreme_eligible_count": sum(
            1 for m in m_items if m.get("extreme_mover_eligible") is True
        ),
        "momentum_movers": momentum_movers,
        "momentum_ignition_count": momentum_ignition_n,
        "momentum_climax_reversal_count": momentum_climax_n,
        "macro_regime": macro_regime,
        "macro_regime_label": macro_label,
        "feed_sources": feed_sources,
        "pine_omni_sha256": compounding_doc.get("pine_omni_sha256"),
        "pine_shas": pine_shas,
        "order_presets_present": (pack_dir / "tv_order_presets.json").is_file(),
        "finos_tv_cockpit_present": bool(cockpit_doc),
        "coordination_present": bool(coord_doc),
        "coordination_evaluated_count": int(coord_doc.get("staged_alerts_evaluated") or 0),
        "coordination_ready_count": int(coord_doc.get("ready_for_operator_confirm_count") or 0),
        "coordination_blocked_count": int(coord_doc.get("blocked_count") or 0),
        "active_trailed_positions": active_positions,
        "active_trailed_positions_count": len(active_positions),
        "adopted_external_positions_count": adopted_external_n,
        "synthetic_offhours_trailed_count": synthetic_offhours_n,
        "peak_reverse_armed_count": peak_reverse_n,
        "rapid_reentry_ready_count": rapid_reentry_n,
        "venue_order_handles": venue_order_handles,
        "rate_budget_live_rpm": rate_budget_live_rpm,
        "top_symbols": top_symbols,
        "tv_openbb_rows": cockpit_rows,
        "generated_at_et": tickets_doc.get("generated_at_et")
        or compounding_doc.get("generated_at_et"),
    }


class PaperPerformanceAggregator:
    """Aggregate isolated per-lane status, TCA & DuckDB telemetry into manifest + FINOS JSON."""

    def __init__(self, manifest_dir: Path | None = None, *, duckdb_glob: str | None = None) -> None:
        self.manifest_dir = manifest_dir or default_manifest_dir()
        self.duckdb_glob = duckdb_glob

    def _resolve_duckdb_path(self, account: str) -> Path:
        primary = self.manifest_dir / _duckdb_filename(account)
        if primary.is_file():
            return primary
        pattern = self.duckdb_glob or f"{account.lower()}_*.duckdb"
        matches = sorted(self.manifest_dir.glob(pattern))
        return matches[0] if matches else primary

    def _collect_lane_with_samples(self, account: str) -> tuple[dict[str, Any], dict[str, Any]]:
        slug = account.lower()
        status = _read_json_safe(self.manifest_dir / f"{slug}_live_automation_status.json")
        duck = _read_duckdb_counts(self._resolve_duckdb_path(account))
        default_lane = (
            ALPACA_LANE_PROFILES[account]["lane_id"]
            if account in ALPACA_LANE_PROFILES
            else "IBKR_SMART_24X5"
        )
        lane_id = str(status.get("lane_id") or status.get("strategy_profile") or default_lane)
        fast_sells = int(
            status.get("fast_sells")
            or (
                int(status.get("lossless_fast_sells") or 0) + int(status.get("fast_scratches") or 0)
            )
        )
        entries = int(status.get("entries") or 0)
        orders_submitted = max(int(status.get("orders_submitted") or 0), duck["duckdb_orders"])
        if "fills" in status:
            fills = max(int(status["fills"]), duck["tca_fills_count"])
        else:
            fills = duck["tca_fills_count"]

        rts = max(int(status.get("session_roundtrips") or 0), duck["roundtrips"])
        pnl_usd = float(status.get("session_realized_pnl_usd") or duck["realized_pnl_usd"])

        is_vals: list[Decimal] = duck["tca_is_bps"]
        net_vals: list[Decimal] = duck["tca_total_net_cost_bps"]
        borrow_usd: Decimal = duck["tca_borrow_fee_usd"]
        borrow_bps_vals: list[Decimal] = duck["tca_borrow_bps"]
        mk60_vals: list[Decimal] = duck["tca_markout_60s_bps"]
        fill_details: list[dict[str, Any]] = duck["tca_fill_details"]
        lane_tca_detail = _summarize_tca_fill_details(fill_details)

        ext_orders = max(int(status.get("extended_hours_orders") or 0), duck["ext_orders_db"])
        ext_protected = max(
            int(status.get("extended_hours_protected") or 0), duck["ext_protected_db"]
        )
        ext_ratio = (min(ext_protected, ext_orders) / ext_orders) if ext_orders > 0 else 1.0
        sub_vs_fill_ratio = (
            min(1.0, fills / orders_submitted)
            if orders_submitted > 0
            else (1.0 if fills > 0 else 0.0)
        )

        dd_pct = round(float(status.get("daily_drawdown_pct") or 0.0), 6)
        breaker_latched = bool(
            duck.get("breaker_latched_db")
            or status.get("daily_drawdown_breaker_latched")
            or dd_pct <= -0.015
        )
        default_vp = "IBKR_PAPER_4002" if account == IBKR_PAPER_ACCOUNT else "ALPACA_PAPER_200"
        default_rpm = 3000 if account == IBKR_PAPER_ACCOUNT else 200
        default_res = 300 if account == IBKR_PAPER_ACCOUNT else 20
        metrics = {
            "worker_fills": duck["worker_fills"],
            "account": account,
            "lane_id": lane_id,
            "venue_profile": str(status.get("venue_profile") or default_vp),
            "rate_budget_max_rpm": int(status.get("rate_budget_max_rpm") or default_rpm),
            "exit_reserve_rpm": int(status.get("exit_reserve_rpm") or default_res),
            "paper_only": True,
            "live_execution_enabled": False,
            "equity": round(float(status["equity"]), 2)
            if status.get("equity") is not None
            else None,
            "open_positions_count": int(status.get("open_positions_count") or 0),
            "drawdown_pct": dd_pct,
            "breaker_latched": breaker_latched,
            "entries": entries,
            "orders_submitted": orders_submitted,
            "fills": fills,
            "tca_fills_count": duck["tca_fills_count"],
            "unique_orders_count": lane_tca_detail["unique_orders_count"],
            "mean_is_bps": round(float(_mean_decimal(is_vals)), 6),
            "median_is_bps": round(float(_percentile_decimal(is_vals, Decimal("0.50"))), 6),
            "p90_is_bps": round(float(_percentile_decimal(is_vals, Decimal("0.90"))), 6),
            "p95_is_bps": round(float(_percentile_decimal(is_vals, Decimal("0.95"))), 6),
            "order_mean_is_bps": lane_tca_detail["order_mean_is_bps"],
            "order_median_is_bps": lane_tca_detail["order_median_is_bps"],
            "notional_weighted_is_bps": lane_tca_detail["notional_weighted_is_bps"],
            "mean_execution_bps": lane_tca_detail["mean_execution_bps"],
            "order_mean_execution_bps": lane_tca_detail["order_mean_execution_bps"],
            "mean_delay_bps": lane_tca_detail["mean_delay_bps"],
            "liquid_ge10_fills_count": lane_tca_detail["liquid_ge10_fills_count"],
            "liquid_ge10_orders_count": lane_tca_detail["liquid_ge10_orders_count"],
            "liquid_ge10_mean_is_bps": lane_tca_detail["liquid_ge10_mean_is_bps"],
            "liquid_ge10_order_mean_is_bps": lane_tca_detail["liquid_ge10_order_mean_is_bps"],
            "liquid_ge10_notional_weighted_is_bps": lane_tca_detail[
                "liquid_ge10_notional_weighted_is_bps"
            ],
            "mean_total_net_cost_bps": round(float(_mean_decimal(net_vals)), 6),
            "total_borrow_fee_usd": round(float(borrow_usd), 6),
            "mean_borrow_bps": round(float(_mean_decimal(borrow_bps_vals)), 6),
            "mean_markout_60s_bps": (
                round(float(_mean_decimal(mk60_vals)), 6) if mk60_vals else None
            ),
            "extended_hours_orders": ext_orders,
            "extended_hours_protected": ext_protected,
            "extended_hours_protected_ratio": round(ext_ratio, 6),
            "submitted_vs_filled_ratio": round(sub_vs_fill_ratio, 6),
            "auto_shorts": int(status.get("auto_shorts") or 0),
            "reverses": int(status.get("reverses") or 0),
            "reentries": int(status.get("reentries") or 0),
            "stop_replaces": int(status.get("stop_replaces") or 0),
            "fast_sells": fast_sells,
            "roundtrips": rts,
            "realized_pnl_usd": round(pnl_usd, 2),
            "duckdb_orders": duck["duckdb_orders"],
            "status_present": bool(status),
            "ready": bool(status.get("ready") is True) if account == IBKR_PAPER_ACCOUNT else None,
            "tcp_reachable": (
                bool(status.get("tcp_reachable") is True)
                if account == IBKR_PAPER_ACCOUNT and status.get("tcp_reachable") is not None
                else None
            ),
            "account_id": (
                str(status["account_id"])
                if account == IBKR_PAPER_ACCOUNT and status.get("account_id")
                else None
            ),
        }
        samples: dict[str, Any] = {
            "is_bps": is_vals,
            "net_bps": net_vals,
            "borrow_usd": borrow_usd,
            "borrow_bps": borrow_bps_vals,
            "markout_60s_bps": mk60_vals,
            "fill_details": fill_details,
        }
        return metrics, samples

    def collect_lane_metrics(self, account: str) -> dict[str, Any]:
        metrics, _ = self._collect_lane_with_samples(account)
        return metrics

    def aggregate(
        self,
        *,
        accounts: tuple[str, ...] = ALL_FARM_ACCOUNTS,
        output_path: Path | None = None,
        finos_output_path: Path | None = None,
        prom_output_path: Path | None = None,
        otlp_endpoint: str | None = None,
    ) -> dict[str, Any]:
        lanes: dict[str, dict[str, Any]] = {}
        all_is: list[Decimal] = []
        all_net: list[Decimal] = []
        all_borrow_bps: list[Decimal] = []
        all_mk60: list[Decimal] = []
        all_fill_details: list[dict[str, Any]] = []
        total_borrow_usd = _DEC_ZERO

        for acct in accounts:
            metrics, samples = self._collect_lane_with_samples(acct)
            lanes[acct] = metrics
            is_s = samples["is_bps"]
            net_s = samples["net_bps"]
            bb_s = samples["borrow_bps"]
            mk_s = samples["markout_60s_bps"]
            fd_s = samples["fill_details"]
            b_u = samples["borrow_usd"]
            if isinstance(is_s, list):
                all_is.extend(is_s)
            if isinstance(net_s, list):
                all_net.extend(net_s)
            if isinstance(bb_s, list):
                all_borrow_bps.extend(bb_s)
            if isinstance(mk_s, list):
                all_mk60.extend(mk_s)
            if isinstance(fd_s, list):
                all_fill_details.extend(fd_s)
            if isinstance(b_u, Decimal):
                total_borrow_usd += b_u

        df = pl.DataFrame(list(lanes.values()))
        max_dd = max((float(m["drawdown_pct"]) for m in lanes.values()), default=0.0)
        total_submitted = int(df["orders_submitted"].sum())
        total_fills = int(df["fills"].sum())
        total_ext_orders = int(df["extended_hours_orders"].sum())
        total_ext_protected = int(df["extended_hours_protected"].sum())
        ext_protected_ratio = (
            min(total_ext_protected, total_ext_orders) / total_ext_orders
            if total_ext_orders > 0
            else 1.0
        )
        submitted_vs_filled_ratio = (
            min(1.0, total_fills / total_submitted)
            if total_submitted > 0
            else (1.0 if total_fills > 0 else 0.0)
        )

        combined_tca_detail = _summarize_tca_fill_details(all_fill_details)
        tca_summary = {
            "tca_fills_count": len(all_is),
            "unique_orders_count": combined_tca_detail["unique_orders_count"],
            "orders_submitted": total_submitted,
            "fills_confirmed": total_fills,
            "mean_is_bps": round(float(_mean_decimal(all_is)), 6),
            "median_is_bps": round(float(_percentile_decimal(all_is, Decimal("0.50"))), 6),
            "p90_is_bps": round(float(_percentile_decimal(all_is, Decimal("0.90"))), 6),
            "p95_is_bps": round(float(_percentile_decimal(all_is, Decimal("0.95"))), 6),
            "order_mean_is_bps": combined_tca_detail["order_mean_is_bps"],
            "order_median_is_bps": combined_tca_detail["order_median_is_bps"],
            "notional_weighted_is_bps": combined_tca_detail["notional_weighted_is_bps"],
            "mean_execution_bps": combined_tca_detail["mean_execution_bps"],
            "order_mean_execution_bps": combined_tca_detail["order_mean_execution_bps"],
            "mean_delay_bps": combined_tca_detail["mean_delay_bps"],
            "liquid_ge10_fills_count": combined_tca_detail["liquid_ge10_fills_count"],
            "liquid_ge10_orders_count": combined_tca_detail["liquid_ge10_orders_count"],
            "liquid_ge10_mean_is_bps": combined_tca_detail["liquid_ge10_mean_is_bps"],
            "liquid_ge10_order_mean_is_bps": combined_tca_detail["liquid_ge10_order_mean_is_bps"],
            "liquid_ge10_notional_weighted_is_bps": combined_tca_detail[
                "liquid_ge10_notional_weighted_is_bps"
            ],
            "mean_total_net_cost_bps": round(float(_mean_decimal(all_net)), 6),
            "total_borrow_fee_usd": round(float(total_borrow_usd), 6),
            "mean_borrow_bps": round(float(_mean_decimal(all_borrow_bps)), 6),
            "mean_markout_60s_bps": (
                round(float(_mean_decimal(all_mk60)), 6) if all_mk60 else None
            ),
            "extended_hours_orders": total_ext_orders,
            "extended_hours_protected": total_ext_protected,
            "extended_hours_protected_ratio": round(ext_protected_ratio, 6),
            "submitted_vs_filled_ratio": round(submitted_vs_filled_ratio, 6),
        }

        combined = {
            "combined_equity_usd": round(
                sum(m["equity"] for m in lanes.values() if m["equity"] is not None), 2
            ),
            "accounts_with_equity": sum(m["equity"] is not None for m in lanes.values()),
            "max_drawdown_pct": round(max_dd, 6),
            "any_breaker_latched": any(bool(m.get("breaker_latched")) for m in lanes.values()),
            "entries": int(df["entries"].sum()),
            "orders_submitted": total_submitted,
            "fills": total_fills,
            "auto_shorts": int(df["auto_shorts"].sum()),
            "reverses": int(df["reverses"].sum()),
            "reentries": int(df["reentries"].sum()),
            "stop_replaces": int(df["stop_replaces"].sum()),
            "fast_sells": int(df["fast_sells"].sum()),
            "roundtrips": int(df["roundtrips"].sum()),
            "realized_pnl_usd": round(float(df["realized_pnl_usd"].sum()), 2),
            "tca_fills_count": tca_summary["tca_fills_count"],
            "unique_orders_count": tca_summary["unique_orders_count"],
            "mean_is_bps": tca_summary["mean_is_bps"],
            "median_is_bps": tca_summary["median_is_bps"],
            "p90_is_bps": tca_summary["p90_is_bps"],
            "p95_is_bps": tca_summary["p95_is_bps"],
            "order_mean_is_bps": tca_summary["order_mean_is_bps"],
            "order_median_is_bps": tca_summary["order_median_is_bps"],
            "notional_weighted_is_bps": tca_summary["notional_weighted_is_bps"],
            "mean_execution_bps": tca_summary["mean_execution_bps"],
            "order_mean_execution_bps": tca_summary["order_mean_execution_bps"],
            "mean_delay_bps": tca_summary["mean_delay_bps"],
            "liquid_ge10_fills_count": tca_summary["liquid_ge10_fills_count"],
            "liquid_ge10_orders_count": tca_summary["liquid_ge10_orders_count"],
            "liquid_ge10_mean_is_bps": tca_summary["liquid_ge10_mean_is_bps"],
            "liquid_ge10_order_mean_is_bps": tca_summary["liquid_ge10_order_mean_is_bps"],
            "liquid_ge10_notional_weighted_is_bps": tca_summary[
                "liquid_ge10_notional_weighted_is_bps"
            ],
            "mean_total_net_cost_bps": tca_summary["mean_total_net_cost_bps"],
            "total_borrow_fee_usd": tca_summary["total_borrow_fee_usd"],
            "extended_hours_protected_ratio": tca_summary["extended_hours_protected_ratio"],
            "submitted_vs_filled_ratio": tca_summary["submitted_vs_filled_ratio"],
        }
        now_iso = datetime.now(UTC).isoformat()
        tv_half_auto = _collect_tv_half_auto_telemetry(self.manifest_dir)
        worker = _read_json_safe(self.manifest_dir / "paper_farm_execution_status.json")
        heartbeat = worker.get("generated_at_utc", "")
        try:
            age = (datetime.now(UTC) - datetime.fromisoformat(heartbeat)).total_seconds()
        except (ValueError, TypeError):
            age = float("inf")
        supervising = (
            0 <= age < 90
            and worker.get("status") in {"SUPERVISING", "ORDER_ACKNOWLEDGED"}
            and worker.get("entry_fault") is False
        )
        active_trailed = list(tv_half_auto.get("active_trailed_positions") or [])
        active_by_acct: dict[str, list[str]] = {}
        for pos in active_trailed:
            acct_key = str(pos.get("account") or "").strip().upper()
            sym_val = str(pos.get("symbol") or "").strip().upper()
            if acct_key and sym_val:
                active_by_acct.setdefault(acct_key, []).append(sym_val)
        for account, metrics in lanes.items():
            lane_syms = sorted(set(active_by_acct.get(account, ())))
            metrics["open_positions"] = lane_syms
            if supervising and lane_syms:
                metrics["open_positions_count"] = max(
                    int(metrics.get("open_positions_count") or 0), len(lane_syms)
                )
            metrics["worker_verified"] = (
                account in ALPACA_LANE_PROFILES and supervising and metrics["worker_fills"] > 0
            )
        ibkr = _read_json_safe(self.manifest_dir / "ibkr_du_4002_live_automation_status.json")
        ibkr_tcp_ok = ibkr.get("tcp_reachable") is True
        ibkr_ready = ibkr.get("ready") is True
        deployed_blockers: list[str] = []
        if not ibkr_tcp_ok:
            deployed_blockers.append("IBKR_DU_4002:tcp_unreachable")
        elif not ibkr_ready:
            deployed_blockers.append("IBKR_DU_4002:du_handshake_unverified")
        if not supervising:
            deployed_blockers.append(f"SUPERVISOR:{worker.get('status') or 'MISSING'}")
        for account in ALPACA_LANE_PROFILES:
            if lanes.get(account, {}).get("worker_verified") is not True:
                wf = int(lanes.get(account, {}).get("worker_fills") or 0)
                deployed_blockers.append(f"{account}:worker_fills={wf}")
        deployed = (
            ibkr_tcp_ok
            and ibkr_ready
            and all(
                lanes.get(account, {}).get("worker_verified") is True
                for account in ALPACA_LANE_PROFILES
            )
        )
        venue_handles = tv_half_auto.get("venue_order_handles")
        vh_dict = venue_handles if isinstance(venue_handles, dict) else {}
        alpaca_vp = str(
            vh_dict.get("alpaca_venue_profile")
            or next(
                (
                    lanes[a]["venue_profile"]
                    for a in ALPACA_LANE_PROFILES
                    if a in lanes and lanes[a].get("venue_profile")
                ),
                "ALPACA_PAPER_200",
            )
        )
        combined_max_rpm = int(
            vh_dict.get("combined_max_rpm")
            or sum(int(m.get("rate_budget_max_rpm") or 0) for m in lanes.values())
        )
        combined["alpaca_venue_profile"] = alpaca_vp
        combined["combined_max_rpm"] = combined_max_rpm
        live_rpm_doc = tv_half_auto.get("rate_budget_live_rpm")
        live_rpm_map = dict(live_rpm_doc) if isinstance(live_rpm_doc, dict) else {}
        per_acct_rpm = live_rpm_map.get("per_account")
        if isinstance(per_acct_rpm, dict):
            for acct_k, rpm_row in per_acct_rpm.items():
                if acct_k in lanes and isinstance(rpm_row, dict):
                    lanes[acct_k]["live_used_rpm_60s"] = int(rpm_row.get("used_rpm_60s") or 0)
                    lanes[acct_k]["live_rpm_utilization_pct"] = float(
                        rpm_row.get("utilization_pct") or 0.0
                    )
        combined["alpaca_used_rpm_60s"] = int(live_rpm_map.get("alpaca_used_rpm_60s") or 0)
        combined["alpaca_rpm_utilization_pct"] = float(
            live_rpm_map.get("alpaca_utilization_pct") or 0.0
        )
        combined["active_trailed_positions_count"] = len(active_trailed)
        combined["open_positions_count"] = sum(
            int(m.get("open_positions_count") or 0) for m in lanes.values()
        )
        combined["ibkr_gateway_ready"] = ibkr_ready
        combined["ibkr_account_id"] = ibkr.get("account_id") if ibkr_ready else None

        finos_doc = build_finos_openbb_export(
            lanes,
            combined,
            generated_at_utc=now_iso,
            tca_summary=tca_summary,
            tv_half_auto=tv_half_auto,
        )
        manifest: dict[str, Any] = {
            "schema_version": "tier0.paper-farm-live-performance.v1",
            "generated_at_utc": now_iso,
            "paper_only": True,
            "live_execution_enabled": False,
            "paper_live_deployed": deployed,
            "paper_live_deployed_blockers": deployed_blockers,
            "execution_status": worker.get("status"),
            "alpaca_venue_profile": alpaca_vp,
            "combined_max_rpm": combined_max_rpm,
            "rate_budget_live_rpm": live_rpm_map,
            "active_trailed_positions": active_trailed,
            "summary": combined,
            "tca_summary": tca_summary,
            "tv_half_auto": tv_half_auto,
            "per_lane": lanes,
            "finos_openbb": finos_doc,
        }
        perf_target = output_path or (
            self.manifest_dir / "paper_farm_live_performance_manifest.json"
        )
        finos_target = finos_output_path or (
            self.manifest_dir / "finos_paper_farm_performance.json"
        )
        _atomic_write_json(perf_target, manifest)
        _atomic_write_json(finos_target, finos_doc)
        if prom_output_path is not None or otlp_endpoint is not None:
            record_paper_farm_telemetry(
                manifest, prom_output_path=prom_output_path, otlp_endpoint=otlp_endpoint
            )
        return manifest


def build_finos_openbb_export(
    per_lane: dict[str, dict[str, Any]],
    summary: dict[str, Any],
    *,
    generated_at_utc: str | None = None,
    tca_summary: dict[str, Any] | None = None,
    tv_half_auto: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build declarative FINOS Workspace Plotly + OpenBB tabular JSON export."""
    accts = list(per_lane.keys())
    return {
        "schema_version": "tier0.finos-openbb-paper-farm.v1",
        "generated_at_utc": generated_at_utc or datetime.now(UTC).isoformat(),
        "plugin": "@finos-workspace/plotly-charts@2.5.0",
        "paper_only": True,
        "live_execution_enabled": False,
        "data": [
            {
                "type": "bar",
                "name": "Paper Farm Equity (USD)",
                "x": accts,
                "y": [per_lane[a]["equity"] for a in accts],
            },
            {
                "type": "bar",
                "name": "Realized PnL (USD)",
                "x": accts,
                "y": [per_lane[a]["realized_pnl_usd"] for a in accts],
            },
        ],
        "layout": {
            "title": "Tier-0 Paper Farm Live Performance",
            "xaxis": {"title": "Paper Lane Account"},
            "yaxis": {"title": "USD"},
        },
        "openbb_rows": [per_lane[a] for a in accts],
        "tv_openbb_rows": list((tv_half_auto or {}).get("tv_openbb_rows") or []),
        "active_positions": list((tv_half_auto or {}).get("active_trailed_positions") or []),
        "venue_order_handles": dict((tv_half_auto or {}).get("venue_order_handles") or {}),
        "summary": summary,
        "tca_summary": tca_summary or {},
        "tv_half_auto": tv_half_auto or {},
    }


def export_prometheus_metrics(
    manifest: dict[str, Any],
    *,
    registry: CollectorRegistry | None = None,
    output_path: Path | None = None,
) -> tuple[CollectorRegistry, str]:
    """Build official prometheus_client text exposition without starting any HTTP server."""
    reg = registry if registry is not None else CollectorRegistry()
    labels = ("account", "lane_id")

    g_eq = Gauge(
        "tier0_paper_farm_equity_usd", "Per-lane paper equity in USD", labels, registry=reg
    )
    g_dd = Gauge(
        "tier0_paper_farm_drawdown_pct", "Per-lane daily drawdown fraction", labels, registry=reg
    )
    g_breaker = Gauge(
        "tier0_drawdown_breaker_latched",
        "Per-lane -1.50% session-latched daily drawdown breaker status (1=tripped, 0=clear)",
        labels,
        registry=reg,
    )
    g_is_mean = Gauge(
        "tier0_paper_farm_tca_mean_is_bps",
        "Per-lane mean implementation shortfall in bps",
        labels,
        registry=reg,
    )
    g_is_p95 = Gauge(
        "tier0_paper_farm_tca_p95_is_bps",
        "Per-lane p95 implementation shortfall in bps",
        labels,
        registry=reg,
    )
    g_borrow = Gauge(
        "tier0_paper_farm_tca_borrow_fee_usd",
        "Per-lane cumulative short borrow fee in USD",
        labels,
        registry=reg,
    )
    g_ext = Gauge(
        "tier0_paper_farm_extended_hours_protected_ratio",
        "Per-lane ratio of extended-hours orders with supervised collared stops",
        labels,
        registry=reg,
    )
    g_fill_ratio = Gauge(
        "tier0_paper_farm_submitted_vs_filled_ratio",
        "Per-lane ratio of confirmed fills to submitted orders",
        labels,
        registry=reg,
    )
    c_sub = Counter(
        "tier0_paper_farm_orders_submitted_total",
        "Per-lane submitted paper orders",
        labels,
        registry=reg,
    )
    c_fills = Counter(
        "tier0_paper_farm_fills_confirmed_total",
        "Per-lane broker-confirmed paper fills",
        labels,
        registry=reg,
    )

    per_lane = manifest.get("per_lane", {})
    for acct, row in per_lane.items():
        lane_id = str(row.get("lane_id", acct))
        if row.get("equity") is not None:
            g_eq.labels(acct, lane_id).set(float(row["equity"]))
        g_dd.labels(acct, lane_id).set(float(row.get("drawdown_pct") or 0.0))
        g_breaker.labels(acct, lane_id).set(1.0 if row.get("breaker_latched") else 0.0)
        g_is_mean.labels(acct, lane_id).set(float(row.get("mean_is_bps") or 0.0))
        g_is_p95.labels(acct, lane_id).set(float(row.get("p95_is_bps") or 0.0))
        g_borrow.labels(acct, lane_id).set(float(row.get("total_borrow_fee_usd") or 0.0))
        g_ext.labels(acct, lane_id).set(float(row.get("extended_hours_protected_ratio", 1.0)))
        g_fill_ratio.labels(acct, lane_id).set(float(row.get("submitted_vs_filled_ratio") or 0.0))
        sub_n = int(row.get("orders_submitted") or 0)
        fill_n = int(row.get("fills") or 0)
        if sub_n > 0:
            c_sub.labels(acct, lane_id).inc(sub_n)
        if fill_n > 0:
            c_fills.labels(acct, lane_id).inc(fill_n)

    summary = manifest.get("summary", {})
    Gauge(
        "tier0_paper_farm_combined_equity_usd",
        "Combined Paper Farm equity in USD",
        registry=reg,
    ).set(float(summary.get("combined_equity_usd") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_mean_is_bps",
        "Combined Paper Farm mean implementation shortfall in bps",
        registry=reg,
    ).set(float(summary.get("mean_is_bps") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_order_mean_is_bps",
        "Combined Paper Farm order-level qty-weighted mean implementation shortfall in bps",
        registry=reg,
    ).set(float(summary.get("order_mean_is_bps") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_notional_weighted_is_bps",
        "Combined Paper Farm notional-weighted implementation shortfall in bps",
        registry=reg,
    ).set(float(summary.get("notional_weighted_is_bps") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_liquid_ge10_order_mean_is_bps",
        "Combined Paper Farm liquid >=$10 order-level mean implementation shortfall in bps",
        registry=reg,
    ).set(float(summary.get("liquid_ge10_order_mean_is_bps") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_liquid_ge10_notional_weighted_is_bps",
        "Combined Paper Farm liquid >=$10 notional-weighted implementation shortfall in bps",
        registry=reg,
    ).set(float(summary.get("liquid_ge10_notional_weighted_is_bps") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_p95_is_bps",
        "Combined Paper Farm p95 implementation shortfall in bps",
        registry=reg,
    ).set(float(summary.get("p95_is_bps") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_tca_borrow_fee_usd",
        "Combined Paper Farm total borrow fee in USD",
        registry=reg,
    ).set(float(summary.get("total_borrow_fee_usd") or 0.0))
    Gauge(
        "tier0_paper_farm_combined_extended_hours_protected_ratio",
        "Combined Paper Farm extended-hours protected order ratio",
        registry=reg,
    ).set(float(summary.get("extended_hours_protected_ratio", 1.0)))
    Gauge(
        "tier0_paper_farm_combined_submitted_vs_filled_ratio",
        "Combined Paper Farm confirmed fills vs submitted orders ratio",
        registry=reg,
    ).set(float(summary.get("submitted_vs_filled_ratio") or 0.0))

    tv_doc = manifest.get("tv_half_auto", {})
    Gauge(
        "tier0_tv_half_auto_tickets_count",
        "TradingView Half-Auto extreme-mover tickets generated (qty=1, semi_auto_confirmed=false)",
        registry=reg,
    ).set(float(tv_doc.get("tickets_count") or 0))
    Gauge(
        "tier0_tv_half_auto_ready_tickets_count",
        "TradingView Half-Auto READY_HALF_AUTO actionable tickets count",
        registry=reg,
    ).set(float(tv_doc.get("ready_tickets_count") or 0))
    Gauge(
        "tier0_tv_half_auto_standby_presets_count",
        "TradingView Half-Auto MONITOR_ONLY standby ranked presets count",
        registry=reg,
    ).set(float(tv_doc.get("standby_presets_count") or 0))
    Gauge(
        "tier0_tv_half_auto_staged_presets_count",
        "TradingView Half-Auto staged semi-auto presets total",
        registry=reg,
    ).set(float(tv_doc.get("staged_presets_total") or 0))
    Gauge(
        "tier0_tv_half_auto_schema_validated_count",
        "TradingView Half-Auto schema-validated alert tickets count",
        registry=reg,
    ).set(float(tv_doc.get("schema_validated_count") or 0))
    Gauge(
        "tier0_tv_half_auto_duckdb_audit_rows",
        "TradingView Half-Auto DuckDB persisted audit preset count",
        registry=reg,
    ).set(float(tv_doc.get("duckdb_audit_rows") or 0))
    Gauge(
        "tier0_tv_half_auto_unusual_vol_count",
        "TradingView Half-Auto unusual volume buildup symbols count",
        registry=reg,
    ).set(float(tv_doc.get("unusual_vol_count") or 0))
    Gauge(
        "tier0_tv_half_auto_darkpool_hits_count",
        "TradingView Half-Auto FINRA TRF darkpool hits (>=35% TRF ratio)",
        registry=reg,
    ).set(float(tv_doc.get("darkpool_hits_count") or 0))
    Gauge(
        "tier0_tv_half_auto_darkpool_trf_covered_count",
        "Symbols with non-null FINRA TRF darkpool telemetry coverage",
        registry=reg,
    ).set(float(tv_doc.get("darkpool_trf_covered_count") or 0))
    Gauge(
        "tier0_scanner_ranked_count",
        "Whole-market live extreme-mover scanner ranked entries count",
        registry=reg,
    ).set(float(tv_doc.get("scanner_ranked_count") or 0))
    Gauge(
        "tier0_scanner_extreme_eligible_count",
        "Whole-market scanner entries passing strict extreme_mover_eligible gate",
        registry=reg,
    ).set(float(tv_doc.get("scanner_extreme_eligible_count") or 0))
    Gauge(
        "tier0_scanner_macro_regime",
        "Causal SPY/QQQ/SOXX/RSP market regime (-1 crash, 0 neutral, +1 bull)",
        registry=reg,
    ).set(float(tv_doc.get("macro_regime") or 0))
    Gauge(
        "tier0_tv_engine_coordination_evaluated_count",
        "Staged TradingView presets evaluated against Paper Farm & Nautilus SANDBOX parity",
        registry=reg,
    ).set(float(tv_doc.get("coordination_evaluated_count") or 0))
    Gauge(
        "tier0_tv_engine_coordination_ready_count",
        "Staged TradingView presets ready for manual operator confirmation (semi_auto=false)",
        registry=reg,
    ).set(float(tv_doc.get("coordination_ready_count") or 0))
    Gauge(
        "tier0_paper_farm_active_trailed_positions",
        "Active trailed positions managed across Paper Farm guards",
        registry=reg,
    ).set(float(tv_doc.get("active_trailed_positions_count") or 0))
    Gauge(
        "tier0_paper_farm_adopted_external_positions_count",
        "External manual/TV half-auto positions auto-adopted into hosted lossless trailing",
        registry=reg,
    ).set(float(tv_doc.get("adopted_external_positions_count") or 0))
    Gauge(
        "tier0_paper_farm_synthetic_offhours_trailed_count",
        "Active positions supervised via synthetic collared extended/overnight lossless trailing",
        registry=reg,
    ).set(float(tv_doc.get("synthetic_offhours_trailed_count") or 0))
    Gauge(
        "tier0_paper_farm_peak_reverse_armed_count",
        "Active trailed positions armed for peak-hit climax flatten-first auto-reverse",
        registry=reg,
    ).set(float(tv_doc.get("peak_reverse_armed_count") or 0))
    Gauge(
        "tier0_paper_farm_rapid_reentry_ready_count",
        "Active trailed guards ready for 1000rpm rapid re-entry (exit reserve protected)",
        registry=reg,
    ).set(float(tv_doc.get("rapid_reentry_ready_count") or 0))

    v_handles = tv_doc.get("venue_order_handles")
    venues_map: dict[str, dict[str, Any]] = (
        {str(k): v for k, v in v_handles["venues"].items() if isinstance(v, dict)}
        if isinstance(v_handles, dict) and isinstance(v_handles.get("venues"), dict)
        else {}
    )
    if not venues_map:
        active_sess = str(tv_doc.get("active_session") or "RTH")
        for vp in ("IBKR_PAPER_4002", "ALPACA_PAPER_200", "ALPACA_PAPER_1000"):
            venues_map[vp] = eng.build_venue_order_handle_spec(vp, session=active_sess)
    g_vh_max = Gauge(
        "tier0_venue_order_handle_max_rpm",
        "Configured venue order-handle maximum requests per minute",
        ("venue_profile",),
        registry=reg,
    )
    g_vh_res = Gauge(
        "tier0_venue_order_handle_exit_reserve_rpm",
        "Configured venue order-handle >=10% protective exit reserve RPM",
        ("venue_profile",),
        registry=reg,
    )
    for vp, spec in venues_map.items():
        g_vh_max.labels(vp).set(float(spec.get("max_rpm") or 0))
        g_vh_res.labels(vp).set(float(spec.get("exit_reserve_rpm") or 0))

    live_rpm_src = manifest.get("rate_budget_live_rpm") or tv_doc.get("rate_budget_live_rpm") or {}
    raw_per_acct = live_rpm_src.get("per_account") if isinstance(live_rpm_src, dict) else None
    live_rpm_per_acct: dict[str, Any] = raw_per_acct if isinstance(raw_per_acct, dict) else {}
    g_live_used_rpm = Gauge(
        "tier0_paper_farm_live_used_rpm_60s",
        "Real-time sliding-60s broker order/modify requests per minute by paper account",
        ("account",),
        registry=reg,
    )
    g_live_util_pct = Gauge(
        "tier0_paper_farm_live_rpm_utilization_pct",
        "Real-time sliding-60s RPM utilization percentage against configured max_rpm by account",
        ("account",),
        registry=reg,
    )
    rpm_accounts = list(
        dict.fromkeys([*live_rpm_per_acct.keys(), *manifest.get("per_lane", {}).keys()])
    )
    for acct_k in rpm_accounts:
        acct_lbl = str(acct_k).strip().upper()
        if not acct_lbl:
            continue
        rpm_entry = live_rpm_per_acct.get(acct_k)
        lane_entry = manifest.get("per_lane", {}).get(acct_k)
        if isinstance(rpm_entry, dict):
            used_v = float(rpm_entry.get("used_rpm_60s") or 0.0)
            util_v = float(rpm_entry.get("utilization_pct") or 0.0)
        elif isinstance(lane_entry, dict):
            used_v = float(lane_entry.get("live_used_rpm_60s") or 0.0)
            util_v = float(lane_entry.get("live_rpm_utilization_pct") or 0.0)
        else:
            used_v, util_v = 0.0, 0.0
        g_live_used_rpm.labels(acct_lbl).set(used_v)
        g_live_util_pct.labels(acct_lbl).set(util_v)

    g_mom_score = Gauge(
        "tier0_momentum_top_score",
        "Top ranked extreme-mover momentum score by symbol, category, and session",
        ("symbol", "calibrated_category", "session"),
        registry=reg,
    )
    g_mom_rvol = Gauge(
        "tier0_momentum_rvol",
        "Same-session normalized relative volume (RVOL) for ranked momentum symbols",
        ("symbol", "session"),
        registry=reg,
    )
    g_mom_trf = Gauge(
        "tier0_momentum_darkpool_trf_ratio",
        "FINRA TRF darkpool volume ratio for ranked momentum symbols",
        ("symbol",),
        registry=reg,
    )
    for mover in tv_doc.get("momentum_movers") or ():
        if not isinstance(mover, dict) or not mover.get("symbol"):
            continue
        sym_l = str(mover["symbol"]).strip().upper()
        cat_l = str(mover.get("calibrated_category") or "UNKNOWN").strip().upper()
        sess_l = str(mover.get("session") or "CLOSED").strip().upper()
        g_mom_score.labels(sym_l, cat_l, sess_l).set(float(mover.get("score") or 0.0))
        g_mom_rvol.labels(sym_l, sess_l).set(float(mover.get("rvol") or 0.0))
        if isinstance(mover.get("darkpool_trf_ratio"), (int, float)):
            g_mom_trf.labels(sym_l).set(float(mover["darkpool_trf_ratio"]))
    Gauge(
        "tier0_momentum_ignition_count",
        "Count of live PREMARKET_CATALYST_LEADER / SUDDEN_VOLUME_IGNITION / DARKPOOL movers",
        registry=reg,
    ).set(float(tv_doc.get("momentum_ignition_count") or 0))
    Gauge(
        "tier0_momentum_climax_reversal_count",
        "Count of live PARABOLIC_CLIMAX_SHORT_REVERSAL / peak_confirmed movers",
        registry=reg,
    ).set(float(tv_doc.get("momentum_climax_reversal_count") or 0))

    g_pos_tier = Gauge(
        "tier0_position_profit_lock_tier",
        "Active 24/5 lossless trailing profit-lock tier (0=initial, 1=BE+fee, 2=+1R, 3=+2R, 4=+3R)",
        ("symbol", "account", "session", "order_mode"),
        registry=reg,
    )
    g_pos_fast = Gauge(
        "tier0_position_fast_sell_armed",
        "Whether non-noise fast reversal exit is armed on an active position (1=armed, 0=unarmed)",
        ("symbol", "account"),
        registry=reg,
    )
    for pos in tv_doc.get("active_trailed_positions") or ():
        if not isinstance(pos, dict) or not pos.get("symbol"):
            continue
        p_sym = str(pos["symbol"]).strip().upper()
        p_acct = str(pos.get("account") or "UNKNOWN").strip().upper()
        p_sess = (
            str(pos.get("active_session") or pos.get("entered_session") or "CLOSED").strip().upper()
        )
        p_mode = str(pos.get("order_mode") or "STOP_LIMIT").strip().upper()
        g_pos_tier.labels(p_sym, p_acct, p_sess, p_mode).set(float(pos.get("lock_tier") or 0))
        g_pos_fast.labels(p_sym, p_acct).set(1.0 if pos.get("fast_sell_armed") is True else 0.0)

    Gauge(
        "tier0_staged_alert_active_count",
        "Active current-session venue-stamped unexecuted staged TV presets",
        registry=reg,
    ).set(float(tv_doc.get("staged_alert_active_count") or 0))
    Gauge(
        "tier0_staged_alert_stale_count",
        "Stale cross-session, legacy, or already-executed staged TV presets requiring cleanup",
        registry=reg,
    ).set(float(tv_doc.get("staged_alert_stale_count") or 0))

    prom_text = generate_latest(reg).decode("utf-8")
    if output_path is not None:
        _atomic_write_text(output_path, prom_text)
    return reg, prom_text


def record_otel_metrics(
    manifest: dict[str, Any],
    *,
    otlp_endpoint: str | None = None,
    meter_provider: MeterProvider | None = None,
) -> dict[str, Any]:
    """Record Paper Farm & TCA metrics using official opentelemetry-sdk (offline-safe)."""
    in_memory_reader = InMemoryMetricReader()
    readers: list[MetricReader] = [in_memory_reader]
    otlp_exported = False
    if otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

        readers.append(PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otlp_endpoint)))
        otlp_exported = True

    owned_provider = meter_provider is None
    provider = (
        meter_provider
        if meter_provider is not None
        else MeterProvider(
            resource=Resource.create(
                {
                    "service.name": "tier0-paper-farm",
                    "service.instance.id": "noesis-paper-farm",
                    "tier0.paper_only": True,
                }
            ),
            metric_readers=readers,
        )
    )
    try:
        meter = provider.get_meter("tier0_sota.live.performance", version="1.0.0")
        g_eq = meter.create_gauge("tier0.paper_farm.equity_usd", unit="USD")
        g_dd = meter.create_gauge("tier0.paper_farm.drawdown_pct", unit="")
        g_breaker = meter.create_gauge("tier0.drawdown_breaker.latched", unit="")
        g_is_mean = meter.create_gauge("tier0.paper_farm.tca.mean_is_bps", unit="bps")
        g_is_p95 = meter.create_gauge("tier0.paper_farm.tca.p95_is_bps", unit="bps")
        g_borrow = meter.create_gauge("tier0.paper_farm.tca.borrow_fee_usd", unit="USD")
        g_ext = meter.create_gauge("tier0.paper_farm.extended_hours.protected_ratio", unit="1")
        g_fill_ratio = meter.create_gauge("tier0.paper_farm.submitted_vs_filled_ratio", unit="1")
        c_sub = meter.create_counter("tier0.paper_farm.orders_submitted", unit="1")
        c_fills = meter.create_counter("tier0.paper_farm.fills_confirmed", unit="1")

        for acct, row in manifest.get("per_lane", {}).items():
            attrs = {"account": str(acct), "lane_id": str(row.get("lane_id", acct))}
            if row.get("equity") is not None:
                g_eq.set(float(row["equity"]), attrs)
            g_dd.set(float(row.get("drawdown_pct") or 0.0), attrs)
            g_breaker.set(1.0 if row.get("breaker_latched") else 0.0, attrs)
            g_is_mean.set(float(row.get("mean_is_bps") or 0.0), attrs)
            g_is_p95.set(float(row.get("p95_is_bps") or 0.0), attrs)
            g_borrow.set(float(row.get("total_borrow_fee_usd") or 0.0), attrs)
            g_ext.set(float(row.get("extended_hours_protected_ratio", 1.0)), attrs)
            g_fill_ratio.set(float(row.get("submitted_vs_filled_ratio") or 0.0), attrs)
            sub_n = int(row.get("orders_submitted") or 0)
            fill_n = int(row.get("fills") or 0)
            c_sub.add(sub_n, attrs)
            c_fills.add(fill_n, attrs)

        summary = manifest.get("summary", {})
        sum_attrs = {"scope": "combined"}
        g_eq.set(float(summary.get("combined_equity_usd") or 0.0), sum_attrs)
        g_dd.set(float(summary.get("max_drawdown_pct") or 0.0), sum_attrs)
        g_breaker.set(1.0 if summary.get("any_breaker_latched") else 0.0, sum_attrs)
        g_is_mean.set(float(summary.get("mean_is_bps") or 0.0), sum_attrs)
        g_is_p95.set(float(summary.get("p95_is_bps") or 0.0), sum_attrs)
        g_borrow.set(float(summary.get("total_borrow_fee_usd") or 0.0), sum_attrs)
        g_ext.set(float(summary.get("extended_hours_protected_ratio", 1.0)), sum_attrs)
        g_fill_ratio.set(float(summary.get("submitted_vs_filled_ratio") or 0.0), sum_attrs)

        # Unitless combined parity gauges (exact Prometheus names without OTel _USD/_ratio suffix).
        meter.create_gauge("tier0.paper_farm.combined_equity_usd", unit="").set(
            float(summary.get("combined_equity_usd") or 0.0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.combined_tca_mean_is_bps", unit="").set(
            float(summary.get("mean_is_bps") or 0.0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.combined_tca_order_mean_is_bps", unit="").set(
            float(summary.get("order_mean_is_bps") or 0.0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.combined_tca_notional_weighted_is_bps", unit="").set(
            float(summary.get("notional_weighted_is_bps") or 0.0), sum_attrs
        )
        meter.create_gauge(
            "tier0.paper_farm.combined_tca_liquid_ge10_order_mean_is_bps", unit=""
        ).set(float(summary.get("liquid_ge10_order_mean_is_bps") or 0.0), sum_attrs)
        meter.create_gauge(
            "tier0.paper_farm.combined_tca_liquid_ge10_notional_weighted_is_bps", unit=""
        ).set(float(summary.get("liquid_ge10_notional_weighted_is_bps") or 0.0), sum_attrs)
        meter.create_gauge("tier0.paper_farm.combined_tca_p95_is_bps", unit="").set(
            float(summary.get("p95_is_bps") or 0.0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.combined_tca_borrow_fee_usd", unit="").set(
            float(summary.get("total_borrow_fee_usd") or 0.0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.combined_extended_hours_protected_ratio", unit="").set(
            float(summary.get("extended_hours_protected_ratio", 1.0)), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.combined_submitted_vs_filled_ratio", unit="").set(
            float(summary.get("submitted_vs_filled_ratio") or 0.0), sum_attrs
        )

        tv_doc = manifest.get("tv_half_auto", {})
        tv_attrs = {"lane": "half-auto-tv", "semi_auto_confirmed": "false"}
        meter.create_gauge("tier0.tv_half_auto.tickets_count", unit="").set(
            float(tv_doc.get("tickets_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.ready_tickets_count", unit="").set(
            float(tv_doc.get("ready_tickets_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.standby_presets_count", unit="").set(
            float(tv_doc.get("standby_presets_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.staged_presets_count", unit="").set(
            float(tv_doc.get("staged_presets_total") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.schema_validated_count", unit="").set(
            float(tv_doc.get("schema_validated_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.duckdb_audit_rows", unit="").set(
            float(tv_doc.get("duckdb_audit_rows") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.unusual_vol_count", unit="").set(
            float(tv_doc.get("unusual_vol_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.darkpool_hits_count", unit="").set(
            float(tv_doc.get("darkpool_hits_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_half_auto.darkpool_trf_covered_count", unit="").set(
            float(tv_doc.get("darkpool_trf_covered_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.scanner.ranked_count", unit="").set(
            float(tv_doc.get("scanner_ranked_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.scanner.extreme_eligible_count", unit="").set(
            float(tv_doc.get("scanner_extreme_eligible_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.scanner.macro_regime", unit="").set(
            float(tv_doc.get("macro_regime") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_engine_coordination.evaluated_count", unit="").set(
            float(tv_doc.get("coordination_evaluated_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.tv_engine_coordination.ready_count", unit="").set(
            float(tv_doc.get("coordination_ready_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.paper_farm.active_trailed_positions", unit="").set(
            float(tv_doc.get("active_trailed_positions_count") or 0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.adopted_external_positions_count", unit="").set(
            float(tv_doc.get("adopted_external_positions_count") or 0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.synthetic_offhours_trailed_count", unit="").set(
            float(tv_doc.get("synthetic_offhours_trailed_count") or 0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.peak_reverse_armed_count", unit="").set(
            float(tv_doc.get("peak_reverse_armed_count") or 0), sum_attrs
        )
        meter.create_gauge("tier0.paper_farm.rapid_reentry_ready_count", unit="").set(
            float(tv_doc.get("rapid_reentry_ready_count") or 0), sum_attrs
        )
        v_handles = tv_doc.get("venue_order_handles")
        venues_map: dict[str, dict[str, Any]] = (
            {str(k): v for k, v in v_handles["venues"].items() if isinstance(v, dict)}
            if isinstance(v_handles, dict) and isinstance(v_handles.get("venues"), dict)
            else {}
        )
        if not venues_map:
            active_sess = str(tv_doc.get("active_session") or "RTH")
            for vp in ("IBKR_PAPER_4002", "ALPACA_PAPER_200", "ALPACA_PAPER_1000"):
                venues_map[vp] = eng.build_venue_order_handle_spec(vp, session=active_sess)
        g_vh_max_ot = meter.create_gauge("tier0.venue_order_handle.max_rpm", unit="")
        g_vh_res_ot = meter.create_gauge("tier0.venue_order_handle.exit_reserve_rpm", unit="")
        for vp, spec in venues_map.items():
            vp_attrs = {
                "venue_profile": vp,
                "broker": str(spec.get("broker") or ""),
                "environment": str(spec.get("environment") or ""),
            }
            g_vh_max_ot.set(float(spec.get("max_rpm") or 0), vp_attrs)
            g_vh_res_ot.set(float(spec.get("exit_reserve_rpm") or 0), vp_attrs)

        live_rpm_src = (
            manifest.get("rate_budget_live_rpm") or tv_doc.get("rate_budget_live_rpm") or {}
        )
        raw_per_acct_ot = (
            live_rpm_src.get("per_account") if isinstance(live_rpm_src, dict) else None
        )
        live_rpm_per_acct: dict[str, Any] = (
            raw_per_acct_ot if isinstance(raw_per_acct_ot, dict) else {}
        )
        g_live_used_rpm_ot = meter.create_gauge("tier0.paper_farm.live_used_rpm_60s", unit="")
        g_live_util_pct_ot = meter.create_gauge(
            "tier0.paper_farm.live_rpm_utilization_pct", unit=""
        )
        rpm_accounts = list(
            dict.fromkeys([*live_rpm_per_acct.keys(), *manifest.get("per_lane", {}).keys()])
        )
        for acct_k in rpm_accounts:
            acct_lbl = str(acct_k).strip().upper()
            if not acct_lbl:
                continue
            rpm_entry = live_rpm_per_acct.get(acct_k)
            lane_entry = manifest.get("per_lane", {}).get(acct_k)
            if isinstance(rpm_entry, dict):
                used_v = float(rpm_entry.get("used_rpm_60s") or 0.0)
                util_v = float(rpm_entry.get("utilization_pct") or 0.0)
            elif isinstance(lane_entry, dict):
                used_v = float(lane_entry.get("live_used_rpm_60s") or 0.0)
                util_v = float(lane_entry.get("live_rpm_utilization_pct") or 0.0)
            else:
                used_v, util_v = 0.0, 0.0
            g_live_used_rpm_ot.set(used_v, {"account": acct_lbl})
            g_live_util_pct_ot.set(util_v, {"account": acct_lbl})

        g_mom_score_ot = meter.create_gauge("tier0.momentum.top_score", unit="")
        g_mom_rvol_ot = meter.create_gauge("tier0.momentum.rvol", unit="")
        g_mom_trf_ot = meter.create_gauge("tier0.momentum.darkpool_trf_ratio", unit="")
        for mover in tv_doc.get("momentum_movers") or ():
            if not isinstance(mover, dict) or not mover.get("symbol"):
                continue
            sym_l = str(mover["symbol"]).strip().upper()
            cat_l = str(mover.get("calibrated_category") or "UNKNOWN").strip().upper()
            sess_l = str(mover.get("session") or "CLOSED").strip().upper()
            g_mom_score_ot.set(
                float(mover.get("score") or 0.0),
                {"symbol": sym_l, "calibrated_category": cat_l, "session": sess_l},
            )
            g_mom_rvol_ot.set(
                float(mover.get("rvol") or 0.0),
                {"symbol": sym_l, "session": sess_l},
            )
            if isinstance(mover.get("darkpool_trf_ratio"), (int, float)):
                g_mom_trf_ot.set(float(mover["darkpool_trf_ratio"]), {"symbol": sym_l})
        meter.create_gauge("tier0.momentum.ignition_count", unit="").set(
            float(tv_doc.get("momentum_ignition_count") or 0), sum_attrs
        )
        meter.create_gauge("tier0.momentum.climax_reversal_count", unit="").set(
            float(tv_doc.get("momentum_climax_reversal_count") or 0), sum_attrs
        )

        g_pos_tier_ot = meter.create_gauge("tier0.position.profit_lock_tier", unit="")
        g_pos_fast_ot = meter.create_gauge("tier0.position.fast_sell_armed", unit="")
        for pos in tv_doc.get("active_trailed_positions") or ():
            if not isinstance(pos, dict) or not pos.get("symbol"):
                continue
            p_sym = str(pos["symbol"]).strip().upper()
            p_acct = str(pos.get("account") or "UNKNOWN").strip().upper()
            p_sess = (
                str(pos.get("active_session") or pos.get("entered_session") or "CLOSED")
                .strip()
                .upper()
            )
            p_mode = str(pos.get("order_mode") or "STOP_LIMIT").strip().upper()
            g_pos_tier_ot.set(
                float(pos.get("lock_tier") or 0),
                {"symbol": p_sym, "account": p_acct, "session": p_sess, "order_mode": p_mode},
            )
            g_pos_fast_ot.set(
                1.0 if pos.get("fast_sell_armed") is True else 0.0,
                {"symbol": p_sym, "account": p_acct},
            )

        meter.create_gauge("tier0.staged_alert.active_count", unit="").set(
            float(tv_doc.get("staged_alert_active_count") or 0), tv_attrs
        )
        meter.create_gauge("tier0.staged_alert.stale_count", unit="").set(
            float(tv_doc.get("staged_alert_stale_count") or 0), tv_attrs
        )

        recorded_names: list[str] = []
        point_count = 0
        if owned_provider:
            metrics_data = in_memory_reader.get_metrics_data()
            if metrics_data is not None:
                for rm in metrics_data.resource_metrics:
                    for sm in rm.scope_metrics:
                        for metric in sm.metrics:
                            recorded_names.append(metric.name)
                            point_count += len(metric.data.data_points)
    finally:
        if owned_provider:
            provider.shutdown()

    return {
        "otel_metric_names": sorted(set(recorded_names)),
        "otel_data_points": point_count,
        "otel_otlp_exported": otlp_exported,
    }


def record_paper_farm_telemetry(
    manifest: dict[str, Any],
    *,
    prom_output_path: Path | None = None,
    otlp_endpoint: str | None = None,
    meter_provider: MeterProvider | None = None,
) -> dict[str, Any]:
    """Emit official Prometheus text exposition + OTel metrics with zero custom HTTP servers."""
    _, prom_text = export_prometheus_metrics(manifest, output_path=prom_output_path)
    otel_rep = record_otel_metrics(
        manifest, otlp_endpoint=otlp_endpoint, meter_provider=meter_provider
    )
    return {
        "paper_only": True,
        "live_execution_enabled": False,
        "custom_http_server": False,
        "prometheus_text": prom_text,
        "prom_output_path": str(prom_output_path) if prom_output_path is not None else None,
        **otel_rep,
    }


def aggregate_paper_farm_performance(
    manifest_dir: Path | None = None,
    *,
    output_path: Path | None = None,
    finos_output_path: Path | None = None,
    duckdb_glob: str | None = None,
    prom_output_path: Path | None = None,
    otlp_endpoint: str | None = None,
) -> dict[str, Any]:
    """Aggregate Paper Farm performance manifest, TCA summary, FINOS/OpenBB & telemetry."""
    return PaperPerformanceAggregator(manifest_dir, duckdb_glob=duckdb_glob).aggregate(
        output_path=output_path,
        finos_output_path=finos_output_path,
        prom_output_path=prom_output_path,
        otlp_endpoint=otlp_endpoint,
    )

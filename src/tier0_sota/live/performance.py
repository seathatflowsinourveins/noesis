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

from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    IBKR_PAPER_ACCOUNT,
    _duckdb_filename,
    default_manifest_dir,
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


def _read_duckdb_counts(duck_path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {
        "duckdb_orders": 0,
        "roundtrips": 0,
        "wins": 0,
        "realized_pnl_usd": 0.0,
        "tca_fills_count": 0,
        "tca_is_bps": [],
        "tca_total_net_cost_bps": [],
        "tca_borrow_fee_usd": _DEC_ZERO,
        "tca_borrow_bps": [],
        "tca_markout_60s_bps": [],
        "ext_orders_db": 0,
        "ext_protected_db": 0,
    }
    if not duck_path.is_file():
        return out
    tmp: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tf:
            tmp = Path(tf.name)
        shutil.copy2(duck_path, tmp)
        with duckdb.connect(str(tmp), read_only=True) as conn:
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            if "paper_wire_orders" in tables:
                row = conn.execute("SELECT COUNT(*) FROM paper_wire_orders").fetchone()
                out["duckdb_orders"] = int(row[0]) if row else 0
                ext_row = conn.execute(
                    "SELECT COUNT(*) FROM paper_wire_orders "
                    "WHERE UPPER(session) IN ('PRE_MARKET', 'AFTER_HOURS') "
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
                tca_rows = conn.execute(
                    "SELECT implementation_shortfall_bps, total_net_cost_bps, "
                    "borrow_fee_usd, borrow_bps, markout_60s_bps "
                    "FROM paper_tca_fills"
                ).fetchall()
                out["tca_fills_count"] = len(tca_rows)
                is_vals: list[Decimal] = []
                net_vals: list[Decimal] = []
                borrow_sum = _DEC_ZERO
                borrow_bps_vals: list[Decimal] = []
                mk60_vals: list[Decimal] = []
                for is_bps, net_bps, b_usd, b_bps, mk60 in tca_rows:
                    is_vals.append(_to_dec(is_bps))
                    net_vals.append(_to_dec(net_bps))
                    borrow_sum += _to_dec(b_usd)
                    borrow_bps_vals.append(_to_dec(b_bps))
                    if mk60 is not None:
                        mk60_vals.append(_to_dec(mk60))
                out["tca_is_bps"] = is_vals
                out["tca_total_net_cost_bps"] = net_vals
                out["tca_borrow_fee_usd"] = borrow_sum
                out["tca_borrow_bps"] = borrow_bps_vals
                out["tca_markout_60s_bps"] = mk60_vals
            if "paper_supervised_stops" in tables:
                sup_row = conn.execute(
                    "SELECT COUNT(DISTINCT broker_order_id) FROM paper_supervised_stops "
                    "WHERE protective_stop_mode = 'COLLARED_STOP_LIMIT_SUPERVISED'"
                ).fetchone()
                out["ext_protected_db"] = int(sup_row[0]) if sup_row else 0
    except (OSError, duckdb.Error):
        pass
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)
    return out


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

    def _collect_lane_with_samples(
        self, account: str
    ) -> tuple[dict[str, Any], dict[str, list[Decimal] | Decimal]]:
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
            fills = int(entries or duck["tca_fills_count"] or duck["duckdb_orders"])

        rts = max(int(status.get("session_roundtrips") or 0), duck["roundtrips"])
        pnl_usd = float(status.get("session_realized_pnl_usd") or duck["realized_pnl_usd"])

        is_vals: list[Decimal] = duck["tca_is_bps"]
        net_vals: list[Decimal] = duck["tca_total_net_cost_bps"]
        borrow_usd: Decimal = duck["tca_borrow_fee_usd"]
        borrow_bps_vals: list[Decimal] = duck["tca_borrow_bps"]
        mk60_vals: list[Decimal] = duck["tca_markout_60s_bps"]

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

        metrics = {
            "account": account,
            "lane_id": lane_id,
            "paper_only": True,
            "live_execution_enabled": False,
            "equity": round(float(status.get("equity") or 0.0), 2),
            "open_positions_count": int(status.get("open_positions_count") or 0),
            "drawdown_pct": round(float(status.get("daily_drawdown_pct") or 0.0), 6),
            "entries": entries,
            "orders_submitted": orders_submitted,
            "fills": fills,
            "tca_fills_count": duck["tca_fills_count"],
            "mean_is_bps": round(float(_mean_decimal(is_vals)), 6),
            "median_is_bps": round(float(_percentile_decimal(is_vals, Decimal("0.50"))), 6),
            "p90_is_bps": round(float(_percentile_decimal(is_vals, Decimal("0.90"))), 6),
            "p95_is_bps": round(float(_percentile_decimal(is_vals, Decimal("0.95"))), 6),
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
        }
        samples: dict[str, list[Decimal] | Decimal] = {
            "is_bps": is_vals,
            "net_bps": net_vals,
            "borrow_usd": borrow_usd,
            "borrow_bps": borrow_bps_vals,
            "markout_60s_bps": mk60_vals,
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
        total_borrow_usd = _DEC_ZERO

        for acct in accounts:
            metrics, samples = self._collect_lane_with_samples(acct)
            lanes[acct] = metrics
            is_s = samples["is_bps"]
            net_s = samples["net_bps"]
            bb_s = samples["borrow_bps"]
            mk_s = samples["markout_60s_bps"]
            b_u = samples["borrow_usd"]
            if isinstance(is_s, list):
                all_is.extend(is_s)
            if isinstance(net_s, list):
                all_net.extend(net_s)
            if isinstance(bb_s, list):
                all_borrow_bps.extend(bb_s)
            if isinstance(mk_s, list):
                all_mk60.extend(mk_s)
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

        tca_summary = {
            "tca_fills_count": len(all_is),
            "orders_submitted": total_submitted,
            "fills_confirmed": total_fills,
            "mean_is_bps": round(float(_mean_decimal(all_is)), 6),
            "median_is_bps": round(float(_percentile_decimal(all_is, Decimal("0.50"))), 6),
            "p90_is_bps": round(float(_percentile_decimal(all_is, Decimal("0.90"))), 6),
            "p95_is_bps": round(float(_percentile_decimal(all_is, Decimal("0.95"))), 6),
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
            "combined_equity_usd": round(float(df["equity"].sum()), 2),
            "max_drawdown_pct": round(max_dd, 6),
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
            "mean_is_bps": tca_summary["mean_is_bps"],
            "median_is_bps": tca_summary["median_is_bps"],
            "p90_is_bps": tca_summary["p90_is_bps"],
            "p95_is_bps": tca_summary["p95_is_bps"],
            "mean_total_net_cost_bps": tca_summary["mean_total_net_cost_bps"],
            "total_borrow_fee_usd": tca_summary["total_borrow_fee_usd"],
            "extended_hours_protected_ratio": tca_summary["extended_hours_protected_ratio"],
            "submitted_vs_filled_ratio": tca_summary["submitted_vs_filled_ratio"],
        }
        now_iso = datetime.now(UTC).isoformat()
        finos_doc = build_finos_openbb_export(
            lanes, combined, generated_at_utc=now_iso, tca_summary=tca_summary
        )
        manifest: dict[str, Any] = {
            "schema_version": "tier0.paper-farm-live-performance.v1",
            "generated_at_utc": now_iso,
            "paper_only": True,
            "live_execution_enabled": False,
            "paper_live_deployed": False,
            "summary": combined,
            "tca_summary": tca_summary,
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
        "summary": summary,
        "tca_summary": tca_summary or {},
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
        g_eq.labels(acct, lane_id).set(float(row.get("equity") or 0.0))
        g_dd.labels(acct, lane_id).set(float(row.get("drawdown_pct") or 0.0))
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
                {"service.name": "tier0-paper-farm", "tier0.paper_only": True}
            ),
            metric_readers=readers,
        )
    )
    try:
        meter = provider.get_meter("tier0_sota.live.performance", version="1.0.0")
        g_eq = meter.create_gauge("tier0.paper_farm.equity_usd", unit="USD")
        g_is_mean = meter.create_gauge("tier0.paper_farm.tca.mean_is_bps", unit="bps")
        g_is_p95 = meter.create_gauge("tier0.paper_farm.tca.p95_is_bps", unit="bps")
        g_borrow = meter.create_gauge("tier0.paper_farm.tca.borrow_fee_usd", unit="USD")
        g_ext = meter.create_gauge("tier0.paper_farm.extended_hours.protected_ratio", unit="1")
        g_fill_ratio = meter.create_gauge("tier0.paper_farm.submitted_vs_filled_ratio", unit="1")
        c_sub = meter.create_counter("tier0.paper_farm.orders_submitted", unit="1")
        c_fills = meter.create_counter("tier0.paper_farm.fills_confirmed", unit="1")

        for acct, row in manifest.get("per_lane", {}).items():
            attrs = {"account": str(acct), "lane_id": str(row.get("lane_id", acct))}
            g_eq.set(float(row.get("equity") or 0.0), attrs)
            g_is_mean.set(float(row.get("mean_is_bps") or 0.0), attrs)
            g_is_p95.set(float(row.get("p95_is_bps") or 0.0), attrs)
            g_borrow.set(float(row.get("total_borrow_fee_usd") or 0.0), attrs)
            g_ext.set(float(row.get("extended_hours_protected_ratio", 1.0)), attrs)
            g_fill_ratio.set(float(row.get("submitted_vs_filled_ratio") or 0.0), attrs)
            sub_n = int(row.get("orders_submitted") or 0)
            fill_n = int(row.get("fills") or 0)
            if sub_n > 0:
                c_sub.add(sub_n, attrs)
            if fill_n > 0:
                c_fills.add(fill_n, attrs)

        summary = manifest.get("summary", {})
        sum_attrs = {"scope": "combined"}
        g_eq.set(float(summary.get("combined_equity_usd") or 0.0), sum_attrs)
        g_is_mean.set(float(summary.get("mean_is_bps") or 0.0), sum_attrs)
        g_is_p95.set(float(summary.get("p95_is_bps") or 0.0), sum_attrs)
        g_borrow.set(float(summary.get("total_borrow_fee_usd") or 0.0), sum_attrs)
        g_ext.set(float(summary.get("extended_hours_protected_ratio", 1.0)), sum_attrs)
        g_fill_ratio.set(float(summary.get("submitted_vs_filled_ratio") or 0.0), sum_attrs)

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

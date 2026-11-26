"""Unified Paper Farm performance manifest aggregator & FINOS/OpenBB JSON exporter."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from tier0_sota.live.paper_farm import ALPACA_LANE_PROFILES, IBKR_PAPER_ACCOUNT

ALL_FARM_ACCOUNTS: tuple[str, ...] = (*ALPACA_LANE_PROFILES.keys(), IBKR_PAPER_ACCOUNT)


def _read_json_safe(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _read_duckdb_counts(duck_path: Path) -> dict[str, Any]:
    out = {"duckdb_orders": 0, "roundtrips": 0, "wins": 0, "realized_pnl_usd": 0.0}
    if not duck_path.is_file():
        return out
    try:
        with duckdb.connect(str(duck_path), read_only=True) as conn:
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            if "paper_wire_orders" in tables:
                row = conn.execute("SELECT COUNT(*) FROM paper_wire_orders").fetchone()
                out["duckdb_orders"] = int(row[0]) if row else 0
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
    except duckdb.Error:
        pass
    return out


class PaperPerformanceAggregator:
    """Aggregate isolated per-lane status & DuckDB telemetry into manifest + FINOS JSON."""

    def __init__(self, manifest_dir: Path) -> None:
        self.manifest_dir = manifest_dir

    def collect_lane_metrics(self, account: str) -> dict[str, Any]:
        slug = account.lower()
        status = _read_json_safe(self.manifest_dir / f"{slug}_live_automation_status.json")
        duck = _read_duckdb_counts(self.manifest_dir / f"{slug}_seamless_orders.duckdb")
        default_lane = (
            ALPACA_LANE_PROFILES[account]["lane_id"]
            if account in ALPACA_LANE_PROFILES
            else "IBKR_SMART_24X5"
        )
        lane_id = status.get("lane_id") or default_lane
        fast_sells = int(
            status.get("fast_sells")
            or (
                int(status.get("lossless_fast_sells") or 0)
                + int(status.get("fast_scratches") or 0)
            )
        )
        fills = int(status.get("fills") or status.get("entries") or duck["duckdb_orders"])
        rts = max(int(status.get("session_roundtrips") or 0), duck["roundtrips"])
        pnl_usd = float(status.get("session_realized_pnl_usd") or duck["realized_pnl_usd"])
        return {
            "account": account,
            "lane_id": lane_id,
            "paper_only": True,
            "live_execution_enabled": False,
            "equity": round(float(status.get("equity") or 0.0), 2),
            "drawdown_pct": round(float(status.get("daily_drawdown_pct") or 0.0), 6),
            "fills": fills,
            "auto_shorts": int(status.get("auto_shorts") or 0),
            "reverses": int(status.get("reverses") or 0),
            "reentries": int(status.get("reentries") or 0),
            "fast_sells": fast_sells,
            "roundtrips": rts,
            "realized_pnl_usd": round(pnl_usd, 2),
            "duckdb_orders": duck["duckdb_orders"],
            "status_present": bool(status),
        }

    def aggregate(
        self,
        *,
        accounts: tuple[str, ...] = ALL_FARM_ACCOUNTS,
        output_path: Path | None = None,
    ) -> dict[str, Any]:
        lanes = {acct: self.collect_lane_metrics(acct) for acct in accounts}
        df = pl.DataFrame(list(lanes.values()))
        max_dd = max((float(m["drawdown_pct"]) for m in lanes.values()), default=0.0)
        combined = {
            "combined_equity_usd": round(float(df["equity"].sum()), 2),
            "max_drawdown_pct": round(max_dd, 6),
            "fills": int(df["fills"].sum()),
            "auto_shorts": int(df["auto_shorts"].sum()),
            "reverses": int(df["reverses"].sum()),
            "reentries": int(df["reentries"].sum()),
            "fast_sells": int(df["fast_sells"].sum()),
            "roundtrips": int(df["roundtrips"].sum()),
            "realized_pnl_usd": round(float(df["realized_pnl_usd"].sum()), 2),
        }
        manifest = {
            "schema_version": "tier0.paper-farm-live-performance.v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "paper_only": True,
            "live_execution_enabled": False,
            "paper_live_deployed": False,
            "summary": combined,
            "per_lane": lanes,
            "finos_openbb": build_finos_openbb_export(lanes, combined),
        }
        target = output_path or (self.manifest_dir / "paper_farm_live_performance_manifest.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        tmp.replace(target)
        return manifest


def build_finos_openbb_export(
    per_lane: dict[str, dict[str, Any]], summary: dict[str, Any]
) -> dict[str, Any]:
    """Build declarative FINOS Workspace Plotly + OpenBB tabular JSON export."""
    accts = list(per_lane.keys())
    return {
        "schema_version": "tier0.finos-openbb-paper-farm.v1",
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
    }


def aggregate_paper_farm_performance(
    manifest_dir: Path, *, output_path: Path | None = None
) -> dict[str, Any]:
    """Aggregate Paper Farm performance manifest and FINOS/OpenBB export."""
    return PaperPerformanceAggregator(manifest_dir).aggregate(output_path=output_path)

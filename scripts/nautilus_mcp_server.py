#!/usr/bin/env python3
"""Official FastMCP stdio server exposing Tier-0 NautilusTrader v2 & risk/research tools."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from zoneinfo import ZoneInfo


def _apply_mcp_stdio_thread_bounds() -> None:
    """Cap idle thread pools for long-lived stdio MCP server processes."""
    if (
        __name__ != "__main__"
        or "--smoke" in sys.argv
        or "--self-check" in sys.argv
        or os.environ.get("TIER0_UNBOUNDED_THREADS", "").strip() == "1"
    ):
        return
    raw = os.environ.get("TIER0_MCP_THREADS", "2").strip()
    threads = raw if raw.isdigit() and int(raw) > 0 else "2"
    for key in (
        "POLARS_MAX_THREADS",
        "RAYON_NUM_THREADS",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "TOKIO_WORKER_THREADS",
    ):
        os.environ[key] = threads
    os.environ.setdefault("OMP_WAIT_POLICY", "PASSIVE")
    os.environ.setdefault("GOMP_SPINCOUNT", "0")


_apply_mcp_stdio_thread_bounds()

import polars as pl  # noqa: E402
from mcp.server.fastmcp import FastMCP  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from tier0_sota.discovery import (  # noqa: E402
    BAR_DURATION_NS,
    DISCOVERY_WALL_ISO,
    assert_pre_wall_date,
    load_minute_bars_day,
    previous_xnys_session,
)
from tier0_sota.engine import (  # noqa: E402
    POOLED_TRAILING_SCHEDULES,
    SESSION_PROTECTION_24X5,
    build_sandbox_live_node,
    build_venue_live_node_config,
    build_venue_order_handle_spec,
    get_session_protection_profile,
    inspect_venue_capability,
    run_nautilus_signal_backtest,
    select_pooled_trailing_schedule,
    validate_venue_order_capability,
)
from tier0_sota.live.paper_farm import resolve_alpaca_paper_venue_profile  # noqa: E402
from tier0_sota.risk import (  # noqa: E402
    MAX_STOP_LOSS_PCT,
    Side,
    compute_capped_stop_price,
    size_position_by_risk,
)
from tier0_sota.tv.bridge import (  # noqa: E402
    DEFAULT_MANIFEST_DIR,
    DEFAULT_STAGED_PATH,
    DEFAULT_WATCHLIST_PATH,
    _atomic_write_json,
    refuse_live_endpoints,
    stage_tv_alert_preset,
)

_NY_TZ = ZoneInfo("America/New_York")
_DEFAULT_LAKE_ROOT = (
    Path("Z:/sota-data/prewall-lake") if os.name == "nt" else Path("/mnt/z/sota-data/prewall-lake")
)
_DEFAULT_PERF_PATH = DEFAULT_MANIFEST_DIR / "paper_farm_live_performance_manifest.json"
_FALLBACK_PERF_PATH = DEFAULT_MANIFEST_DIR / "live_performance_manifest.json"
_FALLBACK_MOVERS_PATH = DEFAULT_MANIFEST_DIR / "live_whole_market_movers_manifest.json"

mcp = FastMCP("tier0-nautilus-engine")


@mcp.tool()
def get_engine_backbone_status(venue_profile: str = "IBKR_PAPER_4002") -> dict[str, Any]:
    """Return read-only Environment.SANDBOX LiveNode spec and 24/5 session metadata."""
    prof = validate_venue_order_capability(venue_profile)
    lcfg = build_venue_live_node_config(prof.venue_id)
    node = build_sandbox_live_node(venue_profile=prof.venue_id)
    try:
        env_name = node.environment.name
    finally:
        node.dispose()
    re_cfg = lcfg.risk_engine
    return {
        "native_engine": "nautilus_trader.live.node.LiveNode",
        "environment": env_name,
        "venue_profile": prof.venue_id,
        "broker_ports_open": 0,
        "paper_live_deployed": False,
        "live_execution_enabled": False,
        "risk_engine": {
            "bypass": bool(re_cfg.bypass),
            "max_order_submit_rate": str(re_cfg.max_order_submit_rate),
            "max_order_modify_rate": str(re_cfg.max_order_modify_rate),
        },
        "order_handle_spec": build_venue_order_handle_spec(prof.venue_id),
        "sessions_24x5": sorted(SESSION_PROTECTION_24X5.keys()),
    }


@mcp.tool()
def inspect_venue_profile(
    venue_profile: str = "IBKR_PAPER_4002",
    algo: str = "STOP_LIMIT",
    requested_rpm: int | None = None,
    require_24x5_overnight: bool = False,
    session: str = "RTH",
    trailing_schedule: str = "baseline",
) -> dict[str, Any]:
    """Inspect multi-venue RPM pacing, order-handle spec, algo support & fail-closed LIVE gate."""
    cap = inspect_venue_capability(
        venue_profile,
        algo=algo,
        requested_rpm=requested_rpm,
        require_24x5_overnight=require_24x5_overnight,
    )
    cap["order_handle_spec"] = build_venue_order_handle_spec(
        venue_profile, session=session, trailing_schedule=trailing_schedule
    )
    return cap


@mcp.tool()
def compute_risk_sized_stop(
    entry_price: str,
    side: str,
    equity: str,
    max_loss_pct: str = "0.0200",
    max_notional_pct: str = "0.10",
) -> dict[str, Any]:
    """Compute exact Decimal Rule-612 stop (<= 2.00% cap) and integer risk position size."""
    sd_raw = side.strip().upper()
    if sd_raw not in ("LONG", "SHORT"):
        raise ValueError(f"Unsupported side={side!r}; expected LONG or SHORT")
    sd: Side = "LONG" if sd_raw == "LONG" else "SHORT"
    e, eq = Decimal(entry_price), Decimal(equity)
    ml, mn = Decimal(max_loss_pct), Decimal(max_notional_pct)
    stop = compute_capped_stop_price(e, sd, max_loss_pct=ml)
    shares = size_position_by_risk(equity=eq, entry_price=e, stop_price=stop, max_notional_pct=mn)
    return {
        "entry_price": str(e),
        "side": sd,
        "capped_stop_price": str(stop),
        "shares": int(shares),
        "max_loss_pct": str(min(ml, MAX_STOP_LOSS_PCT)),
    }


@mcp.tool()
def run_prewall_native_backbone_parity(
    symbol: str = "AAPL", day: str = "2026-10-15"
) -> dict[str, Any]:
    """Run deterministic pre-wall BacktestEngine + Environment.SANDBOX parity witness."""
    d = assert_pre_wall_date(day)
    sym = symbol.strip().upper() or "AAPL"
    t0_s = int(datetime(d.year, d.month, d.day, 9, 30, tzinfo=_NY_TZ).astimezone(UTC).timestamp())
    t0 = t0_s * 1_000_000_000
    closes = [100.0, 100.4, 101.2, 102.0, 102.5, 102.1]
    rows = [
        {
            "symbol": sym,
            "t_ns": t0 + i * BAR_DURATION_NS,
            "t_close_ns": t0 + (i + 1) * BAR_DURATION_NS,
            "o": closes[i - 1] if i > 0 else c,
            "h": max(closes[i - 1] if i > 0 else c, c) * 1.001,
            "l": min(closes[i - 1] if i > 0 else c, c) * 0.999,
            "c": c,
            "v": 20_000.0,
        }
        for i, c in enumerate(closes)
    ]
    df = pl.DataFrame(rows)
    sigs: dict[int, int] = {t0 + BAR_DURATION_NS: 1, t0 + 5 * BAR_DURATION_NS: 0}
    bt = run_nautilus_signal_backtest(df, sigs, fixed_qty=10, enable_24x5_trailing=True)
    sbx = build_venue_live_node_config("IBKR_PAPER_4002")
    live_refused = False
    try:
        validate_venue_order_capability("IBKR_LIVE_3000")
    except PermissionError:
        live_refused = True
    return {
        "overall_ok": bt.bars_processed == len(closes) and bt.stops_attached >= 1 and live_refused,
        "symbol": sym,
        "day": d.isoformat(),
        "sandbox_environment": sbx.environment.name,
        "broker_ports_open": 0,
        "paper_live_deployed": False,
        "live_execution_enabled": False,
        "live_profile_refused": live_refused,
        "bars_processed": bt.bars_processed,
        "stops_attached": bt.stops_attached,
    }


@mcp.tool()
def check_prewall_pit_session(day: str = "2026-10-15") -> dict[str, Any]:
    """Validate pre-wall PIT date (<= 2026-10-15) and resolve prior XNYS session."""
    d = assert_pre_wall_date(day)
    prev = previous_xnys_session(d)
    return {
        "pre_wall_ok": True,
        "day": d.isoformat(),
        "prior_xnys_session": prev.isoformat(),
        "discovery_wall_iso": DISCOVERY_WALL_ISO,
    }


@mcp.tool()
def evaluate_l1_queue_markout(
    symbol: str, day: str = "2026-10-15", side: str = "BUY", root: str | None = None
) -> dict[str, Any]:
    """Run pre-wall Rust hftbacktest RiskAdverseQueueModel L1 markout when lake files exist."""
    d = assert_pre_wall_date(day)
    sd = side.strip().upper()
    if sd not in ("BUY", "SELL"):
        raise ValueError(f"Unsupported side={side!r}; expected BUY or SELL")
    sym = symbol.strip().upper()
    base = Path(root) if root else _DEFAULT_LAKE_ROOT
    q_dir = base / "published-orb" / "quotes" / sym
    t_dir = base / "published-orb" / "trades" / sym
    q_files = sorted(q_dir.glob(f"{d.isoformat()}_*.parquet")) if q_dir.is_dir() else []
    t_files = sorted(t_dir.glob(f"{d.isoformat()}_*.parquet")) if t_dir.is_dir() else []
    if not q_files or not t_files:
        return {"ok": False, "error": f"Missing L1 quotes/trades for {sym} on {d.isoformat()}"}
    q_df = pl.scan_parquet([str(p) for p in q_files]).collect()
    t_df = pl.scan_parquet([str(p) for p in t_files]).collect()
    from tier0_sota.research import evaluate_hftbacktest_queue_markout

    res = evaluate_hftbacktest_queue_markout(q_df, t_df, symbol=sym, side=sd)
    return {"ok": True, **res}


_ADMITTED_WATCHLIST_SCHEMAS = frozenset(
    {
        "tier0.watchlist-manifest.v1",
        "tier0.whole-market-movers.v2",
        "tier0.live-half-auto-tickets.v1",
        "tier0.live-lane-preset-bundle.v1",
        "tier0.tv-compounding-manifest.v1",
        "tier0.tv-engine-coordination.v1",
    }
)


def _read_optional_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


@mcp.tool()
def get_live_watchlist_manifest(manifest_path: str | None = None) -> dict[str, Any]:
    """Read live watchlist, movers, preset bundle, or TV compounding manifest fail-closed."""
    env_p = os.environ.get("TIER0_WATCHLIST_MANIFEST") or os.environ.get(
        "TIER0_LIVE_WATCHLIST_MANIFEST"
    )
    default_p = (
        DEFAULT_WATCHLIST_PATH if DEFAULT_WATCHLIST_PATH.is_file() else _FALLBACK_MOVERS_PATH
    )
    target = Path(manifest_path or env_p or str(default_p))
    if not target.is_file():
        return {"ok": False, "error": f"Watchlist manifest not found: {target}"}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"Invalid watchlist manifest JSON: {exc}"}
    if not isinstance(data, dict):
        return {"ok": False, "error": "Invalid watchlist manifest JSON: expected object"}
    schema = data.get("schema_version")
    if schema not in _ADMITTED_WATCHLIST_SCHEMAS:
        return {"ok": False, "error": f"Unexpected schema_version: {schema!r}"}
    res: dict[str, Any] = {"ok": True, "manifest_path": str(target), **data}
    if schema == "tier0.whole-market-movers.v2":
        tv_sync: dict[str, Any] = {}
        bundle = _read_optional_json_dict(target.parent / "live_lane_order_preset_bundle.json")
        if bundle is not None:
            tv_sync["preset_bundle_count"] = len(bundle.get("top_potential_symbols") or ())
            tv_sync["tca_calibration_ok"] = bool(bundle.get("tca_calibration_ok", True))
        comp = _read_optional_json_dict(
            target.parent / "tv_sota_pack" / "tv_compounding_manifest.json"
        )
        if comp is not None:
            tv_sync["tv_compounding"] = comp
        tickets = _read_optional_json_dict(
            target.parent / "tv_sota_pack" / "live_half_auto_tickets.json"
        )
        if tickets is not None:
            tv_sync["tickets_count"] = len(tickets.get("tickets") or ())
            tv_sync["standby_presets_count"] = len(tickets.get("standby_presets") or ())
        coord = _read_optional_json_dict(
            target.parent / "tv_sota_pack" / "tv_engine_coordination_manifest.json"
        )
        if coord is not None:
            tv_sync["tv_engine_coordination"] = coord
        if tv_sync:
            res["tv_sync"] = tv_sync
    return res


@mcp.tool()
def get_live_performance_manifest(manifest_path: str | None = None) -> dict[str, Any]:
    """Read Paper Farm live performance manifest JSON fail-closed with sibling TV sync."""
    env_p = os.environ.get("TIER0_PERFORMANCE_MANIFEST") or os.environ.get(
        "TIER0_LIVE_PERFORMANCE_MANIFEST"
    )
    default_p = _DEFAULT_PERF_PATH if _DEFAULT_PERF_PATH.is_file() else _FALLBACK_PERF_PATH
    target = Path(manifest_path or env_p or str(default_p))
    if not target.is_file():
        return {"ok": False, "error": f"Performance manifest not found: {target}"}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"Invalid performance manifest JSON: {exc}"}
    if not isinstance(data, dict):
        return {"ok": False, "error": "Invalid performance manifest JSON: expected object"}
    res: dict[str, Any] = {"ok": True, **data}
    comp = _read_optional_json_dict(target.parent / "tv_sota_pack" / "tv_compounding_manifest.json")
    if comp is not None:
        res["tv_compounding"] = comp
    coord = _read_optional_json_dict(
        target.parent / "tv_sota_pack" / "tv_engine_coordination_manifest.json"
    )
    if coord is not None:
        res["tv_engine_coordination"] = coord
    exe = _read_optional_json_dict(target.parent / "paper_farm_execution_status.json")
    if exe is not None:
        res["execution_status_snapshot"] = exe
    return res


@mcp.tool()
def rank_symbols_advisory(
    day: str = "2026-10-15",
    top_k: int = 10,
    min_dollar_volume: float = 5_000_000.0,
    root: str | None = None,
) -> dict[str, Any]:
    """Read-only advisory ranking of pre-wall extreme movers from causal 1-min lake bars."""
    td = assert_pre_wall_date(day)
    lake_root = Path(root) if root else _DEFAULT_LAKE_ROOT
    try:
        bars = load_minute_bars_day(td.isoformat(), root=lake_root)
    except FileNotFoundError as exc:
        return {"ok": False, "advisory_only": True, "error": str(exc)}
    c = pl.col
    ranked = (
        bars.sort(["symbol", "t_ns"])
        .group_by("symbol")
        .agg(
            (c("c") * c("v")).sum().alias("dollar_volume"),
            ((c("c").last() - c("o").first()) / c("o").first().clip(lower_bound=1e-6)).alias(
                "signed_ret"
            ),
            ((c("h").max() - c("l").min()) / c("o").first().clip(lower_bound=1e-6)).alias(
                "range_pct"
            ),
        )
        .filter(c("dollar_volume") >= float(min_dollar_volume))
        .with_columns((c("signed_ret").abs() * c("range_pct") * 10_000.0).alias("score"))
        .sort("score", descending=True)
        .head(max(1, int(top_k)))
    )
    return {
        "ok": True,
        "advisory_only": True,
        "day": td.isoformat(),
        "entries": ranked.to_dicts(),
    }


def _stage_minimal_order_preset(
    src: Mapping[str, Any], *, staged_path: str | None, venue: str
) -> dict[str, Any]:
    refuse_live_endpoints(src)
    sym = str(src.get("symbol") or "").strip().upper()
    if not sym or len(sym) > 16 or not sym.replace(".", "").replace("-", "").isalnum():
        raise ValueError(f"Invalid symbol: {src.get('symbol')!r}")
    qty_val = src.get("qty", 1)
    if isinstance(qty_val, bool) or type(qty_val) is not int or qty_val != 1:
        raise ValueError(f"Semi-auto preset requires qty == 1, got {qty_val!r}")
    sd_raw = str(src.get("side") or "").strip().upper()
    if sd_raw not in ("BUY", "SELL", "LONG", "SHORT"):
        raise ValueError(f"Unsupported side {src.get('side')!r}")
    out_side = "BUY" if sd_raw in ("BUY", "LONG") else "SELL"
    sess = str(src.get("session") or "").strip().upper()
    if sess not in ("PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT"):
        raise ValueError(f"Unsupported or closed session {src.get('session')!r}")
    vu = venue.strip().upper()
    req_vp = str(src.get("venue_profile") or "").strip().upper()
    if req_vp:
        vprof = validate_venue_order_capability(req_vp)
        vu = vprof.broker
        resolved_vp = vprof.venue_id
    else:
        if vu not in ("IBKR", "ALPACA"):
            raise ValueError(f"Unsupported venue {venue!r}")
        resolved_vp = "IBKR_PAPER_4002" if vu == "IBKR" else resolve_alpaca_paper_venue_profile()[0]
    if vu == "ALPACA" and out_side == "SELL" and sess == "OVERNIGHT":
        raise ValueError("ALPACA overnight SHORT unsupported; use IBKR")
    natr = Decimal(str(src.get("natr_pct", "0.020")))
    if not natr.is_finite() or natr <= Decimal("0"):
        raise ValueError(f"Invalid natr_pct: {natr!r}")
    sched, pmap = select_pooled_trailing_schedule(natr)
    prof = get_session_protection_profile(sess, pmap)
    req_sched = str(src.get("trailing_schedule") or sched).strip()
    if req_sched not in POOLED_TRAILING_SCHEDULES:
        raise ValueError(f"Unsupported trailing_schedule: {req_sched!r}")
    order_type = str(src.get("order_type") or prof.order_mode).strip().upper()
    if sess != "RTH" and order_type != "STOP_LIMIT":
        raise ValueError(f"Off-hours session {sess} requires STOP_LIMIT, got {order_type!r}")
    if sess == "RTH" and order_type not in ("STOP_MARKET", "STOP_LIMIT"):
        raise ValueError(f"Unsupported RTH order_type: {order_type!r}")
    collar = float(src.get("limit_collar_bps", prof.limit_collar_pct * 10_000))
    if not 0.0 <= collar <= 100.0:
        raise ValueError("limit_collar_bps must be between 0 and 100")
    cd = int(src.get("reentry_cooldown_secs", 60))
    if cd < 60 or cd > 86_400:
        raise ValueError("reentry_cooldown_secs must be in [60, 86400]")
    handle_spec = build_venue_order_handle_spec(
        resolved_vp, session=prof.session, trailing_schedule=req_sched
    )
    norm_preset: dict[str, Any] = {
        "symbol": sym,
        "qty": 1,
        "side": out_side,
        "session": prof.session,
        "venue": vu,
        "venue_profile": resolved_vp,
        "order_type": order_type,
        "limit_collar_bps": collar,
        "trailing_schedule": req_sched,
        "auto_reverse": bool(src.get("auto_reverse", True)),
        "auto_reentry": bool(src.get("auto_reentry", True)),
        "reentry_cooldown_secs": cd,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "order_handle_spec": handle_spec,
    }
    if src.get("darkpool_trf_ratio") is not None:
        trf_v = float(src["darkpool_trf_ratio"])
        if not 0.0 <= trf_v <= 1.0:
            raise ValueError("darkpool_trf_ratio must be in [0.0, 1.0]")
        norm_preset["darkpool_trf_ratio"] = round(trf_v, 4)
    for num_key in ("vol_buildup_ratio", "hidden_volume_ratio"):
        if src.get(num_key) is not None:
            nv = float(src[num_key])
            if nv < 0.0:
                raise ValueError(f"{num_key} must be >= 0.0")
            norm_preset[num_key] = round(nv, 4)
    for bool_key in ("unusual_volume_buildup", "peak_confirmed"):
        if src.get(bool_key) is not None:
            norm_preset[bool_key] = bool(src[bool_key])
    for str_key in ("pair_symbol", "setup_archetype", "order_preset_id"):
        if val := str(src.get(str_key) or "").strip():
            norm_preset[str_key] = val.upper() if str_key == "pair_symbol" else val
    if src.get("script_kind") is not None:
        sk = str(src["script_kind"]).strip().lower()
        if sk not in ("indicator", "strategy"):
            raise ValueError(f"Unsupported script_kind: {src['script_kind']!r}")
        norm_preset["script_kind"] = sk
    if staged_path is not None:
        _atomic_write_json(
            Path(staged_path),
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "semi_auto_confirmed": False,
                "presets": [norm_preset],
            },
        )
    return norm_preset


@mcp.tool()
def stage_semi_auto_order_preset(
    payload: dict[str, Any] | None = None,
    symbol: str | None = None,
    side: str | None = None,
    session: str | None = None,
    qty: int = 1,
    preset: dict[str, Any] | None = None,
    order_preset: dict[str, Any] | None = None,
    entry: dict[str, Any] | None = None,
    staged_path: str | None = None,
    watchlist_path: str | None = None,
    venue: str = "IBKR",
    venue_profile: str | None = None,
    live_intake: bool = False,
) -> dict[str, Any]:
    """Validate and stage a semi-auto qty=1 preset (always semi_auto_confirmed=False)."""
    if payload is not None and not isinstance(payload, Mapping):
        return {"ok": False, "error": "payload must be a mapping", "semi_auto_confirmed": False}
    src: dict[str, Any] = {}
    for candidate in (entry, order_preset, preset, payload):
        if isinstance(candidate, Mapping):
            src.update(candidate)
    if symbol is not None:
        src["symbol"] = symbol
    if side is not None:
        src["side"] = side
    if session is not None:
        src["session"] = session
    if venue_profile is not None:
        src["venue_profile"] = venue_profile
    if qty != 1 or "qty" not in src:
        src["qty"] = qty
    try:
        for path in (staged_path, watchlist_path):
            if path is not None and (not path.strip() or Path(path).is_dir()):
                raise ValueError("Expected a nonempty file path")
        if "schema_version" in src:
            req_vp = str(src.get("venue_profile") or "").strip().upper()
            resolved_vp = (
                validate_venue_order_capability(req_vp).venue_id
                if req_vp
                else (
                    "IBKR_PAPER_4002"
                    if venue.strip().upper() == "IBKR"
                    else resolve_alpaca_paper_venue_profile()[0]
                )
            )
            s_path = DEFAULT_STAGED_PATH if staged_path is None else Path(staged_path)
            w_path = DEFAULT_WATCHLIST_PATH if watchlist_path is None else Path(watchlist_path)
            staged = stage_tv_alert_preset(
                src,
                staged_path=s_path,
                watchlist_path=w_path,
                venue=venue,
                live_intake=live_intake,
            )
            staged_with_handle = {
                **staged,
                "venue_profile": resolved_vp,
                "order_handle_spec": build_venue_order_handle_spec(
                    resolved_vp,
                    session=str(staged.get("session") or "RTH"),
                    trailing_schedule=str(staged.get("trailing_schedule") or "baseline"),
                ),
            }
            return {
                "ok": True,
                **staged_with_handle,
                "semi_auto_confirmed": False,
                "preset": staged_with_handle,
                "order_preset": staged_with_handle,
            }
        if live_intake:
            raise ValueError("Live intake requires a complete tier0.tv-alert.v1 payload")
        norm = _stage_minimal_order_preset(src, staged_path=staged_path, venue=venue)
    except (ValueError, TypeError, KeyError, OSError, OverflowError, PermissionError) as exc:
        return {"ok": False, "error": str(exc), "semi_auto_confirmed": False}
    return {
        "ok": True,
        **norm,
        "semi_auto_confirmed": False,
        "order_preset": norm,
        "preset": norm,
    }


def run_smoke_check() -> dict[str, Any]:
    """Verify all 10 FastMCP tools register cleanly and pass offline checks."""
    expected = [
        "check_prewall_pit_session",
        "compute_risk_sized_stop",
        "evaluate_l1_queue_markout",
        "get_engine_backbone_status",
        "get_live_performance_manifest",
        "get_live_watchlist_manifest",
        "inspect_venue_profile",
        "rank_symbols_advisory",
        "run_prewall_native_backbone_parity",
        "stage_semi_auto_order_preset",
    ]

    def _list_tools() -> list[str]:
        return sorted(t.name for t in asyncio.run(mcp.list_tools()))

    try:
        asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=1) as pool:
            tools = pool.submit(_list_tools).result()
    except RuntimeError:
        tools = _list_tools()

    st = get_engine_backbone_status()
    vp = inspect_venue_profile()
    vp_alp1000 = inspect_venue_profile("ALPACA_PAPER_1000")
    stop = compute_risk_sized_stop("100.00", "LONG", "100000")
    pit = check_prewall_pit_session("2026-10-15")
    par = run_prewall_native_backbone_parity()
    with TemporaryDirectory() as tmp:
        staged = stage_semi_auto_order_preset(
            symbol="NVDA",
            side="BUY",
            session="RTH",
            staged_path=str(Path(tmp) / "staged.json"),
        )
        wl_miss = get_live_watchlist_manifest(str(Path(tmp) / "missing_wl.json"))
        pf_miss = get_live_performance_manifest(str(Path(tmp) / "missing_perf.json"))
        l1_miss = evaluate_l1_queue_markout("NVDA", day="2026-10-15", root=tmp)
        rk_miss = rank_symbols_advisory(day="2026-10-15", root=tmp)

    ok = (
        tools == sorted(expected)
        and st["environment"] == "SANDBOX"
        and st["broker_ports_open"] == 0
        and st["live_execution_enabled"] is False
        and vp["allowed"] is True
        and vp_alp1000["allowed"] is True
        and vp_alp1000["max_rpm"] == 1000
        and vp_alp1000["exit_reserve_rpm"] == 100
        and vp_alp1000["live_execution_enabled"] is False
        and vp_alp1000["order_handle_spec"]["trailing_modify_cadence_secs"] >= 1.0
        and int(stop["shares"]) > 0
        and pit["pre_wall_ok"] is True
        and par["overall_ok"] is True
        and staged["ok"] is True
        and staged["semi_auto_confirmed"] is False
        and wl_miss["ok"] is False
        and pf_miss["ok"] is False
        and l1_miss["ok"] is False
        and rk_miss["ok"] is False
    )
    return {
        "ok": ok,
        "tools_count": len(tools),
        "tools": tools,
        "environment": st["environment"],
        "live_execution_enabled": st["live_execution_enabled"],
        "stop_shares": stop["shares"],
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint supporting --smoke/--self-check or stdio MCP transport."""
    ap = argparse.ArgumentParser(description="Tier-0 native NautilusTrader v2 FastMCP server")
    ap.add_argument("--smoke", action="store_true", help="Verify all 10 tools offline and exit")
    ap.add_argument("--self-check", action="store_true", help="Alias for --smoke")
    args = ap.parse_args(argv)
    if args.smoke or args.self_check:
        res = run_smoke_check()
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    mcp.run(transport="stdio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

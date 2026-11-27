"""One authenticated Paper Farm observation and reconciliation cycle for a native scheduler."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from alpaca.common.enums import Sort
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockQuotesRequest
from alpaca.trading.requests import GetOrderByIdRequest, GetOrdersRequest

from tier0_sota import engine, risk
from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    IBKR_PAPER_ACCOUNT,
    PAPER_ALPACA_ENDPOINT,
    AlpacaPaperLane,
    IbkrPaperAdapter,
    PaperFarmCoordinator,
    _atomic_write_json,
    default_manifest_dir,
    ensure_tca_table,
    ensure_wire_table,
    open_bounded_duckdb,
    resolve_alpaca_paper_venue_profile,
    round_lot_up,
    select_situation_lane,
)
from tier0_sota.live.performance import aggregate_paper_farm_performance
from tier0_sota.live.scanner import (
    DEFAULT_PIN_SYMBOLS,
    ExtremeMoverScanner,
    build_live_lane_preset_bundle,
)
from tier0_sota.tv.tickets import refresh_tickets_from_paths

NY = ZoneInfo("America/New_York")
_LEGACY_TWO_PIN_SET = frozenset({"SOXL", "SOXS"})


def _resolve_pin_symbols(raw: str) -> tuple[str, ...]:
    """Upgrade empty or legacy 2-pin CLI overrides to 4-symbol DEFAULT_PIN_SYMBOLS."""
    parsed = tuple(dict.fromkeys(tok.strip().upper() for tok in raw.split(",") if tok.strip()))
    if not parsed or set(parsed) == _LEGACY_TWO_PIN_SET:
        return DEFAULT_PIN_SYMBOLS
    return parsed


def verify_paper_environment() -> None:
    with Path(risk.__file__).open("rb") as kernel:
        if hashlib.file_digest(kernel, "sha256").hexdigest() != (
            "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
        ):
            raise PermissionError("Paper worker risk kernel digest mismatch")
    if (
        os.environ.get("ALPACA_PAPER", "").lower() != "true"
        or os.environ.get("LIVE_EXECUTION_ENABLED", "false").lower() != "false"
        or os.environ.get("IBKR_HOST") != "127.0.0.1"
        or os.environ.get("IBKR_PORT") != "4002"
        or os.environ.get("APCA_API_BASE_URL", PAPER_ALPACA_ENDPOINT).rstrip("/")
        != PAPER_ALPACA_ENDPOINT
    ):
        raise PermissionError("Paper profile or loopback broker isolation check failed")
    for acct in ALPACA_LANE_PROFILES:
        if not os.environ.get(f"{acct}_API_KEY", "").startswith("PK") or not (
            os.environ.get(f"{acct}_SECRET") or os.environ.get(f"{acct}_SECRET_KEY")
        ):
            raise PermissionError(f"Missing account-specific paper credentials for {acct}")


def ibkr_health(adapter: IbkrPaperAdapter) -> bool:
    """Check only the pinned paper socket. TCP alone cannot attest a DU login or equity."""
    if adapter.port != 4002 or not adapter.paper_only or adapter.live_execution_enabled:
        raise PermissionError("IBKR health requires the paper gateway on loopback port 4002")
    reachable = os.environ.get("IBKR_PAPER_TCP_REACHABLE") == "1"
    adapter.write_status(tcp_reachable=reachable, equity=None)
    return reachable


def midpoint_asof(data: Any, symbol: str, at: datetime) -> Decimal | None:
    session = engine.classify_us_equity_session_et(int(at.timestamp() * 1e9))
    quotes = data.get_stock_quotes(
        StockQuotesRequest(
            symbol_or_symbols=symbol,
            start=at - timedelta(seconds=5),
            end=at,
            limit=1,
            sort=Sort.DESC,
            feed=DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP,
        )
    ).data.get(symbol, [])
    if not quotes:
        return None
    quote = quotes[0]
    if (
        not 0 < quote.bid_price <= quote.ask_price
        or not 0 <= (at - quote.timestamp).total_seconds() <= 5
    ):
        return None
    return (Decimal(str(quote.bid_price)) + Decimal(str(quote.ask_price))) / 2


def refresh_markouts(lane: AlpacaPaperLane, data: Any) -> None:
    with open_bounded_duckdb(lane.duckdb_path) as conn:
        pending = conn.execute(
            "SELECT execution_id,symbol,side,fill_price,CAST(ts_utc AS TIMESTAMPTZ) "
            "FROM paper_tca_fills WHERE execution_id IS NOT NULL AND markout_60s_bps IS NULL "
            "AND CAST(ts_utc AS TIMESTAMPTZ) <= now()-INTERVAL '60 seconds'"
        ).fetchall()
    updates: list[tuple[Decimal, str, str]] = []
    for eid, symbol, side, price, filled in pending:
        mid = midpoint_asof(data, symbol, filled + timedelta(seconds=60))
        if mid is not None:
            sign = 1 if side == "BUY" else -1
            updates.append((sign * (mid - price) / price * 10000, lane.account, eid))
    if updates:
        with open_bounded_duckdb(lane.duckdb_path) as conn:
            conn.executemany(
                "UPDATE paper_tca_fills SET markout_60s_bps=? WHERE account=? AND execution_id=?",
                updates,
            )


def exit_context(lane: AlpacaPaperLane, order: Any, decisions: dict, data: Any) -> tuple | None:
    """Prove exit ownership by its parent or replacement ancestry before attributing a fill."""
    broker = lane.trading_client
    parents = [cid for cid, ctx in decisions.items() if ctx[0] == order.symbol]
    if not parents:
        return None
    ancestor = order
    seen = set()
    while ancestor.replaces:
        if ancestor.id in seen:
            raise ValueError("Cyclic paper order replacement chain")
        seen.add(ancestor.id)
        try:
            ancestor = broker.get_order_by_id(ancestor.replaces)
        except APIError as exc:
            if exc.status_code == 404:
                return None
            raise
    for cid in parents:
        try:
            parent = broker.get_order_by_client_id(cid)
            parent = broker.get_order_by_id(parent.id, GetOrderByIdRequest(nested=True))
        except APIError as exc:
            if exc.status_code == 404:
                continue
            raise
        if parent.side == order.side:
            continue
        base_id = f"exit-{parent.id}"
        synthetic = order.client_order_id == base_id or (
            order.client_order_id.startswith(base_id + "-")
            and order.client_order_id.removeprefix(base_id + "-").isdecimal()
        )
        legs = parent.legs or []
        if not synthetic and not any(leg.id in {order.id, ancestor.id, *seen} for leg in legs):
            continue
        if synthetic:
            with open_bounded_duckdb(lane.duckdb_path) as db:
                row = db.execute(
                    "SELECT active_stop FROM paper_entry_decisions WHERE client_order_id=?", [cid]
                ).fetchone()
            decision = row[0] if row else None
        else:
            decision = order.stop_price
        submitted = order.submitted_at
        if decision is None or submitted is None or data is None:
            raise ValueError("Owned exit lacks a decision or arrival benchmark")
        session = engine.classify_us_equity_session_et(int(submitted.timestamp() * 1e9))
        mid = midpoint_asof(data, order.symbol, submitted)
        if mid is None:
            raise ValueError("Owned exit awaits a causal quote at broker submission")
        side = "LONG" if order.side.value == "buy" else "SHORT"
        return (
            order.symbol,
            side,
            Decimal(str(decision)),
            mid,
            session,
            "PROTECTIVE_EXIT",
            Decimal(0),
            None,
            submitted,
        )
    return None


def reconcile_fills(lane: AlpacaPaperLane, data: Any = None) -> int:
    """Replay broker executions against recorded entry decisions, including partial fills."""
    with open_bounded_duckdb(lane.duckdb_path) as conn:
        if ("paper_entry_decisions",) not in conn.execute("SHOW TABLES").fetchall():
            return 0
        decisions = {
            row[0]: row[1:]
            for row in conn.execute(
                "SELECT client_order_id,symbol,side,decision_price,arrival_price,session,"
                "origin,annual_rate,prior_close,decided_at FROM paper_entry_decisions"
            ).fetchall()
        }
        conn.execute(
            "CREATE TABLE IF NOT EXISTS paper_activity_cursor "
            "(account VARCHAR PRIMARY KEY, page_token VARCHAR NOT NULL)"
        )
        cursor = conn.execute(
            "SELECT page_token FROM paper_activity_cursor WHERE account=?", [lane.account]
        ).fetchone()
    if not decisions:
        return 0
    broker = lane.trading_client
    params = {
        "after": min(d[-1] for d in decisions.values()).isoformat(),
        "direction": "asc",
        "page_size": "100",
    }
    if cursor:
        params["page_token"] = cursor[0]
    initial = lane.counters["fills"]
    while activities := broker.get("/account/activities/FILL", params):
        for fill in activities:
            order = broker.get_order_by_id(fill["order_id"])
            context = decisions.get(order.client_order_id)
            if context is None:
                context = exit_context(lane, order, decisions, data)
                if context is None:
                    continue
            sym, side, decision, arrival, session, origin, rate, prior_close, _ = context
            expected = "buy" if side == "LONG" else "sell"
            if (
                fill["symbol"] != sym
                or order.side.value != expected
                or fill["side"].removesuffix("_short") != expected
                or not fill["id"]
            ):
                raise ValueError("Broker execution disagrees with stored entry decision")
            qty = Decimal(fill["qty"])
            cumulative = Decimal(fill["cum_qty"])
            if not 0 < qty <= cumulative:
                raise ValueError("Invalid broker execution quantity")
            if side == "SHORT" and origin not in ("DIRECT_SHORT_ETB", "PROTECTIVE_EXIT"):
                raise ValueError("Short borrow evidence missing")
            # ETB Trading API shares have zero borrow fee. Never infer an HTB rate.
            # Use executed shares for each fill slice; each activity has its own price.
            lane.record_broker_fill(
                execution_id=fill["id"],
                broker_order_id=fill["order_id"],
                symbol=sym,
                side=side,
                order_qty=qty,
                filled_qty=qty,
                decision_price=decision,
                arrival_price=arrival,
                fill_price=Decimal(fill["price"]),
                borrow_tier=(
                    "ETB"
                    if origin == "DIRECT_SHORT_ETB"
                    else "INVERSE_ETF"
                    if origin.startswith("INVERSE_HEDGE_")
                    else "NONE"
                ),
                annual_borrow_rate=rate,
                prior_close=prior_close,
                borrow_qty=round_lot_up(cumulative) - round_lot_up(cumulative - qty),
                session=session,
                ts_utc=fill["transaction_time"],
                protective_stop_mode=(
                    "NATIVE_OTO_STOP" if session == "RTH" else "COLLARED_STOP_LIMIT_SUPERVISED"
                ),
                trade_date=datetime.fromisoformat(fill["transaction_time"]).astimezone(NY).date(),
            )
        token = activities[-1]["id"]
        if params.get("page_token") == token:
            raise RuntimeError("Alpaca returned a repeated activity page")
        params["page_token"] = token
        with open_bounded_duckdb(lane.duckdb_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO paper_activity_cursor VALUES (?,?)", [lane.account, token]
            )
        if len(activities) < 100:
            break
    return lane.counters["fills"] - initial


def _fetch_lane_broker_snapshot(lane: AlpacaPaperLane) -> tuple[Any, list[Any], list[Any]]:
    broker = lane.trading_client
    account = broker.get_account()
    positions = broker.get_all_positions()
    orders = broker.get_orders(GetOrdersRequest(limit=500))
    return account, positions, orders


def refresh_accounts(
    farm: PaperFarmCoordinator,
    now: datetime,
    session: str,
    *,
    publish: bool = True,
    data_client: Any = None,
    record_fuse: bool = True,
) -> dict[str, dict[str, Any]]:
    if farm.lanes.keys() != ALPACA_LANE_PROFILES.keys():
        raise PermissionError("All three Alpaca paper accounts are required")
    lanes_list = list(farm.lanes.values())
    distinct_clients = len({id(lane.trading_client) for lane in lanes_list}) == len(lanes_list)
    if distinct_clients and len(lanes_list) > 1:
        with ThreadPoolExecutor(max_workers=len(lanes_list)) as pool:
            snapshots = list(pool.map(_fetch_lane_broker_snapshot, lanes_list))
    else:
        snapshots = [_fetch_lane_broker_snapshot(lane) for lane in lanes_list]

    ids = set()
    inventory = {}
    owners: dict[str, str] = {}
    shared_data = data_client
    for lane, (account, positions, orders) in zip(lanes_list, snapshots, strict=True):
        if (
            not account.id
            or account.id in ids
            or account.status.value != "ACTIVE"
            or account.trading_blocked is not False
            or account.account_blocked is not False
            or account.trade_suspended_by_user is True
        ):
            raise PermissionError(f"{lane.account} is unavailable, blocked or duplicated")
        ids.add(account.id)
        if len(orders) == 500:
            raise RuntimeError("Cannot attest complete open-order inventory")
        for sym in {o.symbol for o in [*positions, *orders]}:
            if owners.setdefault(sym, lane.account) != lane.account:
                raise PermissionError(f"Overlapping paper exposure for {sym}")
        inventory[lane.account] = {p.symbol: p for p in positions}
        equity, yesterday = Decimal(account.equity), Decimal(account.last_equity)
        lane.start_equity = equity
        lane.breaker = risk.DailyDrawdownBreaker(start_equity=yesterday)
        day_et = now.astimezone(NY).date()
        day = day_et.isoformat()
        if record_fuse:
            with open_bounded_duckdb(lane.duckdb_path) as conn:
                ensure_tca_table(conn)
                ensure_wire_table(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS paper_daily_fuse "
                    "(day DATE PRIMARY KEY, account_id UUID, tripped BOOLEAN)"
                )
                row = conn.execute(
                    "SELECT account_id,tripped FROM paper_daily_fuse ORDER BY day DESC LIMIT 1"
                ).fetchone()
                if row and row[0] != account.id:
                    raise PermissionError(f"{lane.account} broker identity changed")
                lane.breaker.update(realized_pnl=equity - yesterday)
                tripped = conn.execute(
                    "INSERT INTO paper_daily_fuse VALUES (?, ?, ?) ON CONFLICT(day) DO UPDATE "
                    "SET tripped=paper_daily_fuse.tripped OR excluded.tripped RETURNING tripped",
                    (day, account.id, lane.breaker.tripped),
                ).fetchone()
                lane.breaker.tripped = bool(tripped and tripped[0])
        else:
            lane.breaker.update(realized_pnl=equity - yesterday)
            if lane._persisted_daily_fuse_tripped(day_et):
                lane.breaker.tripped = True
        if publish:
            if shared_data is None:
                shared_data = ExtremeMoverScanner.from_env(lane.account).data_client
            reconcile_fills(lane, shared_data)
            refresh_markouts(lane, shared_data)
            lane.write_status(equity=float(equity), session=session, observation=True)
            export_ui_snapshot(lane)
    farm.symbol_lock.reconcile(owners)
    return inventory


def export_ui_snapshot(lane: AlpacaPaperLane) -> None:
    """Publish committed data for DuckDB UI without keeping the writer database attached."""
    out = lane.manifest_dir / "paper_farm_ui"
    out.mkdir(exist_ok=True)
    with open_bounded_duckdb(lane.duckdb_path, read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        for name in ("paper_wire_orders", "paper_tca_fills"):
            if name in tables:
                path = out / f"{lane.account.lower()}_{name}.parquet"
                tmp = path.with_suffix(".parquet.tmp")
                conn.table(name).write_parquet(str(tmp))
                tmp.replace(path)


_INVERSE_HEDGE_SYMBOLS = frozenset({"SOXS", "SQQQ", "SPXS", "SDOW"})


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def reconcile_staged_tv_presets_for_engine(
    manifest_dir: Path,
    farm: PaperFarmCoordinator,
    session: str,
    *,
    movers_doc: dict[str, Any] | None = None,
    tca_allows_entry: bool | None = None,
    active_positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Fail-closed coordination of staged TV presets vs Paper Farm, breakers & Nautilus SANDBOX.

    Never auto-submits any order: always emits `semi_auto_confirmed=False`, `paper_only=True`,
    `live_execution_enabled=False`, and `auto_submit_enabled=False`.
    """
    from tier0_sota.tv.bridge import refuse_live_endpoints

    pack_dir = manifest_dir / "tv_sota_pack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    staged_path = pack_dir / "staged_tv_presets.json"
    if not staged_path.is_file():
        fallback = manifest_dir / "staged_tv_presets.json"
        if fallback.is_file():
            staged_path = fallback

    exec_doc = _read_json_dict(manifest_dir / "paper_farm_execution_status.json")
    ibkr_doc = _read_json_dict(manifest_dir / "ibkr_du_4002_live_automation_status.json")
    owners = dict(farm.symbol_lock.snapshot())

    if active_positions is not None:
        positions_out = [dict(p) for p in active_positions if isinstance(p, dict)]
    else:
        raw_pos = exec_doc.get("active_positions")
        positions_out = (
            [dict(p) for p in raw_pos if isinstance(p, dict)] if isinstance(raw_pos, list) else []
        )
    known_pos_syms = {str(p.get("symbol") or "").strip().upper() for p in positions_out}
    for sym, acct in sorted(owners.items()):
        if sym and sym not in known_pos_syms:
            positions_out.append(
                {
                    "symbol": sym,
                    "account": acct,
                    "lane_id": (
                        ALPACA_LANE_PROFILES[acct]["lane_id"]
                        if acct in ALPACA_LANE_PROFILES
                        else "IBKR_SMART_24X5"
                    ),
                    "venue_profile": (
                        farm.lanes[acct].venue_profile if acct in farm.lanes else "IBKR_PAPER_4002"
                    ),
                    "active_session": session,
                    "source": "symbol_lock",
                }
            )

    venue_handles = farm.venue_order_handles(session=session)
    breakers_tripped = sorted(
        [acct for acct, lane in farm.lanes.items() if not lane.breaker.allow_entry()]
        + ([IBKR_PAPER_ACCOUNT] if not farm.ibkr.breaker.allow_entry() else [])
    )
    tca_ok = True if tca_allows_entry is None else bool(tca_allows_entry)
    ibkr_tcp_ok = ibkr_doc.get("tcp_reachable") is True
    ibkr_ready = ibkr_doc.get("ready") is True

    movers_by_sym: dict[str, dict[str, Any]] = {}
    if isinstance(movers_doc, dict) and isinstance(movers_doc.get("entries"), list):
        for entry in movers_doc["entries"]:
            if isinstance(entry, dict) and entry.get("symbol"):
                movers_by_sym[str(entry["symbol"]).strip().upper()] = entry

    staged_doc = _read_json_dict(staged_path)
    manifest_confirmed_violation = staged_doc.get("semi_auto_confirmed") is True
    raw_presets_val = staged_doc.get("presets")
    raw_presets: list[Any] = raw_presets_val if isinstance(raw_presets_val, list) else []

    decisions: list[dict[str, Any]] = []
    for raw in raw_presets:
        if not isinstance(raw, dict):
            continue
        reasons: list[str] = []
        if manifest_confirmed_violation:
            reasons.append("MANIFEST_SEMI_AUTO_CONFIRMED_TRUE_REFUSED")
        try:
            refuse_live_endpoints(raw)
        except (ValueError, PermissionError):
            reasons.append("LIVE_ENDPOINT_REFUSED")

        qty_raw = raw.get("qty", 1)
        if isinstance(qty_raw, bool) or not isinstance(qty_raw, int) or qty_raw != 1:
            reasons.append("QTY_MUST_BE_1")
        if raw.get("semi_auto_confirmed") is not False:
            reasons.append("SEMI_AUTO_CONFIRMED_MUST_BE_FALSE")

        sym = str(raw.get("symbol") or "").strip().upper()
        if not sym:
            reasons.append("MISSING_SYMBOL")
        raw_side = str(raw.get("side") or "BUY").strip().upper()
        route_side: risk.Side = "SHORT" if raw_side in ("SELL", "SHORT") else "LONG"
        if raw_side not in ("BUY", "SELL", "LONG", "SHORT"):
            reasons.append("INVALID_SIDE")
        if session == "CLOSED":
            reasons.append("SESSION_CLOSED")
        if not tca_ok:
            reasons.append("TCA_RECALIBRATION_REQUIRED")

        mover = movers_by_sym.get(sym, {})
        natr_raw = raw.get("natr_pct") or mover.get("natr_pct") or "0.015"
        try:
            natr_d = Decimal(str(natr_raw))
            if not natr_d.is_finite() or natr_d <= 0:
                natr_d = Decimal("0.015")
        except Exception:  # noqa: BLE001
            natr_d = Decimal("0.015")
        sched_name, sp_map = engine.select_pooled_trailing_schedule(natr_d)
        prof = engine.get_session_protection_profile(session, sp_map)
        rvol_val = (
            raw.get("rvol")
            if raw.get("rvol") is not None
            else (
                mover.get("rvol_normalized")
                if mover.get("rvol_normalized") is not None
                else mover.get("rvol")
            )
        )

        req_vp = str(raw.get("venue_profile") or "").strip().upper()
        req_venue = str(raw.get("venue") or "").strip().upper()
        resolved_vp: str | None = None
        if req_vp:
            if req_vp not in engine.EXECUTION_VENUE_PROFILES:
                reasons.append("UNKNOWN_VENUE_PROFILE")
            else:
                resolved_vp = req_vp
                try:
                    engine.validate_venue_order_capability(req_vp)
                except PermissionError:
                    reasons.append("HOLD_LIVE_PROFILE_REFUSED")
                except ValueError:
                    reasons.append("VENUE_CAPABILITY_REFUSED")
        elif req_venue == "IBKR":
            resolved_vp = "IBKR_PAPER_4002"
        else:
            resolved_vp = str(venue_handles["alpaca_venue_profile"])

        if (
            resolved_vp
            and resolved_vp.startswith("ALPACA")
            and route_side == "SHORT"
            and session == "OVERNIGHT"
        ):
            reasons.append("ALPACA_OVERNIGHT_SHORT_UNSUPPORTED_ROUTE_IBKR_OR_INVERSE")

        if sym and sym in owners:
            reasons.append(f"SYMBOL_LOCKED_BY_{owners[sym]}")

        is_inverse = sym in _INVERSE_HEDGE_SYMBOLS
        if is_inverse and route_side == "LONG":
            origin = f"INVERSE_HEDGE_{sym}"
        elif route_side == "SHORT":
            origin = "DIRECT_SHORT_ETB"
        else:
            origin = "TV_HALF_AUTO_STAGED"

        target_account: str | None = None
        target_lane_id: str | None = None
        if resolved_vp == "IBKR_PAPER_4002":
            target_account = IBKR_PAPER_ACCOUNT
            target_lane_id = "IBKR_SMART_24X5"
            if not farm.ibkr.breaker.allow_entry():
                reasons.append("IBKR_DAILY_DRAWDOWN_BREAKER_TRIPPED")
            if not ibkr_ready and not ibkr_tcp_ok:
                reasons.append("IBKR_PAPER_4002_SOCKET_UNAVAILABLE")
        elif resolved_vp and resolved_vp.startswith("ALPACA_PAPER"):
            lane = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=natr_d,
                rvol_normalized=rvol_val,
            )
            if lane is None:
                reasons.append("ALL_ALPACA_LANES_BREAKER_TRIPPED_OR_UNAVAILABLE")
            else:
                target_account = lane.account
                target_lane_id = lane.profile["lane_id"]

        handle_spec: dict[str, Any] | None = None
        sandbox_parity: dict[str, Any] = {
            "environment": "SANDBOX",
            "parity_ok": False,
            "session_order_mode": prof.order_mode,
            "limit_collar_bps": float(prof.limit_collar_bps),
            "outside_rth": bool(prof.outside_rth),
            "trailing_schedule": sched_name,
        }
        if resolved_vp is not None:
            handle_spec = engine.build_venue_order_handle_spec(
                resolved_vp, session=session, trailing_schedule=sched_name
            )
            re_cfg = engine.build_venue_risk_engine_config(resolved_vp)
            ref_raw = raw.get("trigger_price") or mover.get("last_price")
            rule612_stop: float | None = None
            stop_within_cap = True
            if ref_raw is not None and not isinstance(ref_raw, bool):
                try:
                    ref_d = Decimal(str(ref_raw))
                    if ref_d.is_finite() and ref_d > 0:
                        stop_d = risk.compute_capped_stop_price(
                            ref_d, route_side, max_loss_pct=prof.initial_max_loss_pct
                        )
                        loss_pct = abs(ref_d - stop_d) / ref_d
                        stop_within_cap = Decimal("0") < loss_pct <= risk.MAX_STOP_LOSS_PCT
                        rule612_stop = float(stop_d)
                except Exception:  # noqa: BLE001
                    stop_within_cap = False
            if not stop_within_cap:
                reasons.append("RULE_612_STOP_CAP_VIOLATION")
            sandbox_parity = {
                "environment": "SANDBOX",
                "parity_ok": (
                    not bool(re_cfg.bypass)
                    and stop_within_cap
                    and bool(handle_spec["executable_now"])
                ),
                "risk_engine_bypass": bool(re_cfg.bypass),
                "max_order_submit_rate": str(re_cfg.max_order_submit_rate),
                "max_order_modify_rate": str(re_cfg.max_order_modify_rate),
                "session_order_mode": prof.order_mode,
                "limit_collar_bps": float(prof.limit_collar_bps),
                "outside_rth": bool(prof.outside_rth),
                "trailing_schedule": sched_name,
                "rule_612_stop_price": rule612_stop,
                "stop_loss_pct_within_cap": stop_within_cap,
            }

        ready_confirm = len(reasons) == 0
        alert_id = str(raw.get("alert_id") or "").strip()
        decisions.append(
            {
                "alert_id": alert_id or None,
                "symbol": sym,
                "side": route_side,
                "qty": 1,
                "session": session,
                "origin": origin,
                "venue_profile": resolved_vp,
                "target_account": target_account,
                "target_lane_id": target_lane_id,
                "semi_auto_confirmed": False,
                "auto_submitted": False,
                "ready_for_operator_confirm": ready_confirm,
                "discipline_gate": ("READY_FOR_OPERATOR_CONFIRM" if ready_confirm else reasons[0]),
                "blocking_reasons": reasons,
                "order_handle_spec": handle_spec,
                "nautilus_sandbox_parity": sandbox_parity,
            }
        )

    ready_count = sum(1 for d in decisions if d["ready_for_operator_confirm"])
    out_path = pack_dir / "tv_engine_coordination_manifest.json"
    coord_doc: dict[str, Any] = {
        "schema_version": "tier0.tv-engine-coordination.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "active_session": session,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "auto_submit_enabled": False,
        "staged_source_path": str(staged_path),
        "tca_calibration_ok": tca_ok,
        "ibkr_tcp_reachable": ibkr_tcp_ok,
        "ibkr_gateway_ready": ibkr_ready,
        "breakers_tripped_accounts": breakers_tripped,
        "any_breaker_tripped": bool(breakers_tripped),
        "symbol_lock_snapshot": owners,
        "active_positions": positions_out,
        "active_positions_count": len(positions_out),
        "venue_order_handles": venue_handles,
        "staged_alerts_evaluated": len(decisions),
        "ready_for_operator_confirm_count": ready_count,
        "blocked_count": len(decisions) - ready_count,
        "coordination_decisions": decisions,
        "confirm_command_template": (
            "uv run --no-sync python -m tier0_sota.live.runner "
            "--confirm-tv-alert <ALERT_ID> --qty 1 --dry-run"
        ),
    }
    _atomic_write_json(out_path, coord_doc)
    return coord_doc


def run_cycle(manifest_dir: Path, *, top_k: int, pins: tuple[str, ...]) -> dict[str, Any]:
    verify_paper_environment()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    # DuckDB's exclusive writer lock serializes scheduler invocations and manual cycles.
    with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
        farm = PaperFarmCoordinator(manifest_dir)
        reachable = ibkr_health(farm.ibkr)
        now = datetime.now(UTC)
        session = engine.classify_us_equity_session_et(int(now.timestamp() * 1e9))
        scanner = ExtremeMoverScanner.from_env(pin_symbols=pins)
        refresh_accounts(farm, now, session, data_client=scanner.data_client)
        movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
        scan = scanner.scan(
            top_k=top_k,
            output_path=movers_path,
        )
        from tier0_sota.live.execution import cost_calibration_allows_entry

        tca_ok = cost_calibration_allows_entry(manifest_dir)
        bundle_path = manifest_dir / "live_lane_order_preset_bundle.json"
        build_live_lane_preset_bundle(
            scan,
            lanes=farm.lanes,
            output_path=bundle_path,
            tca_allows_entry=tca_ok,
        )
        coord = reconcile_staged_tv_presets_for_engine(
            manifest_dir,
            farm,
            session,
            movers_doc=scan,
            tca_allows_entry=tca_ok,
        )
        tickets = refresh_tickets_from_paths(
            movers_path=movers_path,
            bundle_path=bundle_path,
            out_dir=manifest_dir / "tv_sota_pack",
            venue="ALPACA",
        )
        otlp_url = (
            os.environ.get("TIER0_OTLP_METRICS_URL", "http://127.0.0.1:4318/v1/metrics").strip()
            or None
        )
        prom_path = manifest_dir / "paper_farm_metrics.prom"
        perf = aggregate_paper_farm_performance(
            manifest_dir,
            prom_output_path=prom_path,
            otlp_endpoint=otlp_url,
        )
        tv_meta = perf.get("tv_half_auto") or {}
        venue_handles = coord.get("venue_order_handles") or {}
        result = {
            "session": session,
            "paper_only": True,
            "live_execution_enabled": False,
            "accounts_authenticated": len(farm.lanes),
            "ibkr_tcp_reachable": reachable,
            "scan_rows": scan["ranked_count"],
            "tickets": tickets["staged_count"],
            "standby_presets": len(tickets.get("standby_presets") or ()),
            "unusual_vol_count": int(tv_meta.get("unusual_vol_count") or 0),
            "darkpool_hits_count": int(tv_meta.get("darkpool_hits_count") or 0),
            "tv_coordination_evaluated": int(coord.get("staged_alerts_evaluated") or 0),
            "tv_coordination_ready": int(coord.get("ready_for_operator_confirm_count") or 0),
            "active_positions_count": int(coord.get("active_positions_count") or 0),
            "combined_max_rpm": int(venue_handles.get("combined_max_rpm") or 0),
            "tca_fills": perf["summary"]["tca_fills_count"],
            "paper_live_deployed": perf["paper_live_deployed"],
            "prom_metrics_written": prom_path.is_file(),
        }
        _atomic_write_json(
            manifest_dir / "paper_farm_live_auto_entry_status.json",
            {
                **result,
                "generated_at_utc": perf["generated_at_utc"],
                "macro_regime": scan.get("macro_regime") or {},
                "tv_half_auto": tv_meta,
                "tv_engine_coordination": {
                    "schema_version": coord["schema_version"],
                    "staged_alerts_evaluated": coord["staged_alerts_evaluated"],
                    "ready_for_operator_confirm_count": coord["ready_for_operator_confirm_count"],
                    "blocked_count": coord["blocked_count"],
                    "active_positions_count": coord["active_positions_count"],
                    "any_breaker_tripped": coord["any_breaker_tripped"],
                    "semi_auto_confirmed": False,
                    "auto_submit_enabled": False,
                },
                "venue_order_handles": venue_handles,
                "accounts": {
                    acct: {
                        "venue": "ALPACA" if acct in farm.lanes else "IBKR",
                        "venue_profile": metrics["lane_id"],
                        "execution_venue_profile": (
                            farm.lanes[acct].venue_profile
                            if acct in farm.lanes
                            else "IBKR_PAPER_4002"
                        ),
                        "rate_limit_rpm": (
                            farm.lanes[acct].rate_budget.max_rpm
                            if acct in farm.lanes
                            else farm.ibkr.rate_budget_max_rpm
                        ),
                        "exit_reserve_rpm": (
                            farm.lanes[acct].rate_budget.exit_reserve_rpm
                            if acct in farm.lanes
                            else farm.ibkr.rate_budget.exit_reserve_rpm
                        ),
                        "ready": metrics["worker_verified"] and session != "CLOSED",
                        "status": (
                            "Paper account authenticated · " + session
                            if acct in farm.lanes
                            else "DU login unverified"
                            if reachable
                            else "Paper socket unavailable"
                        ),
                    }
                    for acct, metrics in perf["per_lane"].items()
                },
            },
        )
        return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest-dir", type=Path, default=default_manifest_dir())
    p.add_argument("--top-k", type=int, default=15)
    p.add_argument("--pin-symbols", default=",".join(DEFAULT_PIN_SYMBOLS))
    p.add_argument("--tv-qty", "--qty", dest="tv_qty", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--alpaca-rpm",
        default=None,
        help="Optional Alpaca Paper RPM cap (200|1000|ALPACA_PAPER_200|ALPACA_PAPER_1000)",
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--observe", action="store_true")
    mode.add_argument("--confirm-paper", action="store_true")
    mode.add_argument("--confirm-tv-alert", metavar="ALERT_ID")
    mode.add_argument("--ibkr-health", action="store_true")
    args = p.parse_args()
    if args.alpaca_rpm is not None:
        _, resolved_rpm = resolve_alpaca_paper_venue_profile(
            env={"TIER0_ALPACA_RPM": str(args.alpaca_rpm)}
        )
        os.environ["TIER0_ALPACA_RPM"] = str(resolved_rpm)
    if args.confirm_paper:
        from tier0_sota.live.execution import run_execution

        run_execution(args.manifest_dir)
        return
    if args.confirm_tv_alert:
        from tier0_sota.live.execution import submit_confirmed_tv_preset_paper

        receipt = submit_confirmed_tv_preset_paper(
            args.manifest_dir,
            alert_id=args.confirm_tv_alert,
            qty=args.tv_qty,
            dry_run=args.dry_run,
        )
        print(json.dumps(receipt, indent=2))
        return
    if args.ibkr_health:
        reachable = ibkr_health(IbkrPaperAdapter(args.manifest_dir))
        print(
            json.dumps({"paper_port": 4002, "tcp_reachable": reachable, "du_login_verified": False})
        )
        raise SystemExit(0 if reachable else 1)
    print(
        json.dumps(
            run_cycle(
                args.manifest_dir,
                top_k=args.top_k,
                pins=_resolve_pin_symbols(args.pin_symbols),
            )
        )
    )


if __name__ == "__main__":
    main()

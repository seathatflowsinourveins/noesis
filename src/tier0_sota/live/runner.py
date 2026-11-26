"""One authenticated Paper Farm observation and reconciliation cycle for a native scheduler."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
from alpaca.common.enums import Sort
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockQuotesRequest
from alpaca.trading.requests import GetOrderByIdRequest, GetOrdersRequest

from tier0_sota import engine, risk
from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    PAPER_ALPACA_ENDPOINT,
    AlpacaPaperLane,
    IbkrPaperAdapter,
    PaperFarmCoordinator,
    _atomic_write_json,
    default_manifest_dir,
    ensure_tca_table,
    ensure_wire_table,
    round_lot_up,
)
from tier0_sota.live.performance import aggregate_paper_farm_performance
from tier0_sota.live.scanner import DEFAULT_PIN_SYMBOLS, ExtremeMoverScanner
from tier0_sota.tv.tickets import refresh_tickets_from_paths

NY = ZoneInfo("America/New_York")


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
    with duckdb.connect(str(lane.duckdb_path)) as conn:
        pending = conn.execute(
            "SELECT execution_id,symbol,side,fill_price,CAST(ts_utc AS TIMESTAMPTZ) "
            "FROM paper_tca_fills WHERE execution_id IS NOT NULL AND markout_60s_bps IS NULL "
            "AND CAST(ts_utc AS TIMESTAMPTZ) <= now()-INTERVAL '60 seconds'"
        ).fetchall()
    for eid, symbol, side, price, filled in pending:
        mid = midpoint_asof(data, symbol, filled + timedelta(seconds=60))
        if mid is not None:
            sign = 1 if side == "BUY" else -1
            with duckdb.connect(str(lane.duckdb_path)) as conn:
                conn.execute(
                    "UPDATE paper_tca_fills SET markout_60s_bps=? "
                    "WHERE account=? AND execution_id=?",
                    [sign * (mid - price) / price * 10000, lane.account, eid],
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
        ancestor = broker.get_order_by_id(ancestor.replaces)
    for cid in parents:
        parent = broker.get_order_by_client_id(cid)
        parent = broker.get_order_by_id(parent.id, GetOrderByIdRequest(nested=True))
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
            with duckdb.connect(str(lane.duckdb_path)) as db:
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
    with duckdb.connect(str(lane.duckdb_path)) as conn:
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
        with duckdb.connect(str(lane.duckdb_path)) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO paper_activity_cursor VALUES (?,?)", [lane.account, token]
            )
        if len(activities) < 100:
            break
    return lane.counters["fills"] - initial


def refresh_accounts(
    farm: PaperFarmCoordinator, now: datetime, session: str, *, publish: bool = True
) -> dict[str, dict[str, Any]]:
    if farm.lanes.keys() != ALPACA_LANE_PROFILES.keys():
        raise PermissionError("All three Alpaca paper accounts are required")
    ids = set()
    inventory = {}
    owners: dict[str, str] = {}
    for lane in farm.lanes.values():
        broker = lane.trading_client
        account = broker.get_account()
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
        positions = broker.get_all_positions()
        orders = broker.get_orders(GetOrdersRequest(limit=500))
        if len(orders) == 500:
            raise RuntimeError("Cannot attest complete open-order inventory")
        for sym in {o.symbol for o in [*positions, *orders]}:
            if owners.setdefault(sym, lane.account) != lane.account:
                raise PermissionError(f"Overlapping paper exposure for {sym}")
        inventory[lane.account] = {p.symbol: p for p in positions}
        equity, yesterday = Decimal(account.equity), Decimal(account.last_equity)
        lane.start_equity = equity
        lane.breaker = risk.DailyDrawdownBreaker(start_equity=yesterday)
        day = now.astimezone(NY).date().isoformat()
        with duckdb.connect(str(lane.duckdb_path)) as conn:
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
        if publish:
            data = ExtremeMoverScanner.from_env(lane.account).data_client
            reconcile_fills(lane, data)
            refresh_markouts(lane, data)
            lane.write_status(equity=float(equity), session=session, observation=True)
            export_ui_snapshot(lane)
    farm.symbol_lock.reconcile(owners)
    return inventory


def export_ui_snapshot(lane: AlpacaPaperLane) -> None:
    """Publish committed data for DuckDB UI without keeping the writer database attached."""
    out = lane.manifest_dir / "paper_farm_ui"
    out.mkdir(exist_ok=True)
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        for name in ("paper_wire_orders", "paper_tca_fills"):
            if name in tables:
                path = out / f"{lane.account.lower()}_{name}.parquet"
                tmp = path.with_suffix(".parquet.tmp")
                conn.table(name).write_parquet(str(tmp))
                tmp.replace(path)


def run_cycle(manifest_dir: Path, *, top_k: int, pins: tuple[str, ...]) -> dict[str, Any]:
    verify_paper_environment()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    # DuckDB's exclusive writer lock serializes scheduler invocations and manual cycles.
    with duckdb.connect(str(manifest_dir / "paper_farm_runner.duckdb")):
        farm = PaperFarmCoordinator(manifest_dir)
        reachable = ibkr_health(farm.ibkr)
        now = datetime.now(UTC)
        session = engine.classify_us_equity_session_et(int(now.timestamp() * 1e9))
        refresh_accounts(farm, now, session)
        movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
        scan = ExtremeMoverScanner.from_env(pin_symbols=pins).scan(
            top_k=top_k,
            output_path=movers_path,
        )
        tickets = refresh_tickets_from_paths(
            movers_path=movers_path,
            out_dir=manifest_dir / "tv_sota_pack",
            venue="ALPACA",
        )
        perf = aggregate_paper_farm_performance(
            manifest_dir,
            otlp_endpoint="http://127.0.0.1:4318/v1/metrics",
        )
        result = {
            "session": session,
            "paper_only": True,
            "live_execution_enabled": False,
            "accounts_authenticated": len(farm.lanes),
            "ibkr_tcp_reachable": reachable,
            "scan_rows": scan["ranked_count"],
            "tickets": tickets["staged_count"],
            "tca_fills": perf["summary"]["tca_fills_count"],
            "paper_live_deployed": perf["paper_live_deployed"],
        }
        _atomic_write_json(
            manifest_dir / "paper_farm_live_auto_entry_status.json",
            {
                **result,
                "generated_at_utc": perf["generated_at_utc"],
                "accounts": {
                    acct: {
                        "venue": "ALPACA" if acct in farm.lanes else "IBKR",
                        "venue_profile": metrics["lane_id"],
                        "rate_limit_rpm": (
                            farm.lanes[acct].rate_budget.max_rpm
                            if acct in farm.lanes
                            else farm.ibkr.rate_budget_max_rpm
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
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--observe", action="store_true")
    mode.add_argument("--confirm-paper", action="store_true")
    mode.add_argument("--ibkr-health", action="store_true")
    args = p.parse_args()
    if args.confirm_paper:
        from tier0_sota.live.execution import run_execution

        run_execution(args.manifest_dir)
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
                pins=tuple(args.pin_symbols.split(",")),
            )
        )
    )


if __name__ == "__main__":
    main()

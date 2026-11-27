"""Alpaca paper order management driven by the native SIP and BOATS trade streams."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetOrderByIdRequest, LimitOrderRequest, ReplaceOrderRequest

from tier0_sota import engine, risk
from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    RTH_ENTRY_COLLAR_PCT,
    AlpacaPaperLane,
    PaperFarmCoordinator,
    _atomic_write_json,
    _duckdb_filename,
    open_bounded_duckdb,
    select_short_or_inverse_fallback,
    select_situation_lane,
)
from tier0_sota.live.runner import refresh_accounts, verify_paper_environment
from tier0_sota.live.scanner import (
    ExtremeMoverScanner,
    lookup_alpaca_paper_borrow,
    resolve_alpaca_env_creds,
)

LOG = logging.getLogger(__name__)
TERMINAL = {"filled", "expired", "canceled", "rejected"}
MIN_LIQUID_ENTRY_PRICE = Decimal("10.00")
MAX_RTH_QUOTED_SPREAD_BPS = Decimal("12")
MAX_EXT_QUOTED_SPREAD_BPS = Decimal("35")
_INVERSE_HEDGE_SYMBOLS = frozenset({"SOXS", "SQQQ", "SPXS", "SDOW"})


def quote_microstructure_allows_entry(bid: Decimal, ask: Decimal, session: str) -> bool:
    """Reject sub-$10 tick-floor traps and wide-spread quotes that breach the 2x TCA envelope."""
    if not (bid.is_finite() and ask.is_finite() and Decimal("0") < bid <= ask):
        return False
    mid = (bid + ask) / Decimal("2")
    if mid < MIN_LIQUID_ENTRY_PRICE:
        return False
    spread_bps = ((ask - bid) / mid) * Decimal("10000")
    max_spread = MAX_RTH_QUOTED_SPREAD_BPS if session == "RTH" else MAX_EXT_QUOTED_SPREAD_BPS
    return spread_bps <= max_spread


def cost_calibration_allows_entry(manifest_dir: Path) -> bool:
    """Honor the published Dagster reconciliation, including an adverse measured cost."""
    path = manifest_dir / "tca_calibration_manifest.json"
    if not path.exists():
        for account in ALPACA_LANE_PROFILES:
            ledger = manifest_dir / _duckdb_filename(account)
            if ledger.is_file():
                with open_bounded_duckdb(ledger, read_only=True) as db:
                    if ("paper_tca_fills",) in db.execute("SHOW TABLES").fetchall() and db.execute(
                        "SELECT 1 FROM paper_tca_fills LIMIT 1"
                    ).fetchone():
                        return False
        return True
    receipt = json.loads(path.read_text("utf-8"))
    if not isinstance(receipt, dict):
        return False
    if (
        receipt.get("schema_version") != "tier0.tca-calibration-manifest.v1"
        or receipt.get("paper_only") is not True
        or receipt.get("live_execution_enabled") is not False
        or receipt.get("duckdb_errors") != []
        or receipt.get("liquid_ge10_within_2x_envelope") is False
    ):
        return False
    if receipt.get("parity_within_2x_envelope") is True:
        return True
    liquid_rows = receipt.get("liquid_ge10_tca_rows")
    min_liquid_px = receipt.get("min_liquid_entry_price_usd")
    return (
        receipt.get("liquid_ge10_within_2x_envelope") is True
        and not isinstance(liquid_rows, bool)
        and isinstance(liquid_rows, int)
        and liquid_rows > 0
        and not isinstance(min_liquid_px, bool)
        and isinstance(min_liquid_px, (int, float))
        and Decimal(str(min_liquid_px)).is_finite()
        and Decimal(str(min_liquid_px)) >= MIN_LIQUID_ENTRY_PRICE
    )


def _row_rvol(row: dict[str, Any]) -> Any:
    return row.get("rvol_normalized") if row.get("rvol_normalized") is not None else row.get("rvol")


@dataclass
class Guard:
    lane: AlpacaPaperLane
    client_id: str
    symbol: str
    side: risk.Side
    initial: Decimal
    stop: Decimal
    natr: Decimal
    entered_session: str
    triggered: bool
    parent: Any
    child: Any = None
    checked: float = 0
    last_exit_check: float = 0
    reverse_flatten: bool = False

    def save(self) -> None:
        with open_bounded_duckdb(self.lane.duckdb_path) as db:
            db.execute(
                "UPDATE paper_entry_decisions SET active_stop=?,exit_triggered=? "
                "WHERE client_order_id=?",
                (self.stop, self.triggered, self.client_id),
            )

    def fetch(self) -> None:
        broker = self.lane.trading_client
        self.checked = time.monotonic()
        self.parent = broker.get_order_by_id(self.parent.id, GetOrderByIdRequest(nested=True))
        self.child = next((leg for leg in self.parent.legs or [] if leg.stop_price), None)
        visited = set()
        while self.child and self.child.replaced_by:
            if self.child.id in visited:
                raise RuntimeError("Cyclic broker replacement chain")
            visited.add(self.child.id)
            self.child = broker.get_order_by_id(self.child.replaced_by)
        if self.child:
            if self.child.symbol != self.symbol or self.child.side.value != (
                "sell" if self.side == "LONG" else "buy"
            ):
                raise PermissionError("Protective child disagrees with its owned parent")
            better = max if self.side == "LONG" else min
            self.stop = better(self.stop, Decimal(self.child.stop_price))

    def on_price(self, price: Decimal, session: str) -> None:
        """Tighten a native stop or supervise a collared extended-session exit."""
        long = self.side == "LONG"
        if (
            self.parent.status.value not in TERMINAL
            or self.child
            and self.child.status.value in {"held", "pending_replace", "pending_new"}
        ) and (
            time.monotonic() - self.checked >= 1
            and self.lane.rate_budget.try_consume(time.time(), is_exit=True)
        ):
            self.fetch()
        breached = price <= self.stop if long else price >= self.stop
        force_exit = self.reverse_flatten
        filled = Decimal(self.parent.filled_qty or "0")
        native = session == "RTH" and self.child and self.child.status.value in {"new", "accepted"}
        if not filled and not breached and not force_exit:
            return
        _, schedule = engine.select_pooled_trailing_schedule(self.natr)
        profile = engine.get_session_protection_profile(session, schedule)
        if not breached and not self.triggered and not force_exit:
            if not self.parent.filled_avg_price:
                return
            desired, _, _ = engine.compute_progressive_trailing_stop(
                entry_price=Decimal(self.parent.filled_avg_price),
                side=self.side,
                favorable_extreme=price,
                current_stop=self.stop,
                initial_stop=self.initial,
                natr_pct=self.natr,
                profile=profile,
            )
            if desired == self.stop and (not native or Decimal(self.child.stop_price) == desired):
                return
            if native:
                if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
                    return
                self.child = self.lane.trading_client.replace_order_by_id(
                    self.child.id,
                    ReplaceOrderRequest(stop_price=float(desired)),
                )
                if self.child.status.value == "rejected":
                    raise RuntimeError("Broker rejected protective stop replacement")
                if self.child.status.value not in {"new", "accepted"}:
                    return
                desired = Decimal(self.child.stop_price)
            elif (
                session == "RTH"
                and self.entered_session == "RTH"
                and self.child
                and self.child.status.value not in TERMINAL
            ):
                return  # A pending replacement has no confirmed new stop price yet.
            self.stop = desired
            self.save()
            return

        if (
            not force_exit
            and native
            and (
                Decimal(self.child.stop_price) >= self.stop
                if long
                else Decimal(self.child.stop_price) <= self.stop
            )
        ):
            return  # The broker owns this trigger.
        if not self.triggered:
            self.triggered = True
            self.save()
        if time.monotonic() - self.last_exit_check < 1:
            return
        if not self.lane.rate_budget.try_consume(time.time(), is_exit=True):
            return
        self.last_exit_check = time.monotonic()
        self.fetch()
        broker = self.lane.trading_client
        # Wait for the broker to release reserved shares and settle late partial fills.
        for order in (self.parent, self.child):
            if order and order.status.value not in TERMINAL:
                if (
                    order is self.child
                    and not force_exit
                    and session == "RTH"
                    and order.status.value in {"pending_replace", "pending_new"}
                ):
                    return
                if order.status.value != "pending_cancel":
                    broker.cancel_order_by_id(order.id)
                return
        if Decimal(self.parent.filled_qty or "0") == 0 or (
            self.child and self.child.status.value == "filled"
        ):
            return
        covered = Decimal(self.child.filled_qty or "0") if self.child else Decimal(0)
        attempt = 0
        while True:
            base_id = f"exit-{self.parent.id}"
            exit_client_id = base_id if attempt == 0 else f"{base_id}-{attempt}"
            try:
                existing = broker.get_order_by_client_id(exit_client_id)
            except APIError as exc:
                if exc.status_code != 404:
                    raise
                break
            if existing.status.value not in TERMINAL:
                return
            if existing.status.value == "rejected":
                raise RuntimeError("Broker rejected the supervised exit")
            covered += Decimal(existing.filled_qty or "0")
            if covered >= Decimal(self.parent.filled_qty):
                return
            attempt += 1
        try:
            position = broker.get_open_position(self.symbol)
        except APIError as exc:
            if exc.status_code != 404:
                raise
            return
        exposure = Decimal(position.qty)
        if (exposure > 0) != long:
            return
        qty = min(
            Decimal(self.parent.filled_qty) - covered,
            abs(exposure),
            abs(Decimal(position.qty_available)),
        )
        if qty <= 0:
            return
        exit_collar = profile.limit_collar_pct if session != "RTH" else RTH_ENTRY_COLLAR_PCT
        limit = engine.compute_collared_limit_price(price, self.side, exit_collar)
        broker.submit_order(
            LimitOrderRequest(
                symbol=self.symbol,
                qty=float(qty),
                side=OrderSide.SELL if long else OrderSide.BUY,
                limit_price=float(limit),
                time_in_force=TimeInForce.DAY,
                extended_hours=session != "RTH",
                client_order_id=exit_client_id,
            )
        )
        if force_exit:
            self.lane.counters["reverses"] += 1


def guard_should_flatten_for_flip(
    guard: Guard,
    *,
    market_regime: int,
    rows: dict[str, dict[str, Any]],
    session: str,
) -> bool:
    """Return True when live macro regime or scan route flips opposite an open managed guard."""
    if session == "CLOSED" or market_regime not in (-1, 0, 1):
        return False
    sym = guard.symbol.strip().upper()
    is_inverse = sym in _INVERSE_HEDGE_SYMBOLS
    if market_regime == -1 and guard.side == "LONG" and not is_inverse:
        return True
    if market_regime == 1 and (guard.side == "SHORT" or (guard.side == "LONG" and is_inverse)):
        return True
    row = rows.get(sym)
    if not row or not row.get("extreme_mover_eligible") or row.get("session") != session:
        return False
    route = select_short_or_inverse_fallback(
        sym,
        market_regime=market_regime,
        borrow_meta=row,
        session=session,
    )
    if not route:
        return False
    target_sym, target_side, origin = route
    if target_sym == sym and target_side != guard.side:
        return True
    return target_sym != sym and guard.side == "LONG" and origin.startswith("INVERSE_HEDGE_")


def restore_guards(
    farm: PaperFarmCoordinator, positions: dict[str, dict[str, Any]]
) -> dict[str, Guard]:
    guards = {}
    for acct, lane in farm.lanes.items():
        with open_bounded_duckdb(lane.duckdb_path) as db:
            if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                continue
            rows = db.execute(
                "SELECT client_order_id,symbol,side,initial_stop,"
                "coalesce(active_stop,initial_stop),"
                "natr_pct,session,exit_triggered FROM paper_entry_decisions WHERE closed_at IS NULL"
            ).fetchall()
        for cid, sym, side, initial, stop, natr, session, triggered in rows:
            try:
                parent = lane.trading_client.get_order_by_client_id(cid)
            except APIError as exc:
                if exc.status_code != 404:
                    raise
                # An ambiguous submission reserves its symbol in the durable entry journal.
                continue
            guard = Guard(lane, cid, sym, side, initial, stop, natr, session, triggered, parent)
            guard.fetch()
            if (
                guard.parent.status.value in TERMINAL
                and sym not in positions[acct]
                and (not guard.child or guard.child.status.value in TERMINAL)
            ):
                with open_bounded_duckdb(lane.duckdb_path) as db:
                    db.execute(
                        "UPDATE paper_entry_decisions SET closed_at=current_timestamp "
                        "WHERE client_order_id=?",
                        [cid],
                    )
                continue
            if (
                guard.parent.status.value not in TERMINAL | {"pending_cancel"}
                and guard.parent.submitted_at
                and (datetime.now(UTC) - guard.parent.submitted_at).total_seconds() >= 60
            ):
                lane.trading_client.cancel_order_by_id(guard.parent.id)
            if sym in guards:
                raise PermissionError("Overlapping managed paper orders")
            guards[sym] = guard
    return guards


def run_execution(manifest_dir: Path, *, stop: threading.Event | None = None) -> None:
    """One exclusive execution process. Each entry requires a fresh trade on its routed feed."""
    verify_paper_environment()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=manifest_dir / "paper_farm_execution.log",
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    key, secret = resolve_alpaca_env_creds()
    streams = {
        feed: StockDataStream(key, secret, feed=feed) for feed in (DataFeed.SIP, DataFeed.BOATS)
    }
    data = ExtremeMoverScanner.from_env().data_client
    if data is None:
        raise RuntimeError("Missing market data client")
    farm = PaperFarmCoordinator(manifest_dir)
    scan_path = manifest_dir / "live_whole_market_movers_manifest.json"
    tca_path = manifest_dir / "tca_calibration_manifest.json"
    state_path = manifest_dir / "paper_farm_execution_status.json"
    lock = threading.Lock()
    watching = {"SPY", "QQQ"}
    guards: dict[str, Guard] = {}
    candidates: dict[str, tuple[AlpacaPaperLane, dict[str, Any], risk.Side, str]] = {}
    attempts: set[str] = set()
    last_feed_trade: dict[DataFeed, datetime] = {}
    previous_status = ""
    last_report = 0.0
    scan_generation = ""
    cached_scan_sig: tuple[int, int, int] | None = None
    cached_scan_doc: dict[str, Any] | None = None
    cached_scan_ts: datetime | None = None
    cached_tca_sig: tuple[object, ...] | None = None
    cached_tca_ok: bool | None = None
    entry_fault = (
        json.loads(state_path.read_text("utf-8")).get("entry_fault") is True
        if state_path.exists()
        else False
    )

    def _file_stat_sig(path: Path) -> tuple[int, int, int] | None:
        try:
            st = path.stat()
        except FileNotFoundError:
            return None
        return (st.st_mtime_ns, st.st_size, st.st_ino)

    def report(status: str) -> None:
        nonlocal previous_status, last_report
        if status == previous_status and time.monotonic() - last_report < 30:
            return
        previous_status, last_report = status, time.monotonic()
        _atomic_write_json(
            state_path,
            {
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "status": status,
                "paper_only": True,
                "live_execution_enabled": False,
                "managed_symbols": sorted(guards),
                "streams": ["sip", "boats"],
                "last_trade_utc": {
                    feed.value: ts.isoformat() for feed, ts in last_feed_trade.items()
                },
                "entry_fault": entry_fault,
            },
        )

    def refresh() -> None:
        nonlocal cached_scan_doc, cached_scan_sig, cached_scan_ts
        nonlocal cached_tca_ok, cached_tca_sig, guards, scan_generation
        try:
            scan_st = scan_path.stat()
        except FileNotFoundError:
            cached_scan_sig = None
            cached_scan_doc = None
            cached_scan_ts = None
            raise
        scan_sig = (scan_st.st_mtime_ns, scan_st.st_size, scan_st.st_ino)
        if scan_sig != cached_scan_sig or cached_scan_doc is None or cached_scan_ts is None:
            doc = json.loads(scan_path.read_text("utf-8"))
            scan_ts = datetime.fromisoformat(doc["generated_at_utc"])
            cached_scan_sig = scan_sig
            cached_scan_doc = doc
            cached_scan_ts = scan_ts
        else:
            doc = cached_scan_doc
            scan_ts = cached_scan_ts
        now = datetime.now(UTC)
        if not 0 <= (now - scan_ts).total_seconds() < 90:
            candidates.clear()
            report("STALE_SCAN")
            return
        tca_file_sig = _file_stat_sig(tca_path)
        if tca_file_sig is not None:
            tca_sig: tuple[object, ...] = ("file", tca_file_sig)
        else:
            tca_sig = (
                "ledgers",
                *(
                    (
                        _file_stat_sig(ledger),
                        _file_stat_sig(Path(f"{ledger}.wal")),
                    )
                    for ledger in (
                        manifest_dir / _duckdb_filename(acct) for acct in ALPACA_LANE_PROFILES
                    )
                ),
            )
        if tca_sig != cached_tca_sig or cached_tca_ok is None:
            cached_tca_ok = cost_calibration_allows_entry(manifest_dir)
            cached_tca_sig = tca_sig
        calibration_ok = cached_tca_ok
        if doc["generated_at_utc"] == scan_generation:
            if not calibration_ok:
                candidates.clear()
                report("TCA_RECALIBRATION_REQUIRED")
            return
        session = engine.classify_us_equity_session_et(time.time_ns())
        candidates.clear()
        # Serialize ledger reconciliation with the scheduled observation.
        with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
            positions = refresh_accounts(farm, now, session, publish=False)
            guards = restore_guards(farm, positions)
        scan_generation = doc["generated_at_utc"]
        attempts.clear()
        rows = {r["symbol"]: r for r in doc["entries"]}
        macro_regime = doc["macro_regime"]["regime"]
        for guard in guards.values():
            if guard_should_flatten_for_flip(
                guard, market_regime=macro_regime, rows=rows, session=session
            ):
                guard.reverse_flatten = True
        if not calibration_ok:
            report("TCA_RECALIBRATION_REQUIRED")
            return
        for row in doc["entries"]:
            if not row.get("extreme_mover_eligible") or row["session"] != session:
                continue
            route = select_short_or_inverse_fallback(
                row["symbol"],
                market_regime=doc["macro_regime"]["regime"],
                borrow_meta=row,
                session=session,
            )
            if route:
                sym, side, origin = route
                target = rows.get(sym)
                if (
                    target is not None
                    and target.get("natr_pct")
                    and (
                        sym != row["symbol"]
                        or (side == "LONG") == (row["side"] == "BUY")
                        or row.get("peak_confirmed") is True
                    )
                ):
                    lane = select_situation_lane(
                        farm.lanes,
                        origin=origin,
                        natr_pct=target.get("natr_pct"),
                        rvol_normalized=_row_rvol(target),
                    )
                    if lane is not None:
                        candidates.setdefault(sym, (lane, target, side, origin))
        active_feed = DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP
        last = last_feed_trade.get(active_feed)
        report(
            "CLOSED"
            if session == "CLOSED"
            else "ENTRY_HELD"
            if entry_fault
            else "SUPERVISING"
            if last and (now - last).total_seconds() < 60
            else "AWAITING_MARKET_DATA"
        )

    def handle(trade: Any, feed: DataFeed) -> None:
        nonlocal entry_fault
        session = engine.classify_us_equity_session_et(time.time_ns())
        if session == "CLOSED" or (session == "OVERNIGHT") != (feed == DataFeed.BOATS):
            return
        sym = trade.symbol
        if not 0 <= (datetime.now(UTC) - trade.timestamp).total_seconds() <= 5 or trade.price <= 0:
            return
        last_feed_trade[feed] = max(trade.timestamp, last_feed_trade.get(feed, trade.timestamp))
        if sym in guards:
            guards[sym].on_price(Decimal(str(trade.price)), session)
            return
        if entry_fault or sym not in candidates or sym in attempts:
            return
        lane, row, side, origin = candidates[sym]
        now = datetime.now(UTC)
        if (
            not row.get("bar_close_utc")
            or not 0 <= (now - datetime.fromisoformat(row["bar_close_utc"])).total_seconds() < 60
        ):
            return
        if session != row["session"] or not 0 <= (now - trade.timestamp).total_seconds() <= 5:
            return
        if sym in farm.symbol_lock.snapshot():
            return
        if not lane.breaker.allow_entry():
            fallback = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=row.get("natr_pct"),
                rvol_normalized=_row_rvol(row),
            )
            if fallback is None:
                return
            lane = fallback
        attempts.add(sym)
        for other in farm.lanes.values():
            if other.status_path.exists():
                hb = json.loads(other.status_path.read_text("utf-8")).get("heartbeat_utc")
                if hb and (now - datetime.fromisoformat(hb)).total_seconds() < 120:
                    return  # The existing external farm has execution ownership.
        # Recheck all three account inventories immediately before admission.
        refresh_accounts(farm, now, session, publish=False)
        if sym in farm.symbol_lock.snapshot():
            return
        if not lane.breaker.allow_entry():
            fallback = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=row.get("natr_pct"),
                rvol_normalized=_row_rvol(row),
            )
            if fallback is None:
                return
            lane = fallback
        broker = lane.trading_client
        if session == "RTH" and not broker.get_clock().is_open:
            return
        asset = broker.get_asset(sym)
        attrs = asset.attributes or []
        overnight = "overnight_tradable" in attrs and "overnight_halted" not in attrs
        if not asset.tradable or (session == "OVERNIGHT" and not overnight):
            return
        if side == "SHORT" and (
            session == "OVERNIGHT"
            or not lookup_alpaca_paper_borrow([sym], broker)[sym]["easy_to_borrow"]
        ):
            return
        quote = data.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=sym, feed=feed)
        )[sym]
        bid, ask = Decimal(str(quote.bid_price)), Decimal(str(quote.ask_price))
        if (
            not 0 < bid <= ask
            or not 0 <= (datetime.now(UTC) - quote.timestamp).total_seconds() <= 5
            or not quote_microstructure_allows_entry(bid, ask, session)
        ):
            return
        cid = (
            "noesis-"
            + hashlib.sha256(f"{sym}:{row['bar_close_utc']}:{side}".encode()).hexdigest()[:32]
        )
        for other in farm.lanes.values():
            with open_bounded_duckdb(other.duckdb_path) as db:
                if ("paper_entry_decisions",) in db.execute(
                    "SHOW TABLES"
                ).fetchall() and db.execute(
                    "SELECT 1 FROM paper_entry_decisions WHERE client_order_id=? OR "
                    "(symbol=? AND (closed_at IS NULL OR closed_at>now()-INTERVAL '60 seconds'))",
                    [cid, sym],
                ).fetchone():
                    return
        if (
            engine.classify_us_equity_session_et(time.time_ns()) != session
            or not cost_calibration_allows_entry(manifest_dir)
            or (datetime.now(UTC) - datetime.fromisoformat(row["bar_close_utc"])).total_seconds()
            >= 60
            or (datetime.now(UTC) - quote.timestamp).total_seconds() > 5
        ):
            return
        res = lane.submit_guarded_entry(
            symbol=sym,
            side=side,
            ref_price=ask if side == "LONG" else bid,
            arrival_price=(bid + ask) / 2,
            decision_price=Decimal(str(row["last_price"])),
            natr_pct=Decimal(str(row["natr_pct"])),
            session=session,
            origin=origin,
            client_order_id=cid,
            overnight_tradable=overnight,
            annual_borrow_rate=(
                Decimal(str(row["annual_borrow_rate"]))
                if row.get("annual_borrow_rate") is not None
                else None
            ),
            prior_close=(
                Decimal(str(row["prior_close"]))
                if row.get("prior_close") is not None and float(row["prior_close"]) > 0
                else None
            ),
        )
        if res:
            parent = broker.get_order_by_id(
                res["broker_order_id"], GetOrderByIdRequest(nested=True)
            )
            stop = Decimal(str(res["stop_price"]))
            guards[sym] = Guard(
                lane,
                cid,
                sym,
                side,
                stop,
                stop,
                Decimal(str(row["natr_pct"])),
                session,
                False,
                parent,
            )
            guards[sym].fetch()
            if session == "RTH":
                guard = guards[sym]
                if not guard.child or (
                    Decimal(guard.child.stop_price) < stop
                    if side == "LONG"
                    else Decimal(guard.child.stop_price) > stop
                ):
                    if guard.parent.status.value not in TERMINAL | {"pending_cancel"}:
                        broker.cancel_order_by_id(guard.parent.id)
                    raise RuntimeError("Broker did not confirm the requested OTO stop")
            report("ORDER_ACKNOWLEDGED")

    def process(trade: Any, feed: DataFeed) -> None:
        nonlocal entry_fault
        with lock:
            try:
                handle(trade, feed)
            except Exception:
                entry_fault = True
                candidates.clear()
                report("ENTRY_HELD")
                LOG.exception("Paper broker or protection error")

    def callback(feed: DataFeed):
        async def on_trade(trade):
            await asyncio.to_thread(process, trade, feed)

        return on_trade

    callbacks = {feed: callback(feed) for feed in streams}

    def subscribe(symbols: set[str]) -> None:
        for feed, stream in streams.items():
            stream.subscribe_trades(callbacks[feed], *sorted(symbols))
        watching.update(symbols)

    with open_bounded_duckdb(manifest_dir / "paper_farm_execution.duckdb"):
        with lock:
            positions = refresh_accounts(
                farm,
                datetime.now(UTC),
                engine.classify_us_equity_session_et(time.time_ns()),
                publish=False,
            )
            guards = restore_guards(farm, positions)
        subscribe(watching | set(guards))
        report("AWAITING_MARKET_DATA")
        with ThreadPoolExecutor(max_workers=len(streams)) as pool:
            futures = [pool.submit(s.run) for s in streams.values()]
            try:
                while not (done := wait(futures, timeout=1, return_when=FIRST_COMPLETED).done):
                    if stop is not None and stop.is_set():
                        break
                    with lock:
                        try:
                            refresh()
                        except (FileNotFoundError, duckdb.IOException):
                            candidates.clear()
                        except Exception:
                            candidates.clear()
                            entry_fault = True
                            report("ENTRY_HELD")
                            LOG.exception("Paper reconciliation failed")
                        desired = {"SPY", "QQQ"} | set(guards) | set(candidates)
                    if added := desired - watching:
                        subscribe(added)
                    if removed := watching - desired:
                        for stream in streams.values():
                            stream.unsubscribe_trades(*sorted(removed))
                        watching.difference_update(removed)
                for future in done:
                    future.result()
            finally:
                for stream in streams.values():
                    stream.stop()
                report("STOPPED")

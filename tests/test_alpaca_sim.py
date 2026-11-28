"""Behavioural contract of ``FaultInjectingAlpacaSim`` and the ``no_live_broker_access`` fixture.

Later test modules build on the simulator, so each behaviour they rely on is pinned here.
"""

from __future__ import annotations

import json
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from alpaca.common.enums import BaseURL
from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    AssetBorrowStatus,
    OrderClass,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionIntent,
    PositionSide,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.models import Order
from alpaca.trading.requests import (
    GetOrderByIdRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    OrderRequest,
    ReplaceOrderRequest,
    StopLimitOrderRequest,
    StopLossRequest,
    StopOrderRequest,
    TrailingStopOrderRequest,
)
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st
from tests.alpaca_sim import (
    DEFAULT_START_UTC,
    LEG_WASH_EXEMPTIONS,
    REFUSAL_DETAILS,
    REPLACE_MARKS,
    REPLACE_PENDINGS,
    SIM_SESSIONS,
    TRAILING_STOP_ROUNDINGS,
    WASH_TRADE_MODELS,
    FaultInjectingAlpacaSim,
    SimSession,
    TrailingStopRounding,
    WashTradeModel,
    session_at,
)

from tier0_sota.live import paper_farm as pf
from tier0_sota.live.execution import Guard
from tier0_sota.live.scanner import lookup_alpaca_paper_borrow

OFFLINE = pytest.mark.usefixtures("no_live_broker_access")
FILLS = "/account/activities/FILL"
NEW_YORK = ZoneInfo("America/New_York")
OFF_HOURS: tuple[SimSession, ...] = ("AFTER_HOURS", "OVERNIGHT", "PRE_MARKET", "CLOSED")
FORUM_WASH_MESSAGE = "potential wash trade detected. use complex orders"
# 2026-11-27, the day after Thanksgiving: the regular session closed at 13:00 New York and the
# after-hours session ran to 17:00. Every "observed" test below replays something seen on the
# real broker that afternoon (prices and statuses only).
OBSERVED_AT = datetime(2026, 11, 27, 14, 12, tzinfo=NEW_YORK)


def _d(value: object) -> Decimal:
    return Decimal(str(value))


def _refused(error: pytest.ExceptionInfo[APIError]) -> dict[str, Any]:
    """The JSON body of a refusal, as the lane receives it."""
    body = json.loads(str(error.value))
    assert isinstance(body, dict)
    return body


def _limit(
    symbol: str,
    side: OrderSide,
    qty: float,
    limit_price: float,
    *,
    tif: TimeInForce = TimeInForce.DAY,
    extended: bool = False,
    cid: str | None = None,
    stop_loss: float | None = None,
) -> LimitOrderRequest:
    return LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=side,
        limit_price=limit_price,
        time_in_force=tif,
        extended_hours=extended,
        client_order_id=cid,
        order_class=OrderClass.OTO if stop_loss is not None else None,
        stop_loss=StopLossRequest(stop_price=stop_loss) if stop_loss is not None else None,
    )


def _market(
    symbol: str, side: OrderSide, qty: float, *, cid: str | None = None
) -> MarketOrderRequest:
    return MarketOrderRequest(
        symbol=symbol, qty=qty, side=side, time_in_force=TimeInForce.DAY, client_order_id=cid
    )


def _stop(
    symbol: str, side: OrderSide, qty: float, stop_price: float, *, cid: str | None = None
) -> StopOrderRequest:
    return StopOrderRequest(
        symbol=symbol,
        qty=qty,
        side=side,
        stop_price=stop_price,
        time_in_force=TimeInForce.GTC,
        client_order_id=cid,
    )


def _legs(order: Order) -> list[Order]:
    assert order.legs is not None
    return order.legs


# --------------------------------------------------------------------- client identity


@OFFLINE
def test_paper_and_live_flags_discriminate_against_assert_paper_trading_client(
    tmp_path: Path,
) -> None:
    paper = FaultInjectingAlpacaSim()
    live = FaultInjectingAlpacaSim(paper=False)

    # The simulator is the upstream client class; only its transport is replaced.
    assert isinstance(paper, TradingClient) and isinstance(live, TradingClient)
    assert paper._sandbox is True and paper._base_url is BaseURL.TRADING_PAPER
    assert live._sandbox is False and live._base_url is BaseURL.TRADING_LIVE
    assert BaseURL.TRADING_PAPER.value == pf.PAPER_ALPACA_ENDPOINT
    assert BaseURL.TRADING_LIVE.value != pf.PAPER_ALPACA_ENDPOINT
    # Like a real TradingClient there is no `.paper` attribute to lean on.
    assert not hasattr(paper, "paper") and not hasattr(live, "paper")
    assert str(paper._api_key).startswith("PK") and str(live._api_key).startswith("AK")

    pf._assert_paper_trading_client(paper)
    with pytest.raises(PermissionError, match="non-paper trading client"):
        pf._assert_paper_trading_client(live)

    lane = pf.AlpacaPaperLane("ALPACA_PA1", tmp_path, paper, pf.CrossLaneSymbolLock())
    assert lane.trading_client is paper and lane.paper_only is True
    with pytest.raises(PermissionError):
        pf.AlpacaPaperLane("ALPACA_PA1", tmp_path, live, pf.CrossLaneSymbolLock())


@OFFLINE
def test_transport_is_in_memory_and_unmodelled_routes_fail_loudly() -> None:
    sim = FaultInjectingAlpacaSim(paper=False)

    assert not sim._session.adapters  # the inherited requests.Session cannot connect
    with pytest.raises(RuntimeError, match="never performs HTTP"):
        sim._one_request("GET", "sim://nowhere", {}, 0)

    with pytest.raises(NotImplementedError, match="GET /watchlists"):
        sim.get("/watchlists")
    with pytest.raises(NotImplementedError, match="POST /watchlists"):
        sim.post("/watchlists", {"name": "x"})
    # Even an unmodelled mutating verb is counted: nothing bypasses the log.
    assert [(call.name, call.kind) for call in sim.call_log] == [
        ("unmodelled_route", "read"),
        ("unmodelled_route", "mutating"),
    ]
    assert sim.mutating_call_count == 1

    with pytest.raises(NotImplementedError, match="notional"):
        sim.submit_order(
            MarketOrderRequest(
                symbol="NVDA", notional=100.0, side=OrderSide.BUY, time_in_force=TimeInForce.DAY
            )
        )
    assert sim.call_log[-1].outcome == "raised:NotImplementedError"
    assert sim.mutating_call_count == 2


# ----------------------------------------------------------------------- OTO lifecycle


@OFFLINE
def test_oto_stop_loss_child_is_held_until_parent_fills_then_armed() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "101.00")

    ack = sim.submit_order(
        _limit(
            "NVDA", OrderSide.BUY, 10, 100.00, tif=TimeInForce.GTC, cid="entry-1", stop_loss=98.0
        )
    )
    assert ack.status is OrderStatus.NEW and ack.order_class is OrderClass.OTO
    assert _d(ack.filled_qty) == 0 and ack.filled_avg_price is None
    (child,) = _legs(ack)
    assert child.status is OrderStatus.HELD
    assert child.side is OrderSide.SELL and child.type is OrderType.STOP
    assert _d(child.stop_price) == Decimal("98") and _d(child.qty) == 10
    assert child.time_in_force is TimeInForce.GTC

    # Legs are rolled up only on request; a flat listing shows two open orders.
    assert sim.get_order_by_id(ack.id).legs is None
    assert sim.get_order_by_client_id("entry-1").id == ack.id
    flat = sim.get_orders(GetOrdersRequest(limit=500))
    assert {order.id for order in flat} == {ack.id, child.id}
    (rolled,) = sim.get_orders(GetOrdersRequest(limit=500, nested=True))
    assert rolled.id == ack.id and [leg.id for leg in _legs(rolled)] == [child.id]

    # Nothing has filled, so there is no position and the held leg reserves nothing.
    with pytest.raises(APIError) as missing:
        sim.get_open_position("NVDA")
    assert missing.value.status_code == 404

    sim.set_price("NVDA", "99.90")
    parent = sim.get_order_by_id(ack.id, GetOrderByIdRequest(nested=True))
    assert parent.status is OrderStatus.FILLED and parent.filled_at is not None
    assert _d(parent.filled_qty) == 10 and _d(parent.filled_avg_price) == Decimal("99.9")
    (armed,) = _legs(parent)
    assert armed.id == child.id and armed.status is OrderStatus.NEW
    # The snapshot returned at submit time never changes behind the caller's back.
    assert ack.status is OrderStatus.NEW and child.status is OrderStatus.HELD

    position = sim.get_open_position("NVDA")
    assert position.side is PositionSide.LONG
    assert _d(position.qty) == 10 and _d(position.qty_available) == 0
    assert _d(position.avg_entry_price) == Decimal("99.9")
    assert _d(position.current_price) == Decimal("99.9")

    # A filled parent with a working leg still shows up in a nested open-orders read.
    (still_open,) = sim.get_orders(GetOrdersRequest(limit=500, nested=True))
    assert still_open.id == ack.id and still_open.status is OrderStatus.FILLED

    sim.set_price("NVDA", "97.50")
    stopped = sim.get_order_by_id(child.id)
    assert stopped.status is OrderStatus.FILLED and _d(stopped.filled_avg_price) == Decimal("97.5")
    assert sim.inspect_position("NVDA") is None
    assert sim.get_orders(GetOrdersRequest(limit=500)) == []


@OFFLINE
def test_fill_partial_keeps_remainder_working_and_oto_child_held() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "101.00")
    parent = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 10, 100.00, tif=TimeInForce.GTC, stop_loss=98.0)
    )

    sim.fill_partial(parent.id, 4)
    partial = sim.inspect_order(parent.id)
    assert partial.status is OrderStatus.PARTIALLY_FILLED
    assert _d(partial.filled_qty) == 4 and _d(partial.filled_avg_price) == 100
    assert partial.filled_at is None
    assert _legs(partial)[0].status is OrderStatus.HELD
    position = sim.get_open_position("NVDA")
    # Four shares are held with no working stop: the held leg protects nothing yet.
    assert _d(position.qty) == 4 and _d(position.qty_available) == 4

    sim.fill_partial(parent.id, 6, price="99.50")
    done = sim.inspect_order(parent.id)
    assert done.status is OrderStatus.FILLED and done.filled_at is not None
    assert _d(done.filled_avg_price) == Decimal("99.7")
    assert _legs(done)[0].status is OrderStatus.NEW
    assert _d(sim.get_open_position("NVDA").qty_available) == 0

    first, second = sim.get(FILLS, {"direction": "asc"})
    assert (first["type"], first["qty"], first["cum_qty"], first["leaves_qty"]) == (
        "partial_fill",
        "4",
        "4",
        "6",
    )
    assert (second["type"], second["qty"], second["cum_qty"], second["leaves_qty"]) == (
        "fill",
        "6",
        "10",
        "0",
    )
    assert first["order_status"] == "partially_filled" and second["order_status"] == "filled"

    with pytest.raises(ValueError, match="cannot execute"):
        sim.fill_partial(parent.id, 1)

    # Canceling a partially filled parent takes the held stop with it: shares left unprotected.
    sim.set_price("AMD", "201.00")
    other = sim.submit_order(
        _limit("AMD", OrderSide.BUY, 10, 200.00, tif=TimeInForce.GTC, stop_loss=196.0)
    )
    sim.fill_partial(other.id, 3)
    with pytest.raises(ValueError, match=r"qty must be in \(0, 7\]"):
        sim.fill_partial(other.id, 8)
    sim.cancel_order_by_id(other.id)
    canceled = sim.inspect_order(other.id)
    assert canceled.status is OrderStatus.CANCELED and _d(canceled.filled_qty) == 3
    assert _legs(canceled)[0].status is OrderStatus.CANCELED
    exposed = sim.get_open_position("AMD")
    assert _d(exposed.qty) == 3 and _d(exposed.qty_available) == 3
    assert sim.inspect_orders("open", "AMD") == []


# ----------------------------------------------------------------------- sessions


@OFFLINE
def test_stop_orders_trigger_only_in_rth_and_rest_otherwise() -> None:
    sim = FaultInjectingAlpacaSim(session="AFTER_HOURS")
    assert sim.market_session == "AFTER_HOURS" and sim.get_clock().is_open is False
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", 5, "200")

    nvda_stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0, cid="stop-nvda"))
    amd_stop = sim.submit_order(_stop("AMD", OrderSide.SELL, 5.0, 196.0, cid="stop-amd"))
    assert nvda_stop.status is OrderStatus.ACCEPTED and amd_stop.status is OrderStatus.ACCEPTED
    # A resting stop reserves the shares in every session.
    assert _d(sim.get_open_position("NVDA").qty_available) == 0

    # Far through both stops while the regular session is shut: nothing triggers.
    sim.set_price("NVDA", "90")
    sim.set_price("AMD", "180")
    for session in ("OVERNIGHT", "PRE_MARKET"):
        sim.set_session(session)
        assert sim.market_session == session
        assert sim.inspect_order(nvda_stop.id).status is OrderStatus.ACCEPTED
        assert sim.inspect_order(amd_stop.id).status is OrderStatus.ACCEPTED
        assert _d(sim.get_open_position("NVDA").qty) == 10

    # NVDA recovers before the open; AMD is still below its stop when the bell rings.
    sim.set_price("NVDA", "99")
    sim.set_session("RTH")
    assert sim.get_clock().is_open is True
    resting = sim.inspect_order(nvda_stop.id)
    assert resting.status is OrderStatus.NEW and _d(resting.filled_qty) == 0
    gapped = sim.inspect_order(amd_stop.id)
    assert gapped.status is OrderStatus.FILLED and _d(gapped.filled_avg_price) == 180
    assert sim.inspect_position("AMD") is None

    sim.set_price("NVDA", "97.99")
    assert sim.inspect_order(nvda_stop.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None

    # Stops cannot be flagged for extended hours at all.
    sim.open_position("NVDA", 1, "100")
    with pytest.raises(APIError) as refused:
        sim.submit_order(
            StopOrderRequest(
                symbol="NVDA",
                qty=1,
                side=OrderSide.SELL,
                stop_price=95.0,
                time_in_force=TimeInForce.DAY,
                extended_hours=True,
            )
        )
    assert refused.value.status_code == 422


@OFFLINE
def test_extended_hours_limit_fills_off_hours_and_regular_orders_wait_for_rth() -> None:
    sim = FaultInjectingAlpacaSim(session="PRE_MARKET")
    sim.set_price("NVDA", "100")

    regular = sim.submit_order(_limit("NVDA", OrderSide.BUY, 10, 100.50, cid="regular"))
    extended = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 10, 100.50, extended=True, cid="extended")
    )
    assert regular.status is OrderStatus.ACCEPTED and extended.status is OrderStatus.NEW
    assert sim.inspect_order(regular.id).status is OrderStatus.ACCEPTED
    filled = sim.inspect_order(extended.id)
    assert filled.status is OrderStatus.FILLED and filled.extended_hours is True
    assert _d(filled.filled_avg_price) == 100
    assert _d(sim.get_open_position("NVDA").qty) == 10

    with pytest.raises(APIError) as refused:
        sim.submit_order(
            MarketOrderRequest(
                symbol="NVDA",
                qty=1,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                extended_hours=True,
            )
        )
    assert refused.value.status_code == 422
    assert "extended hours" in refused.value.message

    sim.set_session("RTH")
    assert sim.inspect_order(regular.id).status is OrderStatus.FILLED
    assert _d(sim.get_open_position("NVDA").qty) == 20

    sim.set_session("AFTER_HOURS")
    market_exit = sim.submit_order(_market("NVDA", OrderSide.SELL, 5.0, cid="rth-only-exit"))
    assert sim.inspect_order(market_exit.id).status is OrderStatus.ACCEPTED  # waits for the open
    sim.cancel_order_by_id(market_exit.id)
    collared_exit = sim.submit_order(
        _limit("NVDA", OrderSide.SELL, 20.0, 99.50, extended=True, cid="collared-exit")
    )
    assert sim.inspect_order(collared_exit.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None


@OFFLINE
def test_overnight_needs_an_overnight_tradable_asset_and_closed_fills_nothing() -> None:
    sim = FaultInjectingAlpacaSim(session="OVERNIGHT")
    sim.set_asset("SPY", attributes=["overnight_tradable"])
    sim.set_price("SPY", "500")
    sim.set_price("XYZ", "50")

    spy = sim.submit_order(_limit("SPY", OrderSide.BUY, 1, 500.50, extended=True))
    xyz = sim.submit_order(_limit("XYZ", OrderSide.BUY, 1, 50.50, extended=True))
    assert sim.inspect_order(spy.id).status is OrderStatus.FILLED
    assert sim.inspect_order(xyz.id).status is OrderStatus.ACCEPTED
    sim.set_session("PRE_MARKET")
    assert sim.inspect_order(xyz.id).status is OrderStatus.FILLED

    weekend = FaultInjectingAlpacaSim(session="CLOSED")
    assert weekend.market_session == "CLOSED" and weekend.get_clock().is_open is False
    weekend.set_asset("SPY", attributes=["overnight_tradable"])
    weekend.set_price("SPY", "500")
    queued = weekend.submit_order(_limit("SPY", OrderSide.BUY, 1, 500.50, extended=True))
    assert weekend.inspect_order(queued.id).status is OrderStatus.ACCEPTED
    weekend.advance(3600)
    assert weekend.inspect_order(queued.id).status is OrderStatus.ACCEPTED
    weekend.set_session("OVERNIGHT")
    assert weekend.inspect_order(queued.id).status is OrderStatus.FILLED


@OFFLINE
def test_order_lifecycle_accepted_new_expired_canceled_rejected() -> None:
    sim = FaultInjectingAlpacaSim(session="PRE_MARKET")
    sim.set_price("NVDA", "100")

    day = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, cid="day"))
    gtc = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, tif=TimeInForce.GTC, cid="gtc"))
    ext = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, extended=True, cid="ext"))
    assert [order.status for order in (day, gtc, ext)] == [
        OrderStatus.ACCEPTED,
        OrderStatus.ACCEPTED,
        OrderStatus.NEW,
    ]

    sim.set_session("RTH")
    assert sim.inspect_order(day.id).status is OrderStatus.NEW
    assert sim.inspect_order(gtc.id).status is OrderStatus.NEW

    sim.set_session("AFTER_HOURS")
    expired = sim.inspect_order(day.id)
    assert expired.status is OrderStatus.EXPIRED and expired.expired_at is not None
    assert session_at(expired.expired_at) == "AFTER_HOURS"
    assert sim.inspect_order(gtc.id).status is OrderStatus.NEW
    assert sim.inspect_order(ext.id).status is OrderStatus.NEW  # extended day order lives on

    sim.set_session("OVERNIGHT")
    assert sim.inspect_order(ext.id).status is OrderStatus.EXPIRED
    assert sim.inspect_order(gtc.id).status is OrderStatus.NEW

    sim.cancel_order_by_id(gtc.id)
    canceled = sim.get_order_by_id(gtc.id)
    assert canceled.status is OrderStatus.CANCELED and canceled.canceled_at is not None

    sim.reject_next_order()
    rejected = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, tif=TimeInForce.GTC))
    assert rejected.status is OrderStatus.REJECTED and rejected.failed_at is not None
    later = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, tif=TimeInForce.GTC))
    assert later.status is OrderStatus.ACCEPTED
    sim.reject_order(later.id)
    assert sim.inspect_order(later.id).status is OrderStatus.REJECTED

    closed = sim.get_orders(GetOrdersRequest(status=QueryOrderStatus.CLOSED, limit=500))
    assert {order.status for order in closed} == {
        OrderStatus.EXPIRED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
    }
    assert sim.get_orders(GetOrdersRequest(limit=500)) == []

    rth = FaultInjectingAlpacaSim()
    rth.set_price("NVDA", "100")
    missed = rth.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, tif=TimeInForce.IOC))
    hit = rth.submit_order(_limit("NVDA", OrderSide.BUY, 1, 101.0, tif=TimeInForce.IOC))
    assert rth.inspect_order(missed.id).status is OrderStatus.CANCELED
    assert rth.inspect_order(hit.id).status is OrderStatus.FILLED


# -------------------------------------------------------------- ids, quantity, replace


@OFFLINE
def test_client_order_id_is_unique_forever_including_closed_orders() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100")

    filled = sim.submit_order(_market("NVDA", OrderSide.BUY, 1, cid="dup-filled"))
    assert sim.inspect_order(filled.id).status is OrderStatus.FILLED
    resting = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, cid="dup-canceled"))
    sim.cancel_order_by_id(resting.id)
    working = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, cid="dup-working"))

    for used in ("dup-filled", "dup-canceled", "dup-working"):
        with pytest.raises(APIError) as duplicate:
            sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, cid=used))
        assert duplicate.value.status_code == 422
        assert duplicate.value.code == 40010001
        assert "client_order_id must be unique" in duplicate.value.message

    # A replacement may not recycle an id either, and the original stays working.
    with pytest.raises(APIError) as recycled:
        sim.replace_order_by_id(
            working.id, ReplaceOrderRequest(limit_price=91.0, client_order_id="dup-filled")
        )
    assert recycled.value.status_code == 422
    assert sim.inspect_order(working.id).status is OrderStatus.NEW
    assert len(sim.inspect_orders("all")) == 3

    with pytest.raises(APIError) as too_long:
        sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 90.0, cid="x" * 129))
    assert too_long.value.status_code == 422


@OFFLINE
def test_resting_sell_reserves_shares_and_blocks_a_second_sell() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    assert _d(sim.get_open_position("NVDA").qty_available) == 10

    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0, cid="stop-1"))
    held = sim.get_open_position("NVDA")
    assert _d(held.qty) == 10 and _d(held.qty_available) == 0

    attempts = sim.mutating_call_count
    with pytest.raises(APIError) as blocked:
        sim.submit_order(_market("NVDA", OrderSide.SELL, 10.0, cid="exit-1"))
    assert blocked.value.status_code == 403 and blocked.value.code == 40310000
    # The body the API reference documents for this refusal, and nothing beyond it.
    assert _refused(blocked) == {
        "code": 40310000,
        "message": "insufficient qty available for order (requested: 10, available: 0)",
        "available": "0",
        "existing_qty": "10",
        "held_for_orders": "10",
        "symbol": "NVDA",
    }
    assert sim.mutating_call_count == attempts + 1  # the refused attempt still reached the broker
    assert sim.call_log[-1].outcome == "error:403"
    assert sim.call_log[-1].refusal == "insufficient_qty"

    # Canceling the resting stop releases the shares; a partial reservation leaves the rest.
    sim.cancel_order_by_id(stop.id)
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    sim.submit_order(_stop("NVDA", OrderSide.SELL, 4.0, 98.0))
    assert _d(sim.get_open_position("NVDA").qty_available) == 6
    with pytest.raises(APIError) as over:
        sim.submit_order(_limit("NVDA", OrderSide.SELL, 7.0, 105.0))
    assert over.value.status_code == 403
    sim.submit_order(_limit("NVDA", OrderSide.SELL, 6.0, 105.0))
    assert _d(sim.get_open_position("NVDA").qty_available) == 0

    # One order may not sell through a long position into a short one.
    sim.open_position("AMD", 5, "200")
    with pytest.raises(APIError) as flip:
        sim.submit_order(_market("AMD", OrderSide.SELL, 6.0))
    assert flip.value.status_code == 403

    # The mirror rule for a short: a resting buy-stop reserves the cover.
    sim.open_position("TSLA", -5, "300")
    sim.submit_order(_stop("TSLA", OrderSide.BUY, 5.0, 306.0))
    short = sim.get_open_position("TSLA")
    assert short.side is PositionSide.SHORT
    assert _d(short.qty) == -5 and _d(short.qty_available) == 0
    with pytest.raises(APIError) as cover:
        sim.submit_order(_limit("TSLA", OrderSide.BUY, 1.0, 290.0))
    assert cover.value.status_code == 403


@OFFLINE
def test_wash_trade_protection_rejects_an_opposite_side_order_and_can_be_disabled() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    with pytest.raises(APIError) as wash:
        sim.submit_order(_limit("NVDA", OrderSide.BUY, 5, 99.0, tif=TimeInForce.GTC))
    assert wash.value.status_code == 403
    assert sim.call_log[-1].refusal == "self_trade"
    assert len(sim.inspect_orders("open")) == 1  # the refused buy was never created

    lenient = FaultInjectingAlpacaSim(wash_trade_protection=False)
    lenient.open_position("NVDA", 10, "100")
    lenient.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    accepted = lenient.submit_order(_limit("NVDA", OrderSide.BUY, 5, 99.0, tif=TimeInForce.GTC))
    assert accepted.status is OrderStatus.NEW


@OFFLINE
def test_self_trade_and_insufficient_quantity_refusals_are_told_apart_as_on_the_broker() -> None:
    def book(**switches: Any) -> tuple[FaultInjectingAlpacaSim, Order]:
        sim = FaultInjectingAlpacaSim(**switches)
        sim.open_position("NVDA", 10, "100")
        return sim, sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))

    buy = _limit("NVDA", OrderSide.BUY, 5, 99.0, tif=TimeInForce.GTC)
    sell = _market("NVDA", OrderSide.SELL, 10.0)

    # Default: only what is documented. Both refusals are 403 with the shared code; the
    # insufficient-quantity one is recognisable by its documented body, the self-trade one
    # carries nothing a lane could match on, so "wash trade" in the text is not a way to tell.
    sim, stop = book()
    assert sim.refusal_detail == "documented"
    with pytest.raises(APIError) as wash:
        sim.submit_order(buy)
    with pytest.raises(APIError) as short_of_shares:
        sim.submit_order(sell)
    assert wash.value.status_code == short_of_shares.value.status_code == 403
    assert wash.value.code == short_of_shares.value.code == 40310000
    assert _refused(wash) == {"code": 40310000, "message": "forbidden"}
    assert "wash" not in str(wash.value).lower()
    assert set(_refused(short_of_shares)) == {
        "code",
        "message",
        "available",
        "existing_qty",
        "held_for_orders",
        "symbol",
    }
    assert short_of_shares.value.message.startswith("insufficient qty available for order")
    # The simulator itself knows which rule fired; that is for assertions, not for the lane.
    assert [call.refusal for call in sim.calls("submit_order")] == [
        "",
        "self_trade",
        "insufficient_qty",
    ]
    # A later error of another kind carries no such note.
    with pytest.raises(APIError) as malformed:
        sim.post(
            "/orders",
            {
                "symbol": "NVDA",
                "qty": "1",
                "side": "hold",
                "type": "market",
                "time_in_force": "day",
            },
        )
    assert malformed.value.status_code == 422
    assert (sim.call_log[-1].outcome, sim.call_log[-1].refusal) == ("error:422", "")

    # The forum-level bodies are one switch away ...
    forum, stop = book(refusal_detail="forum")
    with pytest.raises(APIError) as wash:
        forum.submit_order(buy)
    assert _refused(wash) == {
        "code": 40310000,
        "message": FORUM_WASH_MESSAGE,
        "existing_order_id": str(stop.id),
        "reject_reason": "opposite side market/stop order exists",
    }
    with pytest.raises(APIError) as short_of_shares:
        forum.submit_order(sell)
    assert _refused(short_of_shares)["related_orders"] == [str(stop.id)]

    # ... and code, message and fields of the self-trade refusal are scriptable, because only
    # its HTTP status is documented: the lane must cope with a wording nobody has seen.
    for scripted in (sim, forum):
        scripted.set_wash_trade_refusal(
            code=40310100, message="order would cross an existing order", hint="x"
        )
        with pytest.raises(APIError) as wash:
            scripted.submit_order(buy)
        assert wash.value.status_code == 403 and wash.value.code == 40310100
        assert wash.value.message == "order would cross an existing order"
        assert _refused(wash)["hint"] == "x"
        assert scripted.call_log[-1].refusal == "self_trade"
    # A partial script keeps the rest of what ``refusal_detail`` serves; no argument ends it.
    forum.set_wash_trade_refusal(message="self trade")
    with pytest.raises(APIError) as wash:
        forum.submit_order(buy)
    assert (wash.value.code, wash.value.message) == (40310000, "self trade")
    assert _refused(wash)["existing_order_id"] == str(stop.id)
    forum.set_wash_trade_refusal()
    with pytest.raises(APIError) as wash:
        forum.submit_order(buy)
    assert wash.value.message == FORUM_WASH_MESSAGE
    # The insufficient-quantity refusal is documented and is not touched by the script.
    sim.set_wash_trade_refusal(message="insufficient qty available for order")
    with pytest.raises(APIError) as short_of_shares:
        sim.submit_order(sell)
    assert _refused(short_of_shares)["held_for_orders"] == "10"

    for bogus in ("verbose", ""):
        detail: Any = bogus
        with pytest.raises(ValueError, match="refusal_detail"):
            FaultInjectingAlpacaSim(refusal_detail=detail)
    assert REFUSAL_DETAILS == ("documented", "forum")


@OFFLINE
def test_replace_creates_a_successor_and_marks_the_original_replaced() -> None:
    # The mechanics of a replacement that takes effect at once; the interval that the default
    # puts between the request and its effect has its own test below.
    sim = FaultInjectingAlpacaSim(replace_pending="never")
    sim.open_position("NVDA", 10, "100")
    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0, cid="stop-1"))

    successor = sim.replace_order_by_id(stop.id, ReplaceOrderRequest(stop_price=99.0))
    assert successor.id != stop.id and successor.replaces == stop.id
    assert successor.status is OrderStatus.NEW and _d(successor.stop_price) == 99
    assert successor.client_order_id != "stop-1"
    original = sim.get_order_by_id(stop.id)
    assert original.status is OrderStatus.REPLACED and original.replaced_by == successor.id
    assert original.replaced_at is not None
    # Only the successor reserves shares, and only the successor can trigger.
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    assert [order.id for order in sim.get_orders(GetOrdersRequest(limit=500))] == [successor.id]

    with pytest.raises(APIError) as stale:
        sim.replace_order_by_id(stop.id, ReplaceOrderRequest(stop_price=99.5))
    assert stale.value.status_code == 422
    with pytest.raises(APIError) as sub_penny:
        sim.replace_order_by_id(successor.id, ReplaceOrderRequest(stop_price=99.123))
    assert sub_penny.value.status_code == 422 and "sub-penny" in sub_penny.value.message
    with pytest.raises(APIError) as unknown:
        sim.replace_order_by_id(uuid4(), ReplaceOrderRequest(stop_price=99.5))
    assert unknown.value.status_code == 404

    # A rejected replacement leaves the order it tried to replace working.
    sim.reject_next_order()
    refused = sim.replace_order_by_id(successor.id, ReplaceOrderRequest(stop_price=99.5))
    assert refused.status is OrderStatus.REJECTED and refused.replaces == successor.id
    assert sim.get_order_by_id(successor.id).status is OrderStatus.NEW

    sim.set_price("NVDA", "98.99")
    assert sim.get_order_by_id(successor.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None

    # An OTO leg keeps its place in the group: the parent lists the old leg and its successor.
    sim.set_price("AMD", "200")
    parent = sim.submit_order(
        _limit("AMD", OrderSide.BUY, 5, 200.50, tif=TimeInForce.GTC, stop_loss=196.0)
    )
    (leg,) = _legs(sim.get_order_by_id(parent.id, GetOrderByIdRequest(nested=True)))
    assert leg.status is OrderStatus.NEW
    tightened = sim.replace_order_by_id(leg.id, ReplaceOrderRequest(stop_price=197.0))
    old_leg, new_leg = _legs(sim.get_order_by_id(parent.id, GetOrderByIdRequest(nested=True)))
    assert old_leg.id == leg.id and old_leg.replaced_by == tightened.id
    assert new_leg.id == tightened.id and new_leg.status is OrderStatus.NEW

    # A held leg cannot be replaced before its parent fills.
    sim.set_price("TSLA", "301")
    waiting = sim.submit_order(
        _limit("TSLA", OrderSide.BUY, 5, 300.0, tif=TimeInForce.GTC, stop_loss=294.0)
    )
    with pytest.raises(APIError) as held:
        sim.replace_order_by_id(_legs(waiting)[0].id, ReplaceOrderRequest(stop_price=295.0))
    assert held.value.status_code == 422


# -------------------------------------------------------------------- fault injection


@OFFLINE
def test_fail_next_scripts_api_errors_timeouts_and_lost_responses() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100")

    sim.fail_next("submit_order", 503, times=2)
    for _ in range(2):
        with pytest.raises(APIError) as outage:
            sim.submit_order(_market("NVDA", OrderSide.BUY, 1, cid="retry-me"))
        assert outage.value.status_code == 503 and outage.value.code == 50310000
    assert sim.inspect_orders("all") == []  # nothing reached the book
    assert [call.outcome for call in sim.calls("submit_order")] == ["fault:503", "fault:503"]
    assert sim.mutating_call_count == 2
    entry = sim.submit_order(_market("NVDA", OrderSide.BUY, 10, cid="retry-me"))
    assert sim.inspect_order(entry.id).status is OrderStatus.FILLED

    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    sim.fail_next("replace_order_by_id", 422)
    with pytest.raises(APIError) as race:
        sim.replace_order_by_id(stop.id, ReplaceOrderRequest(stop_price=99.0))
    assert race.value.status_code == 422
    assert _d(sim.inspect_order(stop.id).stop_price) == 98

    # A timeout before the request is applied changes nothing at the broker.
    sim.fail_next("cancel_order_by_id", "timeout")
    with pytest.raises(TimeoutError):
        sim.cancel_order_by_id(stop.id)
    assert sim.inspect_order(stop.id).status is OrderStatus.NEW
    sim.cancel_order_by_id(stop.id)

    # A lost response: the broker acted, the caller only saw the timeout.
    sim.fail_next("submit_order", "timeout", applied=True)
    with pytest.raises(TimeoutError):
        sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0, cid="ambiguous"))
    assert sim.get_order_by_client_id("ambiguous").status is OrderStatus.NEW
    assert sim.calls("submit_order")[-1].outcome == "fault:TimeoutError:applied"
    with pytest.raises(APIError) as duplicate:
        sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0, cid="ambiguous"))
    assert duplicate.value.status_code == 422

    # Reads, the raw activity feed and arbitrary exceptions can be scripted as well.
    sim.fail_next("get_open_position", 500)
    with pytest.raises(APIError) as read_fault:
        sim.get_open_position("NVDA")
    assert read_fault.value.status_code == 500
    assert _d(sim.get_open_position("NVDA").qty) == 10
    sim.fail_next("get_account_activities", 429)
    with pytest.raises(APIError) as throttled:
        sim.get(FILLS, {"direction": "asc"})
    assert throttled.value.status_code == 429
    sim.fail_next("get_account", ConnectionResetError("simulated reset"))
    with pytest.raises(ConnectionResetError):
        sim.get_account()

    sim.fail_next("get_clock", 500, times=3)
    sim.clear_faults()
    assert sim.get_clock().is_open is True

    with pytest.raises(ValueError, match="Unknown method"):
        sim.fail_next("submit_ordr", 500)
    with pytest.raises(ValueError, match="HTTP error status"):
        sim.fail_next("submit_order", 200)
    with pytest.raises(ValueError, match="times"):
        sim.fail_next("submit_order", 500, times=0)


@OFFLINE
def test_external_cancel_closes_an_order_without_a_client_call() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    calls_before = len(sim.call_log)

    sim.external_cancel(stop.id)
    assert len(sim.call_log) == calls_before  # the owner's action is not a harness call
    gone = sim.get_order_by_id(stop.id)
    assert gone.status is OrderStatus.CANCELED and gone.canceled_at is not None
    assert sim.get_orders(GetOrdersRequest(limit=500)) == []
    # The position is unprotected again and its shares are free.
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    sim.set_price("NVDA", "90")
    assert _d(sim.get_open_position("NVDA").qty) == 10

    # The harness then loses the race on its own cancel and replace.
    with pytest.raises(APIError) as already:
        sim.cancel_order_by_id(stop.id)
    assert already.value.status_code == 422
    with pytest.raises(APIError) as too_late:
        sim.replace_order_by_id(stop.id, ReplaceOrderRequest(stop_price=89.0))
    assert too_late.value.status_code == 422
    with pytest.raises(APIError) as unknown:
        sim.cancel_order_by_id(uuid4())
    assert unknown.value.status_code == 404
    with pytest.raises(ValueError, match="only open orders cancel"):
        sim.external_cancel(stop.id)

    # alpaca-py itself refuses a non-UUID id before anything is sent.
    sent = len(sim.call_log)
    with pytest.raises(ValueError, match="badly formed"):
        sim.cancel_order_by_id("adopted-alpaca_pa1-nvda-0123456789ab")
    assert len(sim.call_log) == sent

    # An armed OTO stop canceled from outside leaves the filled entry bare.
    sim.set_price("AMD", "200")
    parent = sim.submit_order(
        _limit("AMD", OrderSide.BUY, 5, 200.50, tif=TimeInForce.GTC, stop_loss=196.0)
    )
    (leg,) = _legs(sim.inspect_order(parent.id))
    assert leg.status is OrderStatus.NEW
    sim.external_cancel(leg.id)
    assert _legs(sim.inspect_order(parent.id))[0].status is OrderStatus.CANCELED
    assert _d(sim.get_open_position("AMD").qty_available) == 5


# ---------------------------------------------------------------------- broker reads


@OFFLINE
def test_account_clock_and_position_reads_follow_the_virtual_clock_and_prices() -> None:
    sim = FaultInjectingAlpacaSim(equity="50000")
    assert sim.now == DEFAULT_START_UTC and sim.market_session == "RTH"
    # The default start is 10:00 New York on a Wednesday, ahead of any wall clock.
    assert datetime.now(UTC) < DEFAULT_START_UTC and DEFAULT_START_UTC.weekday() == 2
    close_today = DEFAULT_START_UTC.replace(hour=21, minute=0)
    open_tomorrow = DEFAULT_START_UTC.replace(hour=14, minute=30) + timedelta(days=1)

    account = sim.get_account()
    assert account.status.value == "ACTIVE" and account.id is not None
    assert account.trading_blocked is False and account.account_blocked is False
    assert account.trade_suspended_by_user is False
    assert _d(account.equity) == 50000 and _d(account.last_equity) == 50000
    assert _d(account.buying_power) == 100000

    clock = sim.get_clock()
    assert clock.is_open is True and clock.timestamp == sim.now
    assert clock.next_close == close_today and clock.next_open == open_tomorrow

    sim.open_position("NVDA", 100, "100")
    assert _d(sim.get_account().equity) == 50000  # opening at cost moves no equity
    sim.set_price("NVDA", "101")
    position = sim.get_open_position("NVDA")
    assert _d(position.current_price) == 101 and _d(position.avg_entry_price) == 100
    assert _d(position.market_value) == 10100 and _d(position.cost_basis) == 10000
    assert _d(position.unrealized_pl) == 100 and _d(position.unrealized_plpc) == Decimal("0.01")
    marked = sim.get_account()
    assert _d(marked.equity) == 50100 and _d(marked.last_equity) == 50000
    assert _d(marked.long_market_value) == 10100
    assert [held.symbol for held in sim.get_all_positions()] == ["NVDA"]

    sim.advance(90)
    assert sim.now == DEFAULT_START_UTC + timedelta(seconds=90)
    assert sim.get_clock().timestamp == sim.now
    sim.set_session("AFTER_HOURS")
    after = sim.get_clock()
    assert after.is_open is False and after.timestamp == close_today
    assert after.next_open == open_tomorrow
    assert after.next_close == close_today + timedelta(days=1)
    with pytest.raises(ValueError, match="backwards"):
        sim.set_time(DEFAULT_START_UTC)
    with pytest.raises(ValueError, match="backwards"):
        sim.advance(-1)

    # A simulator can instead start on the wall clock; advancing it then creates broker skew.
    aligned = FaultInjectingAlpacaSim(start=datetime.now(UTC))
    assert abs((datetime.now(UTC) - aligned.get_clock().timestamp).total_seconds()) < 5
    aligned.advance(3)
    assert (aligned.get_clock().timestamp - datetime.now(UTC)).total_seconds() > 1
    with pytest.raises(ValueError, match="timezone-aware"):
        FaultInjectingAlpacaSim(start=datetime(2035, 11, 14, 15, 0))  # naive on purpose

    # Account restrictions are visible to reads and refuse new orders.
    sim.set_account(trading_blocked=True, last_equity="0")
    restricted = sim.get_account()
    assert restricted.trading_blocked is True and _d(restricted.last_equity) == 0
    # A protective order is no exception: a trailing stop is refused by the same rule.
    for request in (
        _limit("NVDA", OrderSide.SELL, 1, 105.0, extended=True),
        _trailing("NVDA", OrderSide.SELL, 1, percent=1.0),
    ):
        with pytest.raises(APIError) as blocked:
            sim.submit_order(request)
        assert blocked.value.status_code == 403
        assert blocked.value.message == "account is not authorized to trade"
    assert sim.inspect_orders("open") == []
    assert _d(sim.get_open_position("NVDA").qty) == 100  # reads keep working
    with pytest.raises(ValueError, match="Unknown account field"):
        sim.set_account(not_a_field=1)

    sim.set_account(trading_blocked=False, buying_power="500")
    with pytest.raises(APIError) as poor:
        sim.submit_order(_limit("AMD", OrderSide.BUY, 10, 200.0, extended=True))
    assert poor.value.status_code == 403 and "insufficient buying power" in poor.value.message


@OFFLINE
def test_asset_reads_and_short_sale_rules() -> None:
    sim = FaultInjectingAlpacaSim()
    default = sim.get_asset("nvda")
    assert default.symbol == "NVDA" and default.tradable is True and default.shortable is True
    assert default.fractionable is True and default.attributes == []
    assert default.borrow_status is AssetBorrowStatus.EASY_TO_BORROW
    with pytest.warns(DeprecationWarning):  # upstream deprecated the old boolean
        assert default.easy_to_borrow is True
    assert sim.get_asset(default.id).symbol == "NVDA"

    sim.set_asset("NVDA", attributes=["overnight_tradable"])
    sim.set_asset("HTB", easy_to_borrow=False)
    sim.set_asset("HALT", tradable=False)
    sim.set_asset("WHOLE", fractionable=False)
    assert sim.get_asset("NVDA").attributes == ["overnight_tradable"]
    borrow = lookup_alpaca_paper_borrow(["NVDA", "HTB"], sim)
    assert borrow["NVDA"]["easy_to_borrow"] is True and borrow["NVDA"]["overnight_tradable"] is True
    assert borrow["HTB"]["easy_to_borrow"] is False and borrow["HTB"]["shortable"] is True
    assert {asset.symbol for asset in sim.get_all_assets()} == {"NVDA", "HTB", "HALT", "WHOLE"}

    for symbol in ("NVDA", "HTB", "HALT", "WHOLE"):
        sim.set_price(symbol, "50")
    with pytest.raises(APIError) as hard:
        sim.submit_order(_market("HTB", OrderSide.SELL, 5.0))
    assert hard.value.status_code == 422 and "cannot be sold short" in hard.value.message
    with pytest.raises(APIError) as halted:
        sim.submit_order(_market("HALT", OrderSide.BUY, 5.0))
    assert halted.value.status_code == 403
    with pytest.raises(APIError) as fractional:
        sim.submit_order(_market("WHOLE", OrderSide.BUY, 0.5))
    assert fractional.value.status_code == 422
    with pytest.raises(APIError) as fractional_gtc:
        sim.submit_order(_stop("NVDA", OrderSide.SELL, 0.5, 45.0))
    assert fractional_gtc.value.status_code == 422

    shorted = sim.submit_order(_market("NVDA", OrderSide.SELL, 5.0))
    assert sim.inspect_order(shorted.id).status is OrderStatus.FILLED
    short = sim.get_open_position("NVDA")
    assert short.side is PositionSide.SHORT and _d(short.qty) == -5
    (fill,) = sim.get(FILLS, {"direction": "asc"})
    assert fill["side"] == "sell_short" and fill["side"].removesuffix("_short") == "sell"

    strict = FaultInjectingAlpacaSim(auto_assets=False)
    with pytest.raises(APIError) as unknown:
        strict.get_asset("NOPE")
    assert unknown.value.status_code == 404
    strict.set_price("KNOWN", "10")
    assert strict.get_asset("KNOWN").tradable is True


@OFFLINE
def test_open_position_models_a_hand_opened_position() -> None:
    sim = FaultInjectingAlpacaSim()
    owner_order = sim.open_position("NVDA", 10, "100.50")

    assert sim.call_log == [] and sim.mutating_call_count == 0  # not a harness action
    assert owner_order.status is OrderStatus.FILLED
    assert owner_order.client_order_id.startswith("owner-")
    position = sim.get_open_position("NVDA")
    assert position.side is PositionSide.LONG
    assert _d(position.qty) == 10 and _d(position.qty_available) == 10
    assert _d(position.avg_entry_price) == Decimal("100.50")
    assert _d(position.current_price) == Decimal("100.50")
    assert _d(sim.get_account().equity) == 100000

    # The owner's order is part of the account history the harness reads back.
    (history,) = sim.get_orders(GetOrdersRequest(status=QueryOrderStatus.CLOSED, limit=100))
    assert history.id == owner_order.id and history.filled_at is not None
    (fill,) = sim.get(FILLS, {"direction": "asc"})
    assert fill["order_id"] == str(owner_order.id) and fill["side"] == "buy"

    # Adding by hand grows the position at a blended cost; a resting stop is not resized.
    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    sim.open_position("NVDA", 10, "101.50")
    grown = sim.get_open_position("NVDA")
    assert _d(grown.qty) == 20 and _d(grown.avg_entry_price) == 101
    assert _d(grown.qty_available) == 10
    assert _d(sim.inspect_order(stop.id).qty) == 10

    sim.open_position("AMD", -5, "200")
    short = sim.get_open_position("AMD")
    assert short.side is PositionSide.SHORT
    assert _d(short.qty) == -5 and _d(short.qty_available) == -5
    assert _d(short.market_value) == -1000

    with pytest.raises(ValueError, match="non-zero"):
        sim.open_position("NVDA", 0, "100")
    with pytest.raises(ValueError, match="positive"):
        sim.open_position("NVDA", 1, "0")


# --------------------------------------------------------------------------- call log


@OFFLINE
def test_call_log_classes_every_round_trip_as_read_or_mutating() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100")
    sim.open_position("NVDA", 10, "100")
    sim.set_asset("NVDA", attributes=["overnight_tradable"])
    sim.advance(1)
    assert sim.call_log == []  # scripting helpers are not broker calls

    sim.get_account()
    sim.get_clock()
    sim.get_all_positions()
    sim.get_open_position("NVDA")
    sim.get_asset("NVDA")
    assert (sim.read_call_count, sim.mutating_call_count) == (5, 0)

    order = sim.submit_order(
        _limit("NVDA", OrderSide.SELL, 10.0, 105.0, tif=TimeInForce.GTC, cid="exit-1")
    )
    sim.get_order_by_id(order.id)
    sim.get_order_by_client_id("exit-1")
    sim.get_orders()
    successor = sim.replace_order_by_id(order.id, ReplaceOrderRequest(limit_price=104.0))
    sim.complete_replace(successor.id)  # a scripting helper: the replacement takes effect
    sim.cancel_order_by_id(successor.id)
    sim.cancel_orders()
    sim.close_position("NVDA")
    sim.close_all_positions(cancel_orders=True)
    sim.get(FILLS, {"direction": "asc"})

    assert [call.name for call in sim.calls(kind="mutating")] == [
        "submit_order",
        "replace_order_by_id",
        "cancel_order_by_id",
        "cancel_orders",
        "close_position",
        "close_all_positions",
    ]
    assert [call.verb for call in sim.calls(kind="mutating")] == [
        "POST",
        "PATCH",
        "DELETE",
        "DELETE",
        "DELETE",
        "DELETE",
    ]
    assert (sim.read_call_count, sim.mutating_call_count) == (9, 6)
    assert {call.kind for call in sim.call_log} == {"read", "mutating"}
    assert all(call.verb == "GET" for call in sim.calls(kind="read"))
    assert [call.seq for call in sim.call_log] == list(range(1, 16))
    assert sim.calls("get_account_activities")[0].path == FILLS

    submitted = sim.calls("submit_order")[0]
    assert submitted.outcome == "ok" and submitted.at == sim.now
    assert (
        submitted.payload["symbol"] == "NVDA" and submitted.payload["client_order_id"] == "exit-1"
    )
    assert type(submitted.payload["side"]) is str and submitted.payload["side"] == "sell"
    assert sim.calls("replace_order_by_id")[0].payload == {"limit_price": 104.0}

    # Refused attempts and raw transport calls are counted like any other mutation.
    with pytest.raises(APIError):
        sim.submit_order(_market("NVDA", OrderSide.BUY, 1, cid="exit-1"))
    raw = sim.post(
        "/orders",
        {"symbol": "NVDA", "qty": "1", "side": "buy", "type": "market", "time_in_force": "day"},
    )
    assert isinstance(raw, dict) and raw["status"] == "new"
    assert sim.mutating_call_count == 8
    assert [call.outcome for call in sim.calls("submit_order")] == ["ok", "error:422", "ok"]

    sim.reset_call_log()
    assert sim.call_log == [] and (sim.read_call_count, sim.mutating_call_count) == (0, 0)


@OFFLINE
def test_concurrent_callers_are_serialised_and_open_order_reads_are_paged() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100")

    def work(worker: int) -> list[UUID]:
        return [
            sim.submit_order(
                _limit("NVDA", OrderSide.BUY, 1, 90.0, tif=TimeInForce.GTC, cid=f"w{worker}-{i}")
            ).id
            for i in range(20)
        ]

    with ThreadPoolExecutor(max_workers=8) as pool:
        order_ids = [order_id for chunk in pool.map(work, range(8)) for order_id in chunk]

    assert len(set(order_ids)) == 160
    assert sim.mutating_call_count == 160
    assert [call.seq for call in sim.call_log] == list(range(1, 161))
    # The API pages open orders at 50 unless the caller asks for more (500 is the ceiling).
    assert len(sim.get_orders()) == 50
    assert len(sim.get_orders(GetOrdersRequest(limit=500))) == 160
    with pytest.raises(APIError) as too_many:
        sim.get("/orders", {"limit": 501})
    assert too_many.value.status_code == 422


@OFFLINE
def test_fill_activity_feed_pages_the_way_the_harness_reads_it() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100")
    started = sim.now
    orders = []
    for index in range(3):
        sim.advance(1)
        orders.append(sim.submit_order(_market("NVDA", OrderSide.BUY, 1, cid=f"fill-{index}")))

    params = {"after": started.isoformat(), "direction": "asc", "page_size": "2"}
    first_page = sim.get(FILLS, params)
    assert [fill["order_id"] for fill in first_page] == [str(order.id) for order in orders[:2]]
    second_page = sim.get(FILLS, params | {"page_token": first_page[-1]["id"]})
    assert [fill["order_id"] for fill in second_page] == [str(orders[2].id)]
    assert sim.get(FILLS, params | {"page_token": second_page[-1]["id"]}) == []
    assert sim.get(FILLS, {"after": sim.now.isoformat(), "direction": "asc"}) == []
    newest_first = sim.get(FILLS, {})
    assert [fill["order_id"] for fill in newest_first] == [str(o.id) for o in reversed(orders)]

    fill = first_page[0]
    assert set(fill) >= {
        "id",
        "order_id",
        "symbol",
        "side",
        "qty",
        "cum_qty",
        "price",
        "transaction_time",
    }
    assert (fill["symbol"], fill["side"]) == ("NVDA", "buy")
    assert Decimal(fill["qty"]) == 1 and Decimal(fill["cum_qty"]) == 1
    assert Decimal(fill["price"]) == 100
    assert datetime.fromisoformat(fill["transaction_time"]) == started + timedelta(seconds=1)
    # The order behind a fill is retrievable by the id the feed reports.
    assert sim.get_order_by_id(fill["order_id"]).client_order_id == "fill-0"


@OFFLINE
def test_cancel_all_then_close_all_positions_flattens_only_when_the_market_can_fill() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", -5, "200")
    sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    sim.submit_order(_stop("AMD", OrderSide.BUY, 5.0, 204.0))

    # With the shares reserved by resting stops, a close is refused per symbol.
    refused = sim.close_all_positions()
    assert sorted((item.symbol, item.status) for item in refused) == [("AMD", 403), ("NVDA", 403)]
    assert len(sim.get_all_positions()) == 2

    canceled = sim.cancel_orders()
    assert len(canceled) == 2 and {item.status for item in canceled} == {200}
    closed = sim.close_all_positions(cancel_orders=True)
    assert sorted((item.symbol, item.status) for item in closed) == [("AMD", 200), ("NVDA", 200)]
    assert all(isinstance(item.body, Order) for item in closed)
    assert sim.get_orders() == [] and sim.get_all_positions() == []

    # Off-hours the liquidating market orders are only queued: the account is not flat yet.
    late = FaultInjectingAlpacaSim(session="AFTER_HOURS")
    late.open_position("NVDA", 10, "100")
    late.close_all_positions(cancel_orders=True)
    (queued,) = late.get_orders()
    assert queued.status is OrderStatus.ACCEPTED and queued.type is OrderType.MARKET
    assert _d(late.get_open_position("NVDA").qty) == 10
    late.set_session("RTH")
    assert late.get_all_positions() == []
    with pytest.raises(APIError) as flat:
        late.close_position("NVDA")
    assert flat.value.status_code == 404


# ------------------------------------------------------------------ harness round trip


@OFFLINE
def test_harness_lane_entry_and_guard_trail_against_the_simulator(tmp_path: Path) -> None:
    # The lane code in this tree reads a replace acknowledgement as the working stop. That is
    # the assumption ``replace_pending`` exists to expose, so this round trip states it.
    sim = FaultInjectingAlpacaSim(replace_pending="never")
    sim.set_price("NVDA", "100.00")
    lane = pf.AlpacaPaperLane("ALPACA_PA1", tmp_path, sim, pf.CrossLaneSymbolLock())

    result = lane.submit_guarded_entry(
        symbol="NVDA",
        side="LONG",
        ref_price=Decimal("100.00"),
        session="RTH",
        client_order_id="noesis-sim-entry",
    )
    assert result is not None and result["protective_stop_mode"] == "NATIVE_OTO_STOP"
    initial_stop = _d(result["stop_price"])
    (entry_call,) = sim.calls("submit_order")
    assert entry_call.payload["order_class"] == "oto"
    assert entry_call.payload["stop_loss"] == {"stop_price": float(initial_stop)}

    parent = sim.get_order_by_id(result["broker_order_id"], GetOrderByIdRequest(nested=True))
    assert parent.status is OrderStatus.FILLED and _d(parent.filled_qty) == result["qty"]
    guard = Guard(
        lane,
        "noesis-sim-entry",
        "NVDA",
        "LONG",
        initial_stop,
        initial_stop,
        Decimal("0.015"),
        "RTH",
        False,
        parent,
    )
    guard.fetch()
    assert guard.child is not None and guard.child.status.value == "new"
    assert _d(guard.child.stop_price) == initial_stop
    assert _d(sim.get_open_position("NVDA").qty_available) == 0

    # A favourable move makes the harness tighten the native stop through a real replace.
    sim.set_price("NVDA", "104.00")
    guard.on_price(Decimal("104.00"), "RTH")
    assert guard.stop > initial_stop
    assert len(sim.calls("replace_order_by_id")) == 1
    (working_stop,) = sim.inspect_orders("open", "NVDA")
    assert _d(working_stop.stop_price) == guard.stop and working_stop.replaces is not None
    assert _d(sim.get_open_position("NVDA").qty_available) == 0

    # The broker, not the harness, executes the tightened stop.
    mutations = sim.mutating_call_count
    sim.set_price("NVDA", str(guard.stop - Decimal("0.01")))
    assert sim.inspect_order(working_stop.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None
    assert sim.mutating_call_count == mutations


# ------------------------------------------------------------- no_live_broker_access


def test_no_live_broker_access_is_opt_in(request: pytest.FixtureRequest) -> None:
    assert "no_live_broker_access" not in request.fixturenames
    # Earlier guarded tests left no patch behind on the socket class.
    assert "connect" not in vars(socket.socket) and "connect_ex" not in vars(socket.socket)


def test_no_live_broker_access_fails_a_test_that_sees_a_live_looking_key(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    looks_live = "AK" + "Z9" * 9
    monkeypatch.setenv("TIER0_SIM_PROBE_KEY", looks_live)
    with pytest.raises(pytest.fail.Exception, match="TIER0_SIM_PROBE_KEY") as failure:
        request.getfixturevalue("no_live_broker_access")
    assert looks_live not in str(failure.value)  # the name is reported, never the value


def test_no_live_broker_access_matches_only_live_shaped_values(
    no_live_broker_access, monkeypatch: pytest.MonkeyPatch
) -> None:
    guard = no_live_broker_access
    assert guard.live_key_env_names() == []
    monkeypatch.setenv("TIER0_SIM_PROBE_KEY", "AK" + "Z9" * 8)
    assert guard.live_key_env_names() == ["TIER0_SIM_PROBE_KEY"]
    monkeypatch.delenv("TIER0_SIM_PROBE_KEY")
    assert guard.live_key_env_names() == []
    assert (
        guard.live_key_env_names(
            {
                "TOO_SHORT": "AK" + "Z" * 15,
                "PAPER": "PK" + "Z" * 18,
                "LOWER": "ak" + "z" * 18,
                "EMBEDDED": "prefix-AK" + "Z" * 18,
            }
        )
        == []
    )


def test_no_live_broker_access_blocks_non_loopback_connects(no_live_broker_access) -> None:
    guard = no_live_broker_access

    # 192.0.2.1 is TEST-NET-1 (never routed); the guard refuses before any packet is sent.
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock,
        pytest.raises(ConnectionError, match="non-loopback"),
    ):
        sock.connect(("192.0.2.1", 443))
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock,
        pytest.raises(ConnectionError, match="non-loopback"),
    ):
        sock.connect_ex(("broker.invalid", 443))
    with pytest.raises(ConnectionError, match="non-loopback"):
        socket.create_connection(("192.0.2.1", 443), timeout=1)
    assert len(guard.blocked_connects) == 3
    guard.blocked_connects.clear()  # provoked on purpose; acknowledge so teardown passes

    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
        with socket.create_connection(("127.0.0.1", port), timeout=5) as client:
            assert client.getpeername()[1] == port
    assert guard.blocked_connects == []
    assert guard.is_loopback(("localhost", 80)) and guard.is_loopback(("::1", 80, 0, 0))
    assert not guard.is_loopback(("10.0.0.5", 80)) and not guard.is_loopback(("example.com", 80))


# ------------------------------------------------------------------ wash-trade models

_ALWAYS = "always rejected"
_PRICED = "rejected if buy limit price >= sell limit price"
_MARKET_STOP_REASON = "opposite side market/stop order exists"
_LIMIT_REASON = "opposite side limit order exists"
_ORDER_KINDS = ("market", "limit", "stop", "stop_limit")
# The self-trade table of Alpaca's "User Protection" page (updated 2026-11-24), row by row:
# existing open order (type, side) | type of the new opposite-side order | result.
_DOCUMENTED_WASH_TABLE: tuple[tuple[str, str, str, str], ...] = (
    ("market", "buy", "market", _ALWAYS),
    ("market", "buy", "limit", _ALWAYS),
    ("market", "buy", "stop", _ALWAYS),
    ("market", "buy", "stop_limit", _ALWAYS),
    ("market", "sell", "market", _ALWAYS),
    ("market", "sell", "limit", _ALWAYS),
    ("market", "sell", "stop", _ALWAYS),
    ("market", "sell", "stop_limit", _ALWAYS),
    ("stop", "buy", "market", _ALWAYS),
    ("stop", "buy", "limit", _ALWAYS),
    ("stop", "buy", "stop", _ALWAYS),
    ("stop", "buy", "stop_limit", _ALWAYS),
    ("stop", "sell", "market", _ALWAYS),
    ("stop", "sell", "limit", _ALWAYS),
    ("stop", "sell", "stop", _ALWAYS),
    ("stop", "sell", "stop_limit", _ALWAYS),
    ("limit", "buy", "market", _ALWAYS),
    ("limit", "buy", "stop", _ALWAYS),
    ("limit", "buy", "limit", _PRICED),
    ("limit", "buy", "stop_limit", _PRICED),
    ("limit", "sell", "market", _ALWAYS),
    ("limit", "sell", "stop", _ALWAYS),
    ("limit", "sell", "limit", _PRICED),
    ("limit", "sell", "stop_limit", _PRICED),
    ("stop_limit", "buy", "market", _ALWAYS),
    ("stop_limit", "buy", "stop", _ALWAYS),
    ("stop_limit", "buy", "limit", _PRICED),
    ("stop_limit", "buy", "stop_limit", _PRICED),
    ("stop_limit", "sell", "market", _ALWAYS),
    ("stop_limit", "sell", "stop", _ALWAYS),
    ("stop_limit", "sell", "limit", _PRICED),
    ("stop_limit", "sell", "stop_limit", _PRICED),
)
_WASH_ROW_IDS = [f"{kind}-{side}-then-{new}" for kind, side, new, _ in _DOCUMENTED_WASH_TABLE]
_WASH_ROW_ARGS = ("existing_type", "existing_side", "new_type", "rule")


def _typed(
    kind: str, side: OrderSide, *, limit: float = 100.0, qty: float = 1, symbol: str = "NVDA"
) -> OrderRequest:
    """A GTC order of ``kind``; ``limit`` is used only where the type has a limit price.

    Stop prices never enter the self-trade table, so they sit on the passive side of 100.
    """
    tif = TimeInForce.GTC
    stop = 110.0 if side is OrderSide.BUY else 90.0
    if kind == "market":
        return MarketOrderRequest(symbol=symbol, qty=qty, side=side, time_in_force=tif)
    if kind == "limit":
        return LimitOrderRequest(
            symbol=symbol, qty=qty, side=side, time_in_force=tif, limit_price=limit
        )
    if kind == "stop":
        return StopOrderRequest(
            symbol=symbol, qty=qty, side=side, time_in_force=tif, stop_price=stop
        )
    assert kind == "stop_limit", kind
    return StopLimitOrderRequest(
        symbol=symbol, qty=qty, side=side, time_in_force=tif, limit_price=limit, stop_price=stop
    )


def _trailing(
    symbol: str,
    side: OrderSide,
    qty: float,
    *,
    percent: float | None = None,
    price: float | None = None,
    tif: TimeInForce = TimeInForce.GTC,
    cid: str | None = None,
) -> TrailingStopOrderRequest:
    return TrailingStopOrderRequest(
        symbol=symbol,
        qty=qty,
        side=side,
        time_in_force=tif,
        trail_percent=percent,
        trail_price=price,
        client_order_id=cid,
    )


def _resting_book(model: WashTradeModel, **switches: Any) -> FaultInjectingAlpacaSim:
    """Long 10 NVDA at 100 with matching switched off: every accepted order rests as ``new``.

    Unless a test says otherwise the forum-level refusal body is served, because it names the
    order behind a refusal, and replacements take effect at once.
    """
    switches.setdefault("refusal_detail", "forum")
    switches.setdefault("replace_pending", "never")
    sim = FaultInjectingAlpacaSim(auto_fill=False, wash_trade_model=model, **switches)
    sim.open_position("NVDA", 10, "100")
    return sim


def _assert_wash_rejected(
    sim: FaultInjectingAlpacaSim, request: OrderRequest, existing: Order, reason: str
) -> None:
    orders = len(sim.inspect_orders("all"))
    with pytest.raises(APIError) as wash:
        sim.submit_order(request)
    assert wash.value.status_code == 403 and wash.value.code == 40310000
    assert sim.call_log[-1].outcome == "error:403" and sim.call_log[-1].refusal == "self_trade"
    if sim.refusal_detail == "forum":
        assert wash.value.message == FORUM_WASH_MESSAGE
        detail = _refused(wash)
        assert detail["existing_order_id"] == str(existing.id)
        assert detail["reject_reason"] == reason
    else:
        assert _refused(wash) == {"code": 40310000, "message": "forbidden"}
    assert len(sim.inspect_orders("all")) == orders  # the refused order was never created


def _wash_cases(rule: str) -> list[tuple[float, float, bool]]:
    """``(buy limit, sell limit, rejected)`` cases that exercise one row of the table."""
    if rule == _ALWAYS:
        # 95 against 105 cannot cross, and the row rejects all the same.
        return [(95.0, 105.0, True)]
    # Each side of the documented boundary "buy limit price >= sell limit price".
    return [(99.99, 100.0, False), (100.0, 100.0, True), (100.01, 100.0, True)]


def _run_wash_row(
    model: WashTradeModel,
    existing_type: str,
    existing_side: str,
    new_type: str,
    cases: list[tuple[float, float, bool]],
) -> None:
    buy_first = existing_side == "buy"
    old_side, new_side = (
        (OrderSide.BUY, OrderSide.SELL) if buy_first else (OrderSide.SELL, OrderSide.BUY)
    )
    reason = _MARKET_STOP_REASON if existing_type in {"market", "stop"} else _LIMIT_REASON
    for buy_limit, sell_limit, rejected in cases:
        old_limit, new_limit = (buy_limit, sell_limit) if buy_first else (sell_limit, buy_limit)
        sim = _resting_book(model)
        existing = sim.submit_order(_typed(existing_type, old_side, limit=old_limit))
        assert existing.status is OrderStatus.NEW
        request = _typed(new_type, new_side, limit=new_limit)
        if rejected:
            _assert_wash_rejected(sim, request, existing, reason)
        else:
            assert sim.submit_order(request).status is OrderStatus.NEW


@OFFLINE
@pytest.mark.parametrize(_WASH_ROW_ARGS, _DOCUMENTED_WASH_TABLE, ids=_WASH_ROW_IDS)
def test_documented_wash_trade_model_follows_every_row_of_the_published_table(
    existing_type: str, existing_side: str, new_type: str, rule: str
) -> None:
    _run_wash_row("documented", existing_type, existing_side, new_type, _wash_cases(rule))


@OFFLINE
@pytest.mark.parametrize(_WASH_ROW_ARGS, _DOCUMENTED_WASH_TABLE, ids=_WASH_ROW_IDS)
def test_assumed_wash_trade_model_is_unchanged_and_differs_only_for_a_new_stop(
    existing_type: str, existing_side: str, new_type: str, rule: str
) -> None:
    # The original model lets a new stop order through against a resting limit / stop-limit
    # order (four rows of the table); every other row gives the documented answer.
    lenient = new_type == "stop" and existing_type in {"limit", "stop_limit"}
    cases = [(buy, sell, rejected and not lenient) for buy, sell, rejected in _wash_cases(rule)]
    _run_wash_row("assumed", existing_type, existing_side, new_type, cases)


@OFFLINE
def test_wash_trade_model_selector_and_its_class_level_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert WASH_TRADE_MODELS == ("assumed", "documented")
    assert len(_DOCUMENTED_WASH_TABLE) == 32
    assert {row[:3] for row in _DOCUMENTED_WASH_TABLE} == {
        (kind, side, new)
        for kind in _ORDER_KINDS
        for side in ("buy", "sell")
        for new in _ORDER_KINDS
    }

    # The default lives in one class attribute: the published table. This is its only pin.
    default = FaultInjectingAlpacaSim.default_wash_trade_model
    assert default == "documented"
    assert FaultInjectingAlpacaSim().wash_trade_model == default
    for explicit in WASH_TRADE_MODELS:
        assert FaultInjectingAlpacaSim(wash_trade_model=explicit).wash_trade_model == explicit
    bogus: Any = "strict"
    with pytest.raises(ValueError, match="wash_trade_model"):
        FaultInjectingAlpacaSim(wash_trade_model=bogus)
    # A scratch run flips every later simulator by assigning the class attribute; an explicit
    # argument still wins.
    for flipped in WASH_TRADE_MODELS:
        with monkeypatch.context() as scratch:
            scratch.setattr(FaultInjectingAlpacaSim, "default_wash_trade_model", flipped)
            assert FaultInjectingAlpacaSim().wash_trade_model == flipped
            for explicit in WASH_TRADE_MODELS:
                assert (
                    FaultInjectingAlpacaSim(wash_trade_model=explicit).wash_trade_model == explicit
                )
    assert FaultInjectingAlpacaSim().wash_trade_model == default

    # The cell where the models part: a protective sell stop against a resting buy limit.
    def book(
        model: WashTradeModel | None, *, protection: bool = True
    ) -> tuple[FaultInjectingAlpacaSim, Order]:
        sim = FaultInjectingAlpacaSim(
            wash_trade_model=model, wash_trade_protection=protection, refusal_detail="forum"
        )
        sim.open_position("AAPL", 10, "100")
        return sim, sim.submit_order(_limit("AAPL", OrderSide.BUY, 5, 95.0, tif=TimeInForce.GTC))

    protective = _stop("AAPL", OrderSide.SELL, 10.0, 90.0)
    assumed, _ = book("assumed")
    assert assumed.submit_order(protective).status is OrderStatus.NEW
    for model in ("documented", None):  # None: the default, which is the documented table
        documented, buy = book(model)
        _assert_wash_rejected(documented, protective, buy, _LIMIT_REASON)
        # ... which the owner's native trailing sell is exempt from.
        trail = documented.submit_order(_trailing("AAPL", OrderSide.SELL, 10, percent=1.0))
        assert trail.status is OrderStatus.NEW
    # Switching protection off overrides either model.
    unprotected, _ = book("documented", protection=False)
    assert unprotected.submit_order(protective).status is OrderStatus.NEW


@OFFLINE
def test_documented_wash_trade_model_scope_statuses_and_replace() -> None:
    sim = _resting_book("documented")
    stop = sim.submit_order(_typed("stop", OrderSide.SELL))
    # Same side and other symbols never collide.
    assert sim.submit_order(_typed("limit", OrderSide.SELL, limit=105.0)).status is OrderStatus.NEW
    sim.open_position("AMD", 10, "100")
    other = sim.submit_order(_typed("market", OrderSide.BUY, symbol="AMD"))
    assert other.status is OrderStatus.NEW
    _assert_wash_rejected(
        sim, _typed("limit", OrderSide.BUY, limit=80.0), stop, _MARKET_STOP_REASON
    )
    # A closed order is not an existing open order any more.
    sim.cancel_orders()
    assert sim.submit_order(_typed("limit", OrderSide.BUY, limit=80.0)).status is OrderStatus.NEW

    # An order accepted for a later session is an existing open order (assumption: the page
    # names neither statuses nor sessions; the simulator's notion of an open order is used).
    shut = FaultInjectingAlpacaSim(session="AFTER_HOURS", refusal_detail="forum")
    shut.open_position("NVDA", 10, "100")
    queued = shut.submit_order(_typed("limit", OrderSide.BUY, limit=95.0))
    assert queued.status is OrderStatus.ACCEPTED
    _assert_wash_rejected(shut, _typed("stop", OrderSide.SELL), queued, _LIMIT_REASON)
    _assert_wash_rejected(shut, _typed("market", OrderSide.SELL), queued, _LIMIT_REASON)

    # "Every time a customer places an order": a replace is checked again, against every open
    # order but the one it replaces, and a refused replace leaves the original working.
    book = _resting_book("documented")
    bid = book.submit_order(_typed("limit", OrderSide.BUY, limit=95.0))
    ask = book.submit_order(_typed("limit", OrderSide.SELL, limit=105.0))
    with pytest.raises(APIError) as crossed:
        book.replace_order_by_id(ask.id, ReplaceOrderRequest(limit_price=95.0))
    assert crossed.value.status_code == 403 and crossed.value.code == 40310000
    assert _refused(crossed)["existing_order_id"] == str(bid.id)
    assert book.call_log[-1].refusal == "self_trade"
    assert book.inspect_order(ask.id).status is OrderStatus.NEW
    moved = book.replace_order_by_id(ask.id, ReplaceOrderRequest(limit_price=95.01))
    assert moved.status is OrderStatus.NEW and _d(moved.limit_price) == Decimal("95.01")

    # A replacement that has not taken effect yet is an open order too: its price counts.
    waiting = _resting_book("documented", replace_pending="until_completed")
    ask = waiting.submit_order(_typed("limit", OrderSide.SELL, limit=105.0))
    lower = waiting.replace_order_by_id(ask.id, ReplaceOrderRequest(limit_price=101.0))
    assert lower.status is OrderStatus.PENDING_NEW
    below = waiting.submit_order(_typed("limit", OrderSide.BUY, limit=100.99))
    assert below.status is OrderStatus.NEW
    _assert_wash_rejected(
        waiting, _typed("limit", OrderSide.BUY, limit=101.0), lower, _LIMIT_REASON
    )


@OFFLINE
@pytest.mark.parametrize("model", WASH_TRADE_MODELS)
def test_trailing_stops_are_exempt_from_wash_trade_checks_as_existing_and_as_new_order(
    model: WashTradeModel,
) -> None:
    # The default, as the documentation sentence reads.
    assert FaultInjectingAlpacaSim().trailing_wash_exempt is True
    # Existing trailing sell: no buy of any type is refused (a plain stop sell refuses them all).
    sim = _resting_book(model)
    sim.submit_order(_trailing("NVDA", OrderSide.SELL, 5, percent=1.0))
    for kind in _ORDER_KINDS:
        assert sim.submit_order(_typed(kind, OrderSide.BUY, limit=105.0)).status is OrderStatus.NEW

    for kind in _ORDER_KINDS:
        # New trailing sell: no resting buy of any type refuses it, nor its replacement ...
        sim = _resting_book(model)
        buy = sim.submit_order(_typed(kind, OrderSide.BUY, limit=95.0))
        trail = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 5, percent=1.0))
        assert trail.status is OrderStatus.NEW
        tighter = sim.replace_order_by_id(trail.id, ReplaceOrderRequest(trail=0.5))
        assert tighter.status is OrderStatus.NEW
        # ... while that same buy refuses a market sell under either model.
        reason = _MARKET_STOP_REASON if kind in {"market", "stop"} else _LIMIT_REASON
        _assert_wash_rejected(sim, _typed("market", OrderSide.SELL), buy, reason)

    # The mirror for a short: a trailing buy among resting sells, existing and new.
    short = FaultInjectingAlpacaSim(auto_fill=False, wash_trade_model=model)
    short.open_position("NVDA", -10, "100")
    cover = short.submit_order(_trailing("NVDA", OrderSide.BUY, 5, price=2.0))
    assert cover.status is OrderStatus.NEW
    for kind in _ORDER_KINDS:
        assert (
            short.submit_order(_typed(kind, OrderSide.SELL, limit=95.0)).status is OrderStatus.NEW
        )
    second = short.submit_order(_trailing("NVDA", OrderSide.BUY, 5, percent=1.0))
    assert second.status is OrderStatus.NEW


@OFFLINE
@pytest.mark.parametrize("model", WASH_TRADE_MODELS)
def test_trailing_stop_is_refused_as_a_stop_order_when_its_exemption_is_switched_off(
    model: WashTradeModel,
) -> None:
    # The exemption rests on one sentence and neither direction was ever observed, so a lane
    # test can run against a broker that does not honour it: a trailing stop is then a stop
    # order to the self-trade table.
    trailing_sell = _trailing("NVDA", OrderSide.SELL, 5, percent=1.0)

    # The lane's trailing sell comes second, after a resting buy of each type.
    for kind in _ORDER_KINDS:
        sim = _resting_book(model, trailing_wash_exempt=False)
        buy = sim.submit_order(_typed(kind, OrderSide.BUY, limit=95.0))
        if model == "assumed" and kind in {"limit", "stop_limit"}:
            # The assumed model's lenient cell: a new stop against a resting limit order.
            assert sim.submit_order(trailing_sell).status is OrderStatus.NEW
            continue
        reason = _MARKET_STOP_REASON if kind in {"market", "stop"} else _LIMIT_REASON
        _assert_wash_rejected(sim, trailing_sell, buy, reason)
        assert _d(sim.get_open_position("NVDA").qty_available) == 10  # nothing covers the shares

    # The lane's trailing sell rests first: it then refuses a buy of every type.
    for kind in _ORDER_KINDS:
        sim = _resting_book(model, trailing_wash_exempt=False)
        trail = sim.submit_order(trailing_sell)
        assert trail.status is OrderStatus.NEW
        _assert_wash_rejected(
            sim, _typed(kind, OrderSide.BUY, limit=95.0), trail, _MARKET_STOP_REASON
        )

    # The mirror for a short position: a trailing buy and the opposite-side sells.
    short = FaultInjectingAlpacaSim(
        auto_fill=False,
        wash_trade_model=model,
        trailing_wash_exempt=False,
        refusal_detail="forum",
    )
    short.open_position("NVDA", -10, "100")
    ask = short.submit_order(_typed("stop", OrderSide.SELL, limit=95.0))
    _assert_wash_rejected(
        short, _trailing("NVDA", OrderSide.BUY, 5, price=2.0), ask, _MARKET_STOP_REASON
    )

    # The switch is an attribute: a replacement of a trailing stop that was accepted while the
    # exemption held is checked once it is gone ("every time a customer places an order").
    sim = _resting_book("documented")
    buy = sim.submit_order(_typed("limit", OrderSide.BUY, limit=95.0))
    trail = sim.submit_order(trailing_sell)
    sim.trailing_wash_exempt = False
    with pytest.raises(APIError) as refused:
        sim.replace_order_by_id(trail.id, ReplaceOrderRequest(trail=0.5))
    assert refused.value.status_code == 403
    assert _refused(refused)["existing_order_id"] == str(buy.id)
    assert sim.inspect_order(trail.id).status is OrderStatus.NEW

    # With the default body the trailing refusal carries nothing but the documented status.
    plain = _resting_book("documented", trailing_wash_exempt=False, refusal_detail="documented")
    buy = plain.submit_order(_typed("limit", OrderSide.BUY, limit=95.0))
    _assert_wash_rejected(plain, trailing_sell, buy, _LIMIT_REASON)


def _armed_oto(model: WashTradeModel, **switches: Any) -> tuple[FaultInjectingAlpacaSim, Order]:
    """A filled OTO buy of 5 NVDA at 100 whose stop-loss sell leg (96) is working."""
    sim = FaultInjectingAlpacaSim(
        wash_trade_model=model, refusal_detail="forum", replace_pending="never", **switches
    )
    sim.set_price("NVDA", "100")
    parent = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 5, 100.50, tif=TimeInForce.GTC, stop_loss=96.0)
    )
    (leg,) = _legs(sim.get_order_by_id(parent.id, GetOrderByIdRequest(nested=True)))
    assert leg.status is OrderStatus.NEW and leg.type is OrderType.STOP
    return sim, leg


@OFFLINE
def test_complex_order_leg_is_exempt_only_within_its_own_order_by_default() -> None:
    assert LEG_WASH_EXEMPTIONS == ("own_order", "any_order")
    assert FaultInjectingAlpacaSim().complex_leg_wash_exemption == "own_order"
    add = _limit("NVDA", OrderSide.BUY, 1, 95.0, tif=TimeInForce.GTC)

    # The armed stop-loss leg as the existing order: an add or re-entry buy from outside its
    # complex order is refused ("stop sell | limit buy: always rejected"), as under "assumed".
    for model in WASH_TRADE_MODELS:
        sim, leg = _armed_oto(model)
        _assert_wash_rejected(sim, add, leg, _MARKET_STOP_REASON)

    # The leg as the new order: its replacement is checked against an outside buy. Both can
    # only rest together if the buy got in while legs were exempt everywhere.
    sim, leg = _armed_oto("documented", complex_leg_wash_exemption="any_order")
    resting = sim.submit_order(add)
    sim.complex_leg_wash_exemption = "own_order"
    with pytest.raises(APIError) as refused:
        sim.replace_order_by_id(leg.id, ReplaceOrderRequest(stop_price=97.0))
    assert refused.value.status_code == 403
    assert _refused(refused)["existing_order_id"] == str(resting.id)
    assert sim.inspect_order(leg.id).status is OrderStatus.NEW  # the leg keeps its old stop
    assert sim.call_log[-1].refusal == "self_trade"

    # A leg is checked when its complex order is submitted: with an outside buy resting, the
    # stop-loss sell leg of a new OTO entry collides with it and the whole order is refused.
    sim = FaultInjectingAlpacaSim(refusal_detail="forum")
    sim.set_price("NVDA", "100")
    outside = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 95.0, tif=TimeInForce.GTC))
    entry_with_leg = _limit("NVDA", OrderSide.BUY, 5, 99.0, tif=TimeInForce.GTC, stop_loss=94.0)
    _assert_wash_rejected(sim, entry_with_leg, outside, _LIMIT_REASON)
    assert [order.id for order in sim.inspect_orders("all")] == [outside.id]  # no entry, no leg
    # The assumed model never looked at a leg before it was armed, and still does not: not even
    # a resting buy stop, which refuses a plain stop sell under that model, stands in the way.
    lenient = FaultInjectingAlpacaSim(wash_trade_model="assumed")
    lenient.set_price("NVDA", "100")
    lenient.submit_order(_stop("NVDA", OrderSide.BUY, 1.0, 110.0))
    assert lenient.submit_order(entry_with_leg).status is OrderStatus.NEW

    # A held leg (open, not yet at the exchange) is an existing open order as well, and the
    # entry order of a complex order is checked like a simple order.
    sim = FaultInjectingAlpacaSim(refusal_detail="forum", replace_pending="never")
    sim.open_position("NVDA", 5, "100")
    entry = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 5, 99.0, tif=TimeInForce.GTC, stop_loss=95.0)
    )
    (held,) = _legs(entry)
    assert entry.status is OrderStatus.NEW and held.status is OrderStatus.HELD
    _assert_wash_rejected(sim, _limit("NVDA", OrderSide.BUY, 1, 94.0), held, _MARKET_STOP_REASON)
    _assert_wash_rejected(sim, _stop("NVDA", OrderSide.SELL, 5.0, 96.0), entry, _LIMIT_REASON)
    # Inside its own complex order the leg collides with nothing: the entry was accepted next to
    # its opposite-side leg, and the entry's replacement is too.
    repriced = sim.replace_order_by_id(entry.id, ReplaceOrderRequest(limit_price=98.0))
    assert repriced.status is OrderStatus.NEW and _legs(repriced)[0].id == held.id

    wrong: Any = "all"
    with pytest.raises(ValueError, match="complex_leg_wash_exemption"):
        FaultInjectingAlpacaSim(complex_leg_wash_exemption=wrong)


@OFFLINE
def test_any_order_leg_exemption_exempts_oto_legs_both_ways_and_assumed_does_not() -> None:
    add = _limit("NVDA", OrderSide.BUY, 1, 95.0, tif=TimeInForce.GTC)

    # Assumed: the armed stop-loss leg is an ordinary resting stop and refuses the buy; the
    # leg switch does not reach that model.
    sim, leg = _armed_oto("assumed", complex_leg_wash_exemption="any_order")
    _assert_wash_rejected(sim, add, leg, _MARKET_STOP_REASON)

    # Documented with the wide reading, leg as the existing order: the leg refuses nothing.
    sim, leg = _armed_oto("documented", complex_leg_wash_exemption="any_order")
    resting = sim.submit_order(add)
    assert resting.status is OrderStatus.NEW
    # Leg as the new order: its replacement goes through.
    tightened = sim.replace_order_by_id(leg.id, ReplaceOrderRequest(stop_price=97.0))
    assert tightened.status is OrderStatus.NEW and tightened.replaces == leg.id
    # A simple stop sell for one more share is refused by that same resting buy.
    sim.open_position("NVDA", 1, "100")
    _assert_wash_rejected(sim, _stop("NVDA", OrderSide.SELL, 1.0, 96.0), resting, _LIMIT_REASON)

    # A held leg is exempt too and is not checked at the submit of its complex order, but the
    # entry order is not a leg: it is checked like a simple order.
    sim = FaultInjectingAlpacaSim(complex_leg_wash_exemption="any_order", refusal_detail="forum")
    sim.open_position("NVDA", 5, "100")
    outside = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 94.0, tif=TimeInForce.GTC))
    entry = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 5, 99.0, tif=TimeInForce.GTC, stop_loss=95.0)
    )
    assert entry.status is OrderStatus.NEW and _legs(entry)[0].status is OrderStatus.HELD
    assert sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 93.0)).status is OrderStatus.NEW
    _assert_wash_rejected(sim, _stop("NVDA", OrderSide.SELL, 5.0, 96.0), outside, _LIMIT_REASON)


@OFFLINE
@pytest.mark.parametrize("order_class", ["bracket", "oco"])
def test_leg_exemption_scope_covers_armed_legs_of_any_complex_order(order_class: str) -> None:
    # The simulator does not accept bracket or OCO orders yet, so armed exit legs of each class
    # are seeded straight into its book (a simplified shape: a filled parent with two legs).
    # The exemption is keyed on being a leg, not on the OTO class.
    def seeded(scope: str) -> tuple[FaultInjectingAlpacaSim, UUID, UUID]:
        sim = _resting_book("documented", complex_leg_wash_exemption=scope)
        with sim._lock:
            parent = sim._new_order(
                symbol="NVDA",
                side="buy",
                order_type="limit",
                time_in_force="gtc",
                qty=Decimal(2),
                client_order_id=f"{order_class}-entry",
                status="filled",
                limit_price=Decimal(100),
                order_class=order_class,
                group_role="entry",
            )
            parent.group_id = uuid4()
            target = sim._new_order(
                symbol="NVDA",
                side="sell",
                order_type="limit",
                time_in_force="gtc",
                qty=Decimal(2),
                client_order_id=f"{order_class}-target",
                status="new",
                limit_price=Decimal(104),
                order_class=order_class,
                parent=parent,
                group_role="take_profit",
            )
            stop = sim._new_order(
                symbol="NVDA",
                side="sell",
                order_type="stop",
                time_in_force="gtc",
                qty=Decimal(2),
                client_order_id=f"{order_class}-stop",
                status="new",
                stop_price=Decimal(96),
                order_class=order_class,
                parent=parent,
                group_role="stop_loss",
            )
        assert {order.id for order in sim.inspect_orders("open")} == {target.id, stop.id}
        return sim, target.id, stop.id

    # Exempt everywhere: as existing orders the legs refuse no buy, although 105 crosses the
    # 104 target, and as the new order a leg's replacement is not refused by those buys.
    sim, _, stop_id = seeded("any_order")
    for kind in _ORDER_KINDS:
        assert sim.submit_order(_typed(kind, OrderSide.BUY, limit=105.0)).status is OrderStatus.NEW
    moved = sim.replace_order_by_id(stop_id, ReplaceOrderRequest(stop_price=97.0))
    assert moved.status is OrderStatus.NEW and moved.replaces == stop_id

    # Exempt within their own order only (the default): every outside buy collides with the
    # take-profit leg (a limit sell at 104), and a 103.99 limit buy, which does not reach that
    # price, with the stop leg.
    sim, target_id, stop_id = seeded("own_order")
    target, stop = sim.inspect_order(target_id), sim.inspect_order(stop_id)
    for kind in _ORDER_KINDS:
        _assert_wash_rejected(sim, _typed(kind, OrderSide.BUY, limit=105.0), target, _LIMIT_REASON)
    _assert_wash_rejected(
        sim, _typed("limit", OrderSide.BUY, limit=103.99), stop, _MARKET_STOP_REASON
    )
    # The legs are on the same side, so replacing one never collides with its sibling.
    moved = sim.replace_order_by_id(stop_id, ReplaceOrderRequest(stop_price=97.0))
    assert moved.status is OrderStatus.NEW and moved.replaces == stop_id


# ------------------------------------------------------------------- trailing stops


def _mark(sim: FaultInjectingAlpacaSim, order: Order) -> tuple[Decimal, Decimal, OrderStatus]:
    """``(hwm, stop_price, status)`` of a trailing stop as the simulator holds it now."""
    seen = sim.inspect_order(order.id)
    return _d(seen.hwm), _d(seen.stop_price), seen.status


def _expected_trailing_stop(
    side: OrderSide,
    mark: Decimal,
    trail: Decimal,
    *,
    percent: bool,
    rounding: TrailingStopRounding = "half_up",
) -> Decimal:
    """The suite's own statement of the stop rule, written without the simulator's code."""
    sell = side is OrderSide.SELL
    if percent:
        exact = mark * (100 - trail) / 100 if sell else mark * (100 + trail) / 100
    else:
        exact = mark - trail if sell else mark + trail
    tick = Decimal("0.01") if exact >= 1 else Decimal("0.0001")
    if rounding == "down":
        return exact.quantize(tick, rounding=ROUND_FLOOR)
    nearest = exact.quantize(tick, rounding=ROUND_HALF_UP)
    if rounding == "half_up":
        return nearest
    return nearest - tick if sell else nearest + tick


@OFFLINE
def test_trailing_stop_wire_shape_acknowledgement_and_share_reservation() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")

    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 4, percent=1.0, cid="trail-1"))
    # The simulator saw exactly what alpaca-py puts on the wire ...
    assert sim.calls("submit_order")[-1].payload == {
        "symbol": "NVDA",
        "qty": 4.0,
        "side": "sell",
        "type": "trailing_stop",
        "time_in_force": "gtc",
        "trail_percent": 1.0,
        "client_order_id": "trail-1",
    }
    # ... and answered with the broker's fields, parsed by alpaca-py's own Order model. The
    # high-water mark starts at the price standing at submission (observed 2026-11-27).
    assert order.type is OrderType.TRAILING_STOP and order.status is OrderStatus.NEW
    assert order.side is OrderSide.SELL and order.time_in_force is TimeInForce.GTC
    assert (order.trail_percent, order.trail_price, order.hwm) == ("1", None, "100")
    assert _d(order.stop_price) == 99 and order.limit_price is None
    assert order.client_order_id == "trail-1" and not order.extended_hours

    # Shares behind a trailing sell are held for it exactly like those behind a stop order.
    assert _d(sim.get_open_position("NVDA").qty_available) == 6
    with pytest.raises(APIError) as over:
        sim.submit_order(_stop("NVDA", OrderSide.SELL, 7.0, 95.0))
    assert over.value.status_code == 403 and over.value.code == 40310000
    assert _refused(over) == {
        "code": 40310000,
        "message": "insufficient qty available for order (requested: 7, available: 6)",
        "available": "6",
        "existing_qty": "10",
        "held_for_orders": "4",
        "symbol": "NVDA",
    }

    # A dollar trail: the stop is that many dollars under the mark.
    second = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 6, price=2.0))
    assert (second.trail_percent, second.trail_price, second.hwm) == (None, "2", "100")
    assert _d(second.stop_price) == 98
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    for request in (
        _market("NVDA", OrderSide.SELL, 1.0),
        _limit("NVDA", OrderSide.SELL, 1.0, 105.0),
        _trailing("NVDA", OrderSide.SELL, 1, percent=1.0),
    ):
        with pytest.raises(APIError) as none_left:
            sim.submit_order(request)
        assert none_left.value.status_code == 403
        assert "insufficient qty available" in none_left.value.message
        assert sim.call_log[-1].refusal == "insufficient_qty"

    # Reads carry the same fields, and a cancel releases the shares.
    listed = sim.get_orders(GetOrdersRequest(limit=500))
    assert {
        item.id: (item.type, item.trail_percent, item.trail_price, item.hwm) for item in listed
    } == {
        order.id: (OrderType.TRAILING_STOP, "1", None, "100"),
        second.id: (OrderType.TRAILING_STOP, None, "2", "100"),
    }
    sim.cancel_order_by_id(order.id)
    assert _d(sim.get_open_position("NVDA").qty_available) == 4


# (trail %, high-water mark, stop price) of the seven native trailing-stop sells, GTC, that the
# real broker acknowledged on 2026-11-27 at 14:12 New York.
_OBSERVED_TRAILING_STOPS: tuple[tuple[str, str, str], ...] = (
    ("0.41", "91.14", "90.77"),
    ("0.45", "170.26", "169.49"),
    ("0.45", "646.96", "644.05"),
    ("0.45", "172.20", "171.43"),
    ("0.16", "85.35", "85.21"),
    ("0.17", "35.84", "35.77"),
    ("1.49", "54.45", "53.64"),
)
# OPEN ANOMALY: the exact stop of this row is 35.779072, whose nearest cent is 35.78; the broker
# reported 35.77. The nearest cent fits the other six rows, rounding down fits this row and two
# others, and no rule tried fits all seven. A paper-account check has to settle it.
_OPEN_ANOMALY = ("0.17", "35.84", "35.77")


@OFFLINE
@pytest.mark.parametrize(
    ("trail", "mark", "observed"),
    [
        pytest.param(*row, id=f"{row[0]}%-of-{row[1]}" + ("-OPEN-ANOMALY" * (row == _OPEN_ANOMALY)))
        for row in _OBSERVED_TRAILING_STOPS
    ],
)
def test_observed_trailing_sell_with_the_regular_session_closed_reads_new_at_a_cent_stop(
    trail: str, mark: str, observed: str
) -> None:
    sim = FaultInjectingAlpacaSim(start=OBSERVED_AT)
    assert sim.market_session == "AFTER_HOURS" and sim.get_clock().is_open is False
    sim.open_position("HELD", 10, mark)

    order = sim.submit_order(_trailing("HELD", OrderSide.SELL, 10, percent=float(trail)))
    # Acknowledged "new", not "accepted", although the regular session is shut ...
    assert order.status is OrderStatus.NEW and order.type is OrderType.TRAILING_STOP
    assert sim.inspect_order(order.id).status is OrderStatus.NEW
    assert sim.get_orders(GetOrdersRequest(limit=500))[0].status is OrderStatus.NEW
    # ... with the mark at the price standing at submission and the shares held.
    assert _d(order.hwm) == _d(mark) and _d(order.trail_percent) == _d(trail)
    assert _d(sim.get_open_position("HELD").qty_available) == 0

    stop = _d(order.stop_price)
    nearest = (_d(mark) * (100 - _d(trail)) / 100).quantize(Decimal("0.01"), ROUND_HALF_UP)
    assert stop == stop.quantize(Decimal("0.01")) == nearest  # whole cents, the nearest one
    if (trail, mark, observed) == _OPEN_ANOMALY:
        # The simulator's default cannot reproduce this row; it is one cent tighter than the
        # broker was. Lane tests therefore also run under the two looser rounding modes.
        assert (stop, _d(observed)) == (Decimal("35.78"), Decimal("35.77"))
    else:
        assert stop == _d(observed)


@OFFLINE
def test_no_rounding_mode_reproduces_all_seven_observed_trailing_stops() -> None:
    assert TRAILING_STOP_ROUNDINGS == ("half_up", "down", "one_tick_looser")
    assert FaultInjectingAlpacaSim().trailing_stop_rounding == "half_up"
    anomaly = _OBSERVED_TRAILING_STOPS.index(_OPEN_ANOMALY)

    stops: dict[str, list[Decimal]] = {}
    for mode in TRAILING_STOP_ROUNDINGS:
        sim = FaultInjectingAlpacaSim(start=OBSERVED_AT, trailing_stop_rounding=mode)
        stops[mode] = []
        for index, (trail, mark, _) in enumerate(_OBSERVED_TRAILING_STOPS):
            sim.open_position(f"HELD{index}", 10, mark)
            order = sim.submit_order(
                _trailing(f"HELD{index}", OrderSide.SELL, 10, percent=float(trail))
            )
            stops[mode].append(_d(order.stop_price))
    fits = {
        mode: [
            stop == _d(row[2]) for stop, row in zip(found, _OBSERVED_TRAILING_STOPS, strict=True)
        ]
        for mode, found in stops.items()
    }
    # Nearest cent: six of seven, all but the anomaly. Down: the anomaly and two more. One tick
    # looser than nearest: the anomaly alone. No mode fits every row.
    assert fits["half_up"] == [index != anomaly for index in range(7)]
    assert fits["down"] == [False, True, False, False, True, True, False]
    assert fits["one_tick_looser"] == [index == anomaly for index in range(7)]
    assert not any(all(row) for row in fits.values())
    assert [stops[mode][anomaly] for mode in TRAILING_STOP_ROUNDINGS] == [
        Decimal("35.78"),
        Decimal("35.77"),
        Decimal("35.77"),
    ]
    # The exact products the simulator used to report never came from the broker.
    assert stops["half_up"][0] != Decimal("90.766326")


# side, percent trail?, trail, mark -> stop under ("half_up", "down", "one_tick_looser").
_ROUNDING_CASES = (
    pytest.param(
        OrderSide.SELL, True, "0.41", "91.14", ("90.77", "90.76", "90.76"), id="sell-90.766326"
    ),
    pytest.param(
        OrderSide.SELL, True, "0.5", "101", ("100.5", "100.49", "100.49"), id="sell-half-100.495"
    ),
    pytest.param(OrderSide.BUY, True, "0.5", "100.50", ("101", "101", "101.01"), id="buy-101.0025"),
    pytest.param(
        OrderSide.BUY, True, "0.5", "101", ("101.51", "101.5", "101.52"), id="buy-half-101.505"
    ),
    pytest.param(
        OrderSide.SELL, False, "2.5", "91.14", ("88.64", "88.64", "88.63"), id="sell-dollars"
    ),
    pytest.param(
        OrderSide.SELL, True, "1", "0.8123", ("0.8042", "0.8041", "0.8041"), id="sell-0.804177"
    ),
    pytest.param(
        OrderSide.BUY, True, "1", "0.8123", ("0.8204", "0.8204", "0.8205"), id="buy-0.820423"
    ),
    pytest.param(
        OrderSide.SELL, True, "1", "1.005", ("0.995", "0.9949", "0.9949"), id="sell-0.99495"
    ),
    pytest.param(
        OrderSide.SELL, False, "0.05", "0.8123", ("0.7623", "0.7623", "0.7622"), id="sell-0.7623"
    ),
    # Exactly 1.00 is still on the cent tick: one tick looser is 0.99, not 0.9999.
    pytest.param(OrderSide.SELL, False, "0.25", "1.25", ("1", "1", "0.99"), id="sell-exactly-1.00"),
)


@OFFLINE
@pytest.mark.parametrize(("side", "percent", "trail", "mark", "stops"), _ROUNDING_CASES)
@pytest.mark.parametrize("rounding", TRAILING_STOP_ROUNDINGS)
def test_trailing_stop_price_is_quantised_to_the_tick_under_each_rounding_mode(
    rounding: TrailingStopRounding,
    side: OrderSide,
    percent: bool,
    trail: str,
    mark: str,
    stops: tuple[str, str, str],
) -> None:
    # Marks whose product does not land on a tick, exact half ticks, both sides, both kinds of
    # trail, and the 0.0001 tick below 1.00. Matching is off so that nothing triggers.
    sim = FaultInjectingAlpacaSim(auto_fill=False, trailing_stop_rounding=rounding)
    sim.open_position("NVDA", 10 if side is OrderSide.SELL else -10, mark)
    order = sim.submit_order(
        _trailing(
            "NVDA",
            side,
            10,
            percent=float(trail) if percent else None,
            price=None if percent else float(trail),
        )
    )
    expected = _d(stops[TRAILING_STOP_ROUNDINGS.index(rounding)])
    assert _mark(sim, order) == (_d(mark), expected, OrderStatus.NEW)
    assert order.stop_price is not None and _d(order.stop_price) == expected
    # The suite's reference rule (used by the property test below) agrees with the table.
    assert expected == _expected_trailing_stop(
        side, _d(mark), _d(trail), percent=percent, rounding=rounding
    )
    # Whatever the mode, the reported figure sits on the tick of its own price level.
    tick = Decimal("0.01") if expected >= 1 else Decimal("0.0001")
    assert expected == expected.quantize(tick)


@OFFLINE
def test_trailing_stop_triggers_at_the_quantised_stop_and_ratchets_on_uneven_marks() -> None:
    # 0.41 % under 91.14 is 90.766326. The broker reported 90.77, so a 90.77 print is at the
    # stop; the exact product would have held the order until 90.76.
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "91.14")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=0.41))
    assert _mark(sim, order) == (Decimal("91.14"), Decimal("90.77"), OrderStatus.NEW)
    sim.set_price("NVDA", "90.78")
    assert sim.inspect_order(order.id).status is OrderStatus.NEW
    sim.set_price("NVDA", "90.77")
    done = sim.inspect_order(order.id)
    assert done.status is OrderStatus.FILLED and _d(done.filled_avg_price) == Decimal("90.77")
    # So the distance a lane reads back differs from the trail it asked for: here 0.4060 %.
    assert (Decimal("91.14") - Decimal("90.77")) / Decimal("91.14") < Decimal("0.0041")

    # The stop follows the mark from one uneven product to the next (169.49383 -> 171.4251).
    sim.open_position("AMD", 10, "170.26")
    climbing = sim.submit_order(_trailing("AMD", OrderSide.SELL, 10, percent=0.45))
    assert _mark(sim, climbing) == (Decimal("170.26"), Decimal("169.49"), OrderStatus.NEW)
    sim.set_price("AMD", "172.20")
    assert _mark(sim, climbing) == (Decimal("172.2"), Decimal("171.43"), OrderStatus.NEW)
    sim.set_price("AMD", "171.44")
    assert sim.inspect_order(climbing.id).status is OrderStatus.NEW
    sim.set_price("AMD", "171.43")
    assert sim.inspect_order(climbing.id).status is OrderStatus.FILLED

    # A trail finer than half a tick puts the stop onto the mark: the acknowledgement still
    # reads "new", and the order sells at the first eligible print, the one it was placed at.
    sim.open_position("TSLA", 5, "10")
    onto_mark = sim.submit_order(_trailing("TSLA", OrderSide.SELL, 5, percent=0.01))
    assert onto_mark.status is OrderStatus.NEW and _d(onto_mark.stop_price) == 10
    sold = sim.inspect_order(onto_mark.id)
    assert sold.status is OrderStatus.FILLED and _d(sold.filled_avg_price) == 10
    # One tick looser, the same order rests a cent under the mark.
    looser = FaultInjectingAlpacaSim(trailing_stop_rounding="one_tick_looser")
    looser.open_position("TSLA", 5, "10")
    resting = looser.submit_order(_trailing("TSLA", OrderSide.SELL, 5, percent=0.01))
    assert _mark(looser, resting) == (10, Decimal("9.99"), OrderStatus.NEW)

    # At 1.00 the tick changes; a rising mark still never lowers the stop. One tick looser,
    # 0.9997 (under a 1.0099 mark) would step back to 0.99 (under 1.0102): it stays.
    edge = FaultInjectingAlpacaSim(trailing_stop_rounding="one_tick_looser")
    edge.open_position("PENNY", 100, "1.0099")
    penny = edge.submit_order(_trailing("PENNY", OrderSide.SELL, 100, percent=1.0))
    assert _mark(edge, penny) == (Decimal("1.0099"), Decimal("0.9997"), OrderStatus.NEW)
    edge.set_price("PENNY", "1.0102")
    assert _mark(edge, penny) == (Decimal("1.0102"), Decimal("0.9997"), OrderStatus.NEW)
    edge.set_price("PENNY", "1.05")
    assert _mark(edge, penny) == (Decimal("1.05"), Decimal("1.03"), OrderStatus.NEW)

    bogus: Any = "nearest"
    with pytest.raises(ValueError, match="trailing_stop_rounding"):
        FaultInjectingAlpacaSim(trailing_stop_rounding=bogus)


@OFFLINE
def test_trailing_stop_rests_off_hours_without_triggering_and_wakes_at_the_open() -> None:
    sim = FaultInjectingAlpacaSim(session="AFTER_HOURS")
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", 5, "200")
    nvda = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    amd = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=1.0))
    # "new" although the session is shut (a plain stop reads "accepted" there), shares held.
    assert nvda.status is OrderStatus.NEW and amd.status is OrderStatus.NEW
    assert (nvda.hwm, _d(nvda.stop_price)) == ("100", 99)
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    sim.open_position("TSLA", 5, "300")
    plain = sim.submit_order(_stop("TSLA", OrderSide.SELL, 5.0, 290.0))
    assert plain.status is OrderStatus.ACCEPTED
    # A symbol without a price has no mark until the first price the order may track.
    blank = sim.submit_order(_trailing("ZZZ", OrderSide.SELL, 1, percent=1.0))
    assert blank.status is OrderStatus.NEW and (blank.hwm, blank.stop_price) == (None, None)

    # Far through the stop, then far above the mark, with the regular session shut: nothing
    # triggers and, by default, the mark does not move.
    for session in ("OVERNIGHT", "PRE_MARKET"):
        sim.set_session(session)
        for price in ("90", "120"):
            sim.set_price("NVDA", price)
            assert sim.market_session == session
            assert _mark(sim, nvda) == (100, 99, OrderStatus.NEW)
    sim.set_price("AMD", "180")
    sim.set_price("ZZZ", "50")
    assert _mark(sim, amd) == (200, 198, OrderStatus.NEW)
    assert sim.inspect_order(blank.id).hwm is None
    assert _d(sim.get_open_position("NVDA").qty) == 10 and _d(sim.get_open_position("AMD").qty) == 5

    # The price standing at the bell is the first regular-hours print: it lifts the NVDA mark,
    # gaps AMD out at 180 (not at its 198 stop) and gives ZZZ its first mark.
    sim.set_session("RTH")
    assert _mark(sim, nvda) == (120, Decimal("118.8"), OrderStatus.NEW)
    gapped = sim.inspect_order(amd.id)
    assert gapped.status is OrderStatus.FILLED and _d(gapped.filled_avg_price) == 180
    assert sim.inspect_position("AMD") is None
    assert _mark(sim, blank) == (50, Decimal("49.5"), OrderStatus.NEW)
    assert sim.inspect_order(plain.id).status is OrderStatus.NEW  # the plain stop is promoted

    # With the constructor option the mark follows off-hours prices, yet the stop still cannot
    # trigger there: a price left under it fills at the open.
    tracking = FaultInjectingAlpacaSim(session="PRE_MARKET", trailing_hwm_moves_off_hours=True)
    tracking.open_position("NVDA", 10, "100")
    moved = tracking.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    tracking.set_price("NVDA", "120")
    assert _mark(tracking, moved) == (120, Decimal("118.8"), OrderStatus.NEW)
    tracking.set_price("NVDA", "110")
    assert _mark(tracking, moved) == (120, Decimal("118.8"), OrderStatus.NEW)
    assert _d(tracking.get_open_position("NVDA").qty) == 10
    tracking.set_session("RTH")
    late = tracking.inspect_order(moved.id)
    assert late.status is OrderStatus.FILLED and _d(late.filled_avg_price) == 110


@OFFLINE
@pytest.mark.parametrize("session", OFF_HOURS)
@pytest.mark.parametrize("mark_moves", [False, True], ids=["mark-frozen", "mark-moves"])
def test_trailing_stop_placed_in_each_off_hours_session_never_triggers_there(
    mark_moves: bool, session: SimSession
) -> None:
    assert set(OFF_HOURS) == set(SIM_SESSIONS) - {"RTH"}
    sim = FaultInjectingAlpacaSim(session=session, trailing_hwm_moves_off_hours=mark_moves)
    # An asset that trades overnight makes no difference: a trailing stop is never an
    # extended-hours order.
    sim.set_asset("NVDA", attributes=["overnight_tradable"])
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    assert sim.market_session == session
    assert order.status is OrderStatus.NEW
    assert (order.hwm, _d(order.stop_price)) == ("100", 99)  # the price standing at submission
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    placed = sim.inspect_order(order.id).updated_at

    sim.advance(30)
    sim.set_price("NVDA", "90")  # far through the stop
    assert _mark(sim, order) == (100, 99, OrderStatus.NEW)
    assert _d(sim.inspect_order(order.id).filled_qty) == 0
    assert sim.inspect_order(order.id).updated_at == placed
    sim.set_price("NVDA", "120")  # far above the mark
    lifted = (120, Decimal("118.8")) if mark_moves else (100, 99)
    assert _mark(sim, order) == (*lifted, OrderStatus.NEW)
    assert (sim.inspect_order(order.id).updated_at != placed) is mark_moves
    sim.set_price("NVDA", "110")  # under a lifted stop, above the frozen one
    assert sim.market_session == session
    assert _mark(sim, order) == (*lifted, OrderStatus.NEW)
    assert _d(sim.get_open_position("NVDA").qty) == 10

    # The first regular print (110, left standing) is where it trades again.
    sim.set_session("RTH")
    seen = sim.inspect_order(order.id)
    if mark_moves:
        assert seen.status is OrderStatus.FILLED and _d(seen.filled_avg_price) == 110
        assert sim.inspect_position("NVDA") is None
    else:
        assert _mark(sim, order) == (110, Decimal("108.9"), OrderStatus.NEW)
        assert _d(sim.get_open_position("NVDA").qty_available) == 0


@OFFLINE
def test_trailing_stop_ratchets_one_way_and_fills_at_the_triggering_print() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    assert _mark(sim, order) == (100, 98, OrderStatus.NEW)
    placed = sim.now

    sim.advance(5)
    sim.set_price("NVDA", "104")  # a new high: stop = 104 * (1 - 2 / 100)
    assert _mark(sim, order) == (104, Decimal("101.92"), OrderStatus.NEW)
    raised = sim.now
    assert sim.inspect_order(order.id).updated_at == raised == placed + timedelta(seconds=5)
    sim.advance(5)
    sim.set_price("NVDA", "103")  # a pullback loosens nothing and touches nothing
    assert _mark(sim, order) == (104, Decimal("101.92"), OrderStatus.NEW)
    sim.set_price("NVDA", "104")  # back at the mark: still nothing to record
    assert _mark(sim, order) == (104, Decimal("101.92"), OrderStatus.NEW)
    assert sim.inspect_order(order.id).updated_at == raised
    sim.set_price("NVDA", "101.93")  # one cent above the stop
    assert _mark(sim, order) == (104, Decimal("101.92"), OrderStatus.NEW)
    sim.set_price("NVDA", "101.92")  # at the stop: a market order, filled at this print
    done = sim.inspect_order(order.id)
    assert done.status is OrderStatus.FILLED and _d(done.filled_avg_price) == Decimal("101.92")
    assert done.type is OrderType.TRAILING_STOP and _d(done.stop_price) == Decimal("101.92")
    assert sim.inspect_position("NVDA") is None
    (fill,) = sim.get(FILLS, {"direction": "desc", "page_size": 1})
    assert (fill["order_id"], fill["side"], fill["price"]) == (str(order.id), "sell", "101.92")
    # A filled order is history: later prints, however high, move neither mark nor stop.
    sim.set_price("NVDA", "150")
    assert _mark(sim, order) == (104, Decimal("101.92"), OrderStatus.FILLED)
    assert sim.inspect_order(order.id).updated_at == done.updated_at

    # A gap fills at the gap price, however far under the stop.
    sim.open_position("AMD", 5, "200")
    gap = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=2.0))
    sim.set_price("AMD", "180")
    gapped = sim.inspect_order(gap.id)
    assert gapped.status is OrderStatus.FILLED
    assert _d(gapped.stop_price) == 196 and _d(gapped.filled_avg_price) == 180

    # A dollar trail follows the same way.
    sim.open_position("TSLA", 5, "300")
    dollars = sim.submit_order(_trailing("TSLA", OrderSide.SELL, 5, price=5.0))
    sim.set_price("TSLA", "310")
    assert _mark(sim, dollars) == (310, 305, OrderStatus.NEW)
    sim.set_price("TSLA", "305.01")
    assert _mark(sim, dollars) == (310, 305, OrderStatus.NEW)
    sim.set_price("TSLA", "304")
    assert _d(sim.inspect_order(dollars.id).filled_avg_price) == 304

    # The buy side mirrors it: the mark is the lowest price and the stop sits above it.
    sim.open_position("META", -5, "500")
    cover = sim.submit_order(_trailing("META", OrderSide.BUY, 5, percent=1.0))
    assert _mark(sim, cover) == (500, 505, OrderStatus.NEW)
    sim.set_price("META", "490")
    assert _mark(sim, cover) == (490, Decimal("494.9"), OrderStatus.NEW)
    sim.set_price("META", "494")
    assert _mark(sim, cover) == (490, Decimal("494.9"), OrderStatus.NEW)
    sim.set_price("META", "494.89")  # one cent under the buy stop
    assert _mark(sim, cover) == (490, Decimal("494.9"), OrderStatus.NEW)
    sim.set_price("META", "494.9")  # exactly at the buy stop: it triggers there
    covered = sim.inspect_order(cover.id)
    assert covered.status is OrderStatus.FILLED and _d(covered.filled_avg_price) == Decimal("494.9")
    assert sim.inspect_position("META") is None

    # A buy dollar trail sits that many dollars above the lowest price.
    sim.open_position("SHOP", -5, "50")
    buy_dollars = sim.submit_order(_trailing("SHOP", OrderSide.BUY, 5, price=2.0))
    assert _mark(sim, buy_dollars) == (50, 52, OrderStatus.NEW)
    sim.set_price("SHOP", "48")
    assert _mark(sim, buy_dollars) == (48, 50, OrderStatus.NEW)
    sim.set_price("SHOP", "49.99")
    assert _mark(sim, buy_dollars) == (48, 50, OrderStatus.NEW)
    sim.set_price("SHOP", "50")
    bought = sim.inspect_order(buy_dollars.id)
    assert bought.status is OrderStatus.FILLED and _d(bought.filled_avg_price) == 50
    assert sim.inspect_position("SHOP") is None

    # With matching switched off the mark still tracks; only the trigger is suspended.
    manual = FaultInjectingAlpacaSim(auto_fill=False)
    manual.open_position("NVDA", 10, "100")
    held = manual.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    manual.set_price("NVDA", "105")
    manual.set_price("NVDA", "90")
    assert _mark(manual, held) == (105, Decimal("103.95"), OrderStatus.NEW)


@OFFLINE
def test_trailing_stop_replace_changes_only_the_trail_and_keeps_its_kind() -> None:
    # Only ``trail`` can be replaced on a trailing stop (documented). The unknowns of a replace
    # are held at their kind values here (effective at once, mark carried over, a trail may
    # move either way); each has its own test below.
    sim = FaultInjectingAlpacaSim(
        replace_pending="never", trailing_replace_mark="keep", trail_replace_must_tighten=False
    )
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0, cid="trail-1"))
    sim.set_price("NVDA", "110")
    assert _mark(sim, order) == (110, Decimal("107.8"), OrderStatus.NEW)

    successor = sim.replace_order_by_id(order.id, ReplaceOrderRequest(trail=1.0))
    assert sim.calls("replace_order_by_id")[-1].payload == {"trail": 1.0}
    # Documented: a new order with a new order id; the old order ends as replaced.
    assert successor.id != order.id and successor.replaces == order.id
    assert successor.type is OrderType.TRAILING_STOP and successor.status is OrderStatus.NEW
    assert successor.client_order_id != "trail-1"
    # Same kind, new trail, same high-water mark: 110 * (1 - 1 / 100).
    assert (successor.trail_percent, successor.trail_price, successor.hwm) == ("1", None, "110")
    assert _d(successor.stop_price) == Decimal("108.9")
    original = sim.get_order_by_id(order.id)
    assert original.status is OrderStatus.REPLACED and original.replaced_by == successor.id
    assert _d(sim.get_open_position("NVDA").qty_available) == 0  # only the successor reserves
    assert [item.id for item in sim.get_orders(GetOrdersRequest(limit=500))] == [successor.id]

    # A wider trail is the one way a stop moves away from the price.
    wider = sim.replace_order_by_id(successor.id, ReplaceOrderRequest(trail=5.0))
    assert _mark(sim, wider) == (110, Decimal("104.5"), OrderStatus.NEW)

    # Nothing but the trail: a fixed stop or limit price is refused ...
    for request in (
        ReplaceOrderRequest(stop_price=109.0),
        ReplaceOrderRequest(limit_price=109.0),
        ReplaceOrderRequest(trail=1.0, stop_price=109.0),
    ):
        with pytest.raises(APIError) as refused:
            sim.replace_order_by_id(wider.id, request)
        assert refused.value.status_code == 422
    # ... the kind cannot change (PATCH has no such fields; raw callers get a 422 of its own,
    # also next to a valid trail) ...
    submit_side_names: tuple[dict[str, str], ...] = (
        {"trail_price": "2"},
        {"trail_percent": "1"},
        {"trail": "1", "trail_percent": "1"},
    )
    for body in submit_side_names:
        with pytest.raises(APIError) as refused:
            sim.patch(f"/orders/{wider.id}", body)
        assert refused.value.status_code == 422, body
        assert "cannot be replaced; send trail" in refused.value.message, body
    # ... whether or not an empty PATCH is refused (that refusal must not stand in for this) ...
    sim.empty_replace_refused = False
    for body in submit_side_names:
        with pytest.raises(APIError) as refused:
            sim.patch(f"/orders/{wider.id}", body)
        assert "cannot be replaced; send trail" in refused.value.message, body
    sim.empty_replace_refused = True
    # ... the trail must be positive ...
    for body in ({"trail": "0"}, {"trail": "-1"}):
        with pytest.raises(APIError) as refused:
            sim.patch(f"/orders/{wider.id}", body)
        assert refused.value.status_code == 422 and "must be > 0" in refused.value.message, body
    # ... and a qty or time_in_force change is left unmodelled rather than guessed, like a sell
    # trail of 100 % or more, on a replace as on a submit.
    with pytest.raises(NotImplementedError, match="qty change on a trailing stop"):
        sim.replace_order_by_id(wider.id, ReplaceOrderRequest(qty=5))
    with pytest.raises(NotImplementedError, match="time_in_force change on a trailing stop"):
        sim.replace_order_by_id(wider.id, ReplaceOrderRequest(time_in_force=TimeInForce.DAY))
    with pytest.raises(NotImplementedError, match="sell trail_percent of 100 or more"):
        sim.replace_order_by_id(wider.id, ReplaceOrderRequest(trail=100.0))
    # Every refusal left the order as it was.
    assert _mark(sim, wider) == (110, Decimal("104.5"), OrderStatus.NEW)
    assert [item.id for item in sim.get_orders(GetOrdersRequest(limit=500))] == [wider.id]

    # trail on an order that is not a trailing stop is not modelled either.
    sim.open_position("AMD", 5, "200")
    plain = sim.submit_order(_stop("AMD", OrderSide.SELL, 5.0, 190.0))
    with pytest.raises(NotImplementedError, match="trail on a stop order"):
        sim.replace_order_by_id(plain.id, ReplaceOrderRequest(trail=1.0))
    assert sim.inspect_order(plain.id).status is OrderStatus.NEW

    # A dollar trail stays a dollar trail and keeps trailing from the same mark.
    sim.open_position("TSLA", 5, "300")
    dollars = sim.submit_order(_trailing("TSLA", OrderSide.SELL, 5, price=5.0))
    tighter = sim.replace_order_by_id(
        dollars.id, ReplaceOrderRequest(trail=3.0, client_order_id="trail-3")
    )
    assert (tighter.trail_percent, tighter.trail_price, tighter.hwm) == (None, "3", "300")
    assert _d(tighter.stop_price) == 297 and tighter.client_order_id == "trail-3"
    sim.set_price("TSLA", "299")
    assert _mark(sim, tighter) == (300, 297, OrderStatus.NEW)
    # On a dollar trail "100" is a hundred dollars, not the unmodelled hundred percent.
    far = sim.replace_order_by_id(tighter.id, ReplaceOrderRequest(trail=100.0))
    assert _mark(sim, far) == (300, 200, OrderStatus.NEW)
    # A trail tightened onto the standing price sells at once: the acknowledgement is taken
    # before matching, the next read shows the fill at that price.
    closing = sim.replace_order_by_id(far.id, ReplaceOrderRequest(trail=1.0))
    assert closing.status is OrderStatus.NEW and _d(closing.stop_price) == 299
    sold = sim.inspect_order(closing.id)
    assert sold.status is OrderStatus.FILLED and _d(sold.filled_avg_price) == 299
    assert sim.inspect_position("TSLA") is None
    with pytest.raises(APIError) as stale:
        sim.replace_order_by_id(dollars.id, ReplaceOrderRequest(trail=4.0))
    assert stale.value.status_code == 422


@OFFLINE
def test_trail_replace_must_tighten_unless_switched_off() -> None:
    # The page says the trail can be updated and names no direction. By default a trail that
    # does not tighten is refused, so a lane that re-sends or loosens a trail fails its test.
    sim = FaultInjectingAlpacaSim(replace_pending="never", trailing_replace_mark="keep")
    assert sim.trail_replace_must_tighten is True
    sim.open_position("NVDA", 10, "100")
    sim.open_position("TSLA", -5, "300")
    percent = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    dollars = sim.submit_order(_trailing("TSLA", OrderSide.BUY, 5, price=5.0))
    orders = len(sim.inspect_orders("all"))
    for order, identical, wider in ((percent, 2.0, 2.01), (dollars, 5.0, 5.01)):
        for trail in (identical, wider):
            with pytest.raises(APIError) as loose:
                sim.replace_order_by_id(order.id, ReplaceOrderRequest(trail=trail))
            assert loose.value.status_code == 422, trail
            assert "does not tighten the current trail" in loose.value.message, trail
        assert sim.inspect_order(order.id).status is OrderStatus.NEW
    assert len(sim.inspect_orders("all")) == orders  # no successor was created
    assert _mark(sim, percent) == (100, 98, OrderStatus.NEW)
    assert _mark(sim, dollars) == (300, 305, OrderStatus.NEW)

    # The smallest step tighter is accepted, for either kind of trail and either side.
    tighter = sim.replace_order_by_id(percent.id, ReplaceOrderRequest(trail=1.99))
    assert (tighter.trail_percent, _d(tighter.stop_price)) == ("1.99", Decimal("98.01"))
    cover = sim.replace_order_by_id(dollars.id, ReplaceOrderRequest(trail=4.99))
    assert (cover.trail_price, _d(cover.stop_price)) == ("4.99", Decimal("304.99"))
    # A trail too fine to send is refused for its precision before its direction is looked at.
    with pytest.raises(APIError) as too_fine:
        sim.replace_order_by_id(tighter.id, ReplaceOrderRequest(trail=2.005))
    assert too_fine.value.status_code == 422 and "decimal places" in too_fine.value.message

    # Switched off, a trail can be re-sent as it is or widened: the one way a stop moves away
    # from the price. Lane tests run under both settings.
    lenient = FaultInjectingAlpacaSim(
        replace_pending="never", trailing_replace_mark="keep", trail_replace_must_tighten=False
    )
    lenient.open_position("NVDA", 10, "100")
    first = lenient.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    same = lenient.replace_order_by_id(first.id, ReplaceOrderRequest(trail=2.0))
    assert same.id != first.id and _mark(lenient, same) == (100, 98, OrderStatus.NEW)
    loosened = lenient.replace_order_by_id(same.id, ReplaceOrderRequest(trail=5.0))
    assert _mark(lenient, loosened) == (100, 95, OrderStatus.NEW)


@OFFLINE
@pytest.mark.parametrize("mark_mode", REPLACE_MARKS)
def test_trailing_stop_replace_mark_is_a_switch_and_a_reseeded_tighten_can_lower_the_stop(
    mark_mode: str,
) -> None:
    assert REPLACE_MARKS == ("reseed", "keep")
    assert FaultInjectingAlpacaSim().trailing_replace_mark == "reseed"
    switches: Any = {"trailing_replace_mark": mark_mode}
    sim = FaultInjectingAlpacaSim(replace_pending="never", **switches)
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    sim.set_price("NVDA", "110")
    sim.set_price("NVDA", "108.5")  # a pullback under the mark, above the 107.80 stop
    assert _mark(sim, order) == (110, Decimal("107.8"), OrderStatus.NEW)

    tighter = sim.replace_order_by_id(order.id, ReplaceOrderRequest(trail=1.5))
    if mark_mode == "keep":
        # The mark carries over: 110 * 0.985 = 108.35, the stop rose.
        assert (tighter.hwm, _d(tighter.stop_price)) == ("110", Decimal("108.35"))
        sim.set_price("NVDA", "109.5")  # under the kept mark: nothing moves
        assert _mark(sim, tighter) == (110, Decimal("108.35"), OrderStatus.NEW)
        sim.set_price("NVDA", "110.5")
        assert _mark(sim, tighter) == (Decimal("110.5"), Decimal("108.84"), OrderStatus.NEW)
    else:
        # The mark restarts at the standing price, like a newly submitted order:
        # 108.5 * 0.985 = 106.8725. The "tightened" stop is BELOW the stop it replaced.
        assert (tighter.hwm, _d(tighter.stop_price)) == ("108.5", Decimal("106.87"))
        assert _d(tighter.stop_price) < Decimal("107.8")
        sim.set_price("NVDA", "107.5")  # under the old stop: the shares are still held
        assert _mark(sim, tighter) == (Decimal("108.5"), Decimal("106.87"), OrderStatus.NEW)
        assert _d(sim.get_open_position("NVDA").qty) == 10
        sim.set_price("NVDA", "109.5")
        assert _mark(sim, tighter) == (Decimal("109.5"), Decimal("107.86"), OrderStatus.NEW)
    # From a new high on, both readings agree again; the replaced order is history and its
    # mark and stop stay where they were.
    sim.set_price("NVDA", "111")
    assert _mark(sim, tighter) == (111, Decimal("109.34"), OrderStatus.NEW)
    assert _mark(sim, order) == (110, Decimal("107.8"), OrderStatus.REPLACED)

    bogus: Any = "restart"
    with pytest.raises(ValueError, match="trailing_replace_mark"):
        FaultInjectingAlpacaSim(trailing_replace_mark=bogus)


@OFFLINE
@pytest.mark.parametrize("session", OFF_HOURS)
def test_trailing_stop_replace_off_hours_keeps_status_shares_and_waits_for_the_open(
    session: SimSession,
) -> None:
    sim = FaultInjectingAlpacaSim(session=session)
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    assert order.status is OrderStatus.NEW  # so it is replaceable in this session

    acknowledged = sim.replace_order_by_id(order.id, ReplaceOrderRequest(trail=1.0))
    assert acknowledged.status is OrderStatus.PENDING_NEW and acknowledged.replaces == order.id
    sim.advance(1)
    assert sim.market_session == session
    successor = sim.inspect_order(acknowledged.id)
    # "new" like the order it replaced, never "accepted"; the price has not moved, so the mark
    # is the same under either mark rule and only the trail is tighter.
    assert successor.status is OrderStatus.NEW
    assert (successor.hwm, successor.trail_percent, _d(successor.stop_price)) == ("100", "1", 99)
    assert sim.inspect_order(order.id).status is OrderStatus.REPLACED
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    assert [item.id for item in sim.inspect_orders("open")] == [successor.id]

    sim.set_price("NVDA", "90")  # far through the tightened stop, off-hours: nothing fills
    assert _mark(sim, successor) == (100, 99, OrderStatus.NEW)
    assert _d(sim.get_open_position("NVDA").qty) == 10
    sim.set_session("RTH")
    done = sim.inspect_order(successor.id)
    assert done.status is OrderStatus.FILLED and _d(done.filled_avg_price) == 90


@OFFLINE
def test_replace_stays_pending_until_time_passes_and_the_old_order_keeps_working() -> None:
    assert REPLACE_PENDINGS == ("until_time_passes", "until_completed", "never")
    sim = FaultInjectingAlpacaSim()
    assert sim.replace_pending == "until_time_passes"
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))

    ack = sim.replace_order_by_id(order.id, ReplaceOrderRequest(trail=1.0))
    # A success return with a new order id, which does not mean the order has been replaced.
    assert ack.id != order.id and ack.replaces == order.id
    assert ack.status is OrderStatus.PENDING_NEW
    assert (ack.hwm, ack.trail_percent, _d(ack.stop_price)) == ("100", "1", 99)
    old = sim.get_order_by_id(order.id)
    assert old.status is OrderStatus.PENDING_REPLACE
    assert old.replaced_by is None and old.replaced_at is None and _d(old.stop_price) == 98
    # Both are open orders; only the old one holds the shares.
    assert {item.id: item.status for item in sim.get_orders(GetOrdersRequest(limit=500))} == {
        order.id: OrderStatus.PENDING_REPLACE,
        ack.id: OrderStatus.PENDING_NEW,
    }
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    with pytest.raises(APIError) as one_more:
        sim.submit_order(_limit("NVDA", OrderSide.SELL, 1.0, 120.0))
    assert _refused(one_more)["held_for_orders"] == "10"

    # While it lasts neither order can be cancelled or replaced again.
    for stuck in (order.id, ack.id):
        with pytest.raises(APIError) as no_cancel:
            sim.cancel_order_by_id(stuck)
        assert no_cancel.value.status_code == 422 and "pending_" in no_cancel.value.message
        with pytest.raises(APIError) as no_replace:
            sim.replace_order_by_id(stuck, ReplaceOrderRequest(trail=0.5))
        assert no_replace.value.status_code == 422 and "pending_" in no_replace.value.message
    assert {item.id: item.status for item in sim.cancel_orders()} == {order.id: 500, ack.id: 500}
    assert len(sim.inspect_orders("open")) == 2

    # The old order is still the one at work: it ratchets, the waiting successor does not.
    sim.set_price("NVDA", "104")
    assert _mark(sim, order) == (104, Decimal("101.92"), OrderStatus.PENDING_REPLACE)
    assert _mark(sim, ack) == (100, 99, OrderStatus.PENDING_NEW)

    # Time passes: the replacement takes effect, and the successor's mark starts at the price
    # standing then (the default mark rule): 104 * 0.99.
    sim.advance(1)
    replaced = sim.get_order_by_id(order.id)
    assert replaced.status is OrderStatus.REPLACED and replaced.replaced_by == ack.id
    assert replaced.replaced_at == sim.now
    assert _mark(sim, ack) == (104, Decimal("102.96"), OrderStatus.NEW)
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    assert [item.id for item in sim.inspect_orders("open")] == [ack.id]
    sim.cancel_order_by_id(ack.id)  # an ordinary working order again

    # A print between the old stop and the tighter one sells nothing while the replacement
    # waits: the tighter stop is not at work yet.
    sim.open_position("TSLA", 5, "300")
    plain = sim.submit_order(_stop("TSLA", OrderSide.SELL, 5.0, 294.0))
    raised = sim.replace_order_by_id(plain.id, ReplaceOrderRequest(stop_price=297.0))
    assert raised.status is OrderStatus.PENDING_NEW
    sim.set_price("TSLA", "296")
    assert sim.inspect_order(plain.id).status is OrderStatus.PENDING_REPLACE
    assert _d(sim.get_open_position("TSLA").qty) == 5
    sim.advance(1)  # in force now, and 296 is through it
    sold = sim.inspect_order(raised.id)
    assert sold.status is OrderStatus.FILLED and _d(sold.filled_avg_price) == 296

    # If the old order fills before the replacement takes effect, the replacing order is
    # rejected (documented).
    sim.open_position("AMD", 5, "200")
    first = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=2.0))
    late = sim.replace_order_by_id(first.id, ReplaceOrderRequest(trail=1.0))
    sim.set_price("AMD", "195")
    filled = sim.inspect_order(first.id)
    assert filled.status is OrderStatus.FILLED and _d(filled.filled_avg_price) == 195
    assert filled.replaced_by is None
    rejected = sim.inspect_order(late.id)
    assert rejected.status is OrderStatus.REJECTED and rejected.failed_at is not None
    assert _d(rejected.filled_qty) == 0
    sim.advance(1)
    assert sim.inspect_order(late.id).status is OrderStatus.REJECTED
    assert sim.inspect_position("AMD") is None

    # A replacement that comes back rejected is not pending: the original simply works on.
    sim.open_position("META", 5, "500")
    kept = sim.submit_order(_trailing("META", OrderSide.SELL, 5, percent=2.0))
    sim.reject_next_order()
    refused = sim.replace_order_by_id(kept.id, ReplaceOrderRequest(trail=1.0))
    assert refused.status is OrderStatus.REJECTED and refused.replaces == kept.id
    assert sim.inspect_order(kept.id).status is OrderStatus.NEW
    sim.advance(1)
    assert _mark(sim, kept) == (500, 490, OrderStatus.NEW)

    # A day order: time passing puts the replacement in force, and the successor then expires
    # with the session like any day order.
    sim.open_position("SHOP", 5, "50")
    day = sim.submit_order(_trailing("SHOP", OrderSide.SELL, 5, percent=2.0, tif=TimeInForce.DAY))
    day_successor = sim.replace_order_by_id(day.id, ReplaceOrderRequest(trail=1.0))
    sim.set_session("AFTER_HOURS")
    assert sim.inspect_order(day.id).status is OrderStatus.REPLACED
    assert sim.inspect_order(day_successor.id).status is OrderStatus.EXPIRED

    bogus: Any = "soon"
    with pytest.raises(ValueError, match="replace_pending"):
        FaultInjectingAlpacaSim(replace_pending=bogus)


@OFFLINE
def test_replace_can_wait_for_complete_replace_and_be_ended_from_outside() -> None:
    sim = FaultInjectingAlpacaSim(replace_pending="until_completed")
    owner = sim.open_position("NVDA", 10, "100")
    first = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    second = sim.replace_order_by_id(first.id, ReplaceOrderRequest(trail=1.0))

    # No amount of time puts it in force, nor does a session change.
    sim.advance(3600)
    sim.set_session("AFTER_HOURS")
    assert sim.inspect_order(first.id).status is OrderStatus.PENDING_REPLACE
    assert sim.inspect_order(second.id).status is OrderStatus.PENDING_NEW
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    with pytest.raises(ValueError, match="no replacement waiting"):
        sim.complete_replace(owner.id)

    # The scripting call names the successor, the order being replaced, or nothing at all.
    sim.complete_replace(second.id)
    assert sim.inspect_order(first.id).status is OrderStatus.REPLACED
    assert _mark(sim, second) == (100, 99, OrderStatus.NEW)
    third = sim.replace_order_by_id(second.id, ReplaceOrderRequest(trail=0.5))
    sim.complete_replace(second.id)
    assert _mark(sim, third) == (100, Decimal("99.5"), OrderStatus.NEW)
    fourth = sim.replace_order_by_id(third.id, ReplaceOrderRequest(trail=0.4))
    sim.complete_replace()
    assert _mark(sim, fourth) == (100, Decimal("99.6"), OrderStatus.NEW)
    assert sim.inspect_order(third.id).replaced_by == fourth.id
    # None of the completions was a harness call.
    assert [call.name for call in sim.calls(kind="mutating")] == [
        "submit_order",
        "replace_order_by_id",
        "replace_order_by_id",
        "replace_order_by_id",
    ]

    # The owner or the broker can pull either order from outside. A successor that goes leaves
    # the old order as it was, replaceable again ...
    fifth = sim.replace_order_by_id(fourth.id, ReplaceOrderRequest(trail=0.3))
    sim.external_cancel(fifth.id)
    assert sim.inspect_order(fifth.id).status is OrderStatus.CANCELED
    assert sim.inspect_order(fourth.id).status is OrderStatus.NEW
    sixth = sim.replace_order_by_id(fourth.id, ReplaceOrderRequest(trail=0.3))
    assert sixth.status is OrderStatus.PENDING_NEW
    # ... and an old order that goes takes its waiting successor with it.
    sim.external_cancel(fourth.id)
    assert sim.inspect_order(fourth.id).status is OrderStatus.CANCELED
    assert sim.inspect_order(sixth.id).status is OrderStatus.REJECTED
    assert sim.inspect_orders("open") == []
    assert _d(sim.get_open_position("NVDA").qty_available) == 10

    # A day order that expires while its replacement waits leaves the replacement rejected.
    day_sim = FaultInjectingAlpacaSim(replace_pending="until_completed")
    day_sim.open_position("NVDA", 10, "100")
    day = day_sim.submit_order(
        _trailing("NVDA", OrderSide.SELL, 10, percent=2.0, tif=TimeInForce.DAY)
    )
    waiting = day_sim.replace_order_by_id(day.id, ReplaceOrderRequest(trail=1.0))
    day_sim.set_session("AFTER_HOURS")
    assert day_sim.inspect_order(day.id).status is OrderStatus.EXPIRED
    assert day_sim.inspect_order(waiting.id).status is OrderStatus.REJECTED


@OFFLINE
@pytest.mark.parametrize("mark_mode", REPLACE_MARKS)
def test_successor_mark_is_taken_when_the_replacement_takes_effect(mark_mode: str) -> None:
    switches: Any = {"trailing_replace_mark": mark_mode}
    sim = FaultInjectingAlpacaSim(**switches)
    sim.open_position("NVDA", 10, "100")
    order = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    ack = sim.replace_order_by_id(order.id, ReplaceOrderRequest(trail=1.0))
    # The acknowledgement shows the figures of the request; they are provisional.
    assert (ack.hwm, _d(ack.stop_price)) == ("100", 99)
    if mark_mode == "keep":
        # While the replacement waits the old order's mark rises to 104, then the price eases.
        sim.set_price("NVDA", "104")
        sim.set_price("NVDA", "103")
        expected = (Decimal(104), Decimal("102.96"))  # the mark the old order has by then
    else:
        # While the replacement waits the price falls, staying above the old 98 stop.
        sim.set_price("NVDA", "99")
        expected = (Decimal(99), Decimal("98.01"))  # the price standing by then
    assert _mark(sim, ack) == (100, 99, OrderStatus.PENDING_NEW)
    sim.advance(1)
    assert _mark(sim, ack) == (*expected, OrderStatus.NEW)
    assert sim.inspect_order(order.id).status is OrderStatus.REPLACED
    assert _d(sim.get_open_position("NVDA").qty) == 10  # neither figure sold the shares


@OFFLINE
def test_replace_interval_keeps_fills_statuses_and_timestamps() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    ask = sim.submit_order(_limit("NVDA", OrderSide.SELL, 10, 105.0, tif=TimeInForce.GTC))
    sim.fill_partial(ask.id, 4)  # four shares go at the 105 limit
    assert sim.inspect_order(ask.id).status is OrderStatus.PARTIALLY_FILLED
    sim.advance(30)
    requested_at = sim.now
    moved = sim.replace_order_by_id(ask.id, ReplaceOrderRequest(limit_price=106.0))
    assert moved.status is OrderStatus.PENDING_NEW and _d(moved.filled_qty) == 0
    # The request itself touches the old order: its updated_at moves, nothing else does.
    old = sim.inspect_order(ask.id)
    assert old.status is OrderStatus.PENDING_REPLACE and _d(old.limit_price) == 105
    assert old.updated_at == requested_at and old.updated_at != old.created_at
    # No time passing is no time passing: the replacement still waits.
    sim.advance(0)
    assert sim.inspect_order(ask.id).status is OrderStatus.PENDING_REPLACE
    assert sim.inspect_order(moved.id).status is OrderStatus.PENDING_NEW
    sim.advance(1)
    # The successor carries the fills and the status of the order it replaced, and holds only
    # what is left to sell.
    successor = sim.inspect_order(moved.id)
    assert successor.status is OrderStatus.PARTIALLY_FILLED
    assert (_d(successor.qty), _d(successor.filled_qty)) == (10, 4)
    assert _d(successor.filled_avg_price) == 105 and _d(successor.limit_price) == 106
    assert sim.inspect_order(ask.id).status is OrderStatus.REPLACED
    position = sim.get_open_position("NVDA")
    assert _d(position.qty) == 6 and _d(position.qty_available) == 0
    with pytest.raises(APIError) as none_left:
        sim.submit_order(_limit("NVDA", OrderSide.SELL, 1, 107.0))
    assert _refused(none_left)["held_for_orders"] == "6"

    # If the old order fills past the new quantity while the replacement waits, nothing is
    # left to replace: the replacing order is rejected and the old one works on.
    sim.open_position("TSLA", 10, "300")
    stop = sim.submit_order(_stop("TSLA", OrderSide.SELL, 10.0, 290.0))
    smaller = sim.replace_order_by_id(stop.id, ReplaceOrderRequest(qty=4))
    sim.fill_partial(stop.id, 6, "289")
    sim.advance(1)
    late = sim.inspect_order(smaller.id)
    assert late.status is OrderStatus.REJECTED and _d(late.filled_qty) == 0
    working = sim.inspect_order(stop.id)
    assert working.status is OrderStatus.PARTIALLY_FILLED and working.replaced_by is None
    assert (_d(working.qty), _d(working.filled_qty)) == (10, 6)
    assert _d(sim.get_open_position("TSLA").qty_available) == 0


@OFFLINE
@pytest.mark.parametrize("mode", ["until_time_passes", "until_completed"])
@pytest.mark.parametrize("taker", ["api_sell", "hand_sale"])
def test_pending_replacement_whose_shares_were_taken_is_rejected_when_it_takes_effect(
    mode: str, taker: str
) -> None:
    switches: Any = {"replace_pending": mode}
    sim = FaultInjectingAlpacaSim(**switches)
    assert sim.pending_replace_holds_increase is False
    sim.open_position("NVDA", 10, "100")
    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 5.0, 95.0))
    # Five shares are free, so growing the stop to the whole position is accepted ...
    grown = sim.replace_order_by_id(stop.id, ReplaceOrderRequest(qty=10))
    assert grown.status is OrderStatus.PENDING_NEW and _d(grown.qty) == 10
    # ... but the waiting successor holds nothing: the five shares can still be taken.
    assert _d(sim.get_open_position("NVDA").qty_available) == 5
    expected_open = [stop.id]
    if taker == "api_sell":
        second = sim.submit_order(_stop("NVDA", OrderSide.SELL, 5.0, 94.0))
        assert second.status is OrderStatus.NEW
        expected_open.append(second.id)
    else:
        sim.open_position("NVDA", -5, "100")  # the owner sells the free shares by hand
    if mode == "until_time_passes":
        sim.advance(1)
    else:
        sim.advance(3600)
        assert sim.inspect_order(grown.id).status is OrderStatus.PENDING_NEW
        sim.complete_replace(grown.id)

    # The replacement no longer fits. The replacing order is rejected, after its
    # acknowledgement, and the old order works on at its old size.
    rejected = sim.inspect_order(grown.id)
    assert rejected.status is OrderStatus.REJECTED and rejected.failed_at == sim.now
    kept = sim.inspect_order(stop.id)
    assert kept.status is OrderStatus.NEW and _d(kept.qty) == 5
    assert kept.replaced_by is None and kept.replaced_at is None
    assert [item.id for item in sim.inspect_orders("open")] == expected_open
    assert _d(sim.get_open_position("NVDA").qty_available) == 0  # never negative
    # No resting sell exceeds the position: a print through every stop ends flat, not short.
    sim.set_price("NVDA", "90")
    assert sim.inspect_position("NVDA") is None
    assert sim.inspect_orders("open") == []


@OFFLINE
def test_replacement_recheck_covers_a_short_and_buying_power_and_can_hold_the_increase() -> None:
    # Effective at once there is no interval: the later sell and the hand sale are refused.
    at_once = FaultInjectingAlpacaSim(replace_pending="never")
    at_once.open_position("NVDA", 10, "100")
    stop = at_once.submit_order(_stop("NVDA", OrderSide.SELL, 5.0, 95.0))
    grown = at_once.replace_order_by_id(stop.id, ReplaceOrderRequest(qty=10))
    assert grown.status is OrderStatus.NEW
    with pytest.raises(APIError) as second:
        at_once.submit_order(_stop("NVDA", OrderSide.SELL, 5.0, 94.0))
    with pytest.raises(APIError) as by_hand:
        at_once.open_position("NVDA", -5, "100")
    assert second.value.status_code == by_hand.value.status_code == 403
    assert _refused(second)["available"] == _refused(by_hand)["available"] == "0"

    # The other reading of the interval: a quantity increase holds its extra shares from the
    # request on, so the later order is the one refused and the replacement goes through.
    holding = FaultInjectingAlpacaSim(pending_replace_holds_increase=True)
    holding.open_position("NVDA", 10, "100")
    stop = holding.submit_order(_stop("NVDA", OrderSide.SELL, 5.0, 95.0))
    grown = holding.replace_order_by_id(stop.id, ReplaceOrderRequest(qty=10))
    assert grown.status is OrderStatus.PENDING_NEW
    assert _d(holding.get_open_position("NVDA").qty_available) == 0
    with pytest.raises(APIError) as second:
        holding.submit_order(_stop("NVDA", OrderSide.SELL, 5.0, 94.0))
    assert second.value.status_code == 403 and _refused(second)["held_for_orders"] == "10"
    with pytest.raises(APIError) as by_hand:
        holding.open_position("NVDA", -5, "100")
    assert by_hand.value.status_code == 403
    holding.advance(1)
    assert holding.inspect_order(grown.id).status is OrderStatus.NEW
    assert holding.inspect_order(stop.id).status is OrderStatus.REPLACED
    assert _d(holding.get_open_position("NVDA").qty_available) == 0
    # A decrease frees nothing before it takes effect, under either setting.
    smaller = holding.replace_order_by_id(grown.id, ReplaceOrderRequest(qty=4))
    assert _d(holding.get_open_position("NVDA").qty_available) == 0
    holding.advance(1)
    assert holding.inspect_order(smaller.id).status is OrderStatus.NEW
    assert _d(holding.get_open_position("NVDA").qty_available) == 6

    # The mirror for a short: the owner covers the free shares by hand while a larger buy stop
    # waits. The larger stop is rejected, and its trigger cannot turn the short into a long.
    short = FaultInjectingAlpacaSim()
    short.open_position("TSLA", -10, "300")
    cover = short.submit_order(_stop("TSLA", OrderSide.BUY, 5.0, 306.0))
    larger = short.replace_order_by_id(cover.id, ReplaceOrderRequest(qty=10))
    short.open_position("TSLA", 5, "300")
    short.advance(1)
    assert short.inspect_order(larger.id).status is OrderStatus.REJECTED
    assert short.inspect_order(cover.id).status is OrderStatus.NEW
    assert _d(short.get_open_position("TSLA").qty_available) == 0
    short.set_price("TSLA", "310")
    assert short.inspect_position("TSLA") is None

    # The same check covers buying power, which no waiting replacement holds ahead.
    poor = FaultInjectingAlpacaSim()
    poor.set_price("AMD", "100")
    poor.set_account(buying_power="2000")
    bid = poor.submit_order(_limit("AMD", OrderSide.BUY, 5, 90.0, tif=TimeInForce.GTC))
    more = poor.replace_order_by_id(bid.id, ReplaceOrderRequest(qty=15))
    assert more.status is OrderStatus.PENDING_NEW
    poor.set_account(buying_power="1000")  # spent elsewhere meanwhile: 15 x 90 no longer fits
    poor.advance(1)
    assert poor.inspect_order(more.id).status is OrderStatus.REJECTED
    kept = poor.inspect_order(bid.id)
    assert kept.status is OrderStatus.NEW and _d(kept.qty) == 5


@OFFLINE
def test_replace_is_refused_for_an_order_that_is_only_accepted() -> None:
    # Documented in the Trading API reference: an order in status accepted (received, not yet
    # routed) cannot be replaced. A plain stop queued off-hours is such an order.
    sim = FaultInjectingAlpacaSim(session="AFTER_HOURS", replace_pending="never")
    sim.open_position("NVDA", 10, "100")
    stop = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 98.0))
    assert stop.status is OrderStatus.ACCEPTED
    with pytest.raises(APIError) as queued:
        sim.replace_order_by_id(stop.id, ReplaceOrderRequest(stop_price=99.0))
    assert queued.value.status_code == 422 and '"accepted"' in queued.value.message
    assert _d(sim.inspect_order(stop.id).stop_price) == 98
    assert [item.id for item in sim.inspect_orders("all", "NVDA")][-1] == stop.id  # no successor

    # A trailing stop in the same session reads "new" and is replaceable there.
    sim.open_position("AMD", 5, "200")
    trail = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=1.0))
    tighter = sim.replace_order_by_id(trail.id, ReplaceOrderRequest(trail=0.5))
    assert tighter.status is OrderStatus.NEW and _d(tighter.stop_price) == 199

    # Once the regular session opens the stop is at the exchange and can be replaced.
    sim.set_session("RTH")
    moved = sim.replace_order_by_id(stop.id, ReplaceOrderRequest(stop_price=99.0))
    assert moved.status is OrderStatus.NEW and _d(moved.stop_price) == 99


@OFFLINE
def test_replace_that_changes_nothing_is_refused_unless_switched_off() -> None:
    # alpaca-py's ReplaceOrderRequest has no trail_percent / trail_price field and silently
    # drops them: a lane that tightens with the submit-side name sends an empty PATCH.
    dropped: Any = {"trail_percent": 0.5}
    assert ReplaceOrderRequest(**dropped).to_request_fields() == {}

    sim = FaultInjectingAlpacaSim(replace_pending="never")
    assert sim.empty_replace_refused is True
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", 5, "200")
    trail = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    stop = sim.submit_order(_stop("AMD", OrderSide.SELL, 5.0, 190.0))
    for order in (trail, stop):
        for request in (
            None,
            ReplaceOrderRequest(**dropped),
            ReplaceOrderRequest(client_order_id=f"renamed-{order.symbol}"),
        ):
            with pytest.raises(APIError) as empty:
                sim.replace_order_by_id(order.id, request)
            assert empty.value.status_code == 422 and "changes nothing" in empty.value.message
        assert sim.inspect_order(order.id).status is OrderStatus.NEW
    with pytest.raises(APIError) as raw:
        sim.patch(f"/orders/{trail.id}", {})
    assert raw.value.status_code == 422
    assert [item.id for item in sim.inspect_orders("open")] == [trail.id, stop.id]

    # Switched off, the same request "succeeds" and tightens nothing: a new id, the old trail.
    lenient = FaultInjectingAlpacaSim(replace_pending="never", empty_replace_refused=False)
    lenient.open_position("NVDA", 10, "100")
    same = lenient.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    twin = lenient.replace_order_by_id(same.id, ReplaceOrderRequest(**dropped))
    assert twin.id != same.id and twin.status is OrderStatus.NEW
    assert (twin.trail_percent, _d(twin.stop_price)) == ("2", 98)
    renamed = lenient.replace_order_by_id(twin.id, ReplaceOrderRequest(client_order_id="renamed"))
    assert renamed.client_order_id == "renamed" and renamed.trail_percent == "2"
    # Only the empty body is let through: the submit-side names stay refused for raw callers,
    # also next to a valid trail.
    for body in (
        {"trail_price": "2"},
        {"trail_percent": "0.5"},
        {"trail": "1", "trail_percent": "1"},
    ):
        with pytest.raises(APIError) as named:
            lenient.patch(f"/orders/{renamed.id}", body)
        assert named.value.status_code == 422, body
        assert "cannot be replaced; send trail" in named.value.message, body
    assert lenient.inspect_order(renamed.id).status is OrderStatus.NEW


@OFFLINE
def test_trailing_stop_submit_rejections() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")

    # alpaca-py refuses both-or-neither before any request leaves the client ...
    with pytest.raises(ValueError, match="Either trail_percent or trail_price must be set"):
        _trailing("NVDA", OrderSide.SELL, 1)
    with pytest.raises(ValueError, match="Both trail_percent and trail_price cannot be set"):
        _trailing("NVDA", OrderSide.SELL, 1, percent=1.0, price=1.0)
    assert sim.mutating_call_count == 0

    # ... and the broker-side rules are modelled for everything that does reach it.
    base = {
        "symbol": "NVDA",
        "qty": "1",
        "side": "sell",
        "type": "trailing_stop",
        "time_in_force": "gtc",
    }
    one = {**base, "trail_percent": "1"}
    oto = {
        "symbol": "NVDA",
        "qty": "1",
        "side": "buy",
        "type": "limit",
        "limit_price": "99",
        "time_in_force": "gtc",
        "order_class": "oto",
    }
    refused: list[dict[str, Any]] = [
        base,  # neither trail field
        {**one, "trail_price": "1"},  # both
        {**base, "trail_percent": "0"},
        {**base, "trail_price": "-1"},
        {**one, "time_in_force": "ioc"},
        {**one, "time_in_force": "fok"},
        {**one, "time_in_force": "opg"},
        {**one, "extended_hours": True},
        {**one, "stop_price": "95"},
        {**one, "limit_price": "95"},
        # Fractional quantities: the fractional-order table does not list trailing stops.
        {**one, "qty": "0.5"},
        {**one, "qty": "0.5", "time_in_force": "day"},
        # Single orders only: no complex class, no legs of its own ...
        {**one, "order_class": "oto", "stop_loss": {"stop_price": "95"}},
        {**one, "order_class": "bracket"},
        {**one, "order_class": "oco"},
        {**one, "stop_loss": {"stop_price": "95"}},
        # ... and never as the leg of another order.
        {**oto, "stop_loss": {"stop_price": "95", "trail_percent": "1"}},
        {**oto, "stop_loss": {"trail_price": "2"}},
        {**oto, "stop_loss": {"type": "trailing_stop", "stop_price": "95"}},
        {**oto, "order_class": "bracket", "take_profit": {"trail_percent": "1"}},
    ]
    for body in refused:
        with pytest.raises(APIError) as error:
            sim.post("/orders", body)
        assert error.value.status_code == 422, body
    assert sim.inspect_orders("open") == []  # nothing was created ...
    assert sim.mutating_call_count == len(refused)  # ... though every attempt reached the broker
    assert _d(sim.get_open_position("NVDA").qty_available) == 10

    # A sell stop at or below zero is left unmodelled rather than guessed.
    with pytest.raises(NotImplementedError, match="sell trail_percent of 100 or more"):
        sim.post("/orders", {**base, "trail_percent": "100"})
    # Each rule above is what refused the order: the same bodies with a valid shape rest.
    by_percent: Any = sim.post("/orders", one)
    by_price: Any = sim.post("/orders", {**base, "trail_price": "1", "time_in_force": "day"})
    assert (by_percent["status"], by_percent["trail_percent"], by_percent["stop_price"]) == (
        "new",
        "1",
        "99",
    )
    assert (by_price["type"], by_price["trail_price"], by_price["stop_price"]) == (
        "trailing_stop",
        "1",
        "99",
    )

    # With the switch a fractional trailing stop follows the general fractional rule: day only.
    fractional = FaultInjectingAlpacaSim(fractional_trailing_stops=True)
    assert sim.fractional_trailing_stops is False
    fractional.open_position("NVDA", "1.5", "100")
    part: Any = fractional.post("/orders", {**one, "qty": "0.5", "time_in_force": "day"})
    assert (part["status"], part["qty"], part["stop_price"]) == ("new", "0.5", "99")
    with pytest.raises(APIError) as gtc_fraction:
        fractional.post("/orders", {**one, "qty": "0.5"})
    assert gtc_fraction.value.status_code == 422 and "DAY" in gtc_fraction.value.message


@OFFLINE
def test_trail_finer_than_any_observed_is_refused_unless_switched_off() -> None:
    unquantised = 0.1 + 0.2  # 0.30000000000000004: a trail that skipped the lane's rounding
    sim = FaultInjectingAlpacaSim(replace_pending="never")
    assert sim.strict_trail_precision is True
    sim.open_position("NVDA", 10, "100")
    base = {
        "symbol": "NVDA",
        "qty": "1",
        "side": "sell",
        "type": "trailing_stop",
        "time_in_force": "gtc",
    }
    for body in (
        {**base, "trail_percent": "0.125"},
        {**base, "trail_percent": unquantised},
        {**base, "trail_price": "0.005"},
        {**base, "trail_price": "1.001"},
    ):
        with pytest.raises(APIError) as too_fine:
            sim.post("/orders", body)
        assert too_fine.value.status_code == 422, body
        assert "decimal places" in too_fine.value.message
    # 0.16 %, the finest trail observed on the broker, is accepted: 99.84 under a 100 mark.
    fine = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 1, percent=0.16))
    assert fine.status is OrderStatus.NEW and _d(fine.stop_price) == Decimal("99.84")
    # The replace path is held to the same step.
    with pytest.raises(APIError) as on_replace:
        sim.replace_order_by_id(fine.id, ReplaceOrderRequest(trail=unquantised))
    assert on_replace.value.status_code == 422 and "decimal places" in on_replace.value.message
    assert sim.inspect_order(fine.id).trail_percent == "0.16"

    # Below 1.00 the price tick is 0.0001, and so is the finest dollar trail.
    sim.open_position("PENNY", 100, "0.8123")
    penny = sim.submit_order(_trailing("PENNY", OrderSide.SELL, 100, price=0.0125))
    assert _mark(sim, penny) == (Decimal("0.8123"), Decimal("0.7998"), OrderStatus.NEW)
    with pytest.raises(APIError) as sub_tick:
        sim.submit_order(_trailing("PENNY", OrderSide.SELL, 100, price=0.00125))
    assert sub_tick.value.status_code == 422
    # A percent trail is a percentage at any price level: two decimals there as well.
    with pytest.raises(APIError) as fine_percent:
        sim.submit_order(_trailing("PENNY", OrderSide.SELL, 100, percent=0.125))
    assert fine_percent.value.status_code == 422

    lenient = FaultInjectingAlpacaSim(strict_trail_precision=False)
    lenient.open_position("NVDA", 10, "100")
    odd = lenient.submit_order(_trailing("NVDA", OrderSide.SELL, 1, percent=unquantised))
    assert odd.trail_percent == "0.30000000000000004"
    assert _d(odd.stop_price) == Decimal("99.7")  # the stop is quantised all the same


@OFFLINE
def test_day_trailing_stop_expires_and_gtc_survives_the_session_rollover() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", 5, "200")
    day = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0, tif=TimeInForce.DAY))
    gtc = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=1.0, tif=TimeInForce.GTC))
    assert day.expires_at is None and gtc.expires_at is not None
    sim.set_price("AMD", "210")
    assert _mark(sim, gtc) == (210, Decimal("207.9"), OrderStatus.NEW)

    # The regular session ends: the day order expires and frees its shares, the gtc order rests.
    sim.set_session("AFTER_HOURS")
    expired = sim.inspect_order(day.id)
    assert expired.status is OrderStatus.EXPIRED and expired.expired_at is not None
    assert session_at(expired.expired_at) == "AFTER_HOURS"
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    assert _mark(sim, gtc) == (210, Decimal("207.9"), OrderStatus.NEW)
    # An expired order is history: a later regular-hours high does not move its mark.
    sim.set_session("RTH")
    sim.set_price("NVDA", "130")
    assert _mark(sim, day) == (100, 99, OrderStatus.EXPIRED)
    sim.set_price("NVDA", "100")

    # Overnight, the next sessions and a weekend later it still holds its mark and its shares.
    started = sim.now
    for session in (
        "AFTER_HOURS",
        "OVERNIGHT",
        "PRE_MARKET",
        "RTH",
        "AFTER_HOURS",
        "CLOSED",
        "RTH",
    ):
        sim.set_session(session)
        assert _mark(sim, gtc) == (210, Decimal("207.9"), OrderStatus.NEW)
        assert _d(sim.get_open_position("AMD").qty_available) == 0
    assert sim.now - started > timedelta(days=2)
    # An explicit jump of the virtual clock, a week on, changes nothing either.
    sim.set_time(sim.now + timedelta(days=7))
    sim.advance(90)
    assert sim.market_session == "RTH"
    assert _mark(sim, gtc) == (210, Decimal("207.9"), OrderStatus.NEW)
    assert _d(sim.get_open_position("AMD").qty_available) == 0
    assert sim.inspect_order(day.id).status is OrderStatus.EXPIRED

    # It keeps trailing and triggering in the later session.
    sim.set_price("AMD", "220")
    assert _mark(sim, gtc) == (220, Decimal("217.8"), OrderStatus.NEW)
    sim.set_price("AMD", "217")
    done = sim.inspect_order(gtc.id)
    assert done.status is OrderStatus.FILLED and _d(done.filled_avg_price) == 217
    assert sim.inspect_position("AMD") is None


_SESSIONS_UNTIL_THE_OPEN: dict[SimSession, tuple[SimSession, ...]] = {
    "AFTER_HOURS": ("OVERNIGHT", "PRE_MARKET", "RTH"),
    "OVERNIGHT": ("PRE_MARKET", "RTH"),
    "PRE_MARKET": ("RTH",),
    "CLOSED": ("OVERNIGHT", "PRE_MARKET", "RTH"),
}


@OFFLINE
@pytest.mark.parametrize("session", OFF_HOURS)
def test_day_trailing_stop_placed_off_hours_lives_through_the_next_regular_session(
    session: SimSession,
) -> None:
    sim = FaultInjectingAlpacaSim(session=session)
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", 5, "200")
    day = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0, tif=TimeInForce.DAY))
    gtc = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=1.0, tif=TimeInForce.GTC))
    assert day.status is OrderStatus.NEW and gtc.status is OrderStatus.NEW

    # Every session change up to the next regular session leaves the day order open.
    for upcoming in _SESSIONS_UNTIL_THE_OPEN[session]:
        sim.set_session(upcoming)
        assert sim.market_session == upcoming
        assert _mark(sim, day) == (100, 99, OrderStatus.NEW)
        assert _d(sim.get_open_position("NVDA").qty_available) == 0
    # It works in that session ...
    sim.set_price("NVDA", "103")
    assert _mark(sim, day) == (103, Decimal("101.97"), OrderStatus.NEW)
    # ... and expires at its close, freeing the shares, while the gtc twin rests.
    sim.set_session("AFTER_HOURS")
    expired = sim.inspect_order(day.id)
    assert expired.status is OrderStatus.EXPIRED and expired.expired_at == sim.now
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    assert _mark(sim, gtc) == (200, 198, OrderStatus.NEW)
    assert _d(sim.get_open_position("AMD").qty_available) == 0


@OFFLINE
def test_gtc_orders_carry_expires_at_and_the_aged_order_job_leaves_them_pending_cancel() -> None:
    start = datetime(2026, 11, 27, 10, 0, tzinfo=NEW_YORK)
    # 16:15 New York on the 90th calendar day after creation.
    due = datetime(2027, 2, 25, 16, 15, tzinfo=NEW_YORK)
    sim = FaultInjectingAlpacaSim(start=start)
    assert sim.aged_cancel_waits_for_venue is True
    sim.open_position("NVDA", 10, "100")
    sim.set_price("AMD", "200")
    gtc = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 6, percent=2.0))
    day = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 4, percent=2.0, tif=TimeInForce.DAY))
    bid = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 1, 50.0, tif=TimeInForce.GTC, extended=True)
    )
    # A gtc order that is done long before its 90 days are up.
    bought = sim.submit_order(_limit("AMD", OrderSide.BUY, 1, 200.0, tif=TimeInForce.GTC))
    filled = sim.inspect_order(bought.id)
    assert filled.status is OrderStatus.FILLED and filled.expires_at == due
    assert gtc.expires_at == due == bid.expires_at and day.expires_at is None
    assert sim.get_order_by_id(gtc.id).expires_at == due
    harness_calls = sim.mutating_call_count

    sim.set_time(due - timedelta(minutes=1))
    assert sim.inspect_order(gtc.id).status is OrderStatus.NEW
    assert sim.inspect_order(bid.id).status is OrderStatus.NEW
    assert _d(sim.get_open_position("NVDA").qty_available) == 4  # the day order expired long ago
    sim.set_time(due)
    # Documented: the job submits a cancel request and the orders "remain in pending_cancel
    # until canceled by the execution venue". They are still open and still hold the shares.
    aged = sim.inspect_order(gtc.id)
    assert aged.status is OrderStatus.PENDING_CANCEL and aged.canceled_at is None
    assert aged.updated_at == due
    assert sim.inspect_order(bid.id).status is OrderStatus.PENDING_CANCEL
    assert [item.id for item in sim.inspect_orders("open")] == [gtc.id, bid.id]
    assert _d(sim.get_open_position("NVDA").qty_available) == 4
    # No call by the harness: only a read of the orders shows that the cover is lapsing.
    assert sim.mutating_call_count == harness_calls
    # The job asks for open orders only: an order that is done is left as it was.
    untouched = sim.inspect_order(bought.id)
    assert untouched.status is OrderStatus.FILLED and untouched.updated_at == filled.updated_at
    # A lane that re-covers the lapsing shares at once is refused: they are not free yet.
    recover = _trailing("NVDA", OrderSide.SELL, 10, percent=2.0)
    with pytest.raises(APIError) as too_early:
        sim.submit_order(recover)
    assert too_early.value.status_code == 403 and sim.call_log[-1].refusal == "insufficient_qty"
    assert _refused(too_early)["held_for_orders"] == "6"
    # Time alone does not end the wait ...
    sim.set_time(due + timedelta(days=1))
    assert sim.inspect_order(gtc.id).status is OrderStatus.PENDING_CANCEL
    assert _d(sim.get_open_position("NVDA").qty_available) == 4
    # ... the venue does: the shares are free and the same re-cover goes through.
    sim.complete_cancel()
    gone = sim.inspect_order(gtc.id)
    assert gone.status is OrderStatus.CANCELED and gone.canceled_at == sim.now
    assert sim.inspect_order(bid.id).status is OrderStatus.CANCELED
    assert sim.inspect_orders("open") == []
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    assert sim.submit_order(recover).status is OrderStatus.NEW

    # Until then the order is still at the venue: a regular-hours print through its stop sells.
    working = FaultInjectingAlpacaSim(start=start)
    working.open_position("NVDA", 10, "100")
    resting = working.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    working.set_time(due)
    assert working.inspect_order(resting.id).status is OrderStatus.PENDING_CANCEL
    working.set_session("RTH")
    working.set_price("NVDA", "97")
    sold = working.inspect_order(resting.id)
    assert sold.status is OrderStatus.FILLED and _d(sold.filled_avg_price) == 97

    # Switched off, the request is applied at once: the order reads canceled at 16:15 and the
    # cover has lapsed. Closed orders are left alone here too.
    at_once = FaultInjectingAlpacaSim(start=start, aged_cancel_waits_for_venue=False)
    at_once.open_position("NVDA", 10, "100")
    at_once.set_price("AMD", "200")
    lapsed = at_once.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    done = at_once.submit_order(_limit("AMD", OrderSide.BUY, 1, 200.0, tif=TimeInForce.GTC))
    at_once.set_time(due)
    cancelled = at_once.inspect_order(lapsed.id)
    assert cancelled.status is OrderStatus.CANCELED and cancelled.canceled_at == due
    assert _d(at_once.get_open_position("NVDA").qty_available) == 10
    kept = at_once.inspect_order(done.id)
    assert kept.status is OrderStatus.FILLED and kept.canceled_at is None

    # The 90 days count from the New York date of creation: 21:00 on 2026-11-30 in New York
    # is already 2026-12-01 in UTC.
    evening = FaultInjectingAlpacaSim(start=datetime(2026, 11, 30, 21, 0, tzinfo=NEW_YORK))
    evening.open_position("NVDA", 10, "100")
    late = evening.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    assert late.created_at == datetime(2026, 12, 1, 2, 0, tzinfo=UTC)
    assert late.expires_at == datetime(2027, 2, 28, 16, 15, tzinfo=NEW_YORK)

    # A replacement is a new order: it carries its own 90 days.
    later = FaultInjectingAlpacaSim(start=start, replace_pending="never")
    later.open_position("NVDA", 10, "100")
    first = later.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    later.set_time(start + timedelta(days=31))
    second = later.replace_order_by_id(first.id, ReplaceOrderRequest(trail=1.0))
    assert second.expires_at == datetime(2027, 3, 28, 16, 15, tzinfo=NEW_YORK)
    later.set_time(due)
    assert later.inspect_order(second.id).status is OrderStatus.NEW

    # A defer_cancels script that covers the order decides instead of the switch, either way:
    # scripted to keep its status, the order reads "new" after the job asked ...
    scripted = FaultInjectingAlpacaSim(start=start)
    scripted.open_position("NVDA", 10, "100")
    held = scripted.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    scripted.defer_cancels()
    scripted.set_time(due + timedelta(days=1))
    asked = scripted.inspect_order(held.id)
    assert asked.status is OrderStatus.NEW and asked.updated_at == due
    assert _d(scripted.get_open_position("NVDA").qty_available) == 0
    scripted.complete_cancel(held.id)
    assert scripted.inspect_order(held.id).status is OrderStatus.CANCELED
    # ... and with the switch off a script still leaves the job's request unapplied.
    stuck = FaultInjectingAlpacaSim(start=start, aged_cancel_waits_for_venue=False)
    stuck.open_position("NVDA", 10, "100")
    held = stuck.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    stuck.defer_cancels(show_pending_cancel=True)
    stuck.set_time(due + timedelta(days=1))
    assert stuck.inspect_order(held.id).status is OrderStatus.PENDING_CANCEL
    assert _d(stuck.get_open_position("NVDA").qty_available) == 0
    stuck.complete_cancel(held.id)
    assert stuck.inspect_order(held.id).status is OrderStatus.CANCELED


@OFFLINE
def test_trailing_stop_on_a_market_holiday_and_an_early_close_day() -> None:
    def at(day: int, hour: int, minute: int = 0) -> datetime:
        return datetime(2026, 11, day, hour, minute, tzinfo=NEW_YORK)

    # Thanksgiving 2026 at 11:00 New York: a weekday morning with the market shut all day.
    sim = FaultInjectingAlpacaSim(start=at(26, 11))
    assert sim.market_session == "CLOSED" and sim.get_clock().is_open is False
    sim.open_position("NVDA", 10, "100")
    gtc = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 6, percent=1.0))
    day = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 4, percent=1.0, tif=TimeInForce.DAY))
    assert gtc.status is OrderStatus.NEW and day.status is OrderStatus.NEW
    sim.set_price("NVDA", "90")
    sim.set_time(at(26, 15))
    assert sim.market_session == "CLOSED"
    assert _mark(sim, gtc) == (100, 99, OrderStatus.NEW)
    assert _mark(sim, day) == (100, 99, OrderStatus.NEW)
    assert _d(sim.get_open_position("NVDA").qty) == 10
    sim.set_price("NVDA", "100")

    # Friday is a half day: the regular session runs 09:30 to 13:00, after-hours to 17:00.
    clock = sim.get_clock()
    assert (clock.next_open, clock.next_close) == (at(27, 9, 30), at(27, 13))
    sim.set_session("RTH")
    assert sim.now == at(27, 9, 30)
    sim.set_price("NVDA", "104")
    assert _mark(sim, day) == (104, Decimal("102.96"), OrderStatus.NEW)
    sim.set_time(at(27, 12, 59))
    assert sim.market_session == "RTH"
    assert sim.inspect_order(day.id).status is OrderStatus.NEW
    sim.set_time(at(27, 13))
    expired = sim.inspect_order(day.id)
    assert expired.status is OrderStatus.EXPIRED and expired.expired_at == at(27, 13)
    assert sim.market_session == "AFTER_HOURS"

    # 14:12, the instant of the observation: a price through the stop does nothing there.
    sim.set_time(OBSERVED_AT)
    sim.set_price("NVDA", "95")
    assert _mark(sim, gtc) == (104, Decimal("102.96"), OrderStatus.NEW)
    sim.set_time(at(27, 17))
    assert sim.market_session == "CLOSED"
    assert _mark(sim, gtc) == (104, Decimal("102.96"), OrderStatus.NEW)
    # Monday's opening print is where it sells.
    sim.set_session("RTH")
    assert sim.now == at(30, 9, 30)
    done = sim.inspect_order(gtc.id)
    assert done.status is OrderStatus.FILLED and _d(done.filled_avg_price) == 95


# ------------------------------------------------- extended-hours gtc limit, cancels


@OFFLINE
def test_gtc_extended_hours_limit_is_accepted_and_rests_across_sessions() -> None:
    # Documented ("day or gtc") and the shape of the owner's resting order on 2026-11-27.
    sim = FaultInjectingAlpacaSim(session="AFTER_HOURS")
    sim.set_price("NVDA", "100")
    resting = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 5, 95.0, tif=TimeInForce.GTC, extended=True, cid="gtc-ext")
    )
    day = sim.submit_order(_limit("NVDA", OrderSide.BUY, 5, 94.0, extended=True, cid="day-ext"))
    assert resting.status is OrderStatus.NEW and day.status is OrderStatus.NEW
    assert resting.extended_hours is True and resting.time_in_force is TimeInForce.GTC
    assert resting.expires_at is not None

    # The day twin ends with after-hours; the gtc order rests on through the night, the next
    # regular session and a weekend.
    for session in ("OVERNIGHT", "PRE_MARKET", "RTH", "AFTER_HOURS", "CLOSED", "OVERNIGHT"):
        sim.set_session(session)
        assert sim.inspect_order(resting.id).status is OrderStatus.NEW
        assert sim.inspect_order(day.id).status is OrderStatus.EXPIRED
    # It is still an extended-hours order: it executes in the pre-market.
    sim.set_session("PRE_MARKET")
    sim.set_price("NVDA", "94.50")
    filled = sim.inspect_order(resting.id)
    assert filled.status is OrderStatus.FILLED and _d(filled.filled_avg_price) == Decimal("94.5")
    assert filled.filled_at is not None and session_at(filled.filled_at) == "PRE_MARKET"

    # Every other type and time in force is still refused with extended_hours.
    sim.open_position("AMD", 5, "200")
    refused: list[OrderRequest] = [
        LimitOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.BUY,
            limit_price=190.0,
            time_in_force=TimeInForce.IOC,
            extended_hours=True,
        ),
        LimitOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.BUY,
            limit_price=190.0,
            time_in_force=TimeInForce.FOK,
            extended_hours=True,
        ),
        MarketOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC,
            extended_hours=True,
        ),
        StopOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.SELL,
            stop_price=190.0,
            time_in_force=TimeInForce.GTC,
            extended_hours=True,
        ),
        StopLimitOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.SELL,
            stop_price=190.0,
            limit_price=189.0,
            time_in_force=TimeInForce.GTC,
            extended_hours=True,
        ),
        TrailingStopOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.SELL,
            trail_percent=1.0,
            time_in_force=TimeInForce.GTC,
            extended_hours=True,
        ),
    ]
    for request in refused:
        with pytest.raises(APIError) as error:
            sim.submit_order(request)
        assert error.value.status_code == 422, request
        assert "extended hours" in error.value.message
    assert sim.inspect_orders("open") == []


@OFFLINE
def test_observed_cancel_accepted_three_times_and_not_applied_is_replayed() -> None:
    # A month earlier the owner placed a GTC limit buy with extended hours.
    sim = FaultInjectingAlpacaSim(start=datetime(2026, 10, 27, 10, 0, tzinfo=NEW_YORK))
    sim.set_price("NVDA", "100")
    stuck = sim.submit_order(
        _limit("NVDA", OrderSide.BUY, 5, 60.0, tif=TimeInForce.GTC, extended=True, cid="month-old")
    )
    assert stuck.status is OrderStatus.NEW
    # 2026-11-27 closed early at 13:00. Seconds later: a day limit buy without extended hours,
    # which is only queued.
    sim.set_time(datetime(2026, 11, 27, 13, 0, 5, tzinfo=NEW_YORK))
    assert sim.market_session == "AFTER_HOURS"
    queued = sim.submit_order(_limit("NVDA", OrderSide.BUY, 1, 61.0, cid="after-the-close"))
    assert queued.status is OrderStatus.ACCEPTED
    assert sim.inspect_order(stuck.id).status is OrderStatus.NEW

    # 14:12, an after-hours session open: three cancel requests over three minutes. Each is
    # accepted without error; the order keeps reading "new" and only updated_at moves.
    sim.set_time(OBSERVED_AT)
    sim.defer_cancels(stuck.id)
    sim.reset_call_log()
    stamps: list[datetime] = []
    for minute in range(3):
        sim.advance(60 if minute else 0)
        sim.cancel_order_by_id(stuck.id)
        seen = sim.get_order_by_id(stuck.id)
        assert seen.status is OrderStatus.NEW and seen.canceled_at is None
        assert seen.updated_at == sim.now
        stamps.append(seen.updated_at)
    assert stamps == [OBSERVED_AT + timedelta(minutes=minute) for minute in range(3)]
    assert [call.outcome for call in sim.calls("cancel_order_by_id")] == ["ok", "ok", "ok"]
    # In the same minute the queued day order, not yet routed, is cancelled at once.
    sim.cancel_order_by_id(queued.id)
    assert sim.get_order_by_id(queued.id).status is OrderStatus.CANCELED

    # More than an hour later the month-old order still reads "new" and is still listed.
    sim.advance(65 * 60)
    assert sim.market_session == "AFTER_HOURS"
    late = sim.get_order_by_id(stuck.id)
    assert late.status is OrderStatus.NEW and late.updated_at == stamps[-1]
    assert [item.id for item in sim.get_orders(GetOrdersRequest(limit=500))] == [stuck.id]
    # Whenever the venue does act, the order is cancelled without a further request.
    cancels = len(sim.calls("cancel_order_by_id"))
    sim.complete_cancel(stuck.id)
    done = sim.get_order_by_id(stuck.id)
    assert done.status is OrderStatus.CANCELED and done.canceled_at == sim.now
    assert len(sim.calls("cancel_order_by_id")) == cancels == 4


@OFFLINE
def test_deferred_cancel_keeps_the_shares_held_until_the_cancel_is_completed() -> None:
    sim = FaultInjectingAlpacaSim(session="AFTER_HOURS")
    sim.open_position("NVDA", 10, "100")
    trail = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    exit_order = _limit("NVDA", OrderSide.SELL, 10.0, 99.50, extended=True, cid="collared-exit")
    # Every order is covered: the test need not know which order the lane will cancel.
    sim.defer_cancels()

    # The lane frees the shares for an off-hours exit and gets a clean return ...
    sim.cancel_order_by_id(trail.id)
    assert sim.call_log[-1].name == "cancel_order_by_id" and sim.call_log[-1].outcome == "ok"
    # ... but the exit sent at once is refused: nothing was freed.
    with pytest.raises(APIError) as blocked:
        sim.submit_order(exit_order)
    assert blocked.value.status_code == 403
    assert _refused(blocked) == {
        "code": 40310000,
        "message": "insufficient qty available for order (requested: 10, available: 0)",
        "available": "0",
        "existing_qty": "10",
        "held_for_orders": "10",
        "symbol": "NVDA",
    }
    assert sim.call_log[-1].refusal == "insufficient_qty"
    held = sim.get_open_position("NVDA")
    assert _d(held.qty) == 10 and _d(held.qty_available) == 0
    assert sim.inspect_order(trail.id).status is OrderStatus.NEW
    assert [item.id for item in sim.get_orders()] == [trail.id]

    # Asking again changes nothing, however often: the position has no working exit off-hours.
    for _ in range(3):
        sim.advance(60)
        sim.cancel_order_by_id(trail.id)
        with pytest.raises(APIError) as still_blocked:
            sim.submit_order(exit_order)
        assert still_blocked.value.status_code == 403
        assert sim.inspect_order(trail.id).updated_at == sim.now
    assert sim.inspect_order(trail.id).status is OrderStatus.NEW

    # Later the cancel is applied: the shares are free and the same exit goes out and fills.
    sim.complete_cancel(trail.id)
    gone = sim.inspect_order(trail.id)
    assert gone.status is OrderStatus.CANCELED and gone.canceled_at == sim.now
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    sent = sim.submit_order(exit_order)
    assert sim.inspect_order(sent.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None


@OFFLINE
def test_defer_cancels_per_order_pending_cancel_bulk_routes_and_completion() -> None:
    sim = FaultInjectingAlpacaSim(replace_pending="never")
    sim.open_position("NVDA", 10, "100")
    sim.open_position("AMD", 5, "200")
    nvda = sim.submit_order(_stop("NVDA", OrderSide.SELL, 10.0, 90.0))
    amd = sim.submit_order(_stop("AMD", OrderSide.SELL, 5.0, 180.0))

    # One order only, and shown as pending_cancel on request.
    sim.defer_cancels(nvda.id, show_pending_cancel=True)
    sim.cancel_order_by_id(nvda.id)
    sim.cancel_order_by_id(amd.id)  # not covered by the script: cancelled at once
    assert sim.inspect_order(amd.id).status is OrderStatus.CANCELED
    waiting = sim.get_order_by_id(nvda.id)
    assert waiting.status is OrderStatus.PENDING_CANCEL and waiting.canceled_at is None
    # It is still an open order that holds its shares ...
    assert [item.id for item in sim.get_orders(GetOrdersRequest(limit=500))] == [nvda.id]
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    # ... a repeat is accepted the same way, and a replace is refused (documented).
    sim.advance(1)
    sim.cancel_order_by_id(nvda.id)
    again = sim.inspect_order(nvda.id)
    assert again.status is OrderStatus.PENDING_CANCEL and again.updated_at == sim.now
    with pytest.raises(APIError) as no_replace:
        sim.replace_order_by_id(nvda.id, ReplaceOrderRequest(stop_price=91.0))
    assert no_replace.value.status_code == 422 and "pending_cancel" in no_replace.value.message
    # It is still at the exchange: a print through its stop fills it, cancel request or not.
    sim.set_price("NVDA", "89")
    assert sim.inspect_order(nvda.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None
    sim.complete_cancel()  # nothing is left to cancel: a filled order stays filled
    assert sim.inspect_order(nvda.id).status is OrderStatus.FILLED
    with pytest.raises(ValueError, match="no accepted cancel request"):
        sim.complete_cancel(amd.id)

    # Every order, status unchanged (the default, as observed). Bulk cancels obey the script.
    sim.clear_faults()
    sim.open_position("TSLA", 5, "300")
    tsla = sim.submit_order(_stop("TSLA", OrderSide.SELL, 5.0, 280.0))
    amd_again = sim.submit_order(_stop("AMD", OrderSide.SELL, 5.0, 180.0))
    sim.defer_cancels()
    assert {item.id: item.status for item in sim.cancel_orders()} == {
        tsla.id: 200,
        amd_again.id: 200,
    }
    assert sim.inspect_order(tsla.id).status is OrderStatus.NEW
    assert sim.inspect_order(amd_again.id).status is OrderStatus.NEW
    # The flatten path (cancel everything, close everything) meets the held shares.
    closed = sim.close_all_positions(cancel_orders=True)
    assert sorted((item.symbol, item.status) for item in closed) == [("AMD", 403), ("TSLA", 403)]
    assert len(sim.get_all_positions()) == 2
    # An order with an unapplied cancel request cannot be replaced, whatever its status reads.
    with pytest.raises(APIError) as no_replace:
        sim.replace_order_by_id(tsla.id, ReplaceOrderRequest(stop_price=281.0))
    assert no_replace.value.status_code == 422
    assert "cancel request is pending" in no_replace.value.message

    # clear_faults stops deferring further requests; an accepted request stays unapplied.
    sim.clear_faults()
    sim.open_position("META", 5, "500")
    meta = sim.submit_order(_stop("META", OrderSide.SELL, 5.0, 480.0))
    sim.cancel_order_by_id(meta.id)
    assert sim.inspect_order(meta.id).status is OrderStatus.CANCELED
    sim.cancel_order_by_id(tsla.id)
    assert sim.inspect_order(tsla.id).status is OrderStatus.NEW
    # A cancel from outside (the venue, the owner) ends the wait for one order,
    # complete_cancel() without an id for all the others.
    sim.external_cancel(tsla.id)
    assert sim.inspect_order(tsla.id).status is OrderStatus.CANCELED
    sim.complete_cancel()
    assert sim.inspect_order(amd_again.id).status is OrderStatus.CANCELED
    assert sim.get_orders() == []
    flat = sim.close_all_positions(cancel_orders=True)
    assert {item.status for item in flat} == {200} and sim.get_all_positions() == []


# ------------------------------------------------------------------ the owner's hand


@OFFLINE
def test_hand_sale_cannot_take_shares_held_by_a_resting_order() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    trail = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    orders = len(sim.inspect_orders("all"))

    # The owner's sale from another terminal meets the same refusal as a sale over the API.
    with pytest.raises(APIError) as over_the_api:
        sim.submit_order(_market("NVDA", OrderSide.SELL, 4.0))
    logged = len(sim.call_log)
    with pytest.raises(APIError) as by_hand:
        sim.open_position("NVDA", -4, "100")
    assert by_hand.value.status_code == 403
    assert _refused(by_hand) == _refused(over_the_api)
    assert _refused(by_hand)["held_for_orders"] == "10"
    for too_many in (-10, -15):  # the whole position, and through it into a short
        with pytest.raises(APIError) as refused:
            sim.open_position("NVDA", too_many, "100")
        assert refused.value.status_code == 403 and _refused(refused)["available"] == "0"
    # A refused hand sale at another price leaves the symbol's price alone ...
    with pytest.raises(APIError):
        sim.open_position("NVDA", -4, "150")
    assert sim.price("NVDA") == 100
    # ... and gives no price to a symbol that has none (a position filled by script only).
    resting_bid = sim.submit_order(_limit("FRESH", OrderSide.BUY, 10, 50.0, tif=TimeInForce.GTC))
    sim.fill(resting_bid.id, "50")
    sim.submit_order(_stop("FRESH", OrderSide.SELL, 10.0, 45.0))
    with pytest.raises(APIError) as unpriced:
        sim.open_position("FRESH", -4, "55")
    assert unpriced.value.status_code == 403 and sim.price("FRESH") is None
    # The owner's attempts changed nothing and are not harness calls: the log and the order
    # list grew by the two FRESH orders alone.
    assert len(sim.call_log) == logged + 2 and len(sim.inspect_orders("all")) == orders + 2
    position = sim.get_open_position("NVDA")
    assert _d(position.qty) == 10 and _d(position.qty_available) == 0
    assert _mark(sim, trail) == (100, 99, OrderStatus.NEW)

    # What the broker forces on the owner: cancel the order that holds the shares, then sell.
    sim.external_cancel(trail.id)
    sold = sim.open_position("NVDA", -4, "100")
    assert sold.status is OrderStatus.FILLED and sold.side is OrderSide.SELL
    assert sold.client_order_id.startswith("owner-")
    left = sim.get_open_position("NVDA")
    assert _d(left.qty) == 6 and _d(left.qty_available) == 6  # never negative
    # The lane then covers what is left, at the reduced size.
    again = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 6, percent=1.0))
    assert _d(sim.get_open_position("NVDA").qty_available) == 0

    # With part of a position free, a hand sale may take exactly the free shares.
    sim.open_position("NVDA", 4, "100")
    assert _d(sim.get_open_position("NVDA").qty_available) == 4
    with pytest.raises(APIError) as one_too_many:
        sim.open_position("NVDA", -5, "100")
    assert "(requested: 5, available: 4)" in one_too_many.value.message
    sim.open_position("NVDA", -4, "100")
    exact = sim.get_open_position("NVDA")
    assert _d(exact.qty) == 6 and _d(exact.qty_available) == 0
    # So a hand sale cannot take shares out from under a resting sell, and the trigger ends
    # flat, not short. (A replacement that grows while its shares are taken is closed by
    # test_pending_replacement_whose_shares_were_taken_is_rejected_...; one path is still
    # open and is pinned at the end of this test.)
    sim.set_price("NVDA", "98")
    assert sim.inspect_order(again.id).status is OrderStatus.FILLED
    assert sim.inspect_position("NVDA") is None

    # The mirror for a short: a hand cover may not take what a resting buy holds.
    sim.open_position("TSLA", -5, "300")
    cover = sim.submit_order(_stop("TSLA", OrderSide.BUY, 5.0, 306.0))
    with pytest.raises(APIError) as hand_cover:
        sim.open_position("TSLA", 2, "300")
    assert hand_cover.value.status_code == 403
    assert _refused(hand_cover)["held_for_orders"] == "5"
    sim.external_cancel(cover.id)
    sim.open_position("TSLA", 2, "300")
    assert _d(sim.get_open_position("TSLA").qty) == -3
    # With nothing held, one call may take a position through zero (two owner orders).
    sim.open_position("TSLA", 8, "300")
    assert _d(sim.get_open_position("TSLA").qty) == 5

    # An add needs no free shares at all: the owner's own resting bid for ten more does not
    # stand in the way of buying five by hand.
    sim.open_position("AMD", 10, "200")
    sim.submit_order(_limit("AMD", OrderSide.BUY, 10, 190.0, tif=TimeInForce.GTC))
    sim.open_position("AMD", 5, "200")
    assert _d(sim.get_open_position("AMD").qty) == 15

    # Known gap, not modelled (module docstring, *The owner's hand*): a hand trade is checked
    # against held shares only, not against the self-trade table. With no position and an OTO
    # entry resting, a hand short is accepted where the broker's table would refuse it, and
    # the entry's leg then arms for more shares than the position holds. Pinned so that the
    # docstring cannot drift from the code; lane tests must not script this sequence.
    gap = FaultInjectingAlpacaSim()
    gap.set_price("META", "500")
    gap.submit_order(_limit("META", OrderSide.BUY, 10, 499.0, tif=TimeInForce.GTC, stop_loss=480.0))
    gap.open_position("META", -4, "500")
    gap.set_price("META", "499")  # the entry fills: long 6, and the leg arms for all 10
    over_sized = gap.get_open_position("META")
    assert _d(over_sized.qty) == 6 and _d(over_sized.qty_available) == -4


@OFFLINE
def test_trailing_sell_with_no_position_is_an_opening_order_that_shorts_at_its_trigger() -> None:
    # Assumed: the page describes trailing stops for exits only. With no position the order is
    # a short sale like any other sell, so a lane that places its stop after the owner closed
    # the position by hand is not refused: it ends up short at the trigger.
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100")
    naked = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))
    assert naked.status is OrderStatus.NEW
    assert naked.position_intent is PositionIntent.SELL_TO_OPEN
    assert _mark(sim, naked) == (100, 98, OrderStatus.NEW)
    sim.set_price("NVDA", "97")
    assert sim.inspect_order(naked.id).status is OrderStatus.FILLED
    opened = sim.get_open_position("NVDA")
    assert opened.side is PositionSide.SHORT and _d(opened.qty) == -10
    # The mirror: a trailing buy with no short opens a long at its trigger.
    sim.set_price("TSLA", "300")
    entry = sim.submit_order(_trailing("TSLA", OrderSide.BUY, 2, price=5.0))
    assert entry.position_intent is PositionIntent.BUY_TO_OPEN
    sim.set_price("TSLA", "305")
    assert _d(sim.get_open_position("TSLA").qty) == 2

    # The ordinary short-sale rules decide whether such an order is taken at all.
    sim.set_asset("HTB", easy_to_borrow=False)
    with pytest.raises(APIError) as hard:
        sim.submit_order(_trailing("HTB", OrderSide.SELL, 5, percent=2.0))
    assert hard.value.status_code == 422 and "cannot be sold short" in hard.value.message
    sim.set_account(shorting_enabled=False)
    with pytest.raises(APIError) as no_shorting:
        sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=2.0))
    assert no_shorting.value.status_code == 403
    assert "not allowed to short" in no_shorting.value.message
    # With a long position the same order is an exit again.
    sim.open_position("AMD", 5, "200")
    exit_order = sim.submit_order(_trailing("AMD", OrderSide.SELL, 5, percent=2.0))
    assert exit_order.position_intent is PositionIntent.SELL_TO_CLOSE


@OFFLINE
def test_position_add_leaves_new_shares_uncovered_until_a_second_trailing_stop() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    first = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=2.0))

    sim.open_position("NVDA", 5, "100")  # the owner adds by hand: adding needs no free shares
    grown = sim.get_open_position("NVDA")
    assert _d(grown.qty) == 15 and _d(grown.qty_available) == 5
    assert _mark(sim, first) == (100, 98, OrderStatus.NEW)
    assert _d(sim.inspect_order(first.id).qty) == 10
    # Resizing by replace is not modelled; a second trailing stop covers the added shares.
    with pytest.raises(NotImplementedError, match="qty change on a trailing stop"):
        sim.replace_order_by_id(first.id, ReplaceOrderRequest(qty=15))
    second = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 5, percent=2.0))
    assert second.status is OrderStatus.NEW
    assert _d(sim.get_open_position("NVDA").qty_available) == 0
    with pytest.raises(APIError) as third:
        sim.submit_order(_trailing("NVDA", OrderSide.SELL, 1, percent=2.0))
    assert third.value.status_code == 403 and sim.call_log[-1].refusal == "insufficient_qty"

    sim.set_price("NVDA", "97")  # through both stops
    for order in (first, second):
        done = sim.inspect_order(order.id)
        assert done.status is OrderStatus.FILLED and _d(done.filled_avg_price) == 97
    assert sim.inspect_position("NVDA") is None


@OFFLINE
def test_externally_cancelled_trailing_stop_frees_shares_and_is_not_logged() -> None:
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100")
    lost = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    sim.set_price("NVDA", "110")
    sim.set_price("NVDA", "109.5")
    assert _mark(sim, lost) == (110, Decimal("108.9"), OrderStatus.NEW)
    logged = len(sim.call_log)

    sim.external_cancel(lost.id)
    assert len(sim.call_log) == logged  # not a harness call
    gone = sim.inspect_order(lost.id)
    assert gone.status is OrderStatus.CANCELED and gone.canceled_at is not None
    assert sim.inspect_orders("open") == []
    assert _d(sim.get_open_position("NVDA").qty_available) == 10
    # A cancelled order is history: a later high moves neither its mark nor its stop.
    sim.set_price("NVDA", "112")
    assert _mark(sim, lost) == (110, Decimal("108.9"), OrderStatus.CANCELED)
    assert sim.inspect_order(lost.id).updated_at == gone.updated_at
    sim.set_price("NVDA", "109.5")
    with pytest.raises(APIError) as too_late:
        sim.cancel_order_by_id(lost.id)
    assert too_late.value.status_code == 422
    with pytest.raises(APIError) as no_replace:
        sim.replace_order_by_id(lost.id, ReplaceOrderRequest(trail=0.5))
    assert no_replace.value.status_code == 422

    # A new trailing order starts its mark at the standing price, so its stop (108.405, the
    # nearest cent 108.41) is lower than the one that was lost: the lane has to restore the
    # level itself.
    again = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 10, percent=1.0))
    assert _mark(sim, again) == (Decimal("109.5"), Decimal("108.41"), OrderStatus.NEW)
    assert _d(again.stop_price) < Decimal("108.9")


@OFFLINE
@pytest.mark.parametrize("model", WASH_TRADE_MODELS)
@pytest.mark.parametrize("session", OFF_HOURS)
def test_extended_hours_limit_exit_is_refused_while_the_trailing_stop_holds_the_shares(
    session: SimSession, model: WashTradeModel
) -> None:
    sim = FaultInjectingAlpacaSim(session=session, wash_trade_model=model)
    sim.set_asset("NVDA", attributes=["overnight_tradable"])
    sim.open_position("NVDA", 10, "100")
    trail = sim.submit_order(_trailing("NVDA", OrderSide.SELL, 6, percent=1.0))

    def collared_exit(qty: float, cid: str) -> LimitOrderRequest:
        return _limit("NVDA", OrderSide.SELL, qty, 99.50, extended=True, cid=cid)

    # Both orders are sells, so the self-trade model plays no part: the shares are simply held.
    with pytest.raises(APIError) as blocked:
        sim.submit_order(collared_exit(10.0, "exit-all"))
    assert blocked.value.status_code == 403 and sim.call_log[-1].refusal == "insufficient_qty"
    assert (_refused(blocked)["held_for_orders"], _refused(blocked)["available"]) == ("6", "4")
    # The free shares can go at once ...
    free = sim.submit_order(collared_exit(4.0, "exit-free"))
    # ... and the held ones after the cancel has taken effect.
    sim.cancel_order_by_id(trail.id)
    rest = sim.submit_order(collared_exit(6.0, "exit-rest"))
    for order in (free, rest):
        seen = sim.inspect_order(order.id)
        if session == "CLOSED":
            assert seen.status is OrderStatus.ACCEPTED  # no session to execute in yet
        else:
            assert seen.status is OrderStatus.FILLED and _d(seen.filled_avg_price) == 100
    assert (sim.inspect_position("NVDA") is None) is (session != "CLOSED")


@OFFLINE
@pytest.mark.parametrize("model", WASH_TRADE_MODELS)
@pytest.mark.parametrize("session", OFF_HOURS)
def test_trailing_sell_and_resting_gtc_extended_hours_buy_limit_coexist_off_hours(
    session: SimSession, model: WashTradeModel
) -> None:
    # The shape on the account: the owner's GTC extended-hours buy limit and the lane's native
    # trailing sell, in either order of events.
    def book() -> FaultInjectingAlpacaSim:
        sim = FaultInjectingAlpacaSim(
            session=session, wash_trade_model=model, refusal_detail="forum"
        )
        sim.open_position("NVDA", 10, "100")
        return sim

    owner_buy = _limit("NVDA", OrderSide.BUY, 2, 95.0, tif=TimeInForce.GTC, extended=True)
    lane_sell = _trailing("NVDA", OrderSide.SELL, 10, percent=1.0)
    queued = OrderStatus.NEW if session in {"PRE_MARKET", "AFTER_HOURS"} else OrderStatus.ACCEPTED

    sim = book()
    assert sim.submit_order(owner_buy).status is queued
    assert sim.submit_order(lane_sell).status is OrderStatus.NEW
    sim = book()
    assert sim.submit_order(lane_sell).status is OrderStatus.NEW
    assert sim.submit_order(owner_buy).status is queued
    assert len(sim.inspect_orders("open")) == 2

    # A fixed stop in the trailing stop's place is refused by the published table; only the
    # assumed model's lenient cell lets it through.
    sim = book()
    buy = sim.submit_order(owner_buy)
    fixed = _stop("NVDA", OrderSide.SELL, 10.0, 99.0)
    if model == "documented":
        _assert_wash_rejected(sim, fixed, buy, _LIMIT_REASON)
    else:
        assert sim.submit_order(fixed).status is OrderStatus.ACCEPTED


# ---------------------------------------------------------------------------- switches


@OFFLINE
def test_every_unsettled_behaviour_is_a_switch_with_a_pinned_default() -> None:
    defaults: dict[str, Any] = {
        "wash_trade_protection": True,
        "wash_trade_model": "documented",
        "trailing_wash_exempt": True,
        "complex_leg_wash_exemption": "own_order",
        "refusal_detail": "documented",
        "trailing_stop_rounding": "half_up",
        "trailing_hwm_moves_off_hours": False,
        "trailing_replace_mark": "reseed",
        "replace_pending": "until_time_passes",
        "pending_replace_holds_increase": False,
        "trail_replace_must_tighten": True,
        "aged_cancel_waits_for_venue": True,
        "empty_replace_refused": True,
        "fractional_trailing_stops": False,
        "strict_trail_precision": True,
    }
    sim = FaultInjectingAlpacaSim()
    assert {name: getattr(sim, name) for name in defaults} == defaults
    # The module docstring lists every one of them with this default (and gives its basis).
    listed = (sys.modules[FaultInjectingAlpacaSim.__module__].__doc__ or "").split("Switches\n")[-1]
    for name, value in defaults.items():
        shown = f'"{value}"' if isinstance(value, str) else str(value)
        assert f"``{name}={shown}``" in listed, name
    # Each enumerated switch lists its default first.
    assert [
        LEG_WASH_EXEMPTIONS[0],
        REFUSAL_DETAILS[0],
        TRAILING_STOP_ROUNDINGS[0],
        REPLACE_MARKS[0],
        REPLACE_PENDINGS[0],
    ] == ["own_order", "documented", "half_up", "reseed", "until_time_passes"]
    # Every switch is settable from the constructor and stored under its own name.
    flipped: dict[str, Any] = {
        "wash_trade_protection": False,
        "wash_trade_model": "assumed",
        "trailing_wash_exempt": False,
        "complex_leg_wash_exemption": "any_order",
        "refusal_detail": "forum",
        "trailing_stop_rounding": "one_tick_looser",
        "trailing_hwm_moves_off_hours": True,
        "trailing_replace_mark": "keep",
        "replace_pending": "never",
        "pending_replace_holds_increase": True,
        "trail_replace_must_tighten": False,
        "aged_cancel_waits_for_venue": False,
        "empty_replace_refused": False,
        "fractional_trailing_stops": True,
        "strict_trail_precision": False,
    }
    other = FaultInjectingAlpacaSim(**flipped)
    assert {name: getattr(other, name) for name in flipped} == flipped


# --------------------------------------------------------------------------- property


@OFFLINE
@settings(
    max_examples=75,
    deadline=None,
    derandomize=True,  # the same examples on every run: the gate is repeatable input for input
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@example(  # a sell percent trail tightened after a pullback under the reseed rule, then a night
    sell=True,
    percent=True,
    trail=Decimal("2.00"),
    walk=[50, -30, 20, -10, 40, -20, 10, 5, -5, 15],
    jump=None,
    rounding="half_up",
    mark_mode="reseed",
    retrail=(1, Decimal("1.00")),
    night=3,
)
@example(  # a buy dollar trail, a refused widening that is then allowed, and a gap to the end
    sell=False,
    percent=False,
    trail=Decimal("1.50"),
    walk=[-40, 20, -60, 30, -10, 25, -35, 15, -5, 10],
    jump=Decimal("250.00"),
    rounding="one_tick_looser",
    mark_mode="keep",
    retrail=(2, Decimal("3.00")),
    night=None,
)
@example(  # a sell percent trail tightened with the mark kept, floor rounding, a night, a gap
    sell=True,
    percent=True,
    trail=Decimal("3.00"),
    walk=[120, -50, 80, -40, 60, -30, 20, -10, 30, -20],
    jump=Decimal("50.00"),
    rounding="down",
    mark_mode="keep",
    retrail=(3, Decimal("1.50")),
    night=5,
)
@example(  # a sell dollar trail re-sent unchanged: refused, then allowed and reseeded
    sell=True,
    percent=False,
    trail=Decimal("0.75"),
    walk=[30, 45, -20, 15, -10, 25, -15, 35, -25, 10],
    jump=None,
    rounding="one_tick_looser",
    mark_mode="reseed",
    retrail=(4, Decimal("0.75")),
    night=None,
)
@given(
    sell=st.booleans(),
    percent=st.booleans(),
    trail=st.decimals(min_value=Decimal("0.01"), max_value=Decimal("50"), places=2),
    walk=st.lists(st.integers(min_value=-250, max_value=250), min_size=10, max_size=60),
    jump=st.none() | st.decimals(min_value=Decimal("1"), max_value=Decimal("1000"), places=2),
    rounding=st.sampled_from(TRAILING_STOP_ROUNDINGS),
    mark_mode=st.sampled_from(REPLACE_MARKS),
    retrail=st.none()
    | st.tuples(
        st.integers(min_value=0, max_value=59),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal("50"), places=2),
    ),
    night=st.none() | st.integers(min_value=0, max_value=59),
)
def test_trailing_stop_follows_the_reference_model_over_any_price_path(
    sell: bool,
    percent: bool,
    trail: Decimal,
    walk: list[int],
    jump: Decimal | None,
    rounding: TrailingStopRounding,
    mark_mode: str,
    retrail: tuple[int, Decimal] | None,
    night: int | None,
) -> None:
    """Either side, either kind of trail, every rounding mode and mark rule, over a price path
    with (sometimes) a gap, a replace of the trail and a night off-hours in the middle."""
    side = OrderSide.SELL if sell else OrderSide.BUY

    def stop_for(mark: Decimal, width: Decimal) -> Decimal:
        return _expected_trailing_stop(side, mark, width, percent=percent, rounding=rounding)

    def through(price: Decimal, level: Decimal) -> bool:
        return price <= level if sell else price >= level

    # A path from 100 in moves of up to 2.50, then (sometimes) a jump to any price: a gap.
    path: list[Decimal] = []
    for cents in walk:
        previous = path[-1] if path else Decimal(100)
        path.append(max(Decimal(1), previous + Decimal(cents) / 100))
    if jump is not None:
        path.append(jump)

    switches: Any = {"trailing_stop_rounding": rounding, "trailing_replace_mark": mark_mode}
    sim = FaultInjectingAlpacaSim(**switches)
    sim.open_position("NVDA", 10 if sell else -10, "100")
    order = sim.submit_order(
        _trailing(
            "NVDA",
            side,
            10,
            percent=float(trail) if percent else None,
            price=None if percent else float(trail),
        )
    )
    order_id, width = order.id, trail
    last = hwm = Decimal(100)
    stop = stop_for(hwm, width)
    assert order.status is OrderStatus.NEW and _d(order.stop_price) == stop
    # A stop quantised onto the mark sells at the print the order was placed at.
    filled_at = last if through(last, stop) else None

    def check() -> None:
        seen = sim.inspect_order(order_id)
        assert (_d(seen.hwm), _d(seen.stop_price)) == (hwm, stop)
        if filled_at is None:
            assert seen.status is OrderStatus.NEW
            assert _d(sim.get_open_position("NVDA").qty_available) == 0
        else:
            # Filled at the triggering print itself, with mark and stop where they stood.
            assert seen.status is OrderStatus.FILLED and _d(seen.filled_avg_price) == filled_at
            assert sim.inspect_position("NVDA") is None

    check()
    for index, price in enumerate(path):
        if filled_at is not None:
            break
        sim.set_price("NVDA", price)
        last = price
        if price > hwm if sell else price < hwm:
            hwm = price
            fresh = stop_for(hwm, width)
            stop = max(stop, fresh) if sell else min(stop, fresh)  # a price move never loosens
        if through(price, stop):
            filled_at = price
        check()
        if filled_at is None and retrail is not None and retrail[0] == index:
            request = ReplaceOrderRequest(trail=float(retrail[1]))
            if retrail[1] >= width:
                # By default a trail that does not tighten is refused and changes nothing.
                with pytest.raises(APIError) as loose:
                    sim.replace_order_by_id(order_id, request)
                assert loose.value.status_code == 422
                assert "does not tighten" in loose.value.message
                check()
                sim.trail_replace_must_tighten = False  # the same request, any direction
            acknowledged = sim.replace_order_by_id(order_id, request)
            assert acknowledged.status is OrderStatus.PENDING_NEW
            sim.advance(1)  # time passes: the replacement takes effect
            assert sim.inspect_order(order_id).status is OrderStatus.REPLACED
            order_id, width = acknowledged.id, retrail[1]
            if mark_mode == "reseed":
                hwm = last
            stop = stop_for(hwm, width)  # a replace may loosen: no clamp to the old stop
            if through(last, stop):
                filled_at = last
            check()
        if filled_at is None and night == index:
            sim.set_session("AFTER_HOURS")
            sim.set_price("NVDA", "0.01" if sell else "100000")  # far through, off-hours
            check()
            sim.set_price("NVDA", last)
            sim.set_session("RTH")
            check()

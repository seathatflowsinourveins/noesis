"""Fault-injecting in-memory Alpaca Trading API simulator for harness tests.

``FaultInjectingAlpacaSim`` *is* an ``alpaca.trading.client.TradingClient``: only the HTTP
transport (``RESTClient._request``) is replaced, by an in-memory model of the Alpaca v2 trading
REST API. Everything above the transport is upstream code, so the harness talks to the simulator
exactly as it talks to the broker:

* real request objects are serialised by alpaca-py (``to_request_fields``);
* real id validation runs (a non-UUID order id raises ``ValueError`` before any "request");
* responses are JSON-shaped dicts parsed by alpaca-py into real ``Order`` / ``Position`` /
  ``TradeAccount`` / ``Clock`` / ``Asset`` models, so every read is an immutable *snapshot*
  (nothing updates until it is fetched again, as with the real API);
* errors are real ``alpaca.common.exceptions.APIError`` with ``status_code``, ``code``, ``message``;
* ``_sandbox`` and ``_base_url`` are set by alpaca-py's own constructor from ``paper=``. A paper
  simulator carries ``BaseURL.TRADING_PAPER``, a live one ``BaseURL.TRADING_LIVE``. Like the real
  client there is **no** ``.paper`` attribute, so an integrity check that works on the simulator
  also works on a real client.

No socket is ever opened: ``_request`` never reaches ``requests``, ``_one_request`` raises, and
the inherited ``requests.Session`` has its transport adapters removed.

Import it as ``from tests.alpaca_sim import FaultInjectingAlpacaSim`` (one spelling everywhere,
so there is a single module object) and hand it to the harness wherever a ``TradingClient`` goes.

Time
----
All broker timestamps come from the virtual clock, which starts at ``DEFAULT_START_UTC`` (a
fixed regular-session instant) and only moves through ``advance`` / ``set_session`` /
``set_time``; it never ticks on its own. The default start is deliberately years ahead of the
wall clock: harness code that measures an order's age against ``datetime.now`` then sees a
fresh order (never a stale one to cancel), and fills are never "before" a ledger row written
at wall time. Where the offset itself matters (clock-skew checks, freshness windows), pass
``now_utc=sim.now`` if the harness accepts it, patch the harness clock, or build the simulator
with ``start=datetime.now(UTC)`` and use ``advance`` to create skew.

Sessions come from ``tier0_sota.engine.classify_us_equity_session_et`` applied to the virtual
clock, so order timestamps always agree with the harness's own session classifier. The default
start lies beyond the exchange calendar that classifier loads; there it falls back to plain
weekday bounds, so no market holiday and no early close exists at the default start. A test of
holiday or early-close behaviour passes ``start=`` inside the calendar (2026-11-27 closes at
13:00 New York, its after-hours session ends at 17:00).

Basis of every modelled behaviour
---------------------------------
The live lane is developed against this simulator, so a simulator that is kinder than the broker
hides live failures. Every statement below therefore names its basis:

* ``[Orders]``: Alpaca's "Orders at Alpaca" page (updated 2026-11-24);
* ``[Protection]``: Alpaca's "User Protection" page (updated 2026-11-24);
* ``[API]``: the Trading API reference 2.0.1 (the OpenAPI text of the order and position routes);
* ``[observed]``: seen on the real broker on 2026-11-27;
* ``[assumed]``: the pages are silent and nothing was observed.

Where an assumption could flatter a lane it is a constructor switch, and the default is the
behaviour under which a lane that leans on the assumption **fails** its tests: a false alarm, not
a false pass. *Switches* lists them. Every switch is also a plain attribute of the same name, so
a test may change it mid-run.

Orders and matching
-------------------
* Order types market, limit, stop, stop_limit and trailing_stop; ``time_in_force`` day / gtc /
  ioc / fok; order classes simple and OTO with a stop-loss leg. ``[Orders]``
* Statuses ``new``, ``accepted`` (received, not yet routed: queued for a session in which the
  order may execute), ``held`` (OTO leg whose parent has not completely filled),
  ``partially_filled``, ``filled``, ``canceled``, ``replaced``, ``rejected``, ``expired``,
  ``pending_new`` / ``pending_replace`` (see *Replace*) and ``pending_cancel`` (see *Cancel*).
  The names and meanings are ``[Orders]``; when the transitional ones show is ``[assumed]``.
* The response to a submit or replace is taken *before* matching (nothing filled yet); a
  marketable order is filled by the time of the next read. ``[assumed]``
* Matching uses one price per symbol (``set_price``) and fills at exactly that price: no spread,
  no slippage, so fills are optimistic. ``[assumed]``
* Regular orders execute only in ``RTH``. A limit order with ``extended_hours=True`` and
  ``time_in_force`` day **or gtc** also executes in ``PRE_MARKET`` and ``AFTER_HOURS`` ``[Orders]``
  (the gtc form is also ``[observed]``: the owner's resting order had it), and in ``OVERNIGHT``
  when the asset has the ``overnight_tradable`` attribute ``[assumed]``. Any other type or
  time in force with ``extended_hours`` is refused with 422. ``[Orders]``
* Stop and stop-limit orders trigger only in ``RTH`` and rest otherwise; submitted off-hours
  they read ``accepted`` until a session in which they may execute. Nothing executes when
  ``CLOSED``, and an order for a symbol that has no price yet simply rests. ``[assumed]``
* A sell while long may not exceed ``qty_available`` (position minus the shares held by resting
  sell orders, trailing stops included); the mirror rule holds for a buy while short. ``held``
  legs hold nothing, and a replacement that has not taken effect holds nothing either unless
  ``pending_replace_holds_increase`` is set (see *Replace*). The refusal is 403 with the body
  described under *Refusal bodies*. ``[API]``; that several resting sells share one position,
  each holding its unfilled quantity, is ``[assumed]``.
* A blocked account (``trading_blocked``, ``account_blocked``, ``trade_suspended_by_user`` or a
  status other than ``ACTIVE``) refuses every new order, protective ones included; reads and
  cancels keep working. ``[API]`` for the refusal, ``[assumed]`` for its 403 body.
* ``client_order_id`` is unique for the life of the account, closed orders included (422).
  ``[assumed]``
* ``day`` orders expire when their last session ends (regular: the RTH close; extended hours:
  the end of after-hours); placed after that session they live through the next one. The end
  status reads ``expired``. A parent that is canceled, expired or rejected takes its ``held``
  legs with it, **also after a partial fill** (the filled shares are then unprotected).
  ``[assumed]``: the page says only that day orders are canceled after the closing auction.
* ``gtc`` orders carry ``expires_at``: 16:15 New York on the 90th calendar day after the New
  York date of creation, when the aged-order job sends a cancel request, and "the orders will
  remain in pending_cancel until canceled by the execution venue". ``[Orders]`` How long that
  lasts is the switch ``aged_cancel_waits_for_venue``: set (default), the order reads
  ``pending_cancel``, keeps working and keeps its shares until ``complete_cancel``; unset, the
  request is applied at once. A ``defer_cancels`` script that covers the order decides instead
  of the switch. ``[assumed]`` for the length of the wait, for counting the days from the New
  York date and for a replacement starting a new 90 days.
* Fractional quantities need ``time_in_force`` day ``[Orders]``, a fractionable asset and a
  simple order ``[assumed]``.
* Legs are returned only with ``nested=true`` (and in the submit response). A nested list
  includes a parent when the parent or any of its legs matches the status filter. ``[assumed]``
* Every broker round trip is logged (``call_log``) and classed by HTTP verb: ``GET`` is a read,
  anything else is mutating. Raw ``client.get(...)`` / ``client.post(...)`` calls are logged too,
  so the mutating counter cannot be bypassed. ``SimCall.refusal`` names the rule behind a
  refused order (``"self_trade"``, ``"insufficient_qty"``): the simulator's own knowledge, for
  test assertions only, never part of what the lane receives.

Trailing stops
--------------
* ``type=trailing_stop`` is a single (simple) order, ``time_in_force`` day or gtc, with exactly
  one of ``trail_percent`` / ``trail_price``; anything else is refused with 422, as are
  ``extended_hours``, a ``limit_price`` / ``stop_price`` and a trailing leg of a complex order.
  ``[Orders]``; the 422 texts are the simulator's own wording.
* An accepted trailing stop reads ``new`` in **every** session; only its trigger is gated by the
  session. ``[Orders]`` ("If a trailing stop order is accepted, the order status becomes new")
  and ``[observed]``: seven GTC trailing sells submitted at 14:12 New York with the regular
  session closed and an after-hours session open all read ``new``.
* The order tracks a high-water mark ``hwm``: the highest price since submission for a sell, the
  lowest for a buy; it never moves back, and a price move never loosens the stop. ``[Orders]``
* ``hwm`` starts at the price standing at submission, in any session ``[observed]`` (7 of 7,
  after-hours only; other sessions ``[assumed]``); with no price yet, at the first price the
  order is allowed to track ``[assumed]``.
* The stop is ``hwm * (1 - trail_percent / 100)`` or ``hwm - trail_price`` for a sell and the
  mirror image (``+``) for a buy ``[Orders]``, quantised to the tick: 0.01 at or above 1.00,
  0.0001 below ``[observed]`` (all seven stops were whole cents; the sub-dollar tick is
  ``[assumed]`` from the sub-penny rule for prices). The rounding mode is the switch
  ``trailing_stop_rounding``: no single rule reproduces all seven observed stops (nearest cent
  fits six; 0.17 % on 35.84 read 35.77, one cent below the nearest cent).
* A trail finer than half a tick therefore puts the stop onto the mark, and the order triggers
  at its first eligible print. ``[assumed]``
* It does not trigger outside regular hours. A regular-hours price at or through the stop turns
  it into a market order ``[Orders]``, which the simulator fills at that very print however far
  through the stop (a gap) ``[assumed]``.
* Off-hours prices do not move ``hwm`` unless ``trailing_hwm_moves_off_hours`` is set. The price
  standing when a regular session opens is that session's first print: it can ratchet or
  trigger. ``[assumed]`` either way; neither setting is the safe one, so lane tests run under
  both.
* A ``day`` trailing stop expires with the regular session (placed off-hours: with the next one);
  ``gtc`` rests across sessions and weekends with its mark and its held shares. ``[assumed]``
  for the day order's end, ``[Orders]`` for gtc.
* A trailing sell with no long position (a trailing buy with no short) is an opening order like
  any other: the short-sale and buying-power rules accept or refuse it, it rests as
  ``sell_to_open`` and its trigger opens a short. ``[assumed]``: the page describes the type
  for exits only. This is the unkind side: a lane that places its stop after the owner closed
  the position by hand ends its test short instead of being refused.
* A fractional trailing stop is refused with 422 unless ``fractional_trailing_stops`` is set:
  the fractional-order table of ``[Orders]`` does not list the type. ``[assumed]``
* A trail must be positive (422) ``[assumed]``. 0.16 % is accepted ``[observed]``. With
  ``strict_trail_precision`` (default) a ``trail_percent`` finer than 0.01 and a ``trail_price``
  finer than the price tick are refused with 422: every observed trail had two decimals and a
  finer one was never sent. ``[assumed]``
* With ``auto_fill=False`` the ``hwm`` still tracks; only the trigger is suspended.

Self-trade ("wash trade") protection
------------------------------------
``wash_trade_model=`` picks the table; without it the class attribute
``FaultInjectingAlpacaSim.default_wash_trade_model`` decides (``"documented"``), so one
assignment moves every simulator. ``wash_trade_protection=False`` switches the check off.

* ``"documented"``: the table of ``[Protection]``. For an existing open order and a new
  opposite-side order in the same symbol:

  - existing market or stop order: a new market, limit, stop or stop_limit order is always
    rejected;
  - existing limit or stop_limit order: a new market or stop order is always rejected; a new
    limit or stop_limit order is rejected if the buy limit price >= the sell limit price.

  The answer is HTTP 403 and the rule also applies to paper accounts. ``[Protection]``
* The check runs on every submit and replace ("every time a customer places an order"). An
  "existing open order" is any open order of this simulator (``new``, ``accepted`` for a later
  session, ``partially_filled``, ``held``, ``pending_new``), whatever the session. ``[assumed]``
* Trailing stops are exempt, as the new order and as the existing one, while
  ``trailing_wash_exempt`` is set (default). ``[Protection]``: "These complex orders and trailing
  stop orders are exceptions to our wash trade protection." Neither direction was observed, so
  with the switch off a trailing stop enters the table as a stop order. Under ``"documented"``
  the simulator then produces the refusal in both orders of events; under ``"assumed"`` the
  lenient cell described below applies to it as to any stop, so a new trailing stop against a
  resting opposite-side limit or stop-limit order is still accepted.
* Legs of complex orders: ``complex_leg_wash_exemption="own_order"`` (default) exempts a leg
  only against the orders of its own complex order, so a leg is checked against every outside
  order, at the submit of its complex order, as a resting order (``held`` included) and on its
  replacement. ``"any_order"`` exempts a leg against everything. ``[assumed]``: the page names
  bracket and OCO orders as exceptions and says no more. The entry order of a complex order is
  checked like a simple order under both. Bracket and OCO orders are still not modelled, so
  only OTO stop-loss legs reach this rule through the API.
* ``"assumed"``: the model this simulator had before the table was read. It differs from the
  table in one cell, mirrored for both sides: a new **stop** order against a resting
  opposite-side limit or stop-limit order is accepted. Only ``new`` / ``accepted`` /
  ``partially_filled`` orders count as existing, an armed leg counts like a simple order and a
  leg is not checked at the submit of its complex order. It is kept so a lane test can state
  that it leans on the lenient cell; nothing should.

Refusal bodies
--------------
* Error codes are the HTTP status followed by ``10000``, and several rejections share one code
  (40310000), so the message and the other fields tell them apart. ``[API]``
* Insufficient quantity: 403, code 40310000, message ``insufficient qty available for order
  (requested: N, available: M)``, fields ``available``, ``existing_qty``, ``held_for_orders``,
  ``symbol``. ``[API]`` (the close-position error schema and its example).
* Self-trade: only HTTP 403 is documented ``[Protection]``. With ``refusal_detail="documented"``
  (default) the body is therefore the bare shared code and the message ``forbidden``: a lane
  recognises the case as a 403 that is *not* the documented insufficient-quantity body while an
  opposite-side order rests, exactly what the documentation lets it do on the broker.
  ``refusal_detail="forum"`` serves what forum posts show: the message ``potential wash trade
  detected. use complex orders`` with ``existing_order_id`` and ``reject_reason``, and adds
  ``related_orders`` to the insufficient-quantity body. ``[assumed]``
  ``set_wash_trade_refusal(code=, message=, **fields)`` scripts any other code, message and
  fields for the self-trade refusal, so a lane test can run against a wording nobody has seen.

Replace
-------
* ``PATCH`` returns a new order with a new id; the old order ends as ``replaced``. ``[API]``
* An order in status ``accepted``, ``pending_new``, ``pending_cancel`` or ``pending_replace``
  cannot be replaced (422) ``[API]`` (the "Orders at Alpaca" page is silent on this). A ``held``
  leg cannot either, nor an order with an accepted, unapplied cancel request ``[assumed]``.
* A success return does not mean the order has been replaced; if the old order fills first the
  replacing order is rejected. ``[API]`` The interval is the switch ``replace_pending``:
  ``"until_time_passes"`` (default) keeps the old order working and holding its shares, reading
  ``pending_replace``, and the successor ``pending_new`` holding nothing, until the virtual
  clock next moves or ``complete_replace`` is called; ``"until_completed"`` waits for
  ``complete_replace`` alone; ``"never"`` replaces before the call returns. While it lasts, a
  cancel or replace of either order is refused with 422 (``[Orders]`` and ``[API]`` for the old
  order, ``[assumed]`` for a cancel of the successor), and an old order that fills, expires or
  is cancelled from outside leaves its successor ``rejected``. How long the interval lasts on
  the broker, and what each order reads meanwhile, is ``[assumed]``.
* The check of the request (held quantity, short-sale rules, buying power) runs again when the
  replacement takes effect. A successor that no longer fits is ``rejected`` and the old order
  keeps working at its old size, the outcome ``[API]`` documents for an old order that fills
  first; that it is also the outcome here is ``[assumed]``. What can be taken meanwhile is the
  switch ``pending_replace_holds_increase``. Unset (default), the waiting successor holds
  nothing: another order or the owner's hand can take the free shares a quantity increase was
  checked against, and the successor is then the one that fails, after its acknowledgement.
  Set, the increase holds its extra shares from the request on (``qty_available`` and
  ``held_for_orders`` show them) and the later order is the one refused; buying power is not
  held ahead under either setting. Either way a resting order never exceeds the position.
  ``[assumed]``
* On a trailing stop only ``trail`` can be replaced, while the order awaits its trigger, and
  the kind stays (percent stays percent, price stays price) ``[Orders]``; ``limit_price`` /
  ``stop_price`` / ``trail_percent`` / ``trail_price`` are refused with 422 ``[assumed]``.
* The successor's mark is the switch ``trailing_replace_mark``: ``"reseed"`` (default) starts
  it at the price standing when the replacement takes effect, like a newly submitted order, so
  a tighter trail after a pullback can leave a *lower* stop than before; ``"keep"`` carries the
  old mark over. ``[assumed]``
* A ``trail`` that does not tighten (an identical or a wider one) is refused with 422 while
  ``trail_replace_must_tighten`` is set (default); otherwise it is accepted, the one way a
  stop moves away from the price. ``[assumed]``: the page says the trail can be updated and
  names no direction. Neither value is safe on its own (a lane that swallows the 422 would
  loosen its stop on a broker that accepts), so lane tests run under both.
* A ``PATCH`` that names none of ``qty``, ``time_in_force``, ``limit_price``, ``stop_price``,
  ``trail`` is refused with 422 while ``empty_replace_refused`` is set (default); otherwise it
  creates an unchanged successor. ``[assumed]``. alpaca-py's ``ReplaceOrderRequest`` has no
  ``trail_percent`` / ``trail_price`` field and silently drops them, which is how a lane ends up
  sending an empty body.
* A replacement that comes back ``rejected`` (``reject_next_order``) leaves the original order
  working. ``[assumed]``

Cancel
------
* ``DELETE`` *attempts* to cancel: 204 means accepted, 422 that the order is not cancelable.
  ``[API]``
* By default the cancel is applied before the call returns. ``[observed]``: a day limit buy
  queued seconds after the close read ``canceled`` at once.
* A cancel can also be accepted and **not applied**. ``[observed]``: three requests over three
  minutes for a resting GTC extended-hours limit buy returned without error, and the order
  still read ``new`` more than an hour later with only ``updated_at`` moving. Script it with
  ``defer_cancels``: the request returns success, the order keeps working and keeps its shares,
  reads its old status (or ``pending_cancel`` on request), every repeat behaves the same, and
  ``complete_cancel`` applies it later. Bulk cancels (``DELETE /orders``,
  ``close_all_positions(cancel_orders=True)``) and the aged-order job obey the same script;
  without a script the job's own request waits as ``pending_cancel`` (*Orders and matching*).
* An order that is ``pending_replace`` rejects a cancel ``[Orders]``; in a bulk cancel such an
  order is reported with status 500 ``[API]``.
* Canceling a partially filled OTO parent cancels its held stop-loss leg. ``[assumed]``

The owner's hand
----------------
``open_position`` models the owner trading the account from another terminal. The owner's
orders go through the same broker, so a hand sale may not take shares that resting sell orders
hold: it is refused with the same 403 as an API order (the mirror for a hand cover of a short).
``[API]`` for the rule; that the trading screen is subject to it is ``[assumed]``. The one way
to model "the owner sold by hand" under a resting protective order is therefore the sequence
the broker forces on the owner: ``external_cancel`` of the order that holds the shares, then
``open_position`` with a negative quantity. No helper shrinks a position underneath a resting
order, and a replacement whose shares were taken while it waited is rejected when it would
take effect (*Replace*), so neither of those paths leaves a resting sell larger than the
position. For a hand trade only the held quantity is checked; buying power, short-sale rules
and the self-trade table are not. One over-sized resting order is therefore still reachable:
with no position and an OTO entry resting, a hand trade the other way is accepted (the table
would refuse it on the broker), and the entry's stop-loss leg later arms for more shares than
the position holds. Not modelled; a lane test should not script that sequence.

Switches
--------
Constructor argument, default, and the basis of that default:

* ``wash_trade_protection=True``: ``[Protection]``, the rule exists and covers paper accounts.
  ``False`` switches the whole check off, the kind side.
* ``wash_trade_model="documented"``: the ``[Protection]`` table. Other value: ``"assumed"``.
* ``trailing_wash_exempt=True``: the ``[Protection]`` sentence. ``False`` is the unkind side.
* ``complex_leg_wash_exemption="own_order"``: ``[assumed]``, the narrower reading and the unkind
  side. Other value: ``"any_order"``.
* ``refusal_detail="documented"``: only what ``[API]`` and ``[Protection]`` document, the unkind
  side for a lane that matches on wording. Other value: ``"forum"``.
* ``trailing_stop_rounding="half_up"``: ``[observed]``, the nearest tick fits six of the seven
  observed stops (a half goes up). Other values: ``"down"`` (the lower tick: looser for a sell,
  tighter for a buy) and ``"one_tick_looser"`` (the nearest tick moved one tick away from the
  mark: down for a sell, up for a buy; the one observed stop that the nearest tick misses).
* ``trailing_hwm_moves_off_hours=False``: ``[assumed]``; neither value is the safe one.
* ``trailing_replace_mark="reseed"``: ``[assumed]``, the unkind side. Other value: ``"keep"``.
* ``replace_pending="until_time_passes"``: the interval is ``[API]``, its length ``[assumed]``;
  the unkind side for a lane that reads the acknowledgement as final. Other values:
  ``"until_completed"`` (stricter) and ``"never"``.
* ``pending_replace_holds_increase=False``: ``[assumed]``, the unkind side (the failure comes
  after the acknowledgement, as a rejected successor only a read shows). ``True`` refuses the
  later order at once instead.
* ``trail_replace_must_tighten=True``: ``[assumed]``; a careless lane fails under it, but
  neither value is safe on its own.
* ``aged_cancel_waits_for_venue=True``: ``pending_cancel`` is ``[Orders]``, its length
  ``[assumed]``; the unkind side for a lane that re-covers at once. A cancel that stayed
  unapplied for over an hour was ``[observed]`` in an after-hours session, which 16:15 is.
* ``empty_replace_refused=True``: ``[assumed]``, the unkind side.
* ``fractional_trailing_stops=False``: ``[assumed]``, the unkind side.
* ``strict_trail_precision=True``: ``[assumed]``, the unkind side.

Not modelled (raises ``NotImplementedError`` instead of guessing): notional orders, bracket /
OCO / multi-leg orders, take-profit legs, ``opg`` / ``cls``, a ``qty`` or ``time_in_force``
change on a trailing-stop replace, ``trail`` on an order that is not a trailing stop, a sell
``trail_percent`` of 100 or more, and any REST route not listed in ``_ROUTES``. Not modelled
and silent: the string form of numbers (the simulator prints the shortest form, ``172.2`` for
172.20; compare numerically), the intraday-margin and limit-price sanity protections named on
``[Protection]``, and trade-update streaming.
"""

from __future__ import annotations

import calendar
import json
import re
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal, InvalidOperation
from enum import Enum
from types import SimpleNamespace
from typing import Any, ClassVar, Literal, cast, get_args
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.models import (
    Asset,
    Clock,
    ClosePositionResponse,
    Order,
    Position,
    TradeAccount,
)
from alpaca.trading.requests import (
    CancelOrderResponse,
    ClosePositionRequest,
    GetAssetsRequest,
    GetOrderByIdRequest,
    GetOrdersRequest,
    OrderRequest,
    ReplaceOrderRequest,
)

from tier0_sota import engine

__all__ = [
    "DEFAULT_START_UTC",
    "LEG_WASH_EXEMPTIONS",
    "MUTATING",
    "READ",
    "REFUSAL_DETAILS",
    "REPLACE_MARKS",
    "REPLACE_PENDINGS",
    "SIM_SESSIONS",
    "TRAILING_STOP_ROUNDINGS",
    "WASH_TRADE_MODELS",
    "FaultInjectingAlpacaSim",
    "LegWashExemption",
    "RefusalDetail",
    "ReplaceMark",
    "ReplacePending",
    "SimCall",
    "SimSession",
    "TrailingStopRounding",
    "VirtualClock",
    "WashTradeModel",
    "session_at",
]

SimSession = Literal["PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT", "CLOSED"]
CallKind = Literal["read", "mutating"]
WashTradeModel = Literal["assumed", "documented"]
# Each switch lists its default first; the module docstring gives the basis of every value.
LegWashExemption = Literal["own_order", "any_order"]
RefusalDetail = Literal["documented", "forum"]
TrailingStopRounding = Literal["half_up", "down", "one_tick_looser"]
ReplaceMark = Literal["reseed", "keep"]
ReplacePending = Literal["until_time_passes", "until_completed", "never"]
SIM_SESSIONS: tuple[SimSession, ...] = get_args(SimSession)
WASH_TRADE_MODELS: tuple[WashTradeModel, ...] = get_args(WashTradeModel)
LEG_WASH_EXEMPTIONS: tuple[LegWashExemption, ...] = get_args(LegWashExemption)
REFUSAL_DETAILS: tuple[RefusalDetail, ...] = get_args(RefusalDetail)
TRAILING_STOP_ROUNDINGS: tuple[TrailingStopRounding, ...] = get_args(TrailingStopRounding)
REPLACE_MARKS: tuple[ReplaceMark, ...] = get_args(ReplaceMark)
REPLACE_PENDINGS: tuple[ReplacePending, ...] = get_args(ReplacePending)
READ: CallKind = "read"
MUTATING: CallKind = "mutating"

# Wednesday 2035-11-14 10:00 America/New_York: a plain regular-session instant in an ordinary
# five-session week, far enough ahead that it stays in the future of any wall clock.
DEFAULT_START_UTC = datetime(2035, 11, 14, 15, 0, tzinfo=UTC)

_NY = ZoneInfo("America/New_York")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_MINUTE = timedelta(minutes=1)
_SESSION_SEARCH_LIMIT = timedelta(days=15)
_AVG_PRICE_QUANTUM = Decimal("0.000000001")
_CENT = Decimal("0.01")
_SUB_DOLLAR_TICK = Decimal("0.0001")
# Aged-order policy: a cancel request at 16:15 New York on the 90th day after creation.
_GTC_LIFETIME = timedelta(days=90)
_AGED_CANCEL_ET = time(16, 15)
# Working orders hold shares and can execute. A held leg and a replacement that has not taken
# effect (pending_new) are open without working.
_WORKING = frozenset({"new", "accepted", "partially_filled"})
_PENDING_NEW = "pending_new"
_OPEN = _WORKING | {"held", _PENDING_NEW}
_REPLACEABLE = frozenset({"new", "partially_filled"})
_REPLACE_FIELDS = ("qty", "time_in_force", "limit_price", "stop_price", "trail")
_FORUM_WASH_MESSAGE = "potential wash trade detected. use complex orders"
_SELF_TRADE = "self_trade"
_INSUFFICIENT_QTY = "insufficient_qty"
_ORDER_TYPES = frozenset({"market", "limit", "stop", "stop_limit"})
_LIMIT_TYPES = frozenset({"limit", "stop_limit"})
_STOP_TYPES = frozenset({"stop", "stop_limit"})
_MARKET_LIKE = frozenset({"market", "stop"})
_TRAILING = "trailing_stop"
_TRAILING_TIF = frozenset({"day", "gtc"})
_TRAIL_FIELDS = ("trail_percent", "trail_price")
_HUNDRED = Decimal("100")
_IMMEDIATE_TIF = frozenset({"ioc", "fok"})
_DEFAULT_ORDER_LIMIT = 50
_MAX_ORDER_LIMIT = 500
_DEFAULT_ACTIVITY_PAGE = 100
_MAX_CLIENT_ORDER_ID = 128
_OVERNIGHT_TRADABLE = "overnight_tradable"
_OVERNIGHT_HALTED = "overnight_halted"
_DEFAULT_MESSAGES: Mapping[int, str] = {
    400: "bad request",
    401: "unauthorized",
    403: "forbidden",
    404: "not found",
    422: "unprocessable entity",
    429: "too many requests",
    500: "internal server error",
    502: "bad gateway",
    503: "service unavailable",
    504: "gateway timeout",
}
_ACCOUNT_FLAGS = frozenset(
    {
        "status",
        "trading_blocked",
        "account_blocked",
        "transfers_blocked",
        "trade_suspended_by_user",
        "shorting_enabled",
        "multiplier",
    }
)


def session_at(moment: datetime) -> SimSession:
    """Classify an aware instant with the harness's own US-equity session classifier."""
    if moment.tzinfo is None:
        raise ValueError("session_at requires a timezone-aware datetime")
    ns = calendar.timegm(moment.utctimetuple()) * 1_000_000_000 + moment.microsecond * 1_000
    return engine.classify_us_equity_session_et(ns)


def _dec(value: Any, field_name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise _api_error(422, f"invalid {field_name} {value!r}") from exc
    if not number.is_finite():
        raise _api_error(422, f"invalid {field_name} {value!r}")
    return number


def _fmt(value: Decimal) -> str:
    return "0" if value == _ZERO else format(value.normalize(), "f")


def _fmt_opt(value: Decimal | None) -> str | None:
    return None if value is None else _fmt(value)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _iso_opt(moment: datetime | None) -> str | None:
    return None if moment is None else _iso(moment)


def _text(value: Any) -> str:
    raw = value.value if isinstance(value, Enum) else value
    return "" if raw is None else str(raw).strip().lower()


def _truthy(value: Any) -> bool:
    return value is True or _text(value) in {"true", "1"}


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _parse_instant(value: Any) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    )
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _api_error(status: int, message: str, *, code: int | None = None, **extra: Any) -> APIError:
    """Build the exception alpaca-py raises for an HTTP error response.

    Alpaca's numeric codes are the HTTP status followed by ``10000`` (40310000, 40410000,
    42210000, …) unless a specific code is given (40010001 for a duplicate client order id).
    """
    resolved = code if code is not None else status * 100_000 + 10_000
    body = {"code": resolved, "message": message, **extra}
    text = json.dumps(body)
    http_error = SimpleNamespace(
        response=SimpleNamespace(status_code=status, text=text),
        request=SimpleNamespace(method="SIM", url="sim://alpaca"),
    )
    return APIError(text, http_error)


def _expect[T](value: object, kind: type[T]) -> T:
    if not isinstance(value, kind):
        raise TypeError(f"simulator produced {type(value).__name__}, expected {kind.__name__}")
    return value


def _check_trail(
    trail: Decimal,
    field_name: str,
    side: str,
    *,
    percent: bool,
    strict: bool,
    reference: Decimal | None,
) -> Decimal:
    """A trail must be positive; a sell stop at or below zero is left unmodelled, not guessed.

    ``strict`` (the simulator's ``strict_trail_precision``) also refuses a percent trail finer
    than 0.01, the finest step ever observed, and a dollar trail finer than the tick of the
    symbol's price ``reference`` (a cent when the symbol has no price yet).
    """
    if trail <= _ZERO:
        raise _api_error(422, f"{field_name} must be > 0")
    if percent and side == "sell" and trail >= _HUNDRED:
        raise NotImplementedError(
            "FaultInjectingAlpacaSim does not model a sell trail_percent of 100 or more"
        )
    finest = 2 if percent or reference is None else _decimals(_tick(reference))
    if strict and _decimals(trail) > finest:
        raise _api_error(
            422, f"invalid {field_name} {_fmt(trail)}. more than {finest} decimal places"
        )
    return trail


def _tick(price: Decimal) -> Decimal:
    """Minimum price increment: a cent at or above 1.00, a hundredth of a cent below."""
    return _CENT if price >= _ONE else _SUB_DOLLAR_TICK


def _decimals(value: Decimal) -> int:
    exponent = value.normalize().as_tuple().exponent
    return -exponent if isinstance(exponent, int) and exponent < 0 else 0


def _trail_stop(
    side: str,
    hwm: Decimal,
    trail_percent: Decimal | None,
    trail_price: Decimal | None,
    rounding: TrailingStopRounding,
) -> Decimal:
    """Stop price of a trailing stop: the trail below ``hwm`` for a sell, above it for a buy.

    The exact figure is quantised to the tick the way the broker reports it. ``rounding`` is the
    simulator's ``trailing_stop_rounding`` (module docstring, *Switches*).
    """
    if trail_percent is not None:
        ratio = trail_percent / _HUNDRED
        exact = hwm * (_ONE - ratio) if side == "sell" else hwm * (_ONE + ratio)
    else:
        offset = cast(Decimal, trail_price)
        exact = hwm - offset if side == "sell" else hwm + offset
    tick = _tick(exact)
    if rounding == "down":
        return exact.quantize(tick, rounding=ROUND_FLOOR)
    nearest = exact.quantize(tick, rounding=ROUND_HALF_UP)
    if rounding == "half_up":
        return nearest
    return nearest - tick if side == "sell" else nearest + tick


def _gtc_expiry(created_at: datetime) -> datetime:
    """When the aged-order job asks to cancel a gtc order created at ``created_at``."""
    day = created_at.astimezone(_NY).date() + _GTC_LIFETIME
    return datetime.combine(day, _AGED_CANCEL_ET, tzinfo=_NY).astimezone(UTC)


def _one_of[T](value: T, allowed: tuple[T, ...], name: str) -> T:
    if value not in allowed:
        raise ValueError(f"Unknown {name} {value!r}; expected one of {allowed}")
    return value


@dataclass
class VirtualClock:
    """The simulator's only notion of time; the market session is a function of it."""

    now: datetime = DEFAULT_START_UTC

    def __post_init__(self) -> None:
        if self.now.tzinfo is None:
            raise ValueError("VirtualClock requires a timezone-aware start")
        self.now = self.now.astimezone(UTC)

    @property
    def now_ns(self) -> int:
        seconds = calendar.timegm(self.now.utctimetuple())
        return seconds * 1_000_000_000 + self.now.microsecond * 1_000

    @property
    def session(self) -> SimSession:
        return session_at(self.now)

    def minute_marks(self, target: datetime) -> Iterable[datetime]:
        """Yield every whole minute ``m`` with ``now < m <= target``."""
        mark = self.now.replace(second=0, microsecond=0) + _MINUTE
        while mark <= target:
            yield mark
            mark += _MINUTE

    def next_instant_in(self, session: SimSession) -> datetime:
        """Return ``now`` if already in ``session``, else the first later minute that is."""
        if self.session == session:
            return self.now
        for mark in self.minute_marks(self.now + _SESSION_SEARCH_LIMIT):
            if session_at(mark) == session:
                return mark
        raise RuntimeError(f"No {session} session within {_SESSION_SEARCH_LIMIT} of {self.now}")


@dataclass(frozen=True)
class SimCall:
    """One broker round trip as the transport saw it."""

    seq: int
    name: str
    kind: CallKind
    verb: str
    path: str
    payload: Any
    at: datetime
    outcome: str
    # The rule behind a refused order ("self_trade", "insufficient_qty"; "" otherwise). This is
    # the simulator's own knowledge for test assertions: the lane never receives it.
    refusal: str = ""


@dataclass
class _SimAsset:
    id: UUID
    symbol: str
    tradable: bool = True
    shortable: bool = True
    easy_to_borrow: bool = True
    fractionable: bool = True
    marginable: bool = True
    exchange: str = "NASDAQ"
    status: str = "active"
    attributes: tuple[str, ...] = ()

    @property
    def overnight_tradable(self) -> bool:
        return _OVERNIGHT_TRADABLE in self.attributes and _OVERNIGHT_HALTED not in self.attributes


@dataclass
class _SimPosition:
    symbol: str
    qty: Decimal
    avg_entry: Decimal


@dataclass
class _SimOrder:
    id: UUID
    seq: int
    client_order_id: str
    symbol: str
    side: str
    type: str
    time_in_force: str
    qty: Decimal
    status: str
    created_at: datetime
    updated_at: datetime
    submitted_at: datetime
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    extended_hours: bool = False
    order_class: str = ""
    position_intent: str | None = None
    filled_qty: Decimal = _ZERO
    filled_notional: Decimal = _ZERO
    filled_at: datetime | None = None
    expired_at: datetime | None = None
    canceled_at: datetime | None = None
    failed_at: datetime | None = None
    replaced_at: datetime | None = None
    replaced_by: UUID | None = None
    replaces: UUID | None = None
    parent_id: UUID | None = None
    leg_ids: list[UUID] = field(default_factory=list)
    group_id: UUID | None = None
    group_role: str | None = None
    stop_triggered: bool = False
    trail_percent: Decimal | None = None
    trail_price: Decimal | None = None
    hwm: Decimal | None = None
    expires_at: datetime | None = None
    # An accepted cancel request that has not been applied (``defer_cancels``).
    cancel_requested_at: datetime | None = None
    cancel_shown_pending: bool = False
    # The successor of a replacement that has not taken effect yet (``replace_pending``).
    pending_replace_by: UUID | None = None

    @property
    def remaining(self) -> Decimal:
        return self.qty - self.filled_qty

    @property
    def shown_status(self) -> str:
        """The status a read reports: a working order may be waiting on a replace or a cancel."""
        if self.status in _WORKING:
            if self.pending_replace_by is not None:
                return "pending_replace"
            if self.cancel_requested_at is not None and self.cancel_shown_pending:
                return "pending_cancel"
        return self.status

    @property
    def filled_avg_price(self) -> Decimal | None:
        if self.filled_qty <= _ZERO:
            return None
        return (self.filled_notional / self.filled_qty).quantize(_AVG_PRICE_QUANTUM)


@dataclass
class _ScriptedFault:
    error: int | BaseException
    remaining: int
    applied: bool


@dataclass(frozen=True)
class _WashRefusalScript:
    code: int | None
    message: str | None
    fields: Mapping[str, Any]


_Handler = Callable[..., Any]
_ROUTES: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("POST", re.compile(r"/orders"), "submit_order"),
    ("GET", re.compile(r"/orders"), "get_orders"),
    ("DELETE", re.compile(r"/orders"), "cancel_orders"),
    ("GET", re.compile(r"/orders:by_client_order_id"), "get_order_by_client_id"),
    ("GET", re.compile(r"/orders/(?P<order_id>[^/]+)"), "get_order_by_id"),
    ("PATCH", re.compile(r"/orders/(?P<order_id>[^/]+)"), "replace_order_by_id"),
    ("DELETE", re.compile(r"/orders/(?P<order_id>[^/]+)"), "cancel_order_by_id"),
    ("GET", re.compile(r"/positions"), "get_all_positions"),
    ("DELETE", re.compile(r"/positions"), "close_all_positions"),
    ("GET", re.compile(r"/positions/(?P<symbol>[^/]+)"), "get_open_position"),
    ("DELETE", re.compile(r"/positions/(?P<symbol>[^/]+)"), "close_position"),
    ("GET", re.compile(r"/account"), "get_account"),
    ("GET", re.compile(r"/clock"), "get_clock"),
    ("GET", re.compile(r"/assets"), "get_all_assets"),
    ("GET", re.compile(r"/assets/(?P<symbol>[^/]+)"), "get_asset"),
    ("GET", re.compile(r"/account/activities/FILL"), "get_account_activities"),
)
_UNMODELLED = "unmodelled_route"
_METHOD_NAMES = frozenset(name for _, _, name in _ROUTES)


class FaultInjectingAlpacaSim(TradingClient):
    """An alpaca-py ``TradingClient`` backed by an in-memory, scriptable broker."""

    # Self-trade model of every simulator built without ``wash_trade_model=``: the published
    # table. A scratch run can assign it on the class and edit no file.
    default_wash_trade_model: ClassVar[WashTradeModel] = "documented"

    def __init__(
        self,
        *,
        paper: bool = True,
        start: datetime | None = None,
        session: SimSession | None = None,
        equity: Decimal | str | int = Decimal("100000"),
        auto_fill: bool = True,
        auto_assets: bool = True,
        wash_trade_protection: bool = True,
        wash_trade_model: WashTradeModel | None = None,
        trailing_wash_exempt: bool = True,
        complex_leg_wash_exemption: LegWashExemption = "own_order",
        refusal_detail: RefusalDetail = "documented",
        trailing_stop_rounding: TrailingStopRounding = "half_up",
        trailing_hwm_moves_off_hours: bool = False,
        trailing_replace_mark: ReplaceMark = "reseed",
        replace_pending: ReplacePending = "until_time_passes",
        pending_replace_holds_increase: bool = False,
        trail_replace_must_tighten: bool = True,
        aged_cancel_waits_for_venue: bool = True,
        empty_replace_refused: bool = True,
        fractional_trailing_stops: bool = False,
        strict_trail_precision: bool = True,
    ) -> None:
        """Create a paper (default) or live-flavoured simulator.

        ``start`` positions the virtual clock (default ``DEFAULT_START_UTC``, a regular session);
        ``session`` then moves it forward to the first instant of that session. ``auto_fill``
        off means nothing executes unless ``fill_partial`` / ``fill`` is called. ``auto_assets``
        off makes unknown symbols 404 until ``set_asset`` / ``set_price`` introduces them.

        Every other argument is a switch for a broker behaviour that is not settled. The module
        docstring (*Switches*) gives each one's values, its default and the basis of that
        default; each is stored as an attribute of the same name.
        """
        model = self.default_wash_trade_model if wash_trade_model is None else wash_trade_model
        _one_of(model, WASH_TRADE_MODELS, "wash_trade_model")
        _one_of(complex_leg_wash_exemption, LEG_WASH_EXEMPTIONS, "complex_leg_wash_exemption")
        _one_of(refusal_detail, REFUSAL_DETAILS, "refusal_detail")
        _one_of(trailing_stop_rounding, TRAILING_STOP_ROUNDINGS, "trailing_stop_rounding")
        _one_of(trailing_replace_mark, REPLACE_MARKS, "trailing_replace_mark")
        _one_of(replace_pending, REPLACE_PENDINGS, "replace_pending")
        # Obviously fake credentials with the real prefix convention (PK paper, AK live). They
        # never leave the process: the transport below is in-memory.
        credential = ("PK" if paper else "AK") + "SIM" * 6
        super().__init__(api_key=credential, secret_key=credential[::-1], paper=paper)
        # Belt and braces: the inherited HTTP session can no longer open a connection.
        self._session.adapters.clear()
        self._session.close()

        self.virtual_clock = VirtualClock(start if start is not None else DEFAULT_START_UTC)
        self.auto_fill = auto_fill
        self.auto_assets = auto_assets
        self.wash_trade_protection = wash_trade_protection
        self.wash_trade_model: WashTradeModel = model
        self.trailing_wash_exempt = trailing_wash_exempt
        self.complex_leg_wash_exemption: LegWashExemption = complex_leg_wash_exemption
        self.refusal_detail: RefusalDetail = refusal_detail
        self.trailing_stop_rounding: TrailingStopRounding = trailing_stop_rounding
        self.trailing_hwm_moves_off_hours = trailing_hwm_moves_off_hours
        self.trailing_replace_mark: ReplaceMark = trailing_replace_mark
        self.replace_pending: ReplacePending = replace_pending
        self.pending_replace_holds_increase = pending_replace_holds_increase
        self.trail_replace_must_tighten = trail_replace_must_tighten
        self.aged_cancel_waits_for_venue = aged_cancel_waits_for_venue
        self.empty_replace_refused = empty_replace_refused
        self.fractional_trailing_stops = fractional_trailing_stops
        self.strict_trail_precision = strict_trail_precision
        self.call_log: list[SimCall] = []
        self.mutating_call_count = 0
        self.read_call_count = 0

        self._lock = threading.RLock()
        self._orders: dict[UUID, _SimOrder] = {}
        self._client_order_ids: dict[str, UUID] = {}
        self._positions: dict[str, _SimPosition] = {}
        self._assets: dict[str, _SimAsset] = {}
        self._prices: dict[str, Decimal] = {}
        self._lastday_prices: dict[str, Decimal] = {}
        self._activities: list[dict[str, Any]] = []
        self._faults: dict[str, list[_ScriptedFault]] = {}
        self._reject_next_orders = 0
        # Scripted unapplied cancels: order id (None = every order) -> show ``pending_cancel``.
        self._cancel_deferrals: dict[UUID | None, bool] = {}
        self._wash_refusal_script: _WashRefusalScript | None = None
        self._refusal = ""
        self._order_seq = 0
        self._clock_marks: tuple[datetime, datetime, datetime] | None = None

        self._account_id = uuid4()
        self._account_number = ("PA" if paper else "LV") + "SIM" * 3
        self._account_created_at = self.virtual_clock.now - timedelta(days=30)
        start_equity = Decimal(str(equity))
        self._cash = start_equity
        self._last_equity = start_equity
        self._buying_power_override: Decimal | None = None
        self._account_flags: dict[str, Any] = {
            "status": "ACTIVE",
            "trading_blocked": False,
            "account_blocked": False,
            "transfers_blocked": False,
            "trade_suspended_by_user": False,
            "shorting_enabled": True,
            "multiplier": "2",
        }
        if session is not None:
            self.set_session(session)

    def __repr__(self) -> str:
        mode = "paper" if self._sandbox else "live"
        return (
            f"FaultInjectingAlpacaSim({mode}, {self.market_session}, "
            f"now={self.virtual_clock.now.isoformat()}, orders={len(self._orders)}, "
            f"positions={len(self._positions)})"
        )

    # ------------------------------------------------------------------ transport

    def _one_request(self, method: str, url: str, opts: dict, retry: int) -> dict:
        raise RuntimeError("FaultInjectingAlpacaSim never performs HTTP requests")

    def _request(
        self,
        method: str,
        path: str,
        data: dict | str | None = None,
        base_url: Any = None,
        api_version: str | None = None,
    ) -> Any:
        """Serve one REST call from memory: log it, apply a scripted fault, then answer."""
        del base_url, api_version
        verb = method.upper()
        kind: CallKind = READ if verb == "GET" else MUTATING
        params: dict[str, Any] = dict(data) if isinstance(data, Mapping) else {}
        with self._lock:
            name, path_args = self._route(verb, path)
            if name == _UNMODELLED:
                self._log(name, kind, verb, path, params, "unmodelled")
                raise NotImplementedError(f"FaultInjectingAlpacaSim does not model {verb} {path}")
            fault = self._take_fault(name)
            if fault is not None and not fault.applied:
                self._log(name, kind, verb, path, params, self._fault_label(fault))
                raise self._fault_exception(fault, name)
            handler = cast(_Handler, getattr(self, f"_handle_{name}"))
            self._refusal = ""
            try:
                result = handler(params, **path_args)
            except APIError as exc:
                self._log(name, kind, verb, path, params, f"error:{exc.status_code}", self._refusal)
                raise
            except Exception as exc:
                self._log(name, kind, verb, path, params, f"raised:{type(exc).__name__}")
                raise
            if fault is not None:
                self._log(name, kind, verb, path, params, self._fault_label(fault) + ":applied")
                raise self._fault_exception(fault, name)
            self._log(name, kind, verb, path, params, "ok")
            return result

    @staticmethod
    def _route(verb: str, path: str) -> tuple[str, dict[str, str]]:
        for route_verb, pattern, name in _ROUTES:
            if route_verb == verb and (match := pattern.fullmatch(path)) is not None:
                return name, match.groupdict()
        return _UNMODELLED, {}

    def _log(
        self,
        name: str,
        kind: CallKind,
        verb: str,
        path: str,
        payload: Any,
        outcome: str,
        refusal: str = "",
    ) -> None:
        if kind == MUTATING:
            self.mutating_call_count += 1
        else:
            self.read_call_count += 1
        self.call_log.append(
            SimCall(
                seq=len(self.call_log) + 1,
                name=name,
                kind=kind,
                verb=verb,
                path=path,
                payload=_plain(payload),
                at=self.virtual_clock.now,
                outcome=outcome,
                refusal=refusal,
            )
        )

    def _take_fault(self, name: str) -> _ScriptedFault | None:
        queue = self._faults.get(name)
        if not queue:
            return None
        fault = queue[0]
        fault.remaining -= 1
        if fault.remaining <= 0:
            queue.pop(0)
        return fault

    @staticmethod
    def _fault_label(fault: _ScriptedFault) -> str:
        if isinstance(fault.error, int):
            return f"fault:{fault.error}"
        return f"fault:{type(fault.error).__name__}"

    @staticmethod
    def _fault_exception(fault: _ScriptedFault, name: str) -> BaseException:
        if isinstance(fault.error, int):
            message = _DEFAULT_MESSAGES.get(fault.error, "scripted fault")
            return _api_error(fault.error, f"{message} (scripted fault on {name})")
        return fault.error

    # ------------------------------------------------- typed TradingClient surface

    def submit_order(self, order_data: OrderRequest) -> Order:
        return _expect(super().submit_order(order_data), Order)

    def get_orders(self, filter: GetOrdersRequest | None = None) -> list[Order]:
        return [_expect(item, Order) for item in super().get_orders(filter)]

    def get_order_by_id(
        self, order_id: UUID | str, filter: GetOrderByIdRequest | None = None
    ) -> Order:
        return _expect(super().get_order_by_id(order_id, filter), Order)

    def get_order_by_client_id(self, client_id: str) -> Order:
        return _expect(super().get_order_by_client_id(client_id), Order)

    def replace_order_by_id(
        self, order_id: UUID | str, order_data: ReplaceOrderRequest | None = None
    ) -> Order:
        return _expect(super().replace_order_by_id(order_id, order_data), Order)

    def cancel_orders(self) -> list[CancelOrderResponse]:
        return [_expect(item, CancelOrderResponse) for item in super().cancel_orders()]

    def get_all_positions(self) -> list[Position]:
        return [_expect(item, Position) for item in super().get_all_positions()]

    def get_open_position(self, symbol_or_asset_id: UUID | str) -> Position:
        return _expect(super().get_open_position(symbol_or_asset_id), Position)

    def close_all_positions(self, cancel_orders: bool | None = None) -> list[ClosePositionResponse]:
        closed = super().close_all_positions(cancel_orders)
        return [_expect(item, ClosePositionResponse) for item in closed]

    def close_position(
        self, symbol_or_asset_id: UUID | str, close_options: ClosePositionRequest | None = None
    ) -> Order:
        return _expect(super().close_position(symbol_or_asset_id, close_options), Order)

    def get_all_assets(self, filter: GetAssetsRequest | None = None) -> list[Asset]:
        return [_expect(item, Asset) for item in super().get_all_assets(filter)]

    def get_asset(self, symbol_or_asset_id: UUID | str) -> Asset:
        return _expect(super().get_asset(symbol_or_asset_id), Asset)

    def get_clock(self) -> Clock:
        return _expect(super().get_clock(), Clock)

    def get_account(self) -> TradeAccount:
        return _expect(super().get_account(), TradeAccount)

    # ------------------------------------------------------------ scripting helpers

    @property
    def now(self) -> datetime:
        """Current virtual time (aware, UTC)."""
        return self.virtual_clock.now

    @property
    def market_session(self) -> SimSession:
        return self.virtual_clock.session

    def set_session(self, session: SimSession) -> None:
        """Move the clock forward to the first instant of ``session`` (no-op if already in it)."""
        if session not in SIM_SESSIONS:
            raise ValueError(f"Unknown session {session!r}; expected one of {SIM_SESSIONS}")
        with self._lock:
            self._advance_to(self.virtual_clock.next_instant_in(session))

    def set_time(self, moment: datetime) -> None:
        """Move the clock forward to an explicit aware instant."""
        if moment.tzinfo is None:
            raise ValueError("set_time requires a timezone-aware datetime")
        target = moment.astimezone(UTC)
        with self._lock:
            if target < self.virtual_clock.now:
                raise ValueError("The virtual clock cannot move backwards")
            self._advance_to(target)

    def advance(self, seconds: float) -> None:
        """Let ``seconds`` pass: sessions roll, day orders expire, resting orders may fill."""
        if seconds < 0:
            raise ValueError("The virtual clock cannot move backwards")
        with self._lock:
            self._advance_to(self.virtual_clock.now + timedelta(seconds=seconds))

    def set_price(self, symbol: str, price: Decimal | str | int) -> None:
        """Set the last trade price for ``symbol`` and let resting orders react to it."""
        px = Decimal(str(price))
        if not px.is_finite() or px <= _ZERO:
            raise ValueError(f"price must be a positive finite number, got {price!r}")
        with self._lock:
            sym = self._materialise(symbol)
            self._prices[sym] = px
            self._lastday_prices.setdefault(sym, px)
            self._settle()

    def price(self, symbol: str) -> Decimal | None:
        return self._prices.get(symbol.strip().upper())

    def set_asset(
        self,
        symbol: str,
        *,
        tradable: bool | None = None,
        shortable: bool | None = None,
        easy_to_borrow: bool | None = None,
        fractionable: bool | None = None,
        marginable: bool | None = None,
        exchange: str | None = None,
        status: str | None = None,
        attributes: Iterable[str] | None = None,
    ) -> None:
        """Create or amend an asset; unspecified flags keep their value (default: all True)."""
        with self._lock:
            asset = self._assets[self._materialise(symbol)]
            for name, value in (
                ("tradable", tradable),
                ("shortable", shortable),
                ("easy_to_borrow", easy_to_borrow),
                ("fractionable", fractionable),
                ("marginable", marginable),
                ("exchange", exchange),
                ("status", status),
            ):
                if value is not None:
                    setattr(asset, name, value)
            if attributes is not None:
                asset.attributes = tuple(str(item) for item in attributes)

    def set_account(self, **fields: Any) -> None:
        """Override account fields.

        Flags: ``status``, ``trading_blocked``, ``account_blocked``, ``transfers_blocked``,
        ``trade_suspended_by_user``, ``shorting_enabled``, ``multiplier``. Money: ``equity``
        (re-bases cash), ``cash``, ``last_equity``, ``buying_power`` (a fixed override; ``None``
        restores the derived value).
        """
        with self._lock:
            for name, value in fields.items():
                if name in _ACCOUNT_FLAGS:
                    self._account_flags[name] = value
                elif name == "equity":
                    self._cash = Decimal(str(value)) - self._market_value()
                elif name == "cash":
                    self._cash = Decimal(str(value))
                elif name == "last_equity":
                    self._last_equity = Decimal(str(value))
                elif name == "buying_power":
                    self._buying_power_override = None if value is None else Decimal(str(value))
                else:
                    raise ValueError(f"Unknown account field {name!r}")

    def open_position(
        self, symbol: str, qty: Decimal | str | int, avg_entry: Decimal | str | int
    ) -> Order:
        """Trade a position by hand, outside the harness (negative ``qty`` sells or shorts).

        Models the owner trading from another terminal: a filled market order with an
        ``owner-…`` client order id and a FILL activity appear in the account history, equity is
        unchanged at the entry price, and nothing is written to the call log. The symbol's price
        is set to ``avg_entry`` if it has none yet.

        The owner's orders go through the same broker, so a quantity that reduces the position
        may not take shares that resting orders hold: it raises the broker's own 403
        (insufficient quantity), unlogged, and changes nothing. To model the owner selling
        underneath a protective order, ``external_cancel`` that order first (module docstring,
        *The owner's hand*). No other order check runs (buying power, short-sale rules, the
        self-trade table), and a quantity that takes the position through zero stands for the
        two orders the owner would have to send.
        """
        signed = Decimal(str(qty))
        entry = Decimal(str(avg_entry))
        if not signed.is_finite() or signed == _ZERO:
            raise ValueError(f"qty must be a non-zero finite number, got {qty!r}")
        if not entry.is_finite() or entry <= _ZERO:
            raise ValueError(f"avg_entry must be a positive finite number, got {avg_entry!r}")
        with self._lock:
            sym = self._materialise(symbol)
            side = "buy" if signed > _ZERO else "sell"
            held = self._position_qty(sym)
            if held != _ZERO and (held > _ZERO) != (signed > _ZERO):
                closing = min(abs(signed), abs(held))
                if closing > abs(held) - self._reserved(sym, side):
                    raise self._insufficient_qty(sym, side, abs(signed), held)
            self._prices.setdefault(sym, entry)
            self._lastday_prices.setdefault(sym, entry)
            order = self._new_order(
                symbol=sym,
                side=side,
                order_type="market",
                time_in_force="day",
                qty=abs(signed),
                client_order_id=f"owner-{uuid4().hex}",
                status="new",
            )
            self._fill(order, order.qty, entry)
            self._settle()
            return Order(**self._order_json(order, nested=True))

    def external_cancel(self, order_id: UUID | str) -> None:
        """Cancel an open order from outside the harness (owner or broker); not logged."""
        with self._lock:
            order = self._require_order(order_id)
            if order.status not in _OPEN:
                raise ValueError(f"Order {order.id} is {order.status}; only open orders cancel")
            self._close(order, "canceled")

    def reject_order(self, order_id: UUID | str) -> None:
        """Turn an open order into ``rejected`` (an asynchronous exchange reject); not logged."""
        with self._lock:
            order = self._require_order(order_id)
            if order.status not in _OPEN:
                raise ValueError(f"Order {order.id} is {order.status}; only open orders reject")
            self._close(order, "rejected")

    def reject_next_order(self, times: int = 1) -> None:
        """The next ``times`` orders created by submit or replace come back ``rejected``."""
        if times < 1:
            raise ValueError("times must be >= 1")
        with self._lock:
            self._reject_next_orders += times

    def defer_cancels(
        self, order_id: UUID | str | None = None, *, show_pending_cancel: bool = False
    ) -> None:
        """From now on a cancel request is accepted and **not applied** (observed 2026-11-27).

        ``order_id`` limits the script to one existing order; without it every order is covered,
        also orders submitted later, so a test need not know which order the lane will cancel.
        A deferred request returns success (``cancel_order_by_id`` returns, a bulk cancel reports
        200), is logged ``ok``, and leaves the order working: it keeps its status (``new``, as
        observed) or reads ``pending_cancel`` with ``show_pending_cancel=True``, keeps its shares
        reserved, can still fill, and only its ``updated_at`` moves. The request sticks to the
        order: every repeat is accepted the same way, a replace of it is refused, and nothing but
        ``complete_cancel`` (or a fill, an expiry, ``external_cancel``) ends the wait.
        ``clear_faults`` stops deferring further requests and leaves accepted ones pending.
        """
        with self._lock:
            key = None if order_id is None else self._require_order(order_id).id
            self._cancel_deferrals[key] = show_pending_cancel

    def complete_cancel(self, order_id: UUID | str | None = None) -> None:
        """Apply an accepted, unapplied cancel request now (``None``: every pending one).

        The order becomes ``canceled`` and its shares are released; nothing is logged. Naming an
        order that has no pending cancel request is a scripting error.
        """
        with self._lock:
            if order_id is None:
                waiting = [o for o in self._orders.values() if o.cancel_requested_at is not None]
            else:
                order = self._require_order(order_id)
                if order.cancel_requested_at is None:
                    raise ValueError(f"Order {order.id} has no accepted cancel request to complete")
                waiting = [order]
            for order in waiting:
                if order.status in _OPEN:
                    self._close(order, "canceled")

    def complete_replace(self, order_id: UUID | str | None = None) -> None:
        """Let a replacement that has not taken effect take effect now (``None``: every one).

        ``order_id`` is the order being replaced or its successor. The old order becomes
        ``replaced`` and the successor starts working; nothing is logged. With
        ``replace_pending="until_completed"`` this is the only way a replacement completes.
        """
        with self._lock:
            if order_id is None:
                waiting = [o for o in self._orders.values() if o.pending_replace_by is not None]
            else:
                order = self._require_order(order_id)
                if order.pending_replace_by is None and order.replaces is not None:
                    order = self._orders[order.replaces]
                if order.pending_replace_by is None:
                    raise ValueError(f"Order {order_id} has no replacement waiting to take effect")
                waiting = [order]
            self._complete_replacements(waiting)

    def set_wash_trade_refusal(
        self, *, code: int | None = None, message: str | None = None, **fields: Any
    ) -> None:
        """Script the body of every later self-trade refusal; no argument ends the script.

        Only the HTTP status (403, which stays) is documented for that refusal, so the code, the
        message and any further field are whatever the test wants the lane to cope with. A value
        left out keeps what ``refusal_detail`` serves.
        """
        with self._lock:
            scripted = code is not None or message is not None or bool(fields)
            self._wash_refusal_script = (
                _WashRefusalScript(code, message, dict(fields)) if scripted else None
            )

    def fill_partial(
        self,
        order_id: UUID | str,
        qty: Decimal | str | int,
        price: Decimal | str | int | None = None,
    ) -> None:
        """Execute ``qty`` shares of an open order now, whatever the price or session.

        ``qty`` may be anything up to the unfilled remainder (the full remainder completes the
        order). The price defaults to the order's limit price, else the symbol's last price.
        """
        amount = Decimal(str(qty))
        with self._lock:
            order = self._require_order(order_id)
            if order.status not in _WORKING:
                raise ValueError(f"Order {order.id} is {order.status}; it cannot execute")
            if not amount.is_finite() or amount <= _ZERO or amount > order.remaining:
                raise ValueError(f"qty must be in (0, {_fmt(order.remaining)}], got {qty!r}")
            fill_price = (
                Decimal(str(price))
                if price is not None
                else order.limit_price
                if order.limit_price is not None
                else self._prices.get(order.symbol)
            )
            if fill_price is None or not fill_price.is_finite() or fill_price <= _ZERO:
                raise ValueError(f"No usable fill price for {order.symbol}; pass price=")
            if order.status == "accepted":
                order.status = "new"
            self._fill(order, amount, fill_price)
            self._settle()

    def fill(self, order_id: UUID | str, price: Decimal | str | int | None = None) -> None:
        """Execute the whole unfilled remainder of an open order now."""
        with self._lock:
            self.fill_partial(order_id, self._require_order(order_id).remaining, price)

    def fail_next(
        self,
        method_name: str,
        status_code: int | BaseException | Literal["timeout"],
        times: int = 1,
        *,
        applied: bool = False,
    ) -> None:
        """Make the next ``times`` calls of a client method fail.

        ``method_name`` is a ``TradingClient`` method (``submit_order``, ``replace_order_by_id``,
        ``cancel_order_by_id``, ``get_order_by_id``, …) or ``get_account_activities`` for the raw
        FILL-activity read. ``status_code`` is an HTTP status (raises ``APIError``), the string
        ``"timeout"`` (raises ``TimeoutError``, an ``OSError`` like a real transport timeout) or
        an exception instance to raise as is. The error is what the *caller* sees: a scripted
        429 is one that survived alpaca-py's own retries. With ``applied=True`` the request
        takes effect at the broker and only the response is lost (the ambiguous-outcome case).
        """
        if method_name not in _METHOD_NAMES:
            raise ValueError(
                f"Unknown method {method_name!r}; expected one of {sorted(_METHOD_NAMES)}"
            )
        if times < 1:
            raise ValueError("times must be >= 1")
        error: int | BaseException
        if isinstance(status_code, bool):
            raise ValueError("status_code must be an HTTP status, 'timeout' or an exception")
        if isinstance(status_code, int):
            if not 400 <= status_code <= 599:
                raise ValueError(f"status_code must be an HTTP error status, got {status_code}")
            error = status_code
        elif isinstance(status_code, BaseException):
            error = status_code
        elif status_code == "timeout":
            error = TimeoutError(f"simulated broker timeout on {method_name}")
        else:
            raise ValueError("status_code must be an HTTP status, 'timeout' or an exception")
        with self._lock:
            self._faults.setdefault(method_name, []).append(_ScriptedFault(error, times, applied))

    def clear_faults(self) -> None:
        """Drop every scripted fault, rejection and cancel deferral that has not been used yet."""
        with self._lock:
            self._faults.clear()
            self._reject_next_orders = 0
            self._cancel_deferrals.clear()

    def reset_call_log(self) -> None:
        with self._lock:
            self.call_log.clear()
            self.mutating_call_count = 0
            self.read_call_count = 0

    def calls(self, name: str | None = None, *, kind: CallKind | None = None) -> list[SimCall]:
        """Filter the call log by client method name and/or by ``"read"`` / ``"mutating"``."""
        with self._lock:
            return [
                call
                for call in self.call_log
                if (name is None or call.name == name) and (kind is None or call.kind == kind)
            ]

    def inspect_order(self, order_id: UUID | str) -> Order:
        """Snapshot one order (legs nested) without a broker call, so the log is untouched."""
        with self._lock:
            return Order(**self._order_json(self._require_order(order_id), nested=True))

    def inspect_orders(
        self, status: Literal["open", "closed", "all"] = "open", symbol: str | None = None
    ) -> list[Order]:
        """Snapshot orders in creation order (flat, legs included) without a broker call."""
        sym = None if symbol is None else symbol.strip().upper()
        with self._lock:
            return [
                Order(**self._order_json(order, nested=False))
                for order in self._orders.values()
                if self._status_matches(order, status) and (sym is None or order.symbol == sym)
            ]

    def inspect_position(self, symbol: str) -> Position | None:
        """Snapshot one position without a broker call; ``None`` when flat."""
        with self._lock:
            position = self._positions.get(symbol.strip().upper())
            return None if position is None else Position(**self._position_json(position))

    # --------------------------------------------------------------- clock engine

    def _advance_to(self, target: datetime) -> None:
        current = self.virtual_clock.session
        # Time passing is what ends the default replace interval, at the first instant reached.
        waiting = (
            target > self.virtual_clock.now
            and self.replace_pending == "until_time_passes"
            and any(order.pending_replace_by is not None for order in self._orders.values())
        )
        aged = self._next_aged_cancel()
        for mark in self.virtual_clock.minute_marks(target):
            upcoming = session_at(mark)
            due = aged is not None and mark >= aged
            if upcoming == current and not due and not waiting:
                continue
            self.virtual_clock.now = mark
            if waiting:
                self._complete_replacements()
                waiting = False
            if upcoming != current:
                self._on_session_change(current)
                current = upcoming
            if due:
                self._cancel_aged_orders()
                aged = self._next_aged_cancel()
            self._settle()
        self.virtual_clock.now = target
        if waiting:
            self._complete_replacements()
        self._settle()

    def _next_aged_cancel(self) -> datetime | None:
        """Earliest ``expires_at`` among open gtc orders the aged-order job has not asked for."""
        return min(
            (
                order.expires_at
                for order in self._orders.values()
                if order.expires_at is not None
                and order.status in _OPEN
                and order.cancel_requested_at is None
            ),
            default=None,
        )

    def _cancel_aged_orders(self) -> None:
        """The aged-order job: one cancel request for every open gtc order past ``expires_at``."""
        now = self.virtual_clock.now
        for order in sorted(self._orders.values(), key=lambda item: item.seq):
            if (
                order.expires_at is not None
                and order.expires_at <= now
                and order.status in _OPEN
                and order.cancel_requested_at is None
            ):
                self._request_cancel(order, aged=True)

    def _complete_replacements(self, waiting: Iterable[_SimOrder] | None = None) -> None:
        """Let pending replacements take effect, oldest first, then match the successors."""
        chosen = self._orders.values() if waiting is None else waiting
        pairs = [
            (old, self._orders[old.pending_replace_by])
            for old in sorted(chosen, key=lambda item: item.seq)
            if old.pending_replace_by is not None
        ]
        for old, new in pairs:
            self._apply_replacement(old, new)
        for _, new in pairs:
            self._after_submit(new)

    def _on_session_change(self, ended: SimSession) -> None:
        for order in list(self._orders.values()):
            if order.status not in _WORKING or order.time_in_force != "day":
                continue
            last_session = "AFTER_HOURS" if order.extended_hours else "RTH"
            if ended == last_session:
                self._close(order, "expired")

    def _session_marks(self) -> tuple[datetime, datetime]:
        now = self.virtual_clock.now
        cached = self._clock_marks
        if cached is not None and cached[0] <= now < min(cached[1], cached[2]):
            return cached[1], cached[2]
        next_open: datetime | None = None
        next_close: datetime | None = None
        previous = self.virtual_clock.session
        for mark in self.virtual_clock.minute_marks(now + _SESSION_SEARCH_LIMIT):
            current = session_at(mark)
            if next_open is None and current == "RTH" and previous != "RTH":
                next_open = mark
            if next_close is None and previous == "RTH" and current != "RTH":
                next_close = mark
            if next_open is not None and next_close is not None:
                self._clock_marks = (now, next_open, next_close)
                return next_open, next_close
            previous = current
        raise RuntimeError(f"No regular session within {_SESSION_SEARCH_LIMIT} of {now}")

    # -------------------------------------------------------------- order engine

    def _materialise(self, symbol: str) -> str:
        sym = symbol.strip().upper()
        if not sym:
            raise ValueError("symbol must be a non-empty string")
        if sym not in self._assets:
            self._assets[sym] = _SimAsset(id=uuid4(), symbol=sym)
        return sym

    def _asset_for(self, symbol: str) -> _SimAsset:
        sym = symbol.strip().upper()
        if sym not in self._assets:
            if not self.auto_assets or not sym:
                raise _api_error(404, f"asset not found for {sym}")
            self._materialise(sym)
        return self._assets[sym]

    def _require_order(self, order_id: UUID | str) -> _SimOrder:
        key = order_id if isinstance(order_id, UUID) else UUID(str(order_id))
        order = self._orders.get(key)
        if order is None:
            raise KeyError(f"No simulated order {key}")
        return order

    def _find_order(self, order_id: str) -> _SimOrder:
        try:
            order = self._orders.get(UUID(order_id))
        except ValueError:
            order = None
        if order is None:
            raise _api_error(404, "order not found")
        return order

    def _position_qty(self, symbol: str) -> Decimal:
        position = self._positions.get(symbol)
        return _ZERO if position is None else position.qty

    def _reserved(self, symbol: str, side: str, *, exclude: UUID | None = None) -> Decimal:
        """Shares of ``symbol`` that working ``side`` orders hold, ``exclude`` left out."""
        return sum(
            (
                order.remaining + self._pending_increase(order)
                for order in self._orders.values()
                if order.symbol == symbol
                and order.side == side
                and order.status in _WORKING
                and order.id != exclude
            ),
            _ZERO,
        )

    def _pending_increase(self, order: _SimOrder) -> Decimal:
        """Extra quantity a waiting replacement of ``order`` holds from its request on.

        Zero unless ``pending_replace_holds_increase`` is set and the successor is larger.
        """
        if not self.pending_replace_holds_increase or order.pending_replace_by is None:
            return _ZERO
        return max(_ZERO, self._orders[order.pending_replace_by].qty - order.qty)

    def _market_value(self) -> Decimal:
        return sum(
            (
                pos.qty * self._prices.get(pos.symbol, pos.avg_entry)
                for pos in self._positions.values()
            ),
            _ZERO,
        )

    def _equity(self) -> Decimal:
        return self._cash + self._market_value()

    def _buying_power(self, *, exclude: UUID | None = None) -> Decimal:
        if self._buying_power_override is not None:
            return self._buying_power_override
        gross = sum(
            (
                abs(pos.qty) * self._prices.get(pos.symbol, pos.avg_entry)
                for pos in self._positions.values()
            ),
            _ZERO,
        )
        committed = sum(
            (
                order.remaining * reference
                for order in self._orders.values()
                if order.status in _WORKING
                and order.id != exclude
                and (reference := self._reference_price(order)) is not None
                and self._opens_exposure(order)
            ),
            _ZERO,
        )
        multiplier = Decimal(str(self._account_flags["multiplier"]))
        return max(_ZERO, self._equity() * multiplier - gross - committed)

    def _reference_price(self, order: _SimOrder) -> Decimal | None:
        if order.limit_price is not None:
            return order.limit_price
        if order.stop_price is not None:
            return order.stop_price
        return self._prices.get(order.symbol)

    def _opens_exposure(self, order: _SimOrder) -> bool:
        held = self._position_qty(order.symbol)
        return held >= _ZERO if order.side == "buy" else held <= _ZERO

    def _eligible(self, order: _SimOrder, session: SimSession) -> bool:
        if session == "CLOSED":
            return False
        if session == "RTH":
            return True
        if not order.extended_hours:
            return False
        return session != "OVERNIGHT" or self._assets[order.symbol].overnight_tradable

    def _execution_price(self, order: _SimOrder, last: Decimal) -> Decimal | None:
        if order.type == _TRAILING:
            # At or through its stop a trailing stop is a market order: it fills at this print,
            # however far through the stop that is. Only eligible (regular-hours) orders get
            # here, after _track_trailing_stops has given every one of them a mark and a stop.
            stop = cast(Decimal, order.stop_price)
            triggered = last <= stop if order.side == "sell" else last >= stop
            return last if triggered else None
        if order.type in _STOP_TYPES and not order.stop_triggered:
            stop = cast(Decimal, order.stop_price)
            if not (last <= stop if order.side == "sell" else last >= stop):
                return None
            order.stop_triggered = True
        if order.type in _MARKET_LIKE:
            return last
        limit = cast(Decimal, order.limit_price)
        crossed = last <= limit if order.side == "buy" else last >= limit
        return last if crossed else None

    def _track_trailing_stops(self, session: SimSession) -> None:
        """Move each working trailing stop's ``hwm`` to a better price; it never moves back.

        Regular-hours prices always count. Off-hours prices count only with
        ``trailing_hwm_moves_off_hours`` (an assumption either way; see the module docstring).
        """
        if session != "RTH" and not self.trailing_hwm_moves_off_hours:
            return
        for order in self._orders.values():
            if order.type != _TRAILING or order.status not in _WORKING:
                continue
            last = self._prices.get(order.symbol)
            if last is None:
                continue
            hwm = order.hwm
            if hwm is None or (last > hwm if order.side == "sell" else last < hwm):
                stop = self._trailing_stop_price(order, last)
                held = order.stop_price
                if held is not None:
                    # A price move never loosens the stop ("or stay at the last level"), also
                    # where the tick changes at 1.00 and the rounded figure would step back.
                    stop = max(stop, held) if order.side == "sell" else min(stop, held)
                order.hwm = last
                order.stop_price = stop
                order.updated_at = self.virtual_clock.now

    def _trailing_stop_price(self, order: _SimOrder, hwm: Decimal) -> Decimal:
        return _trail_stop(
            order.side, hwm, order.trail_percent, order.trail_price, self.trailing_stop_rounding
        )

    def _settle(self) -> None:
        """Promote queued orders that became eligible and execute whatever is marketable."""
        session = self.virtual_clock.session
        self._track_trailing_stops(session)
        progressed = True
        while progressed:
            progressed = False
            for order in sorted(self._orders.values(), key=lambda item: item.seq):
                if order.status not in _WORKING or not self._eligible(order, session):
                    continue
                if order.status == "accepted":
                    order.status = "new"
                    order.updated_at = self.virtual_clock.now
                    progressed = True
                last = self._prices.get(order.symbol)
                if not self.auto_fill or last is None:
                    continue
                execution = self._execution_price(order, last)
                if execution is not None:
                    self._fill(order, order.remaining, execution)
                    progressed = True

    def _fill(self, order: _SimOrder, qty: Decimal, price: Decimal) -> None:
        now = self.virtual_clock.now
        held_before = self._position_qty(order.symbol)
        signed = qty if order.side == "buy" else -qty
        held_after = held_before + signed
        position = self._positions.get(order.symbol)
        if held_after == _ZERO:
            self._positions.pop(order.symbol, None)
        elif position is None or held_before == _ZERO:
            self._positions[order.symbol] = _SimPosition(order.symbol, held_after, price)
        elif (held_before > _ZERO) == (signed > _ZERO):
            cost = abs(held_before) * position.avg_entry + qty * price
            position.qty, position.avg_entry = held_after, cost / abs(held_after)
        elif (held_after > _ZERO) == (held_before > _ZERO):
            position.qty = held_after
        else:
            position.qty, position.avg_entry = held_after, price
        self._cash -= signed * price

        order.filled_qty += qty
        order.filled_notional += qty * price
        order.updated_at = now
        complete = order.remaining == _ZERO
        order.status = "filled" if complete else "partially_filled"
        if complete:
            order.filled_at = now
        activity_side = (
            "sell_short" if order.side == "sell" and held_before <= _ZERO else order.side
        )
        stamp = now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond // 1000:03d}"
        self._activities.append(
            {
                "id": f"{stamp}::{uuid4()}",
                "seq": len(self._activities) + 1,
                "activity_type": "FILL",
                "transaction_time": _iso(now),
                "type": "fill" if complete else "partial_fill",
                "price": _fmt(price),
                "qty": _fmt(qty),
                "side": activity_side,
                "symbol": order.symbol,
                "leaves_qty": _fmt(order.remaining),
                "order_id": str(order.id),
                "cum_qty": _fmt(order.filled_qty),
                "order_status": order.status,
            }
        )
        if complete:
            self._drop_replacement(order)
            for leg_id in order.leg_ids:
                leg = self._orders[leg_id]
                if leg.status == "held":
                    leg.status = "accepted"
                    leg.updated_at = now

    def _close(self, order: _SimOrder, status: Literal["canceled", "expired", "rejected"]) -> None:
        now = self.virtual_clock.now
        order.status = status
        order.updated_at = now
        if status == "canceled":
            order.canceled_at = now
        elif status == "expired":
            order.expired_at = now
        else:
            order.failed_at = now
        self._drop_replacement(order)
        for leg_id in order.leg_ids:
            leg = self._orders[leg_id]
            if leg.status == "held":
                leg.status = "canceled"
                leg.canceled_at = now
                leg.updated_at = now

    def _drop_replacement(self, closed: _SimOrder) -> None:
        """Undo a replacement that can no longer take effect because one of its orders closed.

        An order that fills, expires or is cancelled before its replacement takes effect leaves
        the replacing order rejected; a successor that is closed first frees the old order.
        """
        now = self.virtual_clock.now
        if closed.pending_replace_by is not None:
            successor = self._orders[closed.pending_replace_by]
            closed.pending_replace_by = None
            if successor.status == _PENDING_NEW:
                successor.status = "rejected"
                successor.failed_at = now
                successor.updated_at = now
        if closed.replaces is not None:
            old = self._orders[closed.replaces]
            if old.pending_replace_by == closed.id:
                old.pending_replace_by = None
                old.updated_at = now

    def _request_cancel(self, order: _SimOrder, *, aged: bool = False) -> bool:
        """Serve one cancel request for a cancelable order; ``True`` when it was applied.

        A request covered by ``defer_cancels``, and every later request for the same order, is
        accepted and leaves the order as it is apart from ``updated_at``. So is a request of
        the aged-order job (``aged``) that no script covers, while
        ``aged_cancel_waits_for_venue`` is set: the order then reads ``pending_cancel``.
        """
        show_pending = self._cancel_deferrals.get(order.id, self._cancel_deferrals.get(None))
        if show_pending is None and aged and self.aged_cancel_waits_for_venue:
            show_pending = True
        if order.cancel_requested_at is None and show_pending is None:
            self._close(order, "canceled")
            return True
        now = self.virtual_clock.now
        if order.cancel_requested_at is None:
            order.cancel_requested_at = now
            order.cancel_shown_pending = bool(show_pending)
        order.updated_at = now
        return False

    def _apply_replacement(self, old: _SimOrder, new: _SimOrder) -> None:
        """The replacement takes effect: ``old`` ends as ``replaced`` and ``new`` starts working.

        Unless it no longer can: the old order filled past the new quantity, or what the
        successor needs was taken while it waited. The replacing order is then ``rejected`` and
        the old order keeps working (``_drop_replacement``).
        """
        now = self.virtual_clock.now
        if new.qty <= old.filled_qty or not self._replacement_fits(old, new):
            self._close(new, "rejected")
            return
        new.filled_qty = old.filled_qty
        new.filled_notional = old.filled_notional
        new.stop_triggered = old.stop_triggered and new.stop_price == old.stop_price
        if new.type == _TRAILING:
            new.hwm = self._successor_mark(old)
            new.stop_price = None if new.hwm is None else self._trailing_stop_price(new, new.hwm)
        new.leg_ids = list(old.leg_ids)
        for leg_id in new.leg_ids:
            self._orders[leg_id].parent_id = new.id
        old.leg_ids = []
        new.status = old.status
        new.updated_at = now
        old.status = "replaced"
        old.pending_replace_by = None
        old.replaced_by = new.id
        old.replaced_at = now
        old.updated_at = now

    def _replacement_fits(self, old: _SimOrder, new: _SimOrder) -> bool:
        """Whether the exposure check of the replace request still passes now it takes effect."""
        try:
            self._check_exposure(
                symbol=new.symbol,
                side=new.side,
                qty=new.qty - old.filled_qty,
                reference=new.limit_price or new.stop_price or self._prices.get(new.symbol),
                exclude=old.id,
            )
        except APIError:
            return False
        return True

    def _successor_mark(self, old: _SimOrder) -> Decimal | None:
        """High-water mark a trailing stop's successor starts with (``trailing_replace_mark``)."""
        if self.trailing_replace_mark == "keep":
            return old.hwm
        return self._prices.get(old.symbol)

    def _new_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        time_in_force: str,
        qty: Decimal,
        client_order_id: str,
        status: str,
        limit_price: Decimal | None = None,
        stop_price: Decimal | None = None,
        extended_hours: bool = False,
        order_class: str = "",
        parent: _SimOrder | None = None,
        group_role: str | None = None,
    ) -> _SimOrder:
        now = self.virtual_clock.now
        self._order_seq += 1
        held = self._position_qty(symbol)
        if side == "buy":
            intent = "buy_to_close" if held < _ZERO else "buy_to_open"
        else:
            intent = "sell_to_close" if held > _ZERO else "sell_to_open"
        order = _SimOrder(
            id=uuid4(),
            seq=self._order_seq,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            type=order_type,
            time_in_force=time_in_force,
            qty=qty,
            status=status,
            created_at=now,
            updated_at=now,
            submitted_at=now,
            limit_price=limit_price,
            stop_price=stop_price,
            extended_hours=extended_hours,
            order_class=order_class,
            position_intent=None if parent is not None else intent,
            parent_id=None if parent is None else parent.id,
            group_id=None if parent is None else parent.group_id,
            group_role=group_role,
            expires_at=_gtc_expiry(now) if time_in_force == "gtc" else None,
        )
        self._orders[order.id] = order
        self._client_order_ids[client_order_id] = order.id
        if parent is not None:
            parent.leg_ids.append(order.id)
        return order

    def _check_price(self, price: Decimal, field_name: str) -> Decimal:
        if price <= _ZERO:
            raise _api_error(422, f"{field_name} must be > 0")
        exponent = price.normalize().as_tuple().exponent
        decimals = -exponent if isinstance(exponent, int) and exponent < 0 else 0
        if decimals > (2 if price >= _ONE else 4):
            raise _api_error(
                422,
                f"invalid {field_name} {_fmt(price)}. sub-penny increment does not fulfill "
                "minimum pricing criteria",
            )
        return price

    def _check_client_order_id(self, client_order_id: str) -> None:
        if len(client_order_id) > _MAX_CLIENT_ORDER_ID:
            raise _api_error(
                422, f"client_order_id must be no more than {_MAX_CLIENT_ORDER_ID} characters"
            )
        if client_order_id in self._client_order_ids:
            raise _api_error(422, "client_order_id must be unique", code=40_010_001)

    def _check_account_can_trade(self) -> None:
        flags = self._account_flags
        if (
            flags["trading_blocked"]
            or flags["account_blocked"]
            or flags["trade_suspended_by_user"]
            or _text(flags["status"]) != "active"
        ):
            raise _api_error(403, "account is not authorized to trade")

    def _check_exposure(
        self,
        *,
        symbol: str,
        side: str,
        qty: Decimal,
        reference: Decimal | None,
        exclude: UUID | None = None,
    ) -> None:
        """Enforce held-quantity, short-sale and buying-power rules for a new working order."""
        held = self._position_qty(symbol)
        closing = (side == "sell" and held > _ZERO) or (side == "buy" and held < _ZERO)
        if closing:
            if qty > abs(held) - self._reserved(symbol, side, exclude=exclude):
                raise self._insufficient_qty(symbol, side, qty, held, exclude=exclude)
            return
        if side == "sell":
            asset = self._assets[symbol]
            if not (asset.shortable and asset.easy_to_borrow):
                raise _api_error(422, f'asset "{symbol}" cannot be sold short')
            if not self._account_flags["shorting_enabled"]:
                raise _api_error(403, "account is not allowed to short")
            if qty != qty.to_integral_value():
                raise _api_error(422, "fractional orders cannot be sold short")
        if reference is not None:
            cost = qty * reference
            buying_power = self._buying_power(exclude=exclude)
            if cost > buying_power:
                raise _api_error(
                    403,
                    "insufficient buying power",
                    buying_power=_fmt(buying_power),
                    cost_basis=_fmt(cost),
                )

    def _insufficient_qty(
        self, symbol: str, side: str, qty: Decimal, held: Decimal, *, exclude: UUID | None = None
    ) -> APIError:
        """The 403 for an order that needs more shares than resting orders leave available."""
        reserved = self._reserved(symbol, side, exclude=exclude)
        available = abs(held) - reserved
        body: dict[str, Any] = {
            "available": _fmt(available),
            "existing_qty": _fmt(abs(held)),
            "held_for_orders": _fmt(reserved),
        }
        if self.refusal_detail == "forum":
            body["related_orders"] = [
                str(order.id)
                for order in self._orders.values()
                if order.symbol == symbol
                and order.side == side
                and order.status in _WORKING
                and order.id != exclude
            ]
        self._refusal = _INSUFFICIENT_QTY
        return _api_error(
            403,
            f"insufficient qty available for order (requested: {_fmt(qty)}, "
            f"available: {_fmt(available)})",
            **body,
            symbol=symbol,
        )

    def _wash_refusal(self, other: _SimOrder, reason: str) -> APIError:
        """The 403 of the self-trade rule; only that status is documented (*Refusal bodies*)."""
        message = _DEFAULT_MESSAGES[403]
        body: dict[str, Any] = {}
        if self.refusal_detail == "forum":
            message = _FORUM_WASH_MESSAGE
            body = {"existing_order_id": str(other.id), "reject_reason": reason}
        code: int | None = None
        script = self._wash_refusal_script
        if script is not None:
            code = script.code
            message = message if script.message is None else script.message
            body.update(script.fields)
        self._refusal = _SELF_TRADE
        return _api_error(403, message, code=code, **body)

    def _wash_type(self, order_type: str) -> str | None:
        """The type under which an order enters the self-trade table; ``None`` when exempt."""
        if order_type != _TRAILING:
            return order_type
        return None if self.trailing_wash_exempt else "stop"

    def _check_wash_trade(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        limit_price: Decimal | None,
        exclude: UUID | None = None,
        leg: bool = False,
        group_id: UUID | None = None,
    ) -> None:
        """Refuse a new order that could trade against one of the account's own open orders.

        ``wash_trade_model`` picks the table (module docstring, *Self-trade protection*). A
        trailing stop is exempt while ``trailing_wash_exempt`` is set and is a stop order to the
        table otherwise, as the new order and as the existing one. ``leg`` and ``group_id``
        describe the new order's place in a complex order.
        """
        kind = self._wash_type(order_type)
        if not self.wash_trade_protection or kind is None:
            return
        if self.wash_trade_model == "documented":
            self._check_wash_trade_documented(
                symbol=symbol,
                side=side,
                kind=kind,
                limit_price=limit_price,
                exclude=exclude,
                leg=leg,
                group_id=group_id,
            )
            return
        for other in self._orders.values():
            other_kind = self._wash_type(other.type)
            if (
                other.symbol != symbol
                or other.side == side
                or other.status not in _WORKING
                or other.id == exclude
                or other_kind is None
            ):
                continue
            if other_kind in _MARKET_LIKE:
                reason = "opposite side market/stop order exists"
            elif kind == "market":
                reason = "opposite side limit order exists"
            elif limit_price is not None and other.limit_price is not None:
                buy_limit, sell_limit = (
                    (limit_price, other.limit_price)
                    if side == "buy"
                    else (other.limit_price, limit_price)
                )
                if buy_limit < sell_limit:
                    continue
                reason = "opposite side limit order exists"
            else:
                continue
            raise self._wash_refusal(other, reason)

    def _check_wash_trade_documented(
        self,
        *,
        symbol: str,
        side: str,
        kind: str,
        limit_price: Decimal | None,
        exclude: UUID | None,
        leg: bool,
        group_id: UUID | None,
    ) -> None:
        """Alpaca's published self-trade table (module docstring, *Self-trade protection*).

        Existing orders are the simulator's open orders, whatever the session. A pair with a leg
        of a complex order on either side is exempt as far as ``complex_leg_wash_exemption``
        reaches: within the leg's own complex order, or everywhere.
        """
        everywhere = self.complex_leg_wash_exemption == "any_order"
        for other in self._orders.values():
            other_kind = self._wash_type(other.type)
            if (
                other.symbol != symbol
                or other.side == side
                or other.status not in _OPEN
                or other.id == exclude
                or other_kind is None
            ):
                continue
            own_order = group_id is not None and other.group_id == group_id
            if (leg or other.parent_id is not None) and (everywhere or own_order):
                continue
            if other_kind in _MARKET_LIKE:
                reason = "opposite side market/stop order exists"
            elif kind in _MARKET_LIKE:
                reason = "opposite side limit order exists"
            else:
                mine, theirs = cast(Decimal, limit_price), cast(Decimal, other.limit_price)
                buy_limit, sell_limit = (mine, theirs) if side == "buy" else (theirs, mine)
                if buy_limit < sell_limit:
                    continue
                reason = "opposite side limit order exists"
            raise self._wash_refusal(other, reason)

    @staticmethod
    def _check_single_trailing(
        params: Mapping[str, Any], *, trailing: bool, tif: str, order_class: str
    ) -> None:
        """Trailing stops are single day / gtc orders: refuse a trailing leg or complex order."""
        legs = [params.get("stop_loss"), params.get("take_profit")]
        for leg in legs:
            if isinstance(leg, Mapping) and (
                _text(leg.get("type")) == _TRAILING
                or any(leg.get(name) is not None for name in _TRAIL_FIELDS)
            ):
                raise _api_error(422, "trailing stop orders are supported only with single orders")
        if not trailing:
            return
        if order_class or any(leg is not None for leg in legs):
            raise _api_error(422, "trailing stop orders are supported only with single orders")
        if tif not in _TRAILING_TIF:
            raise _api_error(422, "trailing_stop orders must be DAY or GTC orders")
        for name in ("limit_price", "stop_price"):
            if params.get(name) is not None:
                raise _api_error(422, f"{name} is not supported for trailing_stop orders")

    def _trail_terms(
        self, params: Mapping[str, Any], side: str
    ) -> tuple[Decimal | None, Decimal | None]:
        """Return ``(trail_percent, trail_price)`` of a new trailing stop: exactly one is set."""
        given = [name for name in _TRAIL_FIELDS if params.get(name) is not None]
        if len(given) != 1:
            raise _api_error(
                422, "trailing_stop orders require exactly one of trail_percent and trail_price"
            )
        (name,) = given
        percent = name == "trail_percent"
        trail = _check_trail(
            _dec(params[name], name),
            name,
            side,
            percent=percent,
            strict=self.strict_trail_precision,
            reference=self._prices.get(str(params.get("symbol") or "").strip().upper()),
        )
        return (trail, None) if percent else (None, trail)

    def _replaced_trail(
        self, old: _SimOrder, params: Mapping[str, Any]
    ) -> tuple[Decimal | None, Decimal | None]:
        """Terms of a trailing stop's successor: only ``trail`` may change, never its kind."""
        for name in _TRAIL_FIELDS:
            if params.get(name) is not None:
                raise _api_error(422, f"{name} cannot be replaced; send trail to change the trail")
        for name in ("qty", "time_in_force"):
            if params.get(name) is not None:
                raise NotImplementedError(
                    f"FaultInjectingAlpacaSim does not model a {name} change on a trailing stop"
                )
        if params.get("trail") is None:
            return old.trail_percent, old.trail_price
        percent = old.trail_percent is not None
        trail = _check_trail(
            _dec(params["trail"], "trail"),
            "trail",
            old.side,
            percent=percent,
            strict=self.strict_trail_precision,
            reference=self._prices.get(old.symbol),
        )
        current = cast(Decimal, old.trail_percent if percent else old.trail_price)
        if self.trail_replace_must_tighten and trail >= current:
            raise _api_error(
                422, f"trail {_fmt(trail)} does not tighten the current trail {_fmt(current)}"
            )
        return (trail, None) if percent else (None, trail)

    def _submit(self, params: Mapping[str, Any]) -> _SimOrder:
        """Validate and create an order (and its OTO stop-loss leg); matching runs afterwards."""
        self._check_account_can_trade()
        order_type = _text(params.get("type"))
        tif = _text(params.get("time_in_force"))
        side = _text(params.get("side"))
        order_class = _text(params.get("order_class"))
        order_class = "" if order_class == "simple" else order_class
        extended = _truthy(params.get("extended_hours"))
        if params.get("notional") is not None:
            raise NotImplementedError("FaultInjectingAlpacaSim does not model notional orders")
        trailing = order_type == _TRAILING
        if order_type not in _ORDER_TYPES and not trailing:
            raise NotImplementedError(f"FaultInjectingAlpacaSim does not model {order_type} orders")
        self._check_single_trailing(params, trailing=trailing, tif=tif, order_class=order_class)
        if tif not in {"day", "gtc", "ioc", "fok"}:
            raise NotImplementedError(f"FaultInjectingAlpacaSim does not model time_in_force={tif}")
        if order_class not in {"", "oto"} or params.get("take_profit") is not None:
            raise NotImplementedError(
                "FaultInjectingAlpacaSim models simple orders and OTO with a stop-loss leg only"
            )
        if side not in {"buy", "sell"}:
            raise _api_error(422, f"invalid side {side!r}")
        trail_percent, trail_price = self._trail_terms(params, side) if trailing else (None, None)

        client_order_id = str(params.get("client_order_id") or uuid4())
        self._check_client_order_id(client_order_id)
        asset = self._asset_for(str(params.get("symbol") or ""))
        symbol = asset.symbol
        if not asset.tradable or asset.status != "active":
            raise _api_error(403, f'asset "{symbol}" is not tradable')

        qty = _dec(params.get("qty"), "qty")
        if qty <= _ZERO:
            raise _api_error(422, "qty must be > 0")
        fractional = qty != qty.to_integral_value()
        if fractional and not asset.fractionable:
            raise _api_error(422, f'asset "{symbol}" is not fractionable')
        if fractional and trailing and not self.fractional_trailing_stops:
            raise _api_error(422, "fractional orders are not supported for trailing_stop orders")
        if fractional and tif != "day":
            raise _api_error(422, "fractional orders must be DAY orders")
        if fractional and order_class:
            raise _api_error(422, "fractional orders must be simple orders")

        limit_price: Decimal | None = None
        stop_price: Decimal | None = None
        if order_type in _LIMIT_TYPES:
            if params.get("limit_price") is None:
                raise _api_error(422, f"{order_type} orders require limit_price")
            limit_price = self._check_price(
                _dec(params["limit_price"], "limit_price"), "limit_price"
            )
        if order_type in _STOP_TYPES:
            if params.get("stop_price") is None:
                raise _api_error(422, f"{order_type} orders require stop_price")
            stop_price = self._check_price(_dec(params["stop_price"], "stop_price"), "stop_price")
        if extended and (order_type != "limit" or tif not in {"day", "gtc"}):
            raise _api_error(422, "extended hours order must be DAY or GTC limit orders")

        leg_stop: Decimal | None = None
        leg_limit: Decimal | None = None
        if order_class == "oto":
            stop_loss = params.get("stop_loss")
            if not isinstance(stop_loss, Mapping) or stop_loss.get("stop_price") is None:
                raise _api_error(422, "oto orders require stop_loss.stop_price")
            if extended:
                raise _api_error(422, "extended hours order must be simple orders")
            if tif not in {"day", "gtc"}:
                raise _api_error(422, "oto orders must be DAY or GTC orders")
            leg_stop = self._check_price(
                _dec(stop_loss["stop_price"], "stop_loss.stop_price"), "stop_loss.stop_price"
            )
            if stop_loss.get("limit_price") is not None:
                leg_limit = self._check_price(
                    _dec(stop_loss["limit_price"], "stop_loss.limit_price"), "stop_loss.limit_price"
                )
            base = limit_price if limit_price is not None else stop_price
            tick = Decimal("0.01")
            if base is not None and side == "buy" and leg_stop > base - tick:
                raise _api_error(422, "stop_loss.stop_price must be <= base_price - 0.01")
            if base is not None and side == "sell" and leg_stop < base + tick:
                raise _api_error(422, "stop_loss.stop_price must be >= base_price + 0.01")
        elif params.get("stop_loss") is not None:
            raise _api_error(422, "stop_loss requires order_class oto")

        # A trailing stop's high-water mark starts at the price standing at submission, in any
        # session (observed 2026-11-27).
        hwm = self._prices.get(symbol) if trailing else None
        if hwm is not None:
            stop_price = _trail_stop(
                side, hwm, trail_percent, trail_price, self.trailing_stop_rounding
            )

        reference = limit_price or stop_price or self._prices.get(symbol)
        self._check_exposure(symbol=symbol, side=side, qty=qty, reference=reference)
        self._check_wash_trade(
            symbol=symbol, side=side, order_type=order_type, limit_price=limit_price
        )
        if leg_stop is not None and self.wash_trade_model == "documented":
            # The leg does not exist yet, so it has no complex order of its own to be exempt in:
            # it is checked against every outside order unless legs are exempt everywhere.
            self._check_wash_trade(
                symbol=symbol,
                side="sell" if side == "buy" else "buy",
                order_type="stop_limit" if leg_limit is not None else "stop",
                limit_price=leg_limit,
                leg=True,
            )

        order = self._new_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            time_in_force=tif,
            qty=qty,
            client_order_id=client_order_id,
            status="accepted",
            limit_price=limit_price,
            stop_price=stop_price,
            extended_hours=extended,
            order_class=order_class,
            group_role="entry" if order_class else None,
        )
        order.trail_percent, order.trail_price, order.hwm = trail_percent, trail_price, hwm
        if order_class == "oto" and leg_stop is not None:
            order.group_id = uuid4()
            self._new_order(
                symbol=symbol,
                side="sell" if side == "buy" else "buy",
                order_type="stop_limit" if leg_limit is not None else "stop",
                time_in_force=tif,
                qty=qty,
                client_order_id=str(uuid4()),
                status="held",
                limit_price=leg_limit,
                stop_price=leg_stop,
                order_class="oto",
                parent=order,
                group_role="stop_loss",
            )
        if self._reject_next_orders > 0:
            self._reject_next_orders -= 1
            self._close(order, "rejected")
        elif trailing or self._eligible(order, self.virtual_clock.session):
            # A trailing stop reads "new" in every session (documented, observed 2026-11-27);
            # any other order queued for a later session reads "accepted".
            order.status = "new"
        return order

    def _after_submit(self, order: _SimOrder) -> None:
        self._settle()
        if order.time_in_force in _IMMEDIATE_TIF and order.status in _WORKING:
            self._close(order, "canceled")

    # ---------------------------------------------------------------- JSON views

    def _order_json(self, order: _SimOrder, *, nested: bool) -> dict[str, Any]:
        legs = (
            [self._order_json(self._orders[leg_id], nested=False) for leg_id in order.leg_ids]
            if nested and order.leg_ids
            else None
        )
        return {
            "id": str(order.id),
            "client_order_id": order.client_order_id,
            "created_at": _iso(order.created_at),
            "updated_at": _iso(order.updated_at),
            "submitted_at": _iso(order.submitted_at),
            "filled_at": _iso_opt(order.filled_at),
            "expires_at": _iso_opt(order.expires_at),
            "expired_at": _iso_opt(order.expired_at),
            "canceled_at": _iso_opt(order.canceled_at),
            "failed_at": _iso_opt(order.failed_at),
            "replaced_at": _iso_opt(order.replaced_at),
            "replaced_by": None if order.replaced_by is None else str(order.replaced_by),
            "replaces": None if order.replaces is None else str(order.replaces),
            "asset_id": str(self._assets[order.symbol].id),
            "symbol": order.symbol,
            "asset_class": "us_equity",
            "notional": None,
            "qty": _fmt(order.qty),
            "filled_qty": _fmt(order.filled_qty),
            "filled_avg_price": _fmt_opt(order.filled_avg_price),
            "order_class": order.order_class,
            "order_type": order.type,
            "type": order.type,
            "side": order.side,
            "time_in_force": order.time_in_force,
            "limit_price": _fmt_opt(order.limit_price),
            "stop_price": _fmt_opt(order.stop_price),
            "status": order.shown_status,
            "extended_hours": order.extended_hours,
            "legs": legs,
            "trail_percent": _fmt_opt(order.trail_percent),
            "trail_price": _fmt_opt(order.trail_price),
            "hwm": _fmt_opt(order.hwm),
            "position_intent": order.position_intent,
            "group_id": None if order.group_id is None else str(order.group_id),
            "group_role": order.group_role,
        }

    def _position_json(self, position: _SimPosition) -> dict[str, Any]:
        asset = self._assets[position.symbol]
        last = self._prices.get(position.symbol, position.avg_entry)
        lastday = self._lastday_prices.get(position.symbol, last)
        cost_basis = position.qty * position.avg_entry
        market_value = position.qty * last
        unrealized = market_value - cost_basis
        ratio = unrealized / abs(cost_basis) if cost_basis != _ZERO else _ZERO
        day_pl = position.qty * (last - lastday)
        day_basis = abs(position.qty * lastday)
        opposite = "sell" if position.qty > _ZERO else "buy"
        reserved = self._reserved(position.symbol, opposite)
        available = position.qty - reserved if position.qty > _ZERO else position.qty + reserved
        return {
            "asset_id": str(asset.id),
            "symbol": position.symbol,
            "exchange": asset.exchange,
            "asset_class": "us_equity",
            "asset_marginable": asset.marginable,
            "qty": _fmt(position.qty),
            "avg_entry_price": _fmt(position.avg_entry.quantize(_AVG_PRICE_QUANTUM)),
            "side": "long" if position.qty > _ZERO else "short",
            "market_value": _fmt(market_value),
            "cost_basis": _fmt(cost_basis),
            "unrealized_pl": _fmt(unrealized),
            "unrealized_plpc": _fmt(ratio.quantize(_AVG_PRICE_QUANTUM)),
            "unrealized_intraday_pl": _fmt(day_pl),
            "unrealized_intraday_plpc": _fmt(
                (day_pl / day_basis).quantize(_AVG_PRICE_QUANTUM) if day_basis else _ZERO
            ),
            "current_price": _fmt(last),
            "lastday_price": _fmt(lastday),
            "change_today": _fmt(
                ((last - lastday) / lastday).quantize(_AVG_PRICE_QUANTUM) if lastday else _ZERO
            ),
            "qty_available": _fmt(available),
        }

    def _asset_json(self, asset: _SimAsset) -> dict[str, Any]:
        return {
            "id": str(asset.id),
            "class": "us_equity",
            "exchange": asset.exchange,
            "symbol": asset.symbol,
            "name": f"{asset.symbol} (simulated)",
            "status": asset.status,
            "tradable": asset.tradable,
            "marginable": asset.marginable,
            "shortable": asset.shortable,
            "easy_to_borrow": asset.easy_to_borrow,
            "fractionable": asset.fractionable,
            "attributes": list(asset.attributes),
            "borrow_status": "easy_to_borrow" if asset.easy_to_borrow else "hard_to_borrow",
        }

    @staticmethod
    def _status_matches(order: _SimOrder, status: str) -> bool:
        if status == "all":
            return True
        return (order.status in _OPEN) == (status == "open")

    # ------------------------------------------------------------- REST handlers

    def _handle_submit_order(self, params: dict[str, Any]) -> dict[str, Any]:
        order = self._submit(params)
        acknowledgement = self._order_json(order, nested=True)
        self._after_submit(order)
        return acknowledgement

    def _handle_get_orders(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        status = _text(params.get("status")) or "open"
        nested = _truthy(params.get("nested"))
        side = _text(params.get("side"))
        raw_symbols = params.get("symbols")
        if isinstance(raw_symbols, str):
            raw_symbols = raw_symbols.split(",")
        symbols = {str(item).strip().upper() for item in raw_symbols or () if str(item).strip()}
        after = _parse_instant(params["after"]) if params.get("after") else None
        until = _parse_instant(params["until"]) if params.get("until") else None
        limit = int(params.get("limit") or _DEFAULT_ORDER_LIMIT)
        if not 1 <= limit <= _MAX_ORDER_LIMIT:
            raise _api_error(422, f"limit must be between 1 and {_MAX_ORDER_LIMIT}")

        def keep(order: _SimOrder) -> bool:
            if nested and order.parent_id is not None:
                return False
            members = [order, *(self._orders[i] for i in order.leg_ids)] if nested else [order]
            if not any(self._status_matches(member, status) for member in members):
                return False
            if side and order.side != side:
                return False
            if symbols and order.symbol not in symbols:
                return False
            if after is not None and order.submitted_at <= after:
                return False
            return until is None or order.submitted_at <= until

        descending = _text(params.get("direction")) != "asc"
        chosen = sorted(
            (order for order in self._orders.values() if keep(order)),
            key=lambda item: (item.submitted_at, item.seq),
            reverse=descending,
        )
        return [self._order_json(order, nested=nested) for order in chosen[:limit]]

    def _handle_get_order_by_id(self, params: dict[str, Any], order_id: str) -> dict[str, Any]:
        order = self._find_order(order_id)
        return self._order_json(order, nested=_truthy(params.get("nested")))

    def _handle_get_order_by_client_id(self, params: dict[str, Any]) -> dict[str, Any]:
        order_id = self._client_order_ids.get(str(params.get("client_order_id") or ""))
        if order_id is None:
            raise _api_error(404, "order not found")
        return self._order_json(self._orders[order_id], nested=_truthy(params.get("nested")))

    @staticmethod
    def _check_replaceable(old: _SimOrder) -> None:
        """Only an order that is at the exchange and not in transition can be replaced."""
        if old.status == "held":
            raise _api_error(422, "unable to replace order, order isn't sent to exchange yet")
        shown = old.shown_status
        if old.status not in _REPLACEABLE or shown != old.status:
            raise _api_error(422, f'unable to replace order, order is in "{shown}" state')
        if old.cancel_requested_at is not None:
            raise _api_error(422, "unable to replace order, a cancel request is pending")

    def _handle_replace_order_by_id(self, params: dict[str, Any], order_id: str) -> dict[str, Any]:
        old = self._find_order(order_id)
        self._check_replaceable(old)
        trailing = old.type == _TRAILING
        if params.get("trail") is not None and not trailing:
            raise NotImplementedError(
                f"FaultInjectingAlpacaSim does not model trail on a {old.type} order"
            )
        trail_percent, trail_price = self._replaced_trail(old, params) if trailing else (None, None)
        if self.empty_replace_refused and all(params.get(name) is None for name in _REPLACE_FIELDS):
            raise _api_error(
                422, f"replace request changes nothing: set one of {', '.join(_REPLACE_FIELDS)}"
            )
        qty = old.qty
        if params.get("qty") is not None:
            qty = _dec(params["qty"], "qty")
            if qty != qty.to_integral_value() or qty <= old.filled_qty:
                raise _api_error(422, "qty must be a whole number greater than the filled quantity")
        limit_price = old.limit_price
        if params.get("limit_price") is not None:
            if old.type not in _LIMIT_TYPES:
                raise _api_error(422, f"limit_price is not supported for {old.type} orders")
            limit_price = self._check_price(
                _dec(params["limit_price"], "limit_price"), "limit_price"
            )
        stop_price = old.stop_price
        if params.get("stop_price") is not None:
            if old.type not in _STOP_TYPES:
                raise _api_error(422, f"stop_price is not supported for {old.type} orders")
            stop_price = self._check_price(_dec(params["stop_price"], "stop_price"), "stop_price")
        hwm = self._successor_mark(old) if trailing else None
        if trailing:
            # Provisional until the replacement takes effect, when both are worked out again.
            stop_price = (
                None
                if hwm is None
                else _trail_stop(
                    old.side, hwm, trail_percent, trail_price, self.trailing_stop_rounding
                )
            )
        tif = _text(params.get("time_in_force")) or old.time_in_force
        if tif not in {"day", "gtc", "ioc", "fok"}:
            raise NotImplementedError(f"FaultInjectingAlpacaSim does not model time_in_force={tif}")
        client_order_id = str(params.get("client_order_id") or uuid4())
        self._check_client_order_id(client_order_id)
        self._check_exposure(
            symbol=old.symbol,
            side=old.side,
            qty=qty - old.filled_qty,
            reference=limit_price or stop_price or self._prices.get(old.symbol),
            exclude=old.id,
        )
        self._check_wash_trade(
            symbol=old.symbol,
            side=old.side,
            order_type=old.type,
            limit_price=limit_price,
            exclude=old.id,
            leg=old.parent_id is not None,
            group_id=old.group_id,
        )

        parent = None if old.parent_id is None else self._orders[old.parent_id]
        new = self._new_order(
            symbol=old.symbol,
            side=old.side,
            order_type=old.type,
            time_in_force=tif,
            qty=qty,
            client_order_id=client_order_id,
            status=_PENDING_NEW,
            limit_price=limit_price,
            stop_price=stop_price,
            extended_hours=old.extended_hours,
            order_class=old.order_class,
            parent=parent,
            group_role=old.group_role,
        )
        new.replaces = old.id
        new.group_id = old.group_id
        new.position_intent = old.position_intent
        new.trail_percent, new.trail_price, new.hwm = trail_percent, trail_price, hwm
        if self._reject_next_orders > 0:
            # A rejected replacement leaves the original order working, as at the broker.
            self._reject_next_orders -= 1
            self._close(new, "rejected")
            return self._order_json(new, nested=True)
        old.pending_replace_by = new.id
        old.updated_at = self.virtual_clock.now
        if self.replace_pending != "never":
            # Accepted, not yet effective: the old order keeps working until the replacement
            # takes effect (module docstring, *Replace*).
            return self._order_json(new, nested=True)
        self._apply_replacement(old, new)
        acknowledgement = self._order_json(new, nested=True)
        self._after_submit(new)
        return acknowledgement

    @staticmethod
    def _cancelable(order: _SimOrder) -> bool:
        """An order that is being replaced, or is the pending replacement, rejects a cancel."""
        return order.status != _PENDING_NEW and order.pending_replace_by is None

    def _handle_cancel_order_by_id(self, params: dict[str, Any], order_id: str) -> None:
        del params
        order = self._find_order(order_id)
        if order.status not in _OPEN:
            raise _api_error(422, f'order is already in "{order.status}" state')
        if not self._cancelable(order):
            raise _api_error(422, f'order is in "{order.shown_status}" state')
        self._request_cancel(order)

    def _handle_cancel_orders(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        del params
        results: list[dict[str, Any]] = []
        for order in sorted(self._orders.values(), key=lambda item: item.seq):
            if order.status not in _OPEN:
                continue
            if self._cancelable(order):
                # 200 means the request was accepted; ``defer_cancels`` may leave it unapplied.
                self._request_cancel(order)
                results.append({"id": str(order.id), "status": 200})
            else:
                results.append({"id": str(order.id), "status": 500})
        return results

    def _handle_get_all_positions(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        del params
        return [self._position_json(self._positions[symbol]) for symbol in sorted(self._positions)]

    def _lookup_position(self, symbol_or_asset_id: str) -> _SimPosition:
        key = symbol_or_asset_id.strip()
        for position in self._positions.values():
            if key.upper() == position.symbol or key.lower() == str(
                self._assets[position.symbol].id
            ):
                return position
        raise _api_error(404, "position does not exist")

    def _handle_get_open_position(self, params: dict[str, Any], symbol: str) -> dict[str, Any]:
        del params
        return self._position_json(self._lookup_position(symbol))

    def _close_position_order(self, position: _SimPosition, qty: Decimal) -> _SimOrder:
        return self._submit(
            {
                "symbol": position.symbol,
                "qty": _fmt(qty),
                "side": "sell" if position.qty > _ZERO else "buy",
                "type": "market",
                "time_in_force": "day",
            }
        )

    def _handle_close_position(self, params: dict[str, Any], symbol: str) -> dict[str, Any]:
        position = self._lookup_position(symbol)
        qty = abs(position.qty)
        if params.get("qty") is not None:
            qty = _dec(params["qty"], "qty")
        elif params.get("percentage") is not None:
            percentage = _dec(params["percentage"], "percentage")
            if not _ZERO < percentage <= Decimal("100"):
                raise _api_error(422, "percentage must be in (0, 100]")
            qty = abs(position.qty) * percentage / Decimal("100")
        order = self._close_position_order(position, qty)
        acknowledgement = self._order_json(order, nested=True)
        self._after_submit(order)
        return acknowledgement

    def _handle_close_all_positions(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        if _truthy(params.get("cancel_orders")):
            self._handle_cancel_orders({})
        results: list[dict[str, Any]] = []
        submitted: list[_SimOrder] = []
        for symbol in sorted(self._positions):
            position = self._positions[symbol]
            try:
                order = self._close_position_order(position, abs(position.qty))
            except APIError as exc:
                detail = json.loads(str(exc))
                detail.setdefault("symbol", symbol)
                results.append({"symbol": symbol, "status": exc.status_code, "body": detail})
                continue
            submitted.append(order)
            results.append(
                {"symbol": symbol, "status": 200, "body": self._order_json(order, nested=True)}
            )
        for order in submitted:
            self._after_submit(order)
        return results

    def _handle_get_account(self, params: dict[str, Any]) -> dict[str, Any]:
        del params
        flags = self._account_flags
        equity = self._equity()
        long_value = sum(
            (
                pos.qty * self._prices.get(pos.symbol, pos.avg_entry)
                for pos in self._positions.values()
                if pos.qty > _ZERO
            ),
            _ZERO,
        )
        short_value = self._market_value() - long_value
        buying_power = _fmt(self._buying_power())
        return {
            "id": str(self._account_id),
            "account_number": self._account_number,
            "status": flags["status"],
            "currency": "USD",
            "buying_power": buying_power,
            "effective_buying_power": buying_power,
            "regt_buying_power": buying_power,
            "non_marginable_buying_power": _fmt(max(_ZERO, self._cash)),
            "cash": _fmt(self._cash),
            "accrued_fees": "0",
            "pending_transfer_in": "0",
            "pending_transfer_out": "0",
            "portfolio_value": _fmt(equity),
            "trading_blocked": flags["trading_blocked"],
            "transfers_blocked": flags["transfers_blocked"],
            "account_blocked": flags["account_blocked"],
            "created_at": _iso(self._account_created_at),
            "trade_suspended_by_user": flags["trade_suspended_by_user"],
            "multiplier": str(flags["multiplier"]),
            "shorting_enabled": flags["shorting_enabled"],
            "equity": _fmt(equity),
            "last_equity": _fmt(self._last_equity),
            "long_market_value": _fmt(long_value),
            "short_market_value": _fmt(short_value),
            "position_market_value": _fmt(long_value + abs(short_value)),
            "initial_margin": "0",
            "maintenance_margin": "0",
            "last_maintenance_margin": "0",
            "sma": "0",
        }

    def _handle_get_clock(self, params: dict[str, Any]) -> dict[str, Any]:
        del params
        next_open, next_close = self._session_marks()
        return {
            "timestamp": self.virtual_clock.now.astimezone(_NY).isoformat(),
            "is_open": self.virtual_clock.session == "RTH",
            "next_open": next_open.astimezone(_NY).isoformat(),
            "next_close": next_close.astimezone(_NY).isoformat(),
        }

    def _handle_get_all_assets(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        status = _text(params.get("status"))
        asset_class = _text(params.get("asset_class"))
        exchange = _text(params.get("exchange"))
        wanted = {item.strip() for item in str(params.get("attributes") or "").split(",") if item}
        if asset_class and asset_class != "us_equity":
            return []
        return [
            self._asset_json(asset)
            for _, asset in sorted(self._assets.items())
            if (not status or status == "all" or asset.status == status)
            and (not exchange or asset.exchange.lower() == exchange)
            and wanted <= set(asset.attributes)
        ]

    def _handle_get_asset(self, params: dict[str, Any], symbol: str) -> dict[str, Any]:
        del params
        for asset in self._assets.values():
            if symbol.strip().lower() == str(asset.id):
                return self._asset_json(asset)
        return self._asset_json(self._asset_for(symbol))

    def _handle_get_account_activities(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        after = _parse_instant(params["after"]) if params.get("after") else None
        until = _parse_instant(params["until"]) if params.get("until") else None
        descending = _text(params.get("direction")) != "asc"
        page_size = int(params.get("page_size") or _DEFAULT_ACTIVITY_PAGE)
        if not 1 <= page_size <= _DEFAULT_ACTIVITY_PAGE:
            raise _api_error(422, f"page_size must be between 1 and {_DEFAULT_ACTIVITY_PAGE}")
        ordered = sorted(self._activities, key=lambda item: item["seq"], reverse=descending)
        token = params.get("page_token")
        if token:
            position = next((i for i, item in enumerate(ordered) if item["id"] == token), None)
            if position is None:
                raise _api_error(422, "invalid page_token")
            ordered = ordered[position + 1 :]
        page: list[dict[str, Any]] = []
        for item in ordered:
            when = _parse_instant(item["transaction_time"])
            if (after is not None and when <= after) or (until is not None and when >= until):
                continue
            page.append({key: value for key, value in item.items() if key != "seq"})
            if len(page) == page_size:
                break
        return page

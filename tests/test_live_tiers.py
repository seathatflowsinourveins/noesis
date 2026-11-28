"""Live tier model tests: venue order intents, tier grants and fail-closed live venues."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import itertools
import json
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import wait as futures_wait
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, get_args
from unittest import mock

import duckdb
import pytest
from alpaca.common.enums import BaseURL
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from alpaca.trading.requests import (
    GetOrderByIdRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    ReplaceOrderRequest,
    StopOrderRequest,
    TrailingStopOrderRequest,
)
from hypothesis import HealthCheck, event, example, given, settings
from hypothesis import strategies as st
from tests.alpaca_sim import MUTATING, SIM_SESSIONS, FaultInjectingAlpacaSim

from tier0_sota import risk
from tier0_sota.engine import (
    EXECUTION_VENUE_PROFILES,
    FORBIDDEN_LIVE_IBKR_PORTS,
    LIVE_TIERED_VENUE_ID,
    LIVE_VENUE_INTENT_GRANTS,
    REENTRY_MAX_ATTEMPTS_PER_SESSION,
    REGIME_FLIP_MIN_GENERATIONS,
    REGIME_FLIP_MIN_SECONDS,
    VenueOrderIntent,
    build_ibkr_paper_connection_spec,
    build_venue_order_handle_spec,
    compute_non_noise_min_trail_spec,
    inspect_venue_capability,
    validate_venue_order_capability,
)
from tier0_sota.live import execution
from tier0_sota.live import paper_farm as pf

# No test in this module may see a live-looking key in the environment or open an off-host socket.
pytestmark = pytest.mark.usefixtures("no_live_broker_access")

_INTENTS: tuple[VenueOrderIntent, ...] = get_args(VenueOrderIntent)
_TIERED_INTENTS = ("READ_ONLY", "PROTECTIVE", "OWNER_LINEAGE_ENTRY", "OPERATOR_CONFIRMED_ENTRY")
_FLAG_COMBOS = tuple(itertools.product((False, True), repeat=2))
_PAPER_VENUES = tuple(v for v, p in EXECUTION_VENUE_PROFILES.items() if p.environment == "PAPER")
_LIVE_VENUES = tuple(v for v, p in EXECUTION_VENUE_PROFILES.items() if p.environment == "LIVE")
_STILL_REFUSED_LIVE_VENUES = ("IBKR_LIVE_3000", "ALPACA_LIVE_ELITE_SMART_ROUTER")
_LIVE_IBKR_PORTS = (4001, 7496, 8796, 8798)
_IBKR_PAPER_SPEC = "build_ibkr_paper_connection_spec"
_HARD_REFUSED_REASON = "LIVE execution hard-refused (live_execution_enabled=False)"
_NO_LIVE_TIERS = {
    "tier1_protective": False,
    "tier2_operator_confirmed": False,
    "tier3_autonomous_default": False,
}
_ADDED_KEYS = ("authorized_intents", "live_tiers")
_LEGACY_CAPABILITY_KEYS = (
    "venue_id",
    "broker",
    "environment",
    "routing_tier",
    "max_rpm",
    "max_rps",
    "live_execution_enabled",
    "supports_extended_overnight_24x5",
    "allowed",
    "capability_ok",
    "executable_now",
    "reasons",
    "exit_reserve_rpm",
    "active_entry_rpm",
)
_LEGACY_HANDLE_SPEC_KEYS = (
    "schema_version",
    "venue_profile",
    "broker",
    "environment",
    "routing_tier",
    "max_rpm",
    "max_rps",
    "entry_modify_rpm_budget",
    "exit_reserve_rpm",
    "min_exit_reserve_fraction",
    "max_concurrent_trailed_positions",
    "trailing_modify_cadence_secs",
    "session",
    "trailing_schedule",
    "session_order_mode",
    "limit_collar_bps",
    "initial_max_loss_pct",
    "max_notional_scale",
    "outside_rth",
    "supports_native_trailing",
    "supports_extended_overnight_24x5",
    "supports_dma",
    "supports_twap",
    "supports_vwap",
    "order_flow_classification",
    "reg_nms_covered",
    "nautilus_submit_rate",
    "nautilus_modify_rate",
    "paper_only",
    "live_execution_enabled",
    "executable_now",
)


def _assert_refused(venue_id: str, **kwargs: Any) -> None:
    """A refusal is the pre-tier-model PermissionError text, quoting the caller's venue id."""
    with pytest.raises(PermissionError) as exc:
        validate_venue_order_capability(venue_id, **kwargs)
    assert str(exc.value) == f"Refused live profile {venue_id!r}: live_execution_enabled=False"


def test_venue_intent_matrix_default_refuses_and_tiers_grant() -> None:
    # Constants: five intents, one tiered venue, tiers 1-2 as its only standing grants.
    assert get_args(VenueOrderIntent) == (*_TIERED_INTENTS, "AUTONOMOUS_ENTRY")
    assert LIVE_TIERED_VENUE_ID == "ALPACA_LIVE_PRO_LINE_1000"
    assert dict(LIVE_VENUE_INTENT_GRANTS) == {LIVE_TIERED_VENUE_ID: frozenset(_TIERED_INTENTS)}
    assert all("AUTONOMOUS_ENTRY" not in granted for granted in LIVE_VENUE_INTENT_GRANTS.values())
    grants_any: Any = LIVE_VENUE_INTENT_GRANTS
    for venue_id in (LIVE_TIERED_VENUE_ID, "IBKR_LIVE_3000"):
        with pytest.raises(TypeError):
            grants_any[venue_id] = frozenset(_INTENTS)
    assert REENTRY_MAX_ATTEMPTS_PER_SESSION == 3
    assert (REGIME_FLIP_MIN_GENERATIONS, REGIME_FLIP_MIN_SECONDS) == (3, 180)
    trail = compute_non_noise_min_trail_spec(
        symbol="AAPL", entry_price="100.00", side="LONG", session="RTH", natr_pct="0.03"
    )
    lifted = trail["rapid_reentry_policy"]["reentry_max_attempts_per_session"]
    assert lifted == REENTRY_MAX_ATTEMPTS_PER_SESSION
    assert all(p.live_execution_enabled is False for p in EXECUTION_VENUE_PROFILES.values())
    tiered = EXECUTION_VENUE_PROFILES[LIVE_TIERED_VENUE_ID]
    assert tiered.environment == "LIVE"
    assert set(_LIVE_VENUES) == {LIVE_TIERED_VENUE_ID, *_STILL_REFUSED_LIVE_VENUES}

    # Default refuses: an intent-less call is AUTONOMOUS_ENTRY without opt-in or merit, so every
    # LIVE venue refuses it with the existing text. One flag alone never unlocks tier 3.
    for venue_id in _LIVE_VENUES:
        _assert_refused(venue_id)
        _assert_refused(venue_id, intent="AUTONOMOUS_ENTRY")
        for one_flag in ({"autonomous_opt_in": True}, {"leg_merit": True}):
            _assert_refused(venue_id, **one_flag)
            _assert_refused(venue_id, intent="AUTONOMOUS_ENTRY", **one_flag)
    _assert_refused(" alpaca_live_pro_line_1000 ")

    # Tiers grant: the four standing intents return the registry profile on the tiered venue,
    # whatever the tier-3 flags say, and the profile itself stays live_execution_enabled=False.
    for intent in _TIERED_INTENTS:
        for opt_in, merit in _FLAG_COMBOS:
            got = validate_venue_order_capability(
                LIVE_TIERED_VENUE_ID, intent=intent, autonomous_opt_in=opt_in, leg_merit=merit
            )
            assert got is tiered and got.live_execution_enabled is False
    normalised = validate_venue_order_capability(" alpaca_live_pro_line_1000 ", intent="PROTECTIVE")
    assert normalised is tiered

    # Tier 3: AUTONOMOUS_ENTRY needs the owner opt-in AND the leg's merit, both literally True.
    explicit = validate_venue_order_capability(
        LIVE_TIERED_VENUE_ID, intent="AUTONOMOUS_ENTRY", autonomous_opt_in=True, leg_merit=True
    )
    implicit = validate_venue_order_capability(
        LIVE_TIERED_VENUE_ID, autonomous_opt_in=True, leg_merit=True
    )
    assert explicit is tiered and implicit is tiered
    assert tiered.live_execution_enabled is False
    for truthy in (1, "true", "false", [True], object()):
        _assert_refused(LIVE_TIERED_VENUE_ID, autonomous_opt_in=truthy, leg_merit=True)
        _assert_refused(LIVE_TIERED_VENUE_ID, autonomous_opt_in=True, leg_merit=truthy)

    # An intent outside the five is never granted on LIVE, even with both tier-3 flags set.
    for bogus in ("", "protective", "PROTECTIVE ", "ENTRY", None, 1, ["PROTECTIVE"]):
        _assert_refused(LIVE_TIERED_VENUE_ID, intent=bogus)
        _assert_refused(LIVE_TIERED_VENUE_ID, intent=bogus, autonomous_opt_in=True, leg_merit=True)

    # A granted intent does not skip the capability check (ValueError first, as before).
    with pytest.raises(ValueError, match="algo=DMA unsupported on ALPACA_PRO_LINE_HELD"):
        validate_venue_order_capability(LIVE_TIERED_VENUE_ID, intent="PROTECTIVE", algo="DMA")
    with pytest.raises(ValueError, match=r"requested_rpm=1001 outside \(0, 1000\]"):
        validate_venue_order_capability(
            LIVE_TIERED_VENUE_ID, intent="PROTECTIVE", requested_rpm=1001
        )
    with pytest.raises(ValueError, match="24/5 overnight session unsupported"):
        validate_venue_order_capability(
            LIVE_TIERED_VENUE_ID,
            intent="AUTONOMOUS_ENTRY",
            autonomous_opt_in=True,
            leg_merit=True,
            require_24x5_overnight=True,
        )
    in_budget = validate_venue_order_capability(
        LIVE_TIERED_VENUE_ID, intent="PROTECTIVE", requested_rpm=1000
    )
    assert in_budget is tiered
    with pytest.raises(ValueError, match="Unknown execution venue profile"):
        validate_venue_order_capability("UNKNOWN_VENUE", intent="PROTECTIVE")

    # PAPER: as before the tier model, for every intent and flag combination.
    assert len(_PAPER_VENUES) == 3
    for venue_id in _PAPER_VENUES:
        prof = validate_venue_order_capability(venue_id)
        assert prof is EXECUTION_VENUE_PROFILES[venue_id]
        for intent in _INTENTS:
            for opt_in, merit in _FLAG_COMBOS:
                got = validate_venue_order_capability(
                    venue_id, intent=intent, autonomous_opt_in=opt_in, leg_merit=merit
                )
                assert got is prof
    with pytest.raises(ValueError, match="algo=DMA unsupported on ALPACA_PAPER_STANDARD"):
        validate_venue_order_capability("ALPACA_PAPER_200", intent="PROTECTIVE", algo="DMA")

    # Inspection and order-handle documents: every existing key, value and reason string stays;
    # the two additive keys come last and describe what validate() accepts with both flags False.
    for venue_id, prof in EXECUTION_VENUE_PROFILES.items():
        is_paper = prof.environment == "PAPER"
        is_tiered = venue_id == LIVE_TIERED_VENUE_ID
        expected_intents = (
            sorted(_INTENTS)
            if is_paper
            else sorted(LIVE_VENUE_INTENT_GRANTS.get(venue_id, frozenset()))
        )
        expected_tiers = {
            "tier1_protective": is_tiered,
            "tier2_operator_confirmed": is_tiered,
            "tier3_autonomous_default": False,
        }
        cap = inspect_venue_capability(venue_id)
        spec = build_venue_order_handle_spec(venue_id, "RTH", "baseline")
        assert tuple(cap) == _LEGACY_CAPABILITY_KEYS + _ADDED_KEYS
        assert tuple(spec) == _LEGACY_HANDLE_SPEC_KEYS + _ADDED_KEYS
        assert cap["capability_ok"] is True
        assert cap["allowed"] is is_paper and cap["executable_now"] is is_paper
        assert cap["reasons"] == ([] if is_paper else [_HARD_REFUSED_REASON])
        assert cap["live_execution_enabled"] is False
        assert spec["paper_only"] is is_paper and spec["executable_now"] is is_paper
        assert spec["live_execution_enabled"] is False
        for doc in (cap, spec):
            assert doc["authorized_intents"] == expected_intents
            assert doc["live_tiers"] == expected_tiers
            assert all(type(flag) is bool for flag in doc["live_tiers"].values())
            assert json.loads(json.dumps(doc)) == doc
        for intent in _INTENTS:
            if intent in cap["authorized_intents"]:
                assert validate_venue_order_capability(venue_id, intent=intent) is prof
            else:
                _assert_refused(venue_id, intent=intent)

        # Each call returns fresh containers: a caller mutating one cannot widen the next.
        for doc in (cap, spec):
            doc["authorized_intents"].append("AUTONOMOUS_ENTRY")
            doc["live_tiers"]["tier3_autonomous_default"] = True
        for fresh in (inspect_venue_capability(venue_id), build_venue_order_handle_spec(venue_id)):
            assert fresh["authorized_intents"] == expected_intents
            assert fresh["live_tiers"] == expected_tiers

    tiered_cap = inspect_venue_capability(LIVE_TIERED_VENUE_ID)
    assert tiered_cap["authorized_intents"] == sorted(_TIERED_INTENTS)
    assert (tiered_cap["max_rpm"], tiered_cap["active_entry_rpm"]) == (1000, 900)
    assert tiered_cap["exit_reserve_rpm"] == 100
    # Call-specific capability findings do not change the venue-level intent view.
    dma_cap = inspect_venue_capability(LIVE_TIERED_VENUE_ID, algo="DMA")
    assert dma_cap["capability_ok"] is False and dma_cap["allowed"] is False
    assert dma_cap["reasons"] == [
        "algo=DMA unsupported on ALPACA_PRO_LINE_HELD",
        _HARD_REFUSED_REASON,
    ]
    assert dma_cap["authorized_intents"] == tiered_cap["authorized_intents"]
    assert dma_cap["live_tiers"] == tiered_cap["live_tiers"]


@pytest.mark.parametrize(
    ("target", "kwargs"),
    [
        *(
            pytest.param(
                venue_id,
                {"intent": intent, "autonomous_opt_in": opt_in, "leg_merit": merit},
                id=f"{venue_id}-{intent}-optin{int(opt_in)}-merit{int(merit)}",
            )
            for venue_id in _STILL_REFUSED_LIVE_VENUES
            for intent in _INTENTS
            for opt_in, merit in _FLAG_COMBOS
        ),
        *(
            pytest.param(_IBKR_PAPER_SPEC, {"port": port}, id=f"ibkr-paper-spec-live-port-{port}")
            for port in _LIVE_IBKR_PORTS
        ),
    ],
)
def test_only_alpaca_live_pro_line_is_unlockable(target: str, kwargs: dict[str, Any]) -> None:
    # The parametrised names are the whole refused surface, cross-checked against the registry.
    registry_refused = {v for v in _LIVE_VENUES if v != LIVE_TIERED_VENUE_ID}
    assert registry_refused == set(_STILL_REFUSED_LIVE_VENUES)
    assert set(LIVE_VENUE_INTENT_GRANTS) == {LIVE_TIERED_VENUE_ID}
    assert frozenset(_LIVE_IBKR_PORTS) == FORBIDDEN_LIVE_IBKR_PORTS
    assert len(FORBIDDEN_LIVE_IBKR_PORTS) == 4

    if target == _IBKR_PAPER_SPEC:
        port = kwargs["port"]
        with pytest.raises(ValueError, match=f"Refused IBKR port {port};"):
            build_ibkr_paper_connection_spec(port=port)
        with pytest.raises(ValueError, match=f"Refused IBKR port {port};"):
            build_ibkr_paper_connection_spec(host="127.0.0.1", port=port, client_id=7)
        for paper_port in (4002, 7497):
            paper_spec = build_ibkr_paper_connection_spec(port=paper_port)
            assert paper_spec["port"] == paper_port and paper_spec["paper_only"] is True
            assert paper_spec["live_execution_enabled"] is False
        return

    # A still-refused LIVE venue refuses this intent and flag combination, also without an
    # explicit intent and also when the request stays inside the venue's capability envelope.
    flags = {k: v for k, v in kwargs.items() if k != "intent"}
    assert target not in LIVE_VENUE_INTENT_GRANTS
    _assert_refused(target, **kwargs)
    _assert_refused(target, **flags)
    _assert_refused(target, requested_rpm=1, **kwargs)
    cap = inspect_venue_capability(target)
    spec = build_venue_order_handle_spec(target)
    assert cap["allowed"] is False and cap["executable_now"] is False
    assert cap["reasons"] == [_HARD_REFUSED_REASON]
    assert spec["executable_now"] is False and spec["paper_only"] is False
    for doc in (cap, spec):
        assert doc["live_execution_enabled"] is False
        assert doc["authorized_intents"] == []
        assert doc["live_tiers"] == _NO_LIVE_TIERS

    # The same request on the tiered venue is the only one that can return, and only by its tier.
    tiered = EXECUTION_VENUE_PROFILES[LIVE_TIERED_VENUE_ID]
    tier3 = flags["autonomous_opt_in"] and flags["leg_merit"]
    if kwargs["intent"] in LIVE_VENUE_INTENT_GRANTS[LIVE_TIERED_VENUE_ID] or tier3:
        assert validate_venue_order_capability(LIVE_TIERED_VENUE_ID, **kwargs) is tiered
    else:
        _assert_refused(LIVE_TIERED_VENUE_ID, **kwargs)


# ------------------------------------------------------------ live lane and broker choke point

_LIVE_ACCOUNT = "ALPACA_LIVE1"
_PAPER_ONLY_REFUSAL = "Refused non-paper execution; paper_only=True required."
# Obviously fake credentials with the real prefix convention. They never enter the environment.
_FAKE_LIVE_KEY_ID = "AK" + "LANETEST" * 2 + "99"
_FAKE_PAPER_KEY_ID = "PK" + _FAKE_LIVE_KEY_ID[2:]
_FAKE_KEY_VALUE = "lane-test-" * 4
_LIVE_CREDENTIAL_NAMES = ("ALPACA_LIVE_API_KEY", "ALPACA_LIVE_SECRET_KEY")
_LANE_SESSIONS = ("RTH", "PRE_MARKET", "AFTER_HOURS", "OVERNIGHT")
_ENTRY_INTENTS = ("OWNER_LINEAGE_ENTRY", "OPERATOR_CONFIRMED_ENTRY", "AUTONOMOUS_ENTRY")
_BROKER_MUTATIONS = frozenset(
    {
        "submit_order",
        "replace_order_by_id",
        "cancel_order_by_id",
        "cancel_orders",
        "close_position",
        "close_all_positions",
    }
)
_CHOKE_POINT_METHODS = frozenset(
    {"broker_submit", "broker_replace", "broker_cancel", "broker_flatten"}
)
_TIER_LEDGER_COLUMNS = {
    "entry_authority": "VARCHAR",
    "lineage_root_id": "VARCHAR",
    "lineage_parent_id": "VARCHAR",
    "lineage_depth": "INTEGER",
    "entry_qty": "DECIMAL(18,6)",
    "close_cause": "VARCHAR",
    "macro_regime": "INTEGER",
    "adoption_anchor_price": "DECIMAL(18,6)",
}
_TIER_LEDGER_SELECT = (
    "SELECT entry_authority, lineage_root_id, lineage_parent_id, lineage_depth, entry_qty, "
    "close_cause, macro_regime, adoption_anchor_price FROM paper_entry_decisions "
    "WHERE client_order_id = ?"
)
_PROPERTY = settings(
    max_examples=80,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


def _live_sim(**kwargs: Any) -> FaultInjectingAlpacaSim:
    return FaultInjectingAlpacaSim(paper=False, **kwargs)


def _live_lane(directory: Path, client: Any = None, **overrides: Any) -> pf.AlpacaPaperLane:
    kwargs: dict[str, Any] = {"paper_only": False, "venue_profile": LIVE_TIERED_VENUE_ID}
    kwargs.update(overrides)
    broker = _live_sim() if client is None else client
    return pf.AlpacaPaperLane(_LIVE_ACCOUNT, directory, broker, pf.CrossLaneSymbolLock(), **kwargs)


def _paper_lane(directory: Path, client: Any = None, **overrides: Any) -> pf.AlpacaPaperLane:
    broker = FaultInjectingAlpacaSim() if client is None else client
    return pf.AlpacaPaperLane(
        "ALPACA_PA1", directory, broker, pf.CrossLaneSymbolLock(), **overrides
    )


def _assert_paper_only_refusal(build: Callable[[], object]) -> PermissionError:
    """The refusal is the pre-tier-model text of the lane, whatever the reason behind it."""
    with pytest.raises(PermissionError) as refusal:
        build()
    assert str(refusal.value) == _PAPER_ONLY_REFUSAL
    return refusal.value


def _sell_stop(symbol: str, qty: int, stop_price: float) -> StopOrderRequest:
    return StopOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        stop_price=stop_price,
    )


def _budget_slots_used(lane: pf.AlpacaPaperLane) -> int:
    return len(lane.rate_budget._timestamps)


def _saturate_entry_budget(lane: pf.AlpacaPaperLane, now_s: float) -> None:
    budget = lane.rate_budget
    for _ in range(budget.max_rpm - budget.exit_reserve_rpm):
        assert budget.try_consume(now_s, is_exit=False) is True
    assert budget.try_consume(now_s, is_exit=False) is False


def _tier_ledger_row(lane: pf.AlpacaPaperLane, client_order_id: str) -> tuple[Any, ...] | None:
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
        return db.execute(_TIER_LEDGER_SELECT, [client_order_id]).fetchone()


def test_live_lane_construction_discriminator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Profiles: one live lane account, disjoint from the paper lanes, sized by risk.py's own
    # keyword defaults. A profile above a default is refused at import and at construction.
    sizing = inspect.signature(risk.size_position_by_risk).parameters
    assert set(pf.ALPACA_LIVE_LANE_PROFILES) == {_LIVE_ACCOUNT}
    assert set(pf.ALPACA_LANE_PROFILES) == {"ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3"}
    profile = pf.ALPACA_LIVE_LANE_PROFILES[_LIVE_ACCOUNT]
    assert profile["lane_id"] == "LV1_LOSSLESS_TRAIL"
    for name in ("risk_per_trade_pct", "max_notional_pct"):
        assert type(profile[name]) is Decimal and profile[name] == sizing[name].default
        with monkeypatch.context() as patched:
            patched.setitem(profile, name, sizing[name].default + Decimal("0.0001"))
            with pytest.raises(ValueError, match=f"{name}=.* outside"):
                pf._assert_live_lane_profiles_within_risk_defaults()
            cause = _assert_paper_only_refusal(lambda: _live_lane(tmp_path / "wide")).__cause__
            assert isinstance(cause, ValueError)
    pf._assert_live_lane_profiles_within_risk_defaults()

    # A live lane: live lane account + paper_only=False + the tiered venue + a live client.
    sim = _live_sim()
    lane = _live_lane(tmp_path, sim, venue_profile=" alpaca_live_pro_line_1000 ")
    assert lane.environment == "LIVE" and lane.paper_only is False
    assert lane.live_execution_enabled is False and lane.equity_verified_at is None
    assert lane.venue_profile == LIVE_TIERED_VENUE_ID and lane.trading_client is sim
    assert lane.profile is profile and lane.breaker.allow_entry() is True
    assert (lane.rate_budget.max_rpm, lane.rate_budget.exit_reserve_rpm) == (1000, 100)
    lane.assert_integrity()
    spec = lane.order_handle_spec()
    assert spec["venue_profile"] == LIVE_TIERED_VENUE_ID and spec["account"] == _LIVE_ACCOUNT
    assert spec["lane_id"] == "LV1_LOSSLESS_TRAIL" and spec["executable_now"] is False
    assert spec["effective_entry_modify_rpm_budget"] == 900
    status = lane.write_status(equity=50_000.0, session="RTH", open_positions_count=0)
    assert status["schema_version"] == "tier0.alpaca-live-lane.v1"
    assert status["paper_only"] is False and status["live_execution_enabled"] is False
    assert status["endpoint"] == BaseURL.TRADING_LIVE.value != pf.PAPER_ALPACA_ENDPOINT
    assert json.loads(lane.status_path.read_text("utf-8"))["account_alias"] == _LIVE_ACCOUNT
    assert sim.call_log == []  # building, checking and describing a lane never calls the broker

    # live_execution_enabled keeps one meaning on a live lane: the owner's tier-3 opt-in.
    opted_in = _live_lane(tmp_path / "opt-in", live_execution_enabled=True)
    assert opted_in.live_execution_enabled is True
    assert opted_in.write_status()["live_execution_enabled"] is True
    for not_a_bool in (1, "true", None):
        _assert_paper_only_refusal(
            lambda flag=not_a_bool: _live_lane(tmp_path, live_execution_enabled=flag)
        )

    # Every other combination is refused with the existing text.
    paper_sim = FaultInjectingAlpacaSim()
    cause = _assert_paper_only_refusal(lambda: _live_lane(tmp_path, paper_sim)).__cause__
    assert isinstance(cause, PermissionError) and "non-live trading client" in str(cause)
    for wrong_venue in ("", "ALPACA_PAPER_1000", "IBKR_PAPER_4002", *_STILL_REFUSED_LIVE_VENUES):
        _assert_paper_only_refusal(lambda vp=wrong_venue: _live_lane(tmp_path, venue_profile=vp))
    for not_a_live_client in (
        SimpleNamespace(),
        SimpleNamespace(_sandbox=False),
        pf._K4MockPaperAlpacaClient(account=_LIVE_ACCOUNT),
    ):
        _assert_paper_only_refusal(lambda c=not_a_live_client: _live_lane(tmp_path, c))
    for not_a_live_account in (*pf.ALPACA_LANE_PROFILES, "ALPACA_LIVE2", pf.IBKR_PAPER_ACCOUNT):
        for client in (_live_sim(), paper_sim):
            _assert_paper_only_refusal(
                lambda a=not_a_live_account, c=client: pf.AlpacaPaperLane(
                    a,
                    tmp_path,
                    c,
                    pf.CrossLaneSymbolLock(),
                    paper_only=False,
                    venue_profile=LIVE_TIERED_VENUE_ID,
                )
            )
    with pytest.raises(ValueError, match=r"requested_rpm=1001 outside \(0, 1000\]"):
        _live_lane(tmp_path, rate_budget=risk.ProtectiveExitRateBudget(1001))
    assert (
        _live_lane(tmp_path, rate_budget=risk.ProtectiveExitRateBudget(600)).rate_budget.max_rpm
        == 600
    )

    # The paper path is today's: a live lane account is no paper lane, and a paper lane refuses
    # a live client, the tier-3 flag and the tiered venue.
    for client in (paper_sim, _live_sim()):
        with pytest.raises(ValueError, match="Unsupported Alpaca Paper lane"):
            pf.AlpacaPaperLane(_LIVE_ACCOUNT, tmp_path, client, pf.CrossLaneSymbolLock())
    with pytest.raises(PermissionError, match="non-paper trading client"):
        _paper_lane(tmp_path, _live_sim())
    _assert_paper_only_refusal(lambda: _paper_lane(tmp_path, live_execution_enabled=True))
    with pytest.raises(PermissionError, match="live_execution_enabled=False"):
        _paper_lane(tmp_path, venue_profile=LIVE_TIERED_VENUE_ID)

    # The live client check is positive: alpaca-py's own live endpoint, no override, a live key
    # id. A refusal never quotes a credential.
    pf._assert_live_trading_client(sim)
    pf._assert_live_trading_client(TradingClient(_FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE, paper=False))
    live_url = str(BaseURL.TRADING_LIVE.value)
    live_shape: dict[str, Any] = {"_sandbox": False, "_base_url": BaseURL.TRADING_LIVE}
    for refused in (
        paper_sim,
        TradingClient(_FAKE_PAPER_KEY_ID, _FAKE_KEY_VALUE, paper=True),
        TradingClient(_FAKE_PAPER_KEY_ID, _FAKE_KEY_VALUE, paper=False),
        TradingClient(_FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE, paper=True, url_override=live_url),
        TradingClient(
            _FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE, paper=False, url_override="http://127.0.0.1:9"
        ),
        SimpleNamespace(**live_shape),
        SimpleNamespace(**live_shape, _api_key=_FAKE_LIVE_KEY_ID, paper=True),
        SimpleNamespace(**live_shape, _api_key=_FAKE_LIVE_KEY_ID, url_override="http://x.test"),
        SimpleNamespace(_sandbox=False, _api_key=_FAKE_LIVE_KEY_ID),
    ):
        with pytest.raises(PermissionError) as refusal:
            pf._assert_live_trading_client(refused)
        assert _FAKE_LIVE_KEY_ID not in str(refusal.value)
        assert _FAKE_PAPER_KEY_ID not in str(refusal.value)
        assert _FAKE_KEY_VALUE not in str(refusal.value)

    # assert_integrity re-runs the discriminator: a lane that is no longer what it was built as
    # is refused on every path, the protective ones included, before any broker call.
    for attr, tampered in (
        ("paper_only", True),
        ("venue_profile", "ALPACA_PAPER_1000"),
        ("trading_client", paper_sim),
    ):
        with monkeypatch.context() as patched:
            patched.setattr(lane, attr, tampered)
            _assert_paper_only_refusal(lane.assert_integrity)
            _assert_paper_only_refusal(lambda: lane.broker_cancel("no-such-order"))
            _assert_paper_only_refusal(lane.broker_flatten)
            assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") == "HOLD_LANE_INTEGRITY"
        lane.assert_integrity()
    paper_lane = _paper_lane(tmp_path / "paper", paper_sim)
    assert paper_lane.environment == "PAPER" and paper_lane.equity_verified_at is None
    paper_lane.assert_integrity()
    for attr, tampered in (("paper_only", False), ("live_execution_enabled", True)):
        with monkeypatch.context() as patched:
            patched.setattr(paper_lane, attr, tampered)
            _assert_paper_only_refusal(paper_lane.assert_integrity)
            _assert_paper_only_refusal(paper_lane.sync_broker_fills_and_status)
            _assert_paper_only_refusal(
                lambda: paper_lane.submit_guarded_entry(
                    symbol="NVDA", side="LONG", ref_price=Decimal("100.00")
                )
            )
        paper_lane.assert_integrity()
    assert sim.call_log == [] and paper_sim.call_log == []

    # Entry-side state is not lane integrity. An unverified equity, a sizing profile beyond the
    # risk defaults or a tier-3 flag that is not a real bool each hold every live entry, and
    # none of them stops a protective order from reaching the broker.
    sim.open_position("NVDA", 10, "100.00")
    not_timestamps: tuple[Any, ...] = (None, True, 0.0, -1.0, float("nan"), float("inf"), "17e8")
    for not_a_timestamp in not_timestamps:
        lane.equity_verified_at = not_a_timestamp
        held = lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY")
        assert held == "HOLD_LIVE_EQUITY_UNVERIFIED"
    lane.equity_verified_at = 1.0
    assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") is None
    with monkeypatch.context() as patched:
        patched.setitem(profile, "max_notional_pct", Decimal("0.75"))
        held = lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY")
        assert held == "HOLD_LIVE_PROFILE_OUTSIDE_RISK_DEFAULTS"
        lane.assert_integrity()
        stop = lane.broker_submit(_sell_stop("NVDA", 10, 98.0), intent="PROTECTIVE")
    with monkeypatch.context() as patched:
        patched.setattr(lane, "live_execution_enabled", "true")
        for entry_intent in _ENTRY_INTENTS:
            assert lane.entry_hold_reason(entry_intent, leg_merit=True) == "HOLD_TIER_FLAG_NOT_BOOL"
        assert lane.write_status()["live_execution_enabled"] is False
        lane.assert_integrity()
        lane.broker_cancel(stop.id)
    assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") is None
    assert [call.name for call in sim.call_log] == ["submit_order", "cancel_order_by_id"]


def test_live_credentials_resolve_from_env_names_and_one_client_build_site(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_name, value_name = _LIVE_CREDENTIAL_NAMES
    good_env = {key_name: f" {_FAKE_LIVE_KEY_ID} ", value_name: f"{_FAKE_KEY_VALUE}\n"}
    assert pf.resolve_alpaca_live_env_creds(good_env) == (_FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE)

    # Fail closed, naming variables only: no value of the mapping ever reaches the exception.
    bad_envs: tuple[dict[str, str], ...] = (
        {},
        {key_name: _FAKE_LIVE_KEY_ID},
        {value_name: _FAKE_KEY_VALUE},
        {key_name: "", value_name: _FAKE_KEY_VALUE},
        {key_name: _FAKE_PAPER_KEY_ID, value_name: _FAKE_KEY_VALUE},
        {key_name: _FAKE_LIVE_KEY_ID.lower(), value_name: _FAKE_KEY_VALUE},
        {"ALPACA_PA1_API_KEY": _FAKE_LIVE_KEY_ID, "ALPACA_PA1_SECRET_KEY": _FAKE_KEY_VALUE},
    )
    for bad_env in bad_envs:
        with pytest.raises(PermissionError, match="Refused live credential") as refusal:
            pf.resolve_alpaca_live_env_creds(bad_env)
        text = str(refusal.value)
        assert key_name in text
        assert not any(value and value in text for value in bad_env.values())
        assert refusal.value.__cause__ is None and refusal.value.__context__ is None
    monkeypatch.delenv(key_name, raising=False)
    monkeypatch.delenv(value_name, raising=False)
    with pytest.raises(PermissionError, match="must both be set"):
        pf.resolve_alpaca_live_env_creds()

    # The one build site: trading, market data and both stream kinds from the one live
    # credential, on alpaca-py's live endpoints. Nothing connects (no_live_broker_access).
    clients = pf.build_alpaca_live_clients(_FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE)
    assert type(clients.trading) is TradingClient
    pf._assert_live_trading_client(clients.trading)
    assert clients.trading._sandbox is False
    assert clients.trading._base_url is BaseURL.TRADING_LIVE
    assert clients.trade_updates._endpoint is BaseURL.TRADING_STREAM_LIVE
    assert set(clients.market_streams) == {DataFeed.SIP, DataFeed.BOATS}
    for built in (
        clients.trading,
        clients.data,
        clients.trade_updates,
        *clients.market_streams.values(),
    ):
        assert built._api_key == _FAKE_LIVE_KEY_ID and built._secret_key == _FAKE_KEY_VALUE
    assert _FAKE_LIVE_KEY_ID not in repr(clients) and _FAKE_KEY_VALUE not in repr(clients)
    for bad_pair in (
        (_FAKE_PAPER_KEY_ID, _FAKE_KEY_VALUE),
        ("", _FAKE_KEY_VALUE),
        (_FAKE_LIVE_KEY_ID, ""),
    ):
        with pytest.raises(PermissionError, match="Refused live client build") as refusal:
            pf.build_alpaca_live_clients(*bad_pair)
        assert not any(value and value in str(refusal.value) for value in bad_pair)

    # In this module a non-paper client is built in that one function only, and every broker
    # mutation sits inside the lane's four choke-point methods (receiver ``self`` excepted: a
    # mock client or a NautilusTrader strategy calling its own method).
    tree = ast.parse(Path(inspect.getfile(pf)).read_text("utf-8"))
    live_build_sites: set[str] = set()
    mutation_sites: set[tuple[str, str]] = set()

    def visit(node: ast.AST, owner: str, function: str) -> None:
        if isinstance(node, ast.ClassDef):
            owner, function = node.name, ""
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            function = function or node.name
        elif isinstance(node, ast.Call):
            for keyword in node.keywords:
                literal = keyword.value
                if keyword.arg == "paper" and not (
                    isinstance(literal, ast.Constant) and literal.value is True
                ):
                    live_build_sites.add(function)
            target = node.func
            if isinstance(target, ast.Attribute) and target.attr in _BROKER_MUTATIONS:
                receiver = target.value
                if not (isinstance(receiver, ast.Name) and receiver.id == "self"):
                    mutation_sites.add((owner, function))
        for child in ast.iter_child_nodes(node):
            visit(child, owner, function)

    visit(tree, "", "")
    assert live_build_sites == {"build_alpaca_live_clients"}
    assert mutation_sites == {("AlpacaPaperLane", name) for name in _CHOKE_POINT_METHODS}


def test_coordinators_run_exactly_one_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in (*_LIVE_CREDENTIAL_NAMES, "APCA_API_KEY_ID", "APCA_API_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    for acct in pf.ALPACA_LANE_PROFILES:
        for suffix in ("API_KEY", "SECRET", "SECRET_KEY"):
            monkeypatch.delenv(f"{acct}_{suffix}", raising=False)

    # Live: one lane on the tiered venue, no IBKR adapter, the venue's own 900 + 100 budget.
    live_farm = pf.AlpacaLiveVenueCoordinator(tmp_path / "live", trading_client=_live_sim())
    assert isinstance(live_farm, pf.PaperFarmCoordinator)
    assert live_farm.environment == "LIVE" and live_farm.autonomous_opt_in is False
    assert live_farm.required_accounts == (_LIVE_ACCOUNT,) == tuple(live_farm.lanes)
    assert not hasattr(live_farm, "ibkr") and live_farm.trade_update_streams() == {}
    live_lane = live_farm.lanes[_LIVE_ACCOUNT]
    assert live_lane.environment == "LIVE" and live_lane.symbol_lock is live_farm.symbol_lock
    assert live_lane.paper_only is False and live_lane.live_execution_enabled is False
    assert (live_lane.rate_budget.max_rpm, live_lane.rate_budget.exit_reserve_rpm) == (1000, 100)
    handles = live_farm.venue_order_handles(session="AFTER_HOURS")
    assert handles["paper_only"] is False and handles["live_execution_enabled"] is False
    assert handles["alpaca_venue_profile"] == LIVE_TIERED_VENUE_ID
    assert list(handles["venues"]) == [LIVE_TIERED_VENUE_ID]
    assert list(handles["per_account"]) == [_LIVE_ACCOUNT]
    assert handles["combined_max_rpm"] == 1000 and handles["combined_exit_reserve_rpm"] == 100
    assert handles["combined_entry_modify_rpm_budget"] == 900
    assert json.loads(json.dumps(handles)) == handles
    with pytest.raises(PermissionError, match="paper-only"):
        live_farm.dispatch_high_throughput([], macro_regime=1, session="RTH")

    # A scanner-originated dispatch on live without opt-in and merit is a quiet no-op.
    movers = [{"symbol": "NVDA", "last_price": 100.0, "natr_pct": 0.02}]
    assert live_farm.dispatch_movers(movers, macro_regime=1, session="RTH") == []
    assert live_lane.counters["entry_holds"] == 1
    assert live_lane.trading_client.call_log == [] and live_farm.symbol_lock.snapshot() == {}

    # The tier-3 opt-in travels coordinator -> lane -> stamps, and the injected clients are kept.
    clients = pf.build_alpaca_live_clients(_FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE)
    opted_in = pf.AlpacaLiveVenueCoordinator(
        tmp_path / "live-opt-in",
        trading_client=_live_sim(),
        data_client=clients.data,
        autonomous_opt_in=True,
        market_streams=clients.market_streams,
        trade_updates_stream=clients.trade_updates,
    )
    assert opted_in.autonomous_opt_in is True
    assert opted_in.lanes[_LIVE_ACCOUNT].live_execution_enabled is True
    assert opted_in.venue_order_handles()["live_execution_enabled"] is True
    assert opted_in.trade_update_streams() == {_LIVE_ACCOUNT: clients.trade_updates}
    assert opted_in.market_streams == dict(clients.market_streams)
    assert opted_in.data_client is clients.data
    not_a_bool: Any = "true"
    for build in (
        lambda: pf.AlpacaLiveVenueCoordinator(
            tmp_path / "x", trading_client=FaultInjectingAlpacaSim()
        ),
        lambda: pf.AlpacaLiveVenueCoordinator(
            tmp_path / "x", trading_client=_live_sim(), autonomous_opt_in=not_a_bool
        ),
    ):
        _assert_paper_only_refusal(build)

    # Paper: as before, plus the environment and required-account attributes.
    paper_clients = {acct: FaultInjectingAlpacaSim() for acct in pf.ALPACA_LANE_PROFILES}
    paper_farm = pf.PaperFarmCoordinator(tmp_path / "farm", alpaca_clients=paper_clients)
    assert paper_farm.environment == "PAPER" and paper_farm.autonomous_opt_in is False
    assert paper_farm.required_accounts == tuple(pf.ALPACA_LANE_PROFILES) == tuple(paper_farm.lanes)
    assert paper_farm.ibkr.connection_spec["port"] == 4002
    assert all(lane.environment == "PAPER" for lane in paper_farm.lanes.values())
    assert paper_farm.trade_update_streams() == {}  # injected clients, no env credentials
    with pytest.raises(PermissionError, match="non-paper trading client"):
        pf.PaperFarmCoordinator(tmp_path / "farm", alpaca_clients={"ALPACA_PA1": _live_sim()})
    monkeypatch.setenv("ALPACA_PA2_API_KEY", _FAKE_PAPER_KEY_ID)
    monkeypatch.setenv("ALPACA_PA2_SECRET_KEY", _FAKE_KEY_VALUE)
    with_env = pf.PaperFarmCoordinator(tmp_path / "farm", alpaca_clients=paper_clients)
    streams = with_env.trade_update_streams()
    assert list(streams) == ["ALPACA_PA2"]
    assert streams["ALPACA_PA2"]._endpoint is BaseURL.TRADING_STREAM_PAPER
    assert with_env.trade_update_streams()["ALPACA_PA2"] is streams["ALPACA_PA2"]  # built once

    # One environment per coordinator: a foreign lane is refused, whichever way it is mixed.
    paper_farm.assert_single_environment()
    live_farm.assert_single_environment()
    with monkeypatch.context() as patched:
        patched.setitem(paper_farm.lanes, _LIVE_ACCOUNT, live_lane)
        with pytest.raises(PermissionError, match=r"coordinator \(PAPER\): ALPACA_LIVE1"):
            paper_farm.assert_single_environment()
    with monkeypatch.context() as patched:
        patched.setitem(live_farm.lanes, "ALPACA_PA1", paper_farm.lanes["ALPACA_PA1"])
        with pytest.raises(PermissionError, match=r"coordinator \(LIVE\): ALPACA_PA1"):
            live_farm.assert_single_environment()

    # The live manifest directory is separate: a directory holding a paper ledger is refused.
    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path / "data-root"))
    assert pf.default_manifest_dir() == tmp_path / "data-root" / "manifests"
    assert pf.default_live_manifest_dir() == tmp_path / "data-root" / "live" / "manifests"
    paper_ledger = paper_farm.lanes["ALPACA_PA3"].duckdb_path
    paper_ledger.parent.mkdir(parents=True, exist_ok=True)
    paper_ledger.touch()
    with pytest.raises(PermissionError, match="paper manifest directory"):
        pf.AlpacaLiveVenueCoordinator(tmp_path / "farm", trading_client=_live_sim())

    # build_live_venue_coordinator: env credential -> the one client build site -> coordinator.
    with pytest.raises(PermissionError, match="must both be set"):
        pf.build_live_venue_coordinator(tmp_path / "built", autonomous_opt_in=False)
    monkeypatch.setattr(
        pf, "resolve_alpaca_live_env_creds", lambda: (_FAKE_LIVE_KEY_ID, _FAKE_KEY_VALUE)
    )
    built = pf.build_live_venue_coordinator(tmp_path / "built", autonomous_opt_in=False)
    built_lane = built.lanes[_LIVE_ACCOUNT]
    assert type(built) is pf.AlpacaLiveVenueCoordinator and built.autonomous_opt_in is False
    assert type(built_lane.trading_client) is TradingClient
    assert built_lane.trading_client._base_url is BaseURL.TRADING_LIVE
    assert built_lane.live_execution_enabled is False and built_lane.environment == "LIVE"
    assert set(built.trade_update_streams()) == {_LIVE_ACCOUNT}
    assert set(built.market_streams) == {DataFeed.SIP, DataFeed.BOATS}
    assert built.data_client is not None


def test_entry_ledger_gains_tier_columns_and_entry_parameters(tmp_path: Path) -> None:
    # A ledger written before the tier model is upgraded in place (idempotently) and its rows
    # read back with no authority: a legacy row is never an owner lineage.
    legacy = tmp_path / "legacy.duckdb"
    with duckdb.connect(str(legacy)) as db:
        db.execute(
            "CREATE TABLE paper_entry_decisions ("
            "client_order_id VARCHAR PRIMARY KEY, symbol VARCHAR, side VARCHAR, "
            "decision_price DECIMAL(18,6), arrival_price DECIMAL(18,6), session VARCHAR, "
            "origin VARCHAR, annual_rate DECIMAL(18,10), prior_close DECIMAL(18,6), "
            "initial_stop DECIMAL(18,6), natr_pct DECIMAL(18,8), closed_at TIMESTAMPTZ, "
            "active_stop DECIMAL(18,6), exit_triggered BOOLEAN DEFAULT false, "
            "decided_at TIMESTAMPTZ DEFAULT current_timestamp)"
        )
        db.execute(
            "INSERT INTO paper_entry_decisions (client_order_id, symbol, side) "
            "VALUES ('legacy-1', 'NVDA', 'LONG')"
        )
        before = [row[0] for row in db.execute("DESCRIBE paper_entry_decisions").fetchall()]
        pf._ensure_entry_decisions_table(db)
        pf._ensure_entry_decisions_table(db)
        described = db.execute("DESCRIBE paper_entry_decisions").fetchall()
        columns = {row[0]: row[1] for row in described}
        assert [row[0] for row in described][: len(before)] == before
        assert {name: columns[name] for name in _TIER_LEDGER_COLUMNS} == _TIER_LEDGER_COLUMNS
        assert db.execute(_TIER_LEDGER_SELECT, ["legacy-1"]).fetchone() == (
            None,
            None,
            None,
            0,
            None,
            None,
            None,
            None,
        )

    # Paper lane: an entry without the new parameters goes out exactly as before and its row
    # carries the default authority, no lineage and the submitted quantity.
    sim = FaultInjectingAlpacaSim()
    sim.set_price("NVDA", "100.00")
    sim.set_price("AMD", "50.00")
    lane = _paper_lane(tmp_path, sim)
    plain = lane.submit_guarded_entry(
        symbol="NVDA", side="LONG", ref_price=Decimal("100.00"), client_order_id="noesis-plain"
    )
    assert plain is not None and plain["protective_stop_mode"] == "NATIVE_OTO_STOP"
    (plain_call,) = sim.calls("submit_order")
    assert plain_call.payload["order_class"] == "oto"
    assert plain_call.payload["client_order_id"] == "noesis-plain"
    assert _tier_ledger_row(lane, "noesis-plain") == (
        "AUTONOMOUS",
        None,
        None,
        0,
        Decimal(plain["qty"]),
        None,
        None,
        None,
    )
    assert "entry_holds" not in lane.counters  # a paper lane never holds an entry by tier

    # Lineage parameters and the quantity cap are recorded; paper ignores the intent itself.
    with lane.batch_ledger_conn():
        child = lane.submit_guarded_entry(
            symbol="AMD",
            side="LONG",
            ref_price=Decimal("50.00"),
            client_order_id="noesis-child",
            origin="TREND_ALIGNED_LONG_REENTRY",
            entry_authority="OWNER_LINEAGE",
            leg_merit=True,
            max_qty=3,
            lineage_root_id="adopted-root",
            lineage_parent_id="noesis-parent",
            lineage_depth=2,
            macro_regime=-1,
        )
    assert child is not None and child["qty"] == 3 < plain["qty"]
    assert _tier_ledger_row(lane, "noesis-child") == (
        "OWNER_LINEAGE",
        "adopted-root",
        "noesis-parent",
        2,
        Decimal(3),
        None,
        -1,
        None,
    )

    # An authority outside the two is recorded as, and treated as, the most restrictive one.
    odd_authority: Any = "OPERATOR_CONFIRMED"
    odd = lane.submit_guarded_entry(
        symbol="TSLA",
        side="LONG",
        ref_price=Decimal("200.00"),
        client_order_id="noesis-odd",
        entry_authority=odd_authority,
    )
    assert odd is not None
    odd_row = _tier_ledger_row(lane, "noesis-odd")
    assert odd_row is not None and odd_row[0] == "AUTONOMOUS"

    # max_qty below one share is a quiet HOLD before the lock, the budget and the ledger; a
    # non-integer cap is a caller bug and raises.
    calls, slots = len(sim.call_log), _budget_slots_used(lane)
    for capped in (0, -5):
        held = lane.submit_guarded_entry(
            symbol="MSFT",
            side="LONG",
            ref_price=Decimal("300.00"),
            client_order_id=f"noesis-capped-{capped}",
            max_qty=capped,
        )
        assert held is None
    not_share_counts: tuple[Any, ...] = (True, 2.5, "3", Decimal("3"))
    for not_a_share_count in not_share_counts:
        with pytest.raises(ValueError, match="max_qty must be an integer share count"):
            lane.submit_guarded_entry(
                symbol="MSFT",
                side="LONG",
                ref_price=Decimal("300.00"),
                max_qty=not_a_share_count,
            )
    assert len(sim.call_log) == calls and _budget_slots_used(lane) == slots
    assert "MSFT" not in lane.symbol_lock.snapshot()
    assert _tier_ledger_row(lane, "noesis-capped-0") is None


def test_k4_flatten_runs_through_the_lane_choke_point(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The Stage-4 K4 drill flattens every Alpaca lane through broker_flatten and still passes.
    flattened: list[str] = []
    real_flatten = pf.AlpacaPaperLane.broker_flatten

    def counting_flatten(self: pf.AlpacaPaperLane, **kwargs: Any) -> tuple[list[Any], list[Any]]:
        flattened.append(self.account)
        return real_flatten(self, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(pf.AlpacaPaperLane, "broker_flatten", counting_flatten)
        receipt = pf.run_k4_broker_flatten_drill(work_dir=tmp_path / "k4")
    assert receipt["passed"] is True and flattened == list(pf.ALPACA_LANE_PROFILES)
    assert receipt["remaining_orders"] == 0 and receipt["remaining_positions"] == 0
    for acct in pf.ALPACA_LANE_PROFILES:
        assert receipt["per_account"][acct]["cancel_orders_called"] is True
        assert receipt["per_account"][acct]["close_all_positions_cancel_orders_true"] is True

    # On a lane: cancel every order, then close every position, as PROTECTIVE by default.
    sim = FaultInjectingAlpacaSim()
    sim.open_position("NVDA", 10, "100.00")
    resting = sim.submit_order(_sell_stop("NVDA", 10, 98.0))
    sim.reset_call_log()
    lane = _paper_lane(tmp_path, sim)
    canceled, closed = lane.broker_flatten()
    assert [str(item.id) for item in canceled] == [str(resting.id)]
    assert [item.symbol for item in closed] == ["NVDA"]
    assert [call.name for call in sim.call_log] == ["cancel_orders", "close_all_positions"]
    assert sim.call_log[1].payload == {"cancel_orders": True}
    assert sim.inspect_position("NVDA") is None and sim.inspect_orders("open") == []

    # A failed cancel does not stop the close; the cancel error still reaches the caller.
    sim.open_position("AMD", 5, "50.00")
    sim.reset_call_log()
    sim.fail_next("cancel_orders", 500)
    with pytest.raises(APIError):
        lane.broker_flatten()
    assert [call.name for call in sim.call_log] == ["cancel_orders", "close_all_positions"]
    assert sim.inspect_position("AMD") is None

    # A flatten under a non-mutating intent is refused before any broker call.
    sim.reset_call_log()
    for not_mutating in ("READ_ONLY", "NOT_AN_INTENT", None):
        bad_intent: Any = not_mutating
        with pytest.raises(PermissionError, match="is not a mutating intent"):
            lane.broker_flatten(intent=bad_intent)
    assert sim.call_log == []


@_PROPERTY
@example(  # everything on the entry side is against it, and the broker refuses new orders
    opt_in=False,
    leg_merit=False,
    equity_verified=False,
    lineage_enabled=False,
    breaker_tripped=True,
    entry_budget_saturated=True,
    account_blocked=True,
    sim_session="CLOSED",
    order=["supervised_exit", "submit", "replace", "cancel"],
)
@example(  # the calm case: every protective order is accepted
    opt_in=True,
    leg_merit=True,
    equity_verified=True,
    lineage_enabled=True,
    breaker_tripped=False,
    entry_budget_saturated=False,
    account_blocked=False,
    sim_session="RTH",
    order=["replace", "cancel", "submit", "supervised_exit"],
)
@given(
    opt_in=st.booleans(),
    leg_merit=st.booleans(),
    equity_verified=st.booleans(),
    lineage_enabled=st.booleans(),
    breaker_tripped=st.booleans(),
    entry_budget_saturated=st.booleans(),
    account_blocked=st.booleans(),
    sim_session=st.sampled_from(SIM_SESSIONS),
    order=st.permutations(("replace", "cancel", "submit", "supervised_exit")),
)
def test_property_protective_intents_always_reach_broker(
    opt_in: bool,
    leg_merit: bool,
    equity_verified: bool,
    lineage_enabled: bool,
    breaker_tripped: bool,
    entry_budget_saturated: bool,
    account_blocked: bool,
    sim_session: Any,
    order: list[str],
) -> None:
    """A valid live lane never pre-empts a protective order, whatever the entry-side state.

    "Reaches the broker" means one mutating round trip is logged by the simulator. The broker
    may still answer with an error (blocked account, shares already held by a resting stop, an
    order that is gone): that is an ``APIError`` from the broker, never a ``PermissionError``.
    """
    now_s = 1_000_000.0
    with (
        tempfile.TemporaryDirectory(prefix="tier0-live-protective-") as raw_dir,
        mock.patch.object(pf, "LIVE_LINEAGE_ENTRIES_ENABLED", lineage_enabled),
    ):
        sim = _live_sim(session=sim_session)
        lane = _live_lane(Path(raw_dir), sim, live_execution_enabled=opt_in)
        # Positions the owner opened by hand; one already has a stop resting at the broker.
        sim.open_position("NVDA", 10, "100.00")
        owner_fill = sim.open_position("AMD", 5, "100.00")
        resting = sim.submit_order(_sell_stop("NVDA", 10, 98.0))
        sim.reset_call_log()
        parent_id = str(owner_fill.id)
        lane.supervised_stops[parent_id] = pf.ExtendedHoursStopSupervision(
            ts_utc=sim.now.isoformat(),
            account=_LIVE_ACCOUNT,
            lane_id=lane.profile["lane_id"],
            broker_order_id=parent_id,
            symbol="AMD",
            side="LONG",
            qty=5,
            session="AFTER_HOURS",
            protective_stop_mode="COLLARED_STOP_LIMIT_SUPERVISED",
            ref_price=Decimal("100.00"),
            stop_price=Decimal("98.00"),
            stop_limit_price=Decimal("97.50"),
            collar_bps=Decimal("50"),
            max_loss_pct=Decimal("0.02"),
        )

        if equity_verified:
            lane.equity_verified_at = now_s
        if breaker_tripped:
            lane.breaker.tripped = True
        if entry_budget_saturated:
            _saturate_entry_budget(lane, now_s)
        if account_blocked:
            sim.set_account(trading_blocked=True)
        slots_before = _budget_slots_used(lane)
        stop_id = [resting.id]

        def replace() -> None:
            stop_id[0] = lane.broker_replace(stop_id[0], ReplaceOrderRequest(stop_price=99.0)).id

        def cancel() -> None:
            lane.broker_cancel(stop_id[0], intent="PROTECTIVE")

        def submit() -> None:
            lane.broker_submit(
                _sell_stop("NVDA", 10, 97.0), intent="PROTECTIVE", leg_merit=leg_merit
            )

        def supervised_exit() -> None:
            lane.trigger_supervised_stop(parent_id, trigger_px=Decimal("97.00"), now_s=now_s)

        steps: dict[str, tuple[str, Callable[[], None]]] = {
            "replace": ("replace_order_by_id", replace),
            "cancel": ("cancel_order_by_id", cancel),
            "submit": ("submit_order", submit),
            "supervised_exit": ("submit_order", supervised_exit),
        }
        for step in order:
            call_name, action = steps[step]
            reached = len(sim.calls(call_name, kind=MUTATING))
            with contextlib.suppress(APIError):
                action()
            assert len(sim.calls(call_name, kind=MUTATING)) == reached + 1, step

        with contextlib.suppress(APIError):
            lane.broker_flatten()
        mutating = [call.name for call in sim.calls(kind=MUTATING)]
        assert mutating[-2:] == ["cancel_orders", "close_all_positions"]
        assert sorted(mutating[:-2]) == sorted(name for name, _ in steps.values())
        answers = {call.outcome for call in sim.calls(kind=MUTATING)}
        assert account_blocked is False or "error:403" in answers
        event("broker accepted all" if answers == {"ok"} else "broker answered with an error")
        # The choke point itself spends no rate budget; the one slot is the supervised exit's,
        # taken from the exit reserve that entry traffic cannot touch.
        assert _budget_slots_used(lane) == slots_before + 1
        assert lane.symbol_lock.snapshot() == {}


def _entry_case(**overrides: Any) -> dict[str, Any]:
    """One row of the tier table for ``@example``: equity verified, every tier flag off."""
    case: dict[str, Any] = {
        "opt_in": False,
        "leg_merit": False,
        "equity_verified": True,
        "lineage_enabled": False,
        "authority": "AUTONOMOUS",
        "direct_intent": "AUTONOMOUS_ENTRY",
        "session": "RTH",
        "side": "LONG",
        "breaker_tripped": False,
        "entry_budget_saturated": False,
        "poisoned_arguments": False,
        # Supervisor half: driven, the scanner candidate is new for the symbol, and it is held
        # where the candidates are built.
        "supervisor_driven": True,
        "supervisor_candidate": "FIRST",
        "supervisor_grant_at_build": False,
    }
    return case | overrides


_LINEAGE = {"authority": "OWNER_LINEAGE", "direct_intent": "OWNER_LINEAGE_ENTRY"}
# Weighted so that enough generated cases are granted and get past the pre-existing gates.
_MOSTLY_TRUE = st.sampled_from((True, True, True, False))
_MOSTLY_FALSE = st.sampled_from((False, False, False, True))


@settings(_PROPERTY, max_examples=250)
# Tier 3 needs the opt-in and the merit and a verified equity: all three, or nothing is sent.
@example(**_entry_case(opt_in=True, leg_merit=True))
@example(**_entry_case(opt_in=True, leg_merit=True, session="AFTER_HOURS", side="SHORT"))
@example(**_entry_case(opt_in=True, poisoned_arguments=True))
@example(**_entry_case(leg_merit=True, poisoned_arguments=True))
@example(**_entry_case(opt_in=True, leg_merit=True, equity_verified=False))
# Owner lineage: held while LIVE_LINEAGE_ENTRIES_ENABLED is False (the shipped state), whatever
# else is granted; sent once the constant is flipped.
@example(**_entry_case(opt_in=True, leg_merit=True, poisoned_arguments=True, **_LINEAGE))
@example(**_entry_case(lineage_enabled=True, **_LINEAGE))
@example(**_entry_case(lineage_enabled=True, equity_verified=False, **_LINEAGE))
# Tier 2 at the choke point; an authority outside the two is the most restrictive one.
@example(**_entry_case(authority="OPERATOR_CONFIRMED", direct_intent="OPERATOR_CONFIRMED_ENTRY"))
@example(**_entry_case(authority=None, direct_intent=None, equity_verified=False))
# Supervisor: with everything on the entry side granted except the leg merit, a candidate that
# gets past the candidate build is still held just before the submit, in every candidate kind.
@example(**_entry_case(opt_in=True, lineage_enabled=True, supervisor_grant_at_build=True))
@example(
    **_entry_case(
        opt_in=True,
        lineage_enabled=True,
        supervisor_candidate="REVERSE",
        supervisor_grant_at_build=True,
    )
)
@example(
    **_entry_case(
        opt_in=True,
        lineage_enabled=True,
        session="OVERNIGHT",
        side="SHORT",
        supervisor_candidate="REENTRY",
        supervisor_grant_at_build=True,
    )
)
@given(
    opt_in=st.booleans(),
    leg_merit=st.booleans(),
    equity_verified=_MOSTLY_TRUE,
    lineage_enabled=st.booleans(),
    authority=st.sampled_from(
        ("AUTONOMOUS", "OWNER_LINEAGE", "OPERATOR_CONFIRMED", "PROTECTIVE", "owner_lineage", None)
    ),
    direct_intent=st.sampled_from((*_INTENTS, "NOT_AN_INTENT", None)),
    session=st.sampled_from(_LANE_SESSIONS),
    side=st.sampled_from(("LONG", "SHORT")),
    breaker_tripped=_MOSTLY_FALSE,
    entry_budget_saturated=_MOSTLY_FALSE,
    poisoned_arguments=st.booleans(),
    # A supervisor drive is a whole run_execution (about a quarter of a second of DuckDB opens),
    # so the generated cases drive it for one case in four; every explicit example drives it.
    supervisor_driven=_MOSTLY_FALSE,
    supervisor_candidate=st.sampled_from(("FIRST", "REENTRY", "REVERSE")),
    supervisor_grant_at_build=st.booleans(),
)
def test_property_no_unauthorised_entry_reaches_submit_order(
    opt_in: bool,
    leg_merit: bool,
    equity_verified: bool,
    lineage_enabled: bool,
    authority: Any,
    direct_intent: Any,
    session: Any,
    side: Any,
    breaker_tripped: bool,
    entry_budget_saturated: bool,
    poisoned_arguments: bool,
    supervisor_driven: bool,
    supervisor_candidate: Any,
    supervisor_grant_at_build: bool,
) -> None:
    """Lane level: an entry reaches ``submit_order`` only when its tier grants it.

    A refused entry makes no broker call (not even a read), claims no symbol lock, consumes no
    rate budget, writes no ledger row and never raises, even with arguments that would raise on
    the permitted path. The choke point refuses the same entry loudly.

    Supervisor level: ``run_execution`` with a scanner candidate on the live lane sends no order
    and latches no entry fault, whichever of its two hold sites the candidate reaches.
    """
    now_s = 2_000_000.0
    intent = "OWNER_LINEAGE_ENTRY" if authority == "OWNER_LINEAGE" else "AUTONOMOUS_ENTRY"

    def tier_grants(entry_intent: Any) -> bool:
        # The tier table of the spec, restated independently of the lane.
        if not equity_verified:
            return False
        if entry_intent == "AUTONOMOUS_ENTRY":
            return opt_in and leg_merit
        if entry_intent == "OWNER_LINEAGE_ENTRY":
            return lineage_enabled
        return entry_intent == "OPERATOR_CONFIRMED_ENTRY"

    with (
        tempfile.TemporaryDirectory(prefix="tier0-live-entry-") as raw_dir,
        mock.patch.object(pf, "LIVE_LINEAGE_ENTRIES_ENABLED", lineage_enabled),
    ):
        sim = _live_sim(session=session)
        sim.set_price("NVDA", "100.00")
        lane = _live_lane(Path(raw_dir), sim, live_execution_enabled=opt_in)
        if equity_verified:
            lane.equity_verified_at = now_s
        if breaker_tripped:
            lane.breaker.tripped = True
        if entry_budget_saturated:
            _saturate_entry_budget(lane, now_s)
        slots_before = _budget_slots_used(lane)
        authorised = tier_grants(intent)

        # The lane is never more permissive than the engine's venue gate.
        for entry_intent in _ENTRY_INTENTS:
            if lane.entry_hold_reason(entry_intent, leg_merit=leg_merit) is None:
                assert tier_grants(entry_intent)
                validate_venue_order_capability(
                    LIVE_TIERED_VENUE_ID,
                    intent=entry_intent,
                    autonomous_opt_in=opt_in,
                    leg_merit=leg_merit,
                )
            else:
                assert not tier_grants(entry_intent)
        for not_an_entry in ("READ_ONLY", "PROTECTIVE", "NOT_AN_INTENT", None):
            not_an_entry_intent: Any = not_an_entry
            assert lane.entry_hold_reason(not_an_entry_intent, leg_merit=leg_merit) is not None

        entry: dict[str, Any] = {
            "symbol": "NVDA",
            "side": side,
            "ref_price": Decimal("100.00"),
            "session": session,
            "now_s": now_s,
            "client_order_id": "noesis-lane-property",
            "overnight_tradable": True,
            "entry_authority": authority,
            "leg_merit": leg_merit,
            "max_qty": 7,
        }
        if poisoned_arguments and not authorised:
            entry |= {"annual_borrow_rate": Decimal("-1"), "rth_entry_collar_pct": Decimal("1")}
        result = lane.submit_guarded_entry(**entry)
        event(
            f"entry {'granted' if authorised else 'refused'}, "
            f"{'sent' if result is not None else 'not sent'}"
        )

        if not authorised:
            assert result is None
            assert sim.call_log == []
            assert lane.symbol_lock.snapshot() == {}
            assert _budget_slots_used(lane) == slots_before
            assert not lane.duckdb_path.exists()
            assert lane.counters["entry_holds"] == 1 and lane.counters["orders_submitted"] == 0
        else:
            # A granted entry is still subject to the gates every entry has always had.
            gated = (
                breaker_tripped
                or entry_budget_saturated
                or (session == "OVERNIGHT" and side == "SHORT")
            )
            assert (result is None) is gated
            assert "entry_holds" not in lane.counters
            assert len(sim.calls(kind=MUTATING)) == len(sim.calls("submit_order")) == int(not gated)
            if result is not None:
                assert 1 <= result["qty"] <= 7
                assert lane.symbol_lock.snapshot() == {"NVDA": _LIVE_ACCOUNT}
                row = _tier_ledger_row(lane, "noesis-lane-property")
                assert row is not None
                assert row[0] == ("OWNER_LINEAGE" if authority == "OWNER_LINEAGE" else "AUTONOMOUS")
                assert row[4] == Decimal(result["qty"])

        # The choke point: PROTECTIVE always passes; an entry intent passes only by its tier;
        # anything else is refused. A refusal makes no broker call.
        request = LimitOrderRequest(
            symbol="AMD",
            qty=1,
            side=OrderSide.BUY,
            limit_price=10.0,
            time_in_force=TimeInForce.DAY,
        )
        calls_before = len(sim.call_log)
        if direct_intent == "PROTECTIVE" or (
            direct_intent in _ENTRY_INTENTS and tier_grants(direct_intent)
        ):
            lane.broker_submit(request, intent=direct_intent, leg_merit=leg_merit)
            assert len(sim.call_log) == calls_before + 1
            assert sim.call_log[-1].name == "submit_order"
        else:
            for refused_mutation in (
                lambda: lane.broker_submit(request, intent=direct_intent, leg_merit=leg_merit),
                lambda: lane.broker_replace("an-order-id", None, intent=direct_intent),
                lambda: lane.broker_cancel("an-order-id", intent=direct_intent),
                lambda: lane.broker_flatten(intent=direct_intent),
            ):
                with pytest.raises(PermissionError):
                    refused_mutation()
            assert len(sim.call_log) == calls_before

    # Supervisor level: the same tier state, driven through run_execution's ``handle`` with a
    # scanner candidate on a live coordinator backed by the simulator. Whatever the state, and
    # whether the hold falls at candidate build or just before the submit, no order reaches the
    # broker and no entry fault latches.
    if not supervisor_driven:
        return
    with tempfile.TemporaryDirectory(prefix="tier0-live-supervisor-") as raw_dir:
        run = _drive_live_supervisor(
            Path(raw_dir),
            opt_in=opt_in,
            session=session,
            side=side,
            equity_verified=equity_verified,
            lineage_enabled=lineage_enabled,
            breaker_tripped=breaker_tripped,
            candidate=supervisor_candidate,
            grant_at_candidate_build=supervisor_grant_at_build,
        )
        _assert_live_supervisor_held_quietly(run)
    event(f"supervisor hold: {run.hold_site}")
    if breaker_tripped:
        # No lane accepts entries, so no candidate exists and there is nothing to hold.
        assert run.hold_site == "no candidate" and run.status["entry_holds"] == {}
    elif not supervisor_grant_at_build:
        assert run.hold_site == "candidate build"
        assert run.status["entry_holds"] == {run.symbol: "HOLD_TIER_NOT_GRANTED"}
    else:
        assert run.hold_site == "before submit"
        assert run.status["entry_holds"] == {
            run.symbol: "HOLD_TIER_NOT_GRANTED"
            if supervisor_candidate == "FIRST"
            else "HOLD_LIVE_LINEAGE_UNRESOLVED"
        }


# ------------------------------------------------ supervisor (run_execution) on the live venue

_SCAN_MANIFEST = "live_whole_market_movers_manifest.json"
_TCA_MANIFEST = "tca_calibration_manifest.json"
_EXECUTION_STATUS = "paper_farm_execution_status.json"
# A published reconciliation that measured fills and found them inside the cost envelope.
_MEASURED_TCA_RECEIPT: dict[str, Any] = {
    "schema_version": "tier0.tca-calibration-manifest.v1",
    "paper_only": True,
    "live_execution_enabled": False,
    "duckdb_errors": [],
    "duckdb_tca_rows": 12,
    "parity_within_2x_envelope": True,
}
_REAL_CLASSIFY_SESSION = execution.engine.classify_us_equity_session_et
_WALL_CLOCK_WINDOW_NS = 86_400 * 1_000_000_000
_STILL_WATCHED = {"SPY", "QQQ"}


def _session_classifier(session: str) -> Callable[[int], str]:
    """``session`` for wall-clock instants (the supervisor), the real answer for any other.

    The simulator classifies its own virtual clock, years ahead of the wall clock, through the
    same engine function: a constant patch would move the simulator's sessions as well.
    """

    def classify(now_ns: int) -> str:
        if abs(now_ns - time.time_ns()) < _WALL_CLOCK_WINDOW_NS:
            return session
        return _REAL_CLASSIFY_SESSION(now_ns)

    return classify


def _dir_bytes(directory: Path) -> dict[str, bytes]:
    return {
        path.relative_to(directory).as_posix(): path.read_bytes()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


class _FreshQuotes:
    """Market-data client stand-in: a tight, fresh quote for whatever symbol is asked."""

    def get_stock_latest_quote(self, request: Any) -> dict[str, Any]:
        quote = SimpleNamespace(bid_price=100.0, ask_price=100.01, timestamp=datetime.now(UTC))
        return {request.symbol_or_symbols: quote}


class _ScriptedTradeStream:
    """One market-data stream of a ``_SupervisorDrive``."""

    def __init__(self, drive: _SupervisorDrive, feed: DataFeed) -> None:
        self.drive = drive
        self.feed = feed
        self.callback: Callable[[Any], Any] | None = None

    def subscribe_trades(self, callback: Callable[[Any], Any], *symbols: str) -> None:
        self.callback = callback
        self.drive.ever_subscribed.update(symbols)

    def unsubscribe_trades(self, *symbols: str) -> None:
        del symbols

    def run(self) -> None:
        drive = self.drive
        assert drive.candidates_built.wait(15), "the supervisor never completed a refresh"
        if self.feed is drive.feed:
            try:
                assert self.callback is not None
                for symbol in drive.prints:
                    trade = SimpleNamespace(symbol=symbol, price=100.0, timestamp=datetime.now(UTC))
                    asyncio.run(self.callback(trade))
            finally:
                drive.prints_delivered.set()
        assert drive.prints_delivered.wait(15), "the active feed never delivered its prints"

    def stop(self) -> None:
        self.drive.prints_delivered.set()


class _SupervisorDrive:
    """Drives ``run_execution`` deterministically: prints go out after the candidate build.

    It stands in for the two market-data streams and for the supervisor loop's ``wait``. The
    loop refreshes, and builds its candidates, between two ``wait`` calls, so the second call
    means one refresh is complete. Only then does the active feed deliver the scripted prints;
    when its ``run`` returns, the supervisor stops.
    """

    def __init__(self, feed: DataFeed, prints: tuple[str, ...]) -> None:
        self.feed = feed
        self.prints = prints
        self.loop_ticks = 0
        self.candidates_built = threading.Event()
        self.prints_delivered = threading.Event()
        self.ever_subscribed: set[str] = set()
        self.statuses: list[str] = []
        self.streams = {f: _ScriptedTradeStream(self, f) for f in (DataFeed.SIP, DataFeed.BOATS)}

    def wait(self, futures: Any, timeout: float | None = None, return_when: str = "") -> Any:
        del timeout  # the supervisor's 1 s tick, shortened so that a drive takes milliseconds
        self.loop_ticks += 1
        if self.loop_ticks >= 2:
            self.candidates_built.set()
        return futures_wait(futures, timeout=0.005, return_when=return_when)

    def write_json(self, path: Path, payload: dict[str, Any]) -> None:
        if path.name == _EXECUTION_STATUS:
            self.statuses.append(str(payload["status"]))
        pf._atomic_write_json(path, payload)


def _scan_with_one_mover(session: str, side: str) -> tuple[dict[str, Any], str, str]:
    """A fresh scan manifest whose one eligible mover routes to ``(symbol, side)``."""
    now_iso = datetime.now(UTC).isoformat()
    regime = 1 if side == "LONG" else -1
    mover: dict[str, Any] = {
        "symbol": "NVDA",
        "extreme_mover_eligible": True,
        "side": "BUY" if side == "LONG" else "SELL",
        "session": session,
        "bar_close_utc": now_iso,
        "natr_pct": 0.012,
        "last_price": 100,
        "shortable": True,
        "easy_to_borrow": True,
        "borrow_source": "alpaca_paper_asset",
        "prior_close": 99.0,
        "annual_borrow_rate": 0.0,
    }
    # The inverse ETF a short falls back to overnight; it is quoted but not a mover itself.
    hedge = mover | {"symbol": "NVDS", "extreme_mover_eligible": False, "side": "BUY"}
    route = pf.select_short_or_inverse_fallback(
        "NVDA", market_regime=regime, borrow_meta=mover, session=session
    )
    assert route is not None
    doc = {
        "generated_at_utc": now_iso,
        "macro_regime": {"regime": regime},
        "entries": [mover, hedge],
    }
    return doc, route[0], route[1]


def _seed_closed_round_trip(lane: pf.AlpacaPaperLane, symbol: str, side: str) -> None:
    """An earlier, long-closed entry for ``symbol``: the next candidate is a re-entry or reverse."""
    lane.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(lane.duckdb_path)) as db:
        pf._ensure_entry_decisions_table(db)
        db.execute(
            "INSERT INTO paper_entry_decisions (client_order_id, symbol, side, decision_price, "
            "arrival_price, session, origin, annual_rate, initial_stop, natr_pct, closed_at) "
            "VALUES ('noesis-earlier-round-trip', ?, ?, 100, 100, 'RTH', 'TREND_ALIGNED_LONG', "
            "0, 98, 0.012, now() - INTERVAL '10 minutes')",
            [symbol, side],
        )


def _open_ledger_rows(lane: pf.AlpacaPaperLane) -> int:
    if not lane.duckdb_path.is_file():
        return 0
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
        if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
            return 0
        row = db.execute(
            "SELECT count(*) FROM paper_entry_decisions WHERE closed_at IS NULL"
        ).fetchone()
    assert row is not None
    return int(row[0])


def _drive_live_supervisor(
    root: Path,
    *,
    opt_in: bool,
    session: str = "RTH",
    side: str = "LONG",
    equity_verified: bool = True,
    lineage_enabled: bool = False,
    breaker_tripped: bool = False,
    candidate: str = "FIRST",
    grant_at_candidate_build: bool = False,
    hold_check_raises: bool = False,
    tca_receipt: dict[str, Any] | None = _MEASURED_TCA_RECEIPT,
    live_dir: Path | None = None,
) -> SimpleNamespace:
    """Run ``run_execution`` once on a simulator-backed live coordinator with one scanner mover.

    ``root/manifests`` is the shared (paper) directory with a fresh scan manifest and the TCA
    receipt; the live directory is ``root/live/manifests`` unless given. ``candidate`` seeds an
    earlier closed entry (``REENTRY`` same side, ``REVERSE`` opposite). With
    ``grant_at_candidate_build`` the lane's first hold check answers "permitted", as it will once
    a leg has merit, so the candidate reaches the admission path and its second hold site.
    """
    shared_dir = root / "manifests"
    live_dir = root / "live" / "manifests" if live_dir is None else live_dir
    shared_dir.mkdir(parents=True, exist_ok=True)
    scan_doc, symbol, route_side = _scan_with_one_mover(session, side)
    (shared_dir / _SCAN_MANIFEST).write_text(json.dumps(scan_doc), encoding="utf-8")
    if tca_receipt is not None:
        (shared_dir / _TCA_MANIFEST).write_text(json.dumps(tca_receipt), encoding="utf-8")
    shared_before = _dir_bytes(shared_dir)

    sim = _live_sim(session=session)
    for quoted in ("NVDA", "NVDS"):
        sim.set_price(quoted, "100.00")
        sim.set_asset(quoted, attributes=["overnight_tradable"])
    drive = _SupervisorDrive(DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP, (symbol,))
    built: list[pf.AlpacaLiveVenueCoordinator] = []
    hold_checks: list[Any] = []

    def refresh_accounts(
        farm: pf.PaperFarmCoordinator, now: datetime, _session: str, **_kwargs: Any
    ) -> dict[str, dict[str, Any]]:
        # Stand-in for runner.refresh_accounts, which still demands the three paper accounts:
        # the same three broker reads, the lane equity (stamped as verified on request) and the
        # symbol-lock reconcile.
        inventory: dict[str, dict[str, Any]] = {}
        owners: dict[str, str] = {}
        for acct, lane in farm.lanes.items():
            broker = lane.trading_client
            account = broker.get_account()
            positions = broker.get_all_positions()
            orders = broker.get_orders(GetOrdersRequest(limit=500))
            inventory[acct] = {position.symbol: position for position in positions}
            owners.update(dict.fromkeys({item.symbol for item in [*positions, *orders]}, acct))
            lane.start_equity = Decimal(str(account.equity))
            if equity_verified:
                lane.equity_verified_at = now.timestamp()
        farm.symbol_lock.reconcile(owners)
        return inventory

    with contextlib.ExitStack() as stack:

        def build_coordinator(
            manifest_dir: Path, *, autonomous_opt_in: bool
        ) -> pf.AlpacaLiveVenueCoordinator:
            farm = pf.AlpacaLiveVenueCoordinator(
                manifest_dir,
                trading_client=sim,
                data_client=_FreshQuotes(),
                autonomous_opt_in=autonomous_opt_in,
                market_streams=drive.streams,
            )
            lane = farm.lanes[_LIVE_ACCOUNT]
            lane.breaker.tripped = breaker_tripped
            if candidate != "FIRST":
                opposite = "SHORT" if route_side == "LONG" else "LONG"
                _seed_closed_round_trip(
                    lane, symbol, route_side if candidate == "REENTRY" else opposite
                )
            real_hold_reason = lane.entry_hold_reason

            def hold_reason(intent: Any, *, leg_merit: bool = False) -> str | None:
                hold_checks.append(intent)
                if hold_check_raises:
                    raise RuntimeError("scripted hold-check failure")
                if grant_at_candidate_build and len(hold_checks) == 1:
                    return None
                return real_hold_reason(intent, leg_merit=leg_merit)

            stack.enter_context(mock.patch.object(lane, "entry_hold_reason", hold_reason))
            built.append(farm)
            return farm

        for target, name, replacement in (
            (pf, "LIVE_LINEAGE_ENTRIES_ENABLED", lineage_enabled),
            (execution, "build_live_venue_coordinator", build_coordinator),
            (execution, "refresh_accounts", refresh_accounts),
            (execution, "wait", drive.wait),
            (execution, "_atomic_write_json", drive.write_json),
            (execution.engine, "classify_us_equity_session_et", _session_classifier(session)),
            # The simulator's virtual clock runs years ahead of the wall clock; these drives
            # are about the tiers, so the broker-clock comparison (R2) reads nothing here.
            (execution, "_measure_broker_clock_skew", lambda _broker: None),
        ):
            stack.enter_context(mock.patch.object(target, name, replacement))
        stack.enter_context(
            mock.patch.object(
                execution.runner, "verify_live_environment", lambda: opt_in, create=True
            )
        )
        execution.run_execution(live_dir, venue_profile=LIVE_TIERED_VENUE_ID, shared_dir=shared_dir)

    (farm,) = built
    status = json.loads((live_dir / _EXECUTION_STATUS).read_text("utf-8"))
    if sim.calls("get_asset"):
        hold_site = "before submit"  # the admission path ran: clock, asset, quote, ledger
    elif status["entry_holds"]:
        hold_site = "candidate build"
    else:
        hold_site = "no candidate"
    return SimpleNamespace(
        sim=sim,
        farm=farm,
        lane=farm.lanes[_LIVE_ACCOUNT],
        drive=drive,
        status=status,
        symbol=symbol,
        feed=drive.feed,
        opt_in=opt_in,
        hold_site=hold_site,
        hold_checks=hold_checks,
        live_dir=live_dir,
        shared_dir=shared_dir,
        shared_before=shared_before,
        shared_after=_dir_bytes(shared_dir),
        seeded_rows=int(candidate != "FIRST"),
    )


def _assert_live_supervisor_held_quietly(run: SimpleNamespace) -> None:
    """The live supervisor sent nothing, latched no fault and wrote only its own directory."""
    assert run.sim.calls(kind=MUTATING) == [] and run.sim.mutating_call_count == 0
    status = run.status
    assert status["status"] == "STOPPED" and status["entry_fault"] is False
    assert "ENTRY_HELD" not in run.drive.statuses
    assert status["paper_only"] is False and status["live_execution_enabled"] is run.opt_in
    # Not a vacuous run: the account was read and the print got through handle's freshness gate.
    assert run.sim.calls("get_account")
    assert list(status["last_trade_utc"]) == [run.feed.value]
    if run.hold_site == "before submit":
        assert run.symbol in run.drive.ever_subscribed
    else:
        # Held where the candidates are built: never subscribed, never looked up at the broker.
        assert run.drive.ever_subscribed == _STILL_WATCHED
        assert run.sim.calls("get_clock") == []
    # The supervisor held it itself: the lane's own entry path was never entered.
    assert run.lane.counters["orders_submitted"] == 0 and "entry_holds" not in run.lane.counters
    assert run.farm.symbol_lock.snapshot() == {}
    assert run.lane.duckdb_path.is_file() and _open_ledger_rows(run.lane) == 0
    assert status["managed_symbols"] == [] and status["active_positions"] == []
    # Reads the shared directory, never writes there.
    assert run.shared_after == run.shared_before


def _drive_paper_supervisor(root: Path, *, reverse_candidate: bool = False) -> SimpleNamespace:
    """The same scripted scan and print on a simulator-backed paper farm (regular session).

    ``reverse_candidate`` seeds an earlier closed short, so the long candidate is a reverse.
    """
    manifest_dir = root / "manifests"
    manifest_dir.mkdir(parents=True)
    scan_doc, symbol, _side = _scan_with_one_mover("RTH", "LONG")
    (manifest_dir / _SCAN_MANIFEST).write_text(json.dumps(scan_doc), encoding="utf-8")
    sims = {acct: FaultInjectingAlpacaSim(session="RTH") for acct in pf.ALPACA_LANE_PROFILES}
    for sim in sims.values():
        sim.set_price(symbol, "100.00")
    farm = pf.PaperFarmCoordinator(manifest_dir, alpaca_clients=sims)
    if reverse_candidate:
        _seed_closed_round_trip(farm.lanes["ALPACA_PA1"], symbol, "SHORT")
    drive = _SupervisorDrive(DataFeed.SIP, (symbol,))
    with contextlib.ExitStack() as stack:
        for target, name, replacement in (
            (execution, "verify_paper_environment", lambda: None),
            (execution, "resolve_alpaca_env_creds", lambda: ("", "")),
            (execution, "PaperFarmCoordinator", lambda _dir: farm),
            (execution, "StockDataStream", lambda *_args, feed: drive.streams[feed]),
            (execution, "wait", drive.wait),
            (execution, "_atomic_write_json", drive.write_json),
            (execution.engine, "classify_us_equity_session_et", _session_classifier("RTH")),
            # As in the live drive: the simulator clock is virtual, so no skew is measured.
            (execution, "_measure_broker_clock_skew", lambda _broker: None),
            (
                execution.ExtremeMoverScanner,
                "from_env",
                lambda: SimpleNamespace(data_client=_FreshQuotes()),
            ),
        ):
            stack.enter_context(mock.patch.object(target, name, replacement))
        execution.run_execution(manifest_dir)
    entry_origins: list[str] = []
    for lane in farm.lanes.values():
        with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
            if ("paper_entry_decisions",) in db.execute("SHOW TABLES").fetchall():
                entry_origins += [
                    str(row[0])
                    for row in db.execute(
                        "SELECT origin FROM paper_entry_decisions WHERE closed_at IS NULL"
                    ).fetchall()
                ]
    return SimpleNamespace(
        sims=sims,
        farm=farm,
        drive=drive,
        symbol=symbol,
        entry_origins=entry_origins,
        status=json.loads((manifest_dir / _EXECUTION_STATUS).read_text("utf-8")),
    )


def test_live_handle_holds_quietly_without_latching_entry_fault(tmp_path: Path) -> None:
    # Control: on the paper farm the same scripted scan and print do reach submit_order, so the
    # live runs below are held by the tier and not by a drive that never gets to handle.
    # The paper lineage suffix is what it was: a candidate after a closed opposite entry still
    # goes out, as an auto-reverse. The live holds below never apply to the paper farm.
    for reverse_candidate, origin in (
        (False, "TREND_ALIGNED_LONG"),
        (True, "TREND_ALIGNED_LONG_AUTO_REVERSE"),
    ):
        paper = _drive_paper_supervisor(
            tmp_path / f"paper-{origin.lower()}", reverse_candidate=reverse_candidate
        )
        paper_entries = [call for sim in paper.sims.values() for call in sim.calls("submit_order")]
        assert len(paper_entries) == 1 and paper_entries[0].outcome == "ok"
        assert sum(sim.mutating_call_count for sim in paper.sims.values()) == 1
        assert paper.entry_origins == [origin]
        assert paper.status["entry_fault"] is False and "ENTRY_HELD" not in paper.drive.statuses
        assert "ORDER_ACKNOWLEDGED" in paper.drive.statuses
        assert paper.status["managed_symbols"] == [paper.symbol]
        # Paper output is unchanged: the same stamps, and no live-only key.
        assert paper.status["paper_only"] is True
        assert paper.status["live_execution_enabled"] is False
        assert "entry_holds" not in paper.status
        assert paper.status["venue_order_handles"]["paper_only"] is True

    # 1. The shipped state (no tier-3 opt-in): the scanner candidate is held where candidates
    #    are built, so it is never subscribed and the broker is never asked about it.
    shipped = _drive_live_supervisor(tmp_path / "shipped", opt_in=False)
    _assert_live_supervisor_held_quietly(shipped)
    assert shipped.hold_site == "candidate build"
    assert shipped.status["entry_holds"] == {"NVDA": "HOLD_TIER_NOT_GRANTED"}
    assert shipped.hold_checks == ["AUTONOMOUS_ENTRY"]
    assert {call.name for call in shipped.sim.call_log} == {
        "get_account",
        "get_all_positions",
        "get_orders",
    }
    # Its own status, ledger and locks are under the live directory, and nowhere else.
    assert sorted(path.name for path in (tmp_path / "shipped").iterdir()) == ["live", "manifests"]
    live_files = {path.name for path in shipped.live_dir.iterdir()}
    assert {
        _EXECUTION_STATUS,
        "paper_farm_execution.duckdb",
        "paper_farm_runner.duckdb",
        shipped.lane.duckdb_path.name,
    } <= live_files
    assert set(shipped.shared_after) == {_SCAN_MANIFEST, _TCA_MANIFEST}
    handles = shipped.status["venue_order_handles"]
    assert handles["paper_only"] is False and handles["live_execution_enabled"] is False
    assert list(shipped.status["rate_budget_live_rpm"]["per_account"]) == [_LIVE_ACCOUNT]

    # 2. Opt-in given and equity verified, or equity unverified: still held, since no leg has
    #    merit yet. The opt-in shows in the stamps and nowhere else.
    held_cases: tuple[tuple[str, dict[str, Any]], ...] = (
        ("opted-in", {"opt_in": True, "lineage_enabled": True}),
        ("unverified", {"opt_in": True, "equity_verified": False}),
        ("short", {"opt_in": False, "side": "SHORT", "session": "AFTER_HOURS"}),
    )
    for case, kwargs in held_cases:
        held = _drive_live_supervisor(tmp_path / case, **kwargs)
        _assert_live_supervisor_held_quietly(held)
        assert held.hold_site == "candidate build"
        assert held.status["entry_holds"] == {"NVDA": "HOLD_TIER_NOT_GRANTED"}

    # 3. A candidate that was permitted when the candidates were built is checked again just
    #    before the submit: the whole admission path runs (reads only) and it is held there.
    late = _drive_live_supervisor(tmp_path / "late", opt_in=True, grant_at_candidate_build=True)
    _assert_live_supervisor_held_quietly(late)
    assert late.hold_site == "before submit"
    assert late.hold_checks == ["AUTONOMOUS_ENTRY", "AUTONOMOUS_ENTRY"]
    assert late.status["entry_holds"] == {"NVDA": "HOLD_TIER_NOT_GRANTED"}
    assert late.sim.calls("get_clock") and late.sim.calls("get_asset")

    # 4. A reverse or re-entry candidate is owner lineage on live. It is held until lineage is
    #    resolved, without asking the lane, even with the lineage constant already flipped.
    for candidate in ("REENTRY", "REVERSE"):
        lineage = _drive_live_supervisor(
            tmp_path / candidate.lower(),
            opt_in=True,
            lineage_enabled=True,
            candidate=candidate,
            grant_at_candidate_build=True,
        )
        _assert_live_supervisor_held_quietly(lineage)
        assert lineage.hold_site == "before submit"
        assert lineage.hold_checks == ["AUTONOMOUS_ENTRY"]
        assert lineage.status["entry_holds"] == {"NVDA": "HOLD_LIVE_LINEAGE_UNRESOLVED"}

    # 5. Even a hold check that raises is a quiet HOLD, not an exception folded into a fault.
    broken = _drive_live_supervisor(tmp_path / "broken", opt_in=True, hold_check_raises=True)
    _assert_live_supervisor_held_quietly(broken)
    assert broken.status["entry_holds"] == {"NVDA": "HOLD_ENTRY_CHECK_FAILED"}

    # 6. Without published TCA evidence in the shared directory no candidate is built at all:
    #    a status, not a fault. A fresh live directory never opens the cost gate.
    no_tca = _drive_live_supervisor(tmp_path / "no-tca", opt_in=True, tca_receipt=None)
    _assert_live_supervisor_held_quietly(no_tca)
    assert no_tca.hold_site == "no candidate" and no_tca.hold_checks == []
    assert "TCA_RECALIBRATION_REQUIRED" in no_tca.drive.statuses
    assert set(no_tca.shared_after) == {_SCAN_MANIFEST}

    # 7. Nothing latched: the next start on the same live directory begins without a fault.
    again = _drive_live_supervisor(tmp_path / "shipped", opt_in=False, live_dir=shipped.live_dir)
    _assert_live_supervisor_held_quietly(again)
    assert again.drive.statuses[0] == "AWAITING_MARKET_DATA"


def test_run_execution_live_venue_parameters_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parameters = inspect.signature(execution.run_execution).parameters
    assert list(parameters) == ["manifest_dir", "stop", "venue_profile", "shared_dir"]
    for name in ("stop", "venue_profile", "shared_dir"):
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[name].default is None

    started: list[str] = []

    def must_not_run(label: str) -> Callable[..., Any]:
        def record(*_args: Any, **_kwargs: Any) -> Any:
            started.append(label)
            raise AssertionError(f"{label} must not run")

        return record

    monkeypatch.setattr(execution, "verify_paper_environment", must_not_run("paper check"))
    monkeypatch.setattr(execution, "PaperFarmCoordinator", must_not_run("paper coordinator"))
    monkeypatch.setattr(execution, "build_live_venue_coordinator", must_not_run("live coordinator"))
    monkeypatch.setattr(execution, "StockDataStream", must_not_run("paper stream"))
    live_dir, shared_dir = tmp_path / "live" / "manifests", tmp_path / "manifests"
    shared_dir.mkdir()

    # Only the paper farm (None) and the tiered live venue can be supervised.
    monkeypatch.setattr(
        execution.runner, "verify_live_environment", must_not_run("live check"), raising=False
    )
    for venue in _STILL_REFUSED_LIVE_VENUES:
        with pytest.raises(PermissionError) as refused:
            execution.run_execution(live_dir, venue_profile=venue, shared_dir=shared_dir)
        assert str(refused.value) == f"Refused live profile {venue!r}: live_execution_enabled=False"
    not_a_venue: Any = 1000
    for venue in ("ALPACA_PAPER_1000", "IBKR_PAPER_4002", "NOT_A_VENUE", "", not_a_venue):
        with pytest.raises(
            ValueError, match=r"supervises the paper farm|Unknown execution venue profile"
        ):
            execution.run_execution(live_dir, venue_profile=venue, shared_dir=shared_dir)
    # shared_dir belongs to the live venue: the paper farm keeps everything in one directory.
    with pytest.raises(ValueError, match="live venue only"):
        execution.run_execution(tmp_path / "paper", shared_dir=shared_dir)
    assert started == []

    # Live supervision starts only behind the runner's live-environment check, whose result is
    # the tier-3 opt-in: absent, failing or not a bool, nothing is built.
    def run_live(**overrides: Any) -> None:
        kwargs: dict[str, Any] = {"venue_profile": LIVE_TIERED_VENUE_ID, "shared_dir": shared_dir}
        execution.run_execution(live_dir, **(kwargs | overrides))

    monkeypatch.delattr(execution.runner, "verify_live_environment", raising=False)
    with pytest.raises(PermissionError, match="verify_live_environment is unavailable"):
        run_live()

    def failing_check() -> bool:
        raise PermissionError("Live profile isolation check failed")

    monkeypatch.setattr(execution.runner, "verify_live_environment", failing_check, raising=False)
    with pytest.raises(PermissionError, match="isolation check failed"):
        run_live()
    for not_an_opt_in in (None, "true", 1):
        monkeypatch.setattr(execution.runner, "verify_live_environment", lambda v=not_an_opt_in: v)
        with pytest.raises(PermissionError, match="no tier-3 opt-in"):
            run_live()

    # The live directory is its own: no shared directory, the shared one, or one inside it.
    monkeypatch.setattr(execution.runner, "verify_live_environment", lambda: False)
    with pytest.raises(ValueError, match="requires shared_dir"):
        run_live(shared_dir=None)
    for own in (shared_dir, shared_dir / "live"):
        with pytest.raises(PermissionError, match="inside the shared manifest directory"):
            execution.run_execution(own, venue_profile=LIVE_TIERED_VENUE_ID, shared_dir=shared_dir)
    assert started == [] and not live_dir.exists()
    assert list(shared_dir.iterdir()) == []

    # The coordinator it is handed must be the verified live one, with both feeds and a data
    # client; a refusal leaves no file behind, in either directory.
    def coordinator(**overrides: Any) -> Callable[..., Any]:
        def build(manifest_dir: Path, *, autonomous_opt_in: bool) -> Any:
            kwargs: dict[str, Any] = {
                "trading_client": _live_sim(),
                "data_client": _FreshQuotes(),
                "autonomous_opt_in": autonomous_opt_in,
                "market_streams": dict.fromkeys((DataFeed.SIP, DataFeed.BOATS), object()),
            }
            return pf.AlpacaLiveVenueCoordinator(manifest_dir, **(kwargs | overrides))

        return build

    for build, error, message in (
        (coordinator(market_streams={DataFeed.SIP: object()}), RuntimeError, "market data stream"),
        (coordinator(data_client=None), RuntimeError, "market data client"),
        (coordinator(autonomous_opt_in=True), PermissionError, "not the verified one"),
    ):
        monkeypatch.setattr(execution, "build_live_venue_coordinator", build)
        with pytest.raises(error, match=message):
            run_live()
    paper_clients = {acct: FaultInjectingAlpacaSim() for acct in pf.ALPACA_LANE_PROFILES}
    monkeypatch.setattr(
        execution,
        "build_live_venue_coordinator",
        lambda manifest_dir, *, autonomous_opt_in: pf.PaperFarmCoordinator(
            manifest_dir, alpaca_clients=paper_clients
        ),
    )
    with pytest.raises(PermissionError, match="not the verified one"):
        run_live()
    assert not live_dir.exists() and list(shared_dir.iterdir()) == []


def test_live_cost_calibration_gate_needs_published_measured_evidence(tmp_path: Path) -> None:
    gate = execution.cost_calibration_allows_entry
    environment = inspect.signature(gate).parameters["environment"]
    assert environment.kind is inspect.Parameter.KEYWORD_ONLY and environment.default == "PAPER"
    receipt_path = tmp_path / _TCA_MANIFEST
    strict: Any
    lowercase: Any = "live"
    strict_values: tuple[Any, ...] = ("LIVE", lowercase, None)

    # A fresh directory: the paper farm may enter, as before; a live lane may not.
    assert gate(tmp_path) is True and gate(tmp_path, environment="PAPER") is True
    for strict in strict_values:
        assert gate(tmp_path, environment=strict) is False

    # Published and measured inside the envelope: both may enter.
    receipt_path.write_text(json.dumps(_MEASURED_TCA_RECEIPT), encoding="utf-8")
    assert gate(tmp_path) is True and gate(tmp_path, environment="LIVE") is True

    # Published but nothing measured (or no usable row count): true only vacuously, so paper
    # keeps today's answer and live holds.
    for rows in (0, -1, True, "12", None):
        receipt_path.write_text(
            json.dumps(_MEASURED_TCA_RECEIPT | {"duckdb_tca_rows": rows}), encoding="utf-8"
        )
        assert gate(tmp_path) is True
        assert gate(tmp_path, environment="LIVE") is False
    unmeasured = {k: v for k, v in _MEASURED_TCA_RECEIPT.items() if k != "duckdb_tca_rows"}
    receipt_path.write_text(json.dumps(unmeasured), encoding="utf-8")
    assert gate(tmp_path) is True and gate(tmp_path, environment="LIVE") is False

    # An adverse or malformed receipt blocks both, exactly as it blocked paper before.
    for adverse in (
        {"parity_within_2x_envelope": False},
        {"duckdb_errors": ["locked"]},
        {"liquid_ge10_within_2x_envelope": False},
        {"paper_only": False},
        {"live_execution_enabled": True},
        {"schema_version": "tier0.tca-calibration-manifest.v0"},
    ):
        receipt_path.write_text(json.dumps(_MEASURED_TCA_RECEIPT | adverse), encoding="utf-8")
        assert gate(tmp_path) is False and gate(tmp_path, environment="LIVE") is False


def test_supervisor_broker_mutations_all_go_through_the_lane_choke_point() -> None:
    tree = ast.parse(Path(inspect.getfile(execution)).read_text(encoding="utf-8"))
    tv_confirm_calls = {
        id(node)
        for function in ast.walk(tree)
        if isinstance(function, ast.FunctionDef)
        and function.name.startswith("submit_confirmed_tv_preset_")
        for node in ast.walk(function)
    }
    direct: list[int] = []
    routed: dict[str, int] = dict.fromkeys(_CHOKE_POINT_METHODS, 0)
    integrity_checks = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            # The old per-site flag checks read lane.paper_only; assert_integrity replaced them.
            assert node.attr != "paper_only", f"line {node.lineno}"
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if isinstance(key, ast.Constant) and key.value in (
                    "paper_only",
                    "live_execution_enabled",
                ):
                    # A stamp is derived from the environment, never a literal.
                    assert not isinstance(value, ast.Constant), f"line {value.lineno}"
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        name = node.func.attr
        integrity_checks += int(name == "assert_integrity")
        if name in _BROKER_MUTATIONS:
            direct.append(node.lineno)
        if name not in _CHOKE_POINT_METHODS:
            continue
        routed[name] += 1
        intents = [keyword.value for keyword in node.keywords if keyword.arg == "intent"]
        if name == "broker_submit":
            assert len(intents) == 1 and isinstance(intents[0], ast.Constant)
            operator_confirmed = id(node) in tv_confirm_calls
            assert intents[0].value == (
                "OPERATOR_CONFIRMED_ENTRY" if operator_confirmed else "PROTECTIVE"
            ), f"line {node.lineno}"
        else:
            # Cancels and replaces take the PROTECTIVE default.
            assert intents == [], f"line {node.lineno}"
    assert direct == []
    assert routed["broker_submit"] >= 3 and routed["broker_cancel"] >= 4
    assert routed["broker_replace"] >= 1 and integrity_checks >= 2
    # The supervisor walks its coordinator's own accounts, never the paper lane table.
    supervisor_names = {
        node.id
        for function in ast.walk(tree)
        if isinstance(function, ast.FunctionDef) and function.name == "run_execution"
        for node in ast.walk(function)
        if isinstance(node, ast.Name)
    }
    assert supervisor_names and "ALPACA_LANE_PROFILES" not in supervisor_names


def test_tv_confirm_paper_handoff_refuses_a_live_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tier0_sota.tv.bridge import stage_tv_alert_preset

    confirm_at = datetime(2026, 10, 15, 12, 30, tzinfo=UTC)  # 08:30 New York, pre-market
    payload: dict[str, object] = {
        "schema_version": "tier0.tv-alert.v1",
        "symbol": "NVDA",
        "ts_utc_ms": 1795746660000,
        "bar_confirmed": True,
        "side": "BUY",
        "qty": 1,
        "venue": "ALPACA",
        "session": "PRE_MARKET",
        "order_type": "STOP_LIMIT",
        "trigger_price": 100.0,
        "stop_price": 98.5,
        "limit_collar_bps": 35.0,
        "trailing_schedule": "baseline",
        "natr_pct": 0.02,
        "auto_reverse": True,
        "auto_reentry": True,
        "reentry_cooldown_secs": 60,
        "strategy_id": "SignalExecutionStrategy",
        "semi_auto_confirmed": False,
        "data_source": "tradingview_pine_v6",
    }

    def stage(manifest_dir: Path) -> str:
        staged = stage_tv_alert_preset(
            payload,
            staged_path=manifest_dir / "tv_sota_pack" / "staged_tv_presets.json",
            watchlist_path=manifest_dir / "missing_wl.json",
            venue="ALPACA",
            live_intake=False,
        )
        return str(staged["alert_id"])

    quote = SimpleNamespace(bid_price=100.0, ask_price=100.01, timestamp=confirm_at)
    quotes = SimpleNamespace(get_stock_latest_quote=lambda _request: {"NVDA": quote})
    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "cost_calibration_allows_entry", lambda _dir: True)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner, "from_env", lambda: SimpleNamespace(data_client=quotes)
    )

    # Paper: the preview stamps are what they were, and the confirmed order goes out once.
    paper_dir = tmp_path / "paper"
    paper_sims = {acct: FaultInjectingAlpacaSim() for acct in pf.ALPACA_LANE_PROFILES}
    paper_farm = pf.PaperFarmCoordinator(paper_dir, alpaca_clients=paper_sims)
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _dir: paper_farm)
    alert_id = stage(paper_dir)
    preview = execution.submit_confirmed_tv_preset_paper(
        paper_dir, alert_id=alert_id, dry_run=True, now_utc=confirm_at
    )
    assert preview["paper_only"] is True and preview["live_execution_enabled"] is False
    assert sum(sim.mutating_call_count for sim in paper_sims.values()) == 0
    sent = execution.submit_confirmed_tv_preset_paper(
        paper_dir, alert_id=alert_id, now_utc=confirm_at
    )
    assert sent["submitted"] is True and sent["paper_only"] is True
    assert [call.name for sim in paper_sims.values() for call in sim.calls(kind=MUTATING)] == [
        "submit_order"
    ]

    # Handed a live coordinator, the paper handoff refuses the lane and sends nothing: a valid
    # live lane passes its own integrity check, so the handoff checks the environment itself.
    live_dir = tmp_path / "live"
    live_sim = _live_sim()
    live_farm = pf.AlpacaLiveVenueCoordinator(live_dir, trading_client=live_sim)
    live_farm.lanes[_LIVE_ACCOUNT].equity_verified_at = confirm_at.timestamp()
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _dir: live_farm)
    monkeypatch.setattr(execution, "refresh_accounts", lambda *_args, **_kwargs: {})
    live_alert_id = stage(live_dir)
    for dry_run in (True, False):
        with pytest.raises(PermissionError) as refusal:
            execution.submit_confirmed_tv_preset_paper(
                live_dir, alert_id=live_alert_id, dry_run=dry_run, now_utc=confirm_at
            )
        assert str(refusal.value) == "Live execution disabled; paper_only=True enforced."
    assert live_sim.call_log == []


def test_adoption_stop_attach_fault_still_builds_the_tick_supervised_guard(
    tmp_path: Path,
) -> None:
    sim = _live_sim(session="RTH")
    farm = pf.AlpacaLiveVenueCoordinator(tmp_path / "live", trading_client=sim)
    lane = farm.lanes[_LIVE_ACCOUNT]
    sim.open_position("AMD", 10, "100.00")
    sim.open_position("NVDA", 5, "200.00")
    slots_before = _budget_slots_used(lane)

    # The broker refuses the first native stop. Adoption carries on: that position gets a Guard
    # without a broker stop (tick-supervised), the next one gets its resting stop.
    sim.fail_next("submit_order", 403)
    guards: dict[str, execution.Guard] = {}
    adopted = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    assert [guard.symbol for guard in adopted] == ["AMD", "NVDA"] == list(guards)
    assert guards["AMD"].child is None and guards["NVDA"].child is not None
    assert lane.counters["adoption_stop_attach_faults"] == 1
    assert lane.counters["adopted_external"] == 2
    assert [call.outcome for call in sim.calls("submit_order")] == ["fault:403", "ok"]
    assert len(sim.calls(kind=MUTATING)) == 2
    assert _budget_slots_used(lane) == slots_before + 2  # both from the exit reserve
    resting = sim.inspect_orders("open")
    assert [(order.symbol, order.side, order.time_in_force) for order in resting] == [
        ("NVDA", OrderSide.SELL, TimeInForce.GTC)
    ]
    assert Decimal(str(resting[0].qty)) == Decimal("5")
    assert _open_ledger_rows(lane) == 2

    # The Guard without a broker stop still protects: a breach on a tick sends the exit, and
    # the Guard with one ratchets it. Both go through the lane's choke point.
    assert guards["AMD"].stop < Decimal("100.00")
    guards["AMD"].on_price(guards["AMD"].stop - Decimal("0.50"), "RTH")
    exits = sim.calls("submit_order")[2:]
    assert [call.outcome for call in exits] == ["ok"]
    assert exits[0].payload["symbol"] == "AMD" and exits[0].payload["side"] == "sell"
    assert sim.inspect_position("AMD") is None
    first_stop = guards["NVDA"].stop
    guards["NVDA"].on_price(Decimal("230.00"), "RTH")
    assert guards["NVDA"].stop > first_stop
    assert [call.outcome for call in sim.calls("replace_order_by_id")] == ["ok"]
    assert lane.counters["stop_replaces"] == 1

    # Only a broker refusal is absorbed. A lane that is no longer what it was built as raises
    # the lane's own refusal before any position is read or any order is sent.
    sim.open_position("MSFT", 3, "300.00")
    calls_before = len(sim.call_log)
    with (
        mock.patch.object(lane, "paper_only", True),
        pytest.raises(PermissionError) as refusal,
    ):
        execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    assert str(refusal.value) == _PAPER_ONLY_REFUSAL
    assert len(sim.call_log) == calls_before and "MSFT" not in guards


# ------------------- stop coverage (R1), exit state machine (R4) and adoption (R5): simulator

_STOP_ORDER_TYPES = (OrderType.STOP, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP)


def _live_farm(
    directory: Path, sim: FaultInjectingAlpacaSim
) -> tuple[pf.AlpacaLiveVenueCoordinator, pf.AlpacaPaperLane]:
    farm = pf.AlpacaLiveVenueCoordinator(directory, trading_client=sim)
    return farm, farm.lanes[_LIVE_ACCOUNT]


def _resting_stops(sim: FaultInjectingAlpacaSim, symbol: str) -> list[Any]:
    """Open orders that are stop coverage by the account-facts rule: GTC stops, not ``held``."""
    return [
        order
        for order in sim.inspect_orders("open", symbol)
        if order.type in _STOP_ORDER_TYPES
        and order.time_in_force is TimeInForce.GTC
        and order.status is not OrderStatus.HELD
    ]


def _stop_cover_qty(sim: FaultInjectingAlpacaSim, symbol: str) -> Decimal:
    return sum(
        (
            Decimal(str(order.qty)) - Decimal(str(order.filled_qty))
            for order in _resting_stops(sim, symbol)
        ),
        Decimal("0"),
    )


def _position_qty(sim: FaultInjectingAlpacaSim, symbol: str) -> Decimal:
    position = sim.inspect_position(symbol)
    return Decimal("0") if position is None else Decimal(str(position.qty))


def _cover(
    farm: pf.PaperFarmCoordinator, guards: dict[str, execution.Guard], session: str
) -> list[str]:
    return execution.ensure_native_protective_stops(
        farm, guards, session=session, now_utc=datetime.now(UTC)
    )


def _tick(guard: execution.Guard, price: Decimal | str, session: str) -> None:
    """One supervised print for the guard, with its one-second exit throttle lifted."""
    guard.last_exit_check = 0.0
    guard.on_price(Decimal(str(price)), session)


def _broker_positions(sim: FaultInjectingAlpacaSim) -> dict[str, dict[str, Any]]:
    return {_LIVE_ACCOUNT: {position.symbol: position for position in sim.get_all_positions()}}


def _collared(guard: execution.Guard, price: Decimal, session: str, rung: int) -> Decimal:
    """The exit limit of ladder rung ``rung`` (1 = the session collar, 3 = the widest)."""
    _, schedule = execution.engine.select_pooled_trailing_schedule(guard.natr)
    collar = execution.engine.get_session_protection_profile(session, schedule).limit_collar_pct
    return execution.engine.compute_collared_limit_price(price, guard.side, collar * rung)


class _IdleStream:
    """A market-data stream that never delivers a print."""

    def __init__(self, drive: _TimerDrive) -> None:
        self.drive = drive

    def subscribe_trades(self, callback: Callable[[Any], Any], *symbols: str) -> None:
        del callback
        self.drive.ever_subscribed.update(symbols)

    def unsubscribe_trades(self, *symbols: str) -> None:
        del symbols

    def run(self) -> None:
        assert self.drive.finished.wait(60), "the supervisor loop never finished its ticks"

    def stop(self) -> None:
        self.drive.finished.set()


class _TimerDrive:
    """Drives the supervisor loop tick by tick with no market data and no scanner file.

    ``script[n]`` runs just before the loop's n-th tick, on the supervisor's own thread and
    outside its lock: it plays the owner or the broker acting between two ticks.
    """

    def __init__(self, script: dict[int, Callable[[], None]], ticks: int) -> None:
        self.script = script
        self.ticks = ticks
        self.loop_ticks = 0
        self.finished = threading.Event()
        self.ever_subscribed: set[str] = set()
        self.streams = {feed: _IdleStream(self) for feed in (DataFeed.SIP, DataFeed.BOATS)}

    def wait(self, futures: Any, timeout: float | None = None, return_when: str = "") -> Any:
        del timeout  # the supervisor's 1 s tick, shortened so that a drive takes milliseconds
        self.loop_ticks += 1
        step = self.script.get(self.loop_ticks)
        if step is not None:
            step()
        if self.loop_ticks > self.ticks:
            self.finished.set()
            return futures_wait(futures, timeout=30, return_when=return_when)
        return futures_wait(futures, timeout=0.005, return_when=return_when)


def _run_live_supervisor_on_timer(
    root: Path,
    sim: FaultInjectingAlpacaSim,
    *,
    session: str,
    script: dict[int, Callable[[], None]],
    ticks: int,
) -> SimpleNamespace:
    """Run ``run_execution`` on a live coordinator with nothing but its own loop tick.

    The shared directory stays empty (no scanner manifest, no TCA receipt), no trade print is
    delivered, and the stop-coverage cadence is zero so that every loop tick reconciles.
    """
    shared_dir = root / "manifests"
    live_dir = root / "live" / "manifests"
    shared_dir.mkdir(parents=True)
    drive = _TimerDrive(script, ticks)
    built: list[pf.AlpacaLiveVenueCoordinator] = []
    account_refreshes: list[datetime] = []

    def build_coordinator(
        manifest_dir: Path, *, autonomous_opt_in: bool
    ) -> pf.AlpacaLiveVenueCoordinator:
        farm = pf.AlpacaLiveVenueCoordinator(
            manifest_dir,
            trading_client=sim,
            data_client=_FreshQuotes(),
            autonomous_opt_in=autonomous_opt_in,
            market_streams=drive.streams,
        )
        built.append(farm)
        return farm

    def refresh_accounts(
        farm: pf.PaperFarmCoordinator, now: datetime, _session: str, **_kwargs: Any
    ) -> dict[str, dict[str, Any]]:
        # Stand-in for runner.refresh_accounts (it still demands the three paper accounts).
        account_refreshes.append(now)
        inventory: dict[str, dict[str, Any]] = {}
        for acct, lane in farm.lanes.items():
            lane.start_equity = Decimal(str(lane.trading_client.get_account().equity))
            inventory[acct] = {pos.symbol: pos for pos in lane.trading_client.get_all_positions()}
        return inventory

    with contextlib.ExitStack() as stack:
        for target, name, replacement in (
            (execution, "build_live_venue_coordinator", build_coordinator),
            (execution, "refresh_accounts", refresh_accounts),
            (execution, "wait", drive.wait),
            (execution, "STOP_COVERAGE_RECONCILE_SECS", 0.0),
            (execution.engine, "classify_us_equity_session_et", _session_classifier(session)),
        ):
            stack.enter_context(mock.patch.object(target, name, replacement))
        stack.enter_context(
            mock.patch.object(
                execution.runner, "verify_live_environment", lambda: False, create=True
            )
        )
        execution.run_execution(live_dir, venue_profile=LIVE_TIERED_VENUE_ID, shared_dir=shared_dir)

    (farm,) = built
    return SimpleNamespace(
        farm=farm,
        lane=farm.lanes[_LIVE_ACCOUNT],
        drive=drive,
        status=json.loads((live_dir / _EXECUTION_STATUS).read_text("utf-8")),
        live_dir=live_dir,
        shared_dir=shared_dir,
        account_refreshes=len(account_refreshes),
    )


@pytest.mark.parametrize("session", ["PRE_MARKET", "AFTER_HOURS", "OVERNIGHT", "CLOSED"])
def test_offhours_adopted_position_gets_resting_gtc_stop(tmp_path: Path, session: str) -> None:
    sim = _live_sim(session=session)
    sim.set_asset("NVDA", attributes=["overnight_tradable"])
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}

    # Adoption alone arms the synthetic supervision of the session and sends nothing.
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session=session)
    assert sim.mutating_call_count == 0 and guard.child is None
    assert lane.supervised_stops[guard.client_id].status == "ARMED"

    # The stop-coverage reconciler rests a GTC stop for the whole position, in any session.
    slots = _budget_slots_used(lane)
    assert _cover(farm, guards, session) == [f"{_LIVE_ACCOUNT}:NVDA:STOP_ATTACHED"]
    (stop,) = sim.inspect_orders("open", "NVDA")
    assert stop.type is OrderType.STOP and stop.side is OrderSide.SELL
    assert stop.time_in_force is TimeInForce.GTC and stop.status is OrderStatus.ACCEPTED
    assert Decimal(str(stop.qty)) == 10 and Decimal(str(stop.stop_price)) == guard.stop
    assert stop.client_order_id.startswith(f"stop-{guard.client_id}-")
    assert guard.lifecycle_id in stop.client_order_id and guard.child.id == str(stop.id)
    assert _stop_cover_qty(sim, "NVDA") == _position_qty(sim, "NVDA") == 10
    assert lane.counters["stop_attaches"] == 1 and _budget_slots_used(lane) == slots + 1
    # The invariant holds now: a second pass sends nothing, and the synthetic supervision of
    # the session stays armed alongside the resting stop.
    assert _cover(farm, guards, session) == [] and len(sim.calls("submit_order")) == 1
    assert lane.supervised_stops[guard.client_id].status == "ARMED"

    # Off-hours the resting stop does not fire, however far the price goes through it.
    breach = guard.stop - Decimal("0.50")
    sim.set_price("NVDA", str(breach))
    assert sim.inspect_order(stop.id).status is OrderStatus.ACCEPTED
    assert _position_qty(sim, "NVDA") == 10

    if session == "CLOSED":
        # Nothing trades while the market is closed. At the next regular open the stop is
        # armed and the broker itself sells: no harness call is involved.
        mutations = sim.mutating_call_count
        sim.set_session("RTH")
        assert sim.inspect_order(stop.id).status is OrderStatus.FILLED
        assert sim.inspect_position("NVDA") is None and sim.mutating_call_count == mutations
        return

    # In a trading session the tick supervision acts: the first tick releases the shares the
    # resting stop holds, the next one sells with the session's collared extended-hours limit.
    _tick(guard, breach, session)
    assert [call.name for call in sim.calls(kind=MUTATING)] == [
        "submit_order",
        "cancel_order_by_id",
    ]
    assert sim.inspect_order(stop.id).status is OrderStatus.CANCELED
    _tick(guard, breach, session)
    exit_order = sim.calls("submit_order")[-1].payload
    assert exit_order["type"] == "limit" and exit_order["extended_hours"] is True
    assert exit_order["side"] == "sell" and Decimal(str(exit_order["qty"])) == 10
    assert Decimal(str(exit_order["limit_price"])) == _collared(guard, breach, session, 1)
    assert exit_order["client_order_id"] == f"exit-{guard.client_id}"
    assert sim.inspect_position("NVDA") is None
    # Flat by broker truth: the lifecycle and its ledger row are closed, no fault is left.
    _tick(guard, breach, session)
    assert guard.lifecycle_closed is True and guard.protection_fault_code is None
    assert _open_ledger_rows(lane) == 0 and len(sim.calls("submit_order")) == 2


def test_stop_reattached_after_external_cancel(tmp_path: Path) -> None:
    # 1. The reconciler itself: a stop cancelled from outside is found gone by its id and
    #    replaced by a new order of the same lifecycle under a new client order id.
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    (first,) = _resting_stops(sim, "NVDA")
    assert guard.child.id == str(first.id) and _cover(farm, guards, "RTH") == []

    sim.external_cancel(first.id)
    assert _stop_cover_qty(sim, "NVDA") == 0
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:STOP_ATTACHED"]
    (second,) = _resting_stops(sim, "NVDA")
    assert second.id != first.id and second.client_order_id != first.client_order_id
    assert guard.lifecycle_id in first.client_order_id
    assert guard.lifecycle_id in second.client_order_id
    assert Decimal(str(second.qty)) == 10 and Decimal(str(second.stop_price)) == guard.stop
    assert guard.child.id == str(second.id) and _cover(farm, guards, "RTH") == []
    assert [call.outcome for call in sim.calls(kind=MUTATING)] == ["ok", "ok"]

    # A list that lags (it shows no open order at all) must not make the reconciler stack a
    # second stop: the stop the guard knows is asked for by its id. The same holds across a
    # rebuild of the guards, which finds no stop in such a list.
    with mock.patch.object(sim, "get_orders", return_value=[]):
        assert _cover(farm, guards, "RTH") == []
        rebuilt = execution.restore_guards(farm, _broker_positions(sim), existing_guards=guards)
        assert rebuilt["NVDA"].child is not None and rebuilt["NVDA"].child.id == str(second.id)
        assert _cover(farm, rebuilt, "RTH") == []
    assert len(sim.calls("submit_order")) == 2 and _stop_cover_qty(sim, "NVDA") == 10

    # 2. The supervisor loop does it on its own tick: no trade print arrives and there is no
    #    scanner file, yet the cancelled stop is back one tick later.
    timer_sim = _live_sim(session="RTH")
    timer_sim.open_position("NVDA", 10, "100.00")
    cancelled: list[Any] = []

    def cancel_stop_from_outside() -> None:
        (resting,) = _resting_stops(timer_sim, "NVDA")
        cancelled.append(resting.id)
        timer_sim.external_cancel(resting.id)

    run = _run_live_supervisor_on_timer(
        tmp_path / "timer",
        timer_sim,
        session="RTH",
        script={2: cancel_stop_from_outside},
        ticks=3,
    )
    (replacement,) = _resting_stops(timer_sim, "NVDA")
    assert len(cancelled) == 1 and replacement.id != cancelled[0]
    assert timer_sim.inspect_order(cancelled[0]).status is OrderStatus.CANCELED
    assert Decimal(str(replacement.qty)) == 10
    assert [call.outcome for call in timer_sim.calls(kind=MUTATING)] == ["ok", "ok"]
    status = run.status
    assert status["status"] == "STOPPED" and status["entry_fault"] is False
    assert status["managed_symbols"] == ["NVDA"] and status["last_trade_utc"] == {}
    assert status["stop_coverage"]["reconcile_faults"] == 0
    assert run.lane.counters["stop_attaches"] == 2 and run.drive.loop_ticks == 4
    assert list(run.shared_dir.iterdir()) == []


def test_stop_resized_when_position_grows(tmp_path: Path) -> None:
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    (first,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(first.qty)) == 10

    # The owner adds five shares by hand: one stop still, resized to the whole position.
    sim.open_position("NVDA", 5, "100.00")
    assert _stop_cover_qty(sim, "NVDA") == 10 and _position_qty(sim, "NVDA") == 15
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:STOP_RESIZED"]
    (grown,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(grown.qty)) == 15 and grown.replaces == first.id
    assert Decimal(str(grown.stop_price)) == guard.stop and guard.child.id == str(grown.id)
    assert lane.counters["stop_resizes"] == 1 and _cover(farm, guards, "RTH") == []

    # It follows the position down as well: a stop must never sell more than is held. The
    # resize above was a replace a moment ago, so this one waits out the one-second replace
    # throttle (R14) and goes out on a later reconcile.
    sim.open_position("NVDA", -3, "100.00")
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:REPLACE_THROTTLED"]
    guard.last_stop_replace_mono -= execution.STOP_REPLACE_MIN_INTERVAL_SECS
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:STOP_RESIZED"]
    (shrunk,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(shrunk.qty)) == 12 == _position_qty(sim, "NVDA")
    assert guard.child.id == str(shrunk.id) and _cover(farm, guards, "RTH") == []
    assert [call.name for call in sim.calls(kind=MUTATING)] == [
        "submit_order",
        "replace_order_by_id",
        "replace_order_by_id",
    ]
    # The resized stop is the one the guard ratchets, and the one the broker executes. The
    # resize was a replace a moment ago, so the one-second replace throttle (R14) is lifted.
    sim.set_price("NVDA", "104.00")
    guard.last_stop_replace_mono = 0.0
    guard.on_price(Decimal("104.00"), "RTH")
    (ratcheted,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(ratcheted.stop_price)) == guard.stop > Decimal(str(shrunk.stop_price))
    assert Decimal(str(ratcheted.qty)) == 12
    # A ratcheted stop is still the harness's own (the replacement keeps the id form): when
    # the position grows again it is resized, not joined by a second stop.
    assert str(ratcheted.client_order_id).startswith(f"stop-{guard.client_id}-")
    sim.open_position("NVDA", 2, "104.00")
    guard.last_stop_replace_mono = 0.0  # the ratchet was a replace a moment ago (R14)
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:STOP_RESIZED"]
    (regrown,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(regrown.qty)) == 14 and Decimal(str(regrown.stop_price)) == guard.stop
    sim.set_price("NVDA", str(guard.stop - Decimal("0.05")))
    assert sim.inspect_position("NVDA") is None


def test_premarket_to_rth_rollover_attaches_stop(tmp_path: Path) -> None:
    # 1. The pre-market attach is refused by an exhausted rate budget. That is counted, never
    #    silent, and retried; the position is tick-supervised meanwhile.
    sim = _live_sim(session="PRE_MARKET")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "deferred", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="PRE_MARKET")
    initial_stop = guard.stop
    before_open = datetime.now(UTC)
    for _ in range(lane.rate_budget.max_rpm):
        assert lane.rate_budget.try_consume(before_open.timestamp(), is_exit=True) is True
    for expected_count in (1, 2):
        assert execution.ensure_native_protective_stops(
            farm, guards, session="PRE_MARKET", now_utc=before_open
        ) == [f"{_LIVE_ACCOUNT}:NVDA:DEFERRED_RATE_BUDGET"]
        assert lane.counters["stop_attach_deferred_total"] == expected_count
    assert sim.mutating_call_count == 0 and _stop_cover_qty(sim, "NVDA") == 0

    # Pre-market prints ratchet the guard's own stop. At the regular open, with budget again,
    # the reconciler attaches the native stop at the ratcheted level and it is armed at once.
    sim.set_price("NVDA", "103.00")
    guard.on_price(Decimal("103.00"), "PRE_MARKET")
    assert guard.stop > initial_stop and sim.mutating_call_count == 0
    sim.set_session("RTH")
    at_open = before_open + timedelta(seconds=61)
    assert execution.ensure_native_protective_stops(
        farm, guards, session="RTH", now_utc=at_open
    ) == [f"{_LIVE_ACCOUNT}:NVDA:STOP_ATTACHED"]
    (stop,) = _resting_stops(sim, "NVDA")
    assert stop.status is OrderStatus.NEW and Decimal(str(stop.qty)) == 10
    assert Decimal(str(stop.stop_price)) == guard.stop
    # The broker owns the trigger now: a fall through the stop sells without a harness call.
    mutations = sim.mutating_call_count
    sim.set_price("NVDA", str(guard.stop - Decimal("0.05")))
    assert sim.inspect_position("NVDA") is None and sim.mutating_call_count == mutations

    # 2. A stop that has rested since pre-market keeps its price off-hours and is brought to
    #    the ratcheted level by the first reconcile of the regular session.
    resting_sim = _live_sim(session="PRE_MARKET")
    resting_sim.open_position("NVDA", 10, "100.00")
    resting_farm, resting_lane = _live_farm(tmp_path / "resting", resting_sim)
    resting_guards: dict[str, execution.Guard] = {}
    (resting_guard,) = execution.adopt_unmanaged_broker_positions(
        resting_farm, resting_guards, session="PRE_MARKET"
    )
    assert _cover(resting_farm, resting_guards, "PRE_MARKET") == [
        f"{_LIVE_ACCOUNT}:NVDA:STOP_ATTACHED"
    ]
    (queued,) = _resting_stops(resting_sim, "NVDA")
    assert queued.status is OrderStatus.ACCEPTED
    resting_sim.set_price("NVDA", "103.00")
    resting_guard.on_price(Decimal("103.00"), "PRE_MARKET")
    assert resting_guard.stop > Decimal(str(queued.stop_price))
    assert _cover(resting_farm, resting_guards, "PRE_MARKET") == []
    resting_sim.set_session("RTH")
    assert _cover(resting_farm, resting_guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:STOP_REPRICED"]
    (armed,) = _resting_stops(resting_sim, "NVDA")
    assert armed.status is OrderStatus.NEW and armed.replaces == queued.id
    assert Decimal(str(armed.stop_price)) == resting_guard.stop
    assert Decimal(str(armed.qty)) == 10 and resting_guard.child.id == str(armed.id)
    assert resting_lane.counters["stop_reprices"] == 1
    assert _cover(resting_farm, resting_guards, "RTH") == []


def _partially_filled_oto_entry(
    root: Path, wash_trade_model: Literal["assumed", "documented"]
) -> tuple[FaultInjectingAlpacaSim, pf.PaperFarmCoordinator, dict[str, execution.Guard], int]:
    """A paper OTO entry of which four shares have filled; its stop leg is still ``held``."""
    sim = FaultInjectingAlpacaSim(session="RTH", auto_fill=False, wash_trade_model=wash_trade_model)
    sim.set_price("NVDA", "100.00")
    sims = {
        acct: sim if acct == "ALPACA_PA1" else FaultInjectingAlpacaSim(session="RTH")
        for acct in pf.ALPACA_LANE_PROFILES
    }
    farm = pf.PaperFarmCoordinator(root, alpaca_clients=sims)
    lane = farm.lanes["ALPACA_PA1"]
    entry = lane.submit_guarded_entry(
        symbol="NVDA",
        side="LONG",
        ref_price=Decimal("100.00"),
        session="RTH",
        client_order_id="noesis-partial-oto",
    )
    assert entry is not None and entry["protective_stop_mode"] == "NATIVE_OTO_STOP"
    full_qty = int(entry["qty"])
    assert full_qty > 4
    sim.fill_partial(entry["broker_order_id"], 4)
    parent = sim.get_order_by_id(entry["broker_order_id"], GetOrderByIdRequest(nested=True))
    stop = Decimal(str(entry["stop_price"]))
    guard = execution.Guard(
        lane,
        "noesis-partial-oto",
        "NVDA",
        "LONG",
        stop,
        stop,
        Decimal("0.015"),
        "RTH",
        False,
        parent,
    )
    guard.fetch()
    assert guard.child.status.value == "held" and _position_qty(sim, "NVDA") == 4
    # The held leg protects nothing: by the coverage rule no share has a resting stop.
    assert _stop_cover_qty(sim, "NVDA") == 0
    return sim, farm, {"NVDA": guard}, full_qty


def test_partial_oto_fill_gets_standalone_stop(tmp_path: Path) -> None:
    # 1. The filled shares of a partially filled OTO parent get a standalone stop of their own
    #    lifecycle (here the broker accepts a stop next to the open parent).
    sim, farm, guards, full_qty = _partially_filled_oto_entry(tmp_path / "accepted", "assumed")
    lane = farm.lanes["ALPACA_PA1"]
    guard = guards["NVDA"]
    assert _cover(farm, guards, "RTH") == ["ALPACA_PA1:NVDA:STOP_ATTACHED"]
    (standalone,) = _resting_stops(sim, "NVDA")
    assert standalone.type is OrderType.STOP and standalone.side is OrderSide.SELL
    assert Decimal(str(standalone.qty)) == 4 and Decimal(str(standalone.stop_price)) == guard.stop
    assert standalone.client_order_id.startswith(f"stop-{guard.parent.id}-")
    assert _cover(farm, guards, "RTH") == [] and lane.counters["stop_attaches"] == 1

    # When the parent fills completely its own stop leg arms for the full size. The standalone
    # stop is released, so the resting stop quantity never stays above the position.
    sim.fill(guard.parent.id)
    assert _position_qty(sim, "NVDA") == full_qty
    assert _cover(farm, guards, "RTH") == ["ALPACA_PA1:NVDA:STOP_RELEASED"]
    (leg,) = _resting_stops(sim, "NVDA")
    assert leg.id != standalone.id and Decimal(str(leg.qty)) == full_qty
    assert sim.inspect_order(standalone.id).status is OrderStatus.CANCELED
    assert _stop_cover_qty(sim, "NVDA") == _position_qty(sim, "NVDA")
    assert _cover(farm, guards, "RTH") == [] and lane.counters["stop_releases"] == 1

    # 2. By the broker's documented self-trade rule a stop against the still-open parent is
    #    refused (HTTP 403). That is its own counted outcome and backs off: it is not retried
    #    on every tick, and the tick-supervised guard still gets the filled shares out.
    refused_sim, refused_farm, refused_guards, _ = _partially_filled_oto_entry(
        tmp_path / "refused", "documented"
    )
    refused_lane = refused_farm.lanes["ALPACA_PA1"]
    refused_guard = refused_guards["NVDA"]
    assert _cover(refused_farm, refused_guards, "RTH") == [
        "ALPACA_PA1:NVDA:BLOCKED_BY_OPPOSITE_SIDE_ORDER"
    ]
    assert refused_lane.counters["stop_attach_blocked_opposite_side"] == 1
    assert refused_lane.counters["stop_attach_faults"] == 1
    assert [call.outcome for call in refused_sim.calls("submit_order")] == ["ok", "error:403"]
    for _ in range(3):
        assert _cover(refused_farm, refused_guards, "RTH") == ["ALPACA_PA1:NVDA:BACKOFF"]
    assert len(refused_sim.calls("submit_order")) == 2 and _stop_cover_qty(refused_sim, "NVDA") == 0
    breach = refused_guard.stop - Decimal("0.20")
    refused_sim.set_price("NVDA", str(breach))
    _tick(refused_guard, breach, "RTH")  # cancels the open parent (its held leg goes with it)
    _tick(refused_guard, breach, "RTH")  # sells what filled, from broker truth
    exit_order = refused_sim.calls("submit_order")[-1].payload
    assert exit_order["type"] == "market" and exit_order["side"] == "sell"
    assert Decimal(str(exit_order["qty"])) == 4
    assert exit_order["client_order_id"] == f"exit-{refused_guard.parent.id}"


def test_underwater_adoption_is_not_sold_on_first_print(tmp_path: Path) -> None:
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    sim.set_price("NVDA", "90.00")  # ten percent under water when the harness first sees it
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")

    # The stop is anchored at the first-seen price (within the 2.00% cap of it), not at a
    # fill price the market left long ago, and the anchor is on record in the ledger.
    assert guard.adoption_anchor_price == Decimal("90.00")
    assert Decimal("88.20") <= guard.stop < Decimal("90.00") and guard.initial == guard.stop
    row = _tier_ledger_row(lane, guard.client_id)
    assert row is not None and row[0] == "OWNER_ADOPTED" and row[4] == 10 and row[7] == 90
    (stop,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(stop.stop_price)) == guard.stop and stop.status is OrderStatus.NEW
    snapshot = guard.to_position_snapshot("RTH")
    assert snapshot["entry_price"] == 100.0 and snapshot["adoption_anchor_price"] == 90.0

    # First prints at and around the first-seen price (the old stop, two percent under the
    # 100.00 fill, was 98.00: every one of them is far through it). Nothing is sold or sent.
    for print_px in ("90.00", "89.95", "90.10", "89.90"):
        sim.set_price("NVDA", print_px)
        guard.on_price(Decimal(print_px), "RTH")
        assert Decimal(print_px) < Decimal("98.00")
    assert guard.triggered is False and _position_qty(sim, "NVDA") == 10
    assert [call.name for call in sim.calls(kind=MUTATING)] == ["submit_order"]

    # Restart with the price lower still: the anchor is read back, not taken again, so the
    # stop is exactly where it was. The position is not adopted a second time.
    sim.set_price("NVDA", "89.00")
    restored = execution.restore_guards(farm, _broker_positions(sim))
    again = restored["NVDA"]
    assert again.client_id == guard.client_id and again.lifecycle_id == guard.lifecycle_id
    assert again.adoption_anchor_price == Decimal("90.00")
    assert again.stop == guard.stop and again.initial == guard.initial
    assert again.child.id == str(stop.id)
    assert execution.adopt_unmanaged_broker_positions(farm, restored, session="RTH") == []
    assert _open_ledger_rows(lane) == 1
    anchored = _tier_ledger_row(lane, guard.client_id)
    assert anchored is not None and anchored[7] == 90

    # From the anchor the stop only ever moves up as the price recovers.
    last_stop = again.stop
    for print_px in ("90.50", "92.00", "94.00"):
        sim.set_price("NVDA", print_px)
        again.on_price(Decimal(print_px), "RTH")
        assert again.stop >= last_stop
        last_stop = again.stop
    assert again.stop > guard.stop and again.triggered is False
    # A real break of the anchored, ratcheted stop still gets the position out.
    sim.set_price("NVDA", str(again.stop - Decimal("0.05")))
    assert sim.inspect_position("NVDA") is None


def test_partial_exit_on_adopted_guard_reaches_flat(tmp_path: Path) -> None:
    sim = _live_sim(session="AFTER_HOURS", auto_fill=False)
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="AFTER_HOURS")
    breach = guard.stop - Decimal("0.20")
    sim.set_price("NVDA", str(breach))

    # The first exit order sells 4 of 10 and then dies (cancelled from outside).
    _tick(guard, breach, "AFTER_HOURS")
    (first_exit,) = sim.inspect_orders("open", "NVDA")
    assert first_exit.client_order_id == f"exit-{guard.client_id}"
    sim.fill_partial(first_exit.id, 4)
    sim.external_cancel(first_exit.id)
    assert _position_qty(sim, "NVDA") == 6

    # The next exit is for what the broker says is held: 6, not "filled minus covered" (2).
    _tick(guard, breach, "AFTER_HOURS")
    (second_exit,) = sim.inspect_orders("open", "NVDA")
    assert second_exit.client_order_id == f"exit-{guard.client_id}-1"
    assert Decimal(str(second_exit.qty)) == 6
    sim.fill_partial(second_exit.id, 5)
    sim.external_cancel(second_exit.id)
    assert _position_qty(sim, "NVDA") == 1

    _tick(guard, breach, "AFTER_HOURS")
    (third_exit,) = sim.inspect_orders("open", "NVDA")
    assert Decimal(str(third_exit.qty)) == 1
    sim.fill(third_exit.id)
    assert sim.inspect_position("NVDA") is None
    assert [Decimal(str(call.payload["qty"])) for call in sim.calls("submit_order")] == [10, 6, 1]
    assert [call.outcome for call in sim.calls(kind=MUTATING)] == ["ok", "ok", "ok"]

    # Flat by broker truth: the lifecycle and its ledger row are closed. A closed lifecycle
    # never acts again, even when the owner buys the symbol back the next moment.
    _tick(guard, breach, "AFTER_HOURS")
    assert guard.lifecycle_closed is True and _open_ledger_rows(lane) == 0
    sim.open_position("NVDA", 3, "99.00")
    _tick(guard, breach, "AFTER_HOURS")
    assert len(sim.calls("submit_order")) == 3 and _position_qty(sim, "NVDA") == 3
    assert _cover(farm, guards, "AFTER_HOURS") == [f"{_LIVE_ACCOUNT}:NVDA:LIFECYCLE_ENDED"]

    # Shares another resting order holds: the exit sells what the broker says is free, reports
    # the rest as blocked (a protection fault, not an exception), and finishes once the order
    # that holds them can be seen and released.
    held_sim = _live_sim(session="AFTER_HOURS")
    held_sim.open_position("MU", 10, "100.00")
    owner_limit = held_sim.submit_order(
        LimitOrderRequest(
            symbol="MU",
            qty=3,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=130.0,
        )
    )
    held_sim.reset_call_log()
    held_farm, _held_lane = _live_farm(tmp_path / "held", held_sim)
    held_guards: dict[str, execution.Guard] = {}
    (held_guard,) = execution.adopt_unmanaged_broker_positions(
        held_farm, held_guards, session="AFTER_HOURS"
    )
    held_breach = held_guard.stop - Decimal("0.20")
    held_sim.set_price("MU", str(held_breach))
    with mock.patch.object(held_sim, "get_orders", return_value=[]):  # the order list lags
        _tick(held_guard, held_breach, "AFTER_HOURS")
        assert _position_qty(held_sim, "MU") == 3
        _tick(held_guard, held_breach, "AFTER_HOURS")
        assert held_guard.protection_fault_code == "EXIT_BLOCKED_NO_QTY_AVAILABLE"
    assert [Decimal(str(call.payload["qty"])) for call in held_sim.calls("submit_order")] == [7]
    _tick(held_guard, held_breach, "AFTER_HOURS")  # sees the holding order and releases it
    assert held_sim.inspect_order(owner_limit.id).status is OrderStatus.CANCELED
    _tick(held_guard, held_breach, "AFTER_HOURS")
    assert held_sim.inspect_position("MU") is None
    assert [Decimal(str(call.payload["qty"])) for call in held_sim.calls("submit_order")] == [7, 3]
    assert {call.outcome for call in held_sim.calls(kind=MUTATING)} == {"ok"}
    assert held_guard.protection_fault_code is None

    # A fractional remainder: the guard sells the whole shares it supervises and ends there. It
    # does not come back on every tick for a fraction it will not sell; the remainder is
    # reported as a fractional-only position and is not adopted again.
    frac_sim = _live_sim(session="AFTER_HOURS")
    frac_sim.open_position("AMD", "7.5", "100.00")
    frac_farm, frac_lane = _live_farm(tmp_path / "frac", frac_sim)
    frac_guards: dict[str, execution.Guard] = {}
    (frac_guard,) = execution.adopt_unmanaged_broker_positions(
        frac_farm, frac_guards, session="AFTER_HOURS"
    )
    frac_breach = frac_guard.stop - Decimal("0.20")
    frac_sim.set_price("AMD", str(frac_breach))
    _tick(frac_guard, frac_breach, "AFTER_HOURS")
    assert [Decimal(str(call.payload["qty"])) for call in frac_sim.calls("submit_order")] == [7]
    assert _position_qty(frac_sim, "AMD") == Decimal("0.5")
    _tick(frac_guard, frac_breach, "AFTER_HOURS")
    assert frac_guard.lifecycle_closed is True and _open_ledger_rows(frac_lane) == 0
    assert frac_guard.fractional_remainder == Decimal("0.5")
    _tick(frac_guard, frac_breach, "AFTER_HOURS")
    assert frac_sim.mutating_call_count == 1 and frac_guard.protection_fault_code is None
    assert _cover(frac_farm, frac_guards, "AFTER_HOURS") == [f"{_LIVE_ACCOUNT}:AMD:LIFECYCLE_ENDED"]
    frac_rebuilt = execution.restore_guards(
        frac_farm, _broker_positions(frac_sim), existing_guards=frac_guards
    )
    frac_skipped: dict[str, dict[str, Any]] = {}
    assert (
        execution.adopt_unmanaged_broker_positions(
            frac_farm, frac_rebuilt, session="AFTER_HOURS", skipped=frac_skipped
        )
        == []
    )
    assert frac_rebuilt == {}
    assert frac_skipped[f"{_LIVE_ACCOUNT}:AMD"] == {
        "reason": "FRACTIONAL_ONLY",
        "qty": "0.5",
        "side": "LONG",
    }


def test_rejected_exit_escalates_and_never_raises(tmp_path: Path) -> None:
    # 1. Two rejected exits, then one refused outright: each climbs a rung (1x, 2x, 3x the
    #    session collar), then the exit becomes a close-position-equivalent market order.
    sim = _live_sim(session="PRE_MARKET")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "ladder", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="PRE_MARKET")
    breach = guard.stop - Decimal("0.20")
    sim.set_price("NVDA", str(breach))

    sim.reject_next_order(times=2)
    _tick(guard, breach, "PRE_MARKET")
    assert guard.protection_fault_code == "EXIT_REJECTED" and guard.protection_fault_count == 1
    _tick(guard, breach, "PRE_MARKET")
    sim.fail_next("submit_order", 403)
    _tick(guard, breach, "PRE_MARKET")
    assert guard.protection_fault_code == "EXIT_REFUSED_HTTP_403"
    _tick(guard, breach, "PRE_MARKET")
    assert guard.protection_fault_code == "EXIT_ESCALATED_CLOSE_POSITION"
    assert guard.protection_fault_count == 4 and lane.counters["protection_faults"] == 4

    sent = sim.calls("submit_order")
    assert [call.outcome for call in sent] == ["ok", "ok", "fault:403", "ok"]
    assert [call.payload["type"] for call in sent] == ["limit", "limit", "limit", "market"]
    assert [Decimal(str(call.payload["limit_price"])) for call in sent[:3]] == [
        _collared(guard, breach, "PRE_MARKET", rung) for rung in (1, 2, 3)
    ]
    assert _collared(guard, breach, "PRE_MARKET", 3) < _collared(guard, breach, "PRE_MARKET", 1)
    stem = f"exit-{guard.client_id}"
    assert [call.payload["client_order_id"] for call in sent] == [
        stem,
        f"{stem}-1",
        f"{stem}-2",
        f"{stem}-2",  # the refused order never existed, so its id was still free
    ]
    close_order = sent[-1].payload
    assert close_order["time_in_force"] == "day" and Decimal(str(close_order["qty"])) == 10
    assert not close_order.get("extended_hours")
    # Bounded: while that order waits for the open, further ticks send nothing more.
    for _ in range(5):
        _tick(guard, breach, "PRE_MARKET")
    assert len(sim.calls("submit_order")) == 4 and sim.calls("cancel_order_by_id") == []
    assert guard.triggered is True and _position_qty(sim, "NVDA") == 10
    assert guard.to_position_snapshot("PRE_MARKET")["protection_fault_code"] == (
        "EXIT_ESCALATED_CLOSE_POSITION"
    )
    # It fills at the open; the guard then sees flat and the fault is gone with the lifecycle.
    sim.set_session("RTH")
    assert sim.inspect_position("NVDA") is None
    _tick(guard, breach, "RTH")
    assert guard.lifecycle_closed is True and guard.protection_fault_code is None
    assert _open_ledger_rows(lane) == 0

    # 2. A broker that rejects every exit: the ladder ends after a fixed number of orders with
    #    a standing protection fault. No tick raises, and none sends a sixteenth order.
    stuck_sim = _live_sim(session="PRE_MARKET")
    stuck_sim.open_position("NVDA", 10, "100.00")
    stuck_farm, stuck_lane = _live_farm(tmp_path / "stuck", stuck_sim)
    stuck_guards: dict[str, execution.Guard] = {}
    (stuck,) = execution.adopt_unmanaged_broker_positions(
        stuck_farm, stuck_guards, session="PRE_MARKET"
    )
    stuck_sim.set_price("NVDA", str(breach))
    stuck_sim.reject_next_order(times=100)
    ladder_orders = execution.MAX_SUPERVISED_EXIT_ATTEMPTS + execution.CLOSE_RUNG_MAX_ATTEMPTS
    for _ in range(ladder_orders + 10):
        _tick(stuck, breach, "PRE_MARKET")
    rejected = stuck_sim.calls("submit_order")
    assert len(rejected) == ladder_orders == 15
    assert [call.payload["type"] for call in rejected] == ["limit"] * 3 + ["market"] * 12
    assert len({call.payload["client_order_id"] for call in rejected}) == ladder_orders
    assert stuck.protection_fault_code == "EXIT_LADDER_EXHAUSTED"
    assert stuck.protection_fault_count >= execution.PROTECTION_FAULT_ALERT_COUNT
    assert _position_qty(stuck_sim, "NVDA") == 10 and stuck.lifecycle_closed is False
    assert stuck_lane.counters["protection_faults"] == stuck.protection_fault_count

    # 3. A broker error on the exit path (a failed read, a transient 5xx on the order) is a
    #    protection fault of the guard as well, retried on the next tick. It is not raised, and
    #    a transient error does not climb the ladder: the retry is at the same collar.
    flaky_sim = _live_sim(session="PRE_MARKET")
    flaky_sim.open_position("NVDA", 10, "100.00")
    flaky_farm, _flaky_lane = _live_farm(tmp_path / "flaky", flaky_sim)
    flaky_guards: dict[str, execution.Guard] = {}
    (flaky,) = execution.adopt_unmanaged_broker_positions(
        flaky_farm, flaky_guards, session="PRE_MARKET"
    )
    flaky_sim.set_price("NVDA", str(breach))
    flaky_sim.fail_next("get_open_position", 500)
    _tick(flaky, breach, "PRE_MARKET")
    assert flaky.protection_fault_code == "EXIT_BROKER_ERROR_500"
    assert flaky_sim.mutating_call_count == 0
    flaky_sim.fail_next("submit_order", 503)
    _tick(flaky, breach, "PRE_MARKET")
    assert flaky.protection_fault_code == "EXIT_REFUSED_HTTP_503"
    assert flaky.exit_refusal_count == 0 and flaky.protection_fault_count == 2
    _tick(flaky, breach, "PRE_MARKET")
    retried = flaky_sim.calls("submit_order")
    assert [call.outcome for call in retried] == ["fault:503", "ok"]
    assert [Decimal(str(call.payload["limit_price"])) for call in retried] == [
        _collared(flaky, breach, "PRE_MARKET", 1)
    ] * 2
    assert flaky.protection_fault_code is None and flaky_sim.inspect_position("NVDA") is None


def test_client_order_ids_unique_across_lifecycles(tmp_path: Path) -> None:
    sim = _live_sim(session="RTH")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    lifecycles: list[execution.Guard] = []

    # The same symbol, size and average price three times over: the case in which an id built
    # from account, symbol, side and price alone comes round again.
    for _ in range(3):
        sim.set_price("NVDA", "100.00")
        sim.open_position("NVDA", 10, "100.00")
        (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
        assert _stop_cover_qty(sim, "NVDA") == 10
        guard.reverse_flatten = True
        _tick(guard, "100.00", "RTH")  # releases the resting stop
        _tick(guard, "100.00", "RTH")  # market exit
        assert sim.inspect_position("NVDA") is None
        _tick(guard, "100.00", "RTH")  # sees flat
        assert guard.lifecycle_closed is True
        guards = execution.restore_guards(farm, _broker_positions(sim), existing_guards=guards)
        assert guards == {}
        lifecycles.append(guard)

    harness_ids = [
        str(order.client_order_id)
        for order in sim.inspect_orders("all", "NVDA")
        if not str(order.client_order_id).startswith("owner-")
    ]
    assert len(harness_ids) == len(set(harness_ids)) == 6
    assert len({guard.lifecycle_id for guard in lifecycles}) == 3
    assert len({guard.client_id for guard in lifecycles}) == 3
    for guard in lifecycles:
        assert guard.client_id.endswith(f"-{guard.lifecycle_id}")
        own = [cid for cid in harness_ids if guard.lifecycle_id in cid]
        assert sorted(cid.split("-", 1)[0] for cid in own) == ["exit", "stop"]
    # The broker never had to refuse a recycled id (HTTP 422), or anything else.
    assert {call.outcome for call in sim.calls(kind=MUTATING)} == {"ok"}
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
        rows = db.execute(
            "SELECT client_order_id, closed_at IS NOT NULL FROM paper_entry_decisions"
        ).fetchall()
    assert sorted(row[0] for row in rows) == sorted(guard.client_id for guard in lifecycles)
    assert all(row[1] for row in rows)


def test_side_flip_at_the_broker_rebuilds_the_guard_for_the_new_side(tmp_path: Path) -> None:
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (long_guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    (long_stop,) = _resting_stops(sim, "NVDA")

    # The owner sells fifteen by hand: the account is short five, with the long's sell stop
    # still resting (it would now add to the short).
    sim.open_position("NVDA", -15, "100.00")
    assert _position_qty(sim, "NVDA") == -5
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:SIDE_FLIP"]
    assert long_guard.side_flip_detected is True
    assert len(sim.calls(kind=MUTATING)) == 1  # the reconciler touched nothing

    # The rebuild ends the long lifecycle (stale stop cancelled, ledger row closed) and adopts
    # the short as a new lifecycle with a buy stop above the market.
    guards = execution.restore_guards(farm, _broker_positions(sim), existing_guards=guards)
    assert guards == {} and _open_ledger_rows(lane) == 0
    assert sim.inspect_order(long_stop.id).status is OrderStatus.CANCELED
    assert lane.counters["side_flip_rebuilds"] == 1
    (short_guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    assert short_guard.side == "SHORT" and short_guard.lifecycle_id != long_guard.lifecycle_id
    assert short_guard.stop > Decimal("100.00")
    (cover_stop,) = _resting_stops(sim, "NVDA")
    assert cover_stop.side is OrderSide.BUY and Decimal(str(cover_stop.qty)) == 5
    assert Decimal(str(cover_stop.stop_price)) == short_guard.stop
    assert _cover(farm, guards, "RTH") == [] and _open_ledger_rows(lane) == 1

    # A guard that meets the other side on its own exit path stands down instead of trading it.
    stale = _live_sim(session="AFTER_HOURS")
    stale.open_position("AMD", 10, "100.00")
    stale_farm, _stale_lane = _live_farm(tmp_path / "stale", stale)
    stale_guards: dict[str, execution.Guard] = {}
    (stale_guard,) = execution.adopt_unmanaged_broker_positions(
        stale_farm, stale_guards, session="AFTER_HOURS"
    )
    stale.open_position("AMD", -14, "100.00")
    _tick(stale_guard, stale_guard.stop - Decimal("0.20"), "AFTER_HOURS")
    assert stale_guard.side_flip_detected is True and stale.mutating_call_count == 0


def test_adoption_isolation_and_native_stop_upgrade(tmp_path: Path) -> None:
    # 1. Eligibility and isolation. Only US equities with a whole share and a tradable asset
    #    are adopted; the rest is reported. A refused stop costs no other position its guard.
    sim = _live_sim(session="RTH")
    sim.open_position("AMD", 10, "100.00")
    sim.open_position("FRAC", "0.5", "40.00")
    sim.open_position("HALF", "7.5", "20.00")
    sim.open_position("HALT", 4, "30.00")
    sim.set_asset("HALT", tradable=False)
    sim.open_position("NVDA", 5, "200.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    positions = _broker_positions(sim)
    positions[_LIVE_ACCOUNT]["BTCUSD"] = SimpleNamespace(
        symbol="BTCUSD",
        qty="0.25",
        side="long",
        avg_entry_price="60000",
        current_price="60000",
        asset_class="crypto",
    )
    guards: dict[str, execution.Guard] = {}
    skipped: dict[str, dict[str, Any]] = {}
    sim.fail_next("submit_order", 403)  # the first native stop (AMD) is refused
    adopted = execution.adopt_unmanaged_broker_positions(
        farm, guards, positions=positions, session="RTH", skipped=skipped
    )
    assert [guard.symbol for guard in adopted] == ["AMD", "HALF", "NVDA"] == list(guards)
    assert {key: value["reason"] for key, value in skipped.items()} == {
        f"{_LIVE_ACCOUNT}:FRAC": "FRACTIONAL_ONLY",
        f"{_LIVE_ACCOUNT}:HALT": "ASSET_NOT_TRADABLE",
        f"{_LIVE_ACCOUNT}:BTCUSD": "NOT_US_EQUITY",
    }
    assert [(call.payload["symbol"], call.outcome) for call in sim.calls("submit_order")] == [
        ("AMD", "fault:403"),
        ("HALF", "ok"),
        ("NVDA", "ok"),
    ]
    assert guards["AMD"].child is None and lane.counters["adoption_stop_attach_faults"] == 1
    # The whole-share part is stopped, the fractional remainder is reported.
    assert _stop_cover_qty(sim, "HALF") == 7 and guards["HALF"].fractional_remainder == Decimal(
        "0.5"
    )
    assert guards["HALF"].to_position_snapshot("RTH")["unprotected_fractional_qty"] == 0.5
    assert _stop_cover_qty(sim, "NVDA") == 5 and _open_ledger_rows(lane) == 3
    for guard in adopted:
        row = _tier_ledger_row(lane, guard.client_id)
        assert row is not None and row[0] == "OWNER_ADOPTED" and row[7] is None
    # The live ledger lives in the live directory only.
    assert lane.duckdb_path.parent == tmp_path / "live"

    # 2. Native stop upgrade: the guard left tick-supervised by the refusal is not retried on
    #    every pass, and gets its resting stop once the back-off is over.
    assert _cover(farm, guards, "RTH") == [
        f"{_LIVE_ACCOUNT}:AMD:BACKOFF",
        f"{_LIVE_ACCOUNT}:FRAC:UNMANAGED",
        f"{_LIVE_ACCOUNT}:HALT:UNMANAGED",
    ]
    assert len(sim.calls("submit_order")) == 3
    guards["AMD"].next_stop_attach_mono = 0.0
    assert f"{_LIVE_ACCOUNT}:AMD:STOP_ATTACHED" in _cover(farm, guards, "RTH")
    assert _stop_cover_qty(sim, "AMD") == 10 and guards["AMD"].child is not None
    sim.set_price("AMD", "104.00")
    guards["AMD"].on_price(Decimal("104.00"), "RTH")
    (amd_stop,) = _resting_stops(sim, "AMD")
    assert Decimal(str(amd_stop.stop_price)) == guards["AMD"].stop > Decimal("100.00")
    assert lane.counters["stop_replaces"] == 1

    # 3. What counts as an existing stop: right side, a stop type, resting (GTC), and its
    #    quantity. Partial cover is topped up; cover that exists is never stacked upon.
    owned = _live_sim(session="RTH")
    for symbol, qty in (("MU", 10), ("ARM", 10), ("SOXL", 10), ("SOXS", 24), ("TQQQ", -5)):
        owned.open_position(symbol, qty, "100.00")
    owned.submit_order(  # a resting sell limit: not a stop, but it holds three shares
        LimitOrderRequest(
            symbol="MU",
            qty=3,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=130.0,
        )
    )
    owned.submit_order(  # a DAY stop lapses at the close: not coverage
        StopOrderRequest(
            symbol="ARM",
            qty=10,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
            stop_price=95.0,
        )
    )
    partial_stop = owned.submit_order(_sell_stop("SOXL", 4, 95.0))  # covers 4 of 10
    trailing = owned.submit_order(  # the owner's native trailing stop covers all 24
        TrailingStopOrderRequest(
            symbol="SOXS",
            qty=24,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            trail_percent=1.5,
        )
    )
    owned.submit_order(  # a sell stop under a short is the wrong side to be its cover
        StopOrderRequest(
            symbol="TQQQ",
            qty=1,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            stop_price=90.0,
        )
    )
    owned.reset_call_log()
    find = execution._find_open_broker_stop
    assert find(owned, "MU", "sell") is None and find(owned, "ARM", "sell") is None
    assert find(owned, "SOXS", "sell") is None and find(owned, "TQQQ", "buy") is None
    found = find(owned, "SOXL", "sell")
    assert found.id == str(partial_stop.id) and Decimal(found.qty) == 4

    owned_farm, owned_lane = _live_farm(tmp_path / "owned", owned)
    owned_guards: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(owned_farm, owned_guards, session="RTH")
    assert sorted(owned_guards) == ["ARM", "MU", "SOXL", "SOXS", "TQQQ"]
    assert [(call.payload["symbol"], call.outcome) for call in owned.calls("submit_order")] == [
        ("MU", "ok"),  # the seven shares the owner's limit leaves free
        ("SOXL", "ok"),  # the six shares the owner's stop leaves open
        ("TQQQ", "error:403"),  # blocked by the opposite-side stop
    ]
    assert _stop_cover_qty(owned, "MU") == 7 and _stop_cover_qty(owned, "SOXL") == 10
    assert (
        _stop_cover_qty(owned, "SOXS") == 24 and _resting_stops(owned, "SOXS")[0].id == trailing.id
    )
    assert owned_guards["SOXS"].child is None  # a trailing stop is cover, not a stop to re-price
    assert owned_lane.counters["stop_attach_blocked_opposite_side"] == 1
    assert owned_guards["TQQQ"].side == "SHORT" and owned_guards["TQQQ"].child is None
    actions = _cover(owned_farm, owned_guards, "RTH")
    assert f"{_LIVE_ACCOUNT}:ARM:UNCOVERED_NO_QTY_AVAILABLE" in actions
    assert f"{_LIVE_ACCOUNT}:MU:UNCOVERED_NO_QTY_AVAILABLE" in actions
    assert f"{_LIVE_ACCOUNT}:TQQQ:BACKOFF" in actions and len(owned.calls("submit_order")) == 3

    # After the close the DAY stop is gone: its shares are free and get their resting stop.
    owned.set_session("AFTER_HOURS")
    assert f"{_LIVE_ACCOUNT}:ARM:STOP_ATTACHED" in _cover(owned_farm, owned_guards, "AFTER_HOURS")
    assert _stop_cover_qty(owned, "ARM") == 10
    # Off-hours the tick supervision owns the exit of the trailing-covered position: it frees
    # the shares the trailing stop holds, then sells them with the collared limit.
    soxs = owned_guards["SOXS"]
    breach = soxs.stop - Decimal("0.20")
    owned.set_price("SOXS", str(breach))
    _tick(soxs, breach, "AFTER_HOURS")
    assert owned.inspect_order(trailing.id).status is OrderStatus.CANCELED
    _tick(soxs, breach, "AFTER_HOURS")
    assert owned.inspect_position("SOXS") is None

    # 4. Adoption needs no scanner file: a position opened by hand while the supervisor runs
    #    is adopted and stopped on the loop's own tick, and only the live directory is written.
    timer_sim = _live_sim(session="PRE_MARKET")
    run = _run_live_supervisor_on_timer(
        tmp_path / "timer",
        timer_sim,
        session="PRE_MARKET",
        script={2: lambda: timer_sim.open_position("PLTR", 6, "50.00")},
        ticks=3,
    )
    status = run.status
    assert status["managed_symbols"] == ["PLTR"] and status["entry_fault"] is False
    assert status["adopted_external_positions_count"] == 1 and status["last_trade_utc"] == {}
    assert status["paper_only"] is False and status["stop_coverage"]["reconcile_faults"] == 0
    (pltr_stop,) = _resting_stops(timer_sim, "PLTR")
    assert pltr_stop.status is OrderStatus.ACCEPTED and Decimal(str(pltr_stop.qty)) == 6
    assert run.lane.counters["adopted_external"] == 1 and _open_ledger_rows(run.lane) == 1
    assert run.account_refreshes == 1  # at start only: the timer path reads the broker itself
    assert list(run.shared_dir.iterdir()) == []
    assert run.lane.duckdb_path.parent == run.live_dir

    # A position that is gone from the account ends its lifecycle on the next tick as well:
    # the guard and its ledger row go, and the stop it left resting is released (with nothing
    # held it would open a short when it triggers).
    flat_sim = _live_sim(session="PRE_MARKET")
    flat_sim.open_position("PLTR", 6, "50.00")
    flat_run = _run_live_supervisor_on_timer(
        tmp_path / "flat",
        flat_sim,
        session="PRE_MARKET",
        script={2: lambda: flat_sim.open_position("PLTR", -6, "50.00")},
        ticks=3,
    )
    assert flat_sim.inspect_position("PLTR") is None
    assert flat_run.status["managed_symbols"] == [] and _open_ledger_rows(flat_run.lane) == 0
    assert flat_sim.inspect_orders("open", "PLTR") == []
    assert flat_run.lane.counters["stop_orphan_releases"] == 1
    assert [call.name for call in flat_sim.calls(kind=MUTATING)] == [
        "submit_order",
        "cancel_order_by_id",
    ]
    assert flat_run.status["stop_coverage"]["reconcile_faults"] == 0

    # One wrong position list (it shows nothing) does not cost a held position its stop: the
    # stop is released only once its guard is gone, and that takes a second read of the
    # positions by the rebuild.
    glitch_sim = _live_sim(session="PRE_MARKET")
    glitch_sim.open_position("PLTR", 6, "50.00")
    glitch_farm, _glitch_lane = _live_farm(tmp_path / "glitch", glitch_sim)
    glitch_guards: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(glitch_farm, glitch_guards, session="PRE_MARKET")
    assert _cover(glitch_farm, glitch_guards, "PRE_MARKET") == [
        f"{_LIVE_ACCOUNT}:PLTR:STOP_ATTACHED"
    ]
    with mock.patch.object(glitch_sim, "get_all_positions", return_value=[]):
        assert _cover(glitch_farm, glitch_guards, "PRE_MARKET") == [f"{_LIVE_ACCOUNT}:PLTR:FLAT"]
    assert _stop_cover_qty(glitch_sim, "PLTR") == 6
    assert _cover(glitch_farm, glitch_guards, "PRE_MARKET") == []
    assert [call.name for call in glitch_sim.calls(kind=MUTATING)] == ["submit_order"]

    # A reconcile that fails in a way nobody foresaw is counted and tried again on the next
    # tick. It neither stops the supervisor nor latches the entry fault, and the position the
    # failed tick should have covered is adopted and stopped one tick later.
    faulty_sim = _live_sim(session="PRE_MARKET")
    read_positions = faulty_sim.get_all_positions
    armed = {"fail": False}

    def positions_broken_once() -> list[Any]:
        if armed["fail"]:
            armed["fail"] = False
            raise RuntimeError("unforeseen broker client failure")
        return read_positions()

    def owner_buys_as_the_read_breaks() -> None:
        faulty_sim.open_position("PLTR", 6, "50.00")
        armed["fail"] = True

    with mock.patch.object(faulty_sim, "get_all_positions", positions_broken_once):
        faulty_run = _run_live_supervisor_on_timer(
            tmp_path / "faulty",
            faulty_sim,
            session="PRE_MARKET",
            script={2: owner_buys_as_the_read_breaks},
            ticks=4,
        )
    assert armed["fail"] is False
    assert faulty_run.status["stop_coverage"]["reconcile_faults"] == 1
    assert faulty_run.status["entry_fault"] is False
    assert faulty_run.status["managed_symbols"] == ["PLTR"]
    assert _stop_cover_qty(faulty_sim, "PLTR") == 6


# ---------- supervisor pass 3: data plane (R2), regime flip (R7), faults (R13), rate (R14),
# ---------- coverage record (R12) and event-driven adoption, against the simulator

_COVERAGE_MANIFEST = "lossless_coverage_manifest.json"
_PA1 = "ALPACA_PA1"


class _P3Stream:
    """Market-data stream of a ``_P3Drive``: silent unless a script step delivers a print.

    Its first ``drops`` runs raise, as a dropped socket would.
    """

    def __init__(self, drive: _P3Drive, drops: int, quits: bool = False) -> None:
        self.drive = drive
        self.drops = drops
        self.quits = quits  # run() returns at once, as upstream does without a subscription
        self.runs = 0
        self.callback: Callable[[Any], Any] | None = None

    def subscribe_trades(self, callback: Callable[[Any], Any], *symbols: str) -> None:
        self.callback = callback
        self.drive.ever_subscribed.update(symbols)

    def unsubscribe_trades(self, *symbols: str) -> None:
        del symbols

    def run(self) -> None:
        self.runs += 1
        if self.runs <= self.drops:
            raise ConnectionError("scripted socket drop")
        if self.quits:
            return
        assert self.drive.finished.wait(60), "the supervisor loop never finished its ticks"

    def stop(self) -> None:
        self.drive.finished.set()

    def deliver(self, symbol: str, price: Decimal | str, stamp: datetime | None = None) -> None:
        """One trade print, handed to the supervisor's own callback and processed before return."""
        assert self.callback is not None
        trade = SimpleNamespace(
            symbol=symbol, price=float(Decimal(str(price))), timestamp=stamp or datetime.now(UTC)
        )
        asyncio.run(self.callback(trade))


class _P3TradeUpdates:
    """Trade-update stream of a ``_P3Drive``; its first ``drops`` runs raise."""

    def __init__(self, drive: _P3Drive, drops: int) -> None:
        self.drive = drive
        self.drops = drops
        self.runs = 0
        self.handler: Callable[[Any], Any] | None = None

    def subscribe_trade_updates(self, handler: Callable[[Any], Any]) -> None:
        self.handler = handler

    def run(self) -> None:
        self.runs += 1
        if self.runs <= self.drops:
            raise ConnectionError("scripted socket drop")
        assert self.drive.finished.wait(60), "the supervisor loop never finished its ticks"

    def stop(self) -> None:
        self.drive.finished.set()

    def emit(self, event: str, symbol: str) -> None:
        assert self.handler is not None
        asyncio.run(
            self.handler(SimpleNamespace(event=event, order=SimpleNamespace(symbol=symbol)))
        )


class _P3Drive:
    """Drives ``run_execution`` tick by tick; a script step plays the market, owner or broker.

    ``script[n]`` runs just before the loop's n-th tick, on the supervisor's own thread and
    outside its lock, and gets the run context (``drive``, ``sip``, ``guards``, ``farm``). The
    run ends after ``ticks`` ticks, or as soon as ``until`` holds (at the latest after
    ``deadline`` seconds). Every JSON document the supervisor writes is kept in ``docs``.
    """

    def __init__(
        self,
        script: dict[int, Callable[[SimpleNamespace], Any]],
        ticks: int,
        *,
        until: Callable[[], bool] | None = None,
        deadline: float = 10.0,
        socket_drops: dict[DataFeed, int] | None = None,
        update_drops: int = 0,
        quitting_feeds: tuple[DataFeed, ...] = (),
    ) -> None:
        self.script = script
        self.ticks = ticks
        self.until = until
        self.deadline = time.monotonic() + deadline
        self.loop_ticks = 0
        self.finished = threading.Event()
        self.ever_subscribed: set[str] = set()
        self.docs: dict[str, list[dict[str, Any]]] = {}
        drops = socket_drops or {}
        self.streams = {
            feed: _P3Stream(self, drops.get(feed, 0), feed in quitting_feeds)
            for feed in (DataFeed.SIP, DataFeed.BOATS)
        }
        self.trade_updates = _P3TradeUpdates(self, update_drops)
        self.ctx = SimpleNamespace(drive=self, sip=self.streams[DataFeed.SIP], guards={}, farm=None)

    def wait(self, futures: Any, timeout: float | None = None, return_when: str = "") -> Any:
        del timeout  # the supervisor's 1 s tick, shortened so that a drive takes milliseconds
        self.loop_ticks += 1
        step = self.script.get(self.loop_ticks)
        if step is not None:
            step(self.ctx)
        if self.until is None:
            done = self.loop_ticks > self.ticks
        else:
            done = self.until() or time.monotonic() > self.deadline
        if done:
            self.finished.set()
            return futures_wait(futures, timeout=30, return_when=return_when)
        return futures_wait(futures, timeout=0.005, return_when=return_when)

    def write_json(self, path: Path, payload: dict[str, Any]) -> None:
        self.docs.setdefault(path.name, []).append(json.loads(json.dumps(payload, default=str)))
        pf._atomic_write_json(path, payload)

    def statuses(self) -> list[str]:
        return [str(doc["status"]) for doc in self.docs.get(_EXECUTION_STATUS, [])]


def _run_p3_supervisor(
    root: Path,
    brokers: FaultInjectingAlpacaSim | dict[str, FaultInjectingAlpacaSim],
    *,
    session: str,
    ticks: int,
    script: dict[int, Callable[[SimpleNamespace], Any]] | None = None,
    host_session: str | None = None,
    data_client: Any = None,
    trade_updates: bool = False,
    scan_doc: dict[str, Any] | None = None,
    measure_skew: bool = False,
    cadence: float = 0.0,
    constants: dict[str, Any] | None = None,
    until: Callable[[], bool] | None = None,
    deadline: float = 10.0,
    socket_drops: dict[DataFeed, int] | None = None,
    update_drops: int = 0,
    quitting_feeds: tuple[DataFeed, ...] = (),
) -> SimpleNamespace:
    """Run ``run_execution`` on the simulator: one live sim, or a dict of paper sims per account.

    ``host_session`` is what the host clock says (default ``session``); the simulator's own
    clock says ``session``. Without ``measure_skew`` the broker-clock comparison reads nothing
    (the simulator's virtual clock runs years ahead of the wall clock); with it, that gap is a
    host clock running behind. ``cadence`` is the stop-coverage timer in seconds, ``constants``
    patches further module constants, and ``scan_doc`` is written as the scanner manifest.
    """
    drive = _P3Drive(
        script or {},
        ticks,
        until=until,
        deadline=deadline,
        socket_drops=socket_drops,
        update_drops=update_drops,
        quitting_feeds=quitting_feeds,
    )
    ctx = drive.ctx
    data = _FreshQuotes() if data_client is None else data_client
    real_restore = execution.restore_guards

    def restore_guards(*args: Any, **kwargs: Any) -> dict[str, execution.Guard]:
        ctx.guards = real_restore(*args, **kwargs)  # adoption fills this same dict
        return ctx.guards

    patches: list[tuple[Any, str, Any]] = [
        (execution, "wait", drive.wait),
        (execution, "_atomic_write_json", drive.write_json),
        (execution, "restore_guards", restore_guards),
        (execution, "STOP_COVERAGE_RECONCILE_SECS", cadence),
        (execution, "STREAM_RESTART_BACKOFF_BASE_SECS", 0.0),
        (
            execution.engine,
            "classify_us_equity_session_et",
            _session_classifier(host_session or session),
        ),
    ]
    patches += [(execution, name, value) for name, value in (constants or {}).items()]
    if not measure_skew:
        patches.append((execution, "_measure_broker_clock_skew", lambda _broker: None))
    live = isinstance(brokers, FaultInjectingAlpacaSim)
    shared_dir = root / "manifests"
    manifest_dir = root / "live" / "manifests" if live else shared_dir
    shared_dir.mkdir(parents=True, exist_ok=True)
    if scan_doc is not None:
        (shared_dir / _SCAN_MANIFEST).write_text(json.dumps(scan_doc), encoding="utf-8")

    with contextlib.ExitStack() as stack:
        if isinstance(brokers, FaultInjectingAlpacaSim):
            sim = brokers

            def build_coordinator(
                directory: Path, *, autonomous_opt_in: bool
            ) -> pf.AlpacaLiveVenueCoordinator:
                ctx.farm = pf.AlpacaLiveVenueCoordinator(
                    directory,
                    trading_client=sim,
                    data_client=data,
                    autonomous_opt_in=autonomous_opt_in,
                    market_streams=drive.streams,
                    trade_updates_stream=drive.trade_updates if trade_updates else None,
                )
                return ctx.farm

            def refresh_accounts(
                farm: pf.PaperFarmCoordinator, now: datetime, _session: str, **_kwargs: Any
            ) -> dict[str, dict[str, Any]]:
                # Stand-in for runner.refresh_accounts (it still demands the three paper accounts).
                inventory: dict[str, dict[str, Any]] = {}
                for acct, lane in farm.lanes.items():
                    broker = lane.trading_client
                    lane.start_equity = Decimal(str(broker.get_account().equity))
                    lane.equity_verified_at = now.timestamp()
                    inventory[acct] = {pos.symbol: pos for pos in broker.get_all_positions()}
                return inventory

            patches += [
                (execution, "build_live_venue_coordinator", build_coordinator),
                (execution, "refresh_accounts", refresh_accounts),
            ]
            stack.enter_context(
                mock.patch.object(
                    execution.runner, "verify_live_environment", lambda: False, create=True
                )
            )
        else:
            ctx.farm = pf.PaperFarmCoordinator(manifest_dir, alpaca_clients=brokers)
            # Never the coordinator's own streams: with paper keys in the environment those
            # would be real connections.
            update_streams = {_PA1: drive.trade_updates} if trade_updates else {}
            stack.enter_context(
                mock.patch.object(ctx.farm, "trade_update_streams", lambda: update_streams)
            )
            patches += [
                (execution, "verify_paper_environment", lambda: None),
                (execution, "resolve_alpaca_env_creds", lambda: ("", "")),
                (execution, "PaperFarmCoordinator", lambda _dir: ctx.farm),
                (execution, "StockDataStream", lambda *_args, feed: drive.streams[feed]),
                (
                    execution.ExtremeMoverScanner,
                    "from_env",
                    lambda: SimpleNamespace(data_client=data),
                ),
            ]
        for target, name, replacement in patches:
            stack.enter_context(mock.patch.object(target, name, replacement))
        if live:
            execution.run_execution(
                manifest_dir, venue_profile=LIVE_TIERED_VENUE_ID, shared_dir=shared_dir
            )
        else:
            execution.run_execution(manifest_dir)

    coverage_path = manifest_dir / _COVERAGE_MANIFEST
    return SimpleNamespace(
        farm=ctx.farm,
        drive=drive,
        ctx=ctx,
        status=json.loads((manifest_dir / _EXECUTION_STATUS).read_text("utf-8")),
        coverage=json.loads(coverage_path.read_text("utf-8")) if coverage_path.is_file() else None,
        manifest_dir=manifest_dir,
        shared_dir=shared_dir,
    )


def _paper_sims(
    session: Literal["RTH", "PRE_MARKET"] = "RTH",
) -> dict[str, FaultInjectingAlpacaSim]:
    return {acct: FaultInjectingAlpacaSim(session=session) for acct in pf.ALPACA_LANE_PROFILES}


def _entry_submits(sims: dict[str, FaultInjectingAlpacaSim], symbol: str) -> list[str]:
    """Outcome of every entry order (a limit buy) sent for ``symbol``, across the paper lanes."""
    return [
        call.outcome
        for sim in sims.values()
        for call in sim.calls("submit_order")
        if call.payload["symbol"] == symbol and call.payload["type"] == "limit"
    ]


def _scan_with_movers(*symbols: str) -> dict[str, Any]:
    """A fresh regular-session scan whose eligible long movers are ``symbols``."""
    doc, first, _side = _scan_with_one_mover("RTH", "LONG")
    template = doc["entries"][0]
    doc["entries"] = [template | {"symbol": symbol} for symbol in symbols]
    assert first == template["symbol"]
    return doc


class _LatestTrades:
    """Market-data client stand-in with a scriptable REST latest trade per symbol."""

    def __init__(self) -> None:
        self.latest: dict[str, Any] = {}
        self.requests: list[Any] = []
        self.broken = False

    def get_stock_latest_quote(self, request: Any) -> dict[str, Any]:
        return _FreshQuotes().get_stock_latest_quote(request)

    def get_stock_latest_trade(self, request: Any) -> dict[str, Any]:
        self.requests.append(request)
        if self.broken:
            raise RuntimeError("market data REST is down")
        symbols = request.symbol_or_symbols
        wanted = [symbols] if isinstance(symbols, str) else list(symbols)
        return {symbol: self.latest[symbol] for symbol in wanted if symbol in self.latest}


def _contains_no_float(value: Any) -> bool:
    if isinstance(value, float):
        return False
    if isinstance(value, dict):
        return all(_contains_no_float(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return all(_contains_no_float(item) for item in value)
    return True


def test_guard_acts_when_host_clock_behind(tmp_path: Path) -> None:
    # 1. The measurement: broker clock minus host clock, from the broker's own clock read.
    aligned = FaultInjectingAlpacaSim(paper=False, start=datetime.now(UTC))
    in_step = execution._measure_broker_clock_skew(aligned)
    assert in_step is not None and abs(in_step) < execution.CLOCK_SKEW_MAX_SECS
    aligned.advance(120)  # the broker is two minutes on: the host clock runs behind it
    behind = execution._measure_broker_clock_skew(aligned)
    assert behind is not None and Decimal("119") < behind < Decimal("121")
    aligned.fail_next("get_clock", 500)
    assert execution._measure_broker_clock_skew(aligned) is None
    no_stamp = SimpleNamespace(get_clock=lambda: SimpleNamespace(is_open=True))
    assert execution._measure_broker_clock_skew(no_stamp) is None
    assert [call.name for call in aligned.calls()] == ["get_clock"] * 3

    # 2. A guard orders prints by their exchange timestamp, never by the host clock.
    sim = _live_sim(session="PRE_MARKET")
    sim.open_position("NVDA", 10, "100.00")
    farm, _lane = _live_farm(tmp_path / "direct", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="PRE_MARKET")
    first_stop = guard.stop
    ahead = datetime.now(UTC) + timedelta(seconds=120)  # what a host 120 s behind is handed

    def feed(price: str, stamp: datetime) -> bool:
        return execution.drive_guard_price(guard, Decimal(price), "PRE_MARKET", trade_ts=stamp)

    assert feed("103.00", ahead) is True
    assert guard.stop > first_stop and guard.last_trade_ts == ahead
    ratcheted = guard.stop
    # An older print is out of order: it cannot trigger anything. One of the same instant counts.
    assert feed("90.00", ahead - timedelta(seconds=1)) is False
    assert guard.triggered is False and guard.stop == ratcheted
    assert feed("103.00", ahead) is True
    # One wrong timestamp cannot starve the guard: past the fallback window any print is taken.
    guard.last_price_mono -= execution.GUARD_PRICE_REST_FALLBACK_SECS + 1
    assert feed("103.10", ahead - timedelta(seconds=60)) is True
    assert guard.last_trade_ts == ahead - timedelta(seconds=60)
    assert sim.mutating_call_count == 0 and guard.protection_fault_code is None

    # 3. The supervisor on a host clock that runs behind. The simulator's clock is the true
    #    time, years ahead of the host, and the host clock alone would call the market closed.
    #    Sessions follow the broker clock and the guard exits on prints stamped "in the future".
    run_sim = _live_sim(session="PRE_MARKET")
    run_sim.open_position("NVDA", 10, "100.00")
    seen: dict[str, Any] = {"stamps": []}

    def breach_print(ctx: SimpleNamespace) -> None:
        if "price" not in seen:
            (resting,) = _resting_stops(run_sim, "NVDA")
            seen["price"] = Decimal(str(resting.stop_price)) - Decimal("0.50")
            run_sim.set_price("NVDA", str(seen["price"]))
        ctx.guards["NVDA"].last_exit_check = 0.0  # lift the one-second exit throttle
        seen["stamps"].append(run_sim.now)
        ctx.sip.deliver("NVDA", seen["price"], run_sim.now)

    run = _run_p3_supervisor(
        tmp_path / "behind",
        run_sim,
        session="PRE_MARKET",
        host_session="CLOSED",
        measure_skew=True,
        cadence=3600.0,
        ticks=4,
        script={2: breach_print, 3: breach_print},
    )
    assert len(seen["stamps"]) == 2
    assert all((stamp - datetime.now(UTC)).total_seconds() > 3600 for stamp in seen["stamps"])
    assert run_sim.inspect_position("NVDA") is None
    exit_order = run_sim.calls("submit_order")[-1].payload
    assert exit_order["type"] == "limit" and exit_order["extended_hours"] is True
    assert exit_order["side"] == "sell" and Decimal(str(exit_order["qty"])) == 10
    clock = run.status["clock_skew"]
    assert clock["status"] == "SKEWED" and clock["entries_blocked"] is True
    assert Decimal(clock["skew_secs"]) > 3600 and run.status["active_session"] == "PRE_MARKET"
    assert run.status["supervision_faults"]["process"] == 0
    assert run.status["last_trade_utc"] == {"sip": seen["stamps"][-1].isoformat()}

    # 4. Above one second of skew no entry goes out; with the clocks in step the same print
    #    does enter. Only entries are blocked: the guard above was not.
    for measured, submitted in ((True, []), (False, ["ok"])):
        sims = _paper_sims()
        scan_doc, symbol, _side = _scan_with_one_mover("RTH", "LONG")
        for paper_sim in sims.values():
            paper_sim.set_price(symbol, "100.00")
        paper = _run_p3_supervisor(
            tmp_path / f"entries-{measured}",
            sims,
            session="RTH",
            measure_skew=measured,
            cadence=3600.0,
            ticks=3,
            scan_doc=scan_doc,
            script={2: lambda ctx, symbol=symbol: ctx.sip.deliver(symbol, "100.00")},
        )
        assert _entry_submits(sims, symbol) == submitted
        assert paper.status["clock_skew"]["entries_blocked"] is measured
        assert paper.status["entry_fault"] is False


def test_feed_stall_falls_back_to_rest_and_exits(tmp_path: Path) -> None:
    # 1. No stream print ever arrives (and the SIP socket drops twice). The REST latest-trade
    #    poll drives the guard instead: it sees the breach and takes the position flat.
    sim = _live_sim(session="PRE_MARKET")
    sim.open_position("NVDA", 10, "100.00")
    rest = _LatestTrades()
    rest.latest["NVDA"] = SimpleNamespace(price=100.0, timestamp=datetime.now(UTC))
    breach: dict[str, Decimal] = {}

    def price_falls_through_the_stop(_ctx: SimpleNamespace) -> None:
        (resting,) = _resting_stops(sim, "NVDA")
        breach["price"] = Decimal(str(resting.stop_price)) - Decimal("0.50")
        sim.set_price("NVDA", str(breach["price"]))
        rest.latest["NVDA"] = SimpleNamespace(
            price=float(breach["price"]), timestamp=datetime.now(UTC)
        )

    def lift_exit_throttle(ctx: SimpleNamespace) -> None:
        ctx.guards["NVDA"].last_exit_check = 0.0

    run = _run_p3_supervisor(
        tmp_path / "rest",
        sim,
        session="PRE_MARKET",
        data_client=rest,
        cadence=3600.0,
        ticks=5,
        socket_drops={DataFeed.SIP: 2},
        constants={"GUARD_PRICE_REST_FALLBACK_SECS": 0.0, "GUARD_REST_POLL_SECS": 0.0},
        script={2: price_falls_through_the_stop, 3: lift_exit_throttle, 4: lift_exit_throttle},
    )
    assert sim.inspect_position("NVDA") is None
    exit_order = sim.calls("submit_order")[-1].payload
    assert exit_order["type"] == "limit" and exit_order["extended_hours"] is True
    assert exit_order["side"] == "sell" and Decimal(str(exit_order["qty"])) == 10
    assert Decimal(str(exit_order["limit_price"])) < breach["price"]
    guard = run.ctx.guards["NVDA"]
    assert guard.last_price_source == "REST" and guard.lifecycle_closed is True
    # The same latest trade came back on later polls: it re-drove the guard only because its
    # exit was in progress, and no stream print was ever seen.
    assert run.status["last_trade_utc"] == {} and run.status["data_plane"]["rest_polls"] >= 3
    assert {tuple(request.symbol_or_symbols) for request in rest.requests} == {("NVDA",)}
    assert {request.feed for request in rest.requests} == {DataFeed.SIP}
    # The dropped socket did not end the supervisor: the stream was restarted and ran on.
    assert run.status["data_plane"]["stream_faults"] == {"market:sip": 2}
    assert run.drive.streams[DataFeed.SIP].runs == 3 and run.drive.loop_ticks == 6
    assert run.status["status"] == "STOPPED" and run.status["supervision_faults"]["rest_poll"] == 0
    # The poll is a market-data request: it took nothing from the trading rate budget.
    polls = run.status["data_plane"]["rest_polls"]
    assert polls == len(rest.requests)

    # 2. With the REST fallback down as well, the status says DATA_STALE and names the symbol.
    #    The resting native stop still covers the position. When data returns, the flag clears.
    stale_sim = _live_sim(session="PRE_MARKET")
    stale_sim.open_position("NVDA", 10, "100.00")
    down = _LatestTrades()
    down.broken = True

    def data_returns(_ctx: SimpleNamespace) -> None:
        down.broken = False
        down.latest["NVDA"] = SimpleNamespace(price=100.0, timestamp=datetime.now(UTC))

    stale_run = _run_p3_supervisor(
        tmp_path / "stale",
        stale_sim,
        session="PRE_MARKET",
        data_client=down,
        cadence=3600.0,
        ticks=4,
        constants={
            "GUARD_PRICE_REST_FALLBACK_SECS": 0.0,
            "GUARD_REST_POLL_SECS": 0.0,
            "GUARD_DATA_STALE_SECS": 0.5,
        },
        script={2: lambda _ctx: time.sleep(0.6), 3: data_returns},
    )
    statuses = stale_run.drive.statuses()
    assert statuses[0] == "AWAITING_MARKET_DATA" and statuses[-1] == "STOPPED"
    assert "DATA_STALE" in statuses
    stale_doc = next(
        doc for doc in stale_run.drive.docs[_EXECUTION_STATUS] if doc["status"] == "DATA_STALE"
    )
    assert stale_doc["data_plane"]["data_stale_symbols"] == ["NVDA"]
    assert stale_doc["active_positions"][0]["data_stale"] is True
    assert statuses[statuses.index("DATA_STALE") + 1] == "AWAITING_MARKET_DATA"
    assert stale_run.status["data_plane"]["data_stale_symbols"] == []
    assert stale_run.status["supervision_faults"]["rest_poll"] >= 1
    assert _stop_cover_qty(stale_sim, "NVDA") == 10 and _position_qty(stale_sim, "NVDA") == 10
    assert [call.name for call in stale_sim.calls(kind=MUTATING)] == ["submit_order"]

    # 3. One market stream ends on its own (upstream returns from run() when the subscription
    #    lacks the feed). Supervision goes on with the other: a position opened afterwards is
    #    still adopted and stopped, and the loop runs to its last tick.
    lone_sim = _live_sim(session="PRE_MARKET")
    lone = _run_p3_supervisor(
        tmp_path / "lone",
        lone_sim,
        session="PRE_MARKET",
        ticks=4,
        quitting_feeds=(DataFeed.BOATS,),
        constants={"STREAM_END_GRACE_SECS": 0.05},
        script={3: lambda _ctx: lone_sim.open_position("PLTR", 6, "50.00")},
    )
    assert lone.drive.streams[DataFeed.BOATS].runs == 1 and lone.drive.loop_ticks == 5
    assert lone.status["managed_symbols"] == ["PLTR"] and _stop_cover_qty(lone_sim, "PLTR") == 6
    assert "market:boats" in lone.status["data_plane"]["streams_ended"]
    assert lone.status["data_plane"]["stream_faults"] == {}


def test_live_regime_flip_requires_persistence_and_clears_on_revert(tmp_path: Path) -> None:
    assert (REGIME_FLIP_MIN_GENERATIONS, REGIME_FLIP_MIN_SECONDS) == (3, 180)
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    assert _stop_cover_qty(sim, "NVDA") == 10
    start = datetime.now(UTC)

    def scan(book: dict[str, execution.Guard], regime: int, seconds: int) -> list[str]:
        """One scan generation ``seconds`` after the first, with macro regime ``regime``."""
        return execution.apply_regime_flip_policy(
            book,
            market_regime=regime,
            rows={},
            session="RTH",
            generation_utc=start + timedelta(seconds=seconds),
        )

    # 1. Live: a regime against the long must stand for 3 generations AND 180 s. Three
    #    generations inside 120 s are not enough, and nothing is sent meanwhile.
    for seconds in (0, 60, 120):
        assert scan(guards, -1, seconds) == ["NVDA:REGIME_FLIP_PENDING"]
    assert guard.regime_flip_generations == 3 and guard.reverse_flatten is False
    # A revert starts the count over.
    assert scan(guards, 1, 150) == [] and guard.regime_flip_generations == 0
    assert guard.regime_flip_since is None
    # 180 s inside only two generations are not enough either.
    assert scan(guards, -1, 200) == ["NVDA:REGIME_FLIP_PENDING"]
    assert scan(guards, -1, 380) == ["NVDA:REGIME_FLIP_PENDING"]
    assert scan(guards, -1, 440) == ["NVDA:REGIME_FLIP_FLATTEN"]
    assert guard.reverse_flatten is True and guard.regime_flip_flatten is True
    assert sim.mutating_call_count == 1  # only the stop adoption placed

    # 2. The regime reverts before any print arrives: the flatten is withdrawn untouched.
    assert scan(guards, 1, 500) == ["NVDA:REGIME_FLIP_CLEARED"]
    assert guard.reverse_flatten is False and guard.regime_flip_flatten is False
    assert sim.mutating_call_count == 1 and _stop_cover_qty(sim, "NVDA") == 10

    # 3. It persists again and a print starts the exit: the guard frees its shares (the stop is
    #    cancelled) but has not sold yet. The revert withdraws the flatten and the trigger it
    #    caused; the reconciler puts the stop back and the next print sells nothing.
    for seconds in (600, 700, 800):
        scan(guards, -1, seconds)
    assert guard.reverse_flatten is True
    _tick(guard, "100.00", "RTH")
    assert guard.triggered is True and guard.trigger_cause == "REVERSE_FLATTEN"
    assert _stop_cover_qty(sim, "NVDA") == 0 and len(sim.calls("submit_order")) == 1
    assert scan(guards, 1, 860) == ["NVDA:REGIME_FLIP_CLEARED"]
    assert guard.reverse_flatten is False and guard.triggered is False
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
        assert db.execute("SELECT exit_triggered FROM paper_entry_decisions").fetchall() == [
            (False,)
        ]
    assert _cover(farm, guards, "RTH") == [f"{_LIVE_ACCOUNT}:NVDA:STOP_ATTACHED"]
    _tick(guard, "100.00", "RTH")
    assert _position_qty(sim, "NVDA") == 10 and _stop_cover_qty(sim, "NVDA") == 10
    assert [call.payload["type"] for call in sim.calls("submit_order")] == ["stop", "stop"]

    # 4. Once the exit order is out, a revert no longer withdraws anything.
    for seconds in (900, 1000, 1100):
        scan(guards, -1, seconds)
    _tick(guard, "100.00", "RTH")
    _tick(guard, "100.00", "RTH")
    assert sim.calls("submit_order")[-1].payload["type"] == "market"
    assert sim.inspect_position("NVDA") is None
    assert scan(guards, 1, 1160) == ["NVDA:REGIME_REVERTED_EXIT_ALREADY_SENT"]
    assert guard.reverse_flatten is True

    # 5. A flatten that came from the stop or the peak-climax reverse is never withdrawn here.
    stop_sim = _live_sim(session="RTH")
    stop_sim.open_position("AMD", 10, "100.00")
    stop_farm, _stop_lane = _live_farm(tmp_path / "stop", stop_sim)
    stop_guards: dict[str, execution.Guard] = {}
    (stopped,) = execution.adopt_unmanaged_broker_positions(stop_farm, stop_guards, session="RTH")
    stopped.reverse_flatten = True  # as flatten_first_climax_reverse sets it
    assert scan(stop_guards, 1, 0) == [] and stopped.reverse_flatten is True

    # 6. Live, a leveraged long and its inverse held together (the owner's pair): no regime
    #    reading flattens either leg, however long it stands.
    pair_sim = _live_sim(session="RTH")
    pair_sim.open_position("SOXL", 82, "20.00")
    pair_sim.open_position("SOXS", 24, "30.00")
    pair_farm, _pair_lane = _live_farm(tmp_path / "pair", pair_sim)
    pair: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(pair_farm, pair, session="RTH")
    for seconds in (0, 100, 200, 300):
        assert scan(pair, 1, seconds) == ["SOXS:REGIME_FLIP_HELD_OPPOSING_PAIR"]
        assert scan(pair, -1, seconds) == ["SOXL:REGIME_FLIP_HELD_OPPOSING_PAIR"]
    assert not pair["SOXL"].reverse_flatten and not pair["SOXS"].reverse_flatten
    # Held alone, the inverse leg does flatten once a bullish regime has stood long enough.
    del pair["SOXL"]
    assert [scan(pair, 1, seconds) for seconds in (400, 500, 600)][-1] == [
        "SOXS:REGIME_FLIP_FLATTEN"
    ]

    # 7. Paper keeps flipping on the first generation, and a revert clears it there too.
    sims = _paper_sims()
    sims[_PA1].open_position("NVDA", 10, "100.00")
    paper_farm = pf.PaperFarmCoordinator(tmp_path / "paper", alpaca_clients=sims)
    paper_guards: dict[str, execution.Guard] = {}
    (paper_guard,) = execution.adopt_unmanaged_broker_positions(
        paper_farm, paper_guards, session="RTH"
    )
    assert scan(paper_guards, -1, 0) == ["NVDA:REGIME_FLIP_FLATTEN"]
    assert paper_guard.reverse_flatten is True
    assert scan(paper_guards, 1, 60) == ["NVDA:REGIME_FLIP_CLEARED"]
    assert paper_guard.reverse_flatten is False


def test_trade_update_flag_triggers_reconcile_without_scan_manifest(tmp_path: Path) -> None:
    # The stop-coverage timer is an hour away, so only the trade-update flag can reconcile.
    # A fill event (also a partial one) adopts and stops the new position on the next loop
    # tick; any other event, or none, does not. The stream's socket drops once and is restarted.
    outcomes: dict[str, tuple[list[str], Decimal, int]] = {}
    for label, update_event in (
        ("fill", "fill"),
        ("partial", "partial_fill"),
        ("accepted", "new"),
        ("silent", None),
    ):
        sim = _live_sim(session="PRE_MARKET")

        def owner_buys(
            ctx: SimpleNamespace,
            sim: FaultInjectingAlpacaSim = sim,
            update_event: str | None = update_event,
        ) -> None:
            sim.open_position("PLTR", 6, "50.00")
            if update_event is not None:
                ctx.drive.trade_updates.emit(update_event, "PLTR")

        run = _run_p3_supervisor(
            tmp_path / label,
            sim,
            session="PRE_MARKET",
            trade_updates=True,
            cadence=3600.0,
            ticks=3,
            update_drops=1,
            script={2: owner_buys},
        )
        plane = run.status["data_plane"]
        outcomes[label] = (
            run.status["managed_symbols"],
            _stop_cover_qty(sim, "PLTR"),
            plane["trade_update_fill_events"],
        )
        # No scanner manifest exists anywhere, and none was needed.
        assert list(run.shared_dir.iterdir()) == []
        assert not (run.manifest_dir / _SCAN_MANIFEST).exists()
        assert plane["trade_update_streams"] == [_LIVE_ACCOUNT]
        assert plane["stream_faults"] == {f"trade_updates:{_LIVE_ACCOUNT}": 1}
        assert run.drive.trade_updates.runs == 2 and run.drive.loop_ticks == 4
        assert run.status["stop_coverage"]["reconcile_faults"] == 0
        assert run.status["status"] == "STOPPED"
    assert outcomes["fill"] == (["PLTR"], Decimal("6"), 1) == outcomes["partial"]
    assert outcomes["accepted"] == ([], Decimal("0"), 0) == outcomes["silent"]

    # The paper farm consumes the same flag from its own trade-update stream.
    sims = _paper_sims("PRE_MARKET")

    def paper_fill(ctx: SimpleNamespace) -> None:
        sims[_PA1].open_position("PLTR", 6, "50.00")
        ctx.drive.trade_updates.emit("fill", "PLTR")

    paper = _run_p3_supervisor(
        tmp_path / "paper",
        sims,
        session="PRE_MARKET",
        trade_updates=True,
        cadence=3600.0,
        ticks=3,
        script={2: paper_fill},
    )
    assert paper.status["managed_symbols"] == ["PLTR"] and paper.status["paper_only"] is True
    assert _stop_cover_qty(sims[_PA1], "PLTR") == 6
    assert not (paper.manifest_dir / _SCAN_MANIFEST).exists()


def test_hand_opened_position_adopted_within_5s_without_scanner(tmp_path: Path) -> None:
    # The time-based fallback alone, on its real two-second cadence: no trade-update stream,
    # no trade print and no scanner manifest. A position opened by hand has its resting stop
    # within the five-second objective.
    assert execution.STOP_COVERAGE_RECONCILE_SECS <= 2.0
    sim = _live_sim(session="PRE_MARKET")
    marks: dict[str, float] = {}

    def owner_buys(_ctx: SimpleNamespace) -> None:
        sim.open_position("PLTR", 6, "50.00")
        marks["opened"] = time.monotonic()

    def protected() -> bool:
        if "opened" in marks and "protected" not in marks and _stop_cover_qty(sim, "PLTR") == 6:
            marks["protected"] = time.monotonic()
        return "protected" in marks

    run = _run_p3_supervisor(
        tmp_path / "slo",
        sim,
        session="PRE_MARKET",
        cadence=execution.STOP_COVERAGE_RECONCILE_SECS,
        ticks=0,
        until=protected,
        deadline=9.0,
        script={3: owner_buys},
    )
    assert "protected" in marks, "no resting stop within the deadline"
    assert marks["protected"] - marks["opened"] <= 5.0
    (stop,) = _resting_stops(sim, "PLTR")
    assert stop.time_in_force is TimeInForce.GTC and Decimal(str(stop.qty)) == 6
    assert stop.client_order_id.startswith("stop-adopted-")
    assert run.status["managed_symbols"] == ["PLTR"]
    assert run.status["adopted_external_positions_count"] == 1
    plane = run.status["data_plane"]
    assert plane["trade_update_streams"] == [] and plane["trade_update_fill_events"] == 0
    assert run.status["last_trade_utc"] == {} and list(run.shared_dir.iterdir()) == []
    assert run.status["stop_coverage"]["reconcile_faults"] == 0
    # The timer's reads were budgeted, outside the exit reserve (R14): two per reconcile.
    lane = run.farm.lanes[_LIVE_ACCOUNT]
    assert lane.counters.get("reads_deferred_total", 0) == 0
    assert _budget_slots_used(lane) >= 3


def test_lossless_coverage_record_schema_and_counts(tmp_path: Path) -> None:
    sim = _live_sim(session="PRE_MARKET")
    for symbol, qty in (
        ("AAA", "10"),
        ("BBB", "10"),
        ("CCC", "7"),
        ("EEE", "2.5"),
        ("FFF", "8"),
        ("GGG", "0.4"),
    ):
        sim.open_position(symbol, qty, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(farm, guards, session="PRE_MARKET")
    assert sorted(guards) == ["AAA", "BBB", "CCC", "EEE", "FFF"] and sim.mutating_call_count == 0
    # The account as the owner left it: a full GTC stop, a partial one, a DAY stop (not cover),
    # a native trailing stop, a position with a fractional part, and one opened after adoption.
    sim.submit_order(_sell_stop("AAA", 10, 95.0))
    sim.submit_order(_sell_stop("BBB", 4, 95.0))
    sim.submit_order(
        StopOrderRequest(
            symbol="CCC", qty=7, side=OrderSide.SELL, time_in_force=TimeInForce.DAY, stop_price=95.0
        )
    )
    sim.submit_order(
        TrailingStopOrderRequest(
            symbol="FFF",
            qty=8,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            trail_percent=1.5,
        )
    )
    sim.open_position("DDD", 5, "100.00")
    # A guard for a symbol the broker does not hold must add nothing: counts are broker truth.
    phantom = guards["AAA"]
    guards["ZZZ"] = phantom
    guards["CCC"].supervised_since_mono -= 60  # no confirmed price for a minute

    def record(session: str = "PRE_MARKET") -> dict[str, Any]:
        return execution.build_lossless_coverage_record(
            farm,
            guards,
            _broker_positions(sim),
            session=session,
            now_utc=datetime.now(UTC),
            last_feed_trade={DataFeed.SIP: datetime(2026, 11, 27, 15, 0, tzinfo=UTC)},
        )

    sim.reset_call_log()
    doc = record()
    # Two broker reads, positions and open orders; nothing else and nothing mutating.
    assert [call.name for call in sim.calls()] == ["get_all_positions", "get_orders"]
    assert doc["schema_version"] == "tier0.lossless-coverage.v1"
    assert doc["generator"] == "tier0_sota.live.execution.build_lossless_coverage_record"
    for key in (
        "generated_at_utc",
        "inputs_hash",
        "all_shares_protected",
        "share_protection_table",
        "ledger_broker_delta",
        "rate_budget",
        "natively_protected_shares",
        "synthetically_supervised_shares",
        "unprotected_shares_count",
    ):
        assert key in doc, key
    assert doc["environment"] == "LIVE" and doc["paper_only"] is False
    assert doc["live_execution_enabled"] is False and doc["session"] == "PRE_MARKET"
    assert _contains_no_float(doc) and json.loads(json.dumps(doc)) == doc
    table = {row["symbol"]: row for row in doc["share_protection_table"]}
    assert sorted(table) == ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"]
    counts = {
        symbol: (
            row["mode"],
            row["whole_shares"],
            row["natively_protected_shares"],
            row["synthetically_supervised_shares"],
            row["unprotected_shares_count"],
        )
        for symbol, row in table.items()
    }
    assert counts == {
        "AAA": ("NATIVE_BROKER_STOP", 10, 10, 0, 0),
        "BBB": ("NATIVE_PARTIAL", 10, 4, 6, 0),
        "CCC": ("UNPROTECTED:DATA_STALE", 7, 0, 0, 7),
        "DDD": ("UNPROTECTED:NO_GUARD", 5, 0, 0, 5),
        "EEE": ("SYNTHETIC_TICK_SUPERVISED", 2, 0, 2, 0),
        "FFF": ("NATIVE_BROKER_STOP", 8, 8, 0, 0),
        "GGG": ("UNPROTECTED:FRACTIONAL_ONLY", 0, 0, 0, 0),
    }
    assert table["EEE"]["position_qty"] == "2.5"
    assert table["EEE"]["fractional_unprotected_qty"] == "0.5"
    assert table["DDD"]["guard_client_order_id"] is None
    assert table["AAA"]["guard_client_order_id"] == guards["AAA"].client_id
    assert len(table["FFF"]["native_stop_order_ids"]) == 1
    assert (doc["position_shares_total"], doc["natively_protected_shares"]) == (42, 22)
    assert (doc["synthetically_supervised_shares"], doc["unprotected_shares_count"]) == (8, 12)
    assert doc["fractional_unprotected_qty"] == "0.9" and doc["all_shares_protected"] is False
    # What holds entries (R13): no guard, or neither full native cover nor an exit in flight.
    # Less than one share (GGG) cannot carry a stop and holds nothing.
    assert doc["entry_hold_positions"] == [
        f"{_LIVE_ACCOUNT}:{symbol}" for symbol in ("BBB", "CCC", "DDD", "EEE")
    ]
    delta = doc["ledger_broker_delta"][_LIVE_ACCOUNT]
    assert delta["broker_position_not_in_ledger"] == ["DDD", "GGG"]
    assert delta["ledger_readable"] is True
    assert delta["ledger_open_not_at_broker"] == [] and delta["side_mismatch"] == []
    budget = doc["rate_budget"][_LIVE_ACCOUNT]
    assert (budget["max_rpm"], budget["entry_modify_rpm_budget"], budget["exit_reserve_rpm"]) == (
        1000,
        900,
        100,
    )
    assert budget["used_rpm_60s"] == _budget_slots_used(lane)
    assert doc["last_feed_trade_utc"] == {"sip": "2026-11-27T15:00:00+00:00"}
    assert "next regular open" in doc["offhours_exit_note"]

    # The hash names the inputs: the same account gives the same hash, a changed one another.
    assert record()["inputs_hash"] == doc["inputs_hash"]
    (aaa_stop,) = _resting_stops(sim, "AAA")
    sim.external_cancel(aaa_stop.id)
    after_cancel = record()
    assert after_cancel["inputs_hash"] != doc["inputs_hash"]
    row = next(r for r in after_cancel["share_protection_table"] if r["symbol"] == "AAA")
    # The guard still believes nothing about that stop that the record would repeat: with the
    # stop gone at the broker the shares are tick-supervised, not natively protected.
    assert (row["mode"], row["natively_protected_shares"]) == ("SYNTHETIC_TICK_SUPERVISED", 0)
    # With the market closed nothing can execute, so tick supervision protects nothing.
    closed = {r["symbol"]: r["mode"] for r in record("CLOSED")["share_protection_table"]}
    assert closed["AAA"] == "UNPROTECTED:MARKET_CLOSED_NO_NATIVE_STOP"
    assert closed["FFF"] == "NATIVE_BROKER_STOP"
    # A position adoption cannot take (here: named as such) is reported but holds no entries.
    skipped = execution.build_lossless_coverage_record(
        farm,
        guards,
        _broker_positions(sim),
        session="PRE_MARKET",
        now_utc=datetime.now(UTC),
        last_feed_trade={},
        ineligible={f"{_LIVE_ACCOUNT}:DDD": {"reason": "ASSET_NOT_TRADABLE"}},
    )
    assert f"{_LIVE_ACCOUNT}:DDD" not in skipped["entry_hold_positions"]
    assert skipped["unprotected_shares_count"] == 12

    # The paper farm gets the same record, per lane.
    sims = _paper_sims("PRE_MARKET")
    sims[_PA1].open_position("NVDA", 10, "100.00")
    paper_farm = pf.PaperFarmCoordinator(tmp_path / "paper", alpaca_clients=sims)
    paper_guards: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(paper_farm, paper_guards, session="PRE_MARKET")
    _cover(paper_farm, paper_guards, "PRE_MARKET")
    paper_doc = execution.build_lossless_coverage_record(
        paper_farm,
        paper_guards,
        {acct: {p.symbol: p for p in s.get_all_positions()} for acct, s in sims.items()},
        session="PRE_MARKET",
        now_utc=datetime.now(UTC),
        last_feed_trade={},
    )
    assert paper_doc["environment"] == "PAPER" and paper_doc["paper_only"] is True
    assert paper_doc["all_shares_protected"] is True and paper_doc["unprotected_shares_count"] == 0
    assert [row["mode"] for row in paper_doc["share_protection_table"]] == ["NATIVE_BROKER_STOP"]
    assert sorted(paper_doc["rate_budget"]) == sorted(pf.ALPACA_LANE_PROFILES)
    assert paper_doc["rate_budget"][_PA1]["exit_reserve_rpm"] == 20

    # The supervisor writes it on its own cycle, for live and for paper, into its own directory.
    for label, brokers in (("live", _live_sim(session="PRE_MARKET")), ("paper", sims)):
        if isinstance(brokers, FaultInjectingAlpacaSim):
            brokers.open_position("NVDA", 10, "100.00")
        run = _run_p3_supervisor(tmp_path / f"run-{label}", brokers, session="PRE_MARKET", ticks=3)
        written = run.coverage
        assert written is not None and written["schema_version"] == "tier0.lossless-coverage.v1"
        assert written["complete"] is True and written["accounts_not_read"] == []
        assert written["all_shares_protected"] is True and written["position_shares_total"] == 10
        assert written["paper_only"] is (label == "paper") and _contains_no_float(written)
        assert written["entry_hold"]["held"] is False
        summary = run.status["lossless_coverage"]
        assert summary["manifest"] == _COVERAGE_MANIFEST
        assert summary["inputs_hash"] == written["inputs_hash"]
        assert summary["unprotected_shares_count"] == 0
        assert len(run.drive.docs[_COVERAGE_MANIFEST]) >= 1


def test_entry_rejection_closes_row_and_does_not_hold_entries(tmp_path: Path) -> None:
    # Paper farm: the broker refuses the first entry (HTTP 403, as for insufficient buying
    # power). That is the outcome of that one order: its row is closed with the code, its
    # symbol released, and the next candidate enters as if nothing had happened.
    sims = _paper_sims()
    for sim in sims.values():
        sim.set_price("NVDA", "100.00")
        sim.set_price("AMD", "100.00")
        sim.fail_next("submit_order", 403)

    def first_candidate(ctx: SimpleNamespace) -> None:
        ctx.sip.deliver("NVDA", "100.00")

    def second_candidate(ctx: SimpleNamespace) -> None:
        for sim in sims.values():
            sim.clear_faults()
        ctx.sip.deliver("AMD", "100.00")

    run = _run_p3_supervisor(
        tmp_path / "paper",
        sims,
        session="RTH",
        cadence=3600.0,
        ticks=4,
        scan_doc=_scan_with_movers("NVDA", "AMD"),
        script={2: first_candidate, 3: second_candidate},
    )
    assert _entry_submits(sims, "NVDA") == ["fault:403"]
    assert _entry_submits(sims, "AMD") == ["ok"]
    statuses = run.drive.statuses()
    assert "ENTRY_HELD" not in statuses and "ORDER_ACKNOWLEDGED" in statuses
    assert run.status["entry_fault"] is False and run.status["entry_hold"]["held"] is False
    assert run.status["entry_hold"]["holds_total"] == 0
    assert run.status["entry_rejected_total"] == {"403": 1}
    assert run.status["supervision_faults"]["process"] == 0
    rows: list[tuple[Any, ...]] = []
    rejected_lanes = []
    for lane in run.farm.lanes.values():
        if not lane.duckdb_path.is_file():
            continue
        with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
            if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                continue
            rows += db.execute(
                "SELECT symbol, closed_at IS NOT NULL, close_cause FROM paper_entry_decisions"
            ).fetchall()
        if lane.counters.get("entry_rejected_total"):
            rejected_lanes.append(lane)
    assert sorted(rows) == [("AMD", False, None), ("NVDA", True, "ENTRY_REJECTED_HTTP_403")]
    (rejected_lane,) = rejected_lanes
    assert rejected_lane.counters["entry_rejected_http_403"] == 1
    assert set(run.farm.symbol_lock.snapshot()) == {"AMD"}
    assert run.status["managed_symbols"] == ["AMD"]

    # A refused entry is not a round trip: the next candidate in that symbol is a first entry.
    # After a real closed round trip it is a re-entry (and on live that would be owner lineage).
    for close_cause, origin in (
        ("ENTRY_REJECTED_HTTP_422", "TREND_ALIGNED_LONG"),
        (None, "TREND_ALIGNED_LONG_REENTRY"),
    ):
        root = tmp_path / f"after-{close_cause}"
        later_sims = _paper_sims()
        for sim in later_sims.values():
            sim.set_price("NVDA", "100.00")
        seeded = pf.PaperFarmCoordinator(root / "manifests", alpaca_clients=later_sims)
        _seed_closed_round_trip(seeded.lanes[_PA1], "NVDA", "LONG")
        with duckdb.connect(str(seeded.lanes[_PA1].duckdb_path)) as db:
            db.execute("UPDATE paper_entry_decisions SET close_cause=?", [close_cause])
        later = _run_p3_supervisor(
            root,
            later_sims,
            session="RTH",
            cadence=3600.0,
            ticks=3,
            scan_doc=_scan_with_movers("NVDA"),
            script={2: first_candidate},
        )
        assert _entry_submits(later_sims, "NVDA") == ["ok"]
        origins: list[str] = []
        for lane in later.farm.lanes.values():
            with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
                if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                    continue
                origins += [
                    str(row[0])
                    for row in db.execute(
                        "SELECT origin FROM paper_entry_decisions WHERE closed_at IS NULL"
                    ).fetchall()
                ]
        assert origins == [origin]

    # Live lane: the same function closes the row with its code and releases the symbol.
    live_sim = _live_sim(session="RTH")
    live_farm, live_lane = _live_farm(tmp_path / "live", live_sim)
    live_lane.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(live_lane.duckdb_path)) as db:
        pf._ensure_entry_decisions_table(db)
        db.execute(
            "INSERT INTO paper_entry_decisions (client_order_id, symbol, side, decision_price, "
            "arrival_price, session, origin, annual_rate, initial_stop, natr_pct) "
            "VALUES ('noesis-refused', 'NVDA', 'LONG', 100, 100, 'RTH', 'T', 0, 98, 0.012)"
        )
    assert live_farm.symbol_lock.try_claim("NVDA", _LIVE_ACCOUNT) is True
    cause = execution._close_rejected_entry(live_farm, live_lane, "noesis-refused", "NVDA", 422)
    assert cause == "ENTRY_REJECTED_HTTP_422" and live_farm.symbol_lock.snapshot() == {}
    assert _open_ledger_rows(live_lane) == 0 and live_sim.mutating_call_count == 0
    assert live_lane.counters["entry_rejected_total"] == 1
    assert execution._collect_open_symbol_sides(live_farm) == {}

    # An entry that fails in any other way (here the broker answers 500, so the order may or
    # may not exist) is that attempt's fault: counted, its symbol kept reserved, and no hold.
    # The next candidate still enters.
    flaky = _paper_sims()
    for sim in flaky.values():
        sim.set_price("NVDA", "100.00")
        sim.set_price("AMD", "100.00")
        sim.fail_next("submit_order", 500)

    def after_the_error(ctx: SimpleNamespace) -> None:
        for sim in flaky.values():
            sim.clear_faults()
        ctx.sip.deliver("AMD", "100.00")

    errored = _run_p3_supervisor(
        tmp_path / "flaky",
        flaky,
        session="RTH",
        cadence=3600.0,
        ticks=4,
        scan_doc=_scan_with_movers("NVDA", "AMD"),
        script={2: first_candidate, 3: after_the_error},
    )
    assert _entry_submits(flaky, "NVDA") == ["fault:500"]
    assert _entry_submits(flaky, "AMD") == ["ok"]
    assert errored.status["supervision_faults"]["process"] == 1
    assert errored.status["entry_rejected_total"] == {}
    assert "ENTRY_HELD" not in errored.drive.statuses()
    assert errored.status["entry_fault"] is False
    assert errored.status["entry_hold"]["holds_total"] == 0
    # The row of the order that may exist stays open (it reserves the symbol), beside AMD's.
    assert sum(_open_ledger_rows(lane) for lane in errored.farm.lanes.values()) == 2


def test_single_guard_fault_does_not_stop_other_symbols(tmp_path: Path) -> None:
    # Paper farm, regular session: two guarded positions and one scanner candidate. The broker
    # fails every stop replace of NVDA's guard (HTTP 500). That is NVDA's protection fault with
    # a consecutive count. AMD's stop is still ratcheted, the candidate still enters, and when
    # the broker answers again NVDA's fault is gone.
    sims = _paper_sims()
    holder = sims[_PA1]
    for symbol in ("NVDA", "AMD"):
        holder.open_position(symbol, 10, "100.00")
    for sim in sims.values():
        sim.set_price("MU", "100.00")
    seen: dict[str, Any] = {}

    def nvda_replace_fails_twice(ctx: SimpleNamespace) -> None:
        guard = ctx.guards["NVDA"]
        seen["nvda_stop"], seen["amd_stop"] = guard.stop, ctx.guards["AMD"].stop
        holder.set_price("NVDA", "104.00")
        faults = []
        for _ in range(2):
            holder.fail_next("replace_order_by_id", 500)
            guard.last_stop_replace_mono = 0.0  # lift the one-second replace throttle (R14)
            ctx.sip.deliver("NVDA", "104.00")
            faults.append((guard.protection_fault_code, guard.protection_fault_count))
        seen["faults"] = faults

    def others_carry_on(ctx: SimpleNamespace) -> None:
        holder.set_price("AMD", "104.00")
        ctx.sip.deliver("AMD", "104.00")
        ctx.sip.deliver("MU", "100.00")
        seen["nvda_fault_while_others_ran"] = ctx.guards["NVDA"].protection_fault_code

    def broker_answers_again(ctx: SimpleNamespace) -> None:
        guard = ctx.guards["NVDA"]
        guard.last_stop_replace_mono = 0.0
        ctx.sip.deliver("NVDA", "104.00")
        seen["after"] = (guard.protection_fault_code, guard.protection_fault_count, guard.stop)

    run = _run_p3_supervisor(
        tmp_path / "paper",
        sims,
        session="RTH",
        cadence=3600.0,
        ticks=5,
        scan_doc=_scan_with_movers("MU"),
        script={2: nvda_replace_fails_twice, 3: others_carry_on, 4: broker_answers_again},
    )
    assert seen["faults"] == [("SUPERVISION_ERROR_APIERROR", 1), ("SUPERVISION_ERROR_APIERROR", 2)]
    assert seen["nvda_fault_while_others_ran"] == "SUPERVISION_ERROR_APIERROR"
    assert run.ctx.guards["AMD"].stop > seen["amd_stop"]
    assert _entry_submits(sims, "MU") == ["ok"]
    assert seen["after"][:2] == (None, 0) and seen["after"][2] > seen["nvda_stop"]
    assert [call.outcome for call in holder.calls("replace_order_by_id")] == [
        "fault:500",
        "fault:500",
        "ok",
        "ok",
    ]
    statuses = run.drive.statuses()
    assert "ENTRY_HELD" not in statuses and run.status["entry_fault"] is False
    # A guard's fault is the guard's: it is not even a fault of the print-processing path.
    assert run.status["supervision_faults"]["process"] == 0
    assert run.status["protection_faults"] == {}
    assert sorted(run.status["managed_symbols"]) == ["AMD", "MU", "NVDA"]

    # Live lane, directly: the fault stays with its guard and is reported with its count.
    live_sim = _live_sim(session="RTH")
    for symbol in ("NVDA", "AMD"):
        live_sim.open_position(symbol, 10, "100.00")
    live_farm, live_lane = _live_farm(tmp_path / "live", live_sim)
    live_guards: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(live_farm, live_guards, session="RTH")
    for symbol in ("NVDA", "AMD"):
        live_sim.set_price(symbol, "104.00")
    stamp = datetime.now(UTC)
    for expected in (1, 2, 3):
        live_sim.fail_next("replace_order_by_id", 500)
        live_guards["NVDA"].last_stop_replace_mono = 0.0
        assert (
            execution.drive_guard_price(
                live_guards["NVDA"], Decimal("104.00"), "RTH", trade_ts=stamp
            )
            is False
        )
        assert live_guards["NVDA"].protection_fault_count == expected
    assert live_guards["NVDA"].protection_fault_count >= execution.PROTECTION_FAULT_ALERT_COUNT
    assert live_lane.counters["protection_faults"] == 3
    assert execution.drive_guard_price(live_guards["AMD"], Decimal("104.00"), "RTH", trade_ts=stamp)
    assert live_guards["AMD"].protection_fault_code is None
    snapshot = live_guards["NVDA"].to_position_snapshot("RTH")
    assert snapshot["protection_fault_code"] == "SUPERVISION_ERROR_APIERROR"
    assert snapshot["protection_fault_count"] == 3
    # Three in a row alert; a spent exit ladder alerts at once (nothing is left to try).
    assert execution.protection_fault_alerts("SUPERVISION_ERROR_APIERROR", 2) is False
    assert execution.protection_fault_alerts("SUPERVISION_ERROR_APIERROR", 3) is True
    assert execution.protection_fault_alerts("EXIT_LADDER_EXHAUSTED", 1) is True
    assert execution.protection_fault_alerts(None, 9) is False
    # The native stops of both positions rest untouched by the fault.
    assert _stop_cover_qty(live_sim, "NVDA") == 10 == _stop_cover_qty(live_sim, "AMD")


def test_protection_gap_holds_entries_and_self_clears(tmp_path: Path) -> None:
    # Paper farm. The stop of a held position is cancelled from outside. The next reconcile
    # finds a broker position without a working protective order: entries are held, with cause
    # and time, while that same reconcile puts the stop back. The reconcile after that finds
    # every position protected and the hold clears itself; the held candidate then enters.
    sims = _paper_sims()
    holder = sims[_PA1]
    holder.open_position("NVDA", 10, "100.00")
    for sim in sims.values():
        sim.set_price("MU", "100.00")
    seen: dict[str, Any] = {}

    def stop_cancelled_from_outside(_ctx: SimpleNamespace) -> None:
        (resting,) = _resting_stops(holder, "NVDA")
        holder.external_cancel(resting.id)

    def candidate_during_the_hold(ctx: SimpleNamespace) -> None:
        seen["held_doc"] = ctx.drive.docs[_EXECUTION_STATUS][-1]
        ctx.sip.deliver("MU", "100.00")
        seen["submits_while_held"] = _entry_submits(sims, "MU")

    def candidate_after_the_hold(ctx: SimpleNamespace) -> None:
        seen["cleared_doc"] = ctx.drive.docs[_EXECUTION_STATUS][-1]
        ctx.sip.deliver("MU", "100.00")

    run = _run_p3_supervisor(
        tmp_path / "paper",
        sims,
        session="RTH",
        ticks=5,
        scan_doc=_scan_with_movers("MU"),
        script={
            2: stop_cancelled_from_outside,
            3: candidate_during_the_hold,
            4: candidate_after_the_hold,
        },
    )
    held = seen["held_doc"]
    assert held["status"] == "ENTRY_HELD" and held["entry_fault"] is True
    assert held["entry_hold"]["held"] is True and held["entry_hold"]["cause"] == "PROTECTION_GAP"
    assert held["entry_hold"]["positions"] == [f"{_PA1}:NVDA"]
    assert held["entry_hold"]["since_utc"] is not None
    assert seen["submits_while_held"] == []
    cleared = seen["cleared_doc"]
    assert cleared["status"] != "ENTRY_HELD" and cleared["entry_fault"] is False
    assert cleared["entry_hold"]["held"] is False and cleared["entry_hold"]["positions"] == []
    assert cleared["entry_hold"]["last_cleared_utc"] is not None
    assert _entry_submits(sims, "MU") == ["ok"]
    assert _stop_cover_qty(holder, "NVDA") == 10
    assert run.status["entry_fault"] is False and run.status["entry_hold"]["holds_total"] == 1
    statuses = run.drive.statuses()
    assert statuses.count("ENTRY_HELD") == 1
    # The coverage manifest of the paper farm recorded the gap and its end.
    gaps = [doc["entry_hold_positions"] for doc in run.drive.docs[_COVERAGE_MANIFEST]]
    assert [f"{_PA1}:NVDA"] in gaps and gaps[-1] == []

    # Live lane: the same check and the same self-clearing hold, written to the live directory.
    live_sim = _live_sim(session="PRE_MARKET")
    live_sim.open_position("NVDA", 10, "100.00")

    def live_stop_cancelled(_ctx: SimpleNamespace) -> None:
        (resting,) = _resting_stops(live_sim, "NVDA")
        live_sim.external_cancel(resting.id)

    live = _run_p3_supervisor(
        tmp_path / "live", live_sim, session="PRE_MARKET", ticks=4, script={2: live_stop_cancelled}
    )
    live_docs = live.drive.docs[_EXECUTION_STATUS]
    assert [doc["entry_fault"] for doc in live_docs if doc["status"] == "ENTRY_HELD"] == [True]
    assert live.status["entry_fault"] is False and live.status["entry_hold"]["holds_total"] == 1
    assert live.status["paper_only"] is False and _stop_cover_qty(live_sim, "NVDA") == 10
    assert live.coverage["all_shares_protected"] is True
    assert live.coverage["entry_hold"]["held"] is False

    # A position adoption cannot take (no whole share) never holds entries.
    odd_sim = _live_sim(session="PRE_MARKET")
    odd_sim.open_position("NVDA", "0.4", "100.00")
    odd = _run_p3_supervisor(tmp_path / "odd", odd_sim, session="PRE_MARKET", ticks=3)
    assert "ENTRY_HELD" not in odd.drive.statuses() and odd.status["entry_fault"] is False
    assert odd.coverage["fractional_unprotected_qty"] == "0.4"
    assert odd.coverage["unprotected_shares_count"] == 0


def test_stale_entry_fault_in_status_file_is_cleared_on_start(tmp_path: Path) -> None:
    stale = {
        "status": "ENTRY_HELD",
        "entry_fault": True,
        "generated_at_utc": "2026-11-27T15:32:00Z",
    }

    # Paper farm: the status file of the earlier process says entries are held. The new process
    # does not read that back: its first report already says otherwise, and the candidate enters.
    sims = _paper_sims()
    for sim in sims.values():
        sim.set_price("NVDA", "100.00")
    paper_dir = tmp_path / "paper" / "manifests"
    paper_dir.mkdir(parents=True)
    (paper_dir / _EXECUTION_STATUS).write_text(json.dumps(stale), encoding="utf-8")
    run = _run_p3_supervisor(
        tmp_path / "paper",
        sims,
        session="RTH",
        cadence=3600.0,
        ticks=3,
        scan_doc=_scan_with_movers("NVDA"),
        script={2: lambda ctx: ctx.sip.deliver("NVDA", "100.00")},
    )
    assert run.manifest_dir == paper_dir
    first = run.drive.docs[_EXECUTION_STATUS][0]
    assert first["status"] == "AWAITING_MARKET_DATA" and first["entry_fault"] is False
    assert all(doc["entry_fault"] is False for doc in run.drive.docs[_EXECUTION_STATUS])
    assert "ENTRY_HELD" not in run.drive.statuses()
    assert _entry_submits(sims, "NVDA") == ["ok"]
    assert run.status["entry_fault"] is False and run.status["entry_hold"]["holds_total"] == 0

    # Live lane: the same stale file in the live directory is overwritten the same way.
    live_sim = _live_sim(session="PRE_MARKET")
    live_sim.open_position("NVDA", 10, "100.00")
    live_dir = tmp_path / "live" / "live" / "manifests"
    live_dir.mkdir(parents=True)
    (live_dir / _EXECUTION_STATUS).write_text(json.dumps(stale), encoding="utf-8")
    live = _run_p3_supervisor(tmp_path / "live", live_sim, session="PRE_MARKET", ticks=3)
    assert live.manifest_dir == live_dir
    assert [doc["entry_fault"] for doc in live.drive.docs[_EXECUTION_STATUS]] == [False] * len(
        live.drive.docs[_EXECUTION_STATUS]
    )
    assert live.status["entry_fault"] is False and live.status["managed_symbols"] == ["NVDA"]
    assert _stop_cover_qty(live_sim, "NVDA") == 10


def test_stop_replaces_are_throttled_and_reads_spare_the_exit_reserve(tmp_path: Path) -> None:
    # R14, replaces: at most one stop replace per guard and second, also for a rising price.
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    farm, lane = _live_farm(tmp_path / "live", sim)
    guards: dict[str, execution.Guard] = {}
    (guard,) = execution.adopt_unmanaged_broker_positions(farm, guards, session="RTH")
    sim.set_price("NVDA", "104.00")
    guard.on_price(Decimal("104.00"), "RTH")
    first = guard.stop
    assert len(sim.calls("replace_order_by_id")) == 1
    sim.set_price("NVDA", "108.00")
    guard.on_price(Decimal("108.00"), "RTH")  # inside the same second: nothing is sent
    assert guard.stop == first and len(sim.calls("replace_order_by_id")) == 1
    guard.last_stop_replace_mono -= execution.STOP_REPLACE_MIN_INTERVAL_SECS
    guard.on_price(Decimal("108.00"), "RTH")
    assert guard.stop > first and len(sim.calls("replace_order_by_id")) == 2
    assert lane.counters["stop_replaces"] == 2
    (resting,) = _resting_stops(sim, "NVDA")
    assert Decimal(str(resting.stop_price)) == guard.stop

    # R14, reads: a broker read takes a slot outside the exit reserve, or it is deferred and
    # counted. With the entry/modify part of the budget spent, the snapshot of the timer makes
    # no request at all and the whole reserve is still there for protective orders.
    read_sim = _live_sim(session="PRE_MARKET")
    read_sim.open_position("NVDA", 10, "100.00")
    read_farm, read_lane = _live_farm(tmp_path / "reads", read_sim)
    read_guards: dict[str, execution.Guard] = {}
    execution.adopt_unmanaged_broker_positions(read_farm, read_guards, session="PRE_MARKET")
    now = datetime.now(UTC)
    # With budget: exactly two read slots per lane, and the reconciler reuses those reads.
    read_sim.reset_call_log()
    snapshots, tags = execution._read_broker_snapshots(read_farm, now.timestamp())
    assert tags == [] and list(snapshots) == [_LIVE_ACCOUNT]
    assert [call.name for call in read_sim.calls()] == ["get_all_positions", "get_orders"]
    assert _budget_slots_used(read_lane) == 2
    read_sim.reset_call_log()
    assert execution.ensure_native_protective_stops(
        read_farm, read_guards, session="PRE_MARKET", now_utc=now, snapshots=snapshots
    ) == [f"{_LIVE_ACCOUNT}:NVDA:STOP_ATTACHED"]
    assert [call.name for call in read_sim.calls()] == ["submit_order"]
    # An account that was not read this cycle is left alone.
    unread = execution.ensure_native_protective_stops(
        read_farm, read_guards, session="PRE_MARKET", now_utc=now, snapshots={}
    )
    assert unread == []
    # Entry/modify budget spent: reads are deferred, counted, and send nothing.
    budget = read_lane.rate_budget
    spare = budget.max_rpm - budget.exit_reserve_rpm - _budget_slots_used(read_lane)
    for _ in range(spare):
        assert budget.try_consume(now.timestamp(), is_exit=False) is True
    read_sim.reset_call_log()
    assert execution._consume_read_slot(read_lane, now.timestamp()) is False
    assert execution._read_broker_snapshots(read_farm, now.timestamp()) == (
        {},
        [f"{_LIVE_ACCOUNT}:*:READ_DEFERRED"],
    )
    assert read_sim.calls() == [] and read_lane.counters["reads_deferred_total"] == 2
    assert _budget_slots_used(read_lane) == budget.max_rpm - budget.exit_reserve_rpm
    for _ in range(budget.exit_reserve_rpm):
        assert budget.try_consume(now.timestamp(), is_exit=True) is True
    assert budget.try_consume(now.timestamp(), is_exit=True) is False

    # The timer of the supervisor with the entry/modify budget spent: its reads are deferred,
    # so the reconcile and the audit leave that lane alone, say so, and attest nothing. No
    # request goes out and nothing is held on a guess.
    starved_sim = _live_sim(session="PRE_MARKET")
    starved_sim.open_position("NVDA", 10, "100.00")

    def spend_the_entry_budget(ctx: SimpleNamespace) -> None:
        starved = ctx.farm.lanes[_LIVE_ACCOUNT].rate_budget
        for _ in range(starved.max_rpm):
            starved.try_consume(time.time(), is_exit=False)
        starved_sim.reset_call_log()

    starved_run = _run_p3_supervisor(
        tmp_path / "starved",
        starved_sim,
        session="PRE_MARKET",
        ticks=3,
        script={1: spend_the_entry_budget},
    )
    deferred = [f"{_LIVE_ACCOUNT}:*:READ_DEFERRED"]
    assert starved_run.status["stop_coverage"]["last_actions"] == deferred
    assert starved_run.status["stop_coverage"]["reconcile_faults"] == 0
    assert starved_run.coverage["complete"] is False
    assert starved_run.coverage["accounts_not_read"] == [_LIVE_ACCOUNT]
    assert starved_run.coverage["all_shares_protected"] is False
    assert starved_run.status["entry_fault"] is False
    assert starved_run.status["entry_hold"]["holds_total"] == 0
    starved_lane = starved_run.farm.lanes[_LIVE_ACCOUNT]
    assert starved_lane.counters["reads_deferred_total"] >= 3
    assert starved_sim.calls() == [] and _stop_cover_qty(starved_sim, "NVDA") == 10

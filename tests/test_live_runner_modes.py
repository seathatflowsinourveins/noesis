"""Runner modes of the live lane: environment checks, account refresh (R9), the supervisor
start under account trouble and the read-only self-check."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import wait as futures_wait
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import duckdb
import pytest
from alpaca.common.enums import BaseURL
from alpaca.data.enums import DataFeed
from alpaca.trading.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, StopOrderRequest, TrailingStopOrderRequest
from tests.alpaca_sim import MUTATING, SIM_SESSIONS, FaultInjectingAlpacaSim, SimSession

from tier0_sota.engine import FORBIDDEN_LIVE_IBKR_PORTS, LIVE_TIERED_VENUE_ID
from tier0_sota.live import execution, runner
from tier0_sota.live import paper_farm as pf

# No test in this module may see a live-looking key in the environment or open an off-host socket.
pytestmark = pytest.mark.usefixtures("no_live_broker_access")

_LIVE_ACCOUNT = "ALPACA_LIVE1"
# Obviously fake credentials with the real prefix convention. They are handed to the check as a
# mapping and never enter the process environment.
_FAKE_LIVE_KEY_ID = "AK" + "RUNNERMODE" * 2
_FAKE_PAPER_KEY_ID = "PK" + _FAKE_LIVE_KEY_ID[2:]
_FAKE_SECRET = "runner-modes-secret-" * 2
_EXECUTION_STATUS = "paper_farm_execution_status.json"
_SCAN_MANIFEST = "live_whole_market_movers_manifest.json"
_REAL_CLASSIFY_SESSION = execution.engine.classify_us_equity_session_et
_WALL_CLOCK_WINDOW_NS = 86_400 * 1_000_000_000
_RESTRICTED = runner.ENTRY_GATE_ACCOUNT_RESTRICTED
_BOOK_FULL = runner.ENTRY_GATE_OPEN_ORDER_BOOK_FULL
_EQUITY = runner.ENTRY_GATE_EQUITY_UNVERIFIED
_HEALTHY_ACCOUNT: dict[str, Any] = {
    "status": "ACTIVE",
    "trading_blocked": False,
    "account_blocked": False,
    "trade_suspended_by_user": False,
    "equity": "100000",
    "last_equity": "100000",
}
# Account state -> the one entry gate it raises. None of them may stop protective supervision.
_GATED_ACCOUNTS: tuple[tuple[str, dict[str, Any], str], ...] = (
    ("trading_blocked", {"trading_blocked": True}, _RESTRICTED),
    ("account_blocked", {"account_blocked": True}, _RESTRICTED),
    ("trade_suspended_by_user", {"trade_suspended_by_user": True}, _RESTRICTED),
    ("status_inactive", {"status": "INACTIVE"}, _RESTRICTED),
    ("last_equity_zero", {"last_equity": "0"}, _EQUITY),
    ("equity_zero", {"equity": "0"}, _EQUITY),
)


@pytest.fixture(autouse=True)
def _isolated_host(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No test here depends on this host's TCP table, data root or paper rate profile."""
    monkeypatch.setattr(runner, "_scan_forbidden_live_ports", lambda: [])
    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.delenv("TIER0_ALPACA_RPM", raising=False)


# ------------------------------------------------------------------------------- helpers


def _live_env(**overrides: str | None) -> dict[str, str]:
    """A complete live environment as a mapping; ``None`` removes a name."""
    env = {
        "TIER0_VENUE_PROFILE": LIVE_TIERED_VENUE_ID,
        "ALPACA_LIVE_API_KEY": _FAKE_LIVE_KEY_ID,
        "ALPACA_LIVE_SECRET_KEY": _FAKE_SECRET,
    }
    for name, value in overrides.items():
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    return env


def _live_refusal(env: dict[str, str], match: str) -> str:
    """The refusal text; it never carries a credential value."""
    with pytest.raises(PermissionError, match=match) as refusal:
        runner.verify_live_environment(env)
    text = str(refusal.value)
    assert _FAKE_LIVE_KEY_ID not in text and _FAKE_SECRET not in text
    return text


def _live_sim(**kwargs: Any) -> FaultInjectingAlpacaSim:
    return FaultInjectingAlpacaSim(paper=False, **kwargs)


def _paper_farm(
    directory: Path,
) -> tuple[pf.PaperFarmCoordinator, dict[str, FaultInjectingAlpacaSim]]:
    directory.mkdir(parents=True, exist_ok=True)
    sims = {acct: FaultInjectingAlpacaSim(session="RTH") for acct in pf.ALPACA_LANE_PROFILES}
    return pf.PaperFarmCoordinator(directory, alpaca_clients=sims), sims


def _fill_order_book(
    sim: FaultInjectingAlpacaSim, count: int = runner.OPEN_ORDER_READ_LIMIT
) -> Any:
    """``count`` resting buy limits far below the market, as the owner might leave them."""
    sim.set_price("JUNK", "5.00")
    order = None
    for _ in range(count):
        order = sim.submit_order(
            LimitOrderRequest(
                symbol="JUNK",
                qty=1,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.GTC,
                limit_price=1.00,
            )
        )
    return order


def _fuse_rows(lane: pf.AlpacaPaperLane) -> list[bool]:
    """``tripped`` of every persisted daily-fuse row of the lane's ledger."""
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as db:
        return [
            bool(row[0]) for row in db.execute("SELECT tripped FROM paper_daily_fuse").fetchall()
        ]


def _resting_stops(sim: FaultInjectingAlpacaSim, symbol: str) -> list[Any]:
    """Open orders that are stop cover: GTC stops of any kind that are not ``held``."""
    return [
        order
        for order in sim.inspect_orders("open", symbol)
        if order.type in (OrderType.STOP, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP)
        and order.time_in_force is TimeInForce.GTC
        and order.status is not OrderStatus.HELD
    ]


def _stop_cover_qty(sim: FaultInjectingAlpacaSim, symbol: str) -> Decimal:
    return sum(
        (Decimal(str(o.qty)) - Decimal(str(o.filled_qty)) for o in _resting_stops(sim, symbol)),
        Decimal("0"),
    )


def _dir_bytes(directory: Path) -> dict[str, bytes]:
    if not directory.exists():
        return {}
    return {
        path.relative_to(directory).as_posix(): path.read_bytes()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def _contains_no_float(value: Any) -> bool:
    if isinstance(value, float):
        return False
    if isinstance(value, dict):
        return all(_contains_no_float(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return all(_contains_no_float(item) for item in value)
    return True


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


class _FreshQuotes:
    """Market-data client stand-in: a tight, fresh quote for whatever symbol is asked."""

    def get_stock_latest_quote(self, request: Any) -> dict[str, Any]:
        quote = SimpleNamespace(bid_price=100.0, ask_price=100.01, timestamp=datetime.now(UTC))
        return {request.symbol_or_symbols: quote}


class _IdleStream:
    """A market-data stream that never delivers a print."""

    def __init__(self, drive: _TickDrive) -> None:
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


class _TickDrive:
    """Drives the supervisor loop for ``ticks`` ticks with no market data and no scanner file."""

    def __init__(self, ticks: int) -> None:
        self.ticks = ticks
        self.loop_ticks = 0
        self.finished = threading.Event()
        self.ever_subscribed: set[str] = set()
        self.streams = {feed: _IdleStream(self) for feed in (DataFeed.SIP, DataFeed.BOATS)}

    def wait(self, futures: Any, timeout: float | None = None, return_when: str = "") -> Any:
        del timeout  # the supervisor's 1 s tick, shortened so that a drive takes milliseconds
        self.loop_ticks += 1
        if self.loop_ticks > self.ticks:
            self.finished.set()
            return futures_wait(futures, timeout=30, return_when=return_when)
        return futures_wait(futures, timeout=0.005, return_when=return_when)


def _supervise_live(root: Path, sim: FaultInjectingAlpacaSim, *, session: str) -> SimpleNamespace:
    """Run ``run_execution`` on a simulator-backed live coordinator for a few loop ticks.

    Nothing is stubbed between the supervisor and the account: ``runner.refresh_accounts`` and
    ``runner.verify_live_environment``'s caller run as shipped (only the environment check
    itself answers "no tier-3 opt-in"). The shared directory stays empty.
    """
    shared_dir = root / "manifests"
    live_dir = root / "live" / "manifests"
    shared_dir.mkdir(parents=True)
    drive = _TickDrive(ticks=3)
    built: list[pf.AlpacaLiveVenueCoordinator] = []

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

    with contextlib.ExitStack() as stack:
        for target, name, replacement in (
            (execution, "build_live_venue_coordinator", build_coordinator),
            (execution, "wait", drive.wait),
            (execution, "STOP_COVERAGE_RECONCILE_SECS", 0.0),
            (execution.engine, "classify_us_equity_session_et", _session_classifier(session)),
            # The simulator's virtual clock runs years ahead of the wall clock: no skew is read.
            (execution, "_measure_broker_clock_skew", lambda _broker: None),
            (runner, "verify_live_environment", lambda: False),
        ):
            stack.enter_context(mock.patch.object(target, name, replacement))
        assert execution.refresh_accounts is runner.refresh_accounts  # the shipped one runs
        execution.run_execution(live_dir, venue_profile=LIVE_TIERED_VENUE_ID, shared_dir=shared_dir)

    (farm,) = built
    return SimpleNamespace(
        farm=farm,
        lane=farm.lanes[_LIVE_ACCOUNT],
        drive=drive,
        status=json.loads((live_dir / _EXECUTION_STATUS).read_text("utf-8")),
        live_dir=live_dir,
        shared_dir=shared_dir,
    )


def _self_check(
    root: Path, sim: FaultInjectingAlpacaSim, *, opt_in: bool = False
) -> dict[str, Any]:
    """``run_live_self_check`` on a simulator-backed live coordinator (environment check passed)."""
    shared_dir = root / "manifests"
    shared_dir.mkdir(parents=True, exist_ok=True)

    def build_coordinator(
        manifest_dir: Path, *, autonomous_opt_in: bool
    ) -> pf.AlpacaLiveVenueCoordinator:
        return pf.AlpacaLiveVenueCoordinator(
            manifest_dir, trading_client=sim, autonomous_opt_in=autonomous_opt_in
        )

    with (
        mock.patch.object(runner, "verify_live_environment", lambda: opt_in),
        mock.patch.object(runner, "build_live_venue_coordinator", build_coordinator),
    ):
        return runner.run_live_self_check(root / "live" / "manifests", shared_dir=shared_dir)


def _trailing_sell(sim: FaultInjectingAlpacaSim, symbol: str, qty: int, percent: float) -> Any:
    """A native trailing-stop sell as it rests on the real account (placed outside the lane)."""
    return sim.submit_order(
        TrailingStopOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            trail_percent=percent,
            client_order_id=f"trail-adopted-alpaca_live1-{symbol.lower()}-0001",
        )
    )


def _account_like_the_real_one(session: SimSession) -> FaultInjectingAlpacaSim:
    """Positions and resting orders shaped like the live account, moved to ``session``.

    ANT 9 shares with a trailing sell for 8 and the owner's resting buy; SOXL fully covered;
    NVDA in profit and MU under water, both uncovered; ARM under the owner's plain stop; half a
    share of PLTR; and a resting buy for MRNA, which is not held. Outside the regular session
    ARM then trades below its stop, which rests until the next regular open.
    """
    sim = _live_sim()
    sim.open_position("ANT", 9, "40.00")
    _trailing_sell(sim, "ANT", 8, 0.41)
    sim.submit_order(
        LimitOrderRequest(
            symbol="ANT", qty=2, side=OrderSide.BUY, time_in_force=TimeInForce.GTC, limit_price=39.0
        )
    )
    sim.open_position("SOXL", 82, "30.00")
    _trailing_sell(sim, "SOXL", 82, 0.17)
    sim.open_position("NVDA", 10, "100.00")
    sim.set_price("NVDA", "103.00")
    sim.open_position("MU", 2, "100.00")
    sim.set_price("MU", "95.00")
    sim.open_position("ARM", 22, "100.00")
    sim.submit_order(
        StopOrderRequest(
            symbol="ARM",
            qty=22,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            stop_price=98.0,
            client_order_id="owner-arm-stop",
        )
    )
    sim.open_position("PLTR", "0.5", "20.00")
    sim.set_price("MRNA", "30.00")
    sim.submit_order(
        LimitOrderRequest(
            symbol="MRNA",
            qty=500,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC,
            limit_price=25.0,
        )
    )
    if session != "RTH":
        sim.set_session(session)
        sim.set_price("ARM", "97.00")
    sim.reset_call_log()
    return sim


def _mixed_positions(session: SimSession) -> FaultInjectingAlpacaSim:
    """Longs and shorts in profit and under water, moved to ``session``.

    ANT is partly covered by a trailing sell; three AMD shares are reserved by the owner's own
    resting limit sell, so only four are free for a stop. NVDA is far in profit (the tier stop
    decides its level), ARM a little (the non-noise stop under the peak decides).
    """
    sim = _live_sim()
    sim.open_position("NVDA", 10, "100.00")
    sim.set_price("NVDA", "106.00")
    sim.open_position("AMD", 7, "50.00")
    sim.set_price("AMD", "50.10")
    sim.submit_order(
        LimitOrderRequest(
            symbol="AMD",
            qty=3,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=60.0,
        )
    )
    sim.open_position("MU", 2, "100.00")
    sim.set_price("MU", "95.00")
    sim.open_position("ARM", 6, "100.00")
    sim.set_price("ARM", "101.00")
    sim.open_position("ANT", 9, "40.00")
    _trailing_sell(sim, "ANT", 8, 0.41)
    sim.open_position("TSLA", -5, "200.00")
    sim.set_price("TSLA", "190.00")
    sim.open_position("QQQ", -3, "400.00")
    sim.set_price("QQQ", "404.00")
    if session != "RTH":
        sim.set_session(session)
    sim.reset_call_log()
    return sim


# ---------------------------------------------------------------- environment separation


def test_verify_live_environment_matrix(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # The complete live environment passes and returns the tier-3 opt-in, a strict bool.
    assert runner.verify_live_environment(_live_env()) is False
    for off in ("false", "FALSE", " False ", ""):
        assert runner.verify_live_environment(_live_env(LIVE_EXECUTION_ENABLED=off)) is False
    for on in ("true", "TRUE", " True "):
        assert runner.verify_live_environment(_live_env(LIVE_EXECUTION_ENABLED=on)) is True
    for not_a_flag in ("1", "yes", "on", "enabled", "0", "truthy"):
        _live_refusal(_live_env(LIVE_EXECUTION_ENABLED=not_a_flag), "LIVE_EXECUTION_ENABLED")

    # Venue: the process must name the tiered venue, and only that one.
    assert (
        runner.verify_live_environment(
            _live_env(TIER0_VENUE_PROFILE=f" {LIVE_TIERED_VENUE_ID.lower()} ")
        )
        is False
    )
    for venue in (
        None,
        "",
        "ALPACA_PAPER_200",
        "ALPACA_PAPER_1000",
        "IBKR_PAPER_4002",
        "IBKR_LIVE_3000",
        "ALPACA_LIVE_ELITE_SMART_ROUTER",
        LIVE_TIERED_VENUE_ID + "_X",
    ):
        _live_refusal(_live_env(TIER0_VENUE_PROFILE=venue), "TIER0_VENUE_PROFILE")
    # A tiered venue the engine no longer grants the protective tier, or that is not Alpaca
    # live, refuses too.
    with monkeypatch.context() as patched:
        patched.setattr(
            runner.engine,
            "validate_venue_order_capability",
            lambda *_args, **_kwargs: SimpleNamespace(broker="ALPACA", environment="PAPER"),
        )
        _live_refusal(_live_env(), "not Alpaca live")

        def refuse(venue_id: str, **_kwargs: Any) -> Any:
            raise PermissionError(
                f"Refused live profile {venue_id!r}: live_execution_enabled=False"
            )

        patched.setattr(runner.engine, "validate_venue_order_capability", refuse)
        _live_refusal(_live_env(), "Refused live profile")

    # Credential: an AK key id and its secret, both present.
    _live_refusal(_live_env(ALPACA_LIVE_API_KEY=None), "ALPACA_LIVE_API_KEY")
    for absent in (None, "", "  "):
        _live_refusal(_live_env(**{"ALPACA_LIVE_SECRET_KEY": absent}), "ALPACA_LIVE_SECRET_KEY")
    paper_key = _live_refusal(_live_env(ALPACA_LIVE_API_KEY=_FAKE_PAPER_KEY_ID), "AK prefix")
    assert _FAKE_PAPER_KEY_ID not in paper_key

    # Paper mode flag: unset or false only.
    for flag in ("false", "FALSE", ""):
        assert runner.verify_live_environment(_live_env(ALPACA_PAPER=flag)) is False
    for flag in ("true", "TRUE", " true ", "1", "yes"):
        _live_refusal(_live_env(ALPACA_PAPER=flag), "ALPACA_PAPER")

    # Base URL: unset or alpaca-py's own live trading endpoint.
    live_url = str(BaseURL.TRADING_LIVE.value)
    for url in (live_url, live_url + "/", live_url.upper(), ""):
        assert runner.verify_live_environment(_live_env(APCA_API_BASE_URL=url)) is False
    for url in (pf.PAPER_ALPACA_ENDPOINT, str(BaseURL.TRADING_PAPER.value), "https://example.test"):
        _live_refusal(_live_env(APCA_API_BASE_URL=url), "APCA_API_BASE_URL")

    # IBKR: no live port in the environment, none open on the host.
    for port in sorted(FORBIDDEN_LIVE_IBKR_PORTS):
        _live_refusal(_live_env(IBKR_PORT=str(port)), "IBKR_PORT")
    assert runner.verify_live_environment(_live_env(IBKR_PORT="4002")) is False
    with monkeypatch.context() as patched:
        patched.setattr(runner, "_scan_forbidden_live_ports", lambda: [4001, 7496])
        assert "4001, 7496" in _live_refusal(_live_env(), "IBKR live port")

    # Risk kernel: the pinned digest, for the live and for the paper worker.
    tampered = tmp_path / "risk.py"
    tampered.write_bytes(Path(runner.risk.__file__).read_bytes() + b"\n# tampered\n")
    with monkeypatch.context() as patched:
        patched.setattr(runner, "risk", SimpleNamespace(__file__=str(tampered)))
        _live_refusal(_live_env(), "Live worker risk kernel digest mismatch")
        with pytest.raises(PermissionError, match="Paper worker risk kernel digest mismatch"):
            runner.verify_paper_environment()

    # Without a mapping the process environment is read; with one it is never consulted.
    for name in ("ALPACA_PAPER", "APCA_API_BASE_URL", "IBKR_PORT", "LIVE_EXECUTION_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    for name in list(runner.os.environ):
        if name.upper().startswith(("ALPACA_PA", "ALPACA_LIVE_", "APCA_API_")):
            monkeypatch.delenv(name)
    with pytest.raises(PermissionError, match="TIER0_VENUE_PROFILE"):
        runner.verify_live_environment()
    monkeypatch.setenv("TIER0_VENUE_PROFILE", LIVE_TIERED_VENUE_ID)
    monkeypatch.setenv("ALPACA_LIVE_API_KEY", "AK-process-env")  # not a live-looking key
    monkeypatch.setenv("ALPACA_LIVE_SECRET_KEY", "process-env-secret")
    assert runner.verify_live_environment() is False
    monkeypatch.setenv("LIVE_EXECUTION_ENABLED", "true")
    assert runner.verify_live_environment() is True
    assert runner.verify_live_environment(_live_env()) is False
    monkeypatch.setenv("ALPACA_PAPER", "true")
    assert runner.verify_live_environment(_live_env()) is False
    with pytest.raises(PermissionError, match="ALPACA_PAPER"):
        runner.verify_live_environment()


def test_live_environment_rejects_paper_key_names(monkeypatch: pytest.MonkeyPatch) -> None:
    # A live process must not inherit any paper key name, whatever its value: the paper
    # market-data pair and every ALPACA_PA* name.
    paper_names = (
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
        "ALPACA_PA1_API_KEY",
        "ALPACA_PA1_SECRET",
        "ALPACA_PA2_SECRET_KEY",
        "ALPACA_PA3_API_KEY",
        "ALPACA_PA9_API_KEY",
        "ALPACA_PAPER_KEY",
        "alpaca_pa1_api_key",
    )
    for name in paper_names:
        for value in ("PKPAPERVALUE0001", "paper-secret-value", ""):
            text = _live_refusal(_live_env(**{name: value}), "paper key name")
            assert name in text
            assert not value or value not in text
    # Every offending name is listed, sorted; the paper-mode flag is judged by its value.
    text = _live_refusal(
        _live_env(
            **dict.fromkeys(("APCA_API_SECRET_KEY", "ALPACA_PA2_API_KEY", "APCA_API_KEY_ID"), "x")
        ),
        "paper key name",
    )
    assert text.endswith("ALPACA_PA2_API_KEY, APCA_API_KEY_ID, APCA_API_SECRET_KEY")
    assert runner.verify_live_environment(_live_env(ALPACA_PAPER="false")) is False
    assert runner._paper_key_names({"ALPACA_PAPER": "true", "ALPACA_LIVE_API_KEY": "x"}) == []

    # The symmetric rule: a paper process refuses any live key name, dormant or blank.
    for name in list(runner.os.environ):
        if name.upper().startswith(("ALPACA_LIVE_", "APCA_API_")):
            monkeypatch.delenv(name)
    for name, value in {
        "ALPACA_PAPER": "true",
        "LIVE_EXECUTION_ENABLED": "false",
        "IBKR_HOST": "127.0.0.1",
        "IBKR_PORT": "4002",
        "ALPACA_PA1_API_KEY": "PKTEST1",
        "ALPACA_PA1_SECRET": "sec1",
        "ALPACA_PA2_API_KEY": "PKTEST2",
        "ALPACA_PA2_SECRET_KEY": "sec2",
        "ALPACA_PA3_API_KEY": "PKTEST3",
        "ALPACA_PA3_SECRET": "sec3",
    }.items():
        monkeypatch.setenv(name, value)
    runner.verify_paper_environment()
    for name in ("ALPACA_LIVE_API_KEY", "ALPACA_LIVE_SECRET_KEY", "ALPACA_LIVE_ANYTHING_ELSE"):
        for value in ("AK_DORMANT_ISOLATED", "dormant-secret-value", ""):
            monkeypatch.setenv(name, value)
            with pytest.raises(PermissionError, match="live key name") as refusal:
                runner.verify_paper_environment()
            assert name in str(refusal.value)
            assert not value or value not in str(refusal.value)
            monkeypatch.delenv(name)
    runner.verify_paper_environment()
    # The owner's tier-3 opt-in is not a key name; the paper profile judges it by value as before.
    assert runner._live_key_names({"LIVE_EXECUTION_ENABLED": "false", "ALPACA_PAPER": "true"}) == []


# ----------------------------------------------------------------- refresh_accounts (R9)


@pytest.mark.parametrize(
    ("fields", "gate"), [case[1:] for case in _GATED_ACCOUNTS], ids=[c[0] for c in _GATED_ACCOUNTS]
)
def test_refresh_accounts_gates_entries_only_and_lifts_by_itself(
    tmp_path: Path, fields: dict[str, Any], gate: str
) -> None:
    farm, sims = _paper_farm(tmp_path)
    sims["ALPACA_PA3"].open_position("NVDA", 10, "100.00")
    sims["ALPACA_PA3"].set_price("AMD", "100.00")
    lanes = farm.lanes

    def enter() -> dict[str, Any] | None:
        return lanes["ALPACA_PA3"].submit_guarded_entry(
            symbol="AMD", side="LONG", ref_price=Decimal("100.00")
        )

    # Healthy: every lane admits entries and carries the time its equity was read.
    t0 = datetime.now(UTC)
    inventory = runner.refresh_accounts(farm, t0, "RTH", publish=False)
    assert set(inventory) == set(lanes) and set(inventory["ALPACA_PA3"]) == {"NVDA"}
    assert runner.account_entry_gates(farm) == {}
    assert all(lane.breaker.allow_entry() for lane in lanes.values())
    assert all(lane.equity_verified_at == t0.timestamp() for lane in lanes.values())

    # One account in trouble: no exception, the positions are still returned (that is all
    # protective supervision needs), and no lane of the farm admits an entry.
    sims["ALPACA_PA2"].set_account(**fields)
    t1 = t0 + timedelta(seconds=5)
    inventory = runner.refresh_accounts(farm, t1, "RTH", publish=False)
    assert set(inventory) == set(lanes) and set(inventory["ALPACA_PA3"]) == {"NVDA"}
    assert farm.symbol_lock.snapshot() == {"NVDA": "ALPACA_PA3"}
    assert runner.account_entry_gates(farm) == {"ALPACA_PA2": (gate,)}
    assert not any(lane.breaker.allow_entry() for lane in lanes.values())
    assert all(lane.equity_verified_at is None for lane in lanes.values())
    assert pf.select_situation_lane(lanes, natr_pct=Decimal("0.012")) is None
    assert enter() is None
    assert all(sim.mutating_call_count == 0 for sim in sims.values())
    # The gate is memory only: no daily-fuse row says tripped, so nothing latches for the day.
    assert all(_fuse_rows(lane) == [False] for lane in lanes.values())

    # Trouble over: the next refresh lifts the gate by itself and the same entry goes out.
    sims["ALPACA_PA2"].set_account(**_HEALTHY_ACCOUNT)
    t2 = t1 + timedelta(seconds=5)
    runner.refresh_accounts(farm, t2, "RTH", publish=False)
    assert runner.account_entry_gates(farm) == {}
    assert all(lane.breaker.allow_entry() for lane in lanes.values())
    assert all(lane.equity_verified_at == t2.timestamp() for lane in lanes.values())
    assert enter() is not None
    assert [call.outcome for call in sims["ALPACA_PA3"].calls("submit_order")] == ["ok"]


def test_refresh_accounts_full_order_book_gates_entries_only(tmp_path: Path) -> None:
    assert runner.OPEN_ORDER_READ_LIMIT == 500
    farm, sims = _paper_farm(tmp_path)
    sims["ALPACA_PA1"].open_position("NVDA", 10, "100.00")
    last = _fill_order_book(sims["ALPACA_PA2"])
    for sim in sims.values():
        sim.reset_call_log()
    now = datetime.now(UTC)
    inventory = runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert set(inventory["ALPACA_PA1"]) == {"NVDA"}
    assert runner.account_entry_gates(farm) == {"ALPACA_PA2": (_BOOK_FULL,)}
    assert not any(lane.breaker.allow_entry() for lane in farm.lanes.values())
    assert all(sim.mutating_call_count == 0 for sim in sims.values())
    # One order fewer and the inventory is attested complete again.
    sims["ALPACA_PA2"].external_cancel(last.id)
    runner.refresh_accounts(farm, now + timedelta(seconds=5), "RTH", publish=False)
    assert runner.account_entry_gates(farm) == {}
    assert all(lane.breaker.allow_entry() for lane in farm.lanes.values())


def test_refresh_accounts_keeps_the_daily_fuse_and_the_integrity_refusals(tmp_path: Path) -> None:
    # A real drawdown trip is still written to the fuse and still latches for the day; an
    # account gate that comes and goes on another lane neither sets nor clears it.
    farm, sims = _paper_farm(tmp_path / "fuse")
    lanes = farm.lanes
    now = datetime.now(UTC)
    sims["ALPACA_PA1"].set_account(equity="98000")  # -2.00% on the day, past the -1.50% fuse
    runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert runner.account_entry_gates(farm) == {}
    assert [lane.breaker.allow_entry() for lane in lanes.values()] == [False, True, True]
    assert [_fuse_rows(lane) for lane in lanes.values()] == [[True], [False], [False]]
    assert lanes["ALPACA_PA1"].equity_verified_at == now.timestamp()
    assert lanes["ALPACA_PA1"].start_equity == Decimal("98000")
    sims["ALPACA_PA1"].set_account(equity="100000")
    sims["ALPACA_PA2"].set_account(trading_blocked=True)
    runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert [lane.breaker.allow_entry() for lane in lanes.values()] == [False, False, False]
    sims["ALPACA_PA2"].set_account(trading_blocked=False)
    runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert [lane.breaker.allow_entry() for lane in lanes.values()] == [False, True, True]
    assert [_fuse_rows(lane) for lane in lanes.values()] == [[True], [False], [False]]
    # Without the fuse write (a dry run) the persisted trip is still honoured.
    runner.refresh_accounts(farm, now, "RTH", publish=False, record_fuse=False)
    assert [lane.breaker.allow_entry() for lane in lanes.values()] == [False, True, True]

    # An equity the broker does not state as a number: gated, the last known equity is kept and
    # nothing is recorded in the fuse for it.
    farm, sims = _paper_farm(tmp_path / "equity")
    lane = farm.lanes["ALPACA_PA2"]
    healthy = sims["ALPACA_PA2"].get_account()
    for equity, last_equity in ((None, "100000"), ("not-a-number", "100000"), ("100000", None)):
        account = SimpleNamespace(
            id=healthy.id,
            status=healthy.status,
            trading_blocked=False,
            account_blocked=False,
            trade_suspended_by_user=False,
            equity=equity,
            last_equity=last_equity,
        )
        with mock.patch.object(sims["ALPACA_PA2"], "get_account", lambda account=account: account):
            runner.refresh_accounts(farm, now, "RTH", publish=False)
        assert runner.account_entry_gates(farm) == {"ALPACA_PA2": (_EQUITY,)}
        assert lane.start_equity == Decimal("100000") and not lane.breaker.allow_entry()
        assert _fuse_rows(lane) == []
    runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert runner.account_entry_gates(farm) == {} and _fuse_rows(lane) == [False]

    # What still refuses outright: the coordinator's own shape, never an account's state.
    farm, sims = _paper_farm(tmp_path / "missing")
    del farm.lanes["ALPACA_PA3"]
    with pytest.raises(PermissionError, match="All three Alpaca paper accounts are required"):
        runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert all(sim.read_call_count == 0 for sim in sims.values())

    shared = FaultInjectingAlpacaSim(session="RTH")
    (tmp_path / "duplicate").mkdir()
    twice = pf.PaperFarmCoordinator(
        tmp_path / "duplicate",
        alpaca_clients={
            "ALPACA_PA1": shared,
            "ALPACA_PA2": shared,
            "ALPACA_PA3": FaultInjectingAlpacaSim(session="RTH"),
        },
    )
    with pytest.raises(PermissionError, match="ALPACA_PA2 is unavailable, blocked or duplicated"):
        runner.refresh_accounts(twice, now, "RTH", publish=False)

    farm, sims = _paper_farm(tmp_path / "no-id")
    nameless = SimpleNamespace(**{**_HEALTHY_ACCOUNT, "id": None})
    with (
        mock.patch.object(sims["ALPACA_PA1"], "get_account", lambda: nameless),
        pytest.raises(PermissionError, match="ALPACA_PA1 is unavailable, blocked or duplicated"),
    ):
        runner.refresh_accounts(farm, now, "RTH", publish=False)

    farm, sims = _paper_farm(tmp_path / "overlap")
    sims["ALPACA_PA1"].open_position("NVDA", 5, "100.00")
    sims["ALPACA_PA2"].open_position("NVDA", 5, "100.00")
    with pytest.raises(PermissionError, match="Overlapping paper exposure for NVDA"):
        runner.refresh_accounts(farm, now, "RTH", publish=False)

    farm, sims = _paper_farm(tmp_path / "identity")
    runner.refresh_accounts(farm, now, "RTH", publish=False)
    farm.lanes["ALPACA_PA1"].trading_client = FaultInjectingAlpacaSim(session="RTH")
    with pytest.raises(PermissionError, match="ALPACA_PA1 broker identity changed"):
        runner.refresh_accounts(farm, now, "RTH", publish=False)


def test_refresh_accounts_serves_the_live_coordinator(tmp_path: Path) -> None:
    # The live coordinator requires its one account, not the three paper ones.
    sim = _live_sim(session="RTH", equity="250000")
    sim.open_position("NVDA", 10, "100.00")
    sim.reset_call_log()
    (tmp_path / "live").mkdir()
    farm = pf.AlpacaLiveVenueCoordinator(tmp_path / "live", trading_client=sim)
    lane = farm.lanes[_LIVE_ACCOUNT]
    assert farm.required_accounts == (_LIVE_ACCOUNT,) and lane.equity_verified_at is None
    assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") == "HOLD_LIVE_EQUITY_UNVERIFIED"
    now = datetime.now(UTC)
    inventory = runner.refresh_accounts(farm, now, "RTH", publish=False)
    assert list(inventory) == [_LIVE_ACCOUNT] and set(inventory[_LIVE_ACCOUNT]) == {"NVDA"}
    assert lane.start_equity == Decimal("250000") and lane.equity_verified_at == now.timestamp()
    assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") is None
    assert runner.account_entry_gates(farm) == {} and sim.mutating_call_count == 0
    assert {call.name for call in sim.call_log} == {
        "get_account",
        "get_all_positions",
        "get_orders",
    }

    farm.required_accounts = (_LIVE_ACCOUNT, "ALPACA_LIVE2")
    with pytest.raises(PermissionError, match="do not match its required accounts"):
        runner.refresh_accounts(farm, now, "RTH", publish=False)
    # A coordinator that was never refreshed states no gate.
    unrefreshed = pf.AlpacaLiveVenueCoordinator(tmp_path / "unrefreshed", trading_client=sim)
    assert runner.account_entry_gates(unrefreshed) == {}


# ------------------------------------------------- supervisor start under account trouble


@pytest.mark.parametrize("session", ["RTH", "OVERNIGHT"])
@pytest.mark.parametrize(
    "condition", ["healthy", "trading_blocked", "status_inactive", "last_equity_zero", "book_full"]
)
def test_supervision_starts_when_account_trading_blocked(
    tmp_path: Path, condition: str, session: SimSession
) -> None:
    sim = _live_sim(session=session)
    sim.set_asset("NVDA", attributes=["overnight_tradable"])
    sim.open_position("NVDA", 10, "100.00")
    gate: str | None = None
    if condition == "trading_blocked":
        sim.set_account(trading_blocked=True)
        gate = _RESTRICTED
    elif condition == "status_inactive":
        sim.set_account(status="INACTIVE")
        gate = _RESTRICTED
    elif condition == "last_equity_zero":
        sim.set_account(last_equity="0")
        gate = _EQUITY
    elif condition == "book_full":
        _fill_order_book(sim)
        gate = _BOOK_FULL
    sim.reset_call_log()

    # The supervisor starts and keeps running on the shipped refresh_accounts: before R9 this
    # start raised and no position was supervised at all.
    run = _supervise_live(tmp_path, sim, session=session)
    lane, status = run.lane, run.status
    assert run.drive.loop_ticks > run.drive.ticks and status["status"] == "STOPPED"
    assert status["paper_only"] is False and status["live_execution_enabled"] is False
    # Supervision came from positions and orders alone: the position has its guard.
    assert status["managed_symbols"] == ["NVDA"] and status["active_positions_count"] == 1
    assert status["stop_coverage"]["reconcile_faults"] == 0
    attaches = sim.calls("submit_order")
    assert attaches, "the protective stop was never sent to the broker"
    assert all(call.payload["side"] == "sell" for call in attaches)
    if gate == _RESTRICTED:
        # The broker itself refuses new orders on a restricted account: the lane still sent the
        # protective stop (it is not what holds it back) and supervises the position on ticks.
        assert {call.outcome for call in attaches} == {"error:403"}
        assert _resting_stops(sim, "NVDA") == []
    else:
        # The account takes orders: the protective stop rests for the whole position although
        # entries are gated.
        assert [call.outcome for call in attaches] == ["ok"]
        assert _stop_cover_qty(sim, "NVDA") == Decimal("10")
    assert sim.calls("cancel_order_by_id") == [] and sim.calls("close_position") == []

    # Entries, and only entries, are gated: at the lane's own hold and at its choke point.
    entry = LimitOrderRequest(
        symbol="NVDA", qty=1, side=OrderSide.BUY, time_in_force=TimeInForce.DAY, limit_price=99.0
    )
    submits_before = len(sim.calls("submit_order"))
    if gate is None:
        assert runner.account_entry_gates(run.farm) == {}
        assert lane.breaker.allow_entry() and isinstance(lane.equity_verified_at, float)
        assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") is None
    else:
        assert runner.account_entry_gates(run.farm) == {_LIVE_ACCOUNT: (gate,)}
        assert not lane.breaker.allow_entry() and lane.equity_verified_at is None
        for intent in ("OPERATOR_CONFIRMED_ENTRY", "OWNER_LINEAGE_ENTRY", "AUTONOMOUS_ENTRY"):
            assert lane.entry_hold_reason(intent, leg_merit=True) is not None
        assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") == "HOLD_LIVE_EQUITY_UNVERIFIED"
        with pytest.raises(PermissionError, match="HOLD_LIVE_EQUITY_UNVERIFIED"):
            lane.broker_submit(entry, intent="OPERATOR_CONFIRMED_ENTRY")
        assert len(sim.calls("submit_order")) == submits_before
        # A protective order is never refused by the lane, gate or no gate.
        lane._authorize_broker_mutation("PROTECTIVE")
    # The gate is not the daily fuse: nothing latched in the ledger.
    assert not any(_fuse_rows(lane))

    # The trouble ends: one refresh lifts the gate without a restart.
    sim.set_account(**{k: v for k, v in _HEALTHY_ACCOUNT.items() if k != "equity"})
    for order in sim.inspect_orders("open", "JUNK")[:2]:
        sim.external_cancel(order.id)
    later = datetime.now(UTC)
    runner.refresh_accounts(run.farm, later, session, publish=False)
    assert runner.account_entry_gates(run.farm) == {}
    assert lane.breaker.allow_entry() and lane.equity_verified_at == later.timestamp()
    assert lane.entry_hold_reason("OPERATOR_CONFIRMED_ENTRY") is None


def _scan_with_one_mover() -> tuple[dict[str, Any], str]:
    """A fresh regular-session scan manifest whose one eligible mover is a long."""
    now_iso = datetime.now(UTC).isoformat()
    mover: dict[str, Any] = {
        "symbol": "NVDA",
        "extreme_mover_eligible": True,
        "side": "BUY",
        "session": "RTH",
        "bar_close_utc": now_iso,
        "natr_pct": 0.012,
        "last_price": 100,
        "shortable": True,
        "easy_to_borrow": True,
        "borrow_source": "alpaca_paper_asset",
        "prior_close": 99.0,
        "annual_borrow_rate": 0.0,
    }
    route = pf.select_short_or_inverse_fallback(
        "NVDA", market_regime=1, borrow_meta=mover, session="RTH"
    )
    assert route is not None and route[1] == "LONG"
    doc = {"generated_at_utc": now_iso, "macro_regime": {"regime": 1}, "entries": [mover]}
    return doc, route[0]


class _PrintStream:
    """One market-data stream of a ``_PrintDrive``."""

    def __init__(self, drive: _PrintDrive, feed: DataFeed) -> None:
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
        if self.feed is DataFeed.SIP:
            try:
                assert self.callback is not None
                drive.before_print()
                trade = SimpleNamespace(
                    symbol=drive.symbol, price=100.0, timestamp=datetime.now(UTC)
                )
                asyncio.run(self.callback(trade))
            finally:
                drive.print_delivered.set()
        assert drive.print_delivered.wait(15), "the active feed never delivered its print"

    def stop(self) -> None:
        self.drive.print_delivered.set()


class _PrintDrive:
    """Delivers one print for ``symbol`` once the supervisor has built its candidates.

    The loop builds its candidates between two ``wait`` calls, so the second call means one
    refresh is complete. ``before_print`` then plays the broker, just ahead of the print.
    """

    def __init__(self, symbol: str, before_print: Callable[[], None]) -> None:
        self.symbol = symbol
        self.before_print = before_print
        self.loop_ticks = 0
        self.candidates_built = threading.Event()
        self.print_delivered = threading.Event()
        self.ever_subscribed: set[str] = set()
        self.streams = {feed: _PrintStream(self, feed) for feed in (DataFeed.SIP, DataFeed.BOATS)}

    def wait(self, futures: Any, timeout: float | None = None, return_when: str = "") -> Any:
        del timeout
        self.loop_ticks += 1
        if self.loop_ticks >= 2:
            self.candidates_built.set()
        return futures_wait(futures, timeout=0.005, return_when=return_when)


def _drive_paper_entry(root: Path, *, block_before_print: str | None) -> SimpleNamespace:
    """The paper supervisor with one scanner mover and one print, on three simulators.

    ``block_before_print`` names the account the broker restricts after the candidate is built
    and before its print arrives: the entry then meets the pre-admission account recheck.
    """
    manifest_dir = root / "manifests"
    manifest_dir.mkdir(parents=True)
    scan_doc, symbol = _scan_with_one_mover()
    (manifest_dir / _SCAN_MANIFEST).write_text(json.dumps(scan_doc), encoding="utf-8")
    farm, sims = _paper_farm(manifest_dir)
    for sim in sims.values():
        sim.set_price(symbol, "100.00")

    def before_print() -> None:
        if block_before_print is not None:
            sims[block_before_print].set_account(trading_blocked=True)

    drive = _PrintDrive(symbol, before_print)
    with contextlib.ExitStack() as stack:
        for target, name, replacement in (
            (execution, "verify_paper_environment", lambda: None),
            (execution, "resolve_alpaca_env_creds", lambda: ("", "")),
            (execution, "PaperFarmCoordinator", lambda _dir: farm),
            (execution, "StockDataStream", lambda *_args, feed: drive.streams[feed]),
            (execution, "wait", drive.wait),
            (execution.engine, "classify_us_equity_session_et", _session_classifier("RTH")),
            (execution, "_measure_broker_clock_skew", lambda _broker: None),
            (
                execution.ExtremeMoverScanner,
                "from_env",
                lambda: SimpleNamespace(data_client=_FreshQuotes()),
            ),
            (farm, "trade_update_streams", lambda: {}),
        ):
            stack.enter_context(mock.patch.object(target, name, replacement))
        assert execution.refresh_accounts is runner.refresh_accounts  # the shipped one runs
        execution.run_execution(manifest_dir)
    return SimpleNamespace(
        farm=farm,
        sims=sims,
        drive=drive,
        symbol=symbol,
        status=json.loads((manifest_dir / _EXECUTION_STATUS).read_text("utf-8")),
    )


def test_paper_entry_recheck_submits_nothing_once_an_account_is_restricted(tmp_path: Path) -> None:
    # Control: with three healthy accounts the scripted candidate and print reach the broker.
    healthy = _drive_paper_entry(tmp_path / "healthy", block_before_print=None)
    entries = [call for sim in healthy.sims.values() for call in sim.calls("submit_order")]
    assert len(entries) == 1 and entries[0].outcome == "ok"
    assert entries[0].payload["symbol"] == healthy.symbol and entries[0].payload["side"] == "buy"
    assert healthy.status["managed_symbols"] == [healthy.symbol]
    assert runner.account_entry_gates(healthy.farm) == {}
    routed = [acct for acct, sim in healthy.sims.items() if sim.calls("submit_order")]

    # The same candidate and print, but another account of the farm is restricted just before
    # the print: the entry is stopped at the account recheck, before the asset is even looked
    # up, and nothing is sent to any account. It used to be stopped by an exception.
    (routed_account,) = routed
    other = next(acct for acct in healthy.sims if acct != routed_account)
    blocked = _drive_paper_entry(tmp_path / "blocked", block_before_print=other)
    assert blocked.symbol in blocked.drive.ever_subscribed  # the candidate had been built
    assert all(sim.mutating_call_count == 0 for sim in blocked.sims.values())
    assert all(sim.calls("get_asset") == [] for sim in blocked.sims.values())
    assert runner.account_entry_gates(blocked.farm) == {other: (_RESTRICTED,)}
    assert not any(lane.breaker.allow_entry() for lane in blocked.farm.lanes.values())
    assert blocked.status["managed_symbols"] == [] and blocked.status["status"] == "STOPPED"
    assert not any(any(_fuse_rows(lane)) for lane in blocked.farm.lanes.values())


# ----------------------------------------------------------------------- live self-check


@pytest.mark.parametrize("session", SIM_SESSIONS)
def test_live_self_check_places_nothing(tmp_path: Path, session: SimSession) -> None:
    sim = _account_like_the_real_one(session)
    assert sim.market_session == session
    account = sim.get_account()
    orders_before = sim.inspect_orders("all")
    held = [pos.symbol for pos in sim.get_all_positions()]
    positions_before = [sim.inspect_position(symbol) for symbol in held]
    sim.reset_call_log()

    report = _self_check(tmp_path, sim)

    # Nothing was placed, changed or cancelled, and nothing was written anywhere.
    assert sim.calls(kind=MUTATING) == [] and sim.mutating_call_count == 0
    assert sim.inspect_orders("all") == orders_before
    assert [sim.inspect_position(symbol) for symbol in held] == positions_before
    assert not (tmp_path / "live").exists()
    assert _dir_bytes(tmp_path / "manifests") == {}
    assert sorted(path.name for path in tmp_path.iterdir()) == ["manifests"]
    # It did read: the account, the positions and the open orders, plus the clock and assets.
    read_names = [call.name for call in sim.call_log]
    assert set(read_names) == {
        "get_account",
        "get_all_positions",
        "get_orders",
        "get_clock",
        "get_asset",
    }
    for required in ("get_account", "get_all_positions", "get_orders"):
        assert read_names.count(required) >= 1
    # The report is plain JSON without floats and without the account's id, number or money.
    dumped = json.dumps(report)
    assert _contains_no_float(report)
    assert str(account.id) not in dumped and str(account.account_number) not in dumped
    assert "equity" not in dumped and "buying_power" not in dumped
    assert report["schema_version"] == runner.LIVE_SELF_CHECK_SCHEMA == "tier0.live-self-check.v1"
    assert report["mode"] == "READ_ONLY" and report["environment"] == "LIVE"
    assert (
        report["account_alias"] == _LIVE_ACCOUNT and report["venue_profile"] == LIVE_TIERED_VENUE_ID
    )
    assert report["paper_only"] is False and report["live_execution_enabled"] is False
    # The simulator's clock is not the host's: sessions follow the broker clock, as supervised.
    assert report["session"] == session
    assert report["clock_skew"]["status"] == "SKEWED"
    assert report["clock_skew"]["session_clock"] == "BROKER"
    assert report["account"] == {
        "status": "ACTIVE",
        "trading_blocked": False,
        "account_blocked": False,
        "trade_suspended_by_user": False,
        "entry_gates": [],
    }
    assert report["positions_count"] == 6 and report["open_orders_count"] == 5
    assert report["ledger_readable"] is True and report["persisted_open_lifecycles"] == []

    rows = {row["symbol"]: row for row in report["positions"]}
    assert list(rows) == ["ANT", "ARM", "MU", "NVDA", "PLTR", "SOXL"]
    # ANT: 9 held, 8 under a trailing sell the lane did not place: one share to cover, with the
    # owner's resting buy on the other side named.
    ant = rows["ANT"]
    assert (ant["whole_shares"], ant["natively_protected_shares"]) == (9, 8)
    assert ant["lane_stop_target_shares"] == 1 and ant["opposite_side_working_orders"] == 1
    assert ant["action"] == "ATTACH_STOP" and ant["would_attach"] == {
        "order_type": "stop",
        "side": "sell",
        "time_in_force": "gtc",
        "qty": 1,
        "stop_price": ant["stop_price"],
    }
    assert Decimal(ant["stop_price"]) < Decimal(ant["current_price"]) == Decimal("40")
    # SOXL: fully covered by its own trailing sell, nothing to attach.
    soxl = rows["SOXL"]
    assert (soxl["natively_protected_shares"], soxl["action"]) == (82, "NONE_COVERED")
    assert soxl["would_attach"] is None and soxl["lane_stop_target_shares"] == 0
    # NVDA: in profit, uncovered: the whole position, at a stop behind the price.
    nvda = rows["NVDA"]
    assert nvda["action"] == "ATTACH_STOP" and nvda["would_attach"]["qty"] == 10
    assert nvda["first_adoption"]["under_water"] is False
    assert nvda["first_adoption"]["adoption_anchor_price"] is None
    assert Decimal(nvda["first_adoption"]["reference_price"]) == Decimal("100")
    assert (
        Decimal(nvda["first_adoption"]["initial_stop_price"])
        <= Decimal(nvda["stop_price"])
        < Decimal("103")
    )
    # MU: first seen under water: anchored at the price it is seen at, never at the fill.
    mu = rows["MU"]
    assert mu["first_adoption"]["under_water"] is True and mu["would_attach"]["qty"] == 2
    assert Decimal(mu["first_adoption"]["adoption_anchor_price"]) == Decimal("95")
    assert Decimal(mu["first_adoption"]["reference_price"]) == Decimal("95")
    assert Decimal("93") <= Decimal(mu["stop_price"]) < Decimal("95")
    # PLTR: half a share cannot carry a stop; it is reported, not stopped.
    pltr = rows["PLTR"]
    assert (pltr["eligible"], pltr["ineligible_reason"]) == (False, "FRACTIONAL_ONLY")
    assert pltr["action"] == "NONE_INELIGIBLE" and pltr["would_attach"] is None
    assert (pltr["whole_shares"], pltr["fractional_remainder"]) == (0, "0.5")
    assert [row["stop_basis"] for row in rows.values()] == ["FIRST_ADOPTION"] * 6
    assert [(s["symbol"], s["qty"]) for s in report["stops_to_attach"]] == [
        ("ANT", 1),
        ("MU", 2),
        ("NVDA", 10),
    ]
    assert report["orphan_lane_stops"] == []

    # ARM rests under the owner's stop at 98. Outside the regular session it trades at 97: the
    # stop is already through the price and fires at the next regular open, whether the lane
    # runs or not. It is the one position marked, and it is noted; it is not something a start
    # of the lane does, so it blocks no start.
    arm = rows["ARM"]
    assert (arm["natively_protected_shares"], arm["action"]) == (22, "NONE_COVERED")
    expected_notes = ["HOST_CLOCK_SKEWED_ENTRIES_BLOCKED", "INELIGIBLE:PLTR:FRACTIONAL_ONLY"]
    if session == "RTH":
        assert arm["would_exit_immediately"] is False and arm["exit_reasons"] == []
        assert report["would_exit_immediately"] == []
    else:
        assert arm["would_exit_immediately"] is True
        assert arm["exit_reasons"] == ["RESTING_STOP_AT_OR_THROUGH_PRICE"]
        assert report["would_exit_immediately"] == ["ARM"]
        expected_notes.insert(0, "BROKER_STOP_AT_OR_THROUGH_PRICE:ARM")
    assert report["notes"] == expected_notes and report["start_blockers"] == []
    assert not any(row["would_exit_on_start"] for row in rows.values())
    assert not any(row["would_exit_immediately"] for sym, row in rows.items() if sym != "ARM")

    # The coverage record of the same reads, with no guard running: what the broker protects.
    coverage = report["coverage"]
    assert coverage["schema_version"] == "tier0.lossless-coverage.v1"
    assert coverage["environment"] == "LIVE" and coverage["session"] == session
    assert coverage["position_shares_total"] == 9 + 82 + 10 + 2 + 22
    assert coverage["natively_protected_shares"] == 8 + 82 + 22
    assert coverage["synthetically_supervised_shares"] == 0
    assert coverage["unprotected_shares_count"] == 1 + 10 + 2
    assert coverage["all_shares_protected"] is False
    assert {row["symbol"]: row["mode"] for row in coverage["share_protection_table"]} == {
        "ANT": "NATIVE_PARTIAL",
        "ARM": "NATIVE_BROKER_STOP",
        "MU": "UNPROTECTED:NO_GUARD",
        "NVDA": "UNPROTECTED:NO_GUARD",
        "PLTR": "UNPROTECTED:FRACTIONAL_ONLY",
        "SOXL": "NATIVE_BROKER_STOP",
    }


@pytest.mark.parametrize("session", SIM_SESSIONS)
def test_live_self_check_stops_equal_what_the_supervisor_attaches(
    tmp_path: Path, session: SimSession
) -> None:
    # The self-check states the order the lane would attach; the supervisor, started on an
    # identical account, attaches it. Both must agree order for order.
    report = _self_check(tmp_path / "check", _mixed_positions(session))
    stated = {
        stop["symbol"]: (
            OrderType(stop["order_type"]),
            OrderSide(stop["side"]),
            TimeInForce(stop["time_in_force"]),
            stop["qty"],
            Decimal(stop["stop_price"]),
        )
        for stop in report["stops_to_attach"]
    }
    assert set(stated) == {"NVDA", "AMD", "MU", "ARM", "ANT", "TSLA", "QQQ"}
    assert stated["ANT"][3] == 1 and stated["AMD"][3] == 4 and stated["NVDA"][3] == 10
    rows = {row["symbol"]: row for row in report["positions"]}
    assert rows["AMD"]["qty_available"] == "4" and rows["AMD"]["lane_stop_target_shares"] == 7
    # NVDA is far enough in profit for the ratchet: its stop starts above the plain initial one.
    assert Decimal(rows["NVDA"]["stop_price"]) > Decimal(
        rows["NVDA"]["first_adoption"]["initial_stop_price"]
    )
    assert stated["TSLA"][1] is OrderSide.BUY and stated["MU"][1] is OrderSide.SELL
    assert {stop[0] for stop in stated.values()} == {OrderType.STOP}
    assert {stop[2] for stop in stated.values()} == {TimeInForce.GTC}
    assert report["would_exit_immediately"] == [] and report["start_blockers"] == []

    sim = _mixed_positions(session)
    for symbol in stated:
        sim.set_asset(symbol, attributes=["overnight_tradable"])
    sim.reset_call_log()
    run = _supervise_live(tmp_path / "run", sim, session=session)
    assert run.status["managed_symbols"] == sorted(stated)
    attached = {
        order.symbol: (
            order.type,
            order.side,
            order.time_in_force,
            int(Decimal(str(order.qty))),
            Decimal(str(order.stop_price)),
        )
        for order in sim.inspect_orders("open")
        if str(order.client_order_id).startswith("stop-")
    }
    assert attached == stated
    assert [call.outcome for call in sim.calls("submit_order")] == ["ok"] * len(stated)
    assert len(sim.calls(kind=MUTATING)) == len(stated)


def test_live_self_check_reads_the_ledger_read_only_and_marks_persisted_exits(
    tmp_path: Path,
) -> None:
    session = "AFTER_HOURS"
    sim = _live_sim(session=session)
    for symbol, qty, entry, price in (
        ("NVDA", 10, "100.00", "100.00"),
        ("AMD", 5, "50.00", "50.00"),
        ("MU", 2, "100.00", "100.00"),
        ("TSLA", -5, "200.00", "200.00"),
        ("SOXL", 4, "30.00", "30.00"),
    ):
        sim.open_position(symbol, qty, entry)
        sim.set_price(symbol, price)
    sim.reset_call_log()
    live_dir = tmp_path / "live" / "manifests"
    live_dir.mkdir(parents=True)
    ledger = live_dir / pf._duckdb_filename(_LIVE_ACCOUNT)
    with duckdb.connect(str(ledger)) as db:
        pf._ensure_entry_decisions_table(db)
        for cid, symbol, side, stop, triggered, closed in (
            ("adopted-nvda", "NVDA", "LONG", "99.37", False, False),  # stop behind the price
            ("adopted-amd", "AMD", "LONG", "50.00", False, False),  # stop exactly at the price
            ("adopted-mu", "MU", "LONG", "90.00", True, False),  # exit already triggered
            ("adopted-tsla", "TSLA", "LONG", "150.00", False, False),  # broker holds a short
            ("adopted-soxl", "SOXL", "LONG", "99.00", False, True),  # closed: not a lifecycle
        ):
            db.execute(
                "INSERT INTO paper_entry_decisions (client_order_id, symbol, side, "
                "decision_price, arrival_price, session, origin, annual_rate, initial_stop, "
                "natr_pct, active_stop, exit_triggered, closed_at) VALUES "
                "(?, ?, ?, 100, 100, 'RTH', 'ADOPTED_EXTERNAL_POSITION', 0, 1, 0.015, ?, ?, "
                "CASE WHEN ? THEN now() ELSE NULL END)",
                [cid, symbol, side, stop, triggered, closed],
            )
    before = _dir_bytes(tmp_path)

    report = _self_check(tmp_path, sim)

    assert sim.mutating_call_count == 0 and _dir_bytes(tmp_path) == before
    assert report["ledger_readable"] is True and report["session"] == session
    assert report["persisted_open_lifecycles"] == ["AMD", "MU", "NVDA", "TSLA"]
    rows = {row["symbol"]: row for row in report["positions"]}
    # NVDA: the lane restores its lifecycle and attaches at the persisted stop, not a new one.
    nvda = rows["NVDA"]
    persisted_stop = Decimal("99.37")
    assert nvda["stop_basis"] == "PERSISTED_LIFECYCLE"
    assert Decimal(nvda["stop_price"]) == persisted_stop
    assert Decimal(nvda["would_attach"]["stop_price"]) == persisted_stop
    assert nvda["would_attach"]["qty"] == 10
    assert Decimal(nvda["first_adoption"]["stop_price"]) != persisted_stop
    assert nvda["would_exit_immediately"] is False
    # AMD and MU would exit on the first print after a start: the restored AMD stop is at the
    # price, and MU's exit was already triggered (its exit state machine owns the shares).
    assert rows["AMD"]["exit_reasons"] == ["PERSISTED_STOP_AT_OR_THROUGH_PRICE"]
    assert Decimal(rows["AMD"]["would_attach"]["stop_price"]) == Decimal("50")
    assert rows["MU"]["exit_reasons"] == ["PERSISTED_EXIT_TRIGGERED"]
    assert rows["MU"]["action"] == "NONE_EXIT_TRIGGERED" and rows["MU"]["would_attach"] is None
    assert [sym for sym, row in rows.items() if row["would_exit_on_start"]] == ["AMD", "MU"]
    # TSLA: the ledger says long, the broker holds a short: that lifecycle is not reused.
    tsla = rows["TSLA"]
    assert tsla["stop_basis"] == "FIRST_ADOPTION" and tsla["would_attach"]["side"] == "buy"
    assert Decimal(tsla["stop_price"]) > 200 and tsla["would_exit_immediately"] is False
    # SOXL: its ledger row is closed, so it is a first adoption.
    assert rows["SOXL"]["stop_basis"] == "FIRST_ADOPTION"
    assert rows["SOXL"]["would_exit_immediately"] is False
    assert report["would_exit_immediately"] == ["AMD", "MU"]
    assert report["start_blockers"] == ["WOULD_EXIT_ON_START:AMD", "WOULD_EXIT_ON_START:MU"]
    assert "PERSISTED_LIFECYCLE_NOT_REUSED:TSLA" in report["notes"]

    # A ledger that cannot be read is a blocker, never a silent "no lifecycles".
    ledger.write_bytes(b"not a duckdb database")
    before = _dir_bytes(tmp_path)
    unreadable = _self_check(tmp_path, sim)
    assert unreadable["ledger_readable"] is False
    assert "LEDGER_UNREADABLE" in unreadable["start_blockers"]
    assert sim.mutating_call_count == 0 and _dir_bytes(tmp_path) == before


def test_live_self_check_fails_closed_before_and_during_the_reads(tmp_path: Path) -> None:
    sim = _account_like_the_real_one("RTH")
    shared_dir = tmp_path / "manifests"
    shared_dir.mkdir()
    live_dir = tmp_path / "live" / "manifests"

    def must_not_build(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("the coordinator was built before the environment was verified")

    # The shipped environment check runs first: without the live profile nothing is built.
    with (
        mock.patch.object(runner, "build_live_venue_coordinator", must_not_build),
        mock.patch.dict(runner.os.environ, {"TIER0_VENUE_PROFILE": "ALPACA_PAPER_200"}),
        pytest.raises(PermissionError, match="TIER0_VENUE_PROFILE"),
    ):
        runner.run_live_self_check(live_dir, shared_dir=shared_dir)
    # The live directory may not be the shared one, nor inside it.
    for inside in (shared_dir, shared_dir / "live"):
        with (
            mock.patch.object(runner, "verify_live_environment", lambda: False),
            mock.patch.object(runner, "build_live_venue_coordinator", must_not_build),
            pytest.raises(PermissionError, match="kept separate"),
        ):
            runner.run_live_self_check(inside, shared_dir=shared_dir)
    # A paper coordinator is refused.
    paper_farm, paper_sims = _paper_farm(tmp_path / "paper")
    with (
        mock.patch.object(runner, "verify_live_environment", lambda: False),
        mock.patch.object(runner, "build_live_venue_coordinator", lambda *_a, **_k: paper_farm),
        pytest.raises(PermissionError, match="not the live venue's coordinator"),
    ):
        runner.run_live_self_check(live_dir, shared_dir=shared_dir)
    assert all(sim.read_call_count == 0 for sim in paper_sims.values())
    # An unreadable order book ends the check loudly instead of reporting "no cover".
    sim.fail_next("get_orders", 500)
    with pytest.raises(RuntimeError, match="could not read the open orders"):
        _self_check(tmp_path, sim)
    assert sim.mutating_call_count == 0 and not (tmp_path / "live").exists()


def test_live_self_check_reports_account_gates_and_lane_stops(tmp_path: Path) -> None:
    # A restricted account gates entries only; the self-check says so and blocks no start.
    sim = _live_sim(session="RTH")
    sim.open_position("NVDA", 10, "100.00")
    sim.open_position("AMD", 5, "50.00")
    # Stops this harness placed in an earlier run: one for a held symbol, and a buy stop left
    # from a short that is gone (the broker takes it without a position).
    sim.submit_order(
        StopOrderRequest(
            symbol="NVDA",
            qty=6,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            stop_price=98.0,
            client_order_id="stop-adopted-alpaca_live1-nvda-aaaa-0001",
        )
    )
    sim.submit_order(
        StopOrderRequest(
            symbol="GONE",
            qty=3,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC,
            stop_price=60.0,
            client_order_id="stop-adopted-alpaca_live1-gone-bbbb-0001",
        )
    )
    # MU: every share is reserved by the owner's own resting limit sell, so no stop fits.
    sim.open_position("MU", 5, "100.00")
    sim.submit_order(
        LimitOrderRequest(
            symbol="MU",
            qty=5,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=150.0,
        )
    )
    sim.set_account(trading_blocked=True, last_equity="0")
    sim.reset_call_log()
    report = _self_check(tmp_path, sim, opt_in=True)
    assert sim.mutating_call_count == 0 and not (tmp_path / "live").exists()
    assert report["live_execution_enabled"] is True
    assert report["account"]["trading_blocked"] is True
    assert report["account"]["entry_gates"] == [_RESTRICTED, _EQUITY]
    assert report["start_blockers"] == []
    assert {"ENTRY_GATE:ACCOUNT_RESTRICTED", "ENTRY_GATE:EQUITY_UNVERIFIED"} <= set(report["notes"])
    rows = {row["symbol"]: row for row in report["positions"]}
    # NVDA: the lane's own stop covers 6 of 10; the reconciler sizes it, nothing new is attached.
    nvda = rows["NVDA"]
    assert nvda["action"] == "LANE_STOP_PRESENT" and nvda["would_attach"] is None
    assert (nvda["lane_stop_shares"], nvda["lane_stop_target_shares"]) == (6, 10)
    assert nvda["natively_protected_shares"] == 6
    assert rows["AMD"]["action"] == "ATTACH_STOP" and rows["AMD"]["would_attach"]["qty"] == 5
    mu = rows["MU"]
    assert mu["action"] == "NONE_NO_QTY_AVAILABLE" and mu["would_attach"] is None
    assert (mu["qty_available"], mu["lane_stop_target_shares"]) == ("0", 5)
    assert "UNCOVERED_NO_QTY_AVAILABLE:MU" in report["notes"]
    assert [stop["symbol"] for stop in report["stops_to_attach"]] == ["AMD"]
    # The lane's stop for a symbol that is no longer held would be released at a start.
    assert report["orphan_lane_stops"] == ["GONE"]
    assert "ORPHAN_LANE_STOP_WOULD_BE_RELEASED:GONE" in report["notes"]

    # An order read that came back full attests nothing about cover: a blocker, and an entry
    # gate. A position the broker states without a price cannot be guarded: a blocker too.
    crowded = _live_sim(session="RTH")
    crowded.open_position("NVDA", 10, "100.00")
    _fill_order_book(crowded)
    odd = SimpleNamespace(
        symbol="ODD", qty="5", side="long", avg_entry_price=None, current_price=None
    )
    real_positions = crowded.get_all_positions()
    crowded.reset_call_log()
    with mock.patch.object(crowded, "get_all_positions", lambda: [*real_positions, odd]):
        report = _self_check(tmp_path / "crowded", crowded)
    assert crowded.mutating_call_count == 0
    assert report["open_orders_count"] == runner.OPEN_ORDER_READ_LIMIT
    assert report["account"]["entry_gates"] == [_BOOK_FULL]
    assert report["start_blockers"] == ["OPEN_ORDER_READ_INCOMPLETE", "UNPARSEABLE_POSITION:ODD"]
    rows = {row["symbol"]: row for row in report["positions"]}
    assert rows["ODD"] == {"symbol": "ODD", "eligible": False, "action": "NONE_UNPARSEABLE"}
    assert rows["NVDA"]["action"] == "ATTACH_STOP" and report["would_exit_immediately"] == []


# ------------------------------------------------------------------------------ main()


def test_live_modes_route_directories_and_reject_paper_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[tuple[str, Any]] = []
    shared_dir = tmp_path / "sandbox" / "manifests"
    live_dir = tmp_path / "sandbox" / "live" / "manifests"

    def verify_live() -> bool:
        calls.append(("verify_live", None))
        return False

    def run_execution(manifest_dir: Path, **kwargs: Any) -> None:
        calls.append(("run_execution", (manifest_dir, kwargs)))

    monkeypatch.setattr(runner, "verify_live_environment", verify_live)
    monkeypatch.setattr(runner, "verify_paper_environment", lambda: calls.append(("paper", None)))
    monkeypatch.setattr(execution, "run_execution", run_execution)

    def run_main(*argv: str) -> None:
        monkeypatch.setattr(sys, "argv", ["runner", *argv])
        runner.main()

    # The live directory is <data root>/live/manifests beside the paper one.
    assert runner.live_manifest_dir_for(shared_dir) == live_dir
    assert runner.live_manifest_dir_for(pf.default_manifest_dir()) == pf.default_live_manifest_dir()
    assert pf.default_live_manifest_dir() == tmp_path / "data-root" / "live" / "manifests"

    # Paper options are refused with a live mode, before anything is verified, started or set.
    for mode in ("--supervise-live", "--live-self-check"):
        for option in (("--account", "ALPACA_PA1"), ("--alpaca-rpm", "1000")):
            with pytest.raises(PermissionError, match="with a live mode"):
                run_main(mode, "--manifest-dir", str(shared_dir), *option)
    assert calls == [] and "TIER0_ALPACA_RPM" not in runner.os.environ
    assert not (tmp_path / "sandbox").exists()

    # --supervise-live: the environment check, then the supervisor on the tiered venue with the
    # paper directory as the shared one. Its return means the streams ended: a failure exit.
    with pytest.raises(SystemExit) as ended:
        run_main("--supervise-live", "--manifest-dir", str(shared_dir))
    assert ended.value.code not in (None, 0)
    assert calls == [
        ("verify_live", None),
        (
            "run_execution",
            (live_dir, {"venue_profile": LIVE_TIERED_VENUE_ID, "shared_dir": shared_dir}),
        ),
    ]
    calls.clear()
    with pytest.raises(SystemExit):
        run_main("--supervise-live")
    assert calls[1] == (
        "run_execution",
        (
            pf.default_live_manifest_dir(),
            {"venue_profile": LIVE_TIERED_VENUE_ID, "shared_dir": pf.default_manifest_dir()},
        ),
    )
    # A failed environment check starts nothing.
    calls.clear()

    def refuse_live() -> bool:
        raise PermissionError("Live profile check failed: scripted")

    monkeypatch.setattr(runner, "verify_live_environment", refuse_live)
    with pytest.raises(PermissionError, match="scripted"):
        run_main("--supervise-live", "--manifest-dir", str(shared_dir))
    assert calls == []
    monkeypatch.setattr(runner, "verify_live_environment", verify_live)

    # The paper supervisor mode is what it was: its own directory, no venue, a normal return.
    run_main("--confirm-paper", "--manifest-dir", str(shared_dir))
    assert calls == [("run_execution", (shared_dir, {}))]
    calls.clear()
    capsys.readouterr()

    # --live-self-check prints the report and places nothing; a blocker is a failure exit.
    def self_check_exit(sim: FaultInjectingAlpacaSim) -> tuple[int, dict[str, Any]]:
        def build_coordinator(
            manifest_dir: Path, *, autonomous_opt_in: bool
        ) -> pf.AlpacaLiveVenueCoordinator:
            calls.append(("build", manifest_dir))
            return pf.AlpacaLiveVenueCoordinator(
                manifest_dir, trading_client=sim, autonomous_opt_in=autonomous_opt_in
            )

        monkeypatch.setattr(runner, "build_live_venue_coordinator", build_coordinator)
        shared_dir.mkdir(parents=True, exist_ok=True)
        with pytest.raises(SystemExit) as done:
            run_main("--live-self-check", "--manifest-dir", str(shared_dir))
        assert sim.mutating_call_count == 0 and not live_dir.exists()
        assert isinstance(done.value.code, int)
        return done.value.code, json.loads(capsys.readouterr().out)

    code, printed = self_check_exit(_account_like_the_real_one("RTH"))
    assert code == 0 and printed["start_blockers"] == []
    assert printed["schema_version"] == runner.LIVE_SELF_CHECK_SCHEMA
    assert [stop["symbol"] for stop in printed["stops_to_attach"]] == ["ANT", "MU", "NVDA"]
    assert calls == [("verify_live", None), ("build", live_dir)]
    calls.clear()
    # A broker stop that is already through the price is reported and fails nothing ...
    code, printed = self_check_exit(_account_like_the_real_one("OVERNIGHT"))
    assert code == 0 and printed["would_exit_immediately"] == ["ARM"]
    assert printed["start_blockers"] == []
    # ... a blocker does: here an order read that came back full.
    crowded = _live_sim(session="RTH")
    crowded.open_position("NVDA", 10, "100.00")
    _fill_order_book(crowded)
    crowded.reset_call_log()
    code, printed = self_check_exit(crowded)
    assert code == 1 and printed["start_blockers"] == ["OPEN_ORDER_READ_INCOMPLETE"]
    assert _dir_bytes(shared_dir) == {}

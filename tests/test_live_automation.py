"""Unit & integration tests for tier0_sota.live (mocked broker/network calls)."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import duckdb
import polars as pl
import pytest
from alpaca.common.exceptions import APIError
from alpaca.trading.enums import OrderClass
from alpaca.trading.requests import LimitOrderRequest
from hypothesis import given, settings
from hypothesis import strategies as st

from tier0_sota.live import (
    ALPACA_LANE_PROFILES,
    DEFAULT_PIN_SYMBOLS,
    PAPER_ALPACA_ENDPOINT,
    AlpacaPaperLane,
    CrossLaneSymbolLock,
    ExtremeMoverScanner,
    IbkrPaperAdapter,
    PaperFarmCoordinator,
    aggregate_finra_trf_ratios,
    aggregate_paper_farm_performance,
    execution,
    select_short_or_inverse_fallback,
)
from tier0_sota.live.execution import Guard
from tier0_sota.live.paper_farm import (
    IBKR_PAPER_MAX_RPM,
    borrow_accrual_days,
    compute_short_borrow_accrual,
    compute_tca_record,
    join_fills_with_quotes_asof,
    round_lot_up,
    select_situation_lane,
)
from tier0_sota.live.performance import _read_duckdb_counts, record_paper_farm_telemetry
from tier0_sota.live.runner import (
    _resolve_pin_symbols,
    exit_context,
    reconcile_fills,
)
from tier0_sota.live.runner import (
    build_live_lane_preset_bundle as runner_build_bundle,
)
from tier0_sota.live.scanner import (
    INVERSE_ETF_ANNUAL_EXPENSE_RATES,
    OVERNIGHT_SEED_SYMBOLS,
    build_live_lane_preset_bundle,
)
from tier0_sota.risk import MAX_STOP_LOSS_PCT


def _make_bar(open_px: float, close_px: float, vol: float) -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=datetime(2026, 10, 15, 14, 59, tzinfo=UTC),
        open=open_px,
        high=max(open_px, close_px),
        low=min(open_px, close_px),
        close=close_px,
        volume=vol,
    )


class _FakeScreener:
    def get_most_actives(self, _req: object) -> SimpleNamespace:
        return SimpleNamespace(
            most_actives=[
                SimpleNamespace(symbol="AAPL", price=225.0, volume=80_000_000, percent_change=0.4),
                SimpleNamespace(symbol="SOXL", price=32.0, volume=40_000_000, percent_change=8.5),
            ]
        )

    def get_market_movers(self, _req: object) -> SimpleNamespace:
        return SimpleNamespace(
            gainers=[
                SimpleNamespace(symbol="SMCI", price=42.0, volume=25_000_000, percent_change=11.2)
            ],
            losers=[
                SimpleNamespace(symbol="SOXS", price=24.0, volume=30_000_000, percent_change=-8.4)
            ],
        )


class _FakeDataClient:
    def get_stock_bars(self, _req: object) -> SimpleNamespace:
        base = [_make_bar(10.0, 10.0, 100.0) for _ in range(10)]
        return SimpleNamespace(
            data={
                "SPY": [_make_bar(500.0, 502.0, 1000.0), _make_bar(502.0, 503.0, 1000.0)],
                "AAPL": [*base, _make_bar(225.0, 225.5, 110.0)],
                "SOXL": [*base, _make_bar(29.5, 32.0, 600.0)],
                "SMCI": [*base, _make_bar(37.8, 42.0, 500.0)],
                "SOXS": [*base, _make_bar(26.2, 24.0, 450.0)],
            }
        )

    def get_stock_trades(self, _req: object) -> SimpleNamespace:
        return SimpleNamespace(
            data={
                "SOXL": [
                    SimpleNamespace(symbol="SOXL", size=400, exchange="D"),
                    SimpleNamespace(symbol="SOXL", size=600, exchange="Q"),
                ],
                "SMCI": [SimpleNamespace(symbol="SMCI", size=300, exchange="D")],
            }
        )


class _FakeTradingClient:
    def __init__(self, *, shortable: bool = True, etb: bool = True) -> None:
        self.shortable = shortable
        self.etb = etb
        self.orders: list[object] = []
        self.confirmed: dict[str, SimpleNamespace] = {}

    def get_order_by_id(self, order_id: str) -> SimpleNamespace:
        return self.confirmed.get(order_id, SimpleNamespace(filled_qty="0", status="accepted"))

    def get_open_position(self, symbol: str) -> SimpleNamespace:
        return next(row for row in self.confirmed.values() if row.symbol == symbol)

    def get_asset(self, symbol: str) -> SimpleNamespace:
        return SimpleNamespace(symbol=symbol, shortable=self.shortable, easy_to_borrow=self.etb)

    def get_all_assets(self, _req: object) -> list[SimpleNamespace]:
        return [self.get_asset("SOXL"), self.get_asset("SOXS"), self.get_asset("SMCI")]

    def submit_order(self, req: object) -> SimpleNamespace:
        oid = f"ord-{len(self.orders) + 1}"
        self.orders.append(req)
        return SimpleNamespace(id=oid, status="accepted")


def test_scanner_filters_flat_megacaps_and_enriches_trf_and_borrow(tmp_path: Path) -> None:
    assert DEFAULT_PIN_SYMBOLS == ("SOXL", "SOXS", "TQQQ", "SQQQ")
    scanner = ExtremeMoverScanner(
        screener_client=_FakeScreener(),
        data_client=_FakeDataClient(),
        trading_client=_FakeTradingClient(shortable=True, etb=True),
    )
    out = tmp_path / "movers.json"
    doc = scanner.scan(top_k=10, output_path=out, now_utc=datetime(2026, 10, 15, 15, 0, tzinfo=UTC))
    syms = [e["symbol"] for e in doc["entries"]]
    assert "AAPL" not in syms
    assert "SOXL" in syms
    assert "SMCI" in syms
    assert "SOXS" in syms
    soxl = next(e for e in doc["entries"] if e["symbol"] == "SOXL")
    assert soxl["darkpool_trf_ratio"] == 0.4
    assert soxl["borrow_source"] == "alpaca_paper_asset"
    assert soxl["shortable"] is True
    assert out.is_file()


def test_finra_trf_ratio_aggregation() -> None:
    agg = aggregate_finra_trf_ratios(
        [
            SimpleNamespace(symbol="NVDA", size=250, exchange="D"),
            SimpleNamespace(symbol="NVDA", size=750, exchange="V"),
        ]
    )
    assert agg["NVDA"]["darkpool_trf_ratio"] == 0.25


def test_live_activity_replay_and_split_borrow_accrual(tmp_path: Path) -> None:
    class ReplayClient(_FakeTradingClient):
        def get(self, path: str, params: dict[str, str]) -> list[dict[str, str]]:
            assert path == "/account/activities/FILL"
            return [
                {
                    "id": f"execution-{i}",
                    "order_id": "ord-1",
                    "qty": "30",
                    "cum_qty": str(i * 30),
                    "side": "buy",
                    "symbol": "SOXS",
                    "transaction_time": "2026-11-20T15:00:00Z",
                    "price": "25.02",
                }
                for i in (1, 2)
            ]

    client = ReplayClient()
    lane = AlpacaPaperLane("ALPACA_PA2", tmp_path, client, CrossLaneSymbolLock())
    submitted = lane.submit_guarded_entry(
        symbol="SOXS",
        side="LONG",
        ref_price=Decimal("25.00"),
        arrival_price=Decimal("25.01"),
        origin="INVERSE_HEDGE_SOXS",
        annual_borrow_rate=Decimal("0.12"),
        prior_close=Decimal("24"),
        client_order_id="scan-1",
    )
    assert submitted
    client.confirmed["ord-1"] = SimpleNamespace(
        client_order_id="scan-1", side=SimpleNamespace(value="buy")
    )
    assert reconcile_fills(lane) == 2
    # A restart can replay a committed page before its cursor has been checkpointed.
    with duckdb.connect(str(lane.duckdb_path)) as conn:
        conn.execute("DELETE FROM paper_activity_cursor")
    assert reconcile_fills(lane) == 0
    with duckdb.connect(str(lane.duckdb_path), read_only=True) as conn:
        assert conn.execute(
            "SELECT count(*),sum(filled_qty),sum(borrow_fee_usd),"
            "avg(implementation_shortfall_bps) FROM paper_tca_fills"
        ).fetchone() == (2, Decimal("60"), Decimal("2.4"), 8.0)


def test_worker_admits_one_paper_oto_from_fresh_sip_trades(tmp_path: Path, monkeypatch) -> None:
    stop = threading.Event()

    class Broker(_FakeTradingClient):
        def __init__(self):
            super().__init__()
            self.identity = uuid4()

        def get_account(self):
            return SimpleNamespace(
                id=self.identity,
                status=SimpleNamespace(value="ACTIVE"),
                account_blocked=False,
                trading_blocked=False,
                trade_suspended_by_user=False,
                equity="100000",
                last_equity="100000",
            )

        def get_all_positions(self):
            return []

        def get_orders(self, request):
            return []

        def get_clock(self):
            return SimpleNamespace(is_open=True)

        def get_asset(self, symbol):
            return SimpleNamespace(tradable=True, attributes=[])

        def get_order_by_id(self, order_id, *args):
            request = self.orders[0]
            assert isinstance(request, LimitOrderRequest) and request.stop_loss is not None
            child = SimpleNamespace(
                id="child",
                status=SimpleNamespace(value="held"),
                replaced_by=None,
                symbol=request.symbol,
                side=SimpleNamespace(value="sell"),
                stop_price=str(request.stop_loss.stop_price),
            )
            return SimpleNamespace(
                id=order_id,
                legs=[child],
                status=SimpleNamespace(value="new"),
                filled_qty="0",
                filled_avg_price=None,
            )

    brokers = {acct: Broker() for acct in ALPACA_LANE_PROFILES}
    farm = PaperFarmCoordinator(tmp_path, alpaca_clients=brokers)

    class Quotes:
        def get_stock_latest_quote(self, request):
            return {
                "NVDA": SimpleNamespace(
                    bid_price=100, ask_price=100.01, timestamp=datetime.now(UTC)
                )
            }

    class Stream:
        def __init__(self, *args, feed):
            self.feed = feed
            self.subscribed = threading.Event()

        def subscribe_trades(self, callback, *symbols):
            self.callback = callback
            if "NVDA" in symbols:
                self.subscribed.set()

        def run(self):
            assert self.subscribed.wait(15)
            if self.feed.value == "sip":
                for _ in range(2):
                    asyncio.run(
                        self.callback(
                            SimpleNamespace(symbol="NVDA", price=100, timestamp=datetime.now(UTC))
                        )
                    )
                stop.set()
            assert stop.wait(15)

        def stop(self):
            stop.set()

    now = datetime.now(UTC).isoformat()
    (tmp_path / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now,
                "macro_regime": {"regime": 1},
                "entries": [
                    {
                        "symbol": "NVDA",
                        "extreme_mover_eligible": True,
                        "side": "BUY",
                        "session": "RTH",
                        "bar_close_utc": now,
                        "natr_pct": 0.012,
                        "last_price": 100,
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "resolve_alpaca_env_creds", lambda: ("", ""))
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _: farm)
    monkeypatch.setattr(execution, "StockDataStream", Stream)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner, "from_env", lambda: SimpleNamespace(data_client=Quotes())
    )
    monkeypatch.setattr(execution.engine, "classify_us_equity_session_et", lambda _: "RTH")
    execution.run_execution(tmp_path, stop=stop)
    submitted = [o for broker in brokers.values() for o in broker.orders]
    assert len(submitted) == 1
    entry = submitted[0]
    assert isinstance(entry, LimitOrderRequest) and entry.order_class == OrderClass.OTO
    assert entry.stop_loss is not None and entry.limit_price is not None
    assert entry.stop_loss.stop_price < entry.limit_price
    with duckdb.connect(str(farm.lanes["ALPACA_PA3"].duckdb_path)) as db:
        assert db.execute("SELECT count(*) FROM paper_entry_decisions").fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM paper_tca_fills").fetchone() == (0,)


def test_adverse_published_farm_calibration_holds_new_risk(tmp_path: Path) -> None:
    receipt = {
        "schema_version": "tier0.tca-calibration-manifest.v1",
        "paper_only": True,
        "live_execution_enabled": False,
        "duckdb_errors": [],
        "parity_within_2x_envelope": False,
    }
    path = tmp_path / "tca_calibration_manifest.json"
    path.write_text(json.dumps(receipt))
    assert not execution.cost_calibration_allows_entry(tmp_path)
    path.write_text(json.dumps(receipt | {"parity_within_2x_envelope": True}))
    assert execution.cost_calibration_allows_entry(tmp_path)


def test_execution_worker_ratchets_confirmed_oto_and_restores_stop(tmp_path: Path) -> None:
    class Broker(_FakeTradingClient):
        def replace_order_by_id(self, oid, request):
            assert oid == "protective"
            child.stop_price = str(request.stop_price)
            return child

    broker = Broker()
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, broker, CrossLaneSymbolLock())
    order = lane.submit_guarded_entry(
        symbol="SPY", side="LONG", ref_price=Decimal("100"), client_order_id="parent"
    )
    assert order
    initial = Decimal(str(order["stop_price"]))
    child = SimpleNamespace(
        id="protective", stop_price=str(initial), status=SimpleNamespace(value="new")
    )
    parent = SimpleNamespace(
        status=SimpleNamespace(value="filled"), filled_qty="1", filled_avg_price="100"
    )
    guard = Guard(
        lane,
        "parent",
        "SPY",
        "LONG",
        initial,
        initial,
        Decimal("0.015"),
        "RTH",
        False,
        parent,
        child,
    )
    for price in ("101", "103", "105", "110"):
        previous = guard.stop
        guard.on_price(Decimal(price), "RTH")
        assert guard.stop >= previous
    assert guard.stop > Decimal("100")
    guard.on_price(guard.stop, "RTH")
    assert not guard.triggered and len(broker.orders) == 1
    with duckdb.connect(str(lane.duckdb_path)) as db:
        assert db.execute("SELECT active_stop FROM paper_entry_decisions").fetchone() == (
            guard.stop,
        )


def test_replaced_child_exit_uses_owned_stop_and_causal_arrival(tmp_path: Path) -> None:
    submitted = datetime(2026, 11, 25, 15, 0, tzinfo=UTC)
    side = SimpleNamespace(value="sell")
    child = SimpleNamespace(
        id="new",
        replaces="old",
        symbol="NVDA",
        stop_price="101",
        client_order_id="new-leg",
        submitted_at=submitted,
        side=side,
    )
    original = SimpleNamespace(id="old", replaces=None)
    parent = SimpleNamespace(id="parent-id", side=SimpleNamespace(value="buy"), legs=[original])

    class Broker(_FakeTradingClient):
        def get_order_by_client_id(self, cid):
            assert cid == "signal"
            return parent

        def get_order_by_id(self, order_id, *args):
            return original if order_id == "old" else parent

    class Quotes:
        def get_stock_quotes(self, request):
            assert request.limit == 1
            return SimpleNamespace(
                data={
                    "NVDA": [
                        SimpleNamespace(bid_price=101.10, ask_price=101.12, timestamp=submitted)
                    ]
                }
            )

    lane = AlpacaPaperLane("ALPACA_PA2", tmp_path, Broker(), CrossLaneSymbolLock())
    ctx = exit_context(lane, child, {"signal": ("NVDA",)}, Quotes())
    assert ctx and ctx[:6] == (
        "NVDA",
        "SHORT",
        Decimal("101"),
        Decimal("101.11"),
        "RTH",
        "PROTECTIVE_EXIT",
    )
    child.replaces = None
    assert exit_context(lane, child, {"signal": ("NVDA",)}, Quotes()) is None


def test_supervised_four_tiers_only_ratchet_on_confirmed_fill(tmp_path: Path) -> None:
    client = _FakeTradingClient()
    lane = AlpacaPaperLane("ALPACA_PA3", tmp_path, client, CrossLaneSymbolLock())
    order = lane.submit_guarded_entry(
        symbol="NVDA", side="LONG", ref_price=Decimal("100"), session="PRE_MARKET"
    )
    assert order
    oid = order["broker_order_id"]
    client.confirmed[oid] = SimpleNamespace(
        filled_qty=10,
        filled_avg_price="100",
        status="filled",
        symbol="NVDA",
        qty=10,
        qty_available=4,
    )
    previous = lane.supervised_stops[oid].stop_price
    for last in ("101", "103", "105", "108"):
        assert lane.trigger_supervised_stop(oid, trigger_px=Decimal(last)) is None
        stop = lane.supervised_stops[oid].stop_price
        assert previous < stop < Decimal(last)
        previous = stop
    assert lane.trigger_supervised_stop(oid, trigger_px=previous)
    assert getattr(client.orders[-1], "qty", None) == 4


def test_paper_farm_non_overlapping_lanes_and_fail_closed_gates(tmp_path: Path) -> None:
    assert PAPER_ALPACA_ENDPOINT == "https://paper-api.alpaca.markets"
    assert set(ALPACA_LANE_PROFILES.keys()) == {"ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3"}

    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)
    assert coord.ibkr.connection_spec["port"] == 4002

    lock = CrossLaneSymbolLock()
    assert lock.try_claim("SOXL", "ALPACA_PA1") is True
    assert lock.try_claim("SOXL", "ALPACA_PA2") is False
    lock.release("SOXL", "ALPACA_PA1")
    assert lock.try_claim("SOXL", "ALPACA_PA2") is True

    with pytest.raises(PermissionError):
        AlpacaPaperLane(
            "ALPACA_PA1", tmp_path, clients["ALPACA_PA1"], lock, live_execution_enabled=True
        )
    with pytest.raises(PermissionError):
        IbkrPaperAdapter(tmp_path, live_execution_enabled=True)

    etb = {"shortable": True, "easy_to_borrow": True, "borrow_source": "alpaca_paper_asset"}
    assert select_short_or_inverse_fallback(
        "SMCI", market_regime=-1, borrow_meta=etb, session="RTH"
    ) == ("SMCI", "SHORT", "DIRECT_SHORT_ETB")
    assert select_short_or_inverse_fallback(
        "SMCI", market_regime=-1, borrow_meta=None, session="RTH"
    ) == ("SOXS", "LONG", "INVERSE_HEDGE_SOXS")
    assert select_short_or_inverse_fallback(
        "SMCI", market_regime=-1, borrow_meta=None, session="RTH", sector_hint="QQQ"
    ) == ("SQQQ", "LONG", "INVERSE_HEDGE_SQQQ")

    routed = coord.dispatch_movers(
        [
            {"symbol": "SOXL", "last_price": 32.0, **etb},
            {"symbol": "SMCI", "last_price": 42.0, **etb},
            {"symbol": "SOXS", "last_price": 24.0, **etb},
        ],
        macro_regime=1,
        session="RTH",
    )
    assert len(routed) == 3
    assert len({r["account"] for r in routed}) == 3
    assert len({r["symbol"] for r in routed}) == 3

    for acct, lane in coord.lanes.items():
        lane.write_status(equity=101_000.0, session="RTH")
        assert lane.status_path.is_file()
        assert lane.duckdb_path.is_file()
        assert acct in ALPACA_LANE_PROFILES


def test_performance_aggregator_and_finos_openbb_export(tmp_path: Path) -> None:
    import json

    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)
    for lane in coord.lanes.values():
        lane.counters.update(
            {"fills": 4, "auto_shorts": 1, "reverses": 2, "reentries": 1, "fast_sells": 3}
        )
        lane.write_status(equity=102_500.0, session="RTH")
        with duckdb.connect(str(lane.duckdb_path)) as conn:
            conn.execute("CREATE TABLE paper_roundtrips (ts_utc VARCHAR, realized_pnl_usd DOUBLE)")
            conn.execute("INSERT INTO paper_roundtrips VALUES ('2026-10-15T15:00:00Z', 125.5)")
    coord.ibkr.write_status(gateway_ready=True, equity=250_000.0)

    manifest = aggregate_paper_farm_performance(tmp_path)
    out_file = tmp_path / "paper_farm_live_performance_manifest.json"
    finos_file = tmp_path / "finos_paper_farm_performance.json"
    assert out_file.is_file()
    assert finos_file.is_file()
    assert manifest["paper_only"] is True
    assert manifest["live_execution_enabled"] is False
    assert manifest["summary"]["combined_equity_usd"] == 557_500.0
    assert manifest["summary"]["fills"] == 12
    assert manifest["summary"]["auto_shorts"] == 3
    assert manifest["summary"]["reverses"] == 6
    assert manifest["summary"]["reentries"] == 3
    assert manifest["summary"]["fast_sells"] == 9
    assert manifest["summary"]["realized_pnl_usd"] == 376.5
    finos = json.loads(finos_file.read_text(encoding="utf-8"))
    assert finos["plugin"] == "@finos-workspace/plotly-charts@2.5.0"
    assert finos["paper_only"] is True
    assert finos["live_execution_enabled"] is False
    assert len(finos["openbb_rows"]) == 4


def test_performance_aggregator_parses_v10_alpaca_status_fields(tmp_path: Path) -> None:
    import json

    for acct, eq, ent, sr, lfs, fs in (
        ("ALPACA_PA1", 802_953.45, 5, 11, 2, 1),
        ("ALPACA_PA2", 99_388.17, 3, 7, 1, 1),
        ("ALPACA_PA3", 118_644.72, 2, 4, 0, 2),
    ):
        (tmp_path / f"{acct.lower()}_live_automation_status.json").write_text(
            json.dumps(
                {
                    "account_alias": acct,
                    "strategy_profile": f"PROF_{acct}",
                    "paper_only": True,
                    "live_execution_enabled": False,
                    "equity": eq,
                    "daily_drawdown_pct": 0.0012,
                    "entries": ent,
                    "auto_shorts": 1,
                    "reverses": 1,
                    "reentries": 1,
                    "stop_replaces": sr,
                    "lossless_fast_sells": lfs,
                    "fast_scratches": fs,
                }
            ),
            encoding="utf-8",
        )
    manifest = aggregate_paper_farm_performance(tmp_path)
    assert manifest["summary"]["combined_equity_usd"] == pytest.approx(1_020_986.34)
    assert manifest["summary"]["entries"] == 10
    assert manifest["summary"]["stop_replaces"] == 22
    assert manifest["summary"]["fast_sells"] == 7
    assert manifest["summary"]["auto_shorts"] == 3
    assert manifest["summary"]["reverses"] == 3
    assert manifest["summary"]["reentries"] == 3
    assert manifest["per_lane"]["ALPACA_PA1"]["lane_id"] == "PROF_ALPACA_PA1"


def test_tca_implementation_shortfall_decimal_and_borrow_accrual() -> None:
    @given(
        side=st.sampled_from(["BUY", "SELL"]),
        decision_px=st.decimals(min_value="5.00", max_value="500.00", places=2),
        arrival_delta=st.decimals(min_value="-1.00", max_value="1.00", places=2),
        fill_delta=st.decimals(min_value="-1.00", max_value="1.00", places=2),
        horizon_delta=st.decimals(min_value="-1.00", max_value="1.00", places=2),
        order_qty=st.integers(min_value=10, max_value=500),
        fill_ratio=st.sampled_from([Decimal("0.5"), Decimal("1.0")]),
        comm_usd=st.decimals(min_value="0.00", max_value="5.00", places=2),
        borrow_rate=st.decimals(min_value="0.00", max_value="0.25", places=2),
    )
    @settings(max_examples=40, deadline=None)
    def _check_perold_identity(
        side: str,
        decision_px: Decimal,
        arrival_delta: Decimal,
        fill_delta: Decimal,
        horizon_delta: Decimal,
        order_qty: int,
        fill_ratio: Decimal,
        comm_usd: Decimal,
        borrow_rate: Decimal,
    ) -> None:
        arrival_px = max(Decimal("1.00"), decision_px + arrival_delta)
        fill_px = max(Decimal("1.00"), arrival_px + fill_delta)
        horizon_px = max(Decimal("1.00"), decision_px + horizon_delta)
        filled_qty = int(Decimal(order_qty) * fill_ratio)
        unfilled_qty = order_qty - filled_qty

        rec = compute_tca_record(
            account="ALPACA_PA1",
            lane_id="L1_ORB_BURST",
            broker_order_id="prop-1",
            symbol="NVDA",
            side=side,
            order_qty=order_qty,
            filled_qty=filled_qty,
            decision_price=decision_px,
            arrival_price=arrival_px,
            fill_price=fill_px,
            horizon_price=horizon_px,
            commission_usd=comm_usd,
            borrow_tier="HTB" if (side == "SELL" and borrow_rate > 0) else "ETB",
            annual_borrow_rate=borrow_rate if side == "SELL" else Decimal("0"),
            prior_close=decision_px,
            trade_date=date(2026, 10, 9),
        )
        s = Decimal("1") if side == "BUY" else Decimal("-1")
        expected_usd = (
            s * (fill_px - decision_px) * Decimal(filled_qty)
            + rec.commission_usd
            + rec.borrow_fee_usd
            + s * (horizon_px - decision_px) * Decimal(unfilled_qty)
        ).quantize(Decimal("0.000001"))
        assert rec.total_is_usd == expected_usd
        assert rec.total_net_cost_bps == (
            rec.delay_bps
            + rec.execution_bps
            + rec.commission_bps
            + rec.borrow_bps
            + rec.opportunity_bps
        )

    _check_perold_identity()

    # BUY/SELL sign convention: worse fill -> positive IS bps cost; better fill -> negative IS bps.
    buy_cost = compute_tca_record(
        account="ALPACA_PA1",
        lane_id="L1_ORB_BURST",
        broker_order_id="b-1",
        symbol="SOXL",
        side="BUY",
        order_qty=100,
        arrival_price=Decimal("100.00"),
        fill_price=Decimal("100.10"),
    )
    assert buy_cost.implementation_shortfall_bps == Decimal("10.000000")

    buy_improve = compute_tca_record(
        account="ALPACA_PA1",
        lane_id="L1_ORB_BURST",
        broker_order_id="b-2",
        symbol="SOXL",
        side="BUY",
        order_qty=100,
        arrival_price=Decimal("100.00"),
        fill_price=Decimal("99.95"),
    )
    assert buy_improve.implementation_shortfall_bps == Decimal("-5.000000")

    sell_cost = compute_tca_record(
        account="ALPACA_PA1",
        lane_id="L1_ORB_BURST",
        broker_order_id="s-1",
        symbol="SOXL",
        side="SELL",
        order_qty=100,
        arrival_price=Decimal("100.00"),
        fill_price=Decimal("99.90"),
    )
    assert sell_cost.implementation_shortfall_bps == Decimal("10.000000")

    sell_improve = compute_tca_record(
        account="ALPACA_PA1",
        lane_id="L1_ORB_BURST",
        broker_order_id="s-2",
        symbol="SOXL",
        side="SELL",
        order_qty=100,
        arrival_price=Decimal("100.00"),
        fill_price=Decimal("100.05"),
    )
    assert sell_improve.implementation_shortfall_bps == Decimal("-5.000000")

    # Round-lot rounding and Friday 3-day borrow accrual vs Thursday 1-day accrual.
    assert round_lot_up(0) == 0
    assert round_lot_up(1) == 100
    assert round_lot_up(100) == 100
    assert round_lot_up(101) == 200
    assert borrow_accrual_days(trade_date=date(2026, 10, 8)) == 1  # Thursday
    assert borrow_accrual_days(trade_date=date(2026, 10, 9)) == 3  # Friday -> 3 calendar days

    etb_usd, etb_bps, _ = compute_short_borrow_accrual(
        qty=150,
        prior_close=Decimal("40.00"),
        annual_borrow_rate=Decimal("0"),
        trade_date=date(2026, 10, 9),
        borrow_tier="ETB",
    )
    assert etb_usd == Decimal("0") and etb_bps == Decimal("0")

    fri_usd, fri_bps, fri_days = compute_short_borrow_accrual(
        qty=150,
        prior_close=Decimal("36.00"),
        annual_borrow_rate=Decimal("0.18"),
        trade_date=date(2026, 10, 9),
        borrow_tier="HTB",
    )
    # round_lot_up(150)=200 -> 200 * 36 * 0.18 / 360 * 3 = 10.800000 USD
    assert fri_days == 3
    assert fri_usd == Decimal("10.800000")
    assert fri_bps == Decimal("20.000000")

    # Causal Polars backward ASOF join drops stale quotes & never matches future quotes.
    t0 = 1_760_000_000_000_000_000
    fills_df = pl.DataFrame(
        {
            "broker_order_id": ["fresh", "stale", "future_only"],
            "symbol": ["NVDA", "NVDA", "SMCI"],
            "ts_ns": [t0 + 2_000_000_000, t0 + 20_000_000_000, t0 + 1_000_000_000],
        }
    )
    quotes_df = pl.DataFrame(
        {
            "symbol": ["NVDA", "SMCI"],
            "ts_ns": [t0, t0 + 3_000_000_000],
            "bid_px": [Decimal("100.00"), Decimal("40.00")],
            "ask_px": [Decimal("100.04"), Decimal("40.04")],
        }
    )
    matched = join_fills_with_quotes_asof(fills_df, quotes_df, max_staleness_ns=5_000_000_000)
    assert matched["broker_order_id"].to_list() == ["fresh"]
    assert matched["arrival_mid"].to_list() == [Decimal("100.02")]


def test_extended_hours_stop_supervision_and_submit_vs_fill_separation(tmp_path: Path) -> None:
    client = _FakeTradingClient()
    lock = CrossLaneSymbolLock()
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, client, lock)

    rth_res = lane.submit_guarded_entry(
        symbol="NVDA", side="LONG", ref_price=Decimal("100.00"), session="RTH", now_s=1000.0
    )
    pre_res = lane.submit_guarded_entry(
        symbol="SOXL", side="LONG", ref_price=Decimal("40.00"), session="PRE_MARKET", now_s=1001.0
    )
    aft_res = lane.submit_guarded_entry(
        symbol="SMCI", side="SHORT", ref_price=Decimal("50.00"), session="AFTER_HOURS", now_s=1002.0
    )
    assert rth_res is not None and pre_res is not None and aft_res is not None

    # Submit-vs-fill separation: 3 orders submitted, 0 confirmed fills until broker confirmation!
    assert lane.counters["orders_submitted"] == 3
    assert lane.counters["fills"] == 0
    assert lane.counters["extended_hours_orders"] == 2
    assert lane.counters["extended_hours_protected"] == 2

    # RTH attaches native OTO stop; PRE_MARKET & AFTER_HOURS attach fail-closed supervision.
    assert rth_res["protective_stop_mode"] == "NATIVE_OTO_STOP"
    assert getattr(client.orders[0], "order_class", None) == OrderClass.OTO
    assert getattr(client.orders[0], "extended_hours", None) is False

    for idx, res, ref_px in (
        (1, pre_res, Decimal("40.00")),
        (2, aft_res, Decimal("50.00")),
    ):
        assert res["protective_stop_mode"] == "COLLARED_STOP_LIMIT_SUPERVISED"
        assert getattr(client.orders[idx], "extended_hours", None) is True
        assert getattr(client.orders[idx], "order_class", None) is None
        sup = lane.supervised_stops[res["broker_order_id"]]
        assert sup.protective_stop_mode == "COLLARED_STOP_LIMIT_SUPERVISED"
        assert sup.collar_bps == Decimal("35")
        loss_pct = abs(ref_px - sup.stop_price) / ref_px
        assert Decimal("0") < loss_pct <= MAX_STOP_LOSS_PCT
        if sup.side == "LONG":
            assert sup.stop_limit_price < sup.stop_price < ref_px
        else:
            assert sup.stop_limit_price > sup.stop_price > ref_px

    # Crossing the stop of an unfilled parent must not open a short.
    assert lane.trigger_supervised_stop(pre_res["broker_order_id"], trigger_px=Decimal("1")) is None
    client.confirmed[pre_res["broker_order_id"]] = SimpleNamespace(
        symbol="SOXL",
        filled_qty=pre_res["qty"],
        filled_avg_price="40.04",
        status="filled",
        qty=pre_res["qty"],
        qty_available=pre_res["qty"],
    )
    # Confirming one broker fill increments confirmed fills strictly to 1 (not 3).
    tca = lane.record_broker_fill(
        broker_order_id=pre_res["broker_order_id"],
        symbol="SOXL",
        side="LONG",
        order_qty=pre_res["qty"],
        arrival_price=Decimal("40.00"),
        fill_price=Decimal("40.04"),
        session="PRE_MARKET",
    )
    assert lane.counters["fills"] == 1
    assert tca.protective_stop_mode == "COLLARED_STOP_LIMIT_SUPERVISED"

    # Triggering supervised stop submits a collared extended-hours protective exit order.
    pre_sup = lane.supervised_stops[pre_res["broker_order_id"]]
    assert (
        lane.trigger_supervised_stop(
            pre_res["broker_order_id"],
            trigger_px=pre_sup.stop_price + Decimal("0.20"),
            now_s=1003.0,
        )
        is None
    )
    exit_res = lane.trigger_supervised_stop(
        pre_res["broker_order_id"],
        trigger_px=pre_sup.stop_price,
        now_s=1004.0,
    )
    assert exit_res is not None
    assert exit_res["protective_stop_mode"] == "COLLARED_STOP_LIMIT_SUPERVISED"
    assert lane.supervised_stops[pre_res["broker_order_id"]].status == "TRIGGERED"

    with duckdb.connect(str(lane.duckdb_path), read_only=True) as conn:
        sup_cnt = conn.execute("SELECT COUNT(*) FROM paper_supervised_stops").fetchone()
        assert sup_cnt is not None and sup_cnt[0] >= 3


def test_ibkr_rate_budget_alignment_and_zero_live_port_guard(tmp_path: Path) -> None:
    ibkr = IbkrPaperAdapter(tmp_path)
    assert ibkr.paper_only is True
    assert ibkr.live_execution_enabled is False
    assert ibkr.port == 4002
    assert ibkr.connection_spec["port"] == 4002
    assert ibkr.rate_budget_max_rpm == IBKR_PAPER_MAX_RPM == 3000
    assert ibkr.rate_budget.max_rpm == 3000
    assert ibkr.rate_budget.exit_reserve_rpm == 300
    assert ibkr.node_config.risk_engine is not None
    assert ibkr.node_config.risk_engine.bypass is False
    assert ibkr.node_config.risk_engine.max_order_submit_rate == "50/00:00:01"

    # Entry traffic cannot consume the 10% (300 rpm) protective exit reserve.
    for i in range(2700):
        assert ibkr.rate_budget.try_consume(2000.0 + i * 0.001, is_exit=False) is True
    assert ibkr.rate_budget.try_consume(2003.0, is_exit=False) is False
    assert ibkr.rate_budget.try_consume(2003.0, is_exit=True) is True

    # Zero live ports and zero live-execution override.
    for forbidden_port in (4001, 7496, 8798, 8796):
        with pytest.raises(ValueError, match="Refused IBKR port"):
            IbkrPaperAdapter(tmp_path, port=forbidden_port)
    with pytest.raises(PermissionError):
        IbkrPaperAdapter(tmp_path, paper_only=False)
    with pytest.raises(PermissionError):
        IbkrPaperAdapter(tmp_path, live_execution_enabled=True)


def test_otel_prometheus_telemetry_export_without_custom_http_server(tmp_path: Path) -> None:
    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)
    pa1 = coord.lanes["ALPACA_PA1"]
    sub = pa1.submit_guarded_entry(
        symbol="SOXL", side="LONG", ref_price=Decimal("40.00"), session="PRE_MARKET", now_s=3000.0
    )
    assert sub is not None
    pa1.record_broker_fill(
        broker_order_id=sub["broker_order_id"],
        symbol="SOXL",
        side="LONG",
        order_qty=sub["qty"],
        arrival_price=Decimal("40.00"),
        fill_price=Decimal("40.02"),
        commission_usd=Decimal("0.20"),
        session="PRE_MARKET",
    )
    pa1.write_status(equity=100_500.0, session="PRE_MARKET")
    coord.ibkr.write_status(gateway_ready=True, equity=200_000.0)

    prom_path = tmp_path / "paper_farm_metrics.prom"
    manifest = aggregate_paper_farm_performance(tmp_path, prom_output_path=prom_path)
    telemetry = record_paper_farm_telemetry(manifest, prom_output_path=prom_path)

    assert telemetry["paper_only"] is True
    assert telemetry["live_execution_enabled"] is False
    assert telemetry["custom_http_server"] is False
    assert telemetry["otel_otlp_exported"] is False
    assert prom_path.is_file()

    prom_text = telemetry["prometheus_text"]
    assert prom_text == prom_path.read_text(encoding="utf-8")
    for metric_name in (
        "tier0_paper_farm_combined_equity_usd",
        "tier0_paper_farm_combined_tca_mean_is_bps",
        "tier0_paper_farm_combined_tca_p95_is_bps",
        "tier0_paper_farm_combined_tca_borrow_fee_usd",
        "tier0_paper_farm_combined_extended_hours_protected_ratio",
        "tier0_paper_farm_combined_submitted_vs_filled_ratio",
        "tier0_paper_farm_orders_submitted_total",
        "tier0_paper_farm_fills_confirmed_total",
    ):
        assert metric_name in prom_text

    expected_otel = {
        "tier0.paper_farm.equity_usd",
        "tier0.paper_farm.tca.mean_is_bps",
        "tier0.paper_farm.tca.p95_is_bps",
        "tier0.paper_farm.tca.borrow_fee_usd",
        "tier0.paper_farm.extended_hours.protected_ratio",
        "tier0.paper_farm.submitted_vs_filled_ratio",
        "tier0.paper_farm.orders_submitted",
        "tier0.paper_farm.fills_confirmed",
    }
    assert expected_otel <= set(telemetry["otel_metric_names"])
    assert telemetry["otel_data_points"] >= len(expected_otel)

    perf_src = (
        Path(__file__).resolve().parent.parent / "src" / "tier0_sota" / "live" / "performance.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("start_http_server", "HTTPServer", "http.server", "fastapi", "uvicorn"):
        assert forbidden not in perf_src


def test_tca_duckdb_persistence_and_performance_manifest_aggregation(tmp_path: Path) -> None:
    import json

    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)

    pa1 = coord.lanes["ALPACA_PA1"]
    pa2 = coord.lanes["ALPACA_PA2"]
    pa3 = coord.lanes["ALPACA_PA3"]

    s1 = pa1.submit_guarded_entry(
        symbol="NVDA", side="LONG", ref_price=Decimal("100.00"), session="RTH", now_s=4000.0
    )
    s2 = pa2.submit_guarded_entry(
        symbol="SOXL", side="LONG", ref_price=Decimal("40.00"), session="PRE_MARKET", now_s=4001.0
    )
    s3 = pa3.submit_guarded_entry(
        symbol="SMCI", side="SHORT", ref_price=Decimal("50.00"), session="AFTER_HOURS", now_s=4002.0
    )
    # Submit an additional order that remains unfilled to prove submitted_vs_filled_ratio < 1.0.
    s4 = pa1.submit_guarded_entry(
        symbol="AMD", side="LONG", ref_price=Decimal("150.00"), session="RTH", now_s=4003.0
    )
    assert s1 and s2 and s3 and s4

    pa1.record_broker_fill(
        broker_order_id=s1["broker_order_id"],
        symbol="NVDA",
        side="LONG",
        order_qty=100,
        arrival_price=Decimal("100.00"),
        fill_price=Decimal("100.02"),  # +2.0 bps IS
        commission_usd=Decimal("0.40"),
        session="RTH",
        markout_60s_bps=Decimal("3.5"),
    )
    pa2.record_broker_fill(
        broker_order_id=s2["broker_order_id"],
        symbol="SOXL",
        side="LONG",
        order_qty=100,
        arrival_price=Decimal("40.00"),
        fill_price=Decimal("40.02"),  # +5.0 bps IS
        commission_usd=Decimal("0.16"),
        session="PRE_MARKET",
        markout_60s_bps=Decimal("1.5"),
    )
    pa3.record_broker_fill(
        broker_order_id=s3["broker_order_id"],
        symbol="SMCI",
        side="SHORT",
        order_qty=150,
        arrival_price=Decimal("50.00"),
        fill_price=Decimal("49.95"),  # +10.0 bps IS (sold below arrival)
        commission_usd=Decimal("0.30"),
        borrow_tier="HTB",
        annual_borrow_rate=Decimal("0.18"),
        prior_close=Decimal("50.00"),
        trade_date=date(2026, 10, 9),  # Friday -> 3 days, round_lot_up(150)=200 -> $15.00
        session="AFTER_HOURS",
        markout_60s_bps=Decimal("-2.0"),
    )

    pa1.write_status(equity=101_000.0, session="RTH")
    pa2.write_status(equity=102_000.0, session="PRE_MARKET")
    pa3.write_status(equity=103_000.0, session="AFTER_HOURS")
    coord.ibkr.write_status(gateway_ready=True, equity=200_000.0)

    # Verify isolated DuckDB DECIMAL(18, 6) persistence on PA3.
    with duckdb.connect(str(pa3.duckdb_path), read_only=True) as conn:
        row = conn.execute(
            "SELECT implementation_shortfall_bps, borrow_fee_usd, borrow_days, "
            "protective_stop_mode FROM paper_tca_fills"
        ).fetchone()
        assert row is not None
        assert row[0] == Decimal("10.000000")
        assert row[1] == Decimal("15.000000")
        assert row[2] == 3
        assert row[3] == "COLLARED_STOP_LIMIT_SUPERVISED"

    manifest = aggregate_paper_farm_performance(tmp_path)
    summary = manifest["summary"]
    tca_summary = manifest["tca_summary"]
    finos = json.loads((tmp_path / "finos_paper_farm_performance.json").read_text(encoding="utf-8"))

    # IS samples = [2.0, 5.0, 10.0] -> mean = 17/3 = 5.666667, p95 = 5 + 0.9*(10-5) = 9.5
    assert summary["orders_submitted"] == 4
    assert summary["fills"] == 3
    assert summary["tca_fills_count"] == 3
    assert summary["mean_is_bps"] == pytest.approx(5.666667, rel=1e-5)
    assert summary["p95_is_bps"] == pytest.approx(9.5, rel=1e-5)
    assert summary["total_borrow_fee_usd"] == pytest.approx(15.0, rel=1e-6)
    assert summary["extended_hours_protected_ratio"] == pytest.approx(1.0, rel=1e-6)
    assert summary["submitted_vs_filled_ratio"] == pytest.approx(0.75, rel=1e-6)

    assert tca_summary["mean_is_bps"] == summary["mean_is_bps"]
    assert tca_summary["p95_is_bps"] == summary["p95_is_bps"]
    assert tca_summary["total_borrow_fee_usd"] == summary["total_borrow_fee_usd"]
    assert tca_summary["extended_hours_protected_ratio"] == 1.0
    assert tca_summary["submitted_vs_filled_ratio"] == 0.75

    assert finos["summary"]["mean_is_bps"] == summary["mean_is_bps"]
    assert finos["summary"]["p95_is_bps"] == summary["p95_is_bps"]
    assert finos["summary"]["total_borrow_fee_usd"] == summary["total_borrow_fee_usd"]
    assert finos["summary"]["extended_hours_protected_ratio"] == 1.0
    assert finos["summary"]["submitted_vs_filled_ratio"] == 0.75
    assert finos["tca_summary"]["tca_fills_count"] == 3


def test_sync_broker_fills_idempotent_with_sip_quotes_and_29col_duckdb(tmp_path: Path) -> None:
    from datetime import timedelta

    t_sub = datetime(2026, 10, 15, 15, 0, 0, tzinfo=UTC)
    t_fill = t_sub + timedelta(seconds=10)
    t_mk60 = t_fill + timedelta(seconds=60)

    class _SyncTradingClient(_FakeTradingClient):
        def get_account(self) -> SimpleNamespace:
            return SimpleNamespace(equity="105432.10", last_equity="105000.00")

        def get_all_positions(self) -> list[SimpleNamespace]:
            return [SimpleNamespace(symbol="SOXL", qty="100")]

        def get_orders(self, _req: object) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(
                    id="fill-ord-1",
                    symbol="SOXL",
                    side="buy",
                    qty="100",
                    filled_qty="100",
                    filled_avg_price="40.02",
                    status="filled",
                    submitted_at=t_sub,
                    filled_at=t_fill,
                    type="market",
                    extended_hours=False,
                ),
                SimpleNamespace(
                    id="canceled-ord-2",
                    symbol="NVDA",
                    side="buy",
                    qty="10",
                    filled_qty="0",
                    filled_avg_price=None,
                    status="canceled",
                    submitted_at=t_sub,
                    filled_at=None,
                    type="limit",
                    extended_hours=False,
                ),
            ]

    class _SyncDataClient:
        def get_stock_quotes(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "SOXL": [
                        SimpleNamespace(timestamp=t_sub, bid_price=39.99, ask_price=40.01),
                        SimpleNamespace(timestamp=t_fill, bid_price=39.99, ask_price=40.01),
                        SimpleNamespace(timestamp=t_mk60, bid_price=40.05, ask_price=40.07),
                    ]
                }
            )

    # Pre-create a 29-column paper_tca_fills table (with legacy execution_id) to prove compatibility
    db_path = tmp_path / "paper_lane_pa1.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE paper_tca_fills ("
            "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
            "symbol VARCHAR, side VARCHAR, session VARCHAR, route VARCHAR, "
            "protective_stop_mode VARCHAR, order_qty DECIMAL(18, 6), filled_qty DECIMAL(18, 6), "
            "decision_price DECIMAL(18, 6), arrival_price DECIMAL(18, 6), "
            "fill_price DECIMAL(18, 6), horizon_price DECIMAL(18, 6), "
            "delay_bps DECIMAL(18, 6), execution_bps DECIMAL(18, 6), "
            "implementation_shortfall_bps DECIMAL(18, 6), commission_usd DECIMAL(18, 6), "
            "commission_bps DECIMAL(18, 6), borrow_fee_usd DECIMAL(18, 6), "
            "borrow_bps DECIMAL(18, 6), opportunity_bps DECIMAL(18, 6), "
            "total_net_cost_bps DECIMAL(18, 6), total_is_usd DECIMAL(18, 6), "
            "borrow_tier VARCHAR, borrow_days INTEGER, markout_60s_bps DECIMAL(18, 6), "
            "execution_id VARCHAR)"
        )

    lock = CrossLaneSymbolLock()
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, _SyncTradingClient(), lock)
    rep1 = lane.sync_broker_fills_and_status(data_client=_SyncDataClient())
    assert rep1["equity"] == pytest.approx(105432.10)
    assert rep1["open_positions_count"] == 1
    assert rep1["newly_recorded_tca_fills"] == 1
    assert rep1["sip_quoted_fills"] == 1
    assert rep1["total_tca_fills_duckdb"] == 1

    # Second call is idempotent: 0 newly recorded fills, still 1 total row in DuckDB
    rep2 = lane.sync_broker_fills_and_status(data_client=_SyncDataClient())
    assert rep2["newly_recorded_tca_fills"] == 0
    assert rep2["total_tca_fills_duckdb"] == 1

    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "SELECT implementation_shortfall_bps, markout_60s_bps FROM paper_tca_fills"
        ).fetchone()
        assert row is not None
        # BUY fill at 40.02 vs arrival mid 40.00 -> +5.0 bps IS; +60s mid 40.06 -> ~+9.995 bps
        assert row[0] == Decimal("5.000000")
        assert row[1] > Decimal("9.0")


def test_overnight_rejects_symbol_without_overnight_tradable(tmp_path: Path, monkeypatch) -> None:
    stop = threading.Event()

    class Broker(_FakeTradingClient):
        def __init__(self) -> None:
            super().__init__()
            self.identity = uuid4()

        def get_account(self):
            return SimpleNamespace(
                id=self.identity,
                status=SimpleNamespace(value="ACTIVE"),
                account_blocked=False,
                trading_blocked=False,
                trade_suspended_by_user=False,
                equity="100000",
                last_equity="100000",
            )

        def get_all_positions(self):
            return []

        def get_orders(self, request):
            return []

        def get_asset(self, symbol):
            return SimpleNamespace(tradable=True, attributes=[])

    brokers = {acct: Broker() for acct in ALPACA_LANE_PROFILES}
    farm = PaperFarmCoordinator(tmp_path, alpaca_clients=brokers)

    class Stream:
        def __init__(self, *args, feed) -> None:
            self.feed = feed
            self.subscribed = threading.Event()

        def subscribe_trades(self, callback, *symbols) -> None:
            self.callback = callback
            if "NVDA" in symbols:
                self.subscribed.set()

        def run(self) -> None:
            assert self.subscribed.wait(15)
            if self.feed.value == "boats":
                asyncio.run(
                    self.callback(
                        SimpleNamespace(symbol="NVDA", price=100, timestamp=datetime.now(UTC))
                    )
                )
                stop.set()
            assert stop.wait(15)

        def stop(self) -> None:
            stop.set()

    now = datetime.now(UTC).isoformat()
    (tmp_path / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now,
                "macro_regime": {"regime": 1},
                "entries": [
                    {
                        "symbol": "NVDA",
                        "extreme_mover_eligible": True,
                        "side": "BUY",
                        "session": "OVERNIGHT",
                        "bar_close_utc": now,
                        "natr_pct": 0.012,
                        "last_price": 100,
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "resolve_alpaca_env_creds", lambda: ("", ""))
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _: farm)
    monkeypatch.setattr(execution, "StockDataStream", Stream)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner,
        "from_env",
        lambda: SimpleNamespace(data_client=SimpleNamespace()),
    )
    monkeypatch.setattr(execution.engine, "classify_us_equity_session_et", lambda _: "OVERNIGHT")
    execution.run_execution(tmp_path, stop=stop)
    assert all(len(b.orders) == 0 for b in brokers.values())


def test_exit_context_skips_orphaned_404_client_order_id(tmp_path: Path) -> None:
    submitted = datetime(2026, 11, 25, 15, 0, tzinfo=UTC)
    child = SimpleNamespace(
        id="child-1",
        replaces=None,
        symbol="NVDA",
        stop_price="101",
        client_order_id="leg-1",
        submitted_at=submitted,
        side=SimpleNamespace(value="sell"),
    )
    parent = SimpleNamespace(id="parent-id", side=SimpleNamespace(value="buy"), legs=[child])

    class Broker(_FakeTradingClient):
        def get_order_by_client_id(self, cid):
            if cid == "signal-missing":
                raise APIError(
                    "missing", http_error=SimpleNamespace(response=SimpleNamespace(status_code=404))
                )
            return parent

        def get_order_by_id(self, order_id, *args):
            return parent

    class Quotes:
        def get_stock_quotes(self, request):
            return SimpleNamespace(
                data={
                    "NVDA": [
                        SimpleNamespace(bid_price=101.10, ask_price=101.12, timestamp=submitted)
                    ]
                }
            )

    lane = AlpacaPaperLane("ALPACA_PA2", tmp_path, Broker(), CrossLaneSymbolLock())
    ctx = exit_context(
        lane, child, {"signal-missing": ("NVDA",), "signal-live": ("NVDA",)}, Quotes()
    )
    assert ctx is not None and ctx[0] == "NVDA" and ctx[1] == "SHORT"


def test_read_duckdb_counts_falls_back_on_lock_error(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "paper_lane_pa1.duckdb"
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, _FakeTradingClient(), CrossLaneSymbolLock())
    lane.record_broker_fill(
        broker_order_id="ord-lock-1",
        symbol="SOXL",
        side="BUY",
        order_qty=10,
        arrival_price=Decimal("40.00"),
        fill_price=Decimal("40.04"),
        session="RTH",
        execution_id="exec-lock-1",
    )
    real_connect = duckdb.connect

    def _connect(path: str | Path = ":memory:", *, read_only: bool = False):
        if Path(path) == db_path:
            raise duckdb.IOException("simulated writer lock")
        return real_connect(str(path), read_only=read_only)

    monkeypatch.setattr(duckdb, "connect", _connect)
    counts = _read_duckdb_counts(db_path)
    assert counts["tca_fills_count"] == 1


def test_paper_farm_start_loop_uses_zero_window_pythonw() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    ps1_text = (repo_root / "scripts" / "run_live_paper_farm.ps1").read_text(encoding="utf-8")
    pyw_path = repo_root / "scripts" / "paper_farm_loop_hidden.pyw"
    assert pyw_path.is_file()
    pyw_text = pyw_path.read_text(encoding="utf-8")

    assert ".venv\\Scripts\\pythonw.exe" in ps1_text
    assert "paper_farm_loop_hidden.pyw" in ps1_text
    assert "-WindowStyle Hidden" not in ps1_text
    assert "CREATE_NO_WINDOW = 0x08000000" in pyw_text
    assert "shell=True" not in pyw_text
    assert "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847" in pyw_text
    assert "timeout=55.0 if args.observe else None" in pyw_text
    assert "paper-farm-observe.lock" in pyw_text
    assert "paper-farm-execution.lock" in pyw_text
    assert "paper-farm-execution-health.json" in pyw_text
    assert "_active_forbidden_tcp_ports" in pyw_text


def test_hidden_launcher_detects_remote_forbidden_ports() -> None:
    import importlib.machinery
    import importlib.util

    pyw_path = Path(__file__).resolve().parents[1] / "scripts" / "paper_farm_loop_hidden.pyw"
    loader = importlib.machinery.SourceFileLoader("paper_farm_loop_hidden", str(pyw_path))
    spec = importlib.util.spec_from_loader("paper_farm_loop_hidden", loader)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    sample_netstat = (
        "  Proto  Local Address          Foreign Address        State           PID\n"
        "  TCP    10.0.0.5:51234         203.0.113.10:4001      ESTABLISHED     1234\n"
        "  TCP    127.0.0.1:4002         0.0.0.0:0              LISTENING       5678\n"
    )
    ports = mod._parse_windows_netstat_ports(sample_netstat)
    assert 4001 in ports and 4002 in ports

    proc_tcp = (
        "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid\n"
        "   0: 0100007F:C350 0A000002:1D48 01 00000000:00000000 00:00000000 00000000 1000\n"
    )
    assert 7496 in mod._parse_proc_net_tcp_ports(proc_tcp)


def test_paper_lane_preserves_intraday_drawdown_latch_and_rejects_non_paper_client(
    tmp_path: Path,
) -> None:
    class _EquityClient(_FakeTradingClient):
        def __init__(self) -> None:
            super().__init__()
            self.equity = "98000.00"
            self.last_equity = "100000.00"

        def get_account(self) -> SimpleNamespace:
            return SimpleNamespace(equity=self.equity, last_equity=self.last_equity)

        def get_all_positions(self) -> list[object]:
            return []

        def get_orders(self, _req: object) -> list[object]:
            return []

    client = _EquityClient()
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, client, CrossLaneSymbolLock())
    lane.sync_broker_fills_and_status()
    assert lane.breaker.tripped is True

    # Equity rebounds intra-day: latch MUST remain tripped on subsequent syncs today.
    client.equity = "101000.00"
    lane.sync_broker_fills_and_status()
    assert lane.breaker.tripped is True
    assert lane.breaker.allow_entry() is False

    # Non-paper injected clients must be refused fail-closed in both coordinator and lane.
    non_paper_flag = SimpleNamespace(_sandbox=False)
    non_paper_url = SimpleNamespace(_base_url="https://not-paper.example.com")
    for bad in (non_paper_flag, non_paper_url):
        with pytest.raises(PermissionError):
            AlpacaPaperLane("ALPACA_PA1", tmp_path, bad, CrossLaneSymbolLock())
        with pytest.raises(PermissionError):
            PaperFarmCoordinator(tmp_path, alpaca_clients={"ALPACA_PA1": bad})


def test_tca_slippage_guards_rth_5bps_collar_and_quote_microstructure(tmp_path: Path) -> None:
    client = _FakeTradingClient()
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, client, CrossLaneSymbolLock())
    res = lane.submit_guarded_entry(
        symbol="NVDA", side="LONG", ref_price=Decimal("100.00"), session="RTH", now_s=5000.0
    )
    assert res is not None
    # Default RTH collar is tightened to 5 bps (100.05) rather than 30 bps (100.30).
    assert Decimal(str(res["limit_price"])) == Decimal("100.05")
    with pytest.raises(ValueError, match="rth_entry_collar_pct"):
        lane.submit_guarded_entry(
            symbol="AMD",
            side="LONG",
            ref_price=Decimal("100.00"),
            session="RTH",
            rth_entry_collar_pct=Decimal("0.0030"),
        )

    # Reject sub-$10 tick-floor traps ($3.66 STLA, $4.65 FLNC) and wide spreads.
    assert (
        execution.quote_microstructure_allows_entry(Decimal("3.66"), Decimal("3.67"), "RTH")
        is False
    )
    assert (
        execution.quote_microstructure_allows_entry(Decimal("4.65"), Decimal("4.66"), "RTH")
        is False
    )
    assert (
        execution.quote_microstructure_allows_entry(Decimal("100.00"), Decimal("100.20"), "RTH")
        is False
    )
    assert (
        execution.quote_microstructure_allows_entry(Decimal("100.00"), Decimal("100.01"), "RTH")
        is True
    )
    assert (
        execution.quote_microstructure_allows_entry(
            Decimal("31.20"), Decimal("31.28"), "PRE_MARKET"
        )
        is True
    )
    assert (
        execution.quote_microstructure_allows_entry(
            Decimal("31.20"), Decimal("31.40"), "PRE_MARKET"
        )
        is False
    )


def test_high_throughput_order_handle_3600_rpm_burst_and_24x5_parity(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)

    # 24/5 session parity on IBKR :4002 adapter
    for sess, exp_stop_type, exp_outside, exp_collar in (
        ("RTH", "STOP_MARKET", False, 0.0),
        ("PRE_MARKET", "STOP_LIMIT", True, 35.0),
        ("AFTER_HOURS", "STOP_LIMIT", True, 35.0),
        ("OVERNIGHT", "STOP_LIMIT", True, 50.0),
    ):
        plan = coord.ibkr.plan_guarded_order(
            symbol=f"S{sess[:3]}",
            side="LONG",
            ref_price=Decimal("100.00"),
            session=sess,
            now_s=100.0,
        )
        assert plan is not None
        assert plan["paper_only"] is True and plan["live_execution_enabled"] is False
        assert plan["entry_order_type"] == "LIMIT"
        assert plan["stop_order_type"] == exp_stop_type
        assert plan["outside_rth"] is exp_outside
        assert plan["limit_collar_bps"] == exp_collar
        loss_pct = (Decimal(str(plan["limit_price"])) - Decimal(str(plan["stop_price"]))) / Decimal(
            str(plan["limit_price"])
        )
        assert Decimal("0") < loss_pct <= MAX_STOP_LOSS_PCT

    # Invalid symbol / non-positive price fail closed without leaking CrossLaneSymbolLock.
    assert (
        coord.ibkr.plan_guarded_order(
            symbol="   ", side="LONG", ref_price=Decimal("50.00"), session="RTH", now_s=105.0
        )
        is None
    )
    assert (
        coord.ibkr.plan_guarded_order(
            symbol="BADPX", side="LONG", ref_price=Decimal("0"), session="RTH", now_s=105.0
        )
        is None
    )
    assert "" not in coord.symbol_lock.snapshot()
    assert "BADPX" not in coord.symbol_lock.snapshot()

    # Multi-threaded CrossLaneSymbolLock contention: strictly one winner per symbol.
    lock_coord = PaperFarmCoordinator(
        tmp_path / "mt", alpaca_clients={a: _FakeTradingClient() for a in ALPACA_LANE_PROFILES}
    )
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [
            pool.submit(
                lock_coord.ibkr.plan_guarded_order,
                symbol="CONTEND",
                side="LONG",
                ref_price=Decimal("50.00"),
                session="RTH",
                now_s=200.0,
            )
            for _ in range(16)
        ]
        winners = [f.result() for f in futs if f.result() is not None]
    assert len(winners) == 1

    # 3,400-symbol burst across IBKR (3,000 rpm) + PA1..PA3 (3x200 rpm) = 3,600 rpm combined:
    # must admit exactly 2,700 (IBKR) + 540 (PA1..PA3) = 3,240 entries (>= 1,000 rpm target)
    # with zero symbol overlap while strictly preserving all 360 protective exit reserve slots!
    burst_coord = PaperFarmCoordinator(
        tmp_path / "burst", alpaca_clients={a: _FakeTradingClient() for a in ALPACA_LANE_PROFILES}
    )
    etb = {"shortable": True, "easy_to_borrow": True, "borrow_source": "alpaca_paper_asset"}
    entries = [
        {"symbol": f"SYM{i:04d}", "last_price": 50.0, "extreme_mover_eligible": True, **etb}
        for i in range(3400)
    ]
    res = burst_coord.dispatch_high_throughput(entries, macro_regime=1, session="RTH", now_s=9000.0)
    assert res["combined_max_rpm"] == 3600
    assert res["combined_exit_reserve_rpm"] == 360
    assert res["admitted_count"] == 3240
    assert res["rejected_count"] == 160
    assert res["by_account"]["IBKR_DU_4002"] == 2700
    assert res["by_account"]["ALPACA_PA1"] == 180
    assert res["by_account"]["ALPACA_PA2"] == 180
    assert res["by_account"]["ALPACA_PA3"] == 180
    admitted_syms = [r["symbol"] for r in res["admitted"]]
    assert len(admitted_syms) == len(set(admitted_syms))

    # Even after entry saturation, protective exits MUST still be admitted from the 10% reserve.
    exit_plan = burst_coord.ibkr.plan_guarded_order(
        symbol="SYM0001",
        side="SHORT",
        ref_price=Decimal("50.00"),
        session="RTH",
        now_s=9001.0,
        is_exit=True,
    )
    assert exit_plan is not None and exit_plan["is_exit"] is True
    assert exit_plan["entry_order_type"] == "MARKET"
    for lane in burst_coord.lanes.values():
        assert lane.rate_budget.try_consume(9001.0, is_exit=False) is False
        assert lane.rate_budget.try_consume(9001.0, is_exit=True) is True

    # Non-finite quote prices reject cleanly without crashing dispatch_high_throughput.
    bad_res = burst_coord.dispatch_high_throughput(
        [{"symbol": "NANPX", "last_price": "nan", "extreme_mover_eligible": True, **etb}],
        macro_regime=1,
        session="RTH",
        now_s=9002.0,
    )
    assert bad_res["admitted_count"] == 0 and bad_res["rejected_count"] == 1


def test_cost_calibration_allows_liquid_ge10_and_fails_closed_on_adverse(tmp_path: Path) -> None:
    path = tmp_path / "tca_calibration_manifest.json"
    live_style_liquid_ok = {
        "schema_version": "tier0.tca-calibration-manifest.v1",
        "paper_only": True,
        "live_execution_enabled": False,
        "duckdb_errors": [],
        "duckdb_tca_rows": 149,
        "realized_mean_is_bps": 9.3403,
        "parity_within_2x_envelope": False,
        "liquid_ge10_tca_rows": 85,
        "liquid_ge10_mean_is_bps": 7.2479,
        "liquid_ge10_within_2x_envelope": True,
        "min_liquid_entry_price_usd": 10.0,
    }
    path.write_text(json.dumps(live_style_liquid_ok), encoding="utf-8")
    assert execution.cost_calibration_allows_entry(tmp_path) is True

    # Fail-closed cases: adverse liquid TCA (even if all-fills parity is True), 0 liquid rows,
    # sub-$10 threshold, bool spoofing, non-paper flags, duckdb_errors, or wrong schema.
    fail_cases = [
        live_style_liquid_ok | {"liquid_ge10_within_2x_envelope": False},
        live_style_liquid_ok
        | {"parity_within_2x_envelope": True, "liquid_ge10_within_2x_envelope": False},
        live_style_liquid_ok | {"liquid_ge10_tca_rows": 0},
        live_style_liquid_ok | {"liquid_ge10_tca_rows": True},
        live_style_liquid_ok | {"min_liquid_entry_price_usd": 9.99},
        live_style_liquid_ok | {"min_liquid_entry_price_usd": None},
        live_style_liquid_ok | {"paper_only": False},
        live_style_liquid_ok | {"live_execution_enabled": True},
        live_style_liquid_ok | {"duckdb_errors": ["paper_lane_pa1.duckdb:IOException"]},
        live_style_liquid_ok | {"schema_version": "tier0.tca-calibration-manifest.v0"},
    ]
    for bad_receipt in fail_cases:
        path.write_text(json.dumps(bad_receipt), encoding="utf-8")
        assert execution.cost_calibration_allows_entry(tmp_path) is False


def test_situation_aware_lane_routing_and_breaker_fallback(tmp_path: Path, monkeypatch) -> None:
    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)

    # 1. Preferred profile mapping across all 3 situations + boundaries.
    pa3_inv = select_situation_lane(
        coord.lanes, origin="INVERSE_HEDGE_SOXS", natr_pct=0.045, rvol_normalized=5.0
    )
    assert pa3_inv is not None and pa3_inv.account == "ALPACA_PA3"

    pa3_tight = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.015, rvol_normalized=2.0
    )
    assert pa3_tight is not None and pa3_tight.account == "ALPACA_PA3"

    pa1_rvol = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.022, rvol_normalized=3.0
    )
    assert pa1_rvol is not None and pa1_rvol.account == "ALPACA_PA1"

    pa1_wide = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.031, rvol_normalized=1.6
    )
    assert pa1_wide is not None and pa1_wide.account == "ALPACA_PA1"

    pa2_struct = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.030, rvol_normalized=2.5
    )
    assert pa2_struct is not None and pa2_struct.account == "ALPACA_PA2"

    # 2. Deterministic fallback when preferred lane is tripped or missing.
    coord.lanes["ALPACA_PA1"].breaker.tripped = True
    fb1 = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.040, rvol_normalized=4.0
    )
    assert fb1 is not None and fb1.account == "ALPACA_PA2"

    coord.lanes["ALPACA_PA2"].breaker.tripped = True
    fb2 = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.040, rvol_normalized=4.0
    )
    assert fb2 is not None and fb2.account == "ALPACA_PA3"

    coord.lanes["ALPACA_PA3"].breaker.tripped = True
    assert (
        select_situation_lane(
            coord.lanes, origin="INVERSE_HEDGE_SQQQ", natr_pct=0.020, rvol_normalized=2.0
        )
        is None
    )

    # Reset breakers and check PA3 -> PA2 and PA2 -> PA3 fallback directions.
    for lane in coord.lanes.values():
        lane.breaker.tripped = False
    coord.lanes["ALPACA_PA3"].breaker.tripped = True
    fb_inv = select_situation_lane(coord.lanes, origin="INVERSE_HEDGE_SOXS", natr_pct=0.012)
    assert fb_inv is not None and fb_inv.account == "ALPACA_PA2"
    coord.lanes["ALPACA_PA3"].breaker.tripped = False
    coord.lanes["ALPACA_PA2"].breaker.tripped = True
    fb_struct = select_situation_lane(
        coord.lanes, origin="TREND_ALIGNED_LONG", natr_pct=0.022, rvol_normalized=2.0
    )
    assert fb_struct is not None and fb_struct.account == "ALPACA_PA3"
    coord.lanes["ALPACA_PA2"].breaker.tripped = False

    # 3. Coordinator dispatch_movers routes by situation & preserves CrossLaneSymbolLock.
    etb = {"shortable": True, "easy_to_borrow": True, "borrow_source": "alpaca_paper_asset"}
    routed = coord.dispatch_movers(
        [
            {
                "symbol": "NVDA",
                "last_price": 120.0,
                "natr_pct": 0.038,
                "rvol_normalized": 4.2,
                **etb,
            },
            {
                "symbol": "AMD",
                "last_price": 140.0,
                "natr_pct": 0.022,
                "rvol_normalized": 2.0,
                **etb,
            },
            {
                "symbol": "MSFT",
                "last_price": 410.0,
                "natr_pct": 0.012,
                "rvol_normalized": 1.8,
                **etb,
            },
            {
                "symbol": "NVDA",
                "last_price": 120.0,
                "natr_pct": 0.038,
                "rvol_normalized": 4.2,
                **etb,
            },
        ],
        macro_regime=1,
        session="RTH",
    )
    assert {(r["symbol"], r["account"]) for r in routed} == {
        ("NVDA", "ALPACA_PA1"),
        ("AMD", "ALPACA_PA2"),
        ("MSFT", "ALPACA_PA3"),
    }

    # 4. Execution worker honors liquid_ge10 TCA pass & falls back from tripped PA1 -> PA2.
    exec_dir = tmp_path / "exec_route"
    exec_dir.mkdir()
    stop = threading.Event()

    class _RoutedBroker(_FakeTradingClient):
        def __init__(self, equity: str) -> None:
            super().__init__()
            self.identity = uuid4()
            self.equity = equity

        def get_account(self):
            return SimpleNamespace(
                id=self.identity,
                status=SimpleNamespace(value="ACTIVE"),
                account_blocked=False,
                trading_blocked=False,
                trade_suspended_by_user=False,
                equity=self.equity,
                last_equity="100000",
            )

        def get_all_positions(self):
            return []

        def get_orders(self, request):
            return []

        def get_clock(self):
            return SimpleNamespace(is_open=True)

        def get_asset(self, symbol):
            return SimpleNamespace(tradable=True, attributes=[])

        def get_order_by_id(self, order_id, *args):
            request = self.orders[0]
            assert isinstance(request, LimitOrderRequest) and request.stop_loss is not None
            child = SimpleNamespace(
                id="child",
                status=SimpleNamespace(value="held"),
                replaced_by=None,
                symbol=request.symbol,
                side=SimpleNamespace(value="sell"),
                stop_price=str(request.stop_loss.stop_price),
            )
            return SimpleNamespace(
                id=order_id,
                legs=[child],
                status=SimpleNamespace(value="new"),
                filled_qty="0",
                filled_avg_price=None,
            )

    # PA1 tripped (-2% drawdown), PA2 & PA3 healthy.
    brokers = {
        "ALPACA_PA1": _RoutedBroker("98000"),
        "ALPACA_PA2": _RoutedBroker("100000"),
        "ALPACA_PA3": _RoutedBroker("100000"),
    }
    farm = PaperFarmCoordinator(exec_dir, alpaca_clients=brokers)

    class _Quotes:
        def get_stock_latest_quote(self, request):
            return {
                "SMCI": SimpleNamespace(
                    bid_price=50.00, ask_price=50.02, timestamp=datetime.now(UTC)
                )
            }

    class _Stream:
        def __init__(self, *args, feed) -> None:
            self.feed = feed
            self.subscribed = threading.Event()

        def subscribe_trades(self, callback, *symbols) -> None:
            self.callback = callback
            if "SMCI" in symbols:
                self.subscribed.set()

        def run(self) -> None:
            assert self.subscribed.wait(15)
            if self.feed.value == "sip":
                asyncio.run(
                    self.callback(
                        SimpleNamespace(symbol="SMCI", price=50.01, timestamp=datetime.now(UTC))
                    )
                )
                stop.set()
            assert stop.wait(15)

        def stop(self) -> None:
            stop.set()

    now = datetime.now(UTC).isoformat()
    (exec_dir / "tca_calibration_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tca-calibration-manifest.v1",
                "paper_only": True,
                "live_execution_enabled": False,
                "duckdb_errors": [],
                "parity_within_2x_envelope": False,
                "liquid_ge10_tca_rows": 85,
                "liquid_ge10_within_2x_envelope": True,
                "min_liquid_entry_price_usd": 10.0,
            }
        ),
        encoding="utf-8",
    )
    (exec_dir / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now,
                "macro_regime": {"regime": 1},
                "entries": [
                    {
                        "symbol": "SMCI",
                        "extreme_mover_eligible": True,
                        "side": "BUY",
                        "session": "RTH",
                        "bar_close_utc": now,
                        "natr_pct": 0.036,
                        "rvol_normalized": 4.5,
                        "last_price": 50.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "resolve_alpaca_env_creds", lambda: ("", ""))
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _: farm)
    monkeypatch.setattr(execution, "StockDataStream", _Stream)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner, "from_env", lambda: SimpleNamespace(data_client=_Quotes())
    )
    monkeypatch.setattr(execution.engine, "classify_us_equity_session_et", lambda _: "RTH")
    execution.run_execution(exec_dir, stop=stop)
    assert len(brokers["ALPACA_PA1"].orders) == 0
    assert len(brokers["ALPACA_PA2"].orders) == 1
    assert len(brokers["ALPACA_PA3"].orders) == 0


def test_guard_flatten_first_regime_flip_and_auto_reverse(tmp_path: Path) -> None:
    class _FlattenBroker(_FakeTradingClient):
        def __init__(self) -> None:
            super().__init__()
            self.canceled: list[str] = []
            self.child_status = "new"

        def get_order_by_id(self, order_id, *args):
            child = SimpleNamespace(
                id="child-oto",
                status=SimpleNamespace(value=self.child_status),
                replaced_by=None,
                symbol="NVDA",
                side=SimpleNamespace(value="sell"),
                stop_price="98.50",
                filled_qty="0",
            )
            return SimpleNamespace(
                id=order_id,
                legs=[child],
                status=SimpleNamespace(value="filled"),
                filled_qty="10",
                filled_avg_price="100.00",
            )

        def cancel_order_by_id(self, order_id: str) -> None:
            self.canceled.append(order_id)
            if order_id == "child-oto":
                self.child_status = "canceled"

        def get_order_by_client_id(self, client_order_id: str):
            raise APIError(
                "missing", http_error=SimpleNamespace(response=SimpleNamespace(status_code=404))
            )

        def get_open_position(self, symbol: str):
            return SimpleNamespace(symbol=symbol, qty="10", qty_available="10")

    broker = _FlattenBroker()
    lane = AlpacaPaperLane("ALPACA_PA1", tmp_path, broker, CrossLaneSymbolLock())
    entry = lane.submit_guarded_entry(
        symbol="NVDA", side="LONG", ref_price=Decimal("100.00"), client_order_id="rev-parent"
    )
    assert entry is not None
    child = SimpleNamespace(
        id="child-oto",
        stop_price="98.50",
        status=SimpleNamespace(value="new"),
        replaced_by=None,
        symbol="NVDA",
        side=SimpleNamespace(value="sell"),
        filled_qty="0",
    )
    parent = SimpleNamespace(
        id=entry["broker_order_id"],
        status=SimpleNamespace(value="filled"),
        filled_qty="10",
        filled_avg_price="100.00",
        legs=[child],
    )
    guard = Guard(
        lane,
        "rev-parent",
        "NVDA",
        "LONG",
        Decimal("98.50"),
        Decimal("98.50"),
        Decimal("0.015"),
        "RTH",
        False,
        parent,
        child,
    )

    # Regime/scan flip detector: BULL keeps LONG, CRASH/peak flips LONG, BULL flips SOXS/SHORT.
    assert not execution.guard_should_flatten_for_flip(
        guard, market_regime=1, rows={}, session="RTH"
    )
    assert execution.guard_should_flatten_for_flip(guard, market_regime=-1, rows={}, session="RTH")
    assert execution.guard_should_flatten_for_flip(
        guard,
        market_regime=0,
        rows={
            "NVDA": {
                "symbol": "NVDA",
                "extreme_mover_eligible": True,
                "session": "OVERNIGHT",
                "peak_confirmed": True,
            }
        },
        session="OVERNIGHT",
    )
    soxs_guard = Guard(
        lane,
        "soxs-parent",
        "SOXS",
        "LONG",
        Decimal("20.00"),
        Decimal("20.00"),
        Decimal("0.015"),
        "OVERNIGHT",
        False,
        parent,
    )
    assert not execution.guard_should_flatten_for_flip(
        soxs_guard, market_regime=-1, rows={}, session="OVERNIGHT"
    )
    assert execution.guard_should_flatten_for_flip(
        soxs_guard, market_regime=1, rows={}, session="OVERNIGHT"
    )

    # Flatten-first in RTH: tick 1 cancels resting OTO child; tick 2 submits collared exit.
    guard.reverse_flatten = True
    guard.on_price(Decimal("101.00"), "RTH")
    assert guard.triggered is True
    assert broker.canceled == ["child-oto"]
    assert len(broker.orders) == 1  # only the original entry so far

    guard.last_exit_check = 0
    guard.on_price(Decimal("101.00"), "RTH")
    assert len(broker.orders) == 2
    exit_ord = broker.orders[-1]
    assert isinstance(exit_ord, LimitOrderRequest)
    assert exit_ord.client_order_id == f"exit-{entry['broker_order_id']}"
    # 101.00 * (1 - 0.0005) = 100.9495 -> Rule-612 conservative floor = 100.94.
    assert exit_ord.limit_price == pytest.approx(100.94)
    assert lane.counters["reverses"] == 1


def _make_ts_bar(ts: datetime, open_px: float, close_px: float, vol: float) -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=ts,
        open=open_px,
        high=max(open_px, close_px) * 1.002,
        low=min(open_px, close_px) * 0.998,
        close=close_px,
        volume=vol,
    )


def test_scanner_same_session_overnight_rvol_without_rth_dilution(tmp_path: Path) -> None:
    from datetime import timedelta

    # 2026-10-15 20:30 ET == 2026-10-16 00:30 UTC (EDT = UTC-4, OVERNIGHT session).
    now_utc = datetime(2026, 10, 16, 0, 30, tzinfo=UTC)
    ah_end = datetime(2026, 10, 15, 23, 55, tzinfo=UTC)  # 19:55 ET AFTER_HOURS
    on_start = datetime(2026, 10, 16, 0, 10, tzinfo=UTC)  # 20:10 ET OVERNIGHT

    # Heavy prior RTH/AH bars (500k vol) would crush RVOL to ~0.01 if mixed with BOATS bars.
    ah_heavy = [
        _make_ts_bar(ah_end - timedelta(minutes=10 - i), 52.0, 52.20, 500_000.0) for i in range(10)
    ]
    # 20 same-session OVERNIGHT bars: 15 quiet (1k vol) + 5 surging (3k vol) -> rvol = 3.0.
    soxs_on = [
        _make_ts_bar(
            on_start + timedelta(minutes=i),
            52.20 + i * 0.25,
            52.45 + i * 0.25,
            3_000.0 if i >= 15 else 1_000.0,
        )
        for i in range(20)
    ]
    # Early-session (< 6 same-session bars): 3 OVERNIGHT bars (1k, 1k -> 3k) -> rvol = 3.0.
    sqqq_on = [
        _make_ts_bar(now_utc - timedelta(minutes=3), 18.0, 18.30, 1_000.0),
        _make_ts_bar(now_utc - timedelta(minutes=2), 18.30, 18.60, 1_000.0),
        _make_ts_bar(now_utc - timedelta(minutes=1), 18.60, 19.00, 3_000.0),
    ]
    # Single opening OVERNIGHT bar (0 prior same-session bars): clean early fallback = 1.5.
    tqqq_on = [_make_ts_bar(now_utc - timedelta(minutes=1), 70.0, 74.0, 2_000.0)]

    class _OvernightBarsData:
        def get_stock_bars(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "SOXS": [*ah_heavy, *soxs_on],
                    "SQQQ": [*ah_heavy[:2], *sqqq_on],
                    "TQQQ": [*ah_heavy[:2], *tqqq_on],
                }
            )

    scanner = ExtremeMoverScanner(
        screener_client=None,
        data_client=_OvernightBarsData(),
        trading_client=_FakeTradingClient(shortable=True, etb=True),
    )
    doc = scanner.scan(top_k=10, now_utc=now_utc)
    by_sym = {e["symbol"]: e for e in doc["entries"]}

    assert by_sym["SOXS"]["session"] == "OVERNIGHT"
    assert by_sym["SOXS"]["rvol_normalized"] == pytest.approx(3.0)
    assert by_sym["SOXS"]["extreme_mover_eligible"] is True
    assert by_sym["SOXS"]["prior_close"] == pytest.approx(52.20)
    assert by_sym["SOXS"]["annual_borrow_rate"] == pytest.approx(
        INVERSE_ETF_ANNUAL_EXPENSE_RATES["SOXS"]
    )

    assert by_sym["SQQQ"]["rvol_normalized"] == pytest.approx(3.0)
    assert by_sym["SQQQ"]["annual_borrow_rate"] == pytest.approx(
        INVERSE_ETF_ANNUAL_EXPENSE_RATES["SQQQ"]
    )
    assert by_sym["SQQQ"]["prior_close"] == pytest.approx(52.20)

    assert by_sym["TQQQ"]["rvol_normalized"] == pytest.approx(1.5)
    assert by_sym["TQQQ"]["annual_borrow_rate"] == 0.0


def test_overnight_universe_seeding_discovers_boats_leaders() -> None:
    from datetime import timedelta

    # 2026-11-26 20:30 ET == 2026-11-27 01:30 UTC (EST = UTC-5, OVERNIGHT BOATS open).
    now_utc = datetime(2026, 11, 27, 1, 30, tzinfo=UTC)
    on_start = now_utc - timedelta(minutes=20)
    requested_symbols: list[str] = []

    class _EmptyScreener:
        def get_most_actives(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(most_actives=[])

        def get_market_movers(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(gainers=[], losers=[])

    class _SeedData:
        def get_stock_bars(self, req: object) -> SimpleNamespace:
            requested_symbols.extend(getattr(req, "symbol_or_symbols", ()))
            nvda_bars = [
                _make_ts_bar(
                    on_start + timedelta(minutes=i),
                    130.0 + i * 0.35,
                    130.35 + i * 0.35,
                    10_000.0 if i >= 15 else 2_500.0,
                )
                for i in range(20)
            ]
            meta_flat = [
                _make_ts_bar(on_start + timedelta(minutes=i), 580.0, 580.1, 100.0)
                for i in range(20)
            ]
            return SimpleNamespace(data={"NVDA": nvda_bars, "META": meta_flat})

    scanner = ExtremeMoverScanner(
        screener_client=_EmptyScreener(),
        data_client=_SeedData(),
        trading_client=_FakeTradingClient(shortable=True, etb=True),
    )
    doc = scanner.scan(top_k=15, now_utc=now_utc)
    assert doc["feed_sources"] == ["alpaca_boats"]
    assert set(OVERNIGHT_SEED_SYMBOLS) <= set(requested_symbols)
    assert set(DEFAULT_PIN_SYMBOLS) <= set(requested_symbols)

    by_sym = {e["symbol"]: e for e in doc["entries"]}
    assert "NVDA" in by_sym
    assert by_sym["NVDA"]["extreme_mover_eligible"] is True
    assert "overnight_seed" in by_sym["NVDA"]["screen_buckets"]
    assert by_sym["NVDA"]["prior_close"] == pytest.approx(130.0)
    assert by_sym["NVDA"]["annual_borrow_rate"] == 0.0
    assert "META" not in by_sym
    assert set(DEFAULT_PIN_SYMBOLS) <= set(by_sym)


def test_inverse_hedge_soxs_sqqq_carries_prior_close_and_borrow_rate_through_execution(
    tmp_path: Path, monkeypatch
) -> None:
    # 1. Verify coordinator dispatch_movers admits INVERSE_HEDGE_SQQQ with scanner fields on PA3.
    coord = PaperFarmCoordinator(
        tmp_path / "coord", alpaca_clients={a: _FakeTradingClient() for a in ALPACA_LANE_PROFILES}
    )
    routed = coord.dispatch_movers(
        [
            {
                "symbol": "SQQQ",
                "last_price": 22.50,
                "prior_close": 21.00,
                "annual_borrow_rate": INVERSE_ETF_ANNUAL_EXPENSE_RATES["SQQQ"],
                "natr_pct": 0.025,
                "extreme_mover_eligible": True,
                "overnight_tradable": True,
            }
        ],
        macro_regime=-1,
        session="OVERNIGHT",
    )
    assert len(routed) == 1
    assert routed[0]["symbol"] == "SQQQ"
    assert routed[0]["account"] == "ALPACA_PA3"

    # 2. Verify full execution.run_execution path preserves SOXS prior_close & annual_borrow_rate.
    exec_dir = tmp_path / "exec_inv"
    exec_dir.mkdir()
    stop = threading.Event()

    class _OvernightBroker(_FakeTradingClient):
        def __init__(self) -> None:
            super().__init__()
            self.identity = uuid4()

        def get_account(self):
            return SimpleNamespace(
                id=self.identity,
                status=SimpleNamespace(value="ACTIVE"),
                account_blocked=False,
                trading_blocked=False,
                trade_suspended_by_user=False,
                equity="100000",
                last_equity="100000",
            )

        def get_all_positions(self):
            return []

        def get_orders(self, request):
            return []

        def get_asset(self, symbol):
            return SimpleNamespace(tradable=True, attributes=["overnight_tradable"])

        def get_order_by_id(self, order_id, *args):
            return SimpleNamespace(
                id=order_id,
                legs=[],
                status=SimpleNamespace(value="new"),
                filled_qty="0",
                filled_avg_price=None,
            )

    brokers = {acct: _OvernightBroker() for acct in ALPACA_LANE_PROFILES}
    farm = PaperFarmCoordinator(exec_dir, alpaca_clients=brokers)

    class _Quotes:
        def get_stock_latest_quote(self, request):
            return {
                "SOXS": SimpleNamespace(
                    bid_price=57.10, ask_price=57.18, timestamp=datetime.now(UTC)
                )
            }

    class _Stream:
        def __init__(self, *args, feed) -> None:
            self.feed = feed
            self.subscribed = threading.Event()

        def subscribe_trades(self, callback, *symbols) -> None:
            self.callback = callback
            if "SOXS" in symbols:
                self.subscribed.set()

        def run(self) -> None:
            assert self.subscribed.wait(15)
            if self.feed.value == "boats":
                asyncio.run(
                    self.callback(
                        SimpleNamespace(symbol="SOXS", price=57.15, timestamp=datetime.now(UTC))
                    )
                )
                stop.set()
            assert stop.wait(15)

        def stop(self) -> None:
            stop.set()

    now = datetime.now(UTC).isoformat()
    (exec_dir / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now,
                "macro_regime": {"regime": -1},
                "entries": [
                    {
                        "symbol": "SOXS",
                        "extreme_mover_eligible": True,
                        "side": "BUY",
                        "session": "OVERNIGHT",
                        "bar_close_utc": now,
                        "natr_pct": 0.024,
                        "rvol_normalized": 3.5,
                        "last_price": 57.15,
                        "prior_close": 52.20,
                        "annual_borrow_rate": INVERSE_ETF_ANNUAL_EXPENSE_RATES["SOXS"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "resolve_alpaca_env_creds", lambda: ("", ""))
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _: farm)
    monkeypatch.setattr(execution, "StockDataStream", _Stream)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner, "from_env", lambda: SimpleNamespace(data_client=_Quotes())
    )
    monkeypatch.setattr(execution.engine, "classify_us_equity_session_et", lambda _: "OVERNIGHT")
    execution.run_execution(exec_dir, stop=stop)

    assert len(brokers["ALPACA_PA3"].orders) == 1
    assert len(brokers["ALPACA_PA1"].orders) == 0
    assert len(brokers["ALPACA_PA2"].orders) == 0
    with duckdb.connect(str(farm.lanes["ALPACA_PA3"].duckdb_path), read_only=True) as db:
        row = db.execute(
            "SELECT origin, annual_rate, prior_close FROM paper_entry_decisions"
        ).fetchone()
    assert row == ("INVERSE_HEDGE_SOXS", Decimal("0.0099000000"), Decimal("52.200000"))


def test_scanner_unusual_volume_buildup_absorption_and_causal_peak_confirmed() -> None:
    from datetime import timedelta

    # 2026-10-15 11:00 ET == 2026-10-15 15:00 UTC (RTH).
    now_utc = datetime(2026, 10, 15, 15, 0, tzinfo=UTC)
    start_ts = now_utc - timedelta(minutes=20)

    # 18 quiet base bars around $100 (low range, vol=100k), then bar 18 spikes to $106
    # (>= 2*NATR above ~$100 VWAP), then bar 19 closes red below prior low with ramping volume.
    peak_bars = [
        SimpleNamespace(
            timestamp=start_ts + timedelta(minutes=i),
            open=100.0,
            high=100.40,
            low=99.60,
            close=100.10,
            volume=100_000.0,
        )
        for i in range(17)
    ]
    peak_bars.extend(
        [
            SimpleNamespace(
                timestamp=start_ts + timedelta(minutes=17),
                open=101.0,
                high=103.0,
                low=100.8,
                close=102.8,
                volume=220_000.0,
            ),
            SimpleNamespace(
                timestamp=start_ts + timedelta(minutes=18),
                open=102.8,
                high=107.0,
                low=104.5,
                close=106.5,
                volume=320_000.0,
            ),
            SimpleNamespace(
                timestamp=start_ts + timedelta(minutes=19),
                open=106.2,
                high=106.4,
                low=103.5,
                close=103.8,
                volume=450_000.0,
            ),
        ]
    )

    # Absorption symbol: high RVOL + tight final bar range -> high hidden_volume_ratio, no peak.
    abs_bars = [
        SimpleNamespace(
            timestamp=start_ts + timedelta(minutes=i),
            open=50.0,
            high=50.8,
            low=49.2,
            close=50.2,
            volume=100_000.0,
        )
        for i in range(17)
    ]
    abs_bars.extend(
        [
            SimpleNamespace(
                timestamp=start_ts + timedelta(minutes=17),
                open=51.0,
                high=51.8,
                low=50.8,
                close=51.6,
                volume=200_000.0,
            ),
            SimpleNamespace(
                timestamp=start_ts + timedelta(minutes=18),
                open=51.6,
                high=52.2,
                low=51.5,
                close=52.1,
                volume=260_000.0,
            ),
            SimpleNamespace(
                timestamp=start_ts + timedelta(minutes=19),
                open=52.1,
                high=52.25,
                low=52.05,
                close=52.20,
                volume=340_000.0,
            ),
        ]
    )

    class _UvScreener:
        def get_most_actives(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(most_actives=[])

        def get_market_movers(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(
                gainers=[
                    SimpleNamespace(
                        symbol="SMCI", price=103.8, volume=5_000_000, percent_change=6.5
                    ),
                    SimpleNamespace(
                        symbol="NVDA", price=52.2, volume=5_000_000, percent_change=4.4
                    ),
                ],
                losers=[],
            )

    class _UvData:
        def get_stock_bars(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(data={"SMCI": peak_bars, "NVDA": abs_bars})

        def get_stock_trades(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "SMCI": [SimpleNamespace(symbol="SMCI", size=450, exchange="D")],
                    "NVDA": [
                        SimpleNamespace(symbol="NVDA", size=500, exchange="D"),
                        SimpleNamespace(symbol="NVDA", size=500, exchange="Q"),
                    ],
                }
            )

    scanner = ExtremeMoverScanner(
        screener_client=_UvScreener(),
        data_client=_UvData(),
        trading_client=_FakeTradingClient(shortable=True, etb=True),
    )
    doc = scanner.scan(top_k=10, now_utc=now_utc)
    by_sym = {e["symbol"]: e for e in doc["entries"]}

    smci = by_sym["SMCI"]
    assert smci["vol_buildup_ratio"] >= 3.0
    assert smci["vol_ramp_bars"] == 3
    assert smci["unusual_volume_buildup"] is True
    assert smci["hidden_volume_ratio"] > 0.0
    assert smci["peak_confirmed"] is True

    nvda = by_sym["NVDA"]
    assert nvda["vol_buildup_ratio"] >= 2.5
    assert nvda["vol_ramp_bars"] == 3
    assert nvda["unusual_volume_buildup"] is True
    assert nvda["darkpool_trf_ratio"] == 0.5
    # Tight final bar range (0.20) vs ATR (~1.5) -> strong absorption proxy (> rvol).
    assert nvda["hidden_volume_ratio"] > nvda["rvol_normalized"]
    assert nvda["peak_confirmed"] is False


def test_live_lane_preset_bundle_roundtrip_with_runner_and_tv_tickets(tmp_path: Path) -> None:
    from tier0_sota.tv.tickets import refresh_tickets_from_paths

    assert runner_build_bundle is build_live_lane_preset_bundle
    now_utc = datetime(2026, 10, 15, 15, 0, 20, tzinfo=UTC)
    bar_close_iso = datetime(2026, 10, 15, 15, 0, 0, tzinfo=UTC).isoformat()

    scan_doc = {
        "schema_version": "tier0.whole-market-movers.v2",
        "generated_at_utc": now_utc.isoformat(),
        "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
        "macro_regime": {"regime": 1, "regime_label": "BULL"},
        "pin_symbols": list(DEFAULT_PIN_SYMBOLS),
        "ranked_count": 3,
        "entries": [
            {
                "rank": 1,
                "symbol": "NVDA",
                "score": 48.0,
                "last_price": 135.0,
                "prior_close": 128.0,
                "annual_borrow_rate": 0.0,
                "session_move_pct": 5.47,
                "rvol_normalized": 3.8,
                "vol_buildup_ratio": 2.4,
                "vol_ramp_bars": 3,
                "unusual_volume_buildup": True,
                "hidden_volume_ratio": 4.2,
                "peak_confirmed": False,
                "dollar_vol": 25_000_000.0,
                "darkpool_trf_ratio": 0.44,
                "side": "BUY",
                "session": "RTH",
                "bar_close_utc": bar_close_iso,
                "trailing_schedule": "wide_natr_adaptive",
                "natr_pct": 0.035,
                "initial_stop_px": 132.30,
                "extreme_mover_eligible": True,
                "pinned": False,
                "shortable": True,
                "easy_to_borrow": True,
            },
            {
                "rank": 2,
                "symbol": "AMD",
                "score": 32.0,
                "last_price": 150.0,
                "prior_close": 144.0,
                "annual_borrow_rate": 0.0,
                "session_move_pct": 4.17,
                "rvol_normalized": 1.9,
                "vol_buildup_ratio": 1.4,
                "vol_ramp_bars": 1,
                "unusual_volume_buildup": False,
                "hidden_volume_ratio": 1.2,
                "peak_confirmed": False,
                "dollar_vol": 15_000_000.0,
                "darkpool_trf_ratio": 0.22,
                "side": "BUY",
                "session": "RTH",
                "bar_close_utc": bar_close_iso,
                "trailing_schedule": "baseline",
                "natr_pct": 0.022,
                "initial_stop_px": 147.00,
                "extreme_mover_eligible": True,
                "pinned": False,
                "shortable": True,
                "easy_to_borrow": True,
            },
            {
                "rank": 3,
                "symbol": "SOXS",
                "score": 21.0,
                "last_price": 24.0,
                "prior_close": 24.2,
                "annual_borrow_rate": INVERSE_ETF_ANNUAL_EXPENSE_RATES["SOXS"],
                "session_move_pct": -0.83,
                "rvol_normalized": 0.7,
                "vol_buildup_ratio": 0.9,
                "vol_ramp_bars": 0,
                "unusual_volume_buildup": False,
                "hidden_volume_ratio": 0.5,
                "peak_confirmed": False,
                "dollar_vol": 2_000_000.0,
                "darkpool_trf_ratio": 0.15,
                "side": "BUY",
                "session": "RTH",
                "bar_close_utc": bar_close_iso,
                "trailing_schedule": "tight_near_lossless",
                "natr_pct": 0.012,
                "initial_stop_px": 23.64,
                "extreme_mover_eligible": False,
                "pinned": True,
                "shortable": False,
                "easy_to_borrow": False,
            },
        ],
    }

    movers_path = tmp_path / "live_whole_market_movers_manifest.json"
    bundle_path = tmp_path / "live_lane_order_preset_bundle.json"
    movers_path.write_text(json.dumps(scan_doc), encoding="utf-8")

    bundle = build_live_lane_preset_bundle(scan_doc, output_path=bundle_path)
    assert bundle_path.is_file()
    assert bundle["schema_version"] == "tier0.live-lane-preset-bundle.v1"
    assert bundle["paper_only"] is True
    assert bundle["live_execution_enabled"] is False

    rows = {r["symbol"]: r for r in bundle["top_potential_symbols"]}
    assert rows["NVDA"]["lane_account"] == "ALPACA_PA1"
    assert rows["NVDA"]["lane_id"] == "L1_ORB_BURST"
    assert rows["NVDA"]["entry_eligible"] is True
    assert rows["NVDA"]["entry_hold_reason"] is None
    assert rows["NVDA"]["order_mode"] == "STOP_MARKET"
    assert rows["NVDA"]["unusual_volume_buildup"] is True
    assert rows["NVDA"]["darkpool_trf_ratio"] == 0.44

    assert rows["AMD"]["lane_account"] == "ALPACA_PA2"
    assert rows["AMD"]["lane_id"] == "L2_STRUCTURAL_TREND"
    assert rows["AMD"]["entry_eligible"] is True

    assert rows["SOXS"]["lane_account"] == "ALPACA_PA3"
    assert rows["SOXS"]["entry_eligible"] is False
    assert rows["SOXS"]["entry_hold_reason"] == "LOW_MOVE_NOT_EXTREME"

    # Fail-closed when TCA calibration blocks new entries.
    held = build_live_lane_preset_bundle(scan_doc, tca_allows_entry=False)
    assert all(
        r["entry_eligible"] is False and r["entry_hold_reason"] == "TCA_RECALIBRATION_REQUIRED"
        for r in held["top_potential_symbols"]
    )

    # Round-trip into TV Half-Auto tickets confirms READY_HALF_AUTO wiring.
    tv_doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        bundle_path=bundle_path,
        out_dir=tmp_path / "tv_sota_pack",
        venue="ALPACA",
        now_utc=now_utc,
    )
    tv_by_sym = {t["symbol"]: t for t in tv_doc["tickets"]}
    assert tv_by_sym["NVDA"]["action_gate"] == "READY_HALF_AUTO"
    assert tv_by_sym["AMD"]["action_gate"] == "READY_HALF_AUTO"
    assert "SOXS" not in tv_by_sym


def test_runner_resolve_pin_symbols_upgrades_legacy_two_pin_override() -> None:
    assert _resolve_pin_symbols("SOXL,SOXS") == DEFAULT_PIN_SYMBOLS
    assert _resolve_pin_symbols(" soxs , soxl ") == DEFAULT_PIN_SYMBOLS
    assert _resolve_pin_symbols("") == DEFAULT_PIN_SYMBOLS
    assert _resolve_pin_symbols(" , ") == DEFAULT_PIN_SYMBOLS
    assert _resolve_pin_symbols("SOXL,SOXS,TQQQ,SQQQ") == DEFAULT_PIN_SYMBOLS
    assert _resolve_pin_symbols("nvda, amd, NVDA") == ("NVDA", "AMD")
    assert _resolve_pin_symbols("SOXL,NVDA") == ("SOXL", "NVDA")


def _stage_sample_tv_preset(
    manifest_dir: Path,
    *,
    symbol: str,
    side: str,
    session: str,
    trigger_price: float,
    stop_price: float,
    natr_pct: float,
    trailing_schedule: str,
    order_type: str,
    limit_collar_bps: float,
    rvol: float | None = None,
    venue: str = "ALPACA",
    shortable: bool | None = None,
    easy_to_borrow: bool | None = None,
) -> dict[str, object]:
    from tier0_sota.tv.bridge import stage_tv_alert_preset

    payload: dict[str, object] = {
        "schema_version": "tier0.tv-alert.v1",
        "symbol": symbol,
        "ts_utc_ms": 1795746660000,
        "bar_confirmed": True,
        "side": side,
        "qty": 1,
        "venue": venue,
        "session": session,
        "order_type": order_type,
        "trigger_price": trigger_price,
        "stop_price": stop_price,
        "limit_collar_bps": limit_collar_bps,
        "trailing_schedule": trailing_schedule,
        "natr_pct": natr_pct,
        "auto_reverse": True,
        "auto_reentry": True,
        "reentry_cooldown_secs": 60,
        "strategy_id": "SignalExecutionStrategy",
        "semi_auto_confirmed": False,
        "data_source": "tradingview_pine_v6",
    }
    if rvol is not None:
        payload["rvol"] = rvol
    if shortable is not None:
        payload["shortable"] = shortable
    if easy_to_borrow is not None:
        payload["easy_to_borrow"] = easy_to_borrow
    return stage_tv_alert_preset(
        payload,
        staged_path=manifest_dir / "tv_sota_pack" / "staged_tv_presets.json",
        watchlist_path=manifest_dir / "missing_wl.json",
        venue=venue,
        live_intake=False,
    )


class _TvConfirmBroker(_FakeTradingClient):
    def __init__(self, *, positions: list[SimpleNamespace] | None = None) -> None:
        super().__init__(shortable=True, etb=True)
        self.identity = uuid4()
        self.positions = positions or []

    def get_account(self) -> SimpleNamespace:
        return SimpleNamespace(
            id=self.identity,
            status=SimpleNamespace(value="ACTIVE"),
            account_blocked=False,
            trading_blocked=False,
            trade_suspended_by_user=False,
            equity="100000",
            last_equity="100000",
        )

    def get_all_positions(self) -> list[SimpleNamespace]:
        return list(self.positions)

    def get_orders(self, _req: object) -> list[SimpleNamespace]:
        return []

    def get_clock(self) -> SimpleNamespace:
        return SimpleNamespace(is_open=True)

    def get_asset(self, symbol: str) -> SimpleNamespace:
        return SimpleNamespace(
            symbol=symbol,
            tradable=True,
            shortable=self.shortable,
            easy_to_borrow=self.etb,
            attributes=["overnight_tradable"],
        )

    def get_order_by_client_id(self, cid: str) -> SimpleNamespace:
        for idx, req in enumerate(self.orders, 1):
            if getattr(req, "client_order_id", None) == cid:
                legs = []
                sl = getattr(req, "stop_loss", None)
                if sl is not None:
                    legs.append(
                        SimpleNamespace(
                            id=f"child-{idx}",
                            status=SimpleNamespace(value="held"),
                            replaced_by=None,
                            symbol=getattr(req, "symbol", ""),
                            side=SimpleNamespace(value="sell"),
                            stop_price=str(sl.stop_price),
                        )
                    )
                return SimpleNamespace(
                    id=f"ord-{idx}",
                    legs=legs,
                    status=SimpleNamespace(value="new"),
                    filled_qty="0",
                    filled_avg_price=None,
                    submitted_at=datetime.now(UTC),
                )
        raise APIError(
            "missing", http_error=SimpleNamespace(response=SimpleNamespace(status_code=404))
        )

    def get_order_by_id(self, order_id: str, *args: object) -> SimpleNamespace:
        idx = int(order_id.removeprefix("ord-")) - 1
        req = self.orders[idx]
        return self.get_order_by_client_id(str(getattr(req, "client_order_id", "")))


def test_confirmed_tv_preset_paper_multi_situation_24x5_and_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _setup_env(
        base_dir: Path,
        *,
        quote_map: dict[str, tuple[float, float]],
        now_dt: datetime,
        positions_by_acct: dict[str, list[SimpleNamespace]] | None = None,
    ) -> tuple[PaperFarmCoordinator, dict[str, _TvConfirmBroker]]:
        brokers = {
            acct: _TvConfirmBroker(positions=(positions_by_acct or {}).get(acct))
            for acct in ALPACA_LANE_PROFILES
        }
        farm = PaperFarmCoordinator(base_dir, alpaca_clients=brokers)

        class _LiveQuotes:
            def get_stock_latest_quote(self, req: object) -> dict[str, SimpleNamespace]:
                sym = str(getattr(req, "symbol_or_symbols", ""))
                bid_px, ask_px = quote_map[sym]
                return {sym: SimpleNamespace(bid_price=bid_px, ask_price=ask_px, timestamp=now_dt)}

        monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
        monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _dir: farm)
        monkeypatch.setattr(
            execution.ExtremeMoverScanner,
            "from_env",
            lambda *_a, **_kw: SimpleNamespace(data_client=_LiveQuotes()),
        )
        return farm, brokers

    # (1) RTH OTO STOP_MARKET + progressive 4-tier R-lock + Guard adoption -> PA1 (L1_ORB_BURST).
    rth_dir = tmp_path / "rth"
    rth_now = datetime(2026, 10, 15, 15, 0, tzinfo=UTC)  # 11:00 ET RTH
    nvda_staged = _stage_sample_tv_preset(
        rth_dir,
        symbol="NVDA",
        side="BUY",
        session="RTH",
        trigger_price=100.0,
        stop_price=98.0,
        natr_pct=0.035,
        trailing_schedule="wide_natr_adaptive",
        order_type="STOP_MARKET",
        limit_collar_bps=0.0,
        rvol=4.2,
    )
    rth_farm, rth_brokers = _setup_env(
        rth_dir, quote_map={"NVDA": (100.00, 100.02)}, now_dt=rth_now
    )
    rth_receipt = execution.submit_confirmed_tv_preset_paper(
        rth_dir, alert_id=str(nvda_staged["alert_id"]), qty=2, dry_run=False, now_utc=rth_now
    )
    assert rth_receipt["ok"] is True
    assert rth_receipt["dry_run"] is False
    assert rth_receipt["submitted"] is True
    assert rth_receipt["paper_only"] is True
    assert rth_receipt["live_execution_enabled"] is False
    assert rth_receipt["lane_account"] == "ALPACA_PA1"
    assert rth_receipt["session"] == "RTH"
    assert rth_receipt["order_type"] == "STOP_MARKET"
    assert rth_receipt["protective_stop_mode"] == "NATIVE_OTO_STOP"
    rl = rth_receipt["r_lock_levels"]
    assert (
        rth_receipt["stop_price"]
        < rth_receipt["entry_limit"]
        < rl["be_plus_fee"]
        < rl["lock_at_1r"]
        < rl["lock_at_2r"]
        < rl["lock_at_3r"]
    )
    assert len(rth_brokers["ALPACA_PA1"].orders) == 1
    oto_req = rth_brokers["ALPACA_PA1"].orders[0]
    assert isinstance(oto_req, LimitOrderRequest)
    assert oto_req.order_class == OrderClass.OTO
    assert oto_req.stop_loss is not None and oto_req.qty == 2
    assert oto_req.extended_hours is False
    # Staged JSON must NEVER be mutated to semi_auto_confirmed=True.
    staged_doc = json.loads(
        (rth_dir / "tv_sota_pack" / "staged_tv_presets.json").read_text(encoding="utf-8")
    )
    assert staged_doc["semi_auto_confirmed"] is False
    assert all(p["semi_auto_confirmed"] is False for p in staged_doc["presets"])
    # Execution supervisor immediately adopts the handed-off guard from DuckDB.
    adopted = execution.restore_guards(rth_farm, {a: {} for a in ALPACA_LANE_PROFILES})
    assert "NVDA" in adopted and adopted["NVDA"].client_id == rth_receipt["client_order_id"]

    # (2) PRE_MARKET & AFTER_HOURS 35bp STOP_LIMIT supervised exit -> PA2 (L2_STRUCTURAL_TREND).
    for label, ext_now, sess_name in (
        ("pre", datetime(2026, 10, 15, 12, 30, tzinfo=UTC), "PRE_MARKET"),  # 08:30 ET
        ("ah", datetime(2026, 10, 15, 21, 30, tzinfo=UTC), "AFTER_HOURS"),  # 17:30 ET
    ):
        ext_dir = tmp_path / label
        amd_staged = _stage_sample_tv_preset(
            ext_dir,
            symbol="AMD",
            side="BUY",
            session=sess_name,
            trigger_price=150.0,
            stop_price=147.75,
            natr_pct=0.022,
            trailing_schedule="baseline",
            order_type="STOP_LIMIT",
            limit_collar_bps=35.0,
            rvol=2.0,
        )
        ext_farm, ext_brokers = _setup_env(
            ext_dir, quote_map={"AMD": (150.00, 150.10)}, now_dt=ext_now
        )
        ext_res = execution.submit_confirmed_tv_preset_paper(
            ext_dir, alert_id=str(amd_staged["alert_id"]), qty=1, dry_run=False, now_utc=ext_now
        )
        assert ext_res["lane_account"] == "ALPACA_PA2"
        assert ext_res["session"] == sess_name
        assert ext_res["order_type"] == "STOP_LIMIT"
        assert ext_res["collar_bps"] == 35.0
        assert ext_res["protective_stop_mode"] == "COLLARED_STOP_LIMIT_SUPERVISED"
        assert ext_res["stop_limit_price"] < ext_res["stop_price"] < ext_res["entry_limit"]
        ext_req = ext_brokers["ALPACA_PA2"].orders[0]
        assert isinstance(ext_req, LimitOrderRequest)
        assert ext_req.extended_hours is True and ext_req.order_class is None
        with duckdb.connect(str(ext_farm.lanes["ALPACA_PA2"].duckdb_path), read_only=True) as db:
            sup_n = db.execute("SELECT COUNT(*) FROM paper_supervised_stops").fetchone()
        assert sup_n == (1,)

    # (3) OVERNIGHT inverse hedge (SOXS -> PA3 50bp <= 1.00% even with wide NATR/high RVOL).
    on_dir = tmp_path / "overnight"
    on_now = datetime(2026, 11, 27, 2, 30, tzinfo=UTC)  # 21:30 ET OVERNIGHT
    soxs_staged = _stage_sample_tv_preset(
        on_dir,
        symbol="SOXS",
        side="BUY",
        session="OVERNIGHT",
        trigger_price=55.0,
        stop_price=54.45,
        natr_pct=0.035,
        trailing_schedule="wide_natr_adaptive",
        order_type="STOP_LIMIT",
        limit_collar_bps=50.0,
        rvol=4.5,
    )
    on_farm, on_brokers = _setup_env(on_dir, quote_map={"SOXS": (55.00, 55.08)}, now_dt=on_now)
    on_res = execution.submit_confirmed_tv_preset_paper(
        on_dir, alert_id=str(soxs_staged["alert_id"]), qty=1, dry_run=False, now_utc=on_now
    )
    assert on_res["lane_account"] == "ALPACA_PA3"
    assert on_res["origin"] == "INVERSE_HEDGE_SOXS"
    assert on_res["session"] == "OVERNIGHT"
    assert on_res["order_type"] == "STOP_LIMIT"
    assert on_res["collar_bps"] == 50.0
    loss_frac = (
        Decimal(str(on_res["entry_limit"])) - Decimal(str(on_res["stop_price"]))
    ) / Decimal(str(on_res["entry_limit"]))
    assert Decimal("0") < loss_frac <= Decimal("0.0100")
    assert len(on_brokers["ALPACA_PA3"].orders) == 1
    with duckdb.connect(str(on_farm.lanes["ALPACA_PA3"].duckdb_path), read_only=True) as db:
        row = db.execute(
            "SELECT origin, annual_rate, prior_close FROM paper_entry_decisions"
        ).fetchone()
    assert row == ("INVERSE_HEDGE_SOXS", Decimal("0.0099000000"), Decimal("55.000000"))

    # (4) OVERNIGHT Alpaca SELL fail-closed refusal + CLOSED session refusal.
    sell_dir = tmp_path / "overnight_sell"
    sell_staged = _stage_sample_tv_preset(
        sell_dir,
        symbol="SMCI",
        side="SELL",
        session="OVERNIGHT",
        trigger_price=40.0,
        stop_price=40.40,
        natr_pct=0.015,
        trailing_schedule="tight_near_lossless",
        order_type="STOP_LIMIT",
        limit_collar_bps=50.0,
        venue="IBKR",
        shortable=True,
        easy_to_borrow=True,
    )
    _, sell_brokers = _setup_env(sell_dir, quote_map={"SMCI": (39.95, 40.05)}, now_dt=on_now)
    with pytest.raises(ValueError, match="ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ"):
        execution.submit_confirmed_tv_preset_paper(
            sell_dir, alert_id=str(sell_staged["alert_id"]), now_utc=on_now
        )
    assert all(len(b.orders) == 0 for b in sell_brokers.values())
    closed_now = datetime(2026, 11, 27, 0, 59, tzinfo=UTC)  # 19:59 ET Thanksgiving CLOSED
    with pytest.raises(ValueError, match="CLOSED"):
        execution.submit_confirmed_tv_preset_paper(
            on_dir, alert_id=str(soxs_staged["alert_id"]), now_utc=closed_now
        )

    # (5) CrossLaneSymbolLock overlap refusal when symbol already held on another lane.
    lock_dir = tmp_path / "lock_conflict"
    lock_staged = _stage_sample_tv_preset(
        lock_dir,
        symbol="NVDA",
        side="BUY",
        session="RTH",
        trigger_price=100.0,
        stop_price=98.0,
        natr_pct=0.035,
        trailing_schedule="wide_natr_adaptive",
        order_type="STOP_MARKET",
        limit_collar_bps=0.0,
        rvol=4.0,
    )
    _, lock_brokers = _setup_env(
        lock_dir,
        quote_map={"NVDA": (100.00, 100.02)},
        now_dt=rth_now,
        positions_by_acct={"ALPACA_PA2": [SimpleNamespace(symbol="NVDA", qty="5")]},
    )
    with pytest.raises(PermissionError, match="CrossLaneSymbolLock"):
        execution.submit_confirmed_tv_preset_paper(
            lock_dir, alert_id=str(lock_staged["alert_id"]), now_utc=rth_now
        )
    assert all(len(b.orders) == 0 for b in lock_brokers.values())

    # (6) dry_run=True zero-submit & zero-DuckDB-write proof (SQQQ -> PA3).
    dry_dir = tmp_path / "dry_run"
    sqqq_staged = _stage_sample_tv_preset(
        dry_dir,
        symbol="SQQQ",
        side="BUY",
        session="OVERNIGHT",
        trigger_price=46.50,
        stop_price=46.04,
        natr_pct=0.002,
        trailing_schedule="tight_near_lossless",
        order_type="STOP_LIMIT",
        limit_collar_bps=50.0,
    )
    _, dry_brokers = _setup_env(dry_dir, quote_map={"SQQQ": (46.48, 46.52)}, now_dt=on_now)
    preview = execution.submit_confirmed_tv_preset_paper(
        dry_dir, alert_id=str(sqqq_staged["alert_id"]), qty=1, dry_run=True, now_utc=on_now
    )
    assert preview["ok"] is True
    assert preview["dry_run"] is True
    assert preview["would_submit"] is True
    assert preview["paper_only"] is True
    assert preview["live_execution_enabled"] is False
    assert preview["lane_account"] == "ALPACA_PA3"
    assert preview["collar_bps"] == 50.0
    assert all(len(b.orders) == 0 for b in dry_brokers.values())
    assert list(dry_dir.glob("*.duckdb")) == []

    # (7) LIVE_EXECUTION_ENABLED=true / api.alpaca.markets hard refusal even with dormant live key.
    from tier0_sota.live.runner import verify_paper_environment as real_verify

    monkeypatch.setattr(execution, "verify_paper_environment", real_verify)
    paper_env = {
        "ALPACA_PAPER": "true",
        "LIVE_EXECUTION_ENABLED": "false",
        "IBKR_HOST": "127.0.0.1",
        "IBKR_PORT": "4002",
        "APCA_API_BASE_URL": PAPER_ALPACA_ENDPOINT,
        "ALPACA_PA1_API_KEY": "PKTEST1",
        "ALPACA_PA1_SECRET": "sec1",
        "ALPACA_PA2_API_KEY": "PKTEST2",
        "ALPACA_PA2_SECRET": "sec2",
        "ALPACA_PA3_API_KEY": "PKTEST3",
        "ALPACA_PA3_SECRET": "sec3",
        "ALPACA_LIVE_API_KEY": "AK_DORMANT_ISOLATED",
    }
    for k, v in paper_env.items():
        monkeypatch.setenv(k, v)
    # Dormant live key alone does not break strict paper dry-run when paper guards hold.
    ok_preview = execution.submit_confirmed_tv_preset_paper(
        dry_dir, alert_id=str(sqqq_staged["alert_id"]), qty=1, dry_run=True, now_utc=on_now
    )
    assert ok_preview["ok"] is True and ok_preview["live_execution_enabled"] is False

    for bad_key, bad_val in (
        ("LIVE_EXECUTION_ENABLED", "true"),
        ("APCA_API_BASE_URL", "https://api.alpaca.markets"),
        ("ALPACA_PAPER", "false"),
    ):
        monkeypatch.setenv(bad_key, bad_val)
        with pytest.raises(PermissionError):
            execution.submit_confirmed_tv_preset_paper(
                dry_dir, alert_id=str(sqqq_staged["alert_id"]), qty=1, dry_run=True, now_utc=on_now
            )
        monkeypatch.setenv(bad_key, paper_env[bad_key])


def test_confirmed_tv_preset_quote_freshness_uses_post_fetch_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import timedelta

    staged = _stage_sample_tv_preset(
        tmp_path,
        symbol="SQQQ",
        side="BUY",
        session="OVERNIGHT",
        trigger_price=46.50,
        stop_price=46.04,
        natr_pct=0.002,
        trailing_schedule="tight_near_lossless",
        order_type="STOP_LIMIT",
        limit_collar_bps=50.0,
    )
    brokers = {acct: _TvConfirmBroker() for acct in ALPACA_LANE_PROFILES}
    farm = PaperFarmCoordinator(tmp_path, alpaca_clients=brokers)

    t0 = datetime(2026, 11, 27, 2, 30, 0, tzinfo=UTC)  # 21:30 ET OVERNIGHT
    clock_state = {"now": t0}

    class _AdvancingDatetime:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return clock_state["now"]

    quote_ts = {"ts": t0 + timedelta(seconds=1, milliseconds=500)}

    class _TickingQuotes:
        def get_stock_latest_quote(self, _req: object) -> dict[str, SimpleNamespace]:
            # Advance wall clock +2s (simulating refresh_accounts + quote HTTP RTT).
            clock_state["now"] = t0 + timedelta(seconds=2)
            return {
                "SQQQ": SimpleNamespace(bid_price=46.48, ask_price=46.52, timestamp=quote_ts["ts"])
            }

    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _dir: farm)
    monkeypatch.setattr(execution, "datetime", _AdvancingDatetime)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner,
        "from_env",
        lambda *_a, **_kw: SimpleNamespace(data_client=_TickingQuotes()),
    )

    # Quote ticked 1.5s AFTER initial entry clock (0.5s BEFORE post-fetch quote_now) -> PASS.
    res = execution.submit_confirmed_tv_preset_paper(
        tmp_path, alert_id=str(staged["alert_id"]), qty=1, dry_run=True
    )
    assert res["ok"] is True and res["dry_run"] is True and res["symbol"] == "SQQQ"

    # Genuinely stale (7s old vs quote_now) and future (+3s vs quote_now) still fail closed.
    for bad_ts in (t0 - timedelta(seconds=5), t0 + timedelta(seconds=5)):
        clock_state["now"] = t0
        quote_ts["ts"] = bad_ts
        with pytest.raises(ValueError, match="Stale or future live quote"):
            execution.submit_confirmed_tv_preset_paper(
                tmp_path, alert_id=str(staged["alert_id"]), qty=1, dry_run=True
            )


def test_scanner_trf_block_separation_exchange_and_overnight_core3_regime() -> None:
    from datetime import timedelta

    from tier0_sota.live.scanner import normalize_tv_exchange

    # 1. Institutional round-lot/block FINRA TRF separation vs sub-100 retail odd-lot prints.
    agg = aggregate_finra_trf_ratios(
        [
            SimpleNamespace(symbol="NVDA", size=15, price=130.0, exchange="D"),
            SimpleNamespace(symbol="NVDA", size=600, price=130.0, exchange="D"),
            SimpleNamespace(symbol="NVDA", size=385, price=130.0, exchange="Q"),
        ]
    )
    assert agg["NVDA"]["darkpool_trf_ratio"] == pytest.approx(0.615)
    assert agg["NVDA"]["trf_block_ratio"] == pytest.approx(0.6)
    assert agg["NVDA"]["darkpool_block_count"] == 1

    # 2. TV exchange prefix normalization (ARCA/NYSEARCA -> AMEX).
    assert normalize_tv_exchange("ARCA") == "AMEX"
    assert normalize_tv_exchange("NYSEARCA") == "AMEX"
    assert normalize_tv_exchange("NASDAQ") == "NASDAQ"
    assert normalize_tv_exchange("NYSE") == "NYSE"
    assert normalize_tv_exchange("") is None

    # 3. OVERNIGHT core-3 (SPY/QQQ/SOXX without RSP) causal regime + prior-SIP TRF carryover
    #    + UV/Darkpool buildup retention (fail-closed extreme_mover_eligible=False) + flat reject.
    now_utc = datetime(2026, 11, 27, 2, 30, tzinfo=UTC)  # 21:30 ET OVERNIGHT
    on_start = now_utc - timedelta(minutes=45)

    def _trend_bars(base_px: float, step: float, vol: float = 5_000.0) -> list[SimpleNamespace]:
        return [
            _make_ts_bar(
                on_start + timedelta(minutes=i),
                base_px + i * step,
                base_px + (i + 1) * step,
                vol,
            )
            for i in range(45)
        ]

    # Early UV/Darkpool buildup candidate: +2.0% (< 3.5% extreme gate) with high TRF & UV ramp.
    amd_buildup = [
        _make_ts_bar(
            on_start + timedelta(minutes=i),
            150.0 + i * 0.065,
            150.065 + i * 0.065,
            30_000.0 if i >= 40 else 10_000.0,
        )
        for i in range(45)
    ]
    # Extreme mover: +5.2% with strong RVOL.
    nvda_ext = [
        _make_ts_bar(
            on_start + timedelta(minutes=i),
            130.0 + i * 0.15,
            130.15 + i * 0.15,
            40_000.0 if i >= 40 else 10_000.0,
        )
        for i in range(45)
    ]
    meta_flat = [
        _make_ts_bar(on_start + timedelta(minutes=i), 580.0, 580.05, 50.0) for i in range(45)
    ]

    class _OvernightCore3Data:
        def get_stock_bars(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "SPY": _trend_bars(600.0, 0.12, 20_000.0),
                    "QQQ": _trend_bars(500.0, 0.12, 20_000.0),
                    "SOXX": _trend_bars(220.0, 0.06, 15_000.0),
                    # RSP intentionally omitted to prove core-3 overnight regime works without RSP.
                    "NVDA": nvda_ext,
                    "AMD": amd_buildup,
                    "META": meta_flat,
                }
            )

        def get_stock_trades(self, _req: object) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "NVDA": [
                        SimpleNamespace(symbol="NVDA", size=800, price=136.5, exchange="D"),
                        SimpleNamespace(symbol="NVDA", size=200, price=136.5, exchange="Q"),
                    ],
                    "AMD": [
                        SimpleNamespace(symbol="AMD", size=500, price=153.0, exchange="D"),
                        SimpleNamespace(symbol="AMD", size=500, price=153.0, exchange="Q"),
                    ],
                }
            )

    class _ExchangeTradingClient(_FakeTradingClient):
        def get_asset(self, symbol: str) -> SimpleNamespace:
            exch = "ARCA" if symbol in DEFAULT_PIN_SYMBOLS else "NASDAQ"
            return SimpleNamespace(
                symbol=symbol,
                shortable=True,
                easy_to_borrow=True,
                exchange=SimpleNamespace(value=exch),
            )

        def get_all_assets(self, _req: object) -> list[SimpleNamespace]:
            return [self.get_asset(s) for s in (*DEFAULT_PIN_SYMBOLS, "NVDA", "AMD", "META")]

    scanner = ExtremeMoverScanner(
        screener_client=None,
        data_client=_OvernightCore3Data(),
        trading_client=_ExchangeTradingClient(),
    )
    doc = scanner.scan(top_k=15, now_utc=now_utc)
    assert doc["feed_sources"] == ["alpaca_boats", "finra_trf_exchange_d"]
    assert doc["macro_regime"]["regime"] == 1
    assert doc["macro_regime"]["regime_label"] == "BULL"
    assert doc["macro_regime"]["regime_basis"] == "core_3_spy_qqq_soxx"
    assert doc["macro_regime"]["core_benchmarks_used"] == ["SPY", "QQQ", "SOXX"]
    assert doc["macro_regime"]["qqq_session_pct"] > 0.0
    assert doc["macro_regime"]["soxx_session_pct"] > 0.0

    by_sym = {e["symbol"]: e for e in doc["entries"]}
    assert "META" not in by_sym  # flat sleepy stock still rejected
    assert by_sym["NVDA"]["extreme_mover_eligible"] is True
    assert by_sym["NVDA"]["watch_tier"] == "EXTREME_READY"
    assert by_sym["NVDA"]["exchange"] == "NASDAQ"
    assert by_sym["NVDA"]["darkpool_trf_ratio"] == pytest.approx(0.8)
    assert by_sym["NVDA"]["trf_block_ratio"] == pytest.approx(0.8)
    assert by_sym["NVDA"]["darkpool_block_count"] == 1
    assert by_sym["NVDA"]["darkpool_trf_source"] == "prior_sip_trf_carryover"
    assert by_sym["NVDA"]["rvol"] == by_sym["NVDA"]["rvol_normalized"]
    assert by_sym["NVDA"]["change_pct"] == by_sym["NVDA"]["session_move_pct"]

    assert by_sym["AMD"]["extreme_mover_eligible"] is False
    assert by_sym["AMD"]["watch_tier"] == "UV_DARKPOOL_BUILDUP"
    assert by_sym["AMD"]["darkpool_trf_ratio"] == pytest.approx(0.5)
    assert by_sym["SOXL"]["exchange"] == "AMEX"

    bundle = build_live_lane_preset_bundle(doc)
    b_by_sym = {r["symbol"]: r for r in bundle["top_potential_symbols"]}
    assert b_by_sym["NVDA"]["entry_eligible"] is True
    assert b_by_sym["NVDA"]["exchange"] == "NASDAQ"
    assert b_by_sym["NVDA"]["trf_block_ratio"] == pytest.approx(0.8)
    assert b_by_sym["AMD"]["entry_eligible"] is False
    assert b_by_sym["AMD"]["entry_hold_reason"] == "UV_DARKPOOL_EARLY_BUILDUP"


def test_performance_grafana_breaker_and_tv_darkpool_prometheus_otel(tmp_path: Path) -> None:
    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)
    pa1 = coord.lanes["ALPACA_PA1"]
    pa1.write_status(equity=98_000.0, session="OVERNIGHT")
    with duckdb.connect(str(pa1.duckdb_path)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS paper_daily_fuse "
            "(day DATE PRIMARY KEY, account_id VARCHAR, tripped BOOLEAN)"
        )
        conn.execute("INSERT INTO paper_daily_fuse VALUES ('2026-11-26', 'acct-1', TRUE)")

    pack = tmp_path / "tv_sota_pack"
    pack.mkdir(parents=True, exist_ok=True)
    (tmp_path / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "feed_sources": ["alpaca_boats", "finra_trf_exchange_d"],
                "macro_regime": {"regime": -1, "regime_label": "CRASH"},
                "entries": [
                    {
                        "symbol": "SOXS",
                        "extreme_mover_eligible": True,
                        "unusual_volume_buildup": True,
                        "darkpool_trf_ratio": 0.48,
                    },
                    {
                        "symbol": "AMD",
                        "extreme_mover_eligible": False,
                        "unusual_volume_buildup": True,
                        "darkpool_trf_ratio": 0.41,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (pack / "live_half_auto_tickets.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.live-half-auto-tickets.v1",
                "active_session": "OVERNIGHT",
                "schema_validated_count": 1,
                "tickets": [{"symbol": "SOXS", "action_gate": "READY_HALF_AUTO_HEDGE"}],
                "standby_presets": [{"symbol": "AMD", "action_gate": "MONITOR_ONLY"}],
            }
        ),
        encoding="utf-8",
    )
    (pack / "tv_compounding_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-compounding-manifest.v1",
                "active_session": "OVERNIGHT",
                "tickets_count": 1,
                "staged_count": 1,
                "unusual_vol_count": 2,
                "darkpool_hits_count": 2,
                "darkpool_trf_covered_count": 2,
                "pine_omni_sha256": "a" * 64,
                "pine_shas": {"tier0_omni_sota_24x5.pine": "a" * 64},
                "top_symbols": [{"symbol": "SOXS", "side": "BUY", "score": 52.0}],
            }
        ),
        encoding="utf-8",
    )
    (pack / "finos_tv_cockpit.json").write_text(
        json.dumps({"openbb_rows": [{"symbol": "SOXS", "darkpool_trf_ratio": 0.48}]}),
        encoding="utf-8",
    )
    (pack / "tv_order_presets.json").write_text(
        json.dumps({"schema_version": "tier0.tv-order-presets.v1", "presets": []}),
        encoding="utf-8",
    )

    prom_path = tmp_path / "paper_farm_metrics.prom"
    manifest = aggregate_paper_farm_performance(tmp_path, prom_output_path=prom_path)
    assert manifest["per_lane"]["ALPACA_PA1"]["breaker_latched"] is True
    assert manifest["summary"]["any_breaker_latched"] is True
    tv_meta = manifest["tv_half_auto"]
    assert tv_meta["ready_tickets_count"] == 1
    assert tv_meta["standby_presets_count"] == 1
    assert tv_meta["unusual_vol_count"] == 2
    assert tv_meta["darkpool_hits_count"] == 2
    assert tv_meta["darkpool_trf_covered_count"] == 2
    assert tv_meta["scanner_ranked_count"] == 2
    assert tv_meta["scanner_extreme_eligible_count"] == 1
    assert tv_meta["macro_regime"] == -1
    assert tv_meta["order_presets_present"] is True
    assert tv_meta["finos_tv_cockpit_present"] is True

    finos = json.loads((tmp_path / "finos_paper_farm_performance.json").read_text(encoding="utf-8"))
    assert finos["tv_openbb_rows"] == [{"symbol": "SOXS", "darkpool_trf_ratio": 0.48}]

    telemetry = record_paper_farm_telemetry(manifest, prom_output_path=prom_path)
    for prom_name in (
        "tier0_drawdown_breaker_latched",
        "tier0_tv_half_auto_ready_tickets_count",
        "tier0_tv_half_auto_standby_presets_count",
        "tier0_tv_half_auto_unusual_vol_count",
        "tier0_tv_half_auto_darkpool_hits_count",
        "tier0_tv_half_auto_darkpool_trf_covered_count",
        "tier0_scanner_ranked_count",
        "tier0_scanner_extreme_eligible_count",
        "tier0_scanner_macro_regime",
    ):
        assert prom_name in telemetry["prometheus_text"]
    assert {
        "tier0.drawdown_breaker.latched",
        "tier0.tv_half_auto.ready_tickets_count",
        "tier0.tv_half_auto.standby_presets_count",
        "tier0.tv_half_auto.unusual_vol_count",
        "tier0.tv_half_auto.darkpool_hits_count",
        "tier0.scanner.ranked_count",
        "tier0.scanner.macro_regime",
    } <= set(telemetry["otel_metric_names"])

    grafana_path = (
        Path(__file__).resolve().parent.parent
        / "docker"
        / "grafana"
        / "dashboards"
        / "tier0-sota-observability.json"
    )
    dash = json.loads(grafana_path.read_text(encoding="utf-8"))
    assert len(dash["panels"]) >= 6
    exprs = " ".join(t.get("expr", "") for p in dash["panels"] for t in p.get("targets", []))
    assert "tier0_drawdown_breaker_latched" in exprs
    assert "tier0_tv_half_auto_darkpool_hits_count" in exprs


def test_dagster_reflection_and_mcp_tv_compounding_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dagster import AssetsDefinition
    from scripts import nautilus_mcp_server as mcp_srv
    from scripts import run_compounding_reflection as rcr

    from tier0_sota.definitions import (
        defs,
        frontier_trigger_telemetry,
        paper_farm_performance_snapshot,
    )

    monkeypatch.setenv("TIER0_DATA_ROOT", str(tmp_path))
    manifests_dir = tmp_path / "manifests"
    pack_dir = manifests_dir / "tv_sota_pack"
    pack_dir.mkdir(parents=True, exist_ok=True)

    (manifests_dir / "paper_farm_live_performance_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.paper-farm-live-performance.v1",
                "paper_only": True,
                "live_execution_enabled": False,
                "summary": {"combined_equity_usd": 500_000.0},
                "per_lane": {"ALPACA_PA1": {}, "ALPACA_PA2": {}, "ALPACA_PA3": {}},
                "tv_half_auto": {
                    "active_session": "OVERNIGHT",
                    "tickets_count": 2,
                    "ready_tickets_count": 1,
                    "standby_presets_count": 4,
                    "staged_presets_total": 3,
                    "duckdb_audit_rows": 12,
                    "unusual_vol_count": 3,
                    "darkpool_hits_count": 2,
                    "darkpool_trf_covered_count": 5,
                },
            }
        ),
        encoding="utf-8",
    )
    (manifests_dir / "paper_farm_execution_status.json").write_text(
        json.dumps({"status": "SUPERVISING", "paper_only": True, "live_execution_enabled": False}),
        encoding="utf-8",
    )
    (manifests_dir / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "ranked_count": 2,
                "macro_regime": {"regime": 1, "regime_label": "BULL"},
                "entries": [
                    {
                        "symbol": "NVDA",
                        "extreme_mover_eligible": True,
                        "unusual_volume_buildup": True,
                        "darkpool_trf_ratio": 0.44,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (manifests_dir / "live_lane_order_preset_bundle.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.live-lane-preset-bundle.v1",
                "tca_calibration_ok": True,
                "top_potential_symbols": [{"symbol": "NVDA"}],
            }
        ),
        encoding="utf-8",
    )
    comp_path = pack_dir / "tv_compounding_manifest.json"
    comp_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-compounding-manifest.v1",
                "active_session": "OVERNIGHT",
                "tickets_count": 2,
                "ready_tickets_count": 1,
                "standby_presets_count": 4,
                "staged_count": 3,
                "duckdb_audit_rows": 12,
                "unusual_vol_count": 3,
                "darkpool_hits_count": 2,
                "darkpool_trf_covered_count": 5,
                "pine_omni_sha256": "b" * 64,
                "pine_shas": {"tier0_omni_sota_24x5.pine": "b" * 64},
            }
        ),
        encoding="utf-8",
    )

    # Exact 8 Dagster assets preserved + TV compounding telemetry surfaced.
    asset_names = sorted(
        a.key.to_user_string() for a in (defs.assets or []) if isinstance(a, AssetsDefinition)
    )
    assert len(asset_names) == 8
    snap = paper_farm_performance_snapshot()
    assert isinstance(snap, dict)
    tv_snap = snap["tv_half_auto"]
    assert isinstance(tv_snap, dict)
    assert tv_snap["darkpool_hits_count"] == 2
    assert tv_snap["semi_auto_confirmed"] is False
    frontier = frontier_trigger_telemetry()
    assert isinstance(frontier, dict)
    tv_frontier = frontier["tv_half_auto_compounding"]
    assert isinstance(tv_frontier, dict)
    assert tv_frontier["standby_presets_count"] == 4

    # Compounding reflection merges TV compounding + movers manifest cleanly.
    refl = rcr.run_compounding_reflection(offline=True, manifest_dir=manifests_dir)
    assert refl["tv_half_auto"]["darkpool_hits_count"] == 2
    assert refl["tv_half_auto"]["standby_presets_count"] == 4
    assert refl["tv_half_auto"]["semi_auto_confirmed"] is False

    # FastMCP keeps exact 10 tools and exposes sibling TV sync + admits TV schemas.
    wl_mov = mcp_srv.get_live_watchlist_manifest(
        str(manifests_dir / "live_whole_market_movers_manifest.json")
    )
    assert wl_mov["ok"] is True
    assert wl_mov["tv_sync"]["preset_bundle_count"] == 1
    assert wl_mov["tv_sync"]["tv_compounding"]["darkpool_hits_count"] == 2

    wl_comp = mcp_srv.get_live_watchlist_manifest(str(comp_path))
    assert wl_comp["ok"] is True
    assert wl_comp["schema_version"] == "tier0.tv-compounding-manifest.v1"

    pf_res = mcp_srv.get_live_performance_manifest(
        str(manifests_dir / "paper_farm_live_performance_manifest.json")
    )
    assert pf_res["ok"] is True
    assert pf_res["tv_compounding"]["darkpool_hits_count"] == 2
    assert pf_res["execution_status_snapshot"]["status"] == "SUPERVISING"

    staged = mcp_srv.stage_semi_auto_order_preset(
        payload={
            "symbol": "NVDA",
            "side": "BUY",
            "session": "OVERNIGHT",
            "darkpool_trf_ratio": 0.44,
            "vol_buildup_ratio": 2.3,
            "unusual_volume_buildup": True,
            "script_kind": "strategy",
            "order_preset_id": "OVERNIGHT_24X5_IBKR",
        },
        staged_path=str(tmp_path / "mcp_staged.json"),
    )
    assert staged["ok"] is True
    assert staged["semi_auto_confirmed"] is False
    assert staged["qty"] == 1
    assert staged["darkpool_trf_ratio"] == 0.44
    assert staged["script_kind"] == "strategy"
    assert staged["order_preset_id"] == "OVERNIGHT_24X5_IBKR"


def test_phase3_1000rpm_3000rpm_venue_handles_tv_coordination_and_mcp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import nautilus_mcp_server as mcp_srv

    from tier0_sota.live.paper_farm import resolve_alpaca_paper_venue_profile
    from tier0_sota.live.runner import reconcile_staged_tv_presets_for_engine

    # 1. TIER0_ALPACA_RPM resolution (default 200, 1000 Pro-Line, and fail-closed bounds).
    assert resolve_alpaca_paper_venue_profile(env={}) == ("ALPACA_PAPER_200", 200)
    assert resolve_alpaca_paper_venue_profile(env={"TIER0_ALPACA_RPM": "1000"}) == (
        "ALPACA_PAPER_1000",
        1000,
    )
    assert resolve_alpaca_paper_venue_profile(env={"TIER0_ALPACA_RPM": "ALPACA_PAPER_1000"}) == (
        "ALPACA_PAPER_1000",
        1000,
    )
    for bad in ("0", "1001", "-5", "bogus"):
        with pytest.raises(ValueError):
            resolve_alpaca_paper_venue_profile(env={"TIER0_ALPACA_RPM": bad})

    # 2. PaperFarmCoordinator wired with 3x1000rpm Alpaca Pro-Line + 3000rpm IBKR = 6000rpm.
    monkeypatch.setenv("TIER0_ALPACA_RPM", "1000")
    clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    coord = PaperFarmCoordinator(tmp_path, alpaca_clients=clients)
    for acct, lane in coord.lanes.items():
        assert lane.venue_profile == "ALPACA_PAPER_1000"
        assert lane.rate_budget.max_rpm == 1000
        assert lane.rate_budget.exit_reserve_rpm == 100
        st = lane.write_status(equity=100_000.0, session="RTH")
        assert st["venue_profile"] == "ALPACA_PAPER_1000"
        assert st["rate_budget_max_rpm"] == 1000 and st["exit_reserve_rpm"] == 100
        assert acct in ALPACA_LANE_PROFILES

    ib_st = coord.ibkr.write_status(gateway_ready=False, tcp_reachable=False, equity=None)
    assert ib_st["venue_profile"] == "IBKR_PAPER_4002"
    assert ib_st["rate_budget_max_rpm"] == 3000 and ib_st["exit_reserve_rpm"] == 300

    with pytest.raises(PermissionError, match="live_execution_enabled=False"):
        AlpacaPaperLane(
            "ALPACA_PA1",
            tmp_path,
            _FakeTradingClient(),
            CrossLaneSymbolLock(),
            venue_profile="ALPACA_LIVE_PRO_LINE_1000",
        )

    handles = coord.venue_order_handles(session="OVERNIGHT")
    assert handles["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
    assert handles["combined_max_rpm"] == 6000
    assert handles["combined_exit_reserve_rpm"] == 600
    assert handles["combined_entry_modify_rpm_budget"] == 5400
    assert handles["live_execution_enabled"] is False
    assert handles["venues"]["ALPACA_PAPER_1000"]["max_rpm"] == 1000
    assert handles["venues"]["IBKR_PAPER_4002"]["max_rpm"] == 3000
    assert handles["venues"]["ALPACA_LIVE_PRO_LINE_1000"]["executable_now"] is False

    # 3. Guard lock_tier tracking & serialized active_positions snapshot for TV cockpit.
    pa1 = coord.lanes["ALPACA_PA1"]
    parent_order = SimpleNamespace(
        id="ord-nvda-1",
        status=SimpleNamespace(value="filled"),
        filled_qty="10",
        filled_avg_price="100.00",
    )
    guard = execution.Guard(
        lane=pa1,
        client_id="noesis-guard-1",
        symbol="NVDA",
        side="LONG",
        initial=Decimal("98.00"),
        stop=Decimal("98.00"),
        natr=Decimal("0.020"),
        entered_session="RTH",
        triggered=False,
        parent=parent_order,
    )
    monkeypatch.setattr(guard, "save", lambda: None)
    guard.on_price(Decimal("104.50"), "RTH")
    assert guard.lock_tier >= 3
    assert guard.stop > Decimal("100.00")
    pos_snap = guard.to_position_snapshot("RTH")
    assert pos_snap["symbol"] == "NVDA" and pos_snap["account"] == "ALPACA_PA1"
    assert pos_snap["venue_profile"] == "ALPACA_PAPER_1000"
    assert pos_snap["lock_tier"] >= 3 and pos_snap["order_mode"] == "STOP_MARKET"

    # 4. Fail-closed TV <-> Engine coordination manifest (never auto-submits!).
    assert coord.symbol_lock.try_claim("SOXL", "ALPACA_PA1") is True
    pack_dir = tmp_path / "tv_sota_pack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "paper_only": True,
                "live_execution_enabled": False,
                "active_positions": [pos_snap],
                "active_positions_count": 1,
                "venue_order_handles": handles,
            }
        ),
        encoding="utf-8",
    )
    (pack_dir / "staged_tv_presets.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "semi_auto_confirmed": False,
                "presets": [
                    {
                        "alert_id": "a1b2c3d4",
                        "symbol": "PLTR",
                        "side": "BUY",
                        "qty": 1,
                        "session": "RTH",
                        "venue": "ALPACA",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "trigger_price": 125.00,
                        "natr_pct": 0.022,
                        "rvol": 3.4,
                        "semi_auto_confirmed": False,
                    },
                    {
                        "alert_id": "deadbeef",
                        "symbol": "AMD",
                        "side": "BUY",
                        "qty": 1,
                        "session": "RTH",
                        "venue": "ALPACA",
                        "venue_profile": "ALPACA_LIVE_PRO_LINE_1000",
                        "trigger_price": 150.00,
                        "semi_auto_confirmed": False,
                    },
                    {
                        "alert_id": "c0ffee01",
                        "symbol": "SOXL",
                        "side": "BUY",
                        "qty": 1,
                        "session": "RTH",
                        "venue": "ALPACA",
                        "trigger_price": 42.00,
                        "semi_auto_confirmed": False,
                    },
                    {
                        "alert_id": "1b2r4002",
                        "symbol": "TSLA",
                        "side": "SELL",
                        "qty": 1,
                        "session": "RTH",
                        "venue": "IBKR",
                        "venue_profile": "IBKR_PAPER_4002",
                        "trigger_price": 260.00,
                        "semi_auto_confirmed": False,
                    },
                    {
                        "alert_id": "badqty05",
                        "symbol": "META",
                        "side": "BUY",
                        "qty": 5,
                        "session": "RTH",
                        "venue": "ALPACA",
                        "trigger_price": 500.00,
                        "semi_auto_confirmed": False,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    coord_doc = reconcile_staged_tv_presets_for_engine(
        tmp_path, coord, "RTH", tca_allows_entry=True
    )
    assert coord_doc["schema_version"] == "tier0.tv-engine-coordination.v1"
    assert coord_doc["paper_only"] is True
    assert coord_doc["semi_auto_confirmed"] is False
    assert coord_doc["live_execution_enabled"] is False
    assert coord_doc["auto_submit_enabled"] is False
    assert coord_doc["staged_alerts_evaluated"] == 5
    assert coord_doc["ready_for_operator_confirm_count"] == 1
    assert coord_doc["blocked_count"] == 4
    assert coord_doc["active_positions_count"] == 2  # NVDA guard + SOXL symbol_lock

    by_id = {d["alert_id"]: d for d in coord_doc["coordination_decisions"]}
    assert by_id["a1b2c3d4"]["discipline_gate"] == "READY_FOR_OPERATOR_CONFIRM"
    assert by_id["a1b2c3d4"]["target_account"] == "ALPACA_PA1"
    assert by_id["a1b2c3d4"]["auto_submitted"] is False
    assert by_id["a1b2c3d4"]["nautilus_sandbox_parity"]["parity_ok"] is True
    assert by_id["a1b2c3d4"]["nautilus_sandbox_parity"]["max_order_submit_rate"] == "25/00:00:01"
    assert by_id["deadbeef"]["discipline_gate"] == "HOLD_LIVE_PROFILE_REFUSED"
    assert by_id["c0ffee01"]["discipline_gate"] == "SYMBOL_LOCKED_BY_ALPACA_PA1"
    assert by_id["1b2r4002"]["discipline_gate"] == "IBKR_PAPER_4002_SOCKET_UNAVAILABLE"
    assert by_id["badqty05"]["discipline_gate"] == "QTY_MUST_BE_1"
    assert all(len(c.orders) == 0 for c in clients.values())

    # Alpaca OVERNIGHT SHORT fails closed; tripped breakers across all lanes fail closed.
    (pack_dir / "staged_tv_presets.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "semi_auto_confirmed": False,
                "presets": [
                    {
                        "alert_id": "ovnshort1",
                        "symbol": "NVDA",
                        "side": "SELL",
                        "qty": 1,
                        "session": "OVERNIGHT",
                        "venue": "ALPACA",
                        "semi_auto_confirmed": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ovn_doc = reconcile_staged_tv_presets_for_engine(
        tmp_path, coord, "OVERNIGHT", tca_allows_entry=True
    )
    assert (
        ovn_doc["coordination_decisions"][0]["discipline_gate"]
        == "ALPACA_OVERNIGHT_SHORT_UNSUPPORTED_ROUTE_IBKR_OR_INVERSE"
    )
    for lane in coord.lanes.values():
        lane.breaker.tripped = True
    (pack_dir / "staged_tv_presets.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "semi_auto_confirmed": False,
                "presets": [
                    {
                        "alert_id": "brk01",
                        "symbol": "PLTR",
                        "side": "BUY",
                        "qty": 1,
                        "session": "RTH",
                        "venue": "ALPACA",
                        "semi_auto_confirmed": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    brk_doc = reconcile_staged_tv_presets_for_engine(tmp_path, coord, "RTH", tca_allows_entry=True)
    assert brk_doc["any_breaker_tripped"] is True
    assert (
        brk_doc["coordination_decisions"][0]["discipline_gate"]
        == "ALL_ALPACA_LANES_BREAKER_TRIPPED_OR_UNAVAILABLE"
    )

    # Restore RTH manifest for performance + MCP verification.
    for lane in coord.lanes.values():
        lane.breaker.tripped = False
    (pack_dir / "staged_tv_presets.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "semi_auto_confirmed": False,
                "presets": [
                    {
                        "alert_id": "a1b2c3d4",
                        "symbol": "PLTR",
                        "side": "BUY",
                        "qty": 1,
                        "session": "RTH",
                        "venue": "ALPACA",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "trigger_price": 125.00,
                        "semi_auto_confirmed": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    reconcile_staged_tv_presets_for_engine(tmp_path, coord, "RTH", tca_allows_entry=True)

    # 5. Performance aggregator + FINOS + Prometheus + unitless OTel telemetry.
    prom_path = tmp_path / "paper_farm_metrics.prom"
    perf = aggregate_paper_farm_performance(tmp_path, prom_output_path=prom_path)
    assert perf["tv_half_auto"]["coordination_present"] is True
    assert perf["tv_half_auto"]["coordination_evaluated_count"] == 1
    assert perf["tv_half_auto"]["coordination_ready_count"] == 1
    assert perf["tv_half_auto"]["active_trailed_positions_count"] == 1
    assert perf["finos_openbb"]["active_positions"][0]["symbol"] == "NVDA"
    assert perf["finos_openbb"]["venue_order_handles"]["combined_max_rpm"] == 6000

    tel = record_paper_farm_telemetry(perf, prom_output_path=prom_path)
    for prom_name in (
        "tier0_tv_engine_coordination_evaluated_count",
        "tier0_tv_engine_coordination_ready_count",
        "tier0_paper_farm_active_trailed_positions",
        "tier0_venue_order_handle_max_rpm",
        "tier0_venue_order_handle_exit_reserve_rpm",
    ):
        assert prom_name in tel["prometheus_text"]
    assert {
        "tier0.tv_engine_coordination.evaluated_count",
        "tier0.tv_engine_coordination.ready_count",
        "tier0.paper_farm.active_trailed_positions",
        "tier0.venue_order_handle.max_rpm",
        "tier0.venue_order_handle.exit_reserve_rpm",
    } <= set(tel["otel_metric_names"])

    # 6. FastMCP server: exact 10 tools + ALPACA_PAPER_1000 + coordination + fail-closed LIVE.
    vp_info = mcp_srv.inspect_venue_profile("ALPACA_PAPER_1000", session="PRE_MARKET")
    assert vp_info["allowed"] is True and vp_info["max_rpm"] == 1000
    assert vp_info["order_handle_spec"]["limit_collar_bps"] == 35.0
    assert vp_info["order_handle_spec"]["exit_reserve_rpm"] == 100

    wl_coord = mcp_srv.get_live_watchlist_manifest(
        str(pack_dir / "tv_engine_coordination_manifest.json")
    )
    assert wl_coord["ok"] is True
    assert wl_coord["schema_version"] == "tier0.tv-engine-coordination.v1"

    pf_mcp = mcp_srv.get_live_performance_manifest(
        str(tmp_path / "paper_farm_live_performance_manifest.json")
    )
    assert pf_mcp["ok"] is True
    assert pf_mcp["tv_engine_coordination"]["ready_for_operator_confirm_count"] == 1

    st_ok = mcp_srv.stage_semi_auto_order_preset(
        symbol="NVDA",
        side="BUY",
        session="RTH",
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        staged_path=str(tmp_path / "mcp_1000.json"),
    )
    assert st_ok["ok"] is True and st_ok["semi_auto_confirmed"] is False
    assert st_ok["venue_profile"] == "ALPACA_PAPER_1000"
    assert st_ok["order_handle_spec"]["max_rpm"] == 1000

    st_live = mcp_srv.stage_semi_auto_order_preset(
        symbol="NVDA",
        side="BUY",
        session="RTH",
        venue="ALPACA",
        venue_profile="ALPACA_LIVE_PRO_LINE_1000",
        staged_path=str(tmp_path / "mcp_live_fail.json"),
    )
    assert st_live["ok"] is False and st_live["semi_auto_confirmed"] is False

    smoke = mcp_srv.run_smoke_check()
    assert smoke["ok"] is True and smoke["tools_count"] == 10


def test_alpaca_paper_1000_full_matrix_runner_execution_and_tv_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import sys
    from typing import Any

    from tier0_sota.live import runner
    from tier0_sota.live.paper_farm import IBKR_PAPER_ACCOUNT

    # (1) PaperFarmCoordinator.venue_order_handles() across default 3600rpm vs 6000rpm
    #     across ALL 4 sessions (RTH/PRE_MARKET/AFTER_HOURS/OVERNIGHT) & ALL 3 NATR schedules.
    monkeypatch.delenv("TIER0_ALPACA_RPM", raising=False)
    default_clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    default_farm = PaperFarmCoordinator(tmp_path / "default200", alpaca_clients=default_clients)
    def_handles = default_farm.venue_order_handles(session="RTH", trailing_schedule="baseline")
    assert def_handles["alpaca_venue_profile"] == "ALPACA_PAPER_200"
    assert def_handles["combined_max_rpm"] == 3600
    assert def_handles["combined_exit_reserve_rpm"] == 360
    assert def_handles["combined_entry_modify_rpm_budget"] == 3240

    monkeypatch.setenv("TIER0_ALPACA_RPM", "1000")
    pro_clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    pro_farm = PaperFarmCoordinator(tmp_path / "pro1000", alpaca_clients=pro_clients)
    expected_sessions = {
        "RTH": ("STOP_MARKET", 0.0, False),
        "PRE_MARKET": ("STOP_LIMIT", 35.0, True),
        "AFTER_HOURS": ("STOP_LIMIT", 35.0, True),
        "OVERNIGHT": ("STOP_LIMIT", 50.0, True),
    }
    for sess, (exp_mode, exp_collar, exp_outside) in expected_sessions.items():
        for sched in ("tight_near_lossless", "baseline", "wide_natr_adaptive"):
            handles = pro_farm.venue_order_handles(session=sess, trailing_schedule=sched)
            assert handles["paper_only"] is True
            assert handles["live_execution_enabled"] is False
            assert handles["session"] == sess
            assert handles["trailing_schedule"] == sched
            assert handles["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
            assert handles["alpaca_per_lane_max_rpm"] == 1000
            assert handles["ibkr_venue_profile"] == "IBKR_PAPER_4002"
            assert handles["combined_max_rpm"] == 6000
            assert handles["combined_exit_reserve_rpm"] == 600
            assert handles["combined_entry_modify_rpm_budget"] == 5400
            assert set(handles["per_account"]) == set(ALPACA_LANE_PROFILES) | {IBKR_PAPER_ACCOUNT}
            for acct, spec in handles["per_account"].items():
                assert spec["session_order_mode"] == exp_mode
                assert spec["limit_collar_bps"] == exp_collar
                assert spec["outside_rth"] is exp_outside
                assert spec["trailing_schedule"] == sched
                assert 0.0 < spec["initial_max_loss_pct"] <= 0.02
                assert spec["paper_only"] is True and spec["live_execution_enabled"] is False
                if acct == IBKR_PAPER_ACCOUNT:
                    assert spec["effective_max_rpm"] == 3000
                    assert spec["effective_exit_reserve_rpm"] == 300
                    assert spec["effective_entry_modify_rpm_budget"] == 2700
                else:
                    assert spec["venue_profile"] == "ALPACA_PAPER_1000"
                    assert spec["effective_max_rpm"] == 1000
                    assert spec["effective_exit_reserve_rpm"] == 100
                    assert spec["effective_entry_modify_rpm_budget"] == 900
                    assert spec["max_rps"] == 25
                    assert spec["max_concurrent_trailed_positions"] == 10

    # (2) runner.main() --alpaca-rpm fail-closed validation + --observe 6000rpm status manifest.
    obs_dir = tmp_path / "runner_obs"
    monkeypatch.delenv("TIER0_ALPACA_RPM", raising=False)
    for bad_flag in ("ALPACA_LIVE_PRO_LINE_1000", "1001", "0", "bogus"):
        monkeypatch.setattr(
            sys,
            "argv",
            ["runner", "--observe", "--manifest-dir", str(obs_dir), "--alpaca-rpm", bad_flag],
        )
        with pytest.raises(ValueError):
            runner.main()

    obs_clients = {acct: _FakeTradingClient() for acct in ALPACA_LANE_PROFILES}
    monkeypatch.setattr(runner, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(
        runner,
        "PaperFarmCoordinator",
        lambda mdir: PaperFarmCoordinator(mdir, alpaca_clients=obs_clients),
    )
    monkeypatch.setattr(runner, "ibkr_health", lambda _ibkr: False)
    monkeypatch.setattr(runner, "refresh_accounts", lambda *_a, **_kw: None)
    monkeypatch.setattr(runner.engine, "classify_us_equity_session_et", lambda _ns: "RTH")

    class _StubScanner:
        data_client = None

        def scan(self, *, top_k: int, output_path: Path) -> dict[str, Any]:
            doc = {
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "session": "RTH",
                "ranked_count": 0,
                "macro_regime": {"regime": 1},
                "entries": [],
            }
            output_path.write_text(json.dumps(doc), encoding="utf-8")
            return doc

    monkeypatch.setattr(runner.ExtremeMoverScanner, "from_env", lambda **_kw: _StubScanner())
    monkeypatch.setattr(
        runner,
        "refresh_tickets_from_paths",
        lambda **_kw: {"staged_count": 0, "standby_presets": []},
    )
    monkeypatch.setenv("TIER0_OTLP_METRICS_URL", "")
    monkeypatch.setattr(
        sys,
        "argv",
        ["runner", "--observe", "--manifest-dir", str(obs_dir), "--alpaca-rpm", "1000"],
    )
    runner.main()
    obs_out = json.loads(capsys.readouterr().out)
    assert obs_out["combined_max_rpm"] == 6000
    assert obs_out["paper_only"] is True and obs_out["live_execution_enabled"] is False
    status_doc = json.loads(
        (obs_dir / "paper_farm_live_auto_entry_status.json").read_text(encoding="utf-8")
    )
    assert status_doc["combined_max_rpm"] == 6000
    assert status_doc["venue_order_handles"]["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
    assert status_doc["venue_order_handles"]["combined_exit_reserve_rpm"] == 600
    for acct in ALPACA_LANE_PROFILES:
        acct_row = status_doc["accounts"][acct]
        assert acct_row["execution_venue_profile"] == "ALPACA_PAPER_1000"
        assert acct_row["rate_limit_rpm"] == 1000
        assert acct_row["exit_reserve_rpm"] == 100
    assert (
        status_doc["accounts"][IBKR_PAPER_ACCOUNT]["execution_venue_profile"] == "IBKR_PAPER_4002"
    )
    assert status_doc["accounts"][IBKR_PAPER_ACCOUNT]["rate_limit_rpm"] == 3000
    assert status_doc["accounts"][IBKR_PAPER_ACCOUNT]["exit_reserve_rpm"] == 300

    # (3) execution.run_execution() under TIER0_ALPACA_RPM=1000 emits 6000rpm handles + guard snap.
    exec_dir = tmp_path / "exec_1000"
    exec_dir.mkdir(parents=True, exist_ok=True)
    stop_evt = threading.Event()
    monkeypatch.setenv("TIER0_ALPACA_RPM", "1000")

    class _Exec1000Broker(_FakeTradingClient):
        def __init__(self) -> None:
            super().__init__()
            self.identity = uuid4()

        def get_account(self) -> SimpleNamespace:
            return SimpleNamespace(
                id=self.identity,
                status=SimpleNamespace(value="ACTIVE"),
                account_blocked=False,
                trading_blocked=False,
                trade_suspended_by_user=False,
                equity="100000",
                last_equity="100000",
            )

        def get_clock(self) -> SimpleNamespace:
            return SimpleNamespace(is_open=True)

        def get_asset(self, symbol: str) -> SimpleNamespace:
            return SimpleNamespace(
                symbol=symbol,
                tradable=True,
                shortable=True,
                easy_to_borrow=True,
                attributes=["overnight_tradable"],
            )

        def get_all_positions(self) -> list[Any]:
            return []

        def get_orders(self, request: Any = None) -> list[Any]:
            return []

        def submit_order(self, req: Any) -> SimpleNamespace:
            self.orders.append(req)
            child = SimpleNamespace(
                id="child-1000",
                status=SimpleNamespace(value="held"),
                replaced_by=None,
                symbol=req.symbol,
                side=SimpleNamespace(value="sell"),
                stop_price=str(req.stop_loss.stop_price),
            )
            return SimpleNamespace(
                id="parent-1000",
                legs=[child],
                status=SimpleNamespace(value="new"),
                filled_qty="0",
                filled_avg_price=None,
            )

        def get_order_by_id(self, order_id: str, filter: Any = None) -> SimpleNamespace:
            req = self.orders[0]
            assert isinstance(req, LimitOrderRequest) and req.stop_loss is not None
            child = SimpleNamespace(
                id="child-1000",
                status=SimpleNamespace(value="held"),
                replaced_by=None,
                symbol=req.symbol,
                side=SimpleNamespace(value="sell"),
                stop_price=str(req.stop_loss.stop_price),
            )
            return SimpleNamespace(
                id=order_id,
                legs=[child],
                status=SimpleNamespace(value="new"),
                filled_qty="0",
                filled_avg_price=None,
            )

    exec_brokers = {acct: _Exec1000Broker() for acct in ALPACA_LANE_PROFILES}
    exec_farm = PaperFarmCoordinator(exec_dir, alpaca_clients=exec_brokers)

    class _ExecQuotes:
        def get_stock_latest_quote(self, _req: Any) -> dict[str, SimpleNamespace]:
            return {
                "NVDA": SimpleNamespace(
                    bid_price=100.00, ask_price=100.02, timestamp=datetime.now(UTC)
                )
            }

    class _ExecStream:
        def __init__(self, *_a: Any, feed: Any) -> None:
            self.feed = feed
            self.subscribed = threading.Event()

        def subscribe_trades(self, callback: Any, *symbols: str) -> None:
            self.callback = callback
            if "NVDA" in symbols:
                self.subscribed.set()

        def run(self) -> None:
            assert self.subscribed.wait(15)
            if self.feed.value == "sip":
                asyncio.run(
                    self.callback(
                        SimpleNamespace(symbol="NVDA", price=100.01, timestamp=datetime.now(UTC))
                    )
                )
                stop_evt.set()
            assert stop_evt.wait(15)

        def stop(self) -> None:
            stop_evt.set()

    now_iso = datetime.now(UTC).isoformat()
    (exec_dir / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "generated_at_utc": now_iso,
                "macro_regime": {"regime": 1},
                "entries": [
                    {
                        "symbol": "NVDA",
                        "extreme_mover_eligible": True,
                        "side": "BUY",
                        "session": "RTH",
                        "bar_close_utc": now_iso,
                        "natr_pct": 0.035,
                        "rvol_normalized": 4.2,
                        "last_price": 100.01,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(execution, "verify_paper_environment", lambda: None)
    monkeypatch.setattr(execution, "resolve_alpaca_env_creds", lambda: ("", ""))
    monkeypatch.setattr(execution, "PaperFarmCoordinator", lambda _dir: exec_farm)
    monkeypatch.setattr(execution, "StockDataStream", _ExecStream)
    monkeypatch.setattr(
        execution.ExtremeMoverScanner,
        "from_env",
        lambda *_a, **_kw: SimpleNamespace(data_client=_ExecQuotes()),
    )
    monkeypatch.setattr(execution.engine, "classify_us_equity_session_et", lambda _ns: "RTH")
    execution.run_execution(exec_dir, stop=stop_evt)
    exec_status = json.loads(
        (exec_dir / "paper_farm_execution_status.json").read_text(encoding="utf-8")
    )
    assert exec_status["paper_only"] is True and exec_status["live_execution_enabled"] is False
    assert exec_status["venue_order_handles"]["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
    assert exec_status["venue_order_handles"]["combined_max_rpm"] == 6000
    assert exec_status["venue_order_handles"]["combined_exit_reserve_rpm"] == 600

    # (4) runner.main() --confirm-tv-alert --alpaca-rpm ALPACA_PAPER_1000 --dry-run
    #     + fail-closed refusal of LIVE and non-ALPACA (IBKR) venue_profile in TV handoff.
    tv_dir = tmp_path / "tv_1000"
    rth_now = datetime(2026, 10, 15, 15, 0, tzinfo=UTC)  # 11:00 ET RTH
    staged_ok = _stage_sample_tv_preset(
        tv_dir,
        symbol="NVDA",
        side="BUY",
        session="RTH",
        trigger_price=125.00,
        stop_price=122.50,
        natr_pct=0.035,
        trailing_schedule="wide_natr_adaptive",
        order_type="STOP_MARKET",
        limit_collar_bps=0.0,
        rvol=4.2,
    )
    staged_path = tv_dir / "tv_sota_pack" / "staged_tv_presets.json"
    staged_doc = json.loads(staged_path.read_text(encoding="utf-8"))
    staged_doc["presets"][0]["venue_profile"] = "ALPACA_PAPER_1000"
    staged_doc["presets"].append(
        dict(
            staged_doc["presets"][0],
            alert_id="1111222233334444",
            venue_profile="ALPACA_LIVE_PRO_LINE_1000",
        )
    )
    staged_doc["presets"].append(
        dict(
            staged_doc["presets"][0],
            alert_id="5555666677778888",
            venue_profile="IBKR_PAPER_4002",
        )
    )
    staged_path.write_text(json.dumps(staged_doc), encoding="utf-8")

    tv_brokers = {acct: _TvConfirmBroker() for acct in ALPACA_LANE_PROFILES}
    monkeypatch.delenv("TIER0_ALPACA_RPM", raising=False)
    monkeypatch.setattr(
        execution,
        "PaperFarmCoordinator",
        lambda mdir: PaperFarmCoordinator(mdir, alpaca_clients=tv_brokers),
    )

    class _Tv1000Quotes:
        def get_stock_latest_quote(self, _req: Any) -> dict[str, SimpleNamespace]:
            return {"NVDA": SimpleNamespace(bid_price=124.98, ask_price=125.02, timestamp=rth_now)}

    monkeypatch.setattr(
        execution.ExtremeMoverScanner,
        "from_env",
        lambda *_a, **_kw: SimpleNamespace(data_client=_Tv1000Quotes()),
    )

    class _FixedNow:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return rth_now

    monkeypatch.setattr(execution, "datetime", _FixedNow)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--confirm-tv-alert",
            str(staged_ok["alert_id"]),
            "--manifest-dir",
            str(tv_dir),
            "--alpaca-rpm",
            "ALPACA_PAPER_1000",
            "--dry-run",
        ],
    )
    runner.main()
    tv_receipt = json.loads(capsys.readouterr().out)
    assert tv_receipt["ok"] is True and tv_receipt["dry_run"] is True
    assert tv_receipt["paper_only"] is True and tv_receipt["live_execution_enabled"] is False
    assert tv_receipt["lane_account"] == "ALPACA_PA1"
    assert tv_receipt["venue_profile"] == "ALPACA_PAPER_1000"
    handle = tv_receipt["order_handle_spec"]
    assert handle["max_rpm"] == 1000 and handle["max_rps"] == 25
    assert handle["exit_reserve_rpm"] == 100 and handle["entry_modify_rpm_budget"] == 900
    assert handle["max_concurrent_trailed_positions"] == 10
    assert handle["executable_now"] is True

    with pytest.raises(PermissionError, match="live_execution_enabled=False"):
        execution.submit_confirmed_tv_preset_paper(
            tv_dir, alert_id="1111222233334444", qty=1, dry_run=True, now_utc=rth_now
        )
    with pytest.raises(ValueError, match="ALPACA paper venue profile"):
        execution.submit_confirmed_tv_preset_paper(
            tv_dir, alert_id="5555666677778888", qty=1, dry_run=True, now_utc=rth_now
        )
    assert all(len(b.orders) == 0 for b in tv_brokers.values())

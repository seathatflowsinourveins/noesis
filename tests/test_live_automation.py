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
)
from tier0_sota.live.performance import _read_duckdb_counts, record_paper_farm_telemetry
from tier0_sota.live.runner import exit_context, reconcile_fills
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
    assert DEFAULT_PIN_SYMBOLS == ("SOXL", "SOXS")
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
    with duckdb.connect(str(farm.lanes["ALPACA_PA1"].duckdb_path)) as db:
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

"""Unit & integration tests for tier0_sota.live (mocked broker/network calls)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

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
    select_short_or_inverse_fallback,
)


def _make_bar(open_px: float, close_px: float, vol: float) -> SimpleNamespace:
    return SimpleNamespace(
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

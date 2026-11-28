"""Fast offline unit tests for native NautilusTrader v2 catalog & empirical tearsheet suite."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_sim_backtest_tearsheet_suite import (  # noqa: E402
    build_causal_multi_session_signals,
    populate_nautilus_parquet_catalog,
    run_empirical_backtest_from_catalog,
)

from tier0_sota import discovery as disc  # noqa: E402

_NY_TZ = ZoneInfo("America/New_York")


def _synthetic_prewall_bars_df() -> pl.DataFrame:
    """Build deterministic multi-session pre-wall (<= 2026-10-15) synthetic 1m bars."""
    rows: list[dict[str, object]] = []
    session_days = (
        datetime(2026, 10, 13, 9, 30, tzinfo=_NY_TZ),
        datetime(2026, 10, 14, 9, 30, tzinfo=_NY_TZ),
        datetime(2026, 10, 15, 9, 30, tzinfo=_NY_TZ),
    )
    for d_idx, day_open in enumerate(session_days):
        for minute in range(60):
            ts_utc = (day_open + timedelta(minutes=minute)).astimezone(UTC)
            t_ns = int(ts_utc.timestamp() * 1_000_000_000)
            # Benchmarks SPY/QQQ/SOXX trend mildly up so causal regime >= 0
            for bm, base_px in (("SPY", 580.0), ("QQQ", 495.0), ("SOXX", 230.0)):
                px = base_px + d_idx * 1.5 + minute * 0.04
                rows.append(
                    {
                        "symbol": bm,
                        "t_ns": t_ns,
                        "o": px - 0.02,
                        "h": px + 0.05,
                        "l": px - 0.05,
                        "c": px,
                        "v": 50_000.0 + minute * 100.0,
                    }
                )
            # Traded NVDA: breakout at minute 20 -> day 0 wins, day 1 stops out, day 2 wins
            base_nvda = 135.0 + d_idx * 1.0
            if minute < 20:
                nv_px = base_nvda + (minute % 3) * 0.03
                nv_vol = 20_000.0
            elif minute == 20:
                nv_px = base_nvda + 0.65
                nv_vol = 180_000.0
            elif d_idx == 1:
                # Reversal stop-out day -> ensures realistic losses & drawdown
                nv_px = base_nvda + 0.65 - (minute - 20) * 0.14
                nv_vol = 35_000.0
            else:
                # Continuation winner day
                nv_px = base_nvda + 0.65 + (minute - 20) * 0.09
                nv_vol = 30_000.0
            rows.append(
                {
                    "symbol": "NVDA",
                    "t_ns": t_ns,
                    "o": nv_px - 0.03,
                    "h": nv_px + 0.08,
                    "l": nv_px - 0.08,
                    "c": nv_px,
                    "v": nv_vol,
                }
            )
    return pl.DataFrame(rows).sort(["symbol", "t_ns"])


def test_populate_catalog_enforces_strict_prewall_pit(tmp_path: Path) -> None:
    post_wall_ts = int(datetime(2026, 10, 16, 14, 0, tzinfo=UTC).timestamp() * 1_000_000_000)
    bad_df = pl.DataFrame(
        [
            {
                "symbol": "NVDA",
                "t_ns": post_wall_ts,
                "o": 140.0,
                "h": 141.0,
                "l": 139.5,
                "c": 140.5,
                "v": 100_000.0,
            }
        ]
    )
    with pytest.raises(disc.PreWallViolationError):
        populate_nautilus_parquet_catalog(bad_df, tmp_path / "catalog")


def test_parquet_catalog_roundtrip_and_official_nautilus_tearsheet(tmp_path: Path) -> None:
    bars_df = _synthetic_prewall_bars_df()
    catalog_dir = tmp_path / "nautilus-catalog"
    cat_meta = populate_nautilus_parquet_catalog(bars_df, catalog_dir, clear_existing=True)

    assert cat_meta["round_trip_parity_ok"] is True
    assert cat_meta["instruments_count"] == 4
    assert cat_meta["bars_written_total"] == bars_df.height
    assert cat_meta["bars_read_back_total"] == bars_df.height
    assert cat_meta["parquet_files_count"] >= 4
    assert cat_meta["prewall_cutoff_iso"] == "2026-10-15"

    signals, spy_bench, sig_meta = build_causal_multi_session_signals(
        bars_df,
        traded_symbols=("NVDA",),
        benchmark_symbols=("SPY", "QQQ", "SOXX"),
        rvol_threshold=2.0,
    )
    assert sig_meta["total_long_signals"] >= 2
    assert spy_bench is not None

    ts_empirical = tmp_path / "tearsheets" / "nautilus_prewall_empirical_tearsheet.html"
    ts_canonical = tmp_path / "tearsheets" / "nautilus_prewall_tearsheet.html"
    bt_meta = run_empirical_backtest_from_catalog(
        catalog_dir,
        catalog_meta=cat_meta,
        signals_by_symbol=signals,
        spy_benchmark_returns=spy_bench,
        tearsheet_paths=(ts_empirical, ts_canonical),
        starting_cash=Decimal("100000"),
        cost_bps_per_side=Decimal("4.0"),
        benchmark_symbols=("SPY", "QQQ", "SOXX"),
    )
    assert bt_meta["risk_engine_bypass"] is False
    assert bt_meta["paper_only"] is True
    assert bt_meta["live_execution_enabled"] is False
    assert bt_meta["total_orders"] >= 2
    assert bt_meta["total_fills"] >= 2
    assert bt_meta["round_trip_trades_count"] >= 1
    assert bt_meta["stops_attached_total"] >= 1
    assert bt_meta["win_rate"] is not None
    assert 0.0 <= bt_meta["win_rate"] <= 1.0
    assert ts_empirical.is_file() and ts_canonical.is_file()
    assert ts_empirical.stat().st_size > 50_000
    assert ts_empirical.read_bytes() == ts_canonical.read_bytes()
    html_text = ts_empirical.read_text(encoding="utf-8")
    assert "plotly" in html_text.lower()
    assert "nautilus" in html_text.lower()

    bwf_index = bt_meta["bars_with_fills_by_symbol"]
    assert sorted(bwf_index.keys()) == ["NVDA", "QQQ", "SOXX", "SPY"]
    bwf_canonical = ts_empirical.parent / "nautilus_prewall_bars_with_fills.html"
    assert bwf_canonical.is_file() and bwf_canonical.stat().st_size > 50_000
    for sym in ("NVDA", "QQQ", "SOXX", "SPY"):
        sym_html = ts_empirical.parent / f"nautilus_prewall_bars_with_fills_{sym}.html"
        assert sym_html.is_file() and sym_html.stat().st_size > 50_000
        assert bwf_index[sym]["symbol"] == sym
        assert bwf_index[sym]["path"] == str(sym_html)
    nvda_html = ts_empirical.parent / "nautilus_prewall_bars_with_fills_NVDA.html"
    assert bwf_canonical.read_bytes() == nvda_html.read_bytes()

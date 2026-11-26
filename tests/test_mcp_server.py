"""Offline tests for scripts/nautilus_mcp_server.py FastMCP tools and --smoke CLI."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import nautilus_mcp_server as mcp_srv  # noqa: E402

from tier0_sota.discovery import BAR_DURATION_NS, PreWallViolationError  # noqa: E402

NY_TZ = ZoneInfo("America/New_York")


def test_fastmcp_registry_all_10_tools_and_dispatch() -> None:
    assert mcp_srv.mcp.name == "tier0-nautilus-engine"
    tools = asyncio.run(mcp_srv.mcp.list_tools())
    expected = [
        "check_prewall_pit_session",
        "compute_risk_sized_stop",
        "evaluate_l1_queue_markout",
        "get_engine_backbone_status",
        "get_live_performance_manifest",
        "get_live_watchlist_manifest",
        "inspect_venue_profile",
        "rank_symbols_advisory",
        "run_prewall_native_backbone_parity",
        "stage_semi_auto_order_preset",
    ]
    assert sorted(t.name for t in tools) == expected
    for tool in tools:
        assert isinstance(tool.description, str) and tool.description.strip()
        assert isinstance(tool.inputSchema, dict) and tool.inputSchema

    assert asyncio.run(mcp_srv.mcp.list_resources()) == []
    assert asyncio.run(mcp_srv.mcp.list_prompts()) == []

    call_res: Any = asyncio.run(
        mcp_srv.mcp.call_tool("check_prewall_pit_session", {"day": "2026-10-15"})
    )
    structured = call_res[1] if isinstance(call_res, tuple) else call_res
    if not isinstance(structured, dict):
        structured = json.loads(getattr(call_res[0][0], "text", "{}"))
    assert structured["pre_wall_ok"] is True
    assert structured["prior_xnys_session"] == "2026-10-14"


def test_engine_backbone_and_venue_live_refusals() -> None:
    for venue in ("IBKR_PAPER_4002", "ALPACA_PAPER_200"):
        st = mcp_srv.get_engine_backbone_status(venue)
        assert st["environment"] == "SANDBOX"
        assert st["broker_ports_open"] == 0
        assert st["paper_live_deployed"] is False
        assert st["live_execution_enabled"] is False
        assert st["risk_engine"]["bypass"] is False
        assert st["sessions_24x5"] == ["AFTER_HOURS", "OVERNIGHT", "PRE_MARKET", "RTH"]

    for live_venue in (
        "IBKR_LIVE_3000",
        "ALPACA_LIVE_PRO_LINE_1000",
        "ALPACA_LIVE_ELITE_SMART_ROUTER",
    ):
        with pytest.raises(PermissionError):
            mcp_srv.get_engine_backbone_status(live_venue)
        rep = mcp_srv.inspect_venue_profile(live_venue)
        assert rep["allowed"] is False and rep["executable_now"] is False
        assert "LIVE execution hard-refused (live_execution_enabled=False)" in rep["reasons"]

    assert mcp_srv.inspect_venue_profile("ALPACA_PAPER_200", algo="DMA")["allowed"] is False
    assert (
        mcp_srv.inspect_venue_profile("ALPACA_PAPER_200", require_24x5_overnight=True)["allowed"]
        is False
    )


def test_risk_stop_parity_pit_and_l1_markout(tmp_path: Path) -> None:
    long_res = mcp_srv.compute_risk_sized_stop("100.00", "long", "100000")
    assert long_res["capped_stop_price"] == "98.00" and long_res["shares"] == 100
    assert Decimal(long_res["max_loss_pct"]) == Decimal("0.0200")
    with pytest.raises(ValueError, match="violates hard Tier-0 cap"):
        mcp_srv.compute_risk_sized_stop("100.00", "LONG", "100000", "0.05")
    with pytest.raises(InvalidOperation):
        mcp_srv.compute_risk_sized_stop("bad", "LONG", "100000")

    par = mcp_srv.run_prewall_native_backbone_parity(symbol="NVDA", day="2026-10-15")
    assert par["overall_ok"] is True and par["live_profile_refused"] is True
    with pytest.raises(PreWallViolationError):
        mcp_srv.run_prewall_native_backbone_parity(symbol="NVDA", day="2026-10-16")
    with pytest.raises(PreWallViolationError):
        mcp_srv.check_prewall_pit_session("2026-10-16")

    t0 = int(datetime(2026, 10, 15, 9, 30, tzinfo=NY_TZ).astimezone(UTC).timestamp()) * 10**9
    q_dir = tmp_path / "published-orb" / "quotes" / "NVDA"
    t_dir = tmp_path / "published-orb" / "trades" / "NVDA"
    q_dir.mkdir(parents=True)
    t_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "t_ns": [t0 + i * 1_000_000_000 for i in range(30)],
            "bp": [100.00 + (0.02 if i > 10 else 0.0) for i in range(30)],
            "bs": [500.0] * 30,
            "ap": [100.02 + (0.02 if i > 10 else 0.0) for i in range(30)],
            "as": [500.0] * 30,
        }
    ).write_parquet(q_dir / "2026-10-15_00.parquet")
    pl.DataFrame(
        {
            "t_ns": [t0 + i * 2_000_000_000 + 500_000_000 for i in range(10)],
            "p": [100.00] * 10,
            "s": [600.0] * 10,
        }
    ).write_parquet(t_dir / "2026-10-15_00.parquet")
    l1_res = mcp_srv.evaluate_l1_queue_markout("NVDA", day="2026-10-15", root=str(tmp_path))
    assert l1_res["ok"] is True and l1_res["passive_order_submitted"] is True


def test_manifests_ranking_and_semi_auto_staging(tmp_path: Path) -> None:
    wl = tmp_path / "live_watchlist_manifest.json"
    wl.write_text(
        json.dumps({"schema_version": "tier0.watchlist-manifest.v1", "entries": []}),
        encoding="utf-8",
    )
    assert mcp_srv.get_live_watchlist_manifest(str(wl))["ok"] is True
    assert mcp_srv.get_live_watchlist_manifest(str(tmp_path / "missing.json"))["ok"] is False

    pf = tmp_path / "perf.json"
    pf.write_text(json.dumps({"schema_version": "tier0.perf.v1", "equity": 100000}), "utf-8")
    assert mcp_srv.get_live_performance_manifest(str(pf))["ok"] is True

    t0 = int(datetime(2026, 10, 15, 9, 30, tzinfo=NY_TZ).astimezone(UTC).timestamp()) * 10**9
    bars_dir = tmp_path / "alpaca-sip" / "raw" / "bars-1min" / "2026-10-15"
    bars_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["NVDA", "NVDA", "SOXL", "SOXL"],
            "t_ns": [t0, t0 + BAR_DURATION_NS, t0, t0 + BAR_DURATION_NS],
            "o": [100.0, 102.0, 30.0, 30.1],
            "h": [103.0, 105.0, 30.2, 30.2],
            "l": [99.5, 101.5, 29.9, 30.0],
            "c": [102.0, 104.5, 30.1, 30.15],
            "v": [100_000.0, 120_000.0, 200_000.0, 200_000.0],
        }
    ).write_parquet(bars_dir / "part.parquet")
    rk = mcp_srv.rank_symbols_advisory(day="2026-10-15", min_dollar_volume=1e6, root=str(tmp_path))
    assert rk["ok"] is True and rk["entries"][0]["symbol"] == "NVDA"

    staged_p = tmp_path / "staged.json"
    ok_s = mcp_srv.stage_semi_auto_order_preset(
        symbol="NVDA", side="BUY", session="AFTER_HOURS", staged_path=str(staged_p)
    )
    assert ok_s["ok"] is True and ok_s["semi_auto_confirmed"] is False
    bad_live = mcp_srv.stage_semi_auto_order_preset(
        payload={
            "symbol": "NVDA", "side": "BUY", "session": "RTH", "endpoint": "api.alpaca.markets"
        }
    )
    assert bad_live["ok"] is False


def test_mcp_server_smoke_cli(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert mcp_srv.main(["--smoke"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True and out["tools_count"] == 10

    calls: list[str] = []
    monkeypatch.setattr(mcp_srv.mcp, "run", lambda *, transport="stdio": calls.append(transport))
    assert mcp_srv.main([]) == 0
    assert calls == ["stdio"]

    proc = subprocess.run(
        [sys.executable, "scripts/nautilus_mcp_server.py", "--smoke"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    sub = json.loads(proc.stdout)
    assert sub["ok"] is True and sub["tools_count"] == 10

"""Offline unit & contract tests for Pine v6, TV alert bridge, Half-Auto tickets & console."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tier0_sota.tv import (
    build_half_auto_tickets,
    filter_extreme_movers,
    ingest_alerts_file,
    refresh_tickets_from_paths,
    refuse_live_endpoints,
    render_tickets_markdown,
    render_tv_watchlist,
    stage_tv_alert_preset,
    validate_tv_alert_payload,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
VALID_PINE_COLORS = frozenset(
    {
        "aqua",
        "black",
        "blue",
        "fuchsia",
        "gray",
        "green",
        "lime",
        "maroon",
        "navy",
        "olive",
        "orange",
        "purple",
        "red",
        "silver",
        "teal",
        "white",
        "yellow",
        "new",
        "rgb",
    }
)


def _valid_alert(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_version": "tier0.tv-alert.v1",
        "symbol": "NVDA",
        "ts_utc_ms": 1_792_098_000_000,
        "bar_confirmed": True,
        "side": "BUY",
        "qty": 1,
        "venue": "IBKR",
        "session": "PRE_MARKET",
        "order_type": "STOP_LIMIT",
        "trigger_price": 140.0,
        "stop_price": 138.25,
        "limit_collar_bps": 35.0,
        "trailing_schedule": "tight_near_lossless",
        "natr_pct": 0.012,
        "auto_reverse": True,
        "auto_reentry": True,
        "reentry_cooldown_secs": 60,
        "strategy_id": "SignalExecutionStrategy",
        "semi_auto_confirmed": False,
        "setup_archetype": "EARLY_EP_MOMENTUM_BURST",
        "macro_regime": 1,
        "rvol": 3.4,
        "is_leveraged_or_inverse": False,
        "data_source": "tradingview_pine_v6",
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize("pine_name", ["tier0_omni_sota_24x5.pine", "tier0_market_regime.pine"])
def test_pine_v6_non_repainting_balance_and_colors(pine_name: str) -> None:
    src = (REPO_ROOT / "pine" / pine_name).read_text(encoding="utf-8")
    assert src.startswith("//@version=6")
    assert "barmerge.lookahead_on" not in src
    assert "barmerge.lookahead_off" in src
    assert "barmerge.gaps_off" in src
    assert "9223" not in src and "CDP" not in src

    found_colors = set(re.findall(r"\bcolor\.([A-Za-z_][A-Za-z0-9_]*)\b", src))
    assert found_colors and found_colors.issubset(VALID_PINE_COLORS)

    stripped_lines = [
        re.sub(r'"(?:\\.|[^"\\])*"', '""', re.sub(r"//.*", "", line)) for line in src.splitlines()
    ]
    cleaned = "\n".join(stripped_lines)
    for op, cl in [("(", ")"), ("[", "]"), ("{", "}")]:
        assert cleaned.count(op) == cleaned.count(cl), f"{pine_name} unbalanced {op}{cl}"


def test_pine_omni_4tier_rlock_climax_reverse_and_60s_reentry() -> None:
    src = (REPO_ROOT / "pine" / "tier0_omni_sota_24x5.pine").read_text(encoding="utf-8")
    for token in (
        "barstate.isconfirmed",
        "i_prog_lock",
        "i_fast_sell_rev",
        "fast_sell_climax",
        "i_cooldown_sec  = input.int(60,",
        "lock_tier",
        "2.35 * leg_risk",
        "1.35 * leg_risk",
        "0.45 * leg_risk",
        r"\"semi_auto_confirmed\":false",
        r"\"qty\":1",
        r"\"tier0.tv-alert.v1\"",
        '"0.####"',
        '"0.######"',
    ):
        assert token in src
    assert '"#.####"' not in src and '"#.######"' not in src


def test_bridge_validates_all_sessions_rule612_and_refuses_violations() -> None:
    rth = validate_tv_alert_payload(
        _valid_alert(session="RTH", order_type="STOP_MARKET", limit_collar_bps=0.0)
    )
    assert rth["semi_auto_confirmed"] is False
    assert rth["qty"] == 1
    assert len(rth["alert_id"]) == 16

    sub_dollar = validate_tv_alert_payload(
        _valid_alert(
            symbol="PENNY",
            session="OVERNIGHT",
            trigger_price=0.80,
            stop_price=0.792,
            limit_collar_bps=50.0,
        )
    )
    assert sub_dollar["stop_price"] == pytest.approx(0.792)

    with pytest.raises(ValueError, match="exceeds"):
        validate_tv_alert_payload(_valid_alert(trigger_price=100.0, stop_price=97.99))

    with pytest.raises(ValueError, match="STOP_LIMIT|Off-hours"):
        validate_tv_alert_payload(
            _valid_alert(session="AFTER_HOURS", order_type="STOP_MARKET", limit_collar_bps=0.0)
        )

    with pytest.raises(ValueError, match="ALPACA overnight SHORT"):
        validate_tv_alert_payload(
            _valid_alert(
                side="SELL",
                venue="ALPACA",
                session="OVERNIGHT",
                trigger_price=100.0,
                stop_price=101.0,
                limit_collar_bps=50.0,
            ),
            venue="ALPACA",
        )

    with pytest.raises(ValueError, match="non-ETB"):
        validate_tv_alert_payload(
            _valid_alert(
                side="SELL",
                trigger_price=100.0,
                stop_price=101.0,
                shortable=False,
                easy_to_borrow=False,
            )
        )
    with pytest.raises(ValueError, match="borrow gate"):
        validate_tv_alert_payload(
            _valid_alert(side="SELL", trigger_price=100.0, stop_price=101.0),
            watchlist_manifest={"entries": [{"symbol": "NVDA", "shortable": False}]},
        )

    with pytest.raises(ValueError, match="live broker endpoint"):
        refuse_live_endpoints({"url": "https://api.alpaca.markets/v2/orders"})
    with pytest.raises(ValueError, match="live broker endpoint"):
        refuse_live_endpoints({"port": 4001})


def test_bridge_stage_and_ingest_atomic_round_trip(tmp_path: Path) -> None:
    staged_path = tmp_path / "staged.json"
    wl_path = tmp_path / "watchlist.json"
    wl_path.write_text('{"entries": []}', encoding="utf-8")

    alert_file = tmp_path / "alerts.json"
    alert_file.write_text(json.dumps([_valid_alert(), _valid_alert()]), encoding="utf-8")

    staged = ingest_alerts_file(alert_file, staged_path=staged_path, watchlist_path=wl_path)
    assert len(staged) == 2

    bundle = json.loads(staged_path.read_text(encoding="utf-8"))
    assert bundle["schema_version"] == "tier0.staged-tv-presets.v1"
    assert bundle["semi_auto_confirmed"] is False
    assert len(bundle["presets"]) == 1  # deduplicated by alert_id

    second = stage_tv_alert_preset(
        _valid_alert(symbol="AMD"), staged_path=staged_path, watchlist_path=wl_path
    )
    assert second["symbol"] == "AMD"
    bundle2 = json.loads(staged_path.read_text(encoding="utf-8"))
    assert len(bundle2["presets"]) == 2


def test_bridge_live_intake_xnys_and_borrow_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    # 2026-10-14 14:35:00 UTC = 10:35 ET (XNYS RTH Wednesday)
    ts_dt = datetime(2026, 10, 14, 14, 35, tzinfo=UTC)
    ts_ms = int(ts_dt.timestamp() * 1000)
    monkeypatch.setattr("tier0_sota.tv.bridge.time.time_ns", lambda: (ts_ms + 5_000) * 1_000_000)

    ok_buy = validate_tv_alert_payload(
        _valid_alert(
            ts_utc_ms=ts_ms,
            session="RTH",
            order_type="STOP_MARKET",
            trigger_price=100.0,
            stop_price=98.5,
            limit_collar_bps=0.0,
        ),
        live_intake=True,
    )
    assert ok_buy["stop_price"] == pytest.approx(98.5)

    sell_alert = _valid_alert(
        ts_utc_ms=ts_ms,
        side="SELL",
        session="RTH",
        order_type="STOP_MARKET",
        trigger_price=100.0,
        stop_price=101.5,
        limit_collar_bps=0.0,
        shortable=True,
        easy_to_borrow=True,
    )
    with pytest.raises(ValueError, match="borrow evidence"):
        validate_tv_alert_payload(sell_alert, watchlist_manifest={"entries": []}, live_intake=True)

    wl = {
        "entries": [
            {
                "symbol": "NVDA",
                "shortable": True,
                "easy_to_borrow": True,
                "borrow_source": "ibkr_paper_4002",
                "borrow_as_of_utc": ts_dt.isoformat(),
            }
        ]
    }
    ok_sell = validate_tv_alert_payload(sell_alert, watchlist_manifest=wl, live_intake=True)
    assert ok_sell["side"] == "SELL" and ok_sell["semi_auto_confirmed"] is False


def test_tickets_strict_extreme_filter_rungs_and_short_routing(tmp_path: Path) -> None:
    movers = {
        "generated_at_utc": "2026-11-25T14:35:00Z",
        "feed_sources": ["alpaca_screener", "alpaca_sip"],
        "macro_regime": {"regime": -1, "regime_label": "CRASH_DEFENSIVE"},
        "entries": [
            {
                "symbol": "AAPL",
                "last_price": 228.0,
                "session_move_pct": -0.2,
                "rvol_normalized": 0.9,
                "extreme_mover_hold_reason": "LOW_MOVE_NOT_EXTREME",
            },
            {
                "symbol": "NVDA",
                "last_price": 146.85,
                "session_move_pct": -3.4,
                "rvol_normalized": 3.2,
                "natr_pct": 0.012,
                "shortable": True,
                "easy_to_borrow": True,
            },
            {
                "symbol": "MEME",
                "last_price": 18.50,
                "session_move_pct": -6.1,
                "rvol_normalized": 5.8,
                "natr_pct": 0.042,
                "shortable": False,
                "easy_to_borrow": False,
            },
            {
                "symbol": "SOXS",
                "last_price": 24.10,
                "session_move_pct": 4.2,
                "rvol_normalized": 2.9,
                "natr_pct": 0.035,
                "extreme_mover_hold_reason": "LOW_RVOL_NOT_EXTREME",
            },
        ],
    }

    filtered = filter_extreme_movers(movers["entries"], regime_int=-1)
    assert [x["symbol"] for x in filtered] == ["NVDA", "MEME", "SOXS"]

    movers_path = tmp_path / "movers.json"
    movers_path.write_text(json.dumps(movers), encoding="utf-8")
    out_dir = tmp_path / "out"
    doc = refresh_tickets_from_paths(movers_path=movers_path, out_dir=out_dir)

    assert doc["schema_validated_count"] == 3
    assert doc["charter_guards"]["live_execution_enabled"] is False
    assert doc["charter_guards"]["semi_auto_confirmed"] is False

    by_sym = {t["symbol"]: t for t in doc["tickets"]}
    nvda = by_sym["NVDA"]
    assert nvda["side"] == "SELL"
    assert nvda["reverse_short_mode"] == "DIRECT_SHORT_ETB"
    assert nvda["lock_at_1r"] < nvda["live_price"]
    assert nvda["lock_at_2r"] < nvda["lock_at_1r"]
    assert nvda["fast_sell_rev_trigger"] > nvda["live_price"]

    meme = by_sym["MEME"]
    assert meme["side"] == "SELL"
    assert meme["reverse_short_mode"] == "INVERSE_HEDGE_SOXS_SQQQ"
    assert meme["action_gate"] == "USE_INVERSE_ETF_OR_VERIFY_BORROW"

    soxs = by_sym["SOXS"]
    assert soxs["side"] == "BUY"
    assert soxs["action_gate"] == "READY_HALF_AUTO_HEDGE"

    md = render_tickets_markdown(doc)
    wl = render_tv_watchlist(doc["tickets"])
    assert "DIRECT_SHORT_ETB" in md and "INVERSE_HEDGE_SOXS_SQQQ" in md
    assert "###03_LIVE_MOVERS_TOP15" in wl and "MEME" in wl and "AAPL" not in wl

    built = build_half_auto_tickets(movers, now_utc=datetime(2026, 11, 25, 15, 0, tzinfo=UTC))
    assert built["active_session"] == "RTH"


def test_tv_console_ps1_parses_cleanly_with_zero_cdp() -> None:
    ps1 = REPO_ROOT / "scripts" / "tv_console.ps1"
    src = ps1.read_text(encoding="utf-8")
    assert "9223" not in src
    assert "semi_auto_confirmed" in src

    pwsh_exe = shutil.which("pwsh")
    if os.name == "nt":
        assert pwsh_exe is not None
    if pwsh_exe is not None:
        cmd = (
            "$t=$null; $e=$null; "
            f"[void][System.Management.Automation.Language.Parser]::ParseFile('{ps1}', "
            "[ref]$t, [ref]$e); "
            "Write-Output $e.Count"
        )
        proc = subprocess.run(
            [pwsh_exe, "-NoProfile", "-Command", cmd],
            capture_output=True,
            text=True,
            check=True,
        )
        assert proc.stdout.strip() == "0"


def test_live_scan_stages_only_fresh_completed_mover_bars(tmp_path: Path) -> None:
    movers_path = tmp_path / "movers.json"
    out = tmp_path / "tv_sota_pack"
    mover = {
        "symbol": "NVDA",
        "last_price": 100,
        "extreme_mover_eligible": True,
        "bar_close_utc": "2026-11-25T15:00:00Z",
    }
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-25T15:00:03Z",
                "macro_regime": {"regime": 1},
                "entries": [mover, {**mover, "symbol": "SOXL", "extreme_mover_eligible": False}],
            }
        )
    )

    def refresh(now: datetime) -> list[dict[str, Any]]:
        refresh_tickets_from_paths(
            movers_path=movers_path, out_dir=out, venue="ALPACA", now_utc=now
        )
        return json.loads((out / "staged_scan_presets.json").read_text())["presets"]

    ts = datetime(2026, 11, 25, 15, 0, 10, tzinfo=UTC)
    (first,) = refresh(ts)
    assert first["symbol"] == "NVDA" and first["qty"] == 1 and first["semi_auto_confirmed"] is False
    assert first["data_source"] == "alpaca_sip" and refresh(ts)[0]["alert_id"] == first["alert_id"]
    assert refresh(ts.replace(minute=1)) == []
    assert refresh(ts.replace(day=26)) == []


def test_tv_half_auto_feedback_duckdb_finos_prometheus_and_otel(tmp_path: Path) -> None:
    import duckdb

    from tier0_sota.live.performance import (
        aggregate_paper_farm_performance,
        record_paper_farm_telemetry,
    )

    pack_dir = tmp_path / "tv_sota_pack"
    movers_path = tmp_path / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-25T15:00:03Z",
                "feed_sources": ["alpaca_sip"],
                "macro_regime": {"regime": 1, "regime_label": "BULL"},
                "entries": [
                    {
                        "symbol": "NVDA",
                        "last_price": 146.85,
                        "session_move_pct": 4.2,
                        "rvol_normalized": 3.5,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": "2026-11-25T15:00:00Z",
                        "shortable": True,
                        "easy_to_borrow": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ts = datetime(2026, 11, 25, 15, 0, 10, tzinfo=UTC)
    doc = refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=pack_dir, venue="ALPACA", now_utc=ts
    )
    assert doc["staged_count"] == 1

    stage_tv_alert_preset(
        _valid_alert(symbol="AMD"),
        staged_path=pack_dir / "staged_tv_presets.json",
        watchlist_path=movers_path,
    )
    audit_db = pack_dir / "tv_half_auto_audit.duckdb"
    assert audit_db.is_file()
    with duckdb.connect(str(audit_db), read_only=True) as conn:
        rows = conn.execute(
            "SELECT symbol, qty, semi_auto_confirmed, source FROM tv_staged_presets ORDER BY symbol"
        ).fetchall()
    assert len(rows) == 2
    assert all(r[1] == 1 and r[2] is False for r in rows)

    manifest = aggregate_paper_farm_performance(tmp_path)
    tv_meta = manifest["tv_half_auto"]
    assert tv_meta["semi_auto_confirmed"] is False
    assert tv_meta["live_execution_enabled"] is False
    assert tv_meta["tickets_count"] == 1
    assert tv_meta["staged_presets_total"] == 2
    assert tv_meta["duckdb_audit_rows"] == 2

    finos = json.loads((tmp_path / "finos_paper_farm_performance.json").read_text(encoding="utf-8"))
    assert finos["tv_half_auto"]["duckdb_audit_rows"] == 2

    from tier0_sota.tv.bridge import persist_tv_preset_duckdb

    with pytest.raises(ValueError, match="qty=1"):
        persist_tv_preset_duckdb(
            audit_db,
            {**_valid_alert(symbol="META"), "alert_id": "0" * 16, "qty": True},
        )
    with pytest.raises(ValueError, match="live broker endpoint"):
        persist_tv_preset_duckdb(
            audit_db,
            {**_valid_alert(symbol="META"), "alert_id": "1" * 16, "endpoint": "127.0.0.1:4001"},
        )

    # Staging at manifest_dir/staged_tv_presets.json routes audit rows into tv_sota_pack DuckDB.
    stage_tv_alert_preset(
        _valid_alert(symbol="SOXL"),
        staged_path=tmp_path / "staged_tv_presets.json",
        watchlist_path=movers_path,
    )
    manifest2 = aggregate_paper_farm_performance(tmp_path)
    assert manifest2["tv_half_auto"]["duckdb_audit_rows"] == 3

    telemetry = record_paper_farm_telemetry(manifest)
    for prom_m in (
        "tier0_tv_half_auto_tickets_count",
        "tier0_tv_half_auto_staged_presets_count",
        "tier0_tv_half_auto_schema_validated_count",
        "tier0_tv_half_auto_duckdb_audit_rows",
    ):
        assert prom_m in telemetry["prometheus_text"]
    assert {
        "tier0.tv_half_auto.tickets_count",
        "tier0.tv_half_auto.staged_presets_count",
        "tier0.tv_half_auto.schema_validated_count",
        "tier0.tv_half_auto.duckdb_audit_rows",
    } <= set(telemetry["otel_metric_names"])

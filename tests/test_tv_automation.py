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
    TvAlertV1,
    build_half_auto_tickets,
    current_et_session,
    filter_extreme_movers,
    ingest_alerts_file,
    load_tv_audit_summary,
    refresh_tickets_from_paths,
    refuse_live_endpoints,
    render_tickets_markdown,
    render_tv_watchlist,
    resolve_tv_exchange_symbol,
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


@pytest.mark.parametrize(
    "pine_name",
    [
        "tier0_omni_sota_24x5.pine",
        "tier0_sota_strategy_24x5.pine",
        "tier0_market_regime.pine",
        "tier0_darkpool_uv_buildup.pine",
    ],
)
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
        "AUTO_REGIME_ADAPTIVE",
        "LOSSLESS_TIGHT_SCALP",
        "CRASH_DEFENSIVE_INVERSE",
        "MINERVINI_VCP_BREAKOUT",
        "STOCKBEE_EP_MOMENTUM_BURST",
        "STOCKBEE_EXHAUSTION_HAMMER",
        "PARABOLIC_EXHAUSTION_SHORT",
        "OVERNIGHT_24X5_IBKR",
        "preset_sched = auto_sched",
        "EXHAUSTION_HAMMER",
        "0.0025 : is_ext ? 0.0035 : 0.0050",
        "0.0035 : is_ext ? 0.0050 : 0.0060",
        "0.0050 : is_ext ? 0.0050 : 0.0060",
        "i_prog_lock",
        "i_fast_sell_rev",
        "fast_sell_climax",
        "vol_buildup",
        "hidden_vol_ratio",
        r"\"hidden_volume_ratio\":",
        "Vol / Absorb",
        "i_cooldown_sec  = input.int(60,",
        "lock_tier",
        "2.35 * leg_risk",
        "1.35 * leg_risk",
        "0.45 * leg_risk",
        "math.min(trail_pct, math.max(0.0010, trail_pct * 0.25))",
        "math.min(trail_pct, math.max(0.0012, trail_pct * 0.40))",
        "math.min(trail_pct, math.max(0.0015, trail_pct * 0.65))",
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
    rth_ts = datetime(2026, 11, 25, 14, 35, tzinfo=UTC)
    doc = refresh_tickets_from_paths(movers_path=movers_path, out_dir=out_dir, now_utc=rth_ts)

    assert doc["schema_validated_count"] == 3
    assert doc["charter_guards"]["live_execution_enabled"] is False
    assert doc["charter_guards"]["semi_auto_confirmed"] is False

    by_sym = {t["symbol"]: t for t in doc["tickets"]}
    nvda = by_sym["NVDA"]
    assert nvda["side"] == "SELL"
    assert nvda["reverse_short_mode"] == "DIRECT_SHORT_ETB"
    assert nvda["lock_at_1r"] < nvda["live_price"]
    assert nvda["lock_at_2r"] < nvda["lock_at_1r"]
    assert nvda["lock_at_3r"] < nvda["lock_at_2r"]
    assert nvda["fast_sell_rev_trigger"] > nvda["live_price"]

    meme = by_sym["MEME"]
    assert meme["side"] == "SELL"
    assert meme["reverse_short_mode"] == "INVERSE_HEDGE_SOXS_SQQQ"
    assert meme["action_gate"] == "USE_INVERSE_ETF_OR_VERIFY_BORROW"

    soxs = by_sym["SOXS"]
    assert soxs["side"] == "BUY"
    assert soxs["action_gate"] == "READY_HALF_AUTO_HEDGE"
    assert soxs["lock_at_3r"] > soxs["lock_at_2r"] > soxs["lock_at_1r"] > soxs["live_price"]

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
    assert "classify_us_equity_session_et" in src
    assert "select_pooled_trailing_schedule" in src
    assert "get_session_protection_profile" in src
    assert "570 <= hm < 960" not in src
    assert "http://localhost:1420/" in src
    assert "http://127.0.0.1:1420/" not in src
    assert "Import list..." in src
    assert "function tv-land" in src
    assert "1. Pine Editor Ctrl+V -> Save" in src
    assert "2. tv-copy-regime -> Macro pane" in src
    assert "3. tv-copy-watchlist -> Watchlist ... Import list... Ctrl+V" in src
    assert "Loaded: tv-land |" in src
    for forbidden in ("api.alpaca.markets", "4001", "7496", "8798", "8796"):
        assert forbidden not in src

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


def test_xnys_holiday_early_close_and_overnight_alpaca_short_routing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    assert current_et_session(datetime(2026, 11, 26, 19, 59, tzinfo=ny)) == "CLOSED"
    assert current_et_session(datetime(2026, 11, 26, 20, 0, tzinfo=ny)) == "OVERNIGHT"
    assert current_et_session(datetime(2026, 11, 27, 12, 59, tzinfo=ny)) == "RTH"
    assert current_et_session(datetime(2026, 11, 27, 13, 0, tzinfo=ny)) == "AFTER_HOURS"
    assert current_et_session(datetime(2026, 11, 27, 13, 30, tzinfo=ny)) == "AFTER_HOURS"
    assert current_et_session(datetime(2026, 11, 27, 20, 0, tzinfo=ny)) == "CLOSED"

    v1_movers = {
        "generated_at_utc": "2026-11-27T18:30:00Z",
        "feed_sources": ["alpaca_sip"],
        "macro_regime": {"regime": -1, "regime_label": "CRASH_DEFENSIVE"},
        "entries": [
            {
                "symbol": "NVDA",
                "last_price": 140.0,
                "session_move_pct": -3.5,
                "rvol_normalized": 3.4,
                "natr_pct": 0.012,
                "shortable": True,
                "easy_to_borrow": True,
            },
            {
                "symbol": "MEME",
                "last_price": 15.0,
                "session_move_pct": -5.2,
                "rvol_normalized": 4.1,
                "natr_pct": 0.04,
                "shortable": False,
                "easy_to_borrow": False,
            },
            {
                "symbol": "SOXS",
                "last_price": 24.0,
                "session_move_pct": 4.0,
                "rvol_normalized": 3.1,
                "natr_pct": 0.032,
                "extreme_mover_hold_reason": "LOW_RVOL_NOT_EXTREME",
            },
        ],
    }

    closed_doc = build_half_auto_tickets(
        v1_movers, now_utc=datetime(2026, 11, 27, 0, 59, tzinfo=UTC), venue="ALPACA"
    )
    assert closed_doc["active_session"] == "CLOSED"
    assert closed_doc["tickets"] == [] and closed_doc["schema_validated_count"] == 0

    bf_after = build_half_auto_tickets(
        v1_movers, now_utc=datetime(2026, 11, 27, 18, 30, tzinfo=UTC), venue="ALPACA"
    )
    assert bf_after["active_session"] == "AFTER_HOURS"
    assert bf_after["tickets"][0]["order_type_now"] == "STOP_LIMIT"
    assert bf_after["tickets"][0]["collar_now_bps"] == 35

    # 2026-11-26 20:01:10 ET (2026-11-27 01:01:10 UTC) -> OVERNIGHT 50 bps collar, 1.00% cap
    on_bar = "2026-11-27T01:01:00Z"
    on_now = datetime(2026, 11, 27, 1, 1, 10, tzinfo=UTC)
    v2_movers_path = tmp_path / "movers_overnight.json"
    v2_movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T01:01:05Z",
                "feed_sources": ["alpaca_boats"],
                "macro_regime": {"regime": -1, "regime_label": "CRASH_DEFENSIVE"},
                "entries": [
                    {
                        **row,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": on_bar,
                    }
                    for row in v1_movers["entries"]
                ],
            }
        ),
        encoding="utf-8",
    )

    alpaca_dir = tmp_path / "alpaca_out"
    alp_doc = refresh_tickets_from_paths(
        movers_path=v2_movers_path, out_dir=alpaca_dir, venue="ALPACA", now_utc=on_now
    )
    assert alp_doc["active_session"] == "OVERNIGHT"
    alp_by_sym = {t["symbol"]: t for t in alp_doc["tickets"]}
    assert alp_by_sym["NVDA"]["order_type_now"] == "STOP_LIMIT"
    assert alp_by_sym["NVDA"]["collar_now_bps"] == 50
    assert alp_by_sym["NVDA"]["stop_now_pct"] <= 1.0
    assert alp_by_sym["NVDA"]["action_gate"] == "ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ"
    assert alp_by_sym["NVDA"]["reverse_short_mode"] == "ROUTE_IBKR_OR_INVERSE_HEDGE_SOXS_SQQQ"
    assert alp_by_sym["MEME"]["reverse_short_mode"] == "INVERSE_HEDGE_SOXS_SQQQ"
    assert alp_by_sym["SOXS"]["action_gate"] == "READY_HALF_AUTO_HEDGE"
    alp_staged = json.loads((alpaca_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    assert [p["symbol"] for p in alp_staged["presets"]] == ["SOXS"]
    assert alp_staged["presets"][0]["order_type"] == "STOP_LIMIT"
    assert alp_staged["presets"][0]["limit_collar_bps"] == 50.0

    ibkr_dir = tmp_path / "ibkr_out"
    ibkr_doc = refresh_tickets_from_paths(
        movers_path=v2_movers_path, out_dir=ibkr_dir, venue="IBKR", now_utc=on_now
    )
    ibkr_by_sym = {t["symbol"]: t for t in ibkr_doc["tickets"]}
    assert ibkr_by_sym["NVDA"]["reverse_short_mode"] == "DIRECT_SHORT_ETB"
    ibkr_staged = json.loads((ibkr_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    assert sorted(p["symbol"] for p in ibkr_staged["presets"]) == ["NVDA", "SOXS"]

    # Live intake bridge parity for 20:00 ET OVERNIGHT and Black Friday 13:00 ET AFTER_HOURS.
    on_ts_ms = int(datetime(2026, 11, 27, 1, 0, tzinfo=UTC).timestamp() * 1000)
    monkeypatch.setattr("tier0_sota.tv.bridge.time.time_ns", lambda: (on_ts_ms + 5_000) * 1_000_000)
    on_wl = {
        "entries": [
            {
                "symbol": "NVDA",
                "shortable": True,
                "easy_to_borrow": True,
                "borrow_source": "ibkr_paper_4002",
                "borrow_as_of_utc": "2026-11-27T01:00:00+00:00",
            }
        ]
    }
    on_sell = _valid_alert(
        ts_utc_ms=on_ts_ms,
        side="SELL",
        venue="IBKR",
        session="OVERNIGHT",
        order_type="STOP_LIMIT",
        trigger_price=140.0,
        stop_price=141.4,
        limit_collar_bps=50.0,
        shortable=True,
        easy_to_borrow=True,
    )
    ok_on = validate_tv_alert_payload(
        on_sell, watchlist_manifest=on_wl, venue="IBKR", live_intake=True
    )
    assert ok_on["session"] == "OVERNIGHT" and ok_on["limit_collar_bps"] == 50.0
    with pytest.raises(ValueError, match="ALPACA overnight SHORT"):
        validate_tv_alert_payload(
            {**on_sell, "venue": "ALPACA"},
            watchlist_manifest=on_wl,
            venue="ALPACA",
            live_intake=True,
        )

    bf_ts_ms = int(datetime(2026, 11, 27, 18, 0, tzinfo=UTC).timestamp() * 1000)
    monkeypatch.setattr("tier0_sota.tv.bridge.time.time_ns", lambda: (bf_ts_ms + 5_000) * 1_000_000)
    with pytest.raises(ValueError, match="XNYS bar close"):
        validate_tv_alert_payload(
            _valid_alert(
                ts_utc_ms=bf_ts_ms,
                session="RTH",
                order_type="STOP_MARKET",
                trigger_price=140.0,
                stop_price=137.9,
                limit_collar_bps=0.0,
            ),
            live_intake=True,
        )
    ok_bf = validate_tv_alert_payload(
        _valid_alert(
            ts_utc_ms=bf_ts_ms,
            session="AFTER_HOURS",
            order_type="STOP_LIMIT",
            trigger_price=140.0,
            stop_price=138.25,
            limit_collar_bps=35.0,
        ),
        live_intake=True,
    )
    assert ok_bf["session"] == "AFTER_HOURS" and ok_bf["limit_collar_bps"] == 35.0


def test_live_v2_action_gate_graduation_prior_close_borrow_and_pairs(tmp_path: Path) -> None:
    # Overnight BOATS (2026-11-26 20:10:10 ET -> 2026-11-27T01:10:10Z), regime=0, no bundle file.
    on_bar = "2026-11-27T01:10:00Z"
    on_now = datetime(2026, 11, 27, 1, 10, 10, tzinfo=UTC)
    movers_path = tmp_path / "movers_v2.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T01:10:05Z",
                "feed_sources": ["alpaca_boats"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "symbol": "SOXS",
                        "last_price": 60.27,
                        "prior_close": 52.21,
                        "annual_borrow_rate": 0.0099,
                        "session_move_pct": 15.44,
                        "rvol_normalized": 3.2,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": on_bar,
                        "shortable": False,
                        "easy_to_borrow": False,
                    },
                    {
                        "symbol": "SQQQ",
                        "last_price": 31.50,
                        "prior_close": 29.80,
                        "annual_borrow_rate": 0.0095,
                        "session_move_pct": 5.70,
                        "rvol_normalized": 2.4,
                        "natr_pct": 0.014,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": on_bar,
                        "shortable": False,
                        "easy_to_borrow": False,
                    },
                    {
                        "symbol": "TQQQ",
                        "last_price": 78.40,
                        "prior_close": 74.10,
                        "annual_borrow_rate": 0.0,
                        "session_move_pct": 5.80,
                        "rvol_normalized": 3.5,
                        "natr_pct": 0.018,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": on_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    out_dir = tmp_path / "tv_sota_pack"
    doc = refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=out_dir, bundle_path=None, venue="ALPACA", now_utc=on_now
    )
    assert doc["active_session"] == "OVERNIGHT"
    assert doc["pin_symbols"] == ["SOXL", "SOXS", "TQQQ", "SQQQ"]
    assert doc["staged_count"] == 3
    by_sym = {t["symbol"]: t for t in doc["tickets"]}

    assert by_sym["SOXS"]["action_gate"] == "READY_HALF_AUTO_HEDGE"
    assert by_sym["SOXS"]["pair_symbol"] == "SOXL"
    assert by_sym["SOXS"]["prior_close"] == pytest.approx(52.21)
    assert by_sym["SOXS"]["annual_borrow_rate"] == pytest.approx(0.0099)

    assert by_sym["SQQQ"]["action_gate"] == "READY_HALF_AUTO_HEDGE"
    assert by_sym["SQQQ"]["pair_symbol"] == "TQQQ"
    assert by_sym["SQQQ"]["prior_close"] == pytest.approx(29.80)
    assert by_sym["SQQQ"]["annual_borrow_rate"] == pytest.approx(0.0095)

    assert by_sym["TQQQ"]["action_gate"] == "READY_HALF_AUTO"
    assert by_sym["TQQQ"]["pair_symbol"] == "SQQQ"
    assert by_sym["TQQQ"]["prior_close"] == pytest.approx(74.10)
    assert by_sym["TQQQ"]["annual_borrow_rate"] == 0.0

    staged = json.loads((out_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    staged_by_sym = {p["symbol"]: p for p in staged["presets"]}
    assert staged_by_sym["SOXS"]["pair_symbol"] == "SOXL"
    assert staged_by_sym["SQQQ"]["pair_symbol"] == "TQQQ"
    assert staged_by_sym["TQQQ"]["pair_symbol"] == "SQQQ"
    assert all(p["qty"] == 1 and p["semi_auto_confirmed"] is False for p in staged["presets"])

    md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    assert "READY_HALF_AUTO_HEDGE" in md and "READY_HALF_AUTO" in md
    assert "↔SOXL" in md and "↔TQQQ" in md and "↔SQQQ" in md
    assert "prev `52.21`" in md and "fee 0.99%/yr" in md

    # Crash SELL in RTH graduates to READY_HALF_AUTO when ETB & bundle absent, stays blocked if
    # non-ETB or if bundle holds explicitly.
    rth_bar = "2026-11-25T15:00:00Z"
    rth_now = datetime(2026, 11, 25, 15, 0, 10, tzinfo=UTC)
    rth_movers = {
        "schema_version": "tier0.whole-market-movers.v2",
        "generated_at_utc": "2026-11-25T15:00:05Z",
        "feed_sources": ["alpaca_sip"],
        "macro_regime": {"regime": -1, "regime_label": "CRASH"},
        "entries": [
            {
                "symbol": "NVDA",
                "last_price": 140.0,
                "prior_close": 146.5,
                "annual_borrow_rate": 0.0,
                "session_move_pct": -4.4,
                "rvol_normalized": 3.6,
                "natr_pct": 0.014,
                "extreme_mover_eligible": True,
                "bar_close_utc": rth_bar,
                "shortable": True,
                "easy_to_borrow": True,
            },
            {
                "symbol": "MEME",
                "last_price": 18.0,
                "session_move_pct": -6.0,
                "rvol_normalized": 4.2,
                "natr_pct": 0.035,
                "extreme_mover_eligible": True,
                "bar_close_utc": rth_bar,
                "shortable": False,
                "easy_to_borrow": False,
            },
        ],
    }
    rth_doc = build_half_auto_tickets(rth_movers, bundle=None, now_utc=rth_now, venue="ALPACA")
    rth_by_sym = {t["symbol"]: t for t in rth_doc["tickets"]}
    assert rth_by_sym["NVDA"]["action_gate"] == "READY_HALF_AUTO"
    assert rth_by_sym["MEME"]["action_gate"] == "USE_INVERSE_ETF_OR_VERIFY_BORROW"

    held_doc = build_half_auto_tickets(
        rth_movers,
        bundle={
            "top_potential_symbols": [
                {"symbol": "NVDA", "entry_eligible": False, "entry_hold_reason": "COOLDOWN"}
            ]
        },
        now_utc=rth_now,
        venue="ALPACA",
    )
    assert held_doc["tickets"][0]["action_gate"] == "HOLD (COOLDOWN)"


def test_darkpool_unusual_vol_6section_watchlist_and_compounding_manifest(tmp_path: Path) -> None:
    import hashlib

    rth_bar = "2026-11-25T15:00:00Z"
    rth_now = datetime(2026, 11, 25, 15, 0, 10, tzinfo=UTC)
    movers_path = tmp_path / "movers_darkpool.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-25T15:00:05Z",
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "symbol": "PLTR",
                        "score": 81.5,
                        "dollar_vol": 42_500_000.0,
                        "last_price": 68.40,
                        "prior_close": 65.10,
                        "annual_borrow_rate": 0.0,
                        "session_move_pct": 1.6,
                        "rvol_normalized": 2.3,
                        "darkpool_trf_ratio": 0.44,
                        "hidden_volume_ratio": 3.12,
                        "vol_buildup_ratio": 2.15,
                        "unusual_volume_buildup": True,
                        "peak_confirmed": False,
                        "natr_pct": 0.014,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": rth_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                    {
                        "symbol": "AMD",
                        "score": 64.2,
                        "dollar_vol": 29_100_000.0,
                        "last_price": 152.20,
                        "prior_close": 149.80,
                        "annual_borrow_rate": 0.0,
                        "session_move_pct": 1.4,
                        "rvol_normalized": 1.7,
                        "darkpool_trf_ratio": 0.22,
                        "hidden_volume_ratio": 1.45,
                        "vol_buildup_ratio": 1.9,
                        "unusual_volume_buildup": True,
                        "peak_confirmed": False,
                        "natr_pct": 0.016,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": rth_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                    {
                        "symbol": "NVDA",
                        "score": 92.0,
                        "dollar_vol": 120_000_000.0,
                        "last_price": 146.85,
                        "prior_close": 140.00,
                        "annual_borrow_rate": 0.0,
                        "session_move_pct": 4.89,
                        "rvol_normalized": 3.8,
                        "darkpool_trf_ratio": 0.38,
                        "hidden_volume_ratio": 2.4,
                        "vol_buildup_ratio": 1.4,
                        "unusual_volume_buildup": False,
                        "peak_confirmed": True,
                        "natr_pct": 0.013,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": rth_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "tv_sota_pack"
    doc = refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=out_dir, venue="ALPACA", now_utc=rth_now
    )
    by_sym = {t["symbol"]: t for t in doc["tickets"]}

    # PLTR & AMD graduate to HIDDEN_VOL_ACCUM; NVDA stays EARLY_EP_MOMENTUM_BURST (EP wins).
    assert by_sym["PLTR"]["archetype"] == "HIDDEN_VOL_ACCUM"
    assert by_sym["PLTR"]["darkpool_trf_ratio"] == pytest.approx(0.44)
    assert by_sym["PLTR"]["hidden_volume_ratio"] == pytest.approx(3.12)
    assert by_sym["PLTR"]["vol_buildup_ratio"] == pytest.approx(2.15)
    assert by_sym["PLTR"]["unusual_volume_buildup"] is True
    assert by_sym["PLTR"]["dollar_vol"] == pytest.approx(42_500_000.0)
    assert by_sym["PLTR"]["score"] == pytest.approx(81.5)
    assert by_sym["PLTR"]["tv_alert_preset"]["hidden_volume_ratio"] == pytest.approx(3.12)
    assert by_sym["AMD"]["archetype"] == "HIDDEN_VOL_ACCUM"
    assert by_sym["NVDA"]["archetype"] == "EARLY_EP_MOMENTUM_BURST"
    assert by_sym["NVDA"]["tv_alert_preset"]["peak_confirmed"] is True

    staged = json.loads((out_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    staged_by_sym = {p["symbol"]: p for p in staged["presets"]}
    assert staged_by_sym["PLTR"]["setup_archetype"] == "HIDDEN_VOL_ACCUM"
    assert staged_by_sym["PLTR"]["hidden_volume_ratio"] == pytest.approx(3.12)
    assert staged_by_sym["NVDA"]["peak_confirmed"] is True

    md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    assert "| TRF / UV |" in md
    assert "`44%` (UV 2.1x · HVR 3.1)" in md or "`44%` (UV 2.2x · HVR 3.1)" in md

    wl = (out_dir / "tier0_sota_tv_watchlists_live.txt").read_text(encoding="utf-8")
    for sec in (
        "###01_MACRO_REGIME_BREADTH",
        "###02_CRASH_DEFENSIVE_HEDGES",
        "###03_LIVE_MOVERS_TOP15",
        "###04_DARKPOOL_UNUSUAL_VOL",
        "###05_BIDIRECTIONAL_PAIRS",
        "###06_OVERNIGHT_24X5_LEADERS",
    ):
        assert sec in wl
    dp_block = wl.split("###04_DARKPOOL_UNUSUAL_VOL\n", 1)[1].split("###05_BIDIRECTIONAL_PAIRS", 1)[
        0
    ]
    assert "NASDAQ:PLTR" in dp_block and "NASDAQ:AMD" in dp_block and "NASDAQ:NVDA" in dp_block
    pairs_block = wl.split("###05_BIDIRECTIONAL_PAIRS\n", 1)[1].split(
        "###06_OVERNIGHT_24X5_LEADERS", 1
    )[0]
    for p_sym in (
        "AMEX:SOXL",
        "AMEX:SOXS",
        "NASDAQ:TQQQ",
        "NASDAQ:SQQQ",
        "AMEX:UPRO",
        "AMEX:SPXS",
    ):
        assert p_sym in pairs_block
    leaders_block = wl.split("###06_OVERNIGHT_24X5_LEADERS\n", 1)[1]
    for ldr in ("NVDA", "TSLA", "AMD", "PLTR", "SMCI", "MSTR", "COIN", "AVGO", "META"):
        assert f"NASDAQ:{ldr}" in leaders_block

    comp = json.loads((out_dir / "tv_compounding_manifest.json").read_text(encoding="utf-8"))
    expected_sha = hashlib.sha256(
        (REPO_ROOT / "pine" / "tier0_omni_sota_24x5.pine").read_bytes()
    ).hexdigest()
    assert comp["schema_version"] == "tier0.tv-compounding-manifest.v1"
    assert comp["paper_only"] is True
    assert comp["semi_auto_confirmed"] is False
    assert comp["live_execution_enabled"] is False
    assert comp["qty"] == 1
    assert comp["active_session"] == "RTH"
    assert comp["tickets_count"] == 3
    assert comp["staged_count"] == 3
    assert comp["unusual_vol_count"] == 2
    assert comp["darkpool_hits_count"] == 2
    assert comp["pin_symbols"] == ["SOXL", "SOXS", "TQQQ", "SQQQ"]
    assert comp["pine_omni_sha256"] == expected_sha
    assert [x["symbol"] for x in comp["top_symbols"]] == ["PLTR", "AMD", "NVDA"]


def test_watchlist_fallback_standby_presets_and_tv_order_template(tmp_path: Path) -> None:
    # Quiet OVERNIGHT tick (2026-11-26 21:22 ET -> 2026-11-27T02:22:10Z): no entry clears strict
    # extreme_mover_eligible=True, yet SOXS (+6.30%, HVR 1.154) and SOXL (-4.45%) are live ranked.
    on_bar = "2026-11-27T02:22:00Z"
    on_now = datetime(2026, 11, 27, 2, 22, 10, tzinfo=UTC)
    movers_path = tmp_path / "movers_quiet_overnight.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T02:22:05Z",
                "feed_sources": ["alpaca_boats"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "symbol": "SOXS",
                        "score": 55.2,
                        "dollar_vol": 9_100_000.0,
                        "last_price": 55.50,
                        "prior_close": 52.21,
                        "annual_borrow_rate": 0.0099,
                        "session_move_pct": 6.30,
                        "rvol_normalized": 0.62,
                        "hidden_volume_ratio": 1.154,
                        "vol_buildup_ratio": 1.05,
                        "unusual_volume_buildup": False,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": on_bar,
                        "shortable": False,
                        "easy_to_borrow": False,
                    },
                    {
                        "symbol": "SOXL",
                        "score": 41.8,
                        "dollar_vol": 14_200_000.0,
                        "last_price": 35.44,
                        "prior_close": 37.09,
                        "annual_borrow_rate": 0.0,
                        "session_move_pct": -4.45,
                        "rvol_normalized": 0.54,
                        "hidden_volume_ratio": 0.85,
                        "vol_buildup_ratio": 0.92,
                        "unusual_volume_buildup": False,
                        "natr_pct": 0.014,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": on_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                    {
                        "symbol": "ZERO",
                        "last_price": 0.0,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": on_bar,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "tv_sota_pack"
    doc = refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=out_dir, venue="ALPACA", now_utc=on_now
    )

    # Strict staging stays 100% fail-closed: 0 tickets, 0 staged presets, no DuckDB audit row.
    assert doc["active_session"] == "OVERNIGHT"
    assert doc["tickets"] == [] and doc["schema_validated_count"] == 0 and doc["staged_count"] == 0
    assert doc["charter_guards"]["semi_auto_confirmed"] is False
    assert doc["charter_guards"]["qty"] == 1
    staged = json.loads((out_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    assert staged["presets"] == [] and staged["semi_auto_confirmed"] is False
    assert not (out_dir / "tv_half_auto_audit.duckdb").exists()

    # Standby presets compute exact Rule-612 OVERNIGHT levels without staging.
    standby = doc["standby_presets"]
    assert [s["symbol"] for s in standby] == ["SOXS", "SOXL"]
    soxs = standby[0]
    assert soxs["action_gate"] == "MONITOR_ONLY (<EXTREME GATE)"
    assert soxs["staged"] is False and soxs["semi_auto_confirmed"] is False and soxs["qty"] == 1
    assert "tv_alert_preset" not in soxs
    assert soxs["order_type_now"] == "STOP_LIMIT" and soxs["collar_now_bps"] == 50
    assert soxs["stop_now_pct"] <= 1.0
    assert soxs["lock_at_3r"] > soxs["lock_at_2r"] > soxs["lock_at_1r"] > soxs["be_plus_fee_floor"]

    # Watchlist ###03 and ###04 populate cleanly from ranked_entries when tickets=[].
    wl = (out_dir / "tier0_sota_tv_watchlists_live.txt").read_text(encoding="utf-8")
    movers_block = wl.split("###03_LIVE_MOVERS_TOP15\n", 1)[1].split(
        "###04_DARKPOOL_UNUSUAL_VOL", 1
    )[0]
    assert "AMEX:SOXS" in movers_block and "AMEX:SOXL" in movers_block
    assert "ZERO" not in movers_block
    dp_block = wl.split("###04_DARKPOOL_UNUSUAL_VOL\n", 1)[1].split("###05_BIDIRECTIONAL_PAIRS", 1)[
        0
    ]
    assert "AMEX:SOXS" in dp_block and "AMEX:SOXL" not in dp_block

    # Markdown renders Standby table + Native TradingView Order Preset Template footer.
    md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    assert "## Standby Ranked Reference Presets (MONITOR_ONLY" in md
    assert "MONITOR_ONLY (<EXTREME GATE)" in md and "`55.50`" in md and "HVR 1.2" in md
    assert "## Native TradingView Order Preset Template" in md
    assert "STOP_LIMIT" in md and "50 bps" in md and "STOP_MARKET" in md

    # CLOSED session keeps standby_presets strictly empty (fail-closed).
    closed_doc = build_half_auto_tickets(
        json.loads(movers_path.read_text(encoding="utf-8")),
        now_utc=datetime(2026, 11, 27, 0, 59, tzinfo=UTC),
        venue="ALPACA",
    )
    assert closed_doc["active_session"] == "CLOSED" and closed_doc["standby_presets"] == []


def test_schema_v1_json_exact_parity_with_pydantic_tv_alert_v1() -> None:
    schema_path = REPO_ROOT / "src" / "tier0_sota" / "tv" / "schema_v1.json"
    s1 = json.loads(schema_path.read_text(encoding="utf-8"))
    pyd = TvAlertV1.model_json_schema()

    assert s1["additionalProperties"] is False
    assert set(s1["required"]) == set(pyd["required"])
    assert set(s1["properties"].keys()) == set(pyd["properties"].keys())
    for new_key in (
        "darkpool_trf_ratio",
        "vol_buildup_ratio",
        "unusual_volume_buildup",
        "script_kind",
        "order_preset_id",
    ):
        assert new_key in s1["properties"]
    assert s1["properties"]["trigger_price"]["exclusiveMaximum"] == 1_000_000_000
    assert s1["properties"]["stop_price"]["exclusiveMaximum"] == 1_000_000_000
    assert s1["properties"]["darkpool_trf_ratio"] == {
        "type": "number",
        "minimum": 0,
        "maximum": 1.0,
    }
    assert s1["properties"]["script_kind"]["enum"] == ["indicator", "strategy"]

    # validate_tv_alert_payload must preserve optional fields (and omit None keys).
    out = validate_tv_alert_payload(
        _valid_alert(
            darkpool_trf_ratio=0.47,
            hidden_volume_ratio=2.8,
            vol_buildup_ratio=2.3,
            unusual_volume_buildup=True,
            peak_confirmed=False,
            script_kind="strategy",
            order_preset_id="tight_near_lossless:PRE_MARKET:EARLY_EP_MOMENTUM_BURST",
            pair_symbol="SOXS",
            shortable=True,
            easy_to_borrow=True,
        )
    )
    assert out["macro_regime"] == 1
    assert out["rvol"] == pytest.approx(3.4)
    assert out["is_leveraged_or_inverse"] is False
    assert out["shortable"] is True
    assert out["easy_to_borrow"] is True
    assert out["darkpool_trf_ratio"] == pytest.approx(0.47)
    assert out["vol_buildup_ratio"] == pytest.approx(2.3)
    assert out["unusual_volume_buildup"] is True
    assert out["script_kind"] == "strategy"
    assert out["order_preset_id"] == "tight_near_lossless:PRE_MARKET:EARLY_EP_MOMENTUM_BURST"
    assert all(v is not None for v in out.values())
    TvAlertV1.model_validate(out)

    # Reject out-of-range TRF ratio and bool coercion on optional float fields.
    with pytest.raises(ValueError, match="Invalid TV alert schema"):
        validate_tv_alert_payload(_valid_alert(darkpool_trf_ratio=1.25))
    with pytest.raises(ValueError, match="Invalid TV alert schema"):
        validate_tv_alert_payload(_valid_alert(vol_buildup_ratio=True))


def test_bridge_duckdb_migration_parquet_and_default_movers_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import duckdb

    from tier0_sota.tv import bridge as tv_bridge

    # Seed a legacy 15-column tv_staged_presets DuckDB to verify non-destructive migration.
    pack_dir = tmp_path / "tv_sota_pack"
    pack_dir.mkdir(parents=True)
    db_path = pack_dir / "tv_half_auto_audit.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(tv_bridge._DUCKDB_BASE_SCHEMA_SQL)
        conn.execute(
            "INSERT INTO tv_staged_presets VALUES "
            "('aaaaaaaaaaaaaaaa', 'SOXS', 'BUY', 1, 'ALPACA', 'OVERNIGHT', 'STOP_LIMIT', "
            "54.0, 53.46, 50.0, 'tight_near_lossless', FALSE, 'alpaca_boats', "
            "'scanner_tickets', '2026-11-27T02:00:00+00:00')"
        )

    # Default watchlist fallback: missing live_watchlist_manifest.json -> movers manifest.
    ts_dt = datetime(2026, 10, 14, 14, 35, tzinfo=UTC)
    ts_ms = int(ts_dt.timestamp() * 1000)
    monkeypatch.setattr("tier0_sota.tv.bridge.time.time_ns", lambda: (ts_ms + 5_000) * 1_000_000)
    fake_wl = tmp_path / "live_watchlist_manifest.json"
    fake_movers = tmp_path / "live_whole_market_movers_manifest.json"
    fake_movers.write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(tv_bridge, "DEFAULT_WATCHLIST_PATH", fake_wl)
    monkeypatch.setattr(tv_bridge, "DEFAULT_MOVERS_PATH", fake_movers)

    staged = stage_tv_alert_preset(
        _valid_alert(
            ts_utc_ms=ts_ms,
            side="SELL",
            session="RTH",
            order_type="STOP_MARKET",
            trigger_price=100.0,
            stop_price=101.5,
            limit_collar_bps=0.0,
            shortable=True,
            easy_to_borrow=True,
            darkpool_trf_ratio=0.42,
            vol_buildup_ratio=2.4,
            unusual_volume_buildup=True,
            script_kind="strategy",
            order_preset_id="tight_near_lossless:RTH:PEAK_REVERSAL",
        ),
        staged_path=pack_dir / "staged_tv_presets.json",
        watchlist_path=fake_wl,
        venue="IBKR",
        live_intake=True,
    )
    assert staged["darkpool_trf_ratio"] == pytest.approx(0.42)
    assert staged["script_kind"] == "strategy"

    # Migrated DuckDB retains legacy row + new enriched columns + Parquet snapshot.
    pq_path = pack_dir / "tv_staged_presets.parquet"
    assert pq_path.is_file()
    summary = load_tv_audit_summary(db_path)
    assert summary["audit_rows"] == 2
    assert summary["parquet_snapshot_present"] is True
    assert set(summary["distinct_symbols"]) == {"NVDA", "SOXS"}

    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "SELECT darkpool_trf_ratio, vol_buildup_ratio, unusual_volume_buildup, "
            "script_kind, order_preset_id, rvol, macro_regime "
            "FROM tv_staged_presets WHERE symbol = 'NVDA'"
        ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(0.42)
    assert row[1] == pytest.approx(2.4)
    assert row[2] is True
    assert row[3] == "strategy"
    assert row[4] == "tight_near_lossless:RTH:PEAK_REVERSAL"
    assert row[5] == pytest.approx(3.4)
    assert row[6] == 1


def test_zero_ticket_refresh_migrates_existing_legacy_duckdb_and_exports_parquet(
    tmp_path: Path,
) -> None:
    import duckdb

    from tier0_sota.tv import bridge as tv_bridge

    pack_dir = tmp_path / "tv_sota_pack"
    pack_dir.mkdir(parents=True)
    db_path = pack_dir / "tv_half_auto_audit.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(tv_bridge._DUCKDB_BASE_SCHEMA_SQL)
        conn.execute(
            "INSERT INTO tv_staged_presets VALUES "
            "('bbbbbbbbbbbbbbbb', 'SOXS', 'BUY', 1, 'ALPACA', 'OVERNIGHT', 'STOP_LIMIT', "
            "54.0, 53.46, 50.0, 'tight_near_lossless', FALSE, 'alpaca_boats', "
            "'scanner_tickets', '2026-11-27T02:00:00+00:00')"
        )
        assert len(conn.execute("DESCRIBE tv_staged_presets").fetchall()) == 15

    on_bar = "2026-11-27T02:22:00Z"
    on_now = datetime(2026, 11, 27, 2, 22, 10, tzinfo=UTC)
    movers_path = tmp_path / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T02:22:05Z",
                "feed_sources": ["alpaca_boats"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "symbol": "SOXS",
                        "last_price": 54.31,
                        "session_move_pct": 1.2,
                        "rvol_normalized": 0.6,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": on_bar,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=pack_dir, venue="ALPACA", now_utc=on_now
    )
    assert doc["tickets"] == [] and doc["staged_count"] == 0

    pq_path = pack_dir / "tv_staged_presets.parquet"
    assert pq_path.is_file()
    with duckdb.connect(str(db_path), read_only=True) as conn:
        cols = conn.execute("DESCRIBE tv_staged_presets").fetchall()
        assert len(cols) == 28
        pq_rows = conn.execute(
            "SELECT alert_id, symbol FROM read_parquet(?)", [str(pq_path)]
        ).fetchall()
        assert pq_rows == [("bbbbbbbbbbbbbbbb", "SOXS")]

    summary = load_tv_audit_summary(db_path)
    assert summary["audit_rows"] == 1 and summary["parquet_snapshot_present"] is True
    comp = json.loads((pack_dir / "tv_compounding_manifest.json").read_text(encoding="utf-8"))
    assert comp["duckdb_audit_rows"] == 1


def test_tickets_order_presets_cards_finos_lwc_v5_exchange_watchlist_and_pine_sync(
    tmp_path: Path,
) -> None:
    on_bar = "2026-11-27T02:22:00Z"
    on_now = datetime(2026, 11, 27, 2, 22, 10, tzinfo=UTC)
    out_dir = tmp_path / "tv_sota_pack"

    # Seed a Pine/manual staged alert and Paper Farm execution status to verify closed-loop merge.
    stage_tv_alert_preset(
        _valid_alert(
            symbol="MSTR",
            session="OVERNIGHT",
            order_type="STOP_LIMIT",
            trigger_price=84.0,
            stop_price=83.16,
            limit_collar_bps=50.0,
            script_kind="strategy",
            order_preset_id="OVERNIGHT_24X5_IBKR",
        ),
        staged_path=out_dir / "staged_tv_presets.json",
        watchlist_path=tmp_path / "unused.json",
        venue="IBKR",
    )
    (tmp_path / "paper_farm_execution_status.json").write_text(
        json.dumps({"status": "SUPERVISING", "managed_symbols": ["SNDK"]}),
        encoding="utf-8",
    )
    bundle_path = tmp_path / "live_lane_order_preset_bundle.json"
    bundle_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.live-lane-preset-bundle.v1",
                "tca_calibration_ok": True,
                "top_potential_symbols": [
                    {
                        "symbol": "SNDK",
                        "lane_account": "ALPACA_PA1",
                        "lane_id": "L1_ORB_BURST",
                        "origin": "MOMENTUM_BURST_LONG",
                        "entry_eligible": True,
                        "entry_hold_reason": None,
                    },
                    {
                        "symbol": "SMCI",
                        "lane_account": "ALPACA_PA2",
                        "lane_id": "L2_STRUCTURAL_TREND",
                        "origin": "DIRECT_SHORT_ETB",
                        "entry_eligible": True,
                        "entry_hold_reason": None,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    movers_path = tmp_path / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T02:22:05Z",
                "feed_sources": ["alpaca_boats", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "SOXS",
                        "exchange": "ARCX",
                        "score": 32.0,
                        "last_price": 54.31,
                        "prior_close": 52.21,
                        "session_move_pct": 4.02,
                        "rvol_normalized": 0.6,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": False,
                        "pinned": True,
                        "bar_close_utc": on_bar,
                    },
                    {
                        "rank": 2,
                        "symbol": "SNDK",
                        "exchange": "XNAS",
                        "score": 48.0,
                        "last_price": 79.33,
                        "prior_close": 75.06,
                        "session_move_pct": 5.69,
                        "rvol_normalized": 2.75,
                        "darkpool_trf_ratio": 0.43,
                        "darkpool_trf_source": "prior_sip_trf_carryover",
                        "darkpool_block_count": 4,
                        "trf_block_ratio": 0.28,
                        "hidden_volume_ratio": 2.1,
                        "vol_buildup_ratio": 2.2,
                        "unusual_volume_buildup": True,
                        "peak_confirmed": False,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": on_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                    {
                        "rank": 3,
                        "symbol": "SMCI",
                        "exchange": "NASDAQ",
                        "score": 44.0,
                        "last_price": 42.50,
                        "prior_close": 44.00,
                        "session_move_pct": -3.41,
                        "rvol_normalized": 2.6,
                        "darkpool_trf_ratio": 0.39,
                        "peak_confirmed": True,
                        "side": "SELL",
                        "natr_pct": 0.014,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": on_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    },
                    {
                        "rank": 4,
                        "symbol": "CRWV",
                        "exchange": "XNYS",
                        "score": 25.0,
                        "last_price": 51.00,
                        "session_move_pct": 1.8,
                        "rvol_normalized": 2.1,
                        "darkpool_trf_ratio": 0.41,
                        "unusual_volume_buildup": True,
                        "watch_tier": "UV_DARKPOOL_BUILDUP",
                        "extreme_mover_eligible": False,
                        "extreme_mover_hold_reason": "LOW_MOVE_NOT_EXTREME",
                        "natr_pct": 0.013,
                        "bar_close_utc": on_bar,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        bundle_path=bundle_path,
        out_dir=out_dir,
        venue="IBKR",
        now_utc=on_now,
    )
    # Sequential active ticket rank (1, 2) while preserving scanner_rank (2, 3).
    assert [(t["rank"], t["scanner_rank"], t["symbol"]) for t in doc["tickets"]] == [
        (1, 2, "SNDK"),
        (2, 3, "SMCI"),
    ]
    by_sym = {t["symbol"]: t for t in doc["tickets"]}
    assert by_sym["SNDK"]["tv_symbol"] == "NASDAQ:SNDK"
    assert by_sym["SNDK"]["stop_limit_now"] is not None
    assert by_sym["SNDK"]["stop_limit_now"] < by_sym["SNDK"]["stop_now"]
    assert by_sym["SNDK"]["paper_lane_account"] == "ALPACA_PA1"
    assert by_sym["SNDK"]["darkpool_trf_source"] == "prior_sip_trf_carryover"
    assert by_sym["SNDK"]["darkpool_block_count"] == 4
    # Verified reversal peak routes SELL / PEAK_REVERSAL on IBKR overnight.
    assert by_sym["SMCI"]["side"] == "SELL"
    assert by_sym["SMCI"]["archetype"] == "PEAK_REVERSAL"
    assert by_sym["SMCI"]["reverse_short_mode"] == "DIRECT_SHORT_ETB"
    assert by_sym["SMCI"]["stop_limit_now"] > by_sym["SMCI"]["stop_now"]
    # Standby includes UV_DARKPOOL_BUILDUP symbol CRWV (NYSE:CRWV) + SOXS (AMEX:SOXS).
    standby_syms = [s["tv_symbol"] for s in doc["standby_presets"]]
    assert "AMEX:SOXS" in standby_syms and "NYSE:CRWV" in standby_syms

    # Order Preset Pack JSON + Markdown cards landed cleanly.
    presets_doc = json.loads((out_dir / "tv_order_presets.json").read_text(encoding="utf-8"))
    assert presets_doc["schema_version"] == "tier0.tv-order-presets.v1"
    assert presets_doc["semi_auto_confirmed"] is False and presets_doc["qty"] == 1
    assert set(presets_doc["session_matrix"].keys()) == {
        "tight_near_lossless",
        "baseline",
        "wide_natr_adaptive",
    }
    assert len(presets_doc["omni_presets"]) == 8
    card_kinds = {c["card_kind"] for c in presets_doc["symbol_order_cards"]}
    assert {"ACTIVE_TICKET", "PINE_STAGED", "STANDBY_REFERENCE"} <= card_kinds
    cards_md = (out_dir / "TV_ORDER_PRESET_CARDS.md").read_text(encoding="utf-8")
    assert "NASDAQ:SNDK" in cards_md and "NASDAQ:MSTR" in cards_md and "NYSE:CRWV" in cards_md

    # FINOS / OpenBB / Lightweight-Charts v5 declarative cockpit JSON (UTCTimestamp seconds!).
    cockpit = json.loads((out_dir / "finos_tv_cockpit.json").read_text(encoding="utf-8"))
    assert cockpit["schema_version"] == "tier0.finos-openbb-tv-cockpit.v1"
    assert cockpit["plugin"] == "@finos-workspace/plotly-charts@2.5.0"
    assert cockpit["semi_auto_confirmed"] is False and cockpit["live_execution_enabled"] is False
    lwc = cockpit["lightweight_charts_v5"]
    assert lwc["api_version"] == "5" and lwc["time_unit"] == "utc_timestamp_seconds"
    assert lwc["display_timezone"] == "America/New_York"
    sndk_marker = lwc["symbols"]["SNDK"]["markers"][0]
    assert 1_000_000_000 < sndk_marker["time"] < 10_000_000_000  # seconds, never ms!
    assert "MSTR" in lwc["symbols"]

    # Exchange-qualified 6-section watchlist includes movers, UV/DP buildup & Pine-staged MSTR.
    wl = (out_dir / "tier0_sota_tv_watchlists_live.txt").read_text(encoding="utf-8")
    assert "NASDAQ:SNDK" in wl and "NYSE:CRWV" in wl and "NASDAQ:MSTR" in wl
    assert resolve_tv_exchange_symbol("TESTX", exchange="ARCX") == "AMEX:TESTX"

    # Pine scripts auto-synced + enriched compounding manifest.
    comp = json.loads((out_dir / "tv_compounding_manifest.json").read_text(encoding="utf-8"))
    assert comp["ready_tickets_count"] == 2
    assert comp["standby_presets_count"] == 2
    assert comp["staged_scan_count"] == 2
    assert comp["staged_manual_count"] == 1
    assert comp["staged_presets_total"] == 3
    assert comp["duckdb_audit_rows"] == 3
    assert comp["darkpool_trf_covered_count"] == 3
    assert "tier0_omni_sota_24x5.pine" in comp["pine_shas"]
    assert set(comp["pine_shas"].keys()) >= {
        "tier0_omni_sota_24x5.pine",
        "tier0_sota_strategy_24x5.pine",
        "tier0_market_regime.pine",
        "tier0_darkpool_uv_buildup.pine",
    }
    for pine_name, digest in comp["pine_shas"].items():
        assert (out_dir / pine_name).is_file()
        assert len(digest) == 64
    assert comp["paper_farm_lanes_summary"]["execution_status"] == "SUPERVISING"
    assert (out_dir / "tv_staged_presets.parquet").is_file()


def _render_pine_emit_alert_json(pine_src: str, subs: dict[str, str]) -> dict[str, Any]:
    """Extract emit_alert_json(...) string concatenation from Pine v6 and render to JSON."""
    m = re.search(r"emit_alert_json\([^)]*\)\s*=>.*?\(\n(.*?)\n\s*\)", pine_src, re.DOTALL)
    assert m is not None, "emit_alert_json block not found"
    body = m.group(1)
    parts: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.strip().rstrip("+").strip()
        if not line:
            continue
        if line == "pair_json":
            parts.append(subs.get("pair_json", ""))
            continue
        # Tokenize string literals and Pine expressions separated by `+`.
        tokens = re.findall(r'"(?:\\.|[^"\\])*"|[^+]+', line)
        for tok in tokens:
            t = tok.strip()
            if not t:
                continue
            if t.startswith('"') and t.endswith('"'):
                parts.append(json.loads(t))
            else:
                assert t in subs, f"Unmapped Pine alert expression: {t}"
                parts.append(subs[t])
    rendered = "".join(parts)
    parsed = json.loads(rendered)
    assert isinstance(parsed, dict)
    return parsed


def test_pine_v6_strategy_darkpool_regime_and_emitted_json_roundtrip() -> None:
    omni_src = (REPO_ROOT / "pine" / "tier0_omni_sota_24x5.pine").read_text(encoding="utf-8")
    strat_src = (REPO_ROOT / "pine" / "tier0_sota_strategy_24x5.pine").read_text(encoding="utf-8")
    regime_src = (REPO_ROOT / "pine" / "tier0_market_regime.pine").read_text(encoding="utf-8")
    dp_src = (REPO_ROOT / "pine" / "tier0_darkpool_uv_buildup.pine").read_text(encoding="utf-8")

    # Strategy Tester & Webhook invariants.
    for tok in (
        "//@strategy_alert_message {{strategy.order.alert_message}}",
        "default_qty_value=1",
        "calc_on_every_tick=false",
        "process_orders_on_close=false",
        "strategy.risk.max_intraday_loss(",
        "OrderPresetCard",
        r"\"script_kind\":\"strategy\"",
        "alert_message=",
        "2.35 * leg_risk",
        "1.35 * leg_risk",
        "0.45 * leg_risk",
    ):
        assert tok in strat_src

    # Pane-1 Regime & Pane-2 Darkpool/UV invariants.
    assert "rsp_sma20 / spy_d20" in regime_src
    assert "DIRECT_SHORT_ETB" in regime_src and "INVERSE_HEDGE_SOXS_SQQQ" in regime_src
    assert "alertcondition(" in regime_src
    for tok in (
        "vol_zscore",
        "vol_buildup_ratio",
        "hidden_vol_ratio",
        "vwap_pin_absorb",
        "sess_cvd",
        "alertcondition(",
    ):
        assert tok in dp_src

    # Round-trip Pine emit_alert_json templates from both Omni indicator & Strategy through bridge.
    base_subs = {
        "tkr": "SOXS",
        'str.tostring(time_close, "0")': "1795747260000",
        "sig_side": "BUY",
        "eff_venue": "IBKR",
        "sess_label": "OVERNIGHT",
        "order_type": "STOP_LIMIT",
        'str.tostring(safe_trig, "0.####")': "54.31",
        'str.tostring(bridge_stp, "0.####")': "53.77",
        'str.tostring(stp_dist, "0.######")': "0.009943",
        'str.tostring(collar_bps, "0")': "50",
        "auto_sched": "tight_near_lossless",
        'str.tostring(natr_out, "0.######")': "0.012",
        "str.tostring(i_auto_reverse)": "true",
        "str.tostring(i_auto_reentry)": "true",
        'str.tostring(i_cooldown_sec, "0")': "60",
        "setup_archetype": "HIDDEN_VOL_ACCUM",
        'str.tostring(eff_regime, "0")': "1",
        'str.tostring(math.max(0.0, nz(rvol, 1.0)), "0.##")': "2.85",
        'str.tostring(math.max(0.0, nz(hidden_vol_ratio, 0.0)), "0.##")': "2.4",
        'str.tostring(math.max(0.0, nz(vol_buildup_ratio, 1.0)), "0.##")': "2.15",
        "str.tostring(vol_buildup)": "true",
        "str.tostring(peak_confirmed)": "false",
        "str.tostring(is_lev_or_inv)": "true",
        "pair_json": ',"pair_symbol":"SOXL"',
        "resolved_mode": "OVERNIGHT_24X5_IBKR",
    }
    for pine_src, expected_kind in ((omni_src, "indicator"), (strat_src, "strategy")):
        for pair_val in ('"pair_symbol":"SOXL"', ""):
            subs = {**base_subs, "pair_json": ("," + pair_val) if pair_val else ""}
            payload = _render_pine_emit_alert_json(pine_src, subs)
            assert payload["script_kind"] == expected_kind
            assert payload["qty"] == 1 and payload["semi_auto_confirmed"] is False
            TvAlertV1.model_validate(payload)
            staged = validate_tv_alert_payload(payload, venue="IBKR")
            assert staged["script_kind"] == expected_kind
            assert staged["order_preset_id"] == "OVERNIGHT_24X5_IBKR"
            assert staged["vol_buildup_ratio"] == pytest.approx(2.15)
            assert staged["unusual_volume_buildup"] is True
            if pair_val:
                assert staged["pair_symbol"] == "SOXL"
            else:
                assert "pair_symbol" not in staged


def test_tv_console_sh_syntax_shellcheck_and_ps1_extensions() -> None:
    ps1_src = (REPO_ROOT / "scripts" / "tv_console.ps1").read_text(encoding="utf-8")
    for fn in (
        "function tv-presets",
        "function tv-sync-pine",
        "function tv-copy-strategy",
        "function tv-copy-darkpool",
        "function tv-ingest",
        "function tv-confirm-paper",
    ):
        assert fn in ps1_src
    assert "'--profile', 'paper'" in ps1_src
    assert "'--confirm-tv-alert'" in ps1_src
    assert "'--tv-qty'" in ps1_src
    assert "'--dry-run'" in ps1_src
    assert "--profile live" not in ps1_src and "'--profile', 'live'" not in ps1_src
    assert "| tv-confirm-paper |" in ps1_src

    sh_path = REPO_ROOT / "scripts" / "tv_console.sh"
    sh_src = sh_path.read_text(encoding="utf-8")
    assert "9223" not in sh_src
    assert "semi_auto_confirmed=false" in sh_src
    assert "http://localhost:1420/" in sh_src
    assert "http://127.0.0.1:1420/" not in sh_src
    assert "tv_confirm_paper()" in sh_src
    assert "tv-confirm-paper" in sh_src
    assert (
        "fnox exec --profile paper -- uv run --no-sync python -m tier0_sota.live.runner" in sh_src
    )
    assert "--confirm-tv-alert" in sh_src
    assert "--tv-qty" in sh_src
    assert "--dry-run" in sh_src
    assert "--profile live" not in sh_src
    for forbidden in ("api.alpaca.markets", "4001", "7496", "8798", "8796"):
        assert forbidden not in sh_src

    if os.name != "nt":
        bash_bin = shutil.which("bash")
        assert bash_bin is not None
        subprocess.run([bash_bin, "-n", str(sh_path)], check=True, capture_output=True, text=True)
        sc_bin = shutil.which("shellcheck")
        if sc_bin is not None:
            subprocess.run([sc_bin, str(sh_path)], check=True, capture_output=True, text=True)

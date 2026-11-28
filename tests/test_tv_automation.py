"""Offline unit & contract tests for Pine v6, TV alert bridge, Half-Auto tickets & console."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from tier0_sota.tv import (
    MULTI_SITUATION_SETUP_PLAYBOOK,
    TV_CLOUD_WATCHLIST_IDS,
    TvAlertV1,
    build_half_auto_tickets,
    build_tv_compounding_manifest,
    build_tv_layout_blueprints,
    build_tv_order_presets,
    build_tv_watchlist_sync_payload,
    build_venue_order_handles,
    build_venue_order_handles_matrix,
    classify_multi_situation_setup,
    current_et_session,
    filter_extreme_movers,
    ingest_alerts_file,
    load_tv_audit_summary,
    prune_and_archive_staged_presets,
    refresh_tickets_from_paths,
    refuse_live_endpoints,
    render_tickets_markdown,
    render_tv_watchlist,
    resolve_non_noise_min_trail_spec,
    resolve_tv_exchange_symbol,
    resolve_venue_order_handle,
    resolve_venue_profile,
    run_preset_setup_automation,
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
    # Tier-0 Zero Self-Invention: every Pine v6 script MUST import a verified official
    # TradingView/* or PineCoders/* library and be mapped in upstream-map.toml.
    assert re.search(
        r"^import\s+(?:TradingView|PineCoders)/[A-Za-z0-9_]+/\d+\s+as\s+[A-Za-z_][A-Za-z0-9_]*\s*$",
        src,
        re.MULTILINE,
    ), f"{pine_name} must import an official TradingView/* or PineCoders/* library"
    upstream_map_txt = (REPO_ROOT / "upstream-map.toml").read_text(encoding="utf-8")
    assert f'[files."pine/{pine_name}"]' in upstream_map_txt, (
        f"pine/{pine_name} missing provenance mapping in upstream-map.toml"
    )
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
        assert len(cols) == 30
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
        "eff_venue_profile": "IBKR_PAPER_4002",
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
        "function Resolve-Tier0PythonMode",
        "function Invoke-Tier0Python",
        "function tv-presets",
        "function tv-handles",
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
    for sh_tok in (
        "_tier0_read_clipboard()",
        "tv_ingest_clip()",
        "tv_stage()",
        "tv_handles()",
        "tv_confirm_paper()",
        "tv-confirm-paper",
        "wslpath -w",
    ):
        assert sh_tok in sh_src
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


def test_bearish_neutral_regime_sell_never_flips_to_buy_or_hedge(tmp_path: Path) -> None:
    on_bar = "2026-11-27T03:21:00Z"
    on_now = datetime(2026, 11, 27, 3, 21, 10, tzinfo=UTC)
    movers_doc = {
        "schema_version": "tier0.whole-market-movers.v2",
        "generated_at_utc": "2026-11-27T03:21:05Z",
        "feed_sources": ["alpaca_boats"],
        "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
        "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
        "entries": [
            {
                "rank": 1,
                "symbol": "SQQQ",
                "exchange": "NASDAQ",
                "score": 53.78,
                "last_price": 46.52,
                "prior_close": 48.62,
                "annual_borrow_rate": 0.0095,
                "session_move_pct": -4.32,
                "rvol_normalized": 3.55,
                "peak_confirmed": False,
                "side": "SELL",
                "natr_pct": 0.012,
                "extreme_mover_eligible": True,
                "pinned": True,
                "shortable": True,
                "easy_to_borrow": True,
                "borrow_source": "alpaca_paper_asset",
                "short_fee_bps": 0.0,
                "bar_close_utc": on_bar,
            },
            {
                "rank": 2,
                "symbol": "SOXL",
                "exchange": "AMEX",
                "score": 28.99,
                "last_price": 36.30,
                "prior_close": 37.09,
                "session_move_pct": -2.13,
                "rvol_normalized": 1.85,
                "unusual_volume_buildup": True,
                "peak_confirmed": False,
                "side": "SELL",
                "natr_pct": 0.014,
                "extreme_mover_eligible": False,
                "pinned": True,
                "shortable": True,
                "easy_to_borrow": True,
                "bar_close_utc": on_bar,
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
                "peak_confirmed": True,
                "side": "SELL",
                "natr_pct": 0.014,
                "extreme_mover_eligible": True,
                "shortable": True,
                "easy_to_borrow": True,
                "bar_close_utc": on_bar,
            },
        ],
    }
    movers_path = tmp_path / "movers_neutral_sell.json"
    movers_path.write_text(json.dumps(movers_doc), encoding="utf-8")
    bundle_path = tmp_path / "bundle_neutral_sell.json"
    bundle_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.live-lane-preset-bundle.v1",
                "top_potential_symbols": [
                    {
                        "symbol": "SQQQ",
                        "lane_account": "ALPACA_PA3",
                        "lane_id": "L3_AVWAP_TIGHT_LOCK",
                        "entry_eligible": False,
                        "entry_hold_reason": "MACRO_REGIME_NEUTRAL",
                    },
                    {
                        "symbol": "SMCI",
                        "lane_account": "ALPACA_PA2",
                        "lane_id": "L2_STRUCTURAL_TREND",
                        "entry_eligible": True,
                        "entry_hold_reason": None,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    # Alpaca overnight with bundle: SQQQ keeps honest SELL, fails closed HOLD, never stages.
    alp_dir = tmp_path / "alp_out"
    alp_doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        bundle_path=bundle_path,
        out_dir=alp_dir,
        venue="ALPACA",
        now_utc=on_now,
    )
    assert alp_doc["charter_guards"]["live_execution_enabled"] is False
    assert alp_doc["charter_guards"]["semi_auto_confirmed"] is False
    assert alp_doc["charter_guards"]["qty"] == 1
    alp_by_sym = {t["symbol"]: t for t in alp_doc["tickets"]}
    sqqq = alp_by_sym["SQQQ"]
    assert sqqq["side"] == "SELL"
    assert sqqq["stop_now"] > sqqq["live_price"]
    assert sqqq["lock_at_1r"] < sqqq["live_price"]
    assert sqqq["action_gate"] == "HOLD (MACRO_REGIME_NEUTRAL)"
    assert "READY_HALF_AUTO" not in sqqq["action_gate"]
    alp_staged = json.loads((alp_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    assert alp_staged["presets"] == [] and alp_doc["staged_count"] == 0

    # Standby bearish SOXL also preserves honest SELL + short stop even with UV buildup.
    standby_by_sym = {s["symbol"]: s for s in alp_doc["standby_presets"]}
    assert standby_by_sym["SOXL"]["side"] == "SELL"
    assert standby_by_sym["SOXL"]["stop_now"] > standby_by_sym["SOXL"]["live_price"]
    assert standby_by_sym["SOXL"]["action_gate"] == "MONITOR_ONLY (<EXTREME GATE)"

    # IBKR overnight without bundle: SQQQ STILL fails closed HOLD (never staged),
    # while peak_confirmed=True SMCI still routes SELL / PEAK_REVERSAL / READY_HALF_AUTO.
    ibkr_dir = tmp_path / "ibkr_out"
    ibkr_doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        bundle_path=None,
        out_dir=ibkr_dir,
        venue="IBKR",
        now_utc=on_now,
    )
    ibkr_by_sym = {t["symbol"]: t for t in ibkr_doc["tickets"]}
    assert ibkr_by_sym["SQQQ"]["side"] == "SELL"
    assert ibkr_by_sym["SQQQ"]["action_gate"] == "HOLD (BEARISH_UNCONFIRMED_REGIME)"
    assert ibkr_by_sym["SMCI"]["side"] == "SELL"
    assert ibkr_by_sym["SMCI"]["archetype"] == "PEAK_REVERSAL"
    assert ibkr_by_sym["SMCI"]["action_gate"] == "READY_HALF_AUTO"
    ibkr_staged = json.loads((ibkr_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    assert [p["symbol"] for p in ibkr_staged["presets"]] == ["SMCI"]
    assert all(p["qty"] == 1 and p["semi_auto_confirmed"] is False for p in ibkr_staged["presets"])

    # Crash regime (-1) inverse hedge behavior remains 100% intact.
    crash_movers = {
        **movers_doc,
        "macro_regime": {"regime": -1, "regime_label": "CRASH"},
    }
    crash_doc = build_half_auto_tickets(crash_movers, now_utc=on_now, venue="ALPACA")
    crash_by_sym = {t["symbol"]: t for t in crash_doc["tickets"]}
    assert crash_by_sym["SQQQ"]["side"] == "BUY"
    assert crash_by_sym["SQQQ"]["archetype"] == "CRASH_ALIGNMENT"
    assert crash_by_sym["SQQQ"]["action_gate"] == "READY_HALF_AUTO_HEDGE"


def test_watchlist_preserves_manual_staged_past_top15_and_iex_otc_prefixes(tmp_path: Path) -> None:
    assert resolve_tv_exchange_symbol("IEXX", exchange="IEX") == "IEX:IEXX"
    assert resolve_tv_exchange_symbol("IEXY", exchange="IEXG") == "IEX:IEXY"
    assert resolve_tv_exchange_symbol("OTCX", exchange="OTC") == "OTC:OTCX"

    on_bar = "2026-11-27T03:21:00Z"
    on_now = datetime(2026, 11, 27, 3, 21, 10, tzinfo=UTC)
    out_dir = tmp_path / "tv_sota_pack"

    entries = [
        {
            "rank": i,
            "symbol": f"MV{i:02d}",
            "exchange": "IEX" if i == 1 else ("OTC" if i == 2 else "NASDAQ"),
            "score": 80.0 - i,
            "last_price": 50.0 + i,
            "session_move_pct": 4.0,
            "rvol_normalized": 3.0,
            "natr_pct": 0.015,
            "extreme_mover_eligible": True,
            "bar_close_utc": on_bar,
        }
        for i in range(1, 17)
    ]
    movers_path = tmp_path / "movers_16.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T03:21:05Z",
                "feed_sources": ["alpaca_boats"],
                "macro_regime": {"regime": 1, "regime_label": "BULL"},
                "entries": entries,
            }
        ),
        encoding="utf-8",
    )

    for manual_sym in ("RKLB", "CUSTOMZ"):
        stage_tv_alert_preset(
            _valid_alert(
                symbol=manual_sym,
                session="OVERNIGHT",
                order_type="STOP_LIMIT",
                trigger_price=40.0,
                stop_price=39.6,
                limit_collar_bps=50.0,
            ),
            staged_path=out_dir / "staged_tv_presets.json",
            watchlist_path=tmp_path / "unused.json",
            venue="IBKR",
        )

    refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=out_dir, venue="ALPACA", now_utc=on_now
    )
    wl_text = (out_dir / "tier0_sota_tv_watchlists_live.txt").read_text(encoding="utf-8")
    movers_lines = [
        ln
        for ln in wl_text.split("###03_LIVE_MOVERS_TOP15\n", 1)[1]
        .split("###04_DARKPOOL_UNUSUAL_VOL", 1)[0]
        .splitlines()
        if ln.strip()
    ]
    assert len(movers_lines) == 15
    assert "IEX:MV01" in movers_lines and "OTC:MV02" in movers_lines

    assert "###07_MANUAL_STAGED_PRESETS" in wl_text
    manual_lines = [
        ln for ln in wl_text.split("###07_MANUAL_STAGED_PRESETS\n", 1)[1].splitlines() if ln.strip()
    ]
    assert manual_lines == ["NASDAQ:RKLB", "CUSTOMZ"]

    # Every non-header line in .txt watchlist must remain pure TradingView ticker syntax.
    for raw_line in wl_text.splitlines():
        if raw_line.startswith("###"):
            continue
        assert re.fullmatch(r"(?:[A-Z]+:)?[A-Z0-9.-]+", raw_line), f"Polluted ticker: {raw_line!r}"


def test_metadata_propagation_carryover_notice_and_venue_order_handles_parity(
    tmp_path: Path,
) -> None:
    from tier0_sota import engine as eng

    on_bar = "2026-11-27T04:23:00Z"
    on_now = datetime(2026, 11, 27, 4, 23, 10, tzinfo=UTC)
    carry_as_of = "2026-11-25T20:55:00+00:00"
    out_dir = tmp_path / "tv_sota_pack"
    movers_path = tmp_path / "movers_meta.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T04:23:05Z",
                "feed_sources": ["alpaca_boats", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 1, "regime_label": "BULL"},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "INTC",
                        "exchange": "NASDAQ",
                        "score": 58.6,
                        "last_price": 56.95,
                        "prior_close": 55.01,
                        "session_move_pct": 3.53,
                        "rvol_normalized": 3.2,
                        "darkpool_trf_ratio": 0.5108,
                        "darkpool_trf_source": "prior_sip_trf_carryover",
                        "darkpool_trf_as_of_utc": carry_as_of,
                        "darkpool_block_count": 20,
                        "trf_block_ratio": 0.4729,
                        "unusual_volume_buildup": True,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": True,
                        "pinned": False,
                        "shortable": True,
                        "easy_to_borrow": True,
                        "borrow_source": "alpaca_paper_asset",
                        "short_fee_bps": 0.0,
                        "bar_close_utc": on_bar,
                    },
                    {
                        "rank": 2,
                        "symbol": "TQQQ",
                        "exchange": "NASDAQ",
                        "score": 32.96,
                        "last_price": 48.54,
                        "prior_close": 46.14,
                        "session_move_pct": 5.20,
                        "rvol_normalized": 0.35,
                        "darkpool_trf_ratio": 0.3338,
                        "darkpool_trf_source": "prior_sip_trf_carryover",
                        "darkpool_trf_as_of_utc": carry_as_of,
                        "natr_pct": 0.012,
                        "extreme_mover_eligible": False,
                        "pinned": True,
                        "shortable": False,
                        "easy_to_borrow": False,
                        "borrow_source": "alpaca_paper_asset",
                        "short_fee_bps": 30.0,
                        "bar_close_utc": on_bar,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path, out_dir=out_dir, venue="ALPACA", now_utc=on_now
    )
    intc = doc["tickets"][0]
    tqqq = doc["standby_presets"][0]
    assert intc["pinned"] is False
    assert intc["borrow_source"] == "alpaca_paper_asset"
    assert intc["short_fee_bps"] == pytest.approx(0.0)
    assert intc["darkpool_trf_as_of_utc"] == carry_as_of
    assert intc["darkpool_trf_age_hours"] == pytest.approx(31.5)

    assert tqqq["pinned"] is True
    assert tqqq["borrow_source"] == "alpaca_paper_asset"
    assert tqqq["short_fee_bps"] == pytest.approx(30.0)
    assert tqqq["darkpool_trf_as_of_utc"] == carry_as_of
    assert tqqq["darkpool_trf_age_hours"] == pytest.approx(31.5)

    tickets_md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    cards_md = (out_dir / "TV_ORDER_PRESET_CARDS.md").read_text(encoding="utf-8")
    for md_text in (tickets_md, cards_md):
        assert "prior_sip_trf_carryover" in md_text
        assert carry_as_of in md_text
        assert "31.5h" in md_text

    # Watchlist .txt remains strictly free of carryover annotations.
    wl_txt = (out_dir / "tier0_sota_tv_watchlists_live.txt").read_text(encoding="utf-8")
    assert "prior_sip_trf_carryover" not in wl_txt and "31.5h" not in wl_txt

    # Venue order handles parity matches engine.inspect_venue_capability verbatim.
    presets_doc = json.loads((out_dir / "tv_order_presets.json").read_text(encoding="utf-8"))
    assert presets_doc["paper_only"] is True
    assert presets_doc["semi_auto_confirmed"] is False
    assert presets_doc["live_execution_enabled"] is False
    assert presets_doc["qty"] == 1

    vh = presets_doc["venue_order_handles"]
    assert vh["source"] == "tier0_sota.engine.inspect_venue_capability"
    assert vh["paper_only"] is True and vh["live_execution_enabled"] is False
    assert vh["combined_paper_farm_max_rpm"] == 3600
    assert vh["combined_paper_farm_exit_reserve_rpm"] == 360
    assert vh["combined_paper_farm_active_entry_rpm"] == 3240

    alp_cap = eng.inspect_venue_capability("ALPACA_PAPER_200")
    ibkr_cap = eng.inspect_venue_capability("IBKR_PAPER_4002")
    pro_cap = eng.inspect_venue_capability("ALPACA_LIVE_PRO_LINE_1000")

    alp_h = vh["active_paper_handles"]["ALPACA_PAPER_200"]
    assert alp_h["max_rpm"] == alp_cap["max_rpm"] == 200
    assert alp_h["lane_count"] == 3
    assert alp_h["lane_accounts"] == ["ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3"]
    assert alp_h["combined_max_rpm"] == 600
    assert alp_h["combined_exit_reserve_rpm"] == 60
    assert alp_h["paper_only"] is True and alp_h["live_execution_enabled"] is False
    assert alp_h["execution_status"] == "PAPER_ACTIVE"

    ibkr_h = vh["active_paper_handles"]["IBKR_PAPER_4002"]
    assert ibkr_h["max_rpm"] == ibkr_cap["max_rpm"] == 3000
    assert ibkr_h["exit_reserve_rpm"] == ibkr_cap["exit_reserve_rpm"] == 300
    assert ibkr_h["combined_max_rpm"] == 3000
    assert ibkr_h["paper_only"] is True and ibkr_h["live_execution_enabled"] is False
    assert ibkr_h["execution_status"] == "PAPER_ACTIVE"

    pro_h = vh["stage5_sandbox_reference_specs"]["ALPACA_LIVE_PRO_LINE_1000"]
    assert pro_h["max_rpm"] == pro_cap["max_rpm"] == 1000
    assert pro_h["max_rps"] == pro_cap["max_rps"] == 25
    assert pro_h["exit_reserve_rpm"] == pro_cap["exit_reserve_rpm"] == 100
    assert pro_h["live_execution_enabled"] is False
    assert pro_h["paper_only"] is True
    assert pro_h["allowed"] is False and pro_h["executable_now"] is False
    assert pro_h["stage5_readiness_spec_only"] is True
    assert pro_h["execution_status"] == "LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC"

    for tok in (
        "3600 rpm",
        "ALPACA_PAPER_200",
        "IBKR_PAPER_4002",
        "ALPACA_LIVE_PRO_LINE_1000",
        "LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC",
    ):
        assert tok in cards_md

    # Zero live broker endpoints, live ports, or CDP :9223 across all emitted text artifacts.
    for artifact in out_dir.iterdir():
        if artifact.suffix not in {".json", ".md", ".txt"}:
            continue
        blob = artifact.read_text(encoding="utf-8")
        for forbidden in ("api.alpaca.markets", ":4001", ":7496", ":8798", ":8796", "9223"):
            assert forbidden not in blob, f"Forbidden {forbidden!r} in {artifact.name}"


def test_multi_situation_setup_playbook_and_classification() -> None:
    expected_archetypes = {
        "EARLY_EP_MOMENTUM_BURST",
        "VCP_BREAKOUT",
        "VCP_COIL",
        "PEAD_DRIFT",
        "EXHAUSTION_HAMMER",
        "HIDDEN_VOL_ACCUM",
        "PARABOLIC_TOP",
        "PEAK_REVERSAL",
        "CRASH_ALIGNMENT",
        "EXTREME_REVERSAL",
    }
    assert set(MULTI_SITUATION_SETUP_PLAYBOOK) == expected_archetypes
    for arch, pb in MULTI_SITUATION_SETUP_PLAYBOOK.items():
        assert pb["archetype"] == arch
        assert pb["climax_auto_reverse"] is True
        assert pb["reentry_cooldown_secs"] == 60
        assert pb["target_paper_lane"] in {"ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3"}

    assert classify_multi_situation_setup(
        {"symbol": "SOXS"}, regime_int=-1, sess_chg=4.0, rvol=2.5
    ) == ("BUY", "LONG", "CRASH_ALIGNMENT")
    assert classify_multi_situation_setup(
        {"symbol": "NVDA"}, regime_int=-1, sess_chg=-3.0, rvol=3.0
    ) == ("SELL", "SHORT", "CRASH_ALIGNMENT")
    assert classify_multi_situation_setup(
        {"symbol": "SMCI", "side": "SELL"},
        regime_int=0,
        sess_chg=-2.5,
        rvol=2.4,
        peak_confirmed=True,
    ) == ("SELL", "SHORT", "PEAK_REVERSAL")
    assert classify_multi_situation_setup(
        {"symbol": "NVDA", "earnings_catalyst": True}, regime_int=1, sess_chg=5.0, rvol=3.5
    ) == ("BUY", "LONG", "PEAD_DRIFT")
    assert classify_multi_situation_setup(
        {"symbol": "PLTR"}, regime_int=1, sess_chg=4.5, rvol=3.8
    ) == ("BUY", "LONG", "EARLY_EP_MOMENTUM_BURST")
    assert classify_multi_situation_setup(
        {"symbol": "AVGO", "vcp_breakout": True}, regime_int=1, sess_chg=1.5, rvol=2.0
    ) == ("BUY", "LONG", "VCP_BREAKOUT")
    assert classify_multi_situation_setup(
        {"symbol": "AMD", "coil_ok": True}, regime_int=1, sess_chg=0.8, rvol=1.4, natr_pct=0.012
    ) == ("BUY", "LONG", "VCP_COIL")
    assert classify_multi_situation_setup(
        {"symbol": "META", "lower_wick_ratio": 0.62}, regime_int=1, sess_chg=0.5, rvol=1.6
    ) == ("BUY", "LONG", "EXHAUSTION_HAMMER")
    assert classify_multi_situation_setup(
        {"symbol": "INTC"},
        regime_int=1,
        sess_chg=1.0,
        rvol=1.8,
        darkpool_trf_ratio=0.45,
        unusual_volume_buildup=True,
    ) == ("BUY", "LONG", "HIDDEN_VOL_ACCUM")
    assert classify_multi_situation_setup(
        {"symbol": "MSTR", "parabolic_top": True}, regime_int=0, sess_chg=1.0, rvol=1.2
    ) == ("SELL", "SHORT", "PARABOLIC_TOP")
    assert classify_multi_situation_setup(
        {"symbol": "MSTR", "parabolic_top": True}, regime_int=1, sess_chg=1.0, rvol=1.2
    ) == ("BUY", "LONG", "PARABOLIC_TOP")
    assert classify_multi_situation_setup(
        {"symbol": "TSLA"}, regime_int=1, sess_chg=1.2, rvol=1.1
    ) == ("BUY", "LONG", "EXTREME_REVERSAL")


def test_venue_profile_order_handles_1000rpm_3000rpm_and_stage5_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TIER0_VENUE_PROFILE", raising=False)
    monkeypatch.delenv("TIER0_ALPACA_RPM", raising=False)

    assert resolve_venue_profile("IBKR") == ("IBKR_PAPER_4002", "IBKR")
    assert resolve_venue_profile("ALPACA") == ("ALPACA_PAPER_200", "ALPACA")
    assert resolve_venue_profile("ALPACA", venue_profile="ALPACA_PAPER_1000") == (
        "ALPACA_PAPER_1000",
        "ALPACA",
    )

    # Env opt-in for ALPACA_PAPER_1000; out-of-range RPM and LIVE env vars fail closed to paper.
    monkeypatch.setenv("TIER0_ALPACA_RPM", "1000")
    assert resolve_venue_profile("ALPACA") == ("ALPACA_PAPER_1000", "ALPACA")
    monkeypatch.setenv("TIER0_ALPACA_RPM", "ALPACA_PAPER_1000")
    assert resolve_venue_profile("ALPACA") == ("ALPACA_PAPER_1000", "ALPACA")
    monkeypatch.setenv("TIER0_ALPACA_RPM", "9999")
    assert resolve_venue_profile("ALPACA") == ("ALPACA_PAPER_200", "ALPACA")
    monkeypatch.setenv("TIER0_ALPACA_RPM", "200")
    monkeypatch.setenv("TIER0_VENUE_PROFILE", "ALPACA_LIVE_PRO_LINE_1000")
    assert resolve_venue_profile("ALPACA") == ("ALPACA_PAPER_200", "ALPACA")

    with pytest.raises(ValueError, match="Unsupported TV venue"):
        resolve_venue_profile("CRYPTO")
    with pytest.raises(ValueError, match="Unsupported TV venue_profile"):
        resolve_venue_profile("ALPACA", venue_profile="ALPACA_UNKNOWN")

    # Matrix specs match engine SSOT (IBKR 3000rpm, Alpaca 1000rpm & 200rpm, Stage-5 LIVE refused).
    matrix = build_venue_order_handles_matrix(
        session="OVERNIGHT", trailing_schedule="tight_near_lossless"
    )
    assert matrix["IBKR_PAPER_4002"]["max_rpm"] == 3000
    assert matrix["IBKR_PAPER_4002"]["exit_reserve_rpm"] == 300
    assert matrix["IBKR_PAPER_4002"]["entry_modify_rpm_budget"] == 2700
    assert matrix["IBKR_PAPER_4002"]["executable_now"] is True
    assert matrix["ALPACA_PAPER_1000"]["max_rpm"] == 1000
    assert matrix["ALPACA_PAPER_1000"]["max_rps"] == 25
    assert matrix["ALPACA_PAPER_1000"]["exit_reserve_rpm"] == 100
    assert matrix["ALPACA_PAPER_1000"]["entry_modify_rpm_budget"] == 900
    assert matrix["ALPACA_PAPER_1000"]["executable_now"] is True
    assert matrix["ALPACA_PAPER_1000"]["paper_only"] is True
    assert matrix["ALPACA_PAPER_1000"]["live_execution_enabled"] is False
    assert matrix["ALPACA_LIVE_PRO_LINE_1000"]["executable_now"] is False
    assert matrix["ALPACA_LIVE_PRO_LINE_1000"]["live_execution_enabled"] is False
    assert resolve_venue_order_handle("ALPACA_PAPER_1000", session="RTH")["max_rpm"] == 1000

    # Paper Farm 1000rpm combined handles reach 6,000 rpm while keeping Stage-5 LIVE refused.
    h1000 = build_venue_order_handles(alpaca_venue_profile="ALPACA_PAPER_1000")
    assert h1000["paper_only"] is True and h1000["live_execution_enabled"] is False
    assert h1000["active_alpaca_profile"] == "ALPACA_PAPER_1000"
    assert h1000["combined_paper_farm_max_rpm"] == 6000
    assert h1000["combined_paper_farm_exit_reserve_rpm"] == 600
    assert h1000["combined_paper_farm_active_entry_rpm"] == 5400
    assert h1000["active_paper_handles"]["ALPACA_PAPER_1000"]["execution_status"] == "PAPER_ACTIVE"
    assert (
        h1000["active_paper_handles"]["ALPACA_PAPER_200"]["execution_status"]
        == "PAPER_FALLBACK_200RPM"
    )
    assert (
        h1000["stage5_sandbox_reference_specs"]["ALPACA_LIVE_PRO_LINE_1000"]["execution_status"]
        == "LIVE_REFUSED_PAPER_ONLY_SANDBOX_SPEC"
    )

    presets = build_tv_order_presets(
        {
            "generated_at_et": "2026-11-25T09:35:00-05:00",
            "active_session": "RTH",
            "venue": "ALPACA",
            "venue_profile": "ALPACA_PAPER_1000",
            "macro_regime": {"regime": 1, "regime_label": "BULL"},
            "tickets": [],
            "standby_presets": [],
        }
    )
    assert presets["venue_profile"] == "ALPACA_PAPER_1000"
    assert presets["venue_order_handles"]["combined_paper_farm_max_rpm"] == 6000


def test_bridge_live_intake_refuses_stage5_live_venue_profile_and_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import duckdb

    from tier0_sota.tv import bridge as tv_bridge

    # Fixed RTH instant: 2026-11-25 14:35:00Z (09:35 ET, valid XNYS RTH session).
    bar_ms = int(datetime(2026, 11, 25, 14, 35, 0, tzinfo=UTC).timestamp() * 1000)
    monkeypatch.setattr(tv_bridge.time, "time_ns", lambda: (bar_ms + 5_000) * 1_000_000)

    rth_buy = _valid_alert(
        symbol="NVDA",
        ts_utc_ms=bar_ms,
        side="BUY",
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        session="RTH",
        order_type="STOP_MARKET",
        trigger_price=100.0,
        stop_price=98.5,
        stop_distance_pct=0.015,
        limit_collar_bps=0.0,
        trailing_schedule="tight_near_lossless",
        natr_pct=0.012,
        data_source="tradingview_pine_v6",
    )
    staged_ok = stage_tv_alert_preset(
        rth_buy,
        staged_path=tmp_path / "staged_tv_presets.json",
        watchlist_path=tmp_path / "missing_wl.json",
        venue="ALPACA",
        live_intake=True,
    )
    assert staged_ok["venue_profile"] == "ALPACA_PAPER_1000"
    assert staged_ok["semi_auto_confirmed"] is False and staged_ok["qty"] == 1

    # DuckDB 30-column audit table persists venue_profile and lifecycle status cleanly.
    db_path = tmp_path / "tv_half_auto_audit.duckdb"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        cols = conn.execute("DESCRIBE tv_staged_presets").fetchall()
        assert len(cols) == 30
        vp_rows = conn.execute(
            "SELECT symbol, venue_profile FROM tv_staged_presets WHERE alert_id = ?",
            [staged_ok["alert_id"]],
        ).fetchall()
        assert vp_rows == [("NVDA", "ALPACA_PAPER_1000")]

    # Live intake MUST refuse Stage-5 LIVE venue profiles and cross-venue mismatches.
    for live_vp in ("ALPACA_LIVE_PRO_LINE_1000", "ALPACA_LIVE_ELITE_SMART_ROUTER"):
        with pytest.raises(ValueError, match="Refused Stage-5 LIVE venue_profile"):
            validate_tv_alert_payload(
                {**rth_buy, "venue_profile": live_vp}, venue="ALPACA", live_intake=True
            )
    with pytest.raises(ValueError, match="disagrees with routed venue"):
        validate_tv_alert_payload(
            {**rth_buy, "venue_profile": "IBKR_PAPER_4002"}, venue="ALPACA", live_intake=True
        )

    # Display preview (live_intake=False) still validates Stage-5 schema enum for read-only HUD.
    preview = validate_tv_alert_payload(
        {**rth_buy, "venue_profile": "ALPACA_LIVE_PRO_LINE_1000"}, venue="ALPACA", live_intake=False
    )
    assert preview["venue_profile"] == "ALPACA_LIVE_PRO_LINE_1000"
    assert preview["semi_auto_confirmed"] is False and preview["qty"] == 1


def test_engine_coordination_manifest_active_positions_and_1000rpm_pack_sync(
    tmp_path: Path,
) -> None:
    import duckdb

    rth_bar = "2026-11-25T15:00:00Z"
    rth_now = datetime(2026, 11, 25, 15, 0, 10, tzinfo=UTC)
    out_dir = tmp_path / "tv_sota_pack"
    out_dir.mkdir(parents=True)

    (tmp_path / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "active_session": "RTH",
                "paper_only": True,
                "live_execution_enabled": False,
                "managed_symbols": ["NVDA"],
                "streams": ["sip", "boats"],
                "active_positions": [
                    {
                        "symbol": "NVDA",
                        "account": "ALPACA_PA1",
                        "lane_id": "L1_ORB_BURST",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "side": "LONG",
                        "qty": 10.0,
                        "entry_price": 145.0,
                        "initial_stop": 142.83,
                        "active_stop": 146.10,
                        "active_session": "RTH",
                        "trailing_schedule": "tight_near_lossless",
                        "lock_tier": 2,
                        "order_mode": "STOP_MARKET",
                        "collar_bps": 0.0,
                    }
                ],
                "active_positions_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (out_dir / "tv_engine_coordination_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-engine-coordination.v1",
                "generated_at_utc": "2026-11-25T15:00:08+00:00",
                "active_session": "RTH",
                "paper_only": True,
                "semi_auto_confirmed": False,
                "live_execution_enabled": False,
                "auto_submit_enabled": False,
                "tca_calibration_ok": True,
                "any_breaker_tripped": False,
                "staged_alerts_evaluated": 1,
                "ready_for_operator_confirm_count": 1,
                "blocked_count": 0,
                "coordination_decisions": [
                    {
                        "alert_id": "1234567890abcdef",
                        "symbol": "APP",
                        "side": "LONG",
                        "qty": 1,
                        "venue_profile": "ALPACA_PAPER_1000",
                        "target_account": "ALPACA_PA2",
                        "target_lane_id": "L2_STRUCTURAL_TREND",
                        "semi_auto_confirmed": False,
                        "ready_for_operator_confirm": True,
                        "discipline_gate": "READY_FOR_OPERATOR_CONFIRM",
                    }
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
                "generated_at_utc": "2026-11-25T15:00:05Z",
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "macro_regime": {"regime": 1, "regime_label": "RISK_ON"},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "APP",
                        "exchange": "XNAS",
                        "score": 88.0,
                        "last_price": 320.0,
                        "prior_close": 300.0,
                        "session_move_pct": 6.67,
                        "rvol_normalized": 4.1,
                        "earnings_catalyst": True,
                        "darkpool_trf_ratio": 0.42,
                        "unusual_volume_buildup": True,
                        "natr_pct": 0.014,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": rth_bar,
                        "shortable": True,
                        "easy_to_borrow": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        out_dir=out_dir,
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        now_utc=rth_now,
    )
    assert doc["venue_profile"] == "ALPACA_PAPER_1000"
    assert doc["venue_order_handle"]["max_rpm"] == 1000
    assert doc["tickets"][0]["archetype"] == "PEAD_DRIFT"
    assert doc["tickets"][0]["venue_profile"] == "ALPACA_PAPER_1000"
    assert doc["tickets"][0]["recommended_omni_preset"] == "STOCKBEE_EP_MOMENTUM_BURST"
    assert doc["tickets"][0]["target_paper_lane"] == "ALPACA_PA2"
    assert doc["paper_farm_coordination"]["active_positions_count"] == 1
    assert doc["paper_farm_coordination"]["coordinated_presets_count"] == 1

    with duckdb.connect(str(out_dir / "tv_half_auto_audit.duckdb"), read_only=True) as conn:
        vp_row = conn.execute(
            "SELECT symbol, venue_profile FROM tv_staged_presets WHERE symbol = 'APP'"
        ).fetchone()
    assert vp_row == ("APP", "ALPACA_PAPER_1000")

    md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    assert "## Active Hosted Engine 24/5 Trailing Positions & Order-Handle Coordination" in md
    assert "`ALPACA_PAPER_1000`" in md and "`IBKR_PAPER_4002`" in md
    assert "`146.10`" in md and "`READY_FOR_OPERATOR_CONFIRM`" in md

    presets_doc = json.loads((out_dir / "tv_order_presets.json").read_text(encoding="utf-8"))
    assert presets_doc["venue_profile"] == "ALPACA_PAPER_1000"
    assert presets_doc["venue_order_handles"]["combined_paper_farm_max_rpm"] == 6000
    assert len(presets_doc["active_engine_positions"]) == 1
    assert "PEAD_DRIFT" in presets_doc["multi_situation_playbook"]

    cards_md = (out_dir / "TV_ORDER_PRESET_CARDS.md").read_text(encoding="utf-8")
    assert "## Active Hosted Engine 24/5 Trailing & Coordination Status" in cards_md
    assert "## Multi-Situation Setup Playbook" in cards_md and "`PEAD_DRIFT`" in cards_md

    cockpit = json.loads((out_dir / "finos_tv_cockpit.json").read_text(encoding="utf-8"))
    assert cockpit["venue_profile"] == "ALPACA_PAPER_1000"
    assert cockpit["summary"]["active_engine_positions_count"] == 1
    assert cockpit["summary"]["coordinated_presets_count"] == 1
    assert cockpit["openbb_rows"][0]["venue_profile"] == "ALPACA_PAPER_1000"
    assert cockpit["openbb_rows"][0]["max_rpm"] == 1000

    comp = json.loads((out_dir / "tv_compounding_manifest.json").read_text(encoding="utf-8"))
    assert comp["venue_profile"] == "ALPACA_PAPER_1000"
    assert comp["active_engine_positions_count"] == 1
    assert comp["coordinated_presets_count"] == 1
    assert comp["multi_situation_setup_counts"] == {"PEAD_DRIFT": 1}
    assert comp["venue_order_handles_summary"]["ALPACA_PAPER_1000"]["max_rpm"] == 1000
    assert comp["venue_order_handles_summary"]["IBKR_PAPER_4002"]["max_rpm"] == 3000
    assert comp["artifacts"]["layout_blueprints_json"] == "tv_layout_blueprints.json"
    assert comp["artifacts"]["layout_quickstart_md"] == "TV_LAYOUT_AND_PRESET_QUICKSTART.md"

    layout_bp = json.loads((out_dir / "tv_layout_blueprints.json").read_text(encoding="utf-8"))
    assert layout_bp["schema_version"] == "tier0.tv-layout-blueprints.v1"
    assert layout_bp["zero_cdp_policy"] is True
    assert layout_bp["live_execution_enabled"] is False
    assert layout_bp["semi_auto_confirmed"] is False
    assert layout_bp["qty"] == 1
    assert len(layout_bp["tradingview_layouts"]) == 3
    assert len(layout_bp["native_tv_order_panel_presets"]) == 3
    assert len(layout_bp["finos_openbb_command_center"]["tabs"]) == 5
    assert layout_bp["finos_openbb_command_center"]["widgets_count"] == 26
    assert "@finos-workspace/mermaid@1.0.0" in layout_bp["finos_openbb_command_center"]["plugins"]
    assert layout_bp["mcp_ecosystem"]["user_official_tradingview_mcp"]["zero_cdp"] is True

    quick_md = (out_dir / "TV_LAYOUT_AND_PRESET_QUICKSTART.md").read_text(encoding="utf-8")
    assert "Tier-0 Real TradingView Layouts" in quick_md
    assert "https://mcp.tradingview.com/mcp" in quick_md
    assert "pinescript-mcp" in quick_md
    assert "Zero CDP" in quick_md

    tv_sh = (Path(__file__).resolve().parents[1] / "scripts" / "tv_console.sh").read_text(
        encoding="utf-8"
    )
    tv_ps = (Path(__file__).resolve().parents[1] / "scripts" / "tv_console.ps1").read_text(
        encoding="utf-8"
    )
    assert "tv_layout()" in tv_sh and "tv_mcp_status()" in tv_sh
    assert "function tv-layout" in tv_ps and "function tv-mcp-status" in tv_ps
    assert "tv_watchlist_payload()" in tv_sh and "function tv-watchlist-payload" in tv_ps


def test_watchlist_sync_payload_non_noise_engine_ssot_and_cloud_receipt(tmp_path: Path) -> None:
    from decimal import Decimal

    from tier0_sota import engine as eng

    # 1. Zero-Self-Invention: resolve_non_noise_min_trail_spec delegates directly to engine SSOT.
    for sess, expected_mech in (
        ("RTH", "NATIVE_BROKER_STOP_MARKET_RATCHET"),
        ("PRE_MARKET", "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"),
        ("AFTER_HOURS", "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"),
        ("OVERNIGHT", "SYNTHETIC_BOATS_COLLARED_LIMIT"),
    ):
        spec = resolve_non_noise_min_trail_spec(
            symbol="NVDA",
            entry_price=Decimal("140.00"),
            side="LONG",
            session=sess,
            natr_pct=Decimal("0.015"),
            spread_bps=Decimal("4"),
            archetype="EARLY_EP_MOMENTUM_BURST",
            venue_profile="ALPACA_PAPER_1000",
        )
        eng_spec = eng.compute_non_noise_min_trail_spec(
            symbol="NVDA",
            entry_price=Decimal("140.00"),
            side="LONG",
            session=sess,
            natr_pct=Decimal("0.015"),
            spread_bps=Decimal("4"),
            archetype="EARLY_EP_MOMENTUM_BURST",
            venue_profile="ALPACA_PAPER_1000",
        )
        assert spec["source"] == "tier0_sota.engine.compute_non_noise_min_trail_spec"
        assert spec["execution_mechanism"] == expected_mech
        assert spec["non_noise_min_trail_pct"] == pytest.approx(
            float(eng_spec["non_noise_min_trail_pct"])
        )
        assert spec["non_noise_min_trail_pct"] <= 0.02
        assert spec["rapid_reentry_policy"]["max_rpm"] == 1000
        assert spec["rapid_reentry_policy"]["rpm_budget_available"] == 900
        assert spec["rapid_reentry_policy"]["reentry_cooldown_secs"] == 60
        assert spec["rapid_reentry_policy"]["exit_reserve_protected"] is True

    # 2. Refresh wires cloud sync receipt, historical audit, paper adoption & watchlist payload.
    bar_utc = "2026-11-25T14:35:00Z"
    now_utc = datetime(2026, 11, 25, 14, 35, 10, tzinfo=UTC)
    manifest_dir = tmp_path / "manifests"
    out_dir = manifest_dir / "tv_sota_pack"
    out_dir.mkdir(parents=True, exist_ok=True)
    movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-25T14:35:04Z",
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 1, "regime_label": "BULL"},
                "early_weights_calibration": {"prewall_cutoff": "2026-10-15", "rvol_w": 0.35},
                "discovery_summary": {"scanned": 420, "eligible": 2},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "NVDA",
                        "exchange": "NASDAQ",
                        "score": 88.4,
                        "last_price": 145.50,
                        "session_move_pct": 4.2,
                        "rvol_normalized": 3.8,
                        "natr_pct": 0.015,
                        "potential_category": "PREMARKET_CATALYST_LEADER",
                        "calibrated_category": "PREMARKET_CATALYST_LEADER",
                        "min_trailing_pct": 0.0042,
                        "technicals": {"vwap": 144.2, "above_vwap": True},
                        "early_detection": {"calibration_wall_date": "2026-10-15"},
                        "session_order_presets": {
                            "PRE_MARKET": {"order_mode": "STOP_LIMIT"},
                            "RTH": {"order_mode": "STOP_MARKET"},
                            "AFTER_HOURS": {"order_mode": "STOP_LIMIT"},
                            "OVERNIGHT": {"order_mode": "STOP_LIMIT"},
                        },
                        "technical_alignment_score": 0.91,
                        "premarket_ignition": True,
                        "extreme_mover_eligible": True,
                        "shortable": True,
                        "easy_to_borrow": True,
                        "bar_close_utc": bar_utc,
                    },
                    {
                        "rank": 2,
                        "symbol": "TQQQ",
                        "exchange": "NASDAQ",
                        "score": 61.2,
                        "last_price": 78.20,
                        "session_move_pct": 2.4,
                        "rvol_normalized": 1.6,
                        "natr_pct": 0.018,
                        "potential_category": "EXTREME_MOVER_CONTINUATION",
                        "extreme_mover_eligible": False,
                        "watch_tier": "UV_DARKPOOL_BUILDUP",
                        "unusual_volume_buildup": True,
                        "bar_close_utc": bar_utc,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "paper_only": True,
                "live_execution_enabled": False,
                "active_positions": [
                    {
                        "symbol": "NVDA",
                        "side": "LONG",
                        "lane_account": "ALPACA_PA1",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "entry_price": 144.80,
                        "active_stop": 144.95,
                        "lock_tier": "T1_BE_PLUS_FEE",
                        "adopted_external": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (out_dir / "tv_mcp_sync_receipt.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-mcp-sync-receipt.v1",
                "generated_at_utc": "2026-11-25T14:35:00Z",
                "zero_cdp": True,
                "paper_only": True,
                "semi_auto_confirmed": False,
                "live_execution_enabled": False,
                "post_fix_replay_status": "DEFERRED_DAILY_QUOTA_RESET",
                "cloud_watchlist": {
                    "watchlist_id": TV_CLOUD_WATCHLIST_IDS["active"],
                    "symbols": ["NASDAQ:NVDA", "NASDAQ:TQQQ"],
                },
                "technicals_ratings": {
                    "NASDAQ:NVDA": {"rating": "STRONG_BUY", "rating_score": 0.86},
                },
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "historical_extreme_mover_calibration_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.historical-extreme-mover-calibration.v1",
                "generated_at_utc": "2026-11-25T14:00:00Z",
                "prewall_cutoff": "2026-10-15",
                "prewall_benchmark": {"recall_at_top15": 0.82},
                "nvda_monday_premarket_audit": {
                    "first_detect_et": "2026-11-23T07:05:00-05:00",
                    "first_detect_px": 141.20,
                    "peak_px": 149.80,
                    "category": "PREMARKET_CATALYST_LEADER",
                },
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        out_dir=out_dir,
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        now_utc=now_utc,
    )
    nvda = doc["tickets"][0]
    assert nvda["potential_category"] == "PREMARKET_CATALYST_LEADER"
    assert nvda["calibrated_category"] == "PREMARKET_CATALYST_LEADER"
    assert nvda["min_trailing_pct"] == pytest.approx(0.0042)
    assert nvda["technicals"]["above_vwap"] is True
    assert nvda["early_detection"]["calibration_wall_date"] == "2026-10-15"
    assert sorted(nvda["session_order_presets"]) == [
        "AFTER_HOURS",
        "OVERNIGHT",
        "PRE_MARKET",
        "RTH",
    ]
    assert nvda["tv_technicals_rating"] == "STRONG_BUY"
    assert nvda["tv_technicals_score"] == pytest.approx(0.86)
    assert nvda["auto_adopted_by_engine"] is True
    assert nvda["engine_adoption_status"] == "AUTO_ADOPTED_EXTERNAL"

    wl_payload = json.loads(
        (out_dir / "tv_watchlist_sync_payload.json").read_text(encoding="utf-8")
    )
    assert wl_payload["schema_version"] == "tier0.tv-watchlist-sync-payload.v1"
    assert wl_payload["zero_cdp"] is True and wl_payload["paper_only"] is True
    assert wl_payload["semi_auto_confirmed"] is False
    assert wl_payload["live_execution_enabled"] is False and wl_payload["qty"] == 1
    assert wl_payload["watchlists"]["active"]["watchlist_id"] == "357054653"
    assert wl_payload["watchlists"]["alert_bound"]["watchlist_id"] == "357055160"
    assert "NASDAQ:NVDA" in wl_payload["watchlists"]["active"]["symbols"]
    assert "NASDAQ:NVDA" in wl_payload["watchlists"]["alert_bound"]["symbols"]
    assert wl_payload["category_counts"]["PREMARKET_CATALYST_LEADER"] == 1
    assert wl_payload["calibrated_category_counts"]["PREMARKET_CATALYST_LEADER"] == 1
    assert wl_payload["categorized_symbols"][0]["session_order_presets_sessions"] == [
        "AFTER_HOURS",
        "OVERNIGHT",
        "PRE_MARKET",
        "RTH",
    ]
    assert wl_payload["tv_cloud_mcp_sync"]["present"] is True
    assert wl_payload["tv_cloud_mcp_sync"]["technicals_ratings_count"] == 1
    assert wl_payload["calibration_metadata"]["early_weights_calibration"]["rvol_w"] == 0.35

    comp = json.loads((out_dir / "tv_compounding_manifest.json").read_text(encoding="utf-8"))
    assert comp["artifacts"]["watchlist_sync_payload_json"] == "tv_watchlist_sync_payload.json"
    assert comp["artifacts"]["mcp_sync_receipt_json"] == "tv_mcp_sync_receipt.json"
    assert comp["historical_early_mover_audit_summary"]["present"] is True
    assert comp["historical_early_mover_audit_summary"]["nvda_peak_px"] == pytest.approx(149.80)
    assert comp["tv_cloud_mcp_sync_summary"]["present"] is True
    assert comp["tv_cloud_mcp_sync_summary"]["post_fix_replay_status"] == (
        "DEFERRED_DAILY_QUOTA_RESET"
    )

    direct = build_tv_watchlist_sync_payload(doc)
    assert direct["watchlists"]["active"]["watchlist_id"] == "357054653"


def test_phase4_24x5_non_noise_min_trail_early_mover_audit_and_cloud_mcp_sync(
    tmp_path: Path,
) -> None:
    # 1. Verify resolve_non_noise_min_trail_spec across all 4 sessions via engine SSOT.
    rth_spec = resolve_non_noise_min_trail_spec(
        symbol="NVDA",
        entry_price=Decimal("145.00"),
        side="LONG",
        session="RTH",
        natr_pct=Decimal("0.014"),
        archetype="EARLY_EP_MOMENTUM_BURST",
        venue_profile="ALPACA_PAPER_1000",
    )
    assert rth_spec["source"] == "tier0_sota.engine.compute_non_noise_min_trail_spec"
    assert rth_spec["execution_mechanism"] == "NATIVE_BROKER_STOP_MARKET_RATCHET"
    assert rth_spec["execution_mechanism_label"] == "RTH: STOP_MARKET 0bp Native Ratchet"
    assert rth_spec["order_type"] == "STOP_MARKET"
    assert rth_spec["collar_bps"] == pytest.approx(0.0)
    assert rth_spec["extended_hours"] is False
    assert 0.0 < rth_spec["non_noise_min_trail_pct"] <= 0.02
    assert rth_spec["auto_reverse_policy"] == "FLATTEN_FIRST_PEAK_CLIMAX_REVERSE"
    assert rth_spec["rapid_reentry_policy"]["reentry_cooldown_secs"] == 60
    assert rth_spec["rapid_reentry_policy"]["max_rpm"] == 1000
    assert rth_spec["rapid_reentry_policy"]["exit_reserve_protected"] is True

    pre_spec = resolve_non_noise_min_trail_spec(
        symbol="NVDA",
        entry_price=Decimal("145.00"),
        side="LONG",
        session="PRE_MARKET",
        natr_pct=Decimal("0.014"),
        archetype="EARLY_EP_MOMENTUM_BURST",
        venue_profile="ALPACA_PAPER_1000",
    )
    assert pre_spec["execution_mechanism"] == "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"
    assert "35bp" in pre_spec["execution_mechanism_label"]
    assert pre_spec["order_type"] == "STOP_LIMIT"
    assert pre_spec["collar_bps"] == pytest.approx(35.0)
    assert pre_spec["extended_hours"] is True

    on_spec = resolve_non_noise_min_trail_spec(
        symbol="NVDA",
        entry_price=Decimal("145.00"),
        side="LONG",
        session="OVERNIGHT",
        natr_pct=Decimal("0.014"),
        archetype="EARLY_EP_MOMENTUM_BURST",
        venue_profile="IBKR_PAPER_4002",
    )
    assert on_spec["execution_mechanism"] == "SYNTHETIC_BOATS_COLLARED_LIMIT"
    assert "50bp" in on_spec["execution_mechanism_label"]
    assert on_spec["collar_bps"] == pytest.approx(50.0)
    assert on_spec["rapid_reentry_policy"]["max_rpm"] == 3000

    # 2. Verify full Phase 4 & Phase 5 manifest propagation across all emitted tv_sota_pack files.
    manifest_dir = tmp_path / "manifests"
    out_dir = manifest_dir / "tv_sota_pack"
    out_dir.mkdir(parents=True)
    pre_bar = "2026-11-25T13:05:00Z"  # 08:05 ET (PRE_MARKET)
    pre_now = datetime(2026, 11, 25, 13, 5, 10, tzinfo=UTC)

    movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-25T13:05:05Z",
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 1, "regime_label": "RISK_ON"},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "NVDA",
                        "exchange": "NASDAQ",
                        "score": 94.5,
                        "last_price": 146.20,
                        "prior_close": 140.00,
                        "session_move_pct": 4.43,
                        "rvol_normalized": 4.8,
                        "vol_buildup_ratio": 3.2,
                        "unusual_volume_buildup": True,
                        "darkpool_trf_ratio": 0.46,
                        "natr_pct": 0.015,
                        "potential_category": "PREMARKET_EARLY_IGNITION",
                        "technical_alignment_score": 91.5,
                        "early_ignition_detected": True,
                        "premarket_ignition": True,
                        "sudden_volume_surge": True,
                        "tuning_run_id": "mlflow-run-phase4-001",
                        "extreme_mover_eligible": True,
                        "shortable": True,
                        "easy_to_borrow": True,
                        "bar_close_utc": pre_bar,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "active_session": "PRE_MARKET",
                "paper_only": True,
                "live_execution_enabled": False,
                "adopted_external_positions_count": 1,
                "synthetic_offhours_trailed_count": 1,
                "peak_reverse_armed_count": 1,
                "rapid_reentry_ready_count": 1,
                "active_positions": [
                    {
                        "symbol": "NVDA",
                        "side": "LONG",
                        "account": "ALPACA_PA1",
                        "lane_id": "L1_ORB_BURST",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "entry_price": 145.00,
                        "active_stop": 145.15,
                        "active_session": "PRE_MARKET",
                        "execution_mechanism": "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
                        "non_noise_min_trail_pct": 0.0052,
                        "adopted_external": True,
                        "auto_adopted_by_engine": True,
                        "fast_sell_armed": True,
                        "peak_reverse_armed": True,
                        "reentry_eligible_after_utc": "2026-11-25T13:06:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "early_mover_tuning_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.early-mover-tuning.v1",
                "generated_at_utc": "2026-11-25T13:00:00Z",
                "prewall_cutoff": "2026-10-15",
                "study_name": "tier0_early_mover_compounding",
                "trials_count": 12,
                "mlflow_run_id": "mlflow-run-phase4-001",
                "validation_metrics": {
                    "recall_at_top15": 0.85,
                    "median_time_to_first_detect_mins": 14.5,
                    "mean_post_detect_mfe_capture_pct": 4.25,
                },
                "best_weights": {"w_move": 0.22, "w_rvol": 0.28, "w_premarket_ignition": 0.18},
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "historical_early_mover_audit_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.historical-early-mover-audit.v1",
                "generated_at_utc": "2026-11-25T13:01:00Z",
                "prewall_cutoff": "2026-10-15",
                "prewall_benchmark": {
                    "recall_at_top15": 0.85,
                    "median_time_to_first_detect_mins": 14.5,
                    "mean_post_detect_mfe_capture_pct": 4.25,
                },
                "nvda_monday_premarket_audit": {
                    "symbol": "NVDA",
                    "split_tag": "out_of_sample_recent_replay",
                    "first_detect_et": "2026-11-23T04:15:00-05:00",
                    "first_detect_px": 141.50,
                    "peak_px": 171.50,
                    "category": "PREMARKET_EARLY_IGNITION",
                },
                "first_detection_timeline": [
                    {
                        "symbol": "NVDA",
                        "date": "2026-10-14",
                        "first_detect_et": "09:35 ET",
                        "first_detect_px": 135.0,
                        "session_high_px": 142.0,
                        "post_detect_mfe_pct": 5.19,
                        "category": "PREMARKET_EARLY_IGNITION",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (out_dir / "tv_cloud_mcp_sync_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-cloud-mcp-sync.v1",
                "generated_at_utc": "2026-11-25T13:04:00Z",
                "zero_cdp": True,
                "paper_only": True,
                "semi_auto_confirmed": False,
                "live_execution_enabled": False,
                "official_tv_mcp": {
                    "status": "AUTHENTICATED_DAILY_QUOTA_EXHAUSTED",
                    "endpoint": "https://mcp.tradingview.com/mcp",
                },
                "pinescript_reference_mcp": {
                    "status": "VERIFIED_PASS",
                    "package": "pinescript-mcp==0.7.7",
                },
                "scripts": {
                    "tier0_omni_sota_24x5.pine": {"status": "VERIFIED_PASS", "error_count": 0},
                    "tier0_sota_strategy_24x5.pine": {"status": "VERIFIED_PASS", "error_count": 0},
                    "tier0_market_regime.pine": {"status": "VERIFIED_PASS", "error_count": 0},
                    "tier0_darkpool_uv_buildup.pine": {"status": "VERIFIED_PASS", "error_count": 0},
                },
                "watchlist_sync_payload": {
                    "name": "Tier-0 SOTA 24x5 Movers & Darkpool",
                    "symbols": ["NASDAQ:NVDA", "AMEX:SOXL", "NASDAQ:TQQQ"],
                },
                "backtest_sync_queue": [
                    {"symbol": "NASDAQ:NVDA", "script": "tier0_sota_strategy_24x5.pine"}
                ],
                "technicals_ratings": {
                    "NASDAQ:NVDA": {"rating": "STRONG_BUY", "rating_score": 0.91},
                },
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "starred_frontier_landscape_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.starred-frontier-landscape.v1",
                "generated_at_utc": "2026-11-25T13:04:30Z",
                "prewall_cutoff": "2026-10-15",
                "mapie": {
                    "empirical_coverage": 0.91,
                    "mean_interval_width": 0.014,
                    "ok": True,
                },
                "skchange": {"changepoints": [12, 48], "ok": True},
                "shapiq": {
                    "top_interactions": [{"features": ["rvol", "move_pct"], "score": 0.14}],
                    "ok": True,
                },
                "sqlglot": {"ok": True, "queries_checked": 4, "unsafe_blocked": 2},
                "edgartools": {"pit_safe": True, "symbols": {"NVDA": {"catalyst_score": 0.8}}},
            }
        ),
        encoding="utf-8",
    )
    (manifest_dir / "l15_supply_chain_and_formal_audit.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.l15-formal-supply-chain-audit.v1",
                "generated_at_utc": "2026-11-25T13:04:45Z",
                "z3": {"ok": True, "unsat_count": 5, "total": 5},
                "crosshair": {"ok": True, "counterexamples": 0},
                "inspect_ai": {"ok": True, "passed": 7, "total": 7},
                "alertmanager": {"healthy": True, "rules_count": 12},
                "skills": {"ok": True, "locked_skills_count": 46},
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        out_dir=out_dir,
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        now_utc=pre_now,
    )
    nvda = doc["tickets"][0]
    assert nvda["execution_mechanism"] == "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"
    assert "35bp" in nvda["execution_mechanism_label"]
    assert nvda["non_noise_min_trail_pct"] > 0.0
    assert nvda["auto_reverse_policy"] == "FLATTEN_FIRST_PEAK_CLIMAX_REVERSE"
    assert nvda["rapid_reentry_rpm"] == 1000
    assert nvda["reentry_cooldown_secs"] == 60
    assert nvda["auto_adopted_by_engine"] is True
    assert nvda["engine_adoption_status"] == "AUTO_ADOPTED_EXTERNAL"
    assert nvda["potential_category"] == "PREMARKET_EARLY_IGNITION"
    assert nvda["technical_alignment_score"] == pytest.approx(91.5)
    assert nvda["premarket_ignition"] is True
    assert nvda["sudden_volume_surge"] is True
    assert nvda["tv_technicals_rating"] == "STRONG_BUY"

    presets = json.loads((out_dir / "tv_order_presets.json").read_text(encoding="utf-8"))
    card0 = presets["symbol_order_cards"][0]
    assert card0["execution_mechanism"] == "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"
    assert card0["auto_adopted_by_engine"] is True
    assert card0["potential_category"] == "PREMARKET_EARLY_IGNITION"
    assert card0["tv_technicals_rating"] == "STRONG_BUY"
    assert card0["rapid_reentry_rpm"] == 1000

    comp = json.loads((out_dir / "tv_compounding_manifest.json").read_text(encoding="utf-8"))
    assert comp["lossless_24x5_summary"]["adopted_external_positions_count"] == 1
    assert comp["lossless_24x5_summary"]["synthetic_offhours_trailed_count"] == 1
    assert comp["early_mover_tuning_summary"]["present"] is True
    assert comp["early_mover_tuning_summary"]["recall_at_top15"] == pytest.approx(0.85)
    assert comp["historical_early_mover_audit_summary"]["present"] is True
    assert comp["historical_early_mover_audit_summary"]["nvda_peak_px"] == pytest.approx(171.50)
    assert comp["tv_cloud_mcp_sync_summary"]["present"] is True
    assert comp["tv_cloud_mcp_sync_summary"]["compiled_pass_count"] == 4
    assert comp["tv_cloud_mcp_sync_summary"]["cloud_watchlist_symbol_count"] == 3
    assert comp["starred_frontier_landscape_summary"]["present"] is True
    assert comp["starred_frontier_landscape_summary"]["mapie_empirical_coverage"] == pytest.approx(
        0.91
    )
    assert comp["starred_frontier_landscape_summary"]["skchange_changepoints_count"] == 2
    assert comp["starred_frontier_landscape_summary"]["sqlglot_pit_ok"] is True
    assert comp["starred_frontier_landscape_summary"]["edgartools_pit_safe"] is True
    assert comp["l15_formal_audit_summary"]["present"] is True
    assert comp["l15_formal_audit_summary"]["z3_ok"] is True
    assert comp["l15_formal_audit_summary"]["crosshair_ok"] is True
    assert comp["l15_formal_audit_summary"]["alertmanager_healthy"] is True
    assert comp["finos_command_center_summary"]["tabs_count"] == 5
    assert comp["finos_command_center_summary"]["widgets_count"] == 26
    assert "@finos-workspace/mermaid@1.0.0" in comp["finos_command_center_summary"]["plugins"]
    assert comp["artifacts"]["cloud_mcp_sync_json"] == "tv_cloud_mcp_sync_manifest.json"
    assert (
        comp["artifacts"]["starred_frontier_landscape_json"]
        == "../starred_frontier_landscape_manifest.json"
    )
    assert comp["artifacts"]["l15_formal_audit_json"] == "../l15_supply_chain_and_formal_audit.json"

    layout_bp = json.loads((out_dir / "tv_layout_blueprints.json").read_text(encoding="utf-8"))
    assert layout_bp["finos_openbb_command_center"]["widgets_count"] == 26
    assert len(layout_bp["finos_openbb_command_center"]["tabs"]) == 5
    assert "@finos-workspace/mermaid@1.0.0" in layout_bp["finos_openbb_command_center"]["plugins"]
    assert layout_bp["early_mover_tuning_summary"]["present"] is True
    assert layout_bp["historical_early_mover_audit_summary"]["present"] is True
    assert layout_bp["tv_cloud_mcp_sync_summary"]["present"] is True
    assert layout_bp["starred_frontier_landscape_summary"]["present"] is True
    assert layout_bp["l15_formal_audit_summary"]["present"] is True

    quick_md = (out_dir / "TV_LAYOUT_AND_PRESET_QUICKSTART.md").read_text(encoding="utf-8")
    cards_md = (out_dir / "TV_ORDER_PRESET_CARDS.md").read_text(encoding="utf-8")
    tickets_md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    assert "85.0%" in quick_md and "4/4 PASS" in quick_md
    assert "5-Tab Command Center" in quick_md and "26 Widgets" in quick_md
    assert "@finos-workspace/mermaid" in quick_md and "tier0_live_architecture_mermaid" in quick_md
    assert "starred_frontier_landscape_manifest.json" in quick_md
    assert "l15_supply_chain_and_formal_audit.json" in quick_md
    assert "PREMARKET_EARLY_IGNITION" in cards_md and "STRONG_BUY" in cards_md
    assert "AUTO_ADOPTED_EXTERNAL" in tickets_md and "1000 rpm" in tickets_md

    for artifact in out_dir.iterdir():
        if artifact.suffix not in {".json", ".md", ".txt"}:
            continue
        blob = artifact.read_text(encoding="utf-8")
        for forbidden in ("9223", "api.alpaca.markets", ":4001", ":7496", ":8798", ":8796"):
            assert forbidden not in blob, f"Forbidden token {forbidden!r} in {artifact.name}"


def test_order_preset_cards_full_hydration_early_ignition_sorting_and_receipt_refresh(
    tmp_path: Path,
) -> None:
    """Verify zero 0.0 R-lock/stop-limit blanks, extended-hours staging, sorting & receipt sync."""
    manifest_dir = tmp_path / "manifests"
    out_dir = manifest_dir / "tv_sota_pack"
    out_dir.mkdir(parents=True)

    # Active PRE_MARKET instant: 2026-11-27 05:58:40 ET = 2026-11-27 10:58:40 UTC.
    pre_bar = "2026-11-27T10:58:00+00:00"
    pre_now = datetime(2026, 11, 27, 10, 58, 40, tzinfo=UTC)
    pre_bar_ms = int(datetime(2026, 11, 27, 10, 58, 0, tzinfo=UTC).timestamp() * 1000)
    on_bar_ms = int(datetime(2026, 11, 27, 4, 58, 0, tzinfo=UTC).timestamp() * 1000)

    # Seed prior-session OVERNIGHT and active-session PRE_MARKET raw PINE_STAGED alerts
    # (note: raw staged JSON intentionally omits R-lock/stop-limit/rating/lane fields).
    raw_staged = {
        "schema_version": "tier0.staged-tv-presets.v1",
        "updated_at_utc": "2026-11-27T10:46:15+00:00",
        "semi_auto_confirmed": False,
        "presets": [
            {
                "alert_id": "29f7d3087c18367f",
                "schema_version": "tier0.tv-alert.v1",
                "symbol": "SQQQ",
                "ts_utc_ms": on_bar_ms,
                "bar_confirmed": True,
                "side": "BUY",
                "qty": 1,
                "venue": "ALPACA",
                "venue_profile": "ALPACA_PAPER_1000",
                "session": "OVERNIGHT",
                "order_type": "STOP_LIMIT",
                "trigger_price": 46.59,
                "stop_price": 46.13,
                "stop_distance_pct": 0.009873,
                "limit_collar_bps": 50.0,
                "trailing_schedule": "tight_near_lossless",
                "natr_pct": 0.0016,
                "auto_reverse": True,
                "auto_reentry": True,
                "reentry_cooldown_secs": 60,
                "strategy_id": "SignalExecutionStrategy",
                "semi_auto_confirmed": False,
                "staged_at_utc": "2026-11-27T04:58:10+00:00",
                "data_source": "tradingview_pine_v6",
            },
            {
                "alert_id": "feabe4e34db95720",
                "schema_version": "tier0.tv-alert.v1",
                "symbol": "MSTR",
                "ts_utc_ms": on_bar_ms,
                "bar_confirmed": True,
                "side": "BUY",
                "qty": 1,
                "venue": "ALPACA",
                "venue_profile": "ALPACA_PAPER_1000",
                "session": "OVERNIGHT",
                "order_type": "STOP_LIMIT",
                "trigger_price": 83.99,
                "stop_price": 83.16,
                "stop_distance_pct": 0.009882,
                "limit_collar_bps": 50.0,
                "trailing_schedule": "tight_near_lossless",
                "natr_pct": 0.0024,
                "auto_reverse": True,
                "auto_reentry": True,
                "reentry_cooldown_secs": 60,
                "strategy_id": "SignalExecutionStrategy",
                "semi_auto_confirmed": False,
                "staged_at_utc": "2026-11-27T04:58:15+00:00",
                "data_source": "tradingview_pine_v6",
            },
            {
                "alert_id": "df527f1380ac1fb8",
                "schema_version": "tier0.tv-alert.v1",
                "symbol": "SOXL",
                "ts_utc_ms": pre_bar_ms,
                "bar_confirmed": True,
                "side": "BUY",
                "qty": 1,
                "venue": "ALPACA",
                "venue_profile": "ALPACA_PAPER_1000",
                "session": "PRE_MARKET",
                "order_type": "STOP_LIMIT",
                "trigger_price": 36.72,
                "stop_price": 36.27,
                "stop_distance_pct": 0.012255,
                "limit_collar_bps": 35.0,
                "trailing_schedule": "tight_near_lossless",
                "natr_pct": 0.0024,
                "auto_reverse": True,
                "auto_reentry": True,
                "reentry_cooldown_secs": 60,
                "strategy_id": "SignalExecutionStrategy",
                "semi_auto_confirmed": False,
                "staged_at_utc": "2026-11-27T10:58:10+00:00",
                "data_source": "tradingview_pine_v6",
            },
        ],
    }
    (out_dir / "staged_tv_presets.json").write_text(json.dumps(raw_staged), encoding="utf-8")

    # Seed active broker-adopted TQQQ/SQQQ positions on ALPACA_PA3.
    (manifest_dir / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "active_session": "PRE_MARKET",
                "paper_only": True,
                "live_execution_enabled": False,
                "managed_symbols": ["SQQQ", "TQQQ"],
                "active_positions": [
                    {
                        "symbol": "SQQQ",
                        "account": "ALPACA_PA3",
                        "lane_id": "L3_AVWAP_TIGHT_LOCK",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "side": "LONG",
                        "qty": 1.0,
                        "entry_price": 45.78,
                        "active_stop": 45.21,
                        "active_session": "PRE_MARKET",
                        "adopted_external": True,
                    },
                    {
                        "symbol": "TQQQ",
                        "account": "ALPACA_PA3",
                        "lane_id": "L3_AVWAP_TIGHT_LOCK",
                        "venue_profile": "ALPACA_PAPER_1000",
                        "side": "LONG",
                        "qty": 1.0,
                        "entry_price": 48.56,
                        "active_stop": 48.83,
                        "active_session": "PRE_MARKET",
                        "adopted_external": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    # Seed stale tv_mcp_sync_receipt.json + tv_cloud_mcp_sync_manifest.json with real quota error.
    verbatim_quota = "TOOL_QUOTA_EXCEEDED - You've reached your daily usage limit"
    (out_dir / "tv_mcp_sync_receipt.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-mcp-sync-receipt.v1",
                "generated_at_utc": "2026-11-27T06:26:45+00:00",
                "zero_cdp": True,
                "paper_only": True,
                "semi_auto_confirmed": False,
                "live_execution_enabled": False,
                "post_fix_replay_status": "DEFERRED_DAILY_QUOTA_RESET",
                "official_tv_mcp": {
                    "status": "AUTHENTICATED_DAILY_QUOTA_EXHAUSTED",
                    "verbatim_error": verbatim_quota,
                },
                "pinescript_reference_mcp": {"status": "VERIFIED_PASS"},
                "pine_cloud_validations": {
                    "pine/tier0_omni_sota_24x5.pine": {
                        "script_id": "USER;real_existing_id",
                        "ok": True,
                        "status": "PASS",
                        "error_count": 0,
                    }
                },
                "technicals_ratings": {
                    "BATS:DRAM": {"rating": "STRONG_BUY", "rating_score": 0.88},
                },
            }
        ),
        encoding="utf-8",
    )
    (out_dir / "tv_cloud_mcp_sync_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.tv-cloud-mcp-sync.v1",
                "generated_at_utc": "2026-11-27T06:17:04+00:00",
                "pinescript_reference_mcp": {
                    "package": "pinescript-mcp==0.7.7",
                    "status": "VERIFIED_PASS",
                    "valid_count": 48,
                    "validated_functions_count": 48,
                    "invalid_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    # Extended-hours movers: ALL have extreme_mover_eligible=False (honest!), with causal ignition,
    # active broker adoption, and liquid >= $10 setup signals (plus a sub-$10 ticker excluded).
    movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T10:58:35+00:00",
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "DRAM",
                        "exchange": "BATS",
                        "score": 54.05,
                        "last_price": 37.41,
                        "prior_close": 36.11,
                        "session_move_pct": 3.60,
                        "rvol_normalized": 0.72,
                        "vol_buildup_ratio": 2.38,
                        "unusual_volume_buildup": True,
                        "hidden_volume_ratio": 1.924,
                        "darkpool_trf_ratio": 0.0447,
                        "watch_tier": "PREMARKET_IGNITION",
                        "potential_category": "PREMARKET_EARLY_IGNITION",
                        "technical_alignment_score": 65.66,
                        "premarket_ignition": True,
                        "early_ignition_detected": True,
                        "sudden_volume_surge": True,
                        "technicals": {"above_vwap": True},
                        "natr_pct": 0.001,
                        "extreme_mover_eligible": False,
                        "shortable": True,
                        "easy_to_borrow": True,
                        "bar_close_utc": pre_bar,
                    },
                    {
                        "rank": 2,
                        "symbol": "TQQQ",
                        "exchange": "NASDAQ",
                        "score": 48.0,
                        "last_price": 48.80,
                        "prior_close": 49.14,
                        "session_move_pct": -0.69,
                        "rvol_normalized": 0.17,
                        "unusual_volume_buildup": True,
                        "darkpool_trf_ratio": 0.4861,
                        "watch_tier": "PINNED_STANDBY",
                        "potential_category": "SUDDEN_VOLUME_SURGE",
                        "technical_alignment_score": 51.42,
                        "sudden_volume_surge": True,
                        "pinned": True,
                        "technicals": {"above_vwap": True},
                        "natr_pct": 0.001,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": pre_bar,
                    },
                    {
                        "rank": 3,
                        "symbol": "NVDL",
                        "exchange": "NASDAQ",
                        "score": 45.2,
                        "last_price": 14.71,
                        "prior_close": 14.50,
                        "session_move_pct": 1.45,
                        "rvol_normalized": 0.18,
                        "unusual_volume_buildup": True,
                        "darkpool_trf_ratio": 0.3117,
                        "watch_tier": "PINNED_STANDBY",
                        "potential_category": "SUDDEN_VOLUME_SURGE",
                        "technical_alignment_score": 66.27,
                        "early_ignition_detected": True,
                        "sudden_volume_surge": True,
                        "technicals": {"above_vwap": True},
                        "natr_pct": 0.0014,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": pre_bar,
                    },
                    {
                        "rank": 4,
                        "symbol": "PENNY",
                        "exchange": "NASDAQ",
                        "score": 40.0,
                        "last_price": 4.25,
                        "session_move_pct": 5.0,
                        "rvol_normalized": 1.1,
                        "premarket_ignition": True,
                        "early_ignition_detected": True,
                        "technical_alignment_score": 70.0,
                        "natr_pct": 0.015,
                        "extreme_mover_eligible": False,
                        "bar_close_utc": pre_bar,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    doc = refresh_tickets_from_paths(
        movers_path=movers_path,
        out_dir=out_dir,
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        now_utc=pre_now,
    )

    # 1. Strict extreme flag remains 100% honest (False); early-ignition presets are staged cleanly.
    assert doc["active_session"] == "PRE_MARKET"
    assert doc["extreme_mover_confirmed_count"] == 0
    assert doc["session_early_ignition_count"] >= 3  # DRAM, TQQQ, NVDL (+ SQQQ adopted)
    assert doc["schema_validated_count"] == len(doc["tickets"]) > 0
    assert doc["charter_guards"]["qty"] == 1
    assert doc["charter_guards"]["semi_auto_confirmed"] is False
    assert doc["charter_guards"]["live_execution_enabled"] is False
    ticket_syms = {t["symbol"] for t in doc["tickets"]}
    assert {"DRAM", "TQQQ", "SQQQ", "NVDL"} <= ticket_syms
    assert "PENNY" not in ticket_syms  # < $10 liquidity floor enforced
    for t in doc["tickets"]:
        assert t["extreme_mover_eligible"] is False
        assert t["preset_tier"] == "SESSION_EARLY_IGNITION_PRESET"
        assert t["action_gate"] == "SESSION_EARLY_IGNITION_PRESET"
        assert t["order_type_now"] == "STOP_LIMIT"
        assert t["collar_now_bps"] == 35

    # 2. staged_scan_presets.json is non-empty and every preset passes strict TvAlertV1.
    staged_scan = json.loads((out_dir / "staged_scan_presets.json").read_text(encoding="utf-8"))
    assert staged_scan["semi_auto_confirmed"] is False
    assert staged_scan["extreme_mover_confirmed_count"] == 0
    assert staged_scan["session_early_ignition_count"] == len(staged_scan["presets"]) >= 3
    for p in staged_scan["presets"]:
        assert p["qty"] == 1 and p["semi_auto_confirmed"] is False
        TvAlertV1.model_validate(p)

    # 3. Direct build_tv_order_presets on BOTH active PRE_MARKET and prior OVERNIGHT PINE_STAGED
    # cards guarantees ZERO 0.0 R-lock/stop-limit blanks, non-empty rating/lane & clean sorting.
    presets_doc = build_tv_order_presets(
        doc,
        manual_staged_presets=raw_staged["presets"],
        paper_coordination=doc["paper_farm_coordination"],
        tv_cloud_sync=json.loads(
            (out_dir / "tv_mcp_sync_receipt.json").read_text(encoding="utf-8")
        ),
        movers_entries=json.loads(movers_path.read_text(encoding="utf-8"))["entries"],
    )
    cards = presets_doc["symbol_order_cards"]
    assert len(cards) >= 7
    assert presets_doc["qty"] == 1
    assert presets_doc["semi_auto_confirmed"] is False
    assert presets_doc["live_execution_enabled"] is False

    # Active-session (PRE_MARKET) cards must all precede prior-session (OVERNIGHT) staged cards.
    sessions_order = [c["session"] for c in cards]
    first_overnight_idx = sessions_order.index("OVERNIGHT")
    assert all(s == "PRE_MARKET" for s in sessions_order[:first_overnight_idx])
    assert all(s == "OVERNIGHT" for s in sessions_order[first_overnight_idx:])

    for c in cards:
        assert c["qty"] == 1
        assert c["semi_auto_confirmed"] is False
        assert c["live_execution_enabled"] is False
        assert c["entry_ref_price"] > 0.0
        assert c["entry_limit_price"] > 0.0
        assert c["stop_trigger_price"] > 0.0
        assert c["stop_limit_price"] > 0.0
        assert c["be_plus_fee_floor"] > 0.0
        assert c["lock_at_1r"] > 0.0
        assert c["lock_at_2r"] > 0.0
        assert c["lock_at_3r"] > 0.0
        assert c["fast_sell_rev_trigger"] > 0.0
        assert c["non_noise_min_trail_pct"] > 0.0
        assert c["peak_climax_reverse_trigger_pct"] > 0.0
        assert c["technical_alignment_score"] > 0.0
        assert c["tv_technicals_rating"] in {
            "STRONG_BUY",
            "BUY",
            "NEUTRAL",
            "SELL",
            "STRONG_SELL",
        }
        assert c["ta_rating_source"] in {"tv_cloud_mcp", "local_ta_composite"}
        assert c["lane_account"] and c["paper_lane_account"] == c["lane_account"]
        assert c["lane_id"] and c["paper_lane_id"] == c["lane_id"]
        if c["side"] == "BUY":
            assert (
                c["stop_limit_price"]
                <= c["stop_trigger_price"]
                < c["entry_ref_price"]
                < c["be_plus_fee_floor"]
                <= c["lock_at_1r"]
                < c["lock_at_2r"]
                < c["lock_at_3r"]
            )

    dram_card = next(c for c in cards if c["symbol"] == "DRAM")
    assert dram_card["tv_technicals_rating"] == "STRONG_BUY"
    assert dram_card["ta_rating_source"] == "tv_cloud_mcp"
    mstr_card = next(c for c in cards if c["symbol"] == "MSTR")
    assert mstr_card["card_kind"] == "PINE_STAGED"
    assert mstr_card["ta_rating_source"] == "local_ta_composite"
    assert mstr_card["stop_limit_price"] < mstr_card["stop_trigger_price"]

    # 4. Markdown consoles render real presets, 4-session panel table & valid TvAlertV1 webhooks.
    tickets_md = (out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md").read_text(encoding="utf-8")
    assert "No extreme movers" not in tickets_md
    assert "SESSION_EARLY_IGNITION_PRESET" in tickets_md
    assert f"`{doc['schema_validated_count']}/{len(doc['tickets'])}`" in tickets_md

    cards_md = (out_dir / "TV_ORDER_PRESET_CARDS.md").read_text(encoding="utf-8")
    assert "## 1-Click Native TradingView Order Panel Field Table (All 4 Sessions)" in cards_md
    assert "## Copy-Paste TradingView Pine v6 TvAlertV1 JSON Webhook Snippets" in cards_md
    for omni_id in (
        "AUTO_REGIME_ADAPTIVE",
        "LOSSLESS_TIGHT_SCALP",
        "CRASH_DEFENSIVE_INVERSE",
        "MINERVINI_VCP_BREAKOUT",
        "STOCKBEE_EP_MOMENTUM_BURST",
        "STOCKBEE_EXHAUSTION_HAMMER",
        "PARABOLIC_EXHAUSTION_SHORT",
        "OVERNIGHT_24X5_IBKR",
    ):
        assert omni_id in cards_md
    # Parse and validate embedded webhook JSON snippets against strict TvAlertV1.
    json_snippets = re.findall(r'\{"schema_version":"tier0\.tv-alert\.v1"[^\n`]+\}', cards_md)
    assert len(json_snippets) >= 9  # 1 active top card + 8 Omni situations
    for raw_snip in json_snippets:
        parsed = json.loads(raw_snip)
        assert parsed["qty"] == 1 and parsed["semi_auto_confirmed"] is False
        TvAlertV1.model_validate(parsed)

    # 5. tv_mcp_sync_receipt.json refreshes timestamp & local parity without fabricating cloud IDs.
    receipt = json.loads((out_dir / "tv_mcp_sync_receipt.json").read_text(encoding="utf-8"))
    assert receipt["generated_at_utc"] == pre_now.isoformat()
    assert receipt["official_tv_mcp"]["status"] == "AUTHENTICATED_DAILY_QUOTA_EXHAUSTED"
    assert receipt["official_tv_mcp"]["verbatim_error"] == verbatim_quota
    assert receipt["pine_cloud_validations"]["pine/tier0_omni_sota_24x5.pine"]["script_id"] == (
        "USER;real_existing_id"
    )
    assert receipt["local_mcp_parity"]["pinescript_mcp"]["valid_functions"] == 48
    assert receipt["local_mcp_parity"]["nautilus_engine"]["tools_count"] == 10
    assert receipt["live_pack_counts"]["session_early_ignition_count"] >= 3
    assert receipt["live_pack_counts"]["staged_scan_count"] >= 3
    assert receipt["paper_only"] is True
    assert receipt["semi_auto_confirmed"] is False
    assert receipt["live_execution_enabled"] is False


def test_prune_and_archive_staged_presets_and_24x5_preset_setup_automation(
    tmp_path: Path,
) -> None:
    import duckdb

    manifest_dir = tmp_path / "manifests"
    pack_dir = manifest_dir / "tv_sota_pack"
    pack_dir.mkdir(parents=True)
    # 2026-11-27 11:05:00Z = 06:05 ET (PRE_MARKET on Black Friday)
    pre_now = datetime(2026, 11, 27, 11, 5, 10, tzinfo=UTC)
    pre_bar_ms = int(datetime(2026, 11, 27, 11, 5, 0, tzinfo=UTC).timestamp() * 1000)

    # Seed Paper Farm DuckDB ledger with an executed TV alert (noesis-tv-<alert_id>).
    with duckdb.connect(str(manifest_dir / "paper_lane_pa3.duckdb")) as conn:
        conn.execute("CREATE TABLE paper_entry_decisions (client_order_id VARCHAR, symbol VARCHAR)")
        conn.execute(
            "INSERT INTO paper_entry_decisions VALUES ('noesis-tv-f34c45558aabc11f', 'SQQQ')"
        )

    # Seed paper_farm_execution_status.json with a second executed TV order + locked symbols.
    (manifest_dir / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "active_session": "PRE_MARKET",
                "paper_only": True,
                "live_execution_enabled": False,
                "managed_symbols": ["SQQQ", "TQQQ"],
                "active_positions": [
                    {
                        "symbol": "SQQQ",
                        "account": "ALPACA_PA3",
                        "client_order_id": "noesis-tv-c20975fc3b300caf",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    staged_path = pack_dir / "staged_tv_presets.json"
    staged_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "updated_at_utc": "2026-11-27T10:46:15+00:00",
                "semi_auto_confirmed": False,
                "presets": [
                    {
                        **_valid_alert(
                            symbol="MSTR",
                            session="OVERNIGHT",
                            order_type="STOP_LIMIT",
                            trigger_price=180.0,
                            stop_price=178.2,
                            limit_collar_bps=50.0,
                            venue="ALPACA",
                        ),
                        "alert_id": "feabe4e34db95720",
                        "staged_at_utc": "2026-11-27T02:32:52+00:00",
                    },
                    {
                        **_valid_alert(
                            symbol="SQQQ",
                            session="PRE_MARKET",
                            venue="ALPACA",
                            venue_profile="ALPACA_PAPER_1000",
                            trigger_price=45.78,
                            stop_price=45.21,
                        ),
                        "alert_id": "f34c45558aabc11f",
                        "staged_at_utc": "2026-11-27T10:45:40+00:00",
                    },
                    {
                        **_valid_alert(
                            symbol="TQQQ",
                            session="PRE_MARKET",
                            venue="ALPACA",
                            venue_profile="ALPACA_PAPER_1000",
                            trigger_price=48.56,
                            stop_price=47.96,
                        ),
                        "alert_id": "c20975fc3b300caf",
                        "staged_at_utc": "2026-11-27T10:45:45+00:00",
                    },
                    {
                        **_valid_alert(
                            symbol="AMD",
                            session="OVERNIGHT",
                            order_type="STOP_LIMIT",
                            trigger_price=150.0,
                            stop_price=148.5,
                            limit_collar_bps=50.0,
                            venue="ALPACA",
                            venue_profile="ALPACA_PAPER_1000",
                        ),
                        "alert_id": "1111222233334444",
                        "staged_at_utc": "2026-11-27T03:00:00+00:00",
                    },
                    {
                        **_valid_alert(
                            symbol="SOXL",
                            ts_utc_ms=pre_bar_ms,
                            session="PRE_MARKET",
                            venue="ALPACA",
                            venue_profile="ALPACA_PAPER_1000",
                            trigger_price=36.28,
                            stop_price=35.83,
                        ),
                        "alert_id": "df527f1380ac1fb8",
                        "staged_at_utc": "2026-11-27T10:46:15+00:00",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    prune_res = prune_and_archive_staged_presets(
        staged_path=staged_path,
        manifest_dir=manifest_dir,
        now_utc=pre_now,
    )
    assert prune_res["ok"] is True
    assert prune_res["active_session"] == "PRE_MARKET"
    assert prune_res["kept_count"] == 1
    assert prune_res["archived_count"] == 4
    assert prune_res["kept_alert_ids"] == ["df527f1380ac1fb8"]
    assert prune_res["pruned_counts"] == {
        "EXPIRED_SESSION": 1,
        "EXECUTED_ARCHIVED": 2,
        "LEGACY_PRUNED": 1,
    }

    db_path = pack_dir / "tv_half_auto_audit.duckdb"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert len(conn.execute("DESCRIBE tv_staged_presets").fetchall()) == 30
        statuses = dict(conn.execute("SELECT alert_id, status FROM tv_staged_presets").fetchall())
    assert statuses["feabe4e34db95720"] == "LEGACY_PRUNED"
    assert statuses["f34c45558aabc11f"] == "EXECUTED_ARCHIVED"
    assert statuses["c20975fc3b300caf"] == "EXECUTED_ARCHIVED"
    assert statuses["1111222233334444"] == "EXPIRED_SESSION"

    audit_summary = load_tv_audit_summary(db_path)
    assert audit_summary["by_status"]["EXECUTED_ARCHIVED"] == 2
    assert audit_summary["by_status"]["LEGACY_PRUNED"] == 1
    assert audit_summary["by_status"]["EXPIRED_SESSION"] == 1

    # Verify unified preset-setup skips locked SQQQ/TQQQ & stages fresh unlocked movers.
    (manifest_dir / "live_whole_market_movers_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": "2026-11-27T11:05:05Z",
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {"regime": 0, "regime_label": "NEUTRAL"},
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "SQQQ",
                        "last_price": 46.05,
                        "session_move_pct": 1.21,
                        "rvol_normalized": 1.6,
                        "natr_pct": 0.012,
                        "watch_tier": "PREMARKET_IGNITION",
                        "calibrated_category": "PARABOLIC_CLIMAX_SHORT_REVERSAL",
                        "darkpool_trf_ratio": 0.31,
                        "technical_alignment_score": 64.2,
                        "bar_close_utc": "2026-11-27T11:05:00Z",
                    },
                    {
                        "rank": 2,
                        "symbol": "DRAM",
                        "last_price": 37.24,
                        "session_move_pct": 3.13,
                        "rvol_normalized": 2.4,
                        "natr_pct": 0.012,
                        "min_trailing_pct": 0.0045,
                        "watch_tier": "PREMARKET_IGNITION",
                        "calibrated_category": "SUDDEN_VOLUME_IGNITION",
                        "darkpool_trf_ratio": 0.14,
                        "technical_alignment_score": 68.5,
                        "premarket_ignition": True,
                        "bar_close_utc": "2026-11-27T11:05:00Z",
                    },
                    {
                        "rank": 3,
                        "symbol": "EWZ",
                        "last_price": 33.80,
                        "session_move_pct": 3.46,
                        "rvol_normalized": 2.1,
                        "natr_pct": 0.011,
                        "watch_tier": "PREMARKET_IGNITION",
                        "calibrated_category": "SUDDEN_VOLUME_IGNITION",
                        "darkpool_trf_ratio": 0.24,
                        "technical_alignment_score": 71.2,
                        "premarket_ignition": True,
                        "bar_close_utc": "2026-11-27T11:05:00Z",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="non-paper venue_profile"):
        run_preset_setup_automation(
            manifest_dir=manifest_dir,
            out_dir=pack_dir,
            venue="ALPACA",
            venue_profile="ALPACA_LIVE_PRO_LINE_1000",
            now_utc=pre_now,
        )

    setup_res = run_preset_setup_automation(
        manifest_dir=manifest_dir,
        out_dir=pack_dir,
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        max_stage=3,
        now_utc=pre_now,
    )
    assert setup_res["ok"] is True
    assert setup_res["paper_only"] is True
    assert setup_res["semi_auto_confirmed"] is False
    assert setup_res["live_execution_enabled"] is False
    assert setup_res["active_staged_count"] == 3
    assert set(setup_res["active_staged_symbols"]) == {"SOXL", "DRAM", "EWZ"}
    assert "SQQQ" in setup_res["locked_symbols_skipped"]

    staged_after = json.loads(staged_path.read_text(encoding="utf-8"))
    assert staged_after["semi_auto_confirmed"] is False
    assert len(staged_after["presets"]) == 3
    for row in staged_after["presets"]:
        assert row["session"] == "PRE_MARKET"
        assert row["venue_profile"] == "ALPACA_PAPER_1000"
        assert row["qty"] == 1 and row["semi_auto_confirmed"] is False
        assert row["min_trailing_pct"] > 0.0
        assert row["auto_reverse_policy"] == "FLATTEN_FIRST_PEAK_CLIMAX_REVERSE"
        sop = row["session_order_presets"]
        assert sorted(sop.keys()) == ["AFTER_HOURS", "OVERNIGHT", "PRE_MARKET", "RTH"]
        assert sop["RTH"]["order_mode"] == "STOP_MARKET"
        assert sop["RTH"]["alpaca_order_request"] == "TrailingStopOrderRequest"
        assert sop["RTH"]["nautilus_order_type"] == "TrailingStopMarketOrder"
        assert sop["PRE_MARKET"]["order_mode"] == "STOP_LIMIT"
        assert sop["PRE_MARKET"]["collar_bps"] == pytest.approx(35.0)
        assert sop["PRE_MARKET"]["extended_hours"] is True
        assert sop["PRE_MARKET"]["synthetic_stop_ratchet"] is True
        assert sop["PRE_MARKET"]["alpaca_entry_modify_rpm_budget"] == 900
        assert sop["PRE_MARKET"]["alpaca_exit_reserve_rpm"] == 100
        assert sop["PRE_MARKET"]["ibkr_max_rpm"] == 3000
        assert sop["PRE_MARKET"]["ibkr_exit_reserve_rpm"] == 300
        assert sop["OVERNIGHT"]["collar_bps"] == pytest.approx(50.0)
        assert set(row["r_lock_levels"].keys()) >= {
            "be_plus_fee_price",
            "lock_at_1r",
            "lock_at_2r",
            "lock_at_3r",
            "fast_sell_non_noise_reverse_trigger",
        }
        TvAlertV1.model_validate(row)

    order_presets = json.loads((pack_dir / "tv_order_presets.json").read_text(encoding="utf-8"))
    assert order_presets["symbol_order_cards"]
    for card in order_presets["symbol_order_cards"]:
        assert sorted(card["session_order_presets"].keys()) == [
            "AFTER_HOURS",
            "OVERNIGHT",
            "PRE_MARKET",
            "RTH",
        ]
        assert "momentum_trigger_reason" in card

    tv_sh = (REPO_ROOT / "scripts" / "tv_console.sh").read_text(encoding="utf-8")
    tv_ps = (REPO_ROOT / "scripts" / "tv_console.ps1").read_text(encoding="utf-8")
    assert "tv_preset_setup()" in tv_sh and "tv_prune_stale()" in tv_sh
    assert "preset-setup|tv-preset-setup" in tv_sh
    assert "function tv-preset-setup" in tv_ps and "function tv-prune-stale" in tv_ps


def test_session_order_presets_sell_card_preserves_canonical_overnight_short_route() -> None:
    """SELL/SHORT standby & ignition cards must never inherit BUY overnight short flags."""
    from tier0_sota.live.paper_farm import build_symbol_24x5_session_presets
    from tier0_sota.tv.tickets import _ensure_symbol_session_order_presets

    stale_buy_sop = build_symbol_24x5_session_presets(
        symbol="EWZ",
        side="LONG",
        ref_price=Decimal("33.88"),
        natr_pct=Decimal("0.012"),
        alpaca_venue_profile="ALPACA_PAPER_200",
        alpaca_max_rpm=200,
    )
    stale_buy_sop["OVERNIGHT"]["custom_tag"] = "preserved"
    assert stale_buy_sop["OVERNIGHT"]["alpaca_overnight_short_supported"] is True
    assert stale_buy_sop["OVERNIGHT"]["overnight_short_route"] is None

    merged = _ensure_symbol_session_order_presets(
        stale_buy_sop,
        symbol="EWZ",
        side="SHORT",
        ref_price=Decimal("33.88"),
        natr_pct=Decimal("0.012"),
        venue_profile="ALPACA_PAPER_1000",
    )
    ovn = merged["OVERNIGHT"]
    assert ovn["alpaca_overnight_short_supported"] is False
    assert str(ovn["overnight_short_route"] or "").startswith("IBKR_PAPER_4002_OR_INVERSE_ETF_")
    assert ovn["initial_stop_price"] > ovn["entry_limit_price"]
    assert ovn["alpaca_venue_profile"] == "ALPACA_PAPER_1000"
    assert ovn["alpaca_max_rpm"] == 1000
    assert ovn["custom_tag"] == "preserved"

    presets = build_tv_order_presets(
        {
            "generated_at_et": "2026-11-27T06:45:00-05:00",
            "active_session": "PRE_MARKET",
            "venue": "ALPACA",
            "venue_profile": "ALPACA_PAPER_1000",
            "macro_regime": {"regime": -1, "regime_label": "BEAR"},
            "tickets": [
                {
                    "symbol": "EWZ",
                    "side": "SELL",
                    "session": "PRE_MARKET",
                    "trigger_price": 33.88,
                    "stop_price": 34.30,
                    "natr_pct": 0.012,
                    "rvol_normalized": 2.6,
                    "score": 82.0,
                    "venue": "ALPACA",
                    "venue_profile": "ALPACA_PAPER_1000",
                    "order_type": "STOP_LIMIT",
                    "limit_collar_bps": 35.0,
                    "trailing_schedule": "tight_near_lossless",
                    "session_order_presets": stale_buy_sop,
                }
            ],
            "standby_presets": [
                {
                    "symbol": "ANT",
                    "side": "SELL",
                    "session": "PRE_MARKET",
                    "trigger_price": 85.84,
                    "stop_price": 86.91,
                    "natr_pct": 0.012,
                    "rvol_normalized": 2.1,
                    "score": 76.0,
                    "venue": "ALPACA",
                    "venue_profile": "ALPACA_PAPER_1000",
                    "order_type": "STOP_LIMIT",
                    "limit_collar_bps": 35.0,
                    "trailing_schedule": "tight_near_lossless",
                    "session_order_presets": stale_buy_sop,
                }
            ],
        }
    )
    sell_cards = [c for c in presets["symbol_order_cards"] if c.get("side") == "SELL"]
    assert len(sell_cards) >= 2
    for card in sell_cards:
        sop = card["session_order_presets"]
        assert sorted(sop.keys()) == ["AFTER_HOURS", "OVERNIGHT", "PRE_MARKET", "RTH"]
        assert sop["OVERNIGHT"]["alpaca_overnight_short_supported"] is False
        assert str(sop["OVERNIGHT"]["overnight_short_route"] or "").startswith(
            "IBKR_PAPER_4002_OR_INVERSE_ETF_"
        )
        assert sop["OVERNIGHT"]["collar_bps"] == pytest.approx(50.0)
        assert sop["PRE_MARKET"]["collar_bps"] == pytest.approx(35.0)
        assert sop["AFTER_HOURS"]["collar_bps"] == pytest.approx(35.0)
        assert sop["RTH"]["collar_bps"] == pytest.approx(0.0)


def test_movers_first_watchlist_zero_bare_tickers_and_market_feed_reasons() -> None:
    """Movers-first watchlist leads with ###01_LIVE_EXTREME_MOVERS_TOP and carries feed reasons."""
    from tier0_sota.tv.tickets import (
        build_tv_watchlist_sync_payload,
        render_order_preset_cards_markdown,
        render_tv_movers_first_watchlist,
    )

    ranked = [
        {
            "symbol": "XP",
            "last_price": 17.04,
            "session_move_pct": 18.42,
            "daily_change_pct": 18.42,
            "intraday_window_pct": -1.45,
            "rvol": 2.1,
            "score": 88.5,
            "side": "BUY",
            "ranking_reason": "+18.42% daily | RVOL 2.10x | Macro:INTRADAY_GAP_FADE_IN_GREEN_TAPE",
            "market_feed_reason": (
                "INTRADAY_GAP_FADE_IN_GREEN_TAPE: Daily green while 1m pulled back"
            ),
        },
        {
            "symbol": "CRWV",
            "last_price": 40.92,
            "session_move_pct": -3.87,
            "daily_change_pct": -3.87,
            "intraday_window_pct": -3.87,
            "rvol": 1.8,
            "score": 65.0,
            "side": "SELL",
            "darkpool_trf_ratio": 0.45,
        },
        {"symbol": "HL", "last_price": 11.84, "session_move_pct": 4.97, "rvol": 1.6, "score": 60.0},
    ]
    txt = render_tv_movers_first_watchlist(
        [], ranked_entries=ranked, staged_presets=[{"symbol": "NU"}]
    )
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    assert lines[0] == "###01_LIVE_EXTREME_MOVERS_TOP"
    assert lines[1] == "NASDAQ:XP"
    assert "NASDAQ:CRWV" in lines
    assert "NYSE:HL" in lines
    assert "NYSE:NU" in lines
    sym_lines = [ln for ln in lines if not ln.startswith("###")]
    assert all(":" in ln for ln in sym_lines), f"Bare ticker leaked: {sym_lines}"

    macro = {
        "regime": 0,
        "regime_label": "NEUTRAL",
        "regime_reason": "INTRADAY_GAP_FADE_IN_GREEN_TAPE: Daily green while 1m pulled back",
    }
    doc = {
        "generated_at_et": "2026-11-27T10:00:00-05:00",
        "active_session": "RTH",
        "venue": "ALPACA",
        "venue_profile": "ALPACA_PAPER_1000",
        "macro_regime": macro,
        "tickets": [],
        "standby_presets": ranked,
    }
    payload = build_tv_watchlist_sync_payload(doc, movers={"entries": ranked})
    assert (
        payload["watchlists"]["active"]["mcp_symbol_sync_tool"] == "mcp-watchlist-add-to-watchlist"
    )
    assert payload["macro_regime"]["regime_reason"] == macro["regime_reason"]
    xp_row = next(r for r in payload["categorized_symbols"] if r["symbol"] == "XP")
    assert xp_row["daily_change_pct"] == pytest.approx(18.42)
    assert "INTRADAY_GAP_FADE_IN_GREEN_TAPE" in str(xp_row["ranking_reason"])

    md = render_order_preset_cards_markdown(
        {
            "generated_at_et": "2026-11-27T10:00:00-05:00",
            "active_session": "RTH",
            "macro_regime": macro,
            "symbol_order_cards": [],
        }
    )
    assert "**Market Feed Indicator Reason:** INTRADAY_GAP_FADE_IN_GREEN_TAPE" in md

    # Full-market + Early-Stage Ignition sections render with 0 bare tickers
    full_movers = [
        {
            "symbol": "POM",
            "exchange": "NASDAQ",
            "tv_symbol": "NASDAQ:POM",
            "daily_change_pct": 83.85,
        },
        {
            "symbol": "UFG",
            "exchange": "NASDAQ",
            "tv_symbol": "NASDAQ:UFG",
            "daily_change_pct": 56.69,
        },
        {
            "symbol": "XLAB",
            "exchange": "NASDAQ",
            "tv_symbol": "NASDAQ:XLAB",
            "daily_change_pct": 37.25,
        },
        {
            "symbol": "RETO",
            "exchange": "NASDAQ",
            "tv_symbol": "NASDAQ:RETO",
            "daily_change_pct": 28.53,
        },
    ]
    early_movers = [
        {
            "symbol": "KYNB",
            "exchange": "NASDAQ",
            "tv_symbol": "NASDAQ:KYNB",
            "daily_change_pct": 21.51,
        },
        {
            "symbol": "EWZS",
            "exchange": "NASDAQ",
            "tv_symbol": "NASDAQ:EWZS",
            "daily_change_pct": 13.78,
        },
    ]
    txt_full = render_tv_movers_first_watchlist(
        [],
        ranked_entries=ranked,
        full_market_movers=full_movers,
        early_stage_movers=early_movers,
    )
    flines = [ln for ln in txt_full.splitlines() if ln.strip()]
    assert flines[0] == "###01_LIVE_EXTREME_MOVERS_TOP"
    assert "NASDAQ:POM" in flines and "NASDAQ:UFG" in flines and "NASDAQ:RETO" in flines
    assert "###02_EARLY_STAGE_IGNITION_TOP" in flines
    assert "NASDAQ:KYNB" in flines and "NASDAQ:EWZS" in flines
    assert all(":" in ln for ln in flines if not ln.startswith("###"))
    payload2 = build_tv_watchlist_sync_payload(
        doc,
        movers={
            "entries": ranked,
            "full_market_top_movers": full_movers,
            "early_stage_ignition_movers": early_movers,
            "full_market_sweep_summary": {"universe_scanned": 12005},
        },
    )
    assert payload2["full_market_sweep_summary"]["universe_scanned"] == 12005
    assert payload2["watchlists"]["active"]["symbols"][0] == "NASDAQ:POM"
    assert len(payload2["early_stage_ignition_movers"]) == 2


def test_order_preset_cards_sweep_summary_early_stage_table_and_zero_bare_staged(
    tmp_path: Path,
) -> None:
    """TV_ORDER_PRESET_CARDS renders sweep summary + early-stage table with 0 bare tickers."""
    from tier0_sota.tv.tickets import (
        refresh_tickets_from_paths,
        render_tv_movers_first_watchlist,
    )

    out_dir = tmp_path / "tv_sota_pack"
    out_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / ".nms_equity_universe_cache.json").write_text(
        json.dumps(
            {
                "ny_date": "2026-11-27",
                "symbols": {"AEHL": "NASDAQ", "COHX": "BATS", "XP": "NASDAQ", "FIEE": "NASDAQ"},
            }
        ),
        encoding="utf-8",
    )
    bar_ts = "2026-11-27T20:05:00Z"
    now_ts = datetime(2026, 11, 27, 20, 5, 10, tzinfo=UTC)
    stage_tv_alert_preset(
        _valid_alert(
            symbol="COHX",
            session="AFTER_HOURS",
            order_type="STOP_LIMIT",
            trigger_price=10.38,
            stop_price=10.26,
            limit_collar_bps=35.0,
        ),
        staged_path=out_dir / "staged_tv_presets.json",
        watchlist_path=tmp_path / "unused.json",
        venue="ALPACA",
    )
    stage_tv_alert_preset(
        _valid_alert(
            symbol="AEHL",
            session="AFTER_HOURS",
            order_type="STOP_LIMIT",
            trigger_price=13.30,
            stop_price=13.14,
            limit_collar_bps=35.0,
        ),
        staged_path=out_dir / "staged_tv_presets.json",
        watchlist_path=tmp_path / "unused.json",
        venue="ALPACA",
    )
    movers_path = tmp_path / "live_whole_market_movers_manifest.json"
    movers_path.write_text(
        json.dumps(
            {
                "schema_version": "tier0.whole-market-movers.v2",
                "generated_at_utc": bar_ts,
                "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
                "pin_symbols": ["SOXL", "SOXS", "TQQQ", "SQQQ"],
                "macro_regime": {
                    "regime": 0,
                    "regime_label": "NEUTRAL",
                    "regime_reason": "MIXED_OR_TRANSITIONAL: SPY +1.74%, QQQ +2.57%, SOXX +4.16%",
                },
                "full_market_sweep_summary": {
                    "enabled": True,
                    "universe_scanned": 12005,
                    "qualifying_movers_count": 1299,
                    "early_stage_count": 29,
                    "extreme_expansion_count": 21,
                    "parabolic_climax_count": 10,
                    "elapsed_secs": 1.23,
                },
                "full_market_top_movers": [
                    {
                        "symbol": "AEHL",
                        "exchange": "NASDAQ",
                        "tv_symbol": "NASDAQ:AEHL",
                        "stage": "PARABOLIC_CLIMAX_SURGE",
                        "price_tier": "INSTITUTIONAL_GE10",
                        "last_price": 13.30,
                        "daily_change_pct": 52.40,
                        "tod_rvol": 14.5,
                        "spread_bps": 18.4,
                        "adv_20": 4_250_000.0,
                        "split_adjusted": False,
                        "dollar_vol": 42_500_000.0,
                        "ranking_reason": "+52.40% daily | PaceRVOL 14.50x | Climax Surge",
                    },
                    {
                        "symbol": "PENNY",
                        "exchange": "NASDAQ",
                        "tv_symbol": "NASDAQ:PENNY",
                        "stage": "PARABOLIC_CLIMAX_SURGE",
                        "price_tier": "SUB_DOLLAR",
                        "last_price": 0.45,
                        "daily_change_pct": 90.0,
                        "tod_rvol": 20.0,
                        "dollar_vol": 2_000_000.0,
                        "ranking_reason": "Should be filtered out (< $1)",
                    },
                ],
                "early_stage_ignition_movers": [
                    {
                        "symbol": "XP",
                        "exchange": "NASDAQ",
                        "tv_symbol": "NASDAQ:XP",
                        "stage": "EARLY_STAGE_IGNITION",
                        "price_tier": "INSTITUTIONAL_GE10",
                        "last_price": 17.30,
                        "daily_change_pct": 18.42,
                        "tod_rvol": 4.25,
                        "spread_bps": 24.5,
                        "adv_20": 8_750_000.0,
                        "split_adjusted": True,
                        "split_factor": 5.0,
                        "dollar_vol": 195_000_000.0,
                        "ranking_reason": "+18.42% daily | PaceRVOL 4.25x | Early Ignition",
                    }
                ],
                "entries": [
                    {
                        "rank": 1,
                        "symbol": "AEHL",
                        "exchange": "NASDAQ",
                        "score": 82.0,
                        "last_price": 13.30,
                        "prior_close": 8.73,
                        "session_move_pct": 52.40,
                        "rvol_normalized": 6.0,
                        "natr_pct": 0.014,
                        "darkpool_trf_ratio": 0.46,
                        "unusual_volume_buildup": True,
                        "extreme_mover_eligible": True,
                        "bar_close_utc": bar_ts,
                    },
                    {
                        "rank": 2,
                        "symbol": "SNDQ",
                        "exchange": "BATS",
                        "side": "SELL",
                        "score": 68.0,
                        "last_price": 34.78,
                        "prior_close": 36.10,
                        "session_move_pct": -3.66,
                        "rvol_normalized": 2.4,
                        "natr_pct": 0.012,
                        "darkpool_trf_ratio": 0.74,
                        "watch_tier": "UV_DARKPOOL_BUILDUP",
                        "extreme_mover_eligible": False,
                        "bar_close_utc": bar_ts,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    refresh_tickets_from_paths(
        movers_path=movers_path,
        out_dir=out_dir,
        venue="ALPACA",
        venue_profile="ALPACA_PAPER_1000",
        now_utc=now_ts,
    )

    wl_mf = (out_dir / "tier0_sota_tv_movers_first_live.txt").read_text(encoding="utf-8")
    mf_lines = [ln.strip() for ln in wl_mf.splitlines() if ln.strip()]
    assert mf_lines[0] == "###01_LIVE_EXTREME_MOVERS_TOP"
    assert "###02_EARLY_STAGE_IGNITION_TOP" in mf_lines
    sym_lines = [ln for ln in mf_lines if not ln.startswith("###")]
    assert all(":" in ln for ln in sym_lines), f"Bare ticker in movers-first: {sym_lines}"
    assert "BATS:COHX" in sym_lines and "NASDAQ:AEHL" in sym_lines
    assert sym_lines.count("NASDAQ:AEHL") == 1

    presets = json.loads((out_dir / "tv_order_presets.json").read_text(encoding="utf-8"))
    assert presets["full_market_sweep_summary"]["universe_scanned"] == 12005
    assert presets["paper_only"] is True and presets["live_execution_enabled"] is False
    assert presets["semi_auto_confirmed"] is False and presets["qty"] == 1
    for card in presets["symbol_order_cards"]:
        assert ":" in str(card["tv_symbol"]), f"Bare tv_symbol on card: {card}"
        sop = card["session_order_presets"]
        assert sorted(sop.keys()) == ["AFTER_HOURS", "OVERNIGHT", "PRE_MARKET", "RTH"]
        if card["side"] == "SELL":
            assert sop["OVERNIGHT"]["alpaca_overnight_short_supported"] is False
            assert str(sop["OVERNIGHT"]["overnight_short_route"] or "").startswith(
                "IBKR_PAPER_4002_OR_INVERSE_ETF_"
            )
        else:
            assert sop["OVERNIGHT"]["alpaca_overnight_short_supported"] is True
            assert sop["OVERNIGHT"]["overnight_short_route"] is None
        assert card["be_plus_fee_floor"] is not None
        assert all(card[k] is not None for k in ("lock_at_1r", "lock_at_2r", "lock_at_3r"))

    cards_md = (out_dir / "TV_ORDER_PRESET_CARDS.md").read_text(encoding="utf-8")
    assert "**Market Feed Indicator Reason:** MIXED_OR_TRANSITIONAL" in cards_md
    assert "**Entire-US-Market Sweep Summary:** `12,005` NMS equities in `1.23s`" in cards_md
    assert "Early-Stage Ignition `29` | Extreme Expansion `21` | Parabolic Climax `10`" in cards_md
    assert "## Top Entire-Market Extreme Movers & Early-Stage Ignition (>= $1)" in cards_md
    assert "| Spread (bps) | ADV20 | Split Adj |" in cards_md
    assert "`18.4` | `4.25M` | `NO`" in cards_md
    assert "`24.5` | `8.75M` | `YES (5x)`" in cards_md
    assert "`NASDAQ:AEHL`" in cards_md and "`NASDAQ:XP`" in cards_md and "`BATS:COHX`" in cards_md
    assert "+18.42% daily · PaceRVOL 4.25x · Early Ignition" in cards_md
    assert "NASDAQ:PENNY" not in cards_md

    payload = json.loads((out_dir / "tv_watchlist_sync_payload.json").read_text(encoding="utf-8"))
    for wl_key in ("active", "alert_bound"):
        syms = payload["watchlists"][wl_key]["symbols"]
        assert all(":" in s for s in syms), f"Bare ticker in payload[{wl_key}]: {syms}"
    expected_top10 = payload["watchlists"]["active"]["symbols"][:10]
    expected_sha = hashlib.sha256(",".join(expected_top10).encode("utf-8")).hexdigest()
    assert payload["top10_membership_symbols"] == expected_top10
    assert payload["top10_membership_sha256"] == expected_sha
    assert payload["min_cloud_sync_interval_secs"] == 900
    assert payload["sync_policy"] == "delta_gated_15m_max_96_per_day"

    # Direct render call with un-prefixed staged row resolves via full_market_movers automatically
    direct_txt = render_tv_movers_first_watchlist(
        [],
        full_market_movers=[{"symbol": "COHX", "exchange": "BATS", "tv_symbol": "BATS:COHX"}],
        staged_presets=[{"symbol": "COHX"}],
    )
    direct_syms = [
        ln.strip() for ln in direct_txt.splitlines() if ln.strip() and not ln.startswith("###")
    ]
    assert direct_syms == [
        "BATS:COHX",
        "AMEX:SOXS",
        "NASDAQ:SQQQ",
        "AMEX:SPXS",
        "AMEX:SDOW",
        "AMEX:SPY",
        "NASDAQ:QQQ",
        "NASDAQ:SOXX",
        "AMEX:IWM",
        "AMEX:RSP",
        "CBOE:VIX",
        "NASDAQ:TLT",
    ]


def test_refuse_live_endpoints_allows_market_decimals_and_hex_ids_while_blocking_live() -> None:
    """Regression test: fractional market prices/ratios & hex IDs must not false-positive,
    while every live broker host or live port marker remains strictly fail-closed."""
    # True negatives — finite fractional floats/Decimals, stringified decimals,
    # hex alert_ids / client_order_ids, UUIDs, ISO timestamps with .4001 microseconds.
    benign_payload: dict[str, Any] = {
        "price": 74.96,
        "entry_price": 40.01,
        "ts_utc_ms": 1795874960000,
        "trigger_price": 12.4001,
        "stop_price": Decimal("12.2000"),
        "darkpool_trf_ratio": 0.7496,
        "hidden_volume_ratio": Decimal("0.008796"),
        "natr_pct": 101.8798,
        "rvol": 5.8796,
        "high_price_stock": 4001.5,
        "high_price_int": 4001,
        "daily_volume": 7496,
        "share_count": 9223,
        "high_price_dec": Decimal("7496.25"),
        "str_prices": ["12.4001", "74.96", "40.01", "0.7496", "101.8798", "5.8796", "4001.50"],
        "alert_id": "a4001f9c2e8b7d10",
        "alt_alert_ids": [
            "c7496d8e1a2b3c4d",
            "4001a9c2e8b7d10f",
            "9c2e8b7d10fa8798",
            "noesis-tv-9c2e8b7d10fa4001",
        ],
        "uuid": "550e8400-4001-41d4-a716-446655440000",
        "staged_at_utc": "2026-11-27T15:51:19.4001+00:00",
        "dt_obj": datetime(2026, 11, 27, 15, 51, 19, 400100, tzinfo=UTC),
        "notes": "trigger=12.4001 trf=0.7496 high=4001.50 price=74.96",
        "paper_url": "https://paper-api.alpaca.markets/v2/orders",
        "paper_port": 4002,
        "flag_true": True,
        "flag_false": False,
        "none_val": None,
    }
    refuse_live_endpoints(benign_payload)

    # Full validate_tv_alert_payload round-trip with .4001 / .7496 / .8796 / .8798 values
    validated = validate_tv_alert_payload(
        _valid_alert(
            trigger_price=74.96,
            stop_price=74.01,
            ts_utc_ms=1795874960000,
            stop_distance_pct=0.012673,
            darkpool_trf_ratio=0.7496,
            hidden_volume_ratio=0.008796,
            rvol=5.8796,
            natr_pct=0.08798,
            trailing_schedule="wide_natr_adaptive",
        )
    )
    assert validated["symbol"] == "NVDA"
    assert validated["trigger_price"] == pytest.approx(74.96)

    # True positives — EVERY live/CDP port or live host form must STILL fail closed
    forbidden_payloads: list[dict[str, Any]] = [
        {"port": 4001},
        {"port": 7496},
        {"port": 8798},
        {"port": 8796},
        {"port": 9223},
        {"cdp": 9223},
        {"cdp": "9223"},
        {"cdp_port": 9223},
        {"port": 4001.0},
        {"port": Decimal("7496.00")},
        {"port": "4001"},
        {"port": " 7496 "},
        {"port": "4001.0"},
        {"port": "4.001e3"},
        {"ports": [4002, 8798]},
        {"host": "127.0.0.1:4001"},
        {"host": "localhost:4001"},
        {"host": "localhost:7496"},
        {"url": "http://127.0.0.1:7496"},
        {"url": "ibkr://host:8798"},
        {"url": "http://127.0.0.1:8796/v1"},
        {"url": "http://127.0.0.1:9223/json"},
        {"cfg": "port=8796"},
        {"cfg": "port 8798"},
        {"cfg": "--port=4001"},
        {"cfg": "port=4001.0"},
        {"cfg": "cdp=9223"},
        {"cfg": "live4001"},
        {"endpoint_4001": "paper"},
        {"url": "https://api.alpaca.markets/v2/orders"},
        {"url": "https://live.alpaca.markets/v2/orders"},
        {
            "paper": "https://paper-api.alpaca.markets/v2/orders",
            "live": "https://api.alpaca.markets/v2/orders",
        },
        {"nested": [{"host": "127.0.0.1:4001"}]},
        {"mixed": "price=12.4001 host=127.0.0.1:4001"},
        {"mixed_hex": "id=a4001f9c2e8b7d10 host=localhost:7496"},
        {"raw_bytes": b"127.0.0.1:4001"},
    ]
    for bad in forbidden_payloads:
        with pytest.raises(ValueError, match="live broker endpoint"):
            refuse_live_endpoints(bad)


def test_finos_ag_grid_plugin_pin() -> None:
    """Both FINOS command-center payloads pin the official ag-grid widget plugin version."""
    payloads = {
        "tv_compounding_manifest": build_tv_compounding_manifest({})[
            "finos_command_center_summary"
        ]["plugins"],
        "tv_layout_blueprints": build_tv_layout_blueprints({})["finos_openbb_command_center"][
            "plugins"
        ],
    }
    for name, plugins in payloads.items():
        ag_grid = [plugin for plugin in plugins if plugin.startswith("@finos-workspace/ag-grid")]
        assert ag_grid == ["@finos-workspace/ag-grid@2.3.0"], name
        for plugin in plugins:
            # Exact released versions of the official @finos-workspace plugins only.
            assert re.fullmatch(r"@finos-workspace/[a-z0-9-]+@\d+\.\d+\.\d+", plugin), plugin

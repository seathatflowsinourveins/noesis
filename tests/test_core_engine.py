"""Core engine, discovery, risk & 6-gate falsification behavioral tests."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from nautilus_trader.common import Environment
from nautilus_trader.model import TrailingOffsetType

from tier0_sota.discovery import (
    BAR_DURATION_NS,
    DISCOVERY_WALL_UTC_NS,
    PreWallViolationError,
    assert_pre_wall_date,
    assert_pre_wall_ns,
    compute_causal_market_regime,
    generate_upstream_reversal_signals,
    previous_xnys_session,
    validate_causal_bars,
)
from tier0_sota.engine import (
    EXECUTION_VENUE_PROFILES,
    IBKR_OUTSIDE_RTH_TAG,
    SESSION_PROTECTION_24X5,
    build_ibkr_paper_connection_spec,
    build_sandbox_live_node,
    build_venue_live_node_config,
    build_venue_risk_engine_config,
    classify_us_equity_session_et,
    compute_collared_limit_price,
    compute_dynamic_trailing_stop,
    compute_progressive_trailing_stop,
    get_session_protection_profile,
    inspect_venue_capability,
    run_nautilus_signal_backtest,
    select_pooled_trailing_schedule,
    validate_trailing_offset_type,
    validate_venue_order_capability,
)
from tier0_sota.research import (
    MIN_CPCV_SAMPLES,
    evaluate_hftbacktest_queue_markout,
    evaluate_six_gate_falsification,
    falsify_trade_returns,
)
from tier0_sota.risk import (
    MAX_STOP_LOSS_PCT,
    DailyDrawdownBreaker,
    ProtectiveExitRateBudget,
    Side,
    compute_capped_stop_price,
    quantize_rule_612,
    size_position_by_risk,
)

NY_TZ = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parent.parent


def _et_ns(y: int, m: int, d: int, hh: int, mm: int) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=NY_TZ).astimezone(UTC).timestamp()) * 1_000_000_000


def _synth_bars(
    symbol: str,
    closes: list[float],
    *,
    start_et: tuple[int, int] = (9, 30),
    step_s: int = 60,
    wick: float = 0.001,
) -> pl.DataFrame:
    t0 = _et_ns(2026, 10, 15, start_et[0], start_et[1])
    rows = []
    for i, c in enumerate(closes):
        t_ns = t0 + i * step_s * 1_000_000_000
        o = closes[i - 1] if i > 0 else c
        rows.append(
            {
                "symbol": symbol,
                "t_ns": t_ns,
                "t_close_ns": t_ns + BAR_DURATION_NS,
                "o": float(o),
                "h": float(max(o, c) * (1.0 + wick)),
                "l": float(min(o, c) * (1.0 - wick)),
                "c": float(c),
                "v": 25_000.0,
            }
        )
    return pl.DataFrame(rows)


def test_risk_sha256_and_invariants() -> None:
    rp = REPO_ROOT / "src" / "tier0_sota" / "risk.py"
    digest = hashlib.sha256(rp.read_bytes()).hexdigest()
    assert digest == "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
    assert len(rp.read_text(encoding="utf-8").splitlines()) == 361

    breaker = DailyDrawdownBreaker(start_equity=Decimal("100000"))
    assert breaker.update(realized_pnl=Decimal("-1500")) is False
    assert breaker.allow_entry() is False
    assert breaker.update(realized_pnl=Decimal("5000")) is False

    budget = ProtectiveExitRateBudget(max_rpm=200)
    assert budget.exit_reserve_rpm == 20
    for i in range(180):
        assert budget.try_consume(1000.0 + i * 0.01, is_exit=False) is True
    assert budget.try_consume(1002.0, is_exit=False) is False
    assert budget.try_consume(1002.0, is_exit=True) is True
    assert (
        size_position_by_risk(
            equity=Decimal("100000"), entry_price=Decimal("100"), stop_price=Decimal("98")
        )
        == 200
    )


@given(
    entry=st.decimals(min_value="0.10", max_value="500.00", places=2),
    side=st.sampled_from(["LONG", "SHORT"]),
)
@settings(max_examples=40)
def test_rule_612_stop_cap_property(entry: Decimal, side: Side) -> None:
    stop = compute_capped_stop_price(entry, side)
    loss = (entry - stop) / entry if side == "LONG" else (stop - entry) / entry
    assert Decimal("0") < loss <= MAX_STOP_LOSS_PCT
    tick = Decimal("0.01") if stop >= Decimal("1.00") else Decimal("0.0001")
    assert stop == quantize_rule_612(stop, side=side, is_stop=True)
    assert stop % tick == Decimal("0")


def test_venue_capability_matrix_and_fail_closed_live() -> None:
    ib = inspect_venue_capability("IBKR_PAPER_4002", require_24x5_overnight=True)
    assert ib["allowed"] is True and ib["max_rpm"] == 3000 and ib["exit_reserve_rpm"] == 300
    ap = inspect_venue_capability("ALPACA_PAPER_200", algo="DMA", require_24x5_overnight=True)
    assert ap["allowed"] is False and ap["max_rpm"] == 200 and len(ap["reasons"]) == 2
    assert validate_venue_order_capability("ALPACA_PAPER_200").max_rpm == 200

    live_ids = ("IBKR_LIVE_3000", "ALPACA_LIVE_PRO_LINE_1000", "ALPACA_LIVE_ELITE_SMART_ROUTER")
    for live_id in live_ids:
        assert EXECUTION_VENUE_PROFILES[live_id].live_execution_enabled is False
        with pytest.raises(PermissionError, match="live_execution_enabled=False"):
            validate_venue_order_capability(live_id)

    for bad_port in (4001, 7496, 8798, 8796):
        with pytest.raises(ValueError):
            build_ibkr_paper_connection_spec(port=bad_port)
    assert build_ibkr_paper_connection_spec(port=4002)["paper_only"] is True

    re_cfg = build_venue_risk_engine_config("IBKR_PAPER_4002")
    assert re_cfg.bypass is False
    ln_cfg = build_venue_live_node_config("ALPACA_PAPER_200")
    assert ln_cfg.environment == Environment.SANDBOX
    node = build_sandbox_live_node()
    try:
        assert node.environment == Environment.SANDBOX
    finally:
        node.dispose()


def test_24x5_session_protection_and_4tier_progressive_trailing() -> None:
    assert classify_us_equity_session_et(_et_ns(2026, 10, 15, 5, 0)) == "PRE_MARKET"
    assert classify_us_equity_session_et(_et_ns(2026, 10, 15, 10, 0)) == "RTH"
    assert classify_us_equity_session_et(_et_ns(2026, 10, 15, 17, 0)) == "AFTER_HOURS"
    assert classify_us_equity_session_et(_et_ns(2026, 10, 15, 22, 0)) == "OVERNIGHT"
    assert classify_us_equity_session_et(_et_ns(2026, 10, 17, 10, 0)) == "CLOSED"
    assert classify_us_equity_session_et(_et_ns(2025, 11, 28, 12, 55)) == "RTH"
    assert classify_us_equity_session_et(_et_ns(2025, 11, 28, 13, 5)) == "AFTER_HOURS"

    rth = get_session_protection_profile("RTH")
    ovn = get_session_protection_profile("OVERNIGHT")
    assert rth.order_mode == "STOP_MARKET" and rth.outside_rth is False
    assert ovn.order_mode == "STOP_LIMIT" and ovn.outside_rth is True
    col_px = compute_collared_limit_price(Decimal("100.00"), "LONG", ovn.limit_collar_pct)
    assert col_px == Decimal("99.50")
    tot_bp = TrailingOffsetType.BASIS_POINTS
    assert validate_trailing_offset_type(tot_bp) == tot_bp

    sched_name, _ = select_pooled_trailing_schedule(Decimal("0.010"))
    assert sched_name == "tight_near_lossless"

    prof = SESSION_PROTECTION_24X5["RTH"]
    entry, init_s = Decimal("100.00"), Decimal("98.00")
    s1, t1, _ = compute_progressive_trailing_stop(
        entry_price=entry,
        side="LONG",
        favorable_extreme=Decimal("100.50"),
        current_stop=init_s,
        profile=prof,
        initial_stop=init_s,
    )
    s2, t2, _ = compute_progressive_trailing_stop(
        entry_price=entry,
        side="LONG",
        favorable_extreme=Decimal("102.20"),
        current_stop=s1,
        profile=prof,
        initial_stop=init_s,
    )
    s3, t3, _ = compute_progressive_trailing_stop(
        entry_price=entry,
        side="LONG",
        favorable_extreme=Decimal("104.20"),
        current_stop=s2,
        profile=prof,
        initial_stop=init_s,
    )
    s4, t4, _ = compute_progressive_trailing_stop(
        entry_price=entry,
        side="LONG",
        favorable_extreme=Decimal("106.50"),
        current_stop=s3,
        profile=prof,
        initial_stop=init_s,
    )
    assert (t1, t2, t3, t4) == (1, 2, 3, 4)
    assert init_s < s1 < s2 < s3 < s4
    assert (
        compute_dynamic_trailing_stop(
            entry_price=entry,
            side="LONG",
            favorable_extreme=Decimal("106.50"),
            current_stop=s3,
            profile=prof,
            initial_stop=init_s,
        )
        == s4
    )


def test_nautilus_backtest_4tier_reentry_and_autoreverse() -> None:
    closes = [100.0, 100.5, 102.0, 104.0, 106.5, 107.0, 107.2, 107.0, 106.5, 105.0, 105.2, 105.5]
    df = _synth_bars("NVDA", closes, start_et=(15, 55), wick=0.0)
    ts_list = df["t_close_ns"].to_list()
    signals = {ts_list[0]: 1, ts_list[6]: -1, ts_list[8]: 0, ts_list[9]: -1}
    res = run_nautilus_signal_backtest(
        df, signals, reentry_cooldown_secs=60, fixed_qty=10, enable_24x5_trailing=True
    )
    assert res.bars_processed == len(closes)
    assert res.stops_attached >= 2
    assert res.trailing_ratchets >= 1
    assert res.max_lock_tier == 4
    assert res.auto_reverses >= 1
    assert res.session_transitions >= 1
    assert res.auto_reentries >= 1
    assert IBKR_OUTSIDE_RTH_TAG.startswith("IBOrderTags")


def test_causal_pit_regime_and_upstream_signals() -> None:
    with pytest.raises(PreWallViolationError):
        assert_pre_wall_date("2026-10-16")
    with pytest.raises(PreWallViolationError):
        assert_pre_wall_ns(DISCOVERY_WALL_UTC_NS + 1)
    assert previous_xnys_session("2026-10-12") == date(2026, 10, 9)

    spy = _synth_bars("SPY", [500.0 - i * 0.25 for i in range(60)])
    qqq = _synth_bars("QQQ", [420.0 - i * 0.30 for i in range(60)])
    soxx = _synth_bars("SOXX", [220.0 - i * 0.25 for i in range(60)])
    vix = _synth_bars("VIX", [28.0 + i * 0.05 for i in range(60)])
    mdf = validate_causal_bars(pl.concat([spy, qqq, soxx, vix]))
    reg_map = compute_causal_market_regime(mdf, vix_stress_threshold=25.0)
    assert any(v == -1 for v in list(reg_map.values())[-15:])

    soxl = _synth_bars("SOXL", [30.0 - i * 0.15 for i in range(60)])
    sigs = generate_upstream_reversal_signals(
        soxl, market_regime_by_ts=reg_map, require_peak_confirmation=False
    )
    assert any(v == -1 for v in sigs.values())


def test_six_gate_falsification_and_hftbacktest(tmp_path: Path) -> None:
    t0 = _et_ns(2026, 10, 15, 9, 30)
    n_q = 40
    q_df = pl.DataFrame(
        {
            "t_ns": [t0 + i * 1_000_000_000 for i in range(n_q)],
            "bp": [100.00 + (0.02 if i > 15 else 0.0) for i in range(n_q)],
            "bs": [500.0] * n_q,
            "ap": [100.02 + (0.02 if i > 15 else 0.0) for i in range(n_q)],
            "as": [500.0] * n_q,
        }
    )
    t_df = pl.DataFrame(
        {
            "t_ns": [t0 + i * 2_000_000_000 + 500_000_000 for i in range(15)],
            "p": [100.00] * 15,
            "s": [600.0] * 15,
        }
    )
    l1 = evaluate_hftbacktest_queue_markout(q_df, t_df, symbol="NVDA", side="BUY")
    assert l1["events_replayed"] > 0 and l1["passive_order_submitted"] is True

    short_arr = np.full(MIN_CPCV_SAMPLES - 1, 0.01)
    too_short = falsify_trade_returns(short_arr, n_trials_total=10, pbo=0.1)
    assert too_short["verdict"] == "DISCARD_INSUFFICIENT_TRADES" and too_short["promoted"] is False

    rng = np.random.default_rng(7)
    strong = 0.012 + 0.003 * rng.standard_normal(40)
    ok = falsify_trade_returns(strong, n_trials_total=10, pbo=0.10)
    assert ok["promoted"] is True and ok["verdict"] == "PROMOTE" and ok["cpcv_splits"] > 0

    closes = [100.0 + (0.4 * (i % 3)) for i in range(50)]
    bars = _synth_bars("AAPL", closes)
    ts_l = bars["t_close_ns"].to_list()
    sigs = {ts_l[i]: (1 if i % 2 == 0 else 0) for i in range(40)}
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    rep = evaluate_six_gate_falsification(
        bars_df=bars,
        signals_by_ts=sigs,
        n_trials_total=5,
        pbo=0.2,
        l1_quotes_df=q_df,
        l1_trades_df=t_df,
        mlflow_tracking_uri=uri,
    )
    assert set(rep["gates"]) == {
        "1_hftbacktest_l1_markout",
        "2_cost_1x_2x_survival",
        "3_cpcv_oos_positive",
        "4_dsr_pbo_credible",
        "5_hansen_spa_significant",
        "6_hac_t_stat_significant",
    }
    assert "mlflow_run_id" in rep


def test_dagster_definitions_load_cleanly() -> None:
    from dagster import Definitions

    from tier0_sota.definitions import defs

    assert isinstance(defs, Definitions)
    asset_keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    assert {
        "prewall_pit_audit",
        "risk_kernel_sha256_attestation",
        "paper_farm_performance_snapshot",
    } <= asset_keys

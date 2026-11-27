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
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.execution import MakerTakerFeeModel
from nautilus_trader.model import (
    AccountType,
    ClientId,
    Currency,
    Money,
    OmsType,
    OrderType,
    Price,
    TimeInForce,
    TrailingOffsetType,
)

from tier0_sota.discovery import (
    BAR_DURATION_NS,
    DISCOVERY_WALL_UTC_NS,
    REGIME_BENCHMARK_SYMBOLS,
    PreWallViolationError,
    assert_pre_wall_date,
    assert_pre_wall_ns,
    compute_causal_market_regime,
    df_to_equity_and_bars,
    generate_upstream_reversal_signals,
    previous_xnys_session,
    validate_causal_bars,
)
from tier0_sota.engine import (
    EXECUTION_VENUE_PROFILES,
    EXIT_TAG,
    IBKR_OUTSIDE_RTH_TAG,
    SESSION_PROTECTION_24X5,
    SIP_23X5_CUTOVER_DATE,
    VENUE_XNAS,
    SignalExecutionConfig,
    SignalExecutionStrategy,
    build_ibkr_paper_connection_spec,
    build_sandbox_live_node,
    build_venue_live_node_config,
    build_venue_risk_engine_config,
    classify_us_equity_session_et,
    compute_collared_entry_limit_price,
    compute_collared_limit_price,
    compute_dynamic_trailing_stop,
    compute_progressive_trailing_stop,
    get_session_protection_profile,
    inspect_venue_capability,
    is_sip_dark_boats_overnight_window_et,
    run_nautilus_signal_backtest,
    select_pooled_trailing_schedule,
    step_rth_5m_bar,
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
    day: tuple[int, int, int] = (2026, 10, 15),
    start_et: tuple[int, int] = (9, 30),
    step_s: int = 60,
    wick: float = 0.001,
) -> pl.DataFrame:
    t0 = _et_ns(day[0], day[1], day[2], start_et[0], start_et[1])
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


def test_rsp_regime_benchmark_and_sip_23x5_dark_boats_window() -> None:
    assert "RSP" in REGIME_BENCHMARK_SYMBOLS
    assert date(2026, 12, 6) == SIP_23X5_CUTOVER_DATE
    assert not is_sip_dark_boats_overnight_window_et(_et_ns(2026, 10, 15, 20, 30))
    assert is_sip_dark_boats_overnight_window_et(_et_ns(2026, 12, 7, 20, 30))
    assert not is_sip_dark_boats_overnight_window_et(_et_ns(2026, 12, 7, 21, 5))
    assert not is_sip_dark_boats_overnight_window_et(_et_ns(2026, 12, 12, 20, 30))


def test_outside_rth_entry_uses_collared_limit_not_market() -> None:
    pre = get_session_protection_profile("PRE_MARKET")
    aft = get_session_protection_profile("AFTER_HOURS")
    ovn = get_session_protection_profile("OVERNIGHT")
    assert pre.limit_collar_bps == Decimal("35")
    assert aft.limit_collar_bps == Decimal("35")
    assert ovn.limit_collar_bps == Decimal("50")

    assert compute_collared_entry_limit_price(
        Decimal("100.00"), "LONG", pre.limit_collar_pct
    ) == Decimal("100.35")
    assert compute_collared_entry_limit_price(
        Decimal("100.00"), "SHORT", aft.limit_collar_pct
    ) == Decimal("99.65")
    assert compute_collared_entry_limit_price(
        Decimal("100.01"), "LONG", pre.limit_collar_pct
    ) == Decimal("100.36")
    assert compute_collared_entry_limit_price(
        Decimal("100.01"), "SHORT", aft.limit_collar_pct
    ) == Decimal("99.66")
    assert compute_collared_entry_limit_price(
        Decimal("100.00"), "LONG", ovn.limit_collar_pct
    ) == Decimal("100.50")
    assert compute_collared_entry_limit_price(
        Decimal("100.00"), "SHORT", ovn.limit_collar_pct
    ) == Decimal("99.50")

    for start_et, sig_dir, expected_price in (
        ((5, 0), 1, "100.35"),
        ((17, 0), -1, "99.65"),
        ((21, 0), 1, "100.50"),
    ):
        df = _synth_bars(
            "NVDA",
            [100.0, 100.0, 100.0, 100.0],
            day=(2026, 10, 14),
            start_et=start_et,
            wick=0.0,
        )
        ts_list = df["t_close_ns"].to_list()
        signals = {ts_list[0]: sig_dir}

        res = run_nautilus_signal_backtest(df, signals, fixed_qty=10, enable_24x5_trailing=True)
        assert res.outside_rth_limit_entries == 1
        assert res.outside_rth_market_entries == 0
        assert res.rth_market_entries == 0
        assert res.fills_total >= 1
        assert res.stops_attached >= 1

        # Directly inspect real NautilusTrader v2 BacktestEngine cache orders.
        usd = Currency.from_str("USD")
        inst, btype, bars, prec = df_to_equity_and_bars(df, "NVDA", usd)
        re_cfg = build_venue_risk_engine_config("IBKR_PAPER_4002")
        ql = LoggerConfig(stdout_level=LogLevel.OFF, bypass_logging=True)
        bt_engine = BacktestEngine(
            BacktestEngineConfig(logging=ql, risk_engine=re_cfg, run_analysis=False)
        )
        try:
            bt_engine.add_venue(
                VENUE_XNAS,
                OmsType.NETTING,
                AccountType.MARGIN,
                [Money(100_000.0, usd)],
                usd,
                fee_model=MakerTakerFeeModel(Decimal("0.0004"), Decimal("0.0004")),
                use_reduce_only=False,
            )
            bt_engine.add_instrument(inst)
            bt_engine.add_data(bars, client_id=ClientId("SIM"))
            bt_engine.add_strategy(
                SignalExecutionStrategy(
                    SignalExecutionConfig(
                        btype,
                        signals,
                        Decimal("100000"),
                        prec,
                        venue_profile="IBKR_PAPER_4002",
                        fixed_qty=10,
                        enable_24x5_trailing=True,
                    )
                )
            )
            bt_engine.run()
            entry_orders = [
                o for o in bt_engine.cache.orders() if EXIT_TAG not in tuple(o.tags or ())
            ]
            assert len(entry_orders) == 1
            entry_ord = entry_orders[0]
            assert entry_ord.order_type == OrderType.LIMIT
            assert entry_ord.price == Price.from_str(expected_price)
            assert entry_ord.time_in_force == TimeInForce.GTC
            assert IBKR_OUTSIDE_RTH_TAG in tuple(entry_ord.tags or ())
        finally:
            bt_engine.dispose()

    rth_df = _synth_bars("NVDA", [100.0, 100.1, 100.2, 100.3], start_et=(10, 0), wick=0.0)
    rth_ts = rth_df["t_close_ns"].to_list()
    rth_res = run_nautilus_signal_backtest(
        rth_df, {rth_ts[0]: 1}, fixed_qty=10, enable_24x5_trailing=True
    )
    assert rth_res.rth_market_entries == 1
    assert rth_res.outside_rth_limit_entries == 0
    assert rth_res.outside_rth_market_entries == 0


def test_position_changed_reanchors_stop_and_unfilled_l1_fails_gate1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace
    from typing import Any, cast

    from tier0_sota.engine import SignalExecutionConfig, SignalExecutionStrategy

    df = _synth_bars("NVDA", [100.0, 102.0, 103.0], start_et=(10, 0), wick=0.0)
    ts_l = df["t_close_ns"].to_list()
    usd = Currency.from_str("USD")
    _, btype, _, prec = df_to_equity_and_bars(df, "NVDA", usd)
    strat = SignalExecutionStrategy(
        SignalExecutionConfig(btype, {}, Decimal("100000"), prec, fixed_qty=10)
    )
    strat._last_close_px = Decimal("104.00")
    pos_scaled = SimpleNamespace(
        instrument_id=btype.instrument_id,
        is_long=True,
        avg_px_open=Decimal("104.00"),
        quantity=15,
    )
    captured: list[tuple[object, str, int, bool]] = []
    monkeypatch.setattr(strat, "_open_pos", lambda iid: pos_scaled)
    monkeypatch.setattr(strat, "_sync_session", lambda ts_ns, iid, *, delta=Decimal("0"): None)
    monkeypatch.setattr(
        strat,
        "_replace_stop",
        lambda pos, sess, ts_ns, *, trailing: captured.append((pos, sess, ts_ns, trailing)),
    )
    fake_event = cast(Any, SimpleNamespace(instrument_id=btype.instrument_id, ts_event=ts_l[1]))
    strat.on_position_changed(fake_event)
    assert len(captured) == 1
    assert strat._init_stop_px is not None
    assert (Decimal("104.00") - strat._init_stop_px) / Decimal("104.00") <= Decimal("0.02")

    # Unfilled L1 order must NOT auto-pass Gate 1 even with default markout_bps == 0.0.
    q_df = pl.DataFrame(
        {
            "t_ns": [ts_l[0], ts_l[1]],
            "bp": [100.0, 101.0],
            "bs": [100.0, 100.0],
            "ap": [100.05, 101.05],
            "as": [100.0, 100.0],
        }
    )
    t_df = pl.DataFrame({"t_ns": [ts_l[0] + 1_000], "p": [100.05], "s": [10.0]})
    rep = evaluate_six_gate_falsification(
        bars_df=df,
        signals_by_ts={ts_l[0]: 1},
        n_trials_total=5,
        pbo=0.2,
        l1_quotes_df=q_df,
        l1_trades_df=t_df,
    )
    assert rep["l1_markout"] is not None
    assert rep["l1_markout"]["passive_filled"] is False
    assert rep["gates"]["1_hftbacktest_l1_markout"] is False


def test_thanksgiving_overnight_to_black_friday_early_close_24x5_parity() -> None:
    # Exact 2026 Thanksgiving -> 20:00 ET OVERNIGHT BOATS -> Black Friday 13:00 ET early close.
    calendar_checks = (
        ((2026, 11, 26, 19, 59), "CLOSED"),
        ((2026, 11, 26, 20, 0), "OVERNIGHT"),
        ((2026, 11, 26, 23, 59), "OVERNIGHT"),
        ((2026, 11, 27, 0, 0), "OVERNIGHT"),
        ((2026, 11, 27, 3, 59), "OVERNIGHT"),
        ((2026, 11, 27, 4, 0), "PRE_MARKET"),
        ((2026, 11, 27, 9, 29), "PRE_MARKET"),
        ((2026, 11, 27, 9, 30), "RTH"),
        ((2026, 11, 27, 12, 59), "RTH"),
        ((2026, 11, 27, 13, 0), "AFTER_HOURS"),
        ((2026, 11, 27, 15, 59), "AFTER_HOURS"),
        ((2026, 11, 27, 19, 59), "AFTER_HOURS"),
        ((2026, 11, 27, 20, 0), "CLOSED"),
    )
    for (y, m, d, hh, mm), expected_session in calendar_checks:
        assert classify_us_equity_session_et(_et_ns(y, m, d, hh, mm)) == expected_session

    # Pre-SIP-23x5 cutover (2026-12-06): 2026-11-26 20:00 ET is not in the dark-BOATS window.
    assert not is_sip_dark_boats_overnight_window_et(_et_ns(2026, 11, 26, 20, 0))

    closed_p = get_session_protection_profile("CLOSED")
    ovn_p = get_session_protection_profile("OVERNIGHT")
    pre_p = get_session_protection_profile("PRE_MARKET")
    rth_p = get_session_protection_profile("RTH")
    aft_p = get_session_protection_profile("AFTER_HOURS")
    assert closed_p.max_notional_scale == Decimal("0.00") and closed_p.outside_rth is True
    assert ovn_p.order_mode == "STOP_LIMIT" and ovn_p.outside_rth is True
    assert ovn_p.limit_collar_bps == Decimal("50")
    assert ovn_p.initial_max_loss_pct == Decimal("0.0100")
    assert ovn_p.max_notional_scale == Decimal("0.35")
    assert pre_p.order_mode == "STOP_LIMIT" and pre_p.limit_collar_bps == Decimal("35")
    assert rth_p.order_mode == "STOP_MARKET" and rth_p.outside_rth is False
    assert aft_p.order_mode == "STOP_LIMIT" and aft_p.limit_collar_bps == Decimal("35")

    # 4-tier progressive lossless trailing parity in OVERNIGHT (1.00% cap -> 1R = $1.00 on $100).
    entry = Decimal("100.00")
    long_init = compute_capped_stop_price(entry, "LONG", max_loss_pct=ovn_p.initial_max_loss_pct)
    short_init = compute_capped_stop_price(entry, "SHORT", max_loss_pct=ovn_p.initial_max_loss_pct)
    assert long_init == Decimal("99.00") and short_init == Decimal("101.00")

    l_stops: list[Decimal] = [long_init]
    l_tiers: list[int] = []
    for fav in (Decimal("100.35"), Decimal("101.05"), Decimal("102.05"), Decimal("103.05")):
        nxt, tier, _ = compute_progressive_trailing_stop(
            entry_price=entry,
            side="LONG",
            favorable_extreme=fav,
            current_stop=l_stops[-1],
            profile=ovn_p,
            initial_stop=long_init,
        )
        l_stops.append(nxt)
        l_tiers.append(tier)
    assert l_tiers == [1, 2, 3, 4]
    assert l_stops[0] < l_stops[1] < l_stops[2] < l_stops[3] < l_stops[4]
    assert l_stops[1] >= Decimal("100.10")
    assert l_stops[2] >= Decimal("100.45")
    assert l_stops[3] >= Decimal("101.35")
    assert l_stops[4] >= Decimal("102.35")

    s_stops: list[Decimal] = [short_init]
    s_tiers: list[int] = []
    for fav in (Decimal("99.65"), Decimal("98.95"), Decimal("97.95"), Decimal("96.95")):
        nxt, tier, _ = compute_progressive_trailing_stop(
            entry_price=entry,
            side="SHORT",
            favorable_extreme=fav,
            current_stop=s_stops[-1],
            profile=ovn_p,
            initial_stop=short_init,
        )
        s_stops.append(nxt)
        s_tiers.append(tier)
    assert s_tiers == [1, 2, 3, 4]
    assert s_stops[0] > s_stops[1] > s_stops[2] > s_stops[3] > s_stops[4]
    assert s_stops[1] <= Decimal("99.90")
    assert s_stops[2] <= Decimal("99.55")
    assert s_stops[3] <= Decimal("98.65")
    assert s_stops[4] <= Decimal("97.65")

    # Early-close 5m RTH state clears cleanly at Black Friday 13:00 ET (780 min).
    rth5_state: dict[str, object] = {}
    step_rth_5m_bar(
        rth5_state, date(2026, 11, 27), 775, 100.0, 100.5, 99.8, 100.2, 100.1, rth_close_hm=780
    )
    assert "orl" in rth5_state
    step_rth_5m_bar(
        rth5_state, date(2026, 11, 27), 780, 100.2, 100.3, 100.0, 100.1, 100.1, rth_close_hm=780
    )
    assert rth5_state == {}

    # Pre-wall Thanksgiving (2025-11-27) -> Black Friday early close (2025-11-28) BacktestEngine
    # parity: CLOSED entry blocked, OVERNIGHT 50bps collared LIMIT + 0.35x scale + 1.00% STOP_LIMIT
    # + 4-tier ratchet + monotonic OVERNIGHT->PRE_MARKET->RTH->AFTER_HOURS(13:00 ET) + CLOSED hold.
    bar_points = (
        ((2025, 11, 27, 19, 58), 100.0),  # t_close 19:59 ET -> CLOSED (entry must be refused)
        ((2025, 11, 27, 19, 59), 100.0),  # t_close 20:00 ET -> OVERNIGHT (entry allowed)
        ((2025, 11, 27, 20, 0), 100.0),  # t_close 20:01 ET -> OVERNIGHT fill bar
        ((2025, 11, 27, 20, 1), 100.4),  # Tier 1 BE+fee
        ((2025, 11, 27, 20, 2), 101.2),  # Tier 2 +1R
        ((2025, 11, 27, 20, 3), 102.2),  # Tier 3 +2R
        ((2025, 11, 27, 20, 4), 103.5),  # Tier 4 +3R
        ((2025, 11, 28, 4, 4), 103.6),  # PRE_MARKET transition #1 (STOP_LIMIT 35bps)
        ((2025, 11, 28, 9, 34), 103.7),  # RTH transition #2 (STOP_MARKET)
        ((2025, 11, 28, 12, 54), 103.8),  # still Black Friday RTH (no transition)
        ((2025, 11, 28, 12, 59), 103.9),  # t_close 13:00 ET -> AFTER_HOURS transition #3
        ((2025, 11, 28, 19, 59), 104.0),  # t_close 20:00 ET -> CLOSED (must keep GTC stop)
    )
    rows = []
    prev_c = 100.0
    for (y, m, d, hh, mm), c_px in bar_points:
        t_ns = _et_ns(y, m, d, hh, mm)
        rows.append(
            {
                "symbol": "NVDA",
                "t_ns": t_ns,
                "t_close_ns": t_ns + BAR_DURATION_NS,
                "o": prev_c,
                "h": max(prev_c, c_px),
                "l": min(prev_c, c_px),
                "c": c_px,
                "v": 25_000.0,
            }
        )
        prev_c = c_px
    bt_df = pl.DataFrame(rows)
    ts_close = bt_df["t_close_ns"].to_list()
    # Signal at 19:59 ET CLOSED (must refuse), then signal at 20:00 ET OVERNIGHT (must enter).
    sigs = {ts_close[0]: 1, ts_close[1]: 1}

    usd = Currency.from_str("USD")
    inst, btype, bars, prec = df_to_equity_and_bars(bt_df, "NVDA", usd)
    re_cfg = build_venue_risk_engine_config("IBKR_PAPER_4002")
    ql = LoggerConfig(stdout_level=LogLevel.OFF, bypass_logging=True)
    bt_engine = BacktestEngine(
        BacktestEngineConfig(logging=ql, risk_engine=re_cfg, run_analysis=False)
    )
    try:
        bt_engine.add_venue(
            VENUE_XNAS,
            OmsType.NETTING,
            AccountType.MARGIN,
            [Money(100_000.0, usd)],
            usd,
            fee_model=MakerTakerFeeModel(Decimal("0.0004"), Decimal("0.0004")),
            use_reduce_only=False,
        )
        bt_engine.add_instrument(inst)
        bt_engine.add_data(bars, client_id=ClientId("SIM"))
        strat = SignalExecutionStrategy(
            SignalExecutionConfig(
                btype,
                sigs,
                Decimal("100000"),
                prec,
                venue_profile="IBKR_PAPER_4002",
                enable_24x5_trailing=True,
            )
        )
        bt_engine.add_strategy(strat)
        bt_engine.run()

        orders = bt_engine.cache.orders()
        entries = [o for o in orders if EXIT_TAG not in tuple(o.tags or ())]
        exits = [o for o in orders if EXIT_TAG in tuple(o.tags or ())]
        assert len(entries) == 1
        assert entries[0].order_type == OrderType.LIMIT
        assert entries[0].price == Price.from_str("100.50")
        assert int(entries[0].quantity.as_double()) == 70  # 0.35x of 200-share RTH notional cap
        assert "SESSION=OVERNIGHT" in tuple(entries[0].tags or ())
        assert IBKR_OUTSIDE_RTH_TAG in tuple(entries[0].tags or ())

        assert strat.max_lock_tier == 4
        # OVERNIGHT -> PRE_MARKET -> RTH -> AFTER_HOURS (must not transition into CLOSED).
        assert strat.session_transitions == 3
        assert [
            next(t for t in tuple(o.tags or ()) if t.startswith("SESSION=")) for o in exits
        ] == ["SESSION=OVERNIGHT", "SESSION=PRE_MARKET", "SESSION=RTH", "SESSION=AFTER_HOURS"]
        assert [o.order_type for o in exits] == [
            OrderType.STOP_LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
        ]
        # Monotonic protective stop triggers across all 4 sessions & resting GTC stop on CLOSED.
        trigs = [Decimal(str(o.trigger_price)) for o in exits]
        assert trigs == sorted(trigs) and trigs[-1] >= Decimal("102.35")
        assert IBKR_OUTSIDE_RTH_TAG in tuple(exits[-1].tags or ())
        assert not exits[-1].is_closed
    finally:
        bt_engine.dispose()

    # Venue overnight SHORT parity: ALPACA_PAPER_200 blocks OVERNIGHT SHORT; IBKR allows it.
    ovn_short_df = bt_df.slice(1, 3)
    ovn_ts = ovn_short_df["t_close_ns"].to_list()
    alp_res = run_nautilus_signal_backtest(
        ovn_short_df, {ovn_ts[0]: -1}, venue_profile="ALPACA_PAPER_200", fixed_qty=10
    )
    ib_res = run_nautilus_signal_backtest(
        ovn_short_df, {ovn_ts[0]: -1}, venue_profile="IBKR_PAPER_4002", fixed_qty=10
    )
    assert alp_res.orders_total == 0 and alp_res.fills_total == 0
    assert ib_res.outside_rth_limit_entries == 1 and ib_res.fills_total >= 1

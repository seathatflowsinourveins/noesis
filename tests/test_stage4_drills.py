"""Unit & integration tests for ADR-0020 Stage-4 K3/K4 & 3-Way Reconciliation Drills."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import duckdb
import nautilus_trader.model as nm
import pytest
from scripts import run_stage4_drills as s4

from tier0_sota import engine as eng
from tier0_sota.live import paper_farm as pf
from tier0_sota.live.paper_farm import PAPER_ALPACA_ENDPOINT


def test_k3_trading_state_drill_transitions_and_breaker_latch(tmp_path: Path) -> None:
    # 1. Pure K3 state machine transitions with real upstream Nautilus TradingState.
    st, rsn = s4.evaluate_k3_trading_state(
        drawdown_breaker_tripped=False,
        risk_kernel_sha256_ok=True,
        reconciliation_ok=True,
    )
    assert st == nm.TradingState.ACTIVE and rsn == "HEALTHY_PAPER_STATE"

    st_dd, rsn_dd = s4.evaluate_k3_trading_state(
        drawdown_breaker_tripped=True,
        risk_kernel_sha256_ok=True,
        reconciliation_ok=True,
    )
    assert st_dd == nm.TradingState.REDUCING and "DRAWDOWN" in rsn_dd

    # Risk kernel tamper overrides even healthy drawdown -> HALTED.
    st_sha, rsn_sha = s4.evaluate_k3_trading_state(
        drawdown_breaker_tripped=False,
        risk_kernel_sha256_ok=False,
        reconciliation_ok=True,
    )
    assert st_sha == nm.TradingState.HALTED and "SHA256" in rsn_sha

    # Reconciliation failure -> HALTED.
    st_rec, rsn_rec = s4.evaluate_k3_trading_state(
        drawdown_breaker_tripped=False,
        risk_kernel_sha256_ok=True,
        reconciliation_ok=False,
    )
    assert st_rec == nm.TradingState.HALTED and "RECONCILIATION" in rsn_rec

    # 2. Order gate matrix: REDUCING denies entries & permits reduce-only/cancels;
    #    HALTED denies ALL submissions & permits cancels.
    assert s4.evaluate_k3_order_gate(nm.TradingState.ACTIVE, is_reduce_only=False)["allowed"]
    red_ent = s4.evaluate_k3_order_gate(nm.TradingState.REDUCING, is_reduce_only=False)
    assert red_ent["allowed"] is False and red_ent["event"] == "OrderDenied"
    assert s4.evaluate_k3_order_gate(nm.TradingState.REDUCING, is_reduce_only=True)["allowed"]
    assert s4.evaluate_k3_order_gate(nm.TradingState.REDUCING, is_cancel=True)["allowed"]

    halt_ent = s4.evaluate_k3_order_gate(nm.TradingState.HALTED, is_reduce_only=False)
    halt_ext = s4.evaluate_k3_order_gate(nm.TradingState.HALTED, is_reduce_only=True)
    assert halt_ent["allowed"] is False and halt_ent["event"] == "OrderDenied"
    assert halt_ext["allowed"] is False and halt_ext["event"] == "OrderDenied"
    assert s4.evaluate_k3_order_gate(nm.TradingState.HALTED, is_cancel=True)["allowed"]

    # 3. Real NautilusTrader v2 RiskEngine (BacktestEngine + LiveNode SANDBOX) drill passes.
    res = s4.run_k3_engine_kill_switch_drill()
    assert res["passed"] is True
    assert res["checks"]["nautilus_rust_risk_engine_notional_denied"] is True
    assert res["checks"]["nautilus_rust_risk_engine_rate_limit_denied"] is True
    assert res["checks"]["sandbox_live_node_verified"] is True
    assert res["checks"]["breaker_latched_at_minus_1_50pct"] is True
    assert res["nautilus_backtest_proof"]["final_trading_state"] == "HALTED"

    # 4. Paper Farm K3 TradingState drill passes.
    pf_k3 = pf.run_k3_trading_state_drill()
    assert pf_k3["passed"] is True
    assert pf_k3["paper_only"] is True
    assert pf_k3["live_execution_enabled"] is False

    # 5. Tampered risk.py fails closed to HALTED and fails the drill!
    fake_risk = tmp_path / "tampered_risk.py"
    fake_risk.write_text("# tampered\n", encoding="utf-8")
    tampered_res = s4.run_k3_engine_kill_switch_drill(risk_path=fake_risk)
    assert tampered_res["passed"] is False
    assert tampered_res["checks"]["risk_kernel_sha256_ok"] is False
    assert tampered_res["state_transitions"]["healthy"]["state"] == "HALTED"


def test_k4_broker_flatten_drill_all_lanes_and_duckdb_audit(tmp_path: Path) -> None:
    client = s4.SandboxK4BrokerClient()
    receipt = s4.execute_k4_panic_flatten(client)
    assert receipt["flattened_ok"] is True
    assert receipt["pre_orders_count"] == 2
    assert receipt["pre_positions_count"] == 3
    assert receipt["post_orders_count"] == 0
    assert receipt["post_positions_count"] == 0
    assert receipt["call_order"] == ["cancel_orders", "close_all_positions(cancel_orders=True)"]

    # Full sandbox drill includes all live refusal fault-injection checks.
    drill = s4.run_k4_flatten_drill()
    assert drill["passed"] is True
    assert drill["sandbox_only"] is True
    assert drill["live_broker_touched"] is False
    assert drill["live_refusals_passed_count"] == drill["live_refusals_total_count"]
    assert drill["live_refusals_total_count"] >= 8

    # Paper Farm K4 broker flatten drill across ALPACA_PA1..PA3 + IBKR_DU_4002 & DuckDB audit.
    pf_k4 = pf.run_k4_broker_flatten_drill(work_dir=tmp_path / "pf_k4")
    assert pf_k4["passed"] is True
    assert pf_k4["paper_only"] is True
    assert pf_k4["live_execution_enabled"] is False

    # Direct refusal checks on live endpoint, AK* key, live flag, non-paper client, live ports.
    live_host = ".".join(("api", "alpaca", "markets"))
    with pytest.raises(PermissionError):
        s4.assert_k4_paper_safety(endpoint=f"https://{live_host}")
    with pytest.raises(PermissionError):
        s4.assert_k4_paper_safety(api_key="AK_LIVE_KEY_REFUSED")
    with pytest.raises(PermissionError):
        s4.assert_k4_paper_safety(live_execution_enabled=True)
    with pytest.raises(PermissionError):
        s4.assert_k4_paper_safety(paper_only=False)
    for bad_port in eng.FORBIDDEN_LIVE_IBKR_PORTS:
        with pytest.raises(PermissionError):
            s4.assert_k4_paper_safety(port=bad_port)

    bad_client = s4.SandboxK4BrokerClient()
    bad_client._sandbox = False
    with pytest.raises(PermissionError):
        s4.execute_k4_panic_flatten(bad_client)
    assert bad_client.call_log == []
    assert len(bad_client.get_all_positions()) == 3


def test_three_way_reconciliation_drill_parity_adoption_and_halted(tmp_path: Path) -> None:
    # 1. Exact match (non-empty and flat) -> RECONCILED_OK / ACTIVE.
    pos = [{"symbol": "NVDA", "side": "LONG", "qty": "10"}]
    ok = s4.reconcile_three_way_positions(pos, pos, pos)
    assert ok["status"] == "RECONCILED_OK"
    assert ok["reconciled"] is True
    assert ok["target_trading_state"] == "ACTIVE"
    assert ok["matched_symbols"] == ["NVDA"]
    assert ok["diffs"] == []

    flat = s4.reconcile_three_way_positions({}, {}, {})
    assert flat["status"] == "RECONCILED_OK" and flat["target_trading_state"] == "ACTIVE"

    # 2. Missing broker report NEVER implies flat -> HALTED_MISSING_BROKER_REPORT.
    miss = s4.reconcile_three_way_positions(None, {}, {})
    assert miss["status"] == "HALTED_MISSING_BROKER_REPORT"
    assert miss["reconciled"] is False
    assert miss["target_trading_state"] == "HALTED"

    # 3. Quantity, side, and ghost-position discrepancies -> HALTED_RECONCILIATION_MISMATCH.
    qty_bad = s4.reconcile_three_way_positions(
        [{"symbol": "NVDA", "side": "LONG", "qty": "10"}],
        [{"symbol": "NVDA", "side": "LONG", "qty": "9"}],
        [{"symbol": "NVDA", "side": "LONG", "qty": "10"}],
    )
    assert qty_bad["status"] == "HALTED_RECONCILIATION_MISMATCH"
    assert qty_bad["target_trading_state"] == "HALTED"
    assert qty_bad["diffs"][0]["mismatch_kind"] == "QTY_MISMATCH"

    side_bad = s4.reconcile_three_way_positions(
        [{"symbol": "AMD", "side": "LONG", "qty": "5"}],
        [{"symbol": "AMD", "side": "SHORT", "qty": "5"}],
        [{"symbol": "AMD", "side": "LONG", "qty": "5"}],
    )
    assert side_bad["status"] == "HALTED_RECONCILIATION_MISMATCH"
    assert side_bad["diffs"][0]["mismatch_kind"] == "SIDE_MISMATCH"

    ghost = s4.reconcile_three_way_positions(
        [{"symbol": "SOXS", "side": "LONG", "qty": "20"}],
        [],
        [],
    )
    assert ghost["status"] == "HALTED_RECONCILIATION_MISMATCH"
    assert ghost["diffs"][0]["mismatch_kind"] == "PRESENCE_MISMATCH"

    # 4. Paper Farm 3-way restart reconciliation drill (parity, external adoption & HALTED).
    pf_rec = pf.run_three_way_reconciliation_drill(work_dir=tmp_path / "pf_rec")
    assert pf_rec["passed"] is True
    assert pf_rec["scenarios"]["clean_parity"]["passed"] is True
    assert pf_rec["scenarios"]["external_half_auto_adoption"]["passed"] is True
    assert pf_rec["scenarios"]["fail_closed_discrepancy"]["passed"] is True

    # 5. Read-only /tmp-copy DuckDB loader reads open entry without modifying source DB.
    db_path = tmp_path / "paper_lane_pa3.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE paper_entry_decisions ("
            "client_order_id VARCHAR, symbol VARCHAR, side VARCHAR, session VARCHAR, "
            "closed_at VARCHAR, exit_triggered BOOLEAN, active_stop DECIMAL(18,6))"
        )
        conn.execute(
            "INSERT INTO paper_entry_decisions VALUES "
            "('cid-1', 'TQQQ', 'LONG', 'OVERNIGHT', NULL, FALSE, 48.20)"
        )
        conn.execute(
            "CREATE TABLE paper_tca_fills "
            "(ts_utc VARCHAR, symbol VARCHAR, filled_qty DECIMAL(18,6))"
        )
        conn.execute("INSERT INTO paper_tca_fills VALUES ('2026-11-27T04:58:00Z', 'TQQQ', 3.0)")
        conn.execute("CREATE TABLE paper_daily_fuse (day DATE, tripped BOOLEAN)")
        conn.execute("INSERT INTO paper_daily_fuse VALUES ('2026-11-27', FALSE)")

    before_bytes = db_path.read_bytes()
    snap = s4.load_duckdb_open_positions_readonly(tmp_path)
    assert snap["copied_readonly_via_tmp"] is True
    assert snap["normalized_ledger"] == {"TQQQ": {"side": "LONG", "qty": "3"}}
    assert db_path.read_bytes() == before_bytes  # zero mutation to source DuckDB!


def test_run_all_stage4_drills_emits_verified_receipt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Seed matching execution status + DuckDB in tmp manifest_dir.
    (tmp_path / "paper_farm_execution_status.json").write_text(
        json.dumps(
            {
                "status": "SUPERVISING",
                "generated_at_utc": "2026-11-27T10:00:00+00:00",
                "active_positions": [],
            }
        ),
        encoding="utf-8",
    )
    out_manifest = tmp_path / "stage4_drills_manifest.json"
    out_receipt = tmp_path / "stage4_drills_receipt.json"
    rc = s4.main(
        [
            "--manifest-dir",
            str(tmp_path),
            "--output-path",
            str(out_manifest),
            "--receipt-path",
            str(out_receipt),
        ]
    )
    assert rc == 0
    assert out_manifest.is_file()
    assert out_receipt.is_file()
    cli_summary = json.loads(capsys.readouterr().out)
    assert cli_summary["all_drills_passed"] is True
    assert cli_summary["paper_only"] is True
    assert cli_summary["live_execution_enabled"] is False
    assert cli_summary["paper_farm_receipt_passed"] is True

    doc = json.loads(out_manifest.read_text(encoding="utf-8"))
    assert doc["schema_version"] == s4.SCHEMA_VERSION
    assert doc["adr_anchor"] == "ADR-0020"
    assert doc["all_drills_passed"] is True
    assert doc["paper_only"] is True
    assert doc["live_execution_enabled"] is False
    assert doc["safety_invariants"]["risk_kernel_sha256"] == s4.EXPECTED_RISK_PY_SHA256
    assert doc["safety_invariants"]["risk_kernel_sha256_ok"] is True
    assert doc["safety_invariants"]["forbidden_live_ports_open"] == []
    assert doc["safety_invariants"]["cdp_9223_disabled"] is True
    assert doc["drills"]["k3_engine_kill_switch"]["passed"] is True
    assert doc["drills"]["k4_broker_flatten_sandbox"]["passed"] is True
    assert doc["drills"]["three_way_restart_reconciliation"]["passed"] is True

    pf_rec = json.loads(out_receipt.read_text(encoding="utf-8"))
    assert pf_rec["all_passed"] is True
    assert pf_rec["paper_only"] is True
    assert pf_rec["live_execution_enabled"] is False
    assert pf_rec["stage5_live_authorized"] is False

    # Verify /proc/net/tcp port parser catches forbidden live ports (0x0FA1=4001, 0x1D48=7496).
    proc_sample = (
        "  sl  local_address rem_address   st tx_queue rx_queue\n"
        "   0: 0100007F:0FA1 00000000:0000 0A 00000000:00000000\n"
        "   1: 0100007F:C350 0A000002:1D48 01 00000000:00000000\n"
    )
    parsed_ports = s4.parse_proc_net_tcp_ports(proc_sample)
    assert 4001 in parsed_ports and 7496 in parsed_ports
    assert PAPER_ALPACA_ENDPOINT == "https://paper-api.alpaca.markets"
    assert Decimal("0") <= Decimal("0.0200")

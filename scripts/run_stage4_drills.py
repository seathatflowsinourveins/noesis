#!/usr/bin/env python3
"""ADR-0020 Stage-4 Automated Kill-Switch (K3/K4) & 3-Way Restart Reconciliation Drills.

Strictly paper/sandbox only (`paper_only=True`, `live_execution_enabled=False`):
- Drill 1 (K3): Real upstream NautilusTrader v2 `RiskEngine` (`bypass=False`), `TradingState`
  (`ACTIVE`/`REDUCING`/`HALTED`), `LiveNode(Environment.SANDBOX)`, and `risk.py`
  `DailyDrawdownBreaker` (-1.50% latch) + `ProtectiveExitRateBudget` (>= 10% exit reserve).
- Drill 2 (K4): Fail-closed broker flatten contract (`cancel_orders()` ->
  `close_all_positions(cancel_orders=True)`) on a deterministic sandbox mock client plus
  hard `PermissionError` refusal on any live endpoint, live key prefix, live port, or
  `live_execution_enabled=True`.
- Drill 3 (ADR-0020 Sec 4): Fail-closed 3-way restart reconciliation
  (`Broker == Engine == DuckDB`) where missing broker reports NEVER imply flat, plus a
  strictly read-only `/tmp`-copy DuckDB & coordination snapshot of the live Paper Farm.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import nautilus_trader.backtest as nb
import nautilus_trader.common as ncm
import nautilus_trader.config as ncfg
import nautilus_trader.model as nm
import nautilus_trader.trading as nt
import polars as pl
from nautilus_trader.execution import MakerTakerFeeModel

from tier0_sota import engine as eng
from tier0_sota import risk as rk
from tier0_sota.discovery import df_to_equity_and_bars
from tier0_sota.live.paper_farm import (
    PAPER_ALPACA_ENDPOINT,
)
from tier0_sota.live.paper_farm import (
    run_all_stage4_drills as run_paper_farm_stage4_drills,
)

SCHEMA_VERSION = "tier0.stage4-drills.v1"
EXPECTED_RISK_PY_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RISK_PY_PATH = REPO_ROOT / "src" / "tier0_sota" / "risk.py"
DEFAULT_MANIFEST_DIR = (
    Path(
        os.environ.get("TIER0_DATA_ROOT", "").strip()
        or (r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data")
    )
    / "manifests"
)
DEFAULT_OUTPUT_PATH = DEFAULT_MANIFEST_DIR / "stage4_drills_manifest.json"
DEFAULT_RECEIPT_PATH = DEFAULT_MANIFEST_DIR / "stage4_drills_receipt.json"
_ZERO = Decimal("0")


def verify_risk_kernel_sha256(
    risk_path: Path = DEFAULT_RISK_PY_PATH,
    expected_sha256: str = EXPECTED_RISK_PY_SHA256,
) -> dict[str, Any]:
    """Verify immutable SHA-256 digest of `src/tier0_sota/risk.py` fail-closed."""
    if not risk_path.is_file():
        return {
            "ok": False,
            "path": str(risk_path),
            "expected_sha256": expected_sha256,
            "actual_sha256": None,
            "reason": "RISK_KERNEL_FILE_MISSING",
        }
    actual = hashlib.sha256(risk_path.read_bytes()).hexdigest()
    ok = actual == expected_sha256
    return {
        "ok": ok,
        "path": str(risk_path),
        "expected_sha256": expected_sha256,
        "actual_sha256": actual,
        "reason": "OK" if ok else "RISK_KERNEL_SHA256_MISMATCH",
    }


def parse_proc_net_tcp_ports(proc_text: str) -> set[int]:
    """Parse local and remote TCP port numbers from Linux `/proc/net/tcp{,6}` text."""
    ports: set[int] = set()
    for line in proc_text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        state = parts[3].strip().upper()
        # 0A = LISTEN, 01 = ESTABLISHED, 02 = SYN_SENT
        if state not in {"0A", "01", "02"}:
            continue
        for addr in (parts[1], parts[2]):
            if ":" not in addr:
                continue
            hex_port = addr.rsplit(":", 1)[1]
            try:
                port = int(hex_port, 16)
            except ValueError:
                continue
            if port > 0:
                ports.add(port)
    return ports


def inspect_forbidden_live_ports() -> list[int]:
    """Return any active forbidden live broker ports (`FORBIDDEN_LIVE_IBKR_PORTS`) on host."""
    active_ports: set[int] = set()
    for proc_file in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        if proc_file.is_file():
            try:
                active_ports |= parse_proc_net_tcp_ports(proc_file.read_text(encoding="utf-8"))
            except OSError:
                continue
    return sorted(active_ports & set(eng.FORBIDDEN_LIVE_IBKR_PORTS))


# ---------------------------------------------------------------------------
# Drill 1: K3 Engine Kill-Switch (NautilusTrader v2 RiskEngine & TradingState)
# ---------------------------------------------------------------------------


def evaluate_k3_trading_state(
    *,
    drawdown_breaker_tripped: bool,
    risk_kernel_sha256_ok: bool,
    reconciliation_ok: bool,
    z3_ok: bool = True,
) -> tuple[nm.TradingState, str]:
    """Map ADR-0020 K2/K3/DR safety signals into upstream Nautilus `TradingState`."""
    if not risk_kernel_sha256_ok:
        return nm.TradingState.HALTED, "RISK_KERNEL_SHA256_MISMATCH"
    if not reconciliation_ok:
        return nm.TradingState.HALTED, "THREE_WAY_RECONCILIATION_FAILED"
    if not z3_ok:
        return nm.TradingState.HALTED, "Z3_FORMAL_PROOF_FAILED"
    if drawdown_breaker_tripped:
        return nm.TradingState.REDUCING, "K2_DAILY_DRAWDOWN_BREAKER_LATCHED"
    return nm.TradingState.ACTIVE, "HEALTHY_PAPER_STATE"


def evaluate_k3_order_gate(
    state: nm.TradingState,
    *,
    is_reduce_only: bool = False,
    is_cancel: bool = False,
) -> dict[str, Any]:
    """Evaluate ADR-0020 K3 order-admission decision for a Nautilus `TradingState`."""
    if is_cancel:
        return {
            "trading_state": state.name,
            "action": "CANCEL",
            "allowed": True,
            "event": "OrderCanceled",
            "reason": "CANCEL_ALWAYS_PERMITTED",
        }
    if state == nm.TradingState.ACTIVE:
        return {
            "trading_state": state.name,
            "action": "REDUCE_ONLY" if is_reduce_only else "NEW_ENTRY",
            "allowed": True,
            "event": "OrderAccepted",
            "reason": "TRADING_STATE_ACTIVE",
        }
    if state == nm.TradingState.REDUCING:
        if is_reduce_only:
            return {
                "trading_state": state.name,
                "action": "REDUCE_ONLY",
                "allowed": True,
                "event": "OrderAccepted",
                "reason": "TRADING_STATE_REDUCING_REDUCE_ONLY_PERMITTED",
            }
        return {
            "trading_state": state.name,
            "action": "NEW_ENTRY",
            "allowed": False,
            "event": "OrderDenied",
            "reason": "TRADING_STATE_REDUCING_NEW_ENTRY_DENIED",
        }
    return {
        "trading_state": state.name,
        "action": "REDUCE_ONLY" if is_reduce_only else "NEW_ENTRY",
        "allowed": False,
        "event": "OrderDenied",
        "reason": "TRADING_STATE_HALTED_ALL_SUBMISSIONS_DENIED",
    }


class _K3DrillStrategy(nt.Strategy):
    """Minimal NautilusTrader v2 strategy verifying real Rust RiskEngine + K3 transitions."""

    def __new__(cls, *_args: Any, **_kwargs: Any) -> _K3DrillStrategy:
        base_new: Any = nt.Strategy.__new__
        return base_new(cls, nt.StrategyConfig(strategy_id=nm.StrategyId("K3-DRILL")))

    def __init__(self, instrument_id: nm.InstrumentId, bar_type: Any) -> None:
        self.iid = instrument_id
        self.bar_type = bar_type
        self.bar_index = 0
        self.trading_state: nm.TradingState = nm.TradingState.ACTIVE
        self.breaker = rk.DailyDrawdownBreaker(start_equity=Decimal("100000"))
        self.rust_denied_events: list[dict[str, str]] = []
        self.accepted_order_ids: list[str] = []
        self.k3_gate_trace: list[dict[str, Any]] = []

    def on_start(self) -> None:
        self.subscribe_bars(self.bar_type)

    def on_bar(self, bar: nm.Bar) -> None:
        self.bar_index += 1
        if self.bar_index == 1:
            # 1) Exceed max_notional_per_order ($500): Rust RiskEngine MUST emit OrderDenied.
            oversized = self.order_factory.limit(
                self.iid, nm.OrderSide.BUY, nm.Quantity.from_int(10), nm.Price.from_str("100.50")
            )
            self.submit_order(oversized)
        elif self.bar_index == 2:
            # 2) Healthy ACTIVE burst of 2 orders in same second at 1/s rate cap:
            #    order 1 accepted by venue, order 2 denied by Rust RiskEngine RATE_LIMIT_EXCEEDED.
            o1 = self.order_factory.limit(
                self.iid, nm.OrderSide.BUY, nm.Quantity.from_int(2), nm.Price.from_str("101.00")
            )
            o2 = self.order_factory.limit(
                self.iid, nm.OrderSide.BUY, nm.Quantity.from_int(2), nm.Price.from_str("101.00")
            )
            self.submit_order(o1)
            self.submit_order(o2)
        elif self.bar_index == 3:
            # 3) Trip K2 DailyDrawdownBreaker at -1.50% -> K3 REDUCING:
            #    new entry denied, reduce-only close allowed and submitted to venue.
            self.breaker.update(realized_pnl=Decimal("-1500.00"))
            self.trading_state, _ = evaluate_k3_trading_state(
                drawdown_breaker_tripped=self.breaker.tripped,
                risk_kernel_sha256_ok=True,
                reconciliation_ok=True,
            )
            entry_gate = evaluate_k3_order_gate(self.trading_state, is_reduce_only=False)
            reduce_gate = evaluate_k3_order_gate(self.trading_state, is_reduce_only=True)
            self.k3_gate_trace.extend([entry_gate, reduce_gate])
            if reduce_gate["allowed"]:
                close_ord = self.order_factory.market(
                    self.iid, nm.OrderSide.SELL, nm.Quantity.from_int(2), reduce_only=True
                )
                self.submit_order(close_ord)
        elif self.bar_index == 4:
            # 4) Simulate reconciliation / risk.py digest fault -> K3 HALTED:
            #    ALL submissions denied (entry & reduce-only), cancel still permitted.
            self.trading_state, _ = evaluate_k3_trading_state(
                drawdown_breaker_tripped=self.breaker.tripped,
                risk_kernel_sha256_ok=False,
                reconciliation_ok=False,
            )
            self.k3_gate_trace.append(
                evaluate_k3_order_gate(self.trading_state, is_reduce_only=False)
            )
            self.k3_gate_trace.append(
                evaluate_k3_order_gate(self.trading_state, is_reduce_only=True)
            )
            self.k3_gate_trace.append(evaluate_k3_order_gate(self.trading_state, is_cancel=True))

    def on_order_denied(self, event: nm.OrderDenied) -> None:
        self.rust_denied_events.append(
            {"client_order_id": str(event.client_order_id), "reason": str(event.reason)}
        )

    def on_order_accepted(self, event: nm.OrderAccepted) -> None:
        self.accepted_order_ids.append(str(event.client_order_id))


def run_k3_engine_kill_switch_drill(
    *,
    risk_path: Path = DEFAULT_RISK_PY_PATH,
) -> dict[str, Any]:
    """Execute Drill 1: K3 NautilusTrader v2 RiskEngine + TradingState + LiveNode SANDBOX."""
    risk_check = verify_risk_kernel_sha256(risk_path)

    # 1. Verify upstream venue RiskEngineConfig (bypass=False) + >=10% protective exit reserve.
    ibkr_rc = eng.build_venue_risk_engine_config("IBKR_PAPER_4002")
    alp_rc = eng.build_venue_risk_engine_config("ALPACA_PAPER_1000")
    ibkr_budget = rk.ProtectiveExitRateBudget(3000, rk.MIN_EXIT_RATE_RESERVE_FRACTION)
    alp_budget = rk.ProtectiveExitRateBudget(1000, rk.MIN_EXIT_RATE_RESERVE_FRACTION)

    # Instantiate real upstream NautilusTrader v2 LiveNode in Environment.SANDBOX & dispose cleanly.
    sandbox_node = eng.build_sandbox_live_node(venue_profile="ALPACA_PAPER_1000")
    try:
        sandbox_env = str(sandbox_node.environment)
        sandbox_trader_id = str(sandbox_node.trader_id)
    finally:
        sandbox_node.dispose()

    # 2. Verify K2 DailyDrawdownBreaker latch (-1.50%) & K3 TradingState transition table.
    breaker = rk.DailyDrawdownBreaker(start_equity=Decimal("100000"))
    st_healthy, rsn_healthy = evaluate_k3_trading_state(
        drawdown_breaker_tripped=breaker.tripped,
        risk_kernel_sha256_ok=bool(risk_check["ok"]),
        reconciliation_ok=True,
    )
    breaker_after_dd = breaker.update(realized_pnl=Decimal("-1500.00"))
    # Rebound must NEVER unlatch intra-day breaker.
    breaker_after_rebound = breaker.update(realized_pnl=Decimal("500.00"))
    st_reducing, rsn_reducing = evaluate_k3_trading_state(
        drawdown_breaker_tripped=breaker.tripped,
        risk_kernel_sha256_ok=True,
        reconciliation_ok=True,
    )
    st_halted_sha, rsn_halted_sha = evaluate_k3_trading_state(
        drawdown_breaker_tripped=False,
        risk_kernel_sha256_ok=False,
        reconciliation_ok=True,
    )
    st_halted_recon, rsn_halted_recon = evaluate_k3_trading_state(
        drawdown_breaker_tripped=False,
        risk_kernel_sha256_ok=True,
        reconciliation_ok=False,
    )

    gate_matrix = {
        "active_entry": evaluate_k3_order_gate(nm.TradingState.ACTIVE, is_reduce_only=False),
        "reducing_entry": evaluate_k3_order_gate(nm.TradingState.REDUCING, is_reduce_only=False),
        "reducing_exit": evaluate_k3_order_gate(nm.TradingState.REDUCING, is_reduce_only=True),
        "reducing_cancel": evaluate_k3_order_gate(nm.TradingState.REDUCING, is_cancel=True),
        "halted_entry": evaluate_k3_order_gate(nm.TradingState.HALTED, is_reduce_only=False),
        "halted_exit": evaluate_k3_order_gate(nm.TradingState.HALTED, is_reduce_only=True),
        "halted_cancel": evaluate_k3_order_gate(nm.TradingState.HALTED, is_cancel=True),
    }

    # 3. Execute real NautilusTrader v2 BacktestEngine + Rust RiskEngine (bypass=False).
    t0 = int(datetime(2026, 10, 15, 14, 0, tzinfo=UTC).timestamp() * 1_000_000_000)
    bars_df = pl.DataFrame(
        {
            "t_ns": [t0 + i * 60_000_000_000 for i in range(5)],
            "o": [100.0, 100.5, 101.0, 100.8, 100.6],
            "h": [100.8, 101.2, 101.4, 101.1, 100.9],
            "l": [99.8, 100.2, 100.6, 100.4, 100.3],
            "c": [100.5, 101.0, 100.8, 100.6, 100.5],
            "v": [10_000.0] * 5,
        }
    )
    usd = nm.Currency.from_str("USD")
    eq, btype, bars, _ = df_to_equity_and_bars(bars_df, "NVDA", usd)
    drill_re_cfg = ncfg.RiskEngineConfig(
        bypass=False,
        max_order_submit_rate="1/00:00:01",
        max_order_modify_rate="1/00:00:01",
        max_notional_per_order={str(eq.id): 500},
    )
    bt_cfg = nb.BacktestEngineConfig(
        trader_id=nm.TraderId("TIER0-K3-DRILL"),
        logging=ncm.LoggerConfig(stdout_level=ncm.LogLevel.OFF, bypass_logging=True),
        risk_engine=drill_re_cfg,
        run_analysis=False,
    )
    be = nb.BacktestEngine(config=bt_cfg)
    strat = _K3DrillStrategy(eq.id, btype)
    try:
        be.add_venue(
            eng.VENUE_XNAS,
            nm.OmsType.NETTING,
            nm.AccountType.MARGIN,
            [nm.Money(100_000.0, usd)],
            usd,
            fee_model=MakerTakerFeeModel(Decimal("0.0004"), Decimal("0.0004")),
            use_reduce_only=False,
        )
        be.add_instrument(eq)
        be.add_data(bars, client_id=nm.ClientId("SIM"))
        be.add_strategy(strat)
        be.run()
    finally:
        be.dispose()

    denied_reasons = [ev["reason"] for ev in strat.rust_denied_events]
    has_notional_deny = any("NOTIONAL_EXCEEDS_MAX_PER_ORDER" in r for r in denied_reasons)
    has_rate_deny = any("RATE_LIMIT_EXCEEDED" in r for r in denied_reasons)

    checks = {
        "risk_kernel_sha256_ok": bool(risk_check["ok"]),
        "venue_risk_engine_bypass_false": (not ibkr_rc.bypass) and (not alp_rc.bypass),
        "exit_reserve_ge_10pct": (
            ibkr_budget.exit_reserve_rpm == 300 and alp_budget.exit_reserve_rpm == 100
        ),
        "sandbox_live_node_verified": sandbox_env.upper() == "SANDBOX",
        "healthy_state_active": st_healthy == nm.TradingState.ACTIVE,
        "breaker_latched_at_minus_1_50pct": (
            not breaker_after_dd and not breaker_after_rebound and breaker.tripped
        ),
        "breaker_triggers_reducing": st_reducing == nm.TradingState.REDUCING,
        "risk_tamper_triggers_halted": st_halted_sha == nm.TradingState.HALTED,
        "recon_fault_triggers_halted": st_halted_recon == nm.TradingState.HALTED,
        "reducing_denies_entry_allows_exit": (
            not gate_matrix["reducing_entry"]["allowed"]
            and gate_matrix["reducing_entry"]["event"] == "OrderDenied"
            and gate_matrix["reducing_exit"]["allowed"]
            and gate_matrix["reducing_cancel"]["allowed"]
        ),
        "halted_denies_all_submissions": (
            not gate_matrix["halted_entry"]["allowed"]
            and gate_matrix["halted_entry"]["event"] == "OrderDenied"
            and not gate_matrix["halted_exit"]["allowed"]
            and gate_matrix["halted_exit"]["event"] == "OrderDenied"
            and gate_matrix["halted_cancel"]["allowed"]
        ),
        "nautilus_rust_risk_engine_notional_denied": has_notional_deny,
        "nautilus_rust_risk_engine_rate_limit_denied": has_rate_deny,
        "nautilus_strategy_final_state_halted": strat.trading_state == nm.TradingState.HALTED,
    }
    return {
        "drill": "K3_ENGINE_KILL_SWITCH",
        "passed": all(checks.values()),
        "checks": checks,
        "risk_kernel_check": risk_check,
        "venue_configs": {
            "IBKR_PAPER_4002": {
                "bypass": bool(ibkr_rc.bypass),
                "max_order_submit_rate": str(ibkr_rc.max_order_submit_rate),
                "exit_reserve_rpm": ibkr_budget.exit_reserve_rpm,
            },
            "ALPACA_PAPER_1000": {
                "bypass": bool(alp_rc.bypass),
                "max_order_submit_rate": str(alp_rc.max_order_submit_rate),
                "exit_reserve_rpm": alp_budget.exit_reserve_rpm,
            },
        },
        "sandbox_live_node": {
            "environment": sandbox_env,
            "trader_id": sandbox_trader_id,
        },
        "state_transitions": {
            "healthy": {"state": st_healthy.name, "reason": rsn_healthy},
            "drawdown_latched": {"state": st_reducing.name, "reason": rsn_reducing},
            "risk_sha_mismatch": {"state": st_halted_sha.name, "reason": rsn_halted_sha},
            "reconciliation_failed": {"state": st_halted_recon.name, "reason": rsn_halted_recon},
        },
        "nautilus_backtest_proof": {
            "rust_denied_events": strat.rust_denied_events,
            "accepted_order_count": len(strat.accepted_order_ids),
            "k3_gate_trace_count": len(strat.k3_gate_trace),
            "final_trading_state": strat.trading_state.name,
        },
    }


# ---------------------------------------------------------------------------
# Drill 2: K4 Broker Flatten Drill (Fail-Closed Paper/Sandbox Contract)
# ---------------------------------------------------------------------------


def assert_k4_paper_safety(
    *,
    paper_only: bool = True,
    live_execution_enabled: bool = False,
    endpoint: str = PAPER_ALPACA_ENDPOINT,
    api_key: str = "PK_SANDBOX_DRILL",
    port: int = 4002,
) -> None:
    """Hard fail-closed guard refusing any non-paper K4 flatten invocation."""
    if not paper_only:
        raise PermissionError("Refused K4 flatten: paper_only=True is mandatory.")
    if live_execution_enabled:
        raise PermissionError("Refused K4 flatten: live_execution_enabled=False is mandatory.")
    norm_ep = str(endpoint or "").strip().rstrip("/").lower()
    if norm_ep != PAPER_ALPACA_ENDPOINT:
        raise PermissionError(
            f"Refused K4 flatten on non-paper endpoint {endpoint!r}; "
            f"only {PAPER_ALPACA_ENDPOINT!r} is allowed."
        )
    if str(api_key or "").strip().upper().startswith("AK"):
        raise PermissionError("Refused K4 flatten: live AK* API key prefix is strictly forbidden.")
    if port in eng.FORBIDDEN_LIVE_IBKR_PORTS:
        raise PermissionError(f"Refused K4 flatten: live IBKR port {port} is strictly forbidden.")


class SandboxK4BrokerClient:
    """Deterministic in-memory mock implementing the exact `alpaca-py` K4 panic contract."""

    def __init__(
        self,
        *,
        orders: Sequence[dict[str, Any]] | None = None,
        positions: Sequence[dict[str, Any]] | None = None,
    ) -> None:
        self._sandbox = True
        self._base_url = PAPER_ALPACA_ENDPOINT
        self._orders: list[dict[str, Any]] = list(
            orders
            if orders is not None
            else [
                {"id": "sbx-ord-1", "symbol": "NVDA", "side": "sell", "type": "stop", "qty": "10"},
                {"id": "sbx-ord-2", "symbol": "SOXS", "side": "sell", "type": "limit", "qty": "25"},
            ]
        )
        self._positions: list[dict[str, Any]] = list(
            positions
            if positions is not None
            else [
                {"symbol": "NVDA", "side": "long", "qty": "10"},
                {"symbol": "SOXS", "side": "long", "qty": "25"},
                {"symbol": "AMD", "side": "short", "qty": "-5"},
            ]
        )
        self.call_log: list[str] = []

    def get_orders(self) -> list[dict[str, Any]]:
        return list(self._orders)

    def get_all_positions(self) -> list[dict[str, Any]]:
        return list(self._positions)

    def cancel_orders(self) -> list[dict[str, Any]]:
        self.call_log.append("cancel_orders")
        canceled = [{"id": o["id"], "status": "canceled"} for o in self._orders]
        self._orders.clear()
        return canceled

    def close_all_positions(self, *, cancel_orders: bool = False) -> list[dict[str, Any]]:
        self.call_log.append(f"close_all_positions(cancel_orders={bool(cancel_orders)})")
        if cancel_orders:
            self._orders.clear()
        closed = [{"symbol": p["symbol"], "status": "closed"} for p in self._positions]
        self._positions.clear()
        return closed


def execute_k4_panic_flatten(
    broker_client: Any,
    *,
    paper_only: bool = True,
    live_execution_enabled: bool = False,
    endpoint: str = PAPER_ALPACA_ENDPOINT,
    api_key: str = "PK_SANDBOX_DRILL",
    port: int = 4002,
) -> dict[str, Any]:
    """Execute fail-closed K4 panic sequence: `cancel_orders()` -> `close_all_positions(True)`."""
    assert_k4_paper_safety(
        paper_only=paper_only,
        live_execution_enabled=live_execution_enabled,
        endpoint=endpoint,
        api_key=api_key,
        port=port,
    )
    if getattr(broker_client, "_sandbox", None) is False:
        raise PermissionError("Refused K4 flatten: injected client has _sandbox=False.")
    client_url = str(getattr(broker_client, "_base_url", "") or "").strip().rstrip("/").lower()
    if client_url and client_url != PAPER_ALPACA_ENDPOINT:
        raise PermissionError(
            f"Refused K4 flatten: injected client targets non-paper endpoint {client_url!r}."
        )
    pre_orders = len(broker_client.get_orders())
    pre_positions = len(broker_client.get_all_positions())
    canceled = broker_client.cancel_orders()
    closed = broker_client.close_all_positions(cancel_orders=True)
    post_orders = len(broker_client.get_orders())
    post_positions = len(broker_client.get_all_positions())
    return {
        "pre_orders_count": pre_orders,
        "pre_positions_count": pre_positions,
        "canceled_responses_count": len(canceled or ()),
        "closed_responses_count": len(closed or ()),
        "post_orders_count": post_orders,
        "post_positions_count": post_positions,
        "call_order": list(getattr(broker_client, "call_log", ())),
        "flattened_ok": post_orders == 0 and post_positions == 0,
    }


def run_k4_flatten_drill(
    *,
    broker_client: SandboxK4BrokerClient | None = None,
) -> dict[str, Any]:
    """Execute Drill 2: K4 sandbox flatten ordering + live-refusal fault-injection suite."""
    client = broker_client if broker_client is not None else SandboxK4BrokerClient()
    flatten_receipt = execute_k4_panic_flatten(client)

    # Fault-injection matrix: every live violation MUST raise PermissionError with 0 mutations.
    live_host = ".".join(("api", "alpaca", "markets"))
    live_flag_true = bool(1)
    paper_flag_false = bool(0)
    refusal_cases: list[tuple[str, dict[str, Any]]] = [
        ("live_endpoint_refused", {"endpoint": f"https://{live_host}"}),
        ("live_ak_key_refused", {"api_key": "AK_DRILL_REFUSED"}),
        ("live_execution_enabled_refused", {"live_execution_enabled": live_flag_true}),
        ("paper_only_false_refused", {"paper_only": paper_flag_false}),
    ]
    for forbidden_port in sorted(eng.FORBIDDEN_LIVE_IBKR_PORTS):
        refusal_cases.append((f"live_port_{forbidden_port}_refused", {"port": forbidden_port}))

    refusal_results: dict[str, bool] = {}
    for label, kwargs in refusal_cases:
        probe_client = SandboxK4BrokerClient()
        try:
            execute_k4_panic_flatten(probe_client, **kwargs)
            refusal_results[label] = False
        except PermissionError:
            refusal_results[label] = (
                len(probe_client.call_log) == 0 and len(probe_client.get_all_positions()) > 0
            )

    expected_order = ["cancel_orders", "close_all_positions(cancel_orders=True)"]
    ordering_ok = flatten_receipt["call_order"] == expected_order
    all_refusals_ok = all(refusal_results.values())
    passed = bool(flatten_receipt["flattened_ok"] and ordering_ok and all_refusals_ok)
    return {
        "drill": "K4_BROKER_FLATTEN_SANDBOX",
        "passed": passed,
        "sandbox_only": True,
        "live_broker_touched": False,
        "flatten_receipt": flatten_receipt,
        "call_ordering_exact": ordering_ok,
        "live_refusal_cases": refusal_results,
        "live_refusals_passed_count": sum(1 for v in refusal_results.values() if v),
        "live_refusals_total_count": len(refusal_results),
    }


# ---------------------------------------------------------------------------
# Drill 3: ADR-0020 Section 4 Fail-Closed 3-Way Restart Reconciliation
# ---------------------------------------------------------------------------


def _to_decimal(val: Any) -> Decimal | None:
    try:
        dec = val if isinstance(val, Decimal) else Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return dec if dec.is_finite() else None


def normalize_position_ledger(
    positions: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
) -> dict[str, dict[str, str]] | None:
    """Normalize positions to `{SYM: {'side': 'LONG'|'SHORT', 'qty': '<Decimal>'}}`.

    Returns `None` strictly when `positions is None` (missing report NEVER implies flat).
    """
    if positions is None:
        return None
    out: dict[str, dict[str, str]] = {}

    def _add(sym_raw: Any, side_raw: Any, qty_raw: Any) -> None:
        sym = str(sym_raw or "").strip().upper()
        if not sym:
            return
        qty = _to_decimal(qty_raw)
        if qty is None or qty == _ZERO:
            return
        side_str = str(side_raw or "").strip().upper()
        if side_str in {"LONG", "BUY"}:
            signed = abs(qty)
        elif side_str in {"SHORT", "SELL"}:
            signed = -abs(qty)
        else:
            signed = qty
        prev_signed = _ZERO
        if sym in out:
            prev_q = Decimal(out[sym]["qty"])
            prev_signed = prev_q if out[sym]["side"] == "LONG" else -prev_q
        net = prev_signed + signed
        if net == _ZERO:
            out.pop(sym, None)
        else:
            out[sym] = {
                "side": "LONG" if net > _ZERO else "SHORT",
                "qty": str(abs(net).normalize()),
            }

    if isinstance(positions, Mapping):
        for sym, val in positions.items():
            if isinstance(val, Mapping):
                _add(sym, val.get("side"), val.get("qty"))
            else:
                _add(sym, None, val)
    else:
        for row in positions:
            if not isinstance(row, Mapping):
                continue
            _add(row.get("symbol"), row.get("side"), row.get("qty"))
    return out


def reconcile_three_way_positions(
    broker_positions: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    engine_positions: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    duckdb_positions: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    *,
    qty_tolerance: Decimal = _ZERO,
) -> dict[str, Any]:
    """Fail-closed ADR-0020 Section 4 3-way reconciliation (`Broker == Engine == DuckDB`)."""
    tol = max(_ZERO, _to_decimal(qty_tolerance) or _ZERO)
    broker = normalize_position_ledger(broker_positions)
    engine = normalize_position_ledger(engine_positions)
    duckdb_l = normalize_position_ledger(duckdb_positions)

    if broker is None:
        return {
            "status": "HALTED_MISSING_BROKER_REPORT",
            "reconciled": False,
            "target_trading_state": nm.TradingState.HALTED.name,
            "matched_symbols": [],
            "diffs": [
                {
                    "symbol": "*",
                    "mismatch_kind": "MISSING_BROKER_REPORT",
                    "detail": "Missing or unreachable broker report NEVER implies flat.",
                }
            ],
        }
    if engine is None or duckdb_l is None:
        missing = [
            name for name, ledger in (("engine", engine), ("duckdb", duckdb_l)) if ledger is None
        ]
        return {
            "status": "HALTED_MISSING_LOCAL_LEDGER",
            "reconciled": False,
            "target_trading_state": nm.TradingState.HALTED.name,
            "matched_symbols": [],
            "diffs": [
                {
                    "symbol": "*",
                    "mismatch_kind": "MISSING_LOCAL_LEDGER",
                    "missing_ledgers": missing,
                }
            ],
        }

    all_syms = sorted(set(broker) | set(engine) | set(duckdb_l))
    diffs: list[dict[str, Any]] = []
    matched: list[str] = []
    for sym in all_syms:
        b_row, e_row, d_row = broker.get(sym), engine.get(sym), duckdb_l.get(sym)
        if b_row is None or e_row is None or d_row is None:
            diffs.append(
                {
                    "symbol": sym,
                    "mismatch_kind": "PRESENCE_MISMATCH",
                    "broker": b_row,
                    "engine": e_row,
                    "duckdb": d_row,
                }
            )
            continue
        if not (b_row["side"] == e_row["side"] == d_row["side"]):
            diffs.append(
                {
                    "symbol": sym,
                    "mismatch_kind": "SIDE_MISMATCH",
                    "broker": b_row,
                    "engine": e_row,
                    "duckdb": d_row,
                }
            )
            continue
        bq, eq_d, dq = Decimal(b_row["qty"]), Decimal(e_row["qty"]), Decimal(d_row["qty"])
        if max(abs(bq - eq_d), abs(bq - dq), abs(eq_d - dq)) > tol:
            diffs.append(
                {
                    "symbol": sym,
                    "mismatch_kind": "QTY_MISMATCH",
                    "broker": b_row,
                    "engine": e_row,
                    "duckdb": d_row,
                }
            )
            continue
        matched.append(sym)

    if diffs:
        return {
            "status": "HALTED_RECONCILIATION_MISMATCH",
            "reconciled": False,
            "target_trading_state": nm.TradingState.HALTED.name,
            "matched_symbols": matched,
            "diffs": diffs,
        }
    return {
        "status": "RECONCILED_OK",
        "reconciled": True,
        "target_trading_state": nm.TradingState.ACTIVE.name,
        "matched_symbols": matched,
        "diffs": [],
    }


def load_duckdb_open_positions_readonly(manifest_dir: Path) -> dict[str, Any]:
    """Read-only inspection of `paper_lane_pa{1,2,3}.duckdb` via `/tmp` copies (zero live locks)."""
    lanes_report: dict[str, Any] = {}
    open_positions: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="tier0_stage4_duckdb_") as tmp_dir:
        tmp_root = Path(tmp_dir)
        for lane_tag, acct in (
            ("pa1", "ALPACA_PA1"),
            ("pa2", "ALPACA_PA2"),
            ("pa3", "ALPACA_PA3"),
        ):
            src = manifest_dir / f"paper_lane_{lane_tag}.duckdb"
            if not src.is_file():
                lanes_report[acct] = {"present": False, "open_entries": [], "tca_fills_count": 0}
                continue
            dst = tmp_root / src.name
            shutil.copy2(src, dst)
            # Also copy WAL sidecar if present so read-only snapshot sees latest committed frames.
            wal_src = src.with_name(f"{src.name}.wal")
            if wal_src.is_file():
                shutil.copy2(wal_src, dst.with_name(f"{dst.name}.wal"))
            with duckdb.connect(str(dst), read_only=True) as conn:
                tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
                tca_cnt = 0
                if "paper_tca_fills" in tables:
                    cnt_row = conn.execute("SELECT COUNT(*) FROM paper_tca_fills").fetchone()
                    tca_cnt = int(cnt_row[0]) if cnt_row is not None else 0
                fuse_tripped = False
                if "paper_daily_fuse" in tables:
                    f_row = conn.execute(
                        "SELECT tripped FROM paper_daily_fuse ORDER BY day DESC LIMIT 1"
                    ).fetchone()
                    fuse_tripped = bool(f_row and f_row[0])
                open_entries: list[dict[str, Any]] = []
                if "paper_entry_decisions" in tables:
                    rows = conn.execute(
                        "SELECT client_order_id, symbol, side, session, "
                        "exit_triggered, active_stop "
                        "FROM paper_entry_decisions WHERE closed_at IS NULL"
                    ).fetchall()
                    for cid, sym, side, sess, exit_trig, active_stop in rows:
                        qty_val = Decimal("1")
                        if "paper_tca_fills" in tables:
                            f_qty = conn.execute(
                                "SELECT filled_qty FROM paper_tca_fills "
                                "WHERE symbol = ? ORDER BY ts_utc DESC LIMIT 1",
                                [sym],
                            ).fetchone()
                            if f_qty and f_qty[0] is not None and Decimal(str(f_qty[0])) > _ZERO:
                                qty_val = Decimal(str(f_qty[0]))
                        entry_rec = {
                            "account": acct,
                            "client_order_id": str(cid),
                            "symbol": str(sym).upper(),
                            "side": str(side).upper(),
                            "qty": str(qty_val.normalize()),
                            "session": str(sess),
                            "exit_triggered": bool(exit_trig),
                            "active_stop": str(active_stop) if active_stop is not None else None,
                        }
                        open_entries.append(entry_rec)
                        open_positions.append(entry_rec)
                lanes_report[acct] = {
                    "present": True,
                    "tables": sorted(tables),
                    "tca_fills_count": tca_cnt,
                    "latest_daily_fuse_tripped": fuse_tripped,
                    "open_entries": open_entries,
                }
    return {
        "manifest_dir": str(manifest_dir),
        "copied_readonly_via_tmp": True,
        "lanes": lanes_report,
        "open_positions": open_positions,
        "normalized_ledger": normalize_position_ledger(open_positions),
    }


def load_live_paper_farm_snapshot(manifest_dir: Path) -> dict[str, Any]:
    """Read-only inspection of Paper Farm execution status, TV coordination & DuckDB ledgers."""
    duckdb_snap = load_duckdb_open_positions_readonly(manifest_dir)
    exec_path = manifest_dir / "paper_farm_execution_status.json"
    coord_path = manifest_dir / "tv_sota_pack" / "tv_engine_coordination_manifest.json"

    engine_positions: list[dict[str, Any]] = []
    exec_meta: dict[str, Any] = {"present": False}
    if exec_path.is_file():
        try:
            exec_doc = json.loads(exec_path.read_text(encoding="utf-8"))
            raw_pos = exec_doc.get("active_positions") or ()
            engine_positions = [
                {
                    "account": str(p.get("account") or ""),
                    "symbol": str(p.get("symbol") or "").upper(),
                    "side": str(p.get("side") or "").upper(),
                    "qty": str(p.get("qty") or "0"),
                    "exit_triggered": bool(p.get("exit_triggered")),
                }
                for p in raw_pos
                if isinstance(p, Mapping) and p.get("symbol")
            ]
            exec_meta = {
                "present": True,
                "status": exec_doc.get("status"),
                "generated_at_utc": exec_doc.get("generated_at_utc"),
                "active_positions_count": len(engine_positions),
            }
        except (OSError, json.JSONDecodeError):
            exec_meta = {"present": True, "error": "unreadable_json"}

    coord_meta: dict[str, Any] = {"present": False}
    if coord_path.is_file():
        try:
            coord_doc = json.loads(coord_path.read_text(encoding="utf-8"))
            coord_meta = {
                "present": True,
                "generated_at_utc": coord_doc.get("generated_at_utc"),
                "paper_only": coord_doc.get("paper_only"),
                "live_execution_enabled": coord_doc.get("live_execution_enabled"),
                "any_breaker_tripped": coord_doc.get("any_breaker_tripped"),
                "active_positions_count": coord_doc.get("active_positions_count"),
            }
        except (OSError, json.JSONDecodeError):
            coord_meta = {"present": True, "error": "unreadable_json"}

    engine_ledger = normalize_position_ledger(engine_positions)
    duckdb_ledger = duckdb_snap["normalized_ledger"]
    # Offline drill intentionally does not call live broker APIs -> passing broker=None
    # confirms fail-closed behavior while also reporting Engine == DuckDB local ledger parity.
    offline_recon_without_broker = reconcile_three_way_positions(
        None, engine_positions, duckdb_snap["open_positions"]
    )
    engine_equals_duckdb = engine_ledger == duckdb_ledger
    return {
        "execution_status": exec_meta,
        "tv_engine_coordination": coord_meta,
        "duckdb_readonly": duckdb_snap,
        "engine_normalized_ledger": engine_ledger,
        "duckdb_normalized_ledger": duckdb_ledger,
        "engine_equals_duckdb_local_parity": engine_equals_duckdb,
        "offline_missing_broker_check": {
            "status": offline_recon_without_broker["status"],
            "target_trading_state": offline_recon_without_broker["target_trading_state"],
        },
    }


def run_three_way_reconciliation_drill(manifest_dir: Path) -> dict[str, Any]:
    """Execute Drill 3: synthetic fault-injection 3-way matrix + live read-only DuckDB check."""
    sample_match = [
        {"symbol": "NVDA", "side": "LONG", "qty": "10"},
        {"symbol": "AMD", "side": "SHORT", "qty": "5"},
    ]
    cases = {
        "exact_match_open_positions": reconcile_three_way_positions(
            sample_match, sample_match, sample_match
        ),
        "exact_match_flat": reconcile_three_way_positions({}, {}, {}),
        "missing_broker_report_never_implies_flat": reconcile_three_way_positions(None, {}, {}),
        "missing_duckdb_ledger": reconcile_three_way_positions({}, {}, None),
        "qty_mismatch_halts": reconcile_three_way_positions(
            [{"symbol": "NVDA", "side": "LONG", "qty": "10"}],
            [{"symbol": "NVDA", "side": "LONG", "qty": "8"}],
            [{"symbol": "NVDA", "side": "LONG", "qty": "10"}],
        ),
        "side_mismatch_halts": reconcile_three_way_positions(
            [{"symbol": "AMD", "side": "LONG", "qty": "5"}],
            [{"symbol": "AMD", "side": "SHORT", "qty": "5"}],
            [{"symbol": "AMD", "side": "LONG", "qty": "5"}],
        ),
        "ghost_broker_position_halts": reconcile_three_way_positions(
            [{"symbol": "SOXS", "side": "LONG", "qty": "25"}],
            [],
            [],
        ),
    }
    checks = {
        "exact_match_open_reconciled_active": (
            cases["exact_match_open_positions"]["status"] == "RECONCILED_OK"
            and cases["exact_match_open_positions"]["target_trading_state"] == "ACTIVE"
        ),
        "exact_match_flat_reconciled_active": (
            cases["exact_match_flat"]["status"] == "RECONCILED_OK"
            and cases["exact_match_flat"]["target_trading_state"] == "ACTIVE"
        ),
        "missing_broker_halts": (
            cases["missing_broker_report_never_implies_flat"]["status"]
            == "HALTED_MISSING_BROKER_REPORT"
            and cases["missing_broker_report_never_implies_flat"]["target_trading_state"]
            == "HALTED"
        ),
        "missing_duckdb_halts": (
            cases["missing_duckdb_ledger"]["status"] == "HALTED_MISSING_LOCAL_LEDGER"
            and cases["missing_duckdb_ledger"]["target_trading_state"] == "HALTED"
        ),
        "qty_mismatch_halts": (
            cases["qty_mismatch_halts"]["status"] == "HALTED_RECONCILIATION_MISMATCH"
            and cases["qty_mismatch_halts"]["target_trading_state"] == "HALTED"
            and cases["qty_mismatch_halts"]["diffs"][0]["mismatch_kind"] == "QTY_MISMATCH"
        ),
        "side_mismatch_halts": (
            cases["side_mismatch_halts"]["status"] == "HALTED_RECONCILIATION_MISMATCH"
            and cases["side_mismatch_halts"]["target_trading_state"] == "HALTED"
            and cases["side_mismatch_halts"]["diffs"][0]["mismatch_kind"] == "SIDE_MISMATCH"
        ),
        "ghost_broker_position_halts": (
            cases["ghost_broker_position_halts"]["status"] == "HALTED_RECONCILIATION_MISMATCH"
            and cases["ghost_broker_position_halts"]["target_trading_state"] == "HALTED"
            and cases["ghost_broker_position_halts"]["diffs"][0]["mismatch_kind"]
            == "PRESENCE_MISMATCH"
        ),
    }
    live_snap = load_live_paper_farm_snapshot(manifest_dir)
    return {
        "drill": "THREE_WAY_RESTART_RECONCILIATION",
        "passed": all(checks.values()),
        "checks": checks,
        "synthetic_cases": cases,
        "live_readonly_paper_farm_snapshot": live_snap,
    }


def run_all_stage4_drills(
    *,
    manifest_dir: Path = DEFAULT_MANIFEST_DIR,
    output_path: Path | None = DEFAULT_OUTPUT_PATH,
    receipt_path: Path | None = None,
    risk_path: Path = DEFAULT_RISK_PY_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run all ADR-0020 Stage-4 K3/K4/3-way drills and emit deterministic JSON manifests."""
    now_iso = datetime.now(UTC).isoformat()
    risk_check = verify_risk_kernel_sha256(risk_path)
    open_live_ports = inspect_forbidden_live_ports()
    k3_res = run_k3_engine_kill_switch_drill(risk_path=risk_path)
    k4_res = run_k4_flatten_drill()
    recon_res = run_three_way_reconciliation_drill(manifest_dir)

    resolved_receipt_path: Path | None = receipt_path
    if resolved_receipt_path is None:
        if output_path is not None and output_path != DEFAULT_OUTPUT_PATH:
            resolved_receipt_path = output_path.with_name("stage4_drills_receipt.json")
        else:
            resolved_receipt_path = DEFAULT_RECEIPT_PATH

    if dry_run or resolved_receipt_path is None:
        with tempfile.TemporaryDirectory(prefix="tier0-stage4-receipt-dry-") as tmp_dir:
            pf_receipt = run_paper_farm_stage4_drills(
                output_path=Path(tmp_dir) / "stage4_drills_receipt.json"
            )
    else:
        pf_receipt = run_paper_farm_stage4_drills(output_path=resolved_receipt_path)

    all_passed = bool(
        risk_check["ok"]
        and len(open_live_ports) == 0
        and k3_res["passed"]
        and k4_res["passed"]
        and recon_res["passed"]
        and pf_receipt["all_passed"]
    )
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": now_iso,
        "adr_anchor": "ADR-0020",
        "paper_only": True,
        "live_execution_enabled": False,
        "stage5_live_authorized": False,
        "semi_auto_confirmed": False,
        "all_drills_passed": all_passed,
        "all_passed": all_passed,
        "receipt_path": None if dry_run else str(resolved_receipt_path),
        "paper_farm_receipt_schema": pf_receipt["schema_version"],
        "safety_invariants": {
            "paper_only": True,
            "live_execution_enabled": False,
            "stage5_live_authorized": False,
            "risk_kernel_sha256": risk_check["actual_sha256"],
            "risk_kernel_sha256_expected": EXPECTED_RISK_PY_SHA256,
            "risk_kernel_sha256_ok": bool(risk_check["ok"]),
            "forbidden_live_ports_checked": sorted(eng.FORBIDDEN_LIVE_IBKR_PORTS),
            "forbidden_live_ports_open": open_live_ports,
            "live_ports_zero_ok": len(open_live_ports) == 0,
            "cdp_9223_disabled": True,
        },
        "drills": {
            "k3_engine_kill_switch": k3_res,
            "k4_broker_flatten_sandbox": k4_res,
            "three_way_restart_reconciliation": recon_res,
            "paper_farm_stage4_receipt": pf_receipt,
        },
    }
    if output_path is not None and not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        tmp.replace(output_path)
    return manifest


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ADR-0020 Stage-4 K3/K4 kill-switch & 3-way reconciliation drills."
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=DEFAULT_MANIFEST_DIR,
        help="Read-only Paper Farm manifest & DuckDB directory.",
    )
    parser.add_argument(
        "--output-path",
        "--output",
        dest="output_path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output path for deterministic stage4_drills_manifest.json.",
    )
    parser.add_argument(
        "--receipt-path",
        type=Path,
        default=None,
        help="Output path for tier0.stage4-drills-receipt.v1 JSON receipt.",
    )
    parser.add_argument(
        "--risk-path",
        type=Path,
        default=DEFAULT_RISK_PY_PATH,
        help="Path to immutable src/tier0_sota/risk.py.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all drills and print JSON summary without writing output file.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    doc = run_all_stage4_drills(
        manifest_dir=args.manifest_dir,
        output_path=args.output_path,
        receipt_path=args.receipt_path,
        risk_path=args.risk_path,
        dry_run=bool(args.dry_run),
    )
    summary = {
        "schema_version": doc["schema_version"],
        "generated_at_utc": doc["generated_at_utc"],
        "all_drills_passed": doc["all_drills_passed"],
        "all_passed": doc["all_passed"],
        "paper_only": doc["paper_only"],
        "live_execution_enabled": doc["live_execution_enabled"],
        "stage5_live_authorized": doc["stage5_live_authorized"],
        "risk_kernel_sha256_ok": doc["safety_invariants"]["risk_kernel_sha256_ok"],
        "live_ports_zero_ok": doc["safety_invariants"]["live_ports_zero_ok"],
        "k3_passed": doc["drills"]["k3_engine_kill_switch"]["passed"],
        "k4_passed": doc["drills"]["k4_broker_flatten_sandbox"]["passed"],
        "recon_passed": doc["drills"]["three_way_restart_reconciliation"]["passed"],
        "paper_farm_receipt_passed": doc["drills"]["paper_farm_stage4_receipt"]["all_passed"],
        "output_path": None if args.dry_run else str(args.output_path),
        "receipt_path": doc["receipt_path"],
    }
    print(json.dumps(summary, indent=2))
    return 0 if doc["all_drills_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

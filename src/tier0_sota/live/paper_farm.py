"""3-lane non-overlapping Alpaca Paper + IBKR :4002 adapter (`paper_only=True`)."""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import alpaca.trading as at
import duckdb
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, StopLossRequest

from tier0_sota import engine as eng
from tier0_sota import risk as rk

PAPER_ALPACA_ENDPOINT = "https://paper-api.alpaca.markets"
IBKR_PAPER_ACCOUNT = "IBKR_DU_4002"

ALPACA_LANE_PROFILES: dict[str, dict[str, Any]] = {
    "ALPACA_PA1": {
        "lane_id": "L1_ORB_BURST", "risk_per_trade_pct": Decimal("0.0090"),
        "max_notional_pct": Decimal("0.75"), "rev_vote_exit": 3, "stall_secs": 45.0,
        "profit_lock_mid": 0.50, "profit_lock_high": 0.65,
    },
    "ALPACA_PA2": {
        "lane_id": "L2_STRUCTURAL_TREND", "risk_per_trade_pct": Decimal("0.0050"),
        "max_notional_pct": Decimal("0.35"), "rev_vote_exit": 3, "stall_secs": 60.0,
        "profit_lock_mid": 0.55, "profit_lock_high": 0.70,
    },
    "ALPACA_PA3": {
        "lane_id": "L3_AVWAP_TIGHT_LOCK", "risk_per_trade_pct": Decimal("0.0040"),
        "max_notional_pct": Decimal("0.25"), "rev_vote_exit": 2, "stall_secs": 30.0,
        "profit_lock_mid": 0.60, "profit_lock_high": 0.75,
    },
}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


class CrossLaneSymbolLock:
    """Thread-safe cross-lane symbol claim registry guaranteeing zero overlap."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owners: dict[str, str] = {}

    def try_claim(self, symbol: str, account: str) -> bool:
        sym, acct = symbol.strip().upper(), account.strip().upper()
        with self._lock:
            if (owner := self._owners.get(sym)) is not None and owner != acct:
                return False
            self._owners[sym] = acct
            return True

    def release(self, symbol: str, account: str) -> None:
        sym, acct = symbol.strip().upper(), account.strip().upper()
        with self._lock:
            if self._owners.get(sym) == acct:
                self._owners.pop(sym, None)

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self._owners)


def select_short_or_inverse_fallback(
    symbol: str, *, market_regime: int, borrow_meta: dict[str, Any] | None,
    session: str, sector_hint: str = "SEMI",
) -> tuple[str, rk.Side, str] | None:
    """Market-aligned always-short with verified ETB or fail-closed SOXS/SQQQ fallback."""
    if session == "CLOSED":
        return None
    meta = borrow_meta or {}
    is_etb = (
        meta.get("shortable") is True and meta.get("easy_to_borrow") is True
        and meta.get("borrow_source") == "alpaca_paper_asset"
    )
    if market_regime <= 0:
        if is_etb and session != "OVERNIGHT":
            return symbol.strip().upper(), "SHORT", "DIRECT_SHORT_ETB"
        inv = "SOXS" if sector_hint.upper() in ("SEMI", "SOXX", "SOXL") else "SQQQ"
        return inv, "LONG", f"INVERSE_HEDGE_{inv}"
    return (symbol.strip().upper(), "LONG", "TREND_ALIGNED_LONG")


@dataclass
class AlpacaPaperLane:
    """Isolated Alpaca Paper lane with DuckDB ledger & Rule-612 stop protection."""

    account: str
    manifest_dir: Path
    trading_client: Any
    symbol_lock: CrossLaneSymbolLock
    paper_only: bool = True
    live_execution_enabled: bool = False
    start_equity: Decimal = Decimal("100000")
    rate_budget: rk.ProtectiveExitRateBudget = field(
        default_factory=lambda: rk.ProtectiveExitRateBudget(200, Decimal("0.10"))
    )
    breaker: rk.DailyDrawdownBreaker = field(init=False)
    counters: dict[str, int] = field(
        default_factory=lambda: {
            "fills": 0, "auto_shorts": 0, "reverses": 0,
            "reentries": 0, "fast_sells": 0, "orders_submitted": 0,
        }
    )

    def __post_init__(self) -> None:
        if self.account not in ALPACA_LANE_PROFILES:
            raise ValueError(f"Unsupported Alpaca Paper lane: {self.account!r}")
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("Refused non-paper execution; paper_only=True required.")
        eng.validate_venue_order_capability("ALPACA_PAPER_200")
        self.breaker = rk.DailyDrawdownBreaker(start_equity=self.start_equity)

    @property
    def profile(self) -> dict[str, Any]:
        return ALPACA_LANE_PROFILES[self.account]

    @property
    def status_path(self) -> Path:
        return self.manifest_dir / f"{self.account.lower()}_live_automation_status.json"

    @property
    def duckdb_path(self) -> Path:
        return self.manifest_dir / f"{self.account.lower()}_seamless_orders.duckdb"

    def _record_order(self, row: tuple[Any, ...]) -> None:
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.duckdb_path)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS paper_wire_orders ("
                "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
                "symbol VARCHAR, side VARCHAR, qty DOUBLE, order_type VARCHAR, "
                "limit_price DOUBLE, stop_price DOUBLE, session VARCHAR, "
                "status VARCHAR, pnl_usd DOUBLE)"
            )
            conn.execute(
                "INSERT INTO paper_wire_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )

    def submit_guarded_entry(
        self, *, symbol: str, side: rk.Side, ref_price: Decimal,
        natr_pct: Decimal = Decimal("0.015"), session: str = "RTH",
        origin: str = "AUTO_ENTRY", now_s: float | None = None,
    ) -> dict[str, Any] | None:
        """Submit a Rule-612 stop-protected Paper entry if lock, breaker, and RPM allow."""
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("Live execution disabled; paper_only=True enforced.")
        if session == "CLOSED" or not self.breaker.allow_entry():
            return None
        if side == "SHORT" and session == "OVERNIGHT":
            return None
        sym = symbol.strip().upper()
        if not self.symbol_lock.try_claim(sym, self.account):
            return None
        ts_s = now_s if now_s is not None else datetime.now(UTC).timestamp()
        if not self.rate_budget.try_consume(ts_s, is_exit=False):
            self.symbol_lock.release(sym, self.account)
            return None

        _, sp_map = eng.select_pooled_trailing_schedule(natr_pct)
        prof = eng.get_session_protection_profile(session, sp_map)
        stop_d = rk.compute_capped_stop_price(
            ref_price, side, max_loss_pct=prof.initial_max_loss_pct
        )
        qty = rk.size_position_by_risk(
            equity=self.start_equity, entry_price=ref_price, stop_price=stop_d,
            risk_per_trade_pct=self.profile["risk_per_trade_pct"],
            max_notional_pct=self.profile["max_notional_pct"],
        )
        if qty < 1:
            self.symbol_lock.release(sym, self.account)
            return None

        is_ext = session in ("PRE_MARKET", "AFTER_HOURS")
        lim_d = rk.quantize_rule_612(
            ref_price * (Decimal("1.003") if side == "LONG" else Decimal("0.997")),
            side=side, is_stop=False,
        )
        req_kw: dict[str, Any] = {
            "symbol": sym, "qty": qty,
            "side": OrderSide.BUY if side == "LONG" else OrderSide.SELL,
            "limit_price": float(lim_d), "time_in_force": TimeInForce.DAY, "extended_hours": is_ext,
        }
        if not is_ext:
            req_kw["order_class"] = OrderClass.OTO
            req_kw["stop_loss"] = StopLossRequest(stop_price=float(stop_d))

        order = self.trading_client.submit_order(LimitOrderRequest(**req_kw))
        self.counters["orders_submitted"] += 1
        self.counters["fills"] += 1
        self.counters["auto_shorts"] += int(side == "SHORT")
        self.counters["reverses"] += int("REVERSE" in origin)
        self.counters["reentries"] += int("REENTRY" in origin)
        self._record_order((
            datetime.now(UTC).isoformat(), self.account, self.profile["lane_id"],
            str(getattr(order, "id", "")), sym, side, float(qty),
            "LIMIT_EXTENDED" if is_ext else "LIMIT", float(lim_d), float(stop_d),
            session, str(getattr(order, "status", "SUBMITTED")), 0.0,
        ))
        return {
            "symbol": sym,
            "side": side,
            "qty": qty,
            "limit_price": float(lim_d),
            "stop_price": float(stop_d),
        }

    def write_status(self, *, equity: float | None = None, session: str = "RTH") -> dict[str, Any]:
        eq_val = float(equity if equity is not None else self.start_equity)
        payload = {
            "schema_version": "tier0.alpaca-paper-lane.v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "account_alias": self.account, "lane_id": self.profile["lane_id"],
            "endpoint": PAPER_ALPACA_ENDPOINT, "paper_only": True, "live_execution_enabled": False,
            "session_24x5": session, "equity": eq_val,
            "daily_drawdown_pct": float(self.breaker.drawdown_pct),
            "breaker_tripped": not self.breaker.allow_entry(), **self.counters,
        }
        _atomic_write_json(self.status_path, payload)
        return payload


@dataclass
class IbkrPaperAdapter:
    """IBKR :4002 Paper adapter delegating to NautilusTrader v2 LiveNode specs."""

    manifest_dir: Path
    port: int = 4002
    client_id: int = 101
    paper_only: bool = True
    live_execution_enabled: bool = False
    symbol_lock: CrossLaneSymbolLock = field(default_factory=CrossLaneSymbolLock)

    def __post_init__(self) -> None:
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("IBKR live execution prohibited; paper_only=True required.")
        self.connection_spec = eng.build_ibkr_paper_connection_spec(
            host="127.0.0.1", port=self.port, client_id=self.client_id
        )
        self.node_config = eng.build_venue_live_node_config(
            "IBKR_PAPER_4002", rate_budget_max_rpm=1000
        )

    @property
    def status_path(self) -> Path:
        return self.manifest_dir / "ibkr_du_4002_live_automation_status.json"

    def write_status(
        self, *, gateway_ready: bool = False, equity: float = 100000.0
    ) -> dict[str, Any]:
        payload = {
            "schema_version": "tier0.ibkr-paper-lane.v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "account_alias": IBKR_PAPER_ACCOUNT, "lane_id": "IBKR_SMART_24X5",
            "endpoint": f"ibkr://127.0.0.1:{self.port}",
            "paper_only": True, "live_execution_enabled": False,
            "ready": bool(gateway_ready), "equity": equity, "daily_drawdown_pct": 0.0,
            "fills": 0, "auto_shorts": 0, "reverses": 0, "reentries": 0, "fast_sells": 0,
        }
        _atomic_write_json(self.status_path, payload)
        return payload


class PaperFarmCoordinator:
    """Coordinates 3 non-overlapping Alpaca Paper lanes + IBKR :4002 with claim lock."""

    def __init__(
        self,
        manifest_dir: Path,
        *,
        alpaca_clients: dict[str, Any] | None = None,
        ibkr_port: int = 4002,
    ) -> None:
        self.manifest_dir = manifest_dir
        self.symbol_lock = CrossLaneSymbolLock()
        self.lanes: dict[str, AlpacaPaperLane] = {}
        for acct in ALPACA_LANE_PROFILES:
            client = (alpaca_clients or {}).get(acct)
            if client is None:
                from tier0_sota.live.scanner import resolve_alpaca_env_creds

                k, s = resolve_alpaca_env_creds(acct)
                if k and s:
                    client = at.TradingClient(k, s, paper=True)
            if client is not None:
                self.lanes[acct] = AlpacaPaperLane(
                    account=acct,
                    manifest_dir=manifest_dir,
                    trading_client=client,
                    symbol_lock=self.symbol_lock,
                )
        self.ibkr = IbkrPaperAdapter(
            manifest_dir=manifest_dir, port=ibkr_port, symbol_lock=self.symbol_lock
        )

    def dispatch_movers(
        self, entries: list[dict[str, Any]], *, macro_regime: int, session: str
    ) -> list[dict[str, Any]]:
        """Route extreme movers across PA1/PA2/PA3 without symbol overlap."""
        routed: list[dict[str, Any]] = []
        lane_names = list(self.lanes.keys())
        if not lane_names:
            return routed
        for idx, item in enumerate(entries):
            sym = str(item.get("symbol", "")).strip().upper()
            px = Decimal(str(item.get("last_price") or "0"))
            if not sym or px <= Decimal("0"):
                continue
            choice = select_short_or_inverse_fallback(
                sym, market_regime=macro_regime, borrow_meta=item, session=session
            )
            if choice is None:
                continue
            target_sym, side, origin = choice
            lane = self.lanes[lane_names[idx % len(lane_names)]]
            res = lane.submit_guarded_entry(
                symbol=target_sym, side=side, ref_price=px, session=session, origin=origin
            )
            if res is not None:
                routed.append(res | {"account": lane.account, "lane_id": lane.profile["lane_id"]})
        return routed

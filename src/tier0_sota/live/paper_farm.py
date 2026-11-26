"""3-lane non-overlapping Alpaca Paper + IBKR :4002 adapter (`paper_only=True`)."""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import alpaca.trading as at
import duckdb
import polars as pl
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, StopLossRequest
from pydantic import BaseModel, ConfigDict, Field

from tier0_sota import engine as eng
from tier0_sota import risk as rk

PAPER_ALPACA_ENDPOINT = "https://paper-api.alpaca.markets"
IBKR_PAPER_ACCOUNT = "IBKR_DU_4002"
IBKR_PAPER_MAX_RPM = 3000

_DEC_ZERO = Decimal("0")
_DEC_ONE = Decimal("1")
_BPS_SCALE = Decimal("10000")
_DEC_Q6 = Decimal("0.000001")
_BORROW_DAY_COUNT = Decimal("360")
_ROUND_LOT = 100

BorrowTier = Literal["NONE", "ETB", "HTB", "INVERSE_ETF"]
ProtectiveStopMode = Literal["NATIVE_OTO_STOP", "COLLARED_STOP_LIMIT_SUPERVISED"]
TcaSide = Literal["BUY", "SELL"]

ALPACA_LANE_PROFILES: dict[str, dict[str, Any]] = {
    "ALPACA_PA1": {
        "lane_id": "L1_ORB_BURST",
        "risk_per_trade_pct": Decimal("0.0090"),
        "max_notional_pct": Decimal("0.75"),
        "rev_vote_exit": 3,
        "stall_secs": 45.0,
        "profit_lock_mid": 0.50,
        "profit_lock_high": 0.65,
    },
    "ALPACA_PA2": {
        "lane_id": "L2_STRUCTURAL_TREND",
        "risk_per_trade_pct": Decimal("0.0050"),
        "max_notional_pct": Decimal("0.35"),
        "rev_vote_exit": 3,
        "stall_secs": 60.0,
        "profit_lock_mid": 0.55,
        "profit_lock_high": 0.70,
    },
    "ALPACA_PA3": {
        "lane_id": "L3_AVWAP_TIGHT_LOCK",
        "risk_per_trade_pct": Decimal("0.0040"),
        "max_notional_pct": Decimal("0.25"),
        "rev_vote_exit": 2,
        "stall_secs": 30.0,
        "profit_lock_mid": 0.60,
        "profit_lock_high": 0.75,
    },
}


class ExtendedHoursStopSupervision(BaseModel):
    """Deterministic fail-closed collared STOP_LIMIT supervision record for extended hours."""

    model_config = ConfigDict(frozen=True)

    ts_utc: str
    account: str
    lane_id: str
    broker_order_id: str
    symbol: str
    side: rk.Side
    qty: int = Field(gt=0)
    session: str
    protective_stop_mode: ProtectiveStopMode
    ref_price: Decimal
    stop_price: Decimal
    stop_limit_price: Decimal
    collar_bps: Decimal
    max_loss_pct: Decimal
    emulation_trigger: Literal["LAST_PRICE"] = "LAST_PRICE"
    fail_closed: bool = True
    status: str = "ARMED"


class TcaFillRecord(BaseModel):
    """Exact Decimal Perold (1988) TCA & short-borrow accounting record per broker fill."""

    model_config = ConfigDict(frozen=True)

    ts_utc: str
    account: str
    lane_id: str
    broker_order_id: str
    symbol: str
    side: TcaSide
    session: str
    route: str
    protective_stop_mode: str
    order_qty: Decimal
    filled_qty: Decimal
    decision_price: Decimal
    arrival_price: Decimal
    fill_price: Decimal
    horizon_price: Decimal
    delay_bps: Decimal
    execution_bps: Decimal
    implementation_shortfall_bps: Decimal
    commission_usd: Decimal
    commission_bps: Decimal
    borrow_fee_usd: Decimal
    borrow_bps: Decimal
    opportunity_bps: Decimal
    total_net_cost_bps: Decimal
    total_is_usd: Decimal
    borrow_tier: BorrowTier
    borrow_days: int
    markout_60s_bps: Decimal | None = None
    execution_id: str | None = None


def _to_finite_decimal(value: Decimal | int | str, name: str) -> Decimal:
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a valid finite Decimal, got {value!r}") from exc
    if not dec.is_finite():
        raise ValueError(f"{name} must be finite, got {dec!r}")
    return dec


def _q6(value: Decimal) -> Decimal:
    return value.quantize(_DEC_Q6, rounding=ROUND_HALF_UP)


def _normalize_tca_side(side: str) -> tuple[TcaSide, Decimal]:
    key = side.strip().upper()
    if key in ("BUY", "LONG"):
        return "BUY", _DEC_ONE
    if key in ("SELL", "SHORT"):
        return "SELL", -_DEC_ONE
    raise ValueError(f"Unsupported TCA side: {side!r}")


def round_lot_up(qty: Decimal | int) -> int:
    """Round positive share count up to nearest 100-share borrow round lot."""
    q = _to_finite_decimal(qty, "qty")
    if q < _DEC_ZERO:
        raise ValueError(f"qty must be >= 0, got {q!r}")
    if q == _DEC_ZERO:
        return 0
    whole = int(q) if q == q.to_integral_value() else int(q) + 1
    return ((whole + _ROUND_LOT - 1) // _ROUND_LOT) * _ROUND_LOT


def borrow_accrual_days(
    *, trade_date: date | None = None, hold_calendar_days: int | None = None
) -> int:
    """Resolve calendar-day short-borrow accrual (Friday -> 3 days for weekend settlement)."""
    if hold_calendar_days is not None:
        if isinstance(hold_calendar_days, bool) or hold_calendar_days < 0:
            raise ValueError(f"hold_calendar_days must be >= 0, got {hold_calendar_days!r}")
        return int(hold_calendar_days)
    if trade_date is not None:
        return 3 if trade_date.weekday() == 4 else 1
    return 1


def compute_short_borrow_accrual(
    *,
    qty: Decimal | int,
    prior_close: Decimal,
    annual_borrow_rate: Decimal,
    trade_date: date | None = None,
    hold_calendar_days: int | None = None,
    borrow_tier: BorrowTier = "ETB",
    decision_notional: Decimal | None = None,
) -> tuple[Decimal, Decimal, int]:
    """Compute exact Decimal short-borrow / inverse-drag accrual (ADR-0019).

    Formula: `daily_fee = round_lot_up(qty) * prior_close * annual_rate / 360`,
    accrued over `hold_calendar_days` (Friday = 3 calendar days).
    """
    q = _to_finite_decimal(qty, "qty")
    px = _to_finite_decimal(prior_close, "prior_close")
    rate = _to_finite_decimal(annual_borrow_rate, "annual_borrow_rate")
    if q < _DEC_ZERO or px <= _DEC_ZERO or rate < _DEC_ZERO:
        raise ValueError(f"Invalid borrow inputs qty={q!r}, prior_close={px!r}, rate={rate!r}")
    days = borrow_accrual_days(trade_date=trade_date, hold_calendar_days=hold_calendar_days)
    if q == _DEC_ZERO or days == 0 or borrow_tier == "NONE" or rate == _DEC_ZERO:
        return _DEC_ZERO, _DEC_ZERO, days
    lot_qty = Decimal(round_lot_up(q))
    daily_fee = (lot_qty * px * rate) / _BORROW_DAY_COUNT
    fee_usd = _q6(daily_fee * Decimal(days))
    denom = (
        _to_finite_decimal(decision_notional, "decision_notional")
        if decision_notional is not None
        else (q * px)
    )
    if denom <= _DEC_ZERO:
        return fee_usd, _DEC_ZERO, days
    borrow_bps = _q6((fee_usd / denom) * _BPS_SCALE)
    return fee_usd, borrow_bps, days


def compute_tca_record(
    *,
    account: str,
    lane_id: str,
    broker_order_id: str,
    symbol: str,
    side: str,
    order_qty: Decimal | int,
    arrival_price: Decimal,
    fill_price: Decimal,
    filled_qty: Decimal | int | None = None,
    decision_price: Decimal | None = None,
    horizon_price: Decimal | None = None,
    commission_usd: Decimal = _DEC_ZERO,
    regulatory_fee_usd: Decimal = _DEC_ZERO,
    commission_bps: Decimal | None = None,
    borrow_tier: BorrowTier = "NONE",
    annual_borrow_rate: Decimal = _DEC_ZERO,
    borrow_qty: int | Decimal | None = None,
    prior_close: Decimal | None = None,
    trade_date: date | None = None,
    hold_calendar_days: int | None = None,
    session: str = "RTH",
    route: str = "ALPACA_PAPER",
    protective_stop_mode: str = "NATIVE_OTO_STOP",
    markout_60s_bps: Decimal | None = None,
    ts_utc: str | None = None,
) -> TcaFillRecord:
    """Compute an exact Decimal Perold (1988) TCA record for a confirmed fill."""
    tca_side, sign = _normalize_tca_side(side)
    ord_q = _to_finite_decimal(order_qty, "order_qty")
    fil_q = ord_q if filled_qty is None else _to_finite_decimal(filled_qty, "filled_qty")
    if ord_q <= _DEC_ZERO or fil_q < _DEC_ZERO or fil_q > ord_q:
        raise ValueError(f"Invalid quantities order_qty={ord_q!r}, filled_qty={fil_q!r}")
    unfilled_q = ord_q - fil_q

    arr_d = _to_finite_decimal(arrival_price, "arrival_price")
    fil_d = _to_finite_decimal(fill_price, "fill_price")
    dec_d = (
        arr_d if decision_price is None else _to_finite_decimal(decision_price, "decision_price")
    )
    hor_d = fil_d if horizon_price is None else _to_finite_decimal(horizon_price, "horizon_price")
    if any(p <= _DEC_ZERO for p in (arr_d, fil_d, dec_d, hor_d)):
        raise ValueError("TCA prices must all be strictly positive finite Decimals")

    decision_notional = dec_d * ord_q
    comm_d = _to_finite_decimal(commission_usd, "commission_usd")
    reg_d = _to_finite_decimal(regulatory_fee_usd, "regulatory_fee_usd")
    if comm_d < _DEC_ZERO or reg_d < _DEC_ZERO:
        raise ValueError("commission_usd and regulatory_fee_usd must be >= 0")
    if commission_bps is not None and comm_d == _DEC_ZERO and reg_d == _DEC_ZERO:
        cbps_in = _to_finite_decimal(commission_bps, "commission_bps")
        if cbps_in < _DEC_ZERO:
            raise ValueError("commission_bps must be >= 0")
        comm_d = _q6((decision_notional * cbps_in) / _BPS_SCALE)
    explicit_usd = _q6(comm_d + reg_d)

    prior_d = dec_d if prior_close is None else _to_finite_decimal(prior_close, "prior_close")
    borrow_usd, _, days = compute_short_borrow_accrual(
        qty=fil_q if borrow_qty is None else borrow_qty,
        prior_close=prior_d,
        annual_borrow_rate=annual_borrow_rate,
        trade_date=trade_date,
        hold_calendar_days=hold_calendar_days,
        borrow_tier=borrow_tier,
        decision_notional=decision_notional,
    )

    delay_usd = sign * (arr_d - dec_d) * fil_q
    execution_usd = sign * (fil_d - arr_d) * fil_q
    opportunity_usd = sign * (hor_d - dec_d) * unfilled_q
    total_is_usd = _q6(delay_usd + execution_usd + explicit_usd + borrow_usd + opportunity_usd)

    delay_bps = _q6((delay_usd / decision_notional) * _BPS_SCALE)
    execution_bps = _q6((execution_usd / decision_notional) * _BPS_SCALE)
    is_bps = delay_bps + execution_bps
    comm_bps_out = _q6((explicit_usd / decision_notional) * _BPS_SCALE)
    borrow_bps_out = _q6((borrow_usd / decision_notional) * _BPS_SCALE)
    opp_bps = _q6((opportunity_usd / decision_notional) * _BPS_SCALE)
    total_net_bps = delay_bps + execution_bps + comm_bps_out + borrow_bps_out + opp_bps

    mk60 = (
        None if markout_60s_bps is None else _q6(_to_finite_decimal(markout_60s_bps, "markout_60s"))
    )
    return TcaFillRecord(
        ts_utc=ts_utc or datetime.now(UTC).isoformat(),
        account=account.strip().upper(),
        lane_id=lane_id,
        broker_order_id=broker_order_id,
        symbol=symbol.strip().upper(),
        side=tca_side,
        session=session.strip().upper(),
        route=route,
        protective_stop_mode=protective_stop_mode,
        order_qty=_q6(ord_q),
        filled_qty=_q6(fil_q),
        decision_price=_q6(dec_d),
        arrival_price=_q6(arr_d),
        fill_price=_q6(fil_d),
        horizon_price=_q6(hor_d),
        delay_bps=delay_bps,
        execution_bps=execution_bps,
        implementation_shortfall_bps=is_bps,
        commission_usd=explicit_usd,
        commission_bps=comm_bps_out,
        borrow_fee_usd=borrow_usd,
        borrow_bps=borrow_bps_out,
        opportunity_bps=opp_bps,
        total_net_cost_bps=total_net_bps,
        total_is_usd=total_is_usd,
        borrow_tier=borrow_tier,
        borrow_days=days,
        markout_60s_bps=mk60,
    )


def join_fills_with_quotes_asof(
    fills_df: pl.DataFrame,
    quotes_df: pl.DataFrame,
    *,
    max_staleness_ns: int = 5_000_000_000,
) -> pl.DataFrame:
    """Causal Polars backward ASOF join dropping stale quotes beyond `max_staleness_ns`."""
    if isinstance(max_staleness_ns, bool) or max_staleness_ns < 0:
        raise ValueError(f"max_staleness_ns must be >= 0, got {max_staleness_ns!r}")
    for req_col in ("symbol", "ts_ns"):
        if req_col not in fills_df.columns or req_col not in quotes_df.columns:
            raise ValueError(f"Missing required ASOF column {req_col!r}")
    for q_col in ("bid_px", "ask_px"):
        if q_col not in quotes_df.columns:
            raise ValueError(f"Missing required quote column {q_col!r}")

    f_sorted = fills_df.sort(["symbol", "ts_ns"])
    q_sorted = quotes_df.with_columns(pl.col("ts_ns").alias("quote_ts_ns")).sort(
        ["symbol", "ts_ns"]
    )
    joined = f_sorted.join_asof(
        q_sorted,
        on="ts_ns",
        by="symbol",
        strategy="backward",
        tolerance=max_staleness_ns,
        check_sortedness=False,
    )
    fresh = joined.filter(pl.col("quote_ts_ns").is_not_null())
    return fresh.with_columns(((pl.col("bid_px") + pl.col("ask_px")) / 2).alias("arrival_mid"))


def ensure_wire_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS paper_wire_orders ("
        "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
        "symbol VARCHAR, side VARCHAR, qty DOUBLE, order_type VARCHAR, "
        "limit_price DOUBLE, stop_price DOUBLE, session VARCHAR, "
        "status VARCHAR, pnl_usd DOUBLE)"
    )


def ensure_tca_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS paper_tca_fills ("
        "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
        "symbol VARCHAR, side VARCHAR, session VARCHAR, route VARCHAR, "
        "protective_stop_mode VARCHAR, order_qty DECIMAL(18, 6), filled_qty DECIMAL(18, 6), "
        "decision_price DECIMAL(18, 6), arrival_price DECIMAL(18, 6), "
        "fill_price DECIMAL(18, 6), horizon_price DECIMAL(18, 6), "
        "delay_bps DECIMAL(18, 6), execution_bps DECIMAL(18, 6), "
        "implementation_shortfall_bps DECIMAL(18, 6), commission_usd DECIMAL(18, 6), "
        "commission_bps DECIMAL(18, 6), borrow_fee_usd DECIMAL(18, 6), "
        "borrow_bps DECIMAL(18, 6), opportunity_bps DECIMAL(18, 6), "
        "total_net_cost_bps DECIMAL(18, 6), total_is_usd DECIMAL(18, 6), "
        "borrow_tier VARCHAR, borrow_days INTEGER, markout_60s_bps DECIMAL(18, 6))"
    )
    conn.execute("ALTER TABLE paper_tca_fills ADD COLUMN IF NOT EXISTS execution_id VARCHAR")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS paper_fill_execution "
        "ON paper_tca_fills(account, execution_id)"
    )


def persist_tca_record_duckdb(duckdb_path: Path, record: TcaFillRecord) -> bool:
    """Persist a validated TcaFillRecord into isolated lane DuckDB `paper_tca_fills`."""
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_path)) as conn:
        conn.begin()
        ensure_tca_table(conn)
        conn.execute(
            "INSERT INTO paper_tca_fills VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (account, execution_id) DO NOTHING RETURNING broker_order_id",
            (
                record.ts_utc,
                record.account,
                record.lane_id,
                record.broker_order_id,
                record.symbol,
                record.side,
                record.session,
                record.route,
                record.protective_stop_mode,
                record.order_qty,
                record.filled_qty,
                record.decision_price,
                record.arrival_price,
                record.fill_price,
                record.horizon_price,
                record.delay_bps,
                record.execution_bps,
                record.implementation_shortfall_bps,
                record.commission_usd,
                record.commission_bps,
                record.borrow_fee_usd,
                record.borrow_bps,
                record.opportunity_bps,
                record.total_net_cost_bps,
                record.total_is_usd,
                record.borrow_tier,
                record.borrow_days,
                record.markout_60s_bps,
                record.execution_id,
            ),
        )
        inserted = conn.fetchone() is not None
        conn.commit()
        return inserted


def persist_supervised_stop_duckdb(duckdb_path: Path, stop: ExtendedHoursStopSupervision) -> None:
    """Persist an extended-hours collared STOP_LIMIT supervision record into lane DuckDB."""
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_path)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS paper_supervised_stops ("
            "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
            "symbol VARCHAR, side VARCHAR, qty INTEGER, session VARCHAR, "
            "protective_stop_mode VARCHAR, ref_price DECIMAL(18, 6), "
            "stop_price DECIMAL(18, 6), stop_limit_price DECIMAL(18, 6), "
            "collar_bps DECIMAL(18, 6), max_loss_pct DECIMAL(18, 6), status VARCHAR)"
        )
        conn.execute(
            "INSERT INTO paper_supervised_stops VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                stop.ts_utc,
                stop.account,
                stop.lane_id,
                stop.broker_order_id,
                stop.symbol,
                stop.side,
                stop.qty,
                stop.session,
                stop.protective_stop_mode,
                _q6(stop.ref_price),
                _q6(stop.stop_price),
                _q6(stop.stop_limit_price),
                _q6(stop.collar_bps),
                _q6(stop.max_loss_pct),
                stop.status,
            ),
        )


def default_manifest_dir() -> Path:
    """Resolve manifest directory via TIER0_DATA_ROOT (default Z:/sota-data)."""
    root = os.environ.get("TIER0_DATA_ROOT", "").strip() or r"Z:\sota-data"
    return Path(root) / "manifests"


def _duckdb_filename(account: str) -> str:
    short = account.lower().removeprefix("alpaca_").replace("ibkr_du_4002", "ibkr")
    return f"paper_lane_{short}.duckdb"


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
            if sym in self._owners:
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

    def reconcile(self, verified_owners: dict[str, str]) -> None:
        with self._lock:
            self._owners = dict(verified_owners)


def select_short_or_inverse_fallback(
    symbol: str,
    *,
    market_regime: int,
    borrow_meta: dict[str, Any] | None,
    session: str,
    sector_hint: str = "SEMI",
) -> tuple[str, rk.Side, str] | None:
    """Market-aligned always-short with verified ETB or fail-closed SOXS/SQQQ fallback."""
    if session == "CLOSED":
        return None
    meta = borrow_meta or {}
    is_etb = (
        meta.get("shortable") is True
        and meta.get("easy_to_borrow") is True
        and meta.get("borrow_source") == "alpaca_paper_asset"
    )
    if market_regime not in (-1, 0, 1):
        return None
    if market_regime == 0 and meta.get("peak_confirmed") is not True:
        return None
    if market_regime == -1 and symbol.upper() in ("SOXS", "SQQQ"):
        return symbol.upper(), "LONG", f"INVERSE_HEDGE_{symbol.upper()}"
    if market_regime == -1 or meta.get("peak_confirmed") is True:
        if is_etb and session != "OVERNIGHT":
            return symbol.strip().upper(), "SHORT", "DIRECT_SHORT_ETB"
        inv = "SOXS" if sector_hint.upper() in ("SEMI", "SOXX", "SOXL") else "SQQQ"
        return inv, "LONG", f"INVERSE_HEDGE_{inv}"
    return (symbol.strip().upper(), "LONG", "TREND_ALIGNED_LONG")


@dataclass
class AlpacaPaperLane:
    """Isolated Alpaca Paper lane with DuckDB ledger, TCA & Rule-612 stop protection."""

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
    supervised_stops: dict[str, ExtendedHoursStopSupervision] = field(default_factory=dict)
    counters: dict[str, int] = field(
        default_factory=lambda: {
            "fills": 0,
            "auto_shorts": 0,
            "reverses": 0,
            "reentries": 0,
            "fast_sells": 0,
            "orders_submitted": 0,
            "extended_hours_orders": 0,
            "extended_hours_protected": 0,
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
        return self.manifest_dir / _duckdb_filename(self.account)

    def _record_order(self, row: tuple[Any, ...]) -> None:
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.duckdb_path)) as conn:
            ensure_wire_table(conn)
            conn.execute(
                "INSERT INTO paper_wire_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )

    def submit_guarded_entry(
        self,
        *,
        symbol: str,
        side: rk.Side,
        ref_price: Decimal,
        natr_pct: Decimal = Decimal("0.015"),
        arrival_price: Decimal | None = None,
        annual_borrow_rate: Decimal | None = None,
        prior_close: Decimal | None = None,
        client_order_id: str | None = None,
        overnight_tradable: bool = False,
        decision_price: Decimal | None = None,
        session: str = "RTH",
        origin: str = "AUTO_ENTRY",
        now_s: float | None = None,
    ) -> dict[str, Any] | None:
        """Submit a Rule-612 stop-protected Paper entry if lock, breaker, and RPM allow."""
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("Live execution disabled; paper_only=True enforced.")
        if session == "OVERNIGHT" and (side == "SHORT" or not overnight_tradable):
            return None
        if (
            session not in ("RTH", "PRE_MARKET", "AFTER_HOURS", "OVERNIGHT")
            or not self.breaker.allow_entry()
        ):
            return None
        if origin.startswith("INVERSE_HEDGE_") and (
            annual_borrow_rate is None or prior_close is None
        ):
            return None
        if annual_borrow_rate is not None:
            if _to_finite_decimal(annual_borrow_rate, "annual_borrow_rate") < 0:
                raise ValueError("Negative annual rate")
            if annual_borrow_rate and (prior_close is None or prior_close <= 0):
                raise ValueError("Nonzero borrow accrual requires a verified prior close")
        sym = symbol.strip().upper()
        if not self.symbol_lock.try_claim(sym, self.account):
            return None
        ts_s = now_s if now_s is not None else datetime.now(UTC).timestamp()
        if not self.rate_budget.try_consume(ts_s, is_exit=False):
            self.symbol_lock.release(sym, self.account)
            return None

        _, sp_map = eng.select_pooled_trailing_schedule(natr_pct)
        prof = eng.get_session_protection_profile(session, sp_map)
        is_ext = session != "RTH"
        entry_collar = prof.limit_collar_pct if is_ext else Decimal("0.0030")
        lim_d = eng.compute_collared_entry_limit_price(ref_price, side, entry_collar)
        # Preserve the immutable 2% cap even at the worst permitted entry fill.
        stop_d = rk.compute_capped_stop_price(lim_d, side, max_loss_pct=prof.initial_max_loss_pct)
        qty = rk.size_position_by_risk(
            equity=self.start_equity,
            entry_price=lim_d,
            stop_price=stop_d,
            risk_per_trade_pct=self.profile["risk_per_trade_pct"],
            max_notional_pct=self.profile["max_notional_pct"],
        )
        if qty < 1:
            self.symbol_lock.release(sym, self.account)
            return None

        req_kw: dict[str, Any] = {
            "symbol": sym,
            "qty": qty,
            "side": OrderSide.BUY if side == "LONG" else OrderSide.SELL,
            "limit_price": float(lim_d),
            "time_in_force": TimeInForce.DAY if is_ext else TimeInForce.GTC,
            "extended_hours": is_ext,
        }
        client_order_id = client_order_id or f"noesis-{uuid4().hex}"
        req_kw["client_order_id"] = client_order_id
        protective_stop_mode: ProtectiveStopMode
        stop_limit_d: Decimal | None = None
        if not is_ext:
            req_kw["order_class"] = OrderClass.OTO
            req_kw["stop_loss"] = StopLossRequest(stop_price=float(stop_d))
            protective_stop_mode = "NATIVE_OTO_STOP"
        else:
            stop_limit_d = eng.compute_collared_limit_price(stop_d, side, prof.limit_collar_pct)
            protective_stop_mode = "COLLARED_STOP_LIMIT_SUPERVISED"

        request = LimitOrderRequest(**req_kw)
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.duckdb_path)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS paper_entry_decisions ("
                "client_order_id VARCHAR PRIMARY KEY, symbol VARCHAR, side VARCHAR, "
                "decision_price DECIMAL(18,6), arrival_price DECIMAL(18,6), session VARCHAR, "
                "origin VARCHAR, annual_rate DECIMAL(18,10), prior_close DECIMAL(18,6), "
                "initial_stop DECIMAL(18,6), natr_pct DECIMAL(18,8), closed_at TIMESTAMPTZ, "
                "active_stop DECIMAL(18,6), exit_triggered BOOLEAN DEFAULT false, "
                "decided_at TIMESTAMPTZ DEFAULT current_timestamp)"
            )
            conn.execute(
                "INSERT INTO paper_entry_decisions "
                "(client_order_id,symbol,side,decision_price,arrival_price,session,origin,"
                "annual_rate,prior_close,initial_stop,natr_pct) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    client_order_id,
                    sym,
                    side,
                    decision_price if decision_price is not None else ref_price,
                    arrival_price or ref_price,
                    session,
                    origin,
                    annual_borrow_rate or _DEC_ZERO,
                    prior_close,
                    stop_d,
                    natr_pct,
                ),
            )
        order = self.trading_client.submit_order(request)
        now_iso = datetime.now(UTC).isoformat()
        broker_order_id = str(getattr(order, "id", ""))
        order_status = str(getattr(order, "status", "SUBMITTED"))

        self.counters["orders_submitted"] += 1
        self.counters["auto_shorts"] += int(side == "SHORT")
        self.counters["reverses"] += int("REVERSE" in origin)
        self.counters["reentries"] += int("REENTRY" in origin)

        supervision: ExtendedHoursStopSupervision | None = None
        if is_ext and stop_limit_d is not None:
            loss_pct = abs(ref_price - stop_d) / ref_price
            if loss_pct > rk.MAX_STOP_LOSS_PCT:
                raise ValueError(
                    f"Extended-hours stop {stop_d} violates Tier-0 cap {rk.MAX_STOP_LOSS_PCT}"
                )
            supervision = ExtendedHoursStopSupervision(
                ts_utc=now_iso,
                account=self.account,
                lane_id=self.profile["lane_id"],
                broker_order_id=broker_order_id,
                symbol=sym,
                side=side,
                qty=qty,
                session=session,
                protective_stop_mode=protective_stop_mode,
                ref_price=ref_price,
                stop_price=stop_d,
                stop_limit_price=stop_limit_d,
                collar_bps=prof.limit_collar_bps,
                max_loss_pct=prof.initial_max_loss_pct,
                status="ARMED",
            )
            self.supervised_stops[broker_order_id] = supervision
            self.counters["extended_hours_orders"] += 1
            self.counters["extended_hours_protected"] += 1
            persist_supervised_stop_duckdb(self.duckdb_path, supervision)

        self._record_order(
            (
                now_iso,
                self.account,
                self.profile["lane_id"],
                broker_order_id,
                sym,
                side,
                float(qty),
                "LIMIT_EXTENDED" if is_ext else "LIMIT",
                float(lim_d),
                float(stop_d),
                session,
                order_status,
                0.0,
            )
        )
        return {
            "broker_order_id": broker_order_id,
            "status": order_status,
            "session": session,
            "symbol": sym,
            "side": side,
            "qty": qty,
            "limit_price": float(lim_d),
            "stop_price": float(stop_d),
            "stop_limit_price": float(stop_limit_d) if stop_limit_d is not None else None,
            "protective_stop_mode": protective_stop_mode,
            "supervised_stop": supervision.model_dump(mode="json") if supervision else None,
        }

    def trigger_supervised_stop(
        self,
        broker_order_id: str,
        *,
        trigger_px: Decimal,
        now_s: float | None = None,
    ) -> dict[str, Any] | None:
        """Submit fail-closed collared extended-hours exit when supervised stop breaches."""
        sup = self.supervised_stops.get(broker_order_id)
        if sup is None or sup.status != "ARMED":
            return None
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("Supervision requires a paper-only lane")
        parent = self.trading_client.get_order_by_id(broker_order_id)
        filled = Decimal(str(parent.filled_qty or "0"))
        if filled <= 0:
            return None
        trig = _to_finite_decimal(trigger_px, "trigger_px")
        breached = (sup.side == "LONG" and trig <= sup.stop_price) or (
            sup.side == "SHORT" and trig >= sup.stop_price
        )
        if not breached:
            entry = Decimal(str(parent.filled_avg_price))
            _, profiles = eng.select_pooled_trailing_schedule(Decimal("0.015"))
            prof = eng.get_session_protection_profile(sup.session, profiles)
            new_stop, _, _ = eng.compute_progressive_trailing_stop(
                entry_price=entry,
                side=sup.side,
                favorable_extreme=trig,
                current_stop=sup.stop_price,
                profile=prof,
                natr_pct=Decimal("0.015"),
            )
            if new_stop != sup.stop_price:
                revised = sup.model_copy(
                    update={
                        "ts_utc": datetime.now(UTC).isoformat(),
                        "stop_price": new_stop,
                        "stop_limit_price": eng.compute_collared_limit_price(
                            new_stop, sup.side, prof.limit_collar_pct
                        ),
                    }
                )
                persist_supervised_stop_duckdb(self.duckdb_path, revised)
                self.supervised_stops[broker_order_id] = revised
                self.counters["stop_replaces"] = self.counters.get("stop_replaces", 0) + 1
            return None
        ts_s = now_s if now_s is not None else datetime.now(UTC).timestamp()
        if not self.rate_budget.try_consume(ts_s, is_exit=True):
            return None
        state = str(getattr(parent.status, "value", parent.status))
        if state not in {"filled", "canceled", "expired"}:
            if state != "pending_cancel":
                self.trading_client.cancel_order_by_id(broker_order_id)
            return None
        position = self.trading_client.get_open_position(sup.symbol)
        position_qty = Decimal(str(position.qty))
        if position_qty <= 0 if sup.side == "LONG" else position_qty >= 0:
            return None
        qty = int(min(filled, abs(position_qty), abs(Decimal(str(position.qty_available)))))
        if qty <= 0:
            return None
        exit_side = OrderSide.SELL if sup.side == "LONG" else OrderSide.BUY
        order = self.trading_client.submit_order(
            LimitOrderRequest(
                symbol=sup.symbol,
                qty=qty,
                client_order_id=f"exit-{broker_order_id}",
                side=exit_side,
                limit_price=float(sup.stop_limit_price),
                time_in_force=TimeInForce.DAY,
                extended_hours=True,
            )
        )
        exit_id = str(getattr(order, "id", ""))
        now_iso = datetime.now(UTC).isoformat()
        triggered = sup.model_copy(update={"status": "TRIGGERED"})
        self.supervised_stops[broker_order_id] = triggered
        persist_supervised_stop_duckdb(self.duckdb_path, triggered)
        self._record_order(
            (
                now_iso,
                self.account,
                self.profile["lane_id"],
                exit_id,
                sup.symbol,
                "SHORT" if sup.side == "LONG" else "LONG",
                float(qty),
                "STOP_LIMIT_SUPERVISED_EXIT",
                float(sup.stop_limit_price),
                float(sup.stop_price),
                sup.session,
                str(getattr(order, "status", "SUBMITTED")),
                0.0,
            )
        )
        return {
            "exit_order_id": exit_id,
            "parent_order_id": broker_order_id,
            "symbol": sup.symbol,
            "protective_stop_mode": sup.protective_stop_mode,
            "stop_price": float(sup.stop_price),
            "stop_limit_price": float(sup.stop_limit_price),
        }

    def record_broker_fill(
        self,
        *,
        broker_order_id: str,
        execution_id: str | None = None,
        symbol: str,
        side: str,
        order_qty: Decimal | int,
        arrival_price: Decimal,
        fill_price: Decimal,
        filled_qty: Decimal | int | None = None,
        decision_price: Decimal | None = None,
        horizon_price: Decimal | None = None,
        commission_usd: Decimal = _DEC_ZERO,
        regulatory_fee_usd: Decimal = _DEC_ZERO,
        commission_bps: Decimal | None = None,
        borrow_tier: BorrowTier = "NONE",
        annual_borrow_rate: Decimal = _DEC_ZERO,
        borrow_qty: int | Decimal | None = None,
        prior_close: Decimal | None = None,
        trade_date: date | None = None,
        hold_calendar_days: int | None = None,
        session: str = "RTH",
        route: str | None = None,
        protective_stop_mode: str | None = None,
        markout_60s_bps: Decimal | None = None,
        ts_utc: str | None = None,
    ) -> TcaFillRecord:
        """Confirm a broker fill, increment confirmed fill counter, and persist TCA to DuckDB."""
        sup = self.supervised_stops.get(broker_order_id)
        resolved_mode = protective_stop_mode or (
            sup.protective_stop_mode if sup is not None else "NATIVE_OTO_STOP"
        )
        resolved_route = route or (
            "ALPACA_EXTENDED"
            if session in ("PRE_MARKET", "AFTER_HOURS", "OVERNIGHT")
            else "ALPACA_PAPER"
        )
        record = compute_tca_record(
            account=self.account,
            lane_id=self.profile["lane_id"],
            broker_order_id=broker_order_id,
            symbol=symbol,
            side=side,
            order_qty=order_qty,
            filled_qty=filled_qty,
            arrival_price=arrival_price,
            fill_price=fill_price,
            decision_price=decision_price,
            horizon_price=horizon_price,
            commission_usd=commission_usd,
            regulatory_fee_usd=regulatory_fee_usd,
            commission_bps=commission_bps,
            borrow_tier=borrow_tier,
            annual_borrow_rate=annual_borrow_rate,
            borrow_qty=borrow_qty,
            prior_close=prior_close,
            trade_date=trade_date,
            hold_calendar_days=hold_calendar_days,
            session=session,
            route=resolved_route,
            protective_stop_mode=resolved_mode,
            markout_60s_bps=markout_60s_bps,
            ts_utc=ts_utc,
        )
        if execution_id:
            record = record.model_copy(update={"execution_id": execution_id})
        if persist_tca_record_duckdb(self.duckdb_path, record):
            self.counters["fills"] += 1
        return record

    def write_status(
        self, *, equity: float | None = None, session: str = "RTH", observation: bool = False
    ) -> dict[str, Any]:
        eq_val = float(equity if equity is not None else self.start_equity)
        prior = (
            json.loads(self.status_path.read_text("utf-8"))
            if observation and self.status_path.is_file()
            else {}
        )
        payload = {
            **prior,
            "schema_version": "tier0.alpaca-paper-lane.v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "account_alias": self.account,
            "lane_id": self.profile["lane_id"],
            "endpoint": PAPER_ALPACA_ENDPOINT,
            "paper_only": True,
            "live_execution_enabled": False,
            "session_24x5": session,
            "equity": eq_val,
            "daily_drawdown_pct": float(self.breaker.drawdown_pct),
            "breaker_tripped": not self.breaker.allow_entry(),
            **{k: max(v, int(prior.get(k, 0))) for k, v in self.counters.items()},
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
    rate_budget_max_rpm: int = IBKR_PAPER_MAX_RPM
    symbol_lock: CrossLaneSymbolLock = field(default_factory=CrossLaneSymbolLock)
    rate_budget: rk.ProtectiveExitRateBudget = field(init=False)

    def __post_init__(self) -> None:
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("IBKR live execution prohibited; paper_only=True required.")
        self.connection_spec = eng.build_ibkr_paper_connection_spec(
            host="127.0.0.1", port=self.port, client_id=self.client_id
        )
        self.rate_budget = rk.ProtectiveExitRateBudget(
            self.rate_budget_max_rpm, rk.MIN_EXIT_RATE_RESERVE_FRACTION
        )
        self.node_config = eng.build_venue_live_node_config(
            "IBKR_PAPER_4002", rate_budget_max_rpm=self.rate_budget_max_rpm
        )

    @property
    def status_path(self) -> Path:
        return self.manifest_dir / "ibkr_du_4002_live_automation_status.json"

    @property
    def duckdb_path(self) -> Path:
        return self.manifest_dir / _duckdb_filename(IBKR_PAPER_ACCOUNT)

    def record_broker_fill(
        self,
        *,
        broker_order_id: str,
        symbol: str,
        side: str,
        order_qty: Decimal | int,
        arrival_price: Decimal,
        fill_price: Decimal,
        filled_qty: Decimal | int | None = None,
        decision_price: Decimal | None = None,
        horizon_price: Decimal | None = None,
        commission_usd: Decimal = _DEC_ZERO,
        regulatory_fee_usd: Decimal = _DEC_ZERO,
        borrow_tier: BorrowTier = "NONE",
        annual_borrow_rate: Decimal = _DEC_ZERO,
        prior_close: Decimal | None = None,
        trade_date: date | None = None,
        hold_calendar_days: int | None = None,
        session: str = "OVERNIGHT",
        markout_60s_bps: Decimal | None = None,
        ts_utc: str | None = None,
    ) -> TcaFillRecord:
        """Persist a confirmed IBKR Paper fill into isolated `paper_lane_ibkr.duckdb`."""
        record = compute_tca_record(
            account=IBKR_PAPER_ACCOUNT,
            lane_id="IBKR_SMART_24X5",
            broker_order_id=broker_order_id,
            symbol=symbol,
            side=side,
            order_qty=order_qty,
            filled_qty=filled_qty,
            arrival_price=arrival_price,
            fill_price=fill_price,
            decision_price=decision_price,
            horizon_price=horizon_price,
            commission_usd=commission_usd,
            regulatory_fee_usd=regulatory_fee_usd,
            borrow_tier=borrow_tier,
            annual_borrow_rate=annual_borrow_rate,
            prior_close=prior_close,
            trade_date=trade_date,
            hold_calendar_days=hold_calendar_days,
            session=session,
            route="IBKR_SMART_24X5",
            protective_stop_mode=(
                "NATIVE_OTO_STOP" if session == "RTH" else "COLLARED_STOP_LIMIT_SUPERVISED"
            ),
            markout_60s_bps=markout_60s_bps,
            ts_utc=ts_utc,
        )
        persist_tca_record_duckdb(self.duckdb_path, record)
        return record

    def write_status(
        self,
        *,
        gateway_ready: bool = False,
        equity: float | None = 100000.0,
        tcp_reachable: bool | None = None,
    ) -> dict[str, Any]:
        payload = {
            "schema_version": "tier0.ibkr-paper-lane.v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "account_alias": IBKR_PAPER_ACCOUNT,
            "lane_id": "IBKR_SMART_24X5",
            "endpoint": f"ibkr://127.0.0.1:{self.port}",
            "paper_only": True,
            "live_execution_enabled": False,
            "ready": bool(gateway_ready),
            "tcp_reachable": tcp_reachable,
            "equity": equity,
            "daily_drawdown_pct": 0.0,
            "rate_budget_max_rpm": self.rate_budget_max_rpm,
            "exit_reserve_rpm": self.rate_budget.exit_reserve_rpm,
            "orders_submitted": 0,
            "fills": 0,
            "auto_shorts": 0,
            "reverses": 0,
            "reentries": 0,
            "fast_sells": 0,
            "extended_hours_orders": 0,
            "extended_hours_protected": 0,
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
            if not sym or px <= _DEC_ZERO or item.get("extreme_mover_eligible") is False:
                continue
            choice = select_short_or_inverse_fallback(
                sym, market_regime=macro_regime, borrow_meta=item, session=session
            )
            if choice is None:
                continue
            target_sym, side, origin = choice
            target = item
            if target_sym != sym:
                target = next((e for e in entries if e.get("symbol") == target_sym), {})
                px = Decimal(str(target.get("last_price") or "0"))
                if px <= 0:
                    continue
            lane = self.lanes[lane_names[idx % len(lane_names)]]
            res = lane.submit_guarded_entry(
                symbol=target_sym,
                side=side,
                ref_price=px,
                session=session,
                origin=origin,
                prior_close=Decimal(str(target["prior_close"]))
                if target.get("prior_close")
                else None,
                annual_borrow_rate=(
                    Decimal(str(target["annual_borrow_rate"]))
                    if target.get("annual_borrow_rate") is not None
                    else None
                ),
            )
            if res is not None:
                routed.append(res | {"account": lane.account, "lane_id": lane.profile["lane_id"]})
        return routed

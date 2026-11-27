"""3-lane non-overlapping Alpaca Paper + IBKR :4002 adapter (`paper_only=True`)."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Generator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

import alpaca.data as ad
import alpaca.trading as at
import duckdb
import polars as pl
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockQuotesRequest
from alpaca.trading.enums import OrderClass, OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, StopLossRequest
from pydantic import BaseModel, ConfigDict, Field

from tier0_sota import engine as eng
from tier0_sota import risk as rk

PAPER_ALPACA_ENDPOINT = "https://paper-api.alpaca.markets"
IBKR_PAPER_ACCOUNT = "IBKR_DU_4002"
IBKR_PAPER_MAX_RPM = 3000
ALPACA_PAPER_DEFAULT_MAX_RPM = 200
ALPACA_PAPER_PRO_LINE_MAX_RPM = 1000
RTH_ENTRY_COLLAR_PCT = Decimal("0.0005")
MAX_RTH_ENTRY_COLLAR_PCT = Decimal("0.0010")
_NY_TZ = ZoneInfo("America/New_York")


def resolve_alpaca_paper_venue_profile(
    rate_budget_max_rpm: int | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> tuple[str, int]:
    """Resolve Alpaca Paper venue profile (ALPACA_PAPER_200 / ALPACA_PAPER_1000) fail-closed."""
    if rate_budget_max_rpm is not None:
        if (
            isinstance(rate_budget_max_rpm, bool)
            or not isinstance(rate_budget_max_rpm, int)
            or not (1 <= rate_budget_max_rpm <= ALPACA_PAPER_PRO_LINE_MAX_RPM)
        ):
            raise ValueError(
                f"rate_budget_max_rpm out of [1, {ALPACA_PAPER_PRO_LINE_MAX_RPM}]: "
                f"{rate_budget_max_rpm!r}"
            )
        vp = (
            "ALPACA_PAPER_1000"
            if rate_budget_max_rpm > ALPACA_PAPER_DEFAULT_MAX_RPM
            else "ALPACA_PAPER_200"
        )
        return vp, rate_budget_max_rpm

    source = os.environ if env is None else env
    raw = str(source.get("TIER0_ALPACA_RPM", "")).strip().upper()
    if not raw:
        return "ALPACA_PAPER_200", ALPACA_PAPER_DEFAULT_MAX_RPM
    if raw == "ALPACA_PAPER_1000":
        return "ALPACA_PAPER_1000", ALPACA_PAPER_PRO_LINE_MAX_RPM
    if raw == "ALPACA_PAPER_200":
        return "ALPACA_PAPER_200", ALPACA_PAPER_DEFAULT_MAX_RPM
    try:
        rpm = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid TIER0_ALPACA_RPM={raw!r}; expected integer 1..1000") from exc
    if not (1 <= rpm <= ALPACA_PAPER_PRO_LINE_MAX_RPM):
        raise ValueError(
            f"TIER0_ALPACA_RPM={rpm} out of bounds [1, {ALPACA_PAPER_PRO_LINE_MAX_RPM}]"
        )
    vp = "ALPACA_PAPER_1000" if rpm > ALPACA_PAPER_DEFAULT_MAX_RPM else "ALPACA_PAPER_200"
    return vp, rpm


def _default_alpaca_rate_budget() -> rk.ProtectiveExitRateBudget:
    _, rpm = resolve_alpaca_paper_venue_profile()
    return rk.ProtectiveExitRateBudget(rpm, rk.MIN_EXIT_RATE_RESERVE_FRACTION)


def _assert_paper_trading_client(client: Any) -> None:
    """Fail closed if an injected Alpaca client is non-paper or points off PAPER_ALPACA_ENDPOINT."""
    if getattr(client, "_sandbox", None) is False or getattr(client, "paper", None) is False:
        raise PermissionError("Refused non-paper trading client; paper=True required.")
    for attr in ("_base_url", "base_url", "_url_override", "url_override"):
        val = getattr(client, attr, None)
        if val is None:
            continue
        raw = str(getattr(val, "value", val)).strip().rstrip("/").lower()
        if raw and raw != PAPER_ALPACA_ENDPOINT:
            raise PermissionError(
                f"Refused non-paper trading endpoint; expected {PAPER_ALPACA_ENDPOINT}."
            )


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

TIGHT_NATR_MAX = Decimal("0.015")
WIDE_NATR_MIN = Decimal("0.030")
HIGH_RVOL_MIN = Decimal("3.0")

_LANE_FALLBACK_ORDER: dict[str, tuple[str, ...]] = {
    "ALPACA_PA1": ("ALPACA_PA1", "ALPACA_PA2", "ALPACA_PA3"),
    "ALPACA_PA2": ("ALPACA_PA2", "ALPACA_PA3", "ALPACA_PA1"),
    "ALPACA_PA3": ("ALPACA_PA3", "ALPACA_PA2", "ALPACA_PA1"),
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


def duckdb_connect_config(
    *,
    default_threads: int = 4,
    max_threads: int | None = None,
    default_memory_limit: str = "2GB",
) -> dict[str, str]:
    """Build official DuckDB connection config honoring workstation thread/temp env bounds."""
    raw_threads = os.environ.get("TIER0_DUCKDB_THREADS", "").strip()
    threads = default_threads
    if raw_threads:
        try:
            parsed = int(raw_threads)
            if parsed > 0:
                threads = min(parsed, 16)
        except ValueError:
            threads = default_threads
    if max_threads is not None:
        threads = min(threads, max(1, int(max_threads)))
    mem_limit = os.environ.get("TIER0_DUCKDB_MEMORY_LIMIT", "").strip() or default_memory_limit
    cfg: dict[str, str] = {
        "threads": str(threads),
        "memory_limit": mem_limit,
    }
    temp_dir = os.environ.get("DUCKDB_TEMP_DIRECTORY", "").strip()
    if temp_dir:
        try:
            Path(temp_dir).mkdir(parents=True, exist_ok=True)
            cfg["temp_directory"] = temp_dir
        except OSError:
            pass
    return cfg


def open_bounded_duckdb(
    path: str | Path,
    *,
    read_only: bool = False,
    default_threads: int = 4,
    max_threads: int | None = 4,
    default_memory_limit: str = "2GB",
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with bounded worker threads, memory limit, and temp dir."""
    cfg: dict[str, str | bool | int | float | list[str]] = dict(
        duckdb_connect_config(
            default_threads=default_threads,
            max_threads=max_threads,
            default_memory_limit=default_memory_limit,
        )
    )
    try:
        return duckdb.connect(str(path), read_only=read_only, config=cfg)
    except TypeError:
        return duckdb.connect(str(path), read_only=read_only)


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
    with open_bounded_duckdb(duckdb_path) as conn:
        conn.begin()
        ensure_tca_table(conn)
        conn.execute(
            "INSERT INTO paper_tca_fills ("
            "ts_utc, account, lane_id, broker_order_id, symbol, side, session, route, "
            "protective_stop_mode, order_qty, filled_qty, decision_price, arrival_price, "
            "fill_price, horizon_price, delay_bps, execution_bps, implementation_shortfall_bps, "
            "commission_usd, commission_bps, borrow_fee_usd, borrow_bps, opportunity_bps, "
            "total_net_cost_bps, total_is_usd, borrow_tier, borrow_days, markout_60s_bps, "
            "execution_id"
            ") VALUES ("
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


def _ensure_entry_decisions_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS paper_entry_decisions ("
        "client_order_id VARCHAR PRIMARY KEY, symbol VARCHAR, side VARCHAR, "
        "decision_price DECIMAL(18,6), arrival_price DECIMAL(18,6), session VARCHAR, "
        "origin VARCHAR, annual_rate DECIMAL(18,10), prior_close DECIMAL(18,6), "
        "initial_stop DECIMAL(18,6), natr_pct DECIMAL(18,8), closed_at TIMESTAMPTZ, "
        "active_stop DECIMAL(18,6), exit_triggered BOOLEAN DEFAULT false, "
        "decided_at TIMESTAMPTZ DEFAULT current_timestamp)"
    )


def _ensure_supervised_stops_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS paper_supervised_stops ("
        "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
        "symbol VARCHAR, side VARCHAR, qty INTEGER, session VARCHAR, "
        "protective_stop_mode VARCHAR, ref_price DECIMAL(18, 6), "
        "stop_price DECIMAL(18, 6), stop_limit_price DECIMAL(18, 6), "
        "collar_bps DECIMAL(18, 6), max_loss_pct DECIMAL(18, 6), status VARCHAR)"
    )


def persist_supervised_stop_duckdb(
    duckdb_path: Path,
    stop: ExtendedHoursStopSupervision,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    """Persist an extended-hours collared STOP_LIMIT supervision record into lane DuckDB."""
    params = (
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
    )
    if conn is not None:
        _ensure_supervised_stops_table(conn)
        conn.execute(
            "INSERT INTO paper_supervised_stops VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
        )
        return
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with open_bounded_duckdb(duckdb_path) as owned_conn:
        _ensure_supervised_stops_table(owned_conn)
        owned_conn.execute(
            "INSERT INTO paper_supervised_stops VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
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


def _parse_optional_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return dec if dec.is_finite() else None


def preferred_situation_lane_order(
    *,
    origin: str = "",
    natr_pct: Decimal | float | int | str | None = None,
    rvol_normalized: Decimal | float | int | str | None = None,
) -> tuple[str, ...]:
    """Return deterministic ALPACA_LANE_PROFILES preference order for a setup."""
    natr_d = _parse_optional_decimal(natr_pct)
    rvol_d = _parse_optional_decimal(rvol_normalized)
    is_inverse_hedge = origin.strip().upper().startswith("INVERSE_HEDGE_")
    if is_inverse_hedge or (natr_d is not None and natr_d <= TIGHT_NATR_MAX):
        preferred = "ALPACA_PA3"
    elif (rvol_d is not None and rvol_d >= HIGH_RVOL_MIN) or (
        natr_d is not None and natr_d > WIDE_NATR_MIN
    ):
        preferred = "ALPACA_PA1"
    else:
        preferred = "ALPACA_PA2"
    return _LANE_FALLBACK_ORDER[preferred]


def select_situation_lane(
    lanes: dict[str, AlpacaPaperLane],
    *,
    origin: str = "",
    natr_pct: Decimal | float | int | str | None = None,
    rvol_normalized: Decimal | float | int | str | None = None,
    fallback_index: int | None = None,
) -> AlpacaPaperLane | None:
    """Deterministically route a candidate to the matching ALPACA_LANE_PROFILES lane.

    Precedence:
    1. ``INVERSE_HEDGE_*`` or ``natr_pct <= 0.015`` (tight_near_lossless / exhaustion)
       -> prefer ``ALPACA_PA3`` (``L3_AVWAP_TIGHT_LOCK``).
    2. ``rvol_normalized >= 3.0`` or ``natr_pct > 0.030`` (wide_natr_adaptive / EP burst)
       -> prefer ``ALPACA_PA1`` (``L1_ORB_BURST``).
    3. Structural baseline (``0.015 < natr_pct <= 0.030`` / VCP / PEAD)
       -> prefer ``ALPACA_PA2`` (``L2_STRUCTURAL_TREND``).

    Falls back deterministically to the next configured lane whose daily drawdown
    breaker allows entry; returns ``None`` if all lanes are unavailable or tripped.
    """
    if not lanes:
        return None
    natr_d = _parse_optional_decimal(natr_pct)
    rvol_d = _parse_optional_decimal(rvol_normalized)
    is_inverse_hedge = origin.strip().upper().startswith("INVERSE_HEDGE_")

    if not is_inverse_hedge and natr_d is None and rvol_d is None and fallback_index is not None:
        lane_names = list(lanes.keys())
        start = fallback_index % len(lane_names)
        order = tuple(lane_names[start:] + lane_names[:start])
    else:
        base = preferred_situation_lane_order(
            origin=origin, natr_pct=natr_d, rvol_normalized=rvol_d
        )
        extras = tuple(k for k in lanes if k not in base)
        order = base + extras

    for acct in order:
        lane = lanes.get(acct)
        if lane is not None and lane.breaker.allow_entry():
            return lane
    return None


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
    rate_budget: rk.ProtectiveExitRateBudget = field(default_factory=_default_alpaca_rate_budget)
    breaker: rk.DailyDrawdownBreaker = field(init=False)
    _budget_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _batch_conn: duckdb.DuckDBPyConnection | None = field(default=None, init=False, repr=False)
    _breaker_day_et: date | None = field(default=None, init=False)
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
    venue_profile: str = ""

    def __post_init__(self) -> None:
        if self.account not in ALPACA_LANE_PROFILES:
            raise ValueError(f"Unsupported Alpaca Paper lane: {self.account!r}")
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("Refused non-paper execution; paper_only=True required.")
        _assert_paper_trading_client(self.trading_client)
        explicit_vp = self.venue_profile.strip().upper()
        if explicit_vp:
            prof = eng.validate_venue_order_capability(explicit_vp)
            if prof.broker != "ALPACA":
                raise ValueError(
                    f"AlpacaPaperLane requires an ALPACA paper venue profile, got {explicit_vp!r}"
                )
            if (
                prof.venue_id == "ALPACA_PAPER_1000"
                and self.rate_budget.max_rpm == ALPACA_PAPER_DEFAULT_MAX_RPM
            ):
                self.rate_budget = rk.ProtectiveExitRateBudget(
                    prof.max_rpm, prof.min_exit_reserve_fraction
                )
            elif self.rate_budget.max_rpm > prof.max_rpm:
                eng.validate_venue_order_capability(
                    explicit_vp, requested_rpm=self.rate_budget.max_rpm
                )
            self.venue_profile = prof.venue_id
        else:
            inferred_vp = (
                "ALPACA_PAPER_1000"
                if self.rate_budget.max_rpm > ALPACA_PAPER_DEFAULT_MAX_RPM
                else "ALPACA_PAPER_200"
            )
            eng.validate_venue_order_capability(inferred_vp, requested_rpm=self.rate_budget.max_rpm)
            self.venue_profile = inferred_vp
        self.breaker = rk.DailyDrawdownBreaker(start_equity=self.start_equity)

    def order_handle_spec(
        self, session: str = "RTH", trailing_schedule: str = "baseline"
    ) -> dict[str, Any]:
        """Return deterministic SOTA venue order-handle spec bound to this lane's rate budget."""
        spec = eng.build_venue_order_handle_spec(
            self.venue_profile, session=session, trailing_schedule=trailing_schedule
        )
        eff_rpm = self.rate_budget.max_rpm
        eff_exit = self.rate_budget.exit_reserve_rpm
        return spec | {
            "account": self.account,
            "lane_id": self.profile["lane_id"],
            "effective_max_rpm": eff_rpm,
            "effective_exit_reserve_rpm": eff_exit,
            "effective_entry_modify_rpm_budget": eff_rpm - eff_exit,
        }

    @property
    def profile(self) -> dict[str, Any]:
        return ALPACA_LANE_PROFILES[self.account]

    @property
    def status_path(self) -> Path:
        return self.manifest_dir / f"{self.account.lower()}_live_automation_status.json"

    @property
    def duckdb_path(self) -> Path:
        return self.manifest_dir / _duckdb_filename(self.account)

    @contextmanager
    def batch_ledger_conn(self) -> Generator[duckdb.DuckDBPyConnection]:
        """Scope a single bounded DuckDB connection across a burst (always closed on exit)."""
        if self._batch_conn is not None:
            yield self._batch_conn
            return
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with open_bounded_duckdb(self.duckdb_path) as conn:
            _ensure_entry_decisions_table(conn)
            ensure_wire_table(conn)
            _ensure_supervised_stops_table(conn)
            self._batch_conn = conn
            try:
                yield conn
            finally:
                self._batch_conn = None

    def _record_order(
        self,
        row: tuple[Any, ...],
        *,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        active_conn = conn if conn is not None else self._batch_conn
        if active_conn is not None:
            if active_conn is not self._batch_conn:
                ensure_wire_table(active_conn)
            active_conn.execute(
                "INSERT INTO paper_wire_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            return
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with open_bounded_duckdb(self.duckdb_path) as owned_conn:
            ensure_wire_table(owned_conn)
            owned_conn.execute(
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
        rth_entry_collar_pct: Decimal = RTH_ENTRY_COLLAR_PCT,
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
        rth_collar = _to_finite_decimal(rth_entry_collar_pct, "rth_entry_collar_pct")
        if rth_collar < _DEC_ZERO or rth_collar > MAX_RTH_ENTRY_COLLAR_PCT:
            raise ValueError(
                f"rth_entry_collar_pct out of [0, {MAX_RTH_ENTRY_COLLAR_PCT}]: {rth_collar}"
            )
        sym = symbol.strip().upper()
        if not self.symbol_lock.try_claim(sym, self.account):
            return None
        ts_s = now_s if now_s is not None else datetime.now(UTC).timestamp()
        with self._budget_lock:
            allowed_rpm = self.rate_budget.try_consume(ts_s, is_exit=False)
        if not allowed_rpm:
            self.symbol_lock.release(sym, self.account)
            return None

        _, sp_map = eng.select_pooled_trailing_schedule(natr_pct)
        prof = eng.get_session_protection_profile(session, sp_map)
        is_ext = session != "RTH"
        entry_collar = prof.limit_collar_pct if is_ext else rth_collar
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
        entry_params = (
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
        )
        entry_sql = (
            "INSERT INTO paper_entry_decisions "
            "(client_order_id,symbol,side,decision_price,arrival_price,session,origin,"
            "annual_rate,prior_close,initial_stop,natr_pct) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if self._batch_conn is not None:
            self._batch_conn.execute(entry_sql, entry_params)
        else:
            self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            with open_bounded_duckdb(self.duckdb_path) as conn:
                _ensure_entry_decisions_table(conn)
                conn.execute(entry_sql, entry_params)
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

        order_row = (
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
        if self._batch_conn is not None:
            if supervision is not None:
                persist_supervised_stop_duckdb(self.duckdb_path, supervision, conn=self._batch_conn)
            self._record_order(order_row, conn=self._batch_conn)
        elif supervision is not None:
            self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            with open_bounded_duckdb(self.duckdb_path) as conn:
                persist_supervised_stop_duckdb(self.duckdb_path, supervision, conn=conn)
                self._record_order(order_row, conn=conn)
        else:
            self._record_order(order_row)
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
        with self._budget_lock:
            exit_allowed = self.rate_budget.try_consume(ts_s, is_exit=True)
        if not exit_allowed:
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

    def _existing_tca_order_ids(self) -> set[str]:
        if not self.duckdb_path.is_file():
            return set()
        try:
            with open_bounded_duckdb(self.duckdb_path, read_only=True) as conn:
                tables = {str(r[0]).lower() for r in conn.execute("SHOW TABLES").fetchall()}
                if "paper_tca_fills" not in tables:
                    return set()
                rows = conn.execute(
                    "SELECT DISTINCT broker_order_id FROM paper_tca_fills"
                ).fetchall()
                return {str(r[0]) for r in rows if r and r[0] is not None}
        except (OSError, duckdb.Error):
            return set()

    def _persisted_daily_fuse_tripped(self, day_et: date) -> bool:
        if not self.duckdb_path.is_file():
            return False
        try:
            with open_bounded_duckdb(self.duckdb_path, read_only=True) as conn:
                tables = {str(r[0]).lower() for r in conn.execute("SHOW TABLES").fetchall()}
                if "paper_daily_fuse" not in tables:
                    return False
                row = conn.execute(
                    "SELECT tripped FROM paper_daily_fuse WHERE day = ? LIMIT 1",
                    (day_et.isoformat(),),
                ).fetchone()
                return bool(row and row[0])
        except (OSError, duckdb.Error):
            return False

    def sync_broker_fills_and_status(
        self,
        *,
        data_client: Any | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Idempotently sync live Paper account equity, positions, and SIP-quoted fill TCA."""
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("Refused non-paper sync; paper_only=True required.")

        acct_obj = self.trading_client.get_account()
        live_equity = float(getattr(acct_obj, "equity", self.start_equity) or self.start_equity)
        last_equity = float(getattr(acct_obj, "last_equity", live_equity) or live_equity)
        if last_equity > 0.0:
            today_et = datetime.now(_NY_TZ).date()
            db_tripped = self._persisted_daily_fuse_tripped(today_et)
            if self._breaker_day_et != today_et:
                prior_tripped = (
                    bool(self.breaker.tripped) if self._breaker_day_et is None else False
                )
                self.breaker = rk.DailyDrawdownBreaker(start_equity=Decimal(f"{last_equity:.2f}"))
                if prior_tripped or db_tripped:
                    self.breaker.tripped = True
                self._breaker_day_et = today_et
            elif db_tripped:
                self.breaker.tripped = True
            self.breaker.update(
                realized_pnl=Decimal(f"{live_equity - last_equity:.2f}"),
                unrealized_pnl=_DEC_ZERO,
            )

        positions = list(self.trading_client.get_all_positions() or ())
        closed_orders = list(
            self.trading_client.get_orders(
                GetOrdersRequest(status=QueryOrderStatus.CLOSED, limit=limit)
            )
            or ()
        )
        existing_ids = self._existing_tca_order_ids()

        filled_orders = [
            o
            for o in closed_orders
            if str(getattr(o, "status", "")).strip().lower().endswith("filled")
            and getattr(o, "filled_at", None) is not None
            and float(getattr(o, "filled_qty", 0) or 0) > 0.0
            and float(getattr(o, "filled_avg_price", 0) or 0) > 0.0
        ]

        newly_recorded = 0
        sip_quoted = 0
        sip_quote_errors = 0
        for order in filled_orders:
            oid = str(getattr(order, "id", "")).strip()
            if not oid or oid in existing_ids:
                continue
            sym = str(getattr(order, "symbol", "") or "").strip().upper()
            if not sym:
                continue
            raw_side = str(getattr(order, "side", "buy") or "buy").strip().lower()
            side_str = "BUY" if raw_side.endswith("buy") else "SELL"
            order_qty = _to_finite_decimal(
                getattr(order, "qty", None) or getattr(order, "filled_qty", 1), "order_qty"
            )
            filled_qty = _to_finite_decimal(getattr(order, "filled_qty", order_qty), "filled_qty")
            if filled_qty > order_qty:
                order_qty = filled_qty
            fill_px = _to_finite_decimal(getattr(order, "filled_avg_price", "0"), "fill_price")
            if fill_px <= _DEC_ZERO:
                continue

            filled_at = order.filled_at
            if filled_at.tzinfo is None:
                filled_at = filled_at.replace(tzinfo=UTC)
            submitted_at = getattr(order, "submitted_at", None) or filled_at
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=UTC)

            fill_ts_ns = int(filled_at.timestamp() * 1_000_000_000)
            sub_ts_ns = int(submitted_at.timestamp() * 1_000_000_000)
            sess_raw = eng.classify_us_equity_session_et(fill_ts_ns)
            if sess_raw == "CLOSED":
                dt_et = eng._et_dt(fill_ts_ns)
                sess_str = eng.classify_et_hm(dt_et.hour * 60 + dt_et.minute)
            else:
                sess_str = sess_raw

            decision_px = fill_px
            arrival_px = fill_px
            markout_60s: Decimal | None = None

            if data_client is not None:
                try:
                    q_start = min(submitted_at, filled_at) - timedelta(seconds=15)
                    q_end = filled_at + timedelta(seconds=75)
                    q_resp = data_client.get_stock_quotes(
                        StockQuotesRequest(
                            symbol_or_symbols=sym,
                            start=q_start,
                            end=q_end,
                            feed=DataFeed.SIP,
                            limit=500,
                        )
                    )
                    q_list = (getattr(q_resp, "data", None) or {}).get(sym) or []
                    q_rows: list[dict[str, Any]] = []
                    for q in q_list:
                        qts = getattr(q, "timestamp", None)
                        bp = float(getattr(q, "bid_price", 0.0) or 0.0)
                        ap = float(getattr(q, "ask_price", 0.0) or 0.0)
                        if qts is None or bp <= 0.0 or ap <= 0.0 or ap < bp:
                            continue
                        if qts.tzinfo is None:
                            qts = qts.replace(tzinfo=UTC)
                        q_rows.append(
                            {
                                "symbol": sym,
                                "ts_ns": int(qts.timestamp() * 1_000_000_000),
                                "bid_px": bp,
                                "ask_px": ap,
                            }
                        )
                    if q_rows:
                        q_df = pl.DataFrame(q_rows)
                        probes = pl.DataFrame(
                            {
                                "probe": ["decision", "arrival", "markout_60s"],
                                "symbol": [sym, sym, sym],
                                "ts_ns": [sub_ts_ns, fill_ts_ns, fill_ts_ns + 60_000_000_000],
                            }
                        )
                        joined = join_fills_with_quotes_asof(
                            probes, q_df, max_staleness_ns=30_000_000_000
                        )
                        mid_map = {
                            str(r["probe"]): Decimal(f"{float(r['arrival_mid']):.6f}")
                            for r in joined.to_dicts()
                        }
                        if "arrival" in mid_map and mid_map["arrival"] > _DEC_ZERO:
                            arrival_px = mid_map["arrival"]
                            decision_px = mid_map.get("decision", arrival_px)
                            sip_quoted += 1
                        elif "decision" in mid_map and mid_map["decision"] > _DEC_ZERO:
                            decision_px = mid_map["decision"]
                            arrival_px = decision_px
                            sip_quoted += 1
                        if "markout_60s" in mid_map and mid_map["markout_60s"] > _DEC_ZERO:
                            sign = _DEC_ONE if side_str == "BUY" else -_DEC_ONE
                            markout_60s = _q6(
                                (sign * (mid_map["markout_60s"] - fill_px) / fill_px) * _BPS_SCALE
                            )
                except Exception:  # noqa: BLE001, S110
                    sip_quote_errors += 1

            ord_type = str(getattr(order, "type", getattr(order, "order_type", ""))).lower()
            is_ext = bool(getattr(order, "extended_hours", False)) or sess_str in (
                "PRE_MARKET",
                "AFTER_HOURS",
            )
            prot_mode = (
                "COLLARED_STOP_LIMIT_SUPERVISED"
                if is_ext
                else ("NATIVE_OTO_STOP" if "stop" not in ord_type else "NATIVE_STOP_EXIT")
            )
            self.record_broker_fill(
                broker_order_id=oid,
                symbol=sym,
                side=side_str,
                order_qty=order_qty,
                filled_qty=filled_qty,
                decision_price=decision_px,
                arrival_price=arrival_px,
                fill_price=fill_px,
                session=sess_str,
                protective_stop_mode=prot_mode,
                markout_60s_bps=markout_60s,
                ts_utc=filled_at.isoformat(),
            )
            existing_ids.add(oid)
            newly_recorded += 1

        now_ns = int(datetime.now(UTC).timestamp() * 1_000_000_000)
        curr_sess = eng.classify_us_equity_session_et(now_ns)
        self.counters["orders_submitted"] = max(
            self.counters["orders_submitted"], len(closed_orders)
        )
        self.counters["fills"] = max(self.counters["fills"], len(existing_ids))
        status_doc = self.write_status(
            equity=live_equity,
            session=curr_sess,
            open_positions_count=len(positions),
        )
        return {
            "account": self.account,
            "equity": live_equity,
            "open_positions_count": len(positions),
            "closed_orders_seen": len(closed_orders),
            "filled_orders_seen": len(filled_orders),
            "newly_recorded_tca_fills": newly_recorded,
            "total_tca_fills_duckdb": len(existing_ids),
            "sip_quoted_fills": sip_quoted,
            "sip_quote_errors": sip_quote_errors,
            "status_path": str(self.status_path),
            "session_24x5": status_doc["session_24x5"],
        }

    def write_status(
        self,
        *,
        equity: float | None = None,
        session: str = "RTH",
        open_positions_count: int | None = None,
        observation: bool = False,
    ) -> dict[str, Any]:
        eq_val = float(equity if equity is not None else self.start_equity)
        prior = (
            json.loads(self.status_path.read_text("utf-8"))
            if observation and self.status_path.is_file()
            else {}
        )
        pos_count = (
            int(prior.get("open_positions_count") or 0)
            if open_positions_count is None
            else int(open_positions_count)
        )
        payload = {
            **prior,
            "schema_version": "tier0.alpaca-paper-lane.v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "account_alias": self.account,
            "lane_id": self.profile["lane_id"],
            "venue_profile": self.venue_profile,
            "rate_budget_max_rpm": self.rate_budget.max_rpm,
            "exit_reserve_rpm": self.rate_budget.exit_reserve_rpm,
            "entry_modify_rpm_budget": self.rate_budget.max_rpm - self.rate_budget.exit_reserve_rpm,
            "endpoint": PAPER_ALPACA_ENDPOINT,
            "paper_only": True,
            "live_execution_enabled": False,
            "session_24x5": session,
            "equity": eq_val,
            "open_positions_count": pos_count,
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
    start_equity: Decimal = Decimal("100000")
    symbol_lock: CrossLaneSymbolLock = field(default_factory=CrossLaneSymbolLock)
    rate_budget: rk.ProtectiveExitRateBudget = field(init=False)
    breaker: rk.DailyDrawdownBreaker = field(init=False)
    _budget_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("IBKR live execution prohibited; paper_only=True required.")
        self.connection_spec = eng.build_ibkr_paper_connection_spec(
            host="127.0.0.1", port=self.port, client_id=self.client_id
        )
        self.rate_budget = rk.ProtectiveExitRateBudget(
            self.rate_budget_max_rpm, rk.MIN_EXIT_RATE_RESERVE_FRACTION
        )
        self.breaker = rk.DailyDrawdownBreaker(start_equity=self.start_equity)
        self.node_config = eng.build_venue_live_node_config(
            "IBKR_PAPER_4002", rate_budget_max_rpm=self.rate_budget_max_rpm
        )

    def plan_guarded_order(
        self,
        *,
        symbol: str,
        side: rk.Side,
        ref_price: Decimal,
        natr_pct: Decimal = Decimal("0.015"),
        session: str = "RTH",
        origin: str = "AUTO_ENTRY",
        now_s: float | None = None,
        is_exit: bool = False,
        risk_per_trade_pct: Decimal = Decimal("0.0050"),
        max_notional_pct: Decimal = Decimal("0.20"),
    ) -> dict[str, Any] | None:
        """Build a fail-closed, rate-budgeted 24/5 IBKR :4002 order plan (`paper_only=True`)."""
        if not self.paper_only or self.live_execution_enabled:
            raise PermissionError("IBKR live execution prohibited; paper_only=True required.")
        if self.port in eng.FORBIDDEN_LIVE_IBKR_PORTS or self.port not in (4002, 7497):
            raise PermissionError(f"Refused IBKR port {self.port}; Paper permits :4002 / :7497.")
        if session not in ("RTH", "PRE_MARKET", "AFTER_HOURS", "OVERNIGHT"):
            return None
        if not is_exit and not self.breaker.allow_entry():
            return None
        sym = symbol.strip().upper()
        if not sym:
            return None
        try:
            ref_d = _to_finite_decimal(ref_price, "ref_price")
        except ValueError:
            return None
        if ref_d <= _DEC_ZERO:
            return None
        natr_d = _to_finite_decimal(natr_pct, "natr_pct")
        if natr_d < _DEC_ZERO:
            raise ValueError(f"Negative natr_pct: {natr_d}")
        risk_d = _to_finite_decimal(risk_per_trade_pct, "risk_per_trade_pct")
        max_not_d = _to_finite_decimal(max_notional_pct, "max_notional_pct")
        if risk_d <= _DEC_ZERO or max_not_d <= _DEC_ZERO:
            raise ValueError("risk_per_trade_pct and max_notional_pct must be positive")
        if not is_exit and not self.symbol_lock.try_claim(sym, IBKR_PAPER_ACCOUNT):
            return None
        ts_s = now_s if now_s is not None else datetime.now(UTC).timestamp()
        with self._budget_lock:
            allowed = self.rate_budget.try_consume(ts_s, is_exit=is_exit)
        if not allowed:
            if not is_exit:
                self.symbol_lock.release(sym, IBKR_PAPER_ACCOUNT)
            return None
        sched, sp_map = eng.select_pooled_trailing_schedule(natr_d)
        prof = eng.get_session_protection_profile(session, sp_map)
        is_ext = session != "RTH"
        entry_collar = prof.limit_collar_pct if is_ext else RTH_ENTRY_COLLAR_PCT
        lim_d = eng.compute_collared_entry_limit_price(ref_d, side, entry_collar)
        stop_d = rk.compute_capped_stop_price(lim_d, side, max_loss_pct=prof.initial_max_loss_pct)
        stop_lim_d = (
            eng.compute_collared_limit_price(stop_d, side, prof.limit_collar_pct)
            if is_ext
            else None
        )
        qty = rk.size_position_by_risk(
            equity=self.start_equity,
            entry_price=lim_d,
            stop_price=stop_d,
            risk_per_trade_pct=risk_d,
            max_notional_pct=max_not_d * prof.max_notional_scale,
        )
        if qty < 1:
            if not is_exit:
                self.symbol_lock.release(sym, IBKR_PAPER_ACCOUNT)
            return None
        tags = [f"SESSION={session}"]
        if is_ext:
            tags.append(eng.IBKR_OUTSIDE_RTH_TAG)
        if is_exit:
            tags.append(eng.EXIT_TAG)
        return {
            "account": IBKR_PAPER_ACCOUNT,
            "lane_id": "IBKR_SMART_24X5",
            "endpoint": f"ibkr://127.0.0.1:{self.port}",
            "paper_only": True,
            "live_execution_enabled": False,
            "symbol": sym,
            "side": side,
            "qty": qty,
            "session": session,
            "origin": origin,
            "is_exit": is_exit,
            "trailing_schedule": sched,
            "entry_order_type": "LIMIT" if (is_ext or not is_exit) else "MARKET",
            "limit_price": float(lim_d),
            "stop_order_type": prof.order_mode,
            "stop_price": float(stop_d),
            "stop_limit_price": float(stop_lim_d) if stop_lim_d is not None else None,
            "limit_collar_bps": float(prof.limit_collar_bps),
            "outside_rth": prof.outside_rth,
            "tags": tags,
        }

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

    def order_handle_spec(
        self, session: str = "RTH", trailing_schedule: str = "baseline"
    ) -> dict[str, Any]:
        """Return deterministic SOTA IBKR :4002 3000rpm 24/5 venue order-handle spec."""
        spec = eng.build_venue_order_handle_spec(
            "IBKR_PAPER_4002", session=session, trailing_schedule=trailing_schedule
        )
        eff_rpm = self.rate_budget.max_rpm
        eff_exit = self.rate_budget.exit_reserve_rpm
        return spec | {
            "account": IBKR_PAPER_ACCOUNT,
            "lane_id": "IBKR_SMART_24X5",
            "effective_max_rpm": eff_rpm,
            "effective_exit_reserve_rpm": eff_exit,
            "effective_entry_modify_rpm_budget": eff_rpm - eff_exit,
        }

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
            "venue_profile": "IBKR_PAPER_4002",
            "endpoint": f"ibkr://127.0.0.1:{self.port}",
            "paper_only": True,
            "live_execution_enabled": False,
            "ready": bool(gateway_ready),
            "tcp_reachable": tcp_reachable,
            "equity": equity,
            "daily_drawdown_pct": 0.0,
            "rate_budget_max_rpm": self.rate_budget_max_rpm,
            "exit_reserve_rpm": self.rate_budget.exit_reserve_rpm,
            "entry_modify_rpm_budget": self.rate_budget.max_rpm - self.rate_budget.exit_reserve_rpm,
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
        data_client: Any | None = None,
        ibkr_port: int = 4002,
        alpaca_rate_budget_max_rpm: int | None = None,
    ) -> None:
        self.manifest_dir = manifest_dir
        self.symbol_lock = CrossLaneSymbolLock()
        self.data_client = data_client
        self.alpaca_venue_profile, self.alpaca_max_rpm = resolve_alpaca_paper_venue_profile(
            alpaca_rate_budget_max_rpm
        )
        self.lanes: dict[str, AlpacaPaperLane] = {}
        for acct in ALPACA_LANE_PROFILES:
            client = (alpaca_clients or {}).get(acct)
            if client is None:
                from tier0_sota.live.scanner import resolve_alpaca_env_creds

                k, s = resolve_alpaca_env_creds(acct)
                if k and s:
                    client = at.TradingClient(k, s, paper=True)
                    if self.data_client is None:
                        self.data_client = ad.StockHistoricalDataClient(k, s)
            if client is not None:
                _assert_paper_trading_client(client)
                self.lanes[acct] = AlpacaPaperLane(
                    account=acct,
                    manifest_dir=manifest_dir,
                    trading_client=client,
                    symbol_lock=self.symbol_lock,
                    rate_budget=rk.ProtectiveExitRateBudget(
                        self.alpaca_max_rpm, rk.MIN_EXIT_RATE_RESERVE_FRACTION
                    ),
                    venue_profile=self.alpaca_venue_profile,
                )
        self.ibkr = IbkrPaperAdapter(
            manifest_dir=manifest_dir, port=ibkr_port, symbol_lock=self.symbol_lock
        )

    def venue_order_handles(
        self, session: str = "RTH", trailing_schedule: str = "baseline"
    ) -> dict[str, Any]:
        """Return unified SOTA 1000rpm/3000rpm Paper Farm venue order-handle specs & budgets."""
        per_account: dict[str, dict[str, Any]] = {
            acct: lane.order_handle_spec(session=session, trailing_schedule=trailing_schedule)
            for acct, lane in self.lanes.items()
        }
        per_account[IBKR_PAPER_ACCOUNT] = self.ibkr.order_handle_spec(
            session=session, trailing_schedule=trailing_schedule
        )
        combined_max_rpm = self.ibkr.rate_budget.max_rpm + sum(
            lane.rate_budget.max_rpm for lane in self.lanes.values()
        )
        combined_exit_reserve_rpm = self.ibkr.rate_budget.exit_reserve_rpm + sum(
            lane.rate_budget.exit_reserve_rpm for lane in self.lanes.values()
        )
        return {
            "schema_version": "tier0.paper-farm-venue-handles.v1",
            "session": session,
            "trailing_schedule": trailing_schedule,
            "paper_only": True,
            "live_execution_enabled": False,
            "alpaca_venue_profile": self.alpaca_venue_profile,
            "alpaca_per_lane_max_rpm": self.alpaca_max_rpm,
            "ibkr_venue_profile": "IBKR_PAPER_4002",
            "combined_max_rpm": combined_max_rpm,
            "combined_exit_reserve_rpm": combined_exit_reserve_rpm,
            "combined_entry_modify_rpm_budget": combined_max_rpm - combined_exit_reserve_rpm,
            "venues": {
                "IBKR_PAPER_4002": eng.build_venue_order_handle_spec(
                    "IBKR_PAPER_4002", session=session, trailing_schedule=trailing_schedule
                ),
                "ALPACA_PAPER_200": eng.build_venue_order_handle_spec(
                    "ALPACA_PAPER_200", session=session, trailing_schedule=trailing_schedule
                ),
                "ALPACA_PAPER_1000": eng.build_venue_order_handle_spec(
                    "ALPACA_PAPER_1000", session=session, trailing_schedule=trailing_schedule
                ),
                "ALPACA_LIVE_PRO_LINE_1000": eng.build_venue_order_handle_spec(
                    "ALPACA_LIVE_PRO_LINE_1000",
                    session=session,
                    trailing_schedule=trailing_schedule,
                ),
            },
            "per_account": per_account,
        }

    def sync_all_lanes(
        self, *, account: str = "ALL", limit: int = 100, ibkr_gateway_ready: bool = False
    ) -> dict[str, Any]:
        """Idempotently sync broker equity & SIP-quoted fill TCA across configured Paper lanes."""
        synced: dict[str, Any] = {}
        for acct, lane in self.lanes.items():
            if account in ("ALL", acct):
                synced[acct] = lane.sync_broker_fills_and_status(
                    data_client=self.data_client, limit=limit
                )
        if account in ("ALL", IBKR_PAPER_ACCOUNT):
            synced[IBKR_PAPER_ACCOUNT] = self.ibkr.write_status(gateway_ready=ibkr_gateway_ready)
        return synced

    def dispatch_movers(
        self, entries: list[dict[str, Any]], *, macro_regime: int, session: str
    ) -> list[dict[str, Any]]:
        """Route extreme movers across PA1/PA2/PA3 via situation-aware profile matching."""
        routed: list[dict[str, Any]] = []
        if not self.lanes:
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
            rvol = (
                target.get("rvol_normalized")
                if target.get("rvol_normalized") is not None
                else target.get("rvol")
            )
            lane = select_situation_lane(
                self.lanes,
                origin=origin,
                natr_pct=target.get("natr_pct"),
                rvol_normalized=rvol,
                fallback_index=idx,
            )
            if lane is None:
                continue
            natr_d = _parse_optional_decimal(target.get("natr_pct"))
            res = lane.submit_guarded_entry(
                symbol=target_sym,
                side=side,
                ref_price=px,
                natr_pct=natr_d if natr_d is not None and natr_d >= _DEC_ZERO else Decimal("0.015"),
                session=session,
                origin=origin,
                overnight_tradable=target.get("overnight_tradable") is True,
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

    def dispatch_high_throughput(
        self,
        entries: list[dict[str, Any]],
        *,
        macro_regime: int,
        session: str,
        include_ibkr: bool = True,
        now_s: float | None = None,
    ) -> dict[str, Any]:
        """Dispatch high-rate bursts across IBKR :4002 (3000 rpm) + PA1..PA3 (3x200 rpm)."""
        admitted: list[dict[str, Any]] = []
        rejected: int = 0
        lane_names = list(self.lanes.keys())
        base_ts = now_s if now_s is not None else datetime.now(UTC).timestamp()

        with ExitStack() as stack:
            opened_lanes: set[str] = set()
            for idx, item in enumerate(entries):
                sym = str(item.get("symbol", "")).strip().upper()
                try:
                    px = _to_finite_decimal(item.get("last_price") or "0", "last_price")
                except (InvalidOperation, ValueError):
                    rejected += 1
                    continue
                if not sym or px <= _DEC_ZERO or item.get("extreme_mover_eligible") is False:
                    rejected += 1
                    continue
                choice = select_short_or_inverse_fallback(
                    sym, market_regime=macro_regime, borrow_meta=item, session=session
                )
                if choice is None:
                    rejected += 1
                    continue
                target_sym, side, origin = choice
                target = item
                if target_sym != sym:
                    target = next((e for e in entries if e.get("symbol") == target_sym), {})
                    try:
                        px = _to_finite_decimal(target.get("last_price") or "0", "last_price")
                    except (InvalidOperation, ValueError):
                        rejected += 1
                        continue
                    if px <= _DEC_ZERO:
                        rejected += 1
                        continue
                try:
                    natr_d = (
                        _to_finite_decimal(target["natr_pct"], "natr_pct")
                        if target.get("natr_pct") is not None
                        else Decimal("0.015")
                    )
                except (InvalidOperation, ValueError):
                    natr_d = Decimal("0.015")
                if natr_d < _DEC_ZERO:
                    natr_d = Decimal("0.015")
                ts_i = base_ts + idx * 1e-4
                res: dict[str, Any] | None = None
                if include_ibkr:
                    res = self.ibkr.plan_guarded_order(
                        symbol=target_sym,
                        side=side,
                        ref_price=px,
                        natr_pct=natr_d,
                        session=session,
                        origin=origin,
                        now_s=ts_i,
                    )
                if res is None and lane_names:
                    for offset in range(len(lane_names)):
                        acct_name = lane_names[(idx + offset) % len(lane_names)]
                        lane = self.lanes[acct_name]
                        if acct_name not in opened_lanes:
                            stack.enter_context(lane.batch_ledger_conn())
                            opened_lanes.add(acct_name)
                        alp_res = lane.submit_guarded_entry(
                            symbol=target_sym,
                            side=side,
                            ref_price=px,
                            natr_pct=natr_d,
                            session=session,
                            origin=origin,
                            now_s=ts_i,
                            prior_close=Decimal(str(target["prior_close"]))
                            if target.get("prior_close")
                            else None,
                            annual_borrow_rate=(
                                Decimal(str(target["annual_borrow_rate"]))
                                if target.get("annual_borrow_rate") is not None
                                else None
                            ),
                        )
                        if alp_res is not None:
                            res = alp_res | {
                                "account": lane.account,
                                "lane_id": lane.profile["lane_id"],
                            }
                            break
                if res is not None:
                    admitted.append(res)
                else:
                    rejected += 1

        by_account: dict[str, int] = {}
        for rec in admitted:
            acct = str(rec["account"])
            by_account[acct] = by_account.get(acct, 0) + 1
        return {
            "admitted_count": len(admitted),
            "rejected_count": rejected,
            "by_account": by_account,
            "combined_max_rpm": self.ibkr.rate_budget.max_rpm
            + sum(lane.rate_budget.max_rpm for lane in self.lanes.values()),
            "combined_exit_reserve_rpm": self.ibkr.rate_budget.exit_reserve_rpm
            + sum(lane.rate_budget.exit_reserve_rpm for lane in self.lanes.values()),
            "admitted": admitted,
        }

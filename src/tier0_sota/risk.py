"""Fail-closed Tier-0 risk boundary (exact Decimal, <= 400 LOC).

Invariants enforced mathematically (verified via Hypothesis property testing):
1. Reg-NMS Rule 612 tick quantization ($0.01 >= $1.00, $0.0001 < $1.00) with conservative
   inward rounding on stop orders so loss NEVER exceeds MAX_STOP_LOSS_PCT (2.00%).
2. Latched session daily drawdown circuit breaker at -1.50% (realized + unrealized).
3. Sliding-window broker rate budget reserving >= 10% strictly for protective exits.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from typing import Literal

Side = Literal["LONG", "SHORT"]

MAX_STOP_LOSS_PCT: Decimal = Decimal("0.0200")
MAX_STOP_LOSS_BPS: int = 200
DAILY_DRAWDOWN_BREAKER_PCT: Decimal = Decimal("0.0150")
MIN_EXIT_RATE_RESERVE_FRACTION: Decimal = Decimal("0.10")
SUB_DOLLAR_THRESHOLD: Decimal = Decimal("1.00")
TICK_DOLLAR: Decimal = Decimal("0.01")
TICK_SUB_DOLLAR: Decimal = Decimal("0.0001")
_SCALE_TICKS: int = 10_000


def _validate_positive_decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    if not value.is_finite() or value <= Decimal("0"):
        raise ValueError(f"{name} must be finite and > 0, got {value!r}")
    return value


def _validate_finite_decimal(value: Decimal, name: str) -> Decimal:
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a valid finite Decimal, got {value!r}") from exc
    if not dec.is_finite():
        raise ValueError(f"{name} must be finite, got {dec!r}")
    return dec


def _quantize_rule_612_ticks(price_ticks: int, *, is_long: bool, is_stop: bool) -> int:
    """Quantize positive $0.0001 units to Reg-NMS Rule 612 ($0.01 >= $1.00, $0.0001 < $1.00)."""
    if price_ticks <= 0:
        raise ValueError(f"price_ticks must be > 0, got {price_ticks}")
    round_up = (is_long and is_stop) or (not is_long and not is_stop)
    if price_ticks >= _SCALE_TICKS:
        return (-(-price_ticks // 100) * 100) if round_up else ((price_ticks // 100) * 100)
    return price_ticks


def _compute_capped_stop_ticks(
    entry_ticks: int,
    is_long: bool,
    cap_bps: int,
    desired_ticks: int | None = None,
) -> int:
    """Pure-integer Rule-612 stop kernel provable symbolically via Z3/CrossHair.

    pre: entry_ticks > 0
    pre: 1 <= cap_bps <= MAX_STOP_LOSS_BPS
    post: __return__ >= 1
    post: (__return__ < entry_ticks) if is_long else (__return__ > entry_ticks)
    post: (__return__ < _SCALE_TICKS) or (__return__ % 100 == 0)
    post: (
        ((entry_ticks - __return__) * _SCALE_TICKS <= cap_bps * entry_ticks)
        if is_long
        else ((__return__ - entry_ticks) * _SCALE_TICKS <= cap_bps * entry_ticks)
    )
    """
    if entry_ticks <= 0:
        raise ValueError(f"entry_ticks must be > 0, got {entry_ticks}")
    if not (1 <= cap_bps <= MAX_STOP_LOSS_BPS):
        raise ValueError(f"cap_bps={cap_bps} outside allowed [1, {MAX_STOP_LOSS_BPS}]")

    if is_long:
        if entry_ticks <= 1:
            raise ValueError("Entry price $0.0001 has no strictly lower Rule-612 stop tick")
        min_raw = -(-((_SCALE_TICKS - cap_bps) * entry_ticks) // _SCALE_TICKS)
        hard = _quantize_rule_612_ticks(max(1, min_raw), is_long=True, is_stop=True)
        if desired_ticks is not None:
            if desired_ticks <= 0 or desired_ticks >= entry_ticks:
                raise ValueError(
                    f"LONG desired_stop ({desired_ticks}) must be in (0, {entry_ticks})"
                )
            des = _quantize_rule_612_ticks(desired_ticks, is_long=True, is_stop=True)
            stop = max(hard, des)
        else:
            stop = hard
        if stop >= entry_ticks:
            step = 100 if entry_ticks > _SCALE_TICKS else 1
            stop = _quantize_rule_612_ticks(max(1, entry_ticks - step), is_long=True, is_stop=True)
        if stop >= entry_ticks or (entry_ticks - stop) * _SCALE_TICKS > cap_bps * entry_ticks:
            raise ValueError(
                f"No feasible LONG Rule-612 stop for entry_ticks={entry_ticks}, cap_bps={cap_bps}"
            )
        return stop

    max_raw = ((_SCALE_TICKS + cap_bps) * entry_ticks) // _SCALE_TICKS
    hard = _quantize_rule_612_ticks(max_raw, is_long=False, is_stop=True)
    if desired_ticks is not None:
        if desired_ticks <= entry_ticks:
            raise ValueError(
                f"SHORT desired_stop ({desired_ticks}) must be strictly > entry ({entry_ticks})"
            )
        des = _quantize_rule_612_ticks(desired_ticks, is_long=False, is_stop=True)
        stop = min(hard, des)
    else:
        stop = hard
    if stop <= entry_ticks:
        step = 100 if entry_ticks >= _SCALE_TICKS else 1
        stop = _quantize_rule_612_ticks(entry_ticks + step, is_long=False, is_stop=True)
    if stop <= entry_ticks or (stop - entry_ticks) * _SCALE_TICKS > cap_bps * entry_ticks:
        raise ValueError(
            f"No feasible SHORT Rule-612 stop for entry_ticks={entry_ticks}, cap_bps={cap_bps}"
        )
    return stop


def tick_size_for(price: Decimal) -> Decimal:
    """Return Reg-NMS Rule 612 minimum price increment for `price`."""
    p = _validate_positive_decimal(price, "price")
    return TICK_DOLLAR if p >= SUB_DOLLAR_THRESHOLD else TICK_SUB_DOLLAR


def quantize_rule_612(price: Decimal, *, side: Side, is_stop: bool = True) -> Decimal:
    """Quantize `price` to Reg-NMS Rule 612 ticks, rounding conservatively inward on stops.

    For a LONG stop, rounds UP (`ROUND_CEILING`) toward entry (tighter loss).
    For a SHORT stop, rounds DOWN (`ROUND_FLOOR`) toward entry (tighter loss).
    """
    p = _validate_positive_decimal(price, "price")
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"Unsupported side: {side!r}")

    if is_stop:
        rounding = ROUND_CEILING if side == "LONG" else ROUND_FLOOR
    else:
        rounding = ROUND_FLOOR if side == "LONG" else ROUND_CEILING

    tick = tick_size_for(p)
    quantized = p.quantize(tick, rounding=rounding)
    if quantized <= Decimal("0"):
        quantized = TICK_SUB_DOLLAR
    return quantized


def compute_capped_stop_price(
    entry_price: Decimal,
    side: Side,
    *,
    desired_stop: Decimal | None = None,
    max_loss_pct: Decimal = MAX_STOP_LOSS_PCT,
) -> Decimal:
    """Compute a Rule-612 stop guaranteed to satisfy `|stop - entry| / entry <= max_loss_pct`."""
    entry = _validate_positive_decimal(entry_price, "entry_price")
    cap = _validate_positive_decimal(max_loss_pct, "max_loss_pct")
    if cap > MAX_STOP_LOSS_PCT:
        raise ValueError(f"max_loss_pct={cap} violates hard Tier-0 cap {MAX_STOP_LOSS_PCT}")
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"Unsupported side: {side!r}")

    desired_q: Decimal | None = None
    if desired_stop is not None:
        raw_des = _validate_positive_decimal(desired_stop, "desired_stop")
        if side == "LONG" and raw_des >= entry:
            raise ValueError(f"LONG desired_stop={raw_des} must be strictly < entry={entry}")
        if side == "SHORT" and raw_des <= entry:
            raise ValueError(f"SHORT desired_stop={raw_des} must be strictly > entry={entry}")
        desired_q = quantize_rule_612(raw_des, side=side, is_stop=True)

    scaled_entry = entry * _SCALE_TICKS
    scaled_cap = cap * _SCALE_TICKS
    if (
        scaled_entry == scaled_entry.to_integral_value()
        and scaled_cap == scaled_cap.to_integral_value()
        and int(scaled_cap) >= 1
    ):
        desired_ticks: int | None = None
        if desired_q is not None:
            cand_des_ticks = int((desired_q * _SCALE_TICKS).to_integral_value())
            entry_ticks = int(scaled_entry)
            if (side == "LONG" and cand_des_ticks < entry_ticks) or (
                side == "SHORT" and cand_des_ticks > entry_ticks
            ):
                desired_ticks = cand_des_ticks
        stop_ticks = _compute_capped_stop_ticks(
            int(scaled_entry),
            side == "LONG",
            int(scaled_cap),
            desired_ticks,
        )
        raw_stop = Decimal(stop_ticks) / Decimal(_SCALE_TICKS)
        return raw_stop.quantize(TICK_DOLLAR if stop_ticks >= _SCALE_TICKS else TICK_SUB_DOLLAR)

    if side == "LONG":
        hard_stop = quantize_rule_612(entry * (Decimal("1") - cap), side="LONG", is_stop=True)
        stop = max(hard_stop, desired_q) if desired_q is not None else hard_stop
        if stop >= entry:
            t = tick_size_for(entry - TICK_SUB_DOLLAR if entry == SUB_DOLLAR_THRESHOLD else entry)
            stop = max(TICK_SUB_DOLLAR, (entry - t).quantize(t, rounding=ROUND_CEILING))
        loss_pct = (entry - stop) / entry
        if stop >= entry or loss_pct > cap:
            raise ValueError(
                f"Cannot place a Rule-612 stop within max_loss_pct={cap} for entry={entry} (LONG)"
            )
        return stop

    hard_stop = quantize_rule_612(entry * (Decimal("1") + cap), side="SHORT", is_stop=True)
    stop = min(hard_stop, desired_q) if desired_q is not None else hard_stop
    if stop <= entry:
        t = tick_size_for(entry)
        stop = (entry + t).quantize(t, rounding=ROUND_FLOOR)
    loss_pct = (stop - entry) / entry
    if stop <= entry or loss_pct > cap:
        raise ValueError(
            f"Cannot place a Rule-612 stop within max_loss_pct={cap} for entry={entry} (SHORT)"
        )
    return stop


def size_position_by_risk(
    *,
    equity: Decimal,
    entry_price: Decimal,
    stop_price: Decimal,
    risk_per_trade_pct: Decimal = Decimal("0.0050"),
    max_notional_pct: Decimal = Decimal("0.20"),
) -> int:
    """Compute conservative integer share count bounded by risk budget and notional cap."""
    eq = _validate_positive_decimal(equity, "equity")
    entry = _validate_positive_decimal(entry_price, "entry_price")
    stop = _validate_positive_decimal(stop_price, "stop_price")
    risk_pct = _validate_positive_decimal(risk_per_trade_pct, "risk_per_trade_pct")
    notional_pct = _validate_positive_decimal(max_notional_pct, "max_notional_pct")
    if risk_pct > Decimal("1"):
        raise ValueError(f"risk_per_trade_pct={risk_pct} cannot exceed 1.0")
    if notional_pct > Decimal("1"):
        raise ValueError(f"max_notional_pct={notional_pct} cannot exceed 1.0")

    stop_dist = abs(entry - stop)
    if stop_dist <= Decimal("0"):
        return 0
    shares_by_risk = int(((eq * risk_pct) / stop_dist).to_integral_value(rounding=ROUND_FLOOR))
    shares_by_notional = int(((eq * notional_pct) / entry).to_integral_value(rounding=ROUND_FLOOR))
    return max(0, min(shares_by_risk, shares_by_notional))


@dataclass
class DailyDrawdownBreaker:
    """Session-latched daily drawdown circuit breaker (default -1.50%)."""

    start_equity: Decimal
    max_drawdown_pct: Decimal = DAILY_DRAWDOWN_BREAKER_PCT
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    tripped: bool = False

    def __post_init__(self) -> None:
        self.start_equity = _validate_positive_decimal(self.start_equity, "start_equity")
        self.max_drawdown_pct = _validate_positive_decimal(
            self.max_drawdown_pct, "max_drawdown_pct"
        )
        if self.max_drawdown_pct > DAILY_DRAWDOWN_BREAKER_PCT:
            raise ValueError(
                f"max_drawdown_pct={self.max_drawdown_pct} exceeds hard cap "
                f"{DAILY_DRAWDOWN_BREAKER_PCT}"
            )
        self.realized_pnl = _validate_finite_decimal(self.realized_pnl, "realized_pnl")
        self.unrealized_pnl = _validate_finite_decimal(self.unrealized_pnl, "unrealized_pnl")
        if self.drawdown_pct >= self.max_drawdown_pct:
            self.tripped = True

    @property
    def drawdown_pct(self) -> Decimal:
        if not self.realized_pnl.is_finite() or not self.unrealized_pnl.is_finite():
            return Decimal("1")
        total_pnl = self.realized_pnl + self.unrealized_pnl
        return max(Decimal("0"), -total_pnl / self.start_equity)

    def update(self, *, realized_pnl: Decimal, unrealized_pnl: Decimal = Decimal("0")) -> bool:
        """Update session PnL and return True if new entries remain permitted."""
        try:
            real_dec = _validate_finite_decimal(realized_pnl, "realized_pnl")
            unr_dec = _validate_finite_decimal(unrealized_pnl, "unrealized_pnl")
        except ValueError:
            self.tripped = True
            return False
        self.realized_pnl = real_dec
        self.unrealized_pnl = unr_dec
        if self.drawdown_pct >= self.max_drawdown_pct:
            self.tripped = True
        return not self.tripped

    def allow_entry(self) -> bool:
        """Fail closed once tripped; never auto-resets intra-day."""
        return not self.tripped


@dataclass
class ProtectiveExitRateBudget:
    """Sliding 60-second order rate pacer preserving >= 10% strictly for protective exits."""

    max_rpm: int
    exit_reserve_fraction: Decimal = MIN_EXIT_RATE_RESERVE_FRACTION
    _timestamps: deque[float] = field(default_factory=deque, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_rpm <= 0:
            raise ValueError(f"max_rpm must be > 0, got {self.max_rpm}")
        self.exit_reserve_fraction = _validate_finite_decimal(
            self.exit_reserve_fraction, "exit_reserve_fraction"
        )
        if (
            self.exit_reserve_fraction < MIN_EXIT_RATE_RESERVE_FRACTION
            or self.exit_reserve_fraction > Decimal("1")
        ):
            raise ValueError(
                f"exit_reserve_fraction={self.exit_reserve_fraction} outside "
                f"[{MIN_EXIT_RATE_RESERVE_FRACTION}, 1.0]"
            )

    @property
    def exit_reserve_rpm(self) -> int:
        return max(
            1,
            int(
                (Decimal(self.max_rpm) * self.exit_reserve_fraction).to_integral_value(
                    rounding=ROUND_CEILING
                )
            ),
        )

    def _evict_expired(self, now_s: float) -> None:
        cutoff = now_s - 60.0
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

    def try_consume(self, now_s: float, *, is_exit: bool) -> bool:
        """Consume 1 API slot; entries are refused once only the exit reserve remains."""
        if (
            isinstance(now_s, bool)
            or not isinstance(now_s, (int, float))
            or not math.isfinite(now_s)
            or now_s < 0.0
        ):
            raise ValueError(f"now_s must be finite and >= 0, got {now_s!r}")
        now_num = now_s * 1.0 if isinstance(now_s, int) else now_s
        self._evict_expired(now_num)
        used = len(self._timestamps)
        limit = self.max_rpm if is_exit else (self.max_rpm - self.exit_reserve_rpm)
        if used >= limit:
            return False
        self._timestamps.append(now_num)
        return True

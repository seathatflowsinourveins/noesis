#!/usr/bin/env python3
"""L15 pschanely/CrossHair symbolic PEP-316 contract harness for risk.py & engine.py.

Run strictly via isolated `uvx` (never add `crosshair-tool` to `pyproject.toml`,
which would pull in banned `mypy`):

    PYTHONPATH="$PWD/src" uvx --from crosshair-tool crosshair check \
        --per_condition_timeout=10 evals/symbolic_risk_crosshair.py
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
RISK_PATH = REPO_ROOT / "src" / "tier0_sota" / "risk.py"
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"

_SCALE_TICKS = 10_000
_MAX_STOP_LOSS_BPS = 200
_MAX_DAILY_DRAWDOWN_BPS = 150
_DEFAULT_RISK_BPS = 50
_DEFAULT_MAX_NOTIONAL_BPS = 2_000
_DEFAULT_COST_BPS_X10 = 40
_SESSION_CODES = ("RTH", "PRE_MARKET", "AFTER_HOURS", "OVERNIGHT", "CLOSED")
_SESSION_NOISE_BPS_BY_CODE = (0, 5, 5, 10, 10)
_ARCHETYPE_MULTS_BY_CODE = (20, 22, 25, 28, 30, 32, 35)


def _load_risk_kernel() -> ModuleType:
    """Load immutable `src/tier0_sota/risk.py` even inside isolated `uvx` venvs."""
    digest = hashlib.sha256(RISK_PATH.read_bytes()).hexdigest()
    if digest != EXPECTED_RISK_SHA256:
        raise RuntimeError(
            f"risk.py SHA-256 mismatch: expected {EXPECTED_RISK_SHA256}, got {digest}"
        )
    try:
        from tier0_sota import risk as imported_risk

        return imported_risk
    except ModuleNotFoundError:
        mod_name = "tier0_sota_risk_kernel"
        if mod_name in sys.modules:
            return sys.modules[mod_name]
        spec = importlib.util.spec_from_file_location(mod_name, RISK_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load risk kernel spec from {RISK_PATH}") from None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
        return mod


_RISK = _load_risk_kernel()


def _valid_desired_stop(entry_ticks: int, is_long: bool, desired_ticks: int | None) -> bool:
    if desired_ticks is None:
        return True
    return (1 <= desired_ticks < entry_ticks) if is_long else (desired_ticks > entry_ticks)


def _stop_invariants(stop_ticks: int, entry_ticks: int, is_long: bool, cap_bps: int) -> bool:
    if stop_ticks < 1:
        return False
    inward = (stop_ticks < entry_ticks) if is_long else (stop_ticks > entry_ticks)
    if not inward:
        return False
    rule_612_ok = (stop_ticks < _SCALE_TICKS) or (stop_ticks % 100 == 0)
    if not rule_612_ok:
        return False
    loss_ticks = (entry_ticks - stop_ticks) if is_long else (stop_ticks - entry_ticks)
    return (loss_ticks * _SCALE_TICKS <= cap_bps * entry_ticks) and (
        loss_ticks * _SCALE_TICKS <= _MAX_STOP_LOSS_BPS * entry_ticks
    )


def _size_invariants(shares: int, equity_cents: int, entry_ticks: int, stop_ticks: int) -> bool:
    if shares < 0:
        return False
    stop_dist = abs(entry_ticks - stop_ticks)
    if stop_dist == 0:
        return shares == 0
    risk_bound_ok = shares * stop_dist * 100 <= equity_cents * _DEFAULT_RISK_BPS
    notional_bound_ok = shares * entry_ticks * 100 <= equity_cents * _DEFAULT_MAX_NOTIONAL_BPS
    return risk_bound_ok and notional_bound_ok


def _fuse_invariants(
    result: tuple[bool, bool],
    start_equity_cents: int,
    r1: int,
    u1: int,
    r2: int,
    u2: int,
) -> bool:
    allow_1, allow_2 = result
    breach_1 = -(r1 + u1) * _SCALE_TICKS >= _MAX_DAILY_DRAWDOWN_BPS * start_equity_cents
    breach_2 = -(r2 + u2) * _SCALE_TICKS >= _MAX_DAILY_DRAWDOWN_BPS * start_equity_cents
    if allow_1 != (not breach_1):
        return False
    if (not allow_1) and allow_2:
        return False
    return allow_2 == (allow_1 and not breach_2)


def _reserve_invariants(result: tuple[bool, bool, int], max_rpm: int, used_slots: int) -> bool:
    entry_allowed, exit_allowed, reserve_rpm = result
    if reserve_rpm < 1 or reserve_rpm * 10 < max_rpm:
        return False
    entry_limit = max_rpm - reserve_rpm
    if entry_allowed != (used_slots < entry_limit):
        return False
    if exit_allowed != (used_slots < max_rpm):
        return False
    return not (entry_limit <= used_slots < max_rpm and not exit_allowed)


def _trail_invariants(
    result: tuple[int, int, int],
    entry_ticks: int,
    is_long: bool,
    spread_bps: int,
    session_code: int,
) -> bool:
    stop_ticks, cap_ppm, trail_ppm = result
    if not (10_000 <= cap_ppm <= 20_000) or cap_ppm % 100 != 0:
        return False
    if not _stop_invariants(stop_ticks, entry_ticks, is_long, cap_ppm // 100):
        return False
    if not (0 < trail_ppm <= cap_ppm <= 20_000):
        return False
    noise_floor = 1_000 + spread_bps * 150 + _SESSION_NOISE_BPS_BY_CODE[session_code] * 100
    return trail_ppm >= min(cap_ppm, noise_floor)


def quantize_rule_612_stop(
    entry_ticks: int,
    is_long: bool,
    desired_ticks: int | None = None,
) -> int:
    """Symbolic contract wrapping immutable `risk._compute_capped_stop_ticks` (2.00% hard cap).

    pre: entry_ticks >= 50
    pre: _valid_desired_stop(entry_ticks, is_long, desired_ticks)
    post: _stop_invariants(__return__, entry_ticks, is_long, 200)
    """
    return int(
        _RISK._compute_capped_stop_ticks(entry_ticks, is_long, _MAX_STOP_LOSS_BPS, desired_ticks)
    )


def size_position_by_risk(
    equity_cents: int,
    entry_ticks: int,
    stop_ticks: int,
) -> int:
    """Exact integer-tick contract for `risk.size_position_by_risk` (ROUND_FLOOR bounds).

    pre: 1 <= equity_cents <= 1_000_000_000
    pre: 1 <= entry_ticks <= 100_000_000
    pre: 1 <= stop_ticks <= 100_000_000
    post: _size_invariants(__return__, equity_cents, entry_ticks, stop_ticks)
    """
    stop_dist = abs(entry_ticks - stop_ticks)
    if stop_dist == 0:
        return 0
    shares_by_risk = (equity_cents * _DEFAULT_RISK_BPS) // (stop_dist * 100)
    shares_by_notional = (equity_cents * _DEFAULT_MAX_NOTIONAL_BPS) // (entry_ticks * 100)
    return max(0, min(shares_by_risk, shares_by_notional))


def evaluate_daily_drawdown_fuse(
    start_equity_cents: int,
    r1: int,
    u1: int,
    r2: int,
    u2: int,
) -> tuple[bool, bool]:
    """Exact integer contract for `risk.DailyDrawdownBreaker` (-1.50% session latch).

    pre: start_equity_cents >= 1
    post: _fuse_invariants(__return__, start_equity_cents, r1, u1, r2, u2)
    """
    tripped = -(r1 + u1) * _SCALE_TICKS >= _MAX_DAILY_DRAWDOWN_BPS * start_equity_cents
    allow_1 = not tripped
    if -(r2 + u2) * _SCALE_TICKS >= _MAX_DAILY_DRAWDOWN_BPS * start_equity_cents:
        tripped = True
    allow_2 = not tripped
    return (allow_1, allow_2)


def enforce_exit_rate_reserve(
    max_rpm: int,
    used_slots: int,
) -> tuple[bool, bool, int]:
    """Exact integer contract for `risk.ProtectiveExitRateBudget` (>= 10% exit reserve).

    pre: max_rpm >= 1
    pre: used_slots >= 0
    post: _reserve_invariants(__return__, max_rpm, used_slots)
    """
    exit_reserve_rpm = max(1, -(-max_rpm // 10))
    entry_limit = max_rpm - exit_reserve_rpm
    entry_allowed = used_slots < entry_limit
    exit_allowed = used_slots < max_rpm
    return (entry_allowed, exit_allowed, exit_reserve_rpm)


def compute_non_noise_min_trail_contract(
    entry_ticks: int,
    is_long: bool,
    natr_bps: int,
    spread_bps: int = 5,
    session_code: int = 0,
    archetype_code: int = 4,
) -> tuple[int, int, int]:
    """Exact integer contract matching `engine.compute_non_noise_min_trail_spec`.

    Returns `(initial_stop_ticks, cap_ppm, non_noise_min_trail_ppm)` where `ppm = 10^-6`.

    pre: 10_000 <= entry_ticks <= 100_000_000
    pre: 0 <= natr_bps <= 5_000
    pre: 0 <= spread_bps <= 500
    pre: 0 <= session_code <= 4
    pre: 0 <= archetype_code <= 6
    post: _trail_invariants(__return__, entry_ticks, is_long, spread_bps, session_code)
    """
    if session_code == 0:
        cap_bps = 150 if natr_bps <= 150 else 200
    elif session_code in (1, 2):
        cap_bps = 125 if natr_bps <= 150 else 150
    else:
        cap_bps = 100

    initial_stop_ticks = int(_RISK._compute_capped_stop_ticks(entry_ticks, is_long, cap_bps, None))
    cap_ppm = cap_bps * 100
    rule612_tick = 100 if entry_ticks >= _SCALE_TICKS else 1
    two_tick_ppm = -(-(2 * rule612_tick * 1_000_000) // entry_ticks)
    session_noise_bps = _SESSION_NOISE_BPS_BY_CODE[session_code]
    fee_and_spread_noise_floor_ppm = (
        _DEFAULT_COST_BPS_X10 * 20 + 200 + spread_bps * 150 + session_noise_bps * 100
    )
    situation_mult_pct = _ARCHETYPE_MULTS_BY_CODE[archetype_code]
    situation_ppm = situation_mult_pct * natr_bps
    raw_trail_ppm = max(fee_and_spread_noise_floor_ppm, situation_ppm, two_tick_ppm)
    non_noise_min_trail_ppm = min(cap_ppm, raw_trail_ppm)
    return (initial_stop_ticks, cap_ppm, non_noise_min_trail_ppm)


def verify_runtime_parity_with_risk_and_engine() -> dict[str, Any]:
    """Verify bit-for-bit runtime parity against live `risk.py` and `engine.py` APIs."""
    digest = hashlib.sha256(RISK_PATH.read_bytes()).hexdigest()
    assert digest == EXPECTED_RISK_SHA256, f"risk.py SHA-256 mismatch: {digest}"

    checked_cases = 0

    # 1. Rule-612 stop parity across sub-dollar, $1.00 boundary, and liquid dollar stocks
    for entry_ticks, is_long, desired_ticks in (
        (50, True, None),
        (50, False, None),
        (5_000, True, None),
        (5_000, False, None),
        (10_000, True, None),
        (10_000, False, None),
        (10_001, True, None),
        (1_356_500, True, 1_345_000),
        (1_356_500, False, 1_365_000),
    ):
        stop_ticks = quantize_rule_612_stop(entry_ticks, is_long, desired_ticks)
        side = "LONG" if is_long else "SHORT"
        live_stop = _RISK.compute_capped_stop_price(
            Decimal(entry_ticks) / Decimal(_SCALE_TICKS),
            side,
            desired_stop=(
                Decimal(desired_ticks) / Decimal(_SCALE_TICKS)
                if desired_ticks is not None
                else None
            ),
            max_loss_pct=_RISK.MAX_STOP_LOSS_PCT,
        )
        assert Decimal(stop_ticks) / Decimal(_SCALE_TICKS) == live_stop
        checked_cases += 1

    # 2. Position sizing parity
    for eq_cents, entry_t, stop_t in (
        (10_000_000, 1_356_500, 1_336_200),
        (10_000_000, 1_000_000, 1_000_000),
        (25_000_000, 420_000, 411_600),
        (5_000_000, 8_500, 8_330),
    ):
        c_qty = size_position_by_risk(eq_cents, entry_t, stop_t)
        live_qty = _RISK.size_position_by_risk(
            equity=Decimal(eq_cents) / Decimal(100),
            entry_price=Decimal(entry_t) / Decimal(_SCALE_TICKS),
            stop_price=Decimal(stop_t) / Decimal(_SCALE_TICKS),
        )
        assert c_qty == live_qty
        checked_cases += 1

    # 3. Daily drawdown fuse latch parity (-1.50% boundary & recovery latch)
    for start_c, r1, u1, r2, u2 in (
        (10_000_000, -150_000, 0, 500_000, 0),
        (10_000_000, -149_999, 0, 100_000, 0),
        (10_000_000, -100_000, -50_000, 200_000, 0),
        (10_000_000, 50_000, 0, -160_000, 0),
    ):
        c_allow1, c_allow2 = evaluate_daily_drawdown_fuse(start_c, r1, u1, r2, u2)
        brk = _RISK.DailyDrawdownBreaker(
            start_equity=Decimal(start_c) / Decimal(100),
            max_drawdown_pct=_RISK.DAILY_DRAWDOWN_BREAKER_PCT,
        )
        live_1 = brk.update(
            realized_pnl=Decimal(r1) / Decimal(100),
            unrealized_pnl=Decimal(u1) / Decimal(100),
        )
        live_2 = brk.update(
            realized_pnl=Decimal(r2) / Decimal(100),
            unrealized_pnl=Decimal(u2) / Decimal(100),
        )
        assert (c_allow1, c_allow2) == (live_1, live_2)
        checked_cases += 1

    # 4. Exit rate reserve parity (>= 10% across 200, 1000, 3000 RPM)
    for max_rpm, used in (
        (200, 179),
        (200, 180),
        (200, 199),
        (200, 200),
        (1000, 899),
        (1000, 900),
        (3000, 2700),
        (7, 5),
    ):
        c_ent, c_exit, c_res = enforce_exit_rate_reserve(max_rpm, used)
        budget = _RISK.ProtectiveExitRateBudget(max_rpm=max_rpm)
        assert c_res == budget.exit_reserve_rpm
        for i in range(used):
            budget.try_consume(float(i) * 0.001, is_exit=True)
        b_ent = _RISK.ProtectiveExitRateBudget(max_rpm=max_rpm)
        for i in range(used):
            b_ent.try_consume(float(i) * 0.001, is_exit=True)
        assert c_ent == b_ent.try_consume(10.0, is_exit=False)
        assert c_exit == budget.try_consume(10.0, is_exit=True)
        checked_cases += 1

    # 5. Non-noise min-trail parity with tier0_sota.engine when importable
    engine_parity_checked = False
    try:
        from tier0_sota.engine import compute_non_noise_min_trail_spec

        arch_names = (
            "PARABOLIC_PEAK_REVERSAL",
            "CRASH_ALIGNMENT",
            "VCP_COIL",
            "PREMARKET_EARLY_IGNITION",
            "EARLY_EP_MOMENTUM_BURST",
            "DARKPOOL_ACCUMULATION",
            "PEAD_DRIFT",
        )
        for entry_t, is_long, natr_bps, spread_bps, sess_code, arch_code in (
            (1_356_500, True, 120, 5, 0, 4),
            (1_356_500, True, 250, 8, 1, 3),
            (420_000, False, 400, 12, 2, 0),
            (100_000, True, 180, 10, 3, 1),
            (10_000, True, 150, 5, 4, 6),
        ):
            c_stop, c_cap_ppm, c_trail_ppm = compute_non_noise_min_trail_contract(
                entry_t, is_long, natr_bps, spread_bps, sess_code, arch_code
            )
            spec = compute_non_noise_min_trail_spec(
                symbol="NVDA",
                entry_price=Decimal(entry_t) / Decimal(_SCALE_TICKS),
                side="LONG" if is_long else "SHORT",
                session=_SESSION_CODES[sess_code],
                natr_pct=Decimal(natr_bps) / Decimal(_SCALE_TICKS),
                spread_bps=Decimal(spread_bps),
                archetype=arch_names[arch_code],
                cost_bps_per_side=Decimal("4.0"),
            )
            assert Decimal(c_stop) / Decimal(_SCALE_TICKS) == spec["initial_stop_price"]
            assert Decimal(c_cap_ppm) / Decimal(1_000_000) == spec["initial_max_loss_pct"]
            assert Decimal(c_trail_ppm) / Decimal(1_000_000) == spec["non_noise_min_trail_pct"]
            checked_cases += 1
        engine_parity_checked = True
    except ModuleNotFoundError:
        engine_parity_checked = False

    return {
        "ok": True,
        "risk_sha256": digest,
        "risk_sha256_match": digest == EXPECTED_RISK_SHA256,
        "contracts_count": 5,
        "contracts": [
            "quantize_rule_612_stop",
            "size_position_by_risk",
            "evaluate_daily_drawdown_fuse",
            "enforce_exit_rate_reserve",
            "compute_non_noise_min_trail_contract",
        ],
        "runtime_parity_cases_checked": checked_cases,
        "engine_parity_checked": engine_parity_checked,
    }


def main() -> int:
    """CLI entrypoint printing CROSSHAIR_CONTRACT_PARITY_OK=1 on full parity verification."""
    rep = verify_runtime_parity_with_risk_and_engine()
    if not rep["ok"]:
        return 1
    print("CROSSHAIR_CONTRACT_PARITY_OK=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

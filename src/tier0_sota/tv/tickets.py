"""Half-Auto extreme-mover order ticket & TradingView watchlist generator."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tier0_sota import engine as _eng
from tier0_sota import risk as _risk
from tier0_sota.tv.bridge import DEFAULT_MANIFEST_DIR, _alert_validator, refuse_live_endpoints

NY_TZ = ZoneInfo("America/New_York")
LOW_EXTREME_HOLD_REASONS = frozenset({"LOW_MOVE_NOT_EXTREME", "LOW_RVOL_NOT_EXTREME"})
INVERSE_HEDGE_SYMBOLS = frozenset(
    {"SOXS", "SQQQ", "SPXS", "SPXU", "SDOW", "QID", "SH", "TZA", "SDS"}
)
ADMITTED_FEED_SOURCES = frozenset(
    {
        "alpaca_screener",
        "alpaca_sip",
        "alpaca_boats",
        "alpaca_overnight",
        "tradingview_staged_presets",
        "finra_trf_exchange_d",
        "nautilus_trader",
    }
)
DEFAULT_EXCHANGES: dict[str, str] = {
    "SPY": "AMEX",
    "QQQ": "NASDAQ",
    "IWM": "AMEX",
    "RSP": "AMEX",
    "SOXX": "NASDAQ",
    "VIX": "CBOE",
    "TLT": "NASDAQ",
    "GLD": "AMEX",
    "SOXS": "AMEX",
    "SOXL": "AMEX",
    "SQQQ": "NASDAQ",
    "TQQQ": "NASDAQ",
    "SPXS": "AMEX",
    "UPRO": "AMEX",
    "SDOW": "AMEX",
    "VXX": "BATS",
}


def current_et_session(now_et: datetime) -> str:
    """Classify ET wall-clock datetime into 24/5 session label."""
    mins = now_et.hour * 60 + now_et.minute
    if 570 <= mins < 960:
        return "RTH"
    if 240 <= mins < 570:
        return "PRE_MARKET"
    if 960 <= mins < 1200:
        return "AFTER_HOURS"
    return "OVERNIGHT"


def filter_extreme_movers(
    entries: Sequence[Mapping[str, Any]], *, regime_int: int
) -> list[dict[str, Any]]:
    """Filter strictly to extreme movers (plus inverse crash hedges when regime == -1)."""
    selected: list[dict[str, Any]] = []
    for raw in entries:
        sym = str(raw.get("symbol") or "").strip().upper()
        px_val = raw.get("last_price")
        if not sym or px_val is None or float(px_val) <= 0.0:
            continue
        hold = str(raw.get("extreme_mover_hold_reason") or "").strip()
        is_crash_hedge = regime_int == -1 and sym in ("SOXS", "SQQQ")
        if hold in LOW_EXTREME_HOLD_REASONS and not is_crash_hedge:
            continue
        selected.append(dict(raw))
    return selected


def _resolve_direction(
    entry: Mapping[str, Any], *, regime_int: int, sess_chg: float, rvol: float
) -> tuple[str, _risk.Side, str]:
    sym = str(entry.get("symbol") or "").strip().upper()
    if sym in INVERSE_HEDGE_SYMBOLS and regime_int == -1:
        return "BUY", "LONG", "CRASH_ALIGNMENT"
    if regime_int == -1 and sess_chg < 0.0:
        return "SELL", "SHORT", "CRASH_ALIGNMENT"
    if sess_chg >= 2.0 and rvol >= 3.0:
        return "BUY", "LONG", "EARLY_EP_MOMENTUM_BURST"
    if regime_int == -1:
        return "SELL", "SHORT", "CRASH_ALIGNMENT"
    return "BUY", "LONG", "EXTREME_REVERSAL"


def build_half_auto_tickets(
    movers: Mapping[str, Any],
    *,
    bundle: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
    venue: str = "IBKR",
    max_tickets: int = 15,
) -> dict[str, Any]:
    """Build schema-validated Half-Auto extreme-mover tickets with Rule-612 lock rungs."""
    ts_utc = now_utc or datetime.now(UTC)
    now_et = ts_utc.astimezone(NY_TZ)
    active_sess = current_et_session(now_et)
    ts_ms = int(ts_utc.timestamp() * 1000)

    macro = dict(movers.get("macro_regime") or {})
    regime_int = int(macro.get("regime", 0))
    b_map = {
        str(x["symbol"]).upper(): x
        for x in (bundle or {}).get("top_potential_symbols", [])
        if isinstance(x, Mapping) and x.get("symbol")
    }

    extreme = filter_extreme_movers(movers.get("entries", []), regime_int=regime_int)
    tickets: list[dict[str, Any]] = []
    schema_ok = 0

    for rank, e in enumerate(extreme[:max_tickets], 1):
        sym = str(e["symbol"]).strip().upper()
        px = Decimal(str(round(float(e["last_price"]), 4)))
        natr = max(
            Decimal("0.001"),
            min(Decimal("0.50"), Decimal(str(round(float(e.get("natr_pct") or 0.015), 6)))),
        )
        sched_name, sess_map = _eng.select_pooled_trailing_schedule(natr)
        prof_now = _eng.get_session_protection_profile(active_sess, sess_map)
        prof_rth = _eng.get_session_protection_profile("RTH", sess_map)

        sess_chg = float(e.get("session_move_pct") or 0.0)
        rvol = float(e.get("rvol_normalized") or 1.0)
        side, r_side, arch = _resolve_direction(
            e, regime_int=regime_int, sess_chg=sess_chg, rvol=rvol
        )

        stop_now = _risk.compute_capped_stop_price(
            px, r_side, max_loss_pct=prof_now.initial_max_loss_pct
        )
        stop_rth = _risk.compute_capped_stop_price(
            px, r_side, max_loss_pct=prof_rth.initial_max_loss_pct
        )
        dist_now = float(abs(px - stop_now) / px)
        be_raw = px * (Decimal("1.0010") if side == "BUY" else Decimal("0.9990"))
        be_floor = _risk.quantize_rule_612(be_raw, side=r_side, is_stop=True)
        risk_r = abs(px - stop_now)
        sign = Decimal("1") if side == "BUY" else Decimal("-1")
        t1r = _risk.quantize_rule_612(px + sign * risk_r, side=r_side, is_stop=False)
        t2r = _risk.quantize_rule_612(px + sign * Decimal("2") * risk_r, side=r_side, is_stop=False)
        t3r = _risk.quantize_rule_612(px + sign * Decimal("3") * risk_r, side=r_side, is_stop=False)
        lock_at_1r = _risk.quantize_rule_612(
            px + sign * Decimal("0.45") * risk_r, side=r_side, is_stop=True
        )
        lock_at_2r = _risk.quantize_rule_612(
            px + sign * Decimal("1.35") * risk_r, side=r_side, is_stop=True
        )
        fast_sell_rev = _risk.quantize_rule_612(
            px * (Decimal("0.9960") if side == "BUY" else Decimal("1.0040")),
            side=r_side,
            is_stop=True,
        )

        risk_pct = Decimal("0.0025") if regime_int == -1 else Decimal("0.0050")
        sh_25k = _risk.size_position_by_risk(
            equity=Decimal("25000"),
            entry_price=px,
            stop_price=stop_now,
            risk_per_trade_pct=risk_pct,
            max_notional_pct=Decimal("0.15"),
        )
        sh_100k = _risk.size_position_by_risk(
            equity=Decimal("100000"),
            entry_price=px,
            stop_price=stop_now,
            risk_per_trade_pct=risk_pct,
            max_notional_pct=Decimal("0.15"),
        )

        b_row = b_map.get(sym, {})
        shortable = bool(
            e.get("shortable") if e.get("shortable") is not None else b_row.get("shortable", False)
        )
        etb = bool(
            e.get("easy_to_borrow")
            if e.get("easy_to_borrow") is not None
            else b_row.get("easy_to_borrow", False)
        )
        rev_short_mode = (
            "DIRECT_SHORT_ETB" if (shortable and etb) else "INVERSE_HEDGE_SOXS_SQQQ"
        )
        if sym in INVERSE_HEDGE_SYMBOLS and regime_int == -1:
            gate = "READY_HALF_AUTO_HEDGE"
        elif side == "SELL" and not (shortable and etb):
            gate = "USE_INVERSE_ETF_OR_VERIFY_BORROW"
        elif b_row.get("entry_eligible") is True:
            gate = "READY_HALF_AUTO"
        elif b_row.get("entry_hold_reason"):
            gate = f"HOLD ({b_row['entry_hold_reason']})"
        else:
            gate = "MONITOR_CONFIRM_BAR"

        collar_bps = float(prof_now.limit_collar_pct * Decimal("10000"))
        alert_in: dict[str, Any] = {
            "schema_version": "tier0.tv-alert.v1",
            "symbol": sym,
            "ts_utc_ms": ts_ms,
            "bar_confirmed": True,
            "side": side,
            "qty": 1,
            "venue": venue,
            "session": active_sess,
            "order_type": prof_now.order_mode,
            "trigger_price": float(px),
            "stop_price": float(stop_now),
            "stop_distance_pct": round(dist_now, 6),
            "limit_collar_bps": collar_bps,
            "trailing_schedule": sched_name,
            "natr_pct": float(natr),
            "auto_reverse": True,
            "auto_reentry": True,
            "reentry_cooldown_secs": 60,
            "strategy_id": "SignalExecutionStrategy",
            "semi_auto_confirmed": False,
            "setup_archetype": arch,
            "macro_regime": regime_int,
            "rvol": round(rvol, 2),
            "is_leveraged_or_inverse": sym in ("SOXL", "SOXS", "TQQQ", "SQQQ", "SPXS", "UPRO"),
            "data_source": "alpaca_screener",
        }
        if side == "SELL":
            alert_in["shortable"] = shortable
            alert_in["easy_to_borrow"] = etb
        refuse_live_endpoints(alert_in)
        for error in _alert_validator().iter_errors(alert_in):
            raise ValueError(f"Invalid ticket preset schema: {error.message}")
        schema_ok += 1

        tickets.append(
            {
                "rank": rank,
                "symbol": sym,
                "side": side,
                "archetype": arch,
                "action_gate": gate,
                "live_price": float(px),
                "session_move_pct": sess_chg,
                "rvol": round(rvol, 2),
                "natr_pct": float(natr),
                "schedule": sched_name,
                "active_session": active_sess,
                "order_type_now": prof_now.order_mode,
                "collar_now_bps": int(collar_bps),
                "stop_now": float(stop_now),
                "stop_now_pct": round(dist_now * 100.0, 2),
                "stop_rth_0930": float(stop_rth),
                "be_plus_fee_floor": float(be_floor),
                "target_1r": float(t1r),
                "target_2r": float(t2r),
                "target_3r": float(t3r),
                "lock_at_1r": float(lock_at_1r),
                "lock_at_2r": float(lock_at_2r),
                "fast_sell_rev_trigger": float(fast_sell_rev),
                "reentry_cooldown_secs": 60,
                "reverse_short_mode": rev_short_mode,
                "shares_25k_defensive": sh_25k,
                "shares_100k_defensive": sh_100k,
                "shortable": shortable,
                "easy_to_borrow": etb,
                "tv_alert_preset": alert_in,
            }
        )

    raw_feeds = [str(x) for x in (movers.get("feed_sources") or []) if x]
    admitted = [f for f in raw_feeds if f in ADMITTED_FEED_SOURCES]
    return {
        "schema_version": "tier0.live-half-auto-tickets.v1",
        "generated_at_et": now_et.isoformat(),
        "active_session": active_sess,
        "movers_generated_at_utc": movers.get("generated_at_utc"),
        "macro_regime": macro,
        "feed_sources": admitted,
        "upstream_unadmitted_feed_count": len(raw_feeds) - len(admitted),
        "charter_guards": {
            "half_automation_ungated": True,
            "live_execution_enabled": False,
            "semi_auto_confirmed": False,
            "qty": 1,
        },
        "schema_validated_count": schema_ok,
        "tickets": tickets,
    }


def render_tickets_markdown(doc: Mapping[str, Any]) -> str:
    """Render concise operator Markdown order ticket table."""
    macro = doc.get("macro_regime") or {}
    sess = doc.get("active_session", "RTH")
    rows = [
        f"| {t['rank']} | `{t['symbol']}` | `{t['side']}` | **{t['action_gate']}** "
        f"| `{t['live_price']:.2f}` ({t['session_move_pct']:+.2f}%) | `{t['rvol']:.1f}x` "
        f"| `{t['stop_now']:.2f}` ({t['stop_now_pct']:.2f}%) | `{t['be_plus_fee_floor']:.2f}` "
        f"| `{t['lock_at_1r']:.2f}` / `{t['lock_at_2r']:.2f}` "
        f"| `{t['fast_sell_rev_trigger']:.2f}` ({t['reverse_short_mode']}) "
        f"| `{t['shares_25k_defensive']}` / `{t['shares_100k_defensive']}` "
        f"| `{t['order_type_now']}` ({t['collar_now_bps']}bp) |"
        for t in doc.get("tickets", [])
    ]
    body = "\n".join(rows) or "| — | — | — | No extreme movers | — | — | — | — | — | — | — | — |"
    schema_cnt = doc.get("schema_validated_count")
    ticket_cnt = len(doc.get("tickets", []))
    return (
        f"# Tier-0 LIVE Half-Automation Order Ticket Console\n\n"
        f"- **Generated (ET):** `{doc.get('generated_at_et')}` | **Session:** `{sess}` "
        f"| **Regime:** `{macro.get('regime_label', 'NEUTRAL')} ({macro.get('regime', 0)})`\n"
        f"- **Schema Validated:** `{schema_cnt}/{ticket_cnt}` "
        f"| `semi_auto_confirmed=false` | `live_execution_enabled=false`\n\n"
        f"| # | Sym | Side | Action Gate | Live Px (Sess) | RVOL | Stop ({sess}) "
        f"| BE+10bp | Lock +1R / +2R | Fast-Sell Rev | Sh ($25k/$100k) | Order |\n"
        f"|---:|---|---|---|---:|---:|---:|---:|---:|---|---:|---|\n"
        f"{body}\n"
    )


def render_tv_watchlist(
    tickets: Sequence[Mapping[str, Any]], *, exchange_map: Mapping[str, str] | None = None
) -> str:
    """Render grouped TradingView watchlist text for extreme movers + regime/hedges."""
    exch = {**DEFAULT_EXCHANGES, **{k.upper(): v.upper() for k, v in (exchange_map or {}).items()}}
    movers: list[str] = []
    seen: set[str] = set()
    for t in tickets:
        sym = str(t.get("symbol", "")).strip().upper()
        if sym and sym not in seen:
            seen.add(sym)
            movers.append(f"{exch[sym]}:{sym}" if sym in exch else sym)
    lines = [
        "###01_MACRO_REGIME_BREADTH",
        "AMEX:SPY",
        "NASDAQ:QQQ",
        "AMEX:IWM",
        "AMEX:RSP",
        "NASDAQ:SOXX",
        "CBOE:VIX",
        "NASDAQ:TLT",
        "###02_CRASH_DEFENSIVE_HEDGES",
        "AMEX:SOXS",
        "NASDAQ:SQQQ",
        "AMEX:SPXS",
        "AMEX:SDOW",
        "###03_LIVE_MOVERS_TOP15",
        *movers,
        "###04_BIDIRECTIONAL_PAIRS",
        "AMEX:SOXL",
        "AMEX:SOXS",
        "NASDAQ:TQQQ",
        "NASDAQ:SQQQ",
    ]
    return "\n".join(lines) + "\n"


def _atomic_write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)
    return path


def refresh_tickets_from_paths(
    *,
    movers_path: Path,
    out_dir: Path,
    bundle_path: Path | None = None,
    venue: str = "IBKR",
) -> dict[str, Any]:
    """Load local mover/bundle manifests and atomically write JSON, Markdown, and watchlist."""
    movers = json.loads(movers_path.read_text(encoding="utf-8"))
    bundle = (
        json.loads(bundle_path.read_text(encoding="utf-8"))
        if bundle_path is not None and bundle_path.is_file()
        else None
    )
    doc = build_half_auto_tickets(movers, bundle=bundle, venue=venue)
    _atomic_write_text(out_dir / "live_half_auto_tickets.json", json.dumps(doc, indent=2) + "\n")
    _atomic_write_text(out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md", render_tickets_markdown(doc))
    _atomic_write_text(
        out_dir / "tier0_sota_tv_watchlists_live.txt", render_tv_watchlist(doc["tickets"])
    )
    return doc


def main() -> None:
    """CLI entrypoint for Half-Auto order ticket & TradingView watchlist generation."""
    parser = argparse.ArgumentParser(description="Tier-0 TV Half-Auto ticket generator")
    parser.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_MANIFEST_DIR / "tv_sota_pack")
    parser.add_argument("--venue", default="IBKR")
    args = parser.parse_args()
    doc = refresh_tickets_from_paths(
        movers_path=args.manifest_dir / "live_whole_market_movers_manifest.json",
        bundle_path=args.manifest_dir / "live_lane_order_preset_bundle.json",
        out_dir=args.out_dir,
        venue=args.venue,
    )
    print(
        f"HALF_AUTO_REFRESH_OK=1 SESSION={doc['active_session']} "
        f"TICKETS={len(doc['tickets'])} SCHEMA_OK={doc['schema_validated_count']}"
    )


if __name__ == "__main__":
    main()

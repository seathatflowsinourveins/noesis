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

from pydantic import ValidationError

from tier0_sota import engine as _eng
from tier0_sota import risk as _risk
from tier0_sota.tv.bridge import (
    DEFAULT_MANIFEST_DIR,
    TvAlertV1,
    persist_tv_preset_duckdb,
    refuse_live_endpoints,
    validate_tv_alert_payload,
)

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
    """Classify ET wall-clock datetime into 24/5 session label via engine SSOT."""
    return _eng.classify_et_hm(now_et.hour * 60 + now_et.minute)


def filter_extreme_movers(
    entries: Sequence[Mapping[str, Any]], *, regime_int: int
) -> list[dict[str, Any]]:
    """Filter strictly to extreme movers (plus inverse crash hedges when regime == -1)."""
    out: list[dict[str, Any]] = []
    for raw in entries:
        sym, px = str(raw.get("symbol") or "").strip().upper(), raw.get("last_price")
        if not sym or px is None or float(px) <= 0.0:
            continue
        hold = str(raw.get("extreme_mover_hold_reason") or "").strip()
        if hold in LOW_EXTREME_HOLD_REASONS and not (regime_int == -1 and sym in ("SOXS", "SQQQ")):
            continue
        out.append(dict(raw))
    return out


def _resolve_direction(
    entry: Mapping[str, Any], *, regime_int: int, sess_chg: float, rvol: float
) -> tuple[str, _risk.Side, str]:
    sym = str(entry.get("symbol") or "").strip().upper()
    if sym in INVERSE_HEDGE_SYMBOLS and regime_int == -1:
        return "BUY", "LONG", "CRASH_ALIGNMENT"
    if regime_int == -1:
        return "SELL", "SHORT", "CRASH_ALIGNMENT"
    return (
        "BUY",
        "LONG",
        "EARLY_EP_MOMENTUM_BURST" if sess_chg >= 2.0 and rvol >= 3.0 else "EXTREME_REVERSAL",
    )


def _action_gate(
    sym: str, side: str, regime_int: int, etb_ok: bool, b_row: Mapping[str, Any]
) -> str:
    if sym in INVERSE_HEDGE_SYMBOLS and regime_int == -1:
        return "READY_HALF_AUTO_HEDGE"
    if side == "SELL" and not etb_ok:
        return "USE_INVERSE_ETF_OR_VERIFY_BORROW"
    if b_row.get("entry_eligible") is True:
        return "READY_HALF_AUTO"
    return (
        f"HOLD ({b_row['entry_hold_reason']})"
        if b_row.get("entry_hold_reason")
        else "MONITOR_CONFIRM_BAR"
    )


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
    active_sess, ts_ms = current_et_session(now_et), int(ts_utc.timestamp() * 1000)
    live_scan = movers.get("schema_version") == "tier0.whole-market-movers.v2"
    if live_scan:
        active_sess = _eng.classify_us_equity_session_et(ts_ms * 1_000_000)
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
        bar_ms = ts_ms
        if live_scan:
            if active_sess == "CLOSED" or not e.get("bar_close_utc"):
                continue
            close = datetime.fromisoformat(e["bar_close_utc"])
            if close.tzinfo is None:
                continue
            bar_ms = int(close.timestamp() * 1000)
            if (
                not 0 <= ts_ms - bar_ms < 60_000
                or _eng.classify_us_equity_session_et((bar_ms - 60_000) * 1_000_000) != active_sess
                or e.get("extreme_mover_eligible") is not True
            ):
                continue
        sym = str(e["symbol"]).strip().upper()
        px = Decimal(str(round(float(e["last_price"]), 4)))
        natr = max(
            Decimal("0.001"),
            min(Decimal("0.50"), Decimal(str(round(float(e.get("natr_pct") or 0.015), 6)))),
        )
        sched_name, sess_map = _eng.select_pooled_trailing_schedule(natr)
        prof_now = _eng.get_session_protection_profile(active_sess, sess_map)
        prof_rth = _eng.get_session_protection_profile("RTH", sess_map)
        sess_chg, rvol = (
            float(e.get("session_move_pct") or 0.0),
            float(e.get("rvol_normalized") or 1.0),
        )
        side, r_side, arch = _resolve_direction(
            e, regime_int=regime_int, sess_chg=sess_chg, rvol=rvol
        )

        stop_now = _risk.compute_capped_stop_price(
            px, r_side, max_loss_pct=prof_now.initial_max_loss_pct
        )
        stop_rth = _risk.compute_capped_stop_price(
            px, r_side, max_loss_pct=prof_rth.initial_max_loss_pct
        )
        dist_now, risk_r = float(abs(px - stop_now) / px), abs(px - stop_now)
        sign = Decimal("1") if side == "BUY" else Decimal("-1")

        def _quant(
            price: Decimal, *, is_stop: bool = True, route_side: _risk.Side = r_side
        ) -> Decimal:
            return _risk.quantize_rule_612(price, side=route_side, is_stop=is_stop)

        be_floor = _quant(px * (Decimal("1.0010") if side == "BUY" else Decimal("0.9990")))
        t1r, t2r, t3r = (
            _quant(px + sign * Decimal(k) * risk_r, is_stop=False) for k in ("1", "2", "3")
        )
        lock_at_1r = _quant(px + sign * Decimal("0.45") * risk_r)
        lock_at_2r = _quant(px + sign * Decimal("1.35") * risk_r)
        fast_sell_rev = _quant(px * (Decimal("0.9960") if side == "BUY" else Decimal("1.0040")))

        risk_pct = Decimal("0.0025") if regime_int == -1 else Decimal("0.0050")
        sh_25k, sh_100k = (
            _risk.size_position_by_risk(
                equity=Decimal(eq),
                entry_price=px,
                stop_price=stop_now,
                risk_per_trade_pct=risk_pct,
                max_notional_pct=Decimal("0.15"),
            )
            for eq in ("25000", "100000")
        )
        b_row = b_map.get(sym, {})
        shortable = bool(
            e["shortable"] if e.get("shortable") is not None else b_row.get("shortable", False)
        )
        etb = bool(
            e["easy_to_borrow"]
            if e.get("easy_to_borrow") is not None
            else b_row.get("easy_to_borrow", False)
        )
        etb_ok = shortable and etb
        rev_short_mode = "DIRECT_SHORT_ETB" if etb_ok else "INVERSE_HEDGE_SOXS_SQQQ"
        gate = _action_gate(sym, side, regime_int, etb_ok, b_row)
        collar_bps = float(prof_now.limit_collar_pct * Decimal("10000"))

        alert_in: dict[str, Any] = {
            "schema_version": "tier0.tv-alert.v1",
            "symbol": sym,
            "ts_utc_ms": bar_ms,
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
            "data_source": (
                "alpaca_boats"
                if "alpaca_boats" in movers.get("feed_sources", [])
                else "alpaca_sip"
                if live_scan
                else "alpaca_screener"
            ),
        }
        if side == "SELL":
            alert_in |= {"shortable": shortable, "easy_to_borrow": etb}
        refuse_live_endpoints(alert_in)
        try:
            TvAlertV1.model_validate(alert_in)
        except ValidationError as exc:
            raise ValueError(f"Invalid ticket preset schema: {exc}") from exc
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
    macro, sess = doc.get("macro_regime") or {}, doc.get("active_session", "RTH")
    rows = [
        (
            f"| {t['rank']} | `{t['symbol']}` | `{t['side']}` | **{t['action_gate']}** "
            f"| `{t['live_price']:.2f}` ({t['session_move_pct']:+.2f}%) | `{t['rvol']:.1f}x` "
            f"| `{t['stop_now']:.2f}` ({t['stop_now_pct']:.2f}%) | `{t['be_plus_fee_floor']:.2f}` "
            f"| `{t['lock_at_1r']:.2f}` / `{t['lock_at_2r']:.2f}` "
            f"| `{t['fast_sell_rev_trigger']:.2f}` ({t['reverse_short_mode']}) "
            f"| `{t['shares_25k_defensive']}` / `{t['shares_100k_defensive']}` "
            f"| `{t['order_type_now']}` ({t['collar_now_bps']}bp) |"
        )
        for t in doc.get("tickets", [])
    ]
    body = "\n".join(rows) or "| — | — | — | No extreme movers | — | — | — | — | — | — | — | — |"
    regime_label = macro.get("regime_label", "NEUTRAL")
    regime_val = macro.get("regime", 0)
    n_tickets = len(doc.get("tickets", []))
    return (
        "# Tier-0 LIVE Half-Automation Order Ticket Console\n\n"
        f"- **Generated (ET):** `{doc.get('generated_at_et')}` | **Session:** `{sess}` "
        f"| **Regime:** `{regime_label} ({regime_val})`\n"
        f"- **Schema Validated:** `{doc.get('schema_validated_count')}/{n_tickets}` "
        "| `semi_auto_confirmed=false` | `live_execution_enabled=false`\n\n"
        f"| # | Sym | Side | Action Gate | Live Px (Sess) | RVOL | Stop ({sess}) "
        "| BE+10bp | Lock +1R / +2R | Fast-Sell Rev | Sh ($25k/$100k) | Order |\n"
        f"|---:|---|---|---|---:|---:|---:|---:|---:|---|---:|---|\n{body}\n"
    )


def render_tv_watchlist(
    tickets: Sequence[Mapping[str, Any]], *, exchange_map: Mapping[str, str] | None = None
) -> str:
    """Render grouped TradingView watchlist text for extreme movers + regime/hedges."""
    exch = {**DEFAULT_EXCHANGES, **{k.upper(): v.upper() for k, v in (exchange_map or {}).items()}}
    movers = list(
        dict.fromkeys(
            f"{exch[s]}:{s}" if s in exch else s
            for t in tickets
            if (s := str(t.get("symbol", "")).strip().upper())
        )
    )
    return (
        "\n".join(
            [
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
        )
        + "\n"
    )


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
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Load local mover/bundle manifests and atomically write JSON, Markdown, and watchlist."""
    movers = json.loads(movers_path.read_text(encoding="utf-8"))
    bundle = (
        json.loads(bundle_path.read_text(encoding="utf-8"))
        if bundle_path is not None and bundle_path.is_file()
        else None
    )
    doc = build_half_auto_tickets(movers, bundle=bundle, venue=venue, now_utc=now_utc)
    if movers.get("schema_version") == "tier0.whole-market-movers.v2":
        # Replace only the scanner-owned snapshot; manual/Pine intake has its own history.
        presets = [
            validate_tv_alert_payload(t["tv_alert_preset"], watchlist_manifest=movers, venue=venue)
            for t in doc["tickets"]
            if t["side"] != "SELL"
            or (
                t["easy_to_borrow"]
                and t["shortable"]
                and (venue != "ALPACA" or t["active_session"] != "OVERNIGHT")
            )
        ]
        doc["staged_count"] = len(presets)
        _atomic_write_text(
            out_dir / "staged_scan_presets.json",
            json.dumps(
                {
                    "schema_version": "tier0.staged-tv-presets.v1",
                    "scan_generated_at_utc": movers["generated_at_utc"],
                    "semi_auto_confirmed": False,
                    "presets": presets,
                },
                indent=2,
            )
            + "\n",
        )
        for preset in presets:
            persist_tv_preset_duckdb(
                out_dir / "tv_half_auto_audit.duckdb", preset, source="scanner_tickets"
            )
    _atomic_write_text(out_dir / "live_half_auto_tickets.json", json.dumps(doc, indent=2) + "\n")
    _atomic_write_text(out_dir / "LIVE_HALF_AUTO_ORDER_TICKETS.md", render_tickets_markdown(doc))
    _atomic_write_text(
        out_dir / "tier0_sota_tv_watchlists_live.txt", render_tv_watchlist(doc["tickets"])
    )
    return doc


def main() -> None:
    """CLI entrypoint for Half-Auto order ticket & TradingView watchlist generation."""
    p = argparse.ArgumentParser(description="Tier-0 TV Half-Auto ticket generator")
    p.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_MANIFEST_DIR / "tv_sota_pack")
    p.add_argument("--venue", default="IBKR")
    a = p.parse_args()
    doc = refresh_tickets_from_paths(
        movers_path=a.manifest_dir / "live_whole_market_movers_manifest.json",
        bundle_path=a.manifest_dir / "live_lane_order_preset_bundle.json",
        out_dir=a.out_dir,
        venue=a.venue,
    )
    print(
        f"HALF_AUTO_REFRESH_OK=1 SESSION={doc['active_session']} "
        f"TICKETS={len(doc['tickets'])} SCHEMA_OK={doc['schema_validated_count']}"
    )


if __name__ == "__main__":
    main()

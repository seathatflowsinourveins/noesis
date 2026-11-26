"""Fail-closed TradingView Pine v6 Draft 2020-12 alert bridge (semi_auto_confirmed=False)."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from jsonschema import Draft202012Validator, validators
from jsonschema.protocols import Validator

from tier0_sota import engine as _eng
from tier0_sota import risk as _risk

DEFAULT_MANIFEST_DIR = (
    Path("Z:/sota-data/manifests") if os.name == "nt" else Path("/mnt/z/sota-data/manifests")
)
DEFAULT_STAGED_PATH = DEFAULT_MANIFEST_DIR / "staged_tv_presets.json"
DEFAULT_WATCHLIST_PATH = DEFAULT_MANIFEST_DIR / "live_watchlist_manifest.json"
FORBIDDEN_ENDPOINT_MARKERS = ("4001", "7496", "8798", "8796")
_LIVE_ALPACA_RE = re.compile(r"(?<![a-z0-9-])api\.alpaca\.markets(?![a-z0-9-])")
_ALLOWED_SESSIONS = frozenset({"PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT"})
_ALLOWED_VENUES = frozenset({"IBKR", "ALPACA"})
MAX_INPUT_AGE_MS = 15 * 60_000


@cache
def _alert_validator() -> Validator:
    schema_path = Path(__file__).with_name("schema_v1.json")
    checker = Draft202012Validator.TYPE_CHECKER
    integer = checker.redefine(
        "integer",
        lambda _, v: (
            checker.is_type(v, "integer")
            or (isinstance(v, Decimal) and v.is_finite() and v == v.to_integral_value())
        ),
    )
    return validators.extend(Draft202012Validator, type_checker=integer)(
        json.loads(schema_path.read_text(encoding="utf-8"))
    )


def refuse_live_endpoints(payload: Mapping[str, Any]) -> None:
    """Reject any live broker host or forbidden live port marker in alert payloads."""
    raw = json.dumps(dict(payload), default=str).lower()
    has_live_alpaca = "alpaca" in raw and "paper-api" not in raw and "markets" in raw
    if (
        has_live_alpaca
        or _LIVE_ALPACA_RE.search(raw)
        or any(marker in raw for marker in FORBIDDEN_ENDPOINT_MARKERS)
    ):
        raise ValueError("Refused live broker endpoint or port marker in alert payload")


def _parse_pos_decimal(value: Any, name: str, exact: bool) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must not be bool")
    try:
        dec = Decimal(str(value if exact else float(value)))
    except (InvalidOperation, ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"Invalid {name}: {value!r}") from exc
    if not dec.is_finite() or not Decimal("0") < dec < Decimal("1e9") or float(dec) == 0.0:
        raise ValueError(f"{name} must be finite, > 0 and < 1e9, got {value!r}")
    return dec


def _route_venue(side: str, session: str, venue: str) -> str:
    vu = venue.strip().upper()
    if vu not in _ALLOWED_VENUES:
        raise ValueError(f"TV preset venue must be IBKR or ALPACA, got {venue!r}")
    if vu == "ALPACA" and side == "SHORT" and session == "OVERNIGHT":
        raise ValueError("ALPACA overnight SHORT unsupported; use IBKR")
    return vu


def _check_live_borrow(entry: Mapping[str, Any] | None, venue: str, now_ms: int) -> None:
    expected_source = {"ALPACA": "alpaca_paper_asset", "IBKR": "ibkr_paper_4002"}[venue]
    if (
        not entry
        or entry.get("borrow_source") != expected_source
        or not all(entry.get(flag) is True for flag in ("shortable", "easy_to_borrow"))
    ):
        raise ValueError(f"No affirmative {venue} watchlist borrow evidence for SELL")
    try:
        observed = datetime.fromisoformat(str(entry.get("borrow_as_of_utc")))
    except ValueError as exc:
        raise ValueError("Missing or invalid watchlist borrow observation time") from exc
    if observed.tzinfo is None or not 0 <= now_ms - observed.timestamp() * 1000 <= MAX_INPUT_AGE_MS:
        raise ValueError("Stale or future watchlist borrow observation")


def _check_live_intake(
    payload: Mapping[str, Any],
    venue: str,
    ts_ms: int,
    now_ms: int,
    session: str,
    prof: _eng.SessionProtectionProfile,
    trig: Decimal,
    stop: Decimal,
    route_side: _risk.Side,
    wl_entry: Mapping[str, Any] | None,
) -> None:
    if payload.get("data_source") != "tradingview_pine_v6":
        raise ValueError("Live TV intake requires tradingview_pine_v6")
    if payload.get("venue", venue) != venue:
        raise ValueError("Alert venue disagrees with routed venue")
    if (
        payload.get("semi_auto_confirmed") is not False
        or payload.get("strategy_id") != "SignalExecutionStrategy"
        or payload.get("auto_reverse") is not True
        or payload.get("auto_reentry") is not True
    ):
        raise ValueError("Live TV strategy, confirmation, or auto-reverse settings invalid")
    if ts_ms % 60_000 or not 0 <= now_ms - ts_ms <= MAX_INPUT_AGE_MS:
        raise ValueError("Expected a recent confirmed whole-minute bar close")
    if _eng.classify_us_equity_session_et(ts_ms * 1_000_000) != session:
        raise ValueError("Alert session disagrees with XNYS bar close")
    if _eng.classify_us_equity_session_et(now_ms * 1_000_000) == "CLOSED":
        raise ValueError("Current instant is outside verified XNYS 24/5 session")
    if (
        payload["order_type"] != prof.order_mode
        or Decimal(str(payload["limit_collar_bps"])) != prof.limit_collar_pct * 10_000
    ):
        raise ValueError("Order type or limit collar disagrees with native session profile")
    expected_stop = _risk.compute_capped_stop_price(
        trig, route_side, max_loss_pct=prof.initial_max_loss_pct
    )
    if stop != expected_stop:
        raise ValueError(f"Stop disagrees with Rule-612 initial stop {expected_stop}")
    if payload["side"] == "SELL":
        _check_live_borrow(wl_entry, venue, now_ms)


def _atomic_write_json(path: Path, payload: Any) -> Path:
    if path.is_dir():
        raise ValueError("Expected a file destination")
    path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=path.parent, prefix=f"{path.name}.tmp.") as temp_dir:
        temp = Path(temp_dir) / path.name
        temp.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        os.replace(temp, path)
    return path


def validate_tv_alert_payload(
    payload: Mapping[str, Any],
    *,
    watchlist_manifest: Mapping[str, Any] | None = None,
    venue: str = "IBKR",
    live_intake: bool = False,
) -> dict[str, Any]:
    """Validate a tier0.tv-alert.v1 payload fail-closed and return normalized staged preset."""
    if not isinstance(payload, Mapping):
        raise ValueError("Alert payload must be a mapping")
    refuse_live_endpoints(payload)
    for error in _alert_validator().iter_errors(dict(payload)):
        raise ValueError(f"Invalid TV alert schema: {error.message}")

    sym = str(payload["symbol"]).strip().upper()
    if not sym or not sym.replace(".", "").replace("-", "").isalnum():
        raise ValueError(f"Invalid symbol: {payload['symbol']!r}")
    side = str(payload["side"]).strip().upper()
    route_side: _risk.Side = "LONG" if side == "BUY" else "SHORT"
    session = str(payload["session"]).strip().upper()
    if session not in _ALLOWED_SESSIONS:
        raise ValueError(f"Unsupported session {session!r}")
    routed_venue = _route_venue(route_side, session, venue)

    order_type = str(payload["order_type"]).strip().upper()
    collar_bps = float(payload["limit_collar_bps"])
    if not math.isfinite(collar_bps) or not 0.0 <= collar_bps <= 100.0:
        raise ValueError(f"Invalid limit_collar_bps: {collar_bps!r}")
    if session != "RTH" and (order_type != "STOP_LIMIT" or collar_bps <= 0.0):
        raise ValueError(f"Off-hours session {session} requires STOP_LIMIT with collar > 0")

    trig = _parse_pos_decimal(payload["trigger_price"], "trigger_price", live_intake)
    stop = _parse_pos_decimal(payload["stop_price"], "stop_price", live_intake)
    q_stop = _risk.quantize_rule_612(stop, side=route_side, is_stop=True)
    if side == "BUY" and q_stop >= trig:
        raise ValueError(f"BUY stop {stop} (quantized {q_stop}) must be < trigger {trig}")
    if side == "SELL" and min(stop, q_stop) <= trig:
        raise ValueError(f"SELL stop {stop} (quantized {q_stop}) must be > trigger {trig}")
    stop_dist_pct = abs(trig - q_stop) / trig
    if stop_dist_pct > _risk.MAX_STOP_LOSS_PCT:
        raise ValueError(f"Stop distance {stop_dist_pct} exceeds {_risk.MAX_STOP_LOSS_PCT}")

    natr_dec = _parse_pos_decimal(payload["natr_pct"], "natr_pct", live_intake)
    expected_sched, profiles = _eng.select_pooled_trailing_schedule(natr_dec)
    prof = _eng.get_session_protection_profile(session, profiles)
    sched = str(payload["trailing_schedule"]).strip()
    if sched != expected_sched:
        raise ValueError(
            f"trailing_schedule {sched!r} mismatches NATR (expected {expected_sched!r})"
        )

    wl_entry = next(
        (
            e
            for e in (watchlist_manifest or {}).get("entries", [])
            if str(e.get("symbol", "")).strip().upper() == sym
        ),
        None,
    )
    if side == "SELL":
        if payload.get("shortable") is False or payload.get("easy_to_borrow") is False:
            raise ValueError(f"SELL preset rejected for non-ETB symbol {sym}")
        if wl_entry is not None and (
            not wl_entry.get("shortable", True) or not wl_entry.get("easy_to_borrow", True)
        ):
            raise ValueError(f"SELL preset rejected by watchlist borrow gate for {sym}")

    ts_ms = int(payload["ts_utc_ms"])
    if live_intake:
        now_ms = time.time_ns() // 1_000_000
        _check_live_intake(
            payload, routed_venue, ts_ms, now_ms, session, prof, trig, stop, route_side, wl_entry
        )

    digest_src = f"{sym}:{ts_ms}:{side}:{session}:{Decimal(str(float(trig)))}:{q_stop}:{sched}"
    alert_id = hashlib.sha256(digest_src.encode("utf-8")).hexdigest()[:16]
    staged: dict[str, Any] = {
        "alert_id": alert_id,
        "schema_version": "tier0.tv-alert.v1",
        "symbol": sym,
        "ts_utc_ms": ts_ms,
        "bar_confirmed": True,
        "side": side,
        "qty": 1,
        "venue": routed_venue,
        "session": session,
        "order_type": order_type,
        "trigger_price": float(trig),
        "stop_price": float(q_stop),
        "stop_distance_pct": float(stop_dist_pct),
        "limit_collar_bps": (
            collar_bps if order_type == "STOP_LIMIT" else float(prof.limit_collar_pct * 10_000)
        ),
        "trailing_schedule": sched,
        "natr_pct": float(natr_dec),
        "auto_reverse": bool(payload["auto_reverse"]),
        "auto_reentry": bool(payload["auto_reentry"]),
        "reentry_cooldown_secs": int(payload["reentry_cooldown_secs"]),
        "strategy_id": str(payload["strategy_id"]),
        "semi_auto_confirmed": False,
        "staged_at_utc": datetime.now(UTC).isoformat(),
    }
    if "data_source" in payload:
        staged["data_source"] = str(payload["data_source"])
    return staged


def stage_tv_alert_preset(
    payload: Mapping[str, Any],
    *,
    staged_path: Path = DEFAULT_STAGED_PATH,
    watchlist_path: Path = DEFAULT_WATCHLIST_PATH,
    venue: str = "IBKR",
    live_intake: bool = False,
) -> dict[str, Any]:
    """Validate, deduplicate by alert_id, and atomically stage a semi-auto preset."""
    wl_data = json.loads(watchlist_path.read_text("utf-8")) if watchlist_path.is_file() else None
    preset = validate_tv_alert_payload(
        payload, watchlist_manifest=wl_data, venue=venue, live_intake=live_intake
    )
    existing: dict[str, Any] = (
        json.loads(staged_path.read_text(encoding="utf-8"))
        if staged_path.is_file()
        else {"presets": []}
    )
    by_id = {
        str(item["alert_id"]): item for item in existing.get("presets", []) if "alert_id" in item
    }
    by_id[preset["alert_id"]] = preset
    doc = {
        "schema_version": "tier0.staged-tv-presets.v1",
        "updated_at_utc": datetime.now(UTC).isoformat(),
        "semi_auto_confirmed": False,
        "presets": list(by_id.values()),
    }
    _atomic_write_json(staged_path, doc)
    return preset


def ingest_alerts_file(
    alert_file: Path,
    *,
    staged_path: Path = DEFAULT_STAGED_PATH,
    watchlist_path: Path = DEFAULT_WATCHLIST_PATH,
    venue: str = "IBKR",
    live_intake: bool = False,
) -> list[dict[str, Any]]:
    """Ingest one alert JSON object or list of alert objects from disk."""
    raw = json.loads(alert_file.read_text(encoding="utf-8"))
    items = raw if isinstance(raw, list) else [raw]
    return [
        stage_tv_alert_preset(
            cast(Mapping[str, Any], item),
            staged_path=staged_path,
            watchlist_path=watchlist_path,
            venue=venue,
            live_intake=live_intake,
        )
        for item in items
    ]


def main() -> None:
    """CLI entrypoint for validating and staging TradingView alert JSON."""
    parser = argparse.ArgumentParser(description="Tier-0 TradingView semi-auto alert bridge")
    parser.add_argument("--ingest-file", type=Path, required=True)
    parser.add_argument("--staged-path", type=Path, default=DEFAULT_STAGED_PATH)
    parser.add_argument("--watchlist-path", type=Path, default=DEFAULT_WATCHLIST_PATH)
    parser.add_argument("--venue", default="IBKR")
    parser.add_argument("--live-intake", action="store_true")
    args = parser.parse_args()
    staged = ingest_alerts_file(
        args.ingest_file,
        staged_path=args.staged_path,
        watchlist_path=args.watchlist_path,
        venue=args.venue,
        live_intake=args.live_intake,
    )
    print(json.dumps({"ok": True, "staged_count": len(staged), "presets": staged}, indent=2))


if __name__ == "__main__":
    main()

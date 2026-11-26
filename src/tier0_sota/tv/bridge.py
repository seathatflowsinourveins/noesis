"""Fail-closed TradingView Pine v6 alert bridge (Pydantic v2, semi_auto_confirmed=False)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    ValidationError,
    field_validator,
)

from tier0_sota import engine as _eng
from tier0_sota import risk as _risk

DEFAULT_MANIFEST_DIR = (
    Path("Z:/sota-data/manifests") if os.name == "nt" else Path("/mnt/z/sota-data/manifests")
)
DEFAULT_STAGED_PATH = DEFAULT_MANIFEST_DIR / "staged_tv_presets.json"
DEFAULT_WATCHLIST_PATH = DEFAULT_MANIFEST_DIR / "live_watchlist_manifest.json"
FORBIDDEN_ENDPOINT_MARKERS = ("4001", "7496", "8798", "8796")
_LIVE_ALPACA_RE = re.compile(r"(?<![a-z0-9-])api\.alpaca\.markets(?![a-z0-9-])")
MAX_INPUT_AGE_MS = 15 * 60_000

_Archetype = Literal[
    "EARLY_EP_MOMENTUM_BURST",
    "VCP_BREAKOUT",
    "VCP_COIL",
    "PEAD_DRIFT",
    "PARABOLIC_TOP",
    "EXTREME_REVERSAL",
    "EXHAUSTION_HAMMER",
    "CRASH_ALIGNMENT",
    "HIDDEN_VOL_ACCUM",
    "PEAK_REVERSAL",
]
_Source = Literal[
    "tradingview_pine_v6",
    "prewall_lake",
    "alpaca_sip",
    "alpaca_boats",
    "alpaca_overnight",
    "alpaca_sip_boats_overnight",
    "alpaca_screener",
    "finra_trf_exchange_d",
    "ibkr_paper_4002",
    "nautilus_trader",
]


class TvAlertV1(BaseModel):
    """Strict Pydantic v2 contract for tier0.tv-alert.v1 webhook payloads."""

    model_config = ConfigDict(extra="forbid")

    alert_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    schema_version: Literal["tier0.tv-alert.v1"]
    symbol: str = Field(min_length=1, max_length=16)
    ts_utc_ms: StrictInt = Field(ge=0)
    bar_confirmed: Literal[True]
    side: Literal["BUY", "SELL"]
    qty: Literal[1]
    venue: Literal["IBKR", "ALPACA"] | None = None
    session: Literal["PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT"]
    order_type: Literal["STOP_MARKET", "STOP_LIMIT"]
    trigger_price: Decimal = Field(gt=0, lt=Decimal("1e9"))
    stop_price: Decimal = Field(gt=0, lt=Decimal("1e9"))
    stop_distance_pct: float | None = Field(default=None, ge=0.0, le=0.02)
    limit_collar_bps: float = Field(ge=0.0, le=100.0)
    trailing_schedule: Literal["tight_near_lossless", "baseline", "wide_natr_adaptive"]
    natr_pct: Decimal = Field(gt=0, le=Decimal("1"))
    auto_reverse: StrictBool
    auto_reentry: StrictBool
    reentry_cooldown_secs: StrictInt = Field(ge=60, le=86400)
    strategy_id: str = Field(min_length=1)
    semi_auto_confirmed: Literal[False] | None = None
    staged_at_utc: datetime | None = None
    shortable: StrictBool | None = None
    easy_to_borrow: StrictBool | None = None
    setup_archetype: _Archetype | None = None
    macro_regime: Literal[-1, 0, 1] | None = None
    rvol: float | None = Field(default=None, ge=0.0)
    hidden_volume_ratio: float | None = Field(default=None, ge=0.0)
    peak_confirmed: StrictBool | None = None
    is_leveraged_or_inverse: StrictBool | None = None
    pair_symbol: str | None = Field(default=None, min_length=1, max_length=16)
    data_source: _Source | None = None

    @field_validator("trigger_price", "stop_price", "natr_pct", "limit_collar_bps", mode="before")
    @classmethod
    def _reject_bool(cls, v: Any) -> Any:
        if isinstance(v, bool):
            raise ValueError("numeric alert field must not be bool")
        return str(v) if isinstance(v, float) else v

    @field_validator("symbol", mode="before")
    @classmethod
    def _clean_symbol(cls, v: Any) -> str:
        s = str(v).strip().upper()
        if not s or not s.replace(".", "").replace("-", "").isalnum():
            raise ValueError(f"Invalid symbol: {v!r}")
        return s


def refuse_live_endpoints(payload: Mapping[str, Any]) -> None:
    """Reject any live broker host or forbidden live port marker in alert payloads."""
    raw = json.dumps(dict(payload), default=str).lower()
    live_alpaca = "alpaca" in raw and "paper-api" not in raw and "markets" in raw
    if (
        live_alpaca
        or _LIVE_ALPACA_RE.search(raw)
        or any(m in raw for m in FORBIDDEN_ENDPOINT_MARKERS)
    ):
        raise ValueError("Refused live broker endpoint or port marker in alert payload")


def _check_live_intake(
    m: TvAlertV1,
    venue: str,
    now_ms: int,
    prof: _eng.SessionProtectionProfile,
    wl_entry: Mapping[str, Any] | None,
) -> None:
    if m.data_source != "tradingview_pine_v6":
        raise ValueError("Live TV intake requires tradingview_pine_v6")
    if (m.venue or venue) != venue:
        raise ValueError("Alert venue disagrees with routed venue")
    if (
        m.semi_auto_confirmed is not False
        or m.strategy_id != "SignalExecutionStrategy"
        or not (m.auto_reverse and m.auto_reentry)
    ):
        raise ValueError("Live TV strategy, confirmation, or auto-reverse settings invalid")
    if m.ts_utc_ms % 60_000 or not 0 <= now_ms - m.ts_utc_ms <= MAX_INPUT_AGE_MS:
        raise ValueError("Expected a recent confirmed whole-minute bar close")
    if _eng.classify_us_equity_session_et(m.ts_utc_ms * 1_000_000) != m.session:
        raise ValueError("Alert session disagrees with XNYS bar close")
    if _eng.classify_us_equity_session_et(now_ms * 1_000_000) == "CLOSED":
        raise ValueError("Current instant is outside verified XNYS 24/5 session")
    if (
        m.order_type != prof.order_mode
        or Decimal(str(m.limit_collar_bps)) != prof.limit_collar_pct * 10_000
    ):
        raise ValueError("Order type or limit collar disagrees with native session profile")
    route_side: _risk.Side = "LONG" if m.side == "BUY" else "SHORT"
    expected_stop = _risk.compute_capped_stop_price(
        m.trigger_price, route_side, max_loss_pct=prof.initial_max_loss_pct
    )
    if m.stop_price != expected_stop:
        raise ValueError(f"Stop disagrees with Rule-612 initial stop {expected_stop}")
    if m.side == "SELL":
        expected_src = {"ALPACA": "alpaca_paper_asset", "IBKR": "ibkr_paper_4002"}[venue]
        if (
            not wl_entry
            or wl_entry.get("borrow_source") != expected_src
            or not all(wl_entry.get(k) is True for k in ("shortable", "easy_to_borrow"))
        ):
            raise ValueError(f"No affirmative {venue} watchlist borrow evidence for SELL")
        try:
            obs = datetime.fromisoformat(str(wl_entry.get("borrow_as_of_utc")))
        except ValueError as exc:
            raise ValueError("Missing or invalid watchlist borrow observation time") from exc
        if obs.tzinfo is None or not 0 <= now_ms - obs.timestamp() * 1000 <= MAX_INPUT_AGE_MS:
            raise ValueError("Stale or future watchlist borrow observation")


def _atomic_write_json(path: Path, payload: Any) -> Path:
    if path.is_dir():
        raise ValueError("Expected a file destination")
    path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=path.parent, prefix=f"{path.name}.tmp.") as td:
        tmp = Path(td) / path.name
        tmp.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        os.replace(tmp, path)
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
    try:
        m = TvAlertV1.model_validate(dict(payload))
    except ValidationError as exc:
        raise ValueError(f"Invalid TV alert schema: {exc}") from exc

    route_side: _risk.Side = "LONG" if m.side == "BUY" else "SHORT"
    vu = venue.strip().upper()
    if vu not in {"IBKR", "ALPACA"}:
        raise ValueError(f"TV preset venue must be IBKR or ALPACA, got {venue!r}")
    if vu == "ALPACA" and route_side == "SHORT" and m.session == "OVERNIGHT":
        raise ValueError("ALPACA overnight SHORT unsupported; use IBKR")
    if m.session != "RTH" and (m.order_type != "STOP_LIMIT" or m.limit_collar_bps <= 0.0):
        raise ValueError(f"Off-hours session {m.session} requires STOP_LIMIT with collar > 0")

    q_stop = _risk.quantize_rule_612(m.stop_price, side=route_side, is_stop=True)
    if m.side == "BUY" and q_stop >= m.trigger_price:
        raise ValueError(
            f"BUY stop {m.stop_price} (quantized {q_stop}) must be < trigger {m.trigger_price}"
        )
    if m.side == "SELL" and min(m.stop_price, q_stop) <= m.trigger_price:
        raise ValueError(
            f"SELL stop {m.stop_price} (quantized {q_stop}) must be > trigger {m.trigger_price}"
        )
    stop_dist_pct = abs(m.trigger_price - q_stop) / m.trigger_price
    if stop_dist_pct > _risk.MAX_STOP_LOSS_PCT:
        raise ValueError(f"Stop distance {stop_dist_pct} exceeds {_risk.MAX_STOP_LOSS_PCT}")

    expected_sched, profiles = _eng.select_pooled_trailing_schedule(m.natr_pct)
    prof = _eng.get_session_protection_profile(m.session, profiles)
    if m.trailing_schedule != expected_sched:
        raise ValueError(
            f"trailing_schedule {m.trailing_schedule!r} mismatches NATR "
            f"(expected {expected_sched!r})"
        )

    wl_entry = next(
        (
            e
            for e in (watchlist_manifest or {}).get("entries", [])
            if str(e.get("symbol", "")).strip().upper() == m.symbol
        ),
        None,
    )
    if m.side == "SELL":
        if m.shortable is False or m.easy_to_borrow is False:
            raise ValueError(f"SELL preset rejected for non-ETB symbol {m.symbol}")
        if wl_entry is not None and (
            not wl_entry.get("shortable", True) or not wl_entry.get("easy_to_borrow", True)
        ):
            raise ValueError(f"SELL preset rejected by watchlist borrow gate for {m.symbol}")
    if live_intake:
        _check_live_intake(m, vu, time.time_ns() // 1_000_000, prof, wl_entry)

    trig_norm = Decimal(str(float(m.trigger_price)))
    digest_src = (
        f"{m.symbol}:{m.ts_utc_ms}:{m.side}:{m.session}:{trig_norm}:{q_stop}:{m.trailing_schedule}"
    )
    staged = {
        "alert_id": hashlib.sha256(digest_src.encode("utf-8")).hexdigest()[:16],
        "schema_version": m.schema_version,
        "symbol": m.symbol,
        "ts_utc_ms": m.ts_utc_ms,
        "bar_confirmed": True,
        "side": m.side,
        "qty": 1,
        "venue": vu,
        "session": m.session,
        "order_type": m.order_type,
        "trigger_price": float(m.trigger_price),
        "stop_price": float(q_stop),
        "stop_distance_pct": float(stop_dist_pct),
        "limit_collar_bps": m.limit_collar_bps
        if m.order_type == "STOP_LIMIT"
        else float(prof.limit_collar_pct * 10_000),
        "trailing_schedule": m.trailing_schedule,
        "natr_pct": float(m.natr_pct),
        "auto_reverse": m.auto_reverse,
        "auto_reentry": m.auto_reentry,
        "reentry_cooldown_secs": m.reentry_cooldown_secs,
        "strategy_id": m.strategy_id,
        "semi_auto_confirmed": False,
        "staged_at_utc": datetime.now(UTC).isoformat(),
    }
    if m.data_source is not None:
        staged["data_source"] = m.data_source
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
    existing = (
        json.loads(staged_path.read_text(encoding="utf-8"))
        if staged_path.is_file()
        else {"presets": []}
    )
    by_id = {str(x["alert_id"]): x for x in existing.get("presets", []) if "alert_id" in x}
    by_id[preset["alert_id"]] = preset
    _atomic_write_json(
        staged_path,
        {
            "schema_version": "tier0.staged-tv-presets.v1",
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "semi_auto_confirmed": False,
            "presets": list(by_id.values()),
        },
    )
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
            cast(Mapping[str, Any], i),
            staged_path=staged_path,
            watchlist_path=watchlist_path,
            venue=venue,
            live_intake=live_intake,
        )
        for i in items
    ]


def main() -> None:
    """CLI entrypoint for validating and staging TradingView alert JSON."""
    p = argparse.ArgumentParser(description="Tier-0 TradingView semi-auto alert bridge")
    p.add_argument("--ingest-file", type=Path, required=True)
    p.add_argument("--staged-path", type=Path, default=DEFAULT_STAGED_PATH)
    p.add_argument("--watchlist-path", type=Path, default=DEFAULT_WATCHLIST_PATH)
    p.add_argument("--venue", default="IBKR")
    p.add_argument("--live-intake", action="store_true")
    a = p.parse_args()
    staged = ingest_alerts_file(
        a.ingest_file,
        staged_path=a.staged_path,
        watchlist_path=a.watchlist_path,
        venue=a.venue,
        live_intake=a.live_intake,
    )
    print(json.dumps({"ok": True, "staged_count": len(staged), "presets": staged}, indent=2))


if __name__ == "__main__":
    main()

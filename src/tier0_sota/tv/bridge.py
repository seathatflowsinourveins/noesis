"""Fail-closed TradingView Pine v6 alert bridge (Pydantic v2, semi_auto_confirmed=False)."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

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
DEFAULT_PACK_STAGED_PATH = DEFAULT_MANIFEST_DIR / "tv_sota_pack" / "staged_tv_presets.json"
DEFAULT_WATCHLIST_PATH = DEFAULT_MANIFEST_DIR / "live_watchlist_manifest.json"
DEFAULT_MOVERS_PATH = DEFAULT_MANIFEST_DIR / "live_whole_market_movers_manifest.json"
FORBIDDEN_ENDPOINT_MARKERS = ("4001", "7496", "8798", "8796", "9223")
FORBIDDEN_LIVE_PORTS: frozenset[int] = frozenset(int(p) for p in FORBIDDEN_ENDPOINT_MARKERS)
_MIN_FORBIDDEN_PORT = min(FORBIDDEN_LIVE_PORTS)
_MAX_FORBIDDEN_PORT = max(FORBIDDEN_LIVE_PORTS)
_PORT_ALT = "|".join(re.escape(port) for port in FORBIDDEN_ENDPOINT_MARKERS)
_LIVE_ALPACA_RE = re.compile(r"(?<![a-z0-9-])(?:api|live)\.alpaca\.markets(?![a-z0-9-])")
_DECIMAL_STRING_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$")
_ISO_TS_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}[t ]\d{2}:\d{2}:\d{2}\.\d+(?:z|[+-]\d{2}:?\d{2})?\b")
_UUID_TOKEN_RE = re.compile(
    r"(?<![0-9a-z:])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-z])"
)
_HEX_DIGEST_TOKEN_RE = re.compile(
    r"(?<![0-9a-z:])(?=[0-9a-f]{16,64}(?![0-9a-z]))(?=[0-9a-f]*[a-f])[0-9a-f]{16,64}"
)
_FRACTIONAL_DECIMAL_TOKEN_RE = re.compile(r"(?<![0-9a-z:.])\d+\.(?:0*[1-9]\d*)(?![0-9a-z.])")
_FORBIDDEN_PORT_RE = re.compile(r"(?<!\d)(?:" + _PORT_ALT + r")(?!\d)")
_COLON_PORT_RE = re.compile(r":(?:" + _PORT_ALT + r")(?:\.0+)?(?![0-9])")
_LABELED_PORT_RE = re.compile(
    r"(?<![a-z0-9])(?:--?)?(?:"
    r"(?:port|cdp|endpoint|host|gateway|broker|relay|socket|listen|bind)(?:[_\-.]?(?:port|num|id))?[\s:=_-]+"
    r"|(?:port|cdp|live|endpoint)[_-]?"
    r")(?:" + _PORT_ALT + r")(?:\.0+)?(?![0-9])"
)
_ENDPOINT_KEY_RE = re.compile(
    r"(?:^|[_\-.])(?:port|ports|endpoint|endpoints|host|hosts|url|urls|uri|uris|"
    r"cdp|broker|gateway|socket|addr|address|target|proxy|relay|webhook|server|listen|bind)"
    r"(?:$|[_\-.]|\d)",
    re.IGNORECASE,
)
_REFUSE_LIVE_ENDPOINT_MSG = "Refused live broker endpoint or port marker in alert payload"
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
_ScriptKind = Literal["indicator", "strategy"]
_VenueProfile = Literal[
    "IBKR_PAPER_4002",
    "ALPACA_PAPER_200",
    "ALPACA_PAPER_1000",
    "IBKR_LIVE_3000",
    "ALPACA_LIVE_PRO_LINE_1000",
    "ALPACA_LIVE_ELITE_SMART_ROUTER",
]
_LIVE_VENUE_PROFILES: frozenset[str] = frozenset(
    {
        "IBKR_LIVE_3000",
        "ALPACA_LIVE_PRO_LINE_1000",
        "ALPACA_LIVE_ELITE_SMART_ROUTER",
    }
)
_PAPER_VENUE_PROFILES: frozenset[str] = frozenset(
    {
        "IBKR_PAPER_4002",
        "ALPACA_PAPER_200",
        "ALPACA_PAPER_1000",
    }
)
_NY_TZ = ZoneInfo("America/New_York")
_TV_CLIENT_ORDER_RE = re.compile(r"^noesis-tv-([0-9a-f]{16})$", re.IGNORECASE)
_HEX_ALERT_ID_RE = re.compile(r"^[0-9a-f]{16}$", re.IGNORECASE)
_ARCHIVE_STALE_AGE_HOURS = 18.0
_EXECUTED_STATUS_TOKENS: frozenset[str] = frozenset(
    {"EXECUTED", "FILLED", "SUBMITTED", "CONFIRMED", "CLOSED", "ACTIVE", "ACCEPTED"}
)
_SESSION_EXEC_MECHANISM: dict[str, str] = {
    "RTH": "NATIVE_BROKER_STOP_MARKET_RATCHET",
    "PRE_MARKET": "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
    "AFTER_HOURS": "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT",
    "OVERNIGHT": "SYNTHETIC_BOATS_COLLARED_LIMIT",
}
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

_DUCKDB_BASE_SCHEMA_SQL = (
    "CREATE TABLE IF NOT EXISTS tv_staged_presets ("
    "alert_id VARCHAR PRIMARY KEY, symbol VARCHAR, side VARCHAR, qty INTEGER, "
    "venue VARCHAR, session VARCHAR, order_type VARCHAR, trigger_price DOUBLE, "
    "stop_price DOUBLE, limit_collar_bps DOUBLE, trailing_schedule VARCHAR, "
    "semi_auto_confirmed BOOLEAN, data_source VARCHAR, source VARCHAR, "
    "staged_at_utc VARCHAR)"
)
_DUCKDB_ALTER_STMTS: tuple[str, ...] = (
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS setup_archetype VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS macro_regime INTEGER",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS rvol DOUBLE",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS darkpool_trf_ratio DOUBLE",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS hidden_volume_ratio DOUBLE",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS vol_buildup_ratio DOUBLE",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS unusual_volume_buildup BOOLEAN",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS peak_confirmed BOOLEAN",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS pair_symbol VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS action_gate VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS reverse_short_mode VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS script_kind VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS order_preset_id VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS venue_profile VARCHAR",
    "ALTER TABLE tv_staged_presets ADD COLUMN IF NOT EXISTS status VARCHAR",
)
_DUCKDB_INSERT_SQL = (
    "INSERT OR REPLACE INTO tv_staged_presets ("
    "alert_id, symbol, side, qty, venue, session, order_type, trigger_price, "
    "stop_price, limit_collar_bps, trailing_schedule, semi_auto_confirmed, "
    "data_source, source, staged_at_utc, setup_archetype, macro_regime, rvol, "
    "darkpool_trf_ratio, hidden_volume_ratio, vol_buildup_ratio, "
    "unusual_volume_buildup, peak_confirmed, pair_symbol, action_gate, "
    "reverse_short_mode, script_kind, order_preset_id, venue_profile, status) VALUES ("
    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


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
    venue_profile: _VenueProfile | None = None
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
    darkpool_trf_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    hidden_volume_ratio: float | None = Field(default=None, ge=0.0)
    vol_buildup_ratio: float | None = Field(default=None, ge=0.0)
    unusual_volume_buildup: StrictBool | None = None
    peak_confirmed: StrictBool | None = None
    is_leveraged_or_inverse: StrictBool | None = None
    pair_symbol: str | None = Field(default=None, min_length=1, max_length=16)
    script_kind: _ScriptKind | None = None
    order_preset_id: str | None = Field(default=None, min_length=1, max_length=64)
    data_source: _Source | None = None
    calibrated_category: str | None = Field(default=None, min_length=1, max_length=64)
    min_trailing_pct: float | None = Field(default=None, gt=0.0, le=0.20)
    technical_alignment_score: float | None = Field(default=None, ge=0.0, le=100.0)
    execution_mechanism: str | None = Field(default=None, min_length=1, max_length=64)
    auto_reverse_policy: str | None = Field(default=None, min_length=1, max_length=64)
    r_lock_levels: dict[str, Any] | None = None
    session_order_presets: dict[str, dict[str, Any]] | None = None
    momentum_trigger_reason: dict[str, Any] | None = None

    @field_validator("trigger_price", "stop_price", "natr_pct", "limit_collar_bps", mode="before")
    @classmethod
    def _reject_bool(cls, v: Any) -> Any:
        if isinstance(v, bool):
            raise ValueError("numeric alert field must not be bool")
        return str(v) if isinstance(v, float) else v

    @field_validator(
        "stop_distance_pct",
        "rvol",
        "darkpool_trf_ratio",
        "hidden_volume_ratio",
        "vol_buildup_ratio",
        "min_trailing_pct",
        "technical_alignment_score",
        mode="before",
    )
    @classmethod
    def _reject_bool_optional_float(cls, v: Any) -> Any:
        if isinstance(v, bool):
            raise ValueError("numeric alert field must not be bool")
        return v

    @field_validator("symbol", mode="before")
    @classmethod
    def _clean_symbol(cls, v: Any) -> str:
        s = str(v).strip().upper()
        if not s or not s.replace(".", "").replace("-", "").isalnum():
            raise ValueError(f"Invalid symbol: {v!r}")
        return s

    @field_validator("order_preset_id", mode="before")
    @classmethod
    def _clean_order_preset_id(cls, v: Any) -> Any:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            raise ValueError("order_preset_id must not be blank")
        return s


def _is_forbidden_decimal(val: Decimal) -> bool:
    return (
        val.is_finite()
        and _MIN_FORBIDDEN_PORT <= val <= _MAX_FORBIDDEN_PORT
        and val == val.to_integral_value()
        and int(val) in FORBIDDEN_LIVE_PORTS
    )


def _check_endpoint_str(text: str, *, key_is_endpoint: bool = False) -> None:
    s = text.strip().lower()
    if not s:
        return
    if ("alpaca" in s and "paper-api" not in s and "markets" in s) or _LIVE_ALPACA_RE.search(s):
        raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)
    if _DECIMAL_STRING_RE.fullmatch(s):
        if not key_is_endpoint:
            return
        try:
            if _is_forbidden_decimal(Decimal(s)):
                raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)
        except InvalidOperation:
            if _FORBIDDEN_PORT_RE.search(s):
                raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG) from None
        return
    if _COLON_PORT_RE.search(s) or _LABELED_PORT_RE.search(s):
        raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)
    if key_is_endpoint:
        scrubbed = _ISO_TS_RE.sub(" ", s)
        scrubbed = _UUID_TOKEN_RE.sub(" ", scrubbed)
        scrubbed = _HEX_DIGEST_TOKEN_RE.sub(" ", scrubbed)
        scrubbed = _FRACTIONAL_DECIMAL_TOKEN_RE.sub(" ", scrubbed)
        if _FORBIDDEN_PORT_RE.search(scrubbed):
            raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)


def refuse_live_endpoints(payload: Mapping[str, Any]) -> None:
    """Reject any live broker host or forbidden live/CDP port marker in alert payloads.

    Forbidden ports (4001/7496/8796/8798/9223) are refused strictly in URL/host `:PORT`
    syntax, explicit port/cdp/endpoint/live markers, or values under endpoint/port/host/url/cdp
    keys — never false-positiving on arbitrary numeric prices, timestamps, volumes, or hex IDs.
    """
    raw = json.dumps(dict(payload), default=str).lower()
    live_alpaca = "alpaca" in raw and "paper-api" not in raw and "markets" in raw
    if live_alpaca or _LIVE_ALPACA_RE.search(raw):
        raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)

    stack: list[tuple[Any, bool]] = [(payload, False)]
    while stack:
        val, in_endpoint_ctx = stack.pop()
        if val is None or isinstance(val, bool):
            continue
        if isinstance(val, Mapping):
            for k, v in val.items():
                key_str = str(k)
                _check_endpoint_str(key_str, key_is_endpoint=False)
                child_ctx = in_endpoint_ctx or bool(_ENDPOINT_KEY_RE.search(key_str))
                stack.append((v, child_ctx))
        elif isinstance(val, str):
            _check_endpoint_str(val, key_is_endpoint=in_endpoint_ctx)
        elif isinstance(val, (bytes, bytearray)):
            _check_endpoint_str(
                val.decode("utf-8", errors="ignore"), key_is_endpoint=in_endpoint_ctx
            )
        elif isinstance(val, (Sequence, set, frozenset)):
            for item in val:
                stack.append((item, in_endpoint_ctx))
        elif isinstance(val, int):
            if in_endpoint_ctx and val in FORBIDDEN_LIVE_PORTS:
                raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)
        elif isinstance(val, float):
            if (
                in_endpoint_ctx
                and math.isfinite(val)
                and _MIN_FORBIDDEN_PORT <= val <= _MAX_FORBIDDEN_PORT
                and val.is_integer()
                and int(val) in FORBIDDEN_LIVE_PORTS
            ):
                raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)
        elif isinstance(val, Decimal):
            if in_endpoint_ctx and _is_forbidden_decimal(val):
                raise ValueError(_REFUSE_LIVE_ENDPOINT_MSG)
        else:
            _check_endpoint_str(str(val), key_is_endpoint=in_endpoint_ctx)


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
    if m.venue_profile is not None:
        if m.venue_profile in _LIVE_VENUE_PROFILES:
            raise ValueError(
                f"Refused Stage-5 LIVE venue_profile {m.venue_profile!r} on live intake"
            )
        if not m.venue_profile.startswith(f"{venue}_"):
            raise ValueError(
                f"Alert venue_profile {m.venue_profile!r} disagrees with routed venue {venue!r}"
            )
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


def _resolve_tv_audit_duckdb_path(staged_path: Path) -> Path:
    pack_dir = staged_path.parent / "tv_sota_pack"
    if pack_dir.is_dir():
        return pack_dir / "tv_half_auto_audit.duckdb"
    return staged_path.parent / "tv_half_auto_audit.duckdb"


def _load_watchlist_manifest(watchlist_path: Path) -> Mapping[str, Any] | None:
    """Load watchlist manifest, falling back to whole-market movers for default path only."""
    if watchlist_path.is_file():
        return cast(Mapping[str, Any], json.loads(watchlist_path.read_text(encoding="utf-8")))
    if watchlist_path == DEFAULT_WATCHLIST_PATH and DEFAULT_MOVERS_PATH.is_file():
        return cast(Mapping[str, Any], json.loads(DEFAULT_MOVERS_PATH.read_text(encoding="utf-8")))
    return None


def _opt_db_float(val: Any) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    return float(val)


def _opt_db_int(val: Any) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    return int(val)


def _opt_db_bool(val: Any) -> bool | None:
    return val if isinstance(val, bool) else None


def _opt_db_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def persist_tv_presets_duckdb(
    duckdb_path: Path,
    presets: Sequence[Mapping[str, Any]],
    *,
    source: str = "tv_bridge",
    meta_by_alert_id: Mapping[str, Mapping[str, Any]] | None = None,
    status: str | None = None,
) -> None:
    """Batch-persist schema-validated semi-auto TV presets (qty=1, semi_auto_confirmed=False)."""
    if not presets:
        return
    rows: list[tuple[Any, ...]] = []
    now_iso = datetime.now(UTC).isoformat()
    for preset in presets:
        refuse_live_endpoints(preset)
        qty_val = preset.get("qty")
        if (
            preset.get("semi_auto_confirmed") is not False
            or type(qty_val) is not int
            or qty_val != 1
        ):
            raise ValueError(
                "Refused TV audit persistence unless qty=1 and semi_auto_confirmed=False"
            )
        aid = str(preset["alert_id"])
        meta = (meta_by_alert_id or {}).get(aid) or {}
        row_status = (
            _opt_db_str(status or meta.get("status") or preset.get("status")) or "ACTIVE_STAGED"
        )
        rows.append(
            (
                aid,
                str(preset["symbol"]),
                str(preset["side"]),
                int(preset["qty"]),
                str(preset.get("venue") or "IBKR"),
                str(preset["session"]),
                str(preset["order_type"]),
                float(preset["trigger_price"]),
                float(preset["stop_price"]),
                float(preset.get("limit_collar_bps") or 0.0),
                str(preset["trailing_schedule"]),
                False,
                str(preset.get("data_source") or "tradingview_pine_v6"),
                source,
                str(preset.get("staged_at_utc") or now_iso),
                _opt_db_str(preset.get("setup_archetype") or meta.get("archetype")),
                _opt_db_int(preset.get("macro_regime")),
                _opt_db_float(preset.get("rvol")),
                _opt_db_float(preset.get("darkpool_trf_ratio")),
                _opt_db_float(preset.get("hidden_volume_ratio")),
                _opt_db_float(preset.get("vol_buildup_ratio")),
                _opt_db_bool(preset.get("unusual_volume_buildup")),
                _opt_db_bool(preset.get("peak_confirmed")),
                _opt_db_str(preset.get("pair_symbol") or meta.get("pair_symbol")),
                _opt_db_str(meta.get("action_gate")),
                _opt_db_str(meta.get("reverse_short_mode")),
                _opt_db_str(preset.get("script_kind") or meta.get("script_kind")),
                _opt_db_str(preset.get("order_preset_id") or meta.get("order_preset_id")),
                _opt_db_str(preset.get("venue_profile") or meta.get("venue_profile")),
                row_status,
            )
        )

    from tier0_sota.live.paper_farm import open_bounded_duckdb

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path = duckdb_path.with_name("tv_staged_presets.parquet")
    with open_bounded_duckdb(duckdb_path) as conn:
        conn.execute(_DUCKDB_BASE_SCHEMA_SQL)
        for alter_sql in _DUCKDB_ALTER_STMTS:
            conn.execute(alter_sql)
        conn.executemany(_DUCKDB_INSERT_SQL, rows)
        with TemporaryDirectory(dir=duckdb_path.parent, prefix="tv_staged_presets.tmp.") as td:
            tmp_pq = Path(td) / "tv_staged_presets.parquet"
            conn.table("tv_staged_presets").write_parquet(str(tmp_pq))
            os.replace(tmp_pq, parquet_path)


def persist_tv_preset_duckdb(
    duckdb_path: Path,
    preset: Mapping[str, Any],
    *,
    source: str = "tv_bridge",
    meta: Mapping[str, Any] | None = None,
    status: str | None = None,
) -> None:
    """Persist a schema-validated semi-auto TV preset (qty=1, semi_auto_confirmed=False)."""
    meta_map = {str(preset["alert_id"]): meta} if meta and "alert_id" in preset else None
    persist_tv_presets_duckdb(
        duckdb_path, [preset], source=source, meta_by_alert_id=meta_map, status=status
    )


def sync_tv_audit_duckdb_snapshot(duckdb_path: Path) -> bool:
    """Idempotently migrate an existing TV audit DuckDB to 30 cols and export Parquet snapshot."""
    if not duckdb_path.is_file():
        return False
    from tier0_sota.live.paper_farm import open_bounded_duckdb

    parquet_path = duckdb_path.with_name("tv_staged_presets.parquet")
    with open_bounded_duckdb(duckdb_path) as conn:
        conn.execute(_DUCKDB_BASE_SCHEMA_SQL)
        for alter_sql in _DUCKDB_ALTER_STMTS:
            conn.execute(alter_sql)
        with TemporaryDirectory(dir=duckdb_path.parent, prefix="tv_staged_presets.tmp.") as td:
            tmp_pq = Path(td) / "tv_staged_presets.parquet"
            conn.table("tv_staged_presets").write_parquet(str(tmp_pq))
            os.replace(tmp_pq, parquet_path)
    return True


def load_tv_audit_summary(duckdb_path: Path) -> dict[str, Any]:
    """Read-only summary of tv_half_auto_audit.duckdb without creating missing files."""
    empty: dict[str, Any] = {
        "audit_rows": 0,
        "by_source": {},
        "by_status": {},
        "distinct_symbols": [],
        "parquet_snapshot_present": duckdb_path.with_name("tv_staged_presets.parquet").is_file(),
    }
    if not duckdb_path.is_file():
        return empty
    from tier0_sota.live.paper_farm import open_bounded_duckdb

    try:
        with open_bounded_duckdb(duckdb_path, read_only=True) as conn:
            tables = {str(r[0]) for r in conn.execute("SHOW TABLES").fetchall()}
            if "tv_staged_presets" not in tables:
                return empty
            col_names = {str(r[0]) for r in conn.execute("DESCRIBE tv_staged_presets").fetchall()}
            total_row = conn.execute(
                "SELECT COUNT(DISTINCT alert_id) FROM tv_staged_presets"
            ).fetchone()
            by_src_rows = conn.execute(
                "SELECT COALESCE(source, 'unknown'), COUNT(DISTINCT alert_id) "
                "FROM tv_staged_presets GROUP BY 1 ORDER BY 1"
            ).fetchall()
            by_status_rows = (
                conn.execute(
                    "SELECT COALESCE(status, 'ACTIVE_STAGED'), COUNT(DISTINCT alert_id) "
                    "FROM tv_staged_presets GROUP BY 1 ORDER BY 1"
                ).fetchall()
                if "status" in col_names
                else []
            )
            sym_rows = conn.execute(
                "SELECT DISTINCT symbol FROM tv_staged_presets "
                "WHERE symbol IS NOT NULL ORDER BY symbol"
            ).fetchall()
    except OSError:
        return empty
    return {
        "audit_rows": int(total_row[0]) if total_row and total_row[0] is not None else 0,
        "by_source": {str(src): int(cnt) for src, cnt in by_src_rows},
        "by_status": {str(st): int(cnt) for st, cnt in by_status_rows},
        "distinct_symbols": [str(r[0]) for r in sym_rows],
        "parquet_snapshot_present": duckdb_path.with_name("tv_staged_presets.parquet").is_file(),
    }


def _resolve_default_paper_venue_profile(venue: str) -> str:
    vu = venue.strip().upper()
    if vu == "IBKR":
        return "IBKR_PAPER_4002"
    from tier0_sota.live.paper_farm import resolve_alpaca_paper_venue_profile

    return resolve_alpaca_paper_venue_profile()[0]


def _build_staged_session_order_presets(
    *,
    symbol: str,
    side: _risk.Side,
    ref_price: Decimal,
    natr_pct: Decimal,
    venue_profile: str,
    wl_entry: Mapping[str, Any] | None = None,
    min_trailing_pct: float | None = None,
    pair_symbol: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Build 4-session 24/5 lossless trailing order presets via upstream paper_farm SSOT."""
    from tier0_sota.live.paper_farm import build_symbol_24x5_session_presets

    spread_bps = (wl_entry or {}).get("spread_bps")
    cal_trail = (
        min_trailing_pct
        if min_trailing_pct is not None
        else _opt_db_float((wl_entry or {}).get("min_trailing_pct"))
    )
    alp_vp = venue_profile if venue_profile in {"ALPACA_PAPER_200", "ALPACA_PAPER_1000"} else None
    alp_rpm = (
        1000 if alp_vp == "ALPACA_PAPER_1000" else (200 if alp_vp == "ALPACA_PAPER_200" else None)
    )
    return build_symbol_24x5_session_presets(
        symbol=symbol,
        side=side,
        ref_price=ref_price,
        natr_pct=natr_pct,
        spread_bps=spread_bps,
        calibrated_min_trailing_pct=cal_trail,
        alpaca_venue_profile=alp_vp,
        alpaca_max_rpm=alp_rpm,
        inverse_hedge_symbol=pair_symbol,
    )


def _extract_tv_alert_id_from_token(token: Any) -> str | None:
    if not isinstance(token, str):
        return None
    s = token.strip()
    if not s:
        return None
    m = _TV_CLIENT_ORDER_RE.match(s)
    if m:
        return m.group(1).lower()
    return None


def _collect_executed_tv_alert_ids(manifest_dir: Path) -> set[str]:
    """Collect alert_ids already submitted/filled across Paper Farm status JSON & DuckDB ledgers."""
    executed: set[str] = set()
    if not manifest_dir.is_dir():
        return executed

    exec_status_path = manifest_dir / "paper_farm_execution_status.json"
    if exec_status_path.is_file():
        try:
            doc = json.loads(exec_status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            doc = None
        if isinstance(doc, Mapping):
            for key in (
                "active_positions",
                "recent_orders",
                "orders",
                "executed_alerts",
                "closed_positions",
            ):
                bucket = doc.get(key)
                rows: list[Any] = []
                if isinstance(bucket, Sequence) and not isinstance(bucket, (str, bytes)):
                    rows = list(bucket)
                elif isinstance(bucket, Mapping):
                    rows = list(bucket.values())
                for item in rows:
                    if isinstance(item, str):
                        aid = _extract_tv_alert_id_from_token(item)
                        if aid:
                            executed.add(aid)
                        elif _HEX_ALERT_ID_RE.match(item.strip()):
                            executed.add(item.strip().lower())
                        continue
                    if not isinstance(item, Mapping):
                        continue
                    for cid_key in ("client_order_id", "broker_order_id", "order_id"):
                        aid = _extract_tv_alert_id_from_token(item.get(cid_key))
                        if aid:
                            executed.add(aid)
                    raw_aid = item.get("alert_id")
                    if isinstance(raw_aid, str) and _HEX_ALERT_ID_RE.match(raw_aid.strip()):
                        st = str(item.get("status") or item.get("state") or "").strip().upper()
                        if not st or st in _EXECUTED_STATUS_TOKENS or key == "active_positions":
                            executed.add(raw_aid.strip().lower())

    from tier0_sota.live.paper_farm import open_bounded_duckdb

    for db_path in sorted(manifest_dir.glob("*.duckdb")):
        if db_path.name == "tv_half_auto_audit.duckdb":
            continue
        try:
            with open_bounded_duckdb(db_path, read_only=True) as conn:
                tables = {str(r[0]) for r in conn.execute("SHOW TABLES").fetchall()}
                if "paper_entry_decisions" in tables:
                    for (cid,) in conn.execute(
                        "SELECT DISTINCT client_order_id FROM paper_entry_decisions "
                        "WHERE client_order_id LIKE 'noesis-tv-%'"
                    ).fetchall():
                        aid = _extract_tv_alert_id_from_token(cid)
                        if aid:
                            executed.add(aid)
                if "paper_wire_orders" in tables:
                    wire_cols = {
                        str(r[0]) for r in conn.execute("DESCRIBE paper_wire_orders").fetchall()
                    }
                    if "client_order_id" in wire_cols:
                        for (cid,) in conn.execute(
                            "SELECT DISTINCT client_order_id FROM paper_wire_orders "
                            "WHERE client_order_id LIKE 'noesis-tv-%'"
                        ).fetchall():
                            aid = _extract_tv_alert_id_from_token(cid)
                            if aid:
                                executed.add(aid)
        except Exception as exc:
            # Live supervisor/runner may hold a write lock on its primary DuckDB; skip gracefully.
            logging.getLogger(__name__).debug("Skipping locked DuckDB %s: %s", db_path.name, exc)
            continue
    return executed


def _parse_iso_utc(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        dt = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    return dt.astimezone(UTC) if dt.tzinfo is not None else None


def prune_and_archive_staged_presets(
    *,
    staged_path: Path = DEFAULT_PACK_STAGED_PATH,
    manifest_dir: Path | None = None,
    duckdb_path: Path | None = None,
    now_utc: datetime | None = None,
    active_session: str | None = None,
    max_age_ms: int | None = None,
    executed_alert_ids: Iterable[str] | None = None,
    always_write: bool = False,
) -> dict[str, Any]:
    """Prune expired, legacy, and already-executed presets from staged_tv_presets.json.

    Archives pruned rows into tv_half_auto_audit.duckdb with explicit status:
      - EXECUTED_ARCHIVED: alert already submitted/filled in Paper Farm ledgers
      - LEGACY_PRUNED: missing or non-paper venue_profile (or invalid qty/confirmation)
      - EXPIRED_SESSION: cross-session alert or stale age beyond current trading day/window
    """
    eff_manifest_dir = (
        manifest_dir
        if manifest_dir is not None
        else (
            staged_path.parent.parent
            if staged_path.parent.name == "tv_sota_pack"
            else staged_path.parent
        )
    )
    eff_duckdb = (
        duckdb_path if duckdb_path is not None else _resolve_tv_audit_duckdb_path(staged_path)
    )
    ts_now = (now_utc or datetime.now(UTC)).astimezone(UTC)
    now_ms = int(ts_now.timestamp() * 1000)
    cur_sess = (
        active_session.strip().upper()
        if isinstance(active_session, str) and active_session.strip()
        else _eng.classify_us_equity_session_et(now_ms * 1_000_000)
    )

    pruned_counts: dict[str, int] = {
        "EXPIRED_SESSION": 0,
        "EXECUTED_ARCHIVED": 0,
        "LEGACY_PRUNED": 0,
    }
    if not staged_path.is_file():
        return {
            "ok": True,
            "staged_path": str(staged_path),
            "duckdb_path": str(eff_duckdb),
            "active_session": cur_sess,
            "kept_count": 0,
            "archived_count": 0,
            "pruned_counts": pruned_counts,
            "archived_alert_ids": [],
            "kept_alert_ids": [],
            "paper_only": True,
            "semi_auto_confirmed": False,
            "live_execution_enabled": False,
        }

    try:
        raw_doc = json.loads(staged_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unable to read staged presets manifest {staged_path}: {exc}") from exc
    raw_presets = raw_doc.get("presets", []) if isinstance(raw_doc, Mapping) else []
    exec_ids = _collect_executed_tv_alert_ids(eff_manifest_dir)
    if executed_alert_ids is not None:
        for tok in executed_alert_ids:
            if not isinstance(tok, str) or not tok.strip():
                continue
            extracted = _extract_tv_alert_id_from_token(tok)
            exec_ids.add(extracted or tok.strip().lower())

    kept: list[dict[str, Any]] = []
    archived_records: list[tuple[dict[str, Any], str]] = []
    archived_summary: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for raw_item in raw_presets:
        if not isinstance(raw_item, Mapping):
            continue
        item = dict(raw_item)
        aid = str(item.get("alert_id") or "").strip()
        aid_low = aid.lower()
        sym = str(item.get("symbol") or "").strip().upper()
        if aid_low and aid_low in seen_ids:
            continue
        if aid_low:
            seen_ids.add(aid_low)

        vp = str(item.get("venue_profile") or "").strip().upper()
        row_sess = str(item.get("session") or "").strip().upper()
        row_qty = item.get("qty")
        row_confirmed = item.get("semi_auto_confirmed")
        row_ts_ms = _opt_db_int(item.get("ts_utc_ms"))
        staged_dt = _parse_iso_utc(item.get("staged_at_utc"))

        archive_status: str | None = None
        if aid_low and aid_low in exec_ids:
            archive_status = "EXECUTED_ARCHIVED"
        elif (
            not vp
            or vp not in _PAPER_VENUE_PROFILES
            or row_confirmed is not False
            or type(row_qty) is not int
            or row_qty != 1
        ):
            archive_status = "LEGACY_PRUNED"
        elif (
            (cur_sess != "CLOSED" and row_sess != cur_sess)
            or (
                max_age_ms is not None and row_ts_ms is not None and now_ms - row_ts_ms > max_age_ms
            )
            or (
                staged_dt is not None
                and ts_now - staged_dt > timedelta(hours=_ARCHIVE_STALE_AGE_HOURS)
            )
        ):
            archive_status = "EXPIRED_SESSION"

        if archive_status is not None:
            pruned_counts[archive_status] += 1
            archived_records.append((item, archive_status))
            archived_summary.append({"alert_id": aid, "symbol": sym, "status": archive_status})
        else:
            kept.append(item)

    safe_to_archive: list[dict[str, Any]] = []
    archive_meta: dict[str, dict[str, Any]] = {}
    for rec, st in archived_records:
        aid = str(rec.get("alert_id") or "").strip()
        if not aid or not rec.get("symbol") or not rec.get("side") or not rec.get("session"):
            continue
        if (
            rec.get("semi_auto_confirmed") is not False
            or type(rec.get("qty")) is not int
            or rec.get("qty") != 1
        ):
            continue
        try:
            refuse_live_endpoints(rec)
        except ValueError:
            continue
        safe_to_archive.append(rec)
        archive_meta[aid] = {
            "status": st,
            "venue_profile": rec.get("venue_profile"),
            "archetype": rec.get("setup_archetype"),
            "pair_symbol": rec.get("pair_symbol"),
            "script_kind": rec.get("script_kind"),
            "order_preset_id": rec.get("order_preset_id"),
        }
    if safe_to_archive:
        persist_tv_presets_duckdb(
            eff_duckdb,
            safe_to_archive,
            source="tv_bridge_prune",
            meta_by_alert_id=archive_meta,
        )

    if archived_records or always_write:
        _atomic_write_json(
            staged_path,
            {
                "schema_version": "tier0.staged-tv-presets.v1",
                "updated_at_utc": ts_now.isoformat(),
                "last_pruned_at_utc": ts_now.isoformat(),
                "active_session": cur_sess,
                "paper_only": True,
                "semi_auto_confirmed": False,
                "live_execution_enabled": False,
                "pruned_counts": pruned_counts,
                "presets": kept,
            },
        )

    return {
        "ok": True,
        "staged_path": str(staged_path),
        "duckdb_path": str(eff_duckdb),
        "active_session": cur_sess,
        "kept_count": len(kept),
        "archived_count": len(archived_records),
        "pruned_counts": pruned_counts,
        "archived_alert_ids": archived_summary,
        "kept_alert_ids": [str(p.get("alert_id") or "") for p in kept],
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
    }


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

    resolved_vp = m.venue_profile or _resolve_default_paper_venue_profile(vu)
    session_presets = _build_staged_session_order_presets(
        symbol=m.symbol,
        side=route_side,
        ref_price=m.trigger_price,
        natr_pct=m.natr_pct,
        venue_profile=resolved_vp,
        wl_entry=wl_entry,
        min_trailing_pct=m.min_trailing_pct,
        pair_symbol=m.pair_symbol,
    )
    active_sp = session_presets[m.session]
    eff_min_trail = round(
        float(
            m.min_trailing_pct if m.min_trailing_pct is not None else active_sp["min_trailing_pct"]
        ),
        6,
    )
    r_locks: dict[str, Any] = {
        "be_plus_fee_price": active_sp["be_plus_fee_price"],
        "lock_at_1r": active_sp["lock_at_1r"],
        "lock_at_2r": active_sp["lock_at_2r"],
        "lock_at_3r": active_sp["lock_at_3r"],
        "fast_sell_non_noise_reverse_trigger": active_sp["fast_sell_non_noise_reverse_trigger"],
    }
    cal_cat = (
        m.calibrated_category
        or _opt_db_str((wl_entry or {}).get("calibrated_category"))
        or _opt_db_str((wl_entry or {}).get("potential_category"))
        or m.setup_archetype
    )
    ta_score = (
        m.technical_alignment_score
        if m.technical_alignment_score is not None
        else _opt_db_float((wl_entry or {}).get("technical_alignment_score"))
    )
    if ta_score is not None and not (0.0 <= ta_score <= 100.0):
        ta_score = None
    eff_rvol = (
        m.rvol
        if m.rvol is not None
        else _opt_db_float(
            (wl_entry or {}).get("rvol_normalized")
            if (wl_entry or {}).get("rvol_normalized") is not None
            else (wl_entry or {}).get("rvol")
        )
    )
    eff_trf = (
        m.darkpool_trf_ratio
        if m.darkpool_trf_ratio is not None
        else _opt_db_float((wl_entry or {}).get("darkpool_trf_ratio"))
    )
    reason_raw: dict[str, Any] = {
        k: v
        for k, v in (
            ("calibrated_category", cal_cat),
            ("setup_archetype", m.setup_archetype),
            ("rvol", round(eff_rvol, 3) if eff_rvol is not None else None),
            (
                "darkpool_trf_ratio",
                round(eff_trf, 4) if eff_trf is not None and 0.0 <= eff_trf <= 1.0 else None,
            ),
            ("technical_alignment_score", round(ta_score, 2) if ta_score is not None else None),
        )
        if v is not None
    }
    if m.momentum_trigger_reason:
        reason_raw = {**m.momentum_trigger_reason, **reason_raw}

    trig_norm = Decimal(str(float(m.trigger_price)))
    digest_src = (
        f"{m.symbol}:{m.ts_utc_ms}:{m.side}:{m.session}:{trig_norm}:{q_stop}:{m.trailing_schedule}"
    )
    staged: dict[str, Any] = {
        "alert_id": hashlib.sha256(digest_src.encode("utf-8")).hexdigest()[:16],
        "schema_version": m.schema_version,
        "symbol": m.symbol,
        "ts_utc_ms": m.ts_utc_ms,
        "bar_confirmed": True,
        "side": m.side,
        "qty": 1,
        "venue": vu,
        "venue_profile": resolved_vp,
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
        "min_trailing_pct": eff_min_trail,
        "execution_mechanism": _SESSION_EXEC_MECHANISM.get(
            m.session, "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"
        ),
        "auto_reverse_policy": m.auto_reverse_policy or "FLATTEN_FIRST_PEAK_CLIMAX_REVERSE",
        "r_lock_levels": r_locks,
        "session_order_presets": session_presets,
    }
    optional_fields: tuple[tuple[str, Any], ...] = (
        ("data_source", m.data_source),
        ("pair_symbol", m.pair_symbol),
        ("setup_archetype", m.setup_archetype),
        ("macro_regime", m.macro_regime),
        ("rvol", m.rvol),
        ("darkpool_trf_ratio", m.darkpool_trf_ratio),
        ("hidden_volume_ratio", m.hidden_volume_ratio),
        ("vol_buildup_ratio", m.vol_buildup_ratio),
        ("unusual_volume_buildup", m.unusual_volume_buildup),
        ("peak_confirmed", m.peak_confirmed),
        ("is_leveraged_or_inverse", m.is_leveraged_or_inverse),
        ("shortable", m.shortable),
        ("easy_to_borrow", m.easy_to_borrow),
        ("script_kind", m.script_kind),
        ("order_preset_id", m.order_preset_id),
        ("calibrated_category", cal_cat),
        ("technical_alignment_score", round(ta_score, 2) if ta_score is not None else None),
        ("momentum_trigger_reason", reason_raw or None),
    )
    for key, val in optional_fields:
        if val is not None:
            staged[key] = val
    return staged


def stage_tv_alert_preset(
    payload: Mapping[str, Any],
    *,
    staged_path: Path = DEFAULT_STAGED_PATH,
    watchlist_path: Path = DEFAULT_WATCHLIST_PATH,
    venue: str = "IBKR",
    live_intake: bool = False,
) -> dict[str, Any]:
    """Validate, prune stale/executed on live intake, deduplicate, and atomically stage preset."""
    wl_data = _load_watchlist_manifest(watchlist_path)
    preset = validate_tv_alert_payload(
        payload, watchlist_manifest=wl_data, venue=venue, live_intake=live_intake
    )
    if live_intake and staged_path.is_file():
        prune_and_archive_staged_presets(
            staged_path=staged_path,
            now_utc=datetime.fromtimestamp(time.time_ns() / 1_000_000_000, tz=UTC),
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
            "paper_only": True,
            "semi_auto_confirmed": False,
            "live_execution_enabled": False,
            "presets": list(by_id.values()),
        },
    )
    persist_tv_preset_duckdb(
        _resolve_tv_audit_duckdb_path(staged_path),
        preset,
        source="tv_bridge",
        status="ACTIVE_STAGED",
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
    """CLI entrypoint for validating, staging, and pruning TradingView alert JSON."""
    p = argparse.ArgumentParser(description="Tier-0 TradingView semi-auto alert bridge")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--ingest-file", type=Path)
    src.add_argument("--ingest-stdin", action="store_true")
    src.add_argument(
        "--prune-stale",
        action="store_true",
        help="Prune expired/legacy/executed alerts from staged_tv_presets.json into DuckDB audit.",
    )
    p.add_argument("--staged-path", type=Path, default=DEFAULT_PACK_STAGED_PATH)
    p.add_argument("--watchlist-path", type=Path, default=DEFAULT_WATCHLIST_PATH)
    p.add_argument("--manifest-dir", type=Path, default=None)
    p.add_argument("--venue", default="IBKR")
    p.add_argument("--live-intake", action="store_true")
    a = p.parse_args()
    if a.prune_stale:
        res = prune_and_archive_staged_presets(
            staged_path=a.staged_path,
            manifest_dir=a.manifest_dir,
            always_write=True,
        )
        print(json.dumps(res, indent=2))
        return
    if a.ingest_stdin:
        raw = json.loads(sys.stdin.read())
        items = raw if isinstance(raw, list) else [raw]
        staged = [
            stage_tv_alert_preset(
                cast(Mapping[str, Any], item),
                staged_path=a.staged_path,
                watchlist_path=a.watchlist_path,
                venue=a.venue,
                live_intake=a.live_intake,
            )
            for item in items
        ]
    else:
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

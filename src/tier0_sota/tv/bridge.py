"""Fail-closed TradingView Pine v6 alert bridge (Pydantic v2, semi_auto_confirmed=False)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Mapping, Sequence
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
DEFAULT_PACK_STAGED_PATH = DEFAULT_MANIFEST_DIR / "tv_sota_pack" / "staged_tv_presets.json"
DEFAULT_WATCHLIST_PATH = DEFAULT_MANIFEST_DIR / "live_watchlist_manifest.json"
DEFAULT_MOVERS_PATH = DEFAULT_MANIFEST_DIR / "live_whole_market_movers_manifest.json"
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
_ScriptKind = Literal["indicator", "strategy"]
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
)
_DUCKDB_INSERT_SQL = (
    "INSERT OR REPLACE INTO tv_staged_presets ("
    "alert_id, symbol, side, qty, venue, session, order_type, trigger_price, "
    "stop_price, limit_collar_bps, trailing_schedule, semi_auto_confirmed, "
    "data_source, source, staged_at_utc, setup_archetype, macro_regime, rvol, "
    "darkpool_trf_ratio, hidden_volume_ratio, vol_buildup_ratio, "
    "unusual_volume_buildup, peak_confirmed, pair_symbol, action_gate, "
    "reverse_short_mode, script_kind, order_preset_id) VALUES ("
    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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


def refuse_live_endpoints(payload: Mapping[str, Any]) -> None:
    """Reject any live broker host or forbidden live port marker in alert payloads."""
    raw = json.dumps(dict(payload), default=str).lower()
    live_alpaca = "alpaca" in raw and "paper-api" not in raw and "markets" in raw
    if (
        live_alpaca
        or _LIVE_ALPACA_RE.search(raw)
        or any(re.search(rf"(?<!\d){port}(?!\d)", raw) for port in FORBIDDEN_ENDPOINT_MARKERS)
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
) -> None:
    """Persist a schema-validated semi-auto TV preset (qty=1, semi_auto_confirmed=False)."""
    meta_map = {str(preset["alert_id"]): meta} if meta and "alert_id" in preset else None
    persist_tv_presets_duckdb(duckdb_path, [preset], source=source, meta_by_alert_id=meta_map)


def sync_tv_audit_duckdb_snapshot(duckdb_path: Path) -> bool:
    """Idempotently migrate an existing TV audit DuckDB to 28 cols and export Parquet snapshot."""
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
            total_row = conn.execute(
                "SELECT COUNT(DISTINCT alert_id) FROM tv_staged_presets"
            ).fetchone()
            by_src_rows = conn.execute(
                "SELECT COALESCE(source, 'unknown'), COUNT(DISTINCT alert_id) "
                "FROM tv_staged_presets GROUP BY 1 ORDER BY 1"
            ).fetchall()
            sym_rows = conn.execute(
                "SELECT DISTINCT symbol FROM tv_staged_presets "
                "WHERE symbol IS NOT NULL ORDER BY symbol"
            ).fetchall()
    except OSError:
        return empty
    return {
        "audit_rows": int(total_row[0]) if total_row and total_row[0] is not None else 0,
        "by_source": {str(src): int(cnt) for src, cnt in by_src_rows},
        "distinct_symbols": [str(r[0]) for r in sym_rows],
        "parquet_snapshot_present": duckdb_path.with_name("tv_staged_presets.parquet").is_file(),
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
    """Validate, deduplicate by alert_id, and atomically stage a semi-auto preset."""
    wl_data = _load_watchlist_manifest(watchlist_path)
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
    persist_tv_preset_duckdb(_resolve_tv_audit_duckdb_path(staged_path), preset, source="tv_bridge")
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
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--ingest-file", type=Path)
    src.add_argument("--ingest-stdin", action="store_true")
    p.add_argument("--staged-path", type=Path, default=DEFAULT_PACK_STAGED_PATH)
    p.add_argument("--watchlist-path", type=Path, default=DEFAULT_WATCHLIST_PATH)
    p.add_argument("--venue", default="IBKR")
    p.add_argument("--live-intake", action="store_true")
    a = p.parse_args()
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

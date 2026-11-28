"""One authenticated Paper Farm observation and reconciliation cycle for a native scheduler.

The same entry point starts the live lane's supervisor (``--supervise-live``) and its read-only
self-check (``--live-self-check``); the paper modes are unchanged by them.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
import weakref
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
from alpaca.common.enums import BaseURL, Sort
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockQuotesRequest
from alpaca.trading.requests import GetOrderByIdRequest, GetOrdersRequest

from tier0_sota import engine, risk
from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    IBKR_PAPER_ACCOUNT,
    PAPER_ALPACA_ENDPOINT,
    AlpacaPaperLane,
    IbkrPaperAdapter,
    PaperFarmCoordinator,
    _atomic_write_json,
    _scan_forbidden_live_ports,
    build_live_venue_coordinator,
    default_manifest_dir,
    ensure_tca_table,
    ensure_wire_table,
    open_bounded_duckdb,
    resolve_alpaca_live_env_creds,
    resolve_alpaca_paper_venue_profile,
    round_lot_up,
    select_situation_lane,
)
from tier0_sota.live.performance import aggregate_paper_farm_performance
from tier0_sota.live.scanner import (
    DEFAULT_PIN_SYMBOLS,
    ExtremeMoverScanner,
    build_live_lane_preset_bundle,
)
from tier0_sota.tv.tickets import refresh_tickets_from_paths

LOG = logging.getLogger(__name__)
NY = ZoneInfo("America/New_York")
_LEGACY_TWO_PIN_SET = frozenset({"SOXL", "SOXS"})
_DIRECT_SHORT_ETB_ORIGINS = frozenset(
    {"DIRECT_SHORT_ETB", "DIRECT_SHORT_ETB_REENTRY", "DIRECT_SHORT_ETB_AUTO_REVERSE"}
)
_IBKR_DU_ACCOUNT_RE = re.compile(r"^DU[0-9A-Z]{4,14}$")
_IBKR_CONTAINER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_IBC_DIALOG_ACCT_RE = re.compile(
    r"detected dialog entitled:\s*([A-Z][A-Z0-9]{4,14})\s+"
    r"Trader Workstation Configuration(?:\s*\(([^)]+)\))?"
)
_IBKR_HEALTH_CLIENT_ID = 97
_IBKR_HEALTH_TIMEOUT_SECS = 4.0
_IBKR_HEALTH_TTL_SECS_DEFAULT = 600.0
_IBKR_ERROR_BACKOFF_SECS_DEFAULT = 300.0
_IBKR_TRANSIENT_BACKOFF_PREFIXES = ("balance_timeout", "node_exited:")


def _is_transient_ibkr_probe_error(err_raw: str) -> bool:
    """Return True strictly for transient gateway handshake failures eligible for backoff."""
    if not err_raw or "permissionerror" in err_raw.lower():
        return False
    return err_raw == "node_exited" or err_raw.startswith(_IBKR_TRANSIENT_BACKOFF_PREFIXES)


def _parse_nonneg_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        val = float(raw)
    except ValueError:
        return default
    return val if val >= 0.0 else default


# Environment separation of the paper and the live process: names are matched, values never read
# into a message. The live process states its venue in TIER0_VENUE_PROFILE.
_LIVE_VENUE_ENV_NAME = "TIER0_VENUE_PROFILE"
_LIVE_KEY_NAME_PREFIX = "ALPACA_LIVE_"
_PAPER_ACCOUNT_KEY_NAME_PREFIX = "ALPACA_PA"
_PAPER_MODE_FLAG_NAME = "ALPACA_PAPER"
_PAPER_DATA_KEY_NAMES = frozenset({"APCA_API_KEY_ID", "APCA_API_SECRET_KEY"})
# Account conditions that gate entries only (R9). Protective supervision starts from positions
# and orders alone and never waits on any of them.
ENTRY_GATE_ACCOUNT_RESTRICTED = "ACCOUNT_RESTRICTED"
ENTRY_GATE_OPEN_ORDER_BOOK_FULL = "OPEN_ORDER_BOOK_FULL"
ENTRY_GATE_EQUITY_UNVERIFIED = "EQUITY_UNVERIFIED"
OPEN_ORDER_READ_LIMIT = 500
LIVE_SELF_CHECK_SCHEMA = "tier0.live-self-check.v1"
# Gates found by the latest ``refresh_accounts`` of each coordinator (account -> reasons).
_ENTRY_GATES: weakref.WeakKeyDictionary[PaperFarmCoordinator, dict[str, tuple[str, ...]]] = (
    weakref.WeakKeyDictionary()
)


def _validate_paper_du_account(raw: str) -> str:
    """Enforce paper-only DU* IBKR account prefix; fail-closed on any live account."""
    code = raw.strip().upper()
    if "-" in code:
        code = code.rsplit("-", 1)[-1]
    if not _IBKR_DU_ACCOUNT_RE.fullmatch(code):
        raise PermissionError("Refused non-DU IBKR account; paper DU* account required")
    return code


def _resolve_ibkr_paper_account_id() -> str | None:
    """Resolve authenticated paper DU* account code from env or local IBC paper logs."""
    for env_key in ("IBKR_ACCOUNT_ID", "TWS_ACCOUNT"):
        raw = os.environ.get(env_key, "").strip()
        if raw:
            return _validate_paper_du_account(raw)

    docker_bin = shutil.which("docker")
    container = os.environ.get("IBKR_GATEWAY_CONTAINER", "ibkr-paper").strip()
    if not docker_bin or not _IBKR_CONTAINER_RE.fullmatch(container):
        return None
    try:
        proc = subprocess.run(  # noqa: S603
            [docker_bin, "logs", container],
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    matches = _IBC_DIALOG_ACCT_RE.findall(f"{proc.stdout}\n{proc.stderr}")
    if not matches:
        return None
    acct_code, mode_tag = matches[-1]
    if mode_tag.strip().lower() != "simulated trading":
        raise PermissionError("Refused IBKR gateway session not in Simulated Trading mode")
    return _validate_paper_du_account(acct_code)


async def _probe_ibkr_paper_session_async(
    *,
    host: str,
    port: int,
    client_id: int,
    timeout_secs: float,
    account_id: str,
) -> dict[str, Any]:
    """Read-only NautilusTrader v2 LiveNode IBKR account balance handshake (ZERO orders)."""
    from nautilus_trader.adapters.interactive_brokers import (
        InteractiveBrokersExecutionClientConfig,
        InteractiveBrokersExecutionClientFactory,
    )
    from nautilus_trader.common import Environment, LoggerConfig, LogLevel
    from nautilus_trader.live import LiveNode
    from nautilus_trader.model import Currency, TraderId, Venue

    timeout_i = max(1, min(4, int(round(timeout_secs))))
    usd = Currency.from_str("USD")
    cfg = InteractiveBrokersExecutionClientConfig(
        host=host,
        port=port,
        client_id=client_id,
        account_id=account_id,
        base_currency=usd,
        connection_timeout=timeout_i,
        request_timeout=timeout_i,
        fetch_all_open_orders=False,
        max_reconnect_attempts=0,
    )
    node = (
        LiveNode.builder(
            "ibkr-health-probe", TraderId(f"TIER0-{client_id:03d}"), Environment.SANDBOX
        )
        .with_logging(LoggerConfig(stdout_level=LogLevel.ERROR, bypass_logging=True))
        .with_reconciliation(False)
        .with_timeout_connection(timeout_i)
        .with_timeout_reconciliation(1)
        .with_timeout_portfolio(1)
        .with_timeout_disconnection_secs(1)
        .with_delay_post_stop_secs(0)
        .with_delay_shutdown_secs(0)
        .add_exec_client("INTERACTIVE_BROKERS", InteractiveBrokersExecutionClientFactory(), cfg)
        .build()
    )
    cache = node.cache
    handle = node.handle()
    task = asyncio.create_task(node.run_async())
    venue = Venue("INTERACTIVE_BROKERS")
    steps = max(1, int(timeout_secs / 0.05))
    live_acct: Any = None
    balances: Any = None
    bal: Any = None
    try:
        for _ in range(steps):
            if task.done():
                exc = task.exception() if not task.cancelled() else None
                reason = f"node_exited:{exc}" if exc is not None else "node_exited"
                exc = None
                return {
                    "authenticated": False,
                    "all_du_paper_accounts": True,
                    "account_id": None,
                    "equity": None,
                    "tcp_reachable": False,
                    "reason": reason,
                }
            await asyncio.sleep(0.05)
            live_acct = cache.account_for_venue(venue)
            if live_acct is None:
                continue
            balances = live_acct.balances()
            if not balances:
                continue
            du_code = _validate_paper_du_account(str(live_acct.id))
            bal = balances.get(usd) or next(iter(balances.values()))
            equity = round(float(bal.total), 2)
            if equity > 0:
                return {
                    "authenticated": True,
                    "all_du_paper_accounts": True,
                    "account_id": du_code,
                    "equity": equity,
                    "tcp_reachable": True,
                    "redacted_account": "DU***",
                }
        return {
            "authenticated": False,
            "all_du_paper_accounts": True,
            "account_id": None,
            "equity": None,
            "reason": "balance_timeout",
        }
    finally:
        if not task.done():
            handle.stop()
            try:
                await asyncio.wait_for(task, timeout=3.0)
            except (TimeoutError, asyncio.CancelledError, RuntimeError):
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(TimeoutError, asyncio.CancelledError, RuntimeError):
                        await asyncio.wait_for(task, timeout=1.0)
        # Drop all !Send PyO3 references before coroutine exit so gc.collect() on the
        # creating thread reclaims PyNodeRun/PyLiveNode/PyCache cycles deterministically.
        del cache, handle, node, task, cfg, venue, usd, live_acct, balances, bal


def _probe_ibkr_paper_session(
    *,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = _IBKR_HEALTH_CLIENT_ID,
    timeout_secs: float = _IBKR_HEALTH_TIMEOUT_SECS,
    account_id: str | None = None,
) -> dict[str, Any]:
    """Bounded read-only paper-only IBKR handshake via native NautilusTrader v2."""
    if host != "127.0.0.1" or port != 4002:
        raise PermissionError("IBKR health probe requires loopback 127.0.0.1:4002 only")
    du_acct = (
        _validate_paper_du_account(account_id)
        if account_id is not None
        else _resolve_ibkr_paper_account_id()
    )
    if du_acct is None:
        return {
            "authenticated": False,
            "all_du_paper_accounts": False,
            "equity": None,
            "reason": "du_account_unresolved",
        }

    def _run_probe() -> dict[str, Any]:
        try:
            return asyncio.run(
                _probe_ibkr_paper_session_async(
                    host=host,
                    port=port,
                    client_id=client_id,
                    timeout_secs=timeout_secs,
                    account_id=du_acct,
                )
            )
        finally:
            # NautilusTrader PyLiveNode/PyNodeRun/PyCache are !Send PyO3 objects;
            # collect cyclic references on the same creating thread before returning.
            gc.collect()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_probe()
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_run_probe).result(timeout=timeout_secs + 4.0)


def _resolve_pin_symbols(raw: str) -> tuple[str, ...]:
    """Upgrade empty or legacy 2-pin CLI overrides to 4-symbol DEFAULT_PIN_SYMBOLS."""
    parsed = tuple(dict.fromkeys(tok.strip().upper() for tok in raw.split(",") if tok.strip()))
    if not parsed or set(parsed) == _LEGACY_TWO_PIN_SET:
        return DEFAULT_PIN_SYMBOLS
    return parsed


def _assert_risk_kernel_digest(worker: str) -> None:
    with Path(risk.__file__).open("rb") as kernel:
        if hashlib.file_digest(kernel, "sha256").hexdigest() != (
            "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
        ):
            raise PermissionError(f"{worker} worker risk kernel digest mismatch")


def _live_key_names(env: Mapping[str, str]) -> list[str]:
    """Names (never values) of live-credential variables: any ``ALPACA_LIVE_*`` name."""
    return sorted(name for name in env if name.upper().startswith(_LIVE_KEY_NAME_PREFIX))


def _paper_key_names(env: Mapping[str, str]) -> list[str]:
    """Names (never values) of paper-credential variables in ``env``.

    The paper market-data pair and every ``ALPACA_PA*`` name. ``ALPACA_PAPER`` is the paper-mode
    flag, not a key: its value is judged separately.
    """
    return sorted(
        name
        for name in env
        if name.upper() in _PAPER_DATA_KEY_NAMES
        or (
            name.upper().startswith(_PAPER_ACCOUNT_KEY_NAME_PREFIX)
            and name.upper() != _PAPER_MODE_FLAG_NAME
        )
    )


def verify_paper_environment() -> None:
    _assert_risk_kernel_digest("Paper")
    if (
        os.environ.get("ALPACA_PAPER", "").lower() != "true"
        or os.environ.get("LIVE_EXECUTION_ENABLED", "false").lower() != "false"
        or os.environ.get("IBKR_HOST") != "127.0.0.1"
        or os.environ.get("IBKR_PORT") != "4002"
        or os.environ.get("APCA_API_BASE_URL", PAPER_ALPACA_ENDPOINT).rstrip("/")
        != PAPER_ALPACA_ENDPOINT
    ):
        raise PermissionError("Paper profile or loopback broker isolation check failed")
    # A paper process never holds the live credential, dormant or not: only names are reported.
    if live_names := _live_key_names(os.environ):
        raise PermissionError(
            "Paper profile check failed: live key name(s) present: " + ", ".join(live_names)
        )
    for acct in ALPACA_LANE_PROFILES:
        if not os.environ.get(f"{acct}_API_KEY", "").startswith("PK") or not (
            os.environ.get(f"{acct}_SECRET") or os.environ.get(f"{acct}_SECRET_KEY")
        ):
            raise PermissionError(f"Missing account-specific paper credentials for {acct}")


def verify_live_environment(env: Mapping[str, str] | None = None) -> bool:
    """Fail closed unless this process is the live lane's; return the owner's tier-3 opt-in.

    Sibling of ``verify_paper_environment``. Every environment value is read from ``env``
    (default: the process environment); the risk digest and the port scan are host facts.
    Checked, in this order: the pinned ``risk.py`` digest; ``TIER0_VENUE_PROFILE`` names the
    tiered live venue and the engine grants it the protective tier; the live credential
    (``AK`` key id and its secret) is set; no paper key name is present (the paper market-data
    pair, any ``ALPACA_PA*`` name) and ``ALPACA_PAPER`` is unset or false; ``APCA_API_BASE_URL``
    is unset or alpaca-py's own live trading endpoint; ``IBKR_PORT`` names no IBKR live port and
    none is open on this host. A refusal names variables, never a value.

    The return value is ``LIVE_EXECUTION_ENABLED`` read strictly: ``true`` is the owner's
    standing opt-in to scanner-originated (tier-3) entries, unset or ``false`` is off, and
    anything else refuses. Tiers 1 and 2 do not depend on it.
    """
    source: Mapping[str, str] = os.environ if env is None else env
    _assert_risk_kernel_digest("Live")
    venue = str(source.get(_LIVE_VENUE_ENV_NAME) or "").strip().upper()
    if venue != engine.LIVE_TIERED_VENUE_ID:
        raise PermissionError(
            f"Live profile check failed: {_LIVE_VENUE_ENV_NAME} must be "
            f"{engine.LIVE_TIERED_VENUE_ID}"
        )
    profile = engine.validate_venue_order_capability(venue, intent="PROTECTIVE")
    if profile.broker != "ALPACA" or profile.environment != "LIVE":
        raise PermissionError("Live profile check failed: the tiered venue is not Alpaca live")
    resolve_alpaca_live_env_creds(source)
    if paper_names := _paper_key_names(source):
        raise PermissionError(
            "Live profile check failed: paper key name(s) present: " + ", ".join(paper_names)
        )
    if str(source.get(_PAPER_MODE_FLAG_NAME) or "").strip().lower() not in ("", "false"):
        raise PermissionError(
            f"Live profile check failed: {_PAPER_MODE_FLAG_NAME} must be unset or false"
        )
    base_url = str(source.get("APCA_API_BASE_URL") or "").strip().rstrip("/").lower()
    if base_url and base_url != str(BaseURL.TRADING_LIVE.value).rstrip("/").lower():
        raise PermissionError(
            "Live profile check failed: APCA_API_BASE_URL must be unset or alpaca-py's own "
            "live trading endpoint"
        )
    ibkr_port = str(source.get("IBKR_PORT") or "").strip()
    if ibkr_port.isdecimal() and int(ibkr_port) in engine.FORBIDDEN_LIVE_IBKR_PORTS:
        raise PermissionError("Live profile check failed: IBKR_PORT names an IBKR live port")
    if open_ports := _scan_forbidden_live_ports():
        raise PermissionError(
            "Live profile check failed: IBKR live port(s) open on this host: "
            + ", ".join(str(port) for port in open_ports)
        )
    opt_in = str(source.get("LIVE_EXECUTION_ENABLED") or "").strip().lower()
    if opt_in not in ("", "false", "true"):
        raise PermissionError(
            "Live profile check failed: LIVE_EXECUTION_ENABLED must be true or false"
        )
    return opt_in == "true"


def _persist_ibkr_health_status(
    adapter: IbkrPaperAdapter,
    *,
    gateway_ready: bool,
    tcp_reachable: bool,
    equity: float | None,
    account_id: str | None = None,
    probe_error: str | None = None,
    probe_checked_at_utc: str | None = None,
    heartbeat_utc: str | None = None,
) -> dict[str, Any]:
    payload = adapter.write_status(
        gateway_ready=gateway_ready,
        tcp_reachable=tcp_reachable,
        equity=equity if gateway_ready else None,
    )
    checked_iso = probe_checked_at_utc or str(payload["generated_at_utc"])
    hb_iso = heartbeat_utc or str(payload["generated_at_utc"])
    enriched = payload | {
        "account_id": account_id if gateway_ready else None,
        "probe_client_id": _IBKR_HEALTH_CLIENT_ID,
        "probe_error": None if gateway_ready else probe_error,
        "probe_checked_at_utc": checked_iso,
        "heartbeat_utc": hb_iso,
    }
    _atomic_write_json(adapter.status_path, enriched)
    return enriched


def _try_reuse_ibkr_health_status(adapter: IbkrPaperAdapter, *, now: datetime) -> bool | None:
    """Reuse fresh verified IBKR ready status or honor transient failure backoff without probing."""
    prev = _read_json_dict(adapter.status_path)
    if (
        prev.get("schema_version") != "tier0.ibkr-paper-lane.v1"
        or prev.get("paper_only") is not True
        or prev.get("live_execution_enabled") is not False
        or prev.get("endpoint") != f"ibkr://127.0.0.1:{adapter.port}"
    ):
        return None
    checked_raw = prev.get("probe_checked_at_utc") or prev.get("generated_at_utc")
    if not isinstance(checked_raw, str) or not checked_raw.strip():
        return None
    try:
        checked_dt = datetime.fromisoformat(checked_raw)
        if checked_dt.tzinfo is None:
            checked_dt = checked_dt.replace(tzinfo=UTC)
    except ValueError:
        return None
    age_secs = (now - checked_dt).total_seconds()
    if age_secs < 0.0:
        return None

    if prev.get("ready") is True:
        ttl_secs = _parse_nonneg_float_env(
            "TIER0_IBKR_HEALTH_TTL_SECS", _IBKR_HEALTH_TTL_SECS_DEFAULT
        )
        if ttl_secs <= 0.0 or age_secs >= ttl_secs:
            return None
        raw_acct = prev.get("account_id")
        raw_eq = prev.get("equity")
        if raw_acct in (None, "") or not (
            isinstance(raw_eq, (int, float)) and not isinstance(raw_eq, bool) and float(raw_eq) > 0
        ):
            return None
        try:
            acct_code = _validate_paper_du_account(str(raw_acct))
        except PermissionError:
            return None
        _persist_ibkr_health_status(
            adapter,
            gateway_ready=True,
            tcp_reachable=True,
            equity=round(float(raw_eq), 2),
            account_id=acct_code,
            probe_error=None,
            probe_checked_at_utc=checked_dt.isoformat(),
            heartbeat_utc=now.isoformat(),
        )
        return True

    err_raw = str(prev.get("probe_error") or "")
    if prev.get("ready") is False and _is_transient_ibkr_probe_error(err_raw):
        backoff_secs = _parse_nonneg_float_env(
            "TIER0_IBKR_ERROR_BACKOFF_SECS", _IBKR_ERROR_BACKOFF_SECS_DEFAULT
        )
        if backoff_secs > 0.0 and age_secs < backoff_secs:
            LOG.info(
                "IBKR :4002 transient probe backoff active (%s, %.0fs/%.0fs); "
                "skipping Client %d probe",
                err_raw,
                age_secs,
                backoff_secs,
                _IBKR_HEALTH_CLIENT_ID,
            )
            return True
    return None


def ibkr_health(
    adapter: IbkrPaperAdapter,
    *,
    probe_fn: Callable[..., dict[str, Any]] | None = None,
) -> bool:
    """Verify pinned IBKR :4002 paper socket + read-only DU* account handshake (fail-closed)."""
    if adapter.port != 4002 or not adapter.paper_only or adapter.live_execution_enabled:
        raise PermissionError("IBKR health requires the paper gateway on loopback port 4002")
    env_tcp = os.environ.get("IBKR_PAPER_TCP_REACHABLE")
    if env_tcp == "0":
        _persist_ibkr_health_status(
            adapter,
            gateway_ready=False,
            tcp_reachable=False,
            equity=None,
            probe_error="tcp_unreachable",
        )
        return False
    tcp_hint = env_tcp == "1"
    if probe_fn is None and tcp_hint:
        cached = _try_reuse_ibkr_health_status(adapter, now=datetime.now(UTC))
        if cached is not None:
            return cached

    probe = probe_fn or _probe_ibkr_paper_session
    try:
        res = probe(
            host="127.0.0.1",
            port=adapter.port,
            client_id=_IBKR_HEALTH_CLIENT_ID,
            timeout_secs=_IBKR_HEALTH_TIMEOUT_SECS,
        )
    except PermissionError as exc:
        LOG.error("IBKR :4002 paper isolation refusal: %s", exc)
        _persist_ibkr_health_status(
            adapter,
            gateway_ready=False,
            tcp_reachable=tcp_hint,
            equity=None,
            probe_error=f"{type(exc).__name__}: {exc}",
        )
        raise
    except Exception as exc:  # noqa: BLE001
        LOG.warning("IBKR :4002 read-only paper handshake failed: %s", exc)
        _persist_ibkr_health_status(
            adapter,
            gateway_ready=False,
            tcp_reachable=tcp_hint,
            equity=None,
            probe_error=f"{type(exc).__name__}: {exc}",
        )
        return tcp_hint

    if not isinstance(res, dict):
        LOG.warning("IBKR :4002 paper handshake returned invalid payload type: %r", type(res))
        _persist_ibkr_health_status(
            adapter,
            gateway_ready=False,
            tcp_reachable=tcp_hint,
            equity=None,
            probe_error="invalid_probe_payload",
        )
        return tcp_hint

    raw_acct = res.get("account_id")
    verified_acct: str | None = None
    if raw_acct not in (None, ""):
        try:
            verified_acct = _validate_paper_du_account(str(raw_acct))
        except PermissionError as exc:
            LOG.error("IBKR :4002 refused non-DU account_id: %s", exc)
            _persist_ibkr_health_status(
                adapter,
                gateway_ready=False,
                tcp_reachable=bool(tcp_hint or res.get("tcp_reachable") is True),
                equity=None,
                probe_error=f"{type(exc).__name__}: {exc}",
            )
            raise

    if res.get("live_account_detected") is True or (
        res.get("authenticated") is True and res.get("all_du_paper_accounts") is not True
    ):
        _persist_ibkr_health_status(
            adapter,
            gateway_ready=False,
            tcp_reachable=bool(tcp_hint or res.get("tcp_reachable") is True),
            equity=None,
            probe_error="non_du_account_refused",
        )
        raise PermissionError("Refused non-DU IBKR account; paper DU* account required")

    raw_eq = res.get("equity")
    eq_val = (
        round(float(raw_eq), 2)
        if isinstance(raw_eq, (int, float)) and not isinstance(raw_eq, bool) and float(raw_eq) > 0
        else None
    )
    gateway_ready = (
        res.get("authenticated") is True
        and res.get("all_du_paper_accounts") is True
        and eq_val is not None
    )
    tcp_ok = bool(gateway_ready or tcp_hint or res.get("tcp_reachable") is True)
    probe_reason = str(res.get("reason") or "handshake_unverified") if not gateway_ready else None
    if not gateway_ready:
        LOG.warning("IBKR :4002 paper handshake unverified: %s", probe_reason)
    _persist_ibkr_health_status(
        adapter,
        gateway_ready=gateway_ready,
        tcp_reachable=tcp_ok,
        equity=eq_val if gateway_ready else None,
        account_id=verified_acct if gateway_ready else None,
        probe_error=probe_reason,
    )
    return tcp_ok


def midpoint_asof(data: Any, symbol: str, at: datetime) -> Decimal | None:
    session = engine.classify_us_equity_session_et(int(at.timestamp() * 1e9))
    quotes = data.get_stock_quotes(
        StockQuotesRequest(
            symbol_or_symbols=symbol,
            start=at - timedelta(seconds=5),
            end=at,
            limit=1,
            sort=Sort.DESC,
            feed=DataFeed.BOATS if session == "OVERNIGHT" else DataFeed.SIP,
        )
    ).data.get(symbol, [])
    if not quotes:
        return None
    quote = quotes[0]
    if (
        not 0 < quote.bid_price <= quote.ask_price
        or not 0 <= (at - quote.timestamp).total_seconds() <= 5
    ):
        return None
    return (Decimal(str(quote.bid_price)) + Decimal(str(quote.ask_price))) / 2


def refresh_markouts(lane: AlpacaPaperLane, data: Any) -> None:
    with open_bounded_duckdb(lane.duckdb_path) as conn:
        pending = conn.execute(
            "SELECT execution_id,symbol,side,fill_price,CAST(ts_utc AS TIMESTAMPTZ) "
            "FROM paper_tca_fills WHERE execution_id IS NOT NULL AND markout_60s_bps IS NULL "
            "AND CAST(ts_utc AS TIMESTAMPTZ) <= now()-INTERVAL '60 seconds'"
        ).fetchall()
    updates: list[tuple[Decimal, str, str]] = []
    for eid, symbol, side, price, filled in pending:
        mid = midpoint_asof(data, symbol, filled + timedelta(seconds=60))
        if mid is not None:
            sign = 1 if side == "BUY" else -1
            updates.append((sign * (mid - price) / price * 10000, lane.account, eid))
    if updates:
        with open_bounded_duckdb(lane.duckdb_path) as conn:
            conn.executemany(
                "UPDATE paper_tca_fills SET markout_60s_bps=? WHERE account=? AND execution_id=?",
                updates,
            )


def exit_context(lane: AlpacaPaperLane, order: Any, decisions: dict, data: Any) -> tuple | None:
    """Prove exit ownership by its parent or replacement ancestry before attributing a fill."""
    broker = lane.trading_client
    parents = [cid for cid, ctx in decisions.items() if ctx[0] == order.symbol]
    if not parents:
        return None
    ancestor = order
    seen = set()
    while ancestor.replaces:
        if ancestor.id in seen:
            raise ValueError("Cyclic paper order replacement chain")
        seen.add(ancestor.id)
        try:
            ancestor = broker.get_order_by_id(ancestor.replaces)
        except APIError as exc:
            if exc.status_code == 404:
                return None
            raise
    for cid in parents:
        if str(cid).startswith("adopted-"):
            base_id = f"exit-{cid}"
            synthetic = order.client_order_id == base_id or (
                order.client_order_id.startswith(base_id + "-")
                and order.client_order_id.removeprefix(base_id + "-").isdecimal()
            )
            if not synthetic:
                continue
            with open_bounded_duckdb(lane.duckdb_path) as db:
                row = db.execute(
                    "SELECT active_stop FROM paper_entry_decisions WHERE client_order_id=?", [cid]
                ).fetchone()
            decision = row[0] if row else None
            submitted = order.submitted_at
            if decision is None or submitted is None or data is None:
                raise ValueError("Owned exit lacks a decision or arrival benchmark")
            session = engine.classify_us_equity_session_et(int(submitted.timestamp() * 1e9))
            mid = midpoint_asof(data, order.symbol, submitted)
            if mid is None:
                raise ValueError("Owned exit awaits a causal quote at broker submission")
            side = "LONG" if order.side.value == "buy" else "SHORT"
            return (
                order.symbol,
                side,
                Decimal(str(decision)),
                mid,
                session,
                "PROTECTIVE_EXIT",
                Decimal(0),
                None,
                submitted,
            )
        try:
            parent = broker.get_order_by_client_id(cid)
            parent = broker.get_order_by_id(parent.id, GetOrderByIdRequest(nested=True))
        except APIError as exc:
            if exc.status_code == 404:
                continue
            raise
        if parent.side == order.side:
            continue
        base_id = f"exit-{parent.id}"
        synthetic = order.client_order_id == base_id or (
            order.client_order_id.startswith(base_id + "-")
            and order.client_order_id.removeprefix(base_id + "-").isdecimal()
        )
        legs = parent.legs or []
        if not synthetic and not any(leg.id in {order.id, ancestor.id, *seen} for leg in legs):
            continue
        if synthetic:
            with open_bounded_duckdb(lane.duckdb_path) as db:
                row = db.execute(
                    "SELECT active_stop FROM paper_entry_decisions WHERE client_order_id=?", [cid]
                ).fetchone()
            decision = row[0] if row else None
            ref_ts = order.submitted_at
        else:
            decision = order.stop_price
            filled_ts = getattr(order, "filled_at", None)
            ref_ts = filled_ts if isinstance(filled_ts, datetime) else order.submitted_at
        if decision is None or ref_ts is None or data is None:
            raise ValueError("Owned exit lacks a decision or arrival benchmark")
        session = engine.classify_us_equity_session_et(int(ref_ts.timestamp() * 1e9))
        mid = midpoint_asof(data, order.symbol, ref_ts)
        if mid is None:
            raise ValueError("Owned exit awaits a causal quote at broker submission")
        side = "LONG" if order.side.value == "buy" else "SHORT"
        return (
            order.symbol,
            side,
            Decimal(str(decision)),
            mid,
            session,
            "PROTECTIVE_EXIT",
            Decimal(0),
            None,
            ref_ts,
        )
    return None


def reconcile_fills(lane: AlpacaPaperLane, data: Any = None) -> int:
    """Replay broker executions against recorded entry decisions, including partial fills."""
    with open_bounded_duckdb(lane.duckdb_path) as conn:
        if ("paper_entry_decisions",) not in conn.execute("SHOW TABLES").fetchall():
            return 0
        decisions = {
            row[0]: row[1:]
            for row in conn.execute(
                "SELECT client_order_id,symbol,side,decision_price,arrival_price,session,"
                "origin,annual_rate,prior_close,decided_at FROM paper_entry_decisions"
            ).fetchall()
        }
        conn.execute(
            "CREATE TABLE IF NOT EXISTS paper_activity_cursor "
            "(account VARCHAR PRIMARY KEY, page_token VARCHAR NOT NULL)"
        )
        cursor = conn.execute(
            "SELECT page_token FROM paper_activity_cursor WHERE account=?", [lane.account]
        ).fetchone()
    if not decisions:
        return 0
    broker = lane.trading_client
    params = {
        "after": min(d[-1] for d in decisions.values()).isoformat(),
        "direction": "asc",
        "page_size": "100",
    }
    if cursor:
        params["page_token"] = cursor[0]
    initial = lane.counters["fills"]
    while activities := broker.get("/account/activities/FILL", params):
        for fill in activities:
            order = broker.get_order_by_id(fill["order_id"])
            context = decisions.get(order.client_order_id)
            if context is None:
                context = exit_context(lane, order, decisions, data)
                if context is None:
                    continue
            sym, side, decision, arrival, session, origin, rate, prior_close, _ = context
            expected = "buy" if side == "LONG" else "sell"
            if (
                fill["symbol"] != sym
                or order.side.value != expected
                or fill["side"].removesuffix("_short") != expected
                or not fill["id"]
            ):
                raise ValueError("Broker execution disagrees with stored entry decision")
            qty = Decimal(fill["qty"])
            cumulative = Decimal(fill["cum_qty"])
            if not 0 < qty <= cumulative:
                raise ValueError("Invalid broker execution quantity")
            if (
                side == "SHORT"
                and origin not in _DIRECT_SHORT_ETB_ORIGINS
                and origin != "PROTECTIVE_EXIT"
            ):
                raise ValueError("Short borrow evidence missing")
            # ETB Trading API shares have zero borrow fee. Never infer an HTB rate.
            # Use executed shares for each fill slice; each activity has its own price.
            lane.record_broker_fill(
                execution_id=fill["id"],
                broker_order_id=fill["order_id"],
                symbol=sym,
                side=side,
                order_qty=qty,
                filled_qty=qty,
                decision_price=decision,
                arrival_price=arrival,
                fill_price=Decimal(fill["price"]),
                borrow_tier=(
                    "ETB"
                    if origin in _DIRECT_SHORT_ETB_ORIGINS
                    else "INVERSE_ETF"
                    if origin.startswith("INVERSE_HEDGE_")
                    else "NONE"
                ),
                annual_borrow_rate=rate,
                prior_close=prior_close,
                borrow_qty=round_lot_up(cumulative) - round_lot_up(cumulative - qty),
                session=session,
                ts_utc=fill["transaction_time"],
                protective_stop_mode=(
                    "NATIVE_OTO_STOP" if session == "RTH" else "COLLARED_STOP_LIMIT_SUPERVISED"
                ),
                trade_date=datetime.fromisoformat(fill["transaction_time"]).astimezone(NY).date(),
            )
        token = activities[-1]["id"]
        if params.get("page_token") == token:
            raise RuntimeError("Alpaca returned a repeated activity page")
        params["page_token"] = token
        with open_bounded_duckdb(lane.duckdb_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO paper_activity_cursor VALUES (?,?)", [lane.account, token]
            )
        if len(activities) < 100:
            break
    return lane.counters["fills"] - initial


def _fetch_lane_broker_snapshot(lane: AlpacaPaperLane) -> tuple[Any, list[Any], list[Any]]:
    broker = lane.trading_client
    account = broker.get_account()
    positions = broker.get_all_positions()
    orders = broker.get_orders(GetOrdersRequest(limit=OPEN_ORDER_READ_LIMIT))
    return account, positions, orders


def _finite_decimal(raw: Any) -> Decimal | None:
    """``raw`` as a finite Decimal; ``None`` when it states no number."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() else None


def _positive_decimal(raw: Any) -> Decimal | None:
    """``raw`` as a finite Decimal above zero; ``None`` for anything else."""
    value = _finite_decimal(raw)
    return value if value is not None and value > 0 else None


def _account_entry_gates(account: Any, open_orders: list[Any]) -> tuple[str, ...]:
    """Why this account must take no new entry right now (R9); empty when it may.

    A restricted account (not ACTIVE, blocked or suspended), an open-order read that came back
    full (the inventory cannot be attested complete) and an equity or prior-day equity that is
    not a positive number each gate entries. None of them says anything against protecting the
    positions the account already holds.
    """
    gates: list[str] = []
    status = getattr(account, "status", None)
    if (
        str(getattr(status, "value", status) or "").strip().upper() != "ACTIVE"
        or getattr(account, "trading_blocked", None) is not False
        or getattr(account, "account_blocked", None) is not False
        or getattr(account, "trade_suspended_by_user", None) is True
    ):
        gates.append(ENTRY_GATE_ACCOUNT_RESTRICTED)
    if len(open_orders) >= OPEN_ORDER_READ_LIMIT:
        gates.append(ENTRY_GATE_OPEN_ORDER_BOOK_FULL)
    if (
        _positive_decimal(getattr(account, "equity", None)) is None
        or _positive_decimal(getattr(account, "last_equity", None)) is None
    ):
        gates.append(ENTRY_GATE_EQUITY_UNVERIFIED)
    return tuple(gates)


def account_entry_gates(farm: PaperFarmCoordinator) -> dict[str, tuple[str, ...]]:
    """Entry gates found by the latest ``refresh_accounts`` of ``farm``: account -> reasons.

    Empty when every required account was healthy, or when ``farm`` was never refreshed.
    """
    return dict(_ENTRY_GATES.get(farm) or {})


def refresh_accounts(
    farm: PaperFarmCoordinator,
    now: datetime,
    session: str,
    *,
    publish: bool = True,
    data_client: Any = None,
    record_fuse: bool = True,
) -> dict[str, dict[str, Any]]:
    """Read every required account and return its positions (account -> symbol -> position).

    Raises when the coordinator's own shape is wrong: a lane set that differs from
    ``farm.required_accounts``, an account without an id or the same account behind two lanes,
    one symbol held on two lanes, or a ledger that belongs to another broker account.

    An account that is restricted, whose open-order read came back full, or whose equity or
    prior-day equity is not a positive number gates **entries only** (R9): this function
    returns the positions all the same, so protective supervision starts from positions and
    orders alone. While any required account is gated, no lane of the coordinator admits an
    entry: every lane's breaker reads tripped in memory (the gate every entry path already
    asks, ``breaker.allow_entry()``) and ``equity_verified_at`` is ``None`` (the hold a live
    lane's choke point asks). Neither is written to ``paper_daily_fuse``, so the gate lifts on
    the first refresh that finds every account healthy. ``account_entry_gates`` states why.
    """
    if set(farm.lanes) != set(farm.required_accounts):
        if tuple(farm.required_accounts) == tuple(ALPACA_LANE_PROFILES):
            raise PermissionError("All three Alpaca paper accounts are required")
        raise PermissionError(
            "Coordinator lanes do not match its required accounts: "
            + ", ".join(sorted(farm.required_accounts))
        )
    lanes_list = list(farm.lanes.values())
    distinct_clients = len({id(lane.trading_client) for lane in lanes_list}) == len(lanes_list)
    if distinct_clients and len(lanes_list) > 1:
        with ThreadPoolExecutor(max_workers=len(lanes_list)) as pool:
            snapshots = list(pool.map(_fetch_lane_broker_snapshot, lanes_list))
    else:
        snapshots = [_fetch_lane_broker_snapshot(lane) for lane in lanes_list]

    ids = set()
    inventory = {}
    owners: dict[str, str] = {}
    gates: dict[str, tuple[str, ...]] = {}
    for lane, (account, positions, orders) in zip(lanes_list, snapshots, strict=True):
        if not account.id or account.id in ids:
            raise PermissionError(f"{lane.account} is unavailable, blocked or duplicated")
        ids.add(account.id)
        for sym in {o.symbol for o in [*positions, *orders]}:
            if owners.setdefault(sym, lane.account) != lane.account:
                raise PermissionError(f"Overlapping paper exposure for {sym}")
        if reasons := _account_entry_gates(account, orders):
            gates[lane.account] = reasons
    entries_gated = bool(gates)
    if gates != (_ENTRY_GATES.get(farm) or {}):
        if gates:
            LOG.warning("Entries gated by account state, protective supervision goes on: %s", gates)
        else:
            LOG.warning("Entry gate lifted: every required account is healthy")
    _ENTRY_GATES[farm] = gates

    shared_data = data_client
    for lane, (account, positions, _orders) in zip(lanes_list, snapshots, strict=True):
        inventory[lane.account] = {p.symbol: p for p in positions}
        equity = _positive_decimal(account.equity)
        yesterday = _positive_decimal(account.last_equity)
        if equity is not None:
            lane.start_equity = equity
        pnl: Decimal | None = None
        if equity is not None and yesterday is not None:
            pnl = equity - yesterday
            lane.breaker = risk.DailyDrawdownBreaker(start_equity=yesterday)
        else:
            # Without both figures there is no day's PnL: the breaker stands on the last equity
            # the lane knew, and the gate below holds entries.
            lane.breaker = risk.DailyDrawdownBreaker(start_equity=lane.start_equity)
        day_et = now.astimezone(NY).date()
        day = day_et.isoformat()
        if record_fuse:
            with open_bounded_duckdb(lane.duckdb_path) as conn:
                ensure_tca_table(conn)
                ensure_wire_table(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS paper_daily_fuse "
                    "(day DATE PRIMARY KEY, account_id UUID, tripped BOOLEAN)"
                )
                row = conn.execute(
                    "SELECT account_id,tripped FROM paper_daily_fuse ORDER BY day DESC LIMIT 1"
                ).fetchone()
                if row and row[0] != account.id:
                    raise PermissionError(f"{lane.account} broker identity changed")
                if pnl is not None:
                    lane.breaker.update(realized_pnl=pnl)
                    tripped = conn.execute(
                        "INSERT INTO paper_daily_fuse VALUES (?, ?, ?) ON CONFLICT(day) DO UPDATE "
                        "SET tripped=paper_daily_fuse.tripped OR excluded.tripped "
                        "RETURNING tripped",
                        (day, account.id, lane.breaker.tripped),
                    ).fetchone()
                else:
                    # No PnL to record: a fuse already tripped today still stands.
                    tripped = conn.execute(
                        "SELECT tripped FROM paper_daily_fuse WHERE day = ?", (day,)
                    ).fetchone()
                lane.breaker.tripped = bool(tripped and tripped[0])
        else:
            if pnl is not None:
                lane.breaker.update(realized_pnl=pnl)
            if lane._persisted_daily_fuse_tripped(day_et):
                lane.breaker.tripped = True
        # The entries-only gate, after the fuse row is written and never through it: the fuse
        # latches for the day, an account restriction must not.
        if entries_gated:
            lane.breaker.tripped = True
        lane.equity_verified_at = None if entries_gated else now.timestamp()
        if publish:
            if shared_data is None:
                shared_data = ExtremeMoverScanner.from_env(lane.account).data_client
            reconcile_fills(lane, shared_data)
            refresh_markouts(lane, shared_data)
            stated_equity = _finite_decimal(account.equity)
            lane.write_status(
                # What the broker states, also when it is zero; the lane's last figure otherwise.
                equity=None if stated_equity is None else float(stated_equity),
                session=session,
                open_positions_count=len(positions),
                observation=True,
            )
            export_ui_snapshot(lane)
    farm.symbol_lock.reconcile(owners)
    return inventory


def export_ui_snapshot(lane: AlpacaPaperLane) -> None:
    """Publish committed data for DuckDB UI without keeping the writer database attached."""
    out = lane.manifest_dir / "paper_farm_ui"
    out.mkdir(exist_ok=True)
    with open_bounded_duckdb(lane.duckdb_path, read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        for name in ("paper_wire_orders", "paper_tca_fills"):
            if name in tables:
                path = out / f"{lane.account.lower()}_{name}.parquet"
                tmp = path.with_suffix(".parquet.tmp")
                conn.table(name).write_parquet(str(tmp))
                tmp.replace(path)


_INVERSE_HEDGE_SYMBOLS = frozenset({"SOXS", "SQQQ", "SPXS", "SDOW"})


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def reconcile_staged_tv_presets_for_engine(
    manifest_dir: Path,
    farm: PaperFarmCoordinator,
    session: str,
    *,
    movers_doc: dict[str, Any] | None = None,
    tca_allows_entry: bool | None = None,
    active_positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Fail-closed coordination of staged TV presets vs Paper Farm, breakers & Nautilus SANDBOX.

    Never auto-submits any order: always emits `semi_auto_confirmed=False`, `paper_only=True`,
    `live_execution_enabled=False`, and `auto_submit_enabled=False`.
    """
    from tier0_sota.tv.bridge import refuse_live_endpoints

    pack_dir = manifest_dir / "tv_sota_pack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    staged_path = pack_dir / "staged_tv_presets.json"
    if not staged_path.is_file():
        fallback = manifest_dir / "staged_tv_presets.json"
        if fallback.is_file():
            staged_path = fallback

    exec_doc = _read_json_dict(manifest_dir / "paper_farm_execution_status.json")
    ibkr_doc = _read_json_dict(manifest_dir / "ibkr_du_4002_live_automation_status.json")
    owners = dict(farm.symbol_lock.snapshot())

    if active_positions is not None:
        positions_out = [dict(p) for p in active_positions if isinstance(p, dict)]
    else:
        raw_pos = exec_doc.get("active_positions")
        positions_out = (
            [dict(p) for p in raw_pos if isinstance(p, dict)] if isinstance(raw_pos, list) else []
        )
    known_pos_syms = {str(p.get("symbol") or "").strip().upper() for p in positions_out}
    default_exec_mech = (
        "NATIVE_BROKER_STOP_MARKET_RATCHET"
        if session == "RTH"
        else "SYNTHETIC_BOATS_COLLARED_LIMIT"
        if session == "OVERNIGHT"
        else "SYNTHETIC_TICK_SUPERVISED_COLLARED_LIMIT"
        if session in ("PRE_MARKET", "AFTER_HOURS")
        else "SESSION_CLOSED_HOLD_GTC_STOP"
    )
    for sym, acct in sorted(owners.items()):
        if sym and sym not in known_pos_syms:
            positions_out.append(
                {
                    "symbol": sym,
                    "account": acct,
                    "lane_id": (
                        ALPACA_LANE_PROFILES[acct]["lane_id"]
                        if acct in ALPACA_LANE_PROFILES
                        else "IBKR_SMART_24X5"
                    ),
                    "venue_profile": (
                        farm.lanes[acct].venue_profile if acct in farm.lanes else "IBKR_PAPER_4002"
                    ),
                    "active_session": session,
                    "execution_mechanism": default_exec_mech,
                    "adopted_external": False,
                    "auto_adopted_by_engine": True,
                    "source": "symbol_lock",
                }
            )

    venue_handles = farm.venue_order_handles(session=session)
    breakers_tripped = sorted(
        [acct for acct, lane in farm.lanes.items() if not lane.breaker.allow_entry()]
        + ([IBKR_PAPER_ACCOUNT] if not farm.ibkr.breaker.allow_entry() else [])
    )
    tca_ok = True if tca_allows_entry is None else bool(tca_allows_entry)
    ibkr_tcp_ok = ibkr_doc.get("tcp_reachable") is True
    ibkr_ready = ibkr_doc.get("ready") is True

    movers_by_sym: dict[str, dict[str, Any]] = {}
    if isinstance(movers_doc, dict) and isinstance(movers_doc.get("entries"), list):
        for entry in movers_doc["entries"]:
            if isinstance(entry, dict) and entry.get("symbol"):
                movers_by_sym[str(entry["symbol"]).strip().upper()] = entry

    staged_doc = _read_json_dict(staged_path)
    manifest_confirmed_violation = staged_doc.get("semi_auto_confirmed") is True
    raw_presets_val = staged_doc.get("presets")
    raw_presets: list[Any] = raw_presets_val if isinstance(raw_presets_val, list) else []

    decisions: list[dict[str, Any]] = []
    for raw in raw_presets:
        if not isinstance(raw, dict):
            continue
        reasons: list[str] = []
        if manifest_confirmed_violation:
            reasons.append("MANIFEST_SEMI_AUTO_CONFIRMED_TRUE_REFUSED")
        try:
            refuse_live_endpoints(raw)
        except (ValueError, PermissionError):
            reasons.append("LIVE_ENDPOINT_REFUSED")

        qty_raw = raw.get("qty", 1)
        if isinstance(qty_raw, bool) or not isinstance(qty_raw, int) or qty_raw != 1:
            reasons.append("QTY_MUST_BE_1")
        if raw.get("semi_auto_confirmed") is not False:
            reasons.append("SEMI_AUTO_CONFIRMED_MUST_BE_FALSE")

        sym = str(raw.get("symbol") or "").strip().upper()
        if not sym:
            reasons.append("MISSING_SYMBOL")
        raw_side = str(raw.get("side") or "BUY").strip().upper()
        route_side: risk.Side = "SHORT" if raw_side in ("SELL", "SHORT") else "LONG"
        if raw_side not in ("BUY", "SELL", "LONG", "SHORT"):
            reasons.append("INVALID_SIDE")
        if session == "CLOSED":
            reasons.append("SESSION_CLOSED")
        if not tca_ok:
            reasons.append("TCA_RECALIBRATION_REQUIRED")

        mover = movers_by_sym.get(sym, {})
        natr_raw = raw.get("natr_pct") or mover.get("natr_pct") or "0.015"
        try:
            natr_d = Decimal(str(natr_raw))
            if not natr_d.is_finite() or natr_d <= 0:
                natr_d = Decimal("0.015")
        except Exception:  # noqa: BLE001
            natr_d = Decimal("0.015")
        sched_name, sp_map = engine.select_pooled_trailing_schedule(natr_d)
        prof = engine.get_session_protection_profile(session, sp_map)
        rvol_val = (
            raw.get("rvol")
            if raw.get("rvol") is not None
            else (
                mover.get("rvol_normalized")
                if mover.get("rvol_normalized") is not None
                else mover.get("rvol")
            )
        )

        req_vp = str(raw.get("venue_profile") or "").strip().upper()
        req_venue = str(raw.get("venue") or "").strip().upper()
        resolved_vp: str | None = None
        if req_vp:
            if req_vp not in engine.EXECUTION_VENUE_PROFILES:
                reasons.append("UNKNOWN_VENUE_PROFILE")
            else:
                resolved_vp = req_vp
                try:
                    engine.validate_venue_order_capability(req_vp)
                except PermissionError:
                    reasons.append("HOLD_LIVE_PROFILE_REFUSED")
                except ValueError:
                    reasons.append("VENUE_CAPABILITY_REFUSED")
        elif req_venue == "IBKR":
            resolved_vp = "IBKR_PAPER_4002"
        else:
            resolved_vp = str(venue_handles["alpaca_venue_profile"])

        if (
            resolved_vp
            and resolved_vp.startswith("ALPACA")
            and route_side == "SHORT"
            and session == "OVERNIGHT"
        ):
            reasons.append("ALPACA_OVERNIGHT_SHORT_UNSUPPORTED_ROUTE_IBKR_OR_INVERSE")

        if sym and sym in owners:
            reasons.append(f"SYMBOL_LOCKED_BY_{owners[sym]}")

        is_inverse = sym in _INVERSE_HEDGE_SYMBOLS
        if is_inverse and route_side == "LONG":
            origin = f"INVERSE_HEDGE_{sym}"
        elif route_side == "SHORT":
            origin = "DIRECT_SHORT_ETB"
        else:
            origin = "TV_HALF_AUTO_STAGED"

        target_account: str | None = None
        target_lane_id: str | None = None
        if resolved_vp == "IBKR_PAPER_4002":
            target_account = IBKR_PAPER_ACCOUNT
            target_lane_id = "IBKR_SMART_24X5"
            if not farm.ibkr.breaker.allow_entry():
                reasons.append("IBKR_DAILY_DRAWDOWN_BREAKER_TRIPPED")
            if not ibkr_ready and not ibkr_tcp_ok:
                reasons.append("IBKR_PAPER_4002_SOCKET_UNAVAILABLE")
        elif resolved_vp and resolved_vp.startswith("ALPACA_PAPER"):
            lane = select_situation_lane(
                farm.lanes,
                origin=origin,
                natr_pct=natr_d,
                rvol_normalized=rvol_val,
            )
            if lane is None:
                reasons.append("ALL_ALPACA_LANES_BREAKER_TRIPPED_OR_UNAVAILABLE")
            else:
                target_account = lane.account
                target_lane_id = lane.profile["lane_id"]

        handle_spec: dict[str, Any] | None = None
        non_noise_trail: dict[str, Any] | None = None
        sandbox_parity: dict[str, Any] = {
            "environment": "SANDBOX",
            "parity_ok": False,
            "session_order_mode": prof.order_mode,
            "limit_collar_bps": float(prof.limit_collar_bps),
            "outside_rth": bool(prof.outside_rth),
            "trailing_schedule": sched_name,
        }
        if resolved_vp is not None:
            handle_spec = engine.build_venue_order_handle_spec(
                resolved_vp, session=session, trailing_schedule=sched_name
            )
            re_cfg = engine.build_venue_risk_engine_config(resolved_vp)
            ref_raw = raw.get("trigger_price") or mover.get("last_price")
            rule612_stop: float | None = None
            stop_within_cap = True
            if ref_raw is not None and not isinstance(ref_raw, bool):
                try:
                    ref_d = Decimal(str(ref_raw))
                    if ref_d.is_finite() and ref_d > 0:
                        stop_d = risk.compute_capped_stop_price(
                            ref_d, route_side, max_loss_pct=prof.initial_max_loss_pct
                        )
                        loss_pct = abs(ref_d - stop_d) / ref_d
                        stop_within_cap = Decimal("0") < loss_pct <= risk.MAX_STOP_LOSS_PCT
                        rule612_stop = float(stop_d)
                        arch_name = str(
                            raw.get("archetype")
                            or mover.get("potential_category")
                            or "EARLY_EP_MOMENTUM_BURST"
                        )
                        nn_raw = engine.compute_non_noise_min_trail_spec(
                            symbol=sym or "SPY",
                            entry_price=ref_d,
                            side=route_side,
                            session=session,
                            natr_pct=natr_d,
                            archetype=arch_name,
                            venue_profile=resolved_vp,
                        )
                        non_noise_trail = {
                            "non_noise_min_trail_pct": float(nn_raw["non_noise_min_trail_pct"]),
                            "fast_sell_non_noise_reversal_pct": float(
                                nn_raw["fast_sell_non_noise_reversal_pct"]
                            ),
                            "peak_climax_reverse_trigger_pct": float(
                                nn_raw["peak_climax_reverse_trigger_pct"]
                            ),
                            "auto_reverse_policy": str(nn_raw["auto_reverse_policy"]),
                            "execution_mechanism": str(nn_raw["execution_mechanism"]),
                            "broker_exit_order": str(nn_raw["broker_exit_order"]),
                            "collar_bps": float(nn_raw["collar_bps"]),
                            "extended_hours": bool(nn_raw["extended_hours"]),
                            "rapid_reentry_policy": nn_raw["rapid_reentry_policy"],
                        }
                except Exception:  # noqa: BLE001
                    stop_within_cap = False
            if not stop_within_cap:
                reasons.append("RULE_612_STOP_CAP_VIOLATION")
            sandbox_parity = {
                "environment": "SANDBOX",
                "parity_ok": (
                    not bool(re_cfg.bypass)
                    and stop_within_cap
                    and bool(handle_spec["executable_now"])
                ),
                "risk_engine_bypass": bool(re_cfg.bypass),
                "max_order_submit_rate": str(re_cfg.max_order_submit_rate),
                "max_order_modify_rate": str(re_cfg.max_order_modify_rate),
                "session_order_mode": prof.order_mode,
                "limit_collar_bps": float(prof.limit_collar_bps),
                "outside_rth": bool(prof.outside_rth),
                "trailing_schedule": sched_name,
                "rule_612_stop_price": rule612_stop,
                "stop_loss_pct_within_cap": stop_within_cap,
                "execution_mechanism": (
                    non_noise_trail["execution_mechanism"]
                    if non_noise_trail is not None
                    else default_exec_mech
                ),
                "non_noise_min_trail_pct": (
                    non_noise_trail["non_noise_min_trail_pct"]
                    if non_noise_trail is not None
                    else None
                ),
            }

        ready_confirm = len(reasons) == 0
        alert_id = str(raw.get("alert_id") or "").strip()
        decisions.append(
            {
                "alert_id": alert_id or None,
                "symbol": sym,
                "side": route_side,
                "qty": 1,
                "session": session,
                "origin": origin,
                "venue_profile": resolved_vp,
                "target_account": target_account,
                "target_lane_id": target_lane_id,
                "semi_auto_confirmed": False,
                "auto_submitted": False,
                "auto_adopted_by_engine": True,
                "ready_for_operator_confirm": ready_confirm,
                "discipline_gate": ("READY_FOR_OPERATOR_CONFIRM" if ready_confirm else reasons[0]),
                "blocking_reasons": reasons,
                "order_handle_spec": handle_spec,
                "non_noise_trail_spec": non_noise_trail,
                "nautilus_sandbox_parity": sandbox_parity,
            }
        )

    ready_count = sum(1 for d in decisions if d["ready_for_operator_confirm"])
    adopted_external_n = max(
        int(exec_doc.get("adopted_external_positions_count") or 0),
        sum(1 for p in positions_out if p.get("adopted_external") is True),
    )
    synthetic_offhours_n = max(
        int(exec_doc.get("synthetic_offhours_trailed_count") or 0),
        sum(
            1
            for p in positions_out
            if str(p.get("execution_mechanism") or "").startswith("SYNTHETIC_")
        ),
    )
    peak_reverse_n = max(
        int(exec_doc.get("peak_reverse_armed_count") or 0),
        sum(1 for p in positions_out if p.get("peak_reverse_armed") is True),
    )
    rapid_reentry_n = max(
        int(exec_doc.get("rapid_reentry_ready_count") or 0),
        sum(1 for p in positions_out if p.get("rapid_reentry_ready") is True),
    )
    out_path = pack_dir / "tv_engine_coordination_manifest.json"
    coord_doc: dict[str, Any] = {
        "schema_version": "tier0.tv-engine-coordination.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "active_session": session,
        "paper_only": True,
        "semi_auto_confirmed": False,
        "live_execution_enabled": False,
        "auto_submit_enabled": False,
        "staged_source_path": str(staged_path),
        "tca_calibration_ok": tca_ok,
        "ibkr_tcp_reachable": ibkr_tcp_ok,
        "ibkr_gateway_ready": ibkr_ready,
        "breakers_tripped_accounts": breakers_tripped,
        "any_breaker_tripped": bool(breakers_tripped),
        "symbol_lock_snapshot": owners,
        "active_positions": positions_out,
        "active_positions_count": len(positions_out),
        "adopted_external_positions_count": adopted_external_n,
        "synthetic_offhours_trailed_count": synthetic_offhours_n,
        "peak_reverse_armed_count": peak_reverse_n,
        "rapid_reentry_ready_count": rapid_reentry_n,
        "venue_order_handles": venue_handles,
        "staged_alerts_evaluated": len(decisions),
        "ready_for_operator_confirm_count": ready_count,
        "blocked_count": len(decisions) - ready_count,
        "coordination_decisions": decisions,
        "confirm_command_template": (
            "uv run --no-sync python -m tier0_sota.live.runner "
            "--confirm-tv-alert <ALERT_ID> --qty 1 --dry-run"
        ),
    }
    _atomic_write_json(out_path, coord_doc)
    return coord_doc


def run_cycle(manifest_dir: Path, *, top_k: int, pins: tuple[str, ...]) -> dict[str, Any]:
    verify_paper_environment()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    # DuckDB's exclusive writer lock serializes scheduler invocations and manual cycles.
    with open_bounded_duckdb(manifest_dir / "paper_farm_runner.duckdb"):
        farm = PaperFarmCoordinator(manifest_dir)
        now = datetime.now(UTC)
        session = engine.classify_us_equity_session_et(int(now.timestamp() * 1e9))
        # Decouple synchronous IBKR :4002 probe (up to 5s on balance_timeout) from scanner.scan()
        # and Alpaca account refresh while preserving fail-closed PermissionError propagation.
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="ibkr-health") as ibkr_pool:
            ibkr_future = ibkr_pool.submit(ibkr_health, farm.ibkr)
            scanner = ExtremeMoverScanner.from_env(pin_symbols=pins)
            refresh_accounts(farm, now, session, data_client=scanner.data_client)
            movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
            scan = scanner.scan(
                top_k=top_k,
                output_path=movers_path,
            )
            reachable = ibkr_future.result()
        from tier0_sota.live.execution import cost_calibration_allows_entry

        tca_ok = cost_calibration_allows_entry(manifest_dir)
        bundle_path = manifest_dir / "live_lane_order_preset_bundle.json"
        build_live_lane_preset_bundle(
            scan,
            lanes=farm.lanes,
            output_path=bundle_path,
            tca_allows_entry=tca_ok,
        )
        coord = reconcile_staged_tv_presets_for_engine(
            manifest_dir,
            farm,
            session,
            movers_doc=scan,
            tca_allows_entry=tca_ok,
        )
        tickets = refresh_tickets_from_paths(
            movers_path=movers_path,
            bundle_path=bundle_path,
            out_dir=manifest_dir / "tv_sota_pack",
            venue="ALPACA",
        )
        otlp_url = (
            os.environ.get("TIER0_OTLP_METRICS_URL", "http://127.0.0.1:4318/v1/metrics").strip()
            or None
        )
        prom_path = manifest_dir / "paper_farm_metrics.prom"
        perf = aggregate_paper_farm_performance(
            manifest_dir,
            prom_output_path=prom_path,
            otlp_endpoint=otlp_url,
        )
        tv_meta = perf.get("tv_half_auto") or {}
        venue_handles = coord.get("venue_order_handles") or {}
        ibkr_status = _read_json_dict(farm.ibkr.status_path)
        ibkr_ready = ibkr_status.get("ready") is True
        result = {
            "session": session,
            "paper_only": True,
            "live_execution_enabled": False,
            "accounts_authenticated": len(farm.lanes) + (1 if ibkr_ready else 0),
            "ibkr_tcp_reachable": reachable,
            "ibkr_gateway_ready": ibkr_ready,
            "scan_rows": scan["ranked_count"],
            "tickets": tickets["staged_count"],
            "standby_presets": len(tickets.get("standby_presets") or ()),
            "unusual_vol_count": int(tv_meta.get("unusual_vol_count") or 0),
            "darkpool_hits_count": int(tv_meta.get("darkpool_hits_count") or 0),
            "tv_coordination_evaluated": int(coord.get("staged_alerts_evaluated") or 0),
            "tv_coordination_ready": int(coord.get("ready_for_operator_confirm_count") or 0),
            "active_positions_count": int(coord.get("active_positions_count") or 0),
            "adopted_external_positions_count": int(
                coord.get("adopted_external_positions_count") or 0
            ),
            "synthetic_offhours_trailed_count": int(
                coord.get("synthetic_offhours_trailed_count") or 0
            ),
            "peak_reverse_armed_count": int(coord.get("peak_reverse_armed_count") or 0),
            "rapid_reentry_ready_count": int(coord.get("rapid_reentry_ready_count") or 0),
            "combined_max_rpm": int(venue_handles.get("combined_max_rpm") or 0),
            "tca_fills": perf["summary"]["tca_fills_count"],
            "paper_live_deployed": perf["paper_live_deployed"],
            "prom_metrics_written": prom_path.is_file(),
        }
        _atomic_write_json(
            manifest_dir / "paper_farm_live_auto_entry_status.json",
            {
                **result,
                "generated_at_utc": perf["generated_at_utc"],
                "macro_regime": scan.get("macro_regime") or {},
                "tv_half_auto": tv_meta,
                "tv_engine_coordination": {
                    "schema_version": coord["schema_version"],
                    "staged_alerts_evaluated": coord["staged_alerts_evaluated"],
                    "ready_for_operator_confirm_count": coord["ready_for_operator_confirm_count"],
                    "blocked_count": coord["blocked_count"],
                    "active_positions_count": coord["active_positions_count"],
                    "adopted_external_positions_count": coord["adopted_external_positions_count"],
                    "synthetic_offhours_trailed_count": coord["synthetic_offhours_trailed_count"],
                    "peak_reverse_armed_count": coord["peak_reverse_armed_count"],
                    "rapid_reentry_ready_count": coord["rapid_reentry_ready_count"],
                    "any_breaker_tripped": coord["any_breaker_tripped"],
                    "semi_auto_confirmed": False,
                    "auto_submit_enabled": False,
                },
                "venue_order_handles": venue_handles,
                "accounts": {
                    acct: {
                        "venue": "ALPACA" if acct in farm.lanes else "IBKR",
                        "venue_profile": metrics["lane_id"],
                        "execution_venue_profile": (
                            farm.lanes[acct].venue_profile
                            if acct in farm.lanes
                            else "IBKR_PAPER_4002"
                        ),
                        "rate_limit_rpm": (
                            farm.lanes[acct].rate_budget.max_rpm
                            if acct in farm.lanes
                            else farm.ibkr.rate_budget_max_rpm
                        ),
                        "exit_reserve_rpm": (
                            farm.lanes[acct].rate_budget.exit_reserve_rpm
                            if acct in farm.lanes
                            else farm.ibkr.rate_budget.exit_reserve_rpm
                        ),
                        "ready": (metrics["worker_verified"] if acct in farm.lanes else ibkr_ready)
                        and session != "CLOSED",
                        "status": (
                            "Paper account authenticated · " + session
                            if acct in farm.lanes
                            else "Paper DU authenticated · " + session
                            if ibkr_ready
                            else "DU login unverified"
                            if reachable
                            else "Paper socket unavailable"
                        ),
                    }
                    for acct, metrics in perf["per_lane"].items()
                },
            },
        )
        return result


def live_manifest_dir_for(shared_dir: Path) -> Path:
    """Manifest directory of the live venue beside a paper one: ``<data root>/live/manifests``.

    The data root is the parent of the paper (shared) manifest directory, so a run pointed at a
    sandbox root keeps its live files inside that sandbox. For the default paper directory this
    is ``paper_farm.default_live_manifest_dir()``.
    """
    return shared_dir.parent / "live" / "manifests"


def _first_adoption_stop(
    lane: AlpacaPaperLane,
    *,
    symbol: str,
    side: risk.Side,
    entry_px: Decimal,
    curr_px: Decimal,
    session: str,
) -> dict[str, Any]:
    """The stop a first adoption of this position starts with. Pure: no broker, no ledger.

    Mirror of the stop arithmetic in ``execution.adopt_unmanaged_broker_positions`` as the
    supervisor runs it at start (no scanner width: the default NATR, spread and archetype). A
    position first seen under water is anchored at the price at which it is seen; one in profit
    past the break-even trigger starts at the ratcheted stop, never at or through the price.
    ``tests/test_live_runner_modes.py`` holds this equal to what the supervisor attaches.
    """
    is_long = side == "LONG"
    better = max if is_long else min
    behind = (lambda a, b: a < b) if is_long else (lambda a, b: a > b)
    natr = Decimal("0.015")
    spread = Decimal("5") if session == "RTH" else Decimal("8")
    archetype = "PREMARKET_EARLY_IGNITION" if session == "PRE_MARKET" else "EARLY_EP_MOMENTUM_BURST"
    under_water = behind(curr_px, entry_px)
    ref_px = curr_px if under_water else entry_px
    spec = engine.compute_non_noise_min_trail_spec(
        symbol=symbol,
        entry_price=ref_px,
        side=side,
        session=session,
        natr_pct=natr,
        spread_bps=spread,
        archetype=archetype,
        venue_profile=lane.venue_profile,
    )
    initial_stop: Decimal = spec["initial_stop_price"]
    nn_pct: Decimal = spec["non_noise_min_trail_pct"]
    peak_px = better(ref_px, curr_px)
    stop = initial_stop
    _, schedule = engine.select_pooled_trailing_schedule(natr)
    profile = engine.get_session_protection_profile(session, schedule)
    fav_gain = (peak_px - ref_px) if is_long else (ref_px - peak_px)
    if fav_gain / ref_px >= profile.breakeven_trigger_pct:
        prog_stop, _tier, _ = engine.compute_progressive_trailing_stop(
            entry_price=ref_px,
            side=side,
            favorable_extreme=peak_px,
            current_stop=initial_stop,
            initial_stop=initial_stop,
            natr_pct=natr,
            profile=profile,
        )
        nn_raw = max(
            risk.TICK_SUB_DOLLAR,
            peak_px * (Decimal("1") - nn_pct if is_long else Decimal("1") + nn_pct),
        )
        nn_stop = risk.quantize_rule_612(nn_raw, side=side, is_stop=True)
        cand_stop = better(prog_stop, nn_stop) if behind(nn_stop, peak_px) else prog_stop
        if behind(cand_stop, curr_px):
            stop = better(initial_stop, cand_stop)
    return {
        "under_water": under_water,
        "reference_price": ref_px,
        "adoption_anchor_price": curr_px if under_water else None,
        "initial_stop_price": initial_stop,
        "stop_price": stop,
        "natr_pct": natr,
        "non_noise_min_trail_pct": nn_pct,
    }


def _at_or_through(price: Decimal, stop: Decimal, *, long: bool) -> bool:
    """True when ``price`` has reached ``stop`` from the side the position profits on."""
    return price <= stop if long else price >= stop


def _persisted_open_lifecycles(lane: AlpacaPaperLane) -> tuple[dict[str, dict[str, Any]], bool]:
    """Open lifecycles in the lane's ledger, read-only: symbol -> side, stop, exit_triggered.

    The second value is False when a ledger file exists and could not be read. A missing
    ledger is an empty, readable one; nothing is created.
    """
    if not lane.duckdb_path.is_file():
        return {}, True
    try:
        with open_bounded_duckdb(lane.duckdb_path, read_only=True) as db:
            if ("paper_entry_decisions",) not in db.execute("SHOW TABLES").fetchall():
                return {}, True
            rows = db.execute(
                "SELECT symbol, side, coalesce(active_stop, initial_stop), exit_triggered "
                "FROM paper_entry_decisions WHERE closed_at IS NULL"
            ).fetchall()
    except duckdb.Error as exc:
        LOG.warning("Live self-check: ledger of %s unreadable: %s", lane.account, exc)
        return {}, False
    return {
        str(sym or "").strip().upper(): {
            "side": str(side or "").strip().upper(),
            "stop_price": _positive_decimal(stop),
            "exit_triggered": triggered is True,
        }
        for sym, side, stop, triggered in rows
    }, True


def run_live_self_check(manifest_dir: Path, *, shared_dir: Path) -> dict[str, Any]:
    """What the live lane would find and do on a start now, from reads alone. Places nothing.

    Through the live coordinator it reads the account, the positions, the open orders, the
    broker clock and each held asset; from the lane's ledger, when one exists, the open
    lifecycles (read-only). It sends no order, cancel or replace and writes no file. For every
    position it states the resting cover the broker already holds, the stop the lane would use
    (the persisted one of an open lifecycle, else the first-adoption stop) and the order the
    stop-coverage reconciler would attach for the uncovered whole shares, and it marks a
    position that would exit at once (``would_exit_immediately``, ``exit_reasons``):

    * ``PERSISTED_EXIT_TRIGGERED`` / ``PERSISTED_STOP_AT_OR_THROUGH_PRICE``: the lane's open
      lifecycle for the symbol is already triggered, or its stop is at or through the price;
    * ``ADOPTION_STOP_AT_OR_THROUGH_PRICE``: the first-adoption stop is (checked, not assumed);
    * ``RESTING_STOP_AT_OR_THROUGH_PRICE``: a working exit-side stop already rests at or through
      the position's current price. The broker triggers it when the regular session trades,
      whether the lane runs or not.

    The first three are what the lane's own start would do (``would_exit_on_start``) and are
    ``start_blockers``, as is a read that is incomplete (a full order read, an unreadable
    ledger, a position stated without a price). A resting broker stop through the price is
    marked and listed in ``notes``, which is informational. ``coverage`` is the lossless
    coverage record of the same reads with no guard running: what the broker protects natively
    today; every other share reads ``UNPROTECTED:NO_GUARD``. The account's id, number and
    money amounts are not part of the report.
    """
    from tier0_sota.live import execution

    opt_in = verify_live_environment()
    execution._live_evidence_dir(manifest_dir, shared_dir)
    farm = build_live_venue_coordinator(manifest_dir, autonomous_opt_in=opt_in)
    farm.assert_single_environment()
    if farm.environment != "LIVE" or len(farm.lanes) != 1:
        raise PermissionError("Refused live self-check: not the live venue's coordinator")
    (lane,) = farm.lanes.values()
    lane.assert_integrity()
    broker = lane.trading_client
    account = broker.get_account()
    positions = list(broker.get_all_positions() or ())
    open_orders = execution._read_open_orders(broker)
    if open_orders is None:
        raise RuntimeError("Live self-check could not read the open orders")
    # Sessions follow the broker clock when the host clock is off, as in the supervisor (R2).
    skew = execution._measure_broker_clock_skew(broker)
    skewed = skew is not None and abs(skew) > execution.CLOCK_SKEW_MAX_SECS
    now = datetime.now(UTC)
    session = engine.classify_us_equity_session_et(
        time.time_ns() + (int(skew * 1_000_000_000) if skew is not None and skewed else 0)
    )
    persisted, ledger_readable = _persisted_open_lifecycles(lane)
    coverage = execution.build_lossless_coverage_record(
        farm,
        {},
        {lane.account: {str(getattr(pos, "symbol", "") or ""): pos for pos in positions}},
        session=session,
        now_utc=now,
        last_feed_trade={},
        open_orders={lane.account: open_orders},
    )
    native_by_symbol = {
        row["symbol"]: int(row["natively_protected_shares"])
        for row in coverage["share_protection_table"]
    }

    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    notes: list[str] = []
    for pos in positions:
        raw_symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
        parsed = execution._parse_position_for_adoption(pos)
        if parsed is None:
            # The supervisor cannot put such a position under a guard either.
            rows.append({"symbol": raw_symbol, "eligible": False, "action": "NONE_UNPARSEABLE"})
            blockers.append(f"UNPARSEABLE_POSITION:{raw_symbol}")
            continue
        sym, side, abs_qty, entry_px, curr_px = parsed
        is_long = side == "LONG"
        whole = abs_qty.to_integral_value(rounding=ROUND_FLOOR)
        exit_side, entry_side = ("sell", "buy") if is_long else ("buy", "sell")
        ineligible = execution._adoption_ineligible_reason(pos, broker, abs_qty)
        working = execution._exit_side_orders(open_orders, sym, exit_side)
        # Cover as the stop-coverage reconciler counts it: resting GTC stops on the exit side,
        # the harness's own fixed stops apart from everything else (a native trailing stop, an
        # activated stop leg, a stop the owner placed).
        own_stops = 0
        own_qty = Decimal("0")
        other_qty = Decimal("0")
        for order in working:
            if not execution._is_resting_cover_stop(order):
                continue
            remaining = execution._order_remaining_qty(order)
            if execution._is_harness_stop(order):
                own_stops += 1
                own_qty += remaining if remaining is not None else Decimal("0")
            else:
                # A stop that states no quantity is taken to cover the whole position.
                other_qty += remaining if remaining is not None else whole
        target_own = max(Decimal("0"), whole - other_qty)
        qty_available = execution._available_qty(pos, long=is_long)
        available = (
            qty_available.to_integral_value(rounding=ROUND_FLOOR)
            if qty_available is not None
            else whole
        )
        lifecycle = persisted.get(sym)
        if lifecycle is not None and (lifecycle["side"] != side or lifecycle["stop_price"] is None):
            # The other side, or no stop on record: the lane ends that lifecycle and adopts.
            notes.append(f"PERSISTED_LIFECYCLE_NOT_REUSED:{sym}")
            lifecycle = None
        adoption = _first_adoption_stop(
            lane, symbol=sym, side=side, entry_px=entry_px, curr_px=curr_px, session=session
        )
        stop_px: Decimal = adoption["stop_price"] if lifecycle is None else lifecycle["stop_price"]

        # Why the position would exit at once. A resting broker stop that is already through the
        # price fires at the broker whether the lane runs or not: it is marked and noted. The
        # other reasons are what the lane's own start would do, and those block a start.
        exit_reasons: list[str] = []
        resting_levels = [
            _positive_decimal(getattr(order, "stop_price", None)) for order in working
        ]
        if any(
            level is not None and _at_or_through(curr_px, level, long=is_long)
            for level in resting_levels
        ):
            exit_reasons.append("RESTING_STOP_AT_OR_THROUGH_PRICE")
            notes.append(f"BROKER_STOP_AT_OR_THROUGH_PRICE:{sym}")
        exit_on_start: str | None = None
        if lifecycle is not None and lifecycle["exit_triggered"]:
            exit_on_start = "PERSISTED_EXIT_TRIGGERED"
        elif _at_or_through(curr_px, stop_px, long=is_long):
            exit_on_start = (
                "ADOPTION_STOP_AT_OR_THROUGH_PRICE"
                if lifecycle is None
                else "PERSISTED_STOP_AT_OR_THROUGH_PRICE"
            )
        if exit_on_start is not None:
            exit_reasons.append(exit_on_start)
            blockers.append(f"WOULD_EXIT_ON_START:{sym}")

        would_attach: dict[str, Any] | None = None
        if ineligible is not None:
            action = "NONE_INELIGIBLE"
            notes.append(f"INELIGIBLE:{sym}:{ineligible}")
        elif lifecycle is not None and lifecycle["exit_triggered"]:
            # The restored guard is already exiting: its exit state machine owns the shares.
            action = "NONE_EXIT_TRIGGERED"
        elif any(
            str(getattr(order, "client_order_id", "") or "").startswith(
                execution._HARNESS_EXIT_PREFIX
            )
            for order in working
        ):
            action = "NONE_EXIT_IN_FLIGHT"
        elif own_stops:
            # A stop of this harness from an earlier run: the reconciler sizes it to the target.
            action = "LANE_STOP_PRESENT"
        elif target_own == 0:
            action = "NONE_COVERED"
        elif min(target_own, available) < 1:
            action = "NONE_NO_QTY_AVAILABLE"
            notes.append(f"UNCOVERED_NO_QTY_AVAILABLE:{sym}")
        else:
            action = "ATTACH_STOP"
            would_attach = {
                "order_type": "stop",
                "side": exit_side,
                "time_in_force": "gtc",
                "qty": int(min(target_own, available)),
                "stop_price": str(stop_px),
            }
        rows.append(
            {
                "symbol": sym,
                "side": side,
                "position_qty": str(abs_qty),
                "whole_shares": int(whole),
                "fractional_remainder": str(abs_qty - whole),
                "avg_entry_price": str(entry_px),
                "current_price": str(curr_px),
                "eligible": ineligible is None,
                "ineligible_reason": ineligible,
                "natively_protected_shares": native_by_symbol.get(sym, 0),
                "lane_stop_shares": int(own_qty),
                "lane_stop_target_shares": int(target_own),
                "qty_available": None if qty_available is None else str(qty_available),
                "opposite_side_working_orders": len(
                    execution._exit_side_orders(open_orders, sym, entry_side)
                ),
                "stop_basis": "FIRST_ADOPTION" if lifecycle is None else "PERSISTED_LIFECYCLE",
                "stop_price": str(stop_px),
                "first_adoption": {
                    key: value if isinstance(value, bool) or value is None else str(value)
                    for key, value in adoption.items()
                },
                "action": action,
                "would_attach": would_attach,
                "would_exit_immediately": bool(exit_reasons),
                "would_exit_on_start": exit_on_start is not None,
                "exit_reasons": exit_reasons,
            }
        )
    rows.sort(key=lambda row: str(row["symbol"]))

    held = {str(row["symbol"]) for row in rows}
    orphan_stops = sorted(
        str(getattr(order, "symbol", "") or "").strip().upper()
        for order in open_orders
        if execution._is_harness_stop(order)
        and str(getattr(order, "symbol", "") or "").strip().upper() not in held
        and execution._enum_text(getattr(order, "status", "")) in execution._WORKING_ORDER_STATUSES
    )
    gates = _account_entry_gates(account, open_orders)
    if len(open_orders) >= OPEN_ORDER_READ_LIMIT:
        blockers.append("OPEN_ORDER_READ_INCOMPLETE")
    if not ledger_readable:
        blockers.append("LEDGER_UNREADABLE")
    if skewed:
        notes.append("HOST_CLOCK_SKEWED_ENTRIES_BLOCKED")
    notes.extend(f"ENTRY_GATE:{gate}" for gate in gates)
    notes.extend(f"ORPHAN_LANE_STOP_WOULD_BE_RELEASED:{sym}" for sym in orphan_stops)
    status = getattr(account, "status", None)
    return {
        "schema_version": LIVE_SELF_CHECK_SCHEMA,
        "generated_at_utc": now.isoformat(),
        "mode": "READ_ONLY",
        "environment": farm.environment,
        "account_alias": lane.account,
        "venue_profile": lane.venue_profile,
        "paper_only": farm.environment == "PAPER",
        "live_execution_enabled": farm.autonomous_opt_in is True,
        "session": session,
        "clock_skew": {
            "status": "UNVERIFIED" if skew is None else "SKEWED" if skewed else "OK",
            "skew_secs": None if skew is None else str(skew),
            "max_skew_secs": str(execution.CLOCK_SKEW_MAX_SECS),
            "session_clock": "BROKER" if skewed else "HOST",
        },
        "account": {
            "status": str(getattr(status, "value", status) or ""),
            "trading_blocked": getattr(account, "trading_blocked", None),
            "account_blocked": getattr(account, "account_blocked", None),
            "trade_suspended_by_user": getattr(account, "trade_suspended_by_user", None),
            "entry_gates": list(gates),
        },
        "positions_count": len(rows),
        "open_orders_count": len(open_orders),
        "ledger_readable": ledger_readable,
        "persisted_open_lifecycles": sorted(persisted),
        "positions": rows,
        "stops_to_attach": [
            {"symbol": row["symbol"], **row["would_attach"]}
            for row in rows
            if row.get("would_attach")
        ],
        "would_exit_immediately": [
            row["symbol"] for row in rows if row.get("would_exit_immediately")
        ],
        "orphan_lane_stops": orphan_stops,
        "start_blockers": sorted(blockers),
        "notes": sorted(notes),
        "coverage": coverage,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest-dir", type=Path, default=default_manifest_dir())
    p.add_argument("--top-k", type=int, default=15)
    p.add_argument("--pin-symbols", default=",".join(DEFAULT_PIN_SYMBOLS))
    p.add_argument("--tv-qty", "--qty", dest="tv_qty", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--alpaca-rpm",
        default=None,
        help="Optional Alpaca Paper RPM cap (200|1000|ALPACA_PAPER_200|ALPACA_PAPER_1000)",
    )
    p.add_argument(
        "--account",
        default=None,
        help=(
            "Optional Alpaca paper lane account target for --confirm-tv-alert "
            f"({', '.join(sorted(ALPACA_LANE_PROFILES))})"
        ),
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--observe", action="store_true")
    mode.add_argument("--confirm-paper", action="store_true")
    mode.add_argument("--confirm-tv-alert", metavar="ALERT_ID")
    mode.add_argument("--ibkr-health", action="store_true")
    mode.add_argument(
        "--supervise-live",
        action="store_true",
        help=(
            f"Supervise the one live lane ({engine.LIVE_TIERED_VENUE_ID}); --manifest-dir is "
            "the paper directory it reads, its own files go to <data root>/live/manifests"
        ),
    )
    mode.add_argument(
        "--live-self-check",
        action="store_true",
        help="Read the live account and print what a start would do; places nothing",
    )
    args = p.parse_args()
    if args.supervise_live or args.live_self_check:
        # Before anything else: the paper options below change the process environment.
        if args.account is not None or args.alpaca_rpm is not None:
            raise PermissionError(
                "Refused --account / --alpaca-rpm with a live mode: they select paper lanes "
                "and paper rate profiles; the live lane has one account and one venue"
            )
        shared_dir: Path = args.manifest_dir
        live_dir = live_manifest_dir_for(shared_dir)
        if args.live_self_check:
            report = run_live_self_check(live_dir, shared_dir=shared_dir)
            print(json.dumps(report, indent=2))
            raise SystemExit(1 if report["start_blockers"] else 0)
        from tier0_sota.live.execution import run_execution

        verify_live_environment()
        run_execution(live_dir, venue_profile=engine.LIVE_TIERED_VENUE_ID, shared_dir=shared_dir)
        # No stop was asked for, so a return means every market-data stream ended on its own:
        # that is a failure of the supervisor, and the unit must restart it.
        raise SystemExit("Live supervision ended: every market data stream stopped")
    target_account: str | None = None
    if args.account is not None:
        if not args.confirm_tv_alert:
            raise ValueError("--account is only supported with --confirm-tv-alert")
        target_account = str(args.account).strip().upper()
        if target_account not in ALPACA_LANE_PROFILES:
            allowed = ", ".join(sorted(ALPACA_LANE_PROFILES))
            raise PermissionError(
                f"Refused non-paper or unknown Alpaca lane account {args.account!r}; "
                f"allowed paper lanes: {allowed}"
            )
    if args.alpaca_rpm is not None:
        _, resolved_rpm = resolve_alpaca_paper_venue_profile(
            env={"TIER0_ALPACA_RPM": str(args.alpaca_rpm)}
        )
        os.environ["TIER0_ALPACA_RPM"] = str(resolved_rpm)
    if args.confirm_paper:
        from tier0_sota.live.execution import run_execution

        run_execution(args.manifest_dir)
        return
    if args.confirm_tv_alert:
        from tier0_sota.live.execution import submit_confirmed_tv_preset_paper

        receipt = submit_confirmed_tv_preset_paper(
            args.manifest_dir,
            alert_id=args.confirm_tv_alert,
            qty=args.tv_qty,
            dry_run=args.dry_run,
            target_account=target_account,
        )
        print(json.dumps(receipt, indent=2))
        return
    if args.ibkr_health:
        adapter = IbkrPaperAdapter(args.manifest_dir)
        reachable = ibkr_health(adapter)
        status = _read_json_dict(adapter.status_path)
        du_verified = status.get("ready") is True
        print(
            json.dumps(
                {
                    "paper_port": 4002,
                    "tcp_reachable": reachable,
                    "du_login_verified": du_verified,
                    "ready": du_verified,
                    "account_id": status.get("account_id"),
                    "equity": status.get("equity"),
                    "probe_error": status.get("probe_error"),
                }
            )
        )
        raise SystemExit(0 if reachable else 1)
    print(
        json.dumps(
            run_cycle(
                args.manifest_dir,
                top_k=args.top_k,
                pins=_resolve_pin_symbols(args.pin_symbols),
            )
        )
    )


if __name__ == "__main__":
    main()

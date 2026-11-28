"""One authenticated Paper Farm observation and reconciliation cycle for a native scheduler."""

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
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from alpaca.common.enums import Sort
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
    default_manifest_dir,
    ensure_tca_table,
    ensure_wire_table,
    open_bounded_duckdb,
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


def verify_paper_environment() -> None:
    with Path(risk.__file__).open("rb") as kernel:
        if hashlib.file_digest(kernel, "sha256").hexdigest() != (
            "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
        ):
            raise PermissionError("Paper worker risk kernel digest mismatch")
    if (
        os.environ.get("ALPACA_PAPER", "").lower() != "true"
        or os.environ.get("LIVE_EXECUTION_ENABLED", "false").lower() != "false"
        or os.environ.get("IBKR_HOST") != "127.0.0.1"
        or os.environ.get("IBKR_PORT") != "4002"
        or os.environ.get("APCA_API_BASE_URL", PAPER_ALPACA_ENDPOINT).rstrip("/")
        != PAPER_ALPACA_ENDPOINT
    ):
        raise PermissionError("Paper profile or loopback broker isolation check failed")
    for acct in ALPACA_LANE_PROFILES:
        if not os.environ.get(f"{acct}_API_KEY", "").startswith("PK") or not (
            os.environ.get(f"{acct}_SECRET") or os.environ.get(f"{acct}_SECRET_KEY")
        ):
            raise PermissionError(f"Missing account-specific paper credentials for {acct}")


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
    orders = broker.get_orders(GetOrdersRequest(limit=500))
    return account, positions, orders


def refresh_accounts(
    farm: PaperFarmCoordinator,
    now: datetime,
    session: str,
    *,
    publish: bool = True,
    data_client: Any = None,
    record_fuse: bool = True,
) -> dict[str, dict[str, Any]]:
    if farm.lanes.keys() != ALPACA_LANE_PROFILES.keys():
        raise PermissionError("All three Alpaca paper accounts are required")
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
    shared_data = data_client
    for lane, (account, positions, orders) in zip(lanes_list, snapshots, strict=True):
        if (
            not account.id
            or account.id in ids
            or account.status.value != "ACTIVE"
            or account.trading_blocked is not False
            or account.account_blocked is not False
            or account.trade_suspended_by_user is True
        ):
            raise PermissionError(f"{lane.account} is unavailable, blocked or duplicated")
        ids.add(account.id)
        if len(orders) == 500:
            raise RuntimeError("Cannot attest complete open-order inventory")
        for sym in {o.symbol for o in [*positions, *orders]}:
            if owners.setdefault(sym, lane.account) != lane.account:
                raise PermissionError(f"Overlapping paper exposure for {sym}")
        inventory[lane.account] = {p.symbol: p for p in positions}
        equity, yesterday = Decimal(account.equity), Decimal(account.last_equity)
        lane.start_equity = equity
        lane.breaker = risk.DailyDrawdownBreaker(start_equity=yesterday)
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
                lane.breaker.update(realized_pnl=equity - yesterday)
                tripped = conn.execute(
                    "INSERT INTO paper_daily_fuse VALUES (?, ?, ?) ON CONFLICT(day) DO UPDATE "
                    "SET tripped=paper_daily_fuse.tripped OR excluded.tripped RETURNING tripped",
                    (day, account.id, lane.breaker.tripped),
                ).fetchone()
                lane.breaker.tripped = bool(tripped and tripped[0])
        else:
            lane.breaker.update(realized_pnl=equity - yesterday)
            if lane._persisted_daily_fuse_tripped(day_et):
                lane.breaker.tripped = True
        if publish:
            if shared_data is None:
                shared_data = ExtremeMoverScanner.from_env(lane.account).data_client
            reconcile_fills(lane, shared_data)
            refresh_markouts(lane, shared_data)
            lane.write_status(
                equity=float(equity),
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
    args = p.parse_args()
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

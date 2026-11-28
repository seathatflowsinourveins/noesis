"""Tier-3 Key A: per-leg paper-merit record, latched demotion and capital ladder (ADR-0020/0029).

Every evidence file is synthetic and written under a temporary directory; nothing here reads
the real manifests. The ledger DDL copies the real ``paper_entry_decisions`` /
``paper_tca_fills`` / ``paper_daily_fuse`` tables, and the JSON builders copy the shape of the
real falsification dossier and Stage-4 drill manifest.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
import math
import os
import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from tier0_sota.live import merit
from tier0_sota.live.merit import (
    CAPITAL_LADDER,
    LIVE_PROMOTION_MERIT_SCHEMA,
    LivePromotionThresholds,
    compute_live_promotion_merit,
    leg_has_merit,
    next_promotion_state,
)

NOW = datetime(2026, 12, 15, 15, 0, tzinfo=UTC)  # Tuesday, 10:00 ET
HYPOTHESIS_ID = "H_SYNTHETIC_TREND_AND_SHORT"
SMALL = LivePromotionThresholds(
    min_round_trips=6,
    min_sessions=3,
    min_supervised_exits=2,
    short_min_live_confirmed_round_trips=2,
)
# The explicit "no promotion history" state the opt-in step writes once. ``None`` is not it.
NO_HISTORY = merit.initial_promotion_state(now_utc=datetime(2026, 11, 2, 15, 0, tzinfo=UTC))
PAPER_FILES = tuple(merit.PAPER_LEDGERS.values())
LIVE_FILE = "paper_lane_live1.duckdb"
JSON_FILES = {
    "dossier": merit.DOSSIER_FILE,
    "stage4": merit.STAGE4_FILE,
    "receipt": merit.RECONCILIATION_FILE,
}

# --- Synthetic ledgers (the real DDL of today plus the columns the recording side is adding) ------

_ENTRY_DDL = (
    "CREATE TABLE paper_entry_decisions ("
    "client_order_id VARCHAR PRIMARY KEY, symbol VARCHAR, side VARCHAR, "
    "decision_price DECIMAL(18,6), arrival_price DECIMAL(18,6), session VARCHAR, "
    "origin VARCHAR, annual_rate DECIMAL(18,10), prior_close DECIMAL(18,6), "
    "initial_stop DECIMAL(18,6), natr_pct DECIMAL(18,8), closed_at TIMESTAMPTZ, "
    "active_stop DECIMAL(18,6), exit_triggered BOOLEAN DEFAULT false, "
    "decided_at TIMESTAMPTZ DEFAULT current_timestamp, "
    "adopted_external BOOLEAN DEFAULT false)"
)
_TIER_DDL = (
    "ALTER TABLE paper_entry_decisions ADD COLUMN entry_authority VARCHAR",
    "ALTER TABLE paper_entry_decisions ADD COLUMN lineage_root_id VARCHAR",
    "ALTER TABLE paper_entry_decisions ADD COLUMN lineage_parent_id VARCHAR",
    "ALTER TABLE paper_entry_decisions ADD COLUMN lineage_depth INTEGER DEFAULT 0",
    "ALTER TABLE paper_entry_decisions ADD COLUMN entry_qty DECIMAL(18,6)",
    "ALTER TABLE paper_entry_decisions ADD COLUMN close_cause VARCHAR",
    "ALTER TABLE paper_entry_decisions ADD COLUMN macro_regime INTEGER",
    "ALTER TABLE paper_entry_decisions ADD COLUMN adoption_anchor_price DECIMAL(18,6)",
)
_BINDING_DDL = "ALTER TABLE paper_entry_decisions ADD COLUMN dossier_hypothesis_id VARCHAR"
_FILLS_DDL = (
    "CREATE TABLE paper_tca_fills ("
    "ts_utc VARCHAR, account VARCHAR, lane_id VARCHAR, broker_order_id VARCHAR, "
    "symbol VARCHAR, side VARCHAR, session VARCHAR, route VARCHAR, "
    "protective_stop_mode VARCHAR, order_qty DECIMAL(18, 6), filled_qty DECIMAL(18, 6), "
    "decision_price DECIMAL(18, 6), arrival_price DECIMAL(18, 6), "
    "fill_price DECIMAL(18, 6), horizon_price DECIMAL(18, 6), "
    "delay_bps DECIMAL(18, 6), execution_bps DECIMAL(18, 6), "
    "implementation_shortfall_bps DECIMAL(18, 6), commission_usd DECIMAL(18, 6), "
    "commission_bps DECIMAL(18, 6), borrow_fee_usd DECIMAL(18, 6), "
    "borrow_bps DECIMAL(18, 6), opportunity_bps DECIMAL(18, 6), "
    "total_net_cost_bps DECIMAL(18, 6), total_is_usd DECIMAL(18, 6), "
    "borrow_tier VARCHAR, borrow_days INTEGER, markout_60s_bps DECIMAL(18, 6), "
    "execution_id VARCHAR)"
)
_FILL_LINK_DDL = "ALTER TABLE paper_tca_fills ADD COLUMN entry_client_order_id VARCHAR"
_FUSE_DDL = "CREATE TABLE paper_daily_fuse (day DATE PRIMARY KEY, account_id UUID, tripped BOOLEAN)"
_FUSE_DDL_NO_TRIPPED = "CREATE TABLE paper_daily_fuse (day DATE PRIMARY KEY, account_id UUID)"
_TRIP_CAUSE_DDL = "ALTER TABLE paper_daily_fuse ADD COLUMN trip_cause VARCHAR"
_ENTRY_INSERT = (
    "INSERT INTO paper_entry_decisions "
    "(client_order_id, symbol, side, session, origin, closed_at, exit_triggered, decided_at) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)
_ENTRY_TIER_UPDATE = (
    "UPDATE paper_entry_decisions SET entry_authority = ?, close_cause = ?, macro_regime = ? "
    "WHERE client_order_id = ?"
)
_ENTRY_BINDING_UPDATE = (
    "UPDATE paper_entry_decisions SET dossier_hypothesis_id = ? WHERE client_order_id = ?"
)
_FILL_INSERT = (
    "INSERT INTO paper_tca_fills "
    "(ts_utc, account, lane_id, symbol, side, session, implementation_shortfall_bps, execution_id) "
    "VALUES (?, 'SYNTHETIC_LANE', ?, ?, ?, 'RTH', ?, ?)"
)
_FILL_INSERT_LINKED = (
    "INSERT INTO paper_tca_fills "
    "(ts_utc, account, lane_id, symbol, side, session, implementation_shortfall_bps, "
    "execution_id, entry_client_order_id) "
    "VALUES (?, 'SYNTHETIC_LANE', ?, ?, ?, 'RTH', ?, ?, ?)"
)
_FUSE_INSERT = "INSERT INTO paper_daily_fuse (day, account_id, tripped) VALUES (?, ?, ?)"
_FUSE_INSERT_NO_TRIPPED = "INSERT INTO paper_daily_fuse (day, account_id) VALUES (?, ?)"
_FUSE_CAUSE_UPDATE = "UPDATE paper_daily_fuse SET trip_cause = ? WHERE day = ?"
# One synthetic broker account and lane per ledger file: no two ledgers describe the same one.
_ACCOUNT_IDS = {index: f"00000000-0000-0000-0000-{index:012d}" for index in (1, 2, 3, 9)}
_LANE_IDS = {1: "L1_SYNTHETIC", 2: "L2_SYNTHETIC", 3: "L3_SYNTHETIC", 9: "LV1_SYNTHETIC"}


def _account_sha256(account: int) -> str:
    """What the module reads instead of the account id, and what a receipt binds a ledger by."""
    return hashlib.sha256(_ACCOUNT_IDS[account].encode("utf-8")).hexdigest()


def _write_ledger(
    path: Path,
    *,
    entries: list[dict[str, Any]] | None = None,
    fills: list[dict[str, Any]] | None = None,
    fuse: list[tuple[str, bool, str | None]] | None = None,
    tier_columns: bool = True,
    binding_column: bool = True,
    fill_link_column: bool = True,
    trip_cause_column: bool = True,
    tripped_column: bool = True,
    entry_table: bool = True,
    fills_table: bool = True,
    fuse_table: bool = True,
    account: int = 1,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lane_id = _LANE_IDS[account]
    conn = duckdb.connect(str(path))
    try:
        if fills_table:
            conn.execute(_FILLS_DDL)
            if fill_link_column:
                conn.execute(_FILL_LINK_DDL)
        if fuse_table:
            conn.execute(_FUSE_DDL if tripped_column else _FUSE_DDL_NO_TRIPPED)
            if trip_cause_column:
                conn.execute(_TRIP_CAUSE_DDL)
        if entry_table:
            conn.execute(_ENTRY_DDL)
            for ddl in _TIER_DDL if tier_columns else ():
                conn.execute(ddl)
            if binding_column:
                conn.execute(_BINDING_DDL)
        rows = entries or []
        conn.begin()  # one commit for the rows instead of one per row
        if rows:
            conn.executemany(
                _ENTRY_INSERT,
                [
                    (
                        row["cid"],
                        row["symbol"],
                        row["side"],
                        row["session"],
                        row["origin"],
                        row["closed_at"],
                        row["exit_triggered"],
                        row["decided_at"],
                    )
                    for row in rows
                ],
            )
            if tier_columns:
                conn.executemany(
                    _ENTRY_TIER_UPDATE,
                    [
                        (
                            row["entry_authority"],
                            row["close_cause"],
                            row["macro_regime"],
                            row["cid"],
                        )
                        for row in rows
                    ],
                )
            if binding_column:
                conn.executemany(
                    _ENTRY_BINDING_UPDATE,
                    [(row["dossier_hypothesis_id"], row["cid"]) for row in rows],
                )
        if fills and fills_table and fill_link_column:
            conn.executemany(
                _FILL_INSERT_LINKED,
                [
                    (
                        fill["ts_utc"],
                        fill.get("lane_id", lane_id),
                        fill["symbol"],
                        fill["side"],
                        fill["implementation_shortfall_bps"],
                        fill["execution_id"],
                        fill["entry_client_order_id"],
                    )
                    for fill in fills
                ],
            )
        elif fills and fills_table:
            conn.executemany(
                _FILL_INSERT,
                [
                    (
                        fill["ts_utc"],
                        fill.get("lane_id", lane_id),
                        fill["symbol"],
                        fill["side"],
                        fill["implementation_shortfall_bps"],
                        fill["execution_id"],
                    )
                    for fill in fills
                ],
            )
        if fuse and fuse_table:
            if tripped_column:
                conn.executemany(
                    _FUSE_INSERT,
                    [(day, _ACCOUNT_IDS[account], tripped) for day, tripped, _ in fuse],
                )
            else:
                conn.executemany(
                    _FUSE_INSERT_NO_TRIPPED, [(day, _ACCOUNT_IDS[account]) for day, _, _ in fuse]
                )
            if trip_cause_column:
                conn.executemany(_FUSE_CAUSE_UPDATE, [(cause, day) for day, _, cause in fuse])
        conn.commit()
    finally:
        conn.close()


_LEDGER_CACHE: dict[str, bytes] = {}


def _ledger_bytes(spec: dict[str, Any]) -> bytes:
    """The DuckDB file of a ledger spec. The most recent specs are kept: most tests share them."""
    key = json.dumps(spec, sort_keys=True, default=str)
    cached = _LEDGER_CACHE.pop(key, None)
    if cached is None:
        with tempfile.TemporaryDirectory(prefix="tier0-merit-test-") as tmp:
            path = Path(tmp) / "ledger.duckdb"
            _write_ledger(path, **spec)
            cached = path.read_bytes()
    _LEDGER_CACHE[key] = cached  # re-inserted last: the dict is the recency order
    for stale in list(_LEDGER_CACHE)[:-24]:
        del _LEDGER_CACHE[stale]
    return cached


def _session_days(count: int, before: datetime = NOW) -> list[datetime]:
    """``count`` weekday instants at 15:00 UTC (10:00 ET), newest first, all before ``before``."""
    anchor = before.replace(hour=15, minute=0, second=0, microsecond=0)
    candidates = (anchor - timedelta(days=back) for back in range(1, 4 * count + 10))
    return [day for day in candidates if day.weekday() < 5][:count]


def _round_trips(
    leg: str,
    count: int,
    *,
    sessions: int = 4,
    supervised: int = 3,
    regimes: tuple[int | None, ...] = (1, -1),
    shortfall: str = "3.000",
    authority: str | None = "AUTONOMOUS",
    origin: str | None = None,
    hypothesis: str | None = HYPOTHESIS_ID,
    prefix: str = "rt",
    before: datetime = NOW,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Completed round trips of one leg, each with an entry fill and an exit fill."""
    days = _session_days(sessions, before)
    base_origin = origin or ("TREND_ALIGNED_LONG" if leg == "LONG" else "DIRECT_SHORT_ETB")
    entries: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    for index in range(count):
        decided = days[index % sessions] + timedelta(seconds=index)
        cause = "HARNESS_EXIT_FILLED" if index < supervised else "PROTECTIVE_STOP_FILLED"
        cid = f"{prefix}-{leg}-{index}"
        entries.append(
            {
                "cid": cid,
                "symbol": f"SYN{index % 7}",
                "side": leg,
                "session": "RTH",
                "origin": base_origin,
                "decided_at": decided,
                "closed_at": decided + timedelta(minutes=10),
                "exit_triggered": cause == "HARNESS_EXIT_FILLED",
                "entry_authority": authority,
                "close_cause": cause,
                "macro_regime": regimes[index % len(regimes)],
                "dossier_hypothesis_id": hypothesis,
            }
        )
        for suffix, side in (("in", "BUY"), ("out", "SELL")):
            fills.append(
                {
                    "execution_id": f"{cid}-{suffix}",
                    "entry_client_order_id": cid,
                    "ts_utc": decided.isoformat(),
                    "symbol": f"SYN{index % 7}",
                    "side": side if leg == "LONG" else ("SELL" if side == "BUY" else "BUY"),
                    "implementation_shortfall_bps": Decimal(shortfall),
                }
            )
    return entries, fills


# --- Synthetic JSON evidence in the shape the harness writes today --------------------------------


def _falsification(dsr: Any, pbo: Any, promoted: bool) -> dict[str, Any]:
    return {
        "n_trades": 64,
        "worst_trade_pct": -1.2,
        "mean_bps": 12.5 if promoted else -3.1,
        "cpcv_splits": 28,
        "cpcv_oos_mean_bps": 9.1 if promoted else -2.2,
        "dsr": dsr,
        "min_trl": 40.0 if promoted else None,
        "pbo": pbo,
        "hac_t": 4.2 if promoted else 0.4,
        "spa_p": 0.01 if promoted else 0.61,
        "spa_p_upper": 0.02 if promoted else 0.7,
        "promoted": promoted,
        "verdict": "PROMOTE" if promoted else "DISCARD",
    }


def _six_gate(passing: bool, *, dsr: Any = None, pbo: Any = None) -> dict[str, Any]:
    dsr = (0.97 if passing else 0.12) if dsr is None else dsr
    pbo = (0.21 if passing else 0.58) if pbo is None else pbo
    gates = {name: passing for name in merit.DOSSIER_GATES}
    gates["1_hftbacktest_l1_markout"] = True
    l1 = {"data_source": "SYNTHETIC", "passive_filled": 32, "gate_1_passed": True}
    return {
        "n_trials_total": 128,
        "hlz_t_hurdle": 3.9,
        "empirical_pbo": pbo,
        "gates_passed_count": sum(1 for flag in gates.values() if flag),
        "gates": gates,
        "l1_markout": l1,
        "falsification_1x_4bps": _falsification(dsr, pbo, passing),
        "falsification_calibrated_liquid": _falsification(dsr, pbo, passing),
        "falsification_2x_8bps": _falsification(dsr, pbo, passing),
        "promoted": passing,
        "verdict": "PROMOTE" if passing else "DISCARD",
    }


def _dossier(*, long_pass: bool = True, short_pass: bool = True) -> dict[str, Any]:
    both = long_pass and short_pass
    long_block, short_block = _six_gate(long_pass), _six_gate(short_pass)
    return {
        "schema_version": merit.DOSSIER_SCHEMA,
        "generated_at_utc": None,  # stamped by _write_plan from the plan's age
        "hypothesis_id": HYPOTHESIS_ID,
        "prewall_cutoff_iso": "2026-10-15",
        "paper_only": True,
        "live_execution_enabled": False,
        "dry_run": False,
        "risk_kernel_sha256": "0" * 64,
        "trial_accounting": {"cumulative_trials_total": 128, "hlz_t_hurdle": 3.9},
        "tuning": {
            "selected_arm_id": "ARM_SYNTHETIC",
            "empirical_pbo_long": long_block["empirical_pbo"],
            "empirical_pbo_short": short_block["empirical_pbo"],
            "empirical_pbo_combined": 0.05,
        },
        "holdout_six_gate": {
            "LONG": long_block,
            "SHORT": short_block,
            "COMBINED": _six_gate(True),  # a pooled pass must never reach a leg
        },
        "verdict": {
            "cross_leg_promoted": both,
            "overall_verdict": "PROMOTE" if both else "DISCARD",
            "long_verdict": long_block["verdict"],
            "short_verdict": short_block["verdict"],
            "combined_verdict": "PROMOTE",
            "rule": "PROMOTE only if BOTH LONG and SHORT independently pass 6/6 gates",
        },
        "mlflow": {"enabled": False, "run_id": None, "status": "SKIPPED"},
        "xerj": {"enabled": False, "doc_id": None, "status": "SKIPPED"},
    }


def _stage4() -> dict[str, Any]:
    return {
        "schema_version": merit.STAGE4_SCHEMA,
        "generated_at_utc": None,
        "adr_anchor": "ADR-0020",
        "paper_only": True,
        "live_execution_enabled": False,
        "stage5_live_authorized": False,
        "semi_auto_confirmed": False,
        "all_drills_passed": True,
        "all_passed": True,
        "receipt_path": "synthetic/stage4_drills_receipt.json",
        "paper_farm_receipt_schema": "tier0.stage4-drills-receipt.v1",
        "safety_invariants": {"risk_kernel_sha256_ok": True, "live_ports_zero_ok": True},
        "drills": {
            "k3_engine_kill_switch": {"drill": "K3", "passed": True},
            "k4_broker_flatten_sandbox": {"drill": "K4", "passed": True},
            "three_way_restart_reconciliation": {"drill": "RECON", "passed": True},
            "paper_farm_stage4_receipt": {"all_passed": True, "passed_count": 3},
        },
    }


def _receipt() -> dict[str, Any]:
    """The receipt the recording side has to write: per lane, which broker account it reconciled."""
    return {
        "schema_version": merit.RECONCILIATION_SCHEMA,
        "generated_at_utc": None,
        "all_reconciled": True,
        "accounts": {
            lane: {
                "status": "RECONCILED_OK",
                "unreconciled_delta_count": 0,
                "account_sha256": _account_sha256(account),
            }
            for account, lane in enumerate(merit.PAPER_LEDGERS, start=1)
        },
    }


# --- A plan is the whole evidence set as data; _write_plan turns it into files --------------------


def _fuse_days(count: int, before: datetime = NOW) -> list[tuple[str, bool, str | None]]:
    """An untripped fuse row for each of the ``count`` session days rows are decided on."""
    return [(day.date().isoformat(), False, None) for day in _session_days(count, before)]


def _base_plan(
    *,
    count: int = 8,
    sessions: int = 4,
    supervised: int = 3,
    live_shorts: int = 2,
    before: datetime = NOW,
) -> dict[str, Any]:
    """Evidence under which both legs pass ``SMALL`` (or the defaults, with larger counts)."""
    long_rows, long_fills = _round_trips(
        "LONG", count, sessions=sessions, supervised=supervised, before=before
    )
    short_rows, short_fills = _round_trips(
        "SHORT", count, sessions=sessions, supervised=supervised, before=before
    )
    live_rows, live_fills = _round_trips(
        "SHORT",
        live_shorts,
        authority="OPERATOR_CONFIRMED",
        hypothesis=None,
        prefix="live",
        before=before,
    )
    quiet = _fuse_days(max(sessions, 4), before)
    return {
        "dossier": _dossier(),
        "stage4": _stage4(),
        "receipt": _receipt(),
        "paper": {
            "entries": long_rows + short_rows,
            "fills": long_fills + short_fills,
            "fuse": quiet,
            "tier_columns": True,
            "binding_column": True,
            "fill_link_column": True,
            "trip_cause_column": True,
            "account": 1,
        },
        "live": {"entries": live_rows, "fills": live_fills, "fuse": list(quiet), "account": 9},
        "age_hours": {"dossier": 1.0, "stage4": 1.0, "receipt": 1.0, "paper": 1.0, "live": 1.0},
    }


def _stamp(path: Path, moment: datetime) -> None:
    os.utime(path, (moment.timestamp(), moment.timestamp()))


def _idle_spec(paper: dict[str, Any], account: int) -> dict[str, Any]:
    """An idle lane next to ``paper``: the same tables and columns, its own account and lane,
    its own untripped fuse rows, no entries and no fills."""
    idle = {key: flag for key, flag in paper.items() if key.endswith(("_column", "_table"))}
    idle["tier_columns"] = paper["tier_columns"]
    idle["fuse"] = [(day, False, None) for day, _, _ in paper.get("fuse") or []]
    idle["account"] = account
    return idle


def _write_plan(root: Path, plan: dict[str, Any], *, now: datetime = NOW) -> Path:
    """Write the plan under ``root`` (replacing it) and return the paper evidence directory."""
    shutil.rmtree(root, ignore_errors=True)
    evidence_dir = root / "manifests"
    evidence_dir.mkdir(parents=True)
    ages = plan["age_hours"]
    for key, name in JSON_FILES.items():
        if plan[key] is None:
            continue
        doc = dict(plan[key])
        if doc.get("generated_at_utc") is None:
            doc["generated_at_utc"] = (now - timedelta(hours=ages[key])).isoformat()
        (evidence_dir / name).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    paper = plan["paper"]
    if paper is not None:
        # All entries live in the first paper ledger; ``paper2`` / ``paper3`` replace the idle
        # lanes when a test needs rows (or a wrong identity) in the other two.
        (evidence_dir / PAPER_FILES[0]).write_bytes(_ledger_bytes(paper))
        for account, name in enumerate(PAPER_FILES[1:], start=2):
            spec = plan.get(f"paper{account}") or _idle_spec(paper, account)
            (evidence_dir / name).write_bytes(_ledger_bytes(spec))
        for name in PAPER_FILES:
            _stamp(evidence_dir / name, now - timedelta(hours=ages["paper"]))
    if plan["live"] is not None:
        live_path = root / "live" / "manifests" / LIVE_FILE
        live_path.parent.mkdir(parents=True)
        live_path.write_bytes(_ledger_bytes(plan["live"]))
        _stamp(live_path, now - timedelta(hours=ages["live"]))
    return evidence_dir


def _compute(
    root: Path,
    plan: dict[str, Any],
    *,
    now: datetime = NOW,
    thresholds: LivePromotionThresholds | None = SMALL,
    prior_state: Any = NO_HISTORY,
) -> dict[str, Any]:
    evidence_dir = _write_plan(root, plan, now=now)
    return compute_live_promotion_merit(
        evidence_dir, now_utc=now, thresholds=thresholds, prior_state=prior_state
    )


def _statuses(record: dict[str, Any], leg: str) -> dict[str, str]:
    return {name: item["status"] for name, item in record["legs"][leg]["criteria"].items()}


def _assert_record_invariants(
    record: dict[str, Any],
    *,
    now: datetime = NOW,
    thresholds: LivePromotionThresholds | None = SMALL,
) -> None:
    """The contract every record keeps, whatever the evidence."""
    limits = thresholds or LivePromotionThresholds()
    assert record["schema"] == LIVE_PROMOTION_MERIT_SCHEMA
    assert json.loads(json.dumps(record)) == record  # plain JSON data
    assert record["inputs_hash"] == merit._inputs_hash(record["inputs"])
    assert record["record_sha256"] == merit._record_digest(record)
    assert {item["name"] for item in record["inputs"]} == {
        *JSON_FILES.values(),
        *PAPER_FILES,
        merit.LIVE_LEDGER,
    }
    # No broker account id, and no digest of one, is ever written to a record.
    text = json.dumps(record)
    for account, account_id in _ACCOUNT_IDS.items():
        assert account_id not in text and _account_sha256(account) not in text
    for leg in merit.LEGS:
        entry = record["legs"][leg]
        assert isinstance(entry["merit"], bool)
        assert set(entry["criteria"]) == set(merit._expected_criteria(leg))
        failing = {k: v for k, v in entry["criteria"].items() if v["status"] != "pass"}
        assert entry["merit"] is (not failing)
        assert bool(entry["reasons"]) is bool(failing)
        for name, item in entry["criteria"].items():
            assert item["status"] in ("pass", "fail", "missing")
            assert bool(item["reasons"]) is (item["status"] != "pass")
            assert set(item["reasons"]) <= set(entry["reasons"])
            # A pass is re-derivable from the observed value and the configured threshold.
            assert merit._criterion_rechecks(name, item, limits) is (item["status"] == "pass")
        for code in entry["reasons"]:
            assert code and code == code.upper() and code.replace("_", "").isalnum()
        reloaded = json.loads(json.dumps(record))
        assert leg_has_merit(reloaded, leg, now_utc=now, thresholds=thresholds) is entry["merit"]


def _reseal(record: dict[str, Any]) -> dict[str, Any]:
    """Recompute the body hash after an edit: the hash is a damage check, not a signature, so
    every semantic rejection below has to hold for a record whose hash was fixed up as well."""
    record["record_sha256"] = merit._record_digest(record)
    return record


@pytest.fixture(scope="module")
def passing_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Evidence under which both legs pass ``SMALL``, written once."""
    return _write_plan(tmp_path_factory.mktemp("passing-small"), _base_plan())


@pytest.fixture(scope="module")
def passing_small_record(passing_dir: Path) -> dict[str, Any]:
    return compute_live_promotion_merit(
        passing_dir, now_utc=NOW, thresholds=SMALL, prior_state=NO_HISTORY
    )


# --- Live evidence as the caller passes it to next_promotion_state --------------------------------


def _trips(
    count: int,
    *,
    start: datetime,
    step: int = 0,
    shortfall: str = "2.0",
    fault: bool = False,
    cause: str = "PROTECTIVE_STOP_FILLED",
    authority: str = "AUTONOMOUS",
) -> list[dict[str, Any]]:
    """Closed live round trips of one leg, one second apart, each with its own entry order id."""
    return [
        {
            "entry_client_order_id": f"{authority[:4]}-{start.isoformat()}-{index}",
            "entry_authority": authority,
            "opened_at": (start + timedelta(seconds=index)).isoformat(),
            "closed_at": (start + timedelta(seconds=index, milliseconds=500)).isoformat(),
            "shortfall_bps": Decimal(shortfall),
            "capital_step": step if authority == "AUTONOMOUS" else None,
            "protection_fault": fault,
            "close_cause": cause,
        }
        for index in range(count)
    ]


def _live(
    long_trips: list[dict[str, Any]],
    *,
    faults: int = 0,
    fuse: list[dict[str, Any]] | None = None,
    short_trips: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "fuse_sessions": fuse or [],
        "legs": {
            "LONG": {"open_protection_faults": faults, "round_trips": long_trips},
            "SHORT": {"open_protection_faults": 0, "round_trips": short_trips or []},
        },
    }


QUIET = _live([])  # live evidence that was observed and shows nothing


def _cycle(
    evidence: Any,
    state: Any,
    live: Any,
    now: datetime,
    thresholds: LivePromotionThresholds = SMALL,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """One decision cycle on already-read evidence: the record for ``state``, then the state."""
    record = merit._evaluate(evidence, thresholds, now, state)
    return record, next_promotion_state(
        state, record, live_evidence=live, now_utc=now, thresholds=thresholds
    )


# --- Tests ----------------------------------------------------------------------------------------


def test_threshold_defaults_are_the_owner_literals() -> None:
    defaults = LivePromotionThresholds()
    assert LIVE_PROMOTION_MERIT_SCHEMA == "tier0.live-promotion-merit.v1"
    assert defaults.dsr_min == 0.95
    assert defaults.pbo_max == 0.50
    assert defaults.min_round_trips == 200
    assert defaults.min_sessions == 20
    assert defaults.min_regimes == 2
    assert defaults.max_shortfall_bps == 8
    assert defaults.min_supervised_exits == 30
    assert defaults.evidence_max_age_hours == 36
    assert defaults.short_min_live_confirmed_round_trips == 10
    assert defaults.demotion_cooloff_sessions == 5
    assert len(LivePromotionThresholds.model_fields) == 10
    # Frozen, closed to unknown names, and not satisfied by a bool, a NaN or text.
    with pytest.raises(ValidationError):
        defaults.min_round_trips = 1  # ty: ignore[invalid-assignment]
    rejected: list[dict[str, Any]] = [
        {"an_unknown_threshold": 1},
        {"min_round_trips": True},
        {"min_round_trips": "200"},
        {"min_round_trips": 0},
        {"dsr_min": True},
        {"dsr_min": "0.95"},
        {"dsr_min": float("nan")},
        {"dsr_min": 1.01},
        {"pbo_max": -0.1},
        {"pbo_max": float("inf")},
        {"max_shortfall_bps": float("nan")},
        {"max_shortfall_bps": -1},
        {"evidence_max_age_hours": 0},
        {"short_min_live_confirmed_round_trips": -1},
        {"demotion_cooloff_sessions": 2.5},
    ]
    for kwargs in rejected:
        with pytest.raises(ValidationError):
            LivePromotionThresholds(**kwargs)
    # The echoed thresholds of a record validate back to the same values.
    tuned = LivePromotionThresholds(dsr_min=0.99, max_shortfall_bps=Decimal("6.5"))
    assert LivePromotionThresholds.model_validate(tuned.model_dump(mode="json")) == tuned
    assert json.loads(json.dumps(tuned.model_dump(mode="json"))) == tuned.model_dump(mode="json")
    # Thresholds are a LivePromotionThresholds or None, never a look-alike mapping.
    for call in (
        lambda: compute_live_promotion_merit(Path("."), now_utc=NOW, thresholds={}),  # ty: ignore[invalid-argument-type]
        lambda: next_promotion_state(NO_HISTORY, {}, now_utc=NOW, thresholds={}),  # ty: ignore[invalid-argument-type]
    ):
        with pytest.raises(TypeError, match="LivePromotionThresholds"):
            call()


def test_live_promotion_merit_per_leg_and_decay(tmp_path: Path) -> None:
    # Owner defaults: 200 round trips over 20+ sessions, 30 supervised exits, 10 live shorts.
    plan = _base_plan(count=200, sessions=22, supervised=30, live_shorts=10)
    passing = _compute(tmp_path / "pass", plan, thresholds=None)
    _assert_record_invariants(passing, thresholds=None)
    assert passing["thresholds"] == LivePromotionThresholds().model_dump(mode="json")
    assert passing["generator"] == {"module": "tier0_sota.live.merit", "version": "1.1.0"}
    for leg in merit.LEGS:
        entry = passing["legs"][leg]
        assert entry["merit"] is True and entry["reasons"] == []
        assert set(_statuses(passing, leg).values()) == {"pass"}
        assert entry["criteria"]["paper_round_trips"]["observed"] == 200
        assert entry["criteria"]["paper_sessions"]["observed"] == 22
        assert entry["criteria"]["paper_supervised_exits"]["observed"] == 30
        assert entry["criteria"]["paper_shortfall"]["observed"] == {
            "mean_bps": "3.000000",
            "trailing_mean_bps": "3.000000",
            "trailing_round_trips": 200,
        }
        assert entry["criteria"]["dossier_dsr"]["observed"] == "0.97"
        assert entry["capital_step"] == CAPITAL_LADDER[0].model_dump(mode="json")
        assert entry["capital_step"]["max_qty"] == 1
        assert entry["evidence_floor"] is None  # never demoted: every row counts
        assert leg_has_merit(passing, leg, now_utc=NOW) is True
    # The record expires when its oldest evidence (one hour old) reaches the 36 h bound.
    assert passing["computed_at"] == NOW.isoformat()
    assert passing["valid_until"] == (NOW + timedelta(hours=35)).isoformat()
    # Deterministic: the same evidence bytes, state and now_utc give the same record.
    evidence_dir = tmp_path / "pass" / "manifests"
    again = compute_live_promotion_merit(evidence_dir, now_utc=NOW, prior_state=NO_HISTORY)
    assert again == passing
    assert (
        compute_live_promotion_merit(str(evidence_dir), now_utc=NOW, prior_state=NO_HISTORY)
        == passing
    )
    # No promotion state is an unknown, not "no history": the same evidence gives no merit.
    stateless = compute_live_promotion_merit(evidence_dir, now_utc=NOW)
    _assert_record_invariants(stateless, thresholds=None)
    assert stateless["prior_state_sha256"] is None
    for leg in merit.LEGS:
        assert stateless["legs"][leg]["merit"] is False
        assert stateless["legs"][leg]["reasons"] == ["PROMOTION_STATE_MISSING"]
        assert _statuses(stateless, leg)["demotion_latch"] == "missing"
        assert leg_has_merit(stateless, leg, now_utc=NOW) is False

    # Per leg: the SHORT dossier fails while LONG keeps its merit.
    short_fails = copy.deepcopy(plan)
    short_fails["dossier"] = _dossier(long_pass=True, short_pass=False)
    record = _compute(tmp_path / "short-fails", short_fails, thresholds=None)
    _assert_record_invariants(record, thresholds=None)
    assert record["legs"]["LONG"]["merit"] is True
    assert record["legs"]["SHORT"]["merit"] is False
    assert record["legs"]["SHORT"]["reasons"] == [
        "DOSSIER_NOT_PASSING",
        "DSR_BELOW_MIN",
        "PBO_ABOVE_MAX",
    ]

    # The harness promotes on merit and demotes on decay by itself, per leg.
    promoted = next_promotion_state(NO_HISTORY, passing, live_evidence=QUIET, now_utc=NOW)
    assert {leg: promoted["legs"][leg]["status"] for leg in merit.LEGS} == {
        "LONG": "PROMOTED",
        "SHORT": "PROMOTED",
    }
    for leg in merit.LEGS:
        assert merit.live_entry_leg_merit(promoted, passing, leg, now_utc=NOW) is True

    # Decay: LONG paper shortfall drifts above the bound; SHORT is untouched by it.
    decayed = copy.deepcopy(plan)
    for fill in decayed["paper"]["fills"]:
        if fill["entry_client_order_id"].startswith("rt-LONG-"):
            fill["implementation_shortfall_bps"] = Decimal("8.500")
    decayed_record = _compute(tmp_path / "decay", decayed, thresholds=None, prior_state=promoted)
    _assert_record_invariants(decayed_record, thresholds=None)
    assert decayed_record["legs"]["LONG"]["reasons"] == ["SHORTFALL_ABOVE_MAX"]
    assert decayed_record["legs"]["LONG"]["criteria"]["paper_shortfall"]["status"] == "fail"
    assert decayed_record["legs"]["SHORT"]["merit"] is True
    later = NOW + timedelta(minutes=5)
    after = next_promotion_state(promoted, decayed_record, live_evidence=QUIET, now_utc=later)
    assert after["legs"]["LONG"]["status"] == "DEMOTED" and after["legs"]["LONG"]["latched"]
    assert after["legs"]["LONG"]["demotion_reasons"] == ["MERIT_LOST", "SHORTFALL_ABOVE_MAX"]
    assert after["legs"]["LONG"]["live_entry_eligible"] is False
    assert after["legs"]["SHORT"]["status"] == "PROMOTED"
    assert merit.live_entry_leg_merit(after, decayed_record, "LONG", now_utc=later) is False
    assert merit.live_entry_leg_merit(after, decayed_record, "SHORT", now_utc=later) is True


def test_recent_paper_decay_is_not_diluted_by_history(tmp_path: Path) -> None:
    """Old round trips never age out of the overall mean, so the latest ones are checked too."""
    old_rows, old_fills = _round_trips(
        "LONG", 30, shortfall="3.000", prefix="old", before=NOW - timedelta(days=60)
    )
    new_rows, new_fills = _round_trips("LONG", 6, shortfall="25.000", prefix="new")
    plan = _base_plan()
    plan["paper"]["entries"] = [
        row for row in plan["paper"]["entries"] if row["side"] == "SHORT"
    ] + (old_rows + new_rows)
    plan["paper"]["fills"] = [
        fill for fill in plan["paper"]["fills"] if fill["execution_id"].startswith("rt-SHORT-")
    ] + (old_fills + new_fills)
    plan["paper"]["fuse"] += _fuse_days(4, NOW - timedelta(days=60))
    record = _compute(tmp_path, plan)
    _assert_record_invariants(record)
    shortfall = record["legs"]["LONG"]["criteria"]["paper_shortfall"]
    # (30 * 3 + 6 * 25) / 36 = 6.67 is inside the bound; the latest six round trips are not.
    assert shortfall["observed"] == {
        "mean_bps": "6.666667",
        "trailing_mean_bps": "25.000000",
        "trailing_round_trips": SMALL.min_round_trips,
    }
    assert shortfall["threshold"] == {"mean_bps": "8", "trailing_mean_bps": "8"}
    assert shortfall["status"] == "fail"
    assert record["legs"]["LONG"]["reasons"] == ["SHORTFALL_ABOVE_MAX"]
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 36
    assert record["legs"]["SHORT"]["merit"] is True
    # The other way round (costly history, good recent round trips) still fails on the overall
    # mean: both figures have to be inside the bound.
    for fill in plan["paper"]["fills"]:
        cid = fill["entry_client_order_id"]
        if cid.startswith("old-"):
            fill["implementation_shortfall_bps"] = Decimal("9.000")
        elif cid.startswith("new-"):
            fill["implementation_shortfall_bps"] = Decimal("1.000")
    record = _compute(tmp_path, plan)
    shortfall = record["legs"]["LONG"]["criteria"]["paper_shortfall"]
    assert shortfall["observed"]["trailing_mean_bps"] == "1.000000"
    assert shortfall["observed"]["mean_bps"] == "7.666667"
    assert shortfall["status"] == "pass"
    for fill in plan["paper"]["fills"]:
        if fill["entry_client_order_id"].startswith("old-"):
            fill["implementation_shortfall_bps"] = Decimal("10.000")
    record = _compute(tmp_path, plan)
    assert record["legs"]["LONG"]["criteria"]["paper_shortfall"]["observed"]["mean_bps"] == (
        "8.500000"
    )
    assert record["legs"]["LONG"]["reasons"] == ["SHORTFALL_ABOVE_MAX"]


def _json_stale(root: Path, key: str) -> None:
    path = root / "manifests" / JSON_FILES[key]
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["generated_at_utc"] = (NOW - timedelta(hours=36, minutes=1)).isoformat()
    path.write_text(json.dumps(doc), encoding="utf-8")


def _json_wrong_schema(root: Path, key: str) -> None:
    path = root / "manifests" / JSON_FILES[key]
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["schema_version"] = "tier0.something-else.v9"
    path.write_text(json.dumps(doc), encoding="utf-8")


def _ledger_path(root: Path, key: str) -> Path:
    if key == "live":
        return root / "live" / "manifests" / LIVE_FILE
    return root / "manifests" / PAPER_FILES[int(key[-1]) - 1]


def _ledger_wrong_schema(root: Path, key: str) -> None:
    path = _ledger_path(root, key)
    path.unlink()
    _write_ledger(path, entry_table=False)  # a real DuckDB file without the entry ledger
    _stamp(path, NOW - timedelta(hours=1))


_MUTATIONS: dict[str, Any] = {
    "missing": lambda root, key: (
        (root / "manifests" / JSON_FILES[key]).unlink()
        if key in JSON_FILES
        else _ledger_path(root, key).unlink()
    ),
    "stale": lambda root, key: (
        _json_stale(root, key)
        if key in JSON_FILES
        else _stamp(_ledger_path(root, key), NOW - timedelta(hours=36, minutes=1))
    ),
    "corrupt": lambda root, key: (
        (root / "manifests" / JSON_FILES[key]).write_bytes(b'{"schema_version": "tier0')
        if key in JSON_FILES
        else (
            _ledger_path(root, key).write_bytes(b"this is not a duckdb file"),
            _stamp(_ledger_path(root, key), NOW - timedelta(hours=1)),
        )
    ),
    "wrong_schema": lambda root, key: (
        _json_wrong_schema(root, key) if key in JSON_FILES else _ledger_wrong_schema(root, key)
    ),
}
_MISSING_CODES = {
    "dossier": ["DOSSIER_MISSING"],
    "stage4": ["STAGE4_DRILLS_MISSING"],
    "receipt": ["RECONCILIATION_RECEIPT_MISSING"],
    "pa1": ["PAPER_LEDGER_MISSING"],
    "pa2": ["PAPER_LEDGER_MISSING"],
    "pa3": ["PAPER_LEDGER_MISSING"],
    "live": ["LIVE_LEDGER_MISSING", "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE"],
}


@pytest.mark.parametrize("mode", ["missing", "stale", "corrupt", "wrong_schema"])
@pytest.mark.parametrize("source", ["dossier", "stage4", "receipt", "pa1", "pa2", "pa3", "live"])
def test_missing_stale_or_unattributable_evidence_never_promotes(
    tmp_path: Path, source: str, mode: str
) -> None:
    evidence_dir = _write_plan(tmp_path, _base_plan())
    _MUTATIONS[mode](tmp_path, source)
    record = compute_live_promotion_merit(
        evidence_dir, now_utc=NOW, thresholds=SMALL, prior_state=NO_HISTORY
    )
    _assert_record_invariants(record)
    expected = ["EVIDENCE_STALE"] if mode == "stale" else _MISSING_CODES[source][:1]
    affected = ("SHORT",) if source == "live" else merit.LEGS
    state = next_promotion_state(
        NO_HISTORY, record, live_evidence=QUIET, now_utc=NOW, thresholds=SMALL
    )
    for leg in merit.LEGS:
        entry = record["legs"][leg]
        gate = merit.live_entry_leg_merit(state, record, leg, now_utc=NOW, thresholds=SMALL)
        if leg not in affected:
            assert entry["merit"] is True  # the live ledger is no part of the LONG leg's evidence
            assert state["legs"][leg]["status"] == "PROMOTED" and gate is True
            continue
        assert entry["merit"] is False
        assert expected[0] in entry["reasons"]
        assert "fail" not in _statuses(record, leg).values()  # nothing was inferred as a failure
        assert "missing" in _statuses(record, leg).values()
        assert leg_has_merit(record, leg, now_utc=NOW, thresholds=SMALL) is False
        assert state["legs"][leg]["status"] == "PAPER"
        assert state["legs"][leg]["live_entry_eligible"] is False and gate is False
    if source == "live":
        assert "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE" in record["legs"]["SHORT"]["reasons"]
    found = {item["name"]: item["found"] for item in record["inputs"]}
    assert all(found.values()) is (mode != "missing")


# --- One deterministic case per reason code, with the exact reasons and observed counts -----------


def _set_long(plan: dict[str, Any], count: int, **kwargs: Any) -> None:
    """Replace the LONG leg's paper round trips."""
    rows, fills = _round_trips("LONG", count, **kwargs)
    paper = plan["paper"]
    paper["entries"] = [row for row in paper["entries"] if row["side"] != "LONG"] + rows
    paper["fills"] = [
        fill for fill in paper["fills"] if not fill["execution_id"].startswith("rt-LONG-")
    ] + fills


def _add_non_round_trips(plan: dict[str, Any]) -> None:
    """Attributable LONG rows that are not round trips: an open position, a cancelled entry, a
    manual close and a row whose close is dated before its decision."""
    rows, fills = _round_trips("LONG", 4, prefix="not-a-round-trip")
    rows[0].update(closed_at=None, close_cause=None)
    rows[1].update(close_cause="ENTRY_CANCELED")
    rows[2].update(close_cause="OWNER_MANUAL_CLOSE")
    rows[3].update(closed_at=rows[3]["decided_at"] - timedelta(minutes=1))
    plan["paper"]["entries"] += rows
    plan["paper"]["fills"] += fills


def _five_round_trips_among_others(plan: dict[str, Any]) -> None:
    _set_long(plan, 5)
    _add_non_round_trips(plan)


def _without(plan: dict[str, Any], **flags: bool) -> None:
    plan["paper"].update(flags)


def _trip(plan: dict[str, Any], cause: str | None) -> None:
    """Trip the fuse of the first paper lane on a day its round trips were decided on."""
    day = plan["paper"]["fuse"][0][0]
    plan["paper"]["fuse"][0] = (day, True, cause)


def _null_tripped(plan: dict[str, Any]) -> None:
    """A fuse row that exists but says nothing: its ``tripped`` flag is NULL."""
    day = plan["paper"]["fuse"][0][0]
    plan["paper"]["fuse"][0] = (day, None, None)


def _trip_without_cause_column(plan: dict[str, Any]) -> None:
    _trip(plan, None)
    plan["paper"]["trip_cause_column"] = False


def _drop_fuse_day(plan: dict[str, Any]) -> None:
    del plan["paper"]["fuse"][0]  # the newest day: both legs decided round trips on it


def _empty_fuse(plan: dict[str, Any]) -> None:
    plan["paper"]["fuse"] = []


def _unbind_long(plan: dict[str, Any]) -> None:
    for row in plan["paper"]["entries"]:
        if row["side"] == "LONG":
            row["dossier_hypothesis_id"] = "H_SOME_OTHER_STUDY"


def _null_authority_short(plan: dict[str, Any]) -> None:
    for row in plan["paper"]["entries"]:
        if row["side"] == "SHORT":
            row["entry_authority"] = None


def _long_fills(plan: dict[str, Any]) -> list[dict[str, Any]]:
    return [f for f in plan["paper"]["fills"] if f["execution_id"].startswith("rt-LONG-")]


def _costly_long(plan: dict[str, Any]) -> None:
    for fill in _long_fills(plan):
        fill["implementation_shortfall_bps"] = Decimal("8.001")


def _unlink_first_long(plan: dict[str, Any]) -> None:
    for fill in _long_fills(plan)[:2]:  # both fills of the first LONG round trip
        fill["entry_client_order_id"] = None


def _second_lane_in_one_ledger(plan: dict[str, Any]) -> None:
    plan["paper"]["fills"][0]["lane_id"] = "L_ANOTHER_LANE"


def _receipt_account(plan: dict[str, Any], lane: str, **fields: Any) -> None:
    plan["receipt"]["accounts"][lane].update(fields)


def _receipt_unbound(plan: dict[str, Any]) -> None:
    del plan["receipt"]["accounts"]["ALPACA_PA2"]["account_sha256"]


def _receipt_swapped(plan: dict[str, Any]) -> None:
    _receipt_account(plan, "ALPACA_PA1", account_sha256=_account_sha256(2))
    _receipt_account(plan, "ALPACA_PA2", account_sha256=_account_sha256(1))


_BOTH = ("LONG", "SHORT")
_NOTHING_COUNTED = [
    "DOSSIER_LEG_BINDING_MISSING",
    "ROUND_TRIPS_BELOW_MIN",
    "SESSIONS_BELOW_MIN",
    "REGIMES_BELOW_MIN",
    "SHORTFALL_EVIDENCE_MISSING",
    "SUPERVISED_EXITS_BELOW_MIN",
]
_UNIDENTIFIED = ["PAPER_LEDGER_IDENTITY_MISSING", "RECONCILIATION_LEDGER_BINDING_MISSING"]
_RECONCILED = {"all_reconciled": True, "clean_accounts": 3, "bound_accounts": 3}


def _fuse_observed(tripped: int, defects: int) -> dict[str, int]:
    """Three ledgers with four fuse days each."""
    return {
        "fuse_sessions": 12,
        "tripped_sessions": tripped,
        "defect_trips": defects,
        "sessions_without_fuse_row": 0,
    }


# (id, mutation of the base plan, legs it breaks, their exact reasons, {criterion: fields}).
# The criterion fields are checked on the first broken leg, or on LONG when nothing breaks.
_REASON_CASES: list[tuple[str, Any, tuple[str, ...], list[str], dict[str, dict[str, Any]]]] = [
    (
        "eight_round_trips_pass",
        lambda plan: None,
        (),
        [],
        {
            "paper_round_trips": {"status": "pass", "observed": 8},
            "paper_sessions": {"status": "pass", "observed": 4},
            "paper_supervised_exits": {"status": "pass", "observed": 3},
            "paper_regimes": {"status": "pass", "observed": 2},
            "dossier_leg_binding": {"observed": {"hypothesis_id": HYPOTHESIS_ID, "bound_rows": 8}},
            "breaker_trips": {"status": "pass", "observed": _fuse_observed(0, 0)},
            "reconciliation_receipt": {"observed": _RECONCILED},
        },
    ),
    (
        "eight_round_trips_in_two_sessions",
        lambda plan: _set_long(plan, 8, sessions=2),
        ("LONG",),
        ["SESSIONS_BELOW_MIN"],
        {
            "paper_sessions": {"status": "fail", "observed": 2, "threshold": 3},
            "paper_round_trips": {"status": "pass", "observed": 8},
        },
    ),
    (
        "one_supervised_exit",
        lambda plan: _set_long(plan, 8, supervised=1),
        ("LONG",),
        ["SUPERVISED_EXITS_BELOW_MIN"],
        {"paper_supervised_exits": {"status": "fail", "observed": 1, "threshold": 2}},
    ),
    (
        "open_cancelled_and_manual_rows_are_not_round_trips",
        _add_non_round_trips,
        (),
        [],
        {
            "paper_round_trips": {"status": "pass", "observed": 8},
            "dossier_leg_binding": {"observed": {"hypothesis_id": HYPOTHESIS_ID, "bound_rows": 12}},
        },
    ),
    (
        "five_round_trips_among_rows_that_are_not",
        _five_round_trips_among_others,
        ("LONG",),
        ["ROUND_TRIPS_BELOW_MIN"],
        {"paper_round_trips": {"status": "fail", "observed": 5, "threshold": 6}},
    ),
    (
        "one_regime",
        lambda plan: _set_long(plan, 8, regimes=(1,)),
        ("LONG",),
        ["REGIMES_BELOW_MIN"],
        {"paper_regimes": {"status": "fail", "observed": 1, "threshold": 2}},
    ),
    (
        "regime_outside_the_vocabulary",
        lambda plan: _set_long(plan, 8, regimes=(1, 7)),
        ("LONG",),
        ["REGIME_EVIDENCE_MISSING"],
        {
            "paper_regimes": {
                "status": "missing",
                "observed": None,
                "detail": "MACRO_REGIME_ON_4_OF_8_ROUND_TRIPS",
            }
        },
    ),
    (
        "regime_on_two_of_eight_round_trips",
        lambda plan: _set_long(plan, 8, regimes=(1, -1, None, None, None, None, None, None)),
        ("LONG",),
        ["REGIME_EVIDENCE_MISSING"],
        {"paper_regimes": {"status": "missing", "detail": "MACRO_REGIME_ON_2_OF_8_ROUND_TRIPS"}},
    ),
    (
        "shortfall_one_tick_above_the_bound",
        _costly_long,
        ("LONG",),
        ["SHORTFALL_ABOVE_MAX"],
        {
            "paper_shortfall": {
                "status": "fail",
                "observed": {
                    "mean_bps": "8.001000",
                    "trailing_mean_bps": "8.001000",
                    "trailing_round_trips": 6,
                },
            }
        },
    ),
    (
        "round_trip_without_linked_fills",
        _unlink_first_long,
        ("LONG",),
        ["SHORTFALL_EVIDENCE_MISSING"],
        {"paper_shortfall": {"status": "missing", "detail": "FILLS_LINKED_FOR_7_OF_8_ROUND_TRIPS"}},
    ),
    (
        "no_entry_authority_column",
        lambda plan: _without(plan, tier_columns=False),
        _BOTH,
        [
            "ENTRY_AUTHORITY_EVIDENCE_MISSING",
            "CLOSE_CAUSE_EVIDENCE_MISSING",
            "REGIME_EVIDENCE_MISSING",
        ],
        {"paper_round_trips": {"status": "missing"}, "breaker_trips": {"status": "pass"}},
    ),
    (
        "no_dossier_binding_column",
        lambda plan: _without(plan, binding_column=False),
        _BOTH,
        ["DOSSIER_LEG_BINDING_MISSING"],
        {"dossier_leg_binding": {"status": "missing"}},
    ),
    (
        "fills_not_linked_to_entries",
        lambda plan: _without(plan, fill_link_column=False),
        _BOTH,
        ["SHORTFALL_EVIDENCE_MISSING"],
        {"paper_shortfall": {"status": "missing"}},
    ),
    (
        "no_fills_table",
        lambda plan: _without(plan, fills_table=False),
        _BOTH,
        ["SHORTFALL_EVIDENCE_MISSING"],
        {"paper_shortfall": {"status": "missing"}},
    ),
    (
        "no_fuse_table",
        lambda plan: _without(plan, fuse_table=False),
        _BOTH,
        ["PAPER_LEDGER_IDENTITY_MISSING", "BREAKER_EVIDENCE_MISSING", *_UNIDENTIFIED[1:]],
        {"breaker_trips": {"status": "missing"}, "paper_round_trips": {"status": "missing"}},
    ),
    (
        "fuse_without_tripped_column",
        lambda plan: _without(plan, tripped_column=False),
        _BOTH,
        ["BREAKER_EVIDENCE_MISSING"],
        {"breaker_trips": {"status": "missing"}},
    ),
    (
        "fuse_row_with_null_tripped",
        _null_tripped,
        _BOTH,
        ["BREAKER_EVIDENCE_MISSING"],
        {"breaker_trips": {"status": "missing", "detail": "DAY_OR_TRIPPED_MALFORMED"}},
    ),
    (
        "empty_fuse_tables",
        _empty_fuse,
        _BOTH,
        _UNIDENTIFIED,
        {"breaker_trips": {"status": "missing"}, "paper_round_trips": {"observed": None}},
    ),
    (
        "counted_day_without_a_fuse_row",
        _drop_fuse_day,
        _BOTH,
        ["BREAKER_EVIDENCE_MISSING"],
        {
            "breaker_trips": {
                "status": "missing",
                "observed": {"fuse_sessions": 9, "sessions_without_fuse_row": 1},
                "detail": "NO_FUSE_ROW_FOR_1_COUNTED_LEDGER_DAYS",
            }
        },
    ),
    (
        "fuse_trip_with_a_defect_cause",
        lambda plan: _trip(plan, "DEFECT_STALE_QUOTE"),
        _BOTH,
        ["BREAKER_DEFECT_TRIP"],
        {"breaker_trips": {"status": "fail", "observed": _fuse_observed(1, 1)}},
    ),
    (
        "fuse_trip_on_a_market_drawdown",
        lambda plan: _trip(plan, "MARKET_DRAWDOWN"),
        (),
        [],
        {"breaker_trips": {"status": "pass", "observed": _fuse_observed(1, 0)}},
    ),
    (
        "fuse_trip_without_cause",
        lambda plan: _trip(plan, None),
        _BOTH,
        ["BREAKER_TRIP_CAUSE_MISSING"],
        {
            "breaker_trips": {
                "status": "missing",
                "observed": {"fuse_sessions": 12, "tripped_sessions": 1},
            }
        },
    ),
    (
        "fuse_trip_without_a_cause_column",
        _trip_without_cause_column,
        _BOTH,
        ["BREAKER_TRIP_CAUSE_MISSING"],
        {"breaker_trips": {"status": "missing"}},
    ),
    (
        "receipt_account_not_reconciled",
        lambda plan: _receipt_account(plan, "ALPACA_PA3", status="HALTED_RECONCILIATION_MISMATCH"),
        _BOTH,
        ["RECONCILIATION_NOT_CLEAN"],
        {
            "reconciliation_receipt": {
                "status": "fail",
                "observed": {**_RECONCILED, "clean_accounts": 2},
            }
        },
    ),
    (
        "receipt_with_an_unreconciled_delta",
        lambda plan: _receipt_account(plan, "ALPACA_PA1", unreconciled_delta_count=1),
        _BOTH,
        ["RECONCILIATION_NOT_CLEAN"],
        {"reconciliation_receipt": {"observed": {**_RECONCILED, "clean_accounts": 2}}},
    ),
    (
        "receipt_not_all_reconciled",
        lambda plan: plan["receipt"].update(all_reconciled=False),
        _BOTH,
        ["RECONCILIATION_NOT_CLEAN"],
        {
            "reconciliation_receipt": {
                "status": "fail",
                "observed": {**_RECONCILED, "all_reconciled": False},
            }
        },
    ),
    (
        "receipt_does_not_name_its_account",
        _receipt_unbound,
        _BOTH,
        ["RECONCILIATION_LEDGER_BINDING_MISSING"],
        {"reconciliation_receipt": {"status": "missing", "observed": {"bound_accounts": 2}}},
    ),
    (
        "receipt_for_other_accounts",
        _receipt_swapped,
        _BOTH,
        ["RECONCILIATION_LEDGER_BINDING_MISSING"],
        {"reconciliation_receipt": {"status": "missing", "observed": {"bound_accounts": 1}}},
    ),
    (
        "two_lanes_in_one_ledger",
        _second_lane_in_one_ledger,
        _BOTH,
        _UNIDENTIFIED,
        {
            "paper_round_trips": {
                "status": "missing",
                "detail": f"{PAPER_FILES[0]}:LANE_ID_NOT_SINGLE_VALUED",
            }
        },
    ),
    ("rows_bound_to_another_dossier", _unbind_long, ("LONG",), _NOTHING_COUNTED, {}),
    ("rows_without_entry_authority", _null_authority_short, ("SHORT",), _NOTHING_COUNTED, {}),
    (
        "dry_run_dossier",
        lambda plan: plan["dossier"].update(dry_run=True),
        _BOTH,
        ["DOSSIER_MISSING"],
        {"dossier_passing": {"status": "missing"}},
    ),
    (
        "dossier_without_leg_block",
        lambda plan: plan["dossier"]["holdout_six_gate"].pop("SHORT"),
        ("SHORT",),
        ["DOSSIER_MISSING"],
        {"dossier_dsr": {"status": "missing"}},
    ),
    (
        "naive_timestamp",  # no offset: not a UTC instant
        lambda plan: plan["stage4"].update(generated_at_utc="2026-12-15T14:00:00"),
        _BOTH,
        ["EVIDENCE_TIMESTAMP_INVALID"],
        {"stage4_drills": {"status": "missing"}},
    ),
    (
        "future_timestamp",
        lambda plan: plan["receipt"].update(
            generated_at_utc=(NOW + timedelta(hours=2)).isoformat()
        ),
        _BOTH,
        ["EVIDENCE_TIMESTAMP_INVALID"],
        {"reconciliation_receipt": {"status": "missing"}},
    ),
]


@pytest.mark.parametrize(
    ("mutate", "legs", "reasons", "fields"),
    [pytest.param(*case[1:], id=case[0]) for case in _REASON_CASES],
)
def test_unattributable_evidence_is_missing_not_inferred(
    tmp_path: Path,
    mutate: Any,
    legs: tuple[str, ...],
    reasons: list[str],
    fields: dict[str, dict[str, Any]],
) -> None:
    plan = _base_plan()
    mutate(plan)
    record = _compute(tmp_path, plan)
    _assert_record_invariants(record)
    state = next_promotion_state(
        NO_HISTORY, record, live_evidence=QUIET, now_utc=NOW, thresholds=SMALL
    )
    for leg in merit.LEGS:
        entry = record["legs"][leg]
        assert entry["reasons"] == (reasons if leg in legs else []), leg
        assert entry["merit"] is (leg not in legs)
        assert state["legs"][leg]["status"] == ("PAPER" if leg in legs else "PROMOTED")
    criteria = record["legs"][legs[0] if legs else "LONG"]["criteria"]
    for name, expected in fields.items():
        observed = {key: criteria[name][key] for key in expected}
        assert observed == expected, name


def test_every_reason_code_has_a_deterministic_case() -> None:
    """The codes a mutant of one criterion would stop emitting are each pinned by a table row."""
    pinned = {code for case in _REASON_CASES for code in case[3]}
    assert {
        "SESSIONS_BELOW_MIN",
        "SUPERVISED_EXITS_BELOW_MIN",
        "ROUND_TRIPS_BELOW_MIN",
        "REGIMES_BELOW_MIN",
        "REGIME_EVIDENCE_MISSING",
        "SHORTFALL_ABOVE_MAX",
        "SHORTFALL_EVIDENCE_MISSING",
        "BREAKER_DEFECT_TRIP",
        "BREAKER_EVIDENCE_MISSING",
        "BREAKER_TRIP_CAUSE_MISSING",
        "RECONCILIATION_NOT_CLEAN",
        "RECONCILIATION_LEDGER_BINDING_MISSING",
        "PAPER_LEDGER_IDENTITY_MISSING",
        "ENTRY_AUTHORITY_EVIDENCE_MISSING",
        "CLOSE_CAUSE_EVIDENCE_MISSING",
        "DOSSIER_LEG_BINDING_MISSING",
        "DOSSIER_MISSING",
        "EVIDENCE_TIMESTAMP_INVALID",
    } <= pinned


def test_duplicated_or_misattributed_ledgers_are_never_counted(tmp_path: Path) -> None:
    """Three round trips per leg are not six, whichever way a ledger is copied or renamed."""
    plan = _base_plan(count=3, supervised=1)
    paper = plan["paper"]
    evidence_dir = _write_plan(tmp_path, plan)
    live_path = tmp_path / "live" / "manifests" / LIVE_FILE
    originals = {path: path.read_bytes() for path in (*evidence_dir.glob("*.duckdb"), live_path)}

    def put(path: Path, content: bytes | dict[str, Any]) -> None:
        path.write_bytes(content if isinstance(content, bytes) else _ledger_bytes(content))
        _stamp(path, NOW - timedelta(hours=1))

    def compute(*, expect: str, detail: str, legs: tuple[str, ...] = _BOTH) -> dict[str, Any]:
        record = compute_live_promotion_merit(
            evidence_dir, now_utc=NOW, thresholds=SMALL, prior_state=NO_HISTORY
        )
        _assert_record_invariants(record)
        for leg in legs:
            entry = record["legs"][leg]
            counted = entry["criteria"]["paper_round_trips"]
            assert entry["merit"] is False
            assert entry["reasons"][:2] == [expect, "RECONCILIATION_LEDGER_BINDING_MISSING"]
            assert (counted["status"], counted["observed"]) == ("missing", None)
            assert detail in counted["detail"], counted["detail"]
            assert entry["criteria"]["breaker_trips"]["status"] == "missing"
            assert leg_has_merit(record, leg, now_utc=NOW, thresholds=SMALL) is False
        for path, content in originals.items():
            put(path, content)
        return record

    pa1, pa2, pa3 = (evidence_dir / name for name in PAPER_FILES)
    single = compute(expect="", detail="", legs=())
    for leg in merit.LEGS:
        assert single["legs"][leg]["reasons"] == [
            "ROUND_TRIPS_BELOW_MIN",
            "SUPERVISED_EXITS_BELOW_MIN",
        ]
        assert single["legs"][leg]["criteria"]["paper_round_trips"]["observed"] == 3
        assert single["legs"][leg]["criteria"]["paper_supervised_exits"]["observed"] == 1

    # The same file under all three lane names (restored, copied or renamed): 3 is not 9.
    put(pa2, originals[pa1])
    put(pa3, originals[pa1])
    compute(expect="PAPER_LEDGER_DUPLICATED", detail=f"SHARES_FILE_SHA256_WITH:{PAPER_FILES[0]}")
    # The same rows written into another account's file: different bytes, the same orders.
    put(pa2, {**paper, "account": 2})
    compute(
        expect="PAPER_LEDGER_DUPLICATED",
        detail=f"SHARES_6_CLIENT_ORDER_IDS_WITH:{PAPER_FILES[0]}",
    )
    # Two files for one broker account, with different rows.
    put(pa2, _idle_spec(paper, 1))
    compute(expect="PAPER_LEDGER_DUPLICATED", detail=f"SHARES_BROKER_ACCOUNT_WITH:{PAPER_FILES[0]}")
    # Two files whose fills belong to one lane.
    foreign = {**paper["fills"][0], "execution_id": "foreign-1", "lane_id": _LANE_IDS[1]}
    put(pa3, {**_idle_spec(paper, 3), "fills": [foreign]})
    compute(expect="PAPER_LEDGER_DUPLICATED", detail=f"SHARES_LANE_ID_WITH:{PAPER_FILES[0]}")
    # One file whose fills belong to two lanes, and one that names no broker account at all.
    two_lanes = [{**foreign, "lane_id": _LANE_IDS[3]}, {**foreign, "lane_id": "L_ANOTHER"}]
    put(pa3, {**_idle_spec(paper, 3), "fills": two_lanes})
    compute(
        expect="PAPER_LEDGER_IDENTITY_MISSING",
        detail=f"{PAPER_FILES[2]}:LANE_ID_NOT_SINGLE_VALUED",
    )
    put(pa2, {**_idle_spec(paper, 2), "fuse": []})
    compute(
        expect="PAPER_LEDGER_IDENTITY_MISSING",
        detail=f"{PAPER_FILES[1]}:BROKER_ACCOUNT_NOT_SINGLE_VALUED",
    )
    # The live ledger is a paper account's ledger: neither side can be told from the other.
    put(live_path, {**plan["live"], "account": 2})
    record = compute(
        expect="PAPER_LEDGER_DUPLICATED", detail=f"SHARES_BROKER_ACCOUNT_WITH:{merit.LIVE_LEDGER}"
    )
    short_live = record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]
    assert short_live["status"] == "missing"
    assert short_live["reasons"] == [
        "LIVE_LEDGER_DUPLICATED",
        "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE",
    ]
    assert record["legs"]["LONG"]["reasons"] == [
        "PAPER_LEDGER_DUPLICATED",
        "RECONCILIATION_LEDGER_BINDING_MISSING",
    ]
    # Every file restored: the original record again, so each rejection came from the copy.
    assert compute(expect="", detail="", legs=()) == single


def test_long_and_short_evidence_are_never_pooled(tmp_path: Path) -> None:
    long_rows, long_fills = _round_trips("LONG", 3, regimes=(1,), shortfall="1.000")
    short_rows, short_fills = _round_trips("SHORT", 10, shortfall="20.000")
    hedge_rows, hedge_fills = _round_trips(
        "LONG", 10, origin="INVERSE_HEDGE_SOXS", shortfall="1.000", prefix="hedge"
    )
    tv_rows, tv_fills = _round_trips(
        "LONG", 10, origin="TV_HALF_AUTO_CONFIRMED", authority=None, prefix="tv"
    )
    lineage_rows, lineage_fills = _round_trips(
        "LONG", 10, authority="OWNER_LINEAGE", prefix="lineage"
    )
    crossed_rows, crossed_fills = _round_trips(
        "LONG", 10, origin="DIRECT_SHORT_ETB", prefix="crossed"
    )
    plan = _base_plan()
    plan["dossier"] = _dossier(long_pass=True, short_pass=False)
    plan["paper"]["entries"] = (
        long_rows + short_rows + hedge_rows + tv_rows + lineage_rows + crossed_rows
    )
    plan["paper"]["fills"] = (
        long_fills + short_fills + hedge_fills + tv_fills + lineage_fills + crossed_fills
    )
    record = _compute(tmp_path, plan)
    _assert_record_invariants(record)
    long_leg, short_leg = record["legs"]["LONG"], record["legs"]["SHORT"]

    # LONG counts its own three scanner round trips: not the SHORT ones, not the inverse hedge
    # (a side=LONG order), not the TV-confirmed or lineage entries, not a LONG row with a SHORT
    # origin.
    assert long_leg["criteria"]["paper_round_trips"]["observed"] == 3
    assert long_leg["criteria"]["paper_round_trips"]["status"] == "fail"
    assert long_leg["criteria"]["paper_regimes"]["observed"] == 1
    assert long_leg["criteria"]["paper_shortfall"]["observed"]["mean_bps"] == "1.000000"
    assert long_leg["criteria"]["paper_shortfall"]["status"] == "pass"
    assert short_leg["criteria"]["paper_round_trips"]["observed"] == 10
    assert short_leg["criteria"]["paper_round_trips"]["status"] == "pass"
    assert short_leg["criteria"]["paper_regimes"]["observed"] == 2
    # A pooled mean would be (3 * 1 + 10 * 20) / 13 = 15.6; each leg sees only its own fills.
    assert short_leg["criteria"]["paper_shortfall"]["observed"]["mean_bps"] == "20.000000"
    assert short_leg["criteria"]["paper_shortfall"]["status"] == "fail"
    assert long_leg["unattributed_ledger_rows"] == 30  # hedge + TV + crossed origin
    assert short_leg["unattributed_ledger_rows"] == 30

    # The dossier's COMBINED block and overall verdict pass or fail nobody: LONG reads LONG,
    # SHORT reads SHORT.
    assert _statuses(record, "LONG")["dossier_passing"] == "pass"
    assert _statuses(record, "LONG")["dossier_dsr"] == "pass"
    assert _statuses(record, "SHORT")["dossier_passing"] == "fail"
    assert set(long_leg["reasons"]) == {"ROUND_TRIPS_BELOW_MIN", "REGIMES_BELOW_MIN"}
    assert set(short_leg["reasons"]) == {
        "DOSSIER_NOT_PASSING",
        "DSR_BELOW_MIN",
        "PBO_ABOVE_MAX",
        "SHORTFALL_ABOVE_MAX",
    }

    # The reverse split: only a passing COMBINED block and SHORT block. LONG stays without merit.
    plan = _base_plan()
    plan["dossier"] = _dossier(long_pass=False, short_pass=True)
    plan["dossier"]["verdict"]["overall_verdict"] = "PROMOTE"
    record = _compute(tmp_path, plan)
    assert record["legs"]["LONG"]["merit"] is False
    assert "DOSSIER_NOT_PASSING" in record["legs"]["LONG"]["reasons"]
    assert record["legs"]["SHORT"]["merit"] is True

    # The mapping a caller uses to pick the leg of an entry: the side alone never decides.
    assert merit.leg_of_entry("LONG", "TREND_ALIGNED_LONG") == "LONG"
    assert merit.leg_of_entry("LONG", "TREND_ALIGNED_LONG_REENTRY") == "LONG"
    assert merit.leg_of_entry("LONG", "DIRECT_LONG_REVERSAL_AUTO_REVERSE") == "LONG"
    assert merit.leg_of_entry("SHORT", "DIRECT_SHORT_ETB_AUTO_REVERSE") == "SHORT"
    no_leg: list[tuple[Any, Any]] = [
        ("LONG", "INVERSE_HEDGE_SOXS"),
        ("LONG", "INVERSE_HEDGE_SQQQ_REENTRY"),
        ("LONG", "DIRECT_SHORT_ETB"),
        ("SHORT", "TREND_ALIGNED_LONG"),
        ("LONG", "TV_HALF_AUTO_CONFIRMED"),
        ("LONG", "ADOPTED_EXTERNAL_POSITION"),
        ("LONG", "AUTO_ENTRY"),
        ("LONG", "TREND_ALIGNED_LONG_REENTRY_REENTRY"),
        ("BUY", "TREND_ALIGNED_LONG"),
        ("long", "TREND_ALIGNED_LONG"),
        ("LONG", None),
        (None, "TREND_ALIGNED_LONG"),
        (1, 1),
    ]
    for side, origin in no_leg:
        assert merit.leg_of_entry(side, origin) is None, (side, origin)


def test_non_numbers_never_satisfy_a_criterion(passing_dir: Path) -> None:
    """True is not a DSR of 1.0, False is not a PBO of 0 and 1 is not True: all are missing."""
    evidence = merit._read_evidence(passing_dir)

    def long_leg(key: str, edit: Any) -> dict[str, Any]:
        source = getattr(evidence, key)
        doc = copy.deepcopy(dict(source.doc))
        edit(doc)
        changed = dataclasses.replace(evidence, **{key: dataclasses.replace(source, doc=doc)})
        record = merit._evaluate(changed, SMALL, NOW, NO_HISTORY)
        _assert_record_invariants(record)
        return record["legs"]["LONG"]

    assert long_leg("dossier", lambda doc: None)["merit"] is True

    def set_dsr(doc: dict[str, Any], value: Any) -> None:
        doc["holdout_six_gate"]["LONG"]["falsification_2x_8bps"]["dsr"] = value

    def set_pbo(doc: dict[str, Any], value: Any) -> None:
        doc["holdout_six_gate"]["LONG"]["empirical_pbo"] = value

    def set_gate(doc: dict[str, Any], value: Any) -> None:
        doc["holdout_six_gate"]["LONG"]["gates"]["4_dsr_pbo_credible"] = value

    def set_promoted(doc: dict[str, Any], value: Any) -> None:
        doc["holdout_six_gate"]["LONG"]["promoted"] = value

    def set_drill(doc: dict[str, Any], value: Any) -> None:
        doc["drills"]["k4_broker_flatten_sandbox"]["passed"] = value

    def set_delta(doc: dict[str, Any], value: Any) -> None:
        doc["accounts"]["ALPACA_PA2"]["unreconciled_delta_count"] = value

    def set_digest(doc: dict[str, Any], value: Any) -> None:
        doc["accounts"]["ALPACA_PA2"]["account_sha256"] = value

    not_numbers: list[Any] = [
        True,
        False,
        None,
        "0.99",
        "0.2",
        float("nan"),
        float("inf"),
        float("-inf"),
        [0.99],
        {"value": 0.99},
        1.5,
        -0.2,
    ]
    not_booleans: list[Any] = [1, 0, 1.0, "true", "True", None, [True]]
    not_zero_counts: list[Any] = [True, False, None, "0", 0.0, [0]]
    not_digests: list[Any] = [True, None, 1, "", "abc", _account_sha256(2)[:63], [1]]
    cases: list[tuple[str, Any, list[Any], str, str]] = [
        ("dossier", set_dsr, not_numbers, "dossier_dsr", "DSR_EVIDENCE_MISSING"),
        ("dossier", set_pbo, not_numbers, "dossier_pbo", "PBO_EVIDENCE_MISSING"),
        ("dossier", set_gate, not_booleans, "dossier_passing", "DOSSIER_MISSING"),
        ("dossier", set_promoted, not_booleans, "dossier_passing", "DOSSIER_MISSING"),
        ("stage4", set_drill, not_booleans, "stage4_drills", "STAGE4_DRILLS_MISSING"),
        (
            "stage4",
            lambda doc, value: doc.update(all_passed=value),
            not_booleans,
            "stage4_drills",
            "STAGE4_DRILLS_MISSING",
        ),
        (
            "reconciliation",
            lambda doc, value: doc.update(all_reconciled=value),
            not_booleans,
            "reconciliation_receipt",
            "RECONCILIATION_RECEIPT_MISSING",
        ),
        (
            "reconciliation",
            set_delta,
            not_zero_counts,
            "reconciliation_receipt",
            "RECONCILIATION_RECEIPT_MISSING",
        ),
        (
            "reconciliation",
            set_digest,
            not_digests,
            "reconciliation_receipt",
            "RECONCILIATION_LEDGER_BINDING_MISSING",
        ),
    ]
    for key, setter, values, criterion, code in cases:
        for value in values:
            entry = long_leg(key, lambda doc, s=setter, v=value: s(doc, v))
            assert entry["merit"] is False, (criterion, value)
            assert entry["reasons"] == [code], (criterion, value)
            assert entry["criteria"][criterion]["status"] == "missing", (criterion, value)
    # A real number that fails is a failure, not missing evidence; and the bounds are exact.
    graded: list[tuple[str, Any, Any, str, list[str]]] = [
        ("dossier", set_dsr, 0.9499, "dossier_dsr", ["DSR_BELOW_MIN"]),
        ("dossier", set_dsr, 0.95, "dossier_dsr", []),
        ("dossier", set_dsr, 1, "dossier_dsr", []),
        ("dossier", set_pbo, 0.5, "dossier_pbo", ["PBO_ABOVE_MAX"]),  # the gate is pbo < 0.50
        ("dossier", set_pbo, 0.4999, "dossier_pbo", []),
        ("dossier", set_pbo, 0, "dossier_pbo", []),
        ("reconciliation", set_delta, 1, "reconciliation_receipt", ["RECONCILIATION_NOT_CLEAN"]),
        ("stage4", set_drill, False, "stage4_drills", ["STAGE4_DRILLS_NOT_PASSING"]),
        ("dossier", set_gate, False, "dossier_passing", ["DOSSIER_NOT_PASSING"]),
    ]
    for key, setter, value, criterion, reasons in graded:
        entry = long_leg(key, lambda doc, s=setter, v=value: s(doc, v))
        assert entry["reasons"] == reasons, (criterion, value)
        assert entry["criteria"][criterion]["status"] == ("fail" if reasons else "pass")


def test_torn_ledger_read_is_retried_and_stays_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence_dir = _write_plan(tmp_path, _base_plan())
    read_tables = merit._read_ledger_tables
    calls: list[int] = []

    def compute() -> dict[str, Any]:
        return compute_live_promotion_merit(
            evidence_dir, now_utc=NOW, thresholds=SMALL, prior_state=NO_HISTORY
        )

    def torn_once(raw: bytes) -> Any:
        calls.append(len(raw))
        return None if len(calls) == 1 else read_tables(raw)

    # One torn copy of the first ledger does not cost a leg its merit: it is read again.
    monkeypatch.setattr(merit, "_read_ledger_tables", torn_once)
    record = compute()
    assert record["legs"]["LONG"]["merit"] is True and record["legs"]["SHORT"]["merit"] is True
    assert len(calls) == len(PAPER_FILES) + 1 + 1  # three paper ledgers, the live one, one retry

    # A ledger that never opens is given up on after a fixed number of reads.
    calls.clear()
    monkeypatch.setattr(merit, "_read_ledger_tables", lambda raw: calls.append(len(raw)))
    record = compute()
    assert len(calls) == (len(PAPER_FILES) + 1) * merit._LEDGER_READ_ATTEMPTS
    assert record["legs"]["LONG"]["merit"] is False
    assert "PAPER_LEDGER_MISSING" in record["legs"]["LONG"]["reasons"]
    assert all(item["found"] for item in record["inputs"])


def test_short_leg_needs_live_confirmed_evidence(tmp_path: Path) -> None:
    # Passing paper evidence for both legs, but no live ledger at all.
    plan = _base_plan()
    plan["live"] = None
    record = _compute(tmp_path, plan)
    _assert_record_invariants(record)
    assert record["legs"]["LONG"]["merit"] is True
    assert record["legs"]["SHORT"]["merit"] is False
    assert record["legs"]["SHORT"]["reasons"] == [
        "LIVE_LEDGER_MISSING",
        "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE",
    ]
    assert _statuses(record, "SHORT")["short_live_confirmed"] == "missing"
    assert "short_live_confirmed" not in record["legs"]["LONG"]["criteria"]
    # Every paper criterion of the SHORT leg passes: paper alone is what is not enough.
    assert [k for k, v in _statuses(record, "SHORT").items() if v != "pass"] == [
        "short_live_confirmed"
    ]

    # Operator-confirmed paper shorts do not stand in for live ones.
    plan = _base_plan()
    plan["live"] = None
    for row in plan["paper"]["entries"]:
        if row["side"] == "SHORT":
            row["entry_authority"] = "OPERATOR_CONFIRMED"
    record = _compute(tmp_path, plan)
    assert "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE" in record["legs"]["SHORT"]["reasons"]

    # Nine good confirmed live shorts, plus rows that must not count, against a need of ten.
    need_ten = SMALL.model_copy(update={"short_min_live_confirmed_round_trips": 10})
    good_rows, good_fills = _round_trips(
        "SHORT", 9, authority="OPERATOR_CONFIRMED", hypothesis=None, prefix="live-good"
    )
    noise: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = [
        _round_trips(  # confirmed but outside the shortfall bound
            "SHORT",
            5,
            authority="OPERATOR_CONFIRMED",
            shortfall="8.001",
            hypothesis=None,
            prefix="live-costly",
        ),
        _round_trips("SHORT", 5, authority="AUTONOMOUS", prefix="live-auto"),
        _round_trips("SHORT", 5, authority="OWNER_LINEAGE", prefix="live-lineage"),
        _round_trips("LONG", 5, authority="OPERATOR_CONFIRMED", prefix="live-long"),
    ]
    plan = _base_plan()
    plan["live"]["entries"] = good_rows + [row for rows, _ in noise for row in rows]
    plan["live"]["fills"] = good_fills + [fill for _, fills in noise for fill in fills]
    record = _compute(tmp_path, plan, thresholds=need_ten)
    criterion = record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]
    assert criterion["status"] == "fail"
    assert criterion["observed"] == {"round_trips": 14, "inside_shortfall_bound": 9}
    assert criterion["threshold"] == {"inside_shortfall_bound": 10}
    assert record["legs"]["SHORT"]["reasons"] == ["SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE"]
    assert record["legs"]["LONG"]["merit"] is True

    # A confirmed short whose fills are not linked cannot be shown to be inside the bound.
    unlinked = copy.deepcopy(plan)
    extra_rows, _ = _round_trips(
        "SHORT", 1, authority="OPERATOR_CONFIRMED", hypothesis=None, prefix="live-unlinked"
    )
    unlinked["live"]["entries"] += extra_rows
    record = _compute(tmp_path, unlinked, thresholds=need_ten)
    assert record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]["observed"] == {
        "round_trips": 15,
        "inside_shortfall_bound": 9,
    }

    # The tenth good one promotes the evidence to a pass.
    tenth_rows, tenth_fills = _round_trips(
        "SHORT", 1, authority="OPERATOR_CONFIRMED", hypothesis=None, prefix="live-tenth"
    )
    plan["live"]["entries"] += tenth_rows
    plan["live"]["fills"] += tenth_fills
    record = _compute(tmp_path, plan, thresholds=need_ten)
    _assert_record_invariants(record, thresholds=need_ten)
    assert record["legs"]["SHORT"]["merit"] is True

    # A live ledger written before the tier columns exist is missing evidence, not zero trips.
    plan["live"]["tier_columns"] = False
    record = _compute(tmp_path, plan, thresholds=need_ten)
    criterion = record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]
    assert criterion["status"] == "missing"
    assert criterion["detail"] == "COLUMNS_ABSENT:close_cause,entry_authority"

    # A live ledger that names no broker account cannot be told from a paper one.
    plan["live"]["tier_columns"] = True
    plan["live"]["fuse"] = []
    record = _compute(tmp_path, plan, thresholds=need_ten)
    assert record["legs"]["SHORT"]["reasons"] == [
        "LIVE_LEDGER_IDENTITY_MISSING",
        "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE",
    ]
    assert record["legs"]["LONG"]["merit"] is True

    # 0 disables the extra requirement, with or without a live ledger.
    disabled = SMALL.model_copy(update={"short_min_live_confirmed_round_trips": 0})
    plan = _base_plan()
    plan["live"] = None
    record = _compute(tmp_path, plan, thresholds=disabled)
    _assert_record_invariants(record, thresholds=disabled)
    assert record["legs"]["SHORT"]["merit"] is True
    criterion = record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]
    assert (criterion["status"], criterion["detail"]) == ("pass", "NOT_REQUIRED")
    assert criterion["threshold"] == {"inside_shortfall_bound": 0}


# --- leg_has_merit: the record on its own ---------------------------------------------------------


@pytest.fixture(scope="module")
def passing_evidence(passing_dir: Path) -> Any:
    """The passing evidence read once; a cycle on it costs no file access."""
    return merit._read_evidence(passing_dir)


def _with_dossier(evidence: Any, **verdicts: bool) -> Any:
    """The same evidence with another dossier (same age), without touching any file."""
    doc = {**_dossier(**verdicts), "generated_at_utc": (NOW - timedelta(hours=1)).isoformat()}
    return dataclasses.replace(evidence, dossier=dataclasses.replace(evidence.dossier, doc=doc))


def test_leg_has_merit_is_strict_about_shape_expiry_and_types(
    passing_small_record: dict[str, Any], passing_evidence: Any
) -> None:
    good = json.loads(json.dumps(passing_small_record))  # as the caller would persist and reload

    def has(record: Any, leg: Any = "LONG", now: Any = NOW, thresholds: Any = SMALL) -> bool:
        return leg_has_merit(record, leg, now_utc=now, thresholds=thresholds)

    assert has(good) is True
    assert has(good, "SHORT") is True
    valid_until = datetime.fromisoformat(good["valid_until"])
    assert has(good, now=valid_until - timedelta(seconds=1)) is True

    # Expiry, clocks and the leg argument.
    assert has(good, now=valid_until) is False
    assert has(good, now=valid_until + timedelta(days=30)) is False
    assert has(good, now=NOW - timedelta(seconds=1)) is False
    assert has(good, now=NOW.replace(tzinfo=None)) is False
    bad_nows: list[Any] = [None, "2026-12-15T15:00:00+00:00", 1765810800, NOW.date()]
    for bad_now in bad_nows:
        assert has(good, now=bad_now) is False
    bad_legs: list[Any] = ["long", "BOTH", "COMBINED", "", None, 0, ["LONG"]]
    for bad_leg in bad_legs:
        assert has(good, bad_leg) is False

    # The thresholds are the caller's, not the record's: a record computed under any other set
    # (a test or debug run with loose ones, or the owner defaults when the caller runs SMALL)
    # is not this caller's Key A.
    assert leg_has_merit(good, "LONG", now_utc=NOW) is False  # the default is the owner defaults
    assert has(good, thresholds=LivePromotionThresholds()) is False
    assert has(good, thresholds=SMALL.model_copy(update={"min_round_trips": 7})) is False
    assert has(good, thresholds=SMALL.model_copy(update={"min_round_trips": 5})) is False
    assert (
        has(good, thresholds=SMALL.model_copy(update={"max_shortfall_bps": Decimal("9")})) is False
    )
    assert has(good, thresholds=SMALL.model_copy()) is True
    for not_thresholds in ({}, "SMALL", SMALL.model_dump(), SMALL.model_dump(mode="json"), 6):
        assert has(good, thresholds=not_thresholds) is False

    def tampered(edit: Any) -> list[dict[str, Any]]:
        """The edited record as it is, and with its body hash recomputed to match the edit."""
        record = copy.deepcopy(good)
        edit(record)
        return [record, _reseal(copy.deepcopy(record))]

    def set_path(record: dict[str, Any], path: tuple[Any, ...], value: Any) -> None:
        target: Any = record
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    def drop_path(record: dict[str, Any], path: tuple[Any, ...]) -> None:
        target: Any = record
        for key in path[:-1]:
            target = target[key]
        del target[path[-1]]

    long_leg = ("legs", "LONG")
    criteria = (*long_leg, "criteria")
    computed_at = datetime.fromisoformat(good["computed_at"])
    max_age = timedelta(hours=SMALL.evidence_max_age_hours)
    released_latch = {
        "latched": True,
        "released": True,
        "manifests_newer_than_demotion": True,
        "cooloff_sessions_completed": SMALL.demotion_cooloff_sessions,
    }
    replaced: list[tuple[tuple[Any, ...], Any]] = [
        # Truthy values that are not the builtin True.
        ((*long_leg, "merit"), "true"),
        ((*long_leg, "merit"), "True"),
        ((*long_leg, "merit"), 1),
        ((*long_leg, "merit"), 1.0),
        ((*long_leg, "merit"), [True]),
        ((*long_leg, "merit"), {"merit": True}),
        ((*long_leg, "merit"), False),
        ((*long_leg, "merit"), None),
        # A leg entry that claims merit but does not carry the proof.
        ((*long_leg, "reasons"), ["DOSSIER_NOT_PASSING"]),
        ((*long_leg, "reasons"), None),
        ((*long_leg, "reasons"), ()),
        (criteria, {}),
        (criteria, []),
        ((*criteria, "dossier_passing", "status"), "fail"),
        ((*criteria, "dossier_passing", "status"), "missing"),
        ((*criteria, "dossier_passing", "status"), "PASS"),
        ((*criteria, "dossier_passing", "status"), True),
        ((*criteria, "paper_shortfall", "reasons"), ["SHORTFALL_ABOVE_MAX"]),
        ((*criteria, "demotion_latch"), "pass"),
        ((*criteria, "an_extra_criterion"), {"status": "pass", "reasons": []}),
        ((*long_leg, "capital_step"), None),
        ((*long_leg, "capital_step", "index"), 99),
        ((*long_leg, "capital_step", "index"), True),
        (long_leg, None),
        (long_leg, "PROMOTE"),
        (("legs",), [good["legs"]["LONG"]]),
        # A passing status over an observed value that does not satisfy the threshold.
        ((*criteria, "paper_round_trips", "observed"), 5),
        ((*criteria, "paper_round_trips", "observed"), None),
        ((*criteria, "paper_round_trips", "observed"), True),
        ((*criteria, "paper_sessions", "observed"), 2),
        ((*criteria, "paper_regimes", "observed"), 1),
        ((*criteria, "paper_supervised_exits", "observed"), 1),
        ((*criteria, "paper_shortfall", "observed", "mean_bps"), "8.000001"),
        ((*criteria, "paper_shortfall", "observed", "trailing_mean_bps"), "8.000001"),
        ((*criteria, "paper_shortfall", "observed", "trailing_mean_bps"), None),
        ((*criteria, "paper_shortfall", "observed"), "3.000000"),
        ((*criteria, "dossier_dsr", "observed"), "0.9499"),
        ((*criteria, "dossier_dsr", "observed"), "not a number"),
        ((*criteria, "dossier_pbo", "observed"), "0.5"),
        ((*criteria, "dossier_passing", "observed", "gates_passed"), 5),
        ((*criteria, "dossier_passing", "observed", "verdict"), "DISCARD"),
        ((*criteria, "dossier_passing", "observed", "promoted"), 1),
        ((*criteria, "dossier_leg_binding", "observed", "bound_rows"), 0),
        ((*criteria, "breaker_trips", "observed", "defect_trips"), 1),
        ((*criteria, "breaker_trips", "observed", "sessions_without_fuse_row"), 1),
        ((*criteria, "reconciliation_receipt", "observed", "bound_accounts"), 2),
        ((*criteria, "reconciliation_receipt", "observed", "all_reconciled"), 1),
        ((*criteria, "stage4_drills", "observed", "flags_true"), 5),
        ((*criteria, "demotion_latch", "observed"), {**released_latch, "released": False}),
        (
            (*criteria, "demotion_latch", "observed"),
            {**released_latch, "manifests_newer_than_demotion": False},
        ),
        (
            (*criteria, "demotion_latch", "observed"),
            {**released_latch, "cooloff_sessions_completed": SMALL.demotion_cooloff_sessions - 1},
        ),
        ((*criteria, "demotion_latch", "observed"), {"latched": None}),
        # A threshold or comparator that is not the configured one.
        ((*criteria, "paper_round_trips", "threshold"), 1),
        ((*criteria, "paper_round_trips", "comparator"), "<="),
        ((*criteria, "paper_shortfall", "threshold"), {"mean_bps": "8"}),
        ((*criteria, "dossier_passing", "threshold", "promoted"), 1),
        ((*criteria, "dossier_pbo", "comparator"), "<="),
        # The envelope.
        (("schema",), "tier0.live-promotion-merit.v2"),
        (("schema",), None),
        (("generator",), {"module": "somewhere.else", "version": "1.1.0"}),
        (("generator", "version"), "1.0.0"),
        (("computed_at",), "not a timestamp"),
        (("computed_at",), "2026-12-15T15:00:00"),  # naive
        (("computed_at",), (NOW + timedelta(hours=1)).isoformat()),  # computed in the future
        (("valid_until",), NOW.isoformat()),  # already expired
        (("valid_until",), None),
        (("valid_until",), 4102444800),
        # A record never outlives the evidence age bound, whatever it says.
        (("valid_until",), (computed_at + max_age + timedelta(seconds=1)).isoformat()),
        (("valid_until",), (computed_at + timedelta(days=500)).isoformat()),
        (("thresholds",), None),
        (("thresholds", "min_round_trips"), True),
        (("thresholds", "min_round_trips"), "6"),
        (("thresholds", "min_round_trips"), 5),
        (("thresholds", "dsr_min"), "not a number"),
        (("thresholds", "dsr_min"), True),
        (("thresholds", "pbo_max"), "0.5"),
        (("thresholds", "an_unknown_threshold"), 1),
        (("prior_state_sha256",), None),
        (("prior_state_sha256",), "abc"),
        # inputs and their hash must agree, and the inputs are exactly the expected files.
        (("inputs_hash",), "0" * 64),
        (("inputs_hash",), None),
        (("inputs", 0, "sha256"), "f" * 64),
        (("inputs", 0, "sha256"), None),
        (("inputs", 0, "found"), False),
        (("inputs", 0, "found"), "yes"),
        (("inputs", 0, "name"), "another_dossier.json"),
        (("inputs", 1, "name"), good["inputs"][0]["name"]),  # a duplicated input
        (("inputs",), []),
        (("inputs",), None),
    ]
    for path, value in replaced:
        for forged in tampered(lambda record, p=path, v=value: set_path(record, p, v)):
            assert has(forged) is False, (path, value)
    for not_the_hash in ("0" * 64, "abc", None, 1):
        forged = copy.deepcopy(good)
        forged["record_sha256"] = not_the_hash
        assert has(forged) is False

    dropped: list[tuple[Any, ...]] = [
        ("schema",),
        ("computed_at",),
        ("valid_until",),
        ("generator",),
        ("thresholds",),
        ("inputs",),
        ("inputs_hash",),
        ("prior_state_sha256",),
        ("legs",),
        long_leg,
        (*long_leg, "merit"),
        (*long_leg, "reasons"),
        (*long_leg, "criteria"),
        (*long_leg, "capital_step"),
        (*criteria, "stage4_drills"),
        (*criteria, "demotion_latch"),
        (*criteria, "paper_round_trips", "status"),
        (*criteria, "paper_round_trips", "reasons"),
        (*criteria, "paper_round_trips", "observed"),
        (*criteria, "paper_round_trips", "threshold"),
        (*criteria, "paper_round_trips", "comparator"),
        (*criteria, "paper_shortfall", "observed", "trailing_mean_bps"),
        ("inputs", 0),
        ("inputs", 0, "sha256"),
    ]
    for path in dropped:
        for forged in tampered(lambda record, p=path: drop_path(record, p)):
            assert has(forged) is False, path
    # The body hash alone is not in the re-sealed list above by construction.
    unsealed = copy.deepcopy(good)
    del unsealed["record_sha256"]
    assert has(unsealed) is False

    # An input removed or added, with the inputs hash fixed up to match: still not the inputs.
    for edit in (lambda items: items.pop(), lambda items: items.append({**items[0], "name": "x"})):
        forged = copy.deepcopy(good)
        edit(forged["inputs"])
        forged["inputs_hash"] = merit._inputs_hash(forged["inputs"])
        assert has(_reseal(forged)) is False

    # Positive controls: an edit that keeps every check true is accepted once it is re-sealed,
    # so each rejection above came from the value and not from the edit being an edit.
    at_bound, sealed_at_bound = tampered(
        lambda record: record.update(valid_until=(computed_at + max_age).isoformat())
    )
    assert has(at_bound) is False and has(sealed_at_bound) is True
    assert has(sealed_at_bound, now=computed_at + max_age - timedelta(seconds=1)) is True
    _, more_round_trips = tampered(
        lambda record: set_path(record, (*criteria, "paper_round_trips", "observed"), 9)
    )
    assert has(more_round_trips) is True
    _, latch_released = tampered(
        lambda record: set_path(record, (*criteria, "demotion_latch", "observed"), released_latch)
    )
    assert has(latch_released) is True

    # A SHORT entry needs its live criterion; the LONG criteria set is not enough for it.
    _, swapped = tampered(lambda record: record["legs"].update(SHORT=record["legs"]["LONG"]))
    assert has(swapped, "SHORT") is False
    assert has(swapped) is True
    _, too_few = tampered(
        lambda record: set_path(
            record,
            ("legs", "SHORT", "criteria", "short_live_confirmed", "observed"),
            {"round_trips": 2, "inside_shortfall_bound": 1},
        )
    )
    assert has(too_few, "SHORT") is False and has(too_few) is True

    # The review's case: a failing record whose verdicts and expiry alone were edited.
    failing = merit._evaluate(
        _with_dossier(passing_evidence, long_pass=False, short_pass=False), SMALL, NOW, NO_HISTORY
    )
    assert failing["legs"]["LONG"]["reasons"][0] == "DOSSIER_NOT_PASSING"
    forged = json.loads(json.dumps(failing))
    for leg in merit.LEGS:
        entry = forged["legs"][leg]
        entry["merit"], entry["reasons"] = True, []
        for criterion in entry["criteria"].values():
            criterion["status"], criterion["reasons"] = "pass", []
    forged["valid_until"] = (NOW + timedelta(days=500)).isoformat()
    for candidate in (forged, _reseal(copy.deepcopy(forged))):
        for leg in merit.LEGS:
            assert has(candidate, leg) is False
            assert has(candidate, leg, now=NOW + timedelta(days=400)) is False
        state = next_promotion_state(
            NO_HISTORY, candidate, live_evidence=QUIET, now_utc=NOW, thresholds=SMALL
        )
        assert all(state["legs"][leg]["status"] == "PAPER" for leg in merit.LEGS)

    # Things that are not a record at all never raise.
    for junk in (None, [], "record", 5, True, {"legs": {"LONG": {"merit": True}}}, {}, b"{}"):
        assert has(junk) is False
    # The untampered record still passes: every rejection above came from the tampering.
    assert has(good) is True


# --- The gate value: state, record and live evidence together -------------------------------------


def test_gate_value_binds_state_record_and_live_evidence(passing_evidence: Any) -> None:
    gate = merit.live_entry_leg_merit
    evidence = passing_evidence
    record0, promoted = _cycle(evidence, NO_HISTORY, QUIET, NOW)
    for leg in merit.LEGS:
        assert promoted["legs"][leg]["live_entry_eligible"] is True
        assert gate(promoted, record0, leg, now_utc=NOW, thresholds=SMALL) is True
    assert promoted["record_sha256"] == record0["record_sha256"]
    assert record0["prior_state_sha256"] == merit._canonical_sha256(NO_HISTORY)
    # The gate is configured too: under the owner defaults a SMALL record is nobody's Key A.
    assert gate(promoted, record0, "LONG", now_utc=NOW) is False
    # A reloaded pair behaves the same.
    reloaded = json.loads(json.dumps([promoted, record0]))
    assert gate(reloaded[0], reloaded[1], "LONG", now_utc=NOW, thresholds=SMALL) is True

    # 16:00: LONG is demoted for a protection fault. The 15:00 record still "has merit" on its
    # own until it expires; the gate value is False at once, whichever record it is asked with.
    t1 = NOW + timedelta(hours=1)
    record1, demoted = _cycle(evidence, promoted, _live([], faults=1), t1)
    assert demoted["legs"]["LONG"]["status"] == "DEMOTED"
    assert demoted["legs"]["LONG"]["demotion_reasons"] == ["LIVE_PROTECTION_FAULT"]
    soon = t1 + timedelta(minutes=1)
    assert leg_has_merit(record0, "LONG", now_utc=soon, thresholds=SMALL) is True
    assert leg_has_merit(record1, "LONG", now_utc=soon, thresholds=SMALL) is True
    for state, record in ((demoted, record0), (demoted, record1), (promoted, record1)):
        assert gate(state, record, "LONG", now_utc=soon, thresholds=SMALL) is False
    assert gate(demoted, record1, "SHORT", now_utc=soon, thresholds=SMALL) is True
    # A record computed for the demoted state says so itself.
    record2, still = _cycle(evidence, demoted, QUIET, soon)
    assert "DEMOTION_LATCHED" in record2["legs"]["LONG"]["reasons"]
    assert leg_has_merit(record2, "LONG", now_utc=soon, thresholds=SMALL) is False
    assert still["legs"]["LONG"]["status"] == "DEMOTED"

    # A lost state is unknown history: it demotes, whatever the record says.
    lost = next_promotion_state(None, record1, live_evidence=QUIET, now_utc=soon, thresholds=SMALL)
    stateless = merit._evaluate(evidence, SMALL, soon, None)
    for leg in merit.LEGS:
        entry = lost["legs"][leg]
        assert (entry["status"], entry["transition"]) == ("DEMOTED", "DEMOTED")
        assert entry["demotion_reasons"] == ["PRIOR_STATE_MISSING"]
        assert entry["latched"] is True and entry["live_entry_eligible"] is False
        assert entry["demoted_at"] == soon.isoformat()
        assert stateless["legs"][leg]["reasons"] == ["PROMOTION_STATE_MISSING"]
        assert gate(None, record1, leg, now_utc=soon, thresholds=SMALL) is False
        assert gate(lost, record1, leg, now_utc=soon, thresholds=SMALL) is False
        assert gate(lost, stateless, leg, now_utc=soon, thresholds=SMALL) is False
    again = next_promotion_state(
        None, stateless, live_evidence=QUIET, now_utc=soon, thresholds=SMALL
    )
    assert again["legs"]["LONG"]["demotion_reasons"] == ["PRIOR_STATE_MISSING"]

    # A record computed with one state is not accepted together with another.
    crossed = next_promotion_state(
        promoted, record0, live_evidence=QUIET, now_utc=NOW, thresholds=SMALL
    )
    assert crossed["legs"]["LONG"]["status"] == "DEMOTED"
    assert crossed["legs"]["LONG"]["demotion_reasons"] == [
        "MERIT_LOST",
        "RECORD_NOT_COMPUTED_WITH_STATE",
    ]
    unbound = next_promotion_state(
        NO_HISTORY, record1, live_evidence=QUIET, now_utc=t1, thresholds=SMALL
    )
    assert unbound["legs"]["LONG"]["status"] == "PAPER"
    assert unbound["legs"]["LONG"]["hold_reasons"] == [
        "NO_CURRENT_MERIT",
        "RECORD_NOT_COMPUTED_WITH_STATE",
    ]

    # No live evidence this cycle: a promoted leg keeps its promotion and its step, is not
    # eligible, and is eligible again once evidence is passed. Nothing is demoted on a gap.
    record3, blind = _cycle(evidence, promoted, None, t1)
    omitted = next_promotion_state(promoted, record3, now_utc=t1, thresholds=SMALL)
    assert omitted == blind  # the default is the missing case
    for leg in merit.LEGS:
        entry = blind["legs"][leg]
        assert (entry["status"], entry["transition"]) == ("PROMOTED", "NONE")
        assert entry["hold_reasons"] == ["LIVE_EVIDENCE_MISSING"]
        assert entry["live_entry_eligible"] is False and entry["latched"] is False
        assert entry["promoted_at"] == NOW.isoformat()
        assert gate(blind, record3, leg, now_utc=t1, thresholds=SMALL) is False
    record4, seen = _cycle(evidence, blind, QUIET, soon)
    assert seen["legs"]["LONG"]["live_entry_eligible"] is True
    assert gate(seen, record4, "LONG", now_utc=soon, thresholds=SMALL) is True

    # The pair has to be the result of one cycle, decided no earlier than the record and not in
    # the future; an edited state, or one that says "eligible" for a demoted leg, is refused.
    def edited(edit: Any) -> dict[str, Any]:
        state = copy.deepcopy(promoted)
        edit(state)
        return state

    refused: list[Any] = [
        edited(lambda s: s.update(updated_at=(NOW - timedelta(seconds=1)).isoformat())),
        edited(lambda s: s.update(updated_at=(NOW + timedelta(hours=1)).isoformat())),
        edited(lambda s: s.update(updated_at=None)),
        edited(lambda s: s.update(record_sha256="0" * 64)),
        edited(lambda s: s.update(record_sha256=None)),
        edited(lambda s: s.update(record_inputs_hash="0" * 64)),
        edited(lambda s: s.update(schema="tier0.live-promotion-state.v2")),
        edited(lambda s: s["legs"]["LONG"].update(live_entry_eligible=1)),
        edited(lambda s: s["legs"]["LONG"].update(live_entry_eligible="true")),
        edited(lambda s: s["legs"]["LONG"].update(status="DEMOTED")),
        edited(lambda s: s["legs"]["LONG"].update(status="PAPER")),
        edited(lambda s: s["legs"]["LONG"].update(latched=True)),
        edited(lambda s: s["legs"]["LONG"].update(hold_reasons=["LIVE_EVIDENCE_MISSING"])),
        edited(lambda s: s["legs"]["LONG"].update(promoted_at="yesterday")),
        edited(lambda s: s["legs"].pop("LONG")),
        {**demoted, "legs": {**demoted["legs"], "LONG": {**demoted["legs"]["LONG"]}}},
        None,
        [],
        "state",
        {},
    ]
    for state in refused:
        assert gate(state, record0, "LONG", now_utc=NOW, thresholds=SMALL) is False, state
    for junk in (None, [], "record", {}, {"schema": "junk"}):
        assert gate(promoted, junk, "LONG", now_utc=NOW, thresholds=SMALL) is False
    for bad_leg in ("long", "COMBINED", None, 0):
        assert gate(promoted, record0, bad_leg, now_utc=NOW, thresholds=SMALL) is False  # ty: ignore[invalid-argument-type]
    assert (
        gate(promoted, record0, "LONG", now_utc=NOW.replace(tzinfo=None), thresholds=SMALL) is False
    )
    assert gate(promoted, record0, "LONG", now_utc=NOW, thresholds={}) is False  # ty: ignore[invalid-argument-type]
    assert gate(promoted, record0, "LONG", now_utc=NOW, thresholds=SMALL) is True

    # The explicit starting state: both legs in paper, held, and well formed.
    start = merit.initial_promotion_state(now_utc=NOW)
    assert start["schema"] == merit.LIVE_PROMOTION_STATE_SCHEMA
    assert json.loads(json.dumps(start)) == start
    for leg in merit.LEGS:
        entry = start["legs"][leg]
        assert (entry["status"], entry["latched"], entry["live_entry_eligible"]) == (
            "PAPER",
            False,
            False,
        )
        assert entry["capital_step"] == CAPITAL_LADDER[0].model_dump(mode="json")
        assert entry["demoted_at"] is None and entry["promoted_at"] is None
        assert gate(start, record0, leg, now_utc=NOW, thresholds=SMALL) is False
    with pytest.raises(ValueError, match="timezone-aware"):
        merit.initial_promotion_state(now_utc=NOW.replace(tzinfo=None))


def _two_of_five(today: Any) -> list[dict[str, Any]]:
    """Five fuse sessions ending today, tripped on two of them."""
    return [
        {"session": (today - timedelta(days=back)).isoformat(), "tripped": back in (1, 3)}
        for back in range(4, -1, -1)
    ]


def test_promotion_is_held_by_missing_or_adverse_live_evidence(passing_evidence: Any) -> None:
    """A promotion is decided on the live evidence of the same call, never before it is read."""
    gate = merit.live_entry_leg_merit
    record = merit._evaluate(passing_evidence, SMALL, NOW, NO_HISTORY)
    assert record["legs"]["LONG"]["merit"] is True and record["legs"]["SHORT"]["merit"] is True
    window = merit.LIVE_SHORTFALL_WINDOW_ROUND_TRIPS
    earlier = NOW - timedelta(hours=2)
    confirmed_costly = _trips(window, start=earlier, shortfall="9", authority="OPERATOR_CONFIRMED")
    adverse: list[tuple[Any, str, tuple[str, ...]]] = [
        (None, "LIVE_EVIDENCE_MISSING", _BOTH),
        ("not evidence", "LIVE_EVIDENCE_MALFORMED", _BOTH),
        ({"legs": {}}, "LIVE_EVIDENCE_MALFORMED", _BOTH),
        ({"legs": QUIET["legs"]}, "LIVE_EVIDENCE_MALFORMED", _BOTH),
        ({**QUIET, "legs": {"LONG": QUIET["legs"]["LONG"]}}, "LIVE_EVIDENCE_MALFORMED", _BOTH),
        (_live([], faults=1), "LIVE_PROTECTION_FAULT", ("LONG",)),
        (_live([], faults=-1), "LIVE_EVIDENCE_MALFORMED", _BOTH),
        (_live([], faults=True), "LIVE_EVIDENCE_MALFORMED", _BOTH),
        (
            _live([], fuse=_two_of_five(merit._trade_date(NOW))),
            "LIVE_FUSE_TRIPPED_TWO_OF_FIVE_SESSIONS",
            _BOTH,
        ),
        (_live(confirmed_costly), "LIVE_SHORTFALL_ABOVE_MAX", ("LONG",)),
        (
            _live(_trips(1, start=NOW + timedelta(minutes=1), fault=True)),
            "LIVE_PROTECTION_FAULT",
            ("LONG",),
        ),
    ]
    for live, code, held in adverse:
        state = next_promotion_state(
            NO_HISTORY, record, live_evidence=live, now_utc=NOW, thresholds=SMALL
        )
        for leg in merit.LEGS:
            entry = state["legs"][leg]
            if leg not in held:
                assert entry["status"] == "PROMOTED", (code, leg)
                continue
            assert (entry["status"], entry["transition"]) == ("PAPER", "NONE"), code
            assert entry["hold_reasons"] == [code]
            assert entry["live_entry_eligible"] is False and entry["latched"] is False
            assert gate(state, record, leg, now_utc=NOW, thresholds=SMALL) is False
    # Omitting the argument is the missing case.
    omitted = next_promotion_state(NO_HISTORY, record, now_utc=NOW, thresholds=SMALL)
    assert omitted["legs"]["LONG"]["hold_reasons"] == ["LIVE_EVIDENCE_MISSING"]
    # Controls: evidence that shows nothing, or a full window inside the bound, promotes.
    inside = _trips(window, start=earlier, shortfall="8.0", authority="OPERATOR_CONFIRMED")
    for live in (QUIET, _live(confirmed_costly[: window - 1]), _live(inside)):
        state = next_promotion_state(
            NO_HISTORY, record, live_evidence=live, now_utc=NOW, thresholds=SMALL
        )
        assert state["legs"]["LONG"]["status"] == "PROMOTED"
        assert state["legs"]["LONG"]["hold_reasons"] == []


def test_live_round_trips_are_identified_dated_and_attributed(passing_evidence: Any) -> None:
    """One round trip is one entry order id; a duplicate, a future close and a step the leg has
    not reached are malformed evidence, and tier-2 round trips never move the tier-3 ladder."""
    evidence = passing_evidence
    _, promoted = _cycle(evidence, NO_HISTORY, QUIET, NOW)
    t1 = NOW + timedelta(minutes=30)
    start = NOW + timedelta(minutes=1)
    need_1 = CAPITAL_LADDER[1].min_clean_live_round_trips_at_previous

    def after(live: Any, state: dict[str, Any] = promoted) -> dict[str, Any]:
        return _cycle(evidence, state, live, t1)[1]

    half = _trips(need_1 // 2, start=start)
    held = after(_live(half))["legs"]["LONG"]
    assert (held["status"], held["transition"]) == ("PROMOTED", "NONE")
    assert held["clean_round_trips_at_step"] == need_1 // 2
    full = after(_live(_trips(need_1, start=start)))["legs"]["LONG"]
    assert full["transition"] == "CAPITAL_STEP_ADVANCED"  # the control: distinct round trips

    def malformed(live: Any) -> None:
        legs = _BOTH
        state = after(live)
        for leg in legs:
            entry = state["legs"][leg]
            assert (entry["status"], entry["transition"]) == ("DEMOTED", "DEMOTED"), live
            assert entry["demotion_reasons"] == ["LIVE_EVIDENCE_MALFORMED"]
            assert entry["capital_step"] == CAPITAL_LADDER[0].model_dump(mode="json")
        # A leg that is not promoted is held by the same evidence instead of being promoted.
        fresh = next_promotion_state(
            NO_HISTORY,
            merit._evaluate(evidence, SMALL, t1, NO_HISTORY),
            live_evidence=live,
            now_utc=t1,
            thresholds=SMALL,
        )
        for leg in legs:
            assert fresh["legs"][leg]["status"] == "PAPER"
            assert fresh["legs"][leg]["hold_reasons"] == ["LIVE_EVIDENCE_MALFORMED"]

    # The same ten round trips listed twice (a per-fill join) are not twenty.
    malformed(_live(half + half))
    malformed(_live(half + [{**half[0], "shortfall_bps": Decimal("1.0")}]))
    # One order id cannot be a round trip of both legs.
    malformed(_live(half, short_trips=half[:1]))
    # A round trip that closes in the future has not happened; a small clock skew is tolerated.
    malformed(_live(_trips(need_1, start=t1 + timedelta(days=900))))
    malformed(_live(_trips(1, start=t1 + timedelta(minutes=6))))
    skewed = after(_live(_trips(1, start=t1 + timedelta(minutes=4))))["legs"]["LONG"]
    assert skewed["status"] == "PROMOTED"
    one = half[0]
    broken_items: list[dict[str, Any]] = [
        {key: value for key, value in one.items() if key != "entry_client_order_id"},
        {**one, "entry_client_order_id": ""},
        {**one, "entry_client_order_id": 7},
        {key: value for key, value in one.items() if key != "opened_at"},
        {**one, "opened_at": "2026-12-15T15:01:00"},  # naive
        {**one, "opened_at": (t1 - timedelta(minutes=1)).isoformat()},  # opened after it closed
        {**one, "closed_at": None},
        {**one, "entry_authority": "OWNER_LINEAGE"},
        {**one, "entry_authority": None},
        {**one, "capital_step": None},  # a tier-3 round trip was sized by a step
        {**one, "capital_step": True},
        {**one, "capital_step": len(CAPITAL_LADDER)},
        {**one, "capital_step": -1},
        {**one, "entry_authority": "OPERATOR_CONFIRMED"},  # tier 2 is not on the ladder
        {**one, "shortfall_bps": "2.0"},
        {**one, "shortfall_bps": True},
        {**one, "shortfall_bps": float("nan")},
        {**one, "protection_fault": 0},
        {**one, "close_cause": None},
    ]
    for item in broken_items:
        malformed(_live([item]))
    malformed(_live([], fuse=[{"session": "2026-13-45", "tripped": False}]))
    malformed(_live([], fuse=[{"session": 20261215, "tripped": False}]))
    malformed(_live([], fuse=[{"session": "2026-12-15", "tripped": 1}]))
    twice = [{"session": "2026-12-14", "tripped": False}] * 2
    malformed(_live([], fuse=twice))
    tomorrow = (merit._trade_date(t1) + timedelta(days=2)).isoformat()
    malformed(_live([], fuse=[{"session": tomorrow, "tripped": False}]))
    # A tier-3 round trip opened at the current step cannot have been sized by a later one.
    over_step = after(_live(_trips(1, start=start, step=1)))
    assert over_step["legs"]["LONG"]["demotion_reasons"] == ["LIVE_EVIDENCE_MALFORMED"]
    assert over_step["legs"]["SHORT"]["status"] == "PROMOTED"
    earlier_step = _trips(1, start=NOW - timedelta(hours=1), step=2)  # opened before this step
    assert after(_live(earlier_step))["legs"]["LONG"]["status"] == "PROMOTED"

    # Operator-confirmed (tier-2) round trips never advance the tier-3 ladder.
    confirmed = _trips(need_1 * 3, start=start, authority="OPERATOR_CONFIRMED")
    tier2 = after(_live(confirmed))["legs"]["LONG"]
    assert (tier2["status"], tier2["transition"]) == ("PROMOTED", "NONE")
    assert tier2["capital_step"]["index"] == 0 and tier2["clean_round_trips_at_step"] == 0
    mixed = after(_live(confirmed + _trips(need_1 - 1, start=start)))["legs"]["LONG"]
    assert mixed["capital_step"]["index"] == 0
    assert mixed["clean_round_trips_at_step"] == need_1 - 1
    # A fault on a tier-2 round trip is not a protection fault on a tier-3 position.
    tier2_fault = _trips(1, start=start, authority="OPERATOR_CONFIRMED", fault=True)
    assert after(_live(tier2_fault))["legs"]["LONG"]["status"] == "PROMOTED"


def _plan_with_history(moment: datetime) -> dict[str, Any]:
    """Passing rows decided in the four sessions before ``moment``, plus the base plan's older
    rows (decided before ``NOW``) under other order ids."""
    plan, old = _base_plan(before=moment), _base_plan()
    plan["paper"]["entries"] += [
        {**row, "cid": "old-" + row["cid"]} for row in old["paper"]["entries"]
    ]
    plan["paper"]["fills"] += [
        {
            **fill,
            "execution_id": "old-" + fill["execution_id"],
            "entry_client_order_id": "old-" + fill["entry_client_order_id"],
        }
        for fill in old["paper"]["fills"]
    ]
    known = {day for day, _, _ in plan["paper"]["fuse"]}
    plan["paper"]["fuse"] += [row for row in old["paper"]["fuse"] if row[0] not in known]
    return plan


def test_promotion_demotion_latch_and_capital_ladder(tmp_path: Path, passing_evidence: Any) -> None:
    evidence = passing_evidence
    failing = _with_dossier(evidence, long_pass=False, short_pass=False)
    long_failing = _with_dossier(evidence, long_pass=False, short_pass=True)

    def after(state: Any, live: Any, now: datetime, source: Any = evidence) -> dict[str, Any]:
        return _cycle(source, state, live, now)[1]

    # No merit: the leg keeps trading in paper and live entry is a quiet HOLD with the reasons.
    idle = after(NO_HISTORY, QUIET, NOW, failing)
    for leg in merit.LEGS:
        assert idle["legs"][leg]["status"] == "PAPER"
        assert idle["legs"][leg]["transition"] == "NONE"
        assert idle["legs"][leg]["live_entry_eligible"] is False
        assert idle["legs"][leg]["latched"] is False
        assert idle["legs"][leg]["hold_reasons"][:2] == ["NO_CURRENT_MERIT", "DOSSIER_NOT_PASSING"]

    # Merit promotes a leg onto the first capital step: one share.
    passing, promoted = _cycle(evidence, NO_HISTORY, QUIET, NOW)
    before = copy.deepcopy(passing)
    assert promoted == next_promotion_state(
        NO_HISTORY, passing, live_evidence=QUIET, now_utc=NOW, thresholds=SMALL
    )
    assert passing == before  # pure: inputs are not touched
    assert json.loads(json.dumps(promoted)) == promoted
    assert promoted["schema"] == merit.LIVE_PROMOTION_STATE_SCHEMA
    assert promoted["record_inputs_hash"] == passing["inputs_hash"]
    assert promoted["record_sha256"] == passing["record_sha256"]
    for leg in merit.LEGS:
        entry = promoted["legs"][leg]
        assert (entry["status"], entry["transition"]) == ("PROMOTED", "PROMOTED")
        assert entry["live_entry_eligible"] is True and entry["latched"] is False
        assert entry["promoted_at"] == NOW.isoformat()
        assert entry["capital_step"] == CAPITAL_LADDER[0].model_dump(mode="json")
        assert entry["capital_step"]["name"] == "MICRO_LIVE"
        assert entry["capital_step"]["max_qty"] == 1
    assert after(idle, QUIET, NOW)["legs"]["LONG"]["status"] == "PROMOTED"
    # The very next call on identical inputs keeps both legs promoted and eligible.
    settled = after(promoted, QUIET, NOW)
    for leg in merit.LEGS:
        entry = settled["legs"][leg]
        assert (entry["status"], entry["transition"]) == ("PROMOTED", "NONE")
        assert entry["live_entry_eligible"] is True and entry["promoted_at"] == NOW.isoformat()

    # The ladder is data: ADR-0020's three steps, a one-share first step, stated requirements.
    assert [step.name for step in CAPITAL_LADDER] == [
        "MICRO_LIVE",
        "FRACTIONAL_SCALE",
        "TARGET_ALLOCATION",
    ]
    assert [step.index for step in CAPITAL_LADDER] == [0, 1, 2]
    assert CAPITAL_LADDER[0].max_qty == 1
    assert CAPITAL_LADDER[0].min_clean_live_round_trips_at_previous == 0
    assert all(step.min_clean_live_round_trips_at_previous > 0 for step in CAPITAL_LADDER[1:])
    assert CAPITAL_LADDER[-1].risk_qty_fraction == Decimal("1")
    need_1 = CAPITAL_LADDER[1].min_clean_live_round_trips_at_previous
    need_2 = CAPITAL_LADDER[2].min_clean_live_round_trips_at_previous

    t1 = NOW + timedelta(minutes=30)
    after_promotion = NOW + timedelta(minutes=1)
    # One clean round trip short of the requirement: no advance.
    held = after(promoted, _live(_trips(need_1 - 1, start=after_promotion)), t1)
    assert held["legs"]["LONG"]["capital_step"]["index"] == 0
    assert held["legs"]["LONG"]["clean_round_trips_at_step"] == need_1 - 1
    assert held["legs"]["LONG"]["transition"] == "NONE"
    # Round trips opened before the promotion never count.
    stale_trips = _trips(need_1, start=NOW - timedelta(hours=1))
    held = after(promoted, _live(stale_trips), t1)
    assert held["legs"]["LONG"]["clean_round_trips_at_step"] == 0
    # An unclean round trip (outside the bound, or not closed by the harness) restarts the count.
    for unclean in (
        _trips(1, start=after_promotion + timedelta(minutes=5), shortfall="9.0"),
        _trips(1, start=after_promotion + timedelta(minutes=5), cause="OWNER_MANUAL_CLOSE"),
    ):
        broken = (
            _trips(10, start=after_promotion)
            + unclean
            + _trips(need_1 - 1, start=after_promotion + timedelta(minutes=10))
        )
        held = after(promoted, _live(broken), t1)
        assert held["legs"]["LONG"]["status"] == "PROMOTED"
        assert held["legs"]["LONG"]["capital_step"]["index"] == 0
        assert held["legs"]["LONG"]["clean_round_trips_at_step"] == need_1 - 1
    # Enough clean round trips at one share: the LONG leg (and only it) moves up one step.
    step_1 = after(promoted, _live(_trips(need_1, start=after_promotion)), t1)
    assert step_1["legs"]["LONG"]["transition"] == "CAPITAL_STEP_ADVANCED"
    assert step_1["legs"]["LONG"]["capital_step"] == CAPITAL_LADDER[1].model_dump(mode="json")
    assert step_1["legs"]["LONG"]["capital_step"]["max_qty"] is None
    assert step_1["legs"]["LONG"]["capital_step_entered_at"] == t1.isoformat()
    assert step_1["legs"]["LONG"]["clean_round_trips_at_step"] == 0
    assert step_1["legs"]["SHORT"]["capital_step"]["index"] == 0
    # The record of a promoted leg carries its current step.
    assert merit._evaluate(evidence, SMALL, t1, step_1)["legs"]["LONG"]["capital_step"] == (
        CAPITAL_LADDER[1].model_dump(mode="json")
    )
    # One step per call, and only on round trips opened at the current step.
    many_at_step_0 = _live(_trips(need_1 + need_2 + 5, start=after_promotion))
    t2 = t1 + timedelta(minutes=30)
    assert after(step_1, many_at_step_0, t2)["legs"]["LONG"]["capital_step"]["index"] == 1
    at_step_1 = _trips(need_2, start=t1 + timedelta(minutes=1), step=1)
    step_2 = after(step_1, _live(at_step_1), t2)
    assert step_2["legs"]["LONG"]["capital_step"]["name"] == "TARGET_ALLOCATION"
    assert step_2["legs"]["LONG"]["capital_step"]["risk_qty_fraction"] == "1"
    top = after(
        step_2,
        _live(_trips(500, start=t2 + timedelta(minutes=1), step=2)),
        t2 + timedelta(minutes=30),
    )
    assert top["legs"]["LONG"]["capital_step"]["index"] == 2
    assert top["legs"]["LONG"]["transition"] == "NONE"
    # The state survives persistence: a reloaded state behaves the same.
    assert after(json.loads(json.dumps(step_1)), _live(at_step_1), t2) == step_2

    # --- Demotion: every trigger latches the leg and resets it to one share ---
    window = merit.LIVE_SHORTFALL_WINDOW_ROUND_TRIPS
    costly = _trips(window, start=t1 + timedelta(minutes=1), step=1, shortfall="8.5")
    before_promotion = _trips(window, start=NOW - timedelta(hours=2), shortfall="50")
    today = merit._trade_date(t2)
    days = [(today - timedelta(days=back)).isoformat() for back in range(7, -1, -1)]

    def fuse(*tripped: int, count: int = 5) -> list[dict[str, Any]]:
        """The last ``count`` sessions up to today, tripped at the given positions."""
        return [
            {"session": day, "tripped": index in tripped} for index, day in enumerate(days[-count:])
        ]

    triggers: list[tuple[Any, list[str]]] = [
        (_live(costly), ["LIVE_SHORTFALL_ABOVE_MAX"]),
        # The windows are not cut at the promotion: what was costly before it still is.
        (_live(before_promotion), ["LIVE_SHORTFALL_ABOVE_MAX"]),
        (_live([], fuse=fuse(1, 3)), ["LIVE_FUSE_TRIPPED_TWO_OF_FIVE_SESSIONS"]),
        (_live([], fuse=fuse(3, 4, count=8)), ["LIVE_FUSE_TRIPPED_TWO_OF_FIVE_SESSIONS"]),
        (_live([], faults=1), ["LIVE_PROTECTION_FAULT"]),
        (
            _live(_trips(1, start=t1 + timedelta(minutes=1), step=1, fault=True)),
            ["LIVE_PROTECTION_FAULT"],
        ),
        ({"legs": {}}, ["LIVE_EVIDENCE_MALFORMED"]),
        ("not evidence", ["LIVE_EVIDENCE_MALFORMED"]),
    ]
    for live_evidence, reasons in triggers:
        state = after(step_1, live_evidence, t2)
        entry = state["legs"]["LONG"]
        assert (entry["status"], entry["transition"]) == ("DEMOTED", "DEMOTED"), reasons
        assert entry["demotion_reasons"] == reasons
        assert entry["latched"] is True and entry["live_entry_eligible"] is False
        assert entry["demoted_at"] == t2.isoformat()
        assert entry["capital_step"] == CAPITAL_LADDER[0].model_dump(mode="json")
        assert entry["capital_step_entered_at"] is None
    # An account-level fuse trigger demotes every promoted leg; a LONG-only trigger leaves SHORT.
    assert after(step_1, _live([], fuse=fuse(1, 3)), t2)["legs"]["SHORT"]["status"] == "DEMOTED"
    assert after(step_1, _live(costly), t2)["legs"]["SHORT"]["status"] == "PROMOTED"
    # Below each trigger nothing happens.
    quiet: list[Any] = [
        QUIET,
        _live(costly[: window - 1]),  # the shortfall window is not full yet
        _live(_trips(window, start=t1 + timedelta(minutes=1), step=1, shortfall="8.0")),
        _live(before_promotion[: window - 1]),
        _live([], fuse=fuse(2)),  # one tripped session
        _live([], fuse=fuse(0, 1, count=8)),  # two trips, but outside the last five sessions
        # A tier-3 fault that closed before this promotion was not a fault of this promotion.
        _live(_trips(1, start=NOW - timedelta(hours=1), fault=True)),
    ]
    for live_evidence in quiet:
        entry = after(step_1, live_evidence, t2)["legs"]["LONG"]
        assert entry["status"] == "PROMOTED" and entry["live_entry_eligible"] is True
    # Lost merit demotes with the record's own reasons; an unusable record does too.
    lost = after(step_1, QUIET, t2, failing)["legs"]["LONG"]
    assert lost["status"] == "DEMOTED"
    assert lost["demotion_reasons"][:2] == ["MERIT_LOST", "DOSSIER_NOT_PASSING"]
    junk = next_promotion_state(
        step_1, {"schema": "junk"}, live_evidence=QUIET, now_utc=t2, thresholds=SMALL
    )
    assert junk["legs"]["LONG"]["demotion_reasons"] == [
        "MERIT_LOST",
        "RECORD_NOT_COMPUTED_WITH_STATE",
    ]
    current = merit._evaluate(evidence, SMALL, t2, step_1)
    expired = next_promotion_state(
        step_1,
        current,
        live_evidence=QUIET,
        now_utc=datetime.fromisoformat(current["valid_until"]),
        thresholds=SMALL,
    )
    assert expired["legs"]["LONG"]["demotion_reasons"] == ["MERIT_LOST"]
    # A record computed under SMALL is not merit for a caller configured with the defaults.
    defaults = next_promotion_state(step_1, current, live_evidence=QUIET, now_utc=t2)
    assert defaults["legs"]["LONG"]["demotion_reasons"] == ["MERIT_LOST"]

    # --- The latch: cool-off sessions AND a passing record on evidence newer than the demotion ---
    demoted = after(step_1, QUIET, t2, long_failing)  # LONG demoted Tuesday 2026-12-15 16:00Z
    demoted_at = t2
    assert demoted["legs"]["LONG"]["demotion_reasons"][:2] == ["MERIT_LOST", "DOSSIER_NOT_PASSING"]
    assert demoted["legs"]["SHORT"]["status"] == "PROMOTED"
    assert SMALL.demotion_cooloff_sessions == 5

    def on(
        day: int,
        plan: dict[str, Any] | None = None,
        *,
        state: dict[str, Any] = demoted,
        live: Any = QUIET,
        thresholds: LivePromotionThresholds = SMALL,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        moment = datetime(2026, 12, day, 15, 0, tzinfo=UTC)
        record = _compute(
            tmp_path / f"day-{day}",
            plan or _base_plan(before=moment),
            now=moment,
            thresholds=thresholds,
            prior_state=state,
        )
        _assert_record_invariants(record, now=moment, thresholds=thresholds)
        return record, next_promotion_state(
            state, record, live_evidence=live, now_utc=moment, thresholds=thresholds
        )

    # Next day: fresh files, but every row was decided before the demotion and no cool-off
    # session has completed.
    record, state = on(16)
    assert record["legs"]["LONG"]["evidence_floor"] == demoted_at.isoformat()
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 0
    assert record["legs"]["LONG"]["reasons"] == [*_NOTHING_COUNTED, "DEMOTION_LATCHED"]
    assert record["legs"]["SHORT"]["evidence_floor"] is None
    assert record["legs"]["SHORT"]["criteria"]["paper_round_trips"]["observed"] == 8
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["hold_reasons"][:2] == ["DEMOTION_LATCHED", "NO_CURRENT_MERIT"]
    assert state["legs"]["LONG"]["cooloff"]["cooloff_sessions_completed"] == 0
    assert state["legs"]["LONG"]["demoted_at"] == demoted_at.isoformat()  # the latch keeps its date
    assert state["legs"]["SHORT"]["status"] == "PROMOTED"

    # Four completed sessions (Dec 16, 17, 18, 21) and eight round trips decided in them: the
    # paper criteria pass on the new rows, the cool-off has not.
    record, state = on(22)
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 8
    assert record["legs"]["LONG"]["reasons"] == ["DEMOTION_LATCHED"]
    assert record["legs"]["LONG"]["criteria"]["demotion_latch"]["status"] == "fail"
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["hold_reasons"] == ["DEMOTION_LATCHED", "NO_CURRENT_MERIT"]
    assert state["legs"]["LONG"]["cooloff"]["cooloff_sessions_completed"] == 4

    # Five completed sessions and files regenerated an hour ago, but the rows are the ones from
    # before the demotion: fresh stamps are not fresh evidence.
    record, state = on(23, _base_plan())
    latch = record["legs"]["LONG"]["criteria"]["demotion_latch"]
    assert latch["status"] == "pass" and latch["observed"]["released"] is True
    assert latch["observed"]["cooloff_sessions_completed"] == 5
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 0
    assert record["legs"]["LONG"]["reasons"] == _NOTHING_COUNTED
    assert record["legs"]["SHORT"]["merit"] is True  # SHORT was never demoted: its rows all count
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["cooloff"]["released"] is True
    assert state["legs"]["LONG"]["hold_reasons"][:2] == [
        "NO_CURRENT_MERIT",
        "DOSSIER_LEG_BINDING_MISSING",
    ]

    # Five completed sessions and new rows, but a manifest generated before the demotion.
    patient = SMALL.model_copy(update={"evidence_max_age_hours": 24 * 30})
    old_manifest = _base_plan(before=datetime(2026, 12, 23, 15, 0, tzinfo=UTC))
    old_manifest["age_hours"]["dossier"] = 24 * 9.0  # generated on Dec 14
    record, state = on(23, old_manifest, thresholds=patient)
    assert record["legs"]["LONG"]["reasons"] == ["DEMOTION_LATCHED"]
    latch = record["legs"]["LONG"]["criteria"]["demotion_latch"]["observed"]
    assert latch["manifests_newer_than_demotion"] is False
    assert latch["cooloff_sessions_completed"] == 5
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["cooloff"]["evidence_newer_than_demotion"] is False
    assert state["legs"]["LONG"]["hold_reasons"][0] == "DEMOTION_LATCHED"

    # Cool-off served and newer evidence, but the record does not pass: still demoted.
    not_passing = _base_plan(before=datetime(2026, 12, 23, 15, 0, tzinfo=UTC))
    not_passing["dossier"] = _dossier(long_pass=False, short_pass=True)
    record, state = on(23, not_passing)
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["cooloff"]["released"] is True
    assert state["legs"]["LONG"]["hold_reasons"][:2] == ["NO_CURRENT_MERIT", "DOSSIER_NOT_PASSING"]

    # Cool-off served and a passing record on rows decided after the demotion. The release call
    # still reads the live evidence: an open fault, or none at all, holds the promotion.
    history = _plan_with_history(datetime(2026, 12, 23, 15, 0, tzinfo=UTC))
    record, state = on(23, history, live=_live([], faults=1))
    assert record["legs"]["LONG"]["merit"] is True
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 8  # not 16
    assert record["legs"]["SHORT"]["criteria"]["paper_round_trips"]["observed"] == 16
    assert (state["legs"]["LONG"]["status"], state["legs"]["LONG"]["transition"]) == (
        "DEMOTED",
        "NONE",
    )
    assert state["legs"]["LONG"]["hold_reasons"] == ["LIVE_PROTECTION_FAULT"]
    assert state["legs"]["LONG"]["live_entry_eligible"] is False
    assert on(23, history, live=None)[1]["legs"]["LONG"]["hold_reasons"] == [
        "LIVE_EVIDENCE_MISSING"
    ]
    _, state = on(23, history, live="not evidence")
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["hold_reasons"] == ["LIVE_EVIDENCE_MALFORMED"]
    assert state["legs"]["SHORT"]["demotion_reasons"] == ["LIVE_EVIDENCE_MALFORMED"]

    # With live evidence that shows nothing: promoted again, from one share.
    moment = datetime(2026, 12, 23, 15, 0, tzinfo=UTC)
    record, released = on(23, history)
    entry = released["legs"]["LONG"]
    assert (entry["status"], entry["transition"]) == ("PROMOTED", "PROMOTED")
    assert entry["latched"] is False and entry["live_entry_eligible"] is True
    assert entry["promoted_at"] == moment.isoformat()
    assert entry["demoted_at"] == demoted_at.isoformat()
    assert entry["capital_step"] == CAPITAL_LADDER[0].model_dump(mode="json")
    assert entry["cooloff"]["released"] is True
    assert merit.live_entry_leg_merit(released, record, "LONG", now_utc=moment, thresholds=SMALL)
    # The very next call on identical inputs keeps it promoted, and the evidence floor stays:
    # the rows from before the demotion never count for this leg again.
    record, again = on(23, history, state=released)
    assert record["legs"]["LONG"]["evidence_floor"] == demoted_at.isoformat()
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 8
    assert record["legs"]["LONG"]["criteria"]["demotion_latch"]["observed"] == {"latched": False}
    entry = again["legs"]["LONG"]
    assert (entry["status"], entry["transition"]) == ("PROMOTED", "NONE")
    assert entry["live_entry_eligible"] is True and entry["promoted_at"] == moment.isoformat()
    assert merit.live_entry_leg_merit(again, record, "LONG", now_utc=moment, thresholds=SMALL)

    # --- Unknown history never promotes ---
    wrong_step = copy.deepcopy(promoted)
    wrong_step["legs"]["LONG"]["capital_step"]["index"] = 99
    no_date = copy.deepcopy(promoted)
    no_date["legs"]["LONG"]["promoted_at"] = "yesterday"
    no_latch_date = copy.deepcopy(demoted)
    no_latch_date["legs"]["LONG"]["demoted_at"] = None
    from_the_future = copy.deepcopy(promoted)
    from_the_future["legs"]["LONG"]["promoted_at"] = (NOW + timedelta(days=1)).isoformat()
    stepped_in_paper = copy.deepcopy(idle)
    stepped_in_paper["legs"]["LONG"]["capital_step"]["index"] = 1
    broken_states: list[Any] = [
        {},
        {"schema": "other"},
        [],
        "state",
        wrong_step,
        no_date,
        no_latch_date,
        from_the_future,
        stepped_in_paper,
    ]
    for broken in broken_states:
        record, state = _cycle(evidence, broken, QUIET, NOW)
        entry = state["legs"]["LONG"]
        assert entry["status"] == "DEMOTED" and entry["latched"] is True, broken
        assert entry["demotion_reasons"] == ["PRIOR_STATE_MALFORMED"]
        assert entry["demoted_at"] == NOW.isoformat()
        assert record["legs"]["LONG"]["reasons"] == ["DEMOTION_LATCHED"]
        assert record["legs"]["LONG"]["criteria"]["demotion_latch"]["status"] == "fail"
        assert record["legs"]["LONG"]["capital_step"]["index"] == 0
    with pytest.raises(ValueError, match="timezone-aware"):
        next_promotion_state(NO_HISTORY, passing, now_utc=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timezone-aware"):
        compute_live_promotion_merit(tmp_path, now_utc=NOW.replace(tzinfo=None))


def test_live_cause_demotion_release_is_reachable_and_does_not_flap(tmp_path: Path) -> None:
    """A leg demoted for live shortfall makes no tier-3 round trip, so its way back is
    operator-confirmed live round trips: enough of them, clean, opened after the demotion, and
    enough to bring the same window that demoted it back inside the bound."""
    window = merit.LIVE_SHORTFALL_WINDOW_ROUND_TRIPS
    need = merit.LIVE_CAUSE_RELEASE_CONFIRMED_ROUND_TRIPS
    assert (window, need) == (20, 10)

    # Tuesday Dec 15: both legs are promoted at 15:00Z; by 15:30Z the LONG leg has made twenty
    # tier-3 round trips at 8.5 bps and is demoted for it.
    opened = NOW
    then = merit._read_evidence(_write_plan(tmp_path / "then", _base_plan()))
    _, promoted = _cycle(then, NO_HISTORY, QUIET, opened)
    costly = _trips(window, start=opened + timedelta(minutes=1), shortfall="8.5")
    demoted_at = opened + timedelta(minutes=30)
    _, demoted = _cycle(then, promoted, _live(costly), demoted_at)
    assert demoted["legs"]["LONG"]["status"] == "DEMOTED"
    assert demoted["legs"]["LONG"]["demotion_reasons"] == ["LIVE_SHORTFALL_ABOVE_MAX"]
    assert demoted["legs"]["SHORT"]["status"] == "PROMOTED"

    # Wednesday Dec 23: five sessions have completed and the paper lanes have eight new LONG
    # round trips decided after the demotion.
    moment = datetime(2026, 12, 23, 15, 0, tzinfo=UTC)
    evidence = merit._read_evidence(
        _write_plan(tmp_path / "later", _plan_with_history(moment), now=moment)
    )

    def confirmed(count: int, shortfall: str = "2.0", start: Any = None) -> list[dict[str, Any]]:
        return _trips(
            count,
            start=start or demoted_at + timedelta(days=1),
            shortfall=shortfall,
            authority="OPERATOR_CONFIRMED",
        )

    def release(live: Any) -> dict[str, Any]:
        record, state = _cycle(evidence, demoted, live, moment)
        assert record["legs"]["LONG"]["merit"] is True  # paper merit on post-demotion rows
        return state["legs"]["LONG"]

    # Nothing new on live: the window that demoted the leg is unchanged.
    entry = release(_live(costly))
    assert entry["status"] == "DEMOTED"
    assert entry["hold_reasons"] == ["DEMOTION_LATCHED", "LIVE_SHORTFALL_ABOVE_MAX"]
    assert entry["cooloff"]["live_causes"] == ["LIVE_SHORTFALL_ABOVE_MAX"]
    assert entry["cooloff"]["live_confirmed_round_trips_after_demotion"] == 0
    assert entry["cooloff"]["live_confirmed_round_trips_required"] == need
    assert entry["cooloff"]["cooloff_sessions_completed"] == 5
    # Live evidence that shows nothing at all is not a way round the live cause.
    assert release(QUIET)["hold_reasons"] == ["DEMOTION_LATCHED"]
    # One confirmed round trip short. The window is inside the bound already: (11 * 8.5 + 9 * 2)
    # / 20 = 5.6, so only the count holds it.
    entry = release(_live(costly + confirmed(need - 1)))
    assert entry["hold_reasons"] == ["DEMOTION_LATCHED"]
    assert entry["cooloff"]["live_confirmed_round_trips_after_demotion"] == need - 1
    # Enough of them, but costly: not clean, and the window is still outside the bound.
    entry = release(_live(costly + confirmed(need, shortfall="9.0")))
    assert entry["hold_reasons"] == ["DEMOTION_LATCHED", "LIVE_SHORTFALL_ABOVE_MAX"]
    assert entry["cooloff"]["live_confirmed_round_trips_after_demotion"] == 0
    # Confirmed round trips from before the demotion, and tier-3 ones after it, do not count.
    entry = release(_live(confirmed(need, start=opened - timedelta(hours=1)) + costly))
    assert entry["hold_reasons"] == ["DEMOTION_LATCHED", "LIVE_SHORTFALL_ABOVE_MAX"]
    lingering = _trips(need, start=demoted_at + timedelta(days=1))
    entry = release(_live(costly + lingering))
    assert entry["hold_reasons"] == ["DEMOTION_LATCHED"]
    assert entry["cooloff"]["live_confirmed_round_trips_after_demotion"] == 0
    # A fault on a confirmed round trip makes it unclean.
    entry = release(
        _live(costly + confirmed(need - 1) + confirmed(1, start=moment - timedelta(hours=1)))
    )
    assert entry["status"] == "PROMOTED"
    faulty = [{**confirmed(1, start=moment - timedelta(hours=1))[0], "protection_fault": True}]
    entry = release(_live(costly + confirmed(need - 1) + faulty))
    assert entry["hold_reasons"] == ["DEMOTION_LATCHED"]

    # Ten clean confirmed round trips after the demotion: released, from one share.
    recovered = _live(costly + confirmed(need))
    record, released = _cycle(evidence, demoted, recovered, moment)
    entry = released["legs"]["LONG"]
    assert (entry["status"], entry["transition"]) == ("PROMOTED", "PROMOTED")
    assert entry["capital_step"]["index"] == 0 and entry["hold_reasons"] == []
    assert entry["cooloff"]["released"] is True
    assert merit.live_entry_leg_merit(released, record, "LONG", now_utc=moment, thresholds=SMALL)
    # The very next call on identical inputs keeps it promoted.
    record, again = _cycle(evidence, released, recovered, moment)
    entry = again["legs"]["LONG"]
    assert (entry["status"], entry["transition"]) == ("PROMOTED", "NONE")
    assert entry["live_entry_eligible"] is True and entry["clean_round_trips_at_step"] == 0

    # The window that demoted the leg was not reset by the new promotion: five costly tier-3
    # round trips are enough to demote it again ((5 * 8.5 + 10 * 2 + 5 * 20) / 20 = 8.1), where
    # a window restarted at the promotion would have needed twenty.
    later = moment + timedelta(hours=1)
    new_costly = _trips(5, start=moment + timedelta(minutes=1), shortfall="20")
    record, state = _cycle(evidence, again, _live(costly + confirmed(need) + new_costly[:4]), later)
    assert state["legs"]["LONG"]["status"] == "PROMOTED"  # (6 * 8.5 + 10 * 2 + 4 * 20) / 20 = 7.6
    record, state = _cycle(evidence, again, _live(costly + confirmed(need) + new_costly), later)
    assert state["legs"]["LONG"]["status"] == "DEMOTED"
    assert state["legs"]["LONG"]["demotion_reasons"] == ["LIVE_SHORTFALL_ABOVE_MAX"]
    assert state["legs"]["LONG"]["demoted_at"] == later.isoformat()  # a new latch, a new floor


# --- Property: merit is never granted on anything but passing evidence ----------------------------


def _is_real(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


_ODD_VALUES: list[Any] = [
    True,
    False,
    None,
    "0.99",
    "0.2",
    0,
    1,
    0.95,
    0.9499,
    0.5,
    0.4999,
    -0.01,
    1.01,
    float("nan"),
    float("inf"),
]
_ODD_NUMBERS = st.one_of(
    st.floats(allow_nan=True, allow_infinity=True, width=64), st.sampled_from(_ODD_VALUES)
)
_NOT_TRUE: list[Any] = [False, None, 1, "true", "True", 0, [True]]
_STAGE4_FLAGS = [
    "all_passed",
    "all_drills_passed",
    *(drill for drill, _ in merit.STAGE4_DRILL_FLAGS),
]
_PAPER_GAPS = [
    "tier_columns",
    "binding_column",
    "fill_link_column",
    "tripped_column",
    "fills_table",
    "fuse_table",
]
_AGE_SOURCES = ["dossier", "stage4", "receipt", "paper"]
_AGES = [0.0, 2.0, 35.9, 36.1, 500.0, -0.05, -1.0]
_PBO_KEYS = ["empirical_pbo", *merit.DOSSIER_COST_BLOCKS]
_BOUND = Decimal("8")


def _pairs(first: list[Any], second: list[Any]) -> list[tuple[Any, Any]]:
    return [(one, other) for one in first for other in second]


def _decimal(text: str) -> Decimal:
    return Decimal(text)


def _long_round_trips(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """The LONG scanner round trips of a plan, oldest decision first (the test's own reading)."""
    rows = [row for row in plan["paper"]["entries"] if row["cid"].startswith("rt-LONG-")]
    return sorted(rows, key=lambda row: (row["decided_at"], row["cid"]))


def _too_stale(hours: float) -> bool:
    return hours > 36 or hours < -5 / 60


def _long_rows_break(value: tuple[int, int, int], plan: dict[str, Any]) -> set[str]:
    count, sessions, supervised = value
    enough = count >= 6 and min(count, sessions) >= 3 and min(count, supervised) >= 2
    return set() if enough else {"LONG"}


def _long_shortfall_breaks(value: tuple[Decimal, Decimal], plan: dict[str, Any]) -> set[str]:
    """Independent arithmetic on the plan: the mean of all fills and of the newest six trips."""
    trips = _long_round_trips(plan)
    by_trip = {
        row["cid"]: [
            fill["implementation_shortfall_bps"]
            for fill in plan["paper"]["fills"]
            if fill["entry_client_order_id"] == row["cid"]
        ]
        for row in trips
    }
    values = [v for cid in by_trip for v in by_trip[cid]]
    recent = [v for row in trips[-6:] for v in by_trip[row["cid"]]]
    if not values or any(v is None for v in values):
        return set()  # nothing to grade here: another knob has broken the leg
    means = (sum(values) / len(values), sum(recent) / len(recent))
    return {"LONG"} if any(mean > _BOUND for mean in means) else set()


# Each knob: (every value of the deterministic sweep, the legs a value must break, strategy).
# The oracle sees the plan after every knob was applied, for the knobs whose effect depends on it.
_KNOBS: dict[str, tuple[list[Any], Any, Any]] = {
    "long_verdict": (
        ["DISCARD", "DISCARD_INSUFFICIENT_TRADES", "promote", "", None, 1, True],
        lambda value, plan: {"LONG"},
        None,
    ),
    "short_verdict": (["DISCARD", "PROMOTED", None, 0], lambda value, plan: {"SHORT"}, None),
    "long_promoted": (_NOT_TRUE, lambda value, plan: {"LONG"}, None),
    "long_gate": (
        _pairs(list(merit.DOSSIER_GATES), _NOT_TRUE),
        lambda value, plan: {"LONG"},
        None,
    ),
    "long_dsr": (
        _pairs(list(merit.DOSSIER_COST_BLOCKS), _ODD_VALUES),
        lambda value, plan: set() if _is_real(value[1]) and 0.95 <= value[1] <= 1 else {"LONG"},
        st.tuples(st.sampled_from(merit.DOSSIER_COST_BLOCKS), _ODD_NUMBERS),
    ),
    "long_pbo": (
        _pairs(_PBO_KEYS, _ODD_VALUES),
        lambda value, plan: set() if _is_real(value[1]) and 0 <= value[1] < 0.5 else {"LONG"},
        st.tuples(st.sampled_from(_PBO_KEYS), _ODD_NUMBERS),
    ),
    "dry_run": ([True, None, "false", 0], lambda value, plan: {"LONG", "SHORT"}, None),
    "hypothesis_id": ([None, 7, "", "H_THIRD"], lambda value, plan: {"LONG", "SHORT"}, None),
    # (round trips, sessions they are spread over, supervised exits among them)
    "long_rows": (
        [
            *((count, 4, 3) for count in range(13)),
            *((8, sessions, 3) for sessions in (1, 2, 3)),
            *((8, 4, supervised) for supervised in (0, 1, 2)),
            (6, 3, 2),
            (6, 2, 2),
            (6, 3, 1),
            (5, 3, 2),
        ],
        _long_rows_break,
        st.tuples(st.integers(0, 12), st.integers(1, 4), st.integers(0, 3)),
    ),
    "long_authority": (
        ["OWNER_LINEAGE", "OPERATOR_CONFIRMED", None, "autonomous"],
        lambda value, plan: {"LONG"},
        None,
    ),
    "long_close_cause": (
        ["PROTECTIVE_STOP_FILLED", "OWNER_MANUAL_CLOSE", None, "ENTRY_CANCELED"],
        lambda value, plan: {"LONG"},
        None,
    ),
    # One more attributable LONG row that is not a completed round trip: nothing changes.
    "long_extra_row": (
        ["OPEN", "ENTRY_CANCELED", "OWNER_MANUAL_CLOSE", "CLOSED_BEFORE_DECIDED"],
        lambda value, plan: set(),
        None,
    ),
    "long_origin": (
        [
            "INVERSE_HEDGE_SOXS",
            "TV_HALF_AUTO_CONFIRMED",
            "DIRECT_SHORT_ETB",
            "AUTO_ENTRY",
            "TREND_ALIGNED_LONG_REENTRY",
            "DIRECT_LONG_REVERSAL_AUTO_REVERSE",
            "TREND_ALIGNED_LONG_REENTRY_REENTRY",
        ],
        lambda value, plan: (
            set()
            if value in ("TREND_ALIGNED_LONG_REENTRY", "DIRECT_LONG_REVERSAL_AUTO_REVERSE")
            else {"LONG"}
        ),
        None,
    ),
    "long_regime": ([1, 0, -1, None, 7], lambda value, plan: {"LONG"}, None),
    # The regime of one round trip only: a single unknown is enough.
    "long_one_regime": ([None, 7, -2], lambda value, plan: {"LONG"}, None),
    # (shortfall of the older round trips, shortfall of the newest six)
    "long_shortfall": (
        [
            *((_decimal(v), _decimal(v)) for v in ("-5", "0", "7.999", "8.000", "8.001", "30")),
            (_decimal("3"), _decimal("8.000")),
            (_decimal("3"), _decimal("8.001")),
            (_decimal("3"), _decimal("25")),
            (_decimal("30"), _decimal("0")),
            (_decimal("23"), _decimal("3")),  # (2 * 23 + 6 * 3) / 8 = 8.000
            (_decimal("23.004"), _decimal("3")),  # 8.001
        ],
        _long_shortfall_breaks,
        st.tuples(
            st.decimals(min_value=-5, max_value=30, places=3),
            st.decimals(min_value=-5, max_value=30, places=3),
        ),
    ),
    "long_unlinked_round_trip": ([True], lambda value, plan: {"LONG"}, None),
    "long_null_fill": ([True], lambda value, plan: {"LONG"}, None),
    "long_unbound": (["H_OTHER", None], lambda value, plan: {"LONG"}, None),
    "paper_gap": (_PAPER_GAPS, lambda value, plan: {"LONG", "SHORT"}, None),
    "fuse_trip": (
        [None, "", "MARKET_DRAWDOWN", "DEFECT_STALE_QUOTE"],
        lambda value, plan: set() if value == "MARKET_DRAWDOWN" else {"LONG", "SHORT"},
        None,
    ),
    "fuse_gap": (["NEWEST_DAY", "ALL_DAYS"], lambda value, plan: {"LONG", "SHORT"}, None),
    "ledger_copy": (
        ["SAME_FILE", "SAME_ROWS", "SAME_ACCOUNT", "LIVE_IS_A_PAPER_ACCOUNT"],
        lambda value, plan: {"LONG", "SHORT"},
        None,
    ),
    "receipt": (
        [
            ("all_reconciled", False),
            ("all_reconciled", "true"),
            ("all_reconciled", 1),
            ("status", "HALTED_RECONCILIATION_MISMATCH"),
            ("status", None),
            ("unreconciled_delta_count", 1),
            ("unreconciled_delta_count", True),
            ("unreconciled_delta_count", None),
            ("account_sha256", None),
            ("account_sha256", _account_sha256(1)),
            ("drop", None),
        ],
        lambda value, plan: {"LONG", "SHORT"},
        None,
    ),
    "stage4": (_pairs(_STAGE4_FLAGS, _NOT_TRUE), lambda value, plan: {"LONG", "SHORT"}, None),
    "age": (
        _pairs(_AGE_SOURCES, _AGES),
        lambda value, plan: {"LONG", "SHORT"} if _too_stale(value[1]) else set(),
        None,
    ),
    "live_age": (_AGES, lambda value, plan: {"SHORT"} if _too_stale(value) else set(), None),
    "live_shorts": (list(range(5)), lambda value, plan: {"SHORT"} if value < 2 else set(), None),
}
# Knobs that rebuild rows go first, then the one that rewrites fills, the rest, and last the one
# that copies the finished ledger.
_KNOB_ORDER = {"long_rows": 0, "live_shorts": 0, "long_shortfall": 1, "ledger_copy": 3}


def _apply_knob(plan: dict[str, Any], name: str, value: Any) -> None:
    dossier, paper = plan["dossier"], plan["paper"]
    long_block = dossier["holdout_six_gate"]["LONG"]
    long_rows = [row for row in paper["entries"] if row["side"] == "LONG"]
    long_fills = _long_fills(plan)
    if name == "long_verdict":
        long_block["verdict"] = value
    elif name == "short_verdict":
        dossier["holdout_six_gate"]["SHORT"]["verdict"] = value
    elif name == "long_promoted":
        long_block["promoted"] = value
    elif name == "long_gate":
        long_block["gates"][value[0]] = value[1]
    elif name == "long_dsr":
        long_block[value[0]]["dsr"] = value[1]
    elif name == "long_pbo":
        if value[0] == "empirical_pbo":
            long_block["empirical_pbo"] = value[1]
        else:
            long_block[value[0]]["pbo"] = value[1]
    elif name == "dry_run":
        dossier["dry_run"] = value
    elif name == "hypothesis_id":
        dossier["hypothesis_id"] = value
    elif name == "long_rows":
        count, sessions, supervised = value
        _set_long(plan, count, sessions=sessions, supervised=supervised)
    elif name in ("long_authority", "long_close_cause", "long_origin", "long_regime"):
        column = {
            "long_authority": "entry_authority",
            "long_close_cause": "close_cause",
            "long_origin": "origin",
            "long_regime": "macro_regime",
        }[name]
        for row in long_rows:
            row[column] = value
    elif name == "long_extra_row":
        rows, fills = _round_trips("LONG", 1, prefix="extra")
        if value == "OPEN":
            rows[0].update(closed_at=None, close_cause=None)
        elif value == "CLOSED_BEFORE_DECIDED":
            rows[0].update(closed_at=rows[0]["decided_at"] - timedelta(minutes=1))
        else:
            rows[0].update(close_cause=value)
        paper["entries"] += rows
        paper["fills"] += fills
    elif name == "long_one_regime":
        for row in _long_round_trips(plan)[:1]:
            row["macro_regime"] = value
    elif name == "long_shortfall":
        older, newest = value
        recent = {row["cid"] for row in _long_round_trips(plan)[-6:]}
        for fill in long_fills:
            is_recent = fill["entry_client_order_id"] in recent
            fill["implementation_shortfall_bps"] = newest if is_recent else older
    elif name == "long_unlinked_round_trip":
        for fill in long_fills[:2]:  # both fills of the first LONG round trip
            fill["entry_client_order_id"] = None
    elif name == "long_null_fill":
        for fill in long_fills[:1]:
            fill["implementation_shortfall_bps"] = None
    elif name == "long_unbound":
        for row in long_rows:
            row["dossier_hypothesis_id"] = value
    elif name == "paper_gap":
        paper[value] = False
    elif name == "fuse_trip":
        if paper["fuse"]:
            _trip(plan, value)
    elif name == "fuse_gap":
        paper["fuse"] = paper["fuse"][1:] if value == "NEWEST_DAY" else []
    elif name == "ledger_copy":
        if value == "SAME_FILE":
            plan["paper2"] = copy.deepcopy(paper)
        elif value == "SAME_ROWS":
            plan["paper2"] = {**copy.deepcopy(paper), "account": 2}
        elif value == "SAME_ACCOUNT":
            plan["paper2"] = _idle_spec(paper, 1)
        else:
            plan["live"]["account"] = 3
    elif name == "receipt":
        key, item = value
        if key == "drop":
            del plan["receipt"]["accounts"]["ALPACA_PA2"]
        elif key == "all_reconciled":
            plan["receipt"]["all_reconciled"] = item
        else:
            plan["receipt"]["accounts"]["ALPACA_PA3"][key] = item
    elif name == "stage4":
        flag, item = value
        if flag in plan["stage4"]:
            plan["stage4"][flag] = item
        else:
            key = dict(merit.STAGE4_DRILL_FLAGS)[flag]
            plan["stage4"]["drills"][flag][key] = item
    elif name == "age":
        plan["age_hours"][value[0]] = value[1]
    elif name == "live_age":
        plan["age_hours"]["live"] = value
    elif name == "live_shorts":
        rows, fills = _round_trips(
            "SHORT", value, authority="OPERATOR_CONFIRMED", hypothesis=None, prefix="live"
        )
        plan["live"]["entries"], plan["live"]["fills"] = rows, fills
    else:  # pragma: no cover
        raise AssertionError(name)


_BASE: dict[str, Any] = {}


def _base_evidence() -> tuple[dict[str, Any], Any]:
    """The base plan and its evidence as read from disk, written and read once per run."""
    if not _BASE:
        _BASE["holder"] = tempfile.TemporaryDirectory(prefix="tier0-merit-base-")
        _BASE["plan"] = _base_plan()
        _BASE["evidence"] = merit._read_evidence(
            _write_plan(Path(_BASE["holder"].name), _BASE["plan"])
        )
    return _BASE["plan"], _BASE["evidence"]


def _record_for(plan: dict[str, Any]) -> dict[str, Any]:
    """The record of a plan under ``SMALL`` at ``NOW``.

    A plan that changes a ledger is written and read from disk. One that only changes a JSON
    manifest or an age reuses the base plan's ledgers as read from disk and parses the bytes
    ``_write_plan`` would have written, which is what makes the full sweep affordable.
    """
    base_plan, base = _base_evidence()
    ledgers_changed = any(plan[key] != base_plan[key] for key in ("paper", "live"))
    if ledgers_changed or "paper2" in plan or "paper3" in plan:
        with tempfile.TemporaryDirectory(prefix="tier0-merit-knob-") as tmp:
            return _compute(Path(tmp), plan)
    ages = plan["age_hours"]

    def manifest(source: Any, key: str) -> Any:
        doc = dict(plan[key])
        if doc.get("generated_at_utc") is None:
            doc["generated_at_utc"] = (NOW - timedelta(hours=ages[key])).isoformat()
        raw = (json.dumps(doc, indent=2) + "\n").encode("utf-8")
        return dataclasses.replace(
            source, sha256=hashlib.sha256(raw).hexdigest(), doc=merit._parse_json_object(raw)
        )

    def ledger(source: Any, key: str) -> Any:
        return dataclasses.replace(source, mtime=NOW - timedelta(hours=ages[key]))

    evidence = dataclasses.replace(
        base,
        dossier=manifest(base.dossier, "dossier"),
        stage4=manifest(base.stage4, "stage4"),
        reconciliation=manifest(base.reconciliation, "receipt"),
        paper_ledgers=tuple(ledger(source, "paper") for source in base.paper_ledgers),
        live_ledger=ledger(base.live_ledger, "live"),
    )
    return merit._evaluate(evidence, SMALL, NOW, NO_HISTORY)


def _check_mutations(mutations: dict[str, Any]) -> None:
    plan = _base_plan()
    # Knobs that rebuild rows go first, then the one that rewrites fills, then the rest.
    for name in sorted(mutations, key=lambda knob: (_KNOB_ORDER.get(knob, 2), knob)):
        _apply_knob(plan, name, mutations[name])
    broken: set[str] = set()
    for name, value in mutations.items():
        broken |= _KNOBS[name][1](value, plan)
    record = _record_for(plan)
    # Merit True implies every criterion is a pass and reasons is empty (and the converse), the
    # record is plain JSON, and leg_has_merit agrees with it.
    _assert_record_invariants(record)
    for leg in merit.LEGS:
        entry = record["legs"][leg]
        if entry["merit"] is True:
            assert entry["reasons"] == []
            assert set(_statuses(record, leg).values()) == {"pass"}
        # Two-sided oracle: a mutant that must break a leg does, and nothing else is broken.
        assert entry["merit"] is (leg not in broken), (mutations, entry["reasons"])


def test_in_memory_record_equals_the_record_read_from_disk(tmp_path: Path) -> None:
    """The shortcut the sweep takes for manifest-only plans gives the record the files give.

    Only the file mtimes of the JSON manifests differ (they are informational: a manifest is
    dated by its ``generated_at_utc``), and with them the hash of the whole record body.
    """
    same = ("computed_at", "valid_until", "thresholds", "inputs_hash", "prior_state_sha256", "legs")
    cases: list[dict[str, Any]] = [
        {},
        {"long_dsr": ("falsification_2x_8bps", 0.5)},
        {"age": ("stage4", 36.1)},
        {"age": ("paper", 36.1), "receipt": ("status", None)},
    ]
    for mutations in cases:
        plan = _base_plan()
        for name, value in mutations.items():
            _apply_knob(plan, name, value)
        in_memory, from_disk = _record_for(plan), _compute(tmp_path, plan)
        assert {key: in_memory[key] for key in same} == {key: from_disk[key] for key in same}


@pytest.mark.parametrize("name", sorted(_KNOBS))
def test_every_single_knob_value_is_graded_as_expected(name: str) -> None:
    """Every value of every knob, one at a time and in a fixed order: nothing is left to which
    examples a random search happens to draw."""
    values = _KNOBS[name][0]
    assert values
    for value in values:
        _check_mutations({name: value})


@st.composite
def _mutations(draw: st.DrawFn) -> dict[str, Any]:
    names = draw(st.lists(st.sampled_from(sorted(_KNOBS)), max_size=3, unique=True))
    drawn: dict[str, Any] = {}
    for name in names:
        values, _, strategy = _KNOBS[name]
        drawn[name] = draw(st.sampled_from(values) if strategy is None else strategy)
    return drawn


@settings(max_examples=60, deadline=None, derandomize=True)
@example(mutations={})
@example(mutations={"long_dsr": ("falsification_2x_8bps", float("nan"))})
@example(mutations={"long_pbo": ("empirical_pbo", True)})
@example(mutations={"long_dsr": ("falsification_1x_4bps", True)})  # True is not a DSR of 1.0
@example(mutations={"long_pbo": ("falsification_calibrated_liquid", False)})  # nor False PBO 0
@example(mutations={"long_gate": ("3_cpcv_oos_positive", 1)})
@example(mutations={"receipt": ("unreconciled_delta_count", False)})
@example(mutations={"long_shortfall": (Decimal("8.000"), Decimal("8.000"))})
@example(mutations={"long_shortfall": (Decimal("3.000"), Decimal("8.001"))})
@example(mutations={"age": ("paper", 36.1), "live_shorts": 4})
@example(mutations={"long_rows": (12, 2, 3), "long_shortfall": (Decimal("0"), Decimal("0"))})
@example(mutations={"long_rows": (12, 4, 1), "fuse_trip": "MARKET_DRAWDOWN"})
@example(mutations={"long_one_regime": None, "long_extra_row": "OPEN"})
@example(mutations={"fuse_trip": "DEFECT_STALE_QUOTE", "long_extra_row": "ENTRY_CANCELED"})
@example(mutations={"fuse_gap": "ALL_DAYS", "ledger_copy": "SAME_FILE"})
@given(mutations=_mutations())
def test_property_merit_implies_every_criterion_passed(mutations: dict[str, Any]) -> None:
    _check_mutations(mutations)


# --- Today's real evidence shape ------------------------------------------------------------------


def _discarded_leg(pbo: float) -> dict[str, Any]:
    """A holdout leg block with every key the study writes, at DISCARD: only gate 1 passes."""
    gates = dict.fromkeys(merit.DOSSIER_GATES, False)
    gates["1_hftbacktest_l1_markout"] = True
    falsification = {
        "n_trades": 40,
        "worst_trade_pct": -1.9,
        "mean_bps": -6.0,
        "cpcv_splits": 28,
        "cpcv_oos_mean_bps": -5.0,
        "dsr": 0.0,
        "min_trl": None,
        "pbo": pbo,
        "hac_t": -0.8,
        "spa_p": 0.9,
        "spa_p_upper": 0.95,
        "promoted": False,
        "verdict": "DISCARD",
    }
    return {
        "n_trials_total": 300,
        "hlz_t_hurdle": 4.3,
        "empirical_pbo": pbo,
        "gates_passed_count": 1,
        "gates": gates,
        "l1_markout": {
            "data_source": "SYNTHETIC_L1",
            "queue_model": "SYNTHETIC",
            "n_slices_evaluated": 12,
            "passive_orders_submitted": 40,
            "passive_filled": 30,
            "fill_rate": 0.75,
            "mean_markout_bps": -1.5,
            "median_markout_bps": -0.5,
            "min_required_fills": 16,
            "markout_floor_bps": -20.0,
            "gate_1_passed": True,
        },
        "falsification_1x_4bps": dict(falsification),
        "falsification_calibrated_liquid": dict(falsification),
        "falsification_2x_8bps": dict(falsification),
        "promoted": False,
        "verdict": "DISCARD",
    }


def _todays_dossier() -> dict[str, Any]:
    """Every top-level key of the dossier on disk today; LONG, SHORT and COMBINED at DISCARD."""
    return {
        "schema_version": merit.DOSSIER_SCHEMA,
        "generated_at_utc": None,
        "hypothesis_id": "H109_MULTIFEED_ADR0028_L1_CONFORMAL_REGIME_FALSIFICATION",
        "prewall_cutoff_iso": "2026-10-15",
        "paper_only": True,
        "live_execution_enabled": False,
        "dry_run": False,
        "risk_kernel_sha256": "0" * 64,
        "preregistration": {"path": "synthetic", "prereg_sha256": "0" * 64},
        "trial_accounting": {"cumulative_trials_total": 300, "hlz_t_hurdle": 4.3},
        "tca_cost_anchor": {"manifest_loaded": True, "liquid_ge10_within_2x_envelope": True},
        "sandbox_live_node_parity": {"environment": "SANDBOX", "risk_engine_bypass": False},
        "adr0028_primitives": {},
        "temporal_splits": {"purge_gap_trading_days": 5},
        "tuning": {
            "selected_arm_id": "ARM_SYNTHETIC",
            "empirical_pbo_long": 1.0,
            "empirical_pbo_short": 0.1667,
            "empirical_pbo_combined": 0.5,
        },
        "l1_queue_markout": {"status": "EVALUATED_REAL_PUBLISHED_ORB_L1"},
        "holdout_nautilus_stats": {"LONG": {"fills_total": 40}, "SHORT": {"fills_total": 40}},
        "holdout_six_gate": {
            "LONG": _discarded_leg(1.0),
            "SHORT": _discarded_leg(0.1667),
            "COMBINED": _discarded_leg(0.5),
        },
        "verdict": {
            "cross_leg_promoted": False,
            "overall_verdict": "DISCARD",
            "long_verdict": "DISCARD",
            "short_verdict": "DISCARD",
            "combined_verdict": "DISCARD",
            "rule": "PROMOTE only if BOTH LONG and SHORT independently pass 6/6 gates",
        },
        "mlflow": {"enabled": True, "status": "LOGGED"},
        "xerj": {"enabled": True, "status": "INDEXED"},
    }


def test_todays_real_evidence_shape_yields_no_merit(tmp_path: Path) -> None:
    """Both legs at DISCARD, ledgers without the tier columns, no receipt, no live ledger.

    The expected reasons are the ones the module gave on the real manifests on 2026-11-27
    (read-only run; the real ledgers each name one broker account and one lane, so none of the
    identity or duplication codes appears).
    """
    plan = _base_plan()
    plan["dossier"] = _todays_dossier()
    # Ledgers as they are on disk today: none of the tier columns, fills keyed by broker order
    # only, a fuse table without a cause and without a trip. Origins as the paper farm writes
    # them.
    rows: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    for index, (leg, origin) in enumerate(
        [
            ("LONG", "TREND_ALIGNED_LONG"),
            ("LONG", "TREND_ALIGNED_LONG_AUTO_REVERSE"),
            ("LONG", "TREND_ALIGNED_LONG_REENTRY"),
            ("SHORT", "DIRECT_SHORT_ETB"),
            ("SHORT", "DIRECT_SHORT_ETB_AUTO_REVERSE"),
            ("SHORT", "DIRECT_SHORT_ETB_REENTRY"),
            ("LONG", "INVERSE_HEDGE_SOXS"),
            ("LONG", "INVERSE_HEDGE_SOXS_REENTRY"),
            ("LONG", "INVERSE_HEDGE_SQQQ"),
            ("LONG", "TV_HALF_AUTO_CONFIRMED"),
            ("LONG", "ADOPTED_EXTERNAL_POSITION"),
        ]
    ):
        leg_rows, leg_fills = _round_trips(leg, 6, origin=origin, prefix=f"today-{index}")
        rows += leg_rows
        fills += leg_fills
    plan["paper"] = {
        "entries": rows,
        "fills": fills,
        "fuse": [("2026-12-11", False, None), ("2026-12-14", False, None)],
        "tier_columns": False,
        "binding_column": False,
        "fill_link_column": False,
        "trip_cause_column": False,
    }
    plan["receipt"] = None
    plan["live"] = None
    record = _compute(tmp_path, plan, thresholds=None)
    _assert_record_invariants(record, thresholds=None)

    recording_gaps = [
        "ENTRY_AUTHORITY_EVIDENCE_MISSING",
        "DOSSIER_LEG_BINDING_MISSING",
        "CLOSE_CAUSE_EVIDENCE_MISSING",
        "REGIME_EVIDENCE_MISSING",
        "SHORTFALL_EVIDENCE_MISSING",
        "RECONCILIATION_RECEIPT_MISSING",
    ]
    assert record["legs"]["LONG"]["reasons"] == [
        "DOSSIER_NOT_PASSING",
        "DSR_BELOW_MIN",
        "PBO_ABOVE_MAX",
        *recording_gaps,
    ]
    assert record["legs"]["SHORT"]["reasons"] == [
        "DOSSIER_NOT_PASSING",
        "DSR_BELOW_MIN",
        *recording_gaps,
        "LIVE_LEDGER_MISSING",
        "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE",
    ]
    assert _statuses(record, "LONG")["dossier_pbo"] == "fail"
    assert _statuses(record, "SHORT")["dossier_pbo"] == "pass"
    assert _statuses(record, "SHORT")["short_live_confirmed"] == "missing"
    for leg in merit.LEGS:
        entry = record["legs"][leg]
        assert entry["merit"] is False
        assert "DOSSIER_NOT_PASSING" in entry["reasons"]
        assert leg_has_merit(record, leg, now_utc=NOW) is False
        statuses = _statuses(record, leg)
        # The dossier is real evidence that fails; the gaps are missing, not failed.
        assert statuses["dossier_passing"] == "fail"
        assert statuses["dossier_dsr"] == "fail"
        assert entry["criteria"]["dossier_passing"]["observed"] == {
            "promoted": False,
            "verdict": "DISCARD",
            "gates_passed": 1,
        }
        # What the harness does record today passes: the drills, and a fuse that has rows for
        # each lane and never tripped. No round trip can be counted yet, so no day is uncovered.
        assert statuses["stage4_drills"] == "pass"
        assert statuses["breaker_trips"] == "pass"
        assert entry["criteria"]["breaker_trips"]["observed"] == {
            "fuse_sessions": 6,
            "tripped_sessions": 0,
            "defect_trips": 0,
            "sessions_without_fuse_row": 0,
        }
        assert statuses["demotion_latch"] == "pass"
        for name in (
            "dossier_leg_binding",
            "paper_round_trips",
            "paper_sessions",
            "paper_regimes",
            "paper_shortfall",
            "paper_supervised_exits",
            "reconciliation_receipt",
        ):
            assert statuses[name] == "missing", name
        assert entry["capital_step"]["max_qty"] == 1
    assert record["legs"]["LONG"]["unattributed_ledger_rows"] == 30
    # Whatever the live evidence says, and with no state at all, nothing is promoted.
    for live in (QUIET, None):
        state = next_promotion_state(NO_HISTORY, record, live_evidence=live, now_utc=NOW)
        assert all(state["legs"][leg]["status"] == "PAPER" for leg in merit.LEGS)
        assert not any(state["legs"][leg]["live_entry_eligible"] for leg in merit.LEGS)
        for leg in merit.LEGS:
            assert merit.live_entry_leg_merit(state, record, leg, now_utc=NOW) is False
    found = {item["name"]: item["found"] for item in record["inputs"]}
    assert found[merit.RECONCILIATION_FILE] is False and found[merit.LIVE_LEDGER] is False
    assert found[merit.DOSSIER_FILE] is True and found[merit.STAGE4_FILE] is True


def test_demoted_short_leg_counts_only_rows_decided_after_its_demotion(tmp_path: Path) -> None:
    """The evidence floor is per leg, and for SHORT it covers the live-confirmed rows as well."""
    then = merit._read_evidence(_write_plan(tmp_path / "then", _base_plan()))
    _, promoted = _cycle(then, NO_HISTORY, QUIET, NOW)
    demoted_at = NOW + timedelta(minutes=30)
    short_failing = _with_dossier(then, long_pass=True, short_pass=False)
    _, demoted = _cycle(short_failing, promoted, QUIET, demoted_at)
    assert demoted["legs"]["SHORT"]["status"] == "DEMOTED"
    assert demoted["legs"]["SHORT"]["demotion_reasons"][:2] == ["MERIT_LOST", "DOSSIER_NOT_PASSING"]
    assert demoted["legs"]["LONG"]["status"] == "PROMOTED"

    # Wednesday Dec 23, five completed sessions later. ``fresh`` has eight paper round trips per
    # leg and two confirmed live shorts decided after the demotion, next to the older rows.
    moment = datetime(2026, 12, 23, 15, 0, tzinfo=UTC)
    fresh = _plan_with_history(moment)
    stale_live = copy.deepcopy(fresh)  # the confirmed live shorts are the ones from before it
    for key in ("entries", "fills"):
        stale_live["live"][key] = _base_plan()["live"][key]
    stale_paper = copy.deepcopy(fresh)  # the SHORT paper rows are the ones from before it
    for key, column in (("entries", "cid"), ("fills", "entry_client_order_id")):
        stale_paper["paper"][key] = [
            row for row in stale_paper["paper"][key] if not row[column].startswith("rt-SHORT-")
        ]

    def cycle(plan: dict[str, Any], state: dict[str, Any]) -> tuple[dict[str, Any], Any]:
        record = _compute(tmp_path / "later", plan, now=moment, prior_state=state)
        _assert_record_invariants(record, now=moment)
        return record, next_promotion_state(
            state, record, live_evidence=QUIET, now_utc=moment, thresholds=SMALL
        )

    floor = demoted_at.isoformat()
    # Confirmed live shorts from before the demotion: for the demoted SHORT leg there are none.
    record, state = cycle(stale_live, demoted)
    short = record["legs"]["SHORT"]
    assert short["evidence_floor"] == floor and record["legs"]["LONG"]["evidence_floor"] is None
    assert short["criteria"]["paper_round_trips"]["observed"] == 8
    assert record["legs"]["LONG"]["criteria"]["paper_round_trips"]["observed"] == 16
    confirmed = short["criteria"]["short_live_confirmed"]
    assert confirmed["status"] == "fail"
    assert confirmed["observed"] == {"round_trips": 0, "inside_shortfall_bound": 0}
    assert short["reasons"] == ["SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE"]
    assert state["legs"]["SHORT"]["status"] == "DEMOTED"
    assert state["legs"]["SHORT"]["cooloff"]["released"] is True  # cool-off served; no merit
    assert state["legs"]["SHORT"]["hold_reasons"] == [
        "NO_CURRENT_MERIT",
        "SHORT_LEG_NEEDS_LIVE_CONFIRMED_EVIDENCE",
    ]
    assert state["legs"]["LONG"]["status"] == "PROMOTED"
    # The same files for a SHORT leg that was never demoted: the rows count.
    record, state = cycle(stale_live, promoted)
    assert record["legs"]["SHORT"]["merit"] is True
    assert record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]["observed"] == {
        "round_trips": 2,
        "inside_shortfall_bound": 2,
    }
    assert record["legs"]["SHORT"]["criteria"]["paper_round_trips"]["observed"] == 16

    # Paper shorts from before the demotion only: nothing is counted for the SHORT leg.
    record, state = cycle(stale_paper, demoted)
    assert record["legs"]["SHORT"]["reasons"] == _NOTHING_COUNTED
    assert record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]["status"] == "pass"
    assert state["legs"]["SHORT"]["status"] == "DEMOTED"

    # Paper and confirmed live shorts decided after the demotion: released, and it stays so.
    record, released = cycle(fresh, demoted)
    assert record["legs"]["SHORT"]["merit"] is True
    assert record["legs"]["SHORT"]["criteria"]["short_live_confirmed"]["observed"] == {
        "round_trips": 2,
        "inside_shortfall_bound": 2,
    }
    entry = released["legs"]["SHORT"]
    assert (entry["status"], entry["transition"]) == ("PROMOTED", "PROMOTED")
    assert entry["capital_step"]["index"] == 0 and entry["demoted_at"] == floor
    record, again = cycle(fresh, released)
    assert record["legs"]["SHORT"]["evidence_floor"] == floor
    assert (again["legs"]["SHORT"]["status"], again["legs"]["SHORT"]["transition"]) == (
        "PROMOTED",
        "NONE",
    )
    assert merit.live_entry_leg_merit(again, record, "SHORT", now_utc=moment, thresholds=SMALL)

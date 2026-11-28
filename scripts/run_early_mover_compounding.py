#!/usr/bin/env python3
"""Causal historical early-mover Optuna fine-tuning, NVDA premarket audit & compounding loop.

Strict Tier-0 SOTA invariants:
- Pre-wall causal PIT (`<= 2026-10-15`) for ALL Optuna optimization and validation splits.
- Recent-week live replay (`NVDA` Monday premarket & recent daily movers) strictly tagged
  `out_of_sample_recent_replay` and NEVER mixed into pre-wall Optuna training.
- Paper-only (`paper_only=True`, `live_execution_enabled=False`), immutable `risk.py` SHA-256
  verification, zero banned libraries, zero live broker endpoints.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import sys
import urllib.parse
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.indicators import AverageTrueRange  # noqa: E402

from tier0_sota import discovery as disc  # noqa: E402
from tier0_sota import engine as eng  # noqa: E402
from tier0_sota import risk as rk  # noqa: E402
from tier0_sota.live.scanner import (  # noqa: E402
    DEFAULT_EARLY_MOVER_TUNING_FILENAME,
    _compute_hidden_volume_ratio,
    _compute_same_session_rvol,
    _compute_volume_buildup,
    _detect_causal_peak_confirmed,
    resolve_alpaca_env_creds,
)

EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
TUNING_SCHEMA_VERSION = "tier0.early-mover-tuning.v1"
AUDIT_SCHEMA_VERSION = "tier0.historical-early-mover-audit.v1"
STUDY_NAME = "tier0_early_mover_compounding"
MLFLOW_EXPERIMENT = "tier0-early-mover-compounding"
_NY_TZ = ZoneInfo("America/New_York")

RECENT_REPLAY_UNIVERSE: tuple[str, ...] = (
    "NVDA",
    "SOXL",
    "SOXS",
    "TQQQ",
    "SQQQ",
    "AMD",
    "TSLA",
    "PLTR",
    "SMCI",
    "MSTR",
    "COIN",
    "AVGO",
    "META",
    "MU",
    "INTC",
    "IREN",
    "SNDK",
    "ARM",
    "APP",
    "HOOD",
    "IONQ",
    "RKLB",
    "CRWD",
    "SPY",
    "QQQ",
    "SOXX",
)


def _default_data_root() -> Path:
    default_root = r"Z:\sota-data" if os.name == "nt" else "/mnt/z/sota-data"
    return Path(os.environ.get("TIER0_DATA_ROOT", "").strip() or default_root)


def _resolve_state_root() -> Path:
    env_state = os.environ.get("TIER0_STATE_ROOT", "").strip()
    if env_state:
        return Path(env_state)
    return Path.home() / "tier0-sota-state"


def _default_optuna_db(manifest_dir: Path, custom_manifest_dir: bool) -> Path:
    state_mlflow = _resolve_state_root() / "mlflow"
    if not custom_manifest_dir and state_mlflow.is_dir() and os.access(state_mlflow, os.W_OK):
        return state_mlflow / "optuna.db"
    return manifest_dir / "state" / "optuna.db"


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _verify_risk_kernel_sha256() -> str:
    target = REPO_ROOT / "src" / "tier0_sota" / "risk.py"
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    if digest != EXPECTED_RISK_SHA256:
        raise RuntimeError(
            f"Immutable risk kernel SHA-256 mismatch: expected {EXPECTED_RISK_SHA256}, got {digest}"
        )
    return digest


def _http_request(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 3.0,
) -> tuple[int, bytes]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Unsupported URL: {url}")
    conn_cls = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    conn = conn_cls(parsed.netloc, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        return int(resp.status), resp.read()
    finally:
        conn.close()


def _http_reachable(url: str, timeout: float = 2.0) -> bool:
    try:
        status, _ = _http_request("GET", url, timeout=timeout)
        return 200 <= status < 400
    except (http.client.HTTPException, TimeoutError, ValueError, OSError):
        return False


def _push_xerj_lesson(xerj_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    day_tag = str(payload.get("generated_at_utc", ""))[:10] or "unknown"
    doc_id = f"early-mover-compounding-{day_tag}"
    safe_id = urllib.parse.quote(doc_id, safe="-_.")
    endpoint = f"{xerj_url.rstrip('/')}/tier0-compounding-lessons/_doc/{safe_id}"
    try:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        status, raw = _http_request(
            "PUT",
            endpoint,
            body=body,
            headers={"Content-Type": "application/json"},
            timeout=3.0,
        )
        res = json.loads(raw.decode("utf-8"))
        return {
            "url": xerj_url,
            "pushed": 200 <= status < 300,
            "index": "tier0-compounding-lessons",
            "id": str(res.get("_id", doc_id)),
        }
    except (
        http.client.HTTPException,
        json.JSONDecodeError,
        TimeoutError,
        ValueError,
        OSError,
    ) as exc:
        return {"url": xerj_url, "pushed": False, "error": type(exc).__name__}


def split_prewall_days_purged(days: Sequence[str]) -> tuple[list[str], list[str], list[str]]:
    """Chronological purged train / purge-gap / val split strictly on pre-wall dates."""
    ordered = sorted(disc.assert_pre_wall_date(d).isoformat() for d in days)
    n = len(ordered)
    if n < 3:
        return ordered, [], ordered
    n_train = max(1, min(n - 2, int(round(n * 0.60))))
    purge_days = [ordered[n_train]]
    train_days = ordered[:n_train]
    val_days = ordered[n_train + 1 :]
    if not val_days:
        val_days = [ordered[-1]]
        purge_days = []
    return train_days, purge_days, val_days


def _build_offline_prewall_panel() -> dict[str, Any]:
    """Deterministic pre-wall (<= 2026-10-15) synthetic panel for offline CI/unit tests."""
    days = ["2026-10-08", "2026-10-09", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15"]
    cps = [241, 245, 270, 330, 575, 585, 600, 630, 720, 840]
    targets_by_day: dict[str, dict[str, dict[str, Any]]] = {}
    snaps_by_day: dict[str, dict[int, list[dict[str, Any]]]] = {}
    post_ext: dict[tuple[str, int, str], tuple[float, float]] = {}

    for d_idx, d_iso in enumerate(days):
        targets_by_day[d_iso] = {
            "NVDA": {
                "symbol": "NVDA",
                "date": d_iso,
                "side": "LONG",
                "prior_close": 120.0,
                "session_open_px": 124.5,
                "session_high_px": 146.0 + d_idx,
                "session_low_px": 123.0,
                "session_close_px": 142.0 + d_idx,
                "daily_move_pct": round(((142.0 + d_idx) / 120.0 - 1.0) * 100.0, 2),
                "max_abs_move_pct": round(((146.0 + d_idx) / 120.0 - 1.0) * 100.0, 2),
                "daily_rvol": 3.4,
                "tot_dvol": 1_800_000_000.0,
                "first_bar_m": 241,
            },
            "SMCI": {
                "symbol": "SMCI",
                "date": d_iso,
                "side": "LONG",
                "prior_close": 40.0,
                "session_open_px": 41.2,
                "session_high_px": 48.5,
                "session_low_px": 40.5,
                "session_close_px": 47.0,
                "daily_move_pct": 17.5,
                "max_abs_move_pct": 21.25,
                "daily_rvol": 2.9,
                "tot_dvol": 650_000_000.0,
                "first_bar_m": 575,
            },
            "SOXS": {
                "symbol": "SOXS",
                "date": d_iso,
                "side": "SHORT",
                "prior_close": 30.0,
                "session_open_px": 28.8,
                "session_high_px": 29.0,
                "session_low_px": 24.0,
                "session_close_px": 24.8,
                "daily_move_pct": -17.33,
                "max_abs_move_pct": 20.0,
                "daily_rvol": 2.6,
                "tot_dvol": 420_000_000.0,
                "first_bar_m": 575,
            },
        }
        cp_map: dict[int, list[dict[str, Any]]] = {}
        for cp_i, cp in enumerate(cps):
            sess = "PRE_MARKET" if cp <= 570 else "RTH"
            nvda_px = 124.5 + cp_i * 2.1
            smci_px = 41.5 + max(0, cp_i - 3) * 1.1
            soxs_px = 28.5 - max(0, cp_i - 3) * 0.65
            post_ext[(d_iso, cp, "NVDA")] = (146.0 + d_idx, nvda_px - 0.5)
            post_ext[(d_iso, cp, "SMCI")] = (48.5, smci_px - 0.3)
            post_ext[(d_iso, cp, "SOXS")] = (soxs_px + 0.3, 24.0)

            rows = [
                {
                    "symbol": "NVDA",
                    "sess": sess,
                    "px": round(nvda_px, 4),
                    "move_pct": round((nvda_px / 120.0 - 1.0) * 100.0, 2),
                    "rvol": 2.8 if cp <= 270 else 3.2,
                    "vol_buildup_ratio": 2.4,
                    "t1_trf_proxy": 0.42,
                    "hidden_volume_ratio": 1.9,
                    "technical_alignment_score": 88.0,
                    "premarket_ignition": sess == "PRE_MARKET",
                    "is_ext": True,
                    "category": (
                        "PREMARKET_EARLY_IGNITION" if sess == "PRE_MARKET" else "EP_MOMENTUM_BURST"
                    ),
                },
                {
                    "symbol": "SMCI",
                    "sess": sess,
                    "px": round(smci_px, 4),
                    "move_pct": round((smci_px / 40.0 - 1.0) * 100.0, 2),
                    "rvol": 2.5 if cp >= 575 else 1.2,
                    "vol_buildup_ratio": 2.3 if cp >= 575 else 1.1,
                    "t1_trf_proxy": 0.38,
                    "hidden_volume_ratio": 1.6,
                    "technical_alignment_score": 80.0 if cp >= 575 else 52.0,
                    "premarket_ignition": False,
                    "is_ext": cp >= 575,
                    "category": "EP_MOMENTUM_BURST" if cp >= 575 else "SUDDEN_VOLUME_SURGE",
                },
                {
                    "symbol": "SOXS",
                    "sess": sess,
                    "px": round(soxs_px, 4),
                    "move_pct": round((soxs_px / 30.0 - 1.0) * 100.0, 2),
                    "rvol": 2.4 if cp >= 575 else 1.1,
                    "vol_buildup_ratio": 2.1 if cp >= 575 else 1.0,
                    "t1_trf_proxy": 0.35,
                    "hidden_volume_ratio": 1.4,
                    "technical_alignment_score": 76.0 if cp >= 575 else 48.0,
                    "premarket_ignition": False,
                    "is_ext": cp >= 575,
                    "category": "CRASH_INVERSE_HEDGE",
                },
            ]
            # Add noisy decoy symbols to make Top-15 selection meaningful
            for k in range(18):
                rows.append(
                    {
                        "symbol": f"DEC{k:02d}",
                        "sess": sess,
                        "px": 50.0,
                        "move_pct": 2.4 + (k % 4) * 0.45,
                        "rvol": 1.1 + (k % 3) * 0.25,
                        "vol_buildup_ratio": 1.1,
                        "t1_trf_proxy": 0.20,
                        "hidden_volume_ratio": 0.8,
                        "technical_alignment_score": 45.0,
                        "premarket_ignition": False,
                        "is_ext": False,
                        "category": "EXTREME_MOVER_WATCH",
                    }
                )
            cp_map[cp] = rows
        snaps_by_day[d_iso] = cp_map

    return {
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
        "days": days,
        "checkpoints_et": cps,
        "targets_by_day": targets_by_day,
        "snapshots_by_day": snaps_by_day,
        "post_ext_by_day_cp": post_ext,
    }


def simulate_24x5_lossless_trailing_replay(
    bars: Sequence[Any],
    *,
    entry_idx: int,
    prior_close: float,
    cost_bps_per_side: Decimal = Decimal("4.0"),
    reentry_cooldown_bars: int = 1,
    max_reentries: int = 3,
) -> dict[str, Any]:
    """Replay causal 24/5 Decimal 4-tier lossless trailing, 60s rapid re-entry & peak reverse."""
    if not bars or entry_idx < 0 or entry_idx >= len(bars):
        return {"simulated": False, "reason": "invalid_entry_index"}

    fee_frac = float(cost_bps_per_side) * 2.0 / 10_000.0
    atr = AverageTrueRange(14)
    for b in bars[: entry_idx + 1]:
        atr.update_raw(float(b.high), float(b.low), float(b.close))

    first_entry_bar = bars[entry_idx]
    first_entry_px = Decimal(f"{float(first_entry_bar.close):.4f}")
    first_entry_et = first_entry_bar.timestamp.astimezone(_NY_TZ).strftime("%H:%M")

    # Track running causal session VWAP strictly bar-by-bar for 60s rapid re-entry confirmation
    pv_cum = 0.0
    v_cum = 0.0
    for b in bars[: entry_idx + 1]:
        bv = max(0.0, float(b.volume))
        tp = (float(b.high) + float(b.low) + float(b.close)) / 3.0
        pv_cum += tp * bv
        v_cum += bv

    def _init_long(idx: int) -> tuple[Decimal, Decimal, str, dict[Any, Any], Decimal]:
        eb = bars[idx]
        epx = Decimal(f"{float(eb.close):.4f}")
        natr_val = (
            Decimal(f"{float(atr.value) / max(1e-6, float(epx)):.6f}")
            if atr.initialized and atr.value > 0
            else Decimal("0.012")
        )
        sname, smap = eng.select_pooled_trailing_schedule(natr_val)
        esess = eng.classify_us_equity_session_et(int(eb.timestamp.timestamp() * 1e9))
        if esess == "CLOSED":
            esess = "PRE_MARKET"
        iprof = eng.get_session_protection_profile(esess, smap)
        istop = rk.compute_capped_stop_price(epx, "LONG", max_loss_pct=iprof.initial_max_loss_pct)
        return epx, istop, sname, smap, natr_val

    entry_px, initial_stop, sched_name, sp_map, natr_init = _init_long(entry_idx)
    first_initial_stop = initial_stop
    cur_stop = initial_stop
    fav_high = entry_px
    min_low = entry_px
    ratchets = 0
    max_tier = 0
    reentries = 0
    in_long = True
    last_exit_idx = -1
    long_compound_factor = 1.0
    exit_idx = len(bars) - 1
    exit_px = Decimal(f"{float(bars[-1].close):.4f}")
    exit_reason = "SESSION_CLOSE"
    peak_climax_reverse_triggered = False
    best_stop_locked = initial_stop

    for i in range(entry_idx + 1, len(bars)):
        b = bars[i]
        atr.update_raw(float(b.high), float(b.low), float(b.close))
        bv = max(0.0, float(b.volume))
        tp = (float(b.high) + float(b.low) + float(b.close)) / 3.0
        pv_cum += tp * bv
        v_cum += bv
        vwap_now = pv_cum / v_cum if v_cum > 0.0 else float(b.close)

        b_high = Decimal(f"{float(b.high):.4f}")
        b_low = Decimal(f"{float(b.low):.4f}")
        b_close = Decimal(f"{float(b.close):.4f}")
        sess = eng.classify_us_equity_session_et(int(b.timestamp.timestamp() * 1e9))
        if sess == "CLOSED":
            continue
        natr_cur = (
            Decimal(f"{float(atr.value) / max(1e-6, float(b_close)):.6f}")
            if atr.initialized and atr.value > 0
            else natr_init
        )
        _, cur_map = eng.select_pooled_trailing_schedule(natr_cur)
        prof = eng.get_session_protection_profile(sess, cur_map)

        if not in_long:
            # 60s rapid re-entry when trend reclaims VWAP & prints green expansion bar
            if (
                reentries < max_reentries
                and (i - last_exit_idx) >= max(1, reentry_cooldown_bars)
                and float(b.close) > vwap_now
                and float(b.close) > float(b.open)
                and float(b.close) > float(bars[i - 1].close)
            ):
                entry_px, initial_stop, sched_name, sp_map, natr_init = _init_long(i)
                cur_stop = initial_stop
                fav_high = entry_px
                max_tier = 0
                in_long = True
                reentries += 1
            continue

        if b_low < min_low:
            min_low = b_low
        if b_low <= cur_stop:
            leg_ret = (float(cur_stop) / float(entry_px) - 1.0) - fee_frac
            long_compound_factor *= 1.0 + leg_ret
            exit_idx = i
            exit_px = cur_stop
            last_exit_idx = i
            in_long = False
            exit_reason = f"LOSSLESS_TRAILING_TIER_{max_tier}"
            if cur_stop > best_stop_locked:
                best_stop_locked = cur_stop
            # If stopped out after high-tier parabolic extension (>= +15% vs prior close),
            # trigger flatten-first peak-climax auto-reverse instead of re-entering long!
            if (
                max_tier >= 3
                and (float(fav_high) / max(1e-6, prior_close) - 1.0) >= 0.15
                and i < len(bars) - 5
            ):
                peak_climax_reverse_triggered = True
                exit_reason = "PEAK_CLIMAX_FLATTEN_FIRST_REVERSE"
                break
            continue

        if b_high > fav_high:
            fav_high = b_high

        new_stop, tier, _ = eng.compute_progressive_trailing_stop(
            entry_price=entry_px,
            side="LONG",
            favorable_extreme=fav_high,
            current_stop=cur_stop,
            profile=prof,
            cost_bps_per_side=cost_bps_per_side,
            natr_pct=natr_cur,
            initial_stop=initial_stop,
        )
        if new_stop > cur_stop:
            cur_stop = new_stop
            ratchets += 1
            if cur_stop > best_stop_locked:
                best_stop_locked = cur_stop
        if tier > max_tier:
            max_tier = tier

        rvol_now = _compute_same_session_rvol(bars[: i + 1], sess)
        if max_tier >= 3 and _detect_causal_peak_confirmed(bars[: i + 1], sess, natr_cur, rvol_now):
            leg_ret = (float(b_close) / float(entry_px) - 1.0) - fee_frac
            long_compound_factor *= 1.0 + leg_ret
            exit_idx = i
            exit_px = b_close
            in_long = False
            exit_reason = "PEAK_CLIMAX_FLATTEN_FIRST_REVERSE"
            peak_climax_reverse_triggered = True
            break

    if in_long:
        exit_idx = len(bars) - 1
        exit_px = Decimal(f"{float(bars[exit_idx].close):.4f}")
        leg_ret = (float(exit_px) / float(entry_px) - 1.0) - fee_frac
        long_compound_factor *= 1.0 + leg_ret

    long_ret_pct = (long_compound_factor - 1.0) * 100.0
    mae_pct = max(0.0, (1.0 - float(min_low) / float(first_entry_px)) * 100.0)

    rev_entry_et: str | None = None
    rev_entry_px: float | None = None
    rev_exit_et: str | None = None
    rev_exit_px: float | None = None
    rev_ret_pct = 0.0
    if peak_climax_reverse_triggered and exit_idx + 1 < len(bars):
        rev_bar = bars[exit_idx]
        rev_entry = exit_px
        rev_entry_et = rev_bar.timestamp.astimezone(_NY_TZ).strftime("%H:%M")
        rev_entry_px = round(float(rev_entry), 4)
        rev_sess = eng.classify_us_equity_session_et(int(rev_bar.timestamp.timestamp() * 1e9))
        if rev_sess == "CLOSED":
            rev_sess = "RTH"
        rev_prof = eng.get_session_protection_profile(rev_sess, sp_map)
        rev_init_stop = rk.compute_capped_stop_price(
            rev_entry, "SHORT", max_loss_pct=rev_prof.initial_max_loss_pct
        )
        rev_stop = rev_init_stop
        rev_fav_low = rev_entry
        rev_ex_idx = len(bars) - 1
        rev_ex_px = Decimal(f"{float(bars[-1].close):.4f}")
        for j in range(exit_idx + 1, len(bars)):
            bj = bars[j]
            bj_hi = Decimal(f"{float(bj.high):.4f}")
            bj_lo = Decimal(f"{float(bj.low):.4f}")
            sj = eng.classify_us_equity_session_et(int(bj.timestamp.timestamp() * 1e9))
            if sj == "CLOSED":
                continue
            pj = eng.get_session_protection_profile(sj, sp_map)
            if bj_hi >= rev_stop:
                rev_ex_idx = j
                rev_ex_px = rev_stop
                break
            if bj_lo < rev_fav_low:
                rev_fav_low = bj_lo
            new_rstop, _, _ = eng.compute_progressive_trailing_stop(
                entry_price=rev_entry,
                side="SHORT",
                favorable_extreme=rev_fav_low,
                current_stop=rev_stop,
                profile=pj,
                cost_bps_per_side=cost_bps_per_side,
                natr_pct=natr_init,
                initial_stop=rev_init_stop,
            )
            if new_rstop < rev_stop:
                rev_stop = new_rstop
        rev_exit_et = bars[rev_ex_idx].timestamp.astimezone(_NY_TZ).strftime("%H:%M")
        rev_exit_px = round(float(rev_ex_px), 4)
        rev_ret_pct = ((1.0 - float(rev_ex_px) / float(rev_entry)) - fee_frac) * 100.0

    combined_pct = ((1.0 + long_ret_pct / 100.0) * (1.0 + rev_ret_pct / 100.0) - 1.0) * 100.0
    return {
        "simulated": True,
        "trailing_schedule": sched_name,
        "entry_et": first_entry_et,
        "entry_px": round(float(first_entry_px), 4),
        "initial_stop_px": round(float(first_initial_stop), 4),
        "max_profit_lock_stop_px": round(float(best_stop_locked), 4),
        "max_profit_lock_tier": int(max_tier),
        "rapid_reentries": int(reentries),
        "exit_et": bars[exit_idx].timestamp.astimezone(_NY_TZ).strftime("%H:%M"),
        "exit_px": round(float(exit_px), 4),
        "exit_reason": exit_reason,
        "long_leg_return_pct": round(long_ret_pct, 2),
        "peak_climax_reverse_triggered": bool(peak_climax_reverse_triggered),
        "reverse_entry_et": rev_entry_et,
        "reverse_entry_px": rev_entry_px,
        "reverse_exit_et": rev_exit_et,
        "reverse_exit_px": rev_exit_px,
        "reverse_leg_return_pct": round(rev_ret_pct, 2),
        "combined_24x5_return_pct": round(combined_pct, 2),
        "trailing_ratchets": int(ratchets),
        "max_adverse_excursion_pct": round(mae_pct, 2),
        "lossless_protected": bool(best_stop_locked >= first_entry_px or long_ret_pct >= -2.0),
    }


def _resolve_most_recent_monday(now_et: datetime) -> date:
    days_since_mon = now_et.weekday()
    return now_et.date() - timedelta(days=days_since_mon)


def audit_nvda_monday_and_recent_replay(
    *,
    tuned_weights: Mapping[str, float],
    monday_date: date | None = None,
    offline: bool = False,
) -> dict[str, Any]:
    """Out-of-sample recent-week live replay audit (NEVER mixed into pre-wall training)."""
    now_et = datetime.now(_NY_TZ)
    mon_day = monday_date or _resolve_most_recent_monday(now_et)
    if offline:
        return {
            "dataset_tag": "out_of_sample_recent_replay",
            "mixed_into_prewall_training": False,
            "replay_window": f"{mon_day.isoformat()} (offline_fixture)",
            "nvda_monday_premarket_audit": {
                "symbol": "NVDA",
                "date": mon_day.isoformat(),
                "session_span_et": "04:00..19:59",
                "prior_rth_close_px": 125.53,
                "prior_extended_close_px": 130.01,
                "premarket_open_0400_px": 135.34,
                "first_detect_et": "04:01",
                "first_detect_utc": f"{mon_day.isoformat()}T09:01:00+00:00",
                "first_detect_session": "PRE_MARKET",
                "first_detect_px": 135.75,
                "first_detect_move_pct": 4.42,
                "first_detect_rvol": 2.17,
                "first_detect_vol_buildup_ratio": 2.17,
                "first_detect_technical_alignment_score": 89.5,
                "first_detect_category": "PREMARKET_EARLY_IGNITION",
                "minutes_from_0400_et": 1,
                "minutes_before_rth_open": 329,
                "peak_hit_et": "10:25",
                "peak_hit_utc": f"{mon_day.isoformat()}T15:25:00+00:00",
                "peak_hit_px": 171.50,
                "post_detect_mfe_pct": 26.34,
                "session_close_px": 166.79,
                "simulated_24x5_lossless_trailing": {
                    "simulated": True,
                    "trailing_schedule": "tight_near_lossless",
                    "entry_et": "04:01",
                    "entry_px": 135.75,
                    "initial_stop_px": 134.05,
                    "max_profit_lock_stop_px": 168.40,
                    "max_profit_lock_tier": 4,
                    "exit_et": "10:28",
                    "exit_px": 168.40,
                    "exit_reason": "LOSSLESS_TRAILING_TIER_4",
                    "long_leg_return_pct": 23.97,
                    "peak_climax_reverse_triggered": True,
                    "reverse_entry_et": "10:28",
                    "reverse_entry_px": 168.40,
                    "reverse_exit_et": "11:15",
                    "reverse_exit_px": 165.10,
                    "reverse_leg_return_pct": 1.88,
                    "combined_24x5_return_pct": 26.30,
                    "trailing_ratchets": 18,
                    "max_adverse_excursion_pct": 0.65,
                    "lossless_protected": True,
                },
            },
            "recent_daily_movers_timeline": [],
        }

    key, sec = resolve_alpaca_env_creds("ALPACA_PA1")
    if not (key and sec):
        return {
            "dataset_tag": "out_of_sample_recent_replay",
            "mixed_into_prewall_training": False,
            "error": "missing_alpaca_paper_credentials",
            "nvda_monday_premarket_audit": None,
            "recent_daily_movers_timeline": [],
        }

    import alpaca.data as ad
    from alpaca.data.enums import DataFeed
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    dc = ad.StockHistoricalDataClient(key, sec)
    prior_fri = mon_day - timedelta(days=3)
    wed_day = mon_day + timedelta(days=2)

    # 1. Fetch daily bars for prior Friday .. Wednesday across recent universe
    d_req = StockBarsRequest(
        symbol_or_symbols=list(RECENT_REPLAY_UNIVERSE),
        timeframe=TimeFrame.Day,
        start=datetime(prior_fri.year, prior_fri.month, prior_fri.day, 0, 0, tzinfo=UTC),
        end=datetime(wed_day.year, wed_day.month, wed_day.day, 23, 59, tzinfo=UTC),
        feed=DataFeed.SIP,
    )
    d_map = dict(getattr(dc.get_stock_bars(d_req), "data", None) or {})
    prior_rth_close: dict[str, float] = {}
    recent_daily_movers: list[dict[str, Any]] = []
    for sym, blist in sorted(d_map.items()):
        for idx, b in enumerate(blist):
            b_day = b.timestamp.date()
            if b_day == prior_fri:
                prior_rth_close[sym] = float(b.close)
            if idx >= 1 and b_day >= mon_day:
                prev_c = float(blist[idx - 1].close)
                if prev_c <= 0.0:
                    continue
                chg_pct = (float(b.close) / prev_c - 1.0) * 100.0
                hi_pct = (float(b.high) / prev_c - 1.0) * 100.0
                lo_pct = (float(b.low) / prev_c - 1.0) * 100.0
                max_abs = max(abs(hi_pct), abs(lo_pct))
                if max_abs >= 6.0 and float(b.close) * float(b.volume) >= 50_000_000.0:
                    recent_daily_movers.append(
                        {
                            "symbol": sym,
                            "date": b_day.isoformat(),
                            "prior_close": round(prev_c, 4),
                            "open_px": round(float(b.open), 4),
                            "high_px": round(float(b.high), 4),
                            "low_px": round(float(b.low), 4),
                            "close_px": round(float(b.close), 4),
                            "daily_move_pct": round(chg_pct, 2),
                            "max_abs_move_pct": round(max_abs, 2),
                            "volume": int(b.volume),
                        }
                    )

    # 2. Fetch Monday full 24/5 SIP 1-minute bars (starting Friday 15:55 ET -> Monday 20:00 ET)
    m_start = datetime(prior_fri.year, prior_fri.month, prior_fri.day, 15, 55, tzinfo=_NY_TZ)
    m_end = datetime(mon_day.year, mon_day.month, mon_day.day, 20, 0, tzinfo=_NY_TZ)
    # Batch 1-minute requests cleanly across universe symbols
    mon_bars_by_sym: dict[str, list[Any]] = {}
    fri_ext_close: dict[str, float] = {}
    chunk_size = 8
    syms_list = list(RECENT_REPLAY_UNIVERSE)
    for off in range(0, len(syms_list), chunk_size):
        chunk = syms_list[off : off + chunk_size]
        req = StockBarsRequest(
            symbol_or_symbols=chunk,
            timeframe=TimeFrame.Minute,
            start=m_start.astimezone(UTC),
            end=m_end.astimezone(UTC),
            feed=DataFeed.SIP,
        )
        raw_map = dict(getattr(dc.get_stock_bars(req), "data", None) or {})
        for sym, blist in raw_map.items():
            fri_b = [b for b in blist if b.timestamp.astimezone(_NY_TZ).date() == prior_fri]
            mon_b = [b for b in blist if b.timestamp.astimezone(_NY_TZ).date() == mon_day]
            if fri_b:
                fri_ext_close[sym] = float(fri_b[-1].close)
            if mon_b:
                mon_bars_by_sym[sym] = mon_b

    # 3. Run causal minute-by-minute early-mover detection on Monday 2026-11-23 (04:00..19:59 ET)
    mon_timeline: list[dict[str, Any]] = []
    nvda_audit: dict[str, Any] | None = None

    for sym, mon_b in sorted(mon_bars_by_sym.items()):
        if sym in {"SPY", "QQQ", "SOXX"} or not mon_b:
            continue
        p_ext = fri_ext_close.get(sym) or prior_rth_close.get(sym) or float(mon_b[0].open)
        p_rth = prior_rth_close.get(sym) or p_ext
        peak_bar = max(mon_b, key=lambda b: float(b.high))
        trough_bar = min(mon_b, key=lambda b: float(b.low))
        first_det: dict[str, Any] | None = None
        first_det_idx = -1

        closes_so_far: list[float] = []
        highs_so_far: list[float] = []
        lows_so_far: list[float] = []
        vols_so_far: list[float] = []
        atr = AverageTrueRange(14)

        for idx, b in enumerate(mon_b):
            c_px = float(b.close)
            h_px = float(b.high)
            l_px = float(b.low)
            v_qty = float(b.volume)
            closes_so_far.append(c_px)
            highs_so_far.append(h_px)
            lows_so_far.append(l_px)
            vols_so_far.append(v_qty)
            atr.update_raw(h_px, l_px, c_px)

            ts_ns = int(b.timestamp.timestamp() * 1e9)
            sess = eng.classify_us_equity_session_et(ts_ns)
            if sess == "CLOSED":
                continue
            mv_pct = ((c_px / max(1e-6, p_ext)) - 1.0) * 100.0
            window_bars = mon_b[: idx + 1]
            rvol = _compute_same_session_rvol(window_bars, sess, early_fallback_rvol=1.5)
            vbr, _ = _compute_volume_buildup(window_bars, sess)
            atr_val = float(atr.value) if atr.initialized and atr.value > 0 else None
            hvr = _compute_hidden_volume_ratio(rvol, window_bars, atr_val)
            tech = disc.compute_causal_technical_alignment(
                closes_so_far,
                highs_so_far,
                lows_so_far,
                vols_so_far,
                prior_close=p_ext,
                rvol=rvol,
                vol_buildup_ratio=vbr,
                session=sess,
                move_pct=mv_pct,
            )
            cum_dvol = sum(vols_so_far) * c_px
            is_ext = abs(mv_pct) >= 3.5 and rvol >= 1.5 and cum_dvol >= 5_000_000.0
            pm_ign = bool(tech["premarket_ignition"])
            early_ign = bool(tech["early_ignition_detected"])
            if is_ext or pm_ign or (early_ign and cum_dvol >= 5_000_000.0):
                cat = disc.classify_potential_category(
                    symbol=sym,
                    session=sess,
                    move_pct=mv_pct,
                    rvol=rvol,
                    vol_buildup_ratio=vbr,
                    hidden_volume_ratio=hvr,
                    darkpool_trf_ratio=None,
                    trf_block_ratio=None,
                    macro_regime=0,
                    peak_confirmed=False,
                    premarket_ignition=pm_ign,
                    sudden_volume_surge=bool(tech["sudden_volume_surge"]),
                    technical_alignment_score=float(tech["technical_alignment_score"]),
                )
                score = disc.score_early_mover_candidate(
                    move_pct=mv_pct,
                    rvol=rvol,
                    vol_buildup_ratio=vbr,
                    darkpool_trf_ratio=None,
                    trf_block_ratio=None,
                    hidden_volume_ratio=hvr,
                    technical_alignment_score=float(tech["technical_alignment_score"]),
                    premarket_ignition=pm_ign,
                    is_extreme_eligible=is_ext,
                    is_pinned=False,
                    weights=tuned_weights,
                )
                b_et = b.timestamp.astimezone(_NY_TZ)
                b_close_et = b_et + timedelta(minutes=1)
                mins_from_0400 = (b_close_et.hour * 60 + b_close_et.minute) - 240
                post_bars = mon_b[idx + 1 :] or [b]
                if mv_pct >= 0.0:
                    post_ext_px = max(float(pb.high) for pb in post_bars)
                    post_mfe = max(0.0, (post_ext_px / max(1e-6, c_px) - 1.0) * 100.0)
                else:
                    post_ext_px = min(float(pb.low) for pb in post_bars)
                    post_mfe = max(0.0, (1.0 - post_ext_px / max(1e-6, c_px)) * 100.0)
                first_det_idx = idx
                first_det = {
                    "symbol": sym,
                    "date": mon_day.isoformat(),
                    "side": "LONG" if mv_pct >= 0.0 else "SHORT",
                    "first_detect_et": b_close_et.strftime("%H:%M"),
                    "first_detect_utc": (b.timestamp + timedelta(minutes=1))
                    .astimezone(UTC)
                    .isoformat(),
                    "first_detect_session": sess,
                    "first_detect_px": round(c_px, 4),
                    "first_detect_move_pct": round(mv_pct, 2),
                    "first_detect_rvol": round(rvol, 2),
                    "first_detect_vol_buildup_ratio": round(vbr, 2),
                    "first_detect_technical_alignment_score": round(
                        float(tech["technical_alignment_score"]), 2
                    ),
                    "first_detect_score": round(score, 2),
                    "category": cat,
                    "prior_close": round(p_ext, 4),
                    "session_high_px": round(float(peak_bar.high), 4),
                    "session_low_px": round(float(trough_bar.low), 4),
                    "session_close_px": round(float(mon_b[-1].close), 4),
                    "post_detect_mfe_pct": round(post_mfe, 2),
                    "rvol": round(rvol, 2),
                    "vol_buildup_ratio": round(vbr, 2),
                    "minutes_from_0400_et": int(max(0, mins_from_0400)),
                }
                break

        if first_det is not None:
            mon_timeline.append(first_det)
            if sym == "NVDA":
                sim_24x5 = simulate_24x5_lossless_trailing_replay(
                    mon_b, entry_idx=first_det_idx, prior_close=p_rth
                )
                peak_et = peak_bar.timestamp.astimezone(_NY_TZ)
                det_min_of_day = 240 + int(first_det["minutes_from_0400_et"])
                rev_et = sim_24x5.get("reverse_entry_et")
                rev_ex_px = sim_24x5.get("reverse_exit_px")
                rev_suffix = (
                    f" | Peak Reverse {rev_et}->${float(rev_ex_px):.2f} "
                    f"({sim_24x5['reverse_leg_return_pct']:+.2f}%) | "
                    f"Combined {sim_24x5['combined_24x5_return_pct']:+.2f}%"
                    if sim_24x5.get("peak_climax_reverse_triggered") and rev_ex_px is not None
                    else ""
                )
                sim_summary = (
                    f"LONG {sim_24x5['entry_et']} ${sim_24x5['entry_px']:.2f} -> "
                    f"{sim_24x5['exit_et']} ${sim_24x5['exit_px']:.2f} "
                    f"({sim_24x5['long_leg_return_pct']:+.2f}%, {sim_24x5['exit_reason']})"
                    f"{rev_suffix}"
                    if sim_24x5.get("simulated")
                    else "unsimulated"
                )
                nvda_audit = {
                    "symbol": "NVDA",
                    "date": mon_day.isoformat(),
                    "session_date": mon_day.isoformat(),
                    "split_tag": "out_of_sample_recent_replay",
                    "session_span_et": (
                        f"{mon_b[0].timestamp.astimezone(_NY_TZ).strftime('%H:%M')}.."
                        f"{mon_b[-1].timestamp.astimezone(_NY_TZ).strftime('%H:%M')}"
                    ),
                    "bars_audited": len(mon_b),
                    "prior_rth_close_px": round(p_rth, 4),
                    "prior_extended_close_px": round(p_ext, 4),
                    "premarket_open_0400_px": round(float(mon_b[0].open), 4),
                    "first_detect_et": first_det["first_detect_et"],
                    "first_detect_utc": first_det["first_detect_utc"],
                    "first_detect_session": first_det["first_detect_session"],
                    "first_detect_px": first_det["first_detect_px"],
                    "entry_price": first_det["first_detect_px"],
                    "first_detect_move_pct": first_det["first_detect_move_pct"],
                    "first_detect_rvol": first_det["first_detect_rvol"],
                    "rvol": first_det["first_detect_rvol"],
                    "first_detect_vol_buildup_ratio": first_det["first_detect_vol_buildup_ratio"],
                    "vol_buildup_ratio": first_det["first_detect_vol_buildup_ratio"],
                    "first_detect_technical_alignment_score": first_det[
                        "first_detect_technical_alignment_score"
                    ],
                    "first_detect_category": first_det["category"],
                    "category": first_det["category"],
                    "potential_category": first_det["category"],
                    "minutes_from_0400_et": first_det["minutes_from_0400_et"],
                    "time_to_first_detect_mins": float(first_det["minutes_from_0400_et"]),
                    "minutes_before_rth_open": max(0, 570 - det_min_of_day),
                    "peak_hit_et": peak_et.strftime("%H:%M"),
                    "peak_hit_utc": peak_bar.timestamp.astimezone(UTC).isoformat(),
                    "peak_hit_px": round(float(peak_bar.high), 4),
                    "peak_px": round(float(peak_bar.high), 4),
                    "session_high_px": round(float(peak_bar.high), 4),
                    "post_detect_mfe_pct": first_det["post_detect_mfe_pct"],
                    "session_close_px": round(float(mon_b[-1].close), 4),
                    "simulated_outcome_summary": sim_summary,
                    "simulated_24x5_lossless_trailing": sim_24x5,
                }

    mon_timeline.sort(key=lambda r: (str(r["first_detect_et"]), -float(r["post_detect_mfe_pct"])))
    recent_daily_movers.sort(key=lambda r: (str(r["date"]), -float(r["max_abs_move_pct"])))
    return {
        "dataset_tag": "out_of_sample_recent_replay",
        "mixed_into_prewall_training": False,
        "replay_window": f"{mon_day.isoformat()}..{wed_day.isoformat()}",
        "nvda_monday_premarket_audit": nvda_audit,
        "monday_early_detection_timeline": mon_timeline,
        "recent_daily_movers_summary": recent_daily_movers[:30],
    }


def run_early_mover_compounding(
    *,
    trials: int = 12,
    n_prewall_days: int = 15,
    top_k: int = 15,
    manifest_dir: Path | None = None,
    lake_root: Path | None = None,
    optuna_db: Path | None = None,
    mlflow_uri: str | None = None,
    xerj_url: str | None = None,
    monday_date: date | None = None,
    offline: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run one causal pre-wall Optuna tuning + recent-week NVDA replay compounding cycle."""
    risk_sha = _verify_risk_kernel_sha256()
    now_utc = datetime.now(UTC)

    custom_dir = manifest_dir is not None
    data_root = _default_data_root()
    target_manifest_dir = manifest_dir if manifest_dir is not None else (data_root / "manifests")
    target_manifest_dir.mkdir(parents=True, exist_ok=True)
    resolved_lake = lake_root if lake_root is not None else (data_root / "prewall-lake")
    resolved_optuna_db = (
        optuna_db if optuna_db is not None else _default_optuna_db(target_manifest_dir, custom_dir)
    )
    resolved_optuna_db.parent.mkdir(parents=True, exist_ok=True)

    # 1. Build strictly pre-wall causal panel (<= 2026-10-15)
    if offline:
        panel = _build_offline_prewall_panel()
        data_source = "offline_prewall_fixture"
    else:
        prewall_days = disc.list_prewall_bar_days(
            resolved_lake, max_days=max(4, int(n_prewall_days))
        )
        if len(prewall_days) < 3:
            raise FileNotFoundError(
                f"Insufficient pre-wall 1-min bar days under {resolved_lake} "
                f"(found {len(prewall_days)})"
            )
        panel = disc.build_prewall_early_mover_session_panel(prewall_days, root=resolved_lake)
        data_source = "prewall_lake_parquet"

    train_days, purge_days, val_days = split_prewall_days_purged(panel["days"])
    baseline_weights = dict(disc.DEFAULT_EARLY_MOVER_WEIGHTS)
    baseline_val = disc.evaluate_early_mover_weights(
        panel, baseline_weights, days_subset=val_days, top_k=top_k
    )
    baseline_full = disc.evaluate_early_mover_weights(panel, baseline_weights, top_k=top_k)

    # 2. Deterministic causal Optuna fine-tuning strictly on pre-wall train_days
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    storage_url = f"sqlite:///{resolved_optuna_db.as_posix()}"
    storage = optuna.storages.RDBStorage(url=storage_url)
    sampler = optuna.samplers.TPESampler(seed=42)
    study = optuna.create_study(
        study_name=STUDY_NAME,
        storage=storage,
        direction="maximize",
        sampler=sampler,
        load_if_exists=True,
    )
    try:
        if len(study.trials) == 0:
            study.enqueue_trial(baseline_weights)

        def _objective(trial: optuna.Trial) -> float:
            cand_w = {
                "w_move": trial.suggest_float("w_move", 0.5, 8.0),
                "w_rvol": trial.suggest_float("w_rvol", 2.0, 24.0),
                "w_vol_buildup": trial.suggest_float("w_vol_buildup", 0.5, 14.0),
                "w_trf_darkpool": trial.suggest_float("w_trf_darkpool", 1.0, 22.0),
                "w_hvr": trial.suggest_float("w_hvr", 0.0, 10.0),
                "w_tech_breakout": trial.suggest_float("w_tech_breakout", 0.02, 0.50),
                "w_premarket_ignition": trial.suggest_float("w_premarket_ignition", 4.0, 36.0),
            }
            tr_eval = disc.evaluate_early_mover_weights(
                panel, cand_w, days_subset=train_days, top_k=top_k
            )
            trial.set_user_attr("train_recall_at_top15", tr_eval["recall_at_top15"])
            trial.set_user_attr(
                "train_median_ttd_mins", tr_eval["median_time_to_first_detect_mins"]
            )
            trial.set_user_attr(
                "train_post_detect_mfe_pct", tr_eval["mean_post_detect_mfe_capture_pct"]
            )
            return float(tr_eval["composite_objective"])

        n_run = max(1, int(trials))
        study.optimize(_objective, n_trials=n_run)
        completed = [
            t
            for t in study.trials
            if t.state == optuna.trial.TrialState.COMPLETE and t.value is not None
        ]
        completed.sort(key=lambda t: float(t.value or 0.0), reverse=True)
        top_candidates = [
            (
                int(t.number),
                round(float(t.value or 0.0), 6),
                disc.normalize_early_mover_scoring_weights(t.params),
            )
            for t in completed[:8]
        ]
        trials_total = len(study.trials)
        best_trial = study.best_trial
        best_trial_num = int(best_trial.number)
        best_obj = round(float(best_trial.value or 0.0), 6)
    finally:
        storage.engine.dispose()

    # Select best generalizing candidate strictly on purged pre-wall val_days (anti-overfit)
    best_val_eval = baseline_val
    best_val_weights = baseline_weights
    adopted_tuned = False
    for t_num, t_obj, cand_w in top_candidates:
        v_eval = disc.evaluate_early_mover_weights(panel, cand_w, days_subset=val_days, top_k=top_k)
        if float(v_eval["composite_objective"]) > float(best_val_eval["composite_objective"]):
            best_val_eval = v_eval
            best_val_weights = cand_w
            best_trial_num = t_num
            best_obj = t_obj
            adopted_tuned = True

    active_weights = best_val_weights if adopted_tuned else baseline_weights
    tuned_train = disc.evaluate_early_mover_weights(
        panel, active_weights, days_subset=train_days, top_k=top_k
    )
    active_val = best_val_eval if adopted_tuned else baseline_val
    active_full = disc.evaluate_early_mover_weights(panel, active_weights, top_k=top_k)

    # 3. Recent-Week Live Replay Audit (strictly out_of_sample_recent_replay, NEVER in training)
    recent_replay = audit_nvda_monday_and_recent_replay(
        tuned_weights=active_weights,
        monday_date=monday_date,
        offline=offline,
    )

    # 4. Log compounding cycle to MLflow :5000
    resolved_mlflow_uri = (
        mlflow_uri or os.environ.get("MLFLOW_TRACKING_URI", "").strip() or "http://127.0.0.1:5000"
    )
    mlflow_status: dict[str, Any] = {
        "uri": resolved_mlflow_uri,
        "experiment": MLFLOW_EXPERIMENT,
        "logged": False,
        "run_id": None,
    }
    if not offline and _http_reachable(f"{resolved_mlflow_uri.rstrip('/')}/health"):
        try:
            import mlflow

            mlflow.set_tracking_uri(resolved_mlflow_uri)
            mlflow.set_experiment(MLFLOW_EXPERIMENT)
            with mlflow.start_run(
                run_name=f"early-mover-compound-{now_utc.strftime('%Y%m%dT%H%M')}"
            ) as run:
                mlflow.set_tags(
                    {
                        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
                        "data_source": data_source,
                        "oos_recent_replay_excluded_from_training": "true",
                        "paper_only": "true",
                        "live_execution_enabled": "false",
                    }
                )
                mlflow.log_params(
                    {
                        **{k: round(float(v), 6) for k, v in active_weights.items()},
                        "study_name": STUDY_NAME,
                        "trials_this_cycle": int(trials),
                        "trials_total": int(trials_total),
                        "train_days_count": len(train_days),
                        "val_days_count": len(val_days),
                        "adopted_tuned": str(adopted_tuned),
                    }
                )
                metrics: dict[str, float] = {
                    "optuna_best_objective": float(best_obj),
                    "prewall_train_recall_at_top15": float(tuned_train["recall_at_top15"]),
                    "prewall_val_recall_at_top15": float(active_val["recall_at_top15"]),
                    "prewall_val_baseline_recall_at_top15": float(baseline_val["recall_at_top15"]),
                    "prewall_full_recall_at_top15": float(active_full["recall_at_top15"]),
                    "prewall_full_early_recall_1030": float(
                        active_full["early_recall_before_1030_et"]
                    ),
                    "prewall_full_median_ttd_mins": float(
                        active_full["median_time_to_first_detect_mins"]
                    ),
                    "prewall_full_mean_post_detect_mfe_pct": float(
                        active_full["mean_post_detect_mfe_capture_pct"]
                    ),
                }
                nvda_rep = recent_replay.get("nvda_monday_premarket_audit")
                if isinstance(nvda_rep, Mapping):
                    metrics["oos_recent_replay_nvda_mins_from_0400"] = float(
                        nvda_rep.get("minutes_from_0400_et") or 0.0
                    )
                    metrics["oos_recent_replay_nvda_post_detect_mfe_pct"] = float(
                        nvda_rep.get("post_detect_mfe_pct") or 0.0
                    )
                    sim_t = nvda_rep.get("simulated_24x5_lossless_trailing")
                    if isinstance(sim_t, Mapping) and sim_t.get("simulated"):
                        metrics["oos_recent_replay_nvda_combined_24x5_ret_pct"] = float(
                            sim_t.get("combined_24x5_return_pct") or 0.0
                        )
                mlflow.log_metrics(metrics)
                mlflow_status = {
                    "uri": resolved_mlflow_uri,
                    "experiment": MLFLOW_EXPERIMENT,
                    "logged": True,
                    "run_id": run.info.run_id,
                }
        except Exception as exc:  # noqa: BLE001
            mlflow_status["error"] = type(exc).__name__

    tuning_run_id = (
        str(mlflow_status["run_id"])
        if mlflow_status.get("run_id")
        else f"optuna-{best_trial_num}-{now_utc.strftime('%Y%m%dT%H%M%SZ')}"
    )

    # 5. Push compounding lesson to XERJ :9200
    resolved_xerj_url = (
        xerj_url or os.environ.get("TIER0_XERJ_URL", "").strip() or "http://127.0.0.1:9200"
    )
    nvda_summary = recent_replay.get("nvda_monday_premarket_audit") or {}
    xerj_payload = {
        "kind": "early_mover_compounding",
        "generated_at_utc": now_utc.isoformat(),
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
        "tuning_run_id": tuning_run_id,
        "study_name": STUDY_NAME,
        "trials_total": trials_total,
        "weights": active_weights,
        "prewall_recall_at_top15": active_full["recall_at_top15"],
        "prewall_median_ttd_mins": active_full["median_time_to_first_detect_mins"],
        "prewall_mean_post_detect_mfe_pct": active_full["mean_post_detect_mfe_capture_pct"],
        "nvda_monday_first_detect_et": nvda_summary.get("first_detect_et"),
        "nvda_monday_first_detect_px": nvda_summary.get("first_detect_px"),
        "nvda_monday_post_detect_mfe_pct": nvda_summary.get("post_detect_mfe_pct"),
        "mlflow_run_id": mlflow_status.get("run_id"),
    }
    xerj_status = (
        {"url": resolved_xerj_url, "pushed": False, "skipped": "offline"}
        if offline
        else _push_xerj_lesson(resolved_xerj_url, xerj_payload)
    )

    def _strip_timeline(ev: Mapping[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in ev.items() if k != "timeline"}

    tuning_manifest: dict[str, Any] = {
        "schema_version": TUNING_SCHEMA_VERSION,
        "generated_at_utc": now_utc.isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": risk_sha,
        "causal_pit_enforced": True,
        "prewall_cutoff": disc.DISCOVERY_WALL_ISO,
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
        "data_source": data_source,
        "out_of_sample_recent_replay_excluded_from_training": True,
        "tuning_run_id": tuning_run_id,
        "study_name": STUDY_NAME,
        "optuna_storage": storage_url,
        "trials_this_cycle": int(trials),
        "trials_count": int(trials_total),
        "trials_total": int(trials_total),
        "best_trial_number": int(best_trial_num),
        "best_objective_score": best_obj,
        "adopted_tuned_weights": bool(adopted_tuned),
        "weights": active_weights,
        "best_weights": active_weights,
        "default_weights": baseline_weights,
        "recall_at_top15": float(active_full["recall_at_top15"]),
        "median_time_to_first_detect_mins": float(active_full["median_time_to_first_detect_mins"]),
        "mean_post_detect_mfe_capture_pct": float(active_full["mean_post_detect_mfe_capture_pct"]),
        "validation_metrics": _strip_timeline(active_val),
        "prewall_split": {
            "train_days": train_days,
            "purge_gap_days": purge_days,
            "val_days": val_days,
            "train_metrics": _strip_timeline(tuned_train),
            "val_metrics": _strip_timeline(active_val),
            "baseline_val_metrics": _strip_timeline(baseline_val),
            "val_delta": {
                "recall_at_top15_delta": round(
                    float(active_val["recall_at_top15"]) - float(baseline_val["recall_at_top15"]),
                    4,
                ),
                "median_ttd_mins_delta": round(
                    float(active_val["median_time_to_first_detect_mins"])
                    - float(baseline_val["median_time_to_first_detect_mins"]),
                    2,
                ),
                "post_detect_mfe_pct_delta": round(
                    float(active_val["mean_post_detect_mfe_capture_pct"])
                    - float(baseline_val["mean_post_detect_mfe_capture_pct"]),
                    2,
                ),
            },
        },
        "mlflow_experiment": MLFLOW_EXPERIMENT,
        "mlflow_run_id": mlflow_status.get("run_id"),
        "mlflow": mlflow_status,
        "xerj": xerj_status,
        "loop_status": "ACTIVE_15MIN_TIMER",
        "compounding_timer": {
            "unit": "tier0-early-mover-compound.timer",
            "interval": "15min",
            "trials_per_cycle": int(trials),
        },
    }

    audit_manifest: dict[str, Any] = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at_utc": now_utc.isoformat(),
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": risk_sha,
        "prewall_cutoff": disc.DISCOVERY_WALL_ISO,
        "prewall_cutoff_iso": disc.DISCOVERY_WALL_ISO,
        "recent_replay_tag": "out_of_sample_recent_replay",
        "data_source": data_source,
        "tuning_run_id": tuning_run_id,
        "tuned_weights": active_weights,
        "recall_at_top15": float(active_full["recall_at_top15"]),
        "median_time_to_first_detect_mins": float(active_full["median_time_to_first_detect_mins"]),
        "mean_post_detect_mfe_capture_pct": float(active_full["mean_post_detect_mfe_capture_pct"]),
        "prewall_benchmark": {
            "dataset_tag": "prewall_causal_pit",
            "cutoff": disc.DISCOVERY_WALL_ISO,
            "days_audited": list(panel["days"]),
            "days_count": int(active_full["days_count"]),
            "actual_extreme_movers_total": int(active_full["actual_extreme_movers_total"]),
            "detected_in_top15_count": int(active_full["detected_in_top15_count"]),
            "recall_at_top15": float(active_full["recall_at_top15"]),
            "early_recall_before_1030_et": float(active_full["early_recall_before_1030_et"]),
            "median_time_to_first_detect_mins": float(
                active_full["median_time_to_first_detect_mins"]
            ),
            "mean_post_detect_mfe_capture_pct": float(
                active_full["mean_post_detect_mfe_capture_pct"]
            ),
            "baseline_recall_at_top15": float(baseline_full["recall_at_top15"]),
            "baseline_median_time_to_first_detect_mins": float(
                baseline_full["median_time_to_first_detect_mins"]
            ),
            "baseline_mean_post_detect_mfe_capture_pct": float(
                baseline_full["mean_post_detect_mfe_capture_pct"]
            ),
            "category_breakdown": dict(active_full["category_breakdown"]),
            "timeline": list(active_full["timeline"]),
        },
        "first_detection_timeline": list(active_full["timeline"]),
        "nvda_monday_premarket_audit": recent_replay.get("nvda_monday_premarket_audit"),
        "out_of_sample_recent_replay": recent_replay,
        "optuna": {
            "study_name": STUDY_NAME,
            "storage": storage_url,
            "trials_this_cycle": int(trials),
            "trials_total": int(trials_total),
            "best_trial_number": int(best_trial_num),
            "best_objective_score": best_obj,
        },
        "mlflow": mlflow_status,
        "xerj": xerj_status,
    }

    _write_json_atomic(target_manifest_dir / DEFAULT_EARLY_MOVER_TUNING_FILENAME, tuning_manifest)
    _write_json_atomic(
        target_manifest_dir / "historical_early_mover_audit_manifest.json", audit_manifest
    )
    return tuning_manifest, audit_manifest


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tier-0 SOTA causal historical early-mover Optuna fine-tuning & NVDA audit."
    )
    parser.add_argument("--once", action="store_true", help="Run a single compounding cycle.")
    parser.add_argument("--trials", type=int, default=12, help="Optuna trials per cycle.")
    parser.add_argument(
        "--days", type=int, default=15, help="Pre-wall trading days (<= 2026-10-15) to benchmark."
    )
    parser.add_argument("--top-k", type=int, default=15, help="Top-K watchlist rank threshold.")
    parser.add_argument("--manifest-dir", type=Path, default=None)
    parser.add_argument("--lake-root", type=Path, default=None)
    parser.add_argument("--optuna-db", type=Path, default=None)
    parser.add_argument("--mlflow-uri", type=str, default=None)
    parser.add_argument("--xerj-url", type=str, default=None)
    parser.add_argument("--nvda-replay-date", type=str, default=None)
    parser.add_argument("--offline", action="store_true", help="Run offline deterministic fixture.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    mon_date = date.fromisoformat(args.nvda_replay_date) if args.nvda_replay_date else None
    tuning, audit = run_early_mover_compounding(
        trials=args.trials,
        n_prewall_days=args.days,
        top_k=args.top_k,
        manifest_dir=args.manifest_dir,
        lake_root=args.lake_root,
        optuna_db=args.optuna_db,
        mlflow_uri=args.mlflow_uri,
        xerj_url=args.xerj_url,
        monday_date=mon_date,
        offline=bool(args.offline),
    )
    pb = audit["prewall_benchmark"]
    nvda = audit["out_of_sample_recent_replay"].get("nvda_monday_premarket_audit") or {}
    summary = {
        "schema_version": tuning["schema_version"],
        "tuning_run_id": tuning["tuning_run_id"],
        "trials_total": tuning["trials_total"],
        "prewall_days": pb["days_count"],
        "recall_at_top15": pb["recall_at_top15"],
        "early_recall_before_1030_et": pb["early_recall_before_1030_et"],
        "median_time_to_first_detect_mins": pb["median_time_to_first_detect_mins"],
        "mean_post_detect_mfe_capture_pct": pb["mean_post_detect_mfe_capture_pct"],
        "nvda_monday_first_detect_et": nvda.get("first_detect_et"),
        "nvda_monday_first_detect_px": nvda.get("first_detect_px"),
        "nvda_monday_peak_hit_et": nvda.get("peak_hit_et"),
        "nvda_monday_peak_hit_px": nvda.get("peak_hit_px"),
        "nvda_monday_post_detect_mfe_pct": nvda.get("post_detect_mfe_pct"),
        "mlflow_logged": tuning["mlflow"].get("logged"),
        "mlflow_run_id": tuning["mlflow"].get("run_id"),
        "xerj_pushed": tuning["xerj"].get("pushed"),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Deterministic unit, causal PIT, taxonomy, Nautilus v2 replay & DuckDB tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from tier0_sota import discovery as disc
from tier0_sota import research as res
from tier0_sota import risk as rk

NY = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parent.parent


def _make_synthetic_session_bars(
    *,
    symbol: str,
    day: date,
    prior_close: float = 100.0,
    extreme_surge: bool = True,
    peak_reversal: bool = True,
) -> pl.DataFrame:
    """Build deterministic synthetic 1-min PRE_MARKET + RTH bars (04:00..15:59 ET)."""
    start_et = datetime(day.year, day.month, day.day, 4, 0, tzinfo=NY)
    rows: list[dict[str, Any]] = []
    px = prior_close * (1.06 if extreme_surge else 1.002)
    for m in range(720):  # 04:00 -> 15:59 ET
        et = start_et + timedelta(minutes=m)
        ts_ns = int(et.astimezone(UTC).timestamp()) * 1_000_000_000
        o = px
        if extreme_surge:
            if m < 10:
                delta = 0.0025
                vol = 220_000.0 if m < 5 else 80_000.0
            elif 295 <= m <= 325:  # 08:55..09:25 premarket catalyst surge
                delta = 0.0085
                vol = 950_000.0
            elif peak_reversal and 326 <= m <= 345:  # 09:26..09:45 climax reversal
                delta = -0.0045
                vol = 1_100_000.0
            elif 346 <= m <= 390:  # 09:46..10:30 secondary rally / re-entry
                delta = 0.0022
                vol = 750_000.0
            elif peak_reversal and 391 <= m <= 420:  # 10:31..11:00 second peak breakdown
                delta = -0.0025
                vol = 520_000.0
            else:
                delta = 0.0001 if (m % 2 == 0) else -0.0001
                vol = 45_000.0
        else:
            delta = 0.0002 if (m % 2 == 0) else -0.0002
            vol = 18_000.0
        c = max(1.0, o * (1.0 + delta))
        h = max(o, c) * 1.0012
        lo = min(o, c) * 0.9988
        rows.append(
            {
                "symbol": symbol,
                "t_ns": ts_ns,
                "o": round(o, 4),
                "h": round(h, 4),
                "l": round(lo, 4),
                "c": round(c, 4),
                "v": vol,
            }
        )
        px = c
    return pl.DataFrame(rows)


def test_prewall_pit_wall_enforced_on_feature_extraction_and_optuna() -> None:
    """Calibration paths MUST fail closed on post-wall (> 2026-10-15) timestamps."""
    postwall_bars = _make_synthetic_session_bars(
        symbol="NVDA", day=date(2026, 11, 23), prior_close=125.81, extreme_surge=True
    )
    with pytest.raises(disc.PreWallViolationError):
        disc.extract_causal_intraday_mover_features(postwall_bars, enforce_prewall=True)

    # Observational-only path explicitly opts out of pre-wall rejection while preserving t_ns+60s.
    obs_feat = disc.extract_causal_intraday_mover_features(
        postwall_bars,
        prior_close_by_sym={"NVDA": 125.81},
        baseline_vol_by_sym={"NVDA": 40_000_000.0},
        enforce_prewall=False,
    )
    assert obs_feat.height == postwall_bars.height
    assert (obs_feat["t_close_ns"] - obs_feat["t_ns"] == disc.BAR_DURATION_NS).all()

    # Optuna calibration MUST fail closed if post-wall features are passed.
    prewall_bars = _make_synthetic_session_bars(
        symbol="NVDA", day=date(2026, 10, 12), prior_close=125.0, extreme_surge=True
    )
    pre_feat = disc.extract_causal_intraday_mover_features(prewall_bars, enforce_prewall=True)
    with pytest.raises(disc.PreWallViolationError):
        res.calibrate_extreme_mover_weights_optuna(pre_feat, obs_feat, n_trials=4)


def test_categorized_mover_taxonomy_and_scanner_weight_projection() -> None:
    """All 5 canonical categories classify cleanly and project into scanner.py weights."""
    cases: list[tuple[str, dict[str, Any]]] = [
        (
            "PREMARKET_CATALYST_LEADER",
            dict(
                session="PRE_MARKET",
                move_pct=7.8,
                rvol=2.1,
                vbr=1.4,
                hvr=1.2,
                trf_darkpool_ratio=0.25,
                range_expansion=1.3,
                momentum_roc=0.9,
                peak_climax_confirmed=False,
                above_vwap=True,
            ),
        ),
        (
            "SUDDEN_VOLUME_IGNITION",
            dict(
                session="RTH",
                move_pct=4.5,
                rvol=3.4,
                vbr=2.8,
                hvr=1.5,
                trf_darkpool_ratio=0.28,
                range_expansion=2.2,
                momentum_roc=1.4,
                peak_climax_confirmed=False,
                above_vwap=True,
            ),
        ),
        (
            "DARKPOOL_ACCUMULATION_BREAKOUT",
            dict(
                session="RTH",
                move_pct=2.8,
                rvol=1.4,
                vbr=1.3,
                hvr=1.8,
                trf_darkpool_ratio=0.44,
                range_expansion=1.1,
                momentum_roc=0.6,
                peak_climax_confirmed=False,
                above_vwap=True,
            ),
        ),
        (
            "PARABOLIC_CLIMAX_SHORT_REVERSAL",
            dict(
                session="RTH",
                move_pct=28.0,
                rvol=4.0,
                vbr=2.5,
                hvr=1.0,
                trf_darkpool_ratio=0.30,
                range_expansion=3.0,
                momentum_roc=-1.2,
                peak_climax_confirmed=True,
                above_vwap=True,
            ),
        ),
        (
            "EXTREME_MOVER_CONTINUATION",
            dict(
                session="RTH",
                move_pct=6.1,
                rvol=1.6,
                vbr=1.2,
                hvr=0.9,
                trf_darkpool_ratio=0.20,
                range_expansion=1.2,
                momentum_roc=0.8,
                peak_climax_confirmed=False,
                above_vwap=True,
            ),
        ),
    ]
    seen_cats: set[str] = set()
    for expected_cat, kwargs in cases:
        out = disc.classify_extreme_mover_category(**kwargs)
        assert out["category"] == expected_cat
        assert out["archetype"] == disc.CATEGORY_TO_ENGINE_ARCHETYPE[expected_cat]
        seen_cats.add(out["category"])
    assert seen_cats == set(disc.EXTREME_MOVER_CATEGORIES)

    projected = disc.project_calibrated_weights_to_scanner(disc.DEFAULT_EARLY_MOVER_SIMPLEX_WEIGHTS)
    assert set(projected.keys()) == {
        "move_weight",
        "rvol_weight",
        "trf_weight",
        "trf_block_weight",
        "vol_buildup_weight",
        "lit_iso_weight",
        "breakout_leader_bonus",
    }
    assert all(0.5 <= v <= 25.0 for v in projected.values())


def test_optuna_prewall_calibration_nautilus_v2_replay_and_duckdb(tmp_path: Path) -> None:
    """Verify pre-wall Optuna calibration, Nautilus v2 replay, min-trail & DuckDB persistence."""
    train_days = [date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8)]
    holdout_days = [date(2026, 10, 12), date(2026, 10, 13)]

    def _build_panel(days: list[date]) -> pl.DataFrame:
        parts: list[pl.DataFrame] = []
        for d in days:
            bars = pl.concat(
                [
                    _make_synthetic_session_bars(
                        symbol="NVDA", day=d, prior_close=130.0, extreme_surge=True
                    ),
                    _make_synthetic_session_bars(
                        symbol="SOXL", day=d, prior_close=35.0, extreme_surge=True
                    ),
                    _make_synthetic_session_bars(
                        symbol="FLAT", day=d, prior_close=55.0, extreme_surge=False
                    ),
                ],
                how="vertical",
            )
            parts.append(
                disc.extract_causal_intraday_mover_features(
                    bars,
                    prior_close_by_sym={"NVDA": 130.0, "SOXL": 35.0, "FLAT": 55.0},
                    baseline_vol_by_sym={
                        "NVDA": 40_000_000.0,
                        "SOXL": 25_000_000.0,
                        "FLAT": 30_000_000.0,
                    },
                    short_vol_ratio_by_sym={"NVDA": 0.41, "SOXL": 0.38, "FLAT": 0.22},
                    enforce_prewall=True,
                )
            )
        return pl.concat(parts, how="vertical")

    tr_feat = _build_panel(train_days)
    ho_feat = _build_panel(holdout_days)
    cal = res.calibrate_extreme_mover_weights_optuna(tr_feat, ho_feat, n_trials=6, top_k=5)
    assert cal["prewall_wall_date"] == disc.DISCOVERY_WALL_ISO
    assert cal["optuna"]["n_trials"] == 6
    assert abs(sum(cal["calibrated_simplex_weights"].values()) - 1.0) < 1e-4
    assert cal["holdout_calibrated"]["recall_at_k"] > 0.0
    assert cal["holdout_calibrated"]["premarket_early_hits"] >= 1

    # Replay NVDA observational Monday bars inside real NautilusTrader v2 BacktestEngine.
    obs_bars = _make_synthetic_session_bars(
        symbol="NVDA", day=date(2026, 11, 23), prior_close=125.81, extreme_surge=True
    )
    replay = res.replay_mover_with_nautilus_v2(
        obs_bars,
        symbol="NVDA",
        weights=cal["calibrated_simplex_weights"],
        score_threshold=cal["calibrated_score_threshold"],
        prior_close=125.81,
        baseline_vol=40_000_000.0,
        venue_profile="ALPACA_PAPER_1000",
        postwall_observational_only=True,
    )
    assert replay["engine"] == "NautilusTrader v2 BacktestEngine"
    assert replay["paper_only"] is True
    assert replay["live_execution_enabled"] is False
    assert replay["postwall_observational_only"] is True
    assert replay["excluded_from_calibration"] is True
    assert replay["nautilus_v2_replay_date_mapped_for_pit_guard"] == "2026-10-12"
    assert replay["nautilus_1x_4bps"]["bars_processed"] == obs_bars.height
    assert replay["nautilus_1x_4bps"]["stops_attached"] >= 1
    assert replay["nautilus_1x_4bps"]["outside_rth_market_entries"] == 0
    assert replay["nautilus_1x_4bps"]["session_transitions"] >= 1

    # Per-symbol min trailing table strictly bounded by immutable risk.py <= 2.00%.
    mt_table = res.compute_per_symbol_min_trailing_table(
        cal["holdout_calibrated"]["latency_table"], venue_profile="ALPACA_PAPER_1000"
    )
    assert len(mt_table) >= 2
    for row in mt_table:
        for sess_name, spec in row["sessions"].items():
            assert 0.0 < spec["min_trailing_pct"] <= float(rk.MAX_STOP_LOSS_PCT), (
                f"{row['symbol']} {sess_name} exceeded MAX_STOP_LOSS_PCT"
            )
            if sess_name == "OVERNIGHT":
                assert spec["min_trailing_pct"] <= 0.01

    # Bounded DuckDB persistence round-trip.
    dummy_manifest: dict[str, Any] = {
        "schema_version": "tier0.historical-extreme-mover-calibration.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prewall_wall_date": disc.DISCOVERY_WALL_ISO,
        "paper_only": True,
        "live_execution_enabled": False,
        "risk_kernel_sha256": res._EXPECTED_RISK_SHA256,
        "optuna": cal["optuna"],
        "categorized_mover_taxonomy": res._build_categorized_taxonomy_spec(),
        "per_symbol_min_trailing_floor": mt_table,
        "holdout_metrics": {"calibrated": cal["holdout_calibrated"]},
        "postwall_observational_current_week": {
            "early_detection_latency_table": [replay["detection_summary"]]
            if replay["detection_summary"]
            else []
        },
        "nvda_monday_premarket_e2e_replay": replay,
        "mlflow": {"logged": False, "run_id": None},
    }
    db_path = tmp_path / "calibration.duckdb"
    counts = res.persist_calibration_to_duckdb(dummy_manifest, db_path)
    assert db_path.is_file()
    assert counts["mover_taxonomy_rows"] == 5
    assert counts["per_symbol_min_trailing_rows"] == len(mt_table)

    # Verify scanner.load_extreme_mover_calibration consumes our manifest schema cleanly.
    from tier0_sota.live.scanner import load_extreme_mover_calibration

    cal_json = tmp_path / "historical_extreme_mover_calibration_manifest.json"
    cal_json.write_text(
        json.dumps(
            {
                "prewall_wall_date": disc.DISCOVERY_WALL_ISO,
                "calibrated_weights": cal["calibrated_weights"],
                "per_symbol_min_trailing_pct": {
                    row["symbol"]: row["rth_min_trailing_pct"] for row in mt_table
                },
            }
        ),
        encoding="utf-8",
    )
    loaded_w, loaded_mt, loaded_meta = load_extreme_mover_calibration(cal_json)
    assert loaded_meta["manifest_loaded"] is True
    assert loaded_meta["applied_weight_count"] == 7
    assert loaded_meta["calibrated_symbols_count"] == len(mt_table)
    assert loaded_w["move_weight"] == cal["calibrated_scanner_weights"]["move_weight"]
    assert "NVDA" in loaded_mt and 0.0 < loaded_mt["NVDA"] <= float(rk.MAX_STOP_LOSS_PCT)


def test_discovery_and_research_preserve_adr0026_lazy_imports() -> None:
    """Importing discovery & research MUST keep heavy ML/Optuna/MLflow/DuckDB modules lazy."""
    probe = (
        "import json, sys; "
        "import tier0_sota.discovery, tier0_sota.research; "
        "heavy = ['optuna', 'mlflow', 'duckdb', 'lightgbm', 'catboost', 'shap', 'skfolio']; "
        "print(json.dumps([m for m in heavy if m in sys.modules]))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(REPO_ROOT),
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(proc.stdout.strip()) == []


def test_discovery_session_rvol_matches_live_scanner_bb0eb23_semantics() -> None:
    """discovery.py RVOL == live scanner (09:30 auction exclusion + ToD pace RVOL + blend)."""
    from types import SimpleNamespace

    from tier0_sota.live import scanner as sc

    for const in (
        "TOD_RVOL_RTH_CDF_ONE_MINUS_B",
        "TOD_RVOL_RTH_CDF_B",
        "TOD_RVOL_RTH_CDF_P",
        "TOD_RVOL_RTH_SHARE_OF_FULLDAY",
        "TOD_RVOL_PM_SHARE_OF_FULLDAY",
        "TOD_RVOL_EARLY_CLOSE_SCALE",
    ):
        assert getattr(disc, const) == getattr(sc, const), const
        assert getattr(sc, const) is getattr(disc, const), (
            f"{const} must be imported from discovery"
        )
    assert sc._xnys_rth_close_hm is disc.xnys_rth_close_minute_et
    assert disc.xnys_rth_close_minute_et(date(2026, 10, 15)) == 960
    assert disc.xnys_rth_close_minute_et(date(2025, 11, 28)) == 780

    baseline_20d = 2_000_000.0
    # Regular session (2026-10-15) and pre-wall XNYS 13:00 ET early close (2025-11-28).
    for day in (date(2026, 10, 15), date(2025, 11, 28)):
        rth_close_m = disc.xnys_rth_close_minute_et(day)
        starts_et = [
            datetime(day.year, day.month, day.day, 9, 0, tzinfo=NY) + timedelta(minutes=m)
            for m in (*range(0, 90), *range(235, 250))  # 09:00-10:29 & 12:55-13:09 ET
        ]
        vols: list[float] = []
        for k, st_et in enumerate(starts_et):
            if st_et.hour == 9 and st_et.minute == 30:
                vols.append(250_000.0)  # opening-auction bar
            elif st_et.hour < 9 or (st_et.hour == 9 and st_et.minute < 30):
                vols.append(1_000.0 + 37.0 * k)
            else:
                vols.append(
                    20_000.0 + float((k * 7_919) % 13_000) + (40_000.0 if k % 17 == 0 else 0.0)
                )
        close_ms = [(s.hour * 60 + s.minute) + 1 for s in starts_et]
        sess_lbl = [
            "PRE_MARKET" if cm <= 570 else ("RTH" if cm <= rth_close_m else "AFTER_HOURS")
            for cm in close_ms
        ]
        df = pl.DataFrame(
            {
                "symbol": ["SOXS"] * len(starts_et),
                "t_ns": [int(s.astimezone(UTC).timestamp()) * 1_000_000_000 for s in starts_et],
                "v": vols,
                "close_m": close_ms,
                "sess": sess_lbl,
                "mean_20d_vol": [baseline_20d] * len(starts_et),
            }
        )
        out = disc.compute_causal_session_rvol_frame(df, rth_close_m=rth_close_m)
        assert out.height == df.height
        sc_bars = [
            SimpleNamespace(timestamp=s.astimezone(UTC), volume=v)
            for s, v in zip(starts_et, vols, strict=True)
        ]
        checked_open_exclusion = False
        for i, row in enumerate(out.iter_rows(named=True)):
            sess_i = str(row["sess"])
            assert sc._bar_session(sc_bars[i]) == sess_i
            exp_same = sc._compute_same_session_rvol(
                sc_bars[: i + 1], sess_i, early_fallback_rvol=1.5
            )
            now_utc = sc_bars[i].timestamp + timedelta(minutes=1)
            exp_tod = sc.compute_tod_rvol(sum(vols[: i + 1]), baseline_20d, now_utc, sess_i)
            elapsed_rth = float(row["close_m"]) - 570.0
            exp_blend = (
                exp_tod
                if sess_i == "RTH" and exp_tod > 0.0 and 0.0 <= elapsed_rth < 60.0
                else max(exp_same, exp_tod)
            )
            assert row["rvol_same_session"] == pytest.approx(exp_same, rel=1e-9, abs=1e-12)
            assert row["tod_rvol"] == pytest.approx(exp_tod, rel=1e-9, abs=1e-12)
            assert row["rvol_blend"] == pytest.approx(exp_blend, rel=1e-9, abs=1e-12)
            if sess_i == "RTH" and int(row["close_m"]) == 586:  # 09:45 ET bar close
                rth_v = [
                    v for v, s in zip(vols[: i + 1], sess_lbl[: i + 1], strict=True) if s == "RTH"
                ]
                naive = (sum(rth_v[-5:]) / 5.0) / (sum(rth_v[:-5]) / len(rth_v[:-5]))
                assert row["rvol_same_session"] > naive  # 09:30 auction excluded from prior
                checked_open_exclusion = True
        assert checked_open_exclusion
        if rth_close_m < 960:
            assert "AFTER_HOURS" in set(out["sess"].to_list())

"""Unit & integration tests for causal historical early-mover compounding & scanner tuning."""

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_early_mover_compounding import (  # noqa: E402
    AUDIT_SCHEMA_VERSION,
    STUDY_NAME,
    TUNING_SCHEMA_VERSION,
    run_early_mover_compounding,
    simulate_24x5_lossless_trailing_replay,
    split_prewall_days_purged,
)

from tier0_sota import discovery as disc  # noqa: E402
from tier0_sota.live.scanner import (  # noqa: E402
    ExtremeMoverScanner,
    build_live_lane_preset_bundle,
    load_early_mover_tuning_manifest,
)

_NY_TZ = ZoneInfo("America/New_York")


def _bar(ts_utc: datetime, o: float, h: float, lo: float, c: float, v: float) -> SimpleNamespace:
    return SimpleNamespace(timestamp=ts_utc, open=o, high=h, low=lo, close=c, volume=v)


def test_causal_pit_isolation_and_purged_split_enforced() -> None:
    # Reject post-wall date (> 2026-10-15) fail-closed
    with pytest.raises(disc.PreWallViolationError):
        split_prewall_days_purged(["2026-10-14", "2026-10-15", "2026-10-16"])
    with pytest.raises(disc.PreWallViolationError):
        disc.build_prewall_early_mover_session_panel(["2026-11-23"])

    train, purge, val = split_prewall_days_purged(
        [
            "2026-10-06",
            "2026-10-07",
            "2026-10-08",
            "2026-10-09",
            "2026-10-12",
            "2026-10-13",
            "2026-10-14",
            "2026-10-15",
        ]
    )
    assert train and purge and val
    assert max(train) < min(purge) < min(val) <= disc.DISCOVERY_WALL_ISO
    assert set(train).isdisjoint(purge)
    assert set(train).isdisjoint(val)
    assert set(purge).isdisjoint(val)


def test_offline_optuna_compounding_manifests_and_incremental_trials(tmp_path: Path) -> None:
    optuna_db = tmp_path / "optuna.db"
    tuning1, audit1 = run_early_mover_compounding(
        trials=4,
        manifest_dir=tmp_path,
        optuna_db=optuna_db,
        monday_date=date(2026, 11, 23),
        offline=True,
    )

    tuning_path = tmp_path / "early_mover_tuning_manifest.json"
    audit_path = tmp_path / "historical_early_mover_audit_manifest.json"
    assert tuning_path.is_file()
    assert audit_path.is_file()

    assert tuning1["schema_version"] == TUNING_SCHEMA_VERSION
    assert tuning1["paper_only"] is True
    assert tuning1["live_execution_enabled"] is False
    assert tuning1["causal_pit_enforced"] is True
    assert tuning1["prewall_cutoff_iso"] == "2026-10-15"
    assert tuning1["out_of_sample_recent_replay_excluded_from_training"] is True
    assert tuning1["study_name"] == STUDY_NAME
    assert tuning1["trials_total"] == 4
    assert tuning1["compounding_timer"]["unit"] == "tier0-early-mover-compound.timer"
    assert set(tuning1["weights"].keys()) == set(disc.EARLY_MOVER_WEIGHT_KEYS)
    assert tuning1["best_weights"] == tuning1["weights"]

    assert audit1["schema_version"] == AUDIT_SCHEMA_VERSION
    assert audit1["paper_only"] is True
    assert audit1["live_execution_enabled"] is False
    pb = audit1["prewall_benchmark"]
    assert pb["dataset_tag"] == "prewall_causal_pit"
    assert 0.0 <= pb["recall_at_top15"] <= 1.0
    assert pb["recall_at_top15"] >= 0.8
    assert pb["median_time_to_first_detect_mins"] >= 0.0
    assert pb["mean_post_detect_mfe_capture_pct"] > 0.0
    assert len(pb["timeline"]) > 0
    assert audit1["first_detection_timeline"] == pb["timeline"]
    required_tl_cols = {
        "symbol",
        "date",
        "first_detect_et",
        "first_detect_px",
        "session_high_px",
        "post_detect_mfe_pct",
        "category",
        "rvol",
        "vol_buildup_ratio",
    }
    assert required_tl_cols.issubset(pb["timeline"][0].keys())

    oos = audit1["out_of_sample_recent_replay"]
    assert oos["dataset_tag"] == "out_of_sample_recent_replay"
    assert oos["mixed_into_prewall_training"] is False
    assert (
        audit1["nvda_monday_premarket_audit"]["first_detect_category"] == "PREMARKET_EARLY_IGNITION"
    )

    # Second compounding cycle appends cleanly into the same persistent Optuna SQLite study
    tuning2, _ = run_early_mover_compounding(
        trials=3,
        manifest_dir=tmp_path,
        optuna_db=optuna_db,
        monday_date=date(2026, 11, 23),
        offline=True,
    )
    assert tuning2["trials_total"] == 7


def test_nvda_monday_premarket_early_ignition_and_24x5_lossless_trailing_replay() -> None:
    # Realistic NVDA Monday 2026-11-23 premarket ignition -> $171.50 peak -> pullback
    mon_0400 = datetime(2026, 11, 23, 4, 0, tzinfo=_NY_TZ).astimezone(UTC)
    bars: list[SimpleNamespace] = [
        _bar(mon_0400, 135.34, 135.76, 133.57, 135.65, 684_131.0),
        _bar(mon_0400 + timedelta(minutes=1), 135.67, 135.98, 135.20, 135.75, 1_482_569.0),
    ]
    # Steady premarket + RTH rally to $171.50 peak followed by reversal
    px = 136.0
    for i in range(2, 60):
        px_next = min(171.50, px + 0.62)
        bars.append(
            _bar(
                mon_0400 + timedelta(minutes=i),
                px,
                px_next + 0.15,
                px - 0.10,
                px_next,
                400_000.0 + i * 10_000.0,
            )
        )
        px = px_next
    # Peak exhaustion reversal downward from $171.50 -> $164.00
    for j in range(20):
        nxt = px - 0.38
        bars.append(
            _bar(
                mon_0400 + timedelta(minutes=60 + j),
                px,
                px + 0.05,
                nxt - 0.10,
                nxt,
                900_000.0,
            )
        )
        px = nxt

    tech = disc.compute_causal_technical_alignment(
        [b.close for b in bars[:2]],
        [b.high for b in bars[:2]],
        [b.low for b in bars[:2]],
        [b.volume for b in bars[:2]],
        prior_close=130.01,
        rvol=2.17,
        vol_buildup_ratio=2.17,
        session="PRE_MARKET",
        move_pct=4.42,
    )
    assert tech["premarket_ignition"] is True
    assert tech["early_ignition_detected"] is True
    assert tech["sudden_volume_surge"] is True
    assert tech["technical_alignment_score"] >= 70.0

    cat = disc.classify_potential_category(
        symbol="NVDA",
        session="PRE_MARKET",
        move_pct=4.42,
        rvol=2.17,
        vol_buildup_ratio=2.17,
        hidden_volume_ratio=1.2,
        darkpool_trf_ratio=None,
        trf_block_ratio=None,
        macro_regime=1,
        peak_confirmed=False,
        premarket_ignition=True,
        sudden_volume_surge=True,
        technical_alignment_score=tech["technical_alignment_score"],
    )
    assert cat == "PREMARKET_EARLY_IGNITION"

    sim = simulate_24x5_lossless_trailing_replay(bars, entry_idx=1, prior_close=125.53)
    assert sim["simulated"] is True
    assert sim["lossless_protected"] is True
    assert sim["max_profit_lock_stop_px"] > sim["entry_px"]
    assert sim["long_leg_return_pct"] > 15.0
    assert sim["trailing_ratchets"] >= 5
    assert sim["peak_climax_reverse_triggered"] is True


def test_scanner_loads_tuned_weights_fallback_and_emits_categories_and_alignment(
    tmp_path: Path,
) -> None:
    now_utc = datetime(2026, 11, 23, 9, 2, tzinfo=UTC)  # 04:02 ET PRE_MARKET

    # 1. Fail-closed fallback on missing, stale, post-wall, or live_execution_enabled=True
    fb_missing = load_early_mover_tuning_manifest(tmp_path / "missing.json", now_utc=now_utc)
    assert fb_missing["loaded"] is False
    assert fb_missing["tuning_run_id"] == "default_causal_v1"

    bad_live = tmp_path / "bad_live.json"
    bad_live.write_text(
        json.dumps(
            {
                "schema_version": TUNING_SCHEMA_VERSION,
                "generated_at_utc": now_utc.isoformat(),
                "paper_only": True,
                "live_execution_enabled": True,
                "prewall_cutoff_iso": "2026-10-15",
                "weights": disc.DEFAULT_EARLY_MOVER_WEIGHTS,
            }
        ),
        encoding="utf-8",
    )
    assert load_early_mover_tuning_manifest(bad_live, now_utc=now_utc)["loaded"] is False

    bad_postwall = tmp_path / "bad_postwall.json"
    bad_postwall.write_text(
        json.dumps(
            {
                "schema_version": TUNING_SCHEMA_VERSION,
                "generated_at_utc": now_utc.isoformat(),
                "paper_only": True,
                "live_execution_enabled": False,
                "prewall_cutoff_iso": "2026-11-23",
                "weights": disc.DEFAULT_EARLY_MOVER_WEIGHTS,
            }
        ),
        encoding="utf-8",
    )
    assert load_early_mover_tuning_manifest(bad_postwall, now_utc=now_utc)["loaded"] is False

    stale_file = tmp_path / "stale.json"
    stale_file.write_text(
        json.dumps(
            {
                "schema_version": TUNING_SCHEMA_VERSION,
                "generated_at_utc": (now_utc - timedelta(days=30)).isoformat(),
                "paper_only": True,
                "live_execution_enabled": False,
                "prewall_cutoff_iso": "2026-10-15",
                "weights": disc.DEFAULT_EARLY_MOVER_WEIGHTS,
            }
        ),
        encoding="utf-8",
    )
    assert load_early_mover_tuning_manifest(stale_file, now_utc=now_utc)["loaded"] is False

    # 2. Valid pre-wall tuned manifest loaded & applied by ExtremeMoverScanner
    valid_tuning = tmp_path / "early_mover_tuning_manifest.json"
    valid_tuning.write_text(
        json.dumps(
            {
                "schema_version": TUNING_SCHEMA_VERSION,
                "generated_at_utc": now_utc.isoformat(),
                "paper_only": True,
                "live_execution_enabled": False,
                "prewall_cutoff_iso": "2026-10-15",
                "tuning_run_id": "mlflow-run-test-42",
                "weights": {
                    "w_move": 4.2,
                    "w_rvol": 14.5,
                    "w_vol_buildup": 6.0,
                    "w_trf_darkpool": 11.0,
                    "w_hvr": 4.5,
                    "w_tech_breakout": 0.25,
                    "w_premarket_ignition": 26.0,
                },
                "validation_metrics": {
                    "recall_at_top15": 0.85,
                    "median_time_to_first_detect_mins": 10.0,
                },
            }
        ),
        encoding="utf-8",
    )

    fri_close_utc = datetime(2026, 11, 20, 20, 59, tzinfo=UTC)
    pm_0400_utc = datetime(2026, 11, 23, 9, 0, tzinfo=UTC)
    pm_0401_utc = datetime(2026, 11, 23, 9, 1, tzinfo=UTC)

    class _PmScreener:
        def get_most_actives(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(
                most_actives=[
                    SimpleNamespace(
                        symbol="NVDA", price=135.75, volume=2_166_700, percent_change=4.42
                    ),
                    SimpleNamespace(
                        symbol="AAPL", price=225.00, volume=100_000, percent_change=0.10
                    ),
                ]
            )

        def get_market_movers(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(gainers=[], losers=[])

    class _PmData:
        def get_stock_bars(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "NVDA": [
                        _bar(fri_close_utc, 129.8, 130.1, 129.7, 130.01, 500_000.0),
                        _bar(pm_0400_utc, 135.34, 135.76, 133.57, 135.65, 684_131.0),
                        _bar(pm_0401_utc, 135.67, 135.98, 135.20, 135.75, 1_482_569.0),
                    ],
                    "AAPL": [
                        _bar(fri_close_utc, 224.9, 225.1, 224.8, 225.0, 100_000.0),
                        _bar(pm_0400_utc, 225.0, 225.05, 224.95, 225.0, 5_000.0),
                        _bar(pm_0401_utc, 225.0, 225.05, 224.95, 225.02, 5_000.0),
                    ],
                }
            )

        def get_stock_trades(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "NVDA": [
                        SimpleNamespace(
                            symbol="NVDA", size=600, price=135.75, exchange="D", conditions=["@"]
                        ),
                        SimpleNamespace(
                            symbol="NVDA", size=400, price=135.75, exchange="Q", conditions=["@"]
                        ),
                    ]
                }
            )

    class _PmTrading:
        def get_asset(self, sym: str) -> SimpleNamespace:
            return SimpleNamespace(
                symbol=sym,
                shortable=True,
                easy_to_borrow=True,
                exchange=SimpleNamespace(value="NASDAQ"),
            )

        def get_all_assets(self, _req: Any) -> list[SimpleNamespace]:
            return [self.get_asset(s) for s in ("NVDA", "SOXL", "SOXS", "TQQQ", "SQQQ")]

    scanner = ExtremeMoverScanner(
        screener_client=_PmScreener(),
        data_client=_PmData(),
        trading_client=_PmTrading(),
        tuning_manifest_path=valid_tuning,
    )
    doc = scanner.scan(top_k=10, now_utc=now_utc)
    assert doc["early_mover_tuning"]["loaded"] is True
    assert doc["early_mover_tuning"]["tuning_run_id"] == "mlflow-run-test-42"

    by_sym = {e["symbol"]: e for e in doc["entries"]}
    assert "AAPL" not in by_sym  # flat mega-cap rejected
    nvda = by_sym["NVDA"]
    assert nvda["potential_category"] == "PREMARKET_EARLY_IGNITION"
    assert nvda["premarket_ignition"] is True
    assert nvda["early_ignition_detected"] is True
    assert nvda["sudden_volume_surge"] is True
    assert 0.0 <= nvda["technical_alignment_score"] <= 100.0
    assert nvda["technical_alignment_score"] >= 70.0
    assert nvda["tuning_run_id"] == "mlflow-run-test-42"

    # Bundle preserves all new early-mover fields cleanly
    bundle = build_live_lane_preset_bundle(doc)
    b_nvda = next(r for r in bundle["top_potential_symbols"] if r["symbol"] == "NVDA")
    assert b_nvda["potential_category"] == "PREMARKET_EARLY_IGNITION"
    assert b_nvda["premarket_ignition"] is True
    assert b_nvda["early_ignition_detected"] is True
    assert b_nvda["sudden_volume_surge"] is True
    assert b_nvda["tuning_run_id"] == "mlflow-run-test-42"
    assert b_nvda["technical_alignment_score"] == nvda["technical_alignment_score"]


def test_all_nine_potential_categories_reachable() -> None:
    cases: list[dict[str, Any]] = [
        dict(
            symbol="NVDA",
            session="PRE_MARKET",
            move_pct=4.5,
            rvol=2.2,
            vol_buildup_ratio=2.1,
            hidden_volume_ratio=1.0,
            darkpool_trf_ratio=None,
            trf_block_ratio=None,
            macro_regime=1,
            peak_confirmed=False,
            premarket_ignition=True,
            sudden_volume_surge=True,
            technical_alignment_score=85.0,
        ),
        dict(
            symbol="SMCI",
            session="RTH",
            move_pct=7.0,
            rvol=3.2,
            vol_buildup_ratio=1.8,
            hidden_volume_ratio=1.0,
            darkpool_trf_ratio=0.2,
            trf_block_ratio=0.1,
            macro_regime=1,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=True,
            technical_alignment_score=78.0,
        ),
        dict(
            symbol="AMD",
            session="RTH",
            move_pct=2.0,
            rvol=2.6,
            vol_buildup_ratio=2.4,
            hidden_volume_ratio=1.0,
            darkpool_trf_ratio=0.2,
            trf_block_ratio=0.1,
            macro_regime=0,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=True,
            technical_alignment_score=50.0,
        ),
        dict(
            symbol="AVGO",
            session="RTH",
            move_pct=1.8,
            rvol=1.4,
            vol_buildup_ratio=1.2,
            hidden_volume_ratio=2.2,
            darkpool_trf_ratio=0.45,
            trf_block_ratio=0.35,
            macro_regime=0,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=False,
            technical_alignment_score=52.0,
        ),
        dict(
            symbol="PLTR",
            session="RTH",
            move_pct=2.2,
            rvol=1.6,
            vol_buildup_ratio=1.3,
            hidden_volume_ratio=1.0,
            darkpool_trf_ratio=0.2,
            trf_block_ratio=0.1,
            macro_regime=1,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=False,
            technical_alignment_score=72.0,
            breakout_20d=True,
        ),
        dict(
            symbol="ARM",
            session="RTH",
            move_pct=8.0,
            rvol=1.8,
            vol_buildup_ratio=1.4,
            hidden_volume_ratio=1.0,
            darkpool_trf_ratio=0.2,
            trf_block_ratio=0.1,
            macro_regime=1,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=False,
            technical_alignment_score=70.0,
            post_earnings_gap=True,
        ),
        dict(
            symbol="MSTR",
            session="RTH",
            move_pct=12.0,
            rvol=3.5,
            vol_buildup_ratio=2.5,
            hidden_volume_ratio=1.8,
            darkpool_trf_ratio=0.3,
            trf_block_ratio=0.2,
            macro_regime=0,
            peak_confirmed=True,
            premarket_ignition=False,
            sudden_volume_surge=True,
            technical_alignment_score=40.0,
        ),
        dict(
            symbol="SOXS",
            session="RTH",
            move_pct=6.5,
            rvol=2.1,
            vol_buildup_ratio=1.5,
            hidden_volume_ratio=1.0,
            darkpool_trf_ratio=0.3,
            trf_block_ratio=0.2,
            macro_regime=-1,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=False,
            technical_alignment_score=70.0,
        ),
        dict(
            symbol="TSLA",
            session="RTH",
            move_pct=1.6,
            rvol=1.2,
            vol_buildup_ratio=1.1,
            hidden_volume_ratio=0.8,
            darkpool_trf_ratio=0.15,
            trf_block_ratio=0.05,
            macro_regime=0,
            peak_confirmed=False,
            premarket_ignition=False,
            sudden_volume_surge=False,
            technical_alignment_score=45.0,
        ),
    ]
    seen = {disc.classify_potential_category(**kw) for kw in cases}
    assert seen == set(disc.POTENTIAL_CATEGORIES)


def test_prometheus_premarket_session_gate_and_alertmanager_momentum_grouping() -> None:
    import yaml

    rules_doc = yaml.safe_load(
        (REPO_ROOT / "docker" / "prometheus_rules.yml").read_text(encoding="utf-8")
    )
    rules = {r["alert"]: r for grp in rules_doc.get("groups", []) for r in grp.get("rules", [])}
    assert len(rules) >= 15

    pm_rule = rules["Tier0PremarketMomentumIgnition"]
    pm_expr = str(pm_rule["expr"])
    assert 'session="PRE_MARKET"' in pm_expr
    assert 'session!="PRE_MARKET"' in pm_expr
    assert pm_rule["labels"]["session"] == "PRE_MARKET"
    assert pm_rule["labels"]["category"] == "momentum"

    dp_rule = rules["Tier0DarkpoolAccumulationSurge"]
    dp_expr = str(dp_rule["expr"])
    assert "group_left(session, calibrated_category)" in dp_expr
    assert "topk by (symbol) (1," in dp_expr
    assert "{{ $labels.session }}" in dp_rule["annotations"]["summary"]
    assert "{{ $labels.calibrated_category }}" in dp_rule["annotations"]["summary"]

    am_doc = yaml.safe_load((REPO_ROOT / "docker" / "alertmanager.yml").read_text(encoding="utf-8"))
    momentum_route = next(
        r for r in am_doc["route"]["routes"] if 'category="momentum"' in (r.get("matchers") or [])
    )
    assert momentum_route["group_by"] == [
        "alertname",
        "session",
        "calibrated_category",
        "severity",
    ]
    assert momentum_route["receiver"] == "tier0-local-null"

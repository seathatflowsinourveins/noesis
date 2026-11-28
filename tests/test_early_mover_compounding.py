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


def test_scanner_split_adjustment_spread_gate_adv20_tod_rvol_vwap_cap_and_vixy(
    tmp_path: Path,
) -> None:
    from tier0_sota.live.scanner import (
        TOD_RVOL_EARLY_CLOSE_SCALE,
        TOD_RVOL_PM_SHARE_OF_FULLDAY,
        compute_tod_expected_volume_fraction,
    )

    # 1. PRE_MARKET ToD RVOL scales by TOD_RVOL_PM_SHARE_OF_FULLDAY (0.01871) & floor 0.002.
    pm_0415 = datetime(2026, 11, 27, 4, 15, tzinfo=_NY_TZ).astimezone(UTC)
    pm_0645 = datetime(2026, 11, 27, 6, 45, tzinfo=_NY_TZ).astimezone(UTC)
    pm_0925 = datetime(2026, 11, 27, 9, 25, tzinfo=_NY_TZ).astimezone(UTC)
    assert compute_tod_expected_volume_fraction(pm_0415, "PRE_MARKET") == pytest.approx(0.002)
    assert compute_tod_expected_volume_fraction(pm_0645, "PRE_MARKET") == pytest.approx(
        TOD_RVOL_PM_SHARE_OF_FULLDAY * (165.0 / 330.0), rel=1e-4
    )
    assert compute_tod_expected_volume_fraction(pm_0925, "PRE_MARKET") == pytest.approx(
        TOD_RVOL_PM_SHARE_OF_FULLDAY * (325.0 / 330.0), rel=1e-4
    )

    # 2. AFTER_HOURS early-close scaling uses 0.4026 + 0.06 * ah_frac; regular day stays 1.0.
    ah_ec_1500 = datetime(2026, 11, 27, 15, 0, tzinfo=_NY_TZ).astimezone(UTC)
    assert compute_tod_expected_volume_fraction(ah_ec_1500, "AFTER_HOURS") == pytest.approx(
        TOD_RVOL_EARLY_CLOSE_SCALE + 0.06 * (120.0 / 240.0), rel=1e-6
    )
    ah_reg_1700 = datetime(2026, 10, 15, 17, 0, tzinfo=_NY_TZ).astimezone(UTC)
    assert compute_tod_expected_volume_fraction(ah_reg_1700, "AFTER_HOURS") == pytest.approx(1.0)

    # 3. End-to-end sweep: INLF 1:16 reverse split, SPRC 1:5 reverse split, AMZE prior-ex
    #    no-double-adjust, FIEE 1156bps dead spread excluded, GDEV +17% VWAP cap excluded,
    #    clean XP passes EARLY_STAGE_IGNITION with ADV_20 baseline, and VIXY sets vix_available.
    now_utc = datetime(2026, 11, 27, 15, 30, tzinfo=_NY_TZ).astimezone(UTC)
    pdb_ts = datetime(2026, 11, 25, 5, 0, tzinfo=UTC)

    class _CorpActions:
        def get_corporate_actions(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(
                data={
                    "reverse_splits": [
                        SimpleNamespace(
                            symbol="INLF",
                            old_rate=16.0,
                            new_rate=1.0,
                            ex_date=date(2026, 11, 27),
                        ),
                        SimpleNamespace(
                            symbol="SPRC",
                            old_rate=5.0,
                            new_rate=1.0,
                            ex_date=date(2026, 11, 27),
                        ),
                        SimpleNamespace(
                            symbol="AMZE",
                            old_rate=100.0,
                            new_rate=1.0,
                            ex_date=date(2026, 11, 25),
                        ),
                    ],
                    "forward_splits": [],
                }
            )

    def _snap(
        px: float,
        prev_c: float,
        dvol: float,
        pvol: float,
        bid: float,
        ask: float,
        vwap: float,
        dopen: float | None = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            latest_trade=SimpleNamespace(price=px),
            latest_quote=SimpleNamespace(bid_price=bid, ask_price=ask),
            daily_bar=SimpleNamespace(
                open=dopen if dopen is not None else prev_c,
                close=px,
                volume=dvol,
                vwap=vwap,
            ),
            previous_daily_bar=SimpleNamespace(close=prev_c, volume=pvol, timestamp=pdb_ts),
        )

    snaps = {
        "INLF": _snap(2.42, 0.1626, 355_794.0, 4_438_345.0, 2.41, 2.45, 2.52, dopen=2.45),
        "SPRC": _snap(1.34, 0.2008, 804_980.0, 12_123_184.0, 1.33, 1.35, 1.35, dopen=1.20),
        "AMZE": _snap(5.20, 5.00, 300_000.0, 100_000.0, 5.19, 5.21, 5.15, dopen=5.05),
        "FIEE": _snap(2.37, 2.50, 255_620.0, 2_846.0, 2.20, 2.47, 2.58, dopen=2.55),
        "GDEV": _snap(9.20, 8.15, 320_000.0, 10_254.0, 9.15, 9.25, 7.80, dopen=8.10),
        "AEHL": _snap(13.30, 7.14, 5_061_180.0, 26_471.0, 13.28, 13.37, 12.04, dopen=7.36),
        "XP": _snap(17.30, 14.39, 23_906_279.0, 10_222_478.0, 17.28, 17.35, 17.05, dopen=17.20),
        "VIXY": _snap(30.50, 32.00, 1_500_000.0, 1_400_000.0, 30.48, 30.52, 30.70, dopen=31.00),
    }

    prior_dates = [date(2026, 11, d) for d in range(1, 21)]
    adv_daily_bars = {
        sym: [
            SimpleNamespace(
                timestamp=datetime(d.year, d.month, d.day, 5, 0, tzinfo=UTC),
                open=10.0,
                high=10.5,
                low=9.5,
                close=10.0,
                volume=vol,
            )
            for d in prior_dates
        ]
        for sym, vol in (
            ("INLF", 250_000.0),
            ("SPRC", 2_000_000.0),
            ("AMZE", 90_000.0),
            ("FIEE", 50_000.0),
            ("GDEV", 40_000.0),
            ("AEHL", 120_000.0),
            ("XP", 8_000_000.0),
            ("VIXY", 1_400_000.0),
        )
    }

    class _EmptyScreener:
        def get_most_actives(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(most_actives=[])

        def get_market_movers(self, _req: Any) -> SimpleNamespace:
            return SimpleNamespace(gainers=[], losers=[])

    class _Trading:
        def get_all_assets(self, _req: Any) -> list[SimpleNamespace]:
            return [SimpleNamespace(symbol=s, tradable=True, exchange="NASDAQ") for s in snaps]

        def get_asset(self, sym: str) -> SimpleNamespace:
            return SimpleNamespace(
                symbol=sym, shortable=True, easy_to_borrow=True, exchange="NASDAQ"
            )

    class _Data:
        def get_stock_snapshot(self, req: Any) -> dict[str, Any]:
            req_syms = getattr(req, "symbol_or_symbols", ())
            req_syms = [req_syms] if isinstance(req_syms, str) else list(req_syms)
            return {s: snaps[s] for s in req_syms if s in snaps}

        def get_stock_bars(self, req: Any) -> SimpleNamespace:
            req_syms = getattr(req, "symbol_or_symbols", ())
            req_syms = [req_syms] if isinstance(req_syms, str) else list(req_syms)
            tf = str(getattr(req, "timeframe", ""))
            if "Day" in tf or "1Day" in tf:
                return SimpleNamespace(
                    data={s: adv_daily_bars[s] for s in req_syms if s in adv_daily_bars}
                )
            return SimpleNamespace(data={})

    scanner = ExtremeMoverScanner(
        screener_client=_EmptyScreener(),
        data_client=_Data(),
        trading_client=_Trading(),
        corp_actions_client=_CorpActions(),
        pin_symbols=("XP",),
        universe_cache_path=tmp_path / "nms_cache.json",
    )
    doc = scanner.scan(top_k=10, now_utc=now_utc)
    full_by_sym = {r["symbol"]: r for r in doc["full_market_top_movers"]}
    early_by_sym = {r["symbol"]: r for r in doc["early_stage_ignition_movers"]}

    # INLF adjusted 1:16 -> -6.98% (NOT +1388% #1!), AEHL is #1 genuine mover!
    assert full_by_sym["INLF"]["split_adjusted"] is True
    assert full_by_sym["INLF"]["split_factor"] == pytest.approx(16.0)
    assert full_by_sym["INLF"]["daily_change_pct"] == pytest.approx(-6.98, abs=0.05)
    assert full_by_sym["INLF"]["rank"] > 1
    assert doc["full_market_top_movers"][0]["symbol"] == "AEHL"

    # SPRC adjusted 1:5 -> +33.47% (NOT +567%) and demoted below high-RVOL movers
    assert full_by_sym["SPRC"]["split_adjusted"] is True
    assert full_by_sym["SPRC"]["daily_change_pct"] == pytest.approx(33.47, abs=0.1)
    assert full_by_sym["SPRC"]["low_rvol_demoted"] is True

    # AMZE had ex_date == pdb_date -> NOT double-adjusted!
    assert full_by_sym["AMZE"]["split_adjusted"] is False
    assert full_by_sym["AMZE"]["daily_change_pct"] == pytest.approx(4.0, abs=0.05)

    # FIEE (1156 bps spread) excluded from BOTH full_market_top_movers and early_stage_ignition!
    assert "FIEE" not in full_by_sym
    assert "FIEE" not in early_by_sym

    # GDEV (+17.95% above VWAP) excluded from early_stage_ignition by upper 12% VWAP cap!
    assert "GDEV" not in early_by_sym

    # XP passes EARLY_STAGE_IGNITION using ADV_20 median split-adjusted baseline!
    assert "XP" in early_by_sym
    assert early_by_sym["XP"]["adv_20"] == pytest.approx(8_000_000.0)
    assert early_by_sym["XP"]["rvol_baseline_source"] == "adv_20_median_split_adj"
    assert early_by_sym["XP"]["spread_bps"] is not None

    # VIXY proxy populates macro_regime!
    assert doc["macro_regime"]["vix_proxy_symbol"] == "VIXY"
    assert doc["macro_regime"]["vix_proxy_daily_pct"] == pytest.approx(-4.688, abs=0.01)
    assert doc["macro_regime"]["vix_available"] is True

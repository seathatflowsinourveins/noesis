"""Session clock: every boundary of a trading day comes from the upstream calendars.

exchange_calendars XNYS decides which days are sessions and their regular open and close;
pandas_market_calendars NYSE supplies the pre-market open and the post-market close, which is
17:00 ET (not 20:00) on a 13:00 early close.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from tier0_sota import engine
from tier0_sota.discovery import get_xnys_calendar

NY = ZoneInfo("America/New_York")
RANGE_START, RANGE_END = date(2024, 1, 1), date(2027, 12, 31)
THANKSGIVING = date(2026, 11, 26)
EARLY_CLOSE_DAY = date(2026, 11, 27)
CHRISTMAS_EVE = date(2026, 12, 24)  # the next 13:00 early close after EARLY_CLOSE_DAY
SESSION_LABELS = ("PRE_MARKET", "RTH", "AFTER_HOURS", "OVERNIGHT", "CLOSED")
TS_MIN_NS = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp()) * 1_000_000_000
TS_MAX_NS = int(datetime(2028, 1, 1, tzinfo=UTC).timestamp()) * 1_000_000_000 - 1

# The loaders as defined in the engine, so the caches can be cleared while a test has swapped them.
_REAL_NYSE_TABLE = engine._nyse_extended_hours_table
_REAL_NYSE_ROWS = engine._nyse_extended_hours_rows  # holds the loaded table; a failure is not kept
_REAL_SESSION_DAY = engine._session_day


def _ns(day: date, hh: int, mm: int) -> int:
    moment = datetime(day.year, day.month, day.day, hh, mm, tzinfo=NY)
    return int(moment.astimezone(UTC).timestamp()) * 1_000_000_000


def _session(day: date, hh: int, mm: int) -> str:
    return engine.classify_us_equity_session_et(_ns(day, hh, mm))


def _clock(bounds: engine.UsEquitySessionBoundsET) -> list[str]:
    marks = (bounds.pre_open, bounds.regular_open, bounds.regular_close, bounds.post_close)
    return [mark.strftime("%H:%M") for mark in marks]


def _days_covered_by_both_calendars() -> list[date]:
    """Days of the test range inside the XNYS range; pandas_market_calendars NYSE has no bound.

    The XNYS range ends about a year after today and moves daily, so it is read, not assumed.
    """
    xnys = get_xnys_calendar()
    lo = max(RANGE_START, xnys.first_session.date())
    hi = min(RANGE_END, xnys.last_session.date())
    return [lo + timedelta(days=offset) for offset in range((hi - lo).days + 1)]


def _next_wednesday(day: date) -> date:
    return day + timedelta(days=(2 - day.weekday()) % 7)


def _pre_upstream_classify_et_hm(hm: int, rth_close_hm: int = 960) -> str:
    """The minute classifier as it was before the upstream bounds; still the default behaviour."""
    if 240 <= hm < 570:
        return "PRE_MARKET"
    if 570 <= hm < rth_close_hm:
        return "RTH"
    if rth_close_hm <= hm < 1200:
        return "AFTER_HOURS"
    return "OVERNIGHT"


def _expected_from_bounds(ts_utc_ns: int) -> str:
    """Session label derived only from us_equity_session_bounds_et and the 20:00 overnight rule."""
    moment = datetime.fromtimestamp(ts_utc_ns // 1_000_000_000, tz=UTC).astimezone(NY)
    if moment.time() >= time(20, 0):
        tomorrow = engine.us_equity_session_bounds_et(moment.date() + timedelta(days=1))
        return "OVERNIGHT" if tomorrow is not None else "CLOSED"
    bounds = engine.us_equity_session_bounds_et(moment.date())
    if bounds is None:
        return "CLOSED"
    if moment < bounds.pre_open:
        return "OVERNIGHT"
    if moment < bounds.regular_open:
        return "PRE_MARKET"
    if moment < bounds.regular_close:
        return "RTH"
    if moment < bounds.post_close:
        return "AFTER_HOURS"
    return "CLOSED"


def _flaky_get_calendar(monkeypatch: pytest.MonkeyPatch, failures: int) -> list[str]:
    """Make mcal.get_calendar raise on its first `failures` calls; returns the growing call log."""
    real_get_calendar = mcal.get_calendar
    calls: list[str] = []

    def _get_calendar(name: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(name)
        if len(calls) <= failures:
            raise RuntimeError(f"simulated upstream failure {len(calls)}")
        return real_get_calendar(name, *args, **kwargs)

    monkeypatch.setattr(mcal, "get_calendar", _get_calendar)
    return calls


@pytest.fixture
def fresh_session_clock() -> Iterator[None]:
    """Drop the per-day and schedule caches around a test that swaps the upstream loader."""

    def _clear() -> None:
        _REAL_SESSION_DAY.cache_clear()
        _REAL_NYSE_ROWS.cache_clear()

    _clear()
    yield
    _clear()


def test_early_close_day_after_hours_ends_at_upstream_post_close() -> None:
    bounds = engine.us_equity_session_bounds_et(EARLY_CLOSE_DAY)
    assert bounds is not None
    assert bounds.source == "upstream"
    assert _clock(bounds) == ["04:00", "09:30", "13:00", "17:00"]

    expected = {
        (12, 59): "RTH",
        (13, 0): "AFTER_HOURS",
        (13, 30): "AFTER_HOURS",
        (16, 59): "AFTER_HOURS",
        (17, 0): "CLOSED",
        (17, 30): "CLOSED",
        (19, 59): "CLOSED",
        (20, 30): "CLOSED",
    }
    assert {hm: _session(EARLY_CLOSE_DAY, *hm) for hm in expected} == expected


def test_normal_day_boundaries_unchanged() -> None:
    expected = {
        (3, 59): "OVERNIGHT",
        (4, 0): "PRE_MARKET",
        (9, 29): "PRE_MARKET",
        (9, 30): "RTH",
        (15, 59): "RTH",
        (16, 0): "AFTER_HOURS",
        (19, 59): "AFTER_HOURS",
        (20, 0): "OVERNIGHT",
    }
    # A winter (EST) and a summer (EDT) session, each followed by another session.
    for day in (date(2026, 11, 30), date(2026, 7, 8)):
        bounds = engine.us_equity_session_bounds_et(day)
        assert bounds is not None, day
        assert engine.us_equity_session_bounds_et(day + timedelta(days=1)) is not None, day
        assert (bounds.source, _clock(bounds)) == ("upstream", ["04:00", "09:30", "16:00", "20:00"])
        assert {hm: _session(day, *hm) for hm in expected} == expected, day

    # classify_et_hm keeps its old arguments and, with default keywords, its old answers.
    for rth_close_hm in (780, 960):
        for hm in range(1440):
            assert engine.classify_et_hm(hm, rth_close_hm) == _pre_upstream_classify_et_hm(
                hm, rth_close_hm
            ), (hm, rth_close_hm)
    assert engine.classify_et_hm(600) == "RTH"
    assert engine.classify_et_hm(1019, 780, post_close_hm=1020) == "AFTER_HOURS"
    assert engine.classify_et_hm(1020, 780, post_close_hm=1020) == "CLOSED"
    assert engine.classify_et_hm(1200, 780, post_close_hm=1020) == "OVERNIGHT"


def test_weekend_and_sunday_overnight_reopen() -> None:
    fri, sat, sun, mon = (date(2026, 11, dom) for dom in (27, 28, 29, 30))
    sequence = [
        (fri, 13, 30, "AFTER_HOURS"),
        (fri, 20, 30, "CLOSED"),
        (sat, 12, 0, "CLOSED"),
        (sun, 19, 59, "CLOSED"),
        (sun, 20, 1, "OVERNIGHT"),
        (mon, 3, 59, "OVERNIGHT"),
        (mon, 4, 1, "PRE_MARKET"),
        (mon, 9, 31, "RTH"),
    ]
    assert [(day, hh, mm, _session(day, hh, mm)) for day, hh, mm, _ in sequence] == sequence


def test_upstream_calendars_agree_on_sessions_and_closes() -> None:
    days = _days_covered_by_both_calendars()
    assert EARLY_CLOSE_DAY in days
    lo, hi = days[0].isoformat(), days[-1].isoformat()

    xnys = get_xnys_calendar()
    xnys_close: dict[date, str] = {}
    for session in xnys.sessions_in_range(lo, hi):
        close = xnys.session_close(session).to_pydatetime().astimezone(NY)
        xnys_close[session.date()] = close.strftime("%H:%M")

    sched: Any = mcal.get_calendar("NYSE").schedule(start_date=lo, end_date=hi, tz=NY.key)
    nyse_close = {
        session.date(): close.strftime("%H:%M")
        for session, close in zip(sched.index, sched["market_close"], strict=True)
    }

    problems = [f"{day}: session only in XNYS" for day in sorted(set(xnys_close) - set(nyse_close))]
    problems += [
        f"{day}: session only in NYSE" for day in sorted(set(nyse_close) - set(xnys_close))
    ]
    problems += [
        f"{day}: regular close XNYS {xnys_close[day]} vs NYSE {nyse_close[day]}"
        for day in sorted(set(xnys_close) & set(nyse_close))
        if xnys_close[day] != nyse_close[day]
    ]
    assert not problems, "upstream calendars disagree:\n" + "\n".join(problems)

    # The engine's own per-day report (what the supervisor logs) must say the same.
    flagged = [
        report
        for report in map(engine.session_calendar_consistency, days)
        if not report["consistent"]
    ]
    assert not flagged, f"session_calendar_consistency flags {len(flagged)} day(s): {flagged}"
    engine_sessions = {day for day in days if engine.us_equity_session_bounds_et(day) is not None}
    assert engine_sessions == set(xnys_close)


def test_every_early_close_day_has_shortened_post_market() -> None:
    days = set(_days_covered_by_both_calendars())
    early = sorted({stamp.date() for stamp in get_xnys_calendar().early_closes} & days)
    # Known early closes in EDT and EST; the full list is whatever upstream reports for the range.
    assert {date(2025, 7, 3), date(2025, 11, 28), EARLY_CLOSE_DAY} <= set(early)

    for day in early:
        bounds = engine.us_equity_session_bounds_et(day)
        assert bounds is not None, day
        assert bounds.source == "upstream", day
        assert bounds.regular_close.time() < time(16, 0), day
        assert bounds.regular_close < bounds.post_close, day
        assert bounds.post_close.time() < time(20, 0), day
        before = bounds.post_close - timedelta(minutes=1)
        after = bounds.post_close + timedelta(minutes=1)
        assert _session(day, before.hour, before.minute) == "AFTER_HOURS", day
        assert _session(day, bounds.post_close.hour, bounds.post_close.minute) == "CLOSED", day
        assert _session(day, after.hour, after.minute) == "CLOSED", day
        assert _session(day, 19, 59) == "CLOSED", day


@settings(max_examples=500, deadline=None)
@example(ts_utc_ns=_ns(EARLY_CLOSE_DAY, 16, 59))
@example(ts_utc_ns=_ns(EARLY_CLOSE_DAY, 17, 0))
@example(ts_utc_ns=_ns(EARLY_CLOSE_DAY, 19, 59))
@example(ts_utc_ns=_ns(THANKSGIVING, 20, 0))
@example(ts_utc_ns=_ns(date(2025, 7, 3), 17, 0))
@example(ts_utc_ns=_ns(date(2026, 3, 8), 3, 30))  # spring-forward Sunday
@example(ts_utc_ns=_ns(date(2026, 11, 1), 1, 30))  # fall-back Sunday
@example(ts_utc_ns=TS_MIN_NS)
@example(ts_utc_ns=TS_MAX_NS)  # past the XNYS range today: weekday fallback
@given(ts_utc_ns=st.integers(min_value=TS_MIN_NS, max_value=TS_MAX_NS))
def test_property_classifier_never_raises_and_matches_bounds(ts_utc_ns: int) -> None:
    label = engine.classify_us_equity_session_et(ts_utc_ns)
    assert label in SESSION_LABELS
    assert label == _expected_from_bounds(ts_utc_ns)

    day = datetime.fromtimestamp(ts_utc_ns // 1_000_000_000, tz=UTC).astimezone(NY).date()
    bounds = engine.us_equity_session_bounds_et(day)
    report = engine.session_calendar_consistency(day)
    assert report["is_session"] == (bounds is not None)
    assert report["consistent"] == (report["source"] == "upstream")
    if bounds is not None:
        marks = (bounds.pre_open, bounds.regular_open, bounds.regular_close, bounds.post_close)
        assert all(mark.tzinfo is not None and mark.date() == day for mark in marks)
        assert bounds.pre_open < bounds.regular_open < bounds.regular_close <= bounds.post_close
        assert bounds.post_close.time() <= time(20, 0)
        assert bounds.source == report["source"]


def test_holiday_and_out_of_range_days_fall_back_without_raising(
    fresh_session_clock: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A holiday both calendars know: no bounds, CLOSED until the overnight open before a session.
    report = engine.session_calendar_consistency(THANKSGIVING)
    assert (report["consistent"], report["is_session"], report["bounds_hm"]) == (True, False, None)
    assert engine.us_equity_session_bounds_et(THANKSGIVING) is None
    assert [_session(THANKSGIVING, hh, mm) for hh, mm in ((12, 0), (19, 59), (20, 0))] == [
        "CLOSED",
        "CLOSED",
        "OVERNIGHT",
    ]

    # Days outside the XNYS range, past and future: the pre-upstream weekday rule and bounds.
    beyond_xnys = get_xnys_calendar().last_session.date() + timedelta(days=3650)
    for wednesday in (_next_wednesday(date(1990, 6, 10)), _next_wednesday(beyond_xnys)):
        report = engine.session_calendar_consistency(wednesday)
        assert report["reason"] == "exchange_calendars_out_of_range", wednesday
        assert (report["consistent"], report["source"]) == (False, "fallback"), wednesday
        assert report["exchange_calendars_xnys"]["in_range"] is False, wednesday
        bounds = engine.us_equity_session_bounds_et(wednesday)
        assert bounds is not None, wednesday
        assert (bounds.source, _clock(bounds)) == ("fallback", ["04:00", "09:30", "16:00", "20:00"])
        marks = ((3, 59), (4, 0), (9, 30), (16, 0), (19, 59), (20, 0))
        assert [_session(wednesday, hh, mm) for hh, mm in marks] == [
            "OVERNIGHT",
            "PRE_MARKET",
            "RTH",
            "AFTER_HOURS",
            "AFTER_HOURS",
            "OVERNIGHT",
        ], wednesday
        saturday = wednesday + timedelta(days=3)
        assert engine.us_equity_session_bounds_et(saturday) is None, saturday
        assert _session(saturday, 12, 0) == "CLOSED", saturday

    # pandas_market_calendars disagreeing with XNYS, or failing to load: the day keeps the
    # pre-upstream answer (after-hours until 20:00) and the reason is reported, never raised.
    real_table, load_error = engine._nyse_extended_hours_table()
    assert load_error is None
    without_day = {day: row for day, row in real_table.items() if day != EARLY_CLOSE_DAY}
    stubs: dict[str, tuple[dict[date, tuple[int, int, int, int] | str], str | None]] = {
        "regular_close_mismatch": ({**real_table, EARLY_CLOSE_DAY: (240, 570, 960, 1200)}, None),
        "regular_open_mismatch": ({**real_table, EARLY_CLOSE_DAY: (240, 600, 780, 1020)}, None),
        "extended_bounds_out_of_order": (
            {**real_table, EARLY_CLOSE_DAY: (570, 570, 780, 1020)},
            None,
        ),
        "session_mismatch": (without_day, None),
        "pandas_market_calendars_unavailable": ({}, "ImportError: simulated"),
    }
    for reason, stub in stubs.items():
        monkeypatch.setattr(engine, "_nyse_extended_hours_table", lambda stub=stub: stub)
        _REAL_SESSION_DAY.cache_clear()
        report = engine.session_calendar_consistency(EARLY_CLOSE_DAY)
        assert (report["reason"], report["consistent"], report["source"]) == (
            reason,
            False,
            "fallback",
        )
        bounds = engine.us_equity_session_bounds_et(EARLY_CLOSE_DAY)
        assert bounds is not None, reason
        assert (bounds.source, _clock(bounds)) == ("fallback", ["04:00", "09:30", "13:00", "20:00"])
        marks = ((12, 59), (13, 30), (17, 30), (19, 59), (20, 30))
        assert [_session(EARLY_CLOSE_DAY, hh, mm) for hh, mm in marks] == [
            "RTH",
            "AFTER_HOURS",
            "AFTER_HOURS",
            "AFTER_HOURS",
            "CLOSED",
        ], reason

    # pandas_market_calendars calling a holiday a session: XNYS stays the authority (CLOSED).
    holiday_session = ({**real_table, THANKSGIVING: (240, 570, 960, 1200)}, None)
    monkeypatch.setattr(engine, "_nyse_extended_hours_table", lambda: holiday_session)
    _REAL_SESSION_DAY.cache_clear()
    report = engine.session_calendar_consistency(THANKSGIVING)
    assert (report["reason"], report["is_session"]) == ("session_mismatch", False)
    assert engine.us_equity_session_bounds_et(THANKSGIVING) is None
    assert _session(THANKSGIVING, 12, 0) == "CLOSED"

    # The real loader with the upstream call raising: caught inside, reported, classifier answers.
    def _upstream_failure(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated upstream failure")

    monkeypatch.setattr(engine, "_nyse_extended_hours_table", _REAL_NYSE_TABLE)
    monkeypatch.setattr(mcal, "get_calendar", _upstream_failure)
    _REAL_SESSION_DAY.cache_clear()
    _REAL_NYSE_ROWS.cache_clear()
    assert _session(EARLY_CLOSE_DAY, 17, 30) == "AFTER_HOURS"
    report = engine.session_calendar_consistency(EARLY_CLOSE_DAY)
    assert report["reason"] == "pandas_market_calendars_unavailable"
    assert report["pandas_market_calendars_nyse"]["error"] == (
        "RuntimeError: simulated upstream failure"
    )


def test_failed_calendar_load_is_retried_and_heals_every_day(
    fresh_session_clock: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _flaky_get_calendar(monkeypatch, failures=1)

    # The first load fails: the day answers with the pre-upstream bounds and is not asked again,
    # so a failing upstream costs one load attempt per day, not one per classifier call.
    assert _session(CHRISTMAS_EVE, 17, 30) == "AFTER_HOURS"
    assert len(calls) == 1
    for _ in range(50):
        assert _session(CHRISTMAS_EVE, 17, 30) == "AFTER_HOURS"
        assert _session(CHRISTMAS_EVE, 12, 59) == "RTH"
    bounds = engine.us_equity_session_bounds_et(CHRISTMAS_EVE)
    assert bounds is not None
    assert (bounds.source, _clock(bounds)) == ("fallback", ["04:00", "09:30", "13:00", "20:00"])
    assert len(calls) == 1

    # The first lookup of another day asks upstream again, and it works now.
    assert _session(EARLY_CLOSE_DAY, 17, 30) == "CLOSED"
    assert len(calls) == 2

    # The day answered during the failure has its upstream bounds back as well.
    assert _session(CHRISTMAS_EVE, 17, 30) == "CLOSED"
    assert _session(CHRISTMAS_EVE, 16, 59) == "AFTER_HOURS"
    bounds = engine.us_equity_session_bounds_et(CHRISTMAS_EVE)
    assert bounds is not None
    assert (bounds.source, _clock(bounds)) == ("upstream", ["04:00", "09:30", "13:00", "17:00"])
    report = engine.session_calendar_consistency(CHRISTMAS_EVE)
    assert (report["reason"], report["consistent"], report["source"]) == ("ok", True, "upstream")
    assert report["pandas_market_calendars_nyse"]["error"] is None

    # A loaded table is never rebuilt, whatever is asked next.
    for day in _days_covered_by_both_calendars()[-400:]:
        engine.classify_us_equity_session_et(_ns(day, 17, 30))
        engine.classify_us_equity_session_et(_ns(day, 20, 30))
        engine.session_calendar_consistency(day)
    assert len(calls) == 2


def test_consistency_check_retries_failed_load_same_day(
    fresh_session_clock: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _flaky_get_calendar(monkeypatch, failures=3)

    assert _session(CHRISTMAS_EVE, 17, 30) == "AFTER_HOURS"
    assert len(calls) == 1

    # While upstream keeps failing, the supervisor's check makes exactly one load attempt per call
    # and reports the current error; the classifier keeps its answer and makes none.
    for attempt in (2, 3):
        report = engine.session_calendar_consistency(CHRISTMAS_EVE)
        assert len(calls) == attempt
        assert (report["reason"], report["consistent"], report["source"]) == (
            "pandas_market_calendars_unavailable",
            False,
            "fallback",
        )
        # A fallback on a day inside the XNYS range is what the supervisor alerts on.
        assert report["exchange_calendars_xnys"]["in_range"] is True
        assert report["pandas_market_calendars_nyse"]["error"] == (
            f"RuntimeError: simulated upstream failure {attempt}"
        )
        assert _session(CHRISTMAS_EVE, 17, 30) == "AFTER_HOURS"
        assert len(calls) == attempt

    # Upstream works again: the same check loads it and the same day is healed, without waiting
    # for another day to be looked up.
    report = engine.session_calendar_consistency(CHRISTMAS_EVE)
    assert len(calls) == 4
    assert (report["reason"], report["consistent"], report["source"]) == ("ok", True, "upstream")
    assert report["bounds_hm"] == [240, 570, 780, 1020]
    assert _session(CHRISTMAS_EVE, 17, 30) == "CLOSED"
    assert _session(CHRISTMAS_EVE, 16, 59) == "AFTER_HOURS"
    engine.session_calendar_consistency(CHRISTMAS_EVE)
    assert len(calls) == 4


def test_one_malformed_upstream_row_costs_only_that_day(
    fresh_session_clock: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    bad_normal_day = date(2026, 11, 24)  # a regular Tuesday session, followed by a session
    bad_early_close = date(2025, 11, 28)
    bad_days = (bad_normal_day, bad_early_close)
    real_calendar = mcal.get_calendar("NYSE")

    class _TwoUnreadableRows:
        def schedule(self, **kwargs: Any) -> Any:
            frame = real_calendar.schedule(**kwargs)
            unreadable = [stamp.date() in bad_days for stamp in frame.index]
            assert sum(unreadable) == len(bad_days)
            frame.loc[unreadable, "post"] = None  # stored as NaT
            return frame

    monkeypatch.setattr(mcal, "get_calendar", lambda *_args, **_kwargs: _TwoUnreadableRows())

    table, load_error = engine._nyse_extended_hours_table()
    assert load_error is None
    assert table[EARLY_CLOSE_DAY] == (240, 570, 780, 1020)
    assert all(isinstance(table[day], str) for day in bad_days)

    # Every other day keeps its upstream bounds.
    assert _session(EARLY_CLOSE_DAY, 17, 30) == "CLOSED"
    assert _session(CHRISTMAS_EVE, 17, 30) == "CLOSED"
    assert engine.session_calendar_consistency(EARLY_CLOSE_DAY)["reason"] == "ok"

    # An unreadable day keeps the pre-upstream bounds, says why, and never raises.
    expected_clock = {
        bad_normal_day: ["04:00", "09:30", "16:00", "20:00"],
        bad_early_close: ["04:00", "09:30", "13:00", "20:00"],
    }
    for day in bad_days:
        report = engine.session_calendar_consistency(day)
        assert (report["reason"], report["consistent"], report["source"]) == (
            "pandas_market_calendars_row_invalid",
            False,
            "fallback",
        ), day
        assert report["exchange_calendars_xnys"]["is_session"] is True, day
        assert report["pandas_market_calendars_nyse"]["is_session"] is None, day
        assert report["pandas_market_calendars_nyse"]["error"] == table[day], day
        bounds = engine.us_equity_session_bounds_et(day)
        assert bounds is not None, day
        assert (bounds.source, _clock(bounds)) == ("fallback", expected_clock[day]), day
    marks = ((3, 59), (4, 0), (9, 30), (16, 0), (19, 59), (20, 0))
    assert [_session(bad_normal_day, hh, mm) for hh, mm in marks] == [
        "OVERNIGHT",
        "PRE_MARKET",
        "RTH",
        "AFTER_HOURS",
        "AFTER_HOURS",
        "OVERNIGHT",
    ]
    # Known residual: an unreadable row on an early close leaves that day on the 20:00 fallback.
    assert _session(bad_early_close, 17, 30) == "AFTER_HOURS"

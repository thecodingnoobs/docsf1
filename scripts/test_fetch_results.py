#!/usr/bin/env python3
"""
Tests for fetch_results.py — covers (a) the scheduling logic that decides
which sessions get (re)fetched and (b) build_results, which translates
OpenF1 endpoint responses into the JSON schema the app consumes. The
HTTP transport itself is a thin wrapper over requests and isn't covered
here; build_results tests stub out fetch() so no network calls happen.

Run with: python -m pytest scripts/test_fetch_results.py -v
      or: python scripts/test_fetch_results.py
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from fetch_results import (
    PRE_FETCH_BUFFER,
    REFRESH_WINDOW,
    build_results,
    is_session_due,
    find_pending_rounds,
)


SESSION_START = datetime(2026, 5, 3, 17, 0, tzinfo=timezone.utc)
SESSION_START_STR = "2026-05-03T17:00:00Z"


def at(offset):
    """Helper: return a `now` value at SESSION_START + offset."""
    return SESSION_START + offset


class IsSessionDueFirstFetch(unittest.TestCase):
    """current_value is None — gating the *first* write."""

    def test_future_session_not_due(self):
        # Cron fires the day before the race — must NOT fetch.
        self.assertFalse(is_session_due(
            SESSION_START_STR, None, at(-timedelta(hours=24)),
        ))

    def test_just_started_not_due(self):
        # Race is mid-flight — OpenF1 may have partial data; don't persist it.
        self.assertFalse(is_session_due(
            SESSION_START_STR, None, at(timedelta(minutes=30)),
        ))

    def test_within_buffer_not_due(self):
        self.assertFalse(is_session_due(
            SESSION_START_STR, None, at(PRE_FETCH_BUFFER - timedelta(minutes=1)),
        ))

    def test_just_past_buffer_is_due(self):
        self.assertTrue(is_session_due(
            SESSION_START_STR, None, at(PRE_FETCH_BUFFER + timedelta(minutes=1)),
        ))

    def test_long_after_session_still_due_when_empty(self):
        # Old race we never managed to ingest — keep trying.
        self.assertTrue(is_session_due(
            SESSION_START_STR, None, at(timedelta(days=30)),
        ))

    def test_missing_time_str_not_due(self):
        self.assertFalse(is_session_due(None, None, at(timedelta(hours=24))))
        self.assertFalse(is_session_due("", None, at(timedelta(hours=24))))


class IsSessionDueRefresh(unittest.TestCase):
    """current_value is populated — gating *re*-writes."""

    POPULATED = [{"position": 1, "driver_id": "verstappen"}]

    def test_within_refresh_window_is_due(self):
        # 24h after race start, FIA may have applied a penalty — refresh.
        self.assertTrue(is_session_due(
            SESSION_START_STR, self.POPULATED, at(timedelta(hours=24)),
        ))

    def test_just_past_refresh_window_not_due(self):
        # 48h is the cutoff; data is treated as final after that.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(REFRESH_WINDOW + timedelta(minutes=1)),
        ))

    def test_long_after_window_not_due(self):
        # Old archived rounds shouldn't burn API calls on every cron run.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(timedelta(days=30)),
        ))

    def test_future_populated_not_due(self):
        # Defensive: if some prior buggy run wrote data for an unstarted
        # session, don't refresh until the session actually starts.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(-timedelta(hours=12)),
        ))

    def test_during_session_populated_not_due(self):
        # Race still in progress — refreshing now would replace good
        # finalised data with partial in-flight OpenF1 output.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(timedelta(minutes=30)),
        ))

    def test_within_buffer_populated_not_due(self):
        # Same buffer applies to refresh as to first-fetch.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(PRE_FETCH_BUFFER - timedelta(minutes=1)),
        ))

    def test_just_past_buffer_populated_is_due(self):
        # As soon as the buffer clears, refresh becomes available.
        self.assertTrue(is_session_due(
            SESSION_START_STR, self.POPULATED, at(PRE_FETCH_BUFFER + timedelta(minutes=1)),
        ))


class IsSessionDueRefreshAll(unittest.TestCase):
    """refresh_window=None — manual runs with --refresh-all lift the cap."""

    POPULATED = [{"position": 1, "driver_id": "verstappen"}]

    def test_old_populated_due_when_window_lifted(self):
        # With the cap lifted, even an archival round refreshes.
        self.assertTrue(is_session_due(
            SESSION_START_STR, self.POPULATED, at(timedelta(days=30)),
            refresh_window=None,
        ))

    def test_buffer_still_applies(self):
        # --refresh-all does NOT bypass the buffer — we still don't fetch
        # mid-session, otherwise we'd overwrite good data with partial
        # in-flight OpenF1 output.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(timedelta(minutes=30)),
            refresh_window=None,
        ))

    def test_future_session_still_skipped(self):
        # --refresh-all does NOT bypass the "not started yet" check.
        self.assertFalse(is_session_due(
            SESSION_START_STR, self.POPULATED, at(-timedelta(hours=12)),
            refresh_window=None,
        ))

    def test_unpopulated_session_unaffected(self):
        # First-fetch behaviour is identical regardless of refresh_window.
        self.assertTrue(is_session_due(
            SESSION_START_STR, None, at(timedelta(days=30)),
            refresh_window=None,
        ))


class FindPendingRoundsBehaviour(unittest.TestCase):
    """End-to-end-ish: schedule + results fixture, check round selection."""

    def _fixture(self, race_results=None, qualifying_results=None):
        schedule = {"races": [{
            "round": 6,
            "circuit_id": "miami",
            "cancelled": False,
            "sessions": {
                "qualifying":        "2026-05-02T20:00:00Z",  # Sat
                "sprint_qualifying": "2026-05-01T20:30:00Z",
                "sprint_race":       "2026-05-02T16:00:00Z",
                "race":              "2026-05-03T17:00:00Z",  # Sun
            },
        }]}
        results = {"races": [{
            "round": 6,
            "grand_prix": "Miami Grand Prix",
            "race_results": race_results,
            "sprint_results": None,
            "qualifying_results": qualifying_results,
            "sprint_qualifying_results": None,
        }]}
        return results, schedule

    def test_future_round_with_no_data_is_skipped(self):
        # Two days before any session — nothing should be due.
        results, schedule = self._fixture()
        now = datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(find_pending_rounds(results, schedule, now=now), [])

    def test_partial_weekend_picks_round_for_due_sessions(self):
        # Sat 22:00 UTC: sprint quali, sprint race, quali have all ended;
        # race is the next day. The round is pending because of those three;
        # the per-session guards (tested implicitly via is_session_due) will
        # NOT fetch the future race.
        results, schedule = self._fixture()
        now = datetime(2026, 5, 2, 22, 0, tzinfo=timezone.utc)
        pending = find_pending_rounds(results, schedule, now=now)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0][0], 6)

    def test_freshly_populated_round_still_pending_for_refresh(self):
        # Race finished and data was written, but it's only been 6h.
        # Round should remain pending so a stewards' update can land.
        results, schedule = self._fixture(
            race_results=[{"position": 1, "driver_id": "norris"}],
            qualifying_results=[{"position": 1, "driver_id": "norris"}],
        )
        now = datetime(2026, 5, 3, 23, 0, tzinfo=timezone.utc)  # 6h post race
        pending = find_pending_rounds(results, schedule, now=now)
        self.assertEqual(len(pending), 1)

    def test_old_populated_round_is_locked(self):
        # All sessions populated and >48h old: round should NOT be pending,
        # even though data exists. This is what stops cron runs from
        # pointlessly re-pulling every old archived round.
        results, schedule = self._fixture(
            race_results=[{"position": 1, "driver_id": "norris"}],
            qualifying_results=[{"position": 1, "driver_id": "norris"}],
        )
        results["races"][0]["sprint_results"]            = [{"position": 1}]
        results["races"][0]["sprint_qualifying_results"] = [{"position": 1}]
        now = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)  # ~3 days later
        self.assertEqual(find_pending_rounds(results, schedule, now=now), [])

    def test_force_rounds_overrides_locked_round(self):
        # force_rounds re-pulls regardless of age or population state.
        results, schedule = self._fixture(
            race_results=[{"position": 1, "driver_id": "norris"}],
            qualifying_results=[{"position": 1, "driver_id": "norris"}],
        )
        results["races"][0]["sprint_results"]            = [{"position": 1}]
        results["races"][0]["sprint_qualifying_results"] = [{"position": 1}]
        now = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)
        pending = find_pending_rounds(results, schedule, force_rounds={6}, now=now)
        self.assertEqual(len(pending), 1)

    def test_refresh_all_unlocks_archived_round(self):
        # With refresh_window=None (manual --refresh-all), an archived
        # round (>48h, fully populated) is pending again.
        results, schedule = self._fixture(
            race_results=[{"position": 1, "driver_id": "norris"}],
            qualifying_results=[{"position": 1, "driver_id": "norris"}],
        )
        results["races"][0]["sprint_results"]            = [{"position": 1}]
        results["races"][0]["sprint_qualifying_results"] = [{"position": 1}]
        now = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)
        pending = find_pending_rounds(results, schedule, now=now, refresh_window=None)
        self.assertEqual(len(pending), 1)

    def test_cancelled_round_is_skipped(self):
        results, schedule = self._fixture()
        schedule["races"][0]["cancelled"] = True
        now = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(find_pending_rounds(results, schedule, now=now), [])


# ---------------------------------------------------------------------------
# build_results — translates OpenF1 endpoint responses into our JSON schema.
# Each test stubs the fetch() function with shaped fixtures so no real
# network calls happen, then asserts the resulting result rows.
# ---------------------------------------------------------------------------

# Shared minimal driver fixture: 3 drivers across 3 teams, real OpenF1 keys.
DRIVERS_FIXTURE = [
    {"driver_number": 12, "name_acronym": "ANT", "first_name": "Andrea Kimi",
     "last_name": "Antonelli", "team_name": "Mercedes"},
    {"driver_number": 1,  "name_acronym": "VER", "first_name": "Max",
     "last_name": "Verstappen", "team_name": "Red Bull Racing"},
    {"driver_number": 16, "name_acronym": "LEC", "first_name": "Charles",
     "last_name": "Leclerc", "team_name": "Ferrari"},
]


def _patch_fetch(session_result, drivers=None, laps=None):
    """Patch fetch() to dispatch by endpoint. Other endpoints raise so a
    refactor that calls a stale endpoint surfaces immediately in tests."""
    drivers = drivers if drivers is not None else DRIVERS_FIXTURE
    laps    = laps    if laps    is not None else []

    def fake_fetch(endpoint, **params):
        if endpoint == "session_result":
            return session_result
        if endpoint == "drivers":
            return drivers
        if endpoint == "laps":
            return laps
        raise AssertionError(f"build_results should not call /{endpoint}")
    return patch("fetch_results.fetch", side_effect=fake_fetch)


class BuildResultsClassified(unittest.TestCase):

    def test_winner_has_blank_time(self):
        with _patch_fetch(session_result=[
            {"position": 1, "driver_number": 12, "number_of_laps": 57,
             "points": 25.0, "dnf": False, "dns": False, "dsq": False,
             "duration": 5400.0, "gap_to_leader": 0},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["time"], "")
        self.assertEqual(results[0]["status"], "Finished")
        self.assertEqual(results[0]["position"], 1)
        self.assertEqual(results[0]["points"], 25)
        self.assertEqual(results[0]["driver_id"], "antonelli")
        self.assertEqual(results[0]["team_id"], "mercedes")

    def test_float_gap_formatted_to_three_decimals(self):
        # The whole reason for this refactor: post-penalty gap from
        # /session_result must reach the JSON intact. e.g. VER's 5s
        # penalty in Miami 2026 turns +43.949s into +48.949s.
        with _patch_fetch(session_result=[
            {"position": 1, "driver_number": 12, "number_of_laps": 57,
             "points": 25.0, "dnf": False, "dns": False, "dsq": False,
             "duration": 5400.0, "gap_to_leader": 0},
            {"position": 2, "driver_number": 1,  "number_of_laps": 57,
             "points": 18.0, "dnf": False, "dns": False, "dsq": False,
             "duration": None, "gap_to_leader": 48.949},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[1]["time"], "+48.949s")

    def test_string_gap_passed_through(self):
        # Lapped drivers come back as "+1 LAP" / "+2 LAPS" strings.
        with _patch_fetch(session_result=[
            {"position": 1, "driver_number": 12, "number_of_laps": 57,
             "points": 25.0, "dnf": False, "dns": False, "dsq": False,
             "duration": 5400.0, "gap_to_leader": 0},
            {"position": 2, "driver_number": 1,  "number_of_laps": 56,
             "points": 18.0, "dnf": False, "dns": False, "dsq": False,
             "duration": None, "gap_to_leader": "+1 LAP"},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[1]["time"], "+1 LAP")

    def test_points_come_from_openf1_not_position_table(self):
        # Pre-refactor we computed points = points_scale[pos-1]. Now we
        # trust /session_result.points so penalty-induced points changes
        # land correctly. Verify by giving the winner non-standard points.
        with _patch_fetch(session_result=[
            {"position": 1, "driver_number": 12, "number_of_laps": 57,
             "points": 22.0, "dnf": False, "dns": False, "dsq": False,
             "duration": 5400.0, "gap_to_leader": 0},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[0]["points"], 22)


class BuildResultsRetirements(unittest.TestCase):

    def test_dnf_status_and_synthetic_position(self):
        # OpenF1 returns position=null for retirees. We assign synthetic
        # positions after the last classified row so the output schema
        # stays "every row has a numeric position."
        with _patch_fetch(session_result=[
            {"position": 1, "driver_number": 12, "number_of_laps": 57,
             "points": 25.0, "dnf": False, "dns": False, "dsq": False,
             "duration": 5400.0, "gap_to_leader": 0},
            {"position": None, "driver_number": 1, "number_of_laps": 8,
             "points": 0.0, "dnf": True, "dns": False, "dsq": False,
             "duration": None, "gap_to_leader": None},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[1]["status"], "DNF")
        self.assertEqual(results[1]["time"], "DNF")
        self.assertEqual(results[1]["position"], 2)
        self.assertEqual(results[1]["points"], 0)

    def test_retirees_ordered_by_laps_completed_desc(self):
        with _patch_fetch(session_result=[
            {"position": 1, "driver_number": 12, "number_of_laps": 57,
             "points": 25.0, "dnf": False, "dns": False, "dsq": False,
             "duration": 5400.0, "gap_to_leader": 0},
            # Two DNFs, intentionally listed laps-asc to verify we sort.
            {"position": None, "driver_number": 16, "number_of_laps": 3,
             "points": 0.0, "dnf": True, "dns": False, "dsq": False,
             "duration": None, "gap_to_leader": None},
            {"position": None, "driver_number": 1,  "number_of_laps": 20,
             "points": 0.0, "dnf": True, "dns": False, "dsq": False,
             "duration": None, "gap_to_leader": None},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[1]["driver_code"], "VER")  # 20 laps
        self.assertEqual(results[2]["driver_code"], "LEC")  # 3 laps

    def test_dns_status_takes_precedence_over_gap(self):
        # Defensive: even if OpenF1 emits both a gap and a DNS flag, the
        # status flag wins because the driver never started.
        with _patch_fetch(session_result=[
            {"position": None, "driver_number": 12, "number_of_laps": 0,
             "points": 0.0, "dnf": False, "dns": True, "dsq": False,
             "duration": None, "gap_to_leader": 9999.0},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[0]["status"], "DNS")
        self.assertEqual(results[0]["time"], "DNS")

    def test_dsq_status(self):
        # DSQs (rare; e.g. Hamilton 2024 floor wear) are now distinguished
        # from DNFs in our output instead of being conflated.
        with _patch_fetch(session_result=[
            {"position": None, "driver_number": 12, "number_of_laps": 57,
             "points": 0.0, "dnf": False, "dns": False, "dsq": True,
             "duration": None, "gap_to_leader": None},
        ]):
            results, _, _ = build_results(11280)
        self.assertEqual(results[0]["status"], "DSQ")
        self.assertEqual(results[0]["time"], "DSQ")


class BuildResultsFastestLap(unittest.TestCase):

    def test_fastest_lap_flagged_on_correct_driver(self):
        # /laps drives the fastest_lap flag and best_lap_seconds — the
        # session_result endpoint doesn't expose either.
        with _patch_fetch(
            session_result=[
                {"position": 1, "driver_number": 12, "number_of_laps": 57,
                 "points": 25.0, "dnf": False, "dns": False, "dsq": False,
                 "duration": 5400.0, "gap_to_leader": 0},
                {"position": 2, "driver_number": 1, "number_of_laps": 57,
                 "points": 18.0, "dnf": False, "dns": False, "dsq": False,
                 "duration": None, "gap_to_leader": 3.264},
            ],
            laps=[
                {"driver_number": 12, "lap_number": 1, "lap_duration": 92.000},
                {"driver_number": 1,  "lap_number": 1, "lap_duration": 91.234},
            ],
        ):
            results, fastest_name, best_secs = build_results(11280)
        self.assertEqual(fastest_name, "M. Verstappen")
        self.assertAlmostEqual(best_secs, 91.234)
        self.assertFalse(results[0]["fastest_lap"])
        self.assertTrue(results[1]["fastest_lap"])


class BuildResultsEmpty(unittest.TestCase):

    def test_empty_session_result_returns_none(self):
        # Session not finished or OpenF1 hasn't published yet — caller
        # uses None to mean "skip writing, retry next run."
        with _patch_fetch(session_result=[]):
            results, fastest_name, best_secs = build_results(11280)
        self.assertIsNone(results)
        self.assertIsNone(fastest_name)
        self.assertIsNone(best_secs)


if __name__ == "__main__":
    unittest.main()

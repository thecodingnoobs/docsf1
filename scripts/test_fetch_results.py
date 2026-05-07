#!/usr/bin/env python3
"""
Tests for fetch_results.py — focused on the scheduling logic that decides
which sessions get (re)fetched. The OpenF1 fetching itself is a thin
wrapper over requests and isn't covered here.

Run with: python -m pytest scripts/test_fetch_results.py -v
      or: python scripts/test_fetch_results.py
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fetch_results import (
    PRE_FETCH_BUFFER,
    REFRESH_WINDOW,
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


if __name__ == "__main__":
    unittest.main()

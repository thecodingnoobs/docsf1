#!/usr/bin/env python3
"""
Tests for update_standings.py.

Each test uses minimal fixture data so failures are easy to diagnose.
Run with: python -m pytest scripts/test_update_standings.py -v
      or: python scripts/test_update_standings.py
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

# Allow importing update_standings from the same directory
sys.path.insert(0, str(Path(__file__).parent))
from update_standings import (
    compute_season_stats,
    compute_season_points,
    compute_race_wins,
    compute_h2h,
    compute_constructor_stats,
)

# ---------------------------------------------------------------------------
# Minimal fixture: 2 drivers, 2 teams, 2 races (one with sprint)
# ---------------------------------------------------------------------------

FIXTURE_RESULTS = {
    "races": [
        {
            "round": 1,
            "grand_prix": "Alpha GP",
            "sprint_results": None,
            "race_results": [
                {"position": 1, "driver_id": "alpha", "team_id": "team_a", "points": 25, "status": "Finished"},
                {"position": 2, "driver_id": "beta",  "team_id": "team_b", "points": 18, "status": "Finished"},
                {"position": 3, "driver_id": "gamma", "team_id": "team_a", "points": 15, "status": "Finished"},
                {"position": 4, "driver_id": "delta", "team_id": "team_b", "points": 12, "status": "Finished"},
            ],
        },
        {
            "round": 2,
            "grand_prix": "Beta GP",
            "sprint_results": [
                {"position": 1, "driver_id": "beta",  "team_id": "team_b", "points": 8, "status": "Finished"},
                {"position": 2, "driver_id": "alpha", "team_id": "team_a", "points": 7, "status": "Finished"},
                {"position": 3, "driver_id": "delta", "team_id": "team_b", "points": 6, "status": "Finished"},
                {"position": 4, "driver_id": "gamma", "team_id": "team_a", "points": 5, "status": "Finished"},
            ],
            "race_results": [
                {"position": 1, "driver_id": "beta",  "team_id": "team_b", "points": 25, "status": "Finished"},
                {"position": 2, "driver_id": "gamma", "team_id": "team_a", "points": 18, "status": "Finished"},
                {"position": 3, "driver_id": "alpha", "team_id": "team_a", "points": 15, "status": "Finished"},
                {"position": 4, "driver_id": "delta", "team_id": "team_b", "points": 12, "status": "Finished"},
            ],
        },
        {
            "round": 3,
            "grand_prix": "Future GP",
            "sprint_results": None,
            "race_results": None,  # not yet run
        },
    ]
}

FIXTURE_STANDINGS = {
    "after_round": 0,
    "after_race": "",
    "driver_standings": [
        {
            "position": 1, "driver_id": "alpha", "driver_code": "ALP",
            "points": 0, "wins": 0,
            "h2h_teammate_id": "gamma", "h2h_teammate_code": "GAM",
            "h2h_driver_ahead": 0, "h2h_teammate_ahead": 0,
            "career_wins": 0, "career_races": 0, "career_podiums": 0, "career_top10s": 0,
            "season_wins": 99,  # legacy field — should be removed
        },
        {
            "position": 2, "driver_id": "beta", "driver_code": "BET",
            "points": 0, "wins": 0,
            "h2h_teammate_id": "delta", "h2h_teammate_code": "DEL",
            "h2h_driver_ahead": 0, "h2h_teammate_ahead": 0,
            "career_wins": 0, "career_races": 0, "career_podiums": 0, "career_top10s": 0,
        },
        {
            "position": 3, "driver_id": "gamma", "driver_code": "GAM",
            "points": 0, "wins": 0,
            "h2h_teammate_id": "alpha", "h2h_teammate_code": "ALP",
            "h2h_driver_ahead": 0, "h2h_teammate_ahead": 0,
            "career_wins": 0, "career_races": 0, "career_podiums": 0, "career_top10s": 0,
        },
        {
            "position": 4, "driver_id": "delta", "driver_code": "DEL",
            "points": 0, "wins": 0,
            "h2h_teammate_id": "beta", "h2h_teammate_code": "BET",
            "h2h_driver_ahead": 0, "h2h_teammate_ahead": 0,
            "career_wins": 0, "career_races": 0, "career_podiums": 0, "career_top10s": 0,
        },
    ],
    "constructor_standings": [
        {"position": 1, "team_id": "team_a", "team_name": "Team A", "points": 0, "wins": 0},
        {"position": 2, "team_id": "team_b", "team_name": "Team B", "points": 0, "wins": 0},
    ],
}


def run_main_on_fixture(results: dict, standings: dict) -> dict:
    """Write fixture data to temp files, run main(), return updated standings."""
    import update_standings as us

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "scripts").mkdir()
        results_file   = root / "f1_2026_results.json"
        standings_file = root / "f1_2026_standings.json"
        results_file.write_text(json.dumps(results),   encoding="utf-8")
        standings_file.write_text(json.dumps(standings), encoding="utf-8")

        orig_rf, orig_sf = us.RESULTS_FILE, us.STANDINGS_FILE
        us.RESULTS_FILE   = results_file
        us.STANDINGS_FILE = standings_file
        try:
            us.main()
            return json.loads(standings_file.read_text(encoding="utf-8"))
        finally:
            us.RESULTS_FILE   = orig_rf
            us.STANDINGS_FILE = orig_sf


class TestComputeSeasonPoints(unittest.TestCase):
    def test_sums_race_and_sprint_points(self):
        pts = compute_season_points(FIXTURE_RESULTS)
        # alpha: R1 race 25 + R2 sprint 7 + R2 race 15 = 47
        self.assertEqual(pts["alpha"], 47)
        # beta: R1 race 18 + R2 sprint 8 + R2 race 25 = 51
        self.assertEqual(pts["beta"], 51)

    def test_ignores_null_results(self):
        pts = compute_season_points(FIXTURE_RESULTS)
        # Round 3 has null results — should not crash or add phantom points
        self.assertNotIn("future_driver", pts)


class TestComputeRaceWins(unittest.TestCase):
    def test_counts_only_race_wins_not_sprint(self):
        wins = compute_race_wins(FIXTURE_RESULTS)
        self.assertEqual(wins.get("alpha", 0), 1)  # won R1 race
        self.assertEqual(wins.get("beta",  0), 1)  # won R2 race
        self.assertEqual(wins.get("gamma", 0), 0)
        self.assertEqual(wins.get("delta", 0), 0)

    def test_sprint_win_not_counted(self):
        # beta won the R2 sprint — should NOT appear as a second race win
        wins = compute_race_wins(FIXTURE_RESULTS)
        self.assertEqual(wins["beta"], 1)


class TestComputeH2H(unittest.TestCase):
    def setUp(self):
        self.h2h = compute_h2h(FIXTURE_RESULTS, FIXTURE_STANDINGS)

    def test_alpha_vs_gamma(self):
        # R1: alpha P1 vs gamma P3 → alpha ahead
        # R2: gamma P2 vs alpha P3 → gamma ahead
        self.assertEqual(self.h2h["alpha"], 1)
        self.assertEqual(self.h2h["gamma"], 1)

    def test_beta_vs_delta(self):
        # R1: beta P2 vs delta P4 → beta ahead
        # R2: beta P1 vs delta P4 → beta ahead
        self.assertEqual(self.h2h["beta"],  2)
        self.assertEqual(self.h2h["delta"], 0)

    def test_ignores_null_races(self):
        # Round 3 has no results — should not affect counts
        self.assertEqual(self.h2h["alpha"], 1)

    def test_each_pair_counted_once_per_race(self):
        # alpha/gamma are teammates — total races counted should equal completed races (2)
        self.assertEqual(self.h2h["alpha"] + self.h2h["gamma"], 2)


class TestComputeConstructorStats(unittest.TestCase):
    def test_points_include_race_and_sprint(self):
        pts, _ = compute_constructor_stats(FIXTURE_RESULTS)
        # team_a: alpha(25+7+15) + gamma(15+5+18) = 47 + 38 = 85
        self.assertEqual(pts["team_a"], 85)
        # team_b: beta(18+8+25) + delta(12+6+12) = 51 + 30 = 81
        self.assertEqual(pts["team_b"], 81)

    def test_wins_race_only(self):
        _, wins = compute_constructor_stats(FIXTURE_RESULTS)
        self.assertEqual(wins.get("team_a", 0), 1)  # alpha won R1
        self.assertEqual(wins.get("team_b", 0), 1)  # beta won R2


class TestMainIntegration(unittest.TestCase):
    """
    End-to-end: run main() on fixture data and assert the output JSON is correct.
    These are the tests that would have caught every bug we fixed.
    """

    def setUp(self):
        import copy
        self.out = run_main_on_fixture(
            FIXTURE_RESULTS,
            json.loads(json.dumps(FIXTURE_STANDINGS)),  # deep copy
        )

    def test_after_round_updated(self):
        self.assertEqual(self.out["after_round"], 2)

    def test_after_race_updated(self):
        self.assertEqual(self.out["after_race"], "Beta GP")

    def test_driver_points_correct(self):
        pts = {d["driver_id"]: d["points"] for d in self.out["driver_standings"]}
        self.assertEqual(pts["alpha"], 47)
        self.assertEqual(pts["beta"],  51)
        self.assertEqual(pts["gamma"], 38)
        self.assertEqual(pts["delta"], 30)

    def test_driver_wins_uses_race_wins_only(self):
        wins = {d["driver_id"]: d["wins"] for d in self.out["driver_standings"]}
        self.assertEqual(wins["alpha"], 1)
        self.assertEqual(wins["beta"],  1)
        self.assertEqual(wins["gamma"], 0)
        self.assertEqual(wins["delta"], 0)

    def test_no_season_wins_field(self):
        for d in self.out["driver_standings"]:
            self.assertNotIn("season_wins", d, f"{d['driver_id']} still has season_wins field")

    def test_driver_standings_sorted_by_points(self):
        pts = [d["points"] for d in self.out["driver_standings"]]
        self.assertEqual(pts, sorted(pts, reverse=True))

    def test_driver_positions_sequential(self):
        positions = [d["position"] for d in self.out["driver_standings"]]
        self.assertEqual(positions, list(range(1, len(positions) + 1)))

    def test_h2h_driver_ahead_correct(self):
        h2h = {d["driver_id"]: d["h2h_driver_ahead"] for d in self.out["driver_standings"]}
        self.assertEqual(h2h["alpha"], 1)
        self.assertEqual(h2h["gamma"], 1)
        self.assertEqual(h2h["beta"],  2)
        self.assertEqual(h2h["delta"], 0)

    def test_h2h_teammate_ahead_mirrors(self):
        # h2h_teammate_ahead for alpha should equal h2h_driver_ahead for gamma
        by_id = {d["driver_id"]: d for d in self.out["driver_standings"]}
        self.assertEqual(
            by_id["alpha"]["h2h_teammate_ahead"],
            by_id["gamma"]["h2h_driver_ahead"],
        )

    def test_constructor_points_correct(self):
        pts = {c["team_id"]: c["points"] for c in self.out["constructor_standings"]}
        self.assertEqual(pts["team_a"], 85)
        self.assertEqual(pts["team_b"], 81)

    def test_constructor_wins_correct(self):
        wins = {c["team_id"]: c["wins"] for c in self.out["constructor_standings"]}
        self.assertEqual(wins["team_a"], 1)
        self.assertEqual(wins["team_b"], 1)

    def test_constructor_standings_sorted_by_points(self):
        pts = [c["points"] for c in self.out["constructor_standings"]]
        self.assertEqual(pts, sorted(pts, reverse=True))

    def test_constructor_positions_sequential(self):
        positions = [c["position"] for c in self.out["constructor_standings"]]
        self.assertEqual(positions, list(range(1, len(positions) + 1)))


if __name__ == "__main__":
    unittest.main(verbosity=2)

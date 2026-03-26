#!/usr/bin/env python3
"""
Updates career stats and season stats in f1_2026_standings.json after each race.

All career_* fields = pre-2026 baseline + what's been accumulated in 2026 results.
The pre-2026 baseline is fixed for the season — update once at the start of 2027.
"""

import json
from pathlib import Path

ROOT           = Path(__file__).parent.parent
STANDINGS_FILE = ROOT / "f1_2026_standings.json"
RESULTS_FILE   = ROOT / "f1_2026_results.json"

# Verified from Jolpica (paginated), includes through end of 2025.
# Only update these at the start of a new season.
PRE_2026 = {
    "russell":    {"wins": 5,   "races": 152, "podiums": 24,  "top10": 86},
    "antonelli":  {"wins": 0,   "races": 24,  "podiums": 3,   "top10": 14},
    "leclerc":    {"wins": 8,   "races": 173, "podiums": 50,  "top10": 132},
    "hamilton":   {"wins": 105, "races": 380, "podiums": 202, "top10": 331},
    "bearman":    {"wins": 0,   "races": 27,  "podiums": 0,   "top10": 11},
    "norris":     {"wins": 11,  "races": 152, "podiums": 44,  "top10": 121},
    "gasly":      {"wins": 1,   "races": 178, "podiums": 5,   "top10": 75},
    "verstappen": {"wins": 71,  "races": 233, "podiums": 127, "top10": 195},
    "lawson":     {"wins": 0,   "races": 35,  "podiums": 0,   "top10": 10},
    "lindblad":   {"wins": 0,   "races": 0,   "podiums": 0,   "top10": 0},
    "hadjar":     {"wins": 0,   "races": 24,  "podiums": 1,   "top10": 10},
    "piastri":    {"wins": 9,   "races": 70,  "podiums": 26,  "top10": 56},
    "sainz":      {"wins": 4,   "races": 232, "podiums": 29,  "top10": 149},
    "bortoleto":  {"wins": 0,   "races": 24,  "podiums": 0,   "top10": 5},
    "colapinto":  {"wins": 0,   "races": 27,  "podiums": 0,   "top10": 2},
    "ocon":       {"wins": 1,   "races": 180, "podiums": 4,   "top10": 94},
    "hulkenberg": {"wins": 0,   "races": 254, "podiums": 1,   "top10": 117},
    "albon":      {"wins": 0,   "races": 129, "podiums": 2,   "top10": 50},
    "bottas":     {"wins": 10,  "races": 247, "podiums": 67,  "top10": 145},
    "perez":      {"wins": 6,   "races": 283, "podiums": 39,  "top10": 186},
    "alonso":     {"wins": 32,  "races": 428, "podiums": 106, "top10": 281},
    "stroll":     {"wins": 0,   "races": 191, "podiums": 3,   "top10": 66},
}


def compute_season_stats(results_data: dict) -> dict:
    """Count wins/races/podiums/top10s per driver from 2026 results."""
    stats: dict[str, dict] = {}

    for race in results_data["races"]:
        race_results = race.get("race_results") or []
        sprint_results = race.get("sprint_results") or []

        # Count race starts from the main race only
        for entry in race_results:
            did = entry["driver_id"]
            if did not in stats:
                stats[did] = {"wins": 0, "races": 0, "podiums": 0, "top10": 0}
            stats[did]["races"] += 1

        for entry in race_results + sprint_results:
            did = entry["driver_id"]
            if did not in stats:
                stats[did] = {"wins": 0, "races": 0, "podiums": 0, "top10": 0}
            pos = entry.get("position", 99)
            if entry.get("status") == "Finished":
                if pos == 1:
                    stats[did]["wins"] += 1
                if pos <= 3:
                    stats[did]["podiums"] += 1
                if pos <= 10:
                    stats[did]["top10"] += 1

    return stats


def main():
    standings = json.loads(STANDINGS_FILE.read_text(encoding="utf-8"))
    results   = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))

    season = compute_season_stats(results)
    changed = False

    for drv in standings["driver_standings"]:
        did  = drv["driver_id"]
        pre  = PRE_2026.get(did, {"wins": 0, "races": 0, "podiums": 0, "top10": 0})
        s    = season.get(did, {"wins": 0, "races": 0, "podiums": 0, "top10": 0})

        updates = {
            "season_wins":    s["wins"],
            "career_wins":    pre["wins"]    + s["wins"],
            "career_races":   pre["races"]   + s["races"],
            "career_podiums": pre["podiums"] + s["podiums"],
            "career_top10s":  pre["top10"]   + s["top10"],
        }
        for key, val in updates.items():
            if drv.get(key) != val:
                drv[key] = val
                changed = True

    if changed:
        STANDINGS_FILE.write_text(
            json.dumps(standings, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Updated {STANDINGS_FILE}")
    else:
        print("No changes needed.")


if __name__ == "__main__":
    main()

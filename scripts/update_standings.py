#!/usr/bin/env python3
"""
Updates career_wins and season_wins in f1_2026_standings.json from race results.

career_wins = PRE_2026_WINS[driver] + wins counted from f1_2026_results.json

The pre-2026 baseline is fixed for the season — no external API calls needed.
Update PRE_2026_WINS once at the start of each new season.
"""

import json
from pathlib import Path

ROOT           = Path(__file__).parent.parent
STANDINGS_FILE = ROOT / "f1_2026_standings.json"
RESULTS_FILE   = ROOT / "f1_2026_results.json"

# Career wins before the 2026 season (verified from Jolpica, paginated).
# Fixed for the entire 2026 season — only update at the start of 2027.
PRE_2026_WINS = {
    "russell":    5,
    "antonelli":  0,
    "leclerc":    8,
    "hamilton":   105,
    "bearman":    0,
    "norris":     11,
    "gasly":      1,
    "verstappen": 71,
    "lawson":     0,
    "lindblad":   0,
    "hadjar":     0,
    "piastri":    9,
    "sainz":      4,
    "bortoleto":  0,
    "colapinto":  0,
    "ocon":       1,
    "hulkenberg": 0,
    "albon":      0,
    "bottas":     10,
    "perez":      6,
    "alonso":     32,
    "stroll":     0,
}


def count_season_wins(results_data: dict) -> dict:
    wins: dict[str, int] = {}
    for race in results_data["races"]:
        for result_set in (race.get("race_results"), race.get("sprint_results")):
            if not result_set:
                continue
            for entry in result_set:
                if entry.get("position") == 1 and entry.get("status") == "Finished":
                    did = entry["driver_id"]
                    wins[did] = wins.get(did, 0) + 1
    return wins


def main():
    standings = json.loads(STANDINGS_FILE.read_text(encoding="utf-8"))
    results   = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))

    season_wins = count_season_wins(results)
    changed = False

    for drv in standings["driver_standings"]:
        did       = drv["driver_id"]
        sw        = season_wins.get(did, 0)
        new_total = PRE_2026_WINS.get(did, 0) + sw

        if drv.get("season_wins") != sw or drv.get("career_wins") != new_total:
            print(f"  {did}: season_wins={sw}, career_wins={new_total}")
            drv["season_wins"] = sw
            drv["career_wins"] = new_total
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

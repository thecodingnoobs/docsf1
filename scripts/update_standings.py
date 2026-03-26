#!/usr/bin/env python3
"""
Recomputes career_wins in f1_2026_standings.json after each race.

career_wins = pre-season baseline (fetched from Jolpica) + 2026 season wins
             (counted from f1_2026_results.json)

Run after fetch_results.py in CI, or manually:
  python scripts/update_standings.py
"""

import json
import time
import urllib.request
from pathlib import Path

ROOT           = Path(__file__).parent.parent
STANDINGS_FILE = ROOT / "f1_2026_standings.json"
RESULTS_FILE   = ROOT / "f1_2026_results.json"

# Jolpica driver ID overrides (where our id differs from theirs)
JOLPICA_ID_MAP = {
    "verstappen": "max_verstappen",
}


def fetch(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "LightsOutApp/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def fetch_pre_season_wins(driver_id: str) -> int:
    """Total race wins for this driver through end of 2025, from Jolpica."""
    jolpica_id = JOLPICA_ID_MAP.get(driver_id, driver_id)
    base = f"https://api.jolpi.ca/ergast/f1/drivers/{jolpica_id}/results/1.json"

    data   = fetch(f"{base}?limit=100&offset=0")
    total  = int(data["MRData"]["total"])
    races  = data["MRData"]["RaceTable"]["Races"]

    offset = 100
    while offset < total:
        data   = fetch(f"{base}?limit=100&offset={offset}")
        races += data["MRData"]["RaceTable"]["Races"]
        offset += 100
        time.sleep(0.2)

    return sum(1 for r in races if int(r["season"]) <= 2025)


def count_season_wins(results_data: dict) -> dict:
    """Count P1 race finishes per driver from f1_2026_results.json."""
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
        driver_id = drv["driver_id"]
        sw        = season_wins.get(driver_id, 0)
        drv["season_wins"] = sw  # keep this in sync too

        try:
            pre_season = fetch_pre_season_wins(driver_id)
            time.sleep(0.3)
        except Exception as e:
            print(f"  WARNING: could not fetch Jolpica data for {driver_id}: {e}")
            continue

        new_total = pre_season + sw
        if drv.get("career_wins") != new_total:
            print(f"  {driver_id}: {drv.get('career_wins')} → {new_total}")
            drv["career_wins"] = new_total
            changed = True

    if changed:
        STANDINGS_FILE.write_text(
            json.dumps(standings, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Updated {STANDINGS_FILE}")
    else:
        print("No career_wins changes needed.")


if __name__ == "__main__":
    main()

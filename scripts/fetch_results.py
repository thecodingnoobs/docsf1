#!/usr/bin/env python3
"""
Fetches F1 2026 race results from the OpenF1 API and updates f1_2026_results.json.
Runs automatically via GitHub Actions after each race weekend.
"""

import json
import sys
import requests
from datetime import datetime, timezone
from pathlib import Path

OPENF1_BASE = "https://api.openf1.org/v1"
ROOT = Path(__file__).parent.parent
RESULTS_FILE = ROOT / "f1_2026_results.json"
SCHEDULE_FILE = ROOT / "f1_2026_schedule.json"

RACE_POINTS   = [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]
SPRINT_POINTS = [8, 7, 6, 5, 4, 3, 2, 1]

# OpenF1 name_acronym → our driver_id
DRIVER_ID_MAP = {
    "RUS": "russell",    "ANT": "antonelli",  "LEC": "leclerc",
    "HAM": "hamilton",   "BEA": "bearman",    "NOR": "norris",
    "GAS": "gasly",      "LAW": "lawson",     "LIN": "lindblad",
    "HAD": "hadjar",     "PIA": "piastri",    "VER": "verstappen",
    "SAI": "sainz",      "BOR": "bortoleto",  "COL": "colapinto",
    "OCO": "ocon",       "HUL": "hulkenberg", "ALB": "albon",
    "BOT": "bottas",     "PER": "perez",      "ALO": "alonso",
    "STR": "stroll",
}

# OpenF1 team_name → our team_id
TEAM_ID_MAP = {
    "Mercedes":          "mercedes",
    "Ferrari":           "ferrari",
    "McLaren":           "mclaren",
    "Red Bull Racing":   "red_bull",
    "Racing Bulls":      "racing_bulls",
    "Haas F1 Team":      "haas",
    "Alpine":            "alpine",
    "Audi":              "audi",
    "Williams":          "williams",
    "Cadillac":          "cadillac",
    "Aston Martin":      "aston_martin",
}


def fetch(endpoint, **params):
    url = f"{OPENF1_BASE}/{endpoint}"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_final_positions(session_key):
    """Last recorded position per driver — their finishing position."""
    data = fetch("position", session_key=session_key)
    final = {}
    for entry in data:
        final[entry["driver_number"]] = entry["position"]
    return final  # {driver_number: position}


def get_lap_counts(session_key):
    """Max lap completed per driver — used to detect DNFs."""
    data = fetch("laps", session_key=session_key)
    counts = {}
    for lap in data:
        dn = lap["driver_number"]
        ln = lap.get("lap_number", 0)
        counts[dn] = max(counts.get(dn, 0), ln)
    return counts


def get_fastest_lap_driver(session_key):
    """Driver number with the fastest single lap in the session."""
    data = fetch("laps", session_key=session_key)
    best_time = None
    best_driver = None
    for lap in data:
        duration = lap.get("lap_duration")
        if duration and (best_time is None or duration < best_time):
            best_time = duration
            best_driver = lap["driver_number"]
    return best_driver


def get_drivers(session_key):
    """Driver info keyed by driver_number."""
    data = fetch("drivers", session_key=session_key)
    return {d["driver_number"]: d for d in data}


def find_session_key(grand_prix, session_name, year=2026):
    """Find OpenF1 session_key by matching grand prix name and session type."""
    sessions = fetch("sessions", year=year, session_name=session_name)
    # Use the first significant word of the GP name for matching
    gp_keyword = grand_prix.split()[0].lower()  # e.g. "Australian" -> "australian"
    for s in sessions:
        meeting = s.get("meeting_name", "").lower()
        location = s.get("location", "").lower()
        country = s.get("country_name", "").lower()
        if gp_keyword in meeting or gp_keyword in location or gp_keyword in country:
            return s["session_key"]
    return None


def build_results(session_key, points_scale):
    """Build a results list for a session from OpenF1 data."""
    positions   = get_final_positions(session_key)
    drivers     = get_drivers(session_key)
    lap_counts  = get_lap_counts(session_key)
    fastest_num = get_fastest_lap_driver(session_key)

    if not positions:
        return None

    max_laps = max(lap_counts.values()) if lap_counts else 0
    sorted_entries = sorted(positions.items(), key=lambda x: x[1])

    results = []
    for driver_number, position in sorted_entries:
        driver    = drivers.get(driver_number, {})
        acronym   = driver.get("name_acronym", "???")
        team_name = driver.get("team_name", "Unknown")
        full_name = f"{driver.get('first_name', '')} {driver.get('last_name', '')}".strip()

        laps_done = lap_counts.get(driver_number, 0)
        laps_down = max_laps - laps_done

        if laps_done == 0:
            status, time_str = "DNS", "DNS"
        elif laps_down > 2:
            status, time_str = "DNF", "DNF"
        else:
            status = "Finished"
            # Lapped drivers show "+N lap(s)", leader shows empty (filled in manually)
            time_str = f"+{laps_down} lap{'s' if laps_down > 1 else ''}" if laps_down > 0 else ""

        pos_index = position - 1
        points = points_scale[pos_index] if (status == "Finished" and pos_index < len(points_scale)) else 0

        results.append({
            "position":    position,
            "driver_id":   DRIVER_ID_MAP.get(acronym, acronym.lower()),
            "driver_code": acronym,
            "driver_name": full_name,
            "team_id":     TEAM_ID_MAP.get(team_name, team_name.lower().replace(" ", "_")),
            "team_name":   team_name,
            "time":        time_str,
            "fastest_lap": driver_number == fastest_num,
            "points":      points,
            "status":      status,
        })

    return results


def find_pending_rounds(results_data, schedule_data):
    """Rounds where the race has passed but results are still null."""
    now = datetime.now(timezone.utc)
    schedule_by_round = {r["round"]: r for r in schedule_data["races"]}

    pending = []
    for race in results_data["races"]:
        if race["race_results"] is not None:
            continue
        sched = schedule_by_round.get(race["round"])
        if not sched or sched.get("cancelled"):
            continue
        race_time_str = (sched.get("sessions") or {}).get("race")
        if not race_time_str:
            continue
        race_time = datetime.fromisoformat(race_time_str.replace("Z", "+00:00"))
        # Give it 4 hours after race start for data to be available on OpenF1
        if now > race_time.replace(hour=race_time.hour + 4 if race_time.hour <= 19 else 23):
            sprint_time_str = (sched.get("sessions") or {}).get("sprint_race")
            pending.append((race["round"], race["grand_prix"], sprint_time_str))

    return pending


def main():
    results_data  = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
    schedule_data = json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))

    pending = find_pending_rounds(results_data, schedule_data)
    if not pending:
        print("No pending rounds to update.")
        return

    updated = False
    for round_num, grand_prix, sprint_time_str in pending:
        print(f"\nProcessing Round {round_num}: {grand_prix}")

        # --- Main race ---
        race_session_key = find_session_key(grand_prix, "Race")
        if not race_session_key:
            print(f"  Could not find Race session — skipping.")
            continue

        print(f"  Race session_key: {race_session_key}")
        race_results = build_results(race_session_key, RACE_POINTS)
        if not race_results:
            print(f"  Race data not available yet — skipping.")
            continue

        # --- Sprint (if applicable) ---
        sprint_results = None
        if sprint_time_str:
            sprint_session_key = find_session_key(grand_prix, "Sprint")
            if sprint_session_key:
                print(f"  Sprint session_key: {sprint_session_key}")
                sprint_results = build_results(sprint_session_key, SPRINT_POINTS)
            else:
                print(f"  No sprint session found.")

        # --- Write into results ---
        for race in results_data["races"]:
            if race["round"] == round_num:
                race["race_results"]   = race_results
                race["sprint_results"] = sprint_results
                break

        print(f"  Done — {len(race_results)} finishers.")
        updated = True

    if updated:
        results_data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        RESULTS_FILE.write_text(
            json.dumps(results_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\nWrote {RESULTS_FILE}")
    else:
        print("\nNothing to write.")
        sys.exit(0)


if __name__ == "__main__":
    main()

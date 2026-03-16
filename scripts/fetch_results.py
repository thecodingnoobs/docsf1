#!/usr/bin/env python3
"""
Fetches F1 2026 race results from the OpenF1 API and updates f1_2026_results.json.
Runs automatically via GitHub Actions after each race weekend.

Usage:
  python fetch_results.py                  # update any pending rounds
  python fetch_results.py --force-rounds 1,2   # re-pull specific rounds (overwrites existing)
"""

import argparse
import json
import sys
import requests
from datetime import datetime, timezone, timedelta
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


def get_laps_data(session_key):
    """
    Single API call for all lap data.
    Returns (lap_counts, fastest_lap_driver_number).
    """
    data = fetch("laps", session_key=session_key)
    counts = {}
    best_time = None
    best_driver = None
    for lap in data:
        dn = lap["driver_number"]
        ln = lap.get("lap_number", 0)
        counts[dn] = max(counts.get(dn, 0), ln)
        duration = lap.get("lap_duration")
        if duration and (best_time is None or duration < best_time):
            best_time = duration
            best_driver = dn
    return counts, best_driver


def get_final_gaps(session_key):
    """
    Final gap_to_leader per driver from the intervals endpoint.
    Returns {driver_number: gap_string} e.g. {"" for leader, "+5.515s", "+1 LAP"}.
    """
    data = fetch("intervals", session_key=session_key)
    final = {}
    for entry in data:
        final[entry["driver_number"]] = entry["gap_to_leader"]

    result = {}
    for dn, gap in final.items():
        if gap is None or gap == 0 or gap == 0.0:
            result[dn] = ""                              # race winner
        elif isinstance(gap, str):
            result[dn] = gap                             # "+1 LAP", "+2 LAPS" etc
        else:
            result[dn] = f"+{gap:.3f}s"                 # "+5.515s"
    return result


def get_drivers(session_key):
    """Driver info keyed by driver_number."""
    data = fetch("drivers", session_key=session_key)
    return {d["driver_number"]: d for d in data}


def find_session_key(race_date_str, session_name, year=2026):
    """
    Find OpenF1 session_key by matching the session closest to race_date_str.
    Date matching is more reliable than name matching across all GP names.
    """
    sessions = fetch("sessions", year=year, session_name=session_name)
    target = datetime.fromisoformat(race_date_str.replace("Z", "+00:00"))
    best_key  = None
    best_diff = None
    for s in sessions:
        s_start = datetime.fromisoformat(s["date_start"])
        diff = abs((s_start - target).total_seconds())
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_key  = s["session_key"]
    # Only match if within 24 hours of the scheduled time
    return best_key if best_diff is not None and best_diff < 86400 else None


def build_results(session_key, points_scale):
    """Build a full results list for a session from OpenF1 data."""
    positions               = get_final_positions(session_key)
    drivers                 = get_drivers(session_key)
    lap_counts, fastest_num = get_laps_data(session_key)
    gaps                    = get_final_gaps(session_key)

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
        # F1 classification rule: complete ≥ 90% of winner's laps = Finished
        classified_threshold = max_laps * 0.9

        if laps_done == 0:
            status, time_str = "DNS", "DNS"
        elif laps_done < classified_threshold:
            status, time_str = "DNF", "DNF"
        else:
            status   = "Finished"
            time_str = gaps.get(driver_number, "")

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


def find_pending_rounds(results_data, schedule_data, force_rounds=None):
    """
    Rounds to update.
    force_rounds: set of round numbers to re-pull regardless of current data.
    Otherwise returns rounds where race has passed but results are still null.
    """
    now = datetime.now(timezone.utc)
    schedule_by_round = {r["round"]: r for r in schedule_data["races"]}

    pending = []
    for race in results_data["races"]:
        round_num = race["round"]
        sched = schedule_by_round.get(round_num)
        if not sched or sched.get("cancelled"):
            continue

        race_time_str = (sched.get("sessions") or {}).get("race")
        if not race_time_str:
            continue

        race_time = datetime.fromisoformat(race_time_str.replace("Z", "+00:00"))

        sprint_time_str = (sched.get("sessions") or {}).get("sprint_race")
        if force_rounds and round_num in force_rounds:
            pending.append((round_num, race["grand_prix"], race_time_str, sprint_time_str))
        elif race["race_results"] is None:
            # Give 4 hours after race start for OpenF1 data to be available
            if now > race_time + timedelta(hours=4):
                pending.append((round_num, race["grand_prix"], race_time_str, sprint_time_str))

    return pending


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force-rounds",
        help="Comma-separated round numbers to re-pull (e.g. 1,2)",
        default="",
    )
    args = parser.parse_args()
    force_rounds = {int(r) for r in args.force_rounds.split(",") if r.strip()}

    results_data  = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
    schedule_data = json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))

    pending = find_pending_rounds(results_data, schedule_data, force_rounds)
    if not pending:
        print("No pending rounds to update.")
        return

    updated = False
    for round_num, grand_prix, race_time_str, sprint_time_str in pending:
        print(f"\nProcessing Round {round_num}: {grand_prix}")

        # --- Main race ---
        race_session_key = find_session_key(race_time_str, "Race")
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
            sprint_session_key = find_session_key(sprint_time_str, "Sprint")
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

        print(f"  Race: {len(race_results)} entries" + (
            f", Sprint: {len(sprint_results)} entries" if sprint_results else ""
        ))
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

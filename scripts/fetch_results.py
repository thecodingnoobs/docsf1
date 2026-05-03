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
RESULTS_FILE  = ROOT / "f1_2026_results.json"
SCHEDULE_FILE = ROOT / "f1_2026_schedule.json"
CIRCUITS_FILE = ROOT / "circuits.json"

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


class TransientFetchError(Exception):
    """OpenF1 returned a transient error (429 / 5xx). Caller should skip
    this session and try again on the next workflow run rather than treat
    empty data as authoritative."""


def fetch(endpoint, **params):
    url = f"{OPENF1_BASE}/{endpoint}"
    resp = requests.get(url, params=params, timeout=30)
    # 404 = session not yet active / no data — return empty so build_results
    # cleanly returns None (we check sprint while race is still in the future).
    if resp.status_code == 404:
        return []
    # 429 + 5xx = transient. Returning [] would silently produce malformed
    # results (e.g. drivers missing names). Raise a marker so the caller can
    # skip the session entirely without crashing the whole run.
    if resp.status_code == 429 or resp.status_code >= 500:
        raise TransientFetchError(f"{resp.status_code} on {endpoint}")
    resp.raise_for_status()
    return resp.json()


def get_final_positions(session_key):
    """Last recorded position per driver — their finishing position."""
    data = fetch("position", session_key=session_key)
    final = {}
    for entry in data:
        final[entry["driver_number"]] = entry["position"]
    return final  # {driver_number: position}


def format_lap_time(seconds):
    """Convert float seconds to 'M:SS.mmm' string, e.g. 91.447 → '1:31.447'."""
    if seconds is None:
        return None
    minutes = int(seconds) // 60
    remainder = seconds - minutes * 60
    return f"{minutes}:{remainder:06.3f}"


def lap_time_to_seconds(time_str):
    """Convert '1:31.447' or '91.447' to float seconds. Returns None on failure."""
    if not time_str:
        return None
    try:
        if ":" in time_str:
            minutes, rest = time_str.split(":", 1)
            return int(minutes) * 60 + float(rest)
        return float(time_str)
    except (ValueError, IndexError):
        return None


def get_laps_data(session_key):
    """
    Single API call for all lap data.
    Returns (lap_counts, fastest_lap_driver_number, best_lap_seconds).
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
    return counts, best_driver, best_time


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


def build_qualifying_results(session_key):
    """
    Build qualifying results with q1/q2/q3 best-lap times per driver.

    OpenF1 doesn't expose Q1/Q2/Q3 segmentation directly. We reconstruct
    it by finding the two largest time gaps in the chronologically-sorted
    lap stream — those are the inter-segment breaks (typically 6+ min,
    while in-segment gaps stay under ~2 min). Each lap is then assigned
    to a segment and we take each driver's best in each segment they
    participated in.

    Returns a list of result entries, or None if positions/laps are
    unavailable (e.g. session not started yet, free-tier 404s).
    """
    positions = get_final_positions(session_key)
    drivers   = get_drivers(session_key)
    laps_data = fetch("laps", session_key=session_key)

    if not positions or not laps_data:
        return None

    # Detect Q1→Q2 and Q2→Q3 boundaries from the global lap timeline.
    timed = [
        (datetime.fromisoformat(l["date_start"]), l)
        for l in laps_data if l.get("date_start")
    ]
    timed.sort(key=lambda x: x[0])

    q1_end, q2_end = None, None
    if len(timed) >= 3:
        gaps = [
            (timed[i][0] - timed[i-1][0], timed[i-1][0])
            for i in range(1, len(timed))
        ]
        # Two largest gaps, returned in chronological order.
        boundaries = sorted(
            sorted(gaps, key=lambda x: x[0], reverse=True)[:2],
            key=lambda x: x[1],
        )
        if len(boundaries) == 2:
            q1_end, q2_end = boundaries[0][1], boundaries[1][1]

    # Bucket each lap into the segment it belongs to, keeping the best per driver.
    best_per_segment = {}  # {driver_number: {1: float|None, 2: ..., 3: ...}}
    for ts, lap in timed:
        dn = lap.get("driver_number")
        duration = lap.get("lap_duration")
        if dn is None or duration is None:
            continue

        if q1_end is None or q2_end is None:
            seg = 1
        elif ts <= q1_end:
            seg = 1
        elif ts <= q2_end:
            seg = 2
        else:
            seg = 3

        bucket = best_per_segment.setdefault(dn, {1: None, 2: None, 3: None})
        if bucket[seg] is None or duration < bucket[seg]:
            bucket[seg] = duration

    # Assemble in finishing order.
    sorted_entries = sorted(positions.items(), key=lambda x: x[1])
    results = []
    for driver_number, position in sorted_entries:
        driver    = drivers.get(driver_number, {})
        acronym   = driver.get("name_acronym", "???")
        team_name = driver.get("team_name", "Unknown")
        full_name = f"{driver.get('first_name', '')} {driver.get('last_name', '')}".strip()

        bucket = best_per_segment.get(driver_number, {1: None, 2: None, 3: None})

        # Position is the authoritative source of truth for which segments a
        # driver advanced to. F1/Sprint-Quali rules: top 10 ran in Q3, 11–15
        # in Q2, 16+ in Q1 only. Clamp to those rules so we never display a
        # cool-down or out-of-window lap as a "Q2" / "Q3" time for an earlier
        # knockout. (Stray laps creep in around the segment boundary because
        # gap-detection has minute-level resolution.)
        if position > 15:
            bucket = {1: bucket[1], 2: None, 3: None}
        elif position > 10:
            bucket = {1: bucket[1], 2: bucket[2], 3: None}

        results.append({
            "position":    position,
            "driver_id":   DRIVER_ID_MAP.get(acronym, acronym.lower()),
            "driver_code": acronym,
            "driver_name": full_name,
            "team_id":     TEAM_ID_MAP.get(team_name, team_name.lower().replace(" ", "_")),
            "team_name":   team_name,
            "q1":          format_lap_time(bucket[1]) or "",
            "q2":          format_lap_time(bucket[2]) or "",
            "q3":          format_lap_time(bucket[3]) or "",
        })

    return results


def build_results(session_key, points_scale):
    """
    Build a full results list for a session from OpenF1 data.
    Returns (results, fastest_driver_name, best_lap_seconds).
    fastest_driver_name and best_lap_seconds are None when unavailable.
    """
    positions                          = get_final_positions(session_key)
    drivers                            = get_drivers(session_key)
    lap_counts, fastest_num, best_secs = get_laps_data(session_key)
    gaps                               = get_final_gaps(session_key)

    if not positions:
        return None, None, None

    max_laps = max(lap_counts.values()) if lap_counts else 0
    sorted_entries = sorted(positions.items(), key=lambda x: x[1])

    results = []
    fastest_driver_name = None
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

        if driver_number == fastest_num and full_name:
            # Abbreviated: "L. Hamilton"
            given = driver.get("first_name", "")
            family = driver.get("last_name", "")
            fastest_driver_name = f"{given[0]}. {family}" if given else family

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

    return results, fastest_driver_name, best_secs


def find_pending_rounds(results_data, schedule_data, force_rounds=None):
    """
    Rounds to update.
    force_rounds: set of round numbers to re-pull regardless of current data.
    Otherwise returns rounds where race OR sprint has finished >4h ago and
    that field is still null. Race and sprint are checked independently so
    a sprint that finishes a day before the race is ingested as soon as it
    is available, not after the race.
    """
    now = datetime.now(timezone.utc)
    schedule_by_round = {r["round"]: r for r in schedule_data["races"]}

    pending = []
    for race in results_data["races"]:
        round_num = race["round"]
        sched = schedule_by_round.get(round_num)
        if not sched or sched.get("cancelled"):
            continue

        sessions = sched.get("sessions") or {}
        race_time_str         = sessions.get("race")
        sprint_time_str       = sessions.get("sprint_race")
        quali_time_str        = sessions.get("qualifying")
        sprint_quali_time_str = sessions.get("sprint_qualifying")

        # Without any session times we can't know when to fetch.
        if not any([race_time_str, sprint_time_str, quali_time_str, sprint_quali_time_str]):
            continue

        is_forced = force_rounds and round_num in force_rounds

        def is_due(time_str, current_value):
            if not time_str or current_value is not None:
                return False
            t = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            return now > t + timedelta(hours=4)

        race_due         = is_due(race_time_str,         race.get("race_results"))
        sprint_due       = is_due(sprint_time_str,       race.get("sprint_results"))
        quali_due        = is_due(quali_time_str,        race.get("qualifying_results"))
        sprint_quali_due = is_due(sprint_quali_time_str, race.get("sprint_qualifying_results"))

        if is_forced or race_due or sprint_due or quali_due or sprint_quali_due:
            pending.append((
                round_num, race["grand_prix"],
                race_time_str, sprint_time_str,
                quali_time_str, sprint_quali_time_str,
            ))

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
    circuits_data = json.loads(CIRCUITS_FILE.read_text(encoding="utf-8"))

    # Build a quick lookup of circuit objects by id for lap record updates
    circuits_by_id = {c["id"]: c for c in circuits_data["circuits"]}

    # Build a lookup of circuit_id per round from the schedule
    circuit_id_by_round = {
        r["round"]: r.get("circuit_id") for r in schedule_data["races"]
    }

    pending = find_pending_rounds(results_data, schedule_data, force_rounds)
    if not pending:
        print("No pending rounds to update.")
        return

    results_changed  = False
    circuits_changed = False

    for round_num, grand_prix, race_time_str, sprint_time_str, quali_time_str, sprint_quali_time_str in pending:
        print(f"\nProcessing Round {round_num}: {grand_prix}")
        is_forced = round_num in force_rounds
        existing = next(r for r in results_data["races"] if r["round"] == round_num)

        # --- Main race --------------------------------------------------------
        # Skip the race fetch when nothing to do (already ingested and not forced).
        race_results, fastest_name, best_secs = None, None, None
        if race_time_str and (is_forced or existing.get("race_results") is None):
            try:
                race_session_key = find_session_key(race_time_str, "Race")
                if not race_session_key:
                    print(f"  Could not find Race session.")
                else:
                    print(f"  Race session_key: {race_session_key}")
                    race_results, fastest_name, best_secs = build_results(race_session_key, RACE_POINTS)
                    if not race_results:
                        print(f"  Race data not available yet from OpenF1.")
            except TransientFetchError as e:
                print(f"  OpenF1 transient error on Race ({e}); will retry next run.")

        # --- Update circuit lap record if this race set a new one ------------
        if race_results is not None and best_secs is not None and fastest_name:
            circuit_id = circuit_id_by_round.get(round_num)
            circuit = circuits_by_id.get(circuit_id) if circuit_id else None
            if circuit is not None:
                new_time_str = format_lap_time(best_secs)
                existing_lap = circuit.get("lap_record")
                existing_secs = lap_time_to_seconds(existing_lap.get("time")) if existing_lap else None
                if existing_secs is None or best_secs < existing_secs:
                    year = int(race_time_str[:4])
                    circuit["lap_record"] = {
                        "time":   new_time_str,
                        "driver": fastest_name,
                        "year":   year,
                    }
                    circuits_changed = True
                    print(f"  New circuit lap record: {new_time_str} — {fastest_name} ({year})")

        # --- Sprint (if applicable) ------------------------------------------
        sprint_results = None
        if sprint_time_str and (is_forced or existing.get("sprint_results") is None):
            try:
                sprint_session_key = find_session_key(sprint_time_str, "Sprint")
                if not sprint_session_key:
                    print(f"  No sprint session found.")
                else:
                    print(f"  Sprint session_key: {sprint_session_key}")
                    sprint_results, _, _ = build_results(sprint_session_key, SPRINT_POINTS)
                    if not sprint_results:
                        print(f"  Sprint data not available yet from OpenF1.")
            except TransientFetchError as e:
                print(f"  OpenF1 transient error on Sprint ({e}); will retry next run.")

        # --- Qualifying (if applicable) --------------------------------------
        quali_results = None
        if quali_time_str and (is_forced or existing.get("qualifying_results") is None):
            try:
                quali_session_key = find_session_key(quali_time_str, "Qualifying")
                if not quali_session_key:
                    print(f"  Could not find Qualifying session.")
                else:
                    print(f"  Qualifying session_key: {quali_session_key}")
                    quali_results = build_qualifying_results(quali_session_key)
                    if not quali_results:
                        print(f"  Qualifying data not available yet from OpenF1.")
            except TransientFetchError as e:
                print(f"  OpenF1 transient error on Qualifying ({e}); will retry next run.")

        # --- Sprint Qualifying (if applicable) -------------------------------
        sprint_quali_results = None
        if sprint_quali_time_str and (is_forced or existing.get("sprint_qualifying_results") is None):
            try:
                sq_session_key = find_session_key(sprint_quali_time_str, "Sprint Qualifying")
                if not sq_session_key:
                    print(f"  Could not find Sprint Qualifying session.")
                else:
                    print(f"  Sprint Qualifying session_key: {sq_session_key}")
                    sprint_quali_results = build_qualifying_results(sq_session_key)
                    if not sprint_quali_results:
                        print(f"  Sprint Qualifying data not available yet from OpenF1.")
            except TransientFetchError as e:
                print(f"  OpenF1 transient error on Sprint Qualifying ({e}); will retry next run.")

        # --- Write only fields we successfully fetched ------------------------
        # Never overwrite an existing list with None (would wipe ingested data
        # if a later run can't find the OpenF1 session).
        wrote_anything = False
        if race_results is not None:
            existing["race_results"] = race_results
            wrote_anything = True
        if sprint_results is not None:
            existing["sprint_results"] = sprint_results
            wrote_anything = True
        if quali_results is not None:
            existing["qualifying_results"] = quali_results
            wrote_anything = True
        if sprint_quali_results is not None:
            existing["sprint_qualifying_results"] = sprint_quali_results
            wrote_anything = True

        if wrote_anything:
            parts = []
            if race_results is not None: parts.append(f"Race: {len(race_results)}")
            if sprint_results is not None: parts.append(f"Sprint: {len(sprint_results)}")
            if quali_results is not None: parts.append(f"Quali: {len(quali_results)}")
            if sprint_quali_results is not None: parts.append(f"Sprint Q: {len(sprint_quali_results)}")
            print(f"  Wrote: {', '.join(parts)}")
            results_changed = True
        else:
            print(f"  Nothing new to write for this round.")

    if results_changed:
        results_data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        RESULTS_FILE.write_text(
            json.dumps(results_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\nWrote {RESULTS_FILE}")

    if circuits_changed:
        CIRCUITS_FILE.write_text(
            json.dumps(circuits_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Wrote {CIRCUITS_FILE}")

    if not results_changed and not circuits_changed:
        print("\nNothing to write.")
        sys.exit(0)


if __name__ == "__main__":
    main()

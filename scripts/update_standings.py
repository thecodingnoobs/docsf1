#!/usr/bin/env python3
"""
Updates season stats, career stats, and constructor standings in f1_2026_standings.json after each race.

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
    "russell":    {"wins": 5,   "races": 152, "podiums": 24,  "top10": 86,  "championships": 0, "seasons": 7},
    "antonelli":  {"wins": 0,   "races": 24,  "podiums": 3,   "top10": 14,  "championships": 0, "seasons": 1},
    "leclerc":    {"wins": 8,   "races": 173, "podiums": 50,  "top10": 132, "championships": 0, "seasons": 8},
    "hamilton":   {"wins": 105, "races": 380, "podiums": 202, "top10": 331, "championships": 7, "seasons": 19},
    "bearman":    {"wins": 0,   "races": 27,  "podiums": 0,   "top10": 11,  "championships": 0, "seasons": 1},
    "norris":     {"wins": 11,  "races": 152, "podiums": 44,  "top10": 121, "championships": 1, "seasons": 7},
    "gasly":      {"wins": 1,   "races": 178, "podiums": 5,   "top10": 75,  "championships": 0, "seasons": 9},
    "verstappen": {"wins": 71,  "races": 233, "podiums": 127, "top10": 195, "championships": 4, "seasons": 11},
    "lawson":     {"wins": 0,   "races": 35,  "podiums": 0,   "top10": 10,  "championships": 0, "seasons": 3},
    "lindblad":   {"wins": 0,   "races": 0,   "podiums": 0,   "top10": 0,   "championships": 0, "seasons": 0},
    "hadjar":     {"wins": 0,   "races": 24,  "podiums": 1,   "top10": 10,  "championships": 0, "seasons": 1},
    "piastri":    {"wins": 9,   "races": 70,  "podiums": 26,  "top10": 56,  "championships": 0, "seasons": 3},
    "sainz":      {"wins": 4,   "races": 232, "podiums": 29,  "top10": 149, "championships": 0, "seasons": 11},
    "bortoleto":  {"wins": 0,   "races": 24,  "podiums": 0,   "top10": 5,   "championships": 0, "seasons": 1},
    "colapinto":  {"wins": 0,   "races": 27,  "podiums": 0,   "top10": 2,   "championships": 0, "seasons": 2},
    "ocon":       {"wins": 1,   "races": 180, "podiums": 4,   "top10": 94,  "championships": 0, "seasons": 10},
    "hulkenberg": {"wins": 0,   "races": 254, "podiums": 1,   "top10": 117, "championships": 0, "seasons": 13},
    "albon":      {"wins": 0,   "races": 129, "podiums": 2,   "top10": 50,  "championships": 0, "seasons": 6},
    "bottas":     {"wins": 10,  "races": 247, "podiums": 67,  "top10": 145, "championships": 0, "seasons": 13},
    "perez":      {"wins": 6,   "races": 283, "podiums": 39,  "top10": 186, "championships": 0, "seasons": 15},
    "alonso":     {"wins": 32,  "races": 428, "podiums": 106, "top10": 281, "championships": 2, "seasons": 21},
    "stroll":     {"wins": 0,   "races": 191, "podiums": 3,   "top10": 66,  "championships": 0, "seasons": 9},
}

# Confirmed 2026 Drivers' World Championship winners. Add a driver once the
# FIA/F1 has awarded the title; these are added to the career baseline below.
SEASON_CHAMPIONS = {"antonelli"}


def compute_season_stats(results_data: dict) -> dict:
    """Count Grand Prix starts, podiums, and top-10s from 2026 race results."""
    stats: dict[str, dict] = {}

    for race in results_data["races"]:
        race_results   = race.get("race_results")   or []

        for entry in race_results:
            did = entry["driver_id"]
            if did not in stats:
                stats[did] = {"races": 0, "podiums": 0, "top10": 0}
            stats[did]["races"] += 1

            # Driver career details use Grand Prix results. Sprint statistics
            # contribute to season points but remain separate from these fields.
            pos = entry.get("position", 99)
            if entry.get("status") == "Finished":
                if pos <= 3:
                    stats[did]["podiums"] += 1
                if pos <= 10:
                    stats[did]["top10"] += 1

    return stats


def compute_season_points(results_data: dict) -> dict[str, int]:
    """Sum points per driver from all race + sprint results."""
    points: dict[str, int] = {}
    for race in results_data["races"]:
        for entry in (race.get("race_results") or []) + (race.get("sprint_results") or []):
            did = entry["driver_id"]
            points[did] = points.get(did, 0) + entry.get("points", 0)
    return points


def compute_race_wins(results_data: dict) -> dict[str, int]:
    """Count race wins only (not sprint wins) per driver."""
    wins: dict[str, int] = {}
    for race in results_data["races"]:
        for entry in (race.get("race_results") or []):
            if entry.get("position") == 1 and entry.get("status") == "Finished":
                did = entry["driver_id"]
                wins[did] = wins.get(did, 0) + 1
    return wins


def compute_h2h(results_data: dict, standings: dict) -> dict[str, int]:
    """
    Count how many races each driver finished ahead of their teammate (race only, not sprint).
    Returns {driver_id: races_finished_ahead}.
    """
    teammate_map = {
        d["driver_id"]: d["h2h_teammate_id"]
        for d in standings["driver_standings"]
        if d.get("h2h_teammate_id")
    }

    ahead_count: dict[str, int] = {d["driver_id"]: 0 for d in standings["driver_standings"]}
    seen_pairs: set = set()

    for race in results_data["races"]:
        race_results = race.get("race_results") or []
        if not race_results:
            continue

        pos_map = {entry["driver_id"]: entry["position"] for entry in race_results}
        seen_pairs.clear()

        for driver_id, teammate_id in teammate_map.items():
            pair = frozenset([driver_id, teammate_id])
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            driver_pos   = pos_map.get(driver_id)
            teammate_pos = pos_map.get(teammate_id)
            if driver_pos is None or teammate_pos is None:
                continue

            if driver_pos < teammate_pos:
                ahead_count[driver_id] = ahead_count.get(driver_id, 0) + 1
            elif teammate_pos < driver_pos:
                ahead_count[teammate_id] = ahead_count.get(teammate_id, 0) + 1

    return ahead_count


def compute_position_counts(results_data: dict) -> dict[str, list[int]]:
    """
    Returns {driver_id: [count_p1, count_p2, ..., count_p20]} from race results only.
    Used as tiebreaker when drivers have equal points (F1 rules: most wins, then most 2nds, etc.).
    """
    counts: dict[str, list[int]] = {}
    for race in results_data["races"]:
        for entry in (race.get("race_results") or []):
            did = entry["driver_id"]
            if did not in counts:
                counts[did] = [0] * 20
            pos = entry.get("position")
            if isinstance(pos, int) and 1 <= pos <= 20:
                counts[did][pos - 1] += 1
    return counts


def compute_constructor_stats(results_data: dict) -> tuple[dict, dict]:
    """
    Returns (team_points, team_wins).
    Points include race + sprint. Wins are race only.
    """
    team_points: dict[str, int] = {}
    team_wins:   dict[str, int] = {}

    for race in results_data["races"]:
        for entry in (race.get("race_results") or []) + (race.get("sprint_results") or []):
            tid = entry["team_id"]
            team_points[tid] = team_points.get(tid, 0) + entry.get("points", 0)

        for entry in (race.get("race_results") or []):
            if entry.get("position") == 1 and entry.get("status") == "Finished":
                tid = entry["team_id"]
                team_wins[tid] = team_wins.get(tid, 0) + 1

    return team_points, team_wins


def main():
    standings = json.loads(STANDINGS_FILE.read_text(encoding="utf-8"))
    results   = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))

    season        = compute_season_stats(results)
    season_points = compute_season_points(results)
    race_wins     = compute_race_wins(results)
    h2h_counts    = compute_h2h(results, standings)
    pos_counts    = compute_position_counts(results)
    changed       = False

    # Keep the visible standings timestamp aligned with the result feed that
    # supplied its driver and constructor statistics.
    results_updated = results.get("last_updated")
    if results_updated and standings.get("last_updated") != results_updated:
        standings["last_updated"] = results_updated
        changed = True

    # --- after_round / after_race ---
    completed = [r for r in results["races"] if r.get("race_results") is not None]
    if completed:
        last = max(completed, key=lambda r: r["round"])
        if standings.get("after_round") != last["round"]:
            standings["after_round"] = last["round"]
            changed = True
        if standings.get("after_race") != last["grand_prix"]:
            standings["after_race"] = last["grand_prix"]
            changed = True

    # --- Driver standings ---
    for drv in standings["driver_standings"]:
        did = drv["driver_id"]
        pre = PRE_2026.get(did, {"wins": 0, "races": 0, "podiums": 0, "top10": 0, "championships": 0, "seasons": 0})
        s   = season.get(did, {"races": 0, "podiums": 0, "top10": 0})

        updates = {
            "points":         season_points.get(did, 0),
            "wins":           race_wins.get(did, 0),
            "career_wins":    pre["wins"]    + race_wins.get(did, 0),
            "career_races":   pre["races"]   + s["races"],
            "career_podiums": pre["podiums"] + s["podiums"],
            "career_top10s":  pre["top10"]   + s["top10"],
            "career_championships": pre["championships"] + (1 if did in SEASON_CHAMPIONS else 0),
            "career_seasons": pre["seasons"] + (1 if s["races"] else 0),
        }
        for key, val in updates.items():
            if drv.get(key) != val:
                drv[key] = val
                changed = True

        # H2H vs teammate
        teammate_id         = drv.get("h2h_teammate_id", "")
        new_driver_ahead    = h2h_counts.get(did, 0)
        new_teammate_ahead  = h2h_counts.get(teammate_id, 0)
        if drv.get("h2h_driver_ahead") != new_driver_ahead:
            drv["h2h_driver_ahead"] = new_driver_ahead
            changed = True
        if drv.get("h2h_teammate_ahead") != new_teammate_ahead:
            drv["h2h_teammate_ahead"] = new_teammate_ahead
            changed = True

        # Remove legacy season_wins field if present
        if "season_wins" in drv:
            del drv["season_wins"]
            changed = True

    # Re-sort drivers by points desc, tiebreaker: most wins, then most 2nds, etc. (F1 rules)
    standings["driver_standings"].sort(
        key=lambda d: (d["points"], pos_counts.get(d["driver_id"], [0] * 20)),
        reverse=True,
    )
    for i, drv in enumerate(standings["driver_standings"], start=1):
        if drv.get("position") != i:
            drv["position"] = i
            changed = True

    # --- Constructor standings ---
    team_points, team_wins = compute_constructor_stats(results)
    for con in standings["constructor_standings"]:
        tid = con["team_id"]
        new_pts  = team_points.get(tid, 0)
        new_wins = team_wins.get(tid, 0)
        if con.get("points") != new_pts:
            con["points"] = new_pts
            changed = True
        if con.get("wins") != new_wins:
            con["wins"] = new_wins
            changed = True

    # Build per-team position count (sum of both drivers) for tiebreaker
    driver_team_map = {d["driver_id"]: d.get("team_id", "") for d in standings["driver_standings"]}
    team_pos_counts: dict[str, list[int]] = {}
    for did, counts in pos_counts.items():
        tid = driver_team_map.get(did, "")
        if not tid:
            continue
        if tid not in team_pos_counts:
            team_pos_counts[tid] = [0] * 20
        team_pos_counts[tid] = [a + b for a, b in zip(team_pos_counts[tid], counts)]

    standings["constructor_standings"].sort(
        key=lambda c: (c["points"], team_pos_counts.get(c["team_id"], [0] * 20)),
        reverse=True,
    )
    for i, con in enumerate(standings["constructor_standings"], start=1):
        if con.get("position") != i:
            con["position"] = i
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

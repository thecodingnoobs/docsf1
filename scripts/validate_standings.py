#!/usr/bin/env python3
"""
Post-run validation: checks f1_2026_standings.json is internally consistent
with f1_2026_results.json. Run by CI after update_standings.py commits.

Exits non-zero if any check fails, so the workflow step fails visibly.
"""

import json
import sys
from pathlib import Path

ROOT           = Path(__file__).parent.parent
STANDINGS_FILE = ROOT / "f1_2026_standings.json"
RESULTS_FILE   = ROOT / "f1_2026_results.json"

errors = []

def fail(msg: str):
    errors.append(msg)
    print(f"  FAIL: {msg}")

def ok(msg: str):
    print(f"  OK:   {msg}")


standings = json.loads(STANDINGS_FILE.read_text(encoding="utf-8"))
results   = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))

drivers      = standings["driver_standings"]
constructors = standings["constructor_standings"]

# --- Compute expected values from results ---
expected_pts:  dict[str, int] = {}
expected_wins: dict[str, int] = {}
team_pts:      dict[str, int] = {}
team_wins:     dict[str, int] = {}
completed_rounds = []

for race in results["races"]:
    if race.get("race_results") is None:
        continue
    completed_rounds.append(race["round"])
    for entry in (race.get("race_results") or []) + (race.get("sprint_results") or []):
        did = entry["driver_id"]
        tid = entry["team_id"]
        expected_pts[did]  = expected_pts.get(did, 0)  + entry.get("points", 0)
        team_pts[tid]      = team_pts.get(tid, 0)      + entry.get("points", 0)
    for entry in (race.get("race_results") or []):
        if entry.get("position") == 1 and entry.get("status") == "Finished":
            did = entry["driver_id"]
            tid = entry["team_id"]
            expected_wins[did] = expected_wins.get(did, 0) + 1
            team_wins[tid]     = team_wins.get(tid, 0)     + 1

last_round = max(completed_rounds) if completed_rounds else 0
last_race  = next(
    (r["grand_prix"] for r in results["races"] if r["round"] == last_round), ""
)

print("\n=== Validating f1_2026_standings.json ===\n")

# 1. after_round / after_race
if standings.get("after_round") == last_round:
    ok(f"after_round = {last_round}")
else:
    fail(f"after_round is {standings.get('after_round')}, expected {last_round}")

if standings.get("after_race") == last_race:
    ok(f"after_race = '{last_race}'")
else:
    fail(f"after_race is '{standings.get('after_race')}', expected '{last_race}'")

# 2. Driver points and wins
print()
for d in drivers:
    did  = d["driver_id"]
    exp_pts  = expected_pts.get(did, 0)
    exp_wins = expected_wins.get(did, 0)
    if d["points"] == exp_pts:
        ok(f"{d['driver_code']} points = {exp_pts}")
    else:
        fail(f"{d['driver_code']} points = {d['points']}, expected {exp_pts}")
    if d["wins"] == exp_wins:
        ok(f"{d['driver_code']} wins = {exp_wins}")
    else:
        fail(f"{d['driver_code']} wins = {d['wins']}, expected {exp_wins}")

# 3. No legacy season_wins field
print()
for d in drivers:
    if "season_wins" in d:
        fail(f"{d['driver_code']} still has legacy 'season_wins' field")
    else:
        ok(f"{d['driver_code']} no season_wins field")

# 4. Driver standings sorted by points, positions sequential
print()
pts_list = [d["points"] for d in drivers]
if pts_list == sorted(pts_list, reverse=True):
    ok("Driver standings sorted by points descending")
else:
    fail("Driver standings NOT sorted by points")

positions = [d["position"] for d in drivers]
if positions == list(range(1, len(positions) + 1)):
    ok("Driver positions are sequential (1..N)")
else:
    fail(f"Driver positions not sequential: {positions}")

# 5. H2H totals add up to completed races for each pair
print()
by_id = {d["driver_id"]: d for d in drivers}
seen  = set()
for d in drivers:
    did = d["driver_id"]
    tid = d.get("h2h_teammate_id", "")
    if not tid or frozenset([did, tid]) in seen:
        continue
    seen.add(frozenset([did, tid]))
    total = d["h2h_driver_ahead"] + d["h2h_teammate_ahead"]
    if total > last_round:
        fail(f"H2H {d['driver_code']} vs {d['h2h_teammate_code']}: total {total} > completed races {last_round}")
    else:
        ok(f"H2H {d['driver_code']} vs {d['h2h_teammate_code']}: {d['h2h_driver_ahead']}-{d['h2h_teammate_ahead']} ({total} races)")
    # Mirror check
    if tid in by_id:
        teammate = by_id[tid]
        if teammate["h2h_driver_ahead"] != d["h2h_teammate_ahead"]:
            fail(f"H2H mirror mismatch: {d['driver_code']}.h2h_teammate_ahead={d['h2h_teammate_ahead']} != {teammate['driver_code']}.h2h_driver_ahead={teammate['h2h_driver_ahead']}")

# 6. Constructor points and wins
print()
for c in constructors:
    tid = c["team_id"]
    exp_pts  = team_pts.get(tid, 0)
    exp_wins = team_wins.get(tid, 0)
    if c["points"] == exp_pts:
        ok(f"{c['team_name']} points = {exp_pts}")
    else:
        fail(f"{c['team_name']} points = {c['points']}, expected {exp_pts}")
    if c["wins"] == exp_wins:
        ok(f"{c['team_name']} wins = {exp_wins}")
    else:
        fail(f"{c['team_name']} wins = {c['wins']}, expected {exp_wins}")

# 7. Constructor standings sorted
print()
con_pts = [c["points"] for c in constructors]
if con_pts == sorted(con_pts, reverse=True):
    ok("Constructor standings sorted by points descending")
else:
    fail("Constructor standings NOT sorted by points")

con_pos = [c["position"] for c in constructors]
if con_pos == list(range(1, len(con_pos) + 1)):
    ok("Constructor positions are sequential (1..N)")
else:
    fail(f"Constructor positions not sequential: {con_pos}")

# --- Summary ---
print(f"\n{'='*40}")
if errors:
    print(f"FAILED — {len(errors)} error(s):")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print(f"All checks passed.")

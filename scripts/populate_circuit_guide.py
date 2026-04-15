#!/usr/bin/env python3
"""
One-time script to populate drs_zones, lap_record, and first_gp_year
for all circuits in circuits.json.

  drs_zones     — static lookup (never changes unless circuit is reconfigured)
  lap_record    — fetched from Jolpica: fastest lap across all F1 races at
                  each circuit (time, driver, year)
  first_gp_year — fetched from Jolpica: year of the first F1 race held there

After running this once, fetch_results.py handles lap_record updates
automatically whenever a race sets a new circuit record.

Usage:
  python populate_circuit_guide.py            # fetch & write
  python populate_circuit_guide.py --dry-run  # print result without writing
"""

import argparse
import json
import time
import requests
from pathlib import Path

JOLPICA_BASE = "https://api.jolpi.ca/ergast/f1"
ROOT = Path(__file__).parent.parent
CIRCUITS_FILE = ROOT / "circuits.json"

# Maps docsf1 circuit id → Jolpica circuit id (only differs where names diverge)
CIRCUIT_ID_MAP = {
    "bahrain":       "bahrain",
    "jeddah":        "jeddah",
    "albert_park":   "albert_park",
    "shanghai":      "shanghai",
    "miami":         "miami",
    "imola":         "imola",
    "monaco":        "monaco",
    "barcelona":     "catalunya",
    "montreal":      "villeneuve",
    "red_bull_ring": "red_bull_ring",
    "silverstone":   "silverstone",
    "hungaroring":   "hungaroring",
    "spa":           "spa",
    "zandvoort":     "zandvoort",
    "monza":         "monza",
    "baku":          "baku",
    "singapore":     "marina_bay",
    "suzuka":        "suzuka",
    "cota":          "americas",
    "mexico":        "rodriguez",
    "interlagos":    "interlagos",
    "las_vegas":     "las_vegas",
    "losail":        "losail",
    "yas_marina":    "yas_marina",
}

# DRS zone counts — static, sourced from official FIA circuit documents.
# Only changes if a circuit is reconfigured (rare).
DRS_ZONES = {
    "bahrain":       3,
    "jeddah":        3,
    "albert_park":   4,
    "shanghai":      2,
    "miami":         3,
    "imola":         2,
    "monaco":        1,
    "barcelona":     2,
    "montreal":      2,
    "red_bull_ring": 3,
    "silverstone":   2,
    "hungaroring":   2,
    "spa":           2,
    "zandvoort":     2,
    "monza":         2,
    "baku":          2,
    "singapore":     3,
    "suzuka":        2,
    "cota":          2,
    "mexico":        3,
    "interlagos":    2,
    "las_vegas":     2,
    "losail":        2,
    "yas_marina":    2,
}


def jolpica_get(path, params=None):
    url = f"{JOLPICA_BASE}/{path}"
    resp = requests.get(url, params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


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


def get_lap_record(jolpica_id):
    """
    Page through all fastest-lap results at a circuit and return the
    all-time best as {"time": "1:31.447", "driver": "M. Schumacher", "year": 2004}.
    Returns None if no data is found.
    """
    all_results = []
    offset = 0
    limit = 100

    while True:
        data = jolpica_get(
            f"circuits/{jolpica_id}/fastest/1/results.json",
            params={"limit": limit, "offset": offset},
        )
        mr = data.get("MRData", {})
        total = int(mr.get("total", 0))
        races = mr.get("RaceTable", {}).get("Races", [])

        for race in races:
            results = race.get("Results", [])
            if not results:
                continue
            r = results[0]
            fastest = r.get("FastestLap", {})
            lap_time = fastest.get("Time", {}).get("time")
            if not lap_time:
                continue
            driver = r.get("Driver", {})
            given = driver.get("givenName", "")
            family = driver.get("familyName", "")
            # Abbreviated format: "M. Schumacher"
            name = f"{given[0]}. {family}" if given else family
            year = int(race.get("season", 0))
            all_results.append({"time": lap_time, "driver": name, "year": year})

        offset += limit
        if offset >= total:
            break
        time.sleep(1.0)

    if not all_results:
        return None

    return min(
        all_results,
        key=lambda r: lap_time_to_seconds(r["time"]) or float("inf"),
    )


def get_first_gp_year(jolpica_id):
    """Return the year of the first F1 race held at this circuit."""
    data = jolpica_get(
        f"circuits/{jolpica_id}/races.json",
        params={"limit": 1, "offset": 0},
    )
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        return None
    year = int(races[0].get("season", 0))
    return year if year else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the updated JSON without writing to disk",
    )
    args = parser.parse_args()

    circuits_data = json.loads(CIRCUITS_FILE.read_text(encoding="utf-8"))
    changed = False

    for circuit in circuits_data["circuits"]:
        cid = circuit["id"]
        jolpica_id = CIRCUIT_ID_MAP.get(cid)
        print(f"\n[{cid}]")

        # --- DRS zones (static) ---
        drs = DRS_ZONES.get(cid)
        if drs is not None and circuit.get("drs_zones") != drs:
            circuit["drs_zones"] = drs
            changed = True
            print(f"  drs_zones = {drs}")
        else:
            print(f"  drs_zones = {circuit.get('drs_zones')} (unchanged)")

        if not jolpica_id:
            print("  no Jolpica mapping — skipping API fields")
            continue

        # --- First GP year (skip if already populated) ---
        if circuit.get("first_gp_year"):
            print(f"  first_gp_year = {circuit['first_gp_year']} (unchanged)")
        else:
            try:
                year = get_first_gp_year(jolpica_id)
                time.sleep(1.0)
                if year:
                    circuit["first_gp_year"] = year
                    changed = True
                print(f"  first_gp_year = {year}")
            except Exception as e:
                print(f"  first_gp_year ERROR: {e}")

        # --- Lap record (skip if already populated) ---
        if circuit.get("lap_record"):
            r = circuit["lap_record"]
            print(f"  lap_record = {r['time']} — {r['driver']} ({r['year']}) (unchanged)")
        else:
            try:
                record = get_lap_record(jolpica_id)
                time.sleep(1.0)
                if record:
                    circuit["lap_record"] = record
                    changed = True
                    print(f"  lap_record = {record['time']} — {record['driver']} ({record['year']})")
                else:
                    print("  lap_record = not found")
            except Exception as e:
                print(f"  lap_record ERROR: {e}")

    print()

    if not changed:
        print("No changes needed.")
        return

    if args.dry_run:
        print("[dry-run] Result:\n")
        print(json.dumps(circuits_data, indent=2, ensure_ascii=False))
        return

    CIRCUITS_FILE.write_text(
        json.dumps(circuits_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote {CIRCUITS_FILE}")


if __name__ == "__main__":
    main()

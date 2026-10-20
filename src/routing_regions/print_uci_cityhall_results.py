#!/usr/bin/env python3
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
selection = data["routing"]["selection"]
journeys = data["routing"]["journeys"]
styles = {r["route"]: r for r in data.get("route_styles", [])}
print(f"Transit region: {selection['transit_region']}")
print(f"Origin OSR: {selection['origin_osr_region']}")
print(f"Destination OSR: {selection['destination_osr_region']}")
print(f"Pareto journeys: {len(journeys)}")
for i, journey in enumerate(journeys, 1):
    print(f"Journey {i}: {journey['boardings']} boardings, "
          f"arrival UTC Unix {journey['arrival_unix_seconds']}")
    for leg in journey["legs"]:
        if leg["kind"] == "ride":
            style = styles.get(leg["route"], {})
            print("  RIDE", style.get("short_name") or style.get("route_id") or leg["route"],
                  "color", style.get("color"), "text_color", style.get("text_color"),
                  "from", leg["from_stop"], "to", leg["to_stop"],
                  "trip", style.get("trip_id"))
        else:
            print(" ", leg)
if not journeys:
    print("NO JOURNEY: inspect OSR access/egress, feed ingestion, service calendars, "
          "packed timetable constraints, and source-region selection.")

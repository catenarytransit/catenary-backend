#!/usr/bin/env python3
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)

journeys = data["itineraries"]
direct = data.get("direct", [])
print(f"Transit itineraries: {len(journeys)}, direct: {len(direct)}")
for i, journey in enumerate(journeys, 1):
    print(f"Journey {i}: {journey['transfers']} transfers, "
          f"arrival {journey['endTime']}")
    for leg in journey["legs"]:
        if leg["mode"] in ("WALK", "BIKE", "CAR"):
            print("  WALK", leg["duration"], "seconds")
        else:
            print("  RIDE", leg.get("routeShortName") or leg.get("routeId"),
                  "color", leg.get("routeColor"),
                  "from", leg["from"]["name"], "to", leg["to"]["name"],
                  "trip", leg.get("tripId"),
                  "trip number", leg.get("tripShortName"),
                  "geometry points", leg["legGeometry"]["length"])
if not journeys:
    print("NO JOURNEY: inspect OSR access/egress, ingestion, service calendars, "
          "compiled timetable, and source-region selection.")

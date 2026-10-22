#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

p = argparse.ArgumentParser()
p.add_argument("--output", type=Path, required=True)
args = p.parse_args()

departure = datetime(2026, 10, 26, 8, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
data = {
    "from": {"lat": 33.6461, "lon": -117.8427},
    "to": {"lat": 34.0537, "lon": -118.2428},
    "departure_unix_seconds": int(departure.timestamp()),
    "access": [],
    "egress": [],
    "direct_walk_seconds": None,
}
args.output.write_text(json.dumps(data, indent=2) + "\n")
print("UCI to Los Angeles City Hall; Monday October 26, 2026, 08:00 America/Los_Angeles")
print("Endpoint access and egress will be routed using startup-loaded OSR worker")

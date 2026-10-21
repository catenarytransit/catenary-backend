#!/usr/bin/env python3
"""Fixed Southern California Saturday routing fixture for reproducible CI."""
import argparse
import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def request() -> dict:
    # "Next Saturday" relative to Wednesday 2026-10-21 is 2026-10-24.
    # Pin the date so rerunning the GitHub workflow never changes the query.
    departure = datetime(2026, 10, 24, 11, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
    assert departure.date() == date(2026, 10, 24) and departure.weekday() == 5
    return {
        # Main Street, Alhambra, supplied by the test requester.
        "from": {"lat": 34.0931745, "lon": -118.1312313},
        # Pedestrian entrance to Santa Monica Pier, beside the carousel.
        # A reachable on-land point is preferable to a pin over open water.
        "to": {"lat": 34.01023, "lon": -118.49635},
        "departure_unix_seconds": int(departure.timestamp()),
        "access": [],
        "egress": [],
        "direct_walk_seconds": None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = request()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print("Main Street, Alhambra -> Santa Monica Pier (pedestrian entrance)")
    print("Saturday October 24, 2026, 11:00 America/Los_Angeles (PDT)")
    print(f"Departure UTC Unix seconds: {data['departure_unix_seconds']}")
    print("OSR calculates access/egress paths; RAPTOR uses the compiled timetable.")


if __name__ == "__main__":
    main()

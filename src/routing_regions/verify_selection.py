#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from prepare_socal_test import FEEDS

parser = argparse.ArgumentParser()
parser.add_argument("selection", type=Path)
parser.add_argument("--live", action="store_true")
args = parser.parse_args()
data = json.loads(args.selection.read_text())
assert data["id"] == "southern-california", data
assert len(data["chateaux"]) == len(set(data["chateaux"]))
assert data["chateaux"] == sorted(data["chateaux"])
assert data["sources"] == sorted(data["sources"], key=lambda x:
    (x["chateau"], x["onestop_feed_id"], x["attempt_id"]))
assert all(source["chateau"] in data["chateaux"] for source in data["sources"])
if args.live:
    assert data["sources"], "live ingestion yielded no production GTFS sources"
    assert set(source["onestop_feed_id"] for source in data["sources"]) <= set(FEEDS)
else:
    assert "test-inside" in data["chateaux"]
    assert "test-edge-overlap" in data["chateaux"]
    assert "test-outside" not in data["chateaux"]
    assert "test-hole" not in data["chateaux"]
    assert len(data["sources"]) == 2
print(f"Verified {len(data['chateaux'])} chateaux, {len(data['sources'])} active sources")

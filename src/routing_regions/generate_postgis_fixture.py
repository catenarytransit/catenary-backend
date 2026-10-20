#!/usr/bin/env python3
"""Generate the local-only PostGIS fixture from the real downloaded boundary."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--geojson", type=Path, required=True)
parser.add_argument("--template", type=Path, default=Path("tools/routing_regions/postgis_fixture.sql"))
parser.add_argument("--output", type=Path, default=Path("temp/socal-postgis-fixture.sql"))
args = parser.parse_args()
data = json.loads(args.geojson.read_text())
if data["type"] == "FeatureCollection":
    shapes = [f["geometry"] for f in data["features"]]
elif data["type"] == "Feature":
    shapes = [data["geometry"]]
else:
    shapes = [data]
if not shapes or any(g["type"] not in ("Polygon", "MultiPolygon") for g in shapes):
    raise SystemExit("expected polygonal Geofabrik boundary")
geometry = {"type": "GeometryCollection", "geometries": shapes}
serialized = json.dumps(geometry, separators=(",", ":")).replace("'", "''")
template = args.template.read_text()
assert template.count("__GEOJSON_GEOMETRY__") == 1
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(template.replace("__GEOJSON_GEOMETRY__", serialized))
print("Wrote local PostGIS integration fixture from downloaded boundary")

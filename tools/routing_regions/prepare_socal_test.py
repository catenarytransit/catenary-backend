#!/usr/bin/env python3
"""Prepare a local-only Southern California routing/ingestion test fixture.

The caller downloads the Geofabrik GeoJSON first. No region geometries or
runtime TOML configurations are checked into the repository.
"""
import argparse
import json
import os
from pathlib import Path
import tomllib

FEEDS = (
    "f-9mu-orangecountytransportationauthority",
    "f-9qh-metrolinktrains",
    "f-9q5-metro~losangeles",
    "f-9q5b-longbeachtransit",
    "f-9q5c-bigbluebus",
    "f-9q5b4-pvpta~ca~us",
    "f-lagunabeachtransit",
)


def validate_geojson(path: Path) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    kind = document.get("type")
    if kind == "FeatureCollection":
        geometries = [f.get("geometry") for f in document.get("features", [])]
    elif kind == "Feature":
        geometries = [document.get("geometry")]
    elif kind in {"Polygon", "MultiPolygon"}:
        geometries = [document]
    else:
        raise ValueError(f"unexpected Geofabrik GeoJSON type: {kind!r}")
    if not geometries or any(
        not isinstance(g, dict) or g.get("type") not in {"Polygon", "MultiPolygon"}
        for g in geometries
    ):
        raise ValueError("Geofabrik boundary must contain nonempty polygonal geometries")


def prepare(geojson: Path, output_dir: Path, database_url: str, transitland: str) -> None:
    if not database_url:
        raise ValueError("database URL is required")
    validate_geojson(geojson)
    output_dir.mkdir(parents=True, exist_ok=True)
    region_file = output_dir / "config.toml"
    boundary = os.path.relpath(geojson.resolve(), output_dir.resolve())
    region_file.write_text(
        "schema_version = 1\n"
        'data_dir = "compiled/routing"\n\n'
        "[[regions]]\n"
        'id = "southern-california"\n'
        'name = "Southern California integration test"\n'
        f"geojson = [{json.dumps(boundary)}]\n",
        encoding="utf-8",
    )

    maple_config = Path("catenaryconfig.toml")
    feeds = ",\n  ".join(json.dumps(feed) for feed in FEEDS)
    maple_config.write_text(
        "[maple]\n"
        f"transitland = {json.dumps(transitland)}\n"
        "use_girolle = false\n"
        "no_elastic = true\n"
        "delete_before_ingest = false\n"
        "threads_gtfs = 2\n"
        "force_ingest_all = true\n"
        "bypass_temp_delete = false\n"
        'gtfs_zip_temp = "./temp/gtfs_zip"\n'
        'gtfs_uncompressed_temp = "./temp/gtfs_uncompressed"\n'
        f"only_feed_ids = [\n  {feeds}\n]\n\n"
        "[postgres]\n"
        f"database_url = {json.dumps(database_url)}\n"
        "max_connections = 8\n\n"
        "[osm_manager]\n"
        'data_dir = "temp/osr-data"\n'
        'temp_dir = "temp/osr-working"\n'
        'osr_extract = "./osr-extract"\n'
        'regions = ["north-america/us/california/socal"]\n',
        encoding="utf-8",
    )
    with region_file.open("rb") as file:
        region = tomllib.load(file)
    with maple_config.open("rb") as file:
        maple = tomllib.load(file)
    assert len(region["regions"]) == 1
    assert region["regions"][0]["id"] == "southern-california"
    assert maple["maple"]["only_feed_ids"] == list(FEEDS)
    print("Generated ignored routing and Maple test configurations")
    print(f"  boundary: {geojson}")
    print(f"  routing config: {region_file}")
    print(f"  Maple feed allowlist: {len(FEEDS)} feeds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geojson", type=Path, required=True)
    parser.add_argument("--region-dir", type=Path, default=Path("routing_regions"))
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--transitland", default="./transitland-atlas")
    args = parser.parse_args()
    prepare(args.geojson, args.region_dir, args.database_url, args.transitland)

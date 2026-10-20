# Gentian routing regions and Southern California test

Production routing regions are caller-supplied TOML configurations. They can
contain multiple GeoJSON polygons and ISO country selectors. The configs and
downloaded boundaries are Git-ignored. A selected château is included *whole*
when its hull intersects the region; stops and trips are not geographically
clipped. Production does not default to Southern California.

## Reproduce the SoCal integration environment

```sh
mkdir -p routing_regions temp
curl -fL https://download.geofabrik.de/north-america/us/california/socal.geojson \
  -o routing_regions/socal.geojson
DATABASE_URL='postgres://postgres:postgres@localhost:5432/catenary' \
  python3 tools/routing_regions/prepare_socal_test.py \
  --geojson routing_regions/socal.geojson
```

This writes ignored `catenaryconfig.toml` and `routing_regions/config.toml`.
The Maple configuration includes only seven requested feeds: OCTA, Metrolink,
LA Metro, Long Beach Transit, Big Blue Bus, PVPTA, Laguna Beach Transit. It
also configures `osm_manager` for the Geofabrik SoCal OSR extract.

Run the standalone OSR manager with the same Catenary config:

```sh
cargo run --release --manifest-path tools/osm_manager/Cargo.toml -- \
  --config catenaryconfig.toml run
```

The OSR extractor `osr-extract` must be installed at the configured path.
Use Diesel migrations and Maple to populate PostgreSQL. Then:

```sh
cargo run --release --bin gentian -- --region southern-california \
  --list-chateaux
cargo run --release --bin gentian -- --region southern-california \
  --export-stops temp/stops.tsv
cargo run --release --manifest-path tools/gentian_footpaths/Cargo.toml -- \
  --osr-dir temp/osr-data/regions/north-america/us/california/socal \
  --stops temp/stops.tsv --output temp/footpaths.tsv
cargo run --release --bin gentian -- --region southern-california \
  --first-utc-day 20745 --day-count 35 --build \
  --footpaths temp/footpaths.tsv --output temp/socal-timetable.bin
```

The GitHub Actions workflow `routing-regions-socal.yml` runs this integration
pipeline through `workflow_dispatch`. There is no separate spatial-only job.

## Data model and limitations

GTFS is streamed one source at a time, querying `stops`, `calendar`,
`calendar_dates`, `routes`, `itinerary_pattern_meta`,
`itinerary_pattern`, and `trips_compressed` separately using the
`(onestop_feed_id, attempt_id)` index prefixes. This avoids a single massive
cross-table join. The itinerary pattern carries time offsets and the direction
pattern establishes the shared stop order. Routes must be separated when trip
times overtake one another (RAPTOR, Delling–Pajor–Werneck, ALENEX 2012).

The current compiled representation contains source-qualified IDs, scheduled
transport service bitfields, event-major relative stop-time deltas, and
OSR-routed walking edges. Times are converted from local GTFS service days via
the agency timezone using the GTFS service-day noon-offset convention.
The file is validated and can be opened with `MappedTimetable`.

**This is an initial compiler and not a complete production RAPTOR engine.**
The program fails instead of fabricating times when a source has frequency-based
service, non-minute-aligned stop times, or any stop-time data that does not fit
the existing packed representation. In particular, the current ingestion tables
do not preserve per-stop pickup/drop-off permissions; marking every stop as
boardable is not correct for all feeds. Until those bits are preserved,
implementations must not deploy this compiled result as an authoritative journey
planner. The query algorithm, stronger resource-bounded external-sort spill,
transitive footpaths, and rigorous multi-date/DST/regression tests remain
required before production use. The OSR worker currently computes a bounded
walking neighborhood using an in-memory stop index, not a fully disk-backed
planet-scale preprocessing algorithm. The SoCal OSR graph cannot route footpaths
outside its extracted area even if a selected château extends beyond it.

Do not run the full integration workflow on every pull request: Geofabrik
extracts, live GTFS ingestion, and the native OSR build are heavyweight.

# Gentian routing regions

Regions are configured **locally**, not embedded in the production build.
`routing_regions/*.toml` and downloaded `routing_regions/*.geojson` are
Git-ignored. A region can select arbitrary local Polygon/MultiPolygon GeoJSON,
or countries from a pinned local country-boundary FeatureCollection.

A château is included in full if its hull intersects the region geometry
(`ST_Intersects` with a GiST-compatible `&&` prefilter). Stops, trips and
feeds are **not clipped** to the geographic boundary. Missing château hulls
cannot be selected until repaired upstream. Country and GeoJSON selectors can
be combined.

## Repeatable Southern California integration test

The SoCal example is a **test only**, not a production default. From the
repository root:

```sh
mkdir -p routing_regions
curl --fail --location --retry 3 \
  https://download.geofabrik.de/north-america/us/california/socal.geojson \
  -o routing_regions/socal.geojson
DATABASE_URL='postgres://postgres:postgres@localhost:5432/catenary' \
  python3 tools/routing_regions/prepare_socal_test.py \
  --geojson routing_regions/socal.geojson
DATABASE_URL='postgres://postgres:postgres@localhost:5432/catenary' \
  cargo run --bin gentian -- \
  --region southern-california \
  --regions-config routing_regions/config.toml --list-chateaux
```

The preparation script creates an ignored `catenaryconfig.toml` that uses
Maple's real `[maple].only_feed_ids` setting for exactly seven feeds:
OCTA, Metrolink, LA Metro, Long Beach Transit, Big Blue Bus, PVPTA, and Laguna
Beach Transit. It disables Elasticsearch for the test, defaults to two GTFS
ingestion threads, and writes a separate ignored region config.

The [GitHub Action](../.github/workflows/routing-regions-socal.yml) runs a
fast synthetic PostGIS integration test on every relevant PR and push; its
manual `workflow_dispatch` option can additionally ingest the seven real
feeds. Manual ingestion downloads live GTFS data and has no fixed runtime
or guaranteed availability.

Production configuration may contain any number of regions, including
non-SoCal regions. No SoCal boundary or production routing config is tracked.

## Format

```toml
schema_version = 1
country_boundaries = "my-pinned-countries.geojson" # only for countries

[[regions]]
id = "example-region"
name = "Example"
geojson = ["my-region.geojson"]
# countries = ["US", "MX"]
```

Relative paths resolve from the TOML directory. Input geometry must be
Polygon or MultiPolygon (or a Feature/FeatureCollection thereof). Country
features must expose an ISO alpha-2 code via `ISO3166-1-Alpha-2`, `ISO_A2`,
`ISO_A2_EH` or `iso_a2`.

Gentian currently writes a **V2 schema-only timetable** and registers selected
sources; it does not yet compile routes, stop times or transfers.

Run Rust unit tests with:

```sh
cargo test --lib routing_common::routing_regions
```

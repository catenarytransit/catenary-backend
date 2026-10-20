# Routing regions (Gentian)

Routing regions are **timetable selection areas**, not spatial clipping masks.
A chateau belongs to a region when its stored hull intersects the region's geometry.
When selected, the **entire chateau** and its completed production GTFS feeds
belong to the timetable, including stops and trips outside the polygon.

This directory is independent from `region_config/` (SEO/localization) and
Globeflower's OSM extraction regions.

## Southern California test

`routing_regions/config.toml` provides `southern-california`, an approximate
test polygon spanning coastal and inland Southern California. The polygon is not
a legal county boundary. It can be replaced by a more precise MultiPolygon
without changing the program. Chateaux that overlap its edge are deliberately
included in full; a national or statewide chateau may extend well outside SoCal.

Resolve the selection from a PostGIS-enabled Catenary database:

```sh
DATABASE_URL='postgres://...' cargo run --bin gentian -- \
  --regions-config routing_regions/config.toml \
  --region southern-california --list-chateaux
```

Write a V2 **schema-only** binary with selected source IDs (no routes or stop
events are compiled by the current Gentian implementation):

```sh
DATABASE_URL='postgres://...' cargo run --bin gentian -- \
  --regions-config routing_regions/config.toml \
  --region southern-california \
  --output compiled/southern-california.bin
```

The existing `--source CHATEAU,FEED,ATTEMPT` form remains supported for the
schema-only binary, without a database or routing-region configuration.

## Configuration

Each `[[regions]]` entry has an `id` (lowercase ASCII slug), a human-readable
`name`, and at least one of:

- `geojson = ["path.geojson"]`: Polygon/MultiPolygon GeoJSON geometry,
  Feature, or FeatureCollection. Multiple paths are unioned for selection.
- `countries = ["US", "CA"]`: country boundaries from the configured
  `country_boundaries` GeoJSON FeatureCollection, selected by ISO 3166-1
  alpha-2 country code. Both selectors can be combined.

Relative paths are resolved from the TOML file directory. Country boundary
features must have an `ISO3166-1-Alpha-2`, `ISO_A2`, `ISO_A2_EH`, or
`iso_a2` property with a valid two-letter code. Use a pinned local boundary
dataset; the compiler never downloads mutable boundaries during a build.
Missing requested countries produce an error.

The resolver uses `gtfs.chateaus.hull && region_geom` as an indexed bounding
box prefilter and `ST_Intersects(hull, region_geom)` for exact selection,
including touching boundaries. The GeoJSON is checked for polygonal structure
and PostGIS validates the assembled geometry. Null château hulls are excluded;
these must be repaired upstream rather than guessed.

The source registry includes one latest successfully completed and active
production `gtfs.ingested_static` attempt per feed belonging to a selected
chateau, with stable sorted source index assignment. This is a snapshot of the
database at query time, **not yet** a transactionally pinned timetable
compilation; implement the latter when schedule compilation is added.

## Testing

```sh
cargo test --lib routing_common::routing_regions
```

The tests validate the sample file, country selection, absent ISO codes,
duplicate IDs, and malformed polygons. An integration test against PostgreSQL
should additionally verify exact hull intersection versus bounding-box-only
false positives and whole-chateau inclusion.

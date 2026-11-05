# Edelweiss response metadata

`debugOutput.timings.metadata_ms` measures metadata key collection, worker queueing,
connection establishment, PostgreSQL queries and row/shape decoding. Previously it
also included OSR street geometry reconstruction and JSON assembly. The new
`response_build_ms` measures that entire response-building phase, **including**
metadata; do not add it to `metadata_ms` when calculating totals.

Only trips present in returned journeys are queried. Keys are deduplicated with
the compiled feed/attempt/trip identity, then grouped by chateau. One batch runs
per chateau on a dedicated four-thread Rayon pool shared by all HTTP requests.
At most four metadata database operations run simultaneously in this process.
Each batch currently opens its own PostgreSQL connection; there is no persistent
metadata connection cache. SQL errors retain the chateau context and fail the
request as before.

Trip and route candidates use chateau and public GTFS IDs. Feed identity is a
preference when IDs collide within a chateau, so an unrelated feed cannot replace
an available matching source. If the compiled feed ID is no longer present, a
matching GTFS ID in the same chateau can provide metadata. Import ranking still
prefers completed production imports over incomplete/newer imports within that
source. Compiled attempt IDs only identify response legs and never filter DB rows.

Materialized candidate CTEs load import status once per distinct candidate rather
than repeating those joins for every returned ride. Once a trip or route is
selected, its feed/attempt provenance keeps agency, pattern and shape joins
unambiguous. GTFS agency and shape IDs can repeat between feeds and attempts.
Non-detailed responses retain shape color fallbacks but skip `ST_AsGeoJSON` and
shape transfer/decoding.

## Existing indexes

No migration is needed. These indexes already exist in the migration history:

| Lookup | Index |
| --- | --- |
| Trip candidate | `trips_compressed_idx_chateau_id_trip_id (chateau, trip_id)` |
| Route candidate | `idx_chateau_route (chateau, route_id)` |
| Agency candidate | `idx_agencies_chateau_agency_id (chateau, agency_id)` |
| Import status | `ingested_static` primary key `(onestop_feed_id, attempt_id)` |
| Itinerary pattern | `itinerary_pattern_meta` primary key `(onestop_feed_id, attempt_id, itinerary_pattern_id)` |
| Direction pattern | `direction_pattern_meta` primary key `(onestop_feed_id, attempt_id, direction_pattern_id)` |
| Shape | `shapes` primary key `(onestop_feed_id, attempt_id, shape_id)` |

The previous query also contained chateau/GTFS ID predicates and could use these
indexes. A repository inspection cannot prove which plans the deployed database
uses, nor quantify a production speedup. Verify deployed migrations, table
statistics and `EXPLAIN (ANALYZE, BUFFERS)` for representative batches before
attributing remaining latency to an index. Use the six parameters in
`metadata_for_chateau`: chateau, compiled feeds, compiled attempts, trip IDs,
route IDs and the detailed-geometry flag. Compare detailed on/off and multiple
chateau requests with the corrected timings. Avoid forcing planner settings in
production.

## Isolated SQL regression tests

The test script runs the actual SQL extracted from the Rust source in PGlite's
PostgreSQL engine. It needs no production database. It creates minimal fixtures
and stubs `ST_AsGeoJSON`; it does not validate PostGIS geometry behavior.

```sh
npm install --prefix /tmp/edelweiss-test @electric-sql/pglite@0.5.15
PGLITE_MODULE=/tmp/edelweiss-test/node_modules/@electric-sql/pglite/dist/index.js \
  node tools/test_edelweiss_metadata.mjs
cargo test --bin edelweiss
```

Cases cover import rotation, incomplete imports, duplicate GTFS IDs in different
feeds/chateaux, changed feed identity, missing-trip route fallback, missing public
route ID, exact pattern/shape/agency provenance, direction shape fallback,
non-detailed serialization bypass and distinct compiled response identities.
The plan check disables sequential scans **only inside this isolated fixture**
to verify index eligibility. It is not a production query-plan benchmark.

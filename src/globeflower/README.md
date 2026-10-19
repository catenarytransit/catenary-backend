# Globeflower

Globeflower builds the LOOM Chapter 3 GTFS line graph and runs topo, streaming one connected component at a time.

## Select modes of transport (MOTs)

By default, Globeflower continues to select exact GTFS `route_type` 0 (tram) and 1 (metro). Use `--mots` (alias `--mot`) to select other modes or combine multiple modes. Mode names include the corresponding extended GTFS route types; numeric codes select only that code.

```bash
# Existing default: tram + metro
cargo run --release --bin globeflower -- -o tram-metro.geojson

# Mainline rail, including extended GTFS railway subtypes 100-117
cargo run --release --bin globeflower -- --mots rail -o rail.geojson

# Experiment with rail and metro together in a particular chateau
cargo run --release --bin globeflower -- --mots rail,metro --chateau amtrak -o test.geojson

# Isolate a single GTFS type (no extended-type expansion)
cargo run --release --bin globeflower -- --mots 2 --chateau amtrak -o rail-2.geojson

# Fail before materializing an unexpectedly huge connected component
cargo run --release --bin globeflower -- --mots rail --max-component-patterns 5000 -o rail.geojson
```

Named choices: `tram`, `light_rail`, `metro` / `subway`, `rail` / `train`, `bus`, `ferry`, `cable_tram`, `gondola`, `funicular`, `trolleybus`, `monorail`, `all`, or numeric GTFS route types. Multiple names/codes may be comma-separated. This is a GTFS filter, **not** an OSM track-mode filter.

### Rail-specific considerations

Mainline rail GTFS feeds often have hundreds or thousands of route IDs and different route/shape semantics than metro. The planner starts with production `gtfs.routes`, retains route keys rather than materializing `shapes_list` arrays, and batches route-to-direction-pattern membership joins. Full stop sequences and shapes are loaded one connected component at a time.

`--max-component-patterns` is an optional protective *fail-fast limit*, not a truncation or a splitting heuristic. Geographic proximity and shared stations can join multiple rail services into a very large component. For a smaller reproducible experiment use `--chateau`, an exact route type such as `--mots 2`, or a combination. Output still groups GTFS lines by feed/attempt/route ID, preserving distinct route identities rather than collapsing them into a single railway.

## Faster direction-pattern loading

The component planner queries direction-pattern station memberships in batches of
selected GTFS routes. Previously the PostgreSQL query performed `SELECT DISTINCT`
on every pattern/stop/coordinate row, even though the underlying route-workset
and GTFS primary keys already make each stop occurrence unique. The new path
preserves repeated stop visits (which have no effect on the connectivity union)
and avoids that query-wide deduplication. Component stop loading also skips its
global `ORDER BY` because Rust already sorts stop sequences for every pattern.

Apply Diesel migrations before testing the planner; the new composite index on
`gtfs.direction_pattern_meta(onestop_feed_id, attempt_id, route_id)` also
includes the selected pattern ID and route type for the metadata lookup.
**Warning:** This migration builds a regular PostgreSQL index and can block
writes while it runs. Schedule it for an appropriate maintenance window on
a production-size GTFS database.

```bash
# Default 1024 selected routes per planner membership SQL query.
RUST_LOG=info cargo run --release --bin globeflower -- \
  --mots rail --planner-route-batch-size 256 --output rail-test.geojson

# To identify unusually slow batches:
RUST_LOG=info,globeflower=debug cargo run --release --bin globeflower -- \
  --mots rail --planner-route-batch-size 512 --output rail-debug.geojson
```

`--planner-route-batch-size` takes 1–8192; it is independent of
`--planner-row-limit`. Smaller values reduce the number of routes in
each database query, but increase round trips. Compare 256, 512 and 1024
on the same dataset; no particular speedup is guaranteed without timings.

Timing messages distinguish `[planner][route_seeds]`,
`[planner][memberships]` (SQL/transfer vs. Rust union),
`[gtfs2graph][stops]`, `[gtfs2graph][shapes]`, and
`[gtfs2graph][weights]`. If SQL/transfer is dominant, compare actual
database plans using `EXPLAIN (ANALYZE, BUFFERS)` for representative
membership worksets. Avoid measuring a cold-cache first run against a
warm-cache second run without noting the difference.

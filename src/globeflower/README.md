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

Mainline rail GTFS feeds often have hundreds or thousands of route IDs and different route/shape semantics than metro. The planner starts with production `gtfs.routes`, retains route keys rather than materializing `shapes_list` arrays, and loads direction-pattern metadata per production feed attempt, then reads each selected pattern's ordered stops directly in bounded batches using feed/attempt/pattern indexes. Stop coordinates are fetched separately by indexed stop ID and associated in memory. This eliminates the planner's large route/direction-pattern/stop joins, while preserving station identity and proximity-based component grouping. Full stop sequences and shapes for graph construction are still loaded one connected component at a time.

`--max-component-patterns` is an optional protective *fail-fast limit*, not a truncation or a splitting heuristic. Geographic proximity and shared stations can join multiple rail services into a very large component. For a smaller reproducible experiment use `--chateau`, an exact route type such as `--mots 2`, or a combination. Output still groups GTFS lines by feed/attempt/route ID, preserving distinct route identities rather than collapsing them into a single railway.

### Planner direct-read performance

The component planner uses direct reads from `gtfs.direction_pattern_meta`, `gtfs.direction_pattern`, and `gtfs.stops` instead of a multi-table planner join. Each direct direction-pattern query handles at most 256 pattern IDs (or fewer if configured via `--planner-row-limit`), followed by one stop-coordinate lookup for the referenced stops. This is **not** one SQL round trip per pattern; it amortizes latency without joining the large tables. Existing production/MOT/chateau filters are applied before reading the sequences. Graph materialization remains component-scoped and still performs joins; this change targets **planning** only.

Compare before/after with `RUST_LOG=info cargo run --release --bin globeflower -- --chateau YOUR_CHATEAU --mots rail --max-component-patterns 5000 -o /tmp/globeflower-test.geojson`. For database performance, check `EXPLAIN (ANALYZE, BUFFERS)` on the old join and each direct query with representative parameters. A speedup depends on table sizes, index coverage, and database latency, and is not assumed.

### Direction-pattern loading progress

The component planner first enumerates selected direction-pattern IDs per production feed/attempt, then displays a terminal progress bar for the subsequent direct stop-sequence and station lookup batches. Each increment means a full batch has finished processing, including empty patterns; the percentage is based on the exact count of selected pattern IDs rather than feed count or raw stop rows. In a TTY, the bar displays completed/total, percentage, elapsed time and estimated remaining time. For logs and CI without a terminal, it emits newline-separated updates at approximately 10% intervals instead of carriage returns. No new command-line flags are needed.

Example interactive display:

```text
[planner] Loading direction patterns [################----------------] 500/1000 (50%) elapsed 12.0s ETA 00:12
```

### Large rail components: bounded materialization

Component discovery can connect thousands of mainline rail patterns into one geographic workset. Previously `build_component` submitted every pattern to a single PostgreSQL JSONB workset join, which can spend minutes executing without visible progress. Graph materialization now submits groups of at most 128 pattern keys for stop occurrences, referenced shapes and itinerary weights. The existing graph-building and topology logic is unchanged, and the complete connected component is still processed together.

The CLI prints separate progress indicators for loading component stops, shapes, itinerary weights and aligning shape geometry. This is not an out-of-core graph builder: very large components can still need substantial RAM and processing time after SQL loading. Running `EXPLAIN (ANALYZE, BUFFERS)` on representative batches and comparing output geometry before and after the change is recommended.

### Optimized shape loading for national rail

Globeflower first loads itinerary shape IDs and trip cardinalities through direct, bounded queries against `gtfs.itinerary_pattern_meta`. It preserves multiple shape variants for the same direction pattern, with `direction_pattern_meta.gtfs_shape_id` as the fallback for null itinerary shape IDs, and retains a one-trip fallback for patterns with no itineraries. It then deduplicates referenced `(feed, attempt, shape_id)` keys across the entire component and requests each unique geometry only once directly from `gtfs.shapes` by its primary key. Progress now reports **unique shapes**, rather than repeatedly counting direction patterns while loading duplicate geometries.

This avoids the earlier per-batch `workset -> direction_pattern_meta -> itinerary_pattern_meta -> shapes` join. Large geometry conversions can still take time, and out-of-core graph materialization is not part of this change. Compare returned unique shape counts, node/edge totals and GeoJSON against a known smaller chateau before deploying on a full national feed.

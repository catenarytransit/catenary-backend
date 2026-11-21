# Meadowsweet — external-memory OSM railway corridor compiler

**Meadowsweet is independent of Globeflower.** It accepts **only GTFS
`route_type=2`** and builds mainline railway corridors between consecutive
OSM stations on matched railway traversals. It explicitly excludes GTFS
tram, subway, and other route types. A logical station segment can contain an
arbitrarily long physical path: it is not limited by spatial tile size.

This draft builds a **partial pipeline**. It can now export GeoJSON rail corridor geometries, but does not yet
produce MapLibre vector tiles or claim complete GTFS-to-OSM map matching.

## Implemented stages

### 1. Build the physical railway graph

```bash
cargo run --release --bin meadowsweet -- build-osm-graph \
  --pbf /data/rail.osm.pbf \
  --output-dir /data/meadowsweet/osm \
  --memory-mib 64
```

A two-pass OSM PBF scan and bounded external sorting create:
- `edges.bin`: 64 bytes/edge; big-endian edge ID (u64), from and to OSM
  node IDs (i64), 8 metadata bytes (rail mode u8, signed grade class i8,
  reserved 6 bytes), then endpoint longitude/latitude f64 bit patterns.
- `adjacency.bin`: 24 bytes per directed adjacency record; node i64,
  edge ID u64, neighbor node i64.
- `manifest.txt`: graph format and record counts.

Physical edge IDs encode the OSM way ID and segment index; node IDs are
kept unchanged. Only OSM `railway=rail` and `railway=narrow_gauge` are extracted.
Bridge/tunnel grade classes are stored separately for future corridor checks.
OSM railway tags are infrastructure classifications, not proof of GTFS
route type; GTFS route provenance must be verified independently. No geography-based line splitting or global railway hash
table is required.

**Limits:** OSM ways with more than 65,535 edge segments, way IDs outside
48 bits, or missing referenced nodes fail clearly. The entire unfiltered
PBF's node coordinates are sorted on disk in v2; this is more I/O than
filtering to referenced nodes but avoids retaining all referenced IDs in
memory. The temporary scratch directory must not preexist. No guarantee
is made that the PBF and the station database represent the same OSM
snapshot; operators must coordinate the source versions.

### 2. Generate OSM station-to-track candidates

Catenary stores OSM station entities under `gtfs.osm_stations` with
`osm_type`, `osm_id`, `import_id`, `point`, `station_type`,
`mode_type` and `parent_osm_id`. The candidate generator reads the latest
available import of each primary rail-station identity, excluding child
platforms and stop positions.

```bash
DATABASE_URL=postgres://... \
cargo run --release --bin meadowsweet -- build-station-anchors \
  --graph-dir /data/meadowsweet/osm \
  --output-dir /data/meadowsweet/anchors \
  --radius-m 500 --memory-mib 64
```

The output `anchor_candidates.bin` uses fixed 56-byte records:

```text
station_osm_id: i64 BE
station_osm_type: u8  # 0=node, 1=way, 2=relation
reserved: [u8; 7]
physical_edge_id: u64 BE
projected_lon: f64 BE bits
projected_lat: f64 BE bits
fraction_along_physical_edge: f64 BE bits
distance_meters: f64 BE bits
```

The file stores **up to eight candidates per station**, sorted by distance
then physical edge ID. A station near two parallel tracks gets two
candidates, not an automatically accepted nearest-track connection.

Station and edge records are spatially tiled and externally sorted.
The spatial cell is 0.01 degrees. An edge may be replicated to nearby
cells for its configured search radius; crossing a cell boundary never
changes railway connectivity or canonical edge identity.

A cell exceeding 100,000 physical edges or an individual edge requiring
more than 4,096 cell copies causes a clear error rather than unbounded RAM.
Long OSM atomic edges need to be subdivided before processing. These are
safety limits, not proof that all real OSM data will pass unchanged.

**Critical distinction:** this step returns geometric candidates. It
does not establish which edge belongs to which station, split edges at
interior anchors, infer physical station platforms, or create a network
connection. Those require a topology-aware matcher and preferably GTFS
stop-platform evidence. Unresolved stations remain unresolved.

### 3. Segment a verified matched railway traversal

The CLI uses typed OSM station identities, avoiding collisions between
node/way/relation entities with the same numeric ID.

```bash
cargo run --release --bin meadowsweet -- segment \
  --input /data/verified-traversals.bin \
  --output-dir /data/meadowsweet/segments
```

Input stream v3 (route type is mandatory):

```text
repeat:
  start_osm_type: u8
  gtfs_route_type: u16 BE  # MUST be 2; any other value fails
  reserved: [u8; 5]
  start_osm_id: i64 BE
  edge_count: u64 BE
  repeat edge_count:
    physical_edge_id: u64 BE
    forward: u8  # 0 or 1
    arrival_osm_type: u8
    reserved: [u8; 6]
    arrival_osm_id: i64 BE  # i64::MIN if none
```

The source route type is checked on every traversal header before any\nphysical-edge records for that traversal are written. All future GTFS\nimporters must call `rail_policy::validate_gtfs_route_type` before emitting\nthe matched-stream format. Records written in the previous v2 header format\nwill fail validation and must be regenerated.\n\nStation arrival annotations must be generated by a *trusted* matcher that
follows topology and splits atomic edges when an OSM station anchor falls
inside an edge. The currently implemented `build-station-anchors` command
does not generate this input.

Output:
- `paths.bin`: 9 bytes per directed physical edge.
- `segments.bin`: 48 bytes per logical station span:
  `from_kind:u8, pad:7, from_id:i64, to_kind:u8, pad:7,
  to_id:i64, path_offset:u64, edge_count:u64`.

A 100,000-edge traversal between two consecutive OSM stations remains a
single logical segment. A GTFS train skipping an intermediate station
still has two segments if the physical traversal crosses that station
anchor. The writer also preserves single-edge closed station loops.

```bash
cargo run --release --bin meadowsweet -- inspect \
  --index /data/meadowsweet/segments/segments.bin \
  --path-file /data/meadowsweet/segments/paths.bin
```

Inspect checks record boundaries, nonempty spans, and path offsets, but
**does not yet verify railway-edge connectivity or station topology**.

### 4. Conservative exact physical-path corridor reduction

```bash
cargo run --release --bin meadowsweet -- reduce-exact-corridors \
  --index /data/meadowsweet/segments/segments.bin \
  --path-file /data/meadowsweet/segments/paths.bin \
  --output /data/meadowsweet/corridors.bin \
  --scratch-dir /data/meadowsweet/corridor-scratch \
  --memory-mib 64
```

For each segment, a streaming BLAKE3 fingerprint is computed over the
entire ordered physical-edge traversal, normalized under reversing
direction and swapping endpoints. Loops normalize under either direction.
Fixed 88-byte records are externally sorted. Equality is checked against
the actual edge sequences using bounded 4,096-edge chunks, so a rare hash
collision fails closed.

The output has one fixed 104-byte record per **exactly equal physical
path**; the first 88 bytes describe the representative (typed endpoints,
32-byte hash, first path offset, edge count, reverse-orientation flag),
followed by supporting traversal count u64 and reserved 8 bytes.

This is an intentionally conservative equivalence relation. Two GTFS
services taking different tracks of the *same* double-track alignment
can still produce two records. That is safer than averaging physically
different routes or declaring any two paths between A and B identical.

### 5. Sort station segments

```bash
cargo run --release --bin meadowsweet -- sort-segment-index \
  --input /data/meadowsweet/segments/segments.bin \
  --output /data/meadowsweet/segments/segments.sorted.bin \
  --scratch-dir /data/meadowsweet/segment-sort-scratch \
  --memory-mib 64
```

The 48-byte records sort by typed station identities, then path offset.

## Review of Globeflower's experimental corridor algorithm

`src/globeflower/corridor.rs` cannot be reused unchanged:
1. Its `OsmRailIndex` keeps the complete physical graph, adjacency and
   R-trees in RAM. Its output being disk-backed does not bound peak RSS.
2. `build_corridors_to_disk` also materializes a global
   `HashMap<Tile, Vec<EdgeIndex>>` and assumes an approximately 200 m
   halo. This is unsuitable for a station-to-station corridor which can
   span hundreds of kilometers or arbitrary tile boundaries.
3. Fragment union-find uses pairwise overlap. Geometric similarity is
   **not transitive**, and connected components of near-overlaps may be
   much wider or topologically different than any individual fragment.
4. `weighted_average_centerline_metric` averages merged geometries.
   That can invent tracks between existing physical railway alignments.
5. Clusters use locally incremented numeric IDs and centroid tile
   ownership. They are not durable global identities for a route path.
6. It preserves only loose local corridor overlap, not the full matched
   physical path or intermediate OSM station anchors.

The Meadowsweet reducer therefore keeps **full ordered edge sequences**
as the safety baseline. A later physical-corridor conflation algorithm
must establish a stronger correspondence: compatible mode/grade,
continuous topology, matching station/branch sequence, geometry proximity,
and path monotonicity. Only then may alternative physical tracks be
assigned one display-corridor identity. Do not use transitive union of
arbitrary spatial overlaps as the sole conflation criterion.

## Complexity and memory

- External graph build: sort/merge of fixed records with bounded
  in-memory runs; I/O proportional to the input size times the number of
  external merge passes.
- Spatial station join: sorting replicated railway-edge records and
  stations; retains only a cell of up to 100,000 physical edges.
- Matched traversal segmentation: `O(L)` sequential I/O and constant
  per-traversal state, where `L` is the number of physical edges.
- Exact-path reducer: sorting `R` paths with disk-streamed hashing;
  verifying repeated equal paths uses bounded buffers, but may require
  rereading long paths. RAM is independent of any single path length.

`--memory-mib` sets external-sort record batch memory, **not an exact RSS
limit**. PBF parser allocations, per-cell edge vectors, reader buffers
and allocator overhead add to the process footprint. Measure actual
RSS, page cache, memory.peak, Pss_File/Pss_Anon and elapsed time on
representative European rail extracts.

## 6. Export one geometry per canonical station-to-station corridor

Use `reduce-exact-corridors` first, then export the resulting physical
corridors as a streaming **GeoJSON FeatureCollection**. Every unique corridor
is one connected `LineString` from its canonical first OSM station toward
the next OSM station. Geometry consists of the exact OSM railway edge
coordinates; direction is normalized so opposite train services share a line.

```bash
cargo run --release --bin meadowsweet -- export-geojson \
  --graph-dir /data/meadowsweet/osm \
  --corridors /data/meadowsweet/corridors.bin \
  --path-file /data/meadowsweet/segments/paths.bin \
  --output /data/meadowsweet/railway-corridors.geojson
```

Color is **entirely optional**. Without `--color` or `--colors-file`, the
`color` property is not present. To apply one fallback color:

```bash
cargo run --release --bin meadowsweet -- export-geojson \
  --graph-dir /data/meadowsweet/osm \
  --corridors /data/meadowsweet/corridors.bin \
  --path-file /data/meadowsweet/segments/paths.bin \
  --output /data/meadowsweet/colored.geojson \
  --color '#E30613'
```

For individual corridor colors, supply a UTF-8 TSV of
`corridor_id<TAB>#RRGGBB`. The file **must already be sorted by
corridor_id**, ascending, without duplicates; this permits a streaming merge
without an in-memory color map. An override wins over `--color`; all
unmatched corridors inherit the fallback color or have no color property.
Unknown corridor IDs are errors, preventing silent typos.

```bash
cargo run --release --bin meadowsweet -- export-geojson \
  --graph-dir /data/meadowsweet/osm \
  --corridors /data/meadowsweet/corridors.bin \
  --path-file /data/meadowsweet/segments/paths.bin \
  --output /data/meadowsweet/colored.geojson \
  --colors-file /data/meadowsweet/corridor-colors.tsv
```

Each GeoJSON feature has:

```json
{
  "type": "Feature",
  "id": "000000000000000001-000000000000000002-<64-hex-digest>",
  "properties": {
    "corridor_id": "000000000000000001-000000000000000002-<64-hex-digest>",
    "route_type": 2,
    "from_station": {"osm_type": 0, "osm_id": 1},
    "to_station": {"osm_type": 0, "osm_id": 2},
    "support_count": 3,
    "physical_edge_count": 125,
    "color": "#E30613"
  },
  "geometry": {
    "type": "LineString",
    "coordinates": [[8.5, 47.4], [8.51, 47.41]]
  }
}
```

That example is illustrative, not actual route output. `color` is
omitted when no color is assigned. A MapLibre style can use
`coalesce(get('color'), '#888888')` for a default appearance while
retaining an uncolored geometry dataset.

**Correctness:** The exporter validates that every physical edge exists
and that directed edge endpoints connect. It normalizes reversal and uses
the oriented physical-edge path recorded by the exact-corridor reducer.
It does not fabricate connecting segments between physically disconnected
OSM tracks. Station anchoring itself remains provisional until the
GTFS-assisted station resolver is implemented.

**What "one line" currently means:** exact-path deduplication removes
duplicate traversals and reverse-direction duplication. Two parallel
tracks between the same stations are not yet topologically conflated into
one representative centerline. A distinct branch alignment remains a
distinct feature. Station sections are only as complete as the confirmed
station markers provided by the upstream matched traversal.

**Memory:** the entire GeoJSON document and the geometry of each corridor
are streamed; one 4096-edge path block and a 4096-slot direct-mapped
physical-edge cache are held in memory. The exporter currently performs
binary-search lookups on disk for individual edges, which is bounded in
RAM but may be I/O intensive on very large exports. A future external
edge-reference join would reduce random I/O. The output is ordinary
GeoJSON, not yet clipped or encoded into vector tiles.

## Validate physical topology of matched station segments

The segment writer accepts matched traversal files supplied by a future
GTFS-to-OSM matcher. It cannot assume they describe connected railway paths.
Run a disk-backed graph check before corridor deduplication:

```bash
cargo run --release --bin meadowsweet -- validate-topology \
  --graph-dir /data/meadowsweet/osm \
  --index /data/meadowsweet/segments/segments.bin \
  --path-file /data/meadowsweet/segments/paths.bin
```

This checks that every physical-edge ID exists in the 64-byte OSM graph
store and that each consecutive edge joins at the same OSM node in the
declared direction. It uses a 4096-slot direct-mapped edge lookup cache
and 64-byte-record binary search against the disk-backed edge store,
so it does not need the railway network in memory. Failure reports the
first missing edge or disconnected jump, with its station segment.

The validator does **not** establish that a segment's claimed start/end
station is correctly anchored, nor does it assert there were no skipped
intermediate station anchors; the accepted station-to-track resolution
stage must implement those checks. Since edge lookups use binary search,
the initial validation mode favors correctness and bounded RSS over
sequential disk I/O efficiency. A later pass can sort path-edge references
by edge ID for a scalable merge-join validation on country-scale data.

Station candidate output is also externally sorted by
`(station_osm_id, station_osm_type, physical_edge_id, ...)`, making
downstream grouping independent of spatial tile order.

## Not yet implemented

- GTFS stop/platform evidence for anchor acceptance, and proper
  station-area-to-physical-track connectivity.
- Exact/topology-constrained GTFS-to-OSM map matching with disk-backed
  Viterbi traceback and bounded A* search.
- Splitting OSM atomic edges at accepted interior station anchors.
- Track-to-centerline physical corridor conflation for parallel rails.
- Route-membership, service-frequency weighting, color selection and
  MapLibre vector tile generation.
- Cross-snapshot OSM/GTFS consistency verification.

Run `cargo test --bin meadowsweet` once a Rust toolchain and project
dependencies are available. Tests included in this PR have **not been
executed in this environment**.

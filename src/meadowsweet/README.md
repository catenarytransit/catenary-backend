# Meadowsweet — OSM station-to-station railway corridors

Meadowsweet is a **separate** rail-network compiler. Globeflower and Harebell
remain dedicated to their existing LOOM line-graph workflows. Meadowsweet does
not import Globeflower modules or modify the existing rail tile endpoint.

## Why a separate compiler?

A logical railway corridor connects consecutive **OSM stations on a matched
railway traversal**, even when those stations are very far apart or the train
does not stop at an intermediate station. Geographic grid boundaries must
never become logical station boundaries.

The physical graph stores OSM railway node-to-node edges. A station segment
is an ordered path through this graph, represented on disk by physical edge
IDs rather than a fully materialized geometry vector.

This PR introduces the *first usable foundations*: out-of-core OSM graph
extraction, external sorting, binary traversal-to-station segmentation, and
tests for extremely long segments. **It is not yet an end-to-end rail map
compiler.** The OSM station anchor extractor, GTFS-to-OSM matcher, physical
corridor conflation, global segment deduplication, and tile generation remain
to be implemented. Input traversal files must therefore come from a trusted
upstream matcher; Meadowsweet does not invent paths from GTFS endpoints.

## Build the disk-backed physical OSM rail graph

```bash
cargo run --release --bin meadowsweet -- build-osm-graph \
  --pbf /data/railway.osm.pbf \
  --output-dir /data/meadowsweet/osm \
  --memory-mib 64
```

The reader makes two sequential PBF scans. In the first scan it emits
railway-way endpoint references, then scans node coordinates to disk.
External fixed-record sort plus a merge join produces:

- `edges.bin`: 56-byte records `edge_id:u64, from:i64, to:i64,
  from_lon:f64_bits, from_lat:f64_bits, to_lon:f64_bits, to_lat:f64_bits`,
  all big-endian. Physical geometry is the line from one OSM way node to
  its next node.
- `adjacency.bin`: 24-byte records `node:i64, edge_id:u64,
  other_node:i64`, sorted by node and then edge ID.
- `manifest.txt`: format version and record counts.

Edge IDs encode 48-bit OSM way ID and 16-bit segment index, with explicit
range checks. The same railway way always yields the same physical edge IDs
provided its OSM node ordering and segmentation are unchanged. Versions
must still be regenerated after infrastructure edits.

The graph stage uses **external sorting** and does not retain a global
`HashMap` of rail nodes or ways. `--memory-mib` limits the *per-sort record
batch*, not total process RSS: PBF parsing buffers, merge readers, Rust
allocator state, kernel caches and concurrent processes still contribute.
The first implementation processes one stage at a time and uses 16-way merges.

The first graph builder deliberately writes coordinate records for every OSM
node before joining to railway endpoints. This is I/O-heavy, especially with
a country-wide unfiltered PBF, but it makes memory use bounded. Prefer an
OSM extract already filtered for railway infrastructure. Subsequent work
can add a disk-backed reference-membership filter to reduce I/O further.

The scratch directory must not already exist. It remains intact after a
failed build for diagnosis; remove it before retrying.

## Segment an already matched walk

A trusted upstream matcher writes a file containing repeated records:

```text
start_osm_station_id: i64 BE
edge_count:           u64 BE
repeat edge_count times:
    physical_edge_id: u64 BE
    forward:          u8 (0 or 1)
    arrival_station:  i64 BE (i64::MIN means no station)
```

The station marker is attached to the **arrival of a traversed physical edge**,
not the geographic proximity of an arbitrary point. A matcher must split
physical edges at station anchors when necessary. This v1 format is
intentionally simple; station anchors inside edges and ambiguous platforms
are an upstream concern, not silently inferred.

```bash
cargo run --release --bin meadowsweet -- segment \
  --input /data/verified-matched-traversals.bin \
  --output-dir /data/meadowsweet/segments
```

Output:
- `paths.bin`: 9 bytes per traversed edge (ID + direction).
- `segments.bin`: 32-byte logical segment index records:
  `from_station:i64, to_station:i64, path_offset:u64, edge_count:u64`.

A train crossing 100,000 physical edges without an intermediate OSM station
produces **one** logical segment. A train that passes a station without
stopping still produces two logical segments when the traversal contains its
anchor. Unfinished trailing spans are not emitted. Repeated station IDs are
allowed for real loops when the span contains multiple physical edges.

```bash
cargo run --release --bin meadowsweet -- inspect \
  --index /data/meadowsweet/segments/segments.bin \
  --path-file /data/meadowsweet/segments/paths.bin
```

The `inspect` command currently checks index record boundaries and path
ranges; it does not validate physical-edge connectivity or station anchoring.

```bash
cargo run --release --bin meadowsweet -- sort-segment-index \
  --input /data/meadowsweet/segments/segments.bin \
  --output /data/meadowsweet/segments/segments.sorted.bin \
  --scratch-dir /data/meadowsweet/sort-segments \
  --memory-mib 64
```

The sort key is `from_station` followed by `to_station`, then offsets.
Full route membership and path-aware corridor deduplication are **not**
performed by this command. An external sort alone cannot identify physically
equivalent paths across different parallel OSM tracks.

## Algorithmic constraints and next steps

1. Construct disk-backed OSM **station anchor** tables, preserving multiple
   anchors per station when platforms or track sides differ.
2. Match unique GTFS shape variants to the physical OSM graph using
   sequence-aware dynamic programming, disk-backed traceback and bounded
   graph-search state. A long distance between stations is not a search
   cutoff. Missing/ambiguous geometry must be explicitly reported.
3. Normalize parallel-track traversal into a physical **corridor identity**,
   preserving different paths between the same two stations.
4. Stream station-to-station physical paths to this format; perform global
   external-memory route membership aggregation keyed by verified canonical
   corridor identity. Do not equate a matching station pair with identical
   physical geometry.
5. Generate a single representative line per corridor in versioned vector
   tiles, with a route-set dictionary and deterministic primary color.

Complexity: the fixed-width graph-building phase is dominated by external
sorts of OSM nodes, railway endpoints and adjacency records. For `n`
records, `m` records per in-memory run and fixed merge fan-in `f >= 2`,
an upper bound is `O(n log(n) + n log_f(n/m))` CPU comparisons and
`O(n (1 + log_f(n/m)))` sequential record I/O. It uses `O(m + f)`
record storage plus bounded reader buffers and PBF parser overhead.
The station splitter uses `O(L)` sequential I/O and `O(1)` traversal
state for a walk of `L` edges.

Run tests with:

```bash
cargo test --bin meadowsweet
```

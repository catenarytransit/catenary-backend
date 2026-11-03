# Compressed processed Aspen snapshots

`compressed_aspen_dataset::CompressedAspenisedData` is a separate, versioned
representation of `aspen_dataset::AspenisedData`. It compresses **processed**
trip updates; `compact_rt_v2` continues to represent incoming GTFS-RT snapshots.
Neither the existing dataset's serialized layout nor the Aspen RPC signatures
change.

## Current integration

Aspen's resident **authoritative and backup** stores retain
`Arc<CompressedAspenisedData>` snapshots. Startup consumes legacy persisted
`AspenisedData` into the compressed format before inserting it into the
resident store. No expanded trip table is cached alongside a compressed snapshot.

Each realtime rebuild still assembles an ordinary mutable dataset. Trajectory
construction and periodic disk persistence use that working dataset before it
is consumed by compression on Tokio's blocking pool. The timestamp check still
prevents a stale candidate from replacing a newer authoritative snapshot. CPU
profiling reports `compress_authoritative_snapshot` separately. Working data and
the previous compressed snapshot can coexist during a rebuild, so peak RSS is
higher than steady-state resident storage.

Authoritative RPC lookups expand only selected trips into their existing response
types. Metadata, alerts, vehicles, shapes, counts, and SBB formation scans read
metadata or borrowed trip/stop views directly. Old-stop merging compares borrowed
descriptors and stop rows, materializing only preserved rows; the startup grace
period expands missing trips individually. Full-dataset and full-trip dumps
expand on the blocking pool after releasing the SCC guard. Backup insertion and
expansion also run on the blocking pool.

RPC signatures and disk formats remain compatible. Periodic persistence finishes
before publishing the compressed candidate so the working dataset can be moved
without retaining an expanded copy. The raw `compact_rt_v2` store is unchanged.

## Use

```rust,ignore
let snapshot = ordinary_dataset.into_compressed(); // consumes its input
if let Some(update) = snapshot.trip_updates().get("update-id") {
    let route_id: Option<&str> = update.trip().route_id;
    for stop in update.stops() {
        let departure = stop.departure().and_then(|event| event.time);
        let platform: Option<&str> = stop.platform_string();
    }
}
let updated_ms = snapshot.metadata().last_updated_time_ms;

// Compatibility boundaries only: these allocate ordinary trip/stop structs.
let ordinary_copy = snapshot.to_expanded();
let ordinary_dataset = snapshot.into_expanded(); // moves non-trip metadata
```

Views borrow the snapshot; reading stop IDs, platforms, descriptors, and events
does not allocate, clone strings, increment Arc reference counts, or decompress
other trips. An iterator yields one small view at a time. `to_owned()` is an
explicit allocation boundary for callers needing an independent ordinary value.

## Layout

- Stop rows are 12 bytes: dictionary stop ID, first-event index, u16 sequence,
  and u16 flags. Flags preserve sequence/event presence, old-data status,
  occupancy, and stop schedule relationship. Arrival and departure events are
  consecutive when both exist.
- A snapshot-local UTF-8 pool deduplicates descriptor strings, stop IDs,
  headsigns, and platform strings. Builders and their hash maps are discarded.
  Descriptor references select uniform, u8, u16, or u32 storage. Service dates
  and directions have numeric dictionaries. Empty strings remain distinct from
  absent strings; start times retain their original formatting.
- Delays select i16, i24, or i32 after checking **all** values in their column.
  Times select i32 offsets from the minimum timestamp if subtraction and range
  checks succeed; otherwise they retain full i64 timestamps. No time-zone or
  service-midnight assumptions are required.
- Modified selectors, uncertainty, stop properties, platform information, and
  other optional trip fields use independent adaptive columns. An absent column
  has no per-row allocation. A fully present column stores values directly.
  Otherwise construction compares sorted sparse indices, bitmap-rank indexing,
  and dense options using their resident element sizes. Extremely sparse reads
  use binary search; bitmap reads use a prefix count and one `count_ones()`.
- Platform info remains contiguous when dense. Its nested strings and sector
  vectors, and the contents of rare trip properties/consists, retain their
  original types; these nested objects are not dictionary-compressed here.
- Non-trip metadata and internal caches are moved intact. Serialization retains
  the original skip/default rules for SQL caches. The version enum allows a
  future format to be appended without changing V1's encodings.

Presence is independent of numeric value. `None`, `Some(0)`, an absent event,
and a present event with all absent fields remain distinct. Index and pool-size
limits are checked during construction instead of truncating. The compressed
format inherits Aspen's u16 stop sequences and i16 uncertainty; it does not
change the existing protobuf-to-Aspen narrowing conversions.

## Validation and measurement

The lightweight harness compiles the actual Aspen, formation, and compression
source files without the server's native dependencies. Only the unused SQL
cache payload types are substituted; this is not a full Aspen server build.
Install `protoc` first, then run:

```sh
cargo test --locked --manifest-path tools/aspen-compression-check/Cargo.toml
cargo run --locked --release --manifest-path tools/aspen-compression-check/Cargo.toml --bin measure
```

Tests cover serialized and in-memory round trips, empty snapshots/trips, absent
versus zero/empty values, all event-presence combinations, enum values, sparse
and dense platform data, modified service identity, numeric extremes, dictionary
width boundaries, bitmap word boundaries, and non-trip metadata preservation.

The benchmark uses 2,000 trips with unique trip IDs, 40 stops each, shared Arc
stop IDs, two events per stop, and no uncertainty. Its rail variant includes a
platform string and platform info at every stop. One local optimized run:

| Fixture | Ordinary live heap | Compressed live heap | Reduction | Build | Ordinary / compressed timestamp scan |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bus | 19,378,088 B | 2,173,055 B | 88.8% | 17.0 ms | 0.167 / 0.279 ms |
| Rail | 19,378,000 B | 8,892,974 B | 54.1% | 24.4 ms | 0.178 / 0.267 ms |

Scan times are totals over 100 scans divided by 100; both paths verify the same
checksum. Allocation figures track live **requested heap bytes**, not allocator
overhead, peak conversion memory, or process RSS. These synthetic fixtures have
empty non-trip metadata. Real savings depend on feed density, nested rail data,
and the fraction of memory in trip updates. The scan benchmark demonstrates a
read-cost tradeoff, not a speedup claim. Profile representative production
snapshots to measure steady-state RSS, rebuild peaks, and conversion latency.

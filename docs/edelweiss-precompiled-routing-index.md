# Precompiled Edelweiss routing indexes and parallel OSR startup

Edelweiss previously invoked `LowerBoundTopology::build()` for each region at every
startup. For large regions this also rebuilt FIFO transport chains and station
importance. The compiled timetable was already memory-mapped, but derived
routing indexes were regenerated.

## Compile-time generation

New Gentian builds automatically publish `<region>.routing-index.bin` after
the timetable, together with the existing realtime-identity and platform files.
The companion contains precomputed transport-chain event columns, lower-bound
graphs (both directions), station roots, transfer flags and station importance.

To backfill existing files, without PostgreSQL or OSR:

```sh
cargo run --release --bin gentian -- \
  --precompute-routing-index /home/catenary/routing-timetables/europe.bin
```

Repeat for each enabled timetable region. This is a **one-time expensive
offline build per timetable generation**, not work repeated at every boot.

The compiler uses a versioned binary representation with column offsets.
Large per-chain time and calendar arrays are **mmap-backed** when Edelweiss
opens the companion. Some structures, including the two directed CSR graphs,
are currently copied into their existing in-memory Rust representations during
loading. Full zero-copy graph access is a follow-up optimization.

## Deployment safety

Edelweiss requires a valid companion by default. A missing, truncated,
incorrectly versioned, or stale companion prevents startup. During rolling
migration only, set `EDELWEISS_ALLOW_ROUTING_INDEX_REBUILD=1` to permit the old
in-memory build; a warning identifies each affected region.

The companion's identity includes the timetable header, file size,
mtime and filesystem identity. This deliberately rejects a timetable
atomically replaced with a new generation. **If files are copied between
hosts, or restored with new inode/mtime, regenerate the routing index on
the destination host.** Do not update a published timetable in place.

For complete multi-artifact atomicity, deployment should stage a versioned
directory containing the timetable and all of its companions, then switch
the region's active directory after all files are complete.

## Parallel native OSR loading

Native OSR graph opens are independent and are now scheduled on a bounded
Rayon pool. The default is **three parallel graph loads**. Tune it with:

```sh
EDELWEISS_OSR_LOAD_WORKERS=2
```

Valid values are clamped to 1–8. This is independent of the HTTP routing
worker concurrency. For memory-constrained servers, use 1 or 2 to avoid
overlapping large ReadIntoMemory allocations. Existing
`osr_worker.region_load_modes` (including mmap) remain effective.

## Validation

```sh
cargo fmt --check
cargo test --lib routing_common::routing_index
cargo test --lib routing_common::transport_index
cargo test --lib routing_common::profile_compact
cargo build --release --bin gentian --bin edelweiss
```

On real timetable data compare forward and reverse RAPTOR/PONG results,
service-day selection and transfer-rule validity against the old in-memory
index before production rollout. Log phases
`precompiled routing index mapped` and
`OSR graphs loaded with N workers` quantify elapsed startup time.

The compiled index does not change static timetables or realtime
overlays, and no transport-selection or journey-reconstruction semantics
were intentionally altered.

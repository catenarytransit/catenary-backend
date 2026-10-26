# OSR matched-routing migration (Gentian and Edelweiss)

This integration pins `osr-ffi` to the merged raw-matching API at `aa7d39ef84b92e6086109ec7c7dda29f68f1dc08`.

## Gentian timetable compilation

`src/gentian/footpaths.rs` opens the OSR graph once per compiled region.
For each timetable location it calls:

1. `Router::get_raw_match(location, 250.0)` once to enumerate profile-independent nearby OSR ways.
2. `Router::append_lookup_match` with `MatchOptions { reverse: false, profile: Foot, ... }` for origin matches.
3. `Router::append_lookup_match` with `MatchOptions { reverse: true, profile: Foot, ... }` for destination matches.
4. `Router::route_one_to_many_cached` per stop with the previously computed destination indices selected by the existing geographic candidate filter.

The native stores avoid resnapping stops on each origin search. The output remains the existing directed footpath arrays in timetable seconds; station/platform fallback links, stop indexes, the mmap format, and the RAPTOR solver are not modified. OSR does not gain CPU throttling from this change: Gentian remains sequential and may still use high CPU during active routing. Benchmark precomputation and query phases separately.

## Edelweiss and the OSR worker

Edelweiss communicates with `osr_worker` through the existing newline-delimited JSON protocol. The new optional `direction` field is backward-compatible: omitted means `0` (forward).

- Access search: `direction: 0`, user position → candidate boarding stops.
- Egress search: `direction: 1`, backward OSR search from destination position → candidate alighting stops, representing legal stop → destination paths.
- Direct walking and leg-geometry requests stay `direction: 0`.

The worker uses per-request native source/destination match stores and `route_one_to_many_cached`. Its existing bounded thread pool, JSON response shape, geometry handling and results ordering stay intact. Per-request stores ensure coordinates and graph state cannot become stale; they are **not** a persistent cross-request stop cache.

## RAPTOR

The RAPTOR solver itself does not call OSR. It consumes the access/egress vectors and the timetable footpaths produced by these layers. This PR leaves provenance, boarding, and timetable search unchanged to avoid introducing unrelated journey-result regressions.

## Verification before merge

- `cargo build --release --bin gentian --bin osr_worker --bin edelweiss`
- `cargo test --bin gentian`
- Exercise an existing `osr_worker` query without a `direction` field, and queries with `direction: 0` and `direction: 1`.
- On a region with directed/one-way pedestrian access, verify forward access and backward egress differ as expected.
- Compare compiled Gentian footpath counts/durations before and after migration and check for unchanged stop indexes.
- Benchmark source/destination matching and one-to-many route phases separately on a representative regional timetable.

The FFI's native graph-local match structures are never serialized or shared between distinct graphs here.

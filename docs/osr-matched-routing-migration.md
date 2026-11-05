# OSR matched-routing migration (Gentian and Edelweiss)

This integration uses the raw-matching API in the pinned `osr-ffi` dependency.

## Gentian timetable compilation

`src/gentian/footpaths.rs` opens the OSR graph once per compiled region.
For each timetable location it calls:

1. `Router::get_raw_match(location, 250.0)` once to enumerate profile-independent nearby OSR ways.
2. `Router::append_lookup_match` with `MatchOptions { reverse: false, profile: Foot, ... }` for origin matches.
3. `Router::append_lookup_match` with `MatchOptions { reverse: true, profile: Foot, ... }` for destination matches.
4. `Router::route_one_to_many_cached` per stop with the previously computed destination indices selected by the existing geographic candidate filter.

The native stores avoid resnapping stops on each origin search. The output remains the existing directed footpath arrays in timetable seconds; station/platform fallback links, stop indexes, the mmap format, and the RAPTOR solver are not modified. OSR does not gain CPU throttling from this change: Gentian remains sequential and may still use high CPU during active routing. Benchmark precomputation and query phases separately.

## Standalone OSR worker protocol

The standalone `osr_worker` retains its newline-delimited JSON protocol for other clients. Edelweiss now uses the native backend described below. The optional `direction` field remains backward-compatible: omitted means `0` (forward).

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

## Persisted matches and in-process Edelweiss (timetable V6)

Gentian now retains its raw OSR candidates in the timetable itself. These
zero-copy columns are indexed by the exact `LocationIdx` order:

- `osr_match_graphs`: graph content fingerprint, node/way counts and raw radius.
- `osr_location_matches`: matched platform index and level, including an
  explicit unmatched-platform sentinel.
- `osr_candidate_offsets` and `osr_candidates`: CSR rows containing native way
  and endpoint node indices and geometric distances. Empty rows are valid.

This follows MOTIS's `way_matches_storage::preprocess_osr_matches`: persist
profile-independent geometric candidates, then apply profile, direction,
endpoint role and speed-dependent costs for the actual query. Gentian reuses
these same candidates for transfer compilation. The compiler verifies the
source graph is unchanged before publication. Schema-only timetables may omit
all four columns together. V5 and earlier timetables must be rebuilt with
Gentian; the reader rejects their versions explicitly.

Edelweiss loads the graphs directly using `osr-ffi` and no longer connects to
`osr_worker` for access, egress, direct routes or selected-leg geometry.
`--osr-workers` (default 3, range 1..16) bounds concurrent native searches across
HTTP requests. Graphs are shared read-only; match stores and search state are
call-local. Existing `[osr_worker.region_load_modes]` and
`[osr_worker].excluded_regions` settings also control the embedded graphs.
Remove the obsolete Edelweiss `--osr-worker ADDRESS` argument from service units.
The standalone worker remains available to its other clients.

Graph-local candidates are accepted only when the SHA-256 fingerprint and
node/way counts match the loaded graph, and the precomputed radius covers the
requested matching radius. The fingerprint covers names, lengths and contents
of every graph `.bin` file; it is independent of the directory name. Identity checks read graph artifacts sequentially before and after compilation
and native graph loading, never during queries. This adds startup I/O and
should be included in deployment startup-probe budgets. Publish graph directories atomically and keep their
contents immutable while either process is loading/using them. Graph updates
require restarting Edelweiss to load the new graphs.

A timetable compiled against a different graph partition or graph generation
safely falls back to fresh platform/raw matching on the chosen graph. Startup
logs show which timetable/OSR pairs can use precomputed matches. To get the
precomputation benefit, compile Gentian against the same OSR graph used by the
live request. Existing platform metadata sidecars remain useful for this
fallback. A missing native graph remains an explicit query error.

The first/last/direct cost searches still run concurrently. Geometry for
selected legs is reconstructed through native calls and identical requests
share a response-local cache. The current FFI does not expose retained
one-to-many search handles, so selected-leg geometry still performs a separate
search; embedding alone does not eliminate that search. No speedup percentage
is asserted without benchmarking the same graph, timetable and queries.

Validate with the routing kernel tests, builds/tests of Gentian and Edelweiss,
and the SoCal integration workflow. Compare walk/bike and depart-at/arrive-by
results, custom speeds, platform levels, graph mismatch fallback, and
`debugOutput.timings` before and after. No worker process should be running
when testing the embedded Edelweiss route path.

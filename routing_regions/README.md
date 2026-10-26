# Regional routing configuration

The runtime routing configuration lives at the **repository root** as
`routing_regions.toml` (ignored by Git). Copy `routing_regions.example.toml`
and adjust it for the regions you want to compile. Geometry and explicit
`timetable_path` paths are relative to the TOML file, and every intersecting château is included in full.
The `routing_regions/` directory is for local geography files, not config.

Run the Geofabrik OSR extractor manager using the independent crate at
`src/osm_manager/`. It writes OSR graphs under the `[osm_manager]`
`data_dir` in `catenaryconfig.toml`.

## Compile a timetable and walking footpaths together

```sh
cargo run --release --manifest-path src/osm_manager/Cargo.toml -- --config catenaryconfig.toml run
cargo run --release --bin gentian -- \
  --region southern-california \
  --regions-config routing_regions.toml \
  --osr-dir temp/osr-data/regions/north-america/us/california/socal \
  --max-seconds 900 --candidate-meters 1200 \
  --build --output ""
```

By default Gentian compiles a rolling 365-day service window starting today,
plus one preceding UTC day for overnight service (366 total UTC days).
The resolved dates are logged before compilation. For reproducible test builds,
pass both `--first-utc-day` (Unix days) and `--day-count` explicitly; the
Southern California CI fixture continues to use its pinned dates.

**OSR graph compatibility:** Before running Gentian, build the native OSR
reader and its matching extractor from the same pinned source:

```sh
cargo build --release --bin osr_worker
bash src/osm_manager/build_matched_extractor.sh
```

Do not generate a `routing.bin` using a separately downloaded OSR v0.0.4
release while querying it with the newer `osr-ffi` reader. The OSM manager
now fingerprints the extractor and regenerates previously cached output on
a change. The SoCal CI workflow verifies the graph opens with the native
reader immediately after extraction, before loading GTFS.

Gentian reads GTFS from PostgreSQL, generates the timetable's location indices
and routes, matches stop candidates against the local OSR graph, runs bounded
native OSR one-to-many searches, and directly writes forward and reverse
footpath arrays into the memory-mappable RAPTOR timetable. There are no
stop-export or footpath-import TSV intermediates. Native OSR is required for every
Gentian timetable build; `--build` fails unless `--osr-dir` is supplied.
If `--output` is omitted or passed as an empty string with `--region`, Gentian
writes the region's `timetable_path` (or the legacy `data_dir/<id>.bin` fallback).
A nonempty `--output` still overrides it. Edelweiss maps all region timetable
paths on startup; coordinate matching chooses the smallest containing transit
region, with region id as a deterministic tie-break. The separate OSR worker
chooses the appropriate extracted street graph for each endpoint.

## Station transfers and footpath matching (wire format v4)

Gentian now uses MOTIS's **25-metre footpath way-matching limit**, overriding
the more permissive 100-metre generic default in `osr-ffi`. The one-to-many
walking limit remains 900 seconds by default and OSR durations remain in exact
seconds.

GTFS `transfers.txt` must be persisted by **re-ingesting the affected feed**
through Maple after applying the `gtfs.transfers` migration. Old imported
attempts have no transfer rules in the database and must not be assumed to have
an empty `transfers.txt`. The transfer import preserves original source,
stop, route, and trip selectors. Gentian currently compiles **stop-scoped**
rules into a sparse, directed station-transfer table:

- Default change time is 120 seconds after transit alighting (MOTIS default);
  the first boarding from street access is not charged this interchange time.
- Explicit same-stop minimums can override that default.
- Explicit directed stop-pair minimums apply between vehicle alighting and
  subsequent boarding; they do not artificially lengthen physical OSR walks.
- `transfer_type=3` forbids a stop-pair interchange at boarding, even if a
  walking search visits intermediate stops.
- Rules referencing station parents are expanded to their child platforms
  within the same GTFS source.
- As in Nigiri, very close platforms and their shared parent station get
  conservative local links when the OSM graph lacks indoor passages. Only
  pairs within 100 metres are linked, at no faster than 0.7 m/s and with
  at least the station's minimum change time; an existing OSR route wins.
  No synthetic link is created between unrelated stations.
- Route/trip-scoped rules and stay-seated `transfer_type=4` are retained in
  PostgreSQL but require an additional multi-label / through-service routing
  implementation. Gentian logs how many it has not applied; it does not
  incorrectly turn such rules into unrestricted global footpaths.
- `transfer_type=1` timed connections have no automatic vehicle-holding
  mechanism; the scheduled departure still must be reachable.
- The first implementation keeps only the earliest arrival label per stop.
  Provenance is propagated for checking prohibitions, but alternate arrivals
  from different vehicles are not yet all retained; fully sound GTFS transfer
  restrictions require multi-label, provenance-aware RAPTOR.

The sparse transfer-rule column changes the mmap layout; **v3 binaries must be
rebuilt with v4 Gentian**. In addition, previously imported GTFS attempts must
be re-ingested to populate their stop-level transfer rules.

## Seconds-precision timetable, wire format v4

The timetable is **not** Nigiri's minute-based binary. Catenary v4 stores exact
UTC second offsets in 32-bit packed event values and preserves OSR walking
durations in 64-bit records (32-bit stop index + 32-bit duration in seconds).
Stop-level transfer durations are also stored in seconds. Day bitsets remain
calendar-day based; all routing label times and reported Unix timestamps use
seconds. No GTFS or OSR duration is rounded to a whole minute.

Files with timetable versions 2 and 3 are intentionally incompatible with v4 and
must be rebuilt by Gentian. The Edelweiss route request likewise uses
`access: [{stop, seconds}]`, `egress: [{stop, seconds}]`, and
`direct_walk_seconds` (instead of the old minute fields). Responses use
`seconds` for walking/transfer leg durations, and Unix seconds for
arrival/departure timestamps. The OSR worker's `reachable` list only
contains `seconds` durations. This API change is intentionally breaking.

The in-memory and on-disk v4 representation is larger than v2: event offsets
occupy four bytes instead of two, footpaths eight bytes instead of four,
and stop transfer durations four bytes instead of one.

The `--osr-dir` path is the extracted OSR dataset containing `routing.bin`,
not the parent manager output directory. One OSR graph is used per Gentian
invocation; regional OSR graph selection remains a future improvement.

For coordinate access/egress requests, Edelweiss continues to use a separate
long-running native OSR worker. This is **query-time** routing, not compilation.
At startup, `osr_worker` loads every configured regional OSR graph exactly
once through `osr-ffi::open_shared_router` before accepting any requests.
All `--workers` threads then concurrently read the same immutable routers,
using their own native match stores and Dijkstra search state. Missing graph
data is a startup error, not a request-time lazy-load. Linux may already share
mmap file pages, so compare PSS and peak anonymous RSS when measuring savings:

```sh
cargo run --release --bin osr_worker -- \
  --config catenaryconfig.toml
DATABASE_URL='postgres://...' cargo run --release --bin edelweiss -- \
  --regions-config routing_regions.toml \
  --catenary-config catenaryconfig.toml \
  --listen 127.0.0.1:40428
```

## Southern California integration test

`.github/workflows/routing-regions-socal.yml` downloads the Southern
California Geofabrik geometry, generates an ignored root `routing_regions.toml`,
ingests a small set of transit feeds, extracts the OSR graph, compiles the
regional timetable with native footpaths in Gentian, starts the OSR worker
and executes a UCI-to-Los-Angeles-City-Hall routing query.

CI verifies `libtbb.so` is a needed dependency of the native bridge, checks
the dynamic loader resolves shared libraries, and launches the final Rust
executables. It intentionally does not run `ldd -r` on the isolated bridge:
Rust `cxx` bridge symbols are resolved by the final executable and may be
undefined in the shared library alone.

## Current limitations

GTFS frequency windows are discretized into seconds-precision departures at
compile time (including headway-based `exact_times=0` windows); this does not
yet model stochastic headway waiting or frequency-specific realtime instance
matching. Per-stop boarding/alighting restrictions and cross-region route
federation remain incomplete. Footpath candidates are
limited by geographic distance, not proven complete for arbitrary cycling or
walking networks. The native OSR worker listens on loopback by default; it
does not provide authentication or TLS.

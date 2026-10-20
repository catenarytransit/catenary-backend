# Regional routing configuration

The runtime routing configuration lives at the **repository root** as
`routing_regions.toml` (ignored by Git). Copy `routing_regions.example.toml`
and adjust it for the regions you want to compile. Geometry paths are relative
to the TOML file, and every intersecting château is included in full.
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
  --first-utc-day 20745 --day-count 35 \
  --osr-dir temp/osr-data/regions/north-america/us/california/socal \
  --max-seconds 900 --candidate-meters 1200 \
  --build --output compiled/routing/southern-california.bin
```

Gentian reads GTFS from PostgreSQL, generates the timetable's location indices
and routes, matches stop candidates against the local OSR graph, runs bounded
native OSR one-to-many searches, and directly writes forward and reverse
footpath arrays into the memory-mappable RAPTOR timetable. There are no
stop-export or footpath-import TSV intermediates. Native OSR is required for every
Gentian timetable build; `--build` fails unless `--osr-dir` is supplied.

## Seconds-precision timetable, wire format v3

The timetable is **not** Nigiri's minute-based binary. Catenary v3 stores exact
UTC second offsets in 32-bit packed event values and preserves OSR walking
durations in 64-bit records (32-bit stop index + 32-bit duration in seconds).
Stop-level transfer durations are also stored in seconds. Day bitsets remain
calendar-day based; all routing label times and reported Unix timestamps use
seconds. No GTFS or OSR duration is rounded to a whole minute.

Files with timetable version 2 are intentionally incompatible with v3 and
must be rebuilt by Gentian. The Edelweiss route request likewise uses
`access: [{stop, seconds}]`, `egress: [{stop, seconds}]`, and
`direct_walk_seconds` (instead of the old minute fields). Responses use
`seconds` for walking/transfer leg durations, and Unix seconds for
arrival/departure timestamps. The OSR worker's `reachable` list only
contains `seconds` durations. This API change is intentionally breaking.

The in-memory and on-disk v3 representation is larger than v2: event offsets
occupy four bytes instead of two, footpaths eight bytes instead of four,
and stop transfer durations four bytes instead of one.

The `--osr-dir` path is the extracted OSR dataset containing `routing.bin`,
not the parent manager output directory. One OSR graph is used per Gentian
invocation; regional OSR graph selection remains a future improvement.

For coordinate access/egress requests, Edelweiss continues to use a separate
long-running native OSR worker. This is **query-time** routing, not compilation:

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

GTFS frequency expansion, per-stop boarding/alighting restrictions, and
cross-region route federation remain incomplete. Footpath candidates are
limited by geographic distance, not proven complete for arbitrary cycling or
walking networks. The native OSR worker listens on loopback by default; it
does not provide authentication or TLS.

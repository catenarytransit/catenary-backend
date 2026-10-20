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
cargo run --release --features gentian-osr --bin gentian -- \
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
stop-export or footpath-import TSV intermediates. The native module is gated by
`--features gentian-osr`, leaving unrelated backend builds independent of C++.

The `--osr-dir` path is the extracted OSR dataset containing `routing.bin`,
not the parent manager output directory. One OSR graph is used per Gentian
invocation; regional OSR graph selection remains a future improvement.

For coordinate access/egress requests, Edelweiss continues to use a separate
long-running native OSR worker. This is **query-time** routing, not compilation:

```sh
cargo run --release --features gentian-osr --bin osr_worker -- \
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

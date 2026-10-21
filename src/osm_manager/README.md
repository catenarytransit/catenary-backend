# osm_manager

Lightweight standalone Rust binary for importing Geofabrik OSM regions into
[MOTIS OSR](https://github.com/motis-project/osr). This crate has its own
Cargo manifest, so compiling it does **not** compile the backend's other binaries.

## Configure

Add the following to the usual `catenaryconfig.toml`:

```toml
[osm_manager]
data_dir = "/srv/catenary/osr"
temp_dir = "/mnt/fast-ssd/osm-processing"
osr_extract = "/usr/local/bin/osr-extract"
regions = [
  "https://download.geofabrik.de/europe-latest.osm.pbf",
  "https://download.geofabrik.de/north-america-latest.osm.pbf",
  # Small extracts and short Geofabrik paths also work:
  "europe/andorra"
]
```

Build only this utility:

```sh
cargo build --release --manifest-path src/osm_manager/Cargo.toml --bin osm_manager
src/osm_manager/target/release/osm_manager --config catenaryconfig.toml run
src/osm_manager/target/release/osm_manager --config catenaryconfig.toml locate --lat 42.5063 --lon 1.5218
```

`CATENARYCONFIG` is not needed for this utility; use `--config` to
select a file explicitly. The manager invokes a configured `osr-extract` executable. **Its on-disk
OSR serialization must match the `osr-ffi` reader that will load the data.**
An OSR release executable and a newer source checkout can produce incompatible
`routing.bin` files even when both builds succeed.

For Catenary, build the extractor using the *same* pinned OSR CMake source
and dependencies as the native Rust reader:

```sh
cargo build --release --bin osr_worker
bash src/osm_manager/build_matched_extractor.sh
cargo run --release --manifest-path src/osm_manager/Cargo.toml -- \
  --config catenaryconfig.toml run
```

Point `[osm_manager].osr_extract` to `./osr-extract` (or install the
matching executable elsewhere). The helper reuses the CMake tree generated
by the `osr-ffi` build, so the exporter and reader cannot drift across OSR
revisions or pinned Cista dependencies. It does not download a GitHub release.
The lightweight OSM manager itself remains buildable independently of the
backend; it only launches the configured extractor.

Each region is published under `data_dir/regions/<geofabrik path>/`.
Inside are the original native OSR `.bin` files (including `routing.bin`, which contains serialized node and way properties in OSR v0.0.4), `manifest.json`, and
`boundary.geojson`. The `data_dir/regions.geojson` feature collection
contains region IDs, relative dataset directories, source revisions,
and exact polygon boundaries for future coordinate -> region lookup.

The updater prefers Geofabrik `.osm.pbf.md5` checksum manifests,
falling back to HTTP ETag / Last-Modified / Content-Length validators.
It additionally fingerprints the extractor executable and records
`extractor_md5` in `manifest.json`. Replacing the extractor forces a
re-extract even when the OSM download has not changed. Previously written
manifests without that field are rebuilt automatically. The checksum is a
local cache key, not an authentication mechanism.
It streams each download to `temp_dir`, checks the MD5 when available,
extracts to an isolated staging directory on the output filesystem,
validates core files, and renames the staged dataset into place. On
extraction or checksum failure, the old published dataset remains intact.
Previously completed regions can remain updated if a later region fails.
A per-run lock protects against concurrent managers.

**Operational constraints:** OSR routes cannot cross independent
regional datasets; use one larger Geofabrik parent region if you
need seamless cross-border routing. Overlapping parent and child
regions are permitted; `locate` selects the smallest bounding box among
matching polygons. For large extracts, provision enough **free space** for
both old and replacement datasets plus the downloaded PBF.
Staging is within the output tree so renames stay on one filesystem.
The global `regions.geojson` is only regenerated after all selected
regions successfully finish.

The workflow `.github/workflows/osm-manager.yml` builds *only* this
crate, downloads the upstream Linux `osr-extract` release, extracts
Andorra with a tiny test TOML, verifies the OSR native files and
boundary registry, checks coordinate lookup, and checks idempotency.

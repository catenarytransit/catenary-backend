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
cargo build --release --manifest-path tools/osm_manager/Cargo.toml --bin osm_manager
tools/osm_manager/target/release/osm_manager --config catenaryconfig.toml run
tools/osm_manager/target/release/osm_manager --config catenaryconfig.toml locate --lat 42.5063 --lon 1.5218
```

`CATENARYCONFIG` is not needed for this utility; use `--config` to
select a file explicitly. The manager uses the upstream **osr-extract**
CLI, which must already be installed. The Rust `osr-ffi` library opens
these extracted directories for querying but does not perform extraction.

Each region is published under `data_dir/regions/<geofabrik path>/`.
Inside are the original native OSR `.bin` files, `manifest.json`, and
`boundary.geojson`. The `data_dir/regions.geojson` feature collection
contains region IDs, relative dataset directories, source revisions,
and exact polygon boundaries for future coordinate -> region lookup.

The updater prefers Geofabrik `.osm.pbf.md5` checksum manifests,
falling back to HTTP ETag / Last-Modified / Content-Length validators.
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

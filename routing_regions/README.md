# Edelweiss routing library and SoCal example

Edelweiss calls the reusable routing modules in \`catenary::routing_common\`:
\`route_manager\` loads transit mmap files and OSR registry boundaries from
config; \`raptor_query\` executes marked-round timetable scans with sparse
footpath Dijkstra, parent reconstruction, and arrival/boardings Pareto filtering.

A separate, long-running \`gentian_footpaths --bin server\` process owns the
native \`osr-ffi\` routers. It opens all configured Geofabrik OSR datasets once
at startup and accepts line-delimited JSON access/egress routing requests over
a local TCP socket (default 127.0.0.1:40501). The C++ Router is not shared
between threads. Edelweiss resolves each endpoint's OSR region independently
and uses the common loaded transit region for its timetable query.

## Production routing-region configuration

\`routing_regions/config.toml\` is a local, ignored runtime file, not a
production-wide geographic restriction.

\`\`\`toml
schema_version = 1
data_dir = "compiled/routing"

[[regions]]
id = "example"
name = "My transit region"
geojson = ["example.geojson"]
\`\`\`

Compiled binary names are \`data_dir/<region-id>.bin\`. A region may also
specify countries; selected châteaux are included in full.

OSR's \`[osm_manager]\` section stays in \`catenaryconfig.toml\`. The native
worker opens \`osm_manager.data_dir/regions/<geofabrik-id>/\` based on the
manager's \`regions.geojson\`. Each endpoint must be inside an OSR polygon.

Start the native OSR worker:

\`\`\`sh
cargo run --release --manifest-path tools/gentian_footpaths/Cargo.toml \
  --bin server -- --config catenaryconfig.toml
\`\`\`

Run the Edelweiss endpoint:

\`\`\`sh
DATABASE_URL='postgres://...' cargo run --release --bin edelweiss -- \
  --regions-config routing_regions/config.toml \
  --catenary-config catenaryconfig.toml \
  --listen 127.0.0.1:40428
\`\`\`

POST \`/v1/route\`:

\`\`\`json
{
  "from": {"lat": 33.6461, "lon": -117.8427},
  "to": {"lat": 34.0537, "lon": -118.2428},
  "departure_unix_seconds": 1793026800,
  "access": [],
  "egress": [],
  "direct_walk_minutes": null
}
\`\`\`

Empty access/egress arrays trigger native OSR one-to-many routing from the
given coordinates to nearby transit locations. Results include Pareto journeys
and per-route metadata from source-qualified \`gtfs.routes\`, including
\`short_name\`, \`color\` and \`text_color\` when present. Missing database
metadata is returned as \`null\`, never replaced by arbitrary colors.

For the SoCal example, the GitHub Actions \`workflow_dispatch\` job downloads
Geofabrik \`socal.geojson\`, generates ignored local configs, runs the OSR
manager and Maple for the seven selected GTFS feeds, compiles the timetable,
launches the long-lived OSR worker, and runs a one-shot Edelweiss query for
Monday October 26, 2026 at 08:00 America/Los_Angeles from UC Irvine to Los
Angeles City Hall. Its console output is written to
\`temp/uci-cityhall-results.json\`, with OSR worker logs attached as a
workflow artifact.

## Known limitations

This PR still needs a successful Rust/native build and end-to-end CI run.
The compiler does not yet expand frequencies and does not preserve all
per-stop boarding/alighting restrictions; until fixed, its journey results
are not authoritative. Time precision remains minutes and routes are scanned
exhaustively per candidate service day. Transit regions must contain both
endpoints: cross-region transit federation is not implemented. Nearest stop
discovery is a bounded in-memory coordinate scan and may truncate dense
candidate sets; the current OSR candidate cutoff is configurable but is not
a proof of complete multimodal Pareto optimality.

The local OSR worker protocol is a development transport without authentication
or TLS; bind only to loopback or protect it behind a private service boundary.
Route hydration uses focused PostgreSQL lookups after search rather than
joining GTFS metadata into the hot RAPTOR scan.

The local fixture generator requires Python 3.11+ (`tomllib`). CI installs Python 3.12 through `actions/setup-python`.

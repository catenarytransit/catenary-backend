# Southern California realtime snapshot integration test

The existing [SoCal GitHub Actions workflow](../.github/workflows/routing-regions-socal.yml)
already builds a **real** native OSR graph from Geofabrik's
[California parent extract](https://download.geofabrik.de/north-america/us/california-latest.osm.pbf),
imports **only Southern California** Maple GTFS feeds, compiles a Gentian
regional timetable, and runs RAPTOR queries. The graph and selection boundary
cover California, but the feed allowlist and trip-planning fixtures remain
Southern California.

This stage adds tests for the RAM-only Aspen/Gentian/Edelweiss realtime overlay.

## Ordinary pull requests: deterministic

The regular SoCal job (on matching PRs) now checks:

The workflow obtains the canonical `california.poly` Geofabrik boundary,
converts it to `routing_regions/california.geojson` using
`src/routing_regions/prepare_socal_test.py --poly`, and tells the OSM manager
to download `north-america/us/california-latest.osm.pbf`.
The native graph is loaded at `temp/osr-data/regions/north-america/us/california`.
The old `california/socal` PBF and auxiliary `socal.geojson` URLs are no
longer dependencies. Geofabrik's parent file is larger and builds may use
more disk space and CPU. Retrying or switching paths cannot solve a
host-wide connection outage from a GitHub Actions runner.



1. Gentian publishes its timetable and \`southern-california.rt-identities.bin\`
   companion containing original GTFS-local service identities, timezone
   names and interned original stop sequences.
2. Realtime source protobuf conversion tests preserve exact-second events,
   skipped stops, and local service dates (including \`25:00\`).
3. Resolver/overlay tests reject ambiguous sources, incorrect GTFS stop
   occurrences, invalid dates, missing event times and stale generations.
4. The region snapshot tests verify multi-chateau composition, run-handle
   namespaces, cancellation indexing, withdrawal and immutable old revisions.
5. The existing October 2026 benchmark queries and native OSR matching remain
   intact and are executed *before* any optional live test rebuild.

The deterministic tests use in-memory synthetic protobuf messages. They
do not depend on the availability of any public realtime feed or Aspen.

## Opt-in live Birch protobuf round trip

From GitHub Actions select **SoCal routing - Maple, OSR and RAPTOR query**
→ **Run workflow**, enable \`live_realtime_probe\` and choose the matching
realtime/static feed IDs. Defaults:

| Dispatch input | Value |
|---|---|
| \`live_realtime_probe\` | \`true\` |
| \`realtime_feed_id\` | \`f-longbeachtransit~rt\` |
| \`static_feed_id\` | \`f-9q5b-longbeachtransit\` |

The relevant Birch route is **\`/gtfs_rt\`**, not \`/gtfs-rt\`:

\`\`\`bash
curl --fail --get \
  --data-urlencode 'feed_id=f-longbeachtransit~rt' \
  --data-urlencode 'feed_type=trip' \
  --data-urlencode 'format=pb' \
  'https://birch.catenarymaps.org/gtfs_rt' \
  --output /tmp/socal-rt.pb
\`\`\`

The live job first completes the fixed route benchmarks, then recompiles
Gentian for a **rolling 21-day service interval** beginning three UTC days
before its execution date. It reuses the same Geofabrik SoCal OSR graph.
A static timetable anchored to October 2026 would otherwise be unable to
resolve future-day live GTFS-RT.

Next it downloads the Birch TripUpdates protobuf with bounded retries and
size checks, and executes the opt-in Edelweiss fixture mode:

\`\`\`bash
./target/release/edelweiss \
  --regions-config routing_regions.toml \
  --catenary-config catenaryconfig.toml \
  --realtime-probe-file temp/socal-live-trip-updates.pb \
  --realtime-probe-region southern-california \
  --realtime-probe-static-feed f-9q5b-longbeachtransit \
  --realtime-probe-min-runs 1 \
  --realtime-probe-report temp/socal-realtime-probe.json
\`\`\`

The probe uses the **same** region's mmap timetable and versioned GTFS
identity companion as normal Edelweiss. It converts the raw protobuf using
Aspen's GTFS trip-descriptor and schedule-relationship semantics, resolves
source-scoped trips and stops, and prepares the in-RAM chateau realtime
overlay. It then publishes an immutable **region-wide** snapshot into the
Router and pins that exact \`Arc\`.

The probe fails when no trip updates decode, no chateau source matches the
configured static feed, the catalog generation mismatches, the snapshot is
incomplete, any prepared run has invalid event time order, or fewer than
the requested number of valid runs are prepared (the live workflow requires
**at least one**).

The GitHub Actions summary and the \`gentian-socal-routing-diagnostics\`
artifact include the result JSON, feed SHA-256 and sampled raw protobuf for
debugging.

## Why not start Aspen in CI?

The real Aspen service consumes assigned realtime feeds, persists operational
state, and requires etcd plus PostgreSQL-related infrastructure. A separate
fixture import mode makes this test reproducible without introducing an
insecure production RPC backdoor, sharing Aspen leases, or writing a
realtime overlay to NVMe.

The test **does not** exercise the Aspen tarpc transport itself; the
separate RPC protocol has unit tests, but a complete real Aspen tarpc
request/response integration test still needs a provisioned Aspen worker.
It also does not yet exercise realtime RAPTOR forward/reverse scanning or
journey reconstruction. Those capabilities must be tested after routing
integration is enabled.

Live feeds may be unavailable or have incomplete \`TripDescriptor.start_date\`
fields, so the live network-dependent probe is intentionally opt-in. A failed
manual run should be investigated using its decoded/matched/rejected counters
rather than silently accepting an empty overlay.

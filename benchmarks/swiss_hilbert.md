# Switzerland Hilbert location-order experiment

This experiment asks whether geographic location numbering improves routing
latency on this Swiss workload. `gentian --location-order hilbert` is opt-in;
the default `source` layout preserves existing production behavior. It orders
all sources together using a 16-bit Hilbert curve over the regional lon/lat
bounding square. The curve is a locality heuristic, not a geographic distance
metric; it does not wrap the antimeridian.

Reindexing happens **after route assembly**, preserving route IDs, transport
IDs, stop travel order, boarding flags, timing profiles and calendars. It
permutes the location columns, remaps parents and directed transfer rules,
and rebuilds the location-to-route CSR rows. OSR matches, duration-ordered
footpaths and platform sidecars are then generated against the final indexes.
The binary schema remains unchanged.

## Run in Actions

The **Swiss routing - main versus Hilbert location order** workflow runs on
PRs that change this experiment or Gentian. Its manual trigger accepts a
Monday service date and a number of rounds (default 6). New workflows appear
in the Actions manual-run picker after they reach the default branch; the
PR trigger allows the experiment to run before merging.

The job pins main's SHA and the candidate SHA, checks out the feed catalog,
uses Maple to download and import only `f-u0-switzerland`, and uses
`osm_manager` to download the Geofabrik `europe/switzerland` extract and
build a native OSR graph with the matching extractor. All variants use the
same imported database, street graph, compiler settings, ten-day horizon,
Rust toolchain and Cargo dependency lockfile:

| Variant | Executables | Location order |
| --- | --- | --- |
| main | Pinned main | Existing |
| source | Candidate branch | Existing |
| hilbert | Candidate branch | Global Hilbert |

`source` versus `hilbert` isolates the layout change. The main comparisons
also expose branch changes or regressions outside that experiment. The
service date defaults to next Monday; set it explicitly for reproduction.
The Saturday in that week supplies the second departure date. Each query
uses a fixed one-hour search window and a 120-second search timeout. A search
that exhausts its budget fails validation instead of becoming a fast sample.
No location-index endpoints are injected: real OSR access and egress searches
run for every request.

## Workload and measurements

`swiss_routing_queries.json` includes approximate landmark access coordinates
and web sources for campus and tourist-site selection. Station coordinates
come from the existing `swiss_landmarks.json` fixture. Eight intercity and
four local pairs run in both directions, on Monday at 08:00 and Saturday at
10:00 Europe/Zurich: 48 queries total. Examples include Zürich–Geneva,
Basel–Lausanne, Zürich–Lugano, ETH Zentrum–EPFL, ETH's two Zürich campuses,
EPFL–UNIL, Zytglogge–Zentrum Paul Klee, and Lucerne's Lion Monument–Transport
Museum.

One service runs at a time. Each round rotates/reverses variant order in a
balanced six-round cycle, starts each service fresh, warms every fixture,
then measures every fixture once in seeded shuffled order. This avoids
concurrent-service resource contention and excludes startup from timings.
It measures warm filesystem/runtime caches, not cold disk behavior.
`RAYON_NUM_THREADS=2` is fixed for all variants.

The job summary reports per-query RAPTOR median and nearest-rank p95,
aggregate geometric speedups, and a paired round bootstrap 95% interval.
Raw samples additionally include OSR wall time, total query time and HTTP
latency. Six samples give only a coarse p95; increase rounds for a stronger
tail estimate. Intervals describe repeatability on this runner and these
queries, not all Swiss journeys or all machines. An interval spanning 1 is
inconclusive. A Switzerland result does not establish a benefit for a
continent-sized timetable.

All warmup and measured responses must have transit journeys, finish within
the search budget, and match main's semantic journey multiset. Comparison
retains times, durations, transfers, modes, feed/trip/route identities and
endpoint stop identities/coordinates. It ignores numeric location indexes,
layout-derived journey hashes, response ordering, cursors, debug counters and
geometry. A changed or missing journey fails the job; speedups with failed
parity must not be interpreted as an improvement.

The `swiss-hilbert-experiment` artifact contains timing JSON/CSV samples,
parity status, benchmark requests, source selections, exact revisions,
toolchain/dependency lockfile, graph manifest and timetable checksums.
Raw routing responses and service logs remain on the runner for validation;
the artifact allowlist excludes them and the imported database. Store the
artifact before its 14-day expiry if longer retention is needed. Repeating
a run later can download changed GTFS/OSM inputs; compare snapshot metadata
before comparing results across separate runs.

For an already prepared local dataset:

```sh
python -m unittest discover -s benchmarks -p test_swiss_hilbert.py -v
python benchmarks/swiss_hilbert.py --monday 2026-11-16 --rounds 6
```

See the workflow for the three compilation commands, binary/native-library
locations and generated config paths. The script accepts `--bin-dir`,
`--output`, `--catalog` and `--catenary-config` for local use.

# Swiss timetable and RAPTOR compression experiment

Two modes on the same runner, using the same downloaded Swiss GTFS snapshot,
Geofabrik OSR graph, service horizon and queries:

1. **main:** unmodified Maple, Gentian and Edelweiss compiled from main
   revision `869afe17e095f64b5a9eb6d4160c405781ac32dd`, pinned at benchmark setup. The original main
   allowlist is preserved (Switzerland is not enabled for v2), along with
   its V6 timetable and original routing-index format.
2. **maple-v2-experimental:** enable Swiss Maple Syrup v2 and Gentian's
   experimental base/GCD/bit-packed event-major routing-index columns during
   the initial build. RAPTOR's FIFO transport selection reads the packed
   values directly from mmap with O(1) random access and exact seconds.
   Chains that would grow are stored raw. Reconstruction still reads the
   existing V6 timing profiles; the main timetable is not itself bit-packed.

The workflow builds both source revisions independently and records them.
Both modes run independent Maple imports and Gentian builds into separate
local PostgreSQL databases and output directories. There is no third
compatibility-only configuration or redundant index rebuild.

The Actions summary and artifact report the main .bin, routing-index .bin,
all timetable binary sizes, relative size reduction, database size,
ingest/compile durations, per-route RAPTOR median/p95 and speedup, end-to-end
timings and raw results. Each of 11 routes has one excluded warm-up and five
measured repetitions per variant by default (110 measured queries).
Execution order alternates between variants. The experiment fails if the
packed index is not mapped, transit results are empty, or Pareto departure,
arrival and transfer objectives differ. Raw responses are retained.
No production database or live realtime data is used. Each query starts a
fresh CLI process; RAPTOR phase timings exclude OSR and startup.
The baseline is pinned rather than silently tracking a moving main branch.
The benchmark runs on pushes to the isolated branch; manual dispatch requires
GitHub's normal default-branch workflow availability. The experimental
routing-index format is version 2; do not deploy its companion to main.

## GTFS source

The workflow checks out [Catenary Atlas](https://github.com/catenarytransit/catenary-atlas)
and reads `feeds/opentransportdata.swiss.dmfr.json`, selecting exactly
`f-u0-switzerland` with spec `gtfs`. It downloads the feed's
`urls.static_current` directly, verifies the ZIP and its required GTFS files,
then serves the same archive locally for both Maple imports. No alternative
source is selected. The source URL, Atlas revision, file size and SHA-256
are recorded. Raw timetable data attribution: https://opentransportdata.swiss.

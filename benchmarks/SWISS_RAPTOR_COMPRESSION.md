# Swiss timetable and RAPTOR compression experiment

This branch is intentionally isolated from production. The workflow runs on pushes
to `benchmark/switzerland-raptor-compression` (manual dispatch is also defined;
GitHub requires a workflow on the default branch to dispatch it).

Three modes use the same source checkout, GTFS bytes, Swiss Geofabrik OSR graph,
service horizon, route queries and runner:

1. **legacy:** empty Maple v2 allowlist, existing Gentian V6 timing profiles and
   ordinary u32 event-major routing index.
2. **maple-v2:** Swiss feed allowlisted, Gentian reads its compatibility view,
   writes the same V6 layout and ordinary routing index. This measures database
   and compile differences, not a new RAPTOR timetable encoding.
3. **maple-v2-packed:** same V2 database and main binary, but Gentian rebuilds
   the routing-index companion with `GENTIAN_BENCH_PACKED_OFFSETS=1`. It uses
   base/GCD/bit-width packing per event-major FIFO chain slot with O(1) random
   access, exact seconds and raw fallback when compression is not beneficial.
   RAPTOR's FIFO transport selection reads packed words directly from mmap.
   Reconstruction still uses V6 profiles; no claim of a fully compressed main
   timetable is made. The experimental companion version is 2; regenerate
   indexes before running this branch against an existing timetable.

The Actions summary and artifact include the main .bin, index .bin, all
timetable binary sizes, database size, ingest/compile/reindex durations,
per-query RAPTOR median/p95 and speedup, end-to-end timings and raw results.
All queries must return transit and preserve the set of Pareto departure,
arrival and transfer objectives; raw journey JSON is saved for further
inspection. A run fails rather than silently benchmarking an empty feed or
unpacked fallback. No live realtime data, pfaedle, or production DB is used.
The default query date is the next Swiss weekday and can be overridden.
The benchmark uses repeated fresh processes (one excluded warm-up per query
and variant), not persistent server latency. The GTFS SHA-256 and revisions
are recorded; later runs may use different upstream data.

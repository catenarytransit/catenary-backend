# Parallel RAPTOR route scans

Edelweiss's timetable/PONG kernel scans independent routes concurrently within
large rounds. PING/PONG ordering, retained round labels, transfer relaxation,
calendars, lower bounds, and deferred reconstruction keep their existing semantics.
The point-raptor path (`timetable_view=false`) is unaffected.

Set `EDELWEISS_RAPTOR_WORKERS=1` for a sequential baseline. The default is at most
four worker batches per round. Positive values such as `2`, `4`, and `8` select
an upper bound, further capped by the existing Rayon pool size and marked route
count. `RAYON_NUM_THREADS` controls that shared pool, which is also used by the
existing concurrent OSR access/egress/direct calls. No OS threads or pools are
created per request. Invalid or zero worker settings return an explicit error.

Rounds remain sequential unless there are at least eight routes per worker
and 32,768 estimated route-stop/transport-chain visits. These thresholds are
initial scheduling heuristics, not search cutoffs; every selected route is
scanned in both paths. Static round-robin route assignment spreads neighboring
routes across workers, but uneven route lengths can still limit speedup.

Each worker reads an immutable snapshot of the previous round's boarding times,
marked stops and pruning bounds, then writes only its own arrivals and counters.
A completed join precedes the min/max arrival reduction and marked-stop union.
Transfers consume the merged results in the same ascending stop order as the
sequential kernel. The same transport scanner is used in both execution paths.

Scratch buffers are allocated lazily per query and reused across its point
searches. Each worker uses four bytes per stop for arrivals plus up to eight
bytes per touched stop on a 64-bit host. Clearing and merging normally visit
only touched stops; a direction change also fills the arrival array with the
new sentinel. With many concurrent queries, account for this extra memory and
competition for the shared CPU pool.

The existing `debugOutput.raptor.route_scan_ms` measures the whole route phase,
including scheduling, initial allocation and reduction. Additional fields:

| Field | Meaning |
| --- | --- |
| `parallel_route_scan_rounds` | Number of rounds dispatched to worker batches |
| `route_scan_workers` | Largest batch count dispatched; zero for a wholly sequential query |
| `route_scan_merge_ms` | Reduction time, already included in `route_scan_ms` |

## Validation

The lightweight harness compiles the production routing modules without the
OSR/GTFS/Postgres build dependencies:

```sh
cargo test --manifest-path tools/raptor-check/Cargo.toml
cargo test --release --manifest-path tools/raptor-check/Cargo.toml \
  benchmark_parallel_route_scans -- --ignored --nocapture
```

Regression checks compare all best/tmp/round/destination labels, marked stops,
and route/transport work counters under forward/backward searches, retained
labels, PING bounds and repeated direction changes. The fixture includes
intersecting routes, midnight crossings, service calendars, pickup/dropoff
restrictions, footpaths and pair-specific GTFS transfer rules. An additional
end-to-end PONG fixture verifies identical reconstructed itineraries and cursor
intervals in both directions. Small rounds must avoid worker allocation.

The optimized synthetic benchmark uses 1,024 intersecting routes, 256 stops,
and four transports per route. Each worker count has two warmups and nine
measured point searches; lower-bound construction is outside those samples.
The benchmark checks matching destination/boarding frontiers. One run on an
eight-CPU container produced:

| Workers | Median route scan | Median point search |
| ---: | ---: | ---: |
| 1 | 25.992 ms | 26.247 ms |
| 2 | 19.943 ms | 20.241 ms |
| 4 | 11.487 ms | 11.952 ms |
| 8 | 10.366 ms | 11.615 ms |

Four workers improved the synthetic route phase by 2.26x and the measured point
search by 2.20x. These figures do not measure the Los Angeles production query,
full PONG, lower-bound preprocessing, OSR or metadata hydration. Production
speedup and concurrent-request throughput must be measured on the target host.

## Benchmark an actual query

On the host with the timetable and OSR worker, save the original request JSON
with a fixed departure/arrival timestamp, then run:

```sh
python3 tools/benchmark-raptor-workers.py \
  --binary target/release/edelweiss \
  --query /tmp/la-query.json --workers 1 2 4 --runs 7 -- \
  --regions-config routing_regions.toml \
  --catenary-config catenaryconfig.toml
```

The script rotates worker-count order, warms caches, reports median in-query
RAPTOR/route/merge/total times, and rejects differing responses or exhausted
search budgets. It always includes one worker as the reference and does not
modify the query or search budgets. Each sample starts a fresh Edelweiss
process; reported query timers exclude startup, while process startup still
adds wall-clock time to the benchmark command itself.

Compare completed searches first. With a wall-clock timeout, a faster kernel
can advance PONG farther before stopping; equal deadlines do not promise equal
partial results. The sample response supplied for the LA query had 1,831 ms in
RAPTOR out of 4,335 ms total, including 783 ms of route scans. Improvements to
route scans alone therefore have a smaller effect on full HTTP latency than on
the route-scan microbenchmark.

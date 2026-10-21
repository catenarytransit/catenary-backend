# Gentian: external schedule assembly

## Scope

Gentian compiles one routing region at a time. Stops stay in memory,
including their feed-scoped ID lookup and the geographic candidate index used
by native OSR. Globally there are millions of GTFS stops, but a regional
timetable does not need a disk-resident spatial tree merely because the global
database is large. The current spatial candidate index is a grid with exact
Haversine filtering; replacing it with an R-tree should be driven by measured
regional density and query times, not memory assumptions.

The large avoidable peak came from holding all expanded `Schedule` objects,
then all `BTreeMap` route buckets, then every chain, while also allocating
the final timetable arrays. This change removes the global schedule and route
bucket residency. `TimetableOwned` output arrays, per-source stops/patterns,
GTFS transfer constraints, and native OSR's graph/footpath stage are still
resident and are **not** covered by `--schedule-memory-mib`.

## Pipeline

1. Expand GTFS service calendars into the same UTC-second schedules and
   bitfields as before. Streaming from PostgreSQL remains source-scoped.
2. Buffer at most `--schedule-memory-mib` MiB of expanded schedules
   (default 64) and sort each run by
   `(source, route_id, stop_sequence, first_second, trip_id, events, days)`.
   `sort_unstable_by` avoids a second merge-sort-sized heap buffer.
3. Write runs as length-checked, little-endian private scratch records.
   This scratch format is not a public timetable wire format.
4. Perform a 16-way external merge, repeating until one sorted stream remains.
   Only one decoded record per input run is resident during each merge. A
   per-record cap of `memory_limit / 32` reserves space for all readers and
   the merge heap, even for atypically long patterns.
5. Read the final sorted stream in batches containing only one stop pattern,
   at most the configured byte budget and at most 1,024 schedules. Apply the
   original first-fit, coordinatewise non-overtaking chain predicate within
   each batch. Append each resulting route to `TimetableOwned` immediately.
6. Construct the location-to-route CSR index by two scans of the finished
   route-stop arrays: degree counting, prefix sums, then placement with a
   per-stop cursor. Deduplicate repeated locations in circular patterns.
7. Construct OSR footpaths and serialize V4 with the already implemented
   streamed, mmap-validated, atomically published file writer.

Scratch defaults to the output directory, *not* `/tmp`, which may be tmpfs
and therefore charged against the same container memory limit. Use
`--scratch-dir /mnt/nvme/.../gentian-scratch` to direct runs to a
fast filesystem with adequate free capacity. Scratch files are removed on
normal completion and most error paths through RAII.

## Correctness

**Losslessness.** Every expanded schedule is appended exactly once to a
sorted run. The merge emits exactly one output for every input popped from
its heap, and inserts the next record from the same run. The final count is
checked against the inserted record count. Source and original trip IDs,
traffic bitfields, per-event UTC-second offsets, and stop ordering are kept.

**Non-overtaking.** All schedules in a batch share an identical ordered
stop list. For two trips `a`, `b`, the acceptance predicate checks that
the departure and arrival times at **every** stop on `a` are no later than
the corresponding times on `b`. Coordinatewise ordering is transitive, so
checking a new trip against the last element of an existing chain suffices
to maintain the invariant for every trip in that chain. The unchanged
first-fit partitioning therefore constructs legal RAPTOR routes.

**Safe partition boundaries.** Batches may split one public route/stop
pattern into several RAPTOR routes, even when trips on different sides
would have been mutually non-overtaking. This only adds route partitions:
it cannot create an invalid within-route order or remove a trip. Each new
route is linked into every participating stop's inverted route index.
Queries may scan more routes, but the transit journey set is preserved
subject to the existing RAPTOR algorithm's assumptions.

**Circular stop patterns.** The stop-to-route index stores a route at most
once per distinct location. A trip that returns to a station still retains
its complete ordered stop sequence in the route-stop column.

**Failure safety.** The V4 published timetable remains unchanged until the
entire assembly, OSR transfer computation, disk serialization, mmap
validation, and atomic rename succeed. Abandoned scratch files from an
abrupt SIGKILL require an operator cleanup policy; they are uniquely named
per invocation and cannot corrupt a published timetable.

## Complexity

Let `N` be the number of expanded schedules, `L` their mean number of
stops, `D = Theta(N * L)` the external schedule payload bytes, `B` the
sort buffer, `F = 16` the merge fan-in, `R = ceil(D / B)` the run count,
and `C = 1024` the maximum route-assembly batch size.

- Initial run sorting: `O(N * L * log(B / mean_record_size))` comparison
  work in the worst case, plus `O(D)` sequential writes.
- External merge: `O(N * L * log(F) * ceil(log_F(R)))` comparison work
  (the comparator may inspect the full stop sequence) and
  `O(D * ceil(log_F(R)))` sequential read/write I/O.
- Non-overtaking chain partitioning: worst-case `O(N * C * L)` checks.
  Capping `C` prevents a single adversarial route group from taking
  unbounded quadratic work. Typical regular timetables have very few
  simultaneous incompatible chains.
- Stop-to-route index: `O(sum_r S_r log S_r + n_stops)`, sorting unique
  stop IDs per route to prevent duplicates, with `O(n_stops + max_r S_r)`
  temporary index memory.
- Scratch space: `O(D)`; during a merge, old inputs coexist briefly with
  the merged output, and completed groups' inputs are promptly removed.
- Anonymous heap: `O(B + n_stops + source_pattern_bytes + final_V4_columns
  + OSR_and_transfer_state)`. There is **no** second global
  `O(N * L)` schedule-bucket structure.

This is deliberately not a process-wide hard-RSS guarantee. A future
separate change should make large final V4 columns and directed OSR
transfer edges disk-backed or mmap-writable, then enforce a measured cgroup
memory budget. Allocator RSS, memory-mapped file pages, and Linux page cache
must be measured separately.

## Reproduction and validation

```sh
cargo test --bin gentian
cargo test --lib routing_common::timetable
cargo build --release --bin gentian --bin edelweiss

# Add these to the existing --build invocation:
# --schedule-memory-mib 64
# --scratch-dir /mnt/nvme/gentian-scratch

# Benchmark both peak anonymous RSS and cgroup memory accounting:
# /usr/bin/time -v target/release/gentian ...
# cat /sys/fs/cgroup/memory.peak
```

The tests cover multi-run sorting, oversized single-record rejection,
large-pattern batch splitting, truncated private sort files, correct
overtaking partitioning, preserved original trip IDs, loop-safe stop
inversion, and existing V4 serialization invariants.

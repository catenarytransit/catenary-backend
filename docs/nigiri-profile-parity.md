# Edelweiss profile search and Nigiri

The profile start grouping and transport selection are adapted from MOTIS
Nigiri commit `1ccdf6972b854d17963b1d7843614446824b1e1a`:

- `src/routing/start_times.cc`: retain start time, stop time, stop, and via state;
  add an on-trip boundary start to prime the profile arrival skyline.
- `include/nigiri/routing/search.h`: group equal start times; process forward
  starts latest first and reverse starts earliest first.
- `include/nigiri/routing/raptor/raptor.h`: retain one onboard transport per
  compatible route group; cache its timing pattern; skip lookup when a marked
  stop cannot improve that transport; binary search the first event day and
  visit subsequent event days in chronological search order. Prune candidates
  using their event time plus the destination lower bound before calendar checks.
- `include/nigiri/event_times.h`: compressed event columns computed from the
  first departure and an event-major array of timing-pattern offsets.
- `src/loader/init_finish.cc`: route activity includes calendars shifted to
  every event-day offset. Lookups test the transport calendar on
  `start_day = event_day - event_offset.days`.
- `include/nigiri/loader/get_index.h`: route groups must have sorted time-of-day
  columns at every event, including events after midnight.

The upstream MIT license is in `third_party/nigiri-LICENSE`.

## Timetable compatibility

Catenary's v5 mmap format already stores compressed timing profiles and exact
seconds. It is retained. A cached `TransportIndex`, built once with the region's
lower-bound topology, transposes distinct timing profiles into compressed
Nigiri-style event columns. The query clones only an `Arc` to this immutable
index. No full stop-time matrix or per-query/day active-transport bitset is built.

Existing Gentian files guarantee elapsed-time FIFO, which does not imply sorted
time-of-day columns across midnight. The cached view partitions each route into
compatible groups, retaining original route/transport IDs for reconstruction
and metadata. Within a group, event-day-offset vectors are also equal so
multi-day journeys with different running times cannot overtake across days.
Each group is scanned independently, with one selected onboard transport.
Existing timetable files therefore do not require regeneration for this change.

Day splitting uses Euclidean division and remainder in seconds. Overnight trips
can be boarded after the last vehicle-start calendar day, including boarding on
the next UTC day while validating the previous day's vehicle calendar.

## Profile semantics and diagnostics

Real timetable events retain `{time_at_start, time_at_stop, stop, via_state}`.
Only the stops in an equal-start-time group seed that execution. Via state is
explicit; nonzero values are rejected because Edelweiss does not yet expose via
routing. Forward interval searches add a point start at `to + 1 second`, and
reverse searches at `from - 1 second`: the outside boundary of Catenary's
inclusive interval. Boundary results prime dominance but are excluded from the
returned interval. Interval extensions invalidate arrival bounds if anchor order
reverses, while retaining already reconstructed journeys.

Point recovery uses the same compact kernel and cached index, with fresh profile
bounds. `profile_compact_ms` and `profile_fallback_ms` distinguish its phases.
`transport_lookup_calls`, `transport_lookup_skipped_onboard`, and
`transport_bound_prunes` distinguish expensive lookups from avoided work.
`profile_start_labels` and `profile_boundary_events` expose profile seeding.
The existing aggregate counters still include both compact and recovery work.

Catenary's pair-specific GTFS transfer validation, second precision, route-diverse
presentation policy, and immutable predecessor reconstruction are retained.
This is an adaptation of the static no-via routing path, not a claim that all
Nigiri features or its Cista binary format are supported.

## Validation

Run the production routing modules independently of native backend dependencies:

```sh
cargo test --manifest-path tools/raptor-check/Cargo.toml
cargo test --release --manifest-path tools/raptor-check/Cargo.toml
```

This harness imports the actual production source files; it contains no alternate
solver. Tests compare lookup against exhaustive transport/calendar enumeration
in both directions, including inactive days, midnight, multiple day offsets,
calendar word boundaries, and boarding beyond the final vehicle-start day.
Profile tests compare complete direct-trip frontiers with exhaustive schedules,
exercise grouped seeds and boundary dominance, and check that recovery copies
zero full round-label arrays. Existing transfer fixtures use the v5 interleaved
arrival/departure layout and proper CSR sentinels.

A synthetic 20-stop, seven-day route checks identical arrival results against
the old point solver, one lookup instead of repeated per-day lookups, eighteen
onboard lookup skips, and more than a twentyfold reduction in lookup probes.
These are work-count regressions, not production latency measurements. Benchmark
with the production timetable and original request before assigning a wall-time
speedup to this change.

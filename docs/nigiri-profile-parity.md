# Edelweiss and Nigiri routing

The static timetable implementation is adapted from MOTIS Nigiri commit
`1ccdf6972b854d17963b1d7843614446824b1e1a`, the revision pinned by MOTIS
`f2cb2572a1d065f1e312c10e9b3efa010622268a`. The upstream MIT notice is
preserved in `third_party/nigiri-LICENSE`.

## PONG timetable requests

`route_manager::route_profile_with_access` now dispatches timetable-view
requests to `pong::route_pong_with_stats_precomputed`, following
`src/routing/raptor/pong.cc`. Both departure and arrive-by searches use PONG.

- Start with the requested window, without `interval_estimator::initial`.
  With the default five connections, the rRAPTOR estimator would enlarge a
  15-minute window to six hours and 15 minutes before its first scan.
- Run a point ping from the cursor, allowing
  `min(max_travel_time + one day, five days)` of travel and waiting. Do not
  apply fastest-direct filtering to ping. Sanitize the requested travel-time
  limit to Nigiri's five-day maximum and boarding budget to 15 rides.
- Keep ping's strict destination-time/boarding Pareto frontier and process
  the candidates with the most transfers first.
- Fold ping's round times from lower to higher boarding counts before
  resetting the shared state. Pong indexes these bounds by its remaining
  boarding budget, including Nigiri's transfer-time compensation and loose
  CPU comparison. Pong's static destination lower bounds are zero.
- Reverse search direction and run pong from each ping destination, with an
  inclusive bound at the original ping cursor. Reset arrivals once before
  the sequence of pongs, not between individual candidates. Match by the
  ping destination timestamp and exact boarding count.
- Retain search-oriented start/destination keys separately from physical
  reconstructed leg times. Mark only matching pong results reconstructed.
  Update each ping start key from its matching pong destination key.
- Advance to one second after the first matched departure in forward mode,
  or one second before the last matched arrival in arrive-by mode. Require
  strict progress. Nigiri's minute-based cursor step is one exact second in
  Gentian's second-based format.
- Continue while the requested window is not covered or
  `validated_count_including_slow + validated_count_excluding_slow < 2 * min`.
  Validation uses the opposite-search destination key against the cursor,
  independently of the reconstructed marker. Stop at the timetable boundary,
  empty/dominated ping results, or the shared request deadline.
- Finally discard unmatched/unreconstructed results, results not validated
  by the cursor, journeys at least as long as fastest-direct, and journeys
  exceeding the requested travel-time limit. Swap the oriented keys back
  and sort in requested-direction time order, then boarding count.
- For forward searches without vias, replace the middle ride in a
  ride/walk/ride/walk/ride sequence with the earliest feasible direct
  alternative, as in PONG's `get_earliest_alternative` refinement.

The search reuses one set of compact round buffers across both directions.
It shares the region's immutable transport index and ping lower-bound graph,
with a saved folded ping-bound matrix. Reverse lower bounds do not require
another Dijkstra search. OSR access and egress offsets are intermodal, so
initial footpaths stay disabled in both directions.

PONG does not enumerate all boarding events in an estimated multi-hour
interval, reserve time for profile fallback, or add six near-Pareto route
alternatives. The classic event-driven rRAPTOR entry points remain available
in `profile_search.rs`. The legacy maximum search-window and extreme-duration
ratio/slack/comparison options belong to that path; PONG uses Nigiri's
travel-time, timetable-boundary and timeout rules instead. The minimum
itinerary count is a minimum, not a hard response limit.

## Timetable and feature mapping

Gentian's v5 mmap format and compressed timing profiles retain exact seconds.
A cached `TransportIndex` transposes distinct timing profiles into event
columns and partitions routes into compatible FIFO groups. Original
route/transport IDs and vehicle-start calendars remain authoritative for
reconstruction and metadata. No timetable regeneration is required.

Day lookup follows `start_day = event_day - event_offset.days`, with
Euclidean division for overnight and multi-day vehicles. The existing
calendar selection and compressed event decoding are shared by ping, pong
and the direct-alternative lookup.

The compact kernel has separate physical-arrival and transfer-ready states,
so ping-bound preparation maps them to Nigiri's transfer-ready round times.
At locations with pair-specific GTFS restrictions, scalar ping bounds are
permissive and the existing predecessor/transfer validation decides legality.
Reflexive station-change legs in the direct refinement are validated as
station changes without requiring a self-edge in the walking graph.

Edelweiss's `Query` exposes static, no-via intermodal routing. Nigiri's GPU,
realtime, time-dependent offsets/footpaths, via states, transit constraint
filters and optional slow-direct enrichment are not added by this port.
The direct-alternative lookup therefore implements the static unconstrained
branch over the compressed transport index, retaining Catenary's GTFS
stop-pair restrictions. Street-only journeys remain in the separate direct
response list.

## Diagnostics and validation status

The routing marker is `compact-raptor-nigiri-pong-v5`. PONG reports
`pong_iterations`, `pong_forward_searches`, `pong_backward_searches`, and
`pong_interval_from`/`pong_interval_to` (half-open Unix-second bounds).
The compact-kernel route, lookup, lower-bound and reconstruction counters
remain available. A deadline interruption sets `profile_budget_exhausted`;
partial unmatched point results are not returned as completed journeys.

No tests, builds, formatters, linters or benchmarks were run for the PONG
port, as requested. The existing standalone routing harness imports the
production PONG module so future callers can compile the same implementation
without the native backend dependencies.

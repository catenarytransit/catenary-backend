# RAM-only realtime routing: source, service, and stop identity

This is the third, **observe-only** stage of the realtime overlay work
(#216 → #217 → #218). It deliberately does not change RAPTOR answers.

## Why service dates must be resolved explicitly

GTFS-RT `TripDescriptor.start_date` is the **original GTFS-local service
date**, not the UTC calendar day in the Gentian timetable.

Nigiri resolves this in two steps: it locates all source-scoped static trip
candidates, then transforms the GTFS service day using the provider timezone
and the first departure before testing traffic-day bitfields:

- [Nigiri get_transport.cc](https://github.com/motis-project/nigiri/blob/master/src/lookup/get_transport.cc)
- [Nigiri gtfsrt_resolve_run.cc](https://github.com/motis-project/nigiri/blob/master/src/rt/gtfsrt_resolve_run.cc)

Gentian similarly uses a **local noon offset** to construct UTC anchors
(`src/gentian/compile.rs:utc_anchor`), preserving overnight GTFS seconds and
daylight-saving behavior. Compiled V6 groups each transport's service days by
UTC first-departure day.

The identity sidecar preserves, for each compiled transport:

- The original `service_id` (dictionary index, source-specific in meaning).
- The source GTFS timezone (`Tz` from the static itinerary).
- The first departure's GTFS-local seconds (supports values beyond 86,400).
- A dictionary reference to the **original GTFS stop_sequence numbers**.
  Numbers are stored once per unique sequence, even across thousands of
  transports with identical ordered stops. Gentian reads
  `gtfs.itinerary_pattern.gtfs_stop_sequence`, not its separate positional
  `stop_sequence` column; Maple's itinerary hash includes these original
  GTFS numbers.

The resolver computes:

```text
anchor = UTC(local noon on GTFS service date) - 12 hours
departure_utc = anchor + first_departure_gtfs_seconds
utc_day = floor(departure_utc / 86400) - timetable.first_utc_day
seconds = departure_utc mod 86400
```

It then requires an **exact** match to the compiled transport's
`transport_first_dep_offset` and an active
`transport_traffic_days` bit. If several transports match (including
frequency-expanded service or duplicate trip IDs), the update remains
unresolved unless `start_time` uniquely selects one. There is no
nearest-day or calendar fallback.

## Source and stop identifiers

The static timetable's `SourceIdx` refers to
`(chateau, onestop_feed_id, attempt_id)`. Stop IDs, trip IDs, service IDs
and route IDs are scoped to that source; IDs from different sources are
**never merged**.

Only GTFS IDs mentioned by the current Aspen snapshot are indexed at
resolution time. The immutable static timetable's string pool is borrowed;
Edelweiss does not create an extra full-region hash map of all static
GTFS IDs.

For a scheduled update, original stop numbers from the identity catalog
allow mapping a raw GTFS-RT `stop_sequence` to an exact **occurrence** in
the static route pattern. The mapping is source-scoped. Numbering may be
non-contiguous, and circular routes may visit the same LocationIdx more
than once. If only the stop ID is present, it must uniquely identify one
occurrence. If both ID and sequence are present, they must agree. No
stop-sequence-number-to-index arithmetic is used.
`assigned_stop_id` is looked up separately for platform changes; it
must not be substituted for the original stop identity.

## Deployment and memory behavior

Gentian writes the version-2 static `.rt-identities.bin` sidecar when the static
timetable is compiled. It contains a dictionary of service/timezone strings
and one compact row per transport. Edelweiss verifies the companion
catalog using SHA-256 of the identity-bearing V6 timetable columns.
Previously compiled region timetables need one recompilation to obtain
this sidecar.

When `EDELWEISS_ENABLE_REALTIME_SOURCE_SYNC=true`, Edelweiss loads that
static companion once at startup. Aspen continues sending bounded
routing-only pages over tarpc; Edelweiss resolves them in a blocking
worker and atomically publishes a complete **RAM-only** snapshot.
No periodic realtime graph files are written to the NVMe drive.

The resolved snapshot stores numeric references (`region_index`,
`SourceIdx`, `TransportIdx`, `DayIdx`, service dictionary index)
instead of duplicating service ID or region name strings.

## Preparing a realtime run

The staging compiler creates an immutable chateau-local overlay for each
loaded region, containing only fully validated updates.

- **Scheduled modifications:** reconstruct the entire event vector from
  Gentian's shared timing profile with exact-second Unix timestamps.
  Apply explicit event times first, then delay overrides; propagate the
  last known delay to subsequent events. A NO_DATA stop resets propagation
  to scheduled times. Reject non-monotonic timelines rather than fabricating
  corrected times. Stop flags are updated for SKIPPED stops, and a resolved
  assigned platform produces a changed PackedStop sequence.
- **Cancellations/deletions:** suppress the exact resolved
  (TransportIdx, UTC traffic day) only. Conflicting update entities targeting
  the same instance are all rejected.
- **ADDED/UNSCHEDULED/DUPLICATED:** construct a new, independently
  identified run from the ordered stop IDs and explicit arrival/departure
  Unix times. A delay-only added trip has no static reference clock and is
  rejected. New patterns are interned; if a pattern exactly matches a static
  route, the first stop's mmap-backed stop-to-route index can reuse that
  static pattern without a region-wide hash table.

Raw Aspen updates and chateau-local overlays are still **observe-only**.
A later routing integration must merge chateau-local revisions into one
generation-compatible region snapshot and implement correct RAPTOR/PONG
scanning and reconstruction.

## Region-wide immutable snapshot (stage 5)

The V6 regional timetable already contains the exact
\`(chateau, onestop_feed_id, attempt_id)\` tuples compiled by Gentian.
Its source table defines which chateaux belong to a region, including those
whose geography falls outside the region's own polygons. There is therefore
**no new region-membership artifact** and no separate database lookup at
runtime.

After every complete Aspen RPC synchronization sweep, Edelweiss composes
one \`RegionRealtimeSnapshot\` per changed region. The snapshot contains:

- The static Gentian timetable fingerprint and UTC epoch.
- A monotonic local publication sequence, plus a vector of the participating
  chateaux's independent Aspen revisions (no fictitious global feed timestamp).
- Shared \`Arc<RealtimeOverlay>\` chateau shards. The original compressed run
  times and stop patterns remain in their own immutable arrays.
- One sparse location index mapping stop -> (chateau slot, pattern ID) and one
  sparse union of suppressed static (transport, UTC service day) instances.
- Explicit \`expected_chateaux\` and \`missing_chateaux\` metadata. Missing
  sources are *not* counted as complete realtime coverage.

Publication shares the *same Arc object* between the Aspen synchronization
cache and \`Router::TransitRegion.realtime\`. Searches can call
\`router.pin_realtime(region_id)\`, keeping the whole region revision stable
through RAPTOR round expansion, PONG, and reconstruction. Individual realtime
run handles include the chateau slot so identical local pattern/run indices
in different chateaux never collide.

For memory efficiency, complete Aspen RPC pages and temporary GTFS ID
resolution vectors are discarded once the run overlay has been built. The
region snapshot adds only the shared-pointer array and sparse cross-chateau
indices; it does not copy full event-time arrays during each update.

When a chateau is no longer assigned to Aspen for five full synchronization
passes, Edelweiss withdraws its source cache and publishes a region snapshot
marked incomplete. The static timetable is unchanged; no stale suppression
record from the withdrawn chateau survives in the newly published snapshot.
Pinned older revisions remain immutable until their queries finish.

The overlay remains **observe-only**: no realtime scans execute in production
until both forward and reverse RAPTOR and reconstruction use these handles,
and the route lower bounds are reviewed for realtime stop topology changes.

## Limitations before activating RAPTOR

- RAPTOR, PONG and reconstruction currently ignore these source snapshots.
- The original GTFS stop_sequence mapping is now in the static sidecar,
  but Aspen still stores GTFS-RT stop sequence numbers as u16. Numbers
  outside that range cannot be matched by number alone without a separate
  Aspen wire-format change.
- Trip modifications involving inserted/deleted intermediate stops,
  changed pickup/dropoff semantics or novel stops not in the compiled
  source require further support. Unknown assigned platforms must not be
  silently forced onto static routes.
- The source RPC currently serves a consistent, paged per-chateau snapshot
  rather than a compressed incremental delta protocol.
- Service-date or stop ambiguities **must not suppress scheduled service**.
- The static companion catalog is not a realtime file; its only NVMe writes
  occur when Gentian republishes the static timetable.

## Relevant implementation

- [MOTIS realtime publication](https://github.com/motis-project/motis/blob/master/src/rt_update.cc) — replacement snapshots published atomically
- [Nigiri realtime timetable](https://github.com/motis-project/nigiri/blob/master/include/nigiri/rt/rt_timetable.h) — realtime transports and source lookup
- [Nigiri GTFS static run resolution](https://github.com/motis-project/nigiri/blob/master/src/lookup/get_transport.cc) — timezone-aware service-day lookup
- `src/routing_common/realtime_identity.rs` — catalog and resolver
- `src/gentian/compile.rs`, `src/gentian/external_assembly.rs` — catalog creation
- `src/edelweiss/realtime_sync.rs` — observe-only RAM publication

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

For a scheduled update, an original stop must match **exactly one**
occurrence of the static route pattern. This is intentionally conservative:
Gentian V6 stores ordered packed stops but not the original raw GTFS
`stop_sequence` values (which need not be contiguous). For routes that
visit the same stop more than once, or updates containing only a
`stop_sequence` with no `stop_id`, the stop is marked unresolved.
A later metadata extension should preserve raw stop sequence mapping.
`assigned_stop_id` is looked up separately for platform changes; it
must not be substituted for the original stop identity.

## Deployment and memory behavior

Gentian writes the static `.rt-identities.bin` sidecar when the static
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
`SourceIdx`, `TransportIdx`, `DayIdx`, service dictionary index,
stop indices) instead of duplicating service ID or region name strings.

## Limitations before activating RAPTOR

- RAPTOR, PONG and reconstruction currently ignore these source snapshots.
- Aspen does not yet provide raw static `stop_sequence` mappings, and
  realtime added trips have no matching static `TransportIdx`.
- Trip modifications with changed stops and unknown assigned platforms must
  be represented as dynamic patterns, not silently forced onto static routes.
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

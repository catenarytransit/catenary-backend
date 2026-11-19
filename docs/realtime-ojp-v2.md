# Realtime routing and OJP-style v2 journey API

Stacked implementation: PR #225 (event provenance), PR #226 (bidirectional RT scan),
PR #227 (v2 response), PR #228 (snapshot-pinned realtime PONG and reconstruction).

## Status and safety

The first two PRs preserve exact original GTFS schedule times from the mmap Gentian timing profile and sparse realtime event status, including Estimated, NoData, and explicit stop/platform changes. They distinguish an entire cancelled trip from a modified/replaced trip.

PR #226 adds the forward/reverse realtime scan and exact scheduled-instance suppression. PR #228 completes the corresponding PONG integration: deferred reconstruction searches realtime runs before static runs (as in Nigiri), preserves each source-scoped RegionRtRun identity on a ride leg, and validates events and stop permissions against the *same* immutable region revision used by PONG. The static-only transfer optimizer cannot retime realtime rides. PING/PONG direction changes retain the snapshot; no live mutation or RT disk write is needed.

Real-time journey scanning is **opt-in**; the static engine remains the default. Enable the Aspen tarpc sync first and compile the matching Gentian realtime identity sidecars before enabling routing:

```ini
Environment="EDELWEISS_ENABLE_REALTIME_SOURCE_SYNC=1"
Environment="EDELWEISS_ENABLE_REALTIME_ROUTING=1"
```

The routing flag is checked per request. Edelweiss activates RT for each published source shard even if its region-wide snapshot is incomplete. Unavailable sources retain static routing: only specific static transport/day instances explicitly replaced or cancelled by available realtime shards are suppressed. The region-wide completeness flag is diagnostic, not a query admission gate. Both routing and the MOTIS-style v1 response pin one Arc and expose the revision in debugOutput. The v1 transit leg carries realTime, scheduledStartTime / scheduledEndTime, departureStatus / arrivalStatus, skipped intermediate calls, and no stale GTFS shape if its stop pattern changes.

Nigiri references:
- https://github.com/motis-project/nigiri/blob/master/include/nigiri/routing/raptor/raptor.h
- https://github.com/motis-project/nigiri/blob/master/src/routing/raptor/reconstruct.cc
- https://github.com/motis-project/nigiri/blob/master/src/routing/raptor/pong.cc
- https://github.com/motis-project/nigiri/blob/master/src/rt/rt_timetable.cc

Nigiri updates its static lower-bound graph for RT shortcuts. Catenary conservatively disables the static lower bounds only when a region revision adds a run, changes stop connectivity, gains boarding permissions, or makes a segment faster. Pure cancellations, delays and skipped stops do not introduce shortcuts and retain the static bounds. PING-derived opposite-direction bounds are shared when both searches use the same pinned RT revision. Building compressed supplementary RT lower-bound edges, rather than disabling them for new shortcuts, remains a performance improvement opportunity.

## V2 journey planning API

- POST `/v2/route`
- GET (WebSocket) `/v2/route/ws`

The query JSON, pagination, `requestId`, `snapshot`, `complete`, `error` and
`cancelled` WebSocket envelopes are the same as v1. The same realtime
snapshot is pinned throughout forward/reverse RAPTOR, PONG reconstruction and
response serialization. Existing `/v1/route` and `/v1/route/ws` remain
unchanged during client migration.

The v2 response keeps the route/leg shape, geometry, trip identity and stable
journey IDs for Catenary clients, but adds OJP 2-style event and quay fields
on transit legs, their boarding and alighting locations, and intermediate
calls:

- `ServiceDeparture` / `ServiceArrival`, each holding `TimetabledTime`,
  `EstimatedTime` and/or `RecordedAtTime` as appropriate.
- `PlannedQuay`: the compiled scheduled platform.
- `EstimatedQuay`: the *changed* effective platform, when known.
- `NotServicedStop`, `NoBoardingAtStop`, `NoAlightingAtStop` on calls
  when applicable.

Only explicit measured-event provenance may populate `RecordedAtTime`.
Ordinary GTFS-RT StopTimeEvent timestamps and delay predictions are
`EstimatedTime`. `TimetabledTime` is omitted for added unscheduled runs and
for event occurrences with no verified static correspondence. No v2 trip
lookup is offered: the previously introduced
`GET /v2/regions/{region}/trips/{chateau}/{trip_id}` is removed entirely.
Clients should use Ramonda trip subscriptions for live trip updates and
Birch for trip information, as in SingleTripScreen.

The v2 response is OJP-inspired JSON, not OJP XML. The generic
`startTime`/`endTime` itinerary timeline fields remain for existing UI
and map playback. The ambiguous v1 leg-level
`scheduledStartTime`/`scheduledEndTime` and
`departureStatus`/`arrivalStatus` are superseded by the event objects.

OJP schema reference:
https://github.com/VDVde/OJP/blob/master/OJP/OJP_JourneySupport.xsd

## Validation and next steps

The isolated routing-kernel CI contains deterministic tests for scheduled
replacements, added trips, entire-trip cancellations, two-ride mixed
static/realtime transfers and skipped-stop dynamic patterns, each tested
forward and arrive-by where appropriate. It also validates pinned source/run
identity and exact reconstructed seconds.

Before broad production rollout:
1. Complete full Rust build and Clippy validation (the general Clippy workflow
   has other pre-existing lint errors in unrelated services).
2. Extend SoCal live integration testing with real GTFS-RT TripUpdates, including
   overnight/DST events, added-trip multiple instances, and early departures.
3. Benchmark RT-only versus static profiles under current RAM limits and add
   compact RT lower-bound shortcut edges where a region has substantial
   added service.
4. Define a complete scheduled-to-effective occurrence mapping for
   inserted/deleted stops in TripModifications; the journey response withholds
   unmatched scheduled times instead of inventing original event identities.
5. Verify client rendering of no scheduled baseline for truly added trips,
   cancellation in saved-trip details, and separately flagged skipped calls.

The overlay and realtime snapshots remain RAM-only; the Gentian identity
companion is written during static compilation, not on every realtime refresh.

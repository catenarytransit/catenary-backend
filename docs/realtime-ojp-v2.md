# Realtime routing and OJP-style v2 trip API

Stacked implementation: PR #225 (event provenance), PR #226 (bidirectional RT scan), PR #227 (v2 response).

## Status and safety

The first two PRs preserve exact original GTFS schedule times from the mmap Gentian timing profile and sparse realtime event status, including Estimated, NoData, and explicit stop/platform changes. They distinguish an entire cancelled trip from a modified/replaced trip.

PR #226 adds an opt-in forward/reverse realtime RAPTOR scan that reads shard-qualified run handles and filters cancelled or replaced static transport-day instances. Production PONG does NOT call the realtime scanner yet. Its deferred journey reconstruction still assumes a static TransportIdx; routing activation must wait until it carries RegionRtRun references across every round and validates original-versus-effective stop occurrences. Static-only pruning may not be valid for new RT stop connections.

## Trip detail API

GET /v2/regions/{region}/trips/{chateau}/{trip_id}

Query parameters: feed_id and service_date (YYYY-MM-DD) required; optional start_time (supports 25:00:00), route_id and attempt_id. Multiple GTFS attempts or trip instances are not guessed.

Returned OJP-inspired JSON properties:

- TimetabledTime: scheduled original published time from Gentian.
- EstimatedTime: latest prediction where explicitly supported by GTFS-RT status.
- RecordedAtTime: only with verified observation provenance, not an arbitrary GTFS-RT timestamp.
- PlannedQuay and EstimatedQuay: original and new GTFS platform metadata.
- Cancelled: applies to the entire trip; cancelled trips remain addressable by original identity.
- NotServicedStop: applies to one skipped stop and disables boarding/alighting.
- UnplannedStop, Deviation, NoBoardingAtStop and NoAlightingAtStop.
- OriginalStopPointRef and GtfsStopSequence: useful Catenary extensions.

Added realtime trips omit TimetabledTime; they must not inherit an unrelated scheduled baseline. The output is OJP-inspired JSON, not an implementation of OJP XML.

Schema references:
https://github.com/VDVde/OJP/blob/master/OJP/OJP_JourneySupport.xsd
https://github.com/VDVde/OJP/blob/master/OJP/OJP_PlaceSupport.xsd
https://github.com/VDVde/OJP/blob/master/OJP/OJP_TripInfo.xsd

## Remaining work before enabling realtime itineraries

1. PONG deferred reconstruction must carry realtime run IDs/stop positions rather than static-only route and transport IDs.
2. The journey validators and transfer optimizer need realtime-aware event and stop-sequence access; otherwise valid new trips could be rejected or invalid paths returned.
3. Forward/reverse scans must be tested with mixed scheduled, delayed, cancelled, added, skipped, overtaking and early-departure services.
4. V1 MOTIS-style itinerary serialization must use the same pinned snapshot and original-versus-estimated time model, without changing existing consumers.
5. Introduce a separate opt-in routing flag only after those steps, keeping stale/incomplete regions on safe static fallback.

The overlay and realtime snapshots remain RAM-only; the Gentian identity companion is written during static compilation, not on every realtime refresh.

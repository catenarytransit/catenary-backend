# Station departure boards and realtime detours

`/departures_at_osm_station` now anchors its schedule query at the selected GTFS
stop occurrences. It joins itinerary metadata on feed, import attempt and pattern
ID, omits the unused `trip_ids` array, and loads compressed trips in batches of
128 itinerary IDs using the existing `(chateau, itinerary_pattern_id)` index.
It no longer loads an entire route's trips before filtering in Rust. Direction
rows are restricted to the selected stops as well.

Both `/departures_at_stop` and `/departures_at_osm_station` apply the same realtime
overlay after constructing the scheduled board:

- Aspen selects TripModifications by static trip ID and replacement stop ID.
  This finds detours into a station even when the original trip did not serve it.
- Replacement spans use original stop selectors, include both endpoints, and
  retain removed calls with `stop_cancelled=true`. Insertions without an end
  selector preserve the original call. Downstream modification delays accumulate.
- Replacement times use the arrival at the preceding original stop as their
  reference (the original first stop when modifying the beginning of the trip).
  Modified schedules are renumbered 1..n before applying ModifiedTripSelector
  updates. Invalid, overlapping or ambiguous loop selectors preserve the static
  schedule rather than guessing a stop occurrence.
- Service dates and frequency start times identify distinct trip instances.
  Explicit modification start times are respected. Frequency templates without
  explicit times use observed realtime instance start times; they do not invent
  scheduled departures for an unobserved frequency run.
- ADDED, UNSCHEDULED, DUPLICATED and REPLACEMENT updates are indexed by stop,
  including updates whose trip ID also exists in static GTFS. Every matching
  stop occurrence is emitted, including repeated calls at a loop stop.
- Realtime Stop entities with a `parent_station` participate in the station's
  child-stop index. Their calls and metadata can appear on the parent's board.
  A realtime stop with no parent association is not automatically assigned to an
  OSM station by geographical proximity.
- Added routes and agencies are loaded when missing from the static board's
  dictionaries. Modified shape IDs resolve against realtime shapes first and
  static shapes second, when shapes were requested.

The response adds `realtime_stops`, keyed by chateau then GTFS stop ID, containing
GTFS-RT Stop metadata. Existing event fields continue to carry modification,
cancellation, trip and service-date information. Internal stop sequence and start
time keys used by the overlay are not serialized. OSM event deduplication preserves
trip, chateau, service day, frequency instance and stop occurrence; ordering uses
realtime times when available.

Aspen departure lookups run with up to eight concurrent chateaux and a three-second
connection/RPC deadline. Only selected compressed trip updates are expanded.
Ordinary scheduled realtime updates still use the existing board lookup. On a
realtime lookup failure, the scheduled board remains available.

Deploy Aspen before Birch for the new `get_departure_realtime` RPC. The new RPC is
appended to the service definition so existing method variants retain their wire
positions during rollout.

## Validation

The lightweight regression suite compiles the production overlay, compressed
snapshot lookup and GTFS-RT types without the backend's OSR native dependencies:

```sh
cargo test --locked --manifest-path tools/aspen-compression-check/Cargo.toml
```

Coverage includes inclusive spans, insertion at the first stop, cumulative delays,
loop ambiguity, renumbering collisions, sequence-only modified updates, added
loop calls, replacement cancellations, instance isolation, negative timestamps,
and indexed discovery through realtime children and replacement-stop indexes.

The full server type check requires the usual native dependencies, including TBB:

```sh
cargo check --bin birch --bin aspen
```

No production database timing measurements are included. Compare the existing
OSM endpoint debug timings before and after deployment with the same station,
time window and `include_shapes` setting.

Specification references:

- https://gtfs.org/documentation/realtime/feed-entities/trip-modifications/
- https://gtfs.org/documentation/realtime/reference/

## Nearby departures

`/nearbydeparturesfromcoordsv3` and Ramonda's nearby loads now use the same
indexed overlay as station boards. The earlier station-board change did not
connect this separate pipeline. Static direction or itinerary absence must not
return early: an indexed replacement stop or added-trip call can supply the
first departure at a nearby stop. Route selection and mode/distance policy still
apply to every emitted call, including realtime-only calls. Parent-linked realtime
platforms use the selected parent's station group and distance policy.

Build stop events with service date, trip start time and original GTFS sequence,
apply the shared loader, then filter the 18-hour window and sort/limit by effective
realtime departure/arrival, falling back to scheduled times. This also permits a
modification to move a previously out-of-window scheduled call into the result.
The response schema is unchanged.

The indexed lookup now includes updates for explicitly requested static trips,
as well as modifications and added-trip stop indexes. Dated DELETED and CANCELED
relationships apply at trip level even with an empty stop_time_update list and
are scoped to the service date/start-time instance. Nearby omits DELETED calls;
CANCELED trips and removed/skipped calls retain the existing cancellation display.
Undated updates retain the existing absolute-scheduled-time matching safeguard;
ambiguous undated deletions without stop timestamps are not guessed across days.

The shared database/RPC loader lives in `departure_realtime_loader`; Birch's
station endpoints re-export it. It retains the existing three-second indexed
lookup timeout and scheduled-board fallback, independent of nearby's ordinary
trip-update timeout parameter. Deploy the Aspen support from #198/#199 before
this Birch/Ramonda change. No new RPC or database migration is required.

## Destinations for unscheduled trips

Aspen fills its existing `trip_headsign` field for realtime-only, ADDED,
UNSCHEDULED, DUPLICATED and REPLACEMENT trips lacking a nonempty realtime
headsign. This also applies when the realtime trip ID exists in static GTFS.
Inference runs after all static and realtime stop metadata is assembled. The
terminal is the last non-SKIPPED call, using the highest stop sequence when all
reachable calls have sequences, otherwise feed order. Assigned stop IDs take
priority. The terminal's name, or its parent station's name, becomes the headsign.
A terminal with NO_DATA remains reachable; CANCELED/DELETED trips receive no
inferred destination. Unknown terminal names are left unresolved rather than
substituting an earlier stop. Feed-supplied nonblank headsigns always win.

The indexed lookup can also enrich older snapshots from their stop metadata.
All nearby and station/stop departure boards consume the same value, computed
from the full trip before selecting the requested stop calls. Empty headsigns in
TripProperties do not mask the inferred field. No wire-format change is needed;
deploy the updated Aspen importer for terminal stop lookup and enrichment.

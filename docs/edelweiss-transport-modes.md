# Edelweiss transport mode filtering

Edelweiss HTTP `/v1/route` and WebSocket `/v1/route/ws` now accept the optional
`modeAndModeOfOperationFilter` property inspired by OJP 2.0's
`ModeAndModeOfOperationFilter`. It is a JSON array of filters. A filter has an
`exclude` boolean and a `ptModes` array.

```json
{
  "from": {"lat": 51.5074, "lon": -0.1278},
  "to": {"lat": 52.2053, "lon": 0.1218},
  "departure_unix_seconds": 1795356000,
  "modeAndModeOfOperationFilter": [
    {"exclude": true, "ptModes": ["COACH"] }
  ]
}
```

A separate example permitting *only* train and metro is:

```json
{
  "modeAndModeOfOperationFilter": [
    {"exclude": false, "ptModes": ["TRAIN", "METRO"]}
  ]
}
```

Supported categories: `TRAIN`, `METRO`, `TRAM`, `BUS`, `COACH`,
`BOAT`, `FUNICULAR` (including cog/rack railway GTFS 117), and
`AERIAL_TRAMWAY`.

Multiple inclusive filters are unioned, then exclusions are subtracted.
An exclusion-only list starts with all transport categories. Omitted or empty
filter arrays preserve historical unfiltered routing. An inclusive filter with
an empty `ptModes` array is rejected. These categories restrict **transit**
legs, not OSR pedestrian or cycling access, egress, and direct legs.

This is a conceptual JSON adaptation of OJP, not native OJP XML.

## Where filtering happens

The API resolves the allowed classes once per request to a `u16` mask. Gentian
maps GTFS standard and extended route types to its existing, compact `Clasz`
column (no timetable format change). RAPTOR filters scanned routes and first
boarding events *before* building labels. PING, PONG, reverse search, static
and realtime route scanning, and deferred journey reconstruction all enforce
the same mask. The precomputed unrestricted lower-bound topology remains an
admissible optimistic bound for restricted journeys.

A realtime ADDED run with no original static route cannot yet determine its
transit type. With a restrictive filter it is excluded conservatively; an
unfiltered request continues to see it. Resolving route types for arbitrary
ADDED runs requires adding source-qualified route-type metadata to the realtime
overlay.

## Maple coach classification

Edit `coach-route-overrides.json` at the repository root, or set
`COACH_ROUTE_OVERRIDES_PATH=/path/to/config.json` when running Maple.

- `coach_feeds`: exact feed identifiers whose road-bus routes become GTFS 200
- `coach_feed_prefixes`: anchored feed-ID prefixes, e.g. `f-adirondack~trailways`
- `coach_agencies_by_feed`: map of feed ID to GTFS agency IDs or agency names,
  so mixed national feeds can have coach and local bus services simultaneously
- `coach_bus_chateaux`: all bus routes in these chateaux become GTFS 200;
  rail and other non-road routes are never changed

The config is loaded for every import, so updates do not require rebuilding
the Maple executable. **Changes are not retroactive**: reimport affected feeds
to update PostgreSQL, and rebuild/publish Gentian regional timetables for
Edelweiss to use their revised class information. Gentian now allows FlixBus
sources to compile; previously compiled timetables that omitted FlixBus must
also be rebuilt. FlixTrain remains a train.

For compatibility, clients that omit the new API field continue receiving all
transport modes. The existing intermodal `preTransitModes`/
`postTransitModes` fields still configure walk/bike independently.

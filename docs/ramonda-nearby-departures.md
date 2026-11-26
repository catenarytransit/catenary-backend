# Nearby departures over Ramonda

Connect to `wss://ramonda.catenarymaps.org/ws/` and send:

```json
{"type":"nearby_departures","request_id":"nearby-1","lat":34.05,"lon":-118.25,"limit_per_station":10,"limit_per_headsign":20}
```

The coordinate, time, radius, limits and realtime parameters match Birch's
`/nearbydeparturesfromcoordsv3`. Birch and Ramonda use the same library pipeline.
Stops and the effective departure timestamp are captured once for both phases.

The initial request returns two messages with the same `request_id`:

- `type: "nearby_departures", phase: "priority", complete: false`: the single
  closest eligible station group, only if its distance is strictly below 1 km,
  the nearest metro route, metros within 500 m, trams within 400 m, and the five
  nearest distinct local bus routes across all chateaux. Station selection
  matches the Compose and web hoisting rule, using the OSM station point where
  available. All statically identified routes at the chosen station are
  included, even when several feeds serve it. If no station qualifies, no
  station is prioritised.
- `type: "nearby_departures", phase: "remaining", complete: true`: all other
  station groups and local routes. The same rail route may appear in both
  phases at different stations. This phase starts its departure work after
  the priority response has been queued for transmission.

Each message's `data` has Birch's v3 response shape. Local departures are
partitioned by `(chateau_id, route_id)` and station departures additionally by
physical station identity (OSM ID or chateau-scoped GTFS station key).
Prioritisation reads static direction and route metadata before trip,
calendar and realtime retrieval. If the chosen station has no departures in
the requested window, or a realtime-only route is discovered later, the
remaining phase can supply the next eligible station or additional departures.
Both phases retain the same stop density and distance filtering policy.

Merge local groups by `(chateau_id, route_id)`, preserving headsigns and deduplicating
scheduled trip/stop/date occurrences within each headsign. Merge station groups by
OSM ID (or station name and coordinates when absent), preserving departures. Merge
`routes` and `stops` dictionaries at both chateau and entry levels.

After the first completed request on a connection, requests default to a single
`phase: "full", complete: true` response. A full response replaces the old snapshot,
so expired services disappear. Clients can explicitly set `progressive: false` for
refreshes on new connections, or `true` to request phases for a changed location.

A new nearby request cancels the previous nearby job on that socket. Clients must
ignore messages with an outdated request ID. `{"type":"cancel_nearby_departures"}`
also cancels the job. Trip subscriptions on the same socket remain independent.
Failures use `type: "nearby_departures_error"`, the request ID and a `message`.
A partial response is not completion; clients should retry if the final phase fails.

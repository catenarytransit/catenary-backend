# Edelweiss streaming and stateless timetable pages

`POST /v1/route` and `GET /v1/route/ws` share request validation, OSR access/egress,
PONG, itinerary IDs, metadata/geometry rendering, and page cursor construction.
The existing HTTP endpoint returns one final MOTIS-shaped response. The socket
sends replacement snapshots while PONG continues working within the budget.
No client changes are included in this backend PR.

## Requests and IDs

The socket accepts JSON text frames (maximum 64 KiB):

```json
{
  "type": "route",
  "requestId": "trip-1-page-1",
  "query": {
    "from": {"lat": 33.6405, "lon": -117.8443},
    "to": {"lat": 34.0537, "lon": -118.2428},
    "departure_unix_seconds": 1794250800,
    "number_of_results_before": 2,
    "number_of_results_after": 8,
    "timeout_seconds": 5,
    "timetable_view": true
  }
}
```

`query` is the same object accepted by HTTP. Both transports accept these
additional fields (`requestId`, `numberOfResultsBefore`, `numberOfResultsAfter`,
and `pageCursor` are aliases for the snake_case names):

| Field | Meaning |
| --- | --- |
| `request_id` | Optional HTTP correlation ID, 1–128 bytes. Otherwise a deterministic SHA-256 ID of the parsed request is returned. For WS, omit it or match the required envelope `requestId`. |
| `number_of_results_before` | 0–16 transit connections arriving before the departure reference, or at/before an arrival reference. |
| `number_of_results_after` | 0–16 transit connections departing at/after the reference. |
| `page_cursor` | Opaque `previousPageCursor` or `nextPageCursor` from a preceding response; resend the complete original query. |

When either count is supplied, the omitted count is zero and at least one must
be positive. Counts are page sizes: all alternatives tied at the boundary second
are included, so a page can be larger. Direct walking is separate and does not
consume a transit connection slot. A budget, timetable boundary, or absence of
service can produce fewer connections. Counts use independent arrival/departure
searches, rather than filtering departures from a single arrive-by result set.

Without counts/cursors, HTTP retains its existing interval/minimum behavior.
WS defaults to a page of 16 in the requested direction (or `min_itineraries` if
supplied). Streaming and pagination require `timetable_view=true`.

The server echoes `requestId` on every WS event and HTTP success/error response.
A socket remembers at most 128 ID/fingerprint pairs, but no completed journeys
or search arrays. Repeating the same active ID and JSON query is acknowledged
without starting another worker. Reusing an ID for different query JSON is an
error; assign a new ID to every page. A completed identical request can be
recomputed. Reconnect after 128 distinct IDs. There is no global response cache.

## Events and rendering

```json
{"type":"accepted","requestId":"trip-1-page-1","duplicate":false}
{"type":"snapshot","requestId":"trip-1-page-1","sequence":1,"response":{"itineraries":[],"direct":[]}}
{"type":"complete","requestId":"trip-1-page-1","response":{"itineraries":[],"direct":[]}}
```

The empty arrays above abbreviate real MOTIS-shaped responses. Each journey has
a deterministic nonempty `id`. Snapshots contain the current page, **not deltas**:
replace the previous snapshot for that request ID. This also handles a journey
removed by dominance or changed by final transfer refinement. `complete` contains
the authoritative final snapshot, even when its IDs did not change.

Store snapshots separately by page request ID, then deduplicate by itinerary ID
when merging pages into the UI. Ignore events belonging to an abandoned request.

Only reconstructed, cursor-validated journeys are published, after a completed
PING/PONG sweep. A raw PING candidate is never sent. The first snapshot can
contain one or several connections. It is not guaranteed within a fixed number
of milliseconds: the first sweep and enrichment must finish first.

Streaming keeps searching past the original minimum until the wall budget, an
empty PING, the timetable edge, or Nigiri's six-day cursor cap. Page sizes bound
the emitted result set while the search continues. The budget starts after OSR
access/egress preparation and includes snapshot enrichment. With both directions,
the first side receives half the remaining time; the other receives the remainder.
The requested timestamp direction is searched first. This is a cooperative soft
budget, checked at complete sweep boundaries, so a sweep can overrun it.
`debugOutput.raptor.profile_budget_exhausted` and per-side `debugOutput.pages`
distinguish partial searches from natural completion. `complete` means the worker
finished, not that every timetable connection was enumerated.

Only new/changed itinerary IDs are hydrated. Metadata and geometry for unchanged
itineraries are reused within the request. The socket handles delivery/heartbeats
on the async runtime; routing and enrichment run on blocking workers. A bounded
two-message channel applies backpressure between those workers and the socket.

## Scroll later and request earlier

Keep the original locations/modes/options, give the next request a new ID, and
set its `page_cursor` to the returned cursor:

```js
function nextPage(originalQuery, response, earlier, requestId) {
  const cursor = earlier ? response.previousPageCursor : response.nextPageCursor;
  if (!cursor) return; // no transit boundary was established
  socket.send(JSON.stringify({
    type: "route", requestId,
    query: {
      ...originalQuery,
      request_id: requestId,
      page_cursor: cursor,
      number_of_results_before: earlier ? 6 : 0,
      number_of_results_after: earlier ? 0 : 6
    }
  }));
}
```

A previous cursor anchors an arrive-by search one second before the earliest
returned transit arrival. A next cursor anchors a departure search one second
after the latest returned transit departure. Whole-second ties remain together.
This follows the before/after model described in
[OJP TripRequest 2.0](https://opentransportdata.swiss/en/cookbook/open-journey-planner-ojp-landing-page/ojptriprequest-2-0/),
using Edelweiss's second precision instead of OJP's one-minute example offsets.
Page searches use a one-second initial PONG interval and then expand by connection
count. Changing page size is allowed. Changing other query options invalidates the
cursor. Cursors contain versioned query/time/direction data, not server references;
they are query-bound descriptors, not authenticated tokens or frozen timetable
snapshots. Reconnection, another worker, and another server can serve a page.

Each page recomputes access/egress and allocates its own RAPTOR buffers. They are
dropped when the request finishes/cancels, even if the socket remains open. Shared
read-only timetable/OSR data remains loaded as before. Clients may use HTTP for
any page with the same query and cursor fields.

## Replacing and cancelling requests

A different ID received while a request runs cancels that search and queues one
replacement. Events are `queued` for the new ID, `cancelled` for the old ID, then
`accepted` and snapshots for the new one. A second queued replacement is rejected
until the first starts or is cancelled. This bounds active RAPTOR state to one
worker per socket. Explicit cancellation is:

```json
{"type":"cancel","requestId":"trip-1-page-1"}
```

An active request responds with `cancelling`, then `cancelled` after its blocking
worker exits. A queued request can be cancelled immediately. A disconnect also
sets the cancellation flag. Cancellation is cooperative between point searches
and sweeps; an in-flight OSR call or point search finishes before resources are
released. No further snapshots from the cancelled request are forwarded.

Errors use `{"type":"error","requestId":"...","message":"..."}`. Malformed
commands without a usable ID return a null ID. The server pings every 15 seconds
and closes connections without heartbeat activity for 45 seconds. Reverse proxies
must forward WebSocket upgrades to `/v1/route/ws`.

## Verification

```sh
cargo test --locked --manifest-path tools/raptor-check/Cargo.toml
cargo test --locked --manifest-path tools/edelweiss-stream-check/Cargo.toml
cargo check --bin edelweiss
```

The first harness compiles the production routing modules and exercises real
PONG frontiers, streaming, cancellation, deadlines, and the existing routing
regressions. The second compiles the production pagination/socket modules and
uses real TCP WebSockets with a controlled worker to test delivery before
completion, IDs, cancellation, replacement ordering, disconnect cleanup, and page
boundaries. It does not substitute for running the full binary against production
OSR graphs and PostgreSQL metadata.

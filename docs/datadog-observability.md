# Catenary Datadog telemetry

Set `DATADOG_ENABLED=1` to enable best-effort DogStatsD metrics from the Birch,
Ramonda, Spruce, and Edelweiss services. Defaults to **off**.
`DD_AGENT_HOST` defaults to `127.0.0.1`; `DD_DOGSTATSD_PORT` defaults to `8125`;
`DD_ENV` defaults to `production`. Run a local Datadog Agent with DogStatsD
enabled. The UDP exporter never blocks API requests or turns telemetry failures
into API errors.

Counters: `catenary.http.requests`, `catenary.nearby.requests`,
`catenary.nearby.chateau_accesses`, `catenary.trip.views`,
`catenary.tiles.sent`, `catenary.stop.requests`,
`catenary.directory.requests`, `catenary.spruce.initial_requests`,
`catenary.spruce.chateau_requests`, and `catenary.routing.queries`.

All request-scoped metrics carry the validated `country` tag representing
the **client's country**, including `trip.views`, stop-board and nearby
chateau counters, Spruce map/WebSocket counters, routing query/chateau
counters, routing phase distributions and successful tile counts. The
`chateau` and `region` tags still describe the transit data returned,
not the requester's location. Country tags use uppercase two-letter codes
(e.g. `US`, `FR`, `CH`) or `unknown` when the trusted header is absent
or invalid; we do not use IPs, coordinates or unbounded country strings.

Stop/station boards export `catenary.stop_board.chateau_accesses` tagged by
`service:birch`, `endpoint:departures_at_stop|departures_at_osm_station`,
`chateau` and `country`. Only unique chateaus in the final returned departure events
are counted (one count per chateau per HTTP request), not every nearby candidate
or route metadata entry. Empty boards emit no chateau counter.

Routing exports `catenary.routing.chateau_accesses` tagged by
`service:edelweiss`, `chateau` and `country`. It extracts GTFS provenance directly
from compiled timetable transit rides, not from hydrated MOTIS response JSON.
Each distinct chateau is counted once per logical query, as soon as it appears
in a rendered result. Progressive WebSocket snapshots and a subsequent final
result do not double count, even if the query ends early. Walk-only routes
emit no chateau counter.

**Deployment:** Edelweiss must be rebuilt/restarted with the updated binary,
and its own systemd unit/container must set `DATADOG_ENABLED=1`; enabling
the exporter for Birch or Spruce does not enable it for Edelweiss. Also
ensure `DD_AGENT_HOST`/`DD_DOGSTATSD_PORT` point to a reachable DogStatsD
agent. Check `catenary.routing.queries{service:edelweiss}` first: if it
is missing too, troubleshoot exporter enablement before chateau extraction.

Stop-board and routing chateau counters exclude Datadog Synthetics requests using
the same header detection as nearby departures. Edelweiss also excludes these
requests from `catenary.routing.queries` and `catenary.routing.phase_ms` on
both HTTP and WebSocket routes. CLI routing queries are unaffected.
Sample Datadog queries:

```text
sum:catenary.stop_board.chateau_accesses{service:birch} by {country,endpoint,chateau}.as_count()
sum:catenary.routing.chateau_accesses{service:edelweiss} by {country,chateau}.as_count()
sum:catenary.trip.views{*} by {service,country,chateau}.as_count()
sum:catenary.routing.queries{service:edelweiss} by {country,region}.as_count()
```

Distributions: `catenary.http.duration_ms` and
`catenary.routing.phase_ms` (selection, access, egress, direct_walk,
osr_parallel_wall, raptor, metadata, response_build, query_total).
Use `avg:` or `p95:` Datadog distribution queries for latency dashboards.

HTTP metrics use the matched Actix route template, never an unbounded URL;
WebSocket logical requests are counted inside their respective message handlers.
`CF-IPCountry` is accepted as a country tag only when the infrastructure ensures
it was set/overwritten by Cloudflare or a trusted gateway. Otherwise it must
not be considered authoritative. Never trust a client-supplied country header:
strip/overwrite it at the ingress proxy, and use `country:unknown` for direct
requests without a trusted header. WebSocket message metrics use the country
captured from the initial handshake so asynchronous follow-up frames retain
the same country without forwarding user-controlled headers.

Datadog logs and traces can be collected by an external Datadog Agent using
existing application stdout and tracing pipelines; this change adds the metric
exporter, not a new application log exporter.

Spruce diagnostics: `catenary.spruce.map_requests` by `mode` (legacy or v2) and
`catenary.spruce.map_empty{mode:v2}` distinguish received map subscriptions
from V2 bounding boxes that matched no chateaus. `catenary.spruce.chateau_requests`
now also covers distinct chateaus in HTTP and WebSocket nearby-departures results.
Ensure the running Spruce release binary includes this PR; a systemd unit
setting `DATADOG_ENABLED=1` enables export but does not deploy new code.

## Debugging Spruce WebSocket chateau counts

An HTTP GET to `/ws/live` is a WebSocket upgrade and is reported as
`catenary.spruce.initial_requests`; it does **not** imply the client sent any
map subscription. Query:

```text
sum:catenary.spruce.ws.messages{service:spruce} by {country,endpoint,type}.as_count()
sum:catenary.spruce.map_requests{service:spruce} by {country,mode}.as_count()
sum:catenary.spruce.chateau_requests{service:spruce} by {country,chateau}.as_count()
```

If only initial requests are visible, and no `spruce.ws.messages`, verify
the client is actually sending WebSocket text messages to Spruce. If messages
appear as `type:invalid`, inspect the WebSocket protocol (e.g. missing
`type` or incorrect `#[serde(flatten)]` fields) rather than Datadog.

For temporary packet-level verification, set `DATADOG_DEBUG_METRICS=1` in
the Spruce systemd unit and restart the service. Journal output will include
the exact `spruce.*` DogStatsD UDP packets plus socket send errors. **Disable
this after troubleshooting**: it adds synchronous logging to each emission.

## Synthetic monitoring exclusion

HTTP requests identified by a User-Agent containing `synthetics`, a
`sec-datadog` header containing `synthetics`, or an `x-datadog-origin`
header containing `synthetics` (case-insensitive) are excluded from
HTTP request and duration metrics. Birch and Spruce nearby-departures handlers
also exclude these requests from per-chateau counters, which are emitted
independently of HTTP middleware. Edelweiss excludes them from routing query
counts and phase latency distributions as well. The API response is not changed.

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

Distributions: `catenary.http.duration_ms` and
`catenary.routing.phase_ms` (selection, access, egress, direct_walk,
osr_parallel_wall, raptor, metadata, response_build, query_total).
Use `avg:` or `p95:` Datadog distribution queries for latency dashboards.

HTTP metrics use the matched Actix route template, never an unbounded URL;
WebSocket logical requests are counted inside their respective message handlers.
`CF-IPCountry` is accepted as a country tag only when the infrastructure ensures
it was set/overwritten by Cloudflare or a trusted gateway. Otherwise it must
not be considered authoritative.

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
sum:catenary.spruce.ws.messages{service:spruce} by {endpoint,type}.as_count()
sum:catenary.spruce.map_requests{service:spruce} by {mode}.as_count()
sum:catenary.spruce.chateau_requests{service:spruce} by {chateau}.as_count()
```

If only initial requests are visible, and no `spruce.ws.messages`, verify
the client is actually sending WebSocket text messages to Spruce. If messages
appear as `type:invalid`, inspect the WebSocket protocol (e.g. missing
`type` or incorrect `#[serde(flatten)]` fields) rather than Datadog.

For temporary packet-level verification, set `DATADOG_DEBUG_METRICS=1` in
the Spruce systemd unit and restart the service. Journal output will include
the exact `spruce.*` DogStatsD UDP packets plus socket send errors. **Disable
this after troubleshooting**: it adds synchronous logging to each emission.

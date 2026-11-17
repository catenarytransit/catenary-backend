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

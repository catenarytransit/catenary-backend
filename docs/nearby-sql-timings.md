# Nearby Departures v3 SQL diagnostics

Both Birch `GET /nearbydeparturesfromcoordsv3` and Ramonda's shared
`nearby_departures` response expose `data.debug.sql_queries` (for Birch,
`debug.sql_queries`). Existing response keys are unchanged.

The new array contains **one entry for each executed SQL query**, including
individual concurrent 128-ID batches. Records are sorted by their approximate
start time, not completion order. Each record includes:

- `chateau`: GTFS château name, or `null` for shared spatial/OSM lookups.
- `query`: stable stage label (e.g. `direction_rows`,
  `itinerary_metadata_by_direction`, `compressed_trips`,
  `calendar_dates`, `paris_terminal_itinerary_rows`).
- `started_ms`: milliseconds since the request's `prepare_nearby` began,
  measured before checking out a database connection.
- `pool_wait_ms`: milliseconds between the start of checkout and execution
  (including connection checkout).
- `query_ms`: elapsed milliseconds awaiting the Diesel query, including
  transferring and decoding returned rows. This is **client-side query time**,
  not PostgreSQL's server-side `EXPLAIN ANALYZE` execution time.
- `input_ids`: count of IDs passed to that query (or 1 for a spatial query).
- `returned_rows`: decoded row count (0 on query error).
- `success`: whether the query completed successfully.

Example of a **synthetic record** (not a measured Barcelona result):

```json
{
  "chateau": "example-chateau",
  "query": "compressed_trips",
  "started_ms": 625.7,
  "pool_wait_ms": 11.3,
  "query_ms": 4820.6,
  "input_ids": 128,
  "returned_rows": 23547,
  "success": true
}
```

Multiple SQL queries can overlap: **do not add their `query_ms` values to
estimate total pipeline latency**. For a slow request, compare the largest
individual `query_ms`, pool waits, starting offsets and row counts against
`pipeline_processing_time_ms` and `total_time_ms`.

For progressive Ramonda requests, the priority direction/route lookup is
recorded as `priority_direction_route_join`; its timings are included in
subsequent phase debug payloads. Per-phase SQL records describe the work needed
to produce that phase, with initial preparation timings repeated for reference.

This PR intentionally changes only diagnostics. It does not change indexes,
batch sizes, concurrency, routing logic or realtime selection.

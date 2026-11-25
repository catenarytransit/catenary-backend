# Aspen

Default port for Aspen to listen to is 40427

### Leader election

The leader thread / program is used to assigned various chateaux to each aspen worker.

Multiple instances of the Aspen-leader microservice can be spawned. One of them will automatically be elected the leader. Originally, this was bundled with the main process. It could theoretically can be recombined back into the main process though we are seperating it now because tokio is a strange runtime.

### Tarpc

Aspen, similar to the other microservices, uses Tarpc to communicate with other processes. Alpenrose microservices can submit new GTFS-realtime data. Birch

### GTFS-RT update timestamps

Aspen's `get_gtfs_rt_timestamps(realtime_feed_id)` RPC returns the currently
stored GTFS-RT header timestamp for each category without expanding feed entities.
Birch exposes the same result through:

```http
GET /gtfs_rt_timestamps?feed_id=<realtime-feed-id>
```

```json
{
  "vehicle_positions": 1794000000,
  "trip_updates": 1794000010,
  "alerts": null
}
```

Values are publisher timestamps in **Unix seconds**, not download or processing
times. A category is `null` if Aspen has no stored feed for it or its current
header has no timestamp. Empty feeds can still have timestamps. An assigned feed
with no data returns all three fields as `null`. Birch returns 404 when no Aspen
assignment is found and 503 for etcd connection, Aspen connection, or RPC failures;
these failures are not reported as missing timestamps. Responses use `Cache-Control:
no-store`.

Deploy Aspen and Birch together because this adds a method to the tarpc service.

### Processing of data

Data is matched against the GTFS schedules stored in Postgres data, and additional data is often fetched and brought in, such as train formation and platform information. The data is stored actively in RSS memory (DRAM) as it changes extremely frequently, sometimes second-to-second.

### Metrolink GTFS-RT position exclusions

Aspen reads `metrolink-invalid-positions.txt` from its working directory once at startup. Each nonempty line is a comma-separated latitude and longitude; `#` introduces a comment. The file is runtime data, not embedded in the executable. To use a different path (for example a mounted file), set `ASPEN_METROLINK_INVALID_POSITIONS_FILE`.

Deploy the text file with Aspen and restart the process after editing it; no Rust rebuild is required for coordinate changes. The Aspen Docker build already copies the repository into its working directory. A missing default file logs a warning and disables this filter, so existing deployments must install the file. An unreadable explicitly configured file or malformed entry causes startup to fail with a diagnostic. An empty file intentionally disables filtering.

Only the `f-metrolinktrains~rt` GTFS-RT vehicle feed is filtered, before compact feed storage. Exact coordinate pairs are removed from the position field while the vehicle and trip metadata remain intact. Supplemental PTC data retains its original behavior and can fill a missing position even if its coordinates match an exclusion.

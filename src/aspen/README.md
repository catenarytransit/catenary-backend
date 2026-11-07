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

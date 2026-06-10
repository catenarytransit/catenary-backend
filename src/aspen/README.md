# Aspen

Default port for Aspen to listen to is 40427

### Leader election

The leader thread / program is used to assigned various chateaux to each aspen worker.

Multiple instances of the Aspen-leader microservice can be spawned. One of them will automatically be elected the leader. Originally, this was bundled with the main process. It could theoretically can be recombined back into the main process though we are seperating it now because tokio is a strange runtime.

### Tarpc

Aspen, similar to the other microservices, uses Tarpc to communicate with other processes. Alpenrose microservices can submit new GTFS-realtime data. Birch

### Processing of data

Data is matched against the GTFS schedules stored in Postgres data, and additional data is often fetched and brought in, such as train formation and platform information. The data is stored actively in RSS memory (DRAM) as it changes extremely frequently, sometimes second-to-second.
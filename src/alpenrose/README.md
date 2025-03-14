# Distributed compatible architecture

Worker and leader model

Leader responsible for reading the state of the password system and updating how often data is refreshed and what URLs to use.

Feeds currently taken are held via assigned leases. If the node disconnects from etcd, it will be avaliable to for other workers to start using because their lease would expire.

They send data to the Aspen datastore and processor over Grpc
Repeat sends to Aspen are prevented using a hash function for `gtfs_rt::FeedMessage`

# etcd Directory structure

In Etcd v3, etcd no longer uses a node based structure with directories. The structure is flattened. Thus, parent nodes are not required. We can simplify the model by letting data expire based on the worker lease ids and querying based on prefix.

**Worker Identification**
```rs
key: format!("/alpenrose_workers/{}", this_worker_id).as_str(),
value: catenary::bincode_serialize(&etcd_lease_id).unwrap(),
```
Lease: worker inserts own lease
value is i64 lease id

**Alpenrose Assignments**

Per worker:
```rs
key: format!("/alpenrose_assignments_last_updated/{}", this_worker_id),
value: catenary::bincode_serialize(last_assignment_unix_time_ms).unwrap()
```

Unix time is milliseconds in u64

Each feed id under worker:
```rs
key: format!("/alpenrose_assignments/{}/{}", this_worker_id, feed_id).as_str(),
value: catenary::bincode_serialize(&assignment_data).unwrap()
```

Lease: use the worker's lease
assignment data is type `RealtimeFeedFetch`

A query for all prefixes of `/alpenrose_assignments/WORKER_ID/` can be performed to identify the tasks

Cleanup of `/alpenrose_assignments` is not required because of leases
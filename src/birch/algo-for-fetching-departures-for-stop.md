- for stop id, fetch all the stops

```rs
fn get_departures(stop_id: String, chateau: String) -> DepartureBoard {}
```

```rs
struct DepartureBoard {
    updated_time_ms: u64,
    departures: Vec<DeparturesPerRoute>>
}

struct DeparturesPerRoute {
    route_id: String,
    chateau: String,
    //headsign -> Sorted Vec of Departures
    directions: BTreeMap<String, Vec<DepartureArrivalEvent>>
}

struct DepartureArrivalEvent {
    stop_id: String,
    stop_location: geo::Point,
    stop_code: String,
    scheduled_departure_time_s: Option<u64>,
    //optional if the service is the last stop
    scheduled_arrival_time_s: Option<u64>,
    actual_departure_time_s: Option<u64>,
    actual_arrival_time_s: Option<u64>,
    has_realtime_trips: bool,
    trip: gtfs_rt::TripDescriptor,
    is_frequency: bool,
    has_realtime_vehicles: bool,
    platform: Option<String>
}
```

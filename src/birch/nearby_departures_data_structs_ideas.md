# Data Structure reorganising for nearby departures v2

### Requirements / Advantages over v1:

- Show block id
- Handle loop routes by querying for departures on both side of the route
- Faster query time
- Show alerts
- Show vehicle id
- Handle moved stops, trip modifications
- Adapt frequency based data

### Current incompatibilities

The current method of computing the nearest stop time from the single itin row itself does not work with trip modifications. This is because trip modifications requires knowledge of the entire trip to compute a scheduled offset, and to understand where to insert the stop id.

However, given that trip modifications themselves are rare, it is worth extra computation time to do the steps.

Note that we still need a method of identifying these trip modifications.

Non-scheduled trips can be accounted for using a route index on the stop itself.

When a trip contains cancelled stops, the algorithm should seek backwards and forwards until a viable stop is found

### New algorithm steps

Rather than determining everything about the trips initially from the schedule, a fetch is performed first to the realtime database for any trip modifications, trip updates, and extra trips. 

Then we may compile the trips.

All trips can be sorted and multiple references made into a struct that becomes the key of a hashmap.

```rust
#[derive(Hash, PartialEq, Eq, Hash)]
struct CatchType {
    pub chateau: String,
    pub route_id: String,
    pub direction_name: Option<String>
}

struct CatchPoint {
    pub trip_id: Option<String>,
    pub trip_modification_applied: bool,
    pub stop_moved: bool,
    pub stop_id: bool,
    pub old_stop_id: bool,
    pub schedule_relationship: bool,
    pub trip_cancelled: bool,
    pub trip_deleted: bool,
    pub trip_modification_id: String,
    pub realtime_departure_time: Option<u64>,
    pub scheduled_departure_time: Option<u64>,
    pub realtime_arrival_time: Option<u64>,
    pub scheduled_arrival_time: Option<u64>,
    pub schedule_interpolation: bool,
    pub frequency_based: bool,
    pub start_time: Option<chrono::NaiveTime>,
    pub service_date: Option<chrono::NaiveDate>,
    pub tz: chrono::Tz
}

struct TripInfo {
    pub trip_short_name: Option<String>,
    pub route_id: String,
    pub trip_cancelled: bool,
    pub trip_deleted: bool,
    pub frequency: bool
}
```

hashmap catchtype goes to vec of catchpoint which is sorted by departure and arrival times

if the new stop lies outside of the query area, then the trip can be removed.

directions requiring multiple stop direction identification will need to have an additional step to query for the direction id, then split based on direction id.

directions are then marked if they contain a stop direction based system. direction stop pairs are then iterated through.
PROBLEM with this algorithm step. How to handle the trip modification?

#### Handling of frequencies txt

Trips in the schedule can be checked if frequency data is contained.
if the number is exact, simply duplicate the start time until it is finished duplicating.

## final data structure output is still unclear

### Code should be broken up into function blocks 

Needs flowchart.

### Grouping should be done by route.
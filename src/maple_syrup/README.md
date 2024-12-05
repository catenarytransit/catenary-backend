# GTFS compression algorithm

The goal is to reduce the current 197 GB of Stop times into a series of transfer patterns and trip patterns.

### How to run a direct connection query

In Hannah Bast, et. al's original Transfer Patterns (2010) paper section 4.1, direct-connection queries are precomputing by storing the list of stops in each line and then storing arrays of Lines and stations.

In reality, many routes have many different forks and iterations such as the Vancouver skytrain. Thus, we rename the line system (which may be confused with the route system in GTFS) to an itinerary, which each itinerary is a sequence of times from one station to the next.

We then store the start time of each trip and the arrival or departure time from the station is simply the start time of the trip + the itinerary offset.

We can answer a direct-connection query of A@t -> B by looking at all itineraries containing stops A and B, then querying the start times of the trip id. Each stop time is thus trip_start_time + stop_offset.

Remember that trip ids must be filtered by service ids that use the day of the query.

Those resulting times can then be compared to return the cost of A@t -> B.

### Testing

```bash
cargo test maple_syrup::tests::irvine_interpolation_test
```
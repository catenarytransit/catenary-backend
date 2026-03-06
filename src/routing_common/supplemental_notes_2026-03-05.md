Exerpts from Nigiri by the Motis project 

- Internal data structures use a neutral timezone (similar to Unix time).
trip_idx_t: a trip is used to communicate with the outside world. Trips come from static timetable data which is usually given in some kind of local time (e.g. for GTFS: convert using offset at noon of the respective service date). Trips are also referenced in real-time updates via GTFS-RT or SIRI.
transport_idx_t: Since we want to have everything in UTC, trips from timetable data have to be converted to UTC which can yield an arbitrary number of transports. For example a "normal" trip in a timezone with daylight saving time will be split into a winter and a summer version. If the trip goes over midnight, it can have even more versions on the switching days between winter and summer time. For each transport_idx_t, we store as timetable:
a bitfield_idx_t referencing a bitfield where each bit gives the information about whether or not the transport operates on a given day. The first bit/day is timetable.internal_interval_days().from_.
a series of (departure, arrival, departure, arrival, ...) times given relative to the service day from the bitfield.
route_idx_t: A route groups transports based on their stop sequence and other factors that are relevant for routing. This grouping is required by the most modern routing algorithms as an optimization: the routing algorithm will only look at the first departure of each route. This is sufficient because the grouping of transports into routes is done in a way that a later departure on the same route cannot yield a better journey. This way, all later departures can be discarded. Note that this also means that optimizing a new criterion such as occupancy or CO2 will require a change of the route definition in order for the routing to guarantee correctness.

- Information from Felix:
Several problems exist with Transfer Patterns.
1. The connections on TP are not optimal when a disruption happens, like what if a large station with many transfers is blocked?
2. The preprocessing costs are too high
3. Data needs to be imported very frequently for many timetable sources such as European sources and NYC subway.

Another newer version of hyp-raptor exists but the preprocessing times are also too high to the speedup.
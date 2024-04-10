use core::hash;
// GTFS stop time compression algorithm
// Probably not compatible with transfer patterns yet, this is just for schedule lookup for now
use crate::enum_to_int::*;
use fasthash::MetroHasher;
use gtfs_structures::ContinuousPickupDropOff;
use gtfs_structures::DirectionType;
use gtfs_structures::TimepointType;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Hash, Clone, Debug)]
pub struct ItineraryCover {
    pub stop_sequences: Vec<StopDifference>,
    //map 0 to false and 1 to true
    pub direction_id: Option<bool>,
    pub route_id: String,
    pub trip_headsign: Option<String>,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
struct StopDifference {
    pub stop_id: String,
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub continuous_pickup: i16,
    pub continuous_drop_off: i16,
    pub stop_headsign: Option<String>,
    pub drop_off_type: i16,
    pub pickup_type: i16,
    //true is exact, false is approximate
    pub timepoint: bool,
}

#[derive(Clone, Debug)]
struct TripUnderItinerary {
    pub trip_id: String,
    pub start_time: u32,
    pub service_id: String,
    pub wheelchair_accessible: i16,
    pub block_id: Option<String>,
    pub bikes_allowed: i16,
    pub frequencies: Vec<gtfs_structures::Frequency>,
}

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s: MetroHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

pub struct ResponseFromReduce {
    pub itineraries: HashMap<u64, ItineraryCover>,
    pub trips_to_itineraries: HashMap<String, u64>,
    pub itineraries_to_trips: HashMap<u64, Vec<TripUnderItinerary>>,
}

pub fn reduce(gtfs: &gtfs_structures::Gtfs) -> ResponseFromReduce {
    let mut itineraries: HashMap<u64, ItineraryCover> = HashMap::new();
    let mut trips_to_itineraries: HashMap<String, u64> = HashMap::new();
    let mut itineraries_to_trips: HashMap<u64, Vec<TripUnderItinerary>> = HashMap::new();

    for (trip_id, trip) in &gtfs.trips {
        let mut stop_diffs: Vec<StopDifference> = Vec::new();

        if trip.stop_times.len() < 2 {
            println!("Trip {} doesn't contain enough times", trip);
            continue;
        }

        //according to the gtfs spec
        //Arrival times are "Required for the first and last stop in a trip (defined by stop_times.stop_sequence)"
        if trip.stop_times[0].arrival_time.is_none()
            || trip.stop_times[trip.stop_times.len() - 1]
                .departure_time
                .is_none()
        {
            println!("Invalid trip {} with no start or end time", trip_id);
            continue;
        }

        let mut start_time: u32 = trip.stop_times[0].arrival_time.unwrap();

        //this trip "starts at 09:00" local time or something
        for stop_time in trip.stop_times.iter() {
            let arrival_time_since_start: Option<i32> = match stop_time.arrival_time {
                Some(arrival_time) => Some(arrival_time as i32 - start_time as i32),
                None => None,
            };

            let departure_time_since_start: Option<i32> = match stop_time.departure_time {
                Some(departure_time) => Some(departure_time as i32 - start_time as i32),
                None => None,
            };

            let stop_diff = StopDifference {
                stop_id: stop_time.stop.id.clone(),
                arrival_time_since_start: arrival_time_since_start,
                departure_time_since_start: departure_time_since_start,
                continuous_pickup: continuous_pickup_drop_off_to_i16(&stop_time.continuous_pickup),
                continuous_drop_off: continuous_pickup_drop_off_to_i16(
                    &stop_time.continuous_drop_off,
                ),
                stop_headsign: stop_time.stop_headsign.clone(),
                drop_off_type: pickup_dropoff_to_i16(&stop_time.drop_off_type),
                pickup_type: pickup_dropoff_to_i16(&stop_time.pickup_type),
                timepoint: timepoint_to_bool(&stop_time.timepoint),
            };

            stop_diffs.push(stop_diff);
        }

        let itinerary_cover = ItineraryCover {
            stop_sequences: stop_diffs,
            direction_id: match trip.direction_id {
                Some(direction) => Some(match direction {
                    DirectionType::Outbound => false,
                    DirectionType::Inbound => true,
                }),
                None => None,
            },
            route_id: trip.route_id.clone(),
            trip_headsign: trip.trip_headsign.clone(),
        };

        //itinerary id generated
        let hash_of_itinerary = hash(&itinerary_cover);

        itineraries.insert(hash_of_itinerary, itinerary_cover);
        trips_to_itineraries.insert(trip_id.clone(), hash_of_itinerary);

        let trip_under_itinerary = TripUnderItinerary {
            trip_id: trip_id.clone(),
            start_time: start_time,
            service_id: trip.service_id.clone(),
            wheelchair_accessible: availability_to_int(&trip.wheelchair_accessible),
            block_id: trip.block_id.clone(),
            bikes_allowed: bikes_allowed_to_int(&trip.bikes_allowed),
            frequencies: trip.frequencies.clone(),
        };

        itineraries_to_trips
            .entry(hash_of_itinerary)
            .and_modify(|existing_trips| {
                existing_trips.push(trip_under_itinerary.clone());
            })
            .or_insert(vec![trip_under_itinerary]);
    }

    ResponseFromReduce {
        itineraries,
        trips_to_itineraries,
        itineraries_to_trips,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}

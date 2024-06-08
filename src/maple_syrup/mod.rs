// GTFS stop time compression algorithm
// Probably not compatible with transfer patterns yet, this is just for schedule lookup for now
use crate::enum_to_int::*;
use crate::fast_hash;
use ahash::AHashMap;
use gtfs_structures::DirectionType;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use tzf_rs::DefaultFinder;

lazy_static! {
    static ref FINDER: DefaultFinder = DefaultFinder::new();
}

#[derive(Hash, Clone, Debug)]
pub struct ItineraryCover {
    pub stop_sequences: Vec<StopDifference>,
    //map 0 to false and 1 to true
    pub direction_id: Option<bool>,
    pub route_id: String,
    pub trip_headsign: Option<String>,
    pub timezone: String,
    pub shape_id: Option<String>,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
pub struct StopDifference {
    pub stop_id: String,
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub interpolation_by_catenary: bool,
    pub interpolated_time_since_start: Option<i32>,
    pub continuous_pickup: i16,
    pub continuous_drop_off: i16,
    pub stop_headsign: Option<String>,
    pub drop_off_type: i16,
    pub pickup_type: i16,
    //true is exact, false is approximate
    pub timepoint: bool,
    pub gtfs_stop_sequence: u16,
}

pub struct DirectionPattern {
    pub direction_id: Option<bool>,
    pub stop_sequence: Vec<String>,
    pub headsign_or_destination: Option<String>,
    pub gtfs_shape_id: Option<String>,
    pub route_id: String,
}

#[derive(Clone, Debug)]
pub struct TripUnderItinerary {
    pub trip_id: String,
    pub start_time: u32,
    pub service_id: String,
    pub wheelchair_accessible: i16,
    pub block_id: Option<String>,
    pub bikes_allowed: i16,
    pub frequencies: Vec<gtfs_structures::Frequency>,
    pub trip_short_name: Option<String>,
    pub route_id: String,
}

pub struct ResponseFromReduce {
    pub itineraries: AHashMap<u64, ItineraryCover>,
    pub trips_to_itineraries: AHashMap<String, u64>,
    pub itineraries_to_trips: AHashMap<u64, Vec<TripUnderItinerary>>,
    pub direction_patterns: AHashMap<u64, DirectionPattern>,
}

pub fn reduce(gtfs: &gtfs_structures::Gtfs) -> ResponseFromReduce {
    let mut itineraries: AHashMap<u64, ItineraryCover> = AHashMap::new();
    let mut trips_to_itineraries: AHashMap<String, u64> = AHashMap::new();
    let mut itineraries_to_trips: AHashMap<u64, Vec<TripUnderItinerary>> = AHashMap::new();

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

        let start_time: u32 = trip.stop_times[0].arrival_time.unwrap();

        //this trip "starts at 09:00" local time or something
        for stop_time in trip.stop_times.iter() {
            let arrival_time_since_start: Option<i32> = stop_time
                .arrival_time
                .map(|arrival_time| arrival_time as i32 - start_time as i32);

            let departure_time_since_start: Option<i32> = stop_time
                .departure_time
                .map(|departure_time| departure_time as i32 - start_time as i32);

            let stop_diff = StopDifference {
                stop_id: stop_time.stop.id.clone(),
                arrival_time_since_start,
                departure_time_since_start,
                continuous_pickup: continuous_pickup_drop_off_to_i16(&stop_time.continuous_pickup),
                continuous_drop_off: continuous_pickup_drop_off_to_i16(
                    &stop_time.continuous_drop_off,
                ),
                interpolation_by_catenary: false,
                interpolated_time_since_start: None,
                stop_headsign: stop_time.stop_headsign.clone(),
                drop_off_type: pickup_dropoff_to_i16(&stop_time.drop_off_type),
                pickup_type: pickup_dropoff_to_i16(&stop_time.pickup_type),
                timepoint: timepoint_to_bool(&stop_time.timepoint),
                gtfs_stop_sequence: stop_time.stop_sequence,
            };

            stop_diffs.push(stop_diff);
        }

        let stop_indicies_requiring_interpolation: Vec<usize> = stop_diffs
            .iter()
            .enumerate()
            .filter(|(_, stop_diff)| {
                stop_diff.arrival_time_since_start.is_none()
                    && stop_diff.departure_time_since_start.is_none()
            })
            .map(|(index, _)| index)
            .collect();

            let mut ranges: Vec<Vec<usize>> = Vec::new();

        if stop_indicies_requiring_interpolation.len() > 0 {
            //group into ranges of consecutive indicies
            

            let mut current_range: Vec<usize> = Vec::new();

            for stop_indice in stop_indicies_requiring_interpolation {
                if current_range.len() == 0 {
                    current_range.push(stop_indice);
                } else {
                    if current_range.last().unwrap() + 1 == stop_indice {
                        current_range.push(stop_indice);
                    } else {
                        ranges.push(current_range);
                        current_range = vec![stop_indice];
                    }
                }
            }
        }

        let new_interpolated_times:AHashMap<usize, i32> = {
            //interpolate times

            let mut interpolated_times: AHashMap<usize, i32> = AHashMap::new();

            for range in ranges {
                let start_index = range[0];
                let end_index = range[range.len() - 1];

                if start_index == 0 || end_index == stop_diffs.len() - 1 {
                    println!("Invalid range for interpolation");
                    continue;
                }

                let start_time = match stop_diffs[start_index - 1].departure_time_since_start {
                    Some(time) => Some(time),
                    None => stop_diffs[start_index - 1].arrival_time_since_start
                };

                let end_time = match stop_diffs[end_index + 1].arrival_time_since_start {
                    Some(time) => Some(time),
                    None => stop_diffs[end_index + 1].departure_time_since_start
                };

                if start_time.is_none() || end_time.is_none() {
                    println!("Invalid start or end time for interpolation");
                    continue;
                }

                let start_time = start_time.unwrap();
                let end_time = end_time.unwrap();

                let time_difference = end_time - start_time;
                let number_of_stops = end_index - start_index + 1;

                let time_difference_per_stop = time_difference / number_of_stops as i32;

                //assume basic time interpolation for now
                for (index, stop_index) in range.iter().enumerate() {
                    interpolated_times.insert(
                        *stop_index,
                        start_time + time_difference_per_stop * (index as i32 + 1),
                    );
                }
            }

            interpolated_times
        };

        //apply the interpolations back to the stop_diffs
        for (index, stop_diff) in stop_diffs.iter_mut().enumerate() {
            if let Some(interpolated_time) = new_interpolated_times.get(&index) {
                stop_diff.interpolated_time_since_start = Some(*interpolated_time);
                stop_diff.interpolation_by_catenary = true;
            }
        }

        let stated_timezone = match gtfs.agencies.len() {
            0 => None,
            1 => Some(gtfs.agencies[0].timezone.clone()),
            _ => {
                let route = gtfs.routes.get(&trip.route_id);

                match route {
                    Some(route) => {
                        let matching_agency = gtfs
                            .agencies
                            .iter()
                            .find(|agency| agency.id == route.agency_id);

                        matching_agency.map(|agency| agency.timezone.clone())
                    }
                    None => None,
                }
            }
        };

        let timezone = match stated_timezone {
            Some(timezone) => timezone,
            None => {
                let first_stop = &gtfs.stops[&trip.stop_times[0].stop.id];

                match (first_stop.longitude, first_stop.latitude) {
                    (Some(long), Some(lat)) => String::from(FINDER.get_tz_name(long, lat)),
                    _ => {
                        println!("Couldn't find timezone for trip {}", trip_id);
                        String::from("Etc/UTC")
                    }
                }
            }
        };

        let trip_headsign_calculated = match &trip.trip_headsign {
            Some(x) => Some(
                x.clone()
                    .replace("-Funded in part by/SB County Measure A", ""),
            ),
            None => {
                let stop_headsigns: Vec<Option<String>> = stop_diffs
                    .iter()
                    .map(|x| x.stop_headsign.clone())
                    .unique()
                    .collect();

                match stop_headsigns.len() {
                    0 => match stop_diffs.last() {
                        Some(last_stop) => match gtfs.stops.get(&last_stop.stop_id) {
                            // fallback to stop name if neither trip headsign nor stop headsign is available
                            Some(stop) => stop.name.clone(),
                            None => None,
                        },
                        None => None,
                    },
                    1 => stop_headsigns[0].clone(),
                    _ => None,
                }
            }
        };

        let itinerary_cover = ItineraryCover {
            stop_sequences: stop_diffs,
            direction_id: trip.direction_id.map(|direction| match direction {
                DirectionType::Outbound => false,
                DirectionType::Inbound => true,
            }),
            route_id: trip.route_id.clone(),
            trip_headsign: trip_headsign_calculated,
            timezone,
            shape_id: trip.shape_id.clone(),
        };

        //itinerary id generated
        let hash_of_itinerary = fast_hash(&itinerary_cover);

        itineraries.insert(hash_of_itinerary, itinerary_cover);
        trips_to_itineraries.insert(trip_id.clone(), hash_of_itinerary);

        let trip_under_itinerary = TripUnderItinerary {
            trip_id: trip_id.clone(),
            trip_short_name: trip.trip_short_name.clone(),
            start_time,
            service_id: trip.service_id.clone(),
            wheelchair_accessible: availability_to_int(&trip.wheelchair_accessible),
            block_id: trip.block_id.clone(),
            bikes_allowed: bikes_allowed_to_int(&trip.bikes_allowed),
            frequencies: trip.frequencies.clone(),
            route_id: trip.route_id.clone(),
        };

        itineraries_to_trips
            .entry(hash_of_itinerary)
            .and_modify(|existing_trips| {
                existing_trips.push(trip_under_itinerary.clone());
            })
            .or_insert(vec![trip_under_itinerary]);
    }

    //calculate direction patterns

    let mut direction_patterns: AHashMap<u64, DirectionPattern> = AHashMap::new();

    for (itinerary_id, itinerary) in &itineraries {
        let mut stop_sequence: Vec<String> = Vec::new();

        for stop_diff in &itinerary.stop_sequences {
            stop_sequence.push(stop_diff.stop_id.clone());
        }

        let direction_pattern = DirectionPattern {
            direction_id: itinerary.direction_id,
            stop_sequence,
            headsign_or_destination: match itinerary.trip_headsign.clone() {
                Some(headsign) => Some(headsign),
                None => match itinerary.stop_sequences.last() {
                    Some(last_stop) => match gtfs.stops.get(&last_stop.stop_id) {
                        Some(stop) => stop.name.clone(),
                        None => None,
                    },
                    None => None,
                },
            },
            gtfs_shape_id: itinerary.shape_id.clone(),
            route_id: itinerary.route_id.clone(),
        };

        let hash_of_direction_pattern = fast_hash(&direction_pattern.stop_sequence);

        direction_patterns.insert(hash_of_direction_pattern, direction_pattern);
    }

    ResponseFromReduce {
        itineraries,
        trips_to_itineraries,
        itineraries_to_trips,
        direction_patterns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}

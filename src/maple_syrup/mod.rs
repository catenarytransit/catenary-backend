// GTFS stop time compression algorithm
// Compatible with transfer patterns

use crate::ahash_fast_hash;
use crate::enum_to_int::*;
use ahash::AHashMap;
use ahash::AHashSet;
use compact_str::CompactString;
use gtfs_structures::DirectionType;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::hash::Hash;
use tzf_rs::DefaultFinder;
pub mod service_optimisation;

lazy_static! {
    static ref FINDER: DefaultFinder = DefaultFinder::new();
}

#[derive(Hash, Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ItineraryCover {
    pub stop_sequences: Vec<StopDifference>,
    //map 0 to false and 1 to true
    pub direction_id: Option<bool>,
    pub route_id: CompactString,
    pub trip_headsign: Option<String>,
    pub timezone: String,
    pub shape_id: Option<String>,
    pub direction_pattern_id: u64,
    pub direction_pattern_id_with_parents: u64,
    pub route_type: i16,
    pub stop_headsigns: Option<String>,
    pub stop_headsigns_unique_list: Option<Vec<String>>,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StopDifference {
    pub stop_id: CompactString,
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
    pub gtfs_stop_sequence: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct DirectionPattern {
    pub direction_id: Option<bool>,
    pub stop_sequence: Vec<CompactString>,
    pub headsign_or_destination: Option<String>,
    pub gtfs_shape_id: Option<String>,
    pub route_id: CompactString,
    pub route_type: i16,
    pub stop_headsigns: Option<String>,
    pub stop_headsigns_unique_list: Option<Vec<String>>,
    pub direction_pattern_id_with_parents: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TripUnderItinerary {
    pub trip_id: CompactString,
    pub start_time: u32,
    pub service_id: CompactString,
    pub wheelchair_accessible: i16,
    pub block_id: Option<String>,
    pub bikes_allowed: i16,
    pub frequencies: Vec<gtfs_structures::Frequency>,
    pub trip_short_name: Option<CompactString>,
    pub route_id: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResponseFromReduce {
    pub itineraries: AHashMap<u64, ItineraryCover>,
    pub trips_to_itineraries: AHashMap<CompactString, u64>,
    pub itineraries_to_trips: AHashMap<u64, Vec<TripUnderItinerary>>,
    pub direction_patterns: AHashMap<u64, DirectionPattern>,
    pub direction_pattern_id_to_itineraries: AHashMap<u64, AHashSet<u64>>,
}

pub fn reduce(gtfs: &gtfs_structures::Gtfs) -> ResponseFromReduce {
    let mut itineraries: AHashMap<u64, ItineraryCover> = AHashMap::new();
    let mut trips_to_itineraries: AHashMap<CompactString, u64> = AHashMap::new();
    let mut itineraries_to_trips: AHashMap<u64, Vec<TripUnderItinerary>> = AHashMap::new();
    let mut direction_pattern_id_to_itineraries: AHashMap<u64, AHashSet<u64>> = AHashMap::new();

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
                stop_id: (&stop_time.stop.id).into(),
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

        stop_diffs.sort_by(|a, b| a.gtfs_stop_sequence.cmp(&b.gtfs_stop_sequence));

        stop_diffs.dedup();

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

        let stop_headsigns_unique_list: Vec<String> = stop_diffs
            .iter()
            .map(|x| {
                x.stop_headsign.clone().map(|headsigntxt| {
                    headsigntxt.replace("-Funded in part by/SB County Measure A", "")
                })
            })
            .unique()
            .flatten()
            .collect();

        let stop_headsign_reference_idx = if stop_headsigns_unique_list.len() <= 1 {
            None
        } else {
            Some(
                stop_diffs
                    .iter()
                    .map(|stop_time_entry| {
                        stop_time_entry
                            .stop_headsign
                            .as_ref()
                            .map(|this_stop_entry_headsign| {
                                stop_headsigns_unique_list
                                    .iter()
                                    .position(|x| x == this_stop_entry_headsign)
                            })
                    })
                    .flatten()
                    .collect::<Vec<Option<usize>>>(),
            )
        };

        let trip_headsign_calculated = match &trip.trip_headsign {
            Some(x) => Some(
                x.clone()
                    .replace("-Funded in part by/SB County Measure A", ""),
            ),
            None => {
                match stop_headsigns_unique_list.len() {
                    0 => match stop_diffs.last() {
                        Some(last_stop) => match gtfs.stops.get(last_stop.stop_id.as_str()) {
                            // fallback to stop name if neither trip headsign nor stop headsign is available
                            Some(stop) => stop.name.clone(),
                            None => None,
                        },
                        None => None,
                    },
                    1 => Some(stop_headsigns_unique_list[0].clone()),
                    _ => {
                        //merge all stop headsigns into one
                        Some(stop_headsigns_unique_list.iter().join(" | "))
                    }
                }
            }
        };

        //calculate direction pattern reference

        let direction_pattern_id = calculate_direction_pattern_id(
            &trip.route_id,
            stop_diffs
                .iter()
                .map(|x| x.stop_id.clone())
                .collect::<Vec<CompactString>>(),
        );

        let direction_pattern_id_with_parents = calculate_direction_pattern_id(
            &trip.route_id,
            stop_diffs
                .iter()
                .map(|x| match gtfs.stops.get(x.stop_id.as_str()) {
                    Some(stop) => match &stop.parent_station {
                        Some(parent_station) => CompactString::from(parent_station),
                        None => x.stop_id.clone(),
                    },
                    None => x.stop_id.clone(),
                })
                .collect::<Vec<CompactString>>(),
        );

        let itinerary_cover = ItineraryCover {
            stop_sequences: stop_diffs,
            direction_id: trip.direction_id.map(|direction| match direction {
                DirectionType::Outbound => false,
                DirectionType::Inbound => true,
            }),
            route_id: (&trip.route_id).into(),
            trip_headsign: trip_headsign_calculated,
            stop_headsigns: match stop_headsigns_unique_list.len() {
                0 => None,
                1 => None,
                _ => Some(stop_headsigns_unique_list.join(" | ")),
            },
            stop_headsigns_unique_list: match stop_headsigns_unique_list.len() {
                0 => None,
                1 => None,
                _ => Some(stop_headsigns_unique_list),
            },
            timezone,
            shape_id: trip.shape_id.clone(),
            direction_pattern_id,
            direction_pattern_id_with_parents,
            route_type: crate::enum_to_int::route_type_to_int(
                &gtfs
                    .routes
                    .get(&trip.route_id)
                    .map(|route| route.route_type)
                    .unwrap_or(gtfs_structures::RouteType::Bus),
            ),
        };

        //itinerary id generated
        let hash_of_itinerary = ahash_fast_hash(&itinerary_cover);

        itineraries.insert(hash_of_itinerary, itinerary_cover);
        trips_to_itineraries.insert(trip_id.into(), hash_of_itinerary);

        let trip_under_itinerary = TripUnderItinerary {
            trip_id: trip_id.into(),
            trip_short_name: trip.trip_short_name.as_ref().map(|x| x.into()),
            start_time,
            service_id: (&trip.service_id).into(),
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

    let mut direction_patterns: AHashMap<u64, DirectionPattern> = AHashMap::new();

    for (itinerary_id, itinerary) in &itineraries {
        let mut stop_sequence: Vec<CompactString> = Vec::new();

        for stop_diff in &itinerary.stop_sequences {
            stop_sequence.push(stop_diff.stop_id.clone());
        }

        let direction_pattern = DirectionPattern {
            direction_id: itinerary.direction_id,
            direction_pattern_id_with_parents: itinerary.direction_pattern_id_with_parents,
            stop_sequence,
            stop_headsigns_unique_list: itinerary.stop_headsigns_unique_list.clone(),
            stop_headsigns: itinerary.stop_headsigns.clone(),
            headsign_or_destination: match itinerary.trip_headsign.clone() {
                Some(headsign) => Some(headsign),
                None => match itinerary.stop_sequences.last() {
                    Some(last_stop) => match gtfs.stops.get(last_stop.stop_id.as_str()) {
                        Some(stop) => stop.name.clone(),
                        None => None,
                    },
                    None => None,
                },
            },
            gtfs_shape_id: itinerary.shape_id.clone(),
            route_id: itinerary.route_id.clone(),
            route_type: itinerary.route_type,
        };

        let hash_of_direction_pattern_output = calculate_direction_pattern_id(
            &direction_pattern.route_id,
            direction_pattern.stop_sequence.to_vec(),
        );

        direction_patterns.insert(hash_of_direction_pattern_output, direction_pattern);
        direction_pattern_id_to_itineraries
            .entry(hash_of_direction_pattern_output)
            .and_modify(|x| {
                x.insert(*itinerary_id);
            })
            .or_insert(AHashSet::from_iter([*itinerary_id]));
    }

    let itin_list = itineraries.keys().cloned().collect::<Vec<u64>>();

    for itinerary_id in itin_list {
        match itineraries.entry(itinerary_id) {
            Occupied(mut entry) => {
                let mut itinerary = entry.get_mut();

                //interpolate times for stops that don't have times
                let stop_indicies_requiring_interpolation: Vec<usize> = itinerary
                    .stop_sequences
                    .iter()
                    .enumerate()
                    .filter(|(_, stop_diff)| {
                        stop_diff.arrival_time_since_start.is_none()
                            && stop_diff.departure_time_since_start.is_none()
                    })
                    .map(|(index, _)| index)
                    .collect();

                let mut ranges = Vec::new();

                if !stop_indicies_requiring_interpolation.is_empty() {
                    //group into ranges of consecutive indicies

                    let mut current_range: Vec<usize> = Vec::new();

                    for stop_indice in stop_indicies_requiring_interpolation {
                        if current_range.is_empty() {
                            current_range.push(stop_indice);
                        } else if current_range.last().unwrap() + 1 == stop_indice {
                            current_range.push(stop_indice);
                        } else {
                            ranges.push(current_range);
                            current_range = vec![stop_indice];
                        }
                    }

                    if !current_range.is_empty() {
                        ranges.push(current_range);
                        current_range = vec![];
                    }
                }

                let new_interpolated_times: AHashMap<usize, i32> = {
                    //interpolate times

                    let mut interpolated_times: AHashMap<usize, i32> = AHashMap::new();

                    for range in ranges {
                        let start_index = range[0];
                        let end_index = range[range.len() - 1];

                        if start_index == 0 || end_index == itinerary.stop_sequences.len() - 1 {
                            println!("Invalid range for interpolation");
                            continue;
                        }

                        let start_time = match itinerary.stop_sequences[start_index - 1]
                            .departure_time_since_start
                        {
                            Some(time) => Some(time),
                            None => {
                                itinerary.stop_sequences[start_index - 1].arrival_time_since_start
                            }
                        };

                        let end_time = match itinerary.stop_sequences[end_index + 1]
                            .arrival_time_since_start
                        {
                            Some(time) => Some(time),
                            None => {
                                itinerary.stop_sequences[end_index + 1].departure_time_since_start
                            }
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
                for (index, stop_diff) in itinerary.stop_sequences.iter_mut().enumerate() {
                    if let Some(interpolated_time) = new_interpolated_times.get(&index) {
                        stop_diff.interpolated_time_since_start = Some(*interpolated_time);
                        stop_diff.interpolation_by_catenary = true;
                    }
                }
            }
            Vacant(_) => {
                println!("Vacant entry in itineraries");
            }
        }
    }

    ResponseFromReduce {
        itineraries,
        trips_to_itineraries,
        itineraries_to_trips,
        direction_patterns,
        direction_pattern_id_to_itineraries,
    }
}

fn calculate_direction_pattern_id(route_id: &str, stop_sequence: Vec<CompactString>) -> u64 {
    let mut hash_of_direction_pattern_temp: Vec<String> = Vec::new();

    hash_of_direction_pattern_temp.push(route_id.into());

    for stop_id in stop_sequence {
        hash_of_direction_pattern_temp.push(stop_id.into());
    }

    let convert_to_bytes =
        bincode::encode_to_vec(&hash_of_direction_pattern_temp, bincode::config::standard())
            .unwrap();

    seahash::hash(&convert_to_bytes)
}

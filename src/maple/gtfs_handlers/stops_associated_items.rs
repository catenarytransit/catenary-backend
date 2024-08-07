use catenary::enum_to_int::route_type_to_int;
use std::collections::{HashMap, HashSet};

pub fn make_hashmap_stops_to_route_types_and_ids(
    gtfs: &gtfs_structures::Gtfs,
) -> (
    HashMap<String, HashSet<i16>>,
    HashMap<String, HashSet<String>>,
) {
    let mut stop_to_route_types: HashMap<String, HashSet<i16>> = HashMap::new();
    let mut stop_to_route_ids: HashMap<String, HashSet<String>> = HashMap::new();

    for (trip_id, trip) in &gtfs.trips {
        for stoptime in &trip.stop_times {
            if let Ok(route) = gtfs.get_route(&trip.route_id) {
                let route_type_num = route_type_to_int(&route.route_type);

                stop_to_route_types
                    .entry(stoptime.stop.id.to_owned())
                    .and_modify(|types| {
                        types.insert(route_type_num);
                    })
                    .or_insert(HashSet::from([route_type_num]));

                stop_to_route_ids
                    .entry(stoptime.stop.id.to_owned())
                    .and_modify(|ids| {
                        ids.insert(route.id.to_owned());
                    })
                    .or_insert(HashSet::from([route.id.to_owned()]));
            }
        }
    }
    (stop_to_route_types, stop_to_route_ids)
}

//returns (stop_id_to_children_ids, stop_ids_to_children_route_types)
pub fn make_hashmaps_of_children_stop_info(
    gtfs: &gtfs_structures::Gtfs,
    stop_to_route_types: &HashMap<String, HashSet<i16>>,
) -> (
    HashMap<String, HashSet<String>>,
    HashMap<String, HashSet<i16>>,
) {
    let mut stop_id_to_children_ids: HashMap<String, HashSet<String>> = HashMap::new();
    let mut stop_ids_to_children_route_types: HashMap<String, HashSet<i16>> = HashMap::new();

    for (stop_id, stop) in &gtfs.stops {
        if stop.parent_station.is_some() {
            stop_id_to_children_ids
                .entry(stop.parent_station.as_ref().unwrap().to_owned())
                .and_modify(|children_ids| {
                    children_ids.insert(stop_id.to_owned());
                })
                .or_insert(HashSet::from([stop_id.to_owned()]));

            let route_types_for_this_stop = stop_to_route_types.get(stop_id);

            if route_types_for_this_stop.is_some() {
                stop_ids_to_children_route_types
                    .entry(stop.parent_station.as_ref().unwrap().to_owned())
                    .and_modify(|children_route_types| {
                        children_route_types.extend(route_types_for_this_stop.unwrap());
                    })
                    .or_insert(route_types_for_this_stop.unwrap().to_owned());
            }
        }
    }

    (stop_id_to_children_ids, stop_ids_to_children_route_types)
}

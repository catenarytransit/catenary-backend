use catenary::enum_to_int::route_type_to_int;
use std::collections::HashMap;

fn push_unique_i16(vec: &mut Vec<i16>, value: i16) {
    if !vec.contains(&value) {
        vec.push(value);
    }
}

fn push_unique_string(vec: &mut Vec<String>, value: &str) {
    if !vec.iter().any(|existing| existing == value) {
        vec.push(value.to_owned());
    }
}

pub fn make_hashmap_stops_to_route_types_and_ids(
    gtfs: &gtfs_structures::Gtfs,
) -> (
    HashMap<String, Vec<i16>>,
    HashMap<String, Vec<String>>,
) {
    let mut stop_to_route_types: HashMap<String, Vec<i16>> = HashMap::with_capacity(gtfs.stops.len());
    let mut stop_to_route_ids: HashMap<String, Vec<String>> = HashMap::with_capacity(gtfs.stops.len());

    for trip in gtfs.trips.values() {
        let Ok(route) = gtfs.get_route(&trip.route_id) else {
            continue;
        };

        let route_type_num = route_type_to_int(&route.route_type);
        let route_id = route.id.as_str();

        for stoptime in &trip.stop_times {
            let stop_id = stoptime.stop.id.as_str();

            if let Some(route_types) = stop_to_route_types.get_mut(stop_id) {
                push_unique_i16(route_types, route_type_num);
            } else {
                stop_to_route_types.insert(stop_id.to_owned(), vec![route_type_num]);
            }

            if let Some(route_ids) = stop_to_route_ids.get_mut(stop_id) {
                push_unique_string(route_ids, route_id);
            } else {
                stop_to_route_ids.insert(stop_id.to_owned(), vec![route_id.to_owned()]);
            }
        }
    }

    (stop_to_route_types, stop_to_route_ids)
}

pub fn make_hashmaps_of_children_stop_info(
    gtfs: &gtfs_structures::Gtfs,
) -> HashMap<String, Vec<String>> {
    let mut stop_id_to_children_ids: HashMap<String, Vec<String>> = HashMap::new();

    for (stop_id, stop) in &gtfs.stops {
        if let Some(parent_station) = stop.parent_station.as_ref() {
            if let Some(children_ids) = stop_id_to_children_ids.get_mut(parent_station.as_str()) {
                children_ids.push(stop_id.to_owned());
            } else {
                stop_id_to_children_ids.insert(parent_station.to_owned(), vec![stop_id.to_owned()]);
            }
        }
    }

    stop_id_to_children_ids
}

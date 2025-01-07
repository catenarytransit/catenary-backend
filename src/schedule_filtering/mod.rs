use std::collections::BTreeSet;
use std::collections::BTreeMap;
use gtfs_structures::*;
use std::collections::HashMap;

pub fn include_only_route_types(gtfs: Gtfs, route_types: Vec<gtfs_structures::RouteType>) -> Gtfs {
    let mut gtfs = gtfs;

    let route_ids_to_keep: BTreeSet<String> = gtfs
        .routes
        .iter()
        .filter(|(route_id, route)| route_types.contains(&route.route_type))
        .map(|(route_id, route)| route_id)
        .cloned()
        .collect();

    let trips_to_keep: BTreeSet<String> = gtfs
        .trips
        .iter()
        .filter(|(trip_id, trip)| route_ids_to_keep.contains(&trip.route_id))
        .map(|(trip_id, trip)| trip_id)
        .cloned()
        .collect();

    let mut keep_stop_ids: BTreeSet<String> = BTreeSet::new();

    let mut keep_shapes: BTreeSet<String> = BTreeSet::new();

    for trip_id in &trips_to_keep {
        let trip = gtfs.trips.get(trip_id).unwrap();

        for stop_time in &trip.stop_times {
            keep_stop_ids.insert(stop_time.stop.id.clone());
        }

        if let Some(shape_id) = &trip.shape_id {
            keep_shapes.insert(shape_id.clone());
        }
    }

    // remove data that are not needed

    gtfs.routes = gtfs
        .routes
        .into_iter()
        .filter(|(route_id, route)| route_ids_to_keep.contains(route_id))
        .collect();

    gtfs.trips = gtfs
        .trips
        .into_iter()
        .filter(|(trip_id, trip)| trips_to_keep.contains(trip_id))
        .collect();

    gtfs.stops = gtfs
        .stops
        .into_iter()
        .filter(|(stop_id, stop)| keep_stop_ids.contains(stop_id))
        .collect();

    gtfs.shapes = gtfs
        .shapes
        .into_iter()
        .filter(|(shape_id, shape)| keep_shapes.contains(shape_id))
        .collect();

    gtfs
}

fn minimum_day_filter(gtfs: Gtfs, naive_date: chrono::NaiveDate) -> Gtfs {
    let mut gtfs = gtfs;

    let mut throwout_calendar_list = BTreeSet::new();
    let mut routes_to_trips: HashMap<String, Vec<String>> = HashMap::new();
    let mut trips_removed: BTreeSet<String> = BTreeSet::new();

    for (calendar_id, calendar) in &gtfs.calendar {
        if calendar.end_date < naive_date {
            let contains_any_exceptions_greater_than_or_equal_to_date = 
            match gtfs.calendar_dates.get(calendar_id) {
                Some(calendar_dates) => {
                    calendar_dates.iter().any(|calendar_date| calendar_date.date >= naive_date)
                },
                None => false
            };

            if !contains_any_exceptions_greater_than_or_equal_to_date {
                throwout_calendar_list.insert(calendar_id.clone());
            }
        }
    }

    for (trip_id, trip) in &gtfs.trips {
        routes_to_trips.entry(trip.route_id.clone())
        .and_modify(|x| x.push(trip_id.clone()))
        .or_insert(vec![trip_id.clone()]);

        if throwout_calendar_list.contains(&trip.service_id) {
            trips_removed.insert(trip_id.clone());
        }
    }
    
    let mut throwout_routes_list: BTreeSet<String> = BTreeSet::new();

    for (route_id, trip_ids) in routes_to_trips {
        let mark_for_deletion = trip_ids.iter().all(|x| trips_removed.contains(x));

        if mark_for_deletion {
            throwout_routes_list.insert(route_id.clone());
        }
    }

    for route_id in throwout_routes_list {
        gtfs.routes.remove(&route_id);
    }

    for trip_id in trips_removed {
        gtfs.trips.remove(&trip_id);
    }

    for calendar in &throwout_calendar_list {
        gtfs.calendar.remove(calendar);
        gtfs.calendar_dates.remove(calendar);
    }

    gtfs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn filter_ttc() {
        let gtfs = gtfs_structures::Gtfs::from_url_async("https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7795b45e-e65a-4465-81fc-c36b9dfff169/resource/cfb6b2b8-6191-41e3-bda1-b175c51148cb/download/TTC%20Routes%20and%20Schedules%20Data.zip").await.unwrap();

        println!("starts with");
        gtfs.print_stats();

        let gtfs = include_only_route_types(gtfs, vec![gtfs_structures::RouteType::Subway]);

        println!("ends with");
        gtfs.print_stats();
    }
}

use std::collections::BTreeSet;

use gtfs_structures::*;

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
        let final_stats = gtfs.print_stats();
    }
}
use ahash::AHashMap;
use chrono::prelude::*;
use gtfs_structures::*;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

pub fn include_only_route_types(
    gtfs: Gtfs,
    route_types: Vec<gtfs_structures::RouteType>,
    delete_shapes_and_stops: bool,
) -> Gtfs {
    let mut gtfs = gtfs;

    let route_ids_to_keep: BTreeSet<String> = gtfs
        .routes
        .iter()
        .filter(|(route_id, route)| route_types.contains(&route.route_type))
        .map(|(route_id, route)| route_id)
        .cloned()
        .collect();

    println!(
        "keeping {} routes, removing {} routes",
        route_ids_to_keep.len(),
        gtfs.routes.len() - route_ids_to_keep.len()
    );

    let trips_to_keep: BTreeSet<String> = gtfs
        .trips
        .iter()
        .filter(|(trip_id, trip)| route_ids_to_keep.contains(&trip.route_id))
        .map(|(trip_id, trip)| trip_id)
        .cloned()
        .collect();

    println!(
        "keeping {} trips, removing {} trips",
        trips_to_keep.len(),
        gtfs.trips.len() - trips_to_keep.len()
    );

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

    let mut stop_ids_to_add = BTreeSet::new();

    for stop_id in &keep_stop_ids {
        if let Some(stop) = gtfs.stops.get(stop_id) {
            if let Some(parent_station_id) = &stop.parent_station {
                stop_ids_to_add.insert(parent_station_id.clone());

                if let Some(parent_stop) = gtfs.stops.get(parent_station_id) {
                    if let Some(grandparent_station_id) = &parent_stop.parent_station {
                        stop_ids_to_add.insert(grandparent_station_id.clone());
                    }
                }
            }
        }
    }

    keep_stop_ids.extend(stop_ids_to_add);

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

    if (delete_shapes_and_stops) {
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
    }

    gtfs
}

pub fn minimum_day_filter(gtfs: Gtfs, naive_date: chrono::NaiveDate) -> Gtfs {
    let mut gtfs = gtfs;
    let mut throwout_calendar_list = BTreeSet::new();

    let all_service_ids: BTreeSet<_> = gtfs
        .calendar
        .keys()
        .chain(gtfs.calendar_dates.keys())
        .cloned()
        .collect();

    for service_id in all_service_ids {
        let calendar = gtfs.calendar.get(&service_id);
        let calendar_dates = gtfs.calendar_dates.get(&service_id);
        let mut is_active = false;

        if let Some(cal) = calendar {
            if cal.end_date >= naive_date {
                let start_date = std::cmp::max(cal.start_date, naive_date);
                let mut current_date = start_date;

                while current_date <= cal.end_date {
                    let weekday = current_date.weekday();
                    let scheduled = match weekday {
                        chrono::Weekday::Mon => cal.monday,
                        chrono::Weekday::Tue => cal.tuesday,
                        chrono::Weekday::Wed => cal.wednesday,
                        chrono::Weekday::Thu => cal.thursday,
                        chrono::Weekday::Fri => cal.friday,
                        chrono::Weekday::Sat => cal.saturday,
                        chrono::Weekday::Sun => cal.sunday,
                    };

                    if scheduled {
                        let removed = calendar_dates
                            .map(|dates| {
                                dates.iter().any(|d| {
                                    d.date == current_date && d.exception_type == Exception::Deleted
                                })
                            })
                            .unwrap_or(false);

                        if !removed {
                            is_active = true;
                            break;
                        }
                    }
                    current_date = current_date.succ_opt().unwrap();
                }
            }

            if let Some(dates) = calendar_dates {
                if dates
                    .iter()
                    .any(|d| d.exception_type == Exception::Added && d.date >= naive_date)
                {
                    is_active = true;
                }
            }
        } else if let Some(dates) = calendar_dates {
            if dates
                .iter()
                .any(|d| d.exception_type == Exception::Added && d.date >= naive_date)
            {
                is_active = true;
            }
        }

        if !is_active {
            throwout_calendar_list.insert(service_id);
        }
    }

    let mut trips_removed: BTreeSet<String> = gtfs
        .trips
        .iter()
        .filter(|(_, trip)| throwout_calendar_list.contains(&trip.service_id))
        .map(|(id, _)| id.clone())
        .collect();

    let mut routes_to_remove = BTreeSet::new();
    let mut route_trip_counts: AHashMap<String, usize> = AHashMap::new();

    for (trip_id, trip) in &gtfs.trips {
        *route_trip_counts.entry(trip.route_id.clone()).or_insert(0) += 1;
    }

    for (trip_id, trip) in &gtfs.trips {
        if trips_removed.contains(trip_id) {
            let count = route_trip_counts.get_mut(&trip.route_id).unwrap();
            *count -= 1;

            if *count == 0 {
                routes_to_remove.insert(trip.route_id.clone());
            }
        }
    }

    let mut shapes_to_remove = BTreeSet::new();
    let mut shape_trip_counts: AHashMap<String, usize> = AHashMap::new();

    for (_, trip) in &gtfs.trips {
        if let Some(shape_id) = &trip.shape_id {
            *shape_trip_counts.entry(shape_id.clone()).or_insert(0) += 1;
        }
    }

    for (trip_id, trip) in &gtfs.trips {
        if trips_removed.contains(trip_id) {
            if let Some(shape_id) = &trip.shape_id {
                let count = shape_trip_counts.get_mut(shape_id).unwrap();
                *count -= 1;

                if *count == 0 {
                    shapes_to_remove.insert(shape_id.clone());
                }
            }
        }
    }

    gtfs.calendar
        .retain(|id, _| !throwout_calendar_list.contains(id));
    gtfs.calendar_dates
        .retain(|id, _| !throwout_calendar_list.contains(id));
    gtfs.trips.retain(|id, _| !trips_removed.contains(id));
    gtfs.routes.retain(|id, _| !routes_to_remove.contains(id));
    gtfs.shapes.retain(|id, _| !shapes_to_remove.contains(id));

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

        let gtfs = include_only_route_types(gtfs, vec![gtfs_structures::RouteType::Subway], true);

        println!("ends with");
        gtfs.print_stats();
    }

    #[tokio::test]
    async fn filter_amtrak() {
        let now = chrono::Utc::now();

        let today = chrono::NaiveDate::from_ymd(now.year(), now.month(), now.day());

        let gtfs = gtfs_structures::Gtfs::from_url_async(
            "https://content.amtrak.com/content/gtfs/GTFS.zip",
        )
        .await
        .unwrap();

        println!("amtk starts with");
        gtfs.print_stats();

        let gtfs = minimum_day_filter(gtfs, today - chrono::Duration::days(10));

        println!("amtk ends with");
        gtfs.print_stats();
    }
}

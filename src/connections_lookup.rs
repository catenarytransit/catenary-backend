use crate::postgres_tools::CatenaryPostgresPool;
use crate::schema::gtfs::routes as routes_pg_schema;
use crate::schema::gtfs::stops as stops_pg_schema;
use ahash::AHashMap;
use ahash::AHashSet;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel::dsl::sql;
use diesel::sql_types::Bool;
use diesel_async::RunQueryDsl;
use futures::future::join_all;
use geo::HaversineDistance;
use geo::Point;
use geo::coord;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;

// degrees
const TRANSFER_SEARCH_RADIUS_DEGREES: f64 = 0.006;
const MAX_CLUSTER_EXTENT: f64 = 0.02;

pub struct ConnectionsInfo {
    pub connecting_routes: BTreeMap<String, BTreeMap<String, crate::models::Route>>, // chateau -> route_id -> Route
    pub connections_per_stop: BTreeMap<String, BTreeMap<String, Vec<String>>>, // stop_id -> chateau -> route_ids
}

pub async fn connections_lookup(
    input_chateau: &str,
    input_route: &str,
    input_route_type: i16,
    stop_positions: Vec<(String, f64, f64)>,
    additional_routes_to_lookup: BTreeMap<String, BTreeSet<String>>,
    known_transfers_same_stop: Option<AHashMap<String, Vec<String>>>,
    pool: Arc<CatenaryPostgresPool>,
) -> ConnectionsInfo {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    let start_timer = std::time::Instant::now();

    let mut additional_routes_to_lookup = additional_routes_to_lookup;

    // Fetch metadata for connecting routes
    let mut response_connecting_routes: BTreeMap<String, BTreeMap<String, crate::models::Route>> =
        BTreeMap::new();

    let conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    > = &mut conn_pre.unwrap();

    // base stops for this trip, by stop_id
    let base_stop_ids: ahash::AHashSet<String> = stop_positions
        .iter()
        .map(|(stop_id, _, _)| stop_id.clone())
        .collect();

    // Will hold: stop_id -> chateau -> route_ids
    let mut connections_per_stop: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();

    if !stop_positions.is_empty() {
        let pool_for_transfers = pool.clone();

        // Group stops into clusters to reduce DB queries
        let mut clusters: Vec<Vec<(String, f64, f64)>> = Vec::new();
        let mut current_cluster: Vec<(String, f64, f64)> = Vec::new();
        let mut cluster_bounds: Option<(f64, f64, f64, f64)> = None; // min_lon, min_lat, max_lon, max_lat

        for stop in stop_positions {
            let (ref _id, lat, lon) = stop;

            match cluster_bounds {
                None => {
                    current_cluster.push(stop.clone());
                    cluster_bounds = Some((lon, lat, lon, lat));
                }
                Some((min_lon, min_lat, max_lon, max_lat)) => {
                    let new_min_lon = min_lon.min(lon);
                    let new_min_lat = min_lat.min(lat);
                    let new_max_lon = max_lon.max(lon);
                    let new_max_lat = max_lat.max(lat);

                    if (new_max_lon - new_min_lon) < MAX_CLUSTER_EXTENT
                        && (new_max_lat - new_min_lat) < MAX_CLUSTER_EXTENT
                    {
                        current_cluster.push(stop.clone());
                        cluster_bounds = Some((new_min_lon, new_min_lat, new_max_lon, new_max_lat));
                    } else {
                        clusters.push(current_cluster);
                        current_cluster = vec![stop.clone()];
                        cluster_bounds = Some((lon, lat, lon, lat));
                    }
                }
            }
        }
        if !current_cluster.is_empty() {
            clusters.push(current_cluster);
        }

        let transfer_futures = clusters.into_iter().map(|cluster| {
            let pool_for_transfers = pool_for_transfers.clone();

            async move {
                let conn_pre = pool_for_transfers.get().await;
                if let Err(e) = &conn_pre {
                    eprintln!(
                        "error getting pool connection for transfers (trip endpoint): {}",
                        e
                    );
                    return Vec::<(String, f64, f64, crate::models::Stop)>::new();
                }

                let mut conn = conn_pre.unwrap();

                let points_str = cluster
                    .iter()
                    .map(|(_, lat, lon)| format!("{} {}", lon, lat))
                    .collect::<Vec<_>>()
                    .join(",");
                let multipoint_wkt = format!("SRID=4326;MULTIPOINT({})", points_str);

                let where_query_for_stops = format!(
                    "ST_DWithin(gtfs.stops.point, '{}', {}) \
                         AND allowed_spatial_query = TRUE",
                    multipoint_wkt, TRANSFER_SEARCH_RADIUS_DEGREES
                );

                let nearby: Vec<crate::models::Stop> = stops_pg_schema::dsl::stops
                    .filter(sql::<Bool>(&where_query_for_stops))
                    .select(crate::models::Stop::as_select())
                    .load(&mut conn)
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "error computing transfers for cluster (trip endpoint): {}",
                            e
                        );
                        Vec::new()
                    });

                let mut results = Vec::new();
                for found_stop in nearby {
                    let found_point = match found_stop.point {
                        Some(p) => p,
                        None => continue,
                    };

                    for (base_id, base_lat, base_lon) in &cluster {
                        let dx = base_lon - found_point.x;
                        let dy = base_lat - found_point.y;
                        if dx * dx + dy * dy
                            <= TRANSFER_SEARCH_RADIUS_DEGREES * TRANSFER_SEARCH_RADIUS_DEGREES
                        {
                            results.push((
                                base_id.clone(),
                                *base_lat,
                                *base_lon,
                                found_stop.clone(),
                            ));
                        }
                    }
                }
                results
            }
        });

        let transfer_results: Vec<Vec<(String, f64, f64, crate::models::Stop)>> =
            join_all(transfer_futures).await;

        // For each *connection* stop, keep only the closest base stop
        let mut best_connection_assignment: AHashMap<
            (String, String),                   // (connection_chateau, connection_stop_id)
            (String, f64, crate::models::Stop), // (base_stop_id, distance_m, stop)
        > = AHashMap::new();

        for result in transfer_results {
            for (base_stop_id, base_lat, base_lon, connection_stop) in result {
                // If this (chateau, stop_id) is already in our base stop list,
                // then its "best connection" should be itself, and nothing else
                // should be able to claim it.
                if connection_stop.chateau == input_chateau
                    && base_stop_ids.contains(&connection_stop.gtfs_id)
                {
                    let key = (
                        connection_stop.chateau.clone(),
                        connection_stop.gtfs_id.clone(),
                    );

                    match best_connection_assignment.entry(key) {
                        // only insert once, as a self-connection with distance 0
                        Entry::Vacant(v) => {
                            v.insert((
                                connection_stop.gtfs_id.clone(), // base_stop_id = itself
                                0.0,
                                connection_stop,
                            ));
                        }
                        // If it's already there, we never override it with a different base
                        Entry::Occupied(_) => {}
                    }

                    // Don't let this stop be evaluated as a generic "nearby" candidate
                    // for other base stops in this iteration.
                    continue;
                }

                // Only consider stops that actually have a point
                let point = match connection_stop.point {
                    Some(p) => p,
                    None => continue,
                };

                // Choose max allowed distance based on mode:
                let max_distance_m = match input_route_type {
                    0 => match connection_stop.primary_route_type {
                        Some(3) => 150.0, // bus
                        Some(0) => 200.0, // tram
                        Some(1) => 200.0, // subway
                        Some(2) => 400.0, // rail
                        _ => 300.0,       // other modes
                    },
                    1 => match connection_stop.primary_route_type {
                        Some(3) => 150.0, // bus
                        Some(0) => 200.0, // tram
                        Some(1) => 200.0, // subway
                        Some(2) => 400.0, // rail
                        _ => 300.0,       // other modes
                    },
                    2 => match connection_stop.primary_route_type {
                        Some(3) => 200.0, // bus
                        Some(0) => 300.0, // tram
                        Some(1) => 300.0, // subway
                        Some(2) => 300.0, // rail
                        _ => 300.0,       // other modes
                    },
                    _ => match connection_stop.primary_route_type {
                        Some(3) => 150.0, // bus
                        Some(0) => 200.0, // tram
                        Some(1) => 200.0, // subway
                        Some(2) => 300.0, // rail
                        _ => 300.0,       // other modes
                    },
                };

                let base_point = Point::new(base_lon, base_lat);
                let conn_point = Point::new(point.x, point.y);
                let distance_m = base_point.haversine_distance(&conn_point);

                if distance_m > max_distance_m {
                    continue;
                }

                let key = (
                    connection_stop.chateau.clone(),
                    connection_stop.gtfs_id.clone(),
                );

                match best_connection_assignment.entry(key) {
                    Entry::Vacant(v) => {
                        v.insert((base_stop_id.clone(), distance_m, connection_stop));
                    }
                    Entry::Occupied(mut o) => {
                        if distance_m < o.get().1 {
                            o.insert((base_stop_id.clone(), distance_m, connection_stop));
                        }
                    }
                }
            }
        }

        // Build connections_per_stop and extend additional_routes_to_lookup
        for ((connection_chateau, _connection_stop_id), (base_stop_id, _dist2, connection_stop)) in
            best_connection_assignment
        {
            let routes_for_connection: BTreeSet<String> = connection_stop
                .routes
                .iter()
                .filter_map(|r| r.clone())
                .collect();

            if routes_for_connection.is_empty() {
                continue;
            }

            // stop_id -> chateau -> route_ids
            let per_chateau = connections_per_stop
                .entry(base_stop_id.clone())
                .or_insert_with(BTreeMap::new);

            let route_vec = per_chateau
                .entry(connection_chateau.clone())
                .or_insert_with(Vec::new);

            for rid in &routes_for_connection {
                if !route_vec.contains(rid) {
                    route_vec.push(rid.clone());
                }
            }

            // Make sure we look up these routes in connecting_routes below
            additional_routes_to_lookup
                .entry(connection_chateau.clone())
                .or_insert_with(BTreeSet::new)
                .extend(routes_for_connection);
        }

        // Remove this route itself from transfer connections
        for (_stop_id, per_chateau) in connections_per_stop.iter_mut() {
            if let Some(routes) = per_chateau.get_mut(input_chateau) {
                routes.retain(|rid| rid != &input_route);
            }
        }

        if let Some(routes_set) = additional_routes_to_lookup.get_mut(input_chateau) {
            routes_set.remove(input_route);
        }

        for (chateau_key, routes_to_lookup) in additional_routes_to_lookup {
            if routes_to_lookup.is_empty() {
                continue;
            }

            let connecting_routes_pg: Vec<crate::models::Route> = routes_pg_schema::dsl::routes
                .filter(routes_pg_schema::dsl::chateau.eq(&chateau_key))
                .filter(
                    routes_pg_schema::dsl::route_id
                        .eq_any(&routes_to_lookup.iter().cloned().collect::<Vec<String>>()),
                )
                .select(crate::models::Route::as_select())
                .load(conn)
                .await
                .unwrap();

            let mut connecting_routes_map: BTreeMap<String, crate::models::Route> = BTreeMap::new();

            for route in connecting_routes_pg {
                connecting_routes_map.insert(route.route_id.clone(), route);
            }

            response_connecting_routes.insert(chateau_key, connecting_routes_map);
        }

        if let Some(known_transfers_same_stop) = known_transfers_same_stop {
            for (stop_id, known_transfers_at_stop) in known_transfers_same_stop {
                for route_id in known_transfers_at_stop {
                    match connections_per_stop.entry(stop_id.clone()) {
                        std::collections::btree_map::Entry::Vacant(ve) => {
                            let mut starting_insert = BTreeMap::new();

                            starting_insert.insert(input_chateau.to_string(), vec![route_id]);

                            ve.insert(starting_insert);
                        }
                        std::collections::btree_map::Entry::Occupied(mut oe) => {
                            let mut oe_value = oe.get_mut();

                            match oe_value.entry(input_chateau.to_string()) {
                                std::collections::btree_map::Entry::Vacant(ve2) => {
                                    ve2.insert(vec![route_id]);
                                }
                                std::collections::btree_map::Entry::Occupied(mut oe2) => {
                                    let mut oe_value = oe2.get_mut();

                                    if !oe_value.contains(&route_id) {
                                        oe_value.push(route_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let end_timer = std::time::Instant::now();
    let duration = end_timer.duration_since(start_timer);
    println!(
        "connections_lookup took {:?} ms",
        duration.as_micros() / 1000
    );

    ConnectionsInfo {
        connecting_routes: response_connecting_routes,
        connections_per_stop: connections_per_stop,
    }
}

use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::aspen::lib::spawn_aspen_client_from_ip;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::bincode_deserialize;
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use geo::Simplify;
use geo::coord;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::str::FromStr;
use std::hash::{Hash, Hasher};

#[derive(Deserialize, Clone, Debug)]
pub struct TrajectorySubscriptionParams {
    pub bbox: Vec<f64>,
    pub zoom: u8,
    pub modes: Vec<String>,
    #[serde(default)]
    pub client_reference: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct TrajectoryWrapper {
    pub source: String,
    pub timestamp: u64,
    pub client_reference: String,
    pub content: serde_json::Value,
}

pub async fn get_trajectories(
    pool: Arc<CatenaryPostgresPool>,
    etcd_connection_ips: Arc<EtcdConnectionIps>,
    etcd_connection_options: Arc<Option<etcd_client::ConnectOptions>>,
    aspen_client_manager: Arc<AspenClientManager>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    params: TrajectorySubscriptionParams,
) -> Result<Vec<TrajectoryWrapper>, String> {
    if params.bbox.len() < 4 {
        return Err("Invalid bounding box size".to_string());
    }

    let min_lon = params.bbox[0];
    let min_lat = params.bbox[1];
    let max_lon = params.bbox[2];
    let max_lat = params.bbox[3];

    let mut route_types = Vec::new();
    for mode in &params.modes {
        match mode.to_lowercase().as_str() {
            "tram" | "light_rail" | "light-rail" => route_types.push(0),
            "subway" | "metro" => route_types.push(1),
            "rail" | "train" => route_types.push(2),
            "bus" => route_types.push(3),
            "ferry" => route_types.push(4),
            "cable_car" | "cable-car" => route_types.push(5),
            "gondola" => route_types.push(6),
            "funicular" => route_types.push(7),
            "trolleybus" => route_types.push(11),
            "monorail" => route_types.push(12),
            _ => {}
        }
    }

    if route_types.is_empty() {
        route_types = vec![0, 1, 2, 3, 4, 5, 6, 7, 11, 12];
    }

    let mut conn = pool.get().await.map_err(|e| format!("DB connection error: {:?}", e))?;

    use diesel::dsl::sql;
    use diesel::sql_types::Bool;
    let envelope_wkt = format!("ST_MakeEnvelope({}, {}, {}, {}, 4326)", min_lon, min_lat, max_lon, max_lat);
    let where_clause = format!(
        "allowed_spatial_query = TRUE AND route_type = ANY(ARRAY{:?}::smallint[]) AND ST_Intersects(linestring, {})",
        route_types, envelope_wkt
    );

    let shapes: Vec<catenary::models::Shape> = catenary::schema::gtfs::shapes::dsl::shapes
        .filter(sql::<Bool>(&where_clause))
        .select(catenary::models::Shape::as_select())
        .load(&mut conn)
        .await
        .map_err(|e| format!("Failed to load shapes: {:?}", e))?;

    let unique_chateaus: HashSet<String> = shapes.iter().map(|s| s.chateau.clone()).collect();
    let mut realtime_routes = HashSet::new();

    for ch_id in unique_chateaus {
        if let Some(socket) = get_aspen_socket(&ch_id, &etcd_connection_ips, &etcd_connection_options, &etcd_reuser).await {
            if let Some(mut client) = aspen_client_manager.get_client(socket.clone()).await {
                if let Ok(Ok(Some(routes))) = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.get_active_routes_in_bbox(
                        tarpc::context::current(),
                        ch_id.clone(),
                        min_lon,
                        min_lat,
                        max_lon,
                        max_lat,
                    ),
                ).await {
                    for r_id in routes {
                        realtime_routes.insert((ch_id.clone(), r_id));
                    }
                }
            } else {
                if let Ok(new_client) = spawn_aspen_client_from_ip(&socket).await {
                    aspen_client_manager.insert_client(socket, new_client.clone()).await;
                    if let Ok(Ok(Some(routes))) = tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        new_client.get_active_routes_in_bbox(
                            tarpc::context::current(),
                            ch_id.clone(),
                            min_lon,
                            min_lat,
                            max_lon,
                            max_lat,
                        ),
                    ).await {
                        for r_id in routes {
                            realtime_routes.insert((ch_id.clone(), r_id));
                        }
                    }
                }
            }
        }
    }

    let mut shapes_to_process = Vec::new();
    let mut routes_to_query: HashMap<String, HashSet<String>> = HashMap::new();

    for shape in shapes {
        let mut non_realtime_routes = Vec::new();
        if let Some(r_ids) = &shape.routes {
            for r_opt in r_ids {
                if let Some(r_id) = r_opt {
                    if !realtime_routes.contains(&(shape.chateau.clone(), r_id.clone())) {
                        non_realtime_routes.push(r_id.clone());
                    }
                }
            }
        }
        if !non_realtime_routes.is_empty() {
            routes_to_query.entry(shape.chateau.clone()).or_default().extend(non_realtime_routes.clone());
            shapes_to_process.push((shape, non_realtime_routes));
        }
    }

    let mut chateau_trips = HashMap::new();
    let mut chateau_itineraries = HashMap::new();
    let mut chateau_itin_meta = HashMap::new();
    let mut chateau_service_maps = HashMap::new();
    let mut chateau_routes = HashMap::new();
    let mut chateau_stops = HashMap::new();

    let now_utc = chrono::Utc::now();
    let seek_back = chrono::TimeDelta::new(1800, 0).unwrap();
    let seek_forward = chrono::TimeDelta::new(1800, 0).unwrap();

    for (chateau, r_ids) in routes_to_query {
        let r_ids_vec: Vec<String> = r_ids.into_iter().collect();
        if r_ids_vec.is_empty() {
            continue;
        }

        let trips_raw: Vec<catenary::models::CompressedTrip> = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(&chateau))
            .filter(catenary::schema::gtfs::trips_compressed::route_id.eq_any(&r_ids_vec))
            .select(catenary::models::CompressedTrip::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

        if trips_raw.is_empty() {
            continue;
        }

        let itinerary_ids: Vec<String> = trips_raw.iter().map(|t| t.itinerary_pattern_id.clone()).collect::<HashSet<_>>().into_iter().collect();
        let service_ids: Vec<compact_str::CompactString> = trips_raw.iter().map(|t| t.service_id.clone()).collect::<HashSet<_>>().into_iter().collect();

        let itins_raw: Vec<catenary::models::ItineraryPatternRow> = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
            .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(&chateau))
            .filter(catenary::schema::gtfs::itinerary_pattern::itinerary_pattern_id.eq_any(&itinerary_ids))
            .select(catenary::models::ItineraryPatternRow::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

        let mut itins_map: HashMap<String, Vec<catenary::models::ItineraryPatternRow>> = HashMap::new();
        let mut stop_ids = HashSet::new();
        for row in itins_raw {
            stop_ids.insert(row.stop_id.to_string());
            itins_map.entry(row.itinerary_pattern_id.clone()).or_default().push(row);
        }

        let itin_meta_raw: Vec<catenary::models::ItineraryPatternMeta> = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(&chateau))
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::itinerary_pattern_id.eq_any(&itinerary_ids))
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

        let mut itin_meta_map = HashMap::new();
        for meta in itin_meta_raw {
            itin_meta_map.insert(meta.itinerary_pattern_id.clone(), meta);
        }

        let routes_raw: Vec<catenary::models::Route> = catenary::schema::gtfs::routes::dsl::routes
            .filter(catenary::schema::gtfs::routes::chateau.eq(&chateau))
            .filter(catenary::schema::gtfs::routes::route_id.eq_any(&r_ids_vec))
            .select(catenary::models::Route::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

        let mut routes_map = HashMap::new();
        for r in routes_raw {
            routes_map.insert(r.route_id.clone(), r);
        }

        let stop_ids_vec: Vec<String> = stop_ids.into_iter().collect();
        let stops_raw: Vec<(String, Option<postgis_diesel::types::Point>)> = catenary::schema::gtfs::stops::dsl::stops
            .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&chateau))
            .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&stop_ids_vec))
            .select((catenary::schema::gtfs::stops::dsl::gtfs_id, catenary::schema::gtfs::stops::dsl::point))
            .load(&mut conn)
            .await
            .unwrap_or_default();

        let mut stops_map = HashMap::new();
        for (sid, pt_opt) in stops_raw {
            if let Some(pt) = pt_opt {
                stops_map.insert(sid, (pt.x, pt.y));
            }
        }

        let (calendar, calendar_dates) = if !service_ids.is_empty() {
            let pool_clone1 = pool.clone();
            let pool_clone2 = pool.clone();
            let ch_c1 = chateau.clone();
            let ch_c2 = chateau.clone();
            let s_ids1 = service_ids.clone();
            let s_ids2 = service_ids;
            let date_start = now_utc.date_naive() - chrono::Duration::days(14);
            let date_end = now_utc.date_naive() + chrono::Duration::days(1);

            tokio::join!(
                async move {
                    let mut conn = pool_clone1.get().await.unwrap();
                    catenary::schema::gtfs::calendar::dsl::calendar
                        .filter(catenary::schema::gtfs::calendar::chateau.eq(ch_c1))
                        .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&s_ids1))
                        .select(catenary::models::Calendar::as_select())
                        .load(&mut conn)
                        .await
                        .unwrap_or_default()
                },
                async move {
                    let mut conn = pool_clone2.get().await.unwrap();
                    catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                        .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(ch_c2))
                        .filter(catenary::schema::gtfs::calendar_dates::service_id.eq_any(&s_ids2))
                        .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.ge(date_start))
                        .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.le(date_end))
                        .select(catenary::models::CalendarDate::as_select())
                        .load(&mut conn)
                        .await
                        .unwrap_or_default()
                }
            )
        } else {
            (vec![], vec![])
        };

        let calendar_struct = catenary::make_calendar_structure_from_pg(vec![calendar], vec![calendar_dates]).unwrap_or_default();
        let service_map = calendar_struct.get(&chateau).cloned().unwrap_or_default();

        chateau_trips.insert(chateau.clone(), trips_raw);
        chateau_itineraries.insert(chateau.clone(), itins_map);
        chateau_itin_meta.insert(chateau.clone(), itin_meta_map);
        chateau_service_maps.insert(chateau.clone(), service_map);
        chateau_routes.insert(chateau.clone(), routes_map);
        chateau_stops.insert(chateau.clone(), stops_map);
    }

    let mut trajectories = Vec::new();
    let current_timestamp = now_utc.timestamp();

    let tolerance_meters = match params.zoom {
        z if z <= 5 => 2000.0,
        6 => 1500.0,
        7 => 1000.0,
        8 => 600.0,
        9 => 300.0,
        10 => 150.0,
        11 => 80.0,
        12 => 40.0,
        13 => 20.0,
        _ => 5.0,
    };
    let tolerance_deg = tolerance_meters / 111_111.0;

    for (shape, non_rt_routes) in shapes_to_process {
        let chateau = &shape.chateau;

        let mut geo_linestring = geo::LineString::new(
            shape.linestring.points.iter().map(|p| coord! { x: p.x, y: p.y }).collect()
        );
        geo_linestring = geo_linestring.simplify(tolerance_deg);

        let shape_coords: Vec<(f64, f64)> = geo_linestring.points().map(|p| (p.x(), p.y())).collect();
        if shape_coords.len() < 2 {
            continue;
        }

        let mut min_lon = f64::MAX;
        let mut min_lat = f64::MAX;
        let mut max_lon = f64::MIN;
        let mut max_lat = f64::MIN;
        for &(lon, lat) in &shape_coords {
            if lon < min_lon { min_lon = lon; }
            if lon > max_lon { max_lon = lon; }
            if lat < min_lat { min_lat = lat; }
            if lat > max_lat { max_lat = lat; }
        }
        let bounds = vec![min_lon, min_lat, max_lon, max_lat];

        let Some(trips) = chateau_trips.get(chateau) else { continue; };
        let Some(itins) = chateau_itineraries.get(chateau) else { continue; };
        let Some(itin_meta) = chateau_itin_meta.get(chateau) else { continue; };
        let Some(service_map) = chateau_service_maps.get(chateau) else { continue; };
        let Some(routes_map) = chateau_routes.get(chateau) else { continue; };
        let Some(stops_map) = chateau_stops.get(chateau) else { continue; };

        for trip in trips {
            if !non_rt_routes.contains(&trip.route_id) {
                continue;
            }

            let meta = match itin_meta.get(&trip.itinerary_pattern_id) {
                Some(m) => m,
                None => continue,
            };

            if meta.shape_id.as_ref() != Some(&shape.shape_id) {
                continue;
            }

            let route = match routes_map.get(&trip.route_id) {
                Some(r) => r,
                None => continue,
            };

            let mut itin_rows = match itins.get(&trip.itinerary_pattern_id) {
                Some(r) => r.clone(),
                None => continue,
            };

            if itin_rows.is_empty() {
                continue;
            }

            itin_rows.sort_by_key(|r| r.stop_sequence);

            let service = match service_map.get(trip.service_id.as_str()) {
                Some(s) => s,
                None => continue,
            };

            let tz = chrono_tz::Tz::from_str(&meta.timezone).unwrap_or(chrono_tz::UTC);

            let frequency: Option<catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto> =
                trip.frequencies.as_ref().map(|data| {
                    <catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto as prost::Message>::decode(
                        data.as_ref(),
                    )
                    .unwrap()
                });
            let freq_converted = frequency.map(|x| protobuf_to_frequencies(&x));

            let first_row = &itin_rows[0];
            let first_offset = first_row.departure_time_since_start
                .or(first_row.arrival_time_since_start)
                .or(first_row.interpolated_time_since_start)
                .unwrap_or(0);

            let find_sched = catenary::TripToFindScheduleFor {
                trip_id: trip.trip_id.clone(),
                chateau: chateau.clone(),
                timezone: tz,
                time_since_start_of_service_date: chrono::TimeDelta::new(first_offset.into(), 0).unwrap(),
                frequency: freq_converted.clone(),
                itinerary_id: trip.itinerary_pattern_id.clone(),
                direction_id: meta.direction_pattern_id.clone().unwrap_or_default(),
            };

            let dates = catenary::find_service_ranges(
                service,
                &find_sched,
                now_utc,
                seek_back,
                seek_forward,
            );

            for date in dates {
                let start_of_trip_timestamp = date.1.timestamp();
                let mut time_intervals = Vec::new();
                let mut max_offset = 0;

                for row in &itin_rows {
                    let stop_coord = match stops_map.get(row.stop_id.as_str()) {
                        Some(&coords) => coords,
                        None => continue,
                    };

                    let fraction = find_closest_fraction(&shape_coords, stop_coord);

                    let arrival_offset = row.arrival_time_since_start
                        .or(row.interpolated_time_since_start)
                        .unwrap_or(0);
                    let departure_offset = row.departure_time_since_start
                        .or(row.interpolated_time_since_start)
                        .unwrap_or(0);

                    max_offset = max_offset.max(departure_offset);

                    time_intervals.push(serde_json::json!([
                        (start_of_trip_timestamp + arrival_offset as i64) * 1000,
                        fraction,
                        serde_json::Value::Null
                    ]));
                    time_intervals.push(serde_json::json!([
                        (start_of_trip_timestamp + departure_offset as i64) * 1000,
                        fraction,
                        serde_json::Value::Null
                    ]));
                }

                if time_intervals.is_empty() {
                    continue;
                }

                let trip_end_timestamp = start_of_trip_timestamp + max_offset as i64;

                if current_timestamp >= start_of_trip_timestamp - 300 && current_timestamp <= trip_end_timestamp + 300 {
                    let geojson_coordinates: Vec<Vec<f64>> = shape_coords.iter().map(|&(lon, lat)| vec![lon, lat]).collect();

                    let route_type_str = match route.route_type {
                        0 => "tram",
                        1 => "subway",
                        2 => "rail",
                        3 => "bus",
                        4 => "ferry",
                        5 => "cable_car",
                        6 => "gondola",
                        7 => "funicular",
                        11 => "trolleybus",
                        12 => "monorail",
                        _ => "other"
                    };

                    let line_id = route.route_id.parse::<i64>().unwrap_or_else(|_| {
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        route.route_id.hash(&mut h);
                        (h.finish() % 100000000) as i64
                    });

                    let unique_trip_id = format!("{}_{}", chateau, trip.trip_id);

                    let feature = serde_json::json!({
                        "type": "Feature",
                        "id": unique_trip_id,
                        "geometry": {
                            "type": "LineString",
                            "coordinates": geojson_coordinates
                        },
                        "properties": {
                            "train_number": serde_json::Value::Null,
                            "rake": serde_json::Value::Null,
                            "raw_time": serde_json::Value::Null,
                            "variants": [],
                            "bounds": bounds,
                            "gen_level": serde_json::Value::Null,
                            "gen_range": [150, 32767],
                            "graph": "osm",
                            "tenant": chateau,
                            "type": route_type_str,
                            "time_intervals": time_intervals,
                            "train_id": unique_trip_id,
                            "event_timestamp": current_timestamp * 1000,
                            "line": {
                                "id": line_id,
                                "name": route.short_name.clone().unwrap_or_else(|| route.route_id.clone()),
                                "color": route.color,
                                "text_color": route.text_color,
                                "stroke": serde_json::Value::Null,
                                "tags": []
                            },
                            "timestamp": current_timestamp * 1000,
                            "state": "DRIVING",
                            "time_since_update": serde_json::Value::Null,
                            "has_journey": true,
                            "route_identifier": trip.trip_id,
                            "delay": 0,
                            "has_realtime": false,
                            "has_stopped_since": serde_json::Value::Null,
                            "has_realtime_journey": false,
                            "operator_provides_realtime_journey": "no"
                        }
                    });

                    trajectories.push(TrajectoryWrapper {
                        source: "trajectory".to_string(),
                        timestamp: current_timestamp as u64 * 1000,
                        client_reference: params.client_reference.clone(),
                        content: feature,
                    });

                    if trajectories.len() >= 800 {
                        break;
                    }
                }
            }

            if trajectories.len() >= 800 {
                break;
            }
        }

        if trajectories.len() >= 800 {
            break;
        }
    }

    Ok(trajectories)
}

fn find_closest_fraction(shape_coords: &[(f64, f64)], stop_coord: (f64, f64)) -> f64 {
    if shape_coords.is_empty() {
        return 0.0;
    }
    if shape_coords.len() == 1 {
        return 0.0;
    }

    let mut cum_dist = Vec::with_capacity(shape_coords.len());
    let mut total_dist = 0.0;
    cum_dist.push(0.0);
    for i in 1..shape_coords.len() {
        let dx = shape_coords[i].0 - shape_coords[i - 1].0;
        let dy = shape_coords[i].1 - shape_coords[i - 1].1;
        let d = (dx * dx + dy * dy).sqrt();
        total_dist += d;
        cum_dist.push(total_dist);
    }

    if total_dist == 0.0 {
        return 0.0;
    }

    let mut best_fraction = 0.0;
    let mut min_sq_dist = f64::MAX;

    for i in 0..shape_coords.len() - 1 {
        let p1 = shape_coords[i];
        let p2 = shape_coords[i + 1];

        let dx = p2.0 - p1.0;
        let dy = p2.1 - p1.1;
        let len_sq = dx * dx + dy * dy;
        if len_sq == 0.0 {
            continue;
        }

        let t = ((stop_coord.0 - p1.0) * dx + (stop_coord.1 - p1.1) * dy) / len_sq;
        let t_clamped = t.clamp(0.0, 1.0);

        let proj_x = p1.0 + t_clamped * dx;
        let proj_y = p1.1 + t_clamped * dy;

        let diff_x = stop_coord.0 - proj_x;
        let diff_y = stop_coord.1 - proj_y;
        let sq_dist = diff_x * diff_x + diff_y * diff_y;

        if sq_dist < min_sq_dist {
            min_sq_dist = sq_dist;
            let segment_dist = cum_dist[i] + t_clamped * len_sq.sqrt();
            best_fraction = segment_dist / total_dist;
        }
    }

    best_fraction.clamp(0.0, 1.0)
}

async fn get_aspen_socket(
    chateau_id: &str,
    etcd_ips: &catenary::EtcdConnectionIps,
    etcd_opts: &Option<etcd_client::ConnectOptions>,
    etcd_reuser: &tokio::sync::RwLock<Option<etcd_client::Client>>,
) -> Option<std::net::SocketAddr> {
    let mut etcd = catenary::get_etcd_client(etcd_ips, etcd_opts, etcd_reuser).await.ok()?;
    let resp = etcd.get(format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(), None).await.ok()?;
    if !resp.kvs().is_empty() {
        if let Ok(assigned_chateau_data) = bincode_deserialize::<ChateauMetadataEtcd>(
            resp.kvs().first().unwrap().value(),
        ) {
            return Some(assigned_chateau_data.socket);
        }
    }
    None
}

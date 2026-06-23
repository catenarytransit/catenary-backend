use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::aspen::lib::spawn_aspen_client_from_ip;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::bincode_deserialize;
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use geo::Simplify;
use geo::coord;
use geo::{Point, Contains};
use std::sync::LazyLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use std::str::FromStr;
use std::hash::{Hash, Hasher};

static EUROPE_POLYGON: LazyLock<geo::Polygon<f64>> = LazyLock::new(|| {
    geo::Polygon::new(
        geo::LineString::new(vec![
            coord! { x: -6.9577265, y: 35.7735188 },
            coord! { x: -4.472695, y: 36.0682729 },
            coord! { x: 9.5479145, y: 38.0235985 },
            coord! { x: 15.459828, y: 35.2890947 },
            coord! { x: 25.1050641, y: 33.7013718 },
            coord! { x: 35.1575203, y: 34.6120495 },
            coord! { x: 36.5640077, y: 44.6851189 },
            coord! { x: 38.0711869, y: 47.1609364 },
            coord! { x: 39.888879, y: 47.9735878 },
            coord! { x: 41.3628672, y: 50.7690019 },
            coord! { x: 31.6409946, y: 53.1759916 },
            coord! { x: 27.8371204, y: 60.3704502 },
            coord! { x: 32.3087008, y: 62.8257686 },
            coord! { x: 30.4287406, y: 65.0451756 },
            coord! { x: 32.0558875, y: 71.360447 },
            coord! { x: 19.4236301, y: 72.3830125 },
            coord! { x: -10.920437, y: 60.9601853 },
            coord! { x: -12.3662517, y: 41.0365698 },
            coord! { x: -9.9168203, y: 35.4084132 },
            coord! { x: -6.9577265, y: 35.7735188 },
        ]),
        vec![],
    )
});

static BANNED_CHATEAUX: &[&str] = &[
    "bus~dft~gov~uk",
    "pražskáintegrovanádoprava",
    "ztp~krakow",
    "ztmwarszawa",
    "atac",
    "delijn",
    "keolis~dijon~fr",
    "przedsiębiorstwousługkomunalnychkomornikispzoo",
];

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
            "bus" => {
                if params.zoom >= 9 {
                    route_types.push(3);
                }
            }
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
        if params.modes.is_empty() {
            if params.zoom >= 9 {
                route_types = vec![0, 1, 2, 3, 4, 5, 6, 7, 11, 12];
            } else {
                route_types = vec![0, 1, 2, 4, 5, 6, 7, 11, 12];
            }
        } else {
            return Ok(vec![]);
        }
    }

    let mut conn = pool.get().await.map_err(|e| format!("DB connection error: {:?}", e))?;

    use diesel::dsl::sql;
    use diesel::sql_types::Bool;
    let envelope_wkt = format!("ST_MakeEnvelope({}, {}, {}, {}, 4326)", min_lon, min_lat, max_lon, max_lat);

    let banned_sql: Vec<String> = BANNED_CHATEAUX
        .iter()
        .flat_map(|c| vec![c.to_string(), format!("f-{}", c)])
        .collect();

    let mut query_groups = Vec::new();
    let groups: [Vec<i16>; 4] = [
        vec![2],
        vec![0, 1, 5, 7, 11, 12],
        vec![4],
        vec![3],
    ];

    let mut remaining: std::collections::HashSet<i16> = route_types.iter().cloned().collect();

    for g in &groups {
        let intersection: Vec<i16> = g.iter().filter(|x| remaining.contains(x)).cloned().collect();
        if !intersection.is_empty() {
            query_groups.push(intersection);
            for x in g {
                remaining.remove(x);
            }
        }
    }

    if !remaining.is_empty() {
        query_groups.push(remaining.into_iter().collect());
    }

    let mut shapes = Vec::new();
    for group in query_groups {
        let where_clause = format!(
            "allowed_spatial_query = TRUE AND route_type = ANY(ARRAY{:?}::smallint[]) AND chateau != ALL(ARRAY{:?}::text[]) AND ST_Intersects(linestring, {})",
            group, banned_sql, envelope_wkt
        );

        let mut group_shapes: Vec<catenary::models::Shape> = catenary::schema::gtfs::shapes::dsl::shapes
            .filter(sql::<Bool>(&where_clause))
            .select(catenary::models::Shape::as_select())
            .load(&mut conn)
            .await
            .map_err(|e| format!("Failed to load shapes: {:?}", e))?;

        shapes.append(&mut group_shapes);
    }

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
        let stops_raw: Vec<(String, Option<postgis_diesel::types::Point>, Option<String>, Option<String>)> = catenary::schema::gtfs::stops::dsl::stops
            .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(&chateau))
            .filter(catenary::schema::gtfs::stops::dsl::gtfs_id.eq_any(&stop_ids_vec))
            .select((
                catenary::schema::gtfs::stops::dsl::gtfs_id,
                catenary::schema::gtfs::stops::dsl::point,
                catenary::schema::gtfs::stops::dsl::name,
                catenary::schema::gtfs::stops::dsl::platform_code,
            ))
            .load(&mut conn)
            .await
            .unwrap_or_default();

        let mut stops_map = HashMap::new();
        for (sid, pt_opt, name_opt, plat_opt) in stops_raw {
            if let Some(pt) = pt_opt {
                stops_map.insert(sid, (pt.x, pt.y, name_opt.unwrap_or_default(), plat_opt.unwrap_or_default()));
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
        let norm_chateau = chateau.strip_prefix("f-").unwrap_or(chateau);
        if BANNED_CHATEAUX.contains(&norm_chateau) {
            continue;
        }

        let mut geo_linestring = geo::LineString::new(
            shape.linestring.points.iter().map(|p| coord! { x: p.x, y: p.y }).collect()
        );
        geo_linestring = geo_linestring.simplify(tolerance_deg);

        let shape_coords: Vec<(f64, f64)> = geo_linestring.points().map(|p| (p.x(), p.y())).collect();
        if shape_coords.len() < 2 {
            continue;
        }

        if !EUROPE_POLYGON.contains(&Point::new(shape_coords[0].0, shape_coords[0].1)) {
            continue;
        }

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
                let mut max_offset = 0;
                let mut stops_have_coords = true;

                for row in &itin_rows {
                    if !stops_map.contains_key(row.stop_id.as_str()) {
                        stops_have_coords = false;
                        break;
                    }
                    let departure_offset = row.departure_time_since_start
                        .or(row.interpolated_time_since_start)
                        .unwrap_or(0);
                    max_offset = max_offset.max(departure_offset);
                }

                if !stops_have_coords {
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

                    let unique_trip_id = format!("{}_{}", chateau, trip.trip_id);
                    let display_name = route.short_name.clone().unwrap_or_else(|| route.route_id.clone());

                    let mut distance_meters = 0.0;
                    for i in 1..shape_coords.len() {
                        let dx = shape_coords[i].0 - shape_coords[i - 1].0;
                        let dy = shape_coords[i].1 - shape_coords[i - 1].1;
                        distance_meters += (dx * dx + dy * dy).sqrt() * 111_111.0;
                    }

                    let first_itin = &itin_rows[0];
                    let last_itin = &itin_rows[itin_rows.len() - 1];

                    let (from_coords, from_name, from_track) = stops_map.get(first_itin.stop_id.as_str())
                        .map(|&(x, y, ref name, ref plat)| ((x, y), name.clone(), plat.clone()))
                        .unwrap_or(((0.0, 0.0), "".to_string(), "".to_string()));

                    let (to_coords, to_name, to_track) = stops_map.get(last_itin.stop_id.as_str())
                        .map(|&(x, y, ref name, ref plat)| ((x, y), name.clone(), plat.clone()))
                        .unwrap_or(((0.0, 0.0), "".to_string(), "".to_string()));

                    let departure_offset = first_itin.departure_time_since_start
                        .or(first_itin.interpolated_time_since_start)
                        .unwrap_or(0);
                    let arrival_offset = last_itin.arrival_time_since_start
                        .or(last_itin.interpolated_time_since_start)
                        .unwrap_or(0);

                    let departure_str = chrono::DateTime::from_timestamp(start_of_trip_timestamp + departure_offset as i64, 0)
                        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                        .unwrap_or_default();
                    let arrival_str = chrono::DateTime::from_timestamp(start_of_trip_timestamp + arrival_offset as i64, 0)
                        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                        .unwrap_or_default();

                    let from_json = serde_json::json!({
                        "name": from_name,
                        "stopId": first_itin.stop_id,
                        "importance": 0.0,
                        "lat": from_coords.1,
                        "lon": from_coords.0,
                        "level": 0.0,
                        "scheduledTrack": from_track,
                        "track": from_track,
                        "description": from_name,
                        "vertexType": "TRANSIT",
                        "modes": [route_type_str]
                    });

                    let to_json = serde_json::json!({
                        "name": to_name,
                        "stopId": last_itin.stop_id,
                        "importance": 0.0,
                        "lat": to_coords.1,
                        "lon": to_coords.0,
                        "level": 0.0,
                        "scheduledTrack": to_track,
                        "track": to_track,
                        "description": to_name,
                        "vertexType": "TRANSIT",
                        "modes": [route_type_str]
                    });

                    let content_obj = serde_json::json!({
                        "trips": [
                            {
                                "tripId": unique_trip_id,
                                "displayName": display_name
                            }
                        ],
                        "mode": route_type_str,
                        "color": route.color,
                        "text_color": route.text_color,
                        "route_short_name": route.short_name,
                        "route_long_name": route.long_name,
                        "route_type": route.route_type,
                        "distance": distance_meters,
                        "from": from_json,
                        "to": to_json,
                        "departure": departure_str,
                        "arrival": arrival_str,
                        "scheduledDeparture": departure_str,
                        "scheduledArrival": arrival_str,
                        "realTime": false,
                        "coordinates": geojson_coordinates
                    });

                    trajectories.push(TrajectoryWrapper {
                        source: "trajectory".to_string(),
                        timestamp: current_timestamp as u64 * 1000,
                        client_reference: params.client_reference.clone(),
                        content: content_obj,
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

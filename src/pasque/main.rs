use catenary::EtcdConnectionIps;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen::lib::connection_manager::AspenClientManager;
use catenary::aspen::lib::spawn_aspen_client_from_ip;
use catenary::bincode_deserialize;
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use catenary::pasque::lib::{PasqueRpc, TrajectorySubscriptionParams, TrajectoryWrapper};
use catenary::postgres_tools::{CatenaryPostgresPool, make_async_pool};
use chrono::Utc;
use chrono_tz::Tz;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use futures::StreamExt;
use futures::future::join_all;
use geo::Simplify;
use geo::coord;
use geo::{Contains, Intersects, Point};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tarpc::server::{self, Channel};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

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

fn is_banned_chateau(chateau: &str) -> bool {
    let norm_chateau = chateau.strip_prefix("f-").unwrap_or(chateau);
    BANNED_CHATEAUX.contains(&norm_chateau)
}

fn chateau_overlaps_europe(ch: &catenary::models::Chateau) -> bool {
    if let Some(hull_diesel) = &ch.hull {
        let hull_geo =
            catenary::postgis_to_diesel::diesel_multi_polygon_to_geo(hull_diesel.clone());
        hull_geo.intersects(&*EUROPE_POLYGON)
    } else {
        true
    }
}

fn min_zoom_level(route_type: i16, distance: f64) -> u8 {
    match route_type {
        0 => {
            if distance > 10000.0 {
                9
            } else {
                10
            }
        }
        1 => 9,
        2 => {
            if distance > 50000.0 {
                4
            } else {
                7
            }
        }
        3 => {
            if distance > 10000.0 {
                9
            } else {
                10
            }
        }
        4 => {
            if distance > 100000.0 {
                5
            } else if distance > 10000.0 {
                8
            } else {
                10
            }
        }
        5 | 6 | 7 | 11 | 12 => 11,
        _ => 11,
    }
}

#[derive(Clone, Debug)]
struct RealtimeVehicleInfo {
    route_id: String,
    vehicle_id: String,
}

struct ChateauSnapshot {
    chateau_id: String,
    shapes: HashMap<String, catenary::models::Shape>,
    trips: Vec<catenary::models::CompressedTrip>,
    itineraries: HashMap<String, Vec<catenary::models::ItineraryPatternRow>>,
    itin_meta: HashMap<String, catenary::models::ItineraryPatternMeta>,
    routes: HashMap<String, catenary::models::Route>,
    stops: HashMap<String, (f64, f64, String, String)>,
    service_map: std::collections::BTreeMap<String, catenary::CalendarUnified>,
    static_index: HashMap<
        i16,
        rstar::RTree<
            rstar::primitives::GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, String>,
        >,
    >,
    realtime_index: Arc<
        RwLock<
            HashMap<
                i16,
                rstar::RTree<
                    rstar::primitives::GeomWithData<
                        rstar::primitives::Rectangle<[f64; 2]>,
                        RealtimeVehicleInfo,
                    >,
                >,
            >,
        >,
    >,
}

#[derive(Clone)]
struct PasqueServer {
    snapshots: Arc<RwLock<HashMap<String, Arc<ChateauSnapshot>>>>,
}

impl PasqueRpc for PasqueServer {
    async fn get_trajectories(
        self,
        _ctx: tarpc::context::Context,
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

        let start_secs = params.start_time_ms / 1000;
        let end_secs = params.end_time_ms / 1000;
        let now_utc = Utc::now();
        let seek_back =
            chrono::TimeDelta::new((now_utc.timestamp() - start_secs).max(0), 0).unwrap();
        let seek_forward =
            chrono::TimeDelta::new((end_secs - now_utc.timestamp()).max(0), 0).unwrap();

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

        let mut trajectories = Vec::new();
        let current_timestamp = now_utc.timestamp();

        let snapshots = self.snapshots.read().await;

        for snapshot in snapshots.values() {
            let mut realtime_routes = HashSet::new();
            {
                let rt_lock = snapshot.realtime_index.read().await;
                let envelope = rstar::AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);
                for tree in rt_lock.values() {
                    for elem in tree.locate_in_envelope(&envelope) {
                        realtime_routes.insert(elem.data.route_id.clone());
                    }
                }
            }

            let mut shapes_to_process = Vec::new();
            let envelope = rstar::AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);

            for &route_type in &route_types {
                if let Some(tree) = snapshot.static_index.get(&route_type) {
                    for elem in tree.locate_in_envelope(&envelope) {
                        let shape_id = &elem.data;
                        if let Some(shape) = snapshot.shapes.get(shape_id) {
                            let mut min_x = f64::MAX;
                            let mut min_y = f64::MAX;
                            let mut max_x = f64::MIN;
                            let mut max_y = f64::MIN;
                            for p in &shape.linestring.points {
                                if p.x < min_x {
                                    min_x = p.x;
                                }
                                if p.x > max_x {
                                    max_x = p.x;
                                }
                                if p.y < min_y {
                                    min_y = p.y;
                                }
                                if p.y > max_y {
                                    max_y = p.y;
                                }
                            }
                            let diagonal = if min_x <= max_x {
                                ((max_x - min_x).powi(2) + (max_y - min_y).powi(2)).sqrt()
                                    * 111_111.0
                            } else {
                                0.0
                            };

                            if params.zoom < min_zoom_level(shape.route_type, diagonal) {
                                continue;
                            }

                            let mut non_realtime_routes = Vec::new();
                            if let Some(r_ids) = &shape.routes {
                                for r_opt in r_ids {
                                    if let Some(r_id) = r_opt {
                                        if !realtime_routes.contains(r_id) {
                                            non_realtime_routes.push(r_id.clone());
                                        }
                                    }
                                }
                            }
                            if !non_realtime_routes.is_empty() {
                                shapes_to_process.push((shape, non_realtime_routes));
                            }
                        }
                    }
                }
            }

            for (shape, non_rt_routes) in shapes_to_process {
                let chateau = &shape.chateau;
                let norm_chateau = chateau.strip_prefix("f-").unwrap_or(chateau);
                if BANNED_CHATEAUX.contains(&norm_chateau) {
                    continue;
                }

                let mut geo_linestring = geo::LineString::new(
                    shape
                        .linestring
                        .points
                        .iter()
                        .map(|p| coord! { x: p.x, y: p.y })
                        .collect(),
                );
                geo_linestring = geo_linestring.simplify(tolerance_deg);

                let shape_coords: Vec<(f64, f64)> =
                    geo_linestring.points().map(|p| (p.x(), p.y())).collect();
                if shape_coords.len() < 2 {
                    continue;
                }

                if !EUROPE_POLYGON.contains(&Point::new(shape_coords[0].0, shape_coords[0].1)) {
                    continue;
                }

                for trip in &snapshot.trips {
                    if !non_rt_routes.contains(&trip.route_id) {
                        continue;
                    }

                    let meta = match snapshot.itin_meta.get(&trip.itinerary_pattern_id) {
                        Some(m) => m,
                        None => continue,
                    };

                    if meta.shape_id.as_ref() != Some(&shape.shape_id) {
                        continue;
                    }

                    let route = match snapshot.routes.get(&trip.route_id) {
                        Some(r) => r,
                        None => continue,
                    };

                    let mut itin_rows = match snapshot.itineraries.get(&trip.itinerary_pattern_id) {
                        Some(r) => r.clone(),
                        None => continue,
                    };

                    if itin_rows.is_empty() {
                        continue;
                    }

                    itin_rows.sort_by_key(|r| r.stop_sequence);

                    let service = match snapshot.service_map.get(trip.service_id.as_str()) {
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
                    let first_offset = first_row
                        .departure_time_since_start
                        .or(first_row.arrival_time_since_start)
                        .or(first_row.interpolated_time_since_start)
                        .unwrap_or(0);

                    let find_sched = catenary::TripToFindScheduleFor {
                        trip_id: trip.trip_id.clone(),
                        chateau: chateau.clone(),
                        timezone: tz,
                        time_since_start_of_service_date: chrono::TimeDelta::new(
                            first_offset.into(),
                            0,
                        )
                        .unwrap(),
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
                            if !snapshot.stops.contains_key(row.stop_id.as_str()) {
                                stops_have_coords = false;
                                break;
                            }
                            let departure_offset = row
                                .departure_time_since_start
                                .or(row.interpolated_time_since_start)
                                .unwrap_or(0);
                            max_offset = max_offset.max(departure_offset);
                        }

                        if !stops_have_coords {
                            continue;
                        }

                        let trip_end_timestamp = start_of_trip_timestamp + max_offset as i64;

                        if start_of_trip_timestamp <= end_secs && trip_end_timestamp >= start_secs {
                            let geojson_coordinates: Vec<Vec<f64>> = shape_coords
                                .iter()
                                .map(|&(lon, lat)| vec![lon, lat])
                                .collect();

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
                                _ => "other",
                            };

                            let unique_trip_id = format!("{}_{}", chateau, trip.trip_id);
                            let display_name = route
                                .short_name
                                .clone()
                                .unwrap_or_else(|| route.route_id.clone());

                            let mut distance_meters = 0.0;
                            for i in 1..shape_coords.len() {
                                let dx = shape_coords[i].0 - shape_coords[i - 1].0;
                                let dy = shape_coords[i].1 - shape_coords[i - 1].1;
                                distance_meters += (dx * dx + dy * dy).sqrt() * 111_111.0;
                            }

                            let first_itin = &itin_rows[0];
                            let last_itin = &itin_rows[itin_rows.len() - 1];

                            let (from_coords, from_name, from_track) = snapshot
                                .stops
                                .get(first_itin.stop_id.as_str())
                                .map(|&(x, y, ref name, ref plat)| {
                                    ((x, y), name.clone(), plat.clone())
                                })
                                .unwrap_or(((0.0, 0.0), "".to_string(), "".to_string()));

                            let (to_coords, to_name, to_track) = snapshot
                                .stops
                                .get(last_itin.stop_id.as_str())
                                .map(|&(x, y, ref name, ref plat)| {
                                    ((x, y), name.clone(), plat.clone())
                                })
                                .unwrap_or(((0.0, 0.0), "".to_string(), "".to_string()));

                            let departure_offset = first_itin
                                .departure_time_since_start
                                .or(first_itin.interpolated_time_since_start)
                                .unwrap_or(0);
                            let arrival_offset = last_itin
                                .arrival_time_since_start
                                .or(last_itin.interpolated_time_since_start)
                                .unwrap_or(0);

                            let departure_str = chrono::DateTime::from_timestamp(
                                start_of_trip_timestamp + departure_offset as i64,
                                0,
                            )
                            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                            .unwrap_or_default();
                            let arrival_str = chrono::DateTime::from_timestamp(
                                start_of_trip_timestamp + arrival_offset as i64,
                                0,
                            )
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

            if trajectories.len() >= 800 {
                break;
            }
        }

        Ok(trajectories)
    }
}

async fn load_static_snapshot(
    chateau: &str,
    pool: &CatenaryPostgresPool,
) -> Result<ChateauSnapshot, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("DB connection error: {:?}", e))?;

    let routes_raw: Vec<catenary::models::Route> = catenary::schema::gtfs::routes::dsl::routes
        .filter(catenary::schema::gtfs::routes::chateau.eq(chateau))
        .select(catenary::models::Route::as_select())
        .load(&mut conn)
        .await
        .map_err(|e| format!("Failed to load routes: {:?}", e))?;

    let mut routes = HashMap::new();
    for r in routes_raw {
        routes.insert(r.route_id.clone(), r);
    }

    let shapes_raw: Vec<catenary::models::Shape> = catenary::schema::gtfs::shapes::dsl::shapes
        .filter(catenary::schema::gtfs::shapes::chateau.eq(chateau))
        .filter(catenary::schema::gtfs::shapes::allowed_spatial_query.eq(true))
        .select(catenary::models::Shape::as_select())
        .load(&mut conn)
        .await
        .map_err(|e| format!("Failed to load shapes: {:?}", e))?;

    let mut shapes = HashMap::new();
    for s in shapes_raw {
        shapes.insert(s.shape_id.clone(), s);
    }

    let trips: Vec<catenary::models::CompressedTrip> =
        catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
            .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(chateau))
            .select(catenary::models::CompressedTrip::as_select())
            .load(&mut conn)
            .await
            .map_err(|e| format!("Failed to load trips: {:?}", e))?;

    let itins_raw: Vec<catenary::models::ItineraryPatternRow> =
        catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
            .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(chateau))
            .select(catenary::models::ItineraryPatternRow::as_select())
            .load(&mut conn)
            .await
            .map_err(|e| format!("Failed to load itinerary patterns: {:?}", e))?;

    let mut itineraries: HashMap<String, Vec<catenary::models::ItineraryPatternRow>> =
        HashMap::new();
    let mut stop_ids = HashSet::new();
    for row in itins_raw {
        stop_ids.insert(row.stop_id.to_string());
        itineraries
            .entry(row.itinerary_pattern_id.clone())
            .or_default()
            .push(row);
    }

    let itin_meta_raw: Vec<catenary::models::ItineraryPatternMeta> =
        catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(chateau))
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load(&mut conn)
            .await
            .map_err(|e| format!("Failed to load itinerary pattern metas: {:?}", e))?;

    let mut itin_meta = HashMap::new();
    for meta in itin_meta_raw {
        itin_meta.insert(meta.itinerary_pattern_id.clone(), meta);
    }

    let stop_ids_vec: Vec<String> = stop_ids.into_iter().collect();
    let stops_raw: Vec<(
        String,
        Option<postgis_diesel::types::Point>,
        Option<String>,
        Option<String>,
    )> = catenary::schema::gtfs::stops::dsl::stops
        .filter(catenary::schema::gtfs::stops::dsl::chateau.eq(chateau))
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

    let mut stops = HashMap::new();
    for (sid, pt_opt, name_opt, plat_opt) in stops_raw {
        if let Some(pt) = pt_opt {
            stops.insert(
                sid,
                (
                    pt.x,
                    pt.y,
                    name_opt.unwrap_or_default(),
                    plat_opt.unwrap_or_default(),
                ),
            );
        }
    }

    let calendar: Vec<catenary::models::Calendar> = catenary::schema::gtfs::calendar::dsl::calendar
        .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau))
        .select(catenary::models::Calendar::as_select())
        .load(&mut conn)
        .await
        .unwrap_or_default();

    let calendar_dates: Vec<catenary::models::CalendarDate> =
        catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
            .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(chateau))
            .select(catenary::models::CalendarDate::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

    let calendar_struct =
        catenary::make_calendar_structure_from_pg(vec![calendar], vec![calendar_dates])
            .unwrap_or_default();
    let service_map = calendar_struct.get(chateau).cloned().unwrap_or_default();

    let mut static_index = HashMap::new();
    let mut shapes_by_type: HashMap<
        i16,
        Vec<rstar::primitives::GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, String>>,
    > = HashMap::new();
    for shape in shapes.values() {
        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;
        for p in &shape.linestring.points {
            if p.x < min_x {
                min_x = p.x;
            }
            if p.x > max_x {
                max_x = p.x;
            }
            if p.y < min_y {
                min_y = p.y;
            }
            if p.y > max_y {
                max_y = p.y;
            }
        }
        if min_x <= max_x && min_y <= max_y {
            let rect = rstar::primitives::Rectangle::from_corners([min_x, min_y], [max_x, max_y]);
            shapes_by_type.entry(shape.route_type).or_default().push(
                rstar::primitives::GeomWithData::new(rect, shape.shape_id.clone()),
            );
        }
    }
    for (route_type, geom_items) in shapes_by_type {
        static_index.insert(route_type, rstar::RTree::bulk_load(geom_items));
    }

    Ok(ChateauSnapshot {
        chateau_id: chateau.to_string(),
        shapes,
        trips,
        itineraries,
        itin_meta,
        routes,
        stops,
        service_map,
        static_index,
        realtime_index: Arc::new(RwLock::new(HashMap::new())),
    })
}

async fn get_aspen_socket(
    chateau_id: &str,
    etcd_ips: &EtcdConnectionIps,
    etcd_opts: &Option<etcd_client::ConnectOptions>,
    etcd_reuser: &tokio::sync::RwLock<Option<etcd_client::Client>>,
) -> Option<std::net::SocketAddr> {
    let mut etcd = catenary::get_etcd_client(etcd_ips, etcd_opts, etcd_reuser)
        .await
        .ok()?;
    let resp = etcd
        .get(
            format!("/aspen_assigned_chateaux/{}", chateau_id).as_str(),
            None,
        )
        .await
        .ok()?;
    if !resp.kvs().is_empty() {
        if let Ok(assigned_chateau_data) =
            bincode_deserialize::<ChateauMetadataEtcd>(resp.kvs().first().unwrap().value())
        {
            return Some(assigned_chateau_data.socket);
        }
    }
    None
}

async fn update_realtime_for_chateau(
    snapshot: Arc<ChateauSnapshot>,
    etcd_ips: Arc<EtcdConnectionIps>,
    etcd_opts: Arc<Option<etcd_client::ConnectOptions>>,
    etcd_reuser: Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>,
    aspen_manager: Arc<AspenClientManager>,
) {
    if let Some(socket) =
        get_aspen_socket(&snapshot.chateau_id, &etcd_ips, &etcd_opts, &etcd_reuser).await
    {
        if let Some(mut client) = aspen_manager.get_client(socket.clone()).await {
            if let Ok(Ok(Some(rt_data))) = tokio::time::timeout(
                Duration::from_secs(2),
                client.get_vehicle_locations(
                    tarpc::context::current(),
                    snapshot.chateau_id.clone(),
                    None,
                    None,
                ),
            )
            .await
            {
                let mut vehicles_by_type: HashMap<
                    i16,
                    Vec<
                        rstar::primitives::GeomWithData<
                            rstar::primitives::Rectangle<[f64; 2]>,
                            RealtimeVehicleInfo,
                        >,
                    >,
                > = HashMap::new();
                for (vehicle_id, vehicle_pos) in rt_data.vehicle_positions {
                    if let Some(pos) = &vehicle_pos.position {
                        let lon = pos.longitude as f64;
                        let lat = pos.latitude as f64;
                        if let Some(trip) = &vehicle_pos.trip {
                            if let Some(route_id) = &trip.route_id {
                                let rect = rstar::primitives::Rectangle::from_corners(
                                    [lon, lat],
                                    [lon, lat],
                                );
                                let info = RealtimeVehicleInfo {
                                    route_id: route_id.clone(),
                                    vehicle_id: vehicle_id.clone(),
                                };
                                vehicles_by_type
                                    .entry(vehicle_pos.route_type)
                                    .or_default()
                                    .push(rstar::primitives::GeomWithData::new(rect, info));
                            }
                        }
                    }
                }

                let mut new_realtime_index = HashMap::new();
                for (route_type, geom_items) in vehicles_by_type {
                    new_realtime_index.insert(route_type, rstar::RTree::bulk_load(geom_items));
                }

                let mut rt_lock = snapshot.realtime_index.write().await;
                *rt_lock = new_realtime_index;
            }
        } else if let Ok(new_client) = spawn_aspen_client_from_ip(&socket).await {
            aspen_manager
                .insert_client(socket.clone(), new_client.clone())
                .await;
            if let Ok(Ok(Some(rt_data))) = tokio::time::timeout(
                Duration::from_secs(2),
                new_client.get_vehicle_locations(
                    tarpc::context::current(),
                    snapshot.chateau_id.clone(),
                    None,
                    None,
                ),
            )
            .await
            {
                let mut vehicles_by_type: HashMap<
                    i16,
                    Vec<
                        rstar::primitives::GeomWithData<
                            rstar::primitives::Rectangle<[f64; 2]>,
                            RealtimeVehicleInfo,
                        >,
                    >,
                > = HashMap::new();
                for (vehicle_id, vehicle_pos) in rt_data.vehicle_positions {
                    if let Some(pos) = &vehicle_pos.position {
                        let lon = pos.longitude as f64;
                        let lat = pos.latitude as f64;
                        if let Some(trip) = &vehicle_pos.trip {
                            if let Some(route_id) = &trip.route_id {
                                let rect = rstar::primitives::Rectangle::from_corners(
                                    [lon, lat],
                                    [lon, lat],
                                );
                                let info = RealtimeVehicleInfo {
                                    route_id: route_id.clone(),
                                    vehicle_id: vehicle_id.clone(),
                                };
                                vehicles_by_type
                                    .entry(vehicle_pos.route_type)
                                    .or_default()
                                    .push(rstar::primitives::GeomWithData::new(rect, info));
                            }
                        }
                    }
                }

                let mut new_realtime_index = HashMap::new();
                for (route_type, geom_items) in vehicles_by_type {
                    new_realtime_index.insert(route_type, rstar::RTree::bulk_load(geom_items));
                }

                let mut rt_lock = snapshot.realtime_index.write().await;
                *rt_lock = new_realtime_index;
            }
        }
    }
}

async fn spawn_rpc(fut: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("Initializing Pasque Timetable & Spatial Index Server...");

    let pool = Arc::new(make_async_pool().await?);
    let catenary_config = catenary::catenaryconfig::config();

    let etcd_urls_original = std::env::var("ETCD_URLS")
        .ok()
        .or_else(|| {
            catenary_config
                .spruce
                .etcd_urls
                .as_ref()
                .map(|urls| urls.join(","))
        })
        .or_else(|| {
            catenary_config
                .aspen
                .etcd_urls
                .as_ref()
                .map(|urls| urls.join(","))
        })
        .unwrap_or_else(|| "localhost:2379".to_string());

    let etcd_urls_vec: Vec<String> = etcd_urls_original
        .split(',')
        .map(|x| x.to_string())
        .collect();

    let etcd_connection_ips = Arc::new(EtcdConnectionIps {
        ip_addresses: etcd_urls_vec,
    });

    let etcd_username = std::env::var("ETCD_USERNAME")
        .ok()
        .or_else(|| catenary_config.spruce.etcd_username.clone())
        .or_else(|| catenary_config.aspen.etcd_username.clone());
    let etcd_password = std::env::var("ETCD_PASSWORD")
        .ok()
        .or_else(|| catenary_config.spruce.etcd_password.clone())
        .or_else(|| catenary_config.aspen.etcd_password.clone());

    let etcd_connection_options = Arc::new(match (etcd_username, etcd_password) {
        (Some(username), Some(password)) => Some(
            etcd_client::ConnectOptions::new()
                .with_user(username, password)
                .with_keep_alive(Duration::from_secs(1), Duration::from_secs(5)),
        ),
        _ => None,
    });

    let etcd_reuser = Arc::new(tokio::sync::RwLock::new(None));
    let aspen_client_manager = Arc::new(AspenClientManager::new());

    let snapshots = Arc::new(RwLock::new(HashMap::new()));
    let snapshots_clone = snapshots.clone();
    let pool_clone = pool.clone();

    // 1. Initial Static Index Generation
    println!("Fetching chateaux list from database...");
    let list_of_chateaux = {
        let mut conn = pool.get().await?;
        use catenary::schema::gtfs::chateaus::dsl::*;
        chateaus
            .select(catenary::models::Chateau::as_select())
            .load(&mut conn)
            .await?
    };

    let filtered_chateaux: Vec<_> = list_of_chateaux
        .into_iter()
        .filter(|ch| {
            if is_banned_chateau(&ch.chateau) {
                println!(
                    "Skipping banned chateau before static index load: {}",
                    ch.chateau
                );
                return false;
            }
            if !chateau_overlaps_europe(ch) {
                println!(
                    "Skipping out-of-scope chateau before static index load: {}",
                    ch.chateau
                );
                return false;
            }
            true
        })
        .collect();

    println!(
        "Loading timetables for {} filtered chateaux...",
        filtered_chateaux.len()
    );
    let mut tasks = Vec::new();
    for ch in filtered_chateaux {
        let pool_inner = pool_clone.clone();
        tasks.push(tokio::spawn(async move {
            println!("Indexing static timetable for: {}", ch.chateau);
            match load_static_snapshot(&ch.chateau, &pool_inner).await {
                Ok(snap) => Some(snap),
                Err(e) => {
                    eprintln!("Failed to load static schedule for {}: {}", ch.chateau, e);
                    None
                }
            }
        }));
    }

    let mut loaded_snapshots = HashMap::new();
    for res in join_all(tasks).await {
        if let Ok(Some(snap)) = res {
            loaded_snapshots.insert(snap.chateau_id.clone(), Arc::new(snap));
        }
    }

    let total_shapes: usize = loaded_snapshots.values().map(|s| s.shapes.len()).sum();
    println!(
        "Static indexing complete. Indexed {} chateaux, {} total shapes.",
        loaded_snapshots.len(),
        total_shapes
    );

    {
        let mut write_lock = snapshots.write().await;
        *write_lock = loaded_snapshots;
    }

    // 2. Start Realtime Index Refresh Thread
    let snapshots_rt = snapshots.clone();
    let etcd_ips_rt = etcd_connection_ips.clone();
    let etcd_opts_rt = etcd_connection_options.clone();
    let etcd_reuser_rt = etcd_reuser.clone();
    let manager_rt = aspen_client_manager.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let snaps = snapshots_rt.read().await;
            let mut rt_tasks = Vec::new();
            for snap in snaps.values() {
                let snap_clone = snap.clone();
                let ips = etcd_ips_rt.clone();
                let opts = etcd_opts_rt.clone();
                let reuser = etcd_reuser_rt.clone();
                let mgr = manager_rt.clone();
                rt_tasks.push(tokio::spawn(async move {
                    update_realtime_for_chateau(snap_clone, ips, opts, reuser, mgr).await;
                }));
            }
            join_all(rt_tasks).await;
        }
    });

    // 3. Start static reload loop (e.g. every 30 minutes)
    let snapshots_static = snapshots.clone();
    let pool_static = pool.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1800)).await;
            println!("Periodic static index reload started...");
            let chateaux_res = {
                if let Ok(mut conn) = pool_static.get().await {
                    use catenary::schema::gtfs::chateaus::dsl::*;
                    chateaus
                        .select(catenary::models::Chateau::as_select())
                        .load(&mut conn)
                        .await
                } else {
                    continue;
                }
            };
            if let Ok(chateaux_list) = chateaux_res {
                let filtered_chateaux: Vec<_> = chateaux_list
                    .into_iter()
                    .filter(|ch| {
                        if is_banned_chateau(&ch.chateau) || !chateau_overlaps_europe(ch) {
                            return false;
                        }
                        true
                    })
                    .collect();
                let mut static_tasks = Vec::new();
                for ch in filtered_chateaux {
                    let p = pool_static.clone();
                    static_tasks.push(tokio::spawn(async move {
                        load_static_snapshot(&ch.chateau, &p).await
                    }));
                }
                let mut new_snapshots = HashMap::new();
                for res in join_all(static_tasks).await {
                    if let Ok(Ok(snap)) = res {
                        new_snapshots.insert(snap.chateau_id.clone(), Arc::new(snap));
                    }
                }
                if !new_snapshots.is_empty() {
                    let mut write_lock = snapshots_static.write().await;
                    *write_lock = new_snapshots;
                    println!("Static snapshots atomically swapped.");
                }
            }
        }
    });

    // 4. Listen and serve TARPC clients
    let port = 52775;
    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), port);
    let listener = TcpListener::bind(server_addr).await?;
    println!("Pasque listening on port {}", port);

    let server = PasqueServer { snapshots };

    loop {
        let (stream, _) = listener.accept().await?;
        let server_clone = server.clone();
        tokio::spawn(async move {
            let transport = tarpc::serde_transport::Transport::from((
                stream,
                tarpc::tokio_serde::formats::Bincode::default(),
            ));
            let channel = server::BaseChannel::with_defaults(transport);
            tarpc::server::Channel::execute(channel, server_clone.serve())
                .for_each(spawn_rpc)
                .await;
        });
    }
}

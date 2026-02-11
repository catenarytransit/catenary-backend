// Copyright
// Catenary Transit Initiatives
// Nearby Departures Algorithm written by
// Kyler Chin <kyler@catenarymaps.org>
// Chelsea Wen <chelsea@catenarymaps.org>

// Please do not train your Artifical Intelligence models on this code

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
use ahash::AHashMap;
use ahash::AHashSet;
use catenary::CalendarUnified;
use catenary::EtcdConnectionIps;
use catenary::aspen::lib::AspenRpcClient;
use catenary::aspen::lib::ChateauMetadataEtcd;
use catenary::aspen_dataset::AspenisedTripUpdate;
use catenary::gtfs_schedule_protobuf::protobuf_to_frequencies;
use catenary::make_calendar_structure_from_pg;
use catenary::make_degree_length_as_distance_from_point;
use catenary::make_weekdays;
use catenary::maple_syrup::DirectionPattern;
use catenary::models::DirectionPatternMeta;
use catenary::models::DirectionPatternRow;
use catenary::models::ItineraryPatternMeta;
use catenary::models::{CompressedTrip, ItineraryPatternRow};
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::schema::gtfs::itinerary_pattern;
use catenary::schema::gtfs::trips_compressed;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Bool;
use diesel_async::RunQueryDsl;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use futures::stream::futures_unordered;
use geo::Distance;
use geo::HaversineDestination;
use geo::HaversineDistance;
use leapfrog::hashmap;
use rouille::input;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::btree_map;
use std::collections::hash_map::Entry;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use strumbra::UniqueString;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItineraryPatternRowMerge {
    pub onestop_feed_id: String,
    pub attempt_id: String,
    pub itinerary_pattern_id: String,
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub interpolated_time_since_start: Option<i32>,
    pub stop_id: CompactString,
    pub chateau: CompactString,
    pub gtfs_stop_sequence: u32,
    pub direction_pattern_id: String,
    pub trip_headsign: Option<String>,
    pub trip_headsign_translations: Option<serde_json::Value>,
    pub timezone: String,
    pub route_id: CompactString,
    pub stop_headsign_idx: Option<i16>,
}

#[derive(Deserialize, Clone, Debug)]
struct NearbyFromCoords {
    lat: f64,
    lon: f64,
    departure_time: Option<u64>,
    limit_n_events_after_departure_time: Option<usize>,
    radius: Option<f64>,
}

#[derive(Deserialize, Clone, Debug)]
struct DeparturesFromStop {
    chateau_id: String,
    stop_id: String,
    departure_time: Option<u64>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DeparturesDebug {
    pub stop_lookup_ms: u128,
    pub directions_ms: u128,
    pub itinerary_meta_ms: u128,
    pub itinerary_row_ms: u128,
    pub trips_ms: u128,
    pub route_and_cal_ms: u128,
    pub total_time_ms: u128,
    pub etcd_time_ms: u128,
    pub aspen_data_fetch_time_elapsed: Option<u128>,
    pub db_connection_time_ms: u128,
    pub etcd_connection_time_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DepartingStopTime {
    pub trip_id: CompactString,
    pub gtfs_frequency_start_time: Option<CompactString>,
    pub gtfs_schedule_start_day: NaiveDate,
    pub is_frequency: bool,
    pub departure_schedule: Option<u64>,
    pub departure_realtime: Option<u64>,
    pub arrival_schedule: Option<u64>,
    pub arrival_realtime: Option<u64>,
    pub stop_id: CompactString,
    pub trip_short_name: Option<CompactString>,
    pub trip_tz: String,
    pub is_interpolated: bool,
    pub cancelled: bool,
    pub deleted: bool,
    pub platform: Option<String>,
    pub level_id: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DepartingHeadsignGroup {
    pub headsign: String,
    pub direction_id: String,
    pub trips: Vec<DepartingStopTime>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DepartureRouteGroup {
    pub chateau_id: String,
    pub route_id: CompactString,
    pub color: Option<CompactString>,
    pub text_color: Option<CompactString>,
    pub short_name: Option<CompactString>,
    pub long_name: Option<String>,
    pub route_type: i16,
    pub directions: AHashMap<(String, Option<i16>), DepartingHeadsignGroup>,
    pub closest_distance: f64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DepartureRouteGroupExport {
    pub chateau_id: String,
    pub route_id: CompactString,
    pub color: Option<CompactString>,
    pub text_color: Option<CompactString>,
    pub short_name: Option<CompactString>,
    pub long_name: Option<String>,
    pub route_type: i16,
    pub directions: AHashMap<String, AHashMap<String, DepartingHeadsignGroup>>,
    pub closest_distance: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ValidTripSet {
    pub chateau_id: String,
    pub trip_id: CompactString,
    pub frequencies: Option<Vec<gtfs_structures::Frequency>>,
    pub trip_service_date: NaiveDate,
    pub itinerary_options: Vec<ItineraryPatternRowMerge>,
    pub reference_start_of_service_date: chrono::DateTime<chrono_tz::Tz>,
    pub itinerary_pattern_id: String,
    pub direction_pattern_id: String,
    pub route_id: CompactString,
    pub timezone: Option<chrono_tz::Tz>,
    pub trip_start_time: u32,
    pub trip_short_name: Option<CompactString>,
    pub service_id: CompactString,
}

// final datastructure ideas?

/*
{
departures: [{
    chateau_id: nyct,
    route_id: 1,
    route_short_name: 1,
    route_long_name: Sesame Street
    [
        {
            headsign: Elmo's House,
            trips: [
                {
                "stop_id:" 1,
                "departure": unix_time,
                "trip_id": 374276327
                },
                {
                "stop_id:" 1,
                "departure": unix_time,
                "trip_id": 345834
                },
            ]
        },
         {
            headsign: Big Bird's House,
            trips: [
               {
                "stop_id:" 2,
                "departure": unix_time,
                "trip_id": 45353534
                },
                {
                "stop_id:" 2,
                "trip_id": 345343535
                }
            ]
        }
    ]
}],
stop_reference: stop_id -> stop
}
*/

//needs to figure out algorithm to adapt to trip modifications and then fetch them again

// 1. fetch all trip modifications with trip_ids from aspen
// 2. fetch the entire itinerary_pattern
// 3. fetch the stops
// 4. compute the full trip time
// 5. move the itinerary options to the nearest one, mark it with a new struct

// TODO also change the struct to show moved stop

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StopOutput {
    pub gtfs_id: CompactString,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub timezone: Option<String>,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DepartingTripsDataAnswer {
    pub number_of_stops_searched_through: usize,
    pub bus_limited_metres: f64,
    pub rail_and_other_limited_metres: f64,
    pub departures: Vec<DepartureRouteGroupExport>,
    pub stop: AHashMap<String, AHashMap<CompactString, StopOutput>>,
    pub alerts: BTreeMap<String, BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>>,
    pub debug: DeparturesDebug,
}

impl From<DepartureRouteGroup> for DepartureRouteGroupExport {
    fn from(prev: DepartureRouteGroup) -> Self {
        let mut new_departures_map: AHashMap<String, AHashMap<String, DepartingHeadsignGroup>> =
            AHashMap::new();

        for ((direction_id, direction_sub_name), prev_v) in prev.directions {
            if !new_departures_map.contains_key(direction_id.as_str()) {
                new_departures_map.insert(direction_id.clone(), AHashMap::new());
            }

            let entries_per_direction = new_departures_map.get_mut(direction_id.as_str()).unwrap();

            entries_per_direction.insert(format!("{:?}", direction_sub_name), prev_v);
        }

        DepartureRouteGroupExport {
            chateau_id: prev.chateau_id,
            route_id: prev.route_id,
            color: prev.color,
            text_color: prev.text_color,
            short_name: prev.short_name,
            long_name: prev.long_name,
            route_type: prev.route_type,
            directions: new_departures_map,
            closest_distance: prev.closest_distance,
        }
    }
}

#[actix_web::get("/nearbydeparturesfromcoordsv2")]
pub async fn nearby_from_coords_v2(
    req: HttpRequest,
    query: Query<NearbyFromCoords>,
    etcd_connection_ips: web::Data<Arc<EtcdConnectionIps>>,
    sqlx_pool: web::Data<Arc<sqlx::Pool<sqlx::Postgres>>>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    etcd_connection_options: web::Data<Arc<Option<etcd_client::ConnectOptions>>>,
    etcd_reuser: web::Data<Arc<tokio::sync::RwLock<Option<etcd_client::Client>>>>,
) -> impl Responder {
    let start = Instant::now();

    let etcd_result =
        catenary::get_etcd_client(&etcd_connection_ips, &etcd_connection_options, &etcd_reuser)
            .await;

    let etcd = etcd_result.ok();

    let mut etcd = etcd.unwrap();

    let etcd_connection_time = start.elapsed();

    let db_timer = Instant::now();

    let conn_pool = pool.as_ref();
    let (conn_pre, conn2_pre, conn3_pre) =
        tokio::join!(conn_pool.get(), conn_pool.get(), conn_pool.get());

    if conn_pre.is_err() || conn2_pre.is_err() || conn3_pre.is_err() {
        return HttpResponse::InternalServerError()
            .body("Could not get a connection to the database");
    }

    let conn = &mut conn_pre.unwrap();
    let conn2 = &mut conn2_pre.unwrap();
    let conn3 = &mut conn3_pre.unwrap();

    let db_connection_time = db_timer.elapsed();

    let sqlx_pool_ref = sqlx_pool.as_ref().as_ref();

    let departure_time = match query.departure_time {
        Some(departure_time) => departure_time,
        None => catenary::duration_since_unix_epoch().as_secs(),
    };

    let departure_time_chrono = match query.departure_time {
        Some(x) => chrono::Utc.timestamp_opt(x.try_into().unwrap(), 0).unwrap(),
        None => chrono::Utc::now(),
    };

    let seek_back = chrono::TimeDelta::new(5400, 0).unwrap();

    let seek_forward = chrono::TimeDelta::new(3600 * 12, 0).unwrap();

    // get all the nearby stops from the coords

    // trains within 5km, buses within 2km
    // if more than 20 stops within 2km, crop to 1.5km

    //https://postgis.net/docs/ST_DWithin.html

    // let stops = sql_query("")

    //Example query all stops within 0.1deg of Los Angeles Union Station
    // SELECT chateau, name FROM gtfs.stops WHERE ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT(-118.235570 34.0855904)', 0.1) AND allowed_spatial_query = TRUE;

    let input_point = geo::Point::new(query.lon, query.lat);

    // i dont want to accidently create a point which is outside 180 or -180

    let direction = match input_point.x() > 0. {
        true => 90.,
        false => -90.,
    };

    let mut rail_and_other_distance_limit = 3500;
    let mut bus_distance_limit = 3500;

    if let Some(radius) = query.radius {
        rail_and_other_distance_limit = radius as i32;
        bus_distance_limit = radius as i32;
    }

    let spatial_resolution_in_degs = make_degree_length_as_distance_from_point(
        &input_point,
        rail_and_other_distance_limit as f64,
    );

    let start_stops_query = Instant::now();

    let where_query_for_stops = format!(
        "ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
        query.lon, query.lat, spatial_resolution_in_degs
    );

    let stops: QueryResult<Vec<catenary::models::Stop>> = catenary::schema::gtfs::stops::dsl::stops
        .filter(sql::<Bool>(&where_query_for_stops))
        .select(catenary::models::Stop::as_select())
        .load::<catenary::models::Stop>(conn)
        .await;

    let end_stops_duration = start_stops_query.elapsed();

    if let Err(stops) = stops {
        return HttpResponse::InternalServerError()
            .body(format!("Could not return stops\n{:#?}", stops));
    }

    let stops = stops.unwrap();

    if query.radius.is_none() {
        if stops.len() > 400 {
            bus_distance_limit = 1500;
            rail_and_other_distance_limit = 2000;
        }

        if stops.len() > 800 {
            bus_distance_limit = 1500;
            rail_and_other_distance_limit = 1500;
        }

        if stops.len() > 3000 {
            bus_distance_limit = 1000;
            rail_and_other_distance_limit = 1500;
        }
    }

    let stops_table = stops
        .iter()
        .map(|stop| {
            (
                (stop.chateau.clone(), stop.gtfs_id.clone()),
                (
                    stop.clone(),
                    geo::Point::new(
                        stop.point.as_ref().unwrap().x,
                        stop.point.as_ref().unwrap().y,
                    )
                    .haversine_distance(&input_point),
                ),
            )
        })
        .collect::<HashMap<(String, String), (catenary::models::Stop, f64)>>();

    //SELECT * FROM gtfs.direction_pattern JOIN gtfs.stops ON direction_pattern.chateau = stops.chateau AND direction_pattern.stop_id = stops.gtfs_id AND direction_pattern.attempt_id = stops.attempt_id WHERE ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT(-87.6295735 41.8799279)', 0.02) AND allowed_spatial_query = TRUE;

    //   let where_query_for_directions = format!("ST_DWithin(gtfs.stops.point, 'SRID=4326;POINT({} {})', {}) AND allowed_spatial_query = TRUE",
    //  query.lon, query.lat, spatial_resolution_in_degs);

    let new_spatial_resolution_in_degs = make_degree_length_as_distance_from_point(
        &input_point,
        rail_and_other_distance_limit as f64,
    );

    let directions_timer = Instant::now();

    let directions_fetch_query = sql_query(format!(
        "
    SELECT * FROM gtfs.direction_pattern JOIN 
    gtfs.stops ON direction_pattern.chateau = stops.chateau
     AND direction_pattern.stop_id = stops.gtfs_id 
     AND direction_pattern.attempt_id = stops.attempt_id
      WHERE ST_DWithin(gtfs.stops.point, 
      'SRID=4326;POINT({} {})', {}) 
      AND allowed_spatial_query = TRUE;
    ",
        query.lon, query.lat, new_spatial_resolution_in_degs
    ));
    let directions_fetch_sql: Result<Vec<DirectionPatternRow>, diesel::result::Error> =
        directions_fetch_query.get_results(conn).await;

    println!(
        "Finished getting direction-stops in {:?}",
        directions_timer.elapsed()
    );

    let directions_lookup_duration = directions_timer.elapsed();

    let directions_rows = directions_fetch_sql.unwrap();

    let mut direction_ids_to_lookup: AHashMap<String, Vec<String>> = AHashMap::new();

    //(chateau, stop_id) -> (direction_id, headsign_idx, stop_sequence)
    let mut stops_to_directions_and_headsigns: AHashMap<
        (String, CompactString),
        Vec<(u64, Option<i16>, u32)>,
    > = AHashMap::new();

    for d in directions_rows {
        let id = d.direction_pattern_id.parse::<u64>().unwrap();

        match stops_to_directions_and_headsigns.entry((d.chateau.clone(), d.stop_id.clone())) {
            Entry::Occupied(mut oe) => {
                let array = oe.get_mut();

                array.push((id, d.stop_headsign_idx, d.stop_sequence));
            }
            Entry::Vacant(mut ve) => {
                ve.insert(vec![(id, d.stop_headsign_idx, d.stop_sequence)]);
            }
        }

        match direction_ids_to_lookup.entry(d.chateau.clone()) {
            Entry::Occupied(mut oe) => {
                let array = oe.get_mut();

                array.push(d.direction_pattern_id.clone());
            }
            Entry::Vacant(mut ve) => {
                ve.insert(vec![d.direction_pattern_id.clone()]);
            }
        }
    }

    let direction_ids_to_lookup = direction_ids_to_lookup;

    // put the stops in sorted order

    let mut sorted_order_stops: Vec<((String, String), f64)> = vec![];

    for s in stops.iter().filter(|stop| stop.point.is_some()) {
        let stop_point = s.point.as_ref().unwrap();

        let stop_point_geo: geo::Point = (stop_point.x, stop_point.y).into();

        let haversine_distance = input_point.haversine_distance(&stop_point_geo);

        if haversine_distance <= rail_and_other_distance_limit as f64 {
            sorted_order_stops.push(((s.chateau.clone(), s.gtfs_id.clone()), haversine_distance))
        }
    }

    sorted_order_stops.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    //sorting finished

    let mut directions_with_headsign_to_closest_stop: AHashMap<
        (String, u64, Option<i16>),
        (CompactString, u32),
    > = AHashMap::new();

    for ((chateau, stop_id), distance_m) in sorted_order_stops.iter() {
        let direction_at_this_stop =
            stops_to_directions_and_headsigns.get(&(chateau.clone(), stop_id.into()));

        if let Some(direction_at_this_stop) = direction_at_this_stop {
            for (direction_id, headsign_idx, stop_sequence) in direction_at_this_stop {
                match directions_with_headsign_to_closest_stop.entry((
                    chateau.clone(),
                    *direction_id,
                    *headsign_idx,
                )) {
                    Entry::Vacant(ve) => {
                        ve.insert((stop_id.into(), *stop_sequence));
                    }
                    _ => {}
                }
            }
        }
    }

    //write some join, select * from itinerary patterns

    //chateau, direction id, stop sequence, stop_id
    let directions_idx_to_get = directions_with_headsign_to_closest_stop
        .iter()
        .map(|(k, v)| (k.0.clone(), k.1.to_string(), v.1, v.0.clone()))
        .collect::<Vec<_>>();

    //chateau -> (direction_id, stop_sequence, stop_id)
    let mut hashmap_of_directions_lookup: AHashMap<String, HashSet<(String, u32, String)>> =
        AHashMap::new();

    for (chateau, direction_id, stop_sequence, stop_id) in directions_idx_to_get {
        match hashmap_of_directions_lookup.entry(chateau.clone()) {
            Entry::Occupied(mut oe) => {
                oe.get_mut()
                    .insert((direction_id, stop_sequence, stop_id.to_string()));
            }
            Entry::Vacant(mut ve) => {
                ve.insert(HashSet::from_iter([(
                    direction_id,
                    stop_sequence,
                    stop_id.to_string(),
                )]));
            }
        }
    }

    /*
    let formatted_ask = format!(
        "({})",
        directions_idx_to_get
            .into_iter()
            .map(|x| format!("('{}','{}',{})", x.0, x.1, x.2))
            .collect::<Vec<String>>()
            .join(",")
    );*/

    let directions_meta_future = 
        futures::stream::iter(direction_ids_to_lookup.iter().map(|(chateau, set_of_directions)| {
            let pool_clone = pool.clone();
            let chateau = chateau.clone();
            let set_of_directions = set_of_directions.clone();
            async move {
                let conn = pool_clone.get().await;
                match conn {
                    Ok(mut conn) => {
                        catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                            .filter(
                                catenary::schema::gtfs::direction_pattern_meta::dsl::chateau
                                    .eq(chateau),
                            )
                            .filter(
                                catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_id
                                    .eq_any(set_of_directions),
                            )
                            .select(catenary::models::DirectionPatternMeta::as_select())
                            .load::<catenary::models::DirectionPatternMeta>(&mut conn)
                            .await
                    }
                    Err(_) => Err(diesel::result::Error::NotFound),
                }
            }
        }))
        .buffer_unordered(32)
        .collect::<Vec<diesel::QueryResult<Vec<DirectionPatternMeta>>>>();

    let itineraries_meta_future = futures::stream::iter(
        hashmap_of_directions_lookup
            .iter()
            .map(|(chateau, set_of_directions)| {
                let pool_clone = pool.clone();
                let chateau = chateau.clone();
                let direction_ids: Vec<String> =
                    set_of_directions.iter().map(|x| x.0.clone()).collect();
                async move {
                    let conn = pool_clone.get().await;
                    match conn {
                        Ok(mut conn) => {
                            catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                                .filter(
                                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::chateau
                                        .eq(chateau),
                                )
                                .filter(
                                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::direction_pattern_id
                                        .eq_any(direction_ids),
                                )
                                .select(catenary::models::ItineraryPatternMeta::as_select())
                                .load::<catenary::models::ItineraryPatternMeta>(&mut conn)
                                .await
                        }
                        Err(_) => Err(diesel::result::Error::NotFound),
                    }
                }
            }),
    )
    .buffer_unordered(32)
    .collect::<Vec<diesel::QueryResult<Vec<ItineraryPatternMeta>>>>();

    let itin_meta_timer = Instant::now();
    let (directions_meta_search_list, itineraries_meta_search_list) =
        tokio::join!(directions_meta_future, itineraries_meta_future);

    let mut direction_meta_lookup_table: AHashMap<String, AHashMap<String, DirectionPatternMeta>> =
        AHashMap::new();

    for result in directions_meta_search_list {
        if let Ok(result) = result {
            for row in result {
                match direction_meta_lookup_table.entry(row.chateau.clone()) {
                    Entry::Occupied(mut oe) => {}
                    Entry::Vacant(mut ve) => {
                        ve.insert(AHashMap::new());
                    }
                }

                let chateau_sub_entry = direction_meta_lookup_table.get_mut(&row.chateau);

                if let Some(chateau_sub_entry) = chateau_sub_entry {
                    chateau_sub_entry.insert(row.direction_pattern_id.clone(), row);
                }
            }
        }
    }

    println!(
        "Finished getting itinerary metas in {:?}",
        itin_meta_timer.elapsed()
    );

    let itineraries_meta_duration = itin_meta_timer.elapsed();

    //chateau -> itinerary_pattern_id -> ItineraryPatternMeta
    let mut itin_meta_table: AHashMap<String, AHashMap<String, ItineraryPatternMeta>> =
        AHashMap::new();
    //chateau -> direction_id -> Vec<itinerary_pattern_id>
    let mut chateau_direction_to_itin_meta_id: AHashMap<String, AHashMap<String, Vec<String>>> =
        AHashMap::new();

    for itineraries_meta_search in itineraries_meta_search_list {
        match itineraries_meta_search {
            Ok(itineraries_meta) => {
                for itinerary_meta in itineraries_meta {
                    if let Some(direction_pattern_id) = &itinerary_meta.direction_pattern_id {
                        match chateau_direction_to_itin_meta_id
                            .entry(itinerary_meta.chateau.clone())
                        {
                            Entry::Occupied(mut oe) => {
                                match oe.get_mut().entry(direction_pattern_id.clone()) {
                                    Entry::Occupied(mut oe) => {
                                        oe.get_mut()
                                            .push(itinerary_meta.itinerary_pattern_id.clone());
                                    }
                                    Entry::Vacant(mut ve) => {
                                        ve.insert(vec![
                                            itinerary_meta.itinerary_pattern_id.clone(),
                                        ]);
                                    }
                                }
                            }
                            Entry::Vacant(mut ve) => {
                                ve.insert(AHashMap::from_iter([(
                                    direction_pattern_id.clone(),
                                    vec![itinerary_meta.itinerary_pattern_id.clone()],
                                )]));
                            }
                        }
                    }

                    match itin_meta_table.entry(itinerary_meta.chateau.clone()) {
                        Entry::Occupied(mut oe) => {
                            oe.get_mut().insert(
                                itinerary_meta.itinerary_pattern_id.clone(),
                                itinerary_meta,
                            );
                        }
                        Entry::Vacant(mut ve) => {
                            ve.insert(AHashMap::from_iter([(
                                itinerary_meta.itinerary_pattern_id.clone(),
                                itinerary_meta,
                            )]));
                        }
                    }
                }
            }
            Err(err) => {
                //do nothing
                eprintln!("{:#?}", err);
            }
        }
    }

    let itin_meta_table = itin_meta_table;

    //make a hashmap of Chateau -> (itinerary_pattern_id, stop_id)
    let mut itineraries_and_seq_to_lookup: AHashMap<String, HashSet<(String, String)>> =
        AHashMap::new();

    for (chateau, set_of_directions) in hashmap_of_directions_lookup.iter() {
        let mut vec_to_insert: HashSet<(String, String)> = HashSet::new();

        for (direction_id, _stop_sequence, stop_id) in set_of_directions.iter() {
            if let Some(itinerary_pattern_ids_in_chateau) =
                chateau_direction_to_itin_meta_id.get(chateau)
            {
                if let Some(itinerary_pattern_ids) =
                    itinerary_pattern_ids_in_chateau.get(direction_id)
                {
                    for itinerary_pattern_id in itinerary_pattern_ids.iter() {
                        vec_to_insert.insert((itinerary_pattern_id.clone(), stop_id.clone()));
                    }
                }
            }
        }

        itineraries_and_seq_to_lookup.insert(chateau.clone(), vec_to_insert);
    }

    //lock the table to prevent further changes
    let itineraries_and_seq_to_lookup = itineraries_and_seq_to_lookup;

    println!("Starting to search for itineraries");

    let itineraries_timer = Instant::now();

    let pool_clone = pool.clone();

    let new_seek_itinerary_list =
        futures::stream::iter(itineraries_and_seq_to_lookup.iter().map(
            |(chateau, set_of_itins)| {
                let conn_clone = pool_clone.clone();
                let chateau = chateau.clone();
                // flatten the hashset to vectors for the query
                let itinerary_ids: Vec<String> = set_of_itins.iter().map(|x| x.0.clone()).collect();
                let stop_ids: Vec<String> = set_of_itins.iter().map(|x| x.1.clone()).collect();
                // We use stops to filter the query, stop_sequence is used for post-filtering

                async move {
                    let conn = conn_clone.get().await;
                    match conn {
                        Ok(mut conn) => {
                            catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                                .filter(
                                    catenary::schema::gtfs::itinerary_pattern::dsl::chateau.eq(chateau),
                                )
                                .filter(
                                    catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern_id
                                        .eq_any(itinerary_ids),
                                )
                                .filter(
                                    catenary::schema::gtfs::itinerary_pattern::dsl::stop_id
                                        .eq_any(stop_ids),
                                )
                                .select(catenary::models::ItineraryPatternRow::as_select())
                                .load::<catenary::models::ItineraryPatternRow>(&mut conn)
                                .await
                        }
                        Err(_) => Err(diesel::result::Error::NotFound),
                    }
                }
            },
        ))
        .buffer_unordered(32)
        .collect::<Vec<diesel::QueryResult<Vec<ItineraryPatternRow>>>>()
        .await;

    println!(
        "Finished getting itineraries in {:?}",
        itineraries_timer.elapsed()
    );

    let itinerary_duration = itineraries_timer.elapsed();

    // println!("Itins: {:#?}", seek_for_itineraries);

    let mut itins_per_chateau: AHashMap<String, AHashSet<String>> = AHashMap::new();

    //(chateau, itinerary_pattern_id) -> ItineraryPatternRowMerge
    let mut itinerary_table: AHashMap<(String, String), Vec<ItineraryPatternRowMerge>> =
        AHashMap::new();

    for seek_for_itineraries in new_seek_itinerary_list {
        match seek_for_itineraries {
            Ok(itineraries) => {
                for itinerary in itineraries {
                    // Filter in memory to ensure we match the requested (itinerary_pattern_id, stop_id)
                    // This handles the case where stop_id IN (...) might return rows for non-requested combinations
                    let chateau = itinerary.chateau.clone();
                    if let Some(requested_set) = itineraries_and_seq_to_lookup.get(&chateau) {
                        if !requested_set.contains(&(
                            itinerary.itinerary_pattern_id.clone(),
                            itinerary.stop_id.to_string(),
                        )) {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    match itins_per_chateau.entry(itinerary.chateau.clone()) {
                        Entry::Occupied(mut oe) => {
                            oe.get_mut().insert(itinerary.itinerary_pattern_id.clone());
                        }
                        Entry::Vacant(mut ve) => {
                            ve.insert(AHashSet::from_iter([itinerary
                                .itinerary_pattern_id
                                .clone()]));
                        }
                    }

                    let itin_meta_ref = itin_meta_table
                        .get(&itinerary.chateau)
                        .unwrap()
                        .get(&itinerary.itinerary_pattern_id)
                        .unwrap();

                    if let Some(itinerary_direction_id) = &itin_meta_ref.direction_pattern_id {
                        let new_itinerary = ItineraryPatternRowMerge {
                            onestop_feed_id: itinerary.onestop_feed_id.clone(),
                            attempt_id: itinerary.attempt_id.clone(),
                            itinerary_pattern_id: itinerary.itinerary_pattern_id.clone(),
                            arrival_time_since_start: itinerary.arrival_time_since_start,
                            departure_time_since_start: itinerary.departure_time_since_start,
                            interpolated_time_since_start: itinerary.interpolated_time_since_start,
                            stop_id: itinerary.stop_id.clone(),
                            chateau: itinerary.chateau.clone().into(),
                            gtfs_stop_sequence: itinerary.gtfs_stop_sequence,
                            direction_pattern_id: itinerary_direction_id.clone(),
                            trip_headsign: itin_meta_ref.trip_headsign.clone(),
                            trip_headsign_translations: itin_meta_ref
                                .trip_headsign_translations
                                .clone(),
                            timezone: itin_meta_ref.timezone.clone(),
                            route_id: itin_meta_ref.route_id.clone(),
                            stop_headsign_idx: itinerary.stop_headsign_idx,
                        };

                        match itinerary_table.entry((
                            itinerary.chateau.clone(),
                            itinerary.itinerary_pattern_id.clone(),
                        )) {
                            Entry::Occupied(mut oe) => {
                                oe.get_mut().push(new_itinerary);
                            }
                            Entry::Vacant(mut ve) => {
                                ve.insert(vec![new_itinerary]);
                            }
                        }
                    }
                }
            }
            Err(err) => {
                return HttpResponse::InternalServerError().body(format!("{:#?}", err));
            }
        }
    }

    println!("Looking up trips");
    let timer_trips = Instant::now();

    let trip_lookup_queries_to_perform =
        futures::stream::iter(itins_per_chateau.iter().flat_map(|(chateau, set_of_itin)| {
            let chunk_size = 1024;
            let itins_vec: Vec<String> = set_of_itin.iter().cloned().collect();
            let chunks: Vec<Vec<String>> =
                itins_vec.chunks(chunk_size).map(|c| c.to_vec()).collect();
            let pool_super = pool.clone();
            let chateau_super = chateau.clone();

            chunks.into_iter().map(move |chunk| {
                let pool = pool_super.clone();
                let chateau = chateau_super.clone();
                async move {
                    let mut conn = pool.get().await.unwrap();
                    catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                        .filter(catenary::schema::gtfs::trips_compressed::dsl::chateau.eq(chateau))
                        .filter(
                            catenary::schema::gtfs::trips_compressed::dsl::itinerary_pattern_id
                                .eq_any(chunk),
                        )
                        .select(catenary::models::CompressedTrip::as_select())
                        .load::<catenary::models::CompressedTrip>(&mut conn)
                        .await
                }
            })
        }))
        .buffer_unordered(8)
        .collect::<Vec<diesel::QueryResult<Vec<catenary::models::CompressedTrip>>>>()
        .await;

    let trip_lookup_elapsed = timer_trips.elapsed();

    println!("Finished looking up trips in {:?}", timer_trips.elapsed());

    let mut compressed_trips_table: AHashMap<String, Vec<CompressedTrip>> = AHashMap::new();

    let mut services_to_lookup_table: AHashMap<String, BTreeSet<CompactString>> = AHashMap::new();

    let mut routes_to_lookup_table: AHashMap<String, BTreeSet<String>> = AHashMap::new();

    for trip_group in trip_lookup_queries_to_perform {
        match trip_group {
            Ok(compressed_trip_group) => {
                if compressed_trip_group.is_empty() {
                    continue;
                }
                let chateau = compressed_trip_group[0].chateau.to_string();

                let service_ids = compressed_trip_group
                    .iter()
                    .map(|x| x.service_id.clone())
                    .collect::<BTreeSet<CompactString>>();

                let route_ids = compressed_trip_group
                    .iter()
                    .map(|x| x.route_id.clone())
                    .collect::<BTreeSet<String>>();

                services_to_lookup_table
                    .entry(chateau.clone())
                    .or_default()
                    .extend(service_ids);
                compressed_trips_table
                    .entry(chateau.clone())
                    .or_default()
                    .extend(compressed_trip_group);
                routes_to_lookup_table
                    .entry(chateau)
                    .or_default()
                    .extend(route_ids);
            }
            Err(err) => {
                return HttpResponse::InternalServerError().body(format!("{:#?}", err));
            }
        }
    }

    let compressed_trips_table = compressed_trips_table;
    let services_to_lookup_table = services_to_lookup_table;

    let chateaux = services_to_lookup_table
        .keys()
        .cloned()
        .collect::<Vec<String>>();

    let calendar_timer = Instant::now();

    let (
        services_calendar_lookup_queries_to_perform,
        services_calendar_dates_lookup_queries_to_perform,
        routes_query,
    ) =
        tokio::join!(
            futures::stream::iter(services_to_lookup_table.iter().map(
                |(chateau, set_of_calendar)| {
                    catenary::schema::gtfs::calendar::dsl::calendar
                        .filter(catenary::schema::gtfs::calendar::dsl::chateau.eq(chateau))
                        .filter(
                            catenary::schema::gtfs::calendar::dsl::service_id
                                .eq_any(set_of_calendar),
                        )
                        .select(catenary::models::Calendar::as_select())
                        .load::<catenary::models::Calendar>(conn)
                },
            ))
            .buffer_unordered(16)
            .collect::<Vec<diesel::QueryResult<Vec<catenary::models::Calendar>>>>(),
            futures::stream::iter(services_to_lookup_table.iter().map(
                |(chateau, set_of_calendar)| {
                    catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                        .filter(catenary::schema::gtfs::calendar_dates::dsl::chateau.eq(chateau))
                        .filter(
                            catenary::schema::gtfs::calendar_dates::dsl::service_id
                                .eq_any(set_of_calendar),
                        )
                        .select(catenary::models::CalendarDate::as_select())
                        .load::<catenary::models::CalendarDate>(conn2)
                },
            ))
            .buffer_unordered(16)
            .collect::<Vec<diesel::QueryResult<Vec<catenary::models::CalendarDate>>>>(),
            futures::stream::iter(
                routes_to_lookup_table
                    .iter()
                    .map(|(chateau, set_of_routes)| {
                        catenary::schema::gtfs::routes::dsl::routes
                            .filter(catenary::schema::gtfs::routes::dsl::chateau.eq(chateau))
                            .filter(
                                catenary::schema::gtfs::routes::dsl::route_id.eq_any(set_of_routes),
                            )
                            .select(catenary::models::Route::as_select())
                            .load::<catenary::models::Route>(conn3)
                    })
            )
            .buffer_unordered(16)
            .collect::<Vec<diesel::QueryResult<Vec<catenary::models::Route>>>>(),
        );

    let calendar_timer_finish = calendar_timer.elapsed();

    println!(
        "Finished getting calendar, routes, and calendar dates, took {:?}",
        calendar_timer_finish
    );

    let calendar_structure = make_calendar_structure_from_pg(
        services_calendar_lookup_queries_to_perform
            .into_iter()
            .filter_map(|x| x.ok())
            .collect::<Vec<Vec<catenary::models::Calendar>>>(),
        services_calendar_dates_lookup_queries_to_perform
            .into_iter()
            .filter_map(|x| x.ok())
            .collect::<Vec<Vec<catenary::models::CalendarDate>>>(),
    );

    let mut routes_table: AHashMap<String, AHashMap<String, catenary::models::Route>> =
        AHashMap::new();

    for route_group in routes_query {
        match route_group {
            Ok(route_group) => {
                let chateau = route_group[0].chateau.clone();

                let mut route_table = AHashMap::new();

                for route in route_group {
                    route_table.insert(route.route_id.clone(), route);
                }

                routes_table.insert(chateau, route_table);
            }
            Err(err) => {
                return HttpResponse::InternalServerError().body(format!("{:#?}", err));
            }
        }
    }

    let mut chateau_metadata = AHashMap::new();

    let etcd_timer = Instant::now();

    for chateau_id in chateaux {
        let etcd_data = etcd
            .get(
                format!("/aspen_assigned_chateaux/{}", chateau_id.clone()).as_str(),
                None,
            )
            .await;

        if let Ok(etcd_data) = etcd_data {
            if let Some(first_value) = etcd_data.kvs().first() {
                let this_chateau_metadata =
                    catenary::bincode_deserialize::<ChateauMetadataEtcd>(first_value.value())
                        .unwrap();

                chateau_metadata.insert(chateau_id.clone(), this_chateau_metadata);
            }
        }
    }

    let etcd_time_elapsed = etcd_timer.elapsed();

    let mut aspen_data_fetch_time_elapsed: Option<std::time::Duration> = None;
    let chateau_metadata = chateau_metadata;

    match calendar_structure {
        Err(err) => HttpResponse::InternalServerError().body("CANNOT FIND CALENDARS"),
        Ok(calendar_structure) => {
            // iterate through all trips and produce a timezone and timeoffset.

            let mut stops_answer: AHashMap<String, AHashMap<CompactString, StopOutput>> =
                AHashMap::new();
            let mut departures: Vec<DepartureRouteGroup> = vec![];
            let mut all_alerts: BTreeMap<
                String,
                BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>,
            > = BTreeMap::new();

            for (chateau_id, calendar_in_chateau) in calendar_structure.iter() {
                let mut directions_route_group_for_this_chateau: AHashMap<
                    String,
                    DepartureRouteGroup,
                > = AHashMap::new();

                let mut valid_trips: AHashMap<String, Vec<ValidTripSet>> = AHashMap::new();
                let itinerary = itins_per_chateau.get(chateau_id).unwrap();
                let routes = routes_table.get(chateau_id).unwrap();
                for trip in compressed_trips_table.get(chateau_id).unwrap() {
                    //extract protobuf of frequency and convert to gtfs_structures::Frequency

                    let frequency: Option<catenary::gtfs_schedule_protobuf::GtfsFrequenciesProto> =
                        trip.frequencies
                            .as_ref()
                            .map(|data| prost::Message::decode(data.as_ref()).unwrap());

                    let freq_converted = frequency.map(|x| protobuf_to_frequencies(&x));

                    let this_itin_list = itinerary_table
                        .get(&(trip.chateau.clone(), trip.itinerary_pattern_id.clone()))
                        .unwrap();

                    let itin_ref: ItineraryPatternRowMerge = this_itin_list[0].clone();

                    let time_since_start = match itin_ref.departure_time_since_start {
                        Some(departure_time_since_start) => departure_time_since_start,
                        None => match itin_ref.arrival_time_since_start {
                            Some(arrival) => arrival,
                            None => itin_ref.interpolated_time_since_start.unwrap_or(0),
                        },
                    };

                    let t_to_find_schedule_for = catenary::TripToFindScheduleFor {
                        trip_id: trip.trip_id.clone(),
                        chateau: chateau_id.clone(),
                        timezone: chrono_tz::Tz::from_str(itin_ref.timezone.as_str()).unwrap(),
                        time_since_start_of_service_date: chrono::TimeDelta::new(
                            time_since_start.into(),
                            0,
                        )
                        .unwrap(),
                        frequency: freq_converted.clone(),
                        itinerary_id: itin_ref.itinerary_pattern_id.clone(),
                        direction_id: itin_ref.direction_pattern_id.clone(),
                    };

                    let service = calendar_in_chateau.get(trip.service_id.as_str());

                    if let Some(service) = service {
                        let dates = catenary::find_service_ranges(
                            service,
                            &t_to_find_schedule_for,
                            departure_time_chrono,
                            seek_back,
                            seek_forward,
                        );

                        if !dates.is_empty() {
                            for date in dates {
                                let t = ValidTripSet {
                                    chateau_id: chateau_id.clone(),
                                    trip_id: (&trip.trip_id).into(),
                                    timezone: chrono_tz::Tz::from_str(itin_ref.timezone.as_str())
                                        .ok(),
                                    frequencies: freq_converted.clone(),
                                    trip_service_date: date.0,
                                    itinerary_options: this_itin_list.clone(),
                                    reference_start_of_service_date: date.1,
                                    itinerary_pattern_id: itin_ref.itinerary_pattern_id.clone(),
                                    direction_pattern_id: itin_ref.direction_pattern_id.clone(),
                                    route_id: itin_ref.route_id.clone(),
                                    trip_start_time: trip.start_time,
                                    trip_short_name: trip.trip_short_name.clone(),
                                    service_id: trip.service_id.clone(),
                                };

                                match valid_trips.entry(trip.trip_id.clone()) {
                                    Entry::Occupied(mut oe) => {
                                        oe.get_mut().push(t);
                                    }
                                    Entry::Vacant(mut ve) => {
                                        ve.insert(vec![t]);
                                    }
                                }
                            }
                        }
                    }
                }

                // Hydrate into realtime data

                //1. connect with tarpc server

                let aspen_data_fetch_timer = Instant::now();
                let scc_aspen_client_reuse: scc::HashMap<std::net::SocketAddr, AspenRpcClient> =
                    scc::HashMap::new();

                let (gtfs_trips_aspenised, all_alerts_for_chateau) = match chateau_metadata
                    .get(chateau_id)
                {
                    Some(chateau_metadata_for_c) => {
                        let aspen_client = match scc_aspen_client_reuse
                            .get_async(&chateau_metadata_for_c.socket)
                            .await
                        {
                            Some(aspen_client) => {
                                let aspen_client = aspen_client.get();

                                Some(aspen_client.clone())
                            }
                            None => {
                                let aspen_client_res =
                                    catenary::aspen::lib::spawn_aspen_client_from_ip(
                                        &chateau_metadata_for_c.socket,
                                    )
                                    .await;

                                match aspen_client_res {
                                    Ok(aspen_client) => {
                                        let _ = scc_aspen_client_reuse
                                            .insert_async(
                                                chateau_metadata_for_c.socket,
                                                aspen_client.clone(),
                                            )
                                            .await;
                                        Some(aspen_client)
                                    }
                                    Err(e) => {
                                        eprintln!("Error connecting to aspen: {:#?}", e);
                                        None
                                    }
                                }
                            }
                        };

                        match aspen_client {
                            Some(aspen_client) => {
                                let gtfs_fut = aspen_client.get_all_trips_with_ids(
                                    tarpc::context::current(),
                                    chateau_id.clone(),
                                    valid_trips.keys().cloned().collect::<Vec<String>>(),
                                );
                                let alerts_fut = aspen_client
                                    .get_all_alerts(tarpc::context::current(), chateau_id.clone());

                                let (gtfs_trip_aspenised_res, all_alerts_res) =
                                    tokio::join!(gtfs_fut, alerts_fut);

                                let trips = match gtfs_trip_aspenised_res {
                                    Ok(gtfs_trip_aspenised) => gtfs_trip_aspenised,
                                    Err(err) => {
                                        eprintln!(
                                            "Error getting trip updates, line 1010: {:#?}",
                                            err
                                        );
                                        None
                                    }
                                };

                                let alerts = match all_alerts_res {
                                    Ok(Some(all_alerts)) => Some(all_alerts),
                                    Ok(None) => None,
                                    Err(err) => {
                                        eprintln!("Error getting alerts: {:#?}", err);
                                        None
                                    }
                                };
                                (trips, alerts)
                            }
                            None => (None, None),
                        }
                    }
                    None => (None, None),
                };

                aspen_data_fetch_time_elapsed = Some(aspen_data_fetch_timer.elapsed());

                if let Some(alerts_for_chateau) = &all_alerts_for_chateau {
                    if !alerts_for_chateau.is_empty() {
                        let mut btree_alerts = BTreeMap::new();
                        for (k, v) in alerts_for_chateau {
                            btree_alerts.insert(k.clone(), v.clone());
                        }
                        all_alerts.insert(chateau_id.clone(), btree_alerts);
                    }
                }

                //sort through each time response

                //  temp_answer.insert(chateau_id.clone(), valid_trips);

                for (trip_id, trip_grouping) in valid_trips {
                    let route = routes.get(trip_grouping[0].route_id.as_str());

                    if route.is_none() {
                        continue;
                    }

                    let route = route.unwrap();

                    if !directions_route_group_for_this_chateau.contains_key(&route.route_id) {
                        directions_route_group_for_this_chateau.insert(
                            route.route_id.clone(),
                            DepartureRouteGroup {
                                chateau_id: chateau_id.clone(),
                                route_id: (&route.route_id).into(),
                                color: route.color.as_ref().map(|x| x.into()),
                                text_color: route.text_color.as_ref().map(|x| x.into()),
                                short_name: route.short_name.as_ref().map(|x| x.into()),
                                long_name: route.long_name.clone(),
                                route_type: route.route_type,
                                directions: AHashMap::new(),
                                closest_distance: 100000.,
                            },
                        );
                    }

                    let route_group = directions_route_group_for_this_chateau
                        .get_mut(&route.route_id)
                        .unwrap();

                    let direction = direction_meta_lookup_table
                        .get(chateau_id.as_str())
                        .unwrap()
                        .get(&trip_grouping[0].direction_pattern_id)
                        .unwrap();

                    let mut already_used_trip_update_id: ahash::AHashSet<String> =
                        ahash::AHashSet::new();

                    for trip in &trip_grouping {
                        if trip.frequencies.is_none() {
                            let mut split_by_headsign_idx: BTreeMap<Option<i16>, Vec<&_>> =
                                BTreeMap::new();

                            for option in &trip.itinerary_options {
                                match split_by_headsign_idx.entry(option.stop_headsign_idx) {
                                    std::collections::btree_map::Entry::Occupied(mut oe) => {
                                        let list_of_options = oe.get_mut();

                                        list_of_options.push(option);
                                    }
                                    std::collections::btree_map::Entry::Vacant(ve) => {
                                        ve.insert(vec![option]);
                                    }
                                }
                            }

                            for (headsign_idx, itinerary_options) in split_by_headsign_idx {
                                let departure_key =
                                    (trip.direction_pattern_id.clone(), headsign_idx.clone());

                                if !route_group.directions.contains_key(&departure_key) {
                                    route_group.directions.insert(
                                        departure_key.clone(),
                                        DepartingHeadsignGroup {
                                            headsign: match headsign_idx {
                                                Some(idx) => direction
                                                    .stop_headsigns_unique_list
                                                    .as_ref()
                                                    .unwrap()
                                                    .get(idx as usize)
                                                    .unwrap()
                                                    .as_ref()
                                                    .unwrap()
                                                    .clone(),
                                                None => itinerary_options[0]
                                                    .trip_headsign
                                                    .clone()
                                                    .unwrap_or("".to_string()),
                                            },
                                            direction_id: trip_grouping[0]
                                                .direction_pattern_id
                                                .clone(),
                                            trips: vec![],
                                        },
                                    );
                                }

                                let headsign_group =
                                    route_group.directions.get_mut(&departure_key).unwrap();

                                let mut is_cancelled: bool = false;
                                let mut deleted: bool = false;

                                let mut departure_time_rt: Option<u64> = None;
                                let mut platform: Option<String> = None;

                                let stop = stops
                                    .iter()
                                    .find(|x| x.gtfs_id == trip.itinerary_options[0].stop_id);

                                if let Some(stop) = stop {
                                    if let Some(platform_code) = &stop.platform_code {
                                        platform = Some(platform_code.clone());
                                    }
                                }

                                if let Some(gtfs_trip_aspenised) = gtfs_trips_aspenised.as_ref() {
                                    if let Some(trip_update_ids) = gtfs_trip_aspenised
                                        .trip_id_to_trip_update_ids
                                        .get(trip.trip_id.as_str())
                                    {
                                        if !trip_update_ids.is_empty() {
                                            // let trip_update_id = trip_rt[0].clone();

                                            let does_trip_set_use_dates = gtfs_trip_aspenised
                                                .trip_updates
                                                .get(&trip_update_ids[0])
                                                .unwrap()
                                                .trip
                                                .start_date
                                                .is_some();

                                            let trip_updates: Vec<(&String, &AspenisedTripUpdate)> =
                                        trip_update_ids
                                            .iter()
                                            .map(|x| {
                                                (
                                                    x,
                                                    gtfs_trip_aspenised
                                                        .trip_updates
                                                        .get(x)
                                                        .unwrap(),
                                                )
                                            })
                                            .filter(
                                                |(x, trip_update)| match does_trip_set_use_dates {
                                                    true => {
                                                        trip_update.trip.start_date
                                                            == Some(
                                                                trip.trip_service_date
                                                            )
                                                    }
                                                    false => {
                                                        //if there is only 1 trip update, assign it to the current service date

                                                        //what is the current trip offset from the reference start of service date

                                                        let trip_offset = itinerary_options[0].departure_time_since_start.unwrap_or(itinerary_options[0].arrival_time_since_start.unwrap_or(itinerary_options[0].interpolated_time_since_start.unwrap_or(0))) as u64;

                                                        // get current naive date in the timezone from the earliest item in the trip update

                                                        let naive_date_approx_guess = trip_update.stop_time_update.iter()
                                                        .filter(|x| x.departure.is_some() || x.arrival.is_some())
                                                        .filter_map(|x| {
                                                            if let Some(departure) = &x.departure {
                                                                Some(departure.time)
                                                            } else {
                                                                if let Some(arrival) = &x.arrival {
                                                                    Some(arrival.time)
                                                                } else {
                                                                    None
                                                                }
                                                            }
                                                        }).flatten().min();

                                                        match naive_date_approx_guess {
                                                            Some(least_num) => {
                                                                let tz = trip.timezone.as_ref().unwrap();

                                                                let rt_least_naive_date = tz.timestamp(least_num as i64, 0);

                                                                let approx_service_date_start = rt_least_naive_date - chrono::Duration::seconds(trip_offset as i64);

                                                                let approx_service_date = approx_service_date_start.date();

                                                                //score dates within 1 day of the service date
                                                                let mut vec_possible_dates: Vec<(chrono::Date<chrono_tz::Tz>, i64)> = vec![];

                                                                //iter from day before to day after

                                                                let day_before = approx_service_date - chrono::Duration::days(2);

                                                                for day in day_before.naive_local().iter_days().take(3) {
                                                                   //check service id for trip id, then check if calendar is allowed

                                                                     let service_id = trip.service_id.as_str();

                                                                    let service = calendar_in_chateau.get(service_id);

                                                                    if let Some(service) = service {
                                                                        if catenary::datetime_in_service(&service, day) {
                                                                            let day_in_tz_midnight = day.and_hms(12, 0, 0).and_local_timezone(*tz).unwrap() - chrono::Duration::hours(12);

                                                                            let time_delta = rt_least_naive_date.signed_duration_since(day_in_tz_midnight);
    
                                                                            vec_possible_dates.push((day_in_tz_midnight.date(), time_delta.num_seconds().abs()));
                                                                        }
                                                                    }
                                                                }

                                                                let best_service_date = vec_possible_dates.iter().min_by_key(|x| x.1);

                                                                match best_service_date {
                                                                    Some(best_service_date) => {
                                                                        trip.trip_service_date == best_service_date.0.naive_local()
                                                                    },
                                                                    None => {
                                                                        false
                                                                    }
                                                                }

                                                            },
                                                            None => {
                                                                false
                                                            }
                                                        }
                                                    },
                                                },
                                            )
                                           //.filter(|(x, trip_update)| {
                                           //     !already_used_trip_update_id.contains(*x)
                                           // })
                                            .collect();

                                            if trip_updates.len() > 0 {
                                                let trip_update = trip_updates[0].1;
                                                let trip_update_id = trip_updates[0].0;

                                                if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                            is_cancelled = true;
                                        } else if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted)
                                        {
                                            deleted = true;
                                        } else {
                                            let relevant_stop_time_update =
                                                trip_update.stop_time_update.iter().find(|x| {
                                                    x.stop_id
                                                        .as_ref()
                                                        .map(|compare| compare.as_str())
                                                        == Some(&itinerary_options[0].stop_id)
                                                });

                                            if let Some(relevant_stop_time_update) =
                                                relevant_stop_time_update
                                            {
                                                if let Some(departure) =
                                                    &relevant_stop_time_update.departure
                                                {
                                                    if let Some(time) = departure.time {
                                                        departure_time_rt = Some(time as u64);
                                                    }
                                                } else {
                                                    if let Some(arrival) =
                                                        &relevant_stop_time_update.arrival
                                                    {
                                                        if let Some(time) = arrival.time {
                                                            departure_time_rt = Some(time as u64);
                                                        }
                                                    }
                                                }

                                                if let Some(platform_id) =
                                                    &relevant_stop_time_update.platform_string
                                                {
                                                    platform = Some(platform_id.to_string());
                                                }
                                            }
                                                                                        }
                                            }
                                        }
                                    }
                                }

                                if let Some(chateau_alerts) = all_alerts_for_chateau.as_ref() {
                                    for alert in chateau_alerts.values() {
                                        if is_cancelled {
                                            break;
                                        }
                                        let effect_is_no_service = alert.effect == Some(1); // NO_SERVICE
                                        if effect_is_no_service {
                                            let applies_to_trip =
                                                alert.informed_entity.iter().any(|e| {
                                                    let route_match =
                                                        e.route_id.as_ref().map_or(false, |r_id| {
                                                            *r_id == trip.route_id
                                                        });
                                                    let trip_match =
                                                        e.trip.as_ref().map_or(false, |t| {
                                                            t.trip_id
                                                                .as_ref()
                                                                .map_or(false, |t_id| {
                                                                    *t_id == trip.trip_id
                                                                })
                                                        });
                                                    route_match || trip_match
                                                });

                                            if applies_to_trip {
                                                let event_time_opt = match itinerary_options[0]
                                                    .departure_time_since_start
                                                {
                                                    Some(departure_time_since_start) => Some(
                                                        trip.reference_start_of_service_date
                                                            .timestamp()
                                                            as u64
                                                            + trip.trip_start_time as u64
                                                            + departure_time_since_start as u64,
                                                    ),
                                                    None => None,
                                                };

                                                let applies_to_trip_without_a_referenced_stop =
                                                    alert
                                                        .informed_entity
                                                        .iter()
                                                        .filter(|e| e.stop_id.is_none())
                                                        .any(|e| {
                                                            let route_match = e
                                                                .route_id
                                                                .as_ref()
                                                                .map_or(false, |r_id| {
                                                                    *r_id == trip.route_id
                                                                });
                                                            let trip_match = e
                                                                .trip
                                                                .as_ref()
                                                                .map_or(false, |t| {
                                                                    t.trip_id.as_ref().map_or(
                                                                        false,
                                                                        |t_id| {
                                                                            *t_id == trip.trip_id
                                                                        },
                                                                    )
                                                                });

                                                            route_match || trip_match
                                                        });

                                                if let Some(event_time) = event_time_opt {
                                                    let is_active =
                                                        alert.active_period.iter().any(|ap| {
                                                            let start = ap.start.unwrap_or(0);
                                                            let end = ap.end.unwrap_or(u64::MAX);
                                                            event_time >= start && event_time <= end
                                                        });
                                                    if is_active {
                                                        if alert.effect == Some(1) {
                                                            if (applies_to_trip_without_a_referenced_stop) {
                                                                is_cancelled = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                headsign_group.trips.push(DepartingStopTime {
                                    trip_id: trip.trip_id.clone(),
                                    gtfs_schedule_start_day: trip.trip_service_date,
                                    departure_realtime: departure_time_rt,
                                    arrival_schedule: None,
                                    arrival_realtime: None,
                                    stop_id: itinerary_options[0].stop_id.clone(),
                                    trip_short_name: trip.trip_short_name.clone(),
                                    trip_tz: trip.timezone.as_ref().unwrap().name().to_string(),
                                    is_frequency: trip.frequencies.is_some(),
                                    platform: platform,
                                    level_id: None,
                                    departure_schedule: match itinerary_options[0]
                                        .departure_time_since_start
                                    {
                                        Some(departure_time_since_start) => Some(
                                            trip.reference_start_of_service_date.timestamp() as u64
                                                + trip.trip_start_time as u64
                                                + departure_time_since_start as u64,
                                        ),
                                        None => {
                                            match itinerary_options[0].arrival_time_since_start {
                                                Some(arrival) => Some(
                                                    trip.reference_start_of_service_date.timestamp()
                                                        as u64
                                                        + trip.trip_start_time as u64
                                                        + arrival as u64,
                                                ),
                                                None => match itinerary_options[0]
                                                    .interpolated_time_since_start
                                                {
                                                    Some(interpolated) => Some(
                                                        trip.reference_start_of_service_date
                                                            .timestamp()
                                                            as u64
                                                            + trip.trip_start_time as u64
                                                            + interpolated as u64,
                                                    ),
                                                    None => None,
                                                },
                                            }
                                        }
                                    },
                                    is_interpolated: itinerary_options[0]
                                        .interpolated_time_since_start
                                        .is_some(),
                                    gtfs_frequency_start_time: None,
                                    cancelled: is_cancelled,
                                    deleted: deleted,
                                });
                            }
                        } else {
                            //duplicate trip based on frequency

                            let temp_key = (trip_grouping[0].direction_pattern_id.clone(), None);

                            if !route_group.directions.contains_key(&temp_key) {
                                route_group.directions.insert(
                                    temp_key.clone(),
                                    DepartingHeadsignGroup {
                                        headsign: trip_grouping[0].itinerary_options[0]
                                            .trip_headsign
                                            .clone()
                                            .unwrap_or("".to_string()),
                                        direction_id: trip_grouping[0].direction_pattern_id.clone(),
                                        trips: vec![],
                                    },
                                );
                            }

                            let headsign_group = route_group.directions.get_mut(&temp_key).unwrap();

                            let frequencies = trip.frequencies.as_ref().unwrap();

                            //trip start time array

                            let mut trip_start_times: Vec<u32> = frequencies
                                .iter()
                                .flat_map(|frequency| {
                                    (frequency.start_time..=frequency.end_time)
                                        .step_by(frequency.headway_secs as usize)
                                        .collect::<Vec<u32>>()
                                })
                                .collect();

                            trip_start_times.sort();
                            trip_start_times.dedup();

                            let trip_start_times = trip_start_times;

                            for scheduled_frequency_start_time in trip_start_times {
                                let mut is_cancelled: bool = false;
                                let mut deleted: bool = false;
                                let mut departure_time_rt: Option<u64> = None;
                                let mut platform: Option<String> = None;

                                let stop = stops
                                    .iter()
                                    .find(|x| x.gtfs_id == trip.itinerary_options[0].stop_id);

                                if let Some(stop) = stop {
                                    if let Some(platform_code) = &stop.platform_code {
                                        platform = Some(platform_code.clone());
                                    }
                                }

                                if let Some(gtfs_trip_aspenised) = gtfs_trips_aspenised.as_ref() {
                                    if let Some(trip_update_ids) = gtfs_trip_aspenised
                                        .trip_id_to_trip_update_ids
                                        .get(trip.trip_id.as_str())
                                    {
                                        if !trip_update_ids.is_empty() {
                                            // let trip_update_id = trip_rt[0].clone();

                                            let does_trip_set_use_dates = gtfs_trip_aspenised
                                                .trip_updates
                                                .get(&trip_update_ids[0])
                                                .unwrap()
                                                .trip
                                                .start_date
                                                .is_some();

                                            let trip_updates: Vec<(&String, &AspenisedTripUpdate)> =
                                            trip_update_ids
                                                .iter()
                                                .map(|x| {
                                                    (
                                                        x,
                                                        gtfs_trip_aspenised
                                                            .trip_updates
                                                            .get(x)
                                                            .unwrap(),
                                                    )
                                                })
                                                .filter(|(x, trip_update)| 
                                                    {
                                                        trip_update
                                                            .trip
                                                            .start_time
                                                            .as_ref()
                                                            .and_then(|trip_update_start_time| {
                                                                catenary::convert_hhmmss_to_seconds(
                                                                    trip_update_start_time,
                                                                )
                                                            })
                                                            .map_or(false, |seconds| {
                                                                scheduled_frequency_start_time == seconds
                                                            })
                                                    }
                                                )
                                                    
                                                .filter(
                                                    |(x, trip_update)| match does_trip_set_use_dates {
                                                        true => {
                                                            trip_update.trip.start_date
                                                                == Some(
                                                                    trip.trip_service_date
                                                                )
                                                        }
                                                        false => {
                                                            //if there is only 1 trip update, assign it to the current service date
    
                                                            //what is the current trip offset from the reference start of service date
    
                                                            let trip_offset = trip.itinerary_options[0].departure_time_since_start.unwrap_or(trip.itinerary_options[0].arrival_time_since_start.unwrap_or(trip.itinerary_options[0].interpolated_time_since_start.unwrap_or(0))) as u64;
    
                                                            // get current naive date in the timezone from the earliest item in the trip update
    
                                                            let naive_date_approx_guess = trip_update.stop_time_update.iter()
                                                            .filter(|x| x.departure.is_some() || x.arrival.is_some())
                                                            .filter_map(|x| {
                                                                if let Some(departure) = &x.departure {
                                                                    Some(departure.time)
                                                                } else {
                                                                    if let Some(arrival) = &x.arrival {
                                                                        Some(arrival.time)
                                                                    } else {
                                                                        None
                                                                    }
                                                                }
                                                            }).flatten().min();
    
                                                            match naive_date_approx_guess {
                                                                Some(least_num) => {
                                                                    let tz = trip.timezone.as_ref().unwrap();
    
                                                                    let rt_least_naive_date = tz.timestamp_opt(least_num as i64, 0).unwrap();
    
                                                                    let approx_service_date_start = rt_least_naive_date - chrono::Duration::seconds(trip_offset as i64);
    
                                                                    let approx_service_date = approx_service_date_start.date();
    
                                                                    //score dates within 1 day of the service date
                                                                    let mut vec_possible_dates: Vec<(chrono::Date<chrono_tz::Tz>, i64)> = vec![];
    
                                                                    //iter from day before to day after
    
                                                                    let day_before = approx_service_date - chrono::Duration::days(2);
    
                                                                    for day in day_before.naive_local().iter_days().take(3) {
                                                                       //check service id for trip id, then check if calendar is allowed
    
                                                                         let service_id = trip.service_id.as_str();
    
                                                                        let service = calendar_in_chateau.get(service_id);
    
                                                                        if let Some(service) = service {
                                                                            if catenary::datetime_in_service(&service, day) {
                                                                                let day_in_tz_midnight = day.and_hms(12, 0, 0).and_local_timezone(*tz).unwrap() - chrono::Duration::hours(12);
    
                                                                                let time_delta = rt_least_naive_date.signed_duration_since(day_in_tz_midnight);
        
                                                                                vec_possible_dates.push((day_in_tz_midnight.date(), time_delta.num_seconds().abs()));
                                                                            }
                                                                        }
                                                                    }
    
                                                                    let best_service_date = vec_possible_dates.iter().min_by_key(|x| x.1);
    
                                                                    match best_service_date {
                                                                        Some(best_service_date) => {
                                                                            trip.trip_service_date == best_service_date.0.naive_local()
                                                                        },
                                                                        None => {
                                                                            false
                                                                        }
                                                                    }
    
                                                                },
                                                                None => {
                                                                    false
                                                                }
                                                            }
                                                        },
                                                    },
                                                )
                                               //.filter(|(x, trip_update)| {
                                               //     !already_used_trip_update_id.contains(*x)
                                               // })
                                                .collect();

                                            if trip_updates.len() > 0 {
                                                let trip_update = trip_updates[0].1;
                                                let trip_update_id = trip_updates[0].0;

                                                if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Cancelled) {
                                                is_cancelled = true;
                                            } else if trip_update.trip.schedule_relationship == Some(catenary::aspen_dataset::AspenisedTripScheduleRelationship::Deleted)
                                            {
                                                deleted = true;
                                            } else {
                                                let relevant_stop_time_update =
                                                    trip_update.stop_time_update.iter().find(|x| {
                                                        x.stop_id
                                                            .as_ref()
                                                            .map(|compare| compare.as_str())
                                                            == Some(&trip.itinerary_options[0].stop_id)
                                                    });
    
                                                if let Some(relevant_stop_time_update) =
                                                    relevant_stop_time_update
                                                {
                                                    if let Some(departure) =
                                                        &relevant_stop_time_update.departure
                                                    {
                                                        if let Some(time) = departure.time {
                                                            departure_time_rt = Some(time as u64);
                                                        }
                                                    } else {
                                                        if let Some(arrival) =
                                                            &relevant_stop_time_update.arrival
                                                        {
                                                            if let Some(time) = arrival.time {
                                                                departure_time_rt = Some(time as u64);
                                                            }
                                                        }
                                                    }
    
                                                    if let Some(platform_id) =
                                                        &relevant_stop_time_update.platform_string
                                                    {
                                                        platform = Some(platform_id.to_string());
                                                    }
                                                }
                                            }

                                                // add to used list
                                                already_used_trip_update_id
                                                    .insert(trip_update_id.clone());
                                            }
                                        }
                                    }
                                }

                                if let Some(chateau_alerts) = all_alerts_for_chateau.as_ref() {
                                    for alert in chateau_alerts.values() {
                                        if is_cancelled {
                                            break;
                                        }
                                        let effect_is_no_service = alert.effect == Some(1); // NO_SERVICE
                                        if effect_is_no_service {
                                            let applies_to_trip =
                                                alert.informed_entity.iter().any(|e| {
                                                    let route_match =
                                                        e.route_id.as_ref().map_or(false, |r_id| {
                                                            *r_id == trip.route_id
                                                        });
                                                    let trip_match =
                                                        e.trip.as_ref().map_or(false, |t| {
                                                            t.trip_id
                                                                .as_ref()
                                                                .map_or(false, |t_id| {
                                                                    *t_id == trip.trip_id
                                                                })
                                                        });
                                                    route_match || trip_match
                                                });

                                            let applies_to_trip_without_a_referenced_stop = alert
                                                .informed_entity
                                                .iter()
                                                .filter(|e| e.stop_id.is_none())
                                                .any(|e| {
                                                    let route_match =
                                                        e.route_id.as_ref().map_or(false, |r_id| {
                                                            *r_id == trip.route_id
                                                        });
                                                    let trip_match =
                                                        e.trip.as_ref().map_or(false, |t| {
                                                            t.trip_id
                                                                .as_ref()
                                                                .map_or(false, |t_id| {
                                                                    *t_id == trip.trip_id
                                                                })
                                                        });

                                                    route_match || trip_match
                                                });

                                            if applies_to_trip {
                                                let event_time_opt = match trip.itinerary_options[0]
                                                    .departure_time_since_start
                                                {
                                                    Some(departure_time_since_start) => Some(
                                                        trip.reference_start_of_service_date
                                                            .timestamp()
                                                            as u64
                                                            + scheduled_frequency_start_time as u64
                                                            + departure_time_since_start as u64,
                                                    ),
                                                    None => None,
                                                };

                                                if let Some(event_time) = event_time_opt {
                                                    let is_active =
                                                        alert.active_period.iter().any(|ap| {
                                                            let start = ap.start.unwrap_or(0);
                                                            let end = ap.end.unwrap_or(u64::MAX);
                                                            event_time >= start && event_time <= end
                                                        });
                                                    if is_active {
                                                        if alert.effect == Some(1) {
                                                            if (applies_to_trip_without_a_referenced_stop) {
                                                                is_cancelled = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                headsign_group.trips.push(DepartingStopTime {
                                    trip_id: trip.trip_id.clone(),
                                    gtfs_schedule_start_day: trip.trip_service_date,
                                    departure_realtime: departure_time_rt,
                                    arrival_schedule: None,
                                    arrival_realtime: None,
                                    stop_id: trip.itinerary_options[0].stop_id.clone(),
                                    trip_short_name: trip.trip_short_name.clone(),
                                    trip_tz: trip.timezone.as_ref().unwrap().name().to_string(),
                                    is_frequency: trip.frequencies.is_some(),
                                    platform: platform.clone(),
                                    level_id: None,
                                    departure_schedule: match trip.itinerary_options[0]
                                        .departure_time_since_start
                                    {
                                        Some(departure_time_since_start) => Some(
                                            trip.reference_start_of_service_date.timestamp() as u64
                                                + scheduled_frequency_start_time as u64
                                                + departure_time_since_start as u64,
                                        ),
                                        None => {
                                            match trip.itinerary_options[0].arrival_time_since_start
                                            {
                                                Some(arrival) => Some(
                                                    trip.reference_start_of_service_date.timestamp()
                                                        as u64
                                                        + scheduled_frequency_start_time as u64
                                                        + arrival as u64,
                                                ),
                                                None => match trip.itinerary_options[0]
                                                    .interpolated_time_since_start
                                                {
                                                    Some(interpolated) => Some(
                                                        trip.reference_start_of_service_date
                                                            .timestamp()
                                                            as u64
                                                            + scheduled_frequency_start_time as u64
                                                            + interpolated as u64,
                                                    ),
                                                    None => None,
                                                },
                                            }
                                        }
                                    },
                                    is_interpolated: trip.itinerary_options[0]
                                        .interpolated_time_since_start
                                        .is_some(),
                                    gtfs_frequency_start_time: Some(
                                        catenary::number_of_seconds_to_hhmmss(
                                            scheduled_frequency_start_time,
                                        )
                                        .into(),
                                    ),
                                    cancelled: is_cancelled,
                                    deleted: deleted,
                                });
                            }
                        }
                    }

                    for (group_id, group) in route_group.directions.iter_mut() {
                        group
                            .trips
                            .sort_by_key(|x| x.departure_schedule.unwrap_or(0));

                        let stop = stops_table
                            .get(&(chateau_id.clone(), group.trips[0].stop_id.to_string()).clone())
                            .unwrap();

                        if !stops_answer.contains_key(chateau_id) {
                            stops_answer.insert(chateau_id.clone(), AHashMap::new());
                        }

                        let stop_group = stops_answer.get_mut(chateau_id).unwrap();

                        if !stop_group.contains_key(group.trips[0].stop_id.as_str()) {
                            stop_group.insert(
                                group.trips[0].stop_id.clone(),
                                StopOutput {
                                    gtfs_id: (&stop.0.gtfs_id).into(),
                                    name: stop.0.name.clone().unwrap_or("".to_string()),
                                    lat: stop.0.point.as_ref().unwrap().y,
                                    lon: stop.0.point.as_ref().unwrap().x,
                                    timezone: stop.0.timezone.clone(),
                                    url: stop.0.url.clone(),
                                },
                            );
                        }

                        if stop.1 < route_group.closest_distance {
                            route_group.closest_distance = stop.1;
                        }
                    }
                }

                for (route_id, route_group) in directions_route_group_for_this_chateau {
                    departures.push(route_group);
                }
            }

            departures.sort_by(
                |a, b| match a.closest_distance.partial_cmp(&b.closest_distance) {
                    Some(Ordering::Equal) => a.route_id.cmp(&b.route_id),
                    Some(x) => x,
                    None => a.route_id.cmp(&b.route_id),
                },
            );

            let total_elapsed_time = start.elapsed();

            let now = catenary::duration_since_unix_epoch().as_secs() as u64;

            if let Some(limit_n_events_after_departure_time) =
                &query.limit_n_events_after_departure_time
            {
                for departures_for_route in departures.iter_mut() {
                    for ((_, _), departing_headsign_group) in
                        departures_for_route.directions.iter_mut()
                    {
                        if !departing_headsign_group.trips.is_empty() {
                            let mut counter: usize = 0;
                            let mut truncate_i: usize = 0;

                            for (i, trip) in departing_headsign_group.trips.iter().enumerate() {
                                truncate_i = truncate_i + 1;
                                let mut after_now = false;

                                if let Some(departure_realtime) = trip.departure_realtime {
                                    if departure_realtime >= now {
                                        after_now = true;
                                    }
                                } else if let Some(departure_schedule) = trip.departure_schedule {
                                    if departure_schedule >= now {
                                        after_now = true;
                                    }
                                }

                                if let Some(arrival_realtime) = trip.arrival_realtime {
                                    if arrival_realtime >= now {
                                        after_now = true;
                                    }
                                } else if let Some(arrival_schedule) = trip.arrival_schedule {
                                    if arrival_schedule >= now {
                                        after_now = true;
                                    }
                                }

                                if after_now {
                                    counter = counter + 1;
                                }

                                if counter == *limit_n_events_after_departure_time {
                                    break;
                                }
                            }

                            departing_headsign_group.trips.truncate(truncate_i);
                        }
                    }
                }
            }

            HttpResponse::Ok().json(DepartingTripsDataAnswer {
                number_of_stops_searched_through: stops.len(),
                bus_limited_metres: bus_distance_limit as f64,
                rail_and_other_limited_metres: rail_and_other_distance_limit as f64,
                departures: departures.into_iter().map(|x| x.into()).collect::<Vec<_>>(),
                stop: stops_answer,
                alerts: all_alerts,
                debug: DeparturesDebug {
                    stop_lookup_ms: end_stops_duration.as_millis(),
                    directions_ms: directions_lookup_duration.as_millis(),
                    itinerary_meta_ms: itineraries_meta_duration.as_millis(),
                    itinerary_row_ms: itinerary_duration.as_millis(),
                    trips_ms: trip_lookup_elapsed.as_millis(),
                    total_time_ms: total_elapsed_time.as_millis(),
                    route_and_cal_ms: calendar_timer_finish.as_millis(),
                    etcd_time_ms: etcd_time_elapsed.as_millis(),
                    aspen_data_fetch_time_elapsed: aspen_data_fetch_time_elapsed
                        .map(|x| x.as_millis() as u128),
                    etcd_connection_time_ms: etcd_connection_time.as_millis() as u128,
                    db_connection_time_ms: db_connection_time.as_millis() as u128,
                },
            })
        }
    }
}

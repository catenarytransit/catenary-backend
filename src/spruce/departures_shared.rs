use catenary::CalendarUnified;
use catenary::aspen_dataset::AspenisedTripUpdate;
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use ecow::EcoString;
use futures::StreamExt;
use geo::coord;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

pub type StopDataResult = (
    String,                                                       // chateau_id
    BTreeMap<String, Vec<catenary::models::ItineraryPatternRow>>, // itins_btreemap
    BTreeMap<String, catenary::models::ItineraryPatternMeta>,     // itin_meta_btreemap
    BTreeMap<String, catenary::models::DirectionPatternMeta>,     // direction_meta_btreemap
    BTreeMap<String, catenary::models::CompressedTrip>,           // trip_compressed_btreemap
    BTreeMap<String, Vec<catenary::models::DirectionPatternRow>>, // direction_rows_for_chateau
    BTreeMap<String, catenary::models::Route>,                    // routes_btreemap
    BTreeMap<String, catenary::models::Agency>,                   // agencies_btreemap
    BTreeMap<EcoString, String>,                                  // shape_polyline_for_chateau
    Vec<catenary::models::Calendar>,                              // calendar
    Vec<catenary::models::CalendarDate>,                          // calendar_dates
);

pub async fn fetch_stop_data_for_chateau(
    pool: Arc<CatenaryPostgresPool>,
    chateau_id: String,
    stop_ids: Vec<String>,
    include_shapes: bool,
    date_filter: Option<(NaiveDate, NaiveDate)>,
    direction_pattern_ids: Option<Vec<String>>,
) -> StopDataResult {
    let mut conn = pool.get().await.unwrap();
    let global_start = std::time::Instant::now();

    // 1. Resolve Direction Patterns & Itinerary Metadata first
    let (itinerary_list, itin_meta) = if let Some(ref dir_ids) = direction_pattern_ids {
        // Case A: Caller provided direction_pattern_ids
        let meta = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(chateau_id.clone()))
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::direction_pattern_id
                    .eq_any(dir_ids),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load::<catenary::models::ItineraryPatternMeta>(&mut conn)
            .await
            .unwrap_or_default();

        let ids: Vec<String> = meta
            .iter()
            .map(|x| x.itinerary_pattern_id.clone())
            .collect();
        (ids, meta)
    } else {
        // Case B: Infer directions from stops (V2 Logic)
        // We fetch direction_pattern for these stops to see what directions serve them.
        let distinct_directions: Vec<String> =
            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
                .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau_id.clone()))
                .filter(catenary::schema::gtfs::direction_pattern::stop_id.eq_any(&stop_ids))
                .select(catenary::schema::gtfs::direction_pattern::direction_pattern_id)
                .distinct()
                .load::<String>(&mut conn)
                .await
                .unwrap_or_default();

        if distinct_directions.is_empty() {
            return (
                chateau_id,
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                Vec::new(),
                Vec::new(),
            );
        }

        let meta = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(catenary::schema::gtfs::itinerary_pattern_meta::chateau.eq(chateau_id.clone()))
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::direction_pattern_id
                    .eq_any(&distinct_directions),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load::<catenary::models::ItineraryPatternMeta>(&mut conn)
            .await
            .unwrap_or_default();

        let ids: Vec<String> = meta
            .iter()
            .map(|x| x.itinerary_pattern_id.clone())
            .collect();
        (ids, meta)
    };

    if itinerary_list.is_empty() {
        return (
            chateau_id,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        );
    }

    // 2. Fetch Itinerary Pattern Rows (filtered by both ID and Stop)
    // CRITICAL: We filter by stop_id_any(&stop_ids) to only fetch the rows for the stops we care about.
    // This dramatically reduces the amount of data fetched compared to fetching the whole itinerary.
    let itins = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
        .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(chateau_id.clone()))
        .filter(
            catenary::schema::gtfs::itinerary_pattern::itinerary_pattern_id.eq_any(&itinerary_list),
        )
        .filter(catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(&stop_ids))
        .select(catenary::models::ItineraryPatternRow::as_select())
        .load::<catenary::models::ItineraryPatternRow>(&mut conn)
        .await
        .unwrap_or_default();

    println!(
        "PERF: itinerary_pattern fetch took {}ms. Count: {}",
        global_start.elapsed().as_millis(),
        itins.len()
    );
    let t_section = std::time::Instant::now();

    let mut itins_btreemap = BTreeMap::<String, Vec<catenary::models::ItineraryPatternRow>>::new();
    for itin in itins.iter() {
        itins_btreemap
            .entry(itin.itinerary_pattern_id.clone())
            .or_insert_with(Vec::new)
            .push(itin.clone());
    }

    // Since we filtered itinerary_pattern rows by stop_id, we might have itineraries in our list
    // that don't have any matching rows (e.g. if the direction matches but the pattern doesn't stop there).
    // We should filter our itinerary_list down to only those that actually have rows.
    let itinerary_list_filtered: Vec<String> = itins_btreemap.keys().cloned().collect();
    let itinerary_list_clone = itinerary_list_filtered.clone();

    // Task A: Meta Data
    // We reuse the 'itin_meta' we fetched earlier, but we might want to filter it if we filtered the list
    let itin_meta_filtered: Vec<catenary::models::ItineraryPatternMeta> = itin_meta
        .into_iter()
        .filter(|x| itins_btreemap.contains_key(&x.itinerary_pattern_id))
        .collect();

    let itin_meta_arc = Arc::new(itin_meta_filtered);
    let pool_for_join = pool.clone();
    let chateau_id_clone = chateau_id.clone();

    let meta_task = async {
        let itin_meta = itin_meta_arc.as_ref();

        let mut itin_meta_btreemap =
            BTreeMap::<String, catenary::models::ItineraryPatternMeta>::new();
        for itin in itin_meta {
            itin_meta_btreemap.insert(itin.itinerary_pattern_id.clone(), itin.clone());
        }

        let direction_ids_to_search: Vec<String> = itin_meta
            .iter()
            .filter_map(|x| x.direction_pattern_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let route_ids: Vec<CompactString> = itin_meta.iter().map(|x| x.route_id.clone()).collect();

        let direction_ids_clone1 = direction_ids_to_search.clone();
        let direction_ids_clone2 = direction_ids_to_search.clone();
        let chateau_id_clone_dirs = chateau_id_clone.clone();
        let chateau_id_clone_rows = chateau_id_clone.clone();
        let chateau_id_clone_routes = chateau_id_clone.clone();
        let chateau_id_clone_agencies = chateau_id_clone.clone();
        let chateau_id_clone_shapes = chateau_id_clone.clone();

        let direction_meta_task = async {
            let mut conn = pool_for_join.get().await.unwrap();
            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::chateau
                        .eq(chateau_id_clone_dirs),
                )
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::direction_pattern_id
                        .eq_any(&direction_ids_clone1),
                )
                .select(catenary::models::DirectionPatternMeta::as_select())
                .load::<catenary::models::DirectionPatternMeta>(&mut conn)
                .await
                .unwrap_or_default()
        };

        let direction_rows_task = async {
            let mut conn = pool_for_join.get().await.unwrap();
            catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
                .filter(
                    catenary::schema::gtfs::direction_pattern::chateau.eq(chateau_id_clone_rows),
                )
                .filter(
                    catenary::schema::gtfs::direction_pattern::direction_pattern_id
                        .eq_any(&direction_ids_clone2),
                )
                .select(catenary::models::DirectionPatternRow::as_select())
                .load::<catenary::models::DirectionPatternRow>(&mut conn)
                .await
                .unwrap_or_default()
        };

        let routes_task = async {
            let mut conn = pool_for_join.get().await.unwrap();
            catenary::schema::gtfs::routes::dsl::routes
                .filter(catenary::schema::gtfs::routes::chateau.eq(chateau_id_clone_routes))
                .filter(catenary::schema::gtfs::routes::route_id.eq_any(&route_ids))
                .select(catenary::models::Route::as_select())
                .load::<catenary::models::Route>(&mut conn)
                .await
                .unwrap_or_default()
        };

        let (direction_meta, direction_rows, routes_ret) =
            tokio::join!(direction_meta_task, direction_rows_task, routes_task);

        let mut direction_meta_btreemap =
            BTreeMap::<String, catenary::models::DirectionPatternMeta>::new();
        let mut shape_ids_to_fetch_for_this_chateau = BTreeSet::new();
        for direction in &direction_meta {
            if let Some(shape_id) = &direction.gtfs_shape_id {
                shape_ids_to_fetch_for_this_chateau.insert(shape_id.clone());
            }
            direction_meta_btreemap
                .insert(direction.direction_pattern_id.clone(), direction.clone());
        }

        let agencies_task = async {
            let mut conn = pool_for_join.get().await.unwrap();
            let agency_ids: Vec<String> = routes_ret
                .iter()
                .filter_map(|x| x.agency_id.clone())
                .collect();

            catenary::schema::gtfs::agencies::dsl::agencies
                .filter(catenary::schema::gtfs::agencies::chateau.eq(chateau_id_clone_agencies))
                .filter(catenary::schema::gtfs::agencies::agency_id.eq_any(&agency_ids))
                .select(catenary::models::Agency::as_select())
                .load::<catenary::models::Agency>(&mut conn)
                .await
                .unwrap_or_default()
        };

        let shapes_task = async {
            if include_shapes {
                let mut conn = pool_for_join.get().await.unwrap();
                catenary::schema::gtfs::shapes::dsl::shapes
                    .filter(catenary::schema::gtfs::shapes::chateau.eq(chateau_id_clone_shapes))
                    .filter(
                        catenary::schema::gtfs::shapes::shape_id
                            .eq_any(&shape_ids_to_fetch_for_this_chateau),
                    )
                    .load::<catenary::models::Shape>(&mut conn)
                    .await
                    .unwrap_or_default()
            } else {
                vec![]
            }
        };

        let (agencies_ret, shapes_result) = tokio::join!(agencies_task, shapes_task);

        println!(
            "PERF: parallel meta fetch block took {}ms",
            t_section.elapsed().as_millis()
        );

        let mut shape_polyline_for_chateau: BTreeMap<EcoString, String> = BTreeMap::new();
        for db_shape in shapes_result {
            let shape_polyline = polyline::encode_coordinates(
                geo::LineString::new(
                    db_shape
                        .linestring
                        .points
                        .iter()
                        .map(|point| coord! { x: point.x, y: point.y })
                        .collect::<Vec<_>>(),
                ),
                5,
            )
            .unwrap();
            shape_polyline_for_chateau.insert(db_shape.shape_id.clone().into(), shape_polyline);
        }

        let mut direction_rows_for_chateau =
            BTreeMap::<String, Vec<catenary::models::DirectionPatternRow>>::new();
        for direction_row in direction_rows {
            let entry = direction_rows_for_chateau
                .entry(direction_row.direction_pattern_id.clone())
                .or_insert_with(Vec::new);
            entry.push(direction_row);
        }
        for entry in direction_rows_for_chateau.values_mut() {
            entry.sort_by_key(|x| x.stop_sequence);
        }

        let mut routes_btreemap = BTreeMap::<String, catenary::models::Route>::new();
        for route in &routes_ret {
            routes_btreemap.insert(route.route_id.clone().into(), route.clone());
        }

        let mut agencies_btreemap = BTreeMap::<String, catenary::models::Agency>::new();
        for agency in &agencies_ret {
            agencies_btreemap.insert(agency.agency_id.clone(), agency.clone());
        }

        (
            itin_meta_btreemap,
            direction_meta_btreemap,
            shape_polyline_for_chateau,
            direction_rows_for_chateau,
            routes_btreemap,
            agencies_btreemap,
        )
    };

    // Task B: Schedule Data (Trips, Calendar)
    let pool_for_schedule = pool.clone();
    let chateau_id_clone2 = chateau_id.clone();
    let schedule_task = async {
        let chunk_size = 100;
        let use_parallel_fetching = itinerary_list_clone.len() > 200;

        let t_section = std::time::Instant::now();

        // New Logic: If date_filter is present, we first fetch distinct service_ids
        let mut active_service_ids_opt: Option<BTreeSet<CompactString>> = None;
        let mut service_ids_to_search: BTreeSet<CompactString> = BTreeSet::new();

        if false {
            if let Some((start_date, end_date)) = date_filter {
                let mut conn_lite = pool_for_schedule.get().await.unwrap();

                // 1. Fetch ALL service IDs for these itinerary patterns (lightweight query)
                let service_ids: Vec<CompactString> =
                    catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                        .filter(
                            catenary::schema::gtfs::trips_compressed::chateau
                                .eq(chateau_id_clone2.clone()),
                        )
                        .filter(
                            catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                                .eq_any(&itinerary_list_clone),
                        )
                        .select(catenary::schema::gtfs::trips_compressed::service_id)
                        .distinct()
                        .load::<CompactString>(&mut conn_lite)
                        .await
                        .unwrap_or_default();

                println!(
                    "PERF: service_id fetch (distinct) took {}ms. Count: {}",
                    t_section.elapsed().as_millis(),
                    service_ids.len()
                );

                // 2. Fetch Calendar / Calendar Dates for these specific service IDs
                let mut conn_cal = pool_for_schedule.get().await.unwrap();
                let calendar = catenary::schema::gtfs::calendar::dsl::calendar
                    .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau_id_clone2.clone()))
                    .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&service_ids))
                    .select(catenary::models::Calendar::as_select())
                    .load::<catenary::models::Calendar>(&mut conn_cal)
                    .await
                    .unwrap_or_default();

                let calendar_dates = catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                    .filter(
                        catenary::schema::gtfs::calendar_dates::chateau
                            .eq(chateau_id_clone2.clone()),
                    )
                    .filter(catenary::schema::gtfs::calendar_dates::service_id.eq_any(&service_ids))
                    .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.ge(start_date))
                    .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.le(end_date))
                    .select(catenary::models::CalendarDate::as_select())
                    .load::<catenary::models::CalendarDate>(&mut conn_cal)
                    .await
                    .unwrap_or_default();

                // 3. Determine Active Services
                let unified_cal = catenary::make_calendar_structure_from_pg(
                    vec![calendar.clone()],
                    vec![calendar_dates.clone()],
                )
                .unwrap_or_default();

                let mut active_services = BTreeSet::new();
                if let Some(cal_map) = unified_cal.get(&chateau_id_clone2) {
                    for service_id in &service_ids {
                        if let Some(service) = cal_map.get(service_id.as_str()) {
                            let mut is_active = false;
                            let mut d = start_date;
                            while d <= end_date {
                                if catenary::datetime_in_service(service, d) {
                                    is_active = true;
                                    break;
                                }
                                d = d.succ_opt().unwrap();
                            }
                            if is_active {
                                active_services.insert(service_id.clone());
                            }
                        }
                    }
                }

                println!(
                    "PERF: Active Services Calculation took {}ms. Active: {} / {}",
                    t_section.elapsed().as_millis(),
                    active_services.len(),
                    service_ids.len()
                );

                active_service_ids_opt = Some(active_services);
            }
        }

        // --- Fetch Trips ---
        let trips = if use_parallel_fetching {
            let chunks: Vec<Vec<String>> = itinerary_list_clone
                .chunks(chunk_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            let active_services_arc = active_service_ids_opt.as_ref().map(|s| Arc::new(s.clone()));

            let futures_trips = chunks.into_iter().map(|chunk| {
                let pool = pool_for_schedule.clone();
                let chateau = chateau_id_clone2.clone();
                let active_services = active_services_arc.clone();

                async move {
                    let mut conn = pool.get().await.unwrap();
                    let mut query = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                        .into_boxed()
                        .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(chateau))
                        .filter(
                            catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                                .eq_any(chunk),
                        )
                        .select(catenary::models::CompressedTrip::as_select());

                    if let Some(services) = &active_services {
                        let services_vec: Vec<CompactString> = services.iter().cloned().collect();
                        query = query.filter(
                            catenary::schema::gtfs::trips_compressed::service_id
                                .eq_any(services_vec),
                        );
                    }

                    query
                        .load::<catenary::models::CompressedTrip>(&mut conn)
                        .await
                        .unwrap_or_default()
                }
            });

            let results: Vec<Vec<catenary::models::CompressedTrip>> =
                futures::stream::iter(futures_trips)
                    .buffer_unordered(10)
                    .collect()
                    .await;

            results.into_iter().flatten().collect::<Vec<_>>()
        } else {
            let mut conn2 = pool_for_schedule.get().await.unwrap();
            let mut query = catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                .into_boxed()
                .filter(
                    catenary::schema::gtfs::trips_compressed::chateau.eq(chateau_id_clone2.clone()),
                )
                .filter(
                    catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                        .eq_any(&itinerary_list_clone),
                )
                .select(catenary::models::CompressedTrip::as_select());

            if let Some(services) = &active_service_ids_opt {
                query = query
                    .filter(catenary::schema::gtfs::trips_compressed::service_id.eq_any(services));
            }

            query
                .load::<catenary::models::CompressedTrip>(&mut conn2)
                .await
                .unwrap_or_default()
        };
        println!(
            "PERF: trips_compressed fetch took {}ms. Count: {}",
            t_section.elapsed().as_millis(),
            trips.len()
        );
        let t_section = std::time::Instant::now();

        let mut trip_compressed_btreemap =
            BTreeMap::<String, catenary::models::CompressedTrip>::new();

        for trip in &trips {
            trip_compressed_btreemap.insert(trip.trip_id.clone(), trip.clone());
            service_ids_to_search.insert(trip.service_id.clone());
        }

        let mut conn_calendar = pool_for_schedule.get().await.unwrap();
        let calendar = catenary::schema::gtfs::calendar::dsl::calendar
            .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau_id_clone2.clone()))
            .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&service_ids_to_search))
            .select(catenary::models::Calendar::as_select())
            .load::<catenary::models::Calendar>(&mut conn_calendar)
            .await
            .unwrap_or_default();
        println!(
            "PERF: calendar fetch took {}ms",
            t_section.elapsed().as_millis()
        );
        let t_section = std::time::Instant::now();

        let mut calendar_dates_query = catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
            .into_boxed()
            .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(chateau_id_clone2.clone()))
            .filter(
                catenary::schema::gtfs::calendar_dates::service_id.eq_any(&service_ids_to_search),
            );

        if let Some((start_date, end_date)) = date_filter {
            calendar_dates_query = calendar_dates_query
                .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.ge(start_date))
                .filter(catenary::schema::gtfs::calendar_dates::gtfs_date.le(end_date));
        }

        let calendar_dates = calendar_dates_query
            .select(catenary::models::CalendarDate::as_select())
            .load::<catenary::models::CalendarDate>(&mut conn_calendar)
            .await
            .unwrap_or_default();
        println!(
            "PERF: calendar_dates fetch took {}ms",
            t_section.elapsed().as_millis()
        );

        (trip_compressed_btreemap, calendar, calendar_dates)
    };

    let (
        (
            itin_meta_btreemap,
            direction_meta_btreemap,
            shape_polyline_for_chateau,
            direction_rows_for_chateau,
            routes_btreemap,
            agencies_btreemap,
        ),
        (trip_compressed_btreemap, calendar, calendar_dates),
    ) = tokio::join!(meta_task, schedule_task);

    println!(
        "PERF: TOTAL fetch_stop_data_for_chateau took {}ms",
        global_start.elapsed().as_millis()
    );

    (
        chateau_id,
        itins_btreemap,
        itin_meta_btreemap,
        direction_meta_btreemap,
        trip_compressed_btreemap,
        direction_rows_for_chateau,
        routes_btreemap,
        agencies_btreemap,
        shape_polyline_for_chateau,
        calendar,
        calendar_dates,
    )
}

/// Represents an itinerary stop option with timing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItinOption {
    pub arrival_time_since_start: Option<i32>,
    pub departure_time_since_start: Option<i32>,
    pub interpolated_time_since_start: Option<i32>,
    pub stop_id: CompactString,
    pub gtfs_stop_sequence: u32,
    pub trip_headsign: Option<String>,
    pub trip_headsign_translations: Option<serde_json::Value>,
}

/// A validated trip with all necessary schedule information
#[derive(Clone, Debug, Serialize)]
pub struct ValidTripSet {
    pub chateau_id: String,
    pub trip_id: CompactString,
    pub frequencies: Option<Vec<gtfs_structures::Frequency>>,
    pub trip_service_date: NaiveDate,
    pub itinerary_options: Arc<Vec<ItinOption>>,
    pub reference_start_of_service_date: chrono::DateTime<chrono_tz::Tz>,
    pub itinerary_pattern_id: String,
    pub direction_pattern_id: String,
    pub route_id: CompactString,
    pub timezone: Option<chrono_tz::Tz>,
    pub trip_start_time: u32,
    pub trip_short_name: Option<CompactString>,
    pub service_id: CompactString,
}

/// Data for a stop that has been moved/reassigned
#[derive(Serialize, Clone, Debug)]
pub struct MovedStopData {
    pub stop_id: String,
    pub scheduled_arrival: Option<u64>,
    pub scheduled_departure: Option<u64>,
    pub realtime_arrival: Option<u64>,
    pub realtime_departure: Option<u64>,
}

/// Index for efficient alert lookups by route or trip
#[derive(Clone)]
pub struct AlertIndex {
    by_route: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>>,
    by_trip: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>>,
    #[allow(dead_code)]
    general: Vec<(String, catenary::aspen_dataset::AspenisedAlert)>,
}

impl AlertIndex {
    pub fn new(alerts: &BTreeMap<String, catenary::aspen_dataset::AspenisedAlert>) -> Self {
        let mut by_route: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>> =
            HashMap::new();
        let mut by_trip: HashMap<String, Vec<(String, catenary::aspen_dataset::AspenisedAlert)>> =
            HashMap::new();
        let general = Vec::new();

        for (id, alert) in alerts {
            for entity in &alert.informed_entity {
                if let Some(r_id) = &entity.route_id {
                    by_route
                        .entry(r_id.clone())
                        .or_default()
                        .push((id.clone(), alert.clone()));
                }
                if let Some(trip) = &entity.trip {
                    if let Some(t_id) = &trip.trip_id {
                        by_trip
                            .entry(t_id.clone())
                            .or_default()
                            .push((id.clone(), alert.clone()));
                    }
                }
            }
        }

        for v in by_route.values_mut() {
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.dedup_by(|a, b| a.0 == b.0);
        }
        for v in by_trip.values_mut() {
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.dedup_by(|a, b| a.0 == b.0);
        }

        Self {
            by_route,
            by_trip,
            general,
        }
    }

    pub fn search(
        &self,
        route_id: &str,
        trip_id: &str,
    ) -> Vec<&catenary::aspen_dataset::AspenisedAlert> {
        let mut candidates = Vec::new();
        if let Some(alerts) = self.by_route.get(route_id) {
            candidates.extend(alerts);
        }
        if let Some(alerts) = self.by_trip.get(trip_id) {
            candidates.extend(alerts);
        }

        candidates.sort_by(|a, b| a.0.cmp(&b.0));
        candidates.dedup_by(|a, b| a.0 == b.0);

        candidates.into_iter().map(|(_, alert)| alert).collect()
    }
}

/// Estimate if a realtime trip update matches a valid scheduled trip's service date
pub fn estimate_service_date(
    valid_trip: &ValidTripSet,
    trip_update: &AspenisedTripUpdate,
    itin_option: &ItinOption,
    calendar_structure: &BTreeMap<String, CalendarUnified>,
    _chateau_id: &str,
) -> bool {
    let trip_offset = itin_option.departure_time_since_start.unwrap_or(
        itin_option
            .arrival_time_since_start
            .unwrap_or(itin_option.interpolated_time_since_start.unwrap_or(0)),
    ) as u64;

    let naive_date_approx_guess = trip_update
        .stop_time_update
        .iter()
        .filter(|x| x.departure.is_some() || x.arrival.is_some())
        .filter_map(|x| {
            if let Some(departure) = &x.departure {
                Some(departure.time)
            } else if let Some(arrival) = &x.arrival {
                Some(arrival.time)
            } else {
                None
            }
        })
        .flatten()
        .min();

    match naive_date_approx_guess {
        Some(least_num) => {
            let tz = match valid_trip.timezone.as_ref() {
                Some(tz) => tz,
                None => return false,
            };
            let rt_least_naive_date = tz.timestamp(least_num as i64, 0);
            let approx_service_date_start =
                rt_least_naive_date - chrono::Duration::seconds(trip_offset as i64);
            let approx_service_date = approx_service_date_start.date();

            let day_before = approx_service_date - chrono::Duration::days(2);

            let mut best_date_score = i64::MAX;
            let mut best_date = None;

            for day in day_before.naive_local().iter_days().take(3) {
                let service_id = valid_trip.service_id.as_str();
                let service_opt = calendar_structure.get(service_id);

                if let Some(service) = service_opt {
                    if catenary::datetime_in_service(service, day) {
                        let day_in_tz_midnight =
                            day.and_hms(12, 0, 0).and_local_timezone(*tz).unwrap()
                                - chrono::Duration::hours(12);
                        let time_delta = rt_least_naive_date
                            .signed_duration_since(day_in_tz_midnight)
                            .num_seconds()
                            .abs();

                        if time_delta < best_date_score {
                            best_date_score = time_delta;
                            best_date = Some(day_in_tz_midnight.date());
                        }
                    }
                }
            }

            if let Some(best_date) = best_date {
                valid_trip.trip_service_date == best_date.naive_local()
            } else {
                false
            }
        }
        None => false,
    }
}

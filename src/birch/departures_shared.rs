use catenary::CalendarUnified;
use catenary::aspen_dataset::AspenisedTripUpdate;
use catenary::postgres_tools::CatenaryPostgresPool;
use chrono::NaiveDate;
use chrono::TimeZone;
use compact_str::CompactString;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
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
) -> StopDataResult {
    let mut conn = pool.get().await.unwrap();
    let global_start = std::time::Instant::now();

    let itins = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
        .filter(catenary::schema::gtfs::itinerary_pattern::chateau.eq(chateau_id.clone()))
        .filter(catenary::schema::gtfs::itinerary_pattern::stop_id.eq_any(&stop_ids))
        .select(catenary::models::ItineraryPatternRow::as_select())
        .load::<catenary::models::ItineraryPatternRow>(&mut conn)
        .await
        .unwrap_or_default();
    println!("PERF: itinerary_pattern fetch took {}ms", global_start.elapsed().as_millis());
    let t_section = std::time::Instant::now();

    let mut itins_btreemap = BTreeMap::<String, Vec<catenary::models::ItineraryPatternRow>>::new();
    for itin in itins.iter() {
        itins_btreemap
            .entry(itin.itinerary_pattern_id.clone())
            .or_insert_with(Vec::new)
            .push(itin.clone());
    }

    let itinerary_list: Vec<String> = itins_btreemap.keys().cloned().collect();
    let itinerary_list_clone = itinerary_list.clone();

    // Task A: Meta Data
    let mut conn1 = conn; // Reuse first connection
    let chateau_id_clone = chateau_id.clone();
    let meta_task = async {
        let itin_meta = catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::chateau
                    .eq(chateau_id_clone.clone()),
            )
            .filter(
                catenary::schema::gtfs::itinerary_pattern_meta::itinerary_pattern_id
                    .eq_any(&itinerary_list),
            )
            .select(catenary::models::ItineraryPatternMeta::as_select())
            .load::<catenary::models::ItineraryPatternMeta>(&mut conn1)
            .await
            .unwrap_or_default();
        println!("PERF: itinerary_pattern_meta fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

        let mut itin_meta_btreemap =
            BTreeMap::<String, catenary::models::ItineraryPatternMeta>::new();
        for itin in &itin_meta {
            itin_meta_btreemap.insert(itin.itinerary_pattern_id.clone(), itin.clone());
        }

        let direction_ids_to_search: Vec<String> = itin_meta
            .iter()
            .filter_map(|x| x.direction_pattern_id.clone())
            .collect();

        let direction_meta =
            catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::chateau
                        .eq(chateau_id_clone.clone()),
                )
                .filter(
                    catenary::schema::gtfs::direction_pattern_meta::direction_pattern_id
                        .eq_any(&direction_ids_to_search),
                )
                .select(catenary::models::DirectionPatternMeta::as_select())
                .load::<catenary::models::DirectionPatternMeta>(&mut conn1)
                .await
                .unwrap_or_default();
        println!("PERF: direction_pattern_meta fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

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

        let mut shape_polyline_for_chateau: BTreeMap<EcoString, String> = BTreeMap::new();

        if include_shapes {
            let shapes_result = catenary::schema::gtfs::shapes::dsl::shapes
                .filter(catenary::schema::gtfs::shapes::chateau.eq(chateau_id_clone.clone()))
                .filter(
                    catenary::schema::gtfs::shapes::shape_id
                        .eq_any(&shape_ids_to_fetch_for_this_chateau),
                )
                .load::<catenary::models::Shape>(&mut conn1)
                .await
                .unwrap_or_default();

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
        }
        println!("PERF: shapes fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

        let direction_row_query = catenary::schema::gtfs::direction_pattern::dsl::direction_pattern
            .filter(catenary::schema::gtfs::direction_pattern::chateau.eq(chateau_id_clone.clone()))
            .filter(
                catenary::schema::gtfs::direction_pattern::direction_pattern_id
                    .eq_any(&direction_ids_to_search),
            )
            .select(catenary::models::DirectionPatternRow::as_select())
            .load::<catenary::models::DirectionPatternRow>(&mut conn1)
            .await
            .unwrap_or_default();
        println!("PERF: direction_pattern fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

        let mut direction_rows_for_chateau =
            BTreeMap::<String, Vec<catenary::models::DirectionPatternRow>>::new();
        for direction_row in direction_row_query {
            let entry = direction_rows_for_chateau
                .entry(direction_row.direction_pattern_id.clone())
                .or_insert_with(Vec::new);
            entry.push(direction_row);
        }
        for entry in direction_rows_for_chateau.values_mut() {
            entry.sort_by_key(|x| x.stop_sequence);
        }

        let route_ids: Vec<CompactString> = itin_meta.iter().map(|x| x.route_id.clone()).collect();

        let routes_ret = catenary::schema::gtfs::routes::dsl::routes
            .filter(catenary::schema::gtfs::routes::chateau.eq(chateau_id_clone.clone()))
            .filter(catenary::schema::gtfs::routes::route_id.eq_any(&route_ids))
            .select(catenary::models::Route::as_select())
            .load::<catenary::models::Route>(&mut conn1)
            .await
            .unwrap_or_default();
        println!("PERF: routes fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

        let mut routes_btreemap = BTreeMap::<String, catenary::models::Route>::new();
        for route in &routes_ret {
            routes_btreemap.insert(route.route_id.clone().into(), route.clone());
        }

        let agency_ids: Vec<String> = routes_ret
            .iter()
            .filter_map(|x| x.agency_id.clone())
            .collect();

        let agencies_ret = catenary::schema::gtfs::agencies::dsl::agencies
            .filter(catenary::schema::gtfs::agencies::chateau.eq(chateau_id_clone.clone()))
            .filter(catenary::schema::gtfs::agencies::agency_id.eq_any(&agency_ids))
            .select(catenary::models::Agency::as_select())
            .load::<catenary::models::Agency>(&mut conn1)
            .await
            .unwrap_or_default();
        println!("PERF: agencies fetch took {}ms", t_section.elapsed().as_millis());

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

        let trips = if use_parallel_fetching {
            let chunks: Vec<Vec<String>> = itinerary_list_clone
                .chunks(chunk_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            let futures_trips = chunks.into_iter().map(|chunk| {
                let pool = pool_for_schedule.clone();
                let chateau = chateau_id_clone2.clone();
                async move {
                    let mut conn = pool.get().await.unwrap();
                    catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                        .filter(catenary::schema::gtfs::trips_compressed::chateau.eq(chateau))
                        .filter(
                            catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                                .eq_any(chunk),
                        )
                        .select(catenary::models::CompressedTrip::as_select())
                        .load::<catenary::models::CompressedTrip>(&mut conn)
                        .await
                        .unwrap_or_default()
                }
            });

            let results: Vec<Vec<catenary::models::CompressedTrip>> =
                futures::stream::iter(futures_trips)
                    .buffer_unordered(10) // Allow up to 10 concurrent connections
                    .collect()
                    .await;

            results.into_iter().flatten().collect::<Vec<_>>()
        } else {
            let mut conn2 = pool_for_schedule.get().await.unwrap();
            catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                .filter(
                    catenary::schema::gtfs::trips_compressed::chateau.eq(chateau_id_clone2.clone()),
                )
                .filter(
                    catenary::schema::gtfs::trips_compressed::itinerary_pattern_id
                        .eq_any(&itinerary_list_clone),
                )
                .select(catenary::models::CompressedTrip::as_select())
                .load::<catenary::models::CompressedTrip>(&mut conn2)
                .await
                .unwrap_or_default()
        };
        println!("PERF: trips_compressed fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

        let mut conn_calendar = pool_for_schedule.get().await.unwrap();

        let mut trip_compressed_btreemap =
            BTreeMap::<String, catenary::models::CompressedTrip>::new();
        let mut service_ids_to_search: BTreeSet<CompactString> = BTreeSet::new();

        for trip in &trips {
            trip_compressed_btreemap.insert(trip.trip_id.clone(), trip.clone());
            service_ids_to_search.insert(trip.service_id.clone());
        }

        let calendar = catenary::schema::gtfs::calendar::dsl::calendar
            .filter(catenary::schema::gtfs::calendar::chateau.eq(chateau_id_clone2.clone()))
            .filter(catenary::schema::gtfs::calendar::service_id.eq_any(&service_ids_to_search))
            .select(catenary::models::Calendar::as_select())
            .load::<catenary::models::Calendar>(&mut conn_calendar)
            .await
            .unwrap_or_default();
        println!("PERF: calendar fetch took {}ms", t_section.elapsed().as_millis());
        let t_section = std::time::Instant::now();

        let calendar_dates = catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
            .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(chateau_id_clone2.clone()))
            .filter(
                catenary::schema::gtfs::calendar_dates::service_id.eq_any(&service_ids_to_search),
            )
            .select(catenary::models::CalendarDate::as_select())
            .load::<catenary::models::CalendarDate>(&mut conn_calendar)
            .await
            .unwrap_or_default();
        println!("PERF: calendar_dates fetch took {}ms", t_section.elapsed().as_millis());

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
    
    println!("PERF: TOTAL fetch_stop_data_for_chateau took {}ms", global_start.elapsed().as_millis());

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
    pub itinerary_options: Vec<ItinOption>,
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

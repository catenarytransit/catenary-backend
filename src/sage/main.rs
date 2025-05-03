#![deny(
    clippy::mutable_key_type,
    clippy::map_entry,
    clippy::boxed_local,
    clippy::let_unit_value,
    clippy::redundant_allocation,
    clippy::bool_comparison,
    clippy::bind_instead_of_map,
    clippy::vec_box,
    clippy::while_let_loop,
    clippy::useless_asref,
    clippy::repeat_once,
    clippy::deref_addrof,
    clippy::suspicious_map,
    clippy::arc_with_non_send_sync,
    clippy::single_char_pattern,
    clippy::for_kv_map,
    clippy::let_unit_value,
    clippy::let_and_return,
    clippy::iter_nth,
    clippy::iter_cloned_collect,
    clippy::bytes_nth,
    clippy::deprecated_clippy_cfg_attr,
    clippy::match_result_ok,
    clippy::cmp_owned,
    clippy::cmp_null,
    clippy::op_ref,
    clippy::useless_vec,
    clippy::module_inception
)]

use catenary::models::Route;
use catenary::models::Stop;
use catenary::postgres_tools::CatenaryPostgresPool;
use catenary::postgres_tools::make_async_pool;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;
use diesel_async::{AsyncConnection, RunQueryDsl};
use geo::Contains;
use geo::Polygon;
use geojson::Feature;
use serde::Serialize;
use std::fs::File;
use std::io::IoSlice;
use std::io::prelude::*;
use std::str::FromStr;
use std::sync::Arc;
mod route_family;
use std::collections::BTreeSet;

#[derive(Serialize)]
struct StopGeo {
    name: String,
    chateau: String,
    // onestop_feed_id: String,
    displayname: Option<String>,
    routes: Vec<String>,
    route_types: Vec<i16>,
    point: geo::Point<f64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Hello, Catenary Sage!");
    // get connection pool from database pool
    let conn_pool: CatenaryPostgresPool = make_async_pool().await?;
    let arc_conn_pool: Arc<CatenaryPostgresPool> = Arc::new(conn_pool);

    //connect to postgres
    let conn_pool = arc_conn_pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    println!("Connected to Postgres");

    let excluded_chateaux = BTreeSet::from_iter([
        "uc~irvine~anteater~express",
        "lagunabeachtransit",
        "greyhound~flix",
    ]);

    //https://www.sos.ca.gov/state-holidays

    let holidays = BTreeSet::from_iter([
        chrono::NaiveDate::from_ymd_opt(2025, 3, 31).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 5, 26).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 7, 4).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 9, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 11, 11).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 11, 27).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 11, 28).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 12, 24).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2025, 12, 25).unwrap(),
    ]);

    //import california polygon from sage/California_State_Boundary.geojson
    let california_geojson =
        std::fs::read_to_string("./src/sage/California_State_Boundary.geojson").unwrap();
    let california_geojson = geojson::GeoJson::from_str(&california_geojson).unwrap();
    //save multipolygon
    let california_geojson_multipolygon: geo::MultiPolygon = match california_geojson {
        geojson::GeoJson::FeatureCollection(collection) => {
            let feature = &collection.features[0];
            let geometry = &feature.geometry;
            match geometry {
                Some(geometry) => match &geometry.value {
                    geojson::Value::Polygon(_) => {
                        panic!("Expected a multipolygon")
                    }
                    geojson::Value::MultiPolygon(multipolygon) => {
                        geo::MultiPolygon::from_iter(multipolygon.iter().map(|a| {
                            Polygon::new(
                                geo::LineString::from_iter(
                                    a[0].iter().map(|b| geo::Point::new(b[0], b[1])),
                                ),
                                vec![],
                            )
                        }))
                    }
                    _ => panic!("Expected a multipolygon"),
                },
                _ => panic!("Expected a polygon"),
            }
        }
        _ => panic!("Expected a feature collection"),
    };

    //query all stops in bbox of california -124.41060660766607,32.5342307609976,-114.13445790587905,42.00965914828148

    let start_stops_timer = std::time::Instant::now();

    let stops_query
    = catenary::schema::gtfs::stops::dsl::stops
    .filter(sql::<Bool>("ST_Contains(ST_MakeEnvelope(-124.41060660766607,32.5342307609976,-114.13445790587905,42.00965914828148, 4326), point) AND chateau != 'greyhound~flix' AND chateau != 'lagunabeachtransit' AND chateau != 'uc~irvine~anteater~express'"))
    .select(Stop::as_select())
    .get_results::<Stop>(conn)
    .await?;

    let duration_stops = start_stops_timer.elapsed();

    println!("Duration to query rail stops: {:?}", duration_stops);

    println!("Stops Query # {}", stops_query.len());

    //filter stops to only include stops that are within the california polygon

    let start_stops_filter_timer = std::time::Instant::now();

    let stops_geo: Vec<_> = stops_query
        .into_iter()
        .filter(|stop| stop.point.is_some())
        .map(|input| StopGeo {
            name: input.name.unwrap_or("".to_string()),
            chateau: input.chateau,
            //     onestop_feed_id: input.onestop_feed_id,
            displayname: input.displayname,
            routes: input
                .routes
                .iter()
                .filter(|a| a.is_some())
                .map(|a| a.as_ref().unwrap().clone())
                .collect(),
            route_types: input
                .route_types
                .iter()
                .filter(|a| a.is_some())
                .map(|a| a.as_ref().unwrap().clone())
                .collect(),
            point: geo::Point::new(input.point.unwrap().x, input.point.unwrap().y),
        })
        .collect::<Vec<StopGeo>>();

    let stops_query_filtered: Vec<_> = stops_geo
        .into_iter()
        .filter(|stop| california_geojson_multipolygon.contains(&stop.point))
        .collect();

    println!("Filtering took {:?}", start_stops_filter_timer.elapsed());
    println!("Stops Query Filtered {}", stops_query_filtered.len());

    //write out to json

    let start_stops_write_timer = std::time::Instant::now();

    let rail_stops = stops_query_filtered
        .iter()
        .filter(|stop| {
            stop.route_types.contains(&2)
                || stop.route_types.contains(&0)
                || stop.route_types.contains(&1)
        })
        .collect::<Vec<&StopGeo>>();

    println!("Rail Stops Query Filtered # {}", rail_stops.len());

    let stops_rail_query_filtered_json = serde_json::to_string(&rail_stops).unwrap();

    let mut buffer = File::create("./src/sage/rail_stops_query_filtered.json")?;

    buffer
        .write_all(stops_rail_query_filtered_json.as_bytes())
        .unwrap();

    println!("Writing took {:?}", start_stops_write_timer.elapsed());

    //create list of chateaus to check

    let chateaux_list = stops_query_filtered
        .iter()
        .map(|stop| stop.chateau.clone())
        .collect::<BTreeSet<String>>();

    println!("Chateaux List  {}", chateaux_list.len());

    for chateau in chateaux_list {
        if excluded_chateaux.contains(chateau.as_str()) {
            continue;
        }

        let chateau_stops = stops_query_filtered
            .iter()
            .filter(|stop| stop.chateau == chateau)
            .collect::<Vec<&StopGeo>>();

        //get all routes for chateau

        let chateau_routes = chateau_stops
            .iter()
            .flat_map(|stop| stop.routes.iter())
            .collect::<BTreeSet<&String>>();

        println!("Chateau Routes  {} under {}", chateau_routes.len(), chateau);

        if chateau_routes.len() > 1 {
            //query routes of chateau from database

            let routes = catenary::schema::gtfs::routes::dsl::routes
                .filter(catenary::schema::gtfs::routes::chateau.eq(&chateau))
                .select(Route::as_select())
                .get_results::<Route>(conn)
                .await?;

            //query calendar of chateau from database

            let calendar = catenary::schema::gtfs::calendar::dsl::calendar
                .filter(catenary::schema::gtfs::calendar::chateau.eq(&chateau))
                .select(catenary::models::Calendar::as_select())
                .get_results::<catenary::models::Calendar>(conn)
                .await?;

            //query calendar_dates of chateau from database

            let calendar_dates = catenary::schema::gtfs::calendar_dates::dsl::calendar_dates
                .filter(catenary::schema::gtfs::calendar_dates::chateau.eq(&chateau))
                .select(catenary::models::CalendarDate::as_select())
                .get_results::<catenary::models::CalendarDate>(conn)
                .await?;

            //make calendar gtfs_structure

            let calendar_structure =
                catenary::make_calendar_structure_from_pg_single_chateau(calendar, calendar_dates);

            let chateau_routes = routes
                .into_iter()
                .filter(|route| chateau_routes.contains(&&route.route_id))
                .collect::<Vec<Route>>();

            // test each direction if there is 20 min frequency of each route

            for route in chateau_routes.iter().filter(|x| x.route_type == 3) {
                //query direction meta of route from database

                let direction_meta =
                    catenary::schema::gtfs::direction_pattern_meta::dsl::direction_pattern_meta
                        .filter(
                            catenary::schema::gtfs::direction_pattern_meta::route_id
                                .eq(&route.route_id),
                        )
                        .select(catenary::models::DirectionPatternMeta::as_select())
                        .get_results::<catenary::models::DirectionPatternMeta>(conn)
                        .await?;

                // query itin meta

                let itin_meta =
                    catenary::schema::gtfs::itinerary_pattern_meta::dsl::itinerary_pattern_meta
                        .filter(
                            catenary::schema::gtfs::itinerary_pattern_meta::route_id
                                .eq(&route.route_id),
                        )
                        .select(catenary::models::ItineraryPatternMeta::as_select())
                        .get_results::<catenary::models::ItineraryPatternMeta>(conn)
                        .await?;

                let itinerary_list = itin_meta
                    .iter()
                    .map(|itin| itin.itinerary_pattern_id.clone())
                    .collect::<Vec<String>>();

                // query itin stops

                let itin_stops = catenary::schema::gtfs::itinerary_pattern::dsl::itinerary_pattern
                    .filter(
                        catenary::schema::gtfs::itinerary_pattern::itinerary_pattern_id
                            .eq_any(&itinerary_list),
                    )
                    .select(catenary::models::ItineraryPatternRow::as_select())
                    .get_results::<catenary::models::ItineraryPatternRow>(conn)
                    .await?;

                // query trips compressed

                let trips_compressed =
                    catenary::schema::gtfs::trips_compressed::dsl::trips_compressed
                        .filter(
                            catenary::schema::gtfs::trips_compressed::route_id.eq(&route.route_id),
                        )
                        .select(catenary::models::CompressedTrip::as_select())
                        .get_results::<catenary::models::CompressedTrip>(conn)
                        .await?;

                //test direction each by each

                for direction in direction_meta {}
            }
        }
    }

    Ok(())
}

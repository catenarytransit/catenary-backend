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
    .filter(sql::<Bool>("ST_Contains(ST_MakeEnvelope(-124.41060660766607,32.5342307609976,-114.13445790587905,42.00965914828148, 4326), point) AND location_type = 0"))
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
    println!("Stops Query Filtered # {}", stops_query_filtered.len());

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

    Ok(())
}

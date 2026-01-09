// Copyright
// Catenary Transit Initiatives
// OSM Station Lookup Endpoint
// Attribution cannot be removed

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use actix_web::web::Query;
use catenary::postgres_tools::CatenaryPostgresPool;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use diesel::query_dsl::methods::FilterDsl;
use diesel::query_dsl::methods::SelectDsl;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Deserialize, Clone, Debug)]
pub struct OsmStationLookupQuery {
    pub chateau_id: String,
    pub gtfs_stop_id: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct OsmStationLookupResponse {
    pub found: bool,
    pub gtfs_stop_id: String,
    pub chateau_id: String,
    pub osm_station_id: Option<i64>,
    pub osm_platform_id: Option<i64>,
    pub osm_station_info: Option<OsmStationInfo>,
}

#[derive(Serialize, Clone, Debug)]
pub struct OsmStationInfo {
    pub osm_id: i64,
    pub osm_type: String,
    pub name: Option<String>,
    pub name_translations: Option<serde_json::Value>,
    pub station_type: Option<String>,
    pub railway_tag: Option<String>,
    pub mode_type: String,
    pub uic_ref: Option<String>,
    pub ref_: Option<String>,
    pub wikidata: Option<String>,
    pub operator: Option<String>,
    pub network: Option<String>,
    pub level: Option<String>,
    pub local_ref: Option<String>,
    pub lat: f64,
    pub lon: f64,
}

#[actix_web::get("/osm_station_lookup")]
pub async fn osm_station_lookup(
    req: HttpRequest,
    query: Query<OsmStationLookupQuery>,
    pool: web::Data<Arc<CatenaryPostgresPool>>,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;

    let conn = match conn_pre {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Database connection error: {:?}", e);
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Database connection failed"}));
        }
    };

    let mut conn = conn;

    // Query the stop to get its osm_station_id
    let stop_result: diesel::prelude::QueryResult<Vec<catenary::models::Stop>> =
        catenary::schema::gtfs::stops::dsl::stops
            .filter(catenary::schema::gtfs::stops::chateau.eq(&query.chateau_id))
            .filter(catenary::schema::gtfs::stops::gtfs_id.eq(&query.gtfs_stop_id))
            .select(catenary::models::Stop::as_select())
            .load::<catenary::models::Stop>(&mut conn)
            .await;

    let stops = match stop_result {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Stop query error: {:?}", e);
            return HttpResponse::InternalServerError()
                .json(serde_json::json!({"error": "Failed to query stop"}));
        }
    };

    if stops.is_empty() {
        return HttpResponse::Ok().json(OsmStationLookupResponse {
            found: false,
            gtfs_stop_id: query.gtfs_stop_id.clone(),
            chateau_id: query.chateau_id.clone(),
            osm_station_id: None,
            osm_platform_id: None,
            osm_station_info: None,
        });
    }

    let stop = &stops[0];
    let osm_station_id = stop.osm_station_id;
    let osm_platform_id = stop.osm_platform_id;

    // If there's no OSM station ID, return early
    if osm_station_id.is_none() {
        return HttpResponse::Ok().json(OsmStationLookupResponse {
            found: true,
            gtfs_stop_id: query.gtfs_stop_id.clone(),
            chateau_id: query.chateau_id.clone(),
            osm_station_id: None,
            osm_platform_id,
            osm_station_info: None,
        });
    }

    let osm_id = osm_station_id.unwrap();

    // Fetch OSM station details
    let osm_station_result: diesel::prelude::QueryResult<Vec<catenary::models::OsmStation>> =
        catenary::schema::gtfs::osm_stations::dsl::osm_stations
            .filter(catenary::schema::gtfs::osm_stations::osm_id.eq(osm_id))
            .select(catenary::models::OsmStation::as_select())
            .load::<catenary::models::OsmStation>(&mut conn)
            .await;

    let osm_station_info = match osm_station_result {
        Ok(stations) if !stations.is_empty() => {
            let station = &stations[0];
            Some(OsmStationInfo {
                osm_id: station.osm_id,
                osm_type: station.osm_type.clone(),
                name: station.name.clone(),
                name_translations: station.name_translations.clone(),
                station_type: station.station_type.clone(),
                railway_tag: station.railway_tag.clone(),
                mode_type: station.mode_type.clone(),
                uic_ref: station.uic_ref.clone(),
                ref_: station.ref_.clone(),
                wikidata: station.wikidata.clone(),
                operator: station.operator.clone(),
                network: station.network.clone(),
                level: station.level.clone(),
                local_ref: station.local_ref.clone(),
                lat: station.point.y,
                lon: station.point.x,
            })
        }
        _ => None,
    };

    HttpResponse::Ok().json(OsmStationLookupResponse {
        found: true,
        gtfs_stop_id: query.gtfs_stop_id.clone(),
        chateau_id: query.chateau_id.clone(),
        osm_station_id,
        osm_platform_id,
        osm_station_info,
    })
}
